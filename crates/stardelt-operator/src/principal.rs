//! The `ServicePrincipal` controller — phase 1: validate the spec, look for the credentials Secret
//! and report in the status. It does **not** talk to the identity provider: until that decision is
//! made, an administrator creates the client with `make sync-principal` (stardelt-platform#244).
//!
//! The decisions that matter are pure functions ([`secret_check`], [`principal_state`]) so they are
//! tested without a cluster; [`reconcile_principal`] only reads the Secret and writes the status.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, ResourceExt,
    api::{ListParams, Patch, PatchParams},
    runtime::controller::Action,
};
use serde_json::json;

use crate::connection::ready_condition;
use crate::controller::Context;
use crate::crd::{
    IdentityMode, PlatformInstance, ServicePrincipal, ServicePrincipalSpec, ServicePrincipalStatus,
    principal_client_id,
};
use crate::principal_keycloak::{ClientOutcome, delete_principal_client, ensure_principal_client};

pub const PRINCIPAL_FINALIZER: &str = "stardelt.io/principal-keycloak";

/// What the credentials Secret holds, by key name only: values are never read into messages.
#[derive(Debug, PartialEq, Eq)]
pub enum SecretCheck {
    Missing,
    MissingKeys(Vec<&'static str>),
    Present { client_id: String },
}

/// Read what the credentials Secret holds. Only whether a key is present and non-empty matters;
/// values are carried no further than the `client-id` comparison in [`principal_state`].
pub fn secret_check(secret: Option<&Secret>) -> SecretCheck {
    let Some(secret) = secret else {
        return SecretCheck::Missing;
    };
    let text = |key: &str| -> Option<String> {
        let bytes = secret.data.as_ref()?.get(key)?;
        let text = String::from_utf8_lossy(&bytes.0).trim().to_string();
        (!text.is_empty()).then_some(text)
    };
    let client_id = text("client-id");
    let client_secret = text("client-secret");
    let missing: Vec<&'static str> = [
        ("client-id", client_id.is_none()),
        ("client-secret", client_secret.is_none()),
    ]
    .into_iter()
    .filter_map(|(key, missing)| missing.then_some(key))
    .collect();
    if !missing.is_empty() {
        return SecretCheck::MissingKeys(missing);
    }
    SecretCheck::Present {
        client_id: client_id.expect("checked above"),
    }
}

/// What the controller reports for a principal.
#[derive(Debug, PartialEq, Eq)]
pub struct PrincipalState {
    pub ready: bool,
    pub reason: &'static str,
    pub message: Option<String>,
    pub client_id: Option<String>,
    pub secret_name: Option<String>,
    pub subject: Option<String>,
}

fn not_ready(reason: &'static str, message: String) -> PrincipalState {
    PrincipalState {
        ready: false,
        reason,
        message: Some(message),
        client_id: None,
        secret_name: None,
        subject: None,
    }
}

fn incomplete(secret_name: &str, keys: &[&'static str]) -> PrincipalState {
    let named: Vec<String> = keys.iter().map(|key| format!("`{key}`")).collect();
    not_ready(
        "SecretIncomplete",
        format!(
            "the Secret {secret_name} must hold `client-id` and `client-secret`, both non-empty; \
             missing or empty: {}",
            named.join(", ")
        ),
    )
}

/// The state of the principal `name`.
///
/// Platform mode (no `existingSecretRef`): the Secret `sp-<name>` must hold a `client-id` equal to
/// `principal_client_id(name)`. External mode: the Secret named in `existingSecretRef` must hold
/// both keys, and the identity provider's administrator owns its `client-id`.
pub fn principal_state(
    spec: &ServicePrincipalSpec,
    name: &str,
    check: &SecretCheck,
) -> PrincipalState {
    if let Err(message) = spec.validate(name) {
        return not_ready("InvalidSpec", message);
    }
    match &spec.existing_secret_ref {
        Some(reference) => {
            let secret_name = reference.name.as_str();
            match check {
                SecretCheck::Missing => not_ready(
                    "ExternalSecretMissing",
                    format!(
                        "the Secret {secret_name} named in existingSecretRef does not exist; create \
                         it with the app registration's `client-id` and `client-secret`"
                    ),
                ),
                SecretCheck::MissingKeys(keys) => incomplete(secret_name, keys),
                SecretCheck::Present { .. } => PrincipalState {
                    ready: true,
                    reason: "Provisioned",
                    message: None,
                    client_id: None,
                    secret_name: Some(secret_name.to_string()),
                    subject: None,
                },
            }
        }
        None => {
            let secret_name = principal_client_id(name);
            match check {
                SecretCheck::Missing => not_ready(
                    "ClientNotSetUp",
                    format!(
                        "the identity provider has no client for this principal yet; create it with \
                         `make sync-principal NAME={name} GROUP={}`",
                        spec.owner_group
                    ),
                ),
                SecretCheck::MissingKeys(keys) => incomplete(&secret_name, keys),
                SecretCheck::Present { client_id } if client_id != &secret_name => not_ready(
                    "ClientIdMismatch",
                    format!(
                        "the `client-id` in Secret {secret_name} is not `{secret_name}`: the Secret \
                         belongs to another client; recreate it with `make sync-principal NAME={name} \
                         GROUP={}`",
                        spec.owner_group
                    ),
                ),
                SecretCheck::Present { .. } => PrincipalState {
                    ready: true,
                    reason: "Provisioned",
                    message: None,
                    client_id: Some(secret_name.clone()),
                    secret_name: Some(secret_name),
                    subject: None,
                },
            }
        }
    }
}

/// Attach the identity provider's subject the platform-mode converge reported.
/// Only a ready principal reports one: the field is what Nova maps to `spec.ownerGroup`,
/// and a half-set-up principal must not claim an owner.
pub fn with_subject(mut state: PrincipalState, subject: Option<String>) -> PrincipalState {
    state.subject = if state.ready { subject } else { None };
    state
}

/// Reconcile one `ServicePrincipal`: validate, check the credentials Secret, write the status.
///
/// In `identity.mode: platform` (and without `existingSecretRef`) the operator also converges
/// the Keycloak client and its Secret. External mode and a CR with `existingSecretRef` keep the
/// Secret-check-only behaviour. A Secret value is never put into a message, an event or the status.
pub async fn reconcile_principal(
    sp: Arc<ServicePrincipal>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    if sp.metadata.deletion_timestamp.is_some() {
        return finalize_principal(&sp, &ctx).await;
    }
    let name = sp.name_any();
    let namespace = sp.namespace().unwrap_or_else(|| "default".into());

    // Platform mode creates the client; external mode and existingSecretRef do not.
    let instance = Api::<PlatformInstance>::all(ctx.client.clone())
        .list(&ListParams::default())
        .await?
        .items
        .into_iter()
        .find(|pi| pi.spec.namespace == namespace);
    let platform_mode = sp.spec.existing_secret_ref.is_none()
        && instance
            .as_ref()
            .is_some_and(|pi| pi.spec.identity.mode == IdentityMode::Platform);

    let mut subject: Option<String> = None;
    if platform_mode {
        if let Err(message) = sp.spec.validate(&name) {
            return patch_principal_status(
                &sp,
                &ctx,
                &namespace,
                &name,
                not_ready("InvalidSpec", message),
            )
            .await;
        }
        match ensure_principal_client(&ctx, &namespace, &name, &sp.spec).await {
            ClientOutcome::OwnerGroupMissing { group } => {
                return patch_principal_status(
                    &sp,
                    &ctx,
                    &namespace,
                    &name,
                    not_ready(
                        "OwnerGroupMissing",
                        format!(
                            "group `{group}` does not exist in the identity provider — create it there, or fix spec.ownerGroup"
                        ),
                    ),
                )
                .await;
            }
            ClientOutcome::Unavailable { message } => {
                return patch_principal_status(
                    &sp,
                    &ctx,
                    &namespace,
                    &name,
                    not_ready("KeycloakUnavailable", message),
                )
                .await;
            }
            ClientOutcome::Created { subject: s, .. }
            | ClientOutcome::Converged { subject: s, .. } => {
                subject = Some(s);
            }
        }
    }

    let secret_name = match &sp.spec.existing_secret_ref {
        Some(reference) => reference.name.clone(),
        None => principal_client_id(&name),
    };
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &namespace);
    let state = match secrets.get_opt(&secret_name).await {
        Ok(secret) => principal_state(&sp.spec, &name, &secret_check(secret.as_ref())),
        // Reading every Secret is not the operator's right; the chart grants `get` on Secrets in
        // its own namespace only (Role `stardelt-operator-principals`).
        Err(kube::Error::Api(response)) if response.code == 403 => not_ready(
            "CannotReadSecret",
            format!(
                "the operator may not read Secrets in this namespace: allow `get` on Secret \
                 {secret_name} (Role stardelt-operator-principals)"
            ),
        ),
        Err(error) => return Err(error),
    };
    let state = with_subject(state, subject);

    if state.ready {
        ensure_principal_finalizer(&sp, &ctx).await?;
    }
    patch_principal_status(&sp, &ctx, &namespace, &name, state).await
}

async fn finalize_principal(sp: &ServicePrincipal, ctx: &Context) -> Result<Action, kube::Error> {
    let name = sp.name_any();
    let namespace = sp.namespace().unwrap_or_else(|| "default".into());
    match delete_principal_client(ctx, &namespace, &name).await {
        Err(message) => {
            tracing::warn!(principal = %name, %namespace, %message, "finalize failed");
            Ok(Action::requeue(Duration::from_secs(30)))
        }
        Ok(()) => {
            let principals: Api<ServicePrincipal> = Api::namespaced(ctx.client.clone(), &namespace);
            let finalizers: Vec<String> = sp
                .finalizers()
                .iter()
                .filter(|f| f.as_str() != PRINCIPAL_FINALIZER)
                .cloned()
                .collect();
            principals
                .patch(
                    &name,
                    &PatchParams::default(),
                    &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
                )
                .await?;
            Ok(Action::await_change())
        }
    }
}

async fn ensure_principal_finalizer(
    sp: &ServicePrincipal,
    ctx: &Context,
) -> Result<(), kube::Error> {
    if sp.finalizers().iter().any(|f| f == PRINCIPAL_FINALIZER) {
        return Ok(());
    }
    let name = sp.name_any();
    let namespace = sp.namespace().unwrap_or_else(|| "default".into());
    let mut finalizers = sp.finalizers().to_vec();
    finalizers.push(PRINCIPAL_FINALIZER.to_string());
    let principals: Api<ServicePrincipal> = Api::namespaced(ctx.client.clone(), &namespace);
    principals
        .patch(
            &name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
        )
        .await?;
    Ok(())
}

async fn patch_principal_status(
    sp: &ServicePrincipal,
    ctx: &Context,
    namespace: &str,
    name: &str,
    state: PrincipalState,
) -> Result<Action, kube::Error> {
    let now = k8s_openapi::jiff::Timestamp::now().to_string();
    let previous = sp
        .status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"));
    let status = ServicePrincipalStatus {
        ready: state.ready,
        client_id: state.client_id.clone(),
        secret_name: state.secret_name.clone(),
        subject: state.subject.clone(),
        conditions: vec![ready_condition(
            state.ready,
            state.reason,
            state.message.clone(),
            sp.metadata.generation,
            previous,
            &now,
        )],
        observed_generation: sp.metadata.generation,
    };
    let principals: Api<ServicePrincipal> = Api::namespaced(ctx.client.clone(), namespace);
    principals
        .patch_status(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await?;
    tracing::info!(
        principal = %name,
        %namespace,
        ready = state.ready,
        reason = state.reason,
        "reconciled"
    );
    Ok(Action::requeue(Duration::from_secs(if state.ready {
        60
    } else {
        30
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::ByteString;

    /// A value that must never be readable from a message.
    const CANARY: &str = "canary-7f3a";

    fn spec(value: serde_json::Value) -> ServicePrincipalSpec {
        let mut base = json!({
            "ownerGroup": "analytics",
            "audiences": ["trino"],
        });
        for (k, v) in value.as_object().unwrap() {
            if v.is_null() {
                base.as_object_mut().unwrap().remove(k);
            } else {
                base[k] = v.clone();
            }
        }
        serde_json::from_value(base).expect("spec")
    }

    fn secret(name: &str, entries: &[(&str, &str)]) -> Secret {
        let mut secret = Secret::default();
        secret.metadata.name = Some(name.into());
        secret.metadata.namespace = Some("stardelt".into());
        secret.data = Some(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), ByteString(value.as_bytes().to_vec())))
                .collect(),
        );
        secret
    }

    fn healthy(name: &str) -> Secret {
        secret(name, &[("client-id", name), ("client-secret", CANARY)])
    }

    #[test]
    fn platform_mode_ready_when_secret_has_both_keys() {
        let check = secret_check(Some(&healthy("sp-etl")));
        let state = principal_state(&spec(json!({})), "etl", &check);
        assert!(state.ready, "{state:?}");
        assert_eq!(state.reason, "Provisioned");
        assert_eq!(state.message, None);
        assert_eq!(state.client_id.as_deref(), Some("sp-etl"));
        assert_eq!(state.secret_name.as_deref(), Some("sp-etl"));
    }

    #[test]
    fn only_a_ready_principal_reports_a_subject() {
        let check = secret_check(Some(&healthy("sp-etl")));
        let ready = principal_state(&spec(json!({})), "etl", &check);
        let state = with_subject(ready, Some("e385bcb7-9484-48b9-8d60-8f509fc6cbd9".into()));
        assert!(state.ready, "{state:?}");
        assert_eq!(
            state.subject.as_deref(),
            Some("e385bcb7-9484-48b9-8d60-8f509fc6cbd9")
        );

        let unset = principal_state(&spec(json!({})), "etl", &secret_check(None));
        let state = with_subject(unset, Some("e385bcb7-9484-48b9-8d60-8f509fc6cbd9".into()));
        assert!(!state.ready, "{state:?}");
        assert_eq!(
            state.subject, None,
            "a principal that is not ready claims no owner"
        );
    }

    #[test]
    fn platform_mode_secret_absent_says_how_to_create_it() {
        let state = principal_state(&spec(json!({})), "etl", &secret_check(None));
        assert!(!state.ready);
        assert_eq!(state.reason, "ClientNotSetUp");
        let message = state.message.expect("a message");
        assert!(
            message.contains("make sync-principal NAME=etl GROUP=analytics"),
            "{message}"
        );
    }

    #[test]
    fn empty_client_secret_is_incomplete_and_names_the_key() {
        let check = secret_check(Some(&secret(
            "sp-etl",
            &[("client-id", "sp-etl"), ("client-secret", "")],
        )));
        let state = principal_state(&spec(json!({})), "etl", &check);
        assert!(!state.ready);
        assert_eq!(state.reason, "SecretIncomplete");
        let message = state.message.expect("a message");
        assert!(message.contains("client-secret"), "{message}");
    }

    #[test]
    fn client_id_must_match_the_principal_name() {
        let check = secret_check(Some(&secret(
            "sp-etl",
            &[("client-id", "sp-other"), ("client-secret", CANARY)],
        )));
        let state = principal_state(&spec(json!({})), "etl", &check);
        assert!(!state.ready);
        assert_eq!(state.reason, "ClientIdMismatch");
        let message = state.message.expect("a message");
        assert!(message.contains("sp-etl"), "{message}");
        assert!(!message.contains("sp-other"), "{message}");
    }

    #[test]
    fn invalid_name_is_reported_with_the_validate_error() {
        let spec = spec(json!({}));
        let expected = spec.validate("Etl").expect_err("an invalid name");
        let state = principal_state(&spec, "Etl", &secret_check(None));
        assert!(!state.ready);
        assert_eq!(state.reason, "InvalidSpec");
        let message = state.message.expect("a message");
        assert!(message.contains(&expected), "{message}");
    }

    #[test]
    fn external_mode_ready_uses_the_referenced_secret() {
        let spec = spec(json!({ "existingSecretRef": { "name": "external-app" } }));
        let check = secret_check(Some(&secret(
            "external-app",
            &[
                ("client-id", "app-registration-id"),
                ("client-secret", CANARY),
            ],
        )));
        let state = principal_state(&spec, "etl", &check);
        assert!(state.ready, "{state:?}");
        assert_eq!(state.reason, "Provisioned");
        assert_eq!(state.message, None);
        assert_eq!(state.client_id, None);
        assert_eq!(state.secret_name.as_deref(), Some("external-app"));
    }

    #[test]
    fn external_mode_missing_secret_names_it() {
        let spec = spec(json!({ "existingSecretRef": { "name": "external-app" } }));
        let state = principal_state(&spec, "etl", &secret_check(None));
        assert!(!state.ready);
        assert_eq!(state.reason, "ExternalSecretMissing");
        let message = state.message.expect("a message");
        assert!(message.contains("external-app"), "{message}");
    }

    #[test]
    fn no_secret_value_ever_reaches_a_message() {
        let platform = spec(json!({}));
        let external = spec(json!({ "existingSecretRef": { "name": "external-app" } }));
        let checked = [
            (
                principal_state(&platform, "etl", &secret_check(Some(&healthy("sp-etl")))),
                "platform ready",
            ),
            (
                principal_state(
                    &platform,
                    "etl",
                    &secret_check(Some(&secret(
                        "sp-etl",
                        &[("client-id", "sp-etl"), ("client-secret", CANARY)],
                    ))),
                ),
                "platform secret present",
            ),
            (
                principal_state(
                    &platform,
                    "etl",
                    &secret_check(Some(&secret(
                        "sp-etl",
                        &[("client-id", CANARY), ("client-secret", CANARY)],
                    ))),
                ),
                "platform client-id mismatch",
            ),
            (
                principal_state(
                    &external,
                    "etl",
                    &secret_check(Some(&secret(
                        "external-app",
                        &[("client-id", CANARY), ("client-secret", CANARY)],
                    ))),
                ),
                "external ready",
            ),
            (
                principal_state(
                    &external,
                    "etl",
                    &secret_check(Some(&secret(
                        "external-app",
                        &[("client-id", CANARY), ("client-secret", "")],
                    ))),
                ),
                "external incomplete",
            ),
            (
                principal_state(
                    &platform,
                    "Etl",
                    &secret_check(Some(&secret(
                        "sp-Etl",
                        &[("client-id", CANARY), ("client-secret", CANARY)],
                    ))),
                ),
                "invalid spec",
            ),
        ];
        for (state, case) in checked {
            if let Some(message) = state.message {
                assert!(
                    !message.contains(CANARY),
                    "{case} leaked a secret value: {message}"
                );
            }
        }
    }

    #[test]
    fn secret_check_ignores_whitespace_only_values() {
        let check = secret_check(Some(&secret(
            "sp-etl",
            &[("client-id", "  \n"), ("client-secret", "\t ")],
        )));
        assert_eq!(
            check,
            SecretCheck::MissingKeys(vec!["client-id", "client-secret"])
        );
    }

    /// Every request the mock client sees: method, path, body.
    type Recorded = Arc<std::sync::Mutex<Vec<(String, String, String)>>>;

    /// A kube client whose Secret read answers `403 Forbidden` and whose status patch echoes a
    /// minimal `ServicePrincipal`.
    fn forbidden_mock_client() -> (kube::Client, Recorded) {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    path.clone(),
                    String::from_utf8_lossy(&body).to_string(),
                ));
                let (status, document) = if method == "GET" && path.contains("/secrets/") {
                    (
                        403,
                        json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "status": "Failure",
                            "message": "secrets is forbidden",
                            "reason": "Forbidden",
                            "code": 403
                        }),
                    )
                } else if method == "GET" && path == "/apis/stardelt.io/v1alpha1/platforminstances"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "PlatformInstanceList",
                            "items": []
                        }),
                    )
                } else {
                    (
                        200,
                        json!({
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "ServicePrincipal",
                            "metadata": { "name": "etl", "namespace": "stardelt" },
                            "spec": { "ownerGroup": "analytics" }
                        }),
                    )
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        (kube::Client::new(service, "stardelt"), requests)
    }

    #[tokio::test]
    async fn a_forbidden_secret_read_is_reported_as_cannot_read_secret() {
        let (client, requests) = forbidden_mock_client();
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let principal: ServicePrincipal = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipal",
            "metadata": { "name": "etl", "namespace": "stardelt", "generation": 3 },
            "spec": { "ownerGroup": "analytics" }
        }))
        .expect("deserialize ServicePrincipal");

        let action = reconcile_principal(Arc::new(principal), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));

        let requests = requests.lock().expect("requests poisoned");
        let patch = requests
            .iter()
            .rev()
            .find(|(method, path, _)| method == "PATCH" && path.ends_with("/status"))
            .expect("status patch");
        let body: serde_json::Value = serde_json::from_str(&patch.2).expect("patch body is JSON");
        let status = &body["status"];
        assert_eq!(status["ready"], false);
        assert_eq!(status["observedGeneration"], 3);
        assert!(status.get("clientId").is_none(), "{status}");
        assert!(status.get("secretName").is_none(), "{status}");
        assert_eq!(status["conditions"][0]["reason"], "CannotReadSecret");
        let message = status["conditions"][0]["message"]
            .as_str()
            .expect("a message");
        assert!(
            message.contains("the operator may not read Secrets in this namespace"),
            "{message}"
        );
    }

    fn b64(s: &str) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = s.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let group = (u32::from(chunk[0]) << 16)
                | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                | u32::from(*chunk.get(2).unwrap_or(&0));
            for pos in 0..4 {
                if pos <= chunk.len() {
                    out.push(char::from(TABLE[((group >> (18 - 6 * pos)) & 63) as usize]));
                } else {
                    out.push('=');
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn external_mode_makes_no_keycloak_request() {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    path.clone(),
                    String::from_utf8_lossy(&body).to_string(),
                ));
                let document = if method == "GET" && path.contains("/platforminstances") {
                    json!({
                        "apiVersion": "stardelt.io/v1alpha1",
                        "kind": "PlatformInstanceList",
                        "items": [{
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "PlatformInstance",
                            "metadata": { "name": "demo" },
                            "spec": { "namespace": "stardelt", "identity": { "mode": "external", "issuerUrl": "https://id.example.com/realms/stardelt" } }
                        }]
                    })
                } else if method == "GET" && path.contains("/secrets/external-app") {
                    json!({
                        "apiVersion": "v1", "kind": "Secret",
                        "metadata": { "name": "external-app", "namespace": "stardelt" },
                        "data": { "client-id": b64("app-registration-id"), "client-secret": b64("secret") }
                    })
                } else {
                    json!({
                        "apiVersion": "stardelt.io/v1alpha1",
                        "kind": "ServicePrincipal",
                        "metadata": { "name": "etl", "namespace": "stardelt" },
                        "spec": { "ownerGroup": "analytics", "existingSecretRef": { "name": "external-app" } }
                    })
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(200)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        let client = kube::Client::new(service, "stardelt");
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let principal: ServicePrincipal = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipal",
            "metadata": { "name": "etl", "namespace": "stardelt", "generation": 1 },
            "spec": { "ownerGroup": "analytics", "existingSecretRef": { "name": "external-app" } }
        }))
        .expect("deserialize ServicePrincipal");

        let action = reconcile_principal(Arc::new(principal), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));

        let requests = requests.lock().expect("requests poisoned");
        assert!(
            requests
                .iter()
                .all(|(_, path, _)| !path.contains("/admin/")),
            "no Keycloak admin requests expected: {requests:?}"
        );
        let patch = requests
            .iter()
            .rev()
            .find(|(method, path, _)| method == "PATCH" && path.ends_with("/status"))
            .expect("status patch");
        let body: serde_json::Value = serde_json::from_str(&patch.2).expect("patch body is JSON");
        assert_eq!(body["status"]["ready"], true);
        assert_eq!(body["status"]["conditions"][0]["reason"], "Provisioned");
    }

    fn cm_env() -> serde_json::Value {
        json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": { "name": "keycloak-env", "namespace": "stardelt" },
            "data": { "KC_HOSTNAME": "https://id.data.example.com" }
        })
    }

    fn admin_secret() -> serde_json::Value {
        json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": { "name": "keycloak-admin", "namespace": "stardelt" },
            "data": { "username": b64("admin"), "password": b64("secretpass") }
        })
    }

    #[tokio::test]
    async fn deleting_a_principal_deletes_its_keycloak_client_and_secret() {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let uri = request.uri().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    path.clone(),
                    String::from_utf8_lossy(&body).to_string(),
                ));
                let (status, document) = if method == "GET"
                    && path == "/apis/stardelt.io/v1alpha1/platforminstances"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "PlatformInstanceList",
                            "items": [{
                                "apiVersion": "stardelt.io/v1alpha1",
                                "kind": "PlatformInstance",
                                "metadata": { "name": "demo" },
                                "spec": { "namespace": "stardelt", "identity": { "mode": "platform" } }
                            }]
                        }),
                    )
                } else if method == "GET" && path.contains("/configmaps/keycloak-env") {
                    (200, cm_env())
                } else if method == "GET" && path.contains("/secrets/keycloak-admin") {
                    (200, admin_secret())
                } else if uri.contains("/proxy/realms/master/protocol/openid-connect/token") {
                    (200, json!({"access_token": "tok"}))
                } else if method == "GET"
                    && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=sp-etl")
                {
                    (
                        200,
                        json!([{"id":"kc-1","attributes":{"stardelt-managed":"principal"}}]),
                    )
                } else if method == "DELETE"
                    && uri.contains("/proxy/admin/realms/stardelt/clients/kc-1")
                {
                    (200, json!({}))
                } else if method == "DELETE" && path == "/api/v1/namespaces/stardelt/secrets/sp-etl"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "v1", "kind": "Secret",
                            "metadata": { "name": "sp-etl", "namespace": "stardelt" }
                        }),
                    )
                } else if method == "PATCH"
                    && path
                        == "/apis/stardelt.io/v1alpha1/namespaces/stardelt/serviceprincipals/etl"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "ServicePrincipal",
                            "metadata": { "name": "etl", "namespace": "stardelt", "finalizers": [] },
                            "spec": { "ownerGroup": "analytics" }
                        }),
                    )
                } else if method == "DELETE" || method == "PUT" || method == "POST" {
                    (200, json!({}))
                } else {
                    (
                        404,
                        json!({"kind":"Status","code":404,"reason":"NotFound","message":"not found"}),
                    )
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        let client = kube::Client::new(service, "stardelt");
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let principal: ServicePrincipal = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipal",
            "metadata": {
                "name": "etl",
                "namespace": "stardelt",
                "deletionTimestamp": "2026-11-06T00:00:00Z",
                "finalizers": [PRINCIPAL_FINALIZER]
            },
            "spec": { "ownerGroup": "analytics" }
        }))
        .expect("deserialize ServicePrincipal");

        let action = reconcile_principal(Arc::new(principal), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());

        let requests = requests.lock().expect("requests poisoned");
        assert!(
            requests.iter().any(|(m, p, _)| m == "DELETE"
                && p.contains("/proxy/admin/realms/stardelt/clients/kc-1")),
            "expected Keycloak client delete: {requests:?}"
        );
        assert!(
            requests
                .iter()
                .any(|(m, p, _)| m == "DELETE" && p == "/api/v1/namespaces/stardelt/secrets/sp-etl"),
            "expected Secret delete: {requests:?}"
        );
        let finalizer_patch = requests
            .iter()
            .find(|(m, p, _)| {
                m == "PATCH"
                    && p == "/apis/stardelt.io/v1alpha1/namespaces/stardelt/serviceprincipals/etl"
            })
            .expect("finalizer patch");
        let body: serde_json::Value = serde_json::from_str(&finalizer_patch.2).expect("json");
        assert_eq!(body["metadata"]["finalizers"], json!([]), "{body}");
    }

    #[tokio::test]
    async fn a_client_the_operator_did_not_create_is_left_alone() {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let uri = request.uri().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    path.clone(),
                    String::from_utf8_lossy(&body).to_string(),
                ));
                let (status, document) = if method == "GET"
                    && path == "/apis/stardelt.io/v1alpha1/platforminstances"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "PlatformInstanceList",
                            "items": [{
                                "apiVersion": "stardelt.io/v1alpha1",
                                "kind": "PlatformInstance",
                                "metadata": { "name": "demo" },
                                "spec": { "namespace": "stardelt", "identity": { "mode": "platform" } }
                            }]
                        }),
                    )
                } else if method == "GET" && path.contains("/configmaps/keycloak-env") {
                    (200, cm_env())
                } else if method == "GET" && path.contains("/secrets/keycloak-admin") {
                    (200, admin_secret())
                } else if uri.contains("/proxy/realms/master/protocol/openid-connect/token") {
                    (200, json!({"access_token": "tok"}))
                } else if method == "GET"
                    && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=sp-etl")
                {
                    (200, json!([{"id":"kc-1","attributes":{}}]))
                } else {
                    (
                        404,
                        json!({"kind":"Status","code":404,"reason":"NotFound","message":"not found"}),
                    )
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        let client = kube::Client::new(service, "stardelt");
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let principal: ServicePrincipal = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipal",
            "metadata": {
                "name": "etl",
                "namespace": "stardelt",
                "deletionTimestamp": "2026-11-06T00:00:00Z",
                "finalizers": [PRINCIPAL_FINALIZER]
            },
            "spec": { "ownerGroup": "analytics" }
        }))
        .expect("deserialize ServicePrincipal");

        let action = reconcile_principal(Arc::new(principal), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));

        let requests = requests.lock().expect("requests poisoned");
        assert!(
            !requests.iter().any(|(m, _p, _)| m == "DELETE"),
            "no DELETE expected: {requests:?}"
        );
        assert!(
            !requests.iter().any(|(m, p, _)| {
                m == "PATCH"
                    && p == "/apis/stardelt.io/v1alpha1/namespaces/stardelt/serviceprincipals/etl"
            }),
            "finalizer patch absent: {requests:?}"
        );
    }

    #[tokio::test]
    async fn deleting_in_external_mode_touches_no_keycloak() {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    path.clone(),
                    String::from_utf8_lossy(&body).to_string(),
                ));
                let (status, document) = if method == "GET"
                    && path == "/apis/stardelt.io/v1alpha1/platforminstances"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "PlatformInstanceList",
                            "items": [{
                                "apiVersion": "stardelt.io/v1alpha1",
                                "kind": "PlatformInstance",
                                "metadata": { "name": "demo" },
                                "spec": {
                                    "namespace": "stardelt",
                                    "identity": {
                                        "mode": "external",
                                        "issuerUrl": "https://id.example.com/realms/stardelt"
                                    }
                                }
                            }]
                        }),
                    )
                } else if method == "DELETE" && path == "/api/v1/namespaces/stardelt/secrets/sp-etl"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "v1", "kind": "Secret",
                            "metadata": { "name": "sp-etl", "namespace": "stardelt" }
                        }),
                    )
                } else if method == "PATCH"
                    && path
                        == "/apis/stardelt.io/v1alpha1/namespaces/stardelt/serviceprincipals/etl"
                {
                    (
                        200,
                        json!({
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "ServicePrincipal",
                            "metadata": { "name": "etl", "namespace": "stardelt", "finalizers": [] },
                            "spec": { "ownerGroup": "analytics" }
                        }),
                    )
                } else {
                    (
                        404,
                        json!({"kind":"Status","code":404,"reason":"NotFound","message":"not found"}),
                    )
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        let client = kube::Client::new(service, "stardelt");
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let principal: ServicePrincipal = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipal",
            "metadata": {
                "name": "etl",
                "namespace": "stardelt",
                "deletionTimestamp": "2026-11-06T00:00:00Z",
                "finalizers": [PRINCIPAL_FINALIZER]
            },
            "spec": { "ownerGroup": "analytics" }
        }))
        .expect("deserialize ServicePrincipal");

        let action = reconcile_principal(Arc::new(principal), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());

        let requests = requests.lock().expect("requests poisoned");
        assert!(
            requests.iter().all(|(_, p, _)| !p.contains("/admin/")),
            "no Keycloak admin requests expected: {requests:?}"
        );
        assert!(
            requests
                .iter()
                .any(|(m, p, _)| m == "DELETE" && p == "/api/v1/namespaces/stardelt/secrets/sp-etl"),
            "expected Secret delete: {requests:?}"
        );
        assert!(
            requests.iter().any(|(m, p, _)| {
                m == "PATCH"
                    && p == "/apis/stardelt.io/v1alpha1/namespaces/stardelt/serviceprincipals/etl"
            }),
            "expected finalizer removal: {requests:?}"
        );
    }
}

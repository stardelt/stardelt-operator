//! The identity contract (decision D20): the ConfigMap `stardelt-identity` every building block
//! reads, built from `PlatformInstance.spec.identity` and `spec.baseDomain`. Pure functions, no I/O.
//!
//! The keys are the ones `stardelt-platform` writes today (`chart/templates/identity.yaml`), so a
//! component cannot tell whether the chart or the operator produced them.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::ObjectMeta;

use crate::crd::{IdentityMode, IdentityProvider, PlatformInstanceSpec};

/// The client names an `external` instance may use (the app registrations of D31).
const CLIENT_NAMES: [&str; 7] = [
    "nova",
    "trino",
    "lakekeeper",
    "superset",
    "airflow",
    "jupyterhub",
    "trino-group-sync",
];

/// Name of the ConfigMap in the platform namespace.
pub const CONFIGMAP_NAME: &str = "stardelt-identity";
/// Label on that ConfigMap carrying the mode, as the chart sets it.
pub const MODE_LABEL: &str = "stardelt.io/identity-mode";
/// Realm of the platform's own Keycloak.
pub const PLATFORM_REALM: &str = "stardelt";

/// Why the settings cannot be turned into a contract.
#[derive(Debug, PartialEq, Eq)]
pub struct IdentityError(pub String);

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for IdentityError {}

/// The rules the schema's CEL validations also enforce, in the same words: what the controller
/// reports when the schema was bypassed.
pub fn validate(spec: &PlatformInstanceSpec) -> Result<(), IdentityError> {
    let identity = &spec.identity;
    if identity.mode != IdentityMode::External {
        if identity.provider != IdentityProvider::Oidc
            || identity.api_audience.is_some()
            || !identity.clients.is_empty()
        {
            return Err(IdentityError(
                "platform mode creates its own clients: leave provider, apiAudience and clients out"
                    .into(),
            ));
        }
        return Ok(());
    }
    let url = identity
        .issuer_url
        .as_deref()
        .ok_or_else(|| IdentityError("external mode needs issuerUrl".into()))?;
    if !url.starts_with("https://") {
        return Err(IdentityError(
            "identity.issuerUrl must be an https:// URL".into(),
        ));
    }
    if identity.api_audience.is_none() {
        return Err(IdentityError("external mode needs apiAudience".into()));
    }
    if !identity.clients.contains_key("nova") {
        return Err(IdentityError("external mode needs a nova client".into()));
    }
    if identity
        .clients
        .keys()
        .any(|name| !CLIENT_NAMES.contains(&name.as_str()))
    {
        return Err(IdentityError(
            "client names are nova, trino, lakekeeper, superset, airflow, jupyterhub and trino-group-sync"
                .into(),
        ));
    }
    if identity
        .clients
        .iter()
        .any(|(name, client)| name != "lakekeeper" && client.client_secret_ref.is_none())
    {
        return Err(IdentityError(
            "every client except lakekeeper needs a clientSecretRef".into(),
        ));
    }
    Ok(())
}

/// The user-id claim the components must read: `oid` for Entra, `sub` otherwise. Derived rather
/// than configured because Entra's `sub` differs per application and cannot identify a user
/// across the platform's clients.
pub fn user_claim(spec: &PlatformInstanceSpec) -> &'static str {
    match spec.identity.provider {
        IdentityProvider::Entra => "oid",
        IdentityProvider::Oidc => "sub",
    }
}

/// The issuer URL components must trust: derived for `platform`, given for `external`.
pub fn issuer_url(spec: &PlatformInstanceSpec) -> Result<Option<String>, IdentityError> {
    match spec.identity.mode {
        IdentityMode::None => Ok(None),
        IdentityMode::Platform => {
            let domain = base_domain(spec)?;
            Ok(Some(format!("https://id.{domain}/realms/{PLATFORM_REALM}")))
        }
        IdentityMode::External => {
            let url = spec.identity.issuer_url.as_deref().ok_or_else(|| {
                IdentityError("identity.mode external needs identity.issuerUrl".into())
            })?;
            if !url.starts_with("https://") {
                return Err(IdentityError(
                    "identity.issuerUrl must be an https:// URL".into(),
                ));
            }
            Ok(Some(url.trim_end_matches('/').to_string()))
        }
    }
}

/// The base domain of the instance, or the reason it cannot be used. `keycloak.rs` renders the
/// realm hosts and `KC_HOSTNAME` from the same value.
pub(crate) fn base_domain(spec: &PlatformInstanceSpec) -> Result<&str, IdentityError> {
    spec.base_domain
        .as_deref()
        .ok_or_else(|| IdentityError("identity needs baseDomain".into()))
}

/// The data of ConfigMap `stardelt-identity`, or `None` when sign-in is off (no ConfigMap exists
/// then; components read its absence as "no identity provider").
pub fn contract(
    spec: &PlatformInstanceSpec,
) -> Result<Option<BTreeMap<String, String>>, IdentityError> {
    validate(spec)?;
    let Some(issuer) = issuer_url(spec)? else {
        return Ok(None);
    };
    let domain = base_domain(spec)?;
    let mut data = BTreeMap::from([
        ("issuerUrl".to_string(), issuer),
        (
            "groupClaim".to_string(),
            spec.identity.effective_group_claim().to_string(),
        ),
        ("adminGroup".to_string(), spec.identity.admin_group.clone()),
        ("authMode".to_string(), "oidc".to_string()),
        // Nova answers on the base domain itself; every other application on a subdomain.
        ("publicUrl".to_string(), format!("https://{domain}")),
    ]);
    if spec.identity.mode == IdentityMode::External {
        let provider = match spec.identity.provider {
            IdentityProvider::Oidc => "oidc",
            IdentityProvider::Entra => "entra",
        };
        let audience = spec
            .identity
            .api_audience
            .clone()
            .ok_or_else(|| IdentityError("external mode needs apiAudience".into()))?;
        data.insert("provider".to_string(), provider.to_string());
        data.insert("apiAudience".to_string(), audience);
    }
    Ok(Some(data))
}

/// The ConfigMap `stardelt-identity` to apply in `namespace`, or `None` when sign-in is off.
///
/// It carries no owner reference: uninstalling the platform keeps the contract (decision D18,
/// `deletionPolicy: Retain`), and the chart or `scripts/identity-bootstrap.sh` may already own
/// other keys of the same object (for example `trinoUrl`), which server-side apply leaves alone.
pub fn config_map(
    spec: &PlatformInstanceSpec,
    namespace: &str,
) -> Result<Option<ConfigMap>, IdentityError> {
    let Some(data) = contract(spec)? else {
        return Ok(None);
    };
    Ok(Some(ConfigMap {
        metadata: ObjectMeta {
            name: Some(CONFIGMAP_NAME.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    "app.kubernetes.io/part-of".to_string(),
                    "stardelt".to_string(),
                ),
                (
                    MODE_LABEL.to_string(),
                    mode_label(spec.identity.mode).to_string(),
                ),
            ])),
            ..ObjectMeta::default()
        },
        data: Some(data),
        ..ConfigMap::default()
    }))
}

/// Value of [`MODE_LABEL`].
pub fn mode_label(mode: IdentityMode) -> &'static str {
    match mode {
        IdentityMode::None => "none",
        IdentityMode::Platform => "platform",
        IdentityMode::External => "external",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{ClientSecretRef, IdentityClient, IdentitySpec};

    fn spec(identity: IdentitySpec, base_domain: Option<&str>) -> PlatformInstanceSpec {
        serde_json::from_value(serde_json::json!({
            "identity": identity,
            "baseDomain": base_domain,
        }))
        .expect("a PlatformInstanceSpec")
    }

    fn secret_ref(name: &str) -> ClientSecretRef {
        ClientSecretRef {
            name: name.into(),
            key: "client-secret".into(),
        }
    }

    fn client(client_id: &str, secret: Option<ClientSecretRef>) -> IdentityClient {
        IdentityClient {
            client_id: client_id.into(),
            client_secret_ref: secret,
        }
    }

    fn external(url: &str) -> IdentitySpec {
        IdentitySpec {
            mode: IdentityMode::External,
            issuer_url: Some(url.into()),
            api_audience: Some("api://stardelt".into()),
            clients: BTreeMap::from([(
                "nova".to_string(),
                client("nova-client", Some(secret_ref("idp-nova"))),
            )]),
            ..IdentitySpec::default()
        }
    }

    fn err(spec: &PlatformInstanceSpec) -> String {
        validate(spec).expect_err("invalid identity").0
    }

    #[test]
    fn external_needs_issuer_audience_and_a_nova_client() {
        let valid = spec(
            external("https://idp.example.com"),
            Some("data.example.com"),
        );
        assert_eq!(validate(&valid), Ok(()));

        let mut no_issuer = external("https://idp.example.com");
        no_issuer.issuer_url = None;
        assert_eq!(
            err(&spec(no_issuer, Some("data.example.com"))),
            "external mode needs issuerUrl"
        );

        let http = spec(external("http://idp.example.com"), Some("data.example.com"));
        assert!(err(&http).contains("https"));

        let mut no_audience = external("https://idp.example.com");
        no_audience.api_audience = None;
        assert_eq!(
            err(&spec(no_audience, Some("data.example.com"))),
            "external mode needs apiAudience"
        );

        let mut no_nova = external("https://idp.example.com");
        no_nova.clients.clear();
        assert_eq!(
            err(&spec(no_nova, Some("data.example.com"))),
            "external mode needs a nova client"
        );
    }

    #[test]
    fn platform_mode_refuses_clients_and_audience() {
        let platform = |identity: IdentitySpec| {
            spec(
                IdentitySpec {
                    mode: IdentityMode::Platform,
                    ..identity
                },
                Some("data.example.com"),
            )
        };
        let message =
            "platform mode creates its own clients: leave provider, apiAudience and clients out";
        assert_eq!(
            err(&platform(IdentitySpec {
                api_audience: Some("api://stardelt".into()),
                ..IdentitySpec::default()
            })),
            message
        );
        assert_eq!(
            err(&platform(IdentitySpec {
                clients: BTreeMap::from([(
                    "nova".to_string(),
                    client("nova-client", Some(secret_ref("idp-nova"))),
                )]),
                ..IdentitySpec::default()
            })),
            message
        );
        assert_eq!(
            err(&platform(IdentitySpec {
                provider: IdentityProvider::Entra,
                ..IdentitySpec::default()
            })),
            message
        );
    }

    #[test]
    fn none_mode_refuses_clients() {
        let with_clients = IdentitySpec {
            clients: BTreeMap::from([(
                "nova".to_string(),
                client("nova-client", Some(secret_ref("idp-nova"))),
            )]),
            ..IdentitySpec::default()
        };
        assert_eq!(
            err(&spec(with_clients, None)),
            "platform mode creates its own clients: leave provider, apiAudience and clients out"
        );
    }

    #[test]
    fn client_names_are_limited_to_the_known_set() {
        let mut id = external("https://idp.example.com");
        id.clients.insert(
            "bogus".to_string(),
            client("bogus-client", Some(secret_ref("idp-bogus"))),
        );
        assert_eq!(
            err(&spec(id, Some("data.example.com"))),
            "client names are nova, trino, lakekeeper, superset, airflow, jupyterhub and trino-group-sync"
        );
    }

    #[test]
    fn every_client_but_lakekeeper_needs_a_secret_ref() {
        let mut id = external("https://idp.example.com");
        id.clients
            .insert("trino".to_string(), client("trino-client", None));
        assert_eq!(
            err(&spec(id, Some("data.example.com"))),
            "every client except lakekeeper needs a clientSecretRef"
        );

        let mut fixed = external("https://idp.example.com");
        fixed.clients.insert(
            "trino".to_string(),
            client("trino-client", Some(secret_ref("idp-trino"))),
        );
        assert_eq!(validate(&spec(fixed, Some("data.example.com"))), Ok(()));
    }

    #[test]
    fn lakekeeper_may_be_public() {
        let mut id = external("https://idp.example.com");
        id.clients
            .insert("lakekeeper".to_string(), client("lakekeeper-client", None));
        assert_eq!(validate(&spec(id, Some("data.example.com"))), Ok(()));
    }

    #[test]
    fn secret_ref_key_defaults_to_client_secret() {
        let secret: ClientSecretRef =
            serde_json::from_value(serde_json::json!({ "name": "idp-nova" })).unwrap();
        assert_eq!(secret.key, "client-secret");
    }

    #[test]
    fn entra_defaults_the_group_claim_to_roles() {
        assert_eq!(
            IdentitySpec {
                provider: IdentityProvider::Entra,
                ..IdentitySpec::default()
            }
            .effective_group_claim(),
            "roles"
        );
    }

    #[test]
    fn oidc_defaults_it_to_groups() {
        assert_eq!(IdentitySpec::default().effective_group_claim(), "groups");
    }

    #[test]
    fn an_explicit_group_claim_wins() {
        for provider in [IdentityProvider::Oidc, IdentityProvider::Entra] {
            assert_eq!(
                IdentitySpec {
                    provider,
                    group_claim: Some("custom-groups".into()),
                    ..IdentitySpec::default()
                }
                .effective_group_claim(),
                "custom-groups"
            );
        }
    }

    #[test]
    fn contract_publishes_provider_and_audience_for_external_only() {
        let external_instance = spec(
            external("https://idp.example.com"),
            Some("data.example.com"),
        );
        let data = contract(&external_instance).unwrap().unwrap();
        assert_eq!(data["provider"], "oidc");
        assert_eq!(data["apiAudience"], "api://stardelt");

        let mut entra = external("https://login.microsoftonline.com/tenant/v2.0");
        entra.provider = IdentityProvider::Entra;
        let data = contract(&spec(entra, Some("data.example.com")))
            .unwrap()
            .unwrap();
        assert_eq!(data["provider"], "entra");
        assert_eq!(data["groupClaim"], "roles");

        let platform = spec(
            IdentitySpec {
                mode: IdentityMode::Platform,
                ..IdentitySpec::default()
            },
            Some("data.example.com"),
        );
        let data = contract(&platform).unwrap().unwrap();
        assert!(!data.contains_key("provider"));
        assert!(!data.contains_key("apiAudience"));
        assert_eq!(data["groupClaim"], "groups");
    }

    #[test]
    fn user_claim_is_oid_for_entra_and_sub_otherwise() {
        let entra = spec(
            IdentitySpec {
                provider: IdentityProvider::Entra,
                ..IdentitySpec::default()
            },
            Some("data.example.com"),
        );
        assert_eq!(user_claim(&entra), "oid");
        let oidc = spec(IdentitySpec::default(), Some("data.example.com"));
        assert_eq!(user_claim(&oidc), "sub");
    }

    #[test]
    fn without_sign_in_there_is_no_contract() {
        let s = spec(IdentitySpec::default(), None);
        assert_eq!(contract(&s), Ok(None));
        assert_eq!(issuer_url(&s), Ok(None));
    }

    #[test]
    fn the_platform_mode_derives_the_keycloak_issuer_from_the_base_domain() {
        let s = spec(
            IdentitySpec {
                mode: IdentityMode::Platform,
                ..IdentitySpec::default()
            },
            Some("alpha.dev.stardelt.io"),
        );
        let data = contract(&s).unwrap().unwrap();
        assert_eq!(
            data["issuerUrl"],
            "https://id.alpha.dev.stardelt.io/realms/stardelt"
        );
        assert_eq!(data["publicUrl"], "https://alpha.dev.stardelt.io");
        assert_eq!(data["authMode"], "oidc");
        assert_eq!(data["groupClaim"], "groups");
        assert_eq!(data["adminGroup"], "stardelt-admins");
    }

    #[test]
    fn the_external_mode_passes_the_issuer_through_without_a_trailing_slash() {
        let s = spec(
            external("https://login.microsoftonline.com/tenant-id/v2.0/"),
            Some("data.example.com"),
        );
        let data = contract(&s).unwrap().unwrap();
        assert_eq!(
            data["issuerUrl"],
            "https://login.microsoftonline.com/tenant-id/v2.0"
        );
        assert_eq!(data["publicUrl"], "https://data.example.com");
    }

    #[test]
    fn custom_claim_and_admin_group_are_carried() {
        let mut id = external("https://idp.example.com");
        id.group_claim = Some("roles".into());
        id.admin_group = "data-platform-admins".into();
        let data = contract(&spec(id, Some("data.example.com")))
            .unwrap()
            .unwrap();
        assert_eq!(data["groupClaim"], "roles");
        assert_eq!(data["adminGroup"], "data-platform-admins");
    }

    #[test]
    fn broken_settings_are_errors() {
        let no_url = spec(
            IdentitySpec {
                mode: IdentityMode::External,
                ..IdentitySpec::default()
            },
            Some("d.example.com"),
        );
        assert!(contract(&no_url).unwrap_err().0.contains("issuerUrl"));
        let http = spec(external("http://idp.example.com"), Some("d.example.com"));
        assert!(contract(&http).unwrap_err().0.contains("https"));
        let no_domain = spec(external("https://idp.example.com"), None);
        assert!(contract(&no_domain).unwrap_err().0.contains("baseDomain"));
    }

    #[test]
    fn the_config_map_is_named_labelled_and_carries_the_contract() {
        let s = spec(
            external("https://idp.example.com"),
            Some("data.example.com"),
        );
        let cm = config_map(&s, "stardelt").unwrap().unwrap();
        assert_eq!(cm.metadata.name.as_deref(), Some("stardelt-identity"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("stardelt"));
        let labels = cm.metadata.labels.unwrap();
        assert_eq!(labels["stardelt.io/identity-mode"], "external");
        assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        assert!(
            cm.metadata.owner_references.is_none(),
            "uninstalling keeps the contract"
        );
        assert_eq!(cm.data.unwrap()["issuerUrl"], "https://idp.example.com");
    }

    #[test]
    fn without_sign_in_there_is_no_config_map_and_bad_settings_are_errors() {
        assert!(
            config_map(&spec(IdentitySpec::default(), None), "stardelt")
                .unwrap()
                .is_none()
        );
        assert!(
            config_map(
                &spec(external("http://x"), Some("d.example.com")),
                "stardelt"
            )
            .is_err()
        );
    }

    #[test]
    fn the_mode_label_matches_the_chart() {
        assert_eq!(mode_label(IdentityMode::None), "none");
        assert_eq!(mode_label(IdentityMode::Platform), "platform");
        assert_eq!(mode_label(IdentityMode::External), "external");
    }

    #[test]
    fn the_schema_carries_the_cel_rules_and_the_fields_default_sensibly() {
        use kube::CustomResourceExt;
        let crd = serde_json::to_string(&crate::crd::PlatformInstance::crd()).unwrap();
        assert!(crd.contains("identity.mode external needs identity.issuerUrl"));
        assert!(crd.contains("baseDomain"));
        for message in [
            "platform mode creates its own clients: leave provider, apiAudience and clients out",
            "external mode needs issuerUrl",
            "external mode needs apiAudience",
            "external mode needs a nova client",
            "client names are nova, trino, lakekeeper, superset, airflow, jupyterhub and trino-group-sync",
            "every client except lakekeeper needs a clientSecretRef",
        ] {
            assert!(crd.contains(message), "the schema carries: {message}");
        }
        let s: PlatformInstanceSpec = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(s.identity, IdentitySpec::default());
        assert_eq!(s.base_domain, None);
    }
}

//! The `Connection` controller (decisions D23 and D27): turns a connection into an External Secrets
//! `ExternalSecret` that syncs its secret fields from the store into the Kubernetes Secret
//! `conn-<name>`, and mirrors the sync state into the connection's status.
//!
//! The decisions that matter are pure functions ([`plan`], [`sync_state`]) so they are tested without a
//! cluster; [`reconcile_connection`] only applies the plan and writes the status.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Api, ResourceExt,
    api::{DynamicObject, Patch, PatchParams},
    core::{ApiResource, GroupVersionKind},
    runtime::controller::Action,
};
use serde_json::{Value, json};

use crate::controller::Context;
use crate::crd::{
    Condition, Connection, ConnectionScope, ConnectionSpec, ConnectionStatus, SecretsMode,
    connection_secret_name,
};
use crate::secrets;

/// The External Secrets `ExternalSecret` resource (`externalsecrets.external-secrets.io/v1`).
pub fn external_secret_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("external-secrets.io", "v1", "ExternalSecret"),
        "externalsecrets",
    )
}

/// What the controller does with a connection.
#[derive(Debug, PartialEq)]
pub enum Plan {
    /// Apply this `ExternalSecret`.
    Apply(Value),
    /// The connection needs no credentials: no `ExternalSecret` is created and it is Ready.
    NoSecret,
    /// Do not apply anything; report `reason` (a CamelCase word) and `message` in the status.
    Invalid {
        reason: &'static str,
        message: String,
    },
}

/// The name of the `SecretStore` that may read this connection's path: one store per owner group and one
/// for managed connections (stardelt-platform `scripts/eso-openbao-store.sh`). Store names are Kubernetes
/// names, so a group name with upper-case letters, dots or underscores cannot be mapped yet.
pub fn store_name(spec: &ConnectionSpec) -> Result<String, String> {
    let target = match (spec.scope, spec.owner_group.as_deref()) {
        (ConnectionScope::Managed, _) => "managed".to_string(),
        (ConnectionScope::Group, Some(group)) => group.to_string(),
        (ConnectionScope::Group, None) => {
            return Err("a group connection needs an ownerGroup".to_string());
        }
    };
    let ok = !target.is_empty()
        && target.len() <= 40
        && target.as_bytes()[0].is_ascii_alphanumeric()
        && target.as_bytes()[target.len() - 1] != b'-'
        && target
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(format!("openbao-{target}"))
    } else {
        Err(format!(
            "the group name `{target}` cannot name a secret store yet (lower-case letters, digits and dashes, at most 40)"
        ))
    }
}

/// Decide what to apply for the connection `name` in `namespace` (owner `uid`).
pub fn plan(
    spec: &ConnectionSpec,
    name: &str,
    namespace: &str,
    uid: &str,
    mode: SecretsMode,
) -> Plan {
    if let Err(message) = spec.validate(name) {
        return Plan::Invalid {
            reason: "InvalidSpec",
            message,
        };
    }
    if spec.secret.is_none() {
        return Plan::NoSecret;
    }
    let secret_spec = spec
        .secret
        .as_ref()
        .expect("secret present after NoSecret check");
    // The enterprise's store (decision D23, `secrets.mode: external`): the author names the path,
    // because the path is the enterprise's convention and the platform cannot derive it. Every
    // other mode derives the path from the scope, so a group cannot read another group's secret.
    let (store, store_kind, path) = if mode == SecretsMode::External {
        let Some(remote_ref) = secret_spec.remote_ref.as_deref() else {
            return Plan::Invalid {
                reason: "RemoteRefRequired",
                message: "secrets.mode external: set secret.remoteRef to the path in the enterprise store (for example `apps/crm/db`)".to_string(),
            };
        };
        (
            secrets::ENTERPRISE_STORE.to_string(),
            "ClusterSecretStore",
            remote_ref.to_string(),
        )
    } else {
        if secret_spec.remote_ref.is_some() {
            return Plan::Invalid {
                reason: "RemoteRefNotAllowed",
                message: "secret.remoteRef is only allowed with an external secret store; remove it and the path is derived from the scope".to_string(),
            };
        }
        let store = match store_name(spec) {
            Ok(store) => store,
            Err(message) => {
                return Plan::Invalid {
                    reason: "GroupNameNotSupported",
                    message,
                };
            }
        };
        (store, "SecretStore", spec.derived_remote_ref(name))
    };
    let secret = connection_secret_name(name);
    let data: Vec<Value> = secret_spec
        .keys
        .iter()
        .map(|key| json!({ "secretKey": key, "remoteRef": { "key": path, "property": key } }))
        .collect();
    Plan::Apply(json!({
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {
            "name": secret,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/part-of": "stardelt",
                "stardelt.io/connection": name,
            },
            "ownerReferences": [{
                "apiVersion": "stardelt.io/v1alpha1",
                "kind": "Connection",
                "name": name,
                "uid": uid,
                "controller": true,
                "blockOwnerDeletion": true,
            }],
        },
        "spec": {
            "refreshInterval": spec.refresh_interval,
            "secretStoreRef": { "kind": store_kind, "name": store },
            "target": { "name": secret, "creationPolicy": "Owner", "deletionPolicy": "Delete" },
            "data": data,
        },
    }))
}

/// The sync state External Secrets reports on an `ExternalSecret`.
#[derive(Debug, PartialEq, Eq)]
pub struct SyncState {
    pub ready: bool,
    /// `status.refreshTime`: the last successful sync.
    pub last_sync: Option<String>,
    /// Why it is not ready, when External Secrets says.
    pub message: Option<String>,
}

/// With the target policy `Delete`, External Secrets answers `Ready=True` and the reason `SecretDeleted`
/// when the value does not exist in the store: it removes the Secret and does not call that an error.
/// For a connection it is an error, because the credential is missing, so it is not ready.
const SECRET_DELETED: &str = "SecretDeleted";

pub fn sync_state(external_secret: &Value) -> SyncState {
    let status = &external_secret["status"];
    let ready_condition = status["conditions"]
        .as_array()
        .and_then(|c| c.iter().find(|c| c["type"] == "Ready"));
    let text = |v: &Value| v.as_str().filter(|s| !s.is_empty()).map(str::to_string);
    let value_missing = ready_condition
        .and_then(|c| text(&c["reason"]))
        .is_some_and(|r| r == SECRET_DELETED);
    let ready = ready_condition.is_some_and(|c| c["status"] == "True") && !value_missing;
    SyncState {
        ready,
        last_sync: if ready {
            text(&status["refreshTime"])
        } else {
            None
        },
        message: if ready {
            None
        } else if value_missing {
            Some("the value does not exist in the secret store yet: create the connection in Nova or write the value first".to_string())
        } else {
            ready_condition
                .and_then(|c| text(&c["message"]))
                .or_else(|| Some("waiting for the first sync from the secret store".to_string()))
        },
    }
}

/// The `Ready` condition, keeping the previous transition time while the status is unchanged.
pub(crate) fn ready_condition(
    ready: bool,
    reason: &str,
    message: Option<String>,
    generation: Option<i64>,
    previous: Option<&Condition>,
    now: &str,
) -> Condition {
    let status = if ready { "True" } else { "False" };
    let last_transition_time = match previous {
        Some(p) if p.status == status => p.last_transition_time.clone(),
        _ => Some(now.to_string()),
    };
    Condition {
        type_: "Ready".into(),
        status: status.into(),
        reason: Some(reason.into()),
        message,
        last_transition_time,
        observed_generation: generation,
    }
}

pub async fn reconcile_connection(
    conn: Arc<Connection>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    if conn.metadata.deletion_timestamp.is_some() {
        // The ExternalSecret and the Secret go with the Connection (owner references).
        return Ok(Action::await_change());
    }
    let name = conn.name_any();
    let namespace = conn.namespace().unwrap_or_else(|| "default".into());
    let uid = conn.metadata.uid.clone().unwrap_or_default();

    // Where the store is (decision D23): ConfigMap `stardelt-secrets` in the connection's own
    // namespace, published by the PlatformInstance controller. Absent means `none` — today the
    // platform is one namespace; a connection in another namespace keeps the derived path.
    let mode = match Api::<ConfigMap>::namespaced(ctx.client.clone(), &namespace)
        .get_opt(secrets::MODE_CONFIGMAP)
        .await
    {
        Ok(Some(config_map)) => match config_map
            .data
            .as_ref()
            .and_then(|data| data.get("mode"))
            .map(String::as_str)
        {
            Some("platform") => SecretsMode::Platform,
            Some("external") => SecretsMode::External,
            _ => SecretsMode::None,
        },
        Ok(None) => SecretsMode::None,
        Err(e) => return Err(e),
    };

    // (ready, reason, message, last_sync)
    let (ready, reason, message, last_sync) = match plan(&conn.spec, &name, &namespace, &uid, mode)
    {
        Plan::Invalid { reason, message } => (false, reason, Some(message), None),
        Plan::NoSecret => (
            true,
            "NoSecret",
            Some("this connection carries no credentials".to_string()),
            None,
        ),
        Plan::Apply(external_secret) => {
            let api: Api<DynamicObject> =
                Api::namespaced_with(ctx.client.clone(), &namespace, &external_secret_resource());
            let applied = api
                .patch(
                    &connection_secret_name(&name),
                    &PatchParams::apply("stardelt-operator").force(),
                    &Patch::Apply(&external_secret),
                )
                .await;
            match applied {
                Ok(object) => {
                    let state = sync_state(&serde_json::to_value(&object).unwrap_or(Value::Null));
                    let reason = if state.ready { "Synced" } else { "Syncing" };
                    (state.ready, reason, state.message, state.last_sync)
                }
                Err(kube::Error::Api(response)) if response.code == 404 => (
                    false,
                    "ExternalSecretsNotInstalled",
                    Some(
                        "External Secrets Operator is not installed (make external-secrets)"
                            .to_string(),
                    ),
                    None,
                ),
                Err(error) => return Err(error),
            }
        }
    };

    let now = k8s_openapi::jiff::Timestamp::now().to_string();
    let previous = conn
        .status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"));
    let status = ConnectionStatus {
        ready,
        secret_name: (ready && reason == "Synced").then(|| connection_secret_name(&name)),
        last_sync,
        conditions: vec![ready_condition(
            ready,
            reason,
            message,
            conn.metadata.generation,
            previous,
            &now,
        )],
        observed_generation: conn.metadata.generation,
    };
    let connections: Api<Connection> = Api::namespaced(ctx.client.clone(), &namespace);
    connections
        .patch_status(
            &name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await?;
    tracing::info!(connection = %name, %namespace, ready, reason, "reconciled");
    // A change of the spec or of the status triggers the next run; the timer picks up the sync that
    // finishes while nothing else changes.
    Ok(Action::requeue(Duration::from_secs(if ready {
        300
    } else {
        30
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::SecretsMode;

    fn spec(value: serde_json::Value) -> ConnectionSpec {
        let mut base = json!({
            "scope": "group",
            "ownerGroup": "analytics",
            "type": "sqlserver",
            "config": { "host": "crm.example.com" },
            "secret": { "keys": ["user", "password"] },
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

    fn applied(plan: Plan) -> Value {
        match plan {
            Plan::Apply(v) => v,
            Plan::NoSecret => panic!("expected Apply, got NoSecret"),
            Plan::Invalid { reason, message } => panic!("expected Apply, got {reason}: {message}"),
        }
    }

    fn invalid(plan: Plan) -> (&'static str, String) {
        match plan {
            Plan::Invalid { reason, message } => (reason, message),
            Plan::Apply(_) => panic!("expected Invalid"),
            Plan::NoSecret => panic!("expected Invalid, got NoSecret"),
        }
    }

    #[test]
    fn a_group_connection_syncs_its_group_path_through_its_groups_store() {
        let es = applied(plan(
            &spec(json!({})),
            "crm-db",
            "stardelt",
            "uid-1",
            SecretsMode::None,
        ));
        assert_eq!(es["metadata"]["name"], "conn-crm-db");
        assert_eq!(es["metadata"]["namespace"], "stardelt");
        assert_eq!(es["metadata"]["ownerReferences"][0]["uid"], "uid-1");
        assert_eq!(es["metadata"]["ownerReferences"][0]["kind"], "Connection");
        assert_eq!(es["spec"]["secretStoreRef"]["name"], "openbao-analytics");
        assert_eq!(es["spec"]["refreshInterval"], "15m");
        assert_eq!(es["spec"]["target"]["name"], "conn-crm-db");
        assert_eq!(es["spec"]["target"]["creationPolicy"], "Owner");
        let data = es["spec"]["data"].as_array().unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data[0]["secretKey"], "user");
        assert_eq!(
            data[0]["remoteRef"]["key"],
            "connections/group/analytics/crm-db"
        );
        assert_eq!(data[1]["remoteRef"]["property"], "password");
    }

    #[test]
    fn a_managed_connection_uses_the_managed_store_and_path() {
        let s = spec(
            json!({ "scope": "managed", "ownerGroup": null, "usableBy": ["analytics", "finance"] }),
        );
        let es = applied(plan(&s, "crm-db", "stardelt", "uid-1", SecretsMode::None));
        assert_eq!(es["spec"]["secretStoreRef"]["name"], "openbao-managed");
        assert_eq!(
            es["spec"]["data"][0]["remoteRef"]["key"],
            "connections/managed/crm-db"
        );
    }

    #[test]
    fn an_author_chosen_path_is_refused() {
        let s = spec(
            json!({ "secret": { "keys": ["user", "password"], "remoteRef": "connections/group/finance/x" } }),
        );
        let (reason, message) = invalid(plan(&s, "crm-db", "stardelt", "uid-1", SecretsMode::None));
        assert_eq!(reason, "RemoteRefNotAllowed");
        assert!(message.contains("remoteRef"));
    }

    #[test]
    fn a_group_name_that_cannot_name_a_store_is_reported_not_guessed() {
        for group in ["Analytics", "data.team", "data_team"] {
            let s = spec(json!({ "ownerGroup": group }));
            let (reason, _) = invalid(plan(&s, "crm-db", "stardelt", "uid-1", SecretsMode::None));
            assert_eq!(reason, "GroupNameNotSupported", "{group}");
        }
    }

    #[test]
    fn an_invalid_spec_applies_nothing() {
        let s = spec(json!({ "config": { "port": "1433" } }));
        let (reason, message) = invalid(plan(&s, "crm-db", "stardelt", "uid-1", SecretsMode::None));
        assert_eq!(reason, "InvalidSpec");
        assert!(message.contains("config.host"));
        let (reason, _) = invalid(plan(
            &spec(json!({})),
            "Crm_Db",
            "stardelt",
            "uid-1",
            SecretsMode::None,
        ));
        assert_eq!(reason, "InvalidSpec");
    }

    #[test]
    fn the_sync_state_follows_the_ready_condition() {
        let ready = json!({ "status": {
            "refreshTime": "2026-10-19T17:00:00Z",
            "conditions": [{ "type": "Ready", "status": "True", "reason": "SecretSynced" }]
        }});
        assert_eq!(
            sync_state(&ready),
            SyncState {
                ready: true,
                last_sync: Some("2026-10-19T17:00:00Z".into()),
                message: None
            }
        );
        let failing = json!({ "status": { "conditions": [
            { "type": "Ready", "status": "False", "reason": "SecretSyncedError", "message": "permission denied" }
        ]}});
        let state = sync_state(&failing);
        assert!(!state.ready);
        assert_eq!(state.message.as_deref(), Some("permission denied"));
        // Delete policy and no value in the store: External Secrets says Ready, the connection is not.
        let missing = sync_state(&json!({ "status": {
            "refreshTime": "2026-10-19T17:05:00Z",
            "conditions": [{ "type": "Ready", "status": "True", "reason": "SecretDeleted",
                             "message": "secret deleted due to DeletionPolicy=Delete" }]
        }}));
        assert!(!missing.ready);
        assert_eq!(missing.last_sync, None);
        assert!(
            missing
                .message
                .unwrap()
                .contains("does not exist in the secret store")
        );
        let fresh = sync_state(&json!({ "metadata": {} }));
        assert!(!fresh.ready);
        assert!(fresh.message.unwrap().contains("first sync"));
    }

    #[test]
    fn the_ready_condition_keeps_its_time_while_the_status_is_unchanged() {
        let first = ready_condition(false, "Syncing", None, Some(1), None, "t1");
        assert_eq!(first.last_transition_time.as_deref(), Some("t1"));
        let again = ready_condition(false, "Syncing", None, Some(1), Some(&first), "t2");
        assert_eq!(again.last_transition_time.as_deref(), Some("t1"));
        let flipped = ready_condition(true, "Synced", None, Some(1), Some(&first), "t3");
        assert_eq!(flipped.last_transition_time.as_deref(), Some("t3"));
    }

    #[test]
    fn external_mode_uses_the_enterprise_store_and_the_authors_path() {
        let s =
            spec(json!({ "secret": { "keys": ["user", "password"], "remoteRef": "apps/crm/db" } }));
        let es = applied(plan(
            &s,
            "crm-db",
            "stardelt",
            "uid-1",
            SecretsMode::External,
        ));
        assert_eq!(es["spec"]["secretStoreRef"]["kind"], "ClusterSecretStore");
        assert_eq!(es["spec"]["secretStoreRef"]["name"], "enterprise");
        let data = es["spec"]["data"].as_array().unwrap();
        assert_eq!(data[0]["remoteRef"]["key"], "apps/crm/db");
        assert_eq!(data[1]["remoteRef"]["key"], "apps/crm/db");
    }

    #[test]
    fn external_mode_without_a_remote_ref_is_invalid() {
        let s = spec(json!({}));
        let (reason, message) = invalid(plan(
            &s,
            "crm-db",
            "stardelt",
            "uid-1",
            SecretsMode::External,
        ));
        assert_eq!(reason, "RemoteRefRequired");
        assert!(message.contains("remoteRef"));
    }

    #[test]
    fn platform_mode_still_refuses_a_remote_ref() {
        let s = spec(
            json!({ "secret": { "keys": ["user", "password"], "remoteRef": "connections/group/finance/x" } }),
        );
        let (reason, _) = invalid(plan(
            &s,
            "crm-db",
            "stardelt",
            "uid-1",
            SecretsMode::Platform,
        ));
        assert_eq!(reason, "RemoteRefNotAllowed");
    }

    #[test]
    fn none_mode_keeps_the_derived_path_and_the_namespaced_store() {
        let es = applied(plan(
            &spec(json!({})),
            "crm-db",
            "stardelt",
            "uid-1",
            SecretsMode::None,
        ));
        assert_eq!(es["spec"]["secretStoreRef"]["kind"], "SecretStore");
        assert_eq!(es["spec"]["secretStoreRef"]["name"], "openbao-analytics");
    }

    #[test]
    fn a_kafka_connection_without_a_secret_needs_no_external_secret() {
        let s: ConnectionSpec = serde_json::from_value(json!({
            "scope": "group",
            "ownerGroup": "analytics",
            "type": "kafka",
            "config": { "bootstrapServers": "kafka-kafka-bootstrap.stardelt.svc.cluster.local:9092" }
        }))
        .expect("spec");
        assert!(matches!(
            plan(&s, "bus", "stardelt", "uid-1", SecretsMode::None),
            Plan::NoSecret
        ));
    }

    #[test]
    fn a_postgresql_connection_without_a_secret_is_invalid() {
        let s: ConnectionSpec = serde_json::from_value(json!({
            "scope": "group",
            "ownerGroup": "analytics",
            "type": "postgresql",
            "config": { "host": "db.example.com" }
        }))
        .expect("spec");
        assert!(matches!(
            plan(&s, "db", "stardelt", "uid-1", SecretsMode::None),
            Plan::Invalid {
                reason: "InvalidSpec",
                ..
            }
        ));
    }
}

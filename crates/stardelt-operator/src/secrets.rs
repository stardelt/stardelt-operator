//! The enterprise's secret store in `secrets.mode: external` (decision D23): one cluster-scoped
//! `ClusterSecretStore` named [`ENTERPRISE_STORE`], rendered from the CR's verbatim provider block.
//! The decisions are pure functions so they are tested without a cluster.

use std::collections::BTreeMap;

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    api::ObjectMeta,
    core::{ApiResource, GroupVersionKind},
};
use serde_json::Value;

use crate::crd::{PlatformInstanceSpec, SecretsMode};

/// The name every external-mode `ExternalSecret` references.
pub const ENTERPRISE_STORE: &str = "enterprise";

/// The Secret that holds the sandbox store's recovery key and initial root token (platform
/// `scripts/openbao-bootstrap.sh`). Sandbox only: a root token in a Secret is acceptable for a
/// throwaway store and for nothing else.
pub const INIT_SECRET: &str = "openbao-init";

/// The API-server service proxy path for one endpoint of the store. The operator reaches the
/// store through the API server, so no HTTP client, Service or port-forward is needed and the
/// response body never passes through a container log.
pub fn store_proxy_path(namespace: &str, path: &str) -> String {
    format!("/api/v1/namespaces/{namespace}/services/http:openbao:8200/proxy{path}")
}

/// The store path an owner group may read, and nothing else (decision D27).
pub fn group_path_prefix(group: &str) -> String {
    format!("connections/group/{group}/")
}

/// The OpenBao policy an owner group's store signs in with: read on its own prefix only.
/// The platform writes the same text in `scripts/eso-openbao-store.sh`.
pub fn group_policy(group: &str) -> String {
    format!(
        "path \"secret/data/{}*\" {{ capabilities = [\"read\"] }}\n",
        group_path_prefix(group)
    )
}

/// Policy name and Kubernetes-auth role name (the platform uses one string for both).
pub fn group_role_name(namespace: &str, group: &str) -> String {
    format!("eso-{namespace}-{group}")
}

/// The ServiceAccount External Secrets signs in as; the store's role is bound to its name.
pub fn group_service_account(namespace: &str, group: &str) -> Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": format!("eso-{group}"),
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "stardelt-operator",
                "stardelt.io/component": "openbao",
            },
        },
    })
}

/// The namespaced `SecretStore` (`external-secrets.io/v1`) of one owner group.
pub fn group_secret_store(namespace: &str, group: &str) -> Value {
    serde_json::json!({
        "apiVersion": "external-secrets.io/v1",
        "kind": "SecretStore",
        "metadata": {
            "name": format!("openbao-{group}"),
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "stardelt-operator",
                "stardelt.io/component": "openbao",
            },
        },
        "spec": {
            "provider": {
                "vault": {
                    "server": format!("http://openbao.{namespace}.svc.cluster.local:8200"),
                    "path": "secret",
                    "version": "v2",
                    "auth": {
                        "kubernetes": {
                            "mountPath": "kubernetes",
                            "role": group_role_name(namespace, group),
                            "serviceAccountRef": { "name": format!("eso-{group}") },
                        },
                    },
                },
            },
        },
    })
}

/// The `SecretStore` API resource (`secretstores.external-secrets.io/v1`, namespaced).
pub fn secret_store_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("external-secrets.io", "v1", "SecretStore"),
        "secretstores",
    )
}

/// The body of `bao operator init` with one recovery share (auto-unseal: no unseal shares).
/// `secret_shares: 0` is what the static seal needs; the platform script leaves the CLI's 5/3
/// default, which the static seal never uses, so only the recovery pair has to match it.
pub fn init_request_body() -> Value {
    serde_json::json!({
        "recovery_shares": 1,
        "recovery_threshold": 1,
        "secret_shares": 0,
        "secret_threshold": 0
    })
}

/// Secret `openbao-init` for `spec.namespace`. `recovery_key` is the base64 string the store
/// returned in `recovery_keys_b64[0]`, stored unchanged, exactly as the platform's script does.
pub fn init_secret(namespace: &str, recovery_key: &str, root_token: &str) -> Secret {
    let mut data = BTreeMap::new();
    data.insert(
        "recovery-key".to_string(),
        ByteString(recovery_key.as_bytes().to_vec()),
    );
    data.insert(
        "root-token".to_string(),
        ByteString(root_token.as_bytes().to_vec()),
    );
    Secret {
        metadata: ObjectMeta {
            name: Some(INIT_SECRET.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/part-of".to_string(),
                "stardelt".to_string(),
            )])),
            ..ObjectMeta::default()
        },
        data: Some(data),
        ..Secret::default()
    }
}

/// The ConfigMap a namespace's `Connection` objects read to learn where their store is. Same shape
/// as `stardelt-identity`: the platform publishes it, the controllers read it and default to `none`.
pub const MODE_CONFIGMAP: &str = "stardelt-secrets";

/// Value of the `stardelt.io/secrets-mode` label and the `mode` key.
pub fn mode_label(mode: SecretsMode) -> &'static str {
    match mode {
        SecretsMode::None => "none",
        SecretsMode::Platform => "platform",
        SecretsMode::External => "external",
    }
}

/// ConfigMap `stardelt-secrets` for `spec.namespace`: key `mode` is `none`, `platform` or `external`.
pub fn mode_config_map(spec: &PlatformInstanceSpec, namespace: &str) -> ConfigMap {
    let mode = mode_label(spec.secrets.mode);
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(MODE_CONFIGMAP.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    "app.kubernetes.io/part-of".to_string(),
                    "stardelt".to_string(),
                ),
                ("stardelt.io/secrets-mode".to_string(), mode.to_string()),
            ])),
            ..ObjectMeta::default()
        },
        data: Some(BTreeMap::from([("mode".to_string(), mode.to_string())])),
        ..ConfigMap::default()
    }
}

/// The External Secrets `ClusterSecretStore` resource (`clustersecretstores.external-secrets.io/v1`).
pub fn cluster_secret_store_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("external-secrets.io", "v1", "ClusterSecretStore"),
        "clustersecretstores",
    )
}

/// The single provider block of the CR (`vault`, `aws`, `azurekv`, …). Exactly one provider key:
/// two would make the choice silent, and External Secrets stores one provider per object.
pub fn provider_block(provider: &Value) -> Result<(&str, &Value), String> {
    let obj = provider
        .as_object()
        .ok_or_else(|| "secrets.external.provider must be an object".to_string())?;
    if obj.is_empty() {
        return Err("secrets.external.provider must have exactly one provider key".to_string());
    }
    if obj.len() != 1 {
        return Err("secrets.external.provider must have exactly one provider key".to_string());
    }
    let (key, value) = obj.iter().next().unwrap();
    Ok((key.as_str(), value))
}

fn find_secret_ref_name(v: &Value) -> Option<String> {
    match v {
        Value::Object(map) => {
            for key in ["secretRef", "tokenSecretRef", "authSecretRef"] {
                if let Some(Value::Object(ref_obj)) = map.get(key)
                    && let Some(Value::String(name)) = ref_obj.get("name")
                {
                    return Some(name.clone());
                }
            }
            for val in map.values() {
                if let Some(name) = find_secret_ref_name(val) {
                    return Some(name);
                }
            }
            None
        }
        Value::Array(arr) => {
            for val in arr {
                if let Some(name) = find_secret_ref_name(val) {
                    return Some(name);
                }
            }
            None
        }
        _ => None,
    }
}

/// The Secret that holds the store's credentials, **by name**: a `secretRef`, `tokenSecretRef` or
/// `authSecretRef` object with a `name`, at any depth of the provider block. A block without one is
/// refused — the platform never carries a credential in a CR.
pub fn credential_secret_name(provider: &Value) -> Result<String, String> {
    if let Value::Object(root) = provider
        && let Some(Value::Object(vault)) = root.get("vault")
        && vault.contains_key("token")
        && !vault.contains_key("tokenSecretRef")
    {
        return Err("secrets.external.provider must reference a Secret by name (secretRef/tokenSecretRef/authSecretRef), never inline credentials".to_string());
    }

    if let Some(name) = find_secret_ref_name(provider) {
        return Ok(name);
    }
    Err("secrets.external.provider must reference a Secret by name (secretRef/tokenSecretRef/authSecretRef)".to_string())
}

/// A provider block that authenticates without any stored credential: Vault's Kubernetes
/// auth exchanges the workload's own service-account token, so there is no Secret to name.
/// `auth.kubernetes` needs both `role` and `serviceAccountRef.name` — the two fields
/// External Secrets requires; anything less is not a usable Kubernetes-auth store.
pub fn uses_kubernetes_auth(provider: &Value) -> bool {
    provider
        .get("vault")
        .and_then(|v| v.get("auth"))
        .and_then(|a| a.get("kubernetes"))
        .is_some_and(|k| {
            k.get("role").and_then(Value::as_str).is_some()
                && k.get("serviceAccountRef")
                    .and_then(|s| s.get("name"))
                    .and_then(Value::as_str)
                    .is_some()
        })
}

/// The `ClusterSecretStore` for this instance: `Ok(None)` for `none`/`platform`, `Err` when
/// `external` mode is incomplete, `Ok(Some(_))` with the provider block passed through unchanged.
pub fn external_store(spec: &PlatformInstanceSpec) -> Result<Option<Value>, String> {
    match spec.secrets.mode {
        SecretsMode::None | SecretsMode::Platform => Ok(None),
        SecretsMode::External => {
            let external = spec.secrets.external.as_ref().ok_or_else(|| {
                "secrets.mode external needs secrets.external.provider".to_string()
            })?;
            let provider = &external.provider;
            let (provider_name, _provider_value) = provider_block(provider)?;
            if uses_kubernetes_auth(provider) {
                // Kubernetes auth carries no credential, but an inline token next to it is still a
                // credential in the CR and stays refused.
                if let Value::Object(root) = provider
                    && let Some(Value::Object(vault)) = root.get("vault")
                    && vault.contains_key("token")
                    && !vault.contains_key("tokenSecretRef")
                {
                    return Err("secrets.external.provider must reference a Secret by name (secretRef/tokenSecretRef/authSecretRef), never inline credentials".to_string());
                }
            } else {
                let _ = credential_secret_name(provider)?;
            }
            Ok(Some(serde_json::json!({
                "apiVersion": "external-secrets.io/v1",
                "kind": "ClusterSecretStore",
                "metadata": {
                    "name": ENTERPRISE_STORE,
                    "labels": {
                        "app.kubernetes.io/managed-by": "stardelt-operator",
                        "stardelt.io/component": "secret-store",
                        "stardelt.io/provider": provider_name
                    }
                },
                "spec": {
                    "provider": provider
                }
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::ExternalSecretsSpec;

    #[test]
    fn provider_block_accepts_one_key_and_rejects_empty_and_two() {
        assert!(provider_block(&serde_json::json!({})).is_err());
        assert!(provider_block(&serde_json::json!({"vault": {}, "aws": {}})).is_err());
        let v = serde_json::json!({"vault": {"server": "x"}});
        let (k, _) = provider_block(&v).unwrap();
        assert_eq!(k, "vault");
    }

    #[test]
    fn credential_secret_name_finds_refs() {
        let v1 = serde_json::json!({"vault": {"auth": {"tokenSecretRef": {"name": "vault-token", "key": "token"}}}});
        assert_eq!(credential_secret_name(&v1).unwrap(), "vault-token");
        let v2 = serde_json::json!({"aws": {"auth": {"secretRef": {"name": "aws-creds", "key": "access-key"}}}});
        assert_eq!(credential_secret_name(&v2).unwrap(), "aws-creds");
        let v3 = serde_json::json!({"azurekv": {"auth": {"authSecretRef": {"name": "az-creds", "key": "client-secret"}}}});
        assert_eq!(credential_secret_name(&v3).unwrap(), "az-creds");
    }

    #[test]
    fn credential_secret_name_rejects_inline() {
        let err = credential_secret_name(
            &serde_json::json!({"vault": {"server": "…", "token": "s3cret"}}),
        )
        .unwrap_err();
        assert!(err.contains("secretRef"));
    }

    #[test]
    fn mode_config_map_carries_the_mode_for_each_kind() {
        use crate::crd::{ComponentValues, IdentitySpec, IngressSettings, Pillars, Profile};
        let base = PlatformInstanceSpec {
            profile: Profile::Demo,
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: ComponentValues::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: IdentitySpec::default(),
            secrets: crate::crd::SecretsSpec {
                mode: SecretsMode::None,
                external: None,
                groups: Vec::new(),
            },
            ingress: IngressSettings::default(),
            monitoring: Default::default(),
            mcp: Default::default(),
        };
        for (mode, label) in [
            (SecretsMode::None, "none"),
            (SecretsMode::Platform, "platform"),
            (SecretsMode::External, "external"),
        ] {
            let mut spec = base.clone();
            spec.secrets.mode = mode;
            let cm = mode_config_map(&spec, "stardelt");
            assert_eq!(cm.metadata.name.as_deref(), Some("stardelt-secrets"));
            assert_eq!(cm.metadata.namespace.as_deref(), Some("stardelt"));
            let labels = cm.metadata.labels.as_ref().unwrap();
            assert_eq!(labels.get("stardelt.io/secrets-mode").unwrap(), label);
            assert_eq!(labels.get("app.kubernetes.io/part-of").unwrap(), "stardelt");
            let data = cm.data.as_ref().unwrap();
            assert_eq!(data.get("mode").unwrap(), label);
        }
    }

    #[test]
    fn external_store_behaviour() {
        use crate::crd::{ComponentValues, IdentitySpec, IngressSettings, Pillars, Profile};
        let mut spec = PlatformInstanceSpec {
            profile: Profile::Demo,
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: ComponentValues::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: IdentitySpec::default(),
            secrets: crate::crd::SecretsSpec {
                mode: SecretsMode::None,
                external: None,
                groups: Vec::new(),
            },
            ingress: IngressSettings::default(),
            monitoring: Default::default(),
            mcp: Default::default(),
        };
        assert!(external_store(&spec).unwrap().is_none());
        spec.secrets.mode = SecretsMode::Platform;
        assert!(external_store(&spec).unwrap().is_none());
        spec.secrets.mode = SecretsMode::External;
        assert!(external_store(&spec).is_err());
        spec.secrets.external = Some(ExternalSecretsSpec {
            provider: serde_json::json!({"vault": {"server": "https://vault.example.com", "path": "secret", "version": "v2", "auth": {"tokenSecretRef": {"name": "vault-token", "key": "token"}}}}),
        });
        let store = external_store(&spec).unwrap().unwrap();
        assert_eq!(store["metadata"]["name"], "enterprise");
        assert_eq!(
            store["spec"]["provider"],
            spec.secrets.external.as_ref().unwrap().provider
        );
    }

    #[test]
    fn kubernetes_auth_needs_no_credential_secret() {
        use crate::crd::{ComponentValues, IdentitySpec, IngressSettings, Pillars, Profile};
        let kubernetes_auth = serde_json::json!({"vault": {
            "server": "https://vault.example.com", "path": "secret", "version": "v2",
            "auth": {"kubernetes": {"mountPath": "kubernetes", "role": "stardelt",
                                     "serviceAccountRef": {"name": "stardelt-secret-store", "namespace": "stardelt"}}}
        }});
        assert!(uses_kubernetes_auth(&kubernetes_auth));
        // The old rule still refuses the block: this test pins that the exception is deliberate.
        assert!(credential_secret_name(&kubernetes_auth).is_err());
        let spec = PlatformInstanceSpec {
            profile: Profile::Demo,
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: ComponentValues::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: IdentitySpec::default(),
            secrets: crate::crd::SecretsSpec {
                mode: SecretsMode::External,
                external: Some(ExternalSecretsSpec {
                    provider: kubernetes_auth.clone(),
                }),
                groups: Vec::new(),
            },
            ingress: IngressSettings::default(),
            monitoring: Default::default(),
            mcp: Default::default(),
        };
        let store = external_store(&spec).unwrap().unwrap();
        assert_eq!(store["spec"]["provider"], kubernetes_auth);
        // A block with neither a Secret reference nor complete Kubernetes auth is still refused,
        // and so is an inline token next to Kubernetes auth.
        for refused in [
            serde_json::json!({"vault": {"server": "https://vault.example.com"}}),
            serde_json::json!({"vault": {"server": "https://vault.example.com", "token": "s3cret",
                "auth": {"kubernetes": {"role": "stardelt",
                    "serviceAccountRef": {"name": "stardelt-secret-store"}}}}}),
        ] {
            let mut refused_spec = spec.clone();
            refused_spec.secrets.external = Some(ExternalSecretsSpec { provider: refused });
            assert!(external_store(&refused_spec).is_err());
        }
    }

    #[test]
    fn init_request_and_secret_match_the_platform_script() {
        assert_eq!(
            store_proxy_path("stardelt", "/v1/sys/init"),
            "/api/v1/namespaces/stardelt/services/http:openbao:8200/proxy/v1/sys/init"
        );
        let body = init_request_body();
        assert_eq!(body["recovery_shares"], 1);
        assert_eq!(body["recovery_threshold"], 1);
        assert_eq!(body["secret_shares"], 0);
        let secret = init_secret("stardelt", "cmVjb3Zlcnk=", "s.roottoken");
        assert_eq!(secret.metadata.name.as_deref(), Some("openbao-init"));
        let data = secret.data.expect("data");
        assert_eq!(data["recovery-key"].0, b"cmVjb3Zlcnk=".to_vec());
        assert_eq!(data["root-token"].0, b"s.roottoken".to_vec());
    }
}

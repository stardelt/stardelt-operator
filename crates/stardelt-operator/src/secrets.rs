//! The enterprise's secret store in `secrets.mode: external` (decision D23): one cluster-scoped
//! `ClusterSecretStore` named [`ENTERPRISE_STORE`], rendered from the CR's verbatim provider block.
//! The decisions are pure functions so they are tested without a cluster.

use kube::core::{ApiResource, GroupVersionKind};
use serde_json::Value;

use crate::crd::{PlatformInstanceSpec, SecretsMode};

/// The name every external-mode `ExternalSecret` references.
pub const ENTERPRISE_STORE: &str = "enterprise";

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
            let _ = credential_secret_name(provider)?;
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
    fn external_store_behaviour() {
        use crate::crd::{ComponentValues, IdentitySpec, IngressSettings, Pillars, Profile};
        let mut spec = PlatformInstanceSpec {
            profile: Profile::Demo,
            pillars: Pillars::default(),
            dry_run: true,
            component_values: ComponentValues::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: IdentitySpec::default(),
            secrets: crate::crd::SecretsSpec {
                mode: SecretsMode::None,
                external: None,
            },
            ingress: IngressSettings::default(),
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
}

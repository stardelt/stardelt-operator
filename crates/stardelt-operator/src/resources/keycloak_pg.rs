//! The `keycloak-pg` CloudNative-PG `Cluster`.
//!
//! Backs Keycloak's realm/user data. Single-instance for the MVP. CNPG
//! auto-creates the `keycloak-pg-app` Secret (user creds) that the Keycloak
//! HelmRelease references. Mirrors `cnpg.rs` (lakekeeper-pg).

use kube::core::GroupVersionKind;
use serde_json::{Value, json};

use crate::api::PlatformInstance;

pub const CLUSTER_NAME: &str = "keycloak-pg";
pub const DB_NAME: &str = "keycloak";

pub fn gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("postgresql.cnpg.io", "v1", "Cluster")
}

pub fn build(pi: &PlatformInstance, owner: Value) -> Value {
    let ns = &pi.spec.namespace;
    json!({
        "apiVersion": "postgresql.cnpg.io/v1",
        "kind": "Cluster",
        "metadata": {
            "name": CLUSTER_NAME,
            "namespace": ns,
            "labels": super::labels(),
            "ownerReferences": [owner],
        },
        "spec": {
            "instances": 1,
            "storage": { "size": pi.spec.storage.postgres_size },
            "bootstrap": {
                "initdb": { "database": DB_NAME, "owner": "keycloak" }
            },
            "resources": {
                "requests": { "cpu": "50m", "memory": "256Mi" },
                "limits":   { "cpu": "1",   "memory": "512Mi" }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::PlatformInstance;

    fn instance() -> PlatformInstance {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "platform.stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "t" },
            "spec": { "namespace": "stardelt" }
        }))
        .unwrap()
    }

    #[test]
    fn keycloak_pg_initdb_targets_keycloak_db() {
        let c = build(&instance(), json!({}));
        assert_eq!(c["spec"]["bootstrap"]["initdb"]["database"], "keycloak");
        assert_eq!(c["metadata"]["name"], "keycloak-pg");
    }
}

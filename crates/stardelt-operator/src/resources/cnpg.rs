//! The `lakekeeper-pg` CloudNative-PG `Cluster`.
//!
//! Backs Lakekeeper's catalog metadata. Single-instance for the MVP. CNPG
//! auto-creates the `lakekeeper-pg-app` Secret (user creds) that the Lakekeeper
//! HelmRelease references via `externalDatabase`.
//!
//! No `k8s-openapi` binding exists for `postgresql.cnpg.io/v1 Cluster`, so this
//! is built as a JSON body and applied as a `DynamicObject`.

use kube::core::GroupVersionKind;
use serde_json::{Value, json};

use crate::api::PlatformInstance;

pub const CLUSTER_NAME: &str = "lakekeeper-pg";

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
                "initdb": { "database": "lakekeeper", "owner": "lakekeeper" }
            },
            "resources": {
                "requests": { "cpu": "50m", "memory": "256Mi" },
                "limits":   { "cpu": "1",   "memory": "512Mi" }
            }
        }
    })
}

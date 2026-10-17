//! stardelt CRDs — the plug specification.
//!
//! These types are the contract between a user's declarative intent and the
//! upstream charts/operators the reconciler applies. See the master spec §5.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Deployment size / capability profile.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    #[default]
    Demo,
    Full,
    Airgap,
}

fn default_profile() -> Profile {
    Profile::Demo
}

/// Which pillars a platform instance enables.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct Pillars {
    #[serde(default = "yes")]
    pub lakehouse: bool,
    #[serde(default)]
    pub batch: bool,
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub ml: bool,
}

fn yes() -> bool {
    true
}

impl Default for Pillars {
    fn default() -> Self {
        Self { lakehouse: true, batch: false, streaming: false, ml: false }
    }
}

/// Cluster-scoped root resource: declares which pillars and profile are installed.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "PlatformInstance",
    status = "PlatformInstanceStatus",
    shortname = "platform",
    doc = "A stardelt platform instance."
)]
pub struct PlatformInstanceSpec {
    #[serde(default = "default_profile")]
    pub profile: Profile,
    #[serde(default)]
    pub pillars: Pillars,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct PlatformInstanceStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Namespaced isolation boundary: a tenant's data and identities.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "Tenant",
    namespaced,
    status = "TenantStatus",
    shortname = "tenant",
    doc = "A tenant: an isolation boundary for data and identities."
)]
pub struct TenantSpec {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default = "default_profile")]
    pub profile: Profile,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct TenantStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Catalog implementation (the plug behind the Iceberg REST contract).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CatalogImpl {
    #[default]
    Lakekeeper,
    Polaris,
}

/// Object-store implementation.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
pub enum StorageImpl {
    #[default]
    #[serde(rename = "seaweedfs")]
    Seaweedfs,
    #[serde(rename = "byoS3")]
    ByoS3,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct CatalogSpec {
    #[serde(default)]
    pub implementation: CatalogImpl,
}

impl Default for CatalogSpec {
    fn default() -> Self {
        Self { implementation: CatalogImpl::Lakekeeper }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct StorageSpec {
    #[serde(default)]
    pub implementation: StorageImpl,
    #[serde(default)]
    pub bucket: Option<String>,
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self { implementation: StorageImpl::Seaweedfs, bucket: None }
    }
}

/// A managed lakehouse: catalog + storage (+ query) on object storage.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "Lakehouse",
    namespaced,
    status = "LakehouseStatus",
    shortname = "lh",
    doc = "A managed Trino + catalog + Iceberg unit on object storage."
)]
pub struct LakehouseSpec {
    #[serde(default)]
    pub catalog: CatalogSpec,
    #[serde(default)]
    pub storage: StorageSpec,
    #[serde(default = "default_profile")]
    pub profile: Profile,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct LakehouseStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub components: Vec<ComponentStatus>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ComponentStatus {
    pub name: String,
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct Condition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn lakehouse_defaults_are_lakekeeper_seaweedfs_demo() {
        let lh: Lakehouse = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt" },
            "spec": {}
        }))
        .expect("deserialize Lakehouse");
        assert_eq!(lh.spec.catalog.implementation, CatalogImpl::Lakekeeper);
        assert_eq!(lh.spec.storage.implementation, StorageImpl::Seaweedfs);
        assert_eq!(lh.spec.profile, Profile::Demo);
    }

    #[test]
    fn lakehouse_parses_explicit_implementations() {
        let lh: Lakehouse = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt" },
            "spec": {
                "catalog": { "implementation": "polaris" },
                "storage": { "implementation": "byoS3", "bucket": "s3://acme-lake" },
                "profile": "full"
            }
        }))
        .expect("deserialize Lakehouse");
        assert_eq!(lh.spec.catalog.implementation, CatalogImpl::Polaris);
        assert_eq!(lh.spec.storage.implementation, StorageImpl::ByoS3);
        assert_eq!(lh.spec.storage.bucket.as_deref(), Some("s3://acme-lake"));
        assert_eq!(lh.spec.profile, Profile::Full);
    }

    #[test]
    fn platform_instance_defaults_to_demo_lakehouse_only() {
        let pi: PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {}
        }))
        .expect("deserialize PlatformInstance");
        assert_eq!(pi.spec.profile, Profile::Demo);
        assert!(pi.spec.pillars.lakehouse);
        assert!(!pi.spec.pillars.batch);
    }

    #[test]
    fn crd_manifest_generation_succeeds() {
        // kube can render a CRD manifest from the type; the schema must be valid.
        let crd = Lakehouse::crd();
        assert_eq!(crd.spec.names.kind, "Lakehouse");
        assert_eq!(crd.spec.group, "stardelt.io");
    }
}

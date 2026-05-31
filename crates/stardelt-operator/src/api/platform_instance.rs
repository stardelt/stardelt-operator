//! The `PlatformInstance` custom resource.
//!
//! Cluster-scoped. A single `PlatformInstance` declares the whole stardelt core
//! stack (object storage → catalog → query engine → orchestration → BI → UI) as
//! one unit. Its controller reconciles all components in dependency order and
//! records per-component readiness in `status.conditions`.
//!
//! Group/version: `platform.stardelt.io/v1alpha1`.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::readiness::Condition;

/// Pinned upstream chart versions. Mirror of the constants in
/// `stardelt-platform/Makefile` and `stardelt-demos/kind/up.sh` — keep all three
/// in sync when bumping (see CLAUDE.md).
pub mod chart_versions {
    pub const CNPG: &str = "0.28.2";
    pub const SEAWEEDFS: &str = "4.25.1";
    pub const LAKEKEEPER: &str = "0.11.0";
    pub const TRINO: &str = "1.42.2";
    pub const AIRFLOW: &str = "1.21.0";
    pub const SUPERSET: &str = "0.15.5";
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "platform.stardelt.io",
    version = "v1alpha1",
    kind = "PlatformInstance",
    status = "PlatformInstanceStatus",
    shortname = "pi",
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PlatformInstanceSpec {
    /// Target namespace for the stack. Created if absent. Default: `stardelt`.
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Object-storage (SeaweedFS S3) settings. Drives both the SeaweedFS
    /// HelmRelease values and the `ozone-s3-creds` Secret, so they always match.
    #[serde(default)]
    pub s3: S3Spec,

    /// Persistent-volume sizing for the data plane.
    #[serde(default)]
    pub storage: StorageSpec,

    /// Per-component knobs and enable flags.
    #[serde(default)]
    pub components: Components,

    /// Optional chart-version overrides. When unset, the pinned
    /// [`chart_versions`] constants are used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chart_versions: Option<ChartVersionOverrides>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3Spec {
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub region: String,
}

impl Default for S3Spec {
    fn default() -> Self {
        // Matches helm-values/seaweedfs.yaml + s3-credentials.example.yaml.
        Self {
            access_key: "stardelt-access-key".into(),
            secret_key: "stardelt-secret-key".into(),
            bucket: "lakehouse".into(),
            region: "us-east-1".into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// SeaweedFS volume-server data size (holds Iceberg Parquet). Default `20Gi`.
    #[serde(default = "default_volume_size")]
    pub volume_size: String,
    /// CNPG Postgres data size. Default `2Gi`.
    #[serde(default = "default_pg_size")]
    pub postgres_size: String,
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            volume_size: default_volume_size(),
            postgres_size: default_pg_size(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Components {
    #[serde(default)]
    pub trino: TrinoSpec,
    #[serde(default = "default_true")]
    pub airflow_enabled: bool,
    #[serde(default = "default_true")]
    pub superset_enabled: bool,
    #[serde(default)]
    pub nova: NovaSpec,
}

impl Default for Components {
    fn default() -> Self {
        Self {
            trino: TrinoSpec::default(),
            airflow_enabled: true,
            superset_enabled: true,
            nova: NovaSpec::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrinoSpec {
    #[serde(default = "default_workers")]
    pub workers: u32,
}

impl Default for TrinoSpec {
    fn default() -> Self {
        Self {
            workers: default_workers(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NovaSpec {
    #[serde(default = "default_nova_image")]
    pub image: String,
}

impl Default for NovaSpec {
    fn default() -> Self {
        Self {
            image: default_nova_image(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChartVersionOverrides {
    pub cnpg: Option<String>,
    pub seaweedfs: Option<String>,
    pub lakekeeper: Option<String>,
    pub trino: Option<String>,
    pub airflow: Option<String>,
    pub superset: Option<String>,
}

/// Operator-managed status. `conditions` carries one entry per reconcile step
/// plus a top-level `Ready`.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlatformInstanceStatus {
    /// Human-readable summary, e.g. `Reconciling: Lakekeeper` or `Ready`.
    #[serde(default)]
    pub phase: String,
    /// `"True"` once every component is ready, else `"False"`.
    #[serde(default)]
    pub ready: String,
    /// The `.metadata.generation` this status reflects.
    #[serde(default)]
    pub observed_generation: i64,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

fn default_namespace() -> String {
    "stardelt".into()
}
fn default_volume_size() -> String {
    "20Gi".into()
}
fn default_pg_size() -> String {
    "2Gi".into()
}
fn default_workers() -> u32 {
    1
}
fn default_true() -> bool {
    true
}
fn default_nova_image() -> String {
    "stardelt/nova:dev".into()
}

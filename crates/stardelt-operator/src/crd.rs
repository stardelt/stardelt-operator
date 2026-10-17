//! stardelt CRDs — the plug specification.
//!
//! These types are the contract between a user's declarative intent and the
//! upstream charts/operators the reconciler applies. See the master spec §5.

use std::collections::BTreeMap;

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
        Self {
            lakehouse: true,
            batch: false,
            streaming: false,
            ml: false,
        }
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
    #[serde(default, rename = "displayName")]
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
        Self {
            implementation: CatalogImpl::Lakekeeper,
        }
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
        Self {
            implementation: StorageImpl::Seaweedfs,
            bucket: None,
        }
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
    /// Plan only (no cluster changes) until explicitly disabled.  Safe default.
    #[serde(default = "default_true", rename = "dryRun")]
    pub dry_run: bool,
    /// Extra Helm values applied to every component (`--set key=value`).
    #[serde(default)]
    pub set: std::collections::BTreeMap<String, String>,
    /// Take over Helm releases this operator did not create. Off by default so a
    /// Lakehouse can never silently clobber an existing install.
    #[serde(default)]
    pub adopt: bool,
}

fn default_true() -> bool {
    true
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

/// Ingestion domain — the deliberate three-way split of the ingestion contract
/// (master spec §5, plugs 8–10). One contract, three defaults.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IngestionDomain {
    /// SaaS connectors — default implementation Apache SeaTunnel.
    #[default]
    Saas,
    /// Enterprise / edge integration — default implementation Apache NiFi.
    Enterprise,
    /// Change data capture — default implementation Debezium (+ Kafka).
    Cdc,
}

/// An ingestion source: one contract, three domain defaults.
///
/// The source itself is deliberately opaque to the operator. `source` carries the
/// connector's own key/value parameters (host, port, database, topics, tables, …)
/// and is read by the plug (SeaTunnel / NiFi / Debezium), not by the reconciler —
/// that keeps the CRD stable while connectors evolve.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "IngestionSource",
    namespaced,
    status = "IngestionSourceStatus",
    shortname = "ingest",
    doc = "An ingestion source: one contract with three domain defaults (saas, enterprise, cdc)."
)]
pub struct IngestionSourceSpec {
    #[serde(default)]
    pub domain: IngestionDomain,
    /// Destination table, e.g. `warehouse.raw.events`. Ingestion lands in Iceberg
    /// through the REST catalog — never in a side channel.
    pub target: String,
    /// Optional cron schedule; when unset the source follows its domain default
    /// (continuous for cdc, otherwise the connector's own cadence).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// Connector-specific parameters, passed through to the plug verbatim.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub source: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct IngestionSourceStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// What a pipeline step does — the step contract behind `Pipeline`
/// (master spec §5, plugs 6, 7 and 12).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StepKind {
    /// SQL transform, executed by the pipeline's engine.
    Sql,
    /// Python transform.
    Python,
    /// dbt-core model run.
    Dbt,
    /// Data-quality gate (Soda + dbt tests).
    Quality,
}

/// One step of a `Pipeline`. Steps run in declaration order.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct PipelineStep {
    pub name: String,
    pub kind: StepKind,
    /// Step-specific parameters (model path, SQL file, gate threshold, …).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub config: BTreeMap<String, String>,
}

/// Engine that orchestrates (and for `spark`, executes) the pipeline.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PipelineEngine {
    /// Default implementation Apache Airflow 3.
    #[default]
    Airflow,
    /// Apache Spark (Spark Connect) for heavy compute.
    Spark,
}

/// A transform pipeline: an ordered list of steps, run by one engine.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "Pipeline",
    namespaced,
    status = "PipelineStatus",
    shortname = "pipe",
    doc = "A transform pipeline: ordered sql/python/dbt/quality steps on one engine."
)]
pub struct PipelineSpec {
    /// Steps in apply order; must not be empty.
    #[schemars(length(min = 1))]
    pub steps: Vec<PipelineStep>,
    /// Optional cron schedule; a pipeline without a schedule is run on demand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    #[serde(default)]
    pub engine: PipelineEngine,
}

/// Last-known run of a pipeline. Intentionally minimal: the authoritative history
/// belongs to the engine adapter (Airflow / Spark); this is the operator's view.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct PipelineRun {
    pub id: String,
    /// Free-form until the adapters land (`queued` | `running` | `succeeded` | `failed`).
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct PipelineStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub runs: Vec<PipelineRun>,
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
        assert!(lh.spec.dry_run, "new Lakehouses must plan only");
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

    #[test]
    fn ingestion_source_defaults_to_saas_with_empty_source() {
        let src: IngestionSource = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "IngestionSource",
            "metadata": { "name": "events", "namespace": "stardelt" },
            "spec": { "target": "warehouse.raw.events" }
        }))
        .expect("deserialize IngestionSource");
        assert_eq!(src.spec.domain, IngestionDomain::Saas);
        assert_eq!(src.spec.target, "warehouse.raw.events");
        assert_eq!(src.spec.schedule, None);
        assert!(src.spec.source.is_empty());
        assert_eq!(src.metadata.namespace.as_deref(), Some("stardelt"));
    }

    #[test]
    fn ingestion_source_explicit_cdc_spec_round_trips() {
        let spec = serde_json::json!({
            "domain": "cdc",
            "target": "warehouse.raw.customers",
            "schedule": "*/5 * * * *",
            "source": { "connector": "postgres", "table": "public.customers" }
        });
        let parsed: IngestionSourceSpec =
            serde_json::from_value(spec.clone()).expect("deserialize IngestionSourceSpec");
        assert_eq!(parsed.domain, IngestionDomain::Cdc);
        assert_eq!(parsed.schedule.as_deref(), Some("*/5 * * * *"));
        assert_eq!(
            parsed.source.get("connector").map(String::as_str),
            Some("postgres")
        );
        assert_eq!(serde_json::to_value(&parsed).expect("serialize"), spec);
    }

    #[test]
    fn ingestion_source_rejects_unknown_domain() {
        let err = serde_json::from_value::<IngestionSourceSpec>(serde_json::json!({
            "domain": "webhook",
            "target": "warehouse.raw.events"
        }));
        assert!(err.is_err(), "unknown domains must not silently default");
    }

    #[test]
    fn pipeline_defaults_to_airflow_unless_stated() {
        let p: Pipeline = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Pipeline",
            "metadata": { "name": "daily", "namespace": "stardelt" },
            "spec": {
                "steps": [
                    { "name": "load", "kind": "sql" },
                    { "name": "assert", "kind": "quality", "config": { "gate": "warn" } }
                ]
            }
        }))
        .expect("deserialize Pipeline");
        assert_eq!(p.spec.engine, PipelineEngine::Airflow);
        assert_eq!(p.spec.schedule, None);
        assert_eq!(p.spec.steps.len(), 2);
        assert_eq!(p.spec.steps[0].kind, StepKind::Sql);
        assert!(p.spec.steps[0].config.is_empty());
        assert_eq!(p.spec.steps[1].kind, StepKind::Quality);
        assert_eq!(
            p.spec.steps[1].config.get("gate").map(String::as_str),
            Some("warn")
        );
    }

    #[test]
    fn pipeline_explicit_spark_spec_round_trips() {
        let spec = serde_json::json!({
            "steps": [
                { "name": "build", "kind": "dbt", "config": { "selector": "tag:nightly" } },
                { "name": "enrich", "kind": "python" }
            ],
            "schedule": "0 3 * * *",
            "engine": "spark"
        });
        let parsed: PipelineSpec =
            serde_json::from_value(spec.clone()).expect("deserialize PipelineSpec");
        assert_eq!(parsed.engine, PipelineEngine::Spark);
        assert_eq!(parsed.steps[0].kind, StepKind::Dbt);
        assert_eq!(parsed.steps[1].kind, StepKind::Python);
        assert_eq!(serde_json::to_value(&parsed).expect("serialize"), spec);
    }

    #[test]
    fn pipeline_status_carries_runs() {
        let run = PipelineRun {
            id: "2026-10-17T03:00:00Z".into(),
            state: "succeeded".into(),
            started_at: Some("2026-10-17T03:00:00Z".into()),
        };
        let status = PipelineStatus {
            phase: "Ready".into(),
            conditions: vec![],
            runs: vec![run],
        };
        let json = serde_json::to_value(&status).expect("serialize PipelineStatus");
        assert_eq!(json["runs"][0]["state"], "succeeded");
        assert_eq!(json["conditions"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn ingestion_and_pipeline_crd_manifests_generate() {
        let ingest = IngestionSource::crd();
        assert_eq!(ingest.spec.names.kind, "IngestionSource");
        assert_eq!(ingest.spec.scope, "Namespaced");
        let pipeline = Pipeline::crd();
        assert_eq!(pipeline.spec.names.kind, "Pipeline");
        assert_eq!(pipeline.spec.scope, "Namespaced");
    }
}

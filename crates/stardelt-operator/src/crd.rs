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
    /// Multi-user notebooks (JupyterHub). Off by default: a notebook stack is
    /// heavy, and a cluster that only serves queries should not carry it.
    #[serde(default)]
    pub notebooks: bool,
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
            notebooks: false,
            streaming: false,
            ml: false,
        }
    }
}

/// A reference to one key of a ConfigMap or Secret.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct ValuesKeyRef {
    /// Name of the ConfigMap or Secret.
    pub name: String,
    /// Key holding the YAML values document.
    pub key: String,
    /// Namespace of the referenced object. Defaults to the resource's own
    /// namespace; required for cluster-scoped resources (`PlatformInstance`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// External source of one component's Helm values: set exactly one of
/// `configMapRef` or `secretRef`.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
pub struct ValuesSource {
    /// Read the values document from this ConfigMap key.
    #[serde(
        default,
        rename = "configMapRef",
        skip_serializing_if = "Option::is_none"
    )]
    pub config_map_ref: Option<ValuesKeyRef>,
    /// Read the values document from this Secret key (decoded, then parsed as YAML).
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<ValuesKeyRef>,
}

/// Per-component Helm value overrides, shared by the root CRDs.
///
/// Administrators configure each plug (chart values, sizing, storage class,
/// ingress, image registry) here. The operator ships golden defaults per
/// component — the curated settings that make the stack work — and these
/// fields layer on top. Merge order per component, later wins:
///
/// 1. the operator's golden defaults,
/// 2. `valuesFrom` (external YAML document),
/// 3. `values` (free-form YAML inline in the CR),
/// 4. `set` (passed to Helm as `--set`, which Helm always applies last).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct ComponentValues {
    /// Free-form Helm values per component, e.g. `values.trino.worker.resources`.
    /// Deep-merged over the golden defaults, so one key can be pinned without
    /// restating the rest.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(schema_with = "component_values_schema")]
    pub values: BTreeMap<String, serde_json::Value>,
    /// `--set` overrides per component: `set.<component>.<helm.key>: "value"`.
    /// Highest precedence — Helm applies `--set` after any values document.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub set: BTreeMap<String, BTreeMap<String, String>>,
    /// External values documents per component, read from a ConfigMap or Secret key.
    #[serde(
        default,
        rename = "valuesFrom",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub values_from: BTreeMap<String, ValuesSource>,
}

/// CRD schema for `values.<component>`: free-form YAML kept verbatim by the API
/// server (`x-kubernetes-preserve-unknown-fields`).
///
/// schemars 1.x represents a schema as raw JSON instead of the old typed tree
/// (`SchemaObject`/`InstanceType`/`ObjectValidation` are gone), so this is written
/// as the object it is: an object whose values are free-form objects.
fn component_values_schema(_: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
    serde_json::json!({
        "type": "object",
        "additionalProperties": {
            "type": "object",
            "x-kubernetes-preserve-unknown-fields": true
        }
    })
    .try_into()
    .expect("the schema literal above is valid JSON Schema")
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
    /// Plan only (no cluster changes) until explicitly disabled. Safe default.
    #[serde(default = "default_true", rename = "dryRun")]
    pub dry_run: bool,
    /// Per-component Helm value overrides for the platform's own components
    /// (today: `cnpg`).
    #[serde(flatten)]
    pub component_values: ComponentValues,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct PlatformInstanceStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub components: Vec<ComponentStatus>,
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
    /// Take over Helm releases this operator did not create. Off by default so a
    /// Lakehouse can never silently clobber an existing install.
    #[serde(default)]
    pub adopt: bool,
    /// Per-component Helm value overrides for the planned components
    /// (`seaweedfs`, `lakekeeper` / `polaris`, `trino`).
    #[serde(flatten)]
    pub component_values: ComponentValues,
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
        assert!(pi.spec.dry_run, "a new PlatformInstance must plan only");
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

    #[test]
    fn component_value_overrides_default_to_empty() {
        let lh: Lakehouse = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt" },
            "spec": {}
        }))
        .expect("deserialize Lakehouse");
        assert!(lh.spec.component_values.values.is_empty());
        assert!(lh.spec.component_values.set.is_empty());
        assert!(lh.spec.component_values.values_from.is_empty());

        let pi: PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {}
        }))
        .expect("deserialize PlatformInstance");
        assert!(pi.spec.component_values.values.is_empty());
        assert!(pi.spec.component_values.set.is_empty());
        assert!(pi.spec.component_values.values_from.is_empty());
    }

    #[test]
    fn lakehouse_parses_per_component_values_set_and_values_from() {
        let lh: Lakehouse = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt" },
            "spec": {
                "values": {
                    "trino": {
                        "coordinator": { "jvm": { "maxHeapSize": "4G" } }
                    }
                },
                "set": { "trino": { "server.workers": "3" } },
                "valuesFrom": {
                    "seaweedfs": {
                        "configMapRef": { "name": "sw-values", "key": "values.yaml" }
                    },
                    "lakekeeper": {
                        "secretRef": { "name": "keeper-values", "key": "values.yaml", "namespace": "stardelt" }
                    }
                }
            }
        }))
        .expect("deserialize Lakehouse");

        let overrides = &lh.spec.component_values;
        assert_eq!(
            overrides.values["trino"]["coordinator"]["jvm"]["maxHeapSize"],
            serde_json::json!("4G")
        );
        assert_eq!(overrides.set["trino"]["server.workers"], "3");

        let sw = &overrides.values_from["seaweedfs"];
        let cm = sw.config_map_ref.as_ref().expect("configMapRef");
        assert_eq!(cm.name, "sw-values");
        assert_eq!(cm.key, "values.yaml");
        assert_eq!(cm.namespace, None);
        assert!(sw.secret_ref.is_none());

        let keeper = &overrides.values_from["lakekeeper"];
        let secret = keeper.secret_ref.as_ref().expect("secretRef");
        assert_eq!(secret.name, "keeper-values");
        assert_eq!(secret.namespace.as_deref(), Some("stardelt"));
        assert!(keeper.config_map_ref.is_none());
    }

    #[test]
    fn platform_instance_parses_component_values() {
        let pi: PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "values": { "cnpg": { "replicaCount": 2 } },
                "set": { "cnpg": { "image.tag": "1.25.0" } },
                "valuesFrom": {
                    "cnpg": { "configMapRef": { "name": "cnpg-values", "key": "values.yaml", "namespace": "stardelt" } }
                }
            }
        }))
        .expect("deserialize PlatformInstance");
        assert_eq!(
            pi.spec.component_values.values["cnpg"]["replicaCount"],
            serde_json::json!(2)
        );
        assert_eq!(pi.spec.component_values.set["cnpg"]["image.tag"], "1.25.0");
        assert!(
            pi.spec.component_values.values_from["cnpg"]
                .config_map_ref
                .is_some()
        );
    }

    #[test]
    fn lakehouse_crd_documents_free_form_component_values() {
        use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::JSONSchemaPropsOrBool;

        let crd = Lakehouse::crd();
        let schema = crd.spec.versions[0]
            .schema
            .as_ref()
            .and_then(|s| s.open_api_v3_schema.as_ref())
            .expect("openAPIV3Schema");
        let spec = schema
            .properties
            .as_ref()
            .and_then(|p| p.get("spec"))
            .expect("spec schema");
        let spec_properties = spec.properties.as_ref().expect("spec properties");

        for field in ["values", "set", "valuesFrom"] {
            assert!(
                spec_properties.contains_key(field),
                "spec.{field} must be part of the CRD schema"
            );
        }

        // `values.<component>` must survive API-server pruning.
        let values = spec_properties.get("values").expect("values schema");
        let per_component = match values
            .additional_properties
            .as_ref()
            .expect("additionalProperties")
        {
            JSONSchemaPropsOrBool::Schema(props) => props,
            JSONSchemaPropsOrBool::Bool(_) => panic!("values.<component> must carry a schema"),
        };
        assert_eq!(
            per_component.x_kubernetes_preserve_unknown_fields,
            Some(true),
            "free-form values must set x-kubernetes-preserve-unknown-fields"
        );
        assert_eq!(
            per_component.type_.as_deref(),
            Some("object"),
            "values.<component> is a values document, i.e. an object"
        );
    }
}

//! stardelt CRDs — the plug specification.
//!
//! These types are the contract between a user's declarative intent and the
//! upstream charts/operators the reconciler applies. See the master spec §5.

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Deployment size / capability profile.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
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

/// Where users sign in (decision D20). Two modes besides none: an identity provider the
/// organization already runs (`external`, for example Entra or Okta), or one the platform runs
/// itself (`platform`, Keycloak). An organization often runs the platform several times against
/// one external provider; each instance then has its own app registrations and group mapping.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IdentityMode {
    /// No sign-in: every component keeps its development default. Only for an unreachable stack.
    #[default]
    None,
    /// The platform installs and configures Keycloak (realm `stardelt`) at `id.<baseDomain>`.
    Platform,
    /// An existing OIDC provider; `issuerUrl` is required.
    External,
}

fn default_group_claim() -> String {
    "groups".into()
}

fn default_admin_group() -> String {
    "stardelt-admins".into()
}

/// Identity settings of a platform instance; the operator publishes them to the components as
/// the ConfigMap `stardelt-identity` (see [`crate::identity::contract`]).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct IdentitySpec {
    #[serde(default)]
    pub mode: IdentityMode,
    /// OIDC issuer URL, `https://` only. Required for `external`; for `platform` it is derived
    /// from `baseDomain` and must be left out.
    #[serde(default, rename = "issuerUrl", skip_serializing_if = "Option::is_none")]
    pub issuer_url: Option<String>,
    /// Name of the token claim that carries group (or role) names.
    #[serde(default = "default_group_claim", rename = "groupClaim")]
    pub group_claim: String,
    /// Members of this group are platform administrators.
    #[serde(default = "default_admin_group", rename = "adminGroup")]
    pub admin_group: String,
}

impl Default for IdentitySpec {
    fn default() -> Self {
        Self {
            mode: IdentityMode::None,
            issuer_url: None,
            group_claim: default_group_claim(),
            admin_group: default_admin_group(),
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
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "!has(self.identity) || !has(self.identity.mode) || self.identity.mode != 'external' || (has(self.identity.issuerUrl) && self.identity.issuerUrl.startsWith('https://'))",
        "message": "identity.mode external needs identity.issuerUrl, an https:// URL"
    },
    {
        "rule": "!has(self.identity) || !has(self.identity.mode) || self.identity.mode != 'platform' || !has(self.identity.issuerUrl)",
        "message": "identity.mode platform derives the issuer from baseDomain; leave identity.issuerUrl out"
    },
    {
        "rule": "!has(self.identity) || !has(self.identity.mode) || self.identity.mode == 'none' || has(self.baseDomain)",
        "message": "identity needs baseDomain (the application hosts and redirect addresses are built from it)"
    }
]))]
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
    /// Namespace the platform's own components are installed into.
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Public DNS name the platform is served under, for example `data.example.com`: Nova answers
    /// on the name itself and every other application on a subdomain (`id`, `trino`, `superset`...).
    #[serde(
        default,
        rename = "baseDomain",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(
        length(min = 3, max = 253),
        regex(pattern = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$")
    )]
    pub base_domain: Option<String>,
    /// How users sign in; see [`IdentitySpec`].
    #[serde(default)]
    pub identity: IdentitySpec,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct PlatformInstanceStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub components: Vec<ComponentStatus>,
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

/// What happens to the Lakehouse's data when the resource is deleted (decision D18).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum DeletionPolicy {
    /// Uninstall the releases; the catalog database, its PVC and the object store survive.
    #[default]
    Retain,
    /// Uninstall the releases and delete the data (catalog PVC, object store PVCs).
    Delete,
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
    /// What happens to the data when this resource is deleted (decision D18). Defaults to Retain.
    #[serde(default, rename = "deletionPolicy")]
    pub deletion_policy: DeletionPolicy,
    /// Per-component Helm value overrides for the planned components
    /// (`seaweedfs`, `lakekeeper` / `polaris`, `trino`).
    #[serde(flatten)]
    pub component_values: ComponentValues,
}

fn default_true() -> bool {
    true
}

fn default_namespace() -> String {
    "stardelt".into()
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct LakehouseStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
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
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
}

/// What a pipeline task does — the task contract behind `Pipeline`
/// (master spec §5, plugs 6, 7 and 12). `sql` is the first kind the platform
/// runs (scheduled jobs phase J0); the others are accepted so manifests can be
/// written ahead of their runtimes.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskKind {
    /// SQL statement, run through the query engine as the run-as principal.
    Sql,
    /// Python transform.
    Python,
    /// dbt-core model run.
    Dbt,
    /// Data-quality gate (Soda + dbt tests).
    Quality,
}

/// One task of a `Pipeline`. A task starts when every task in `dependsOn` has succeeded.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct PipelineTask {
    /// Unique within the pipeline; becomes the task id in the engine.
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    )]
    pub name: String,
    pub kind: TaskKind,
    /// Names of the tasks that must succeed first.
    #[serde(default, rename = "dependsOn", skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = 8))]
    pub depends_on: Vec<String>,
    /// The statement of a `sql` task. `${run.date}`, `${run.id}` and
    /// `${params.<name>}` are expanded when the task runs, not in the resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 100000))]
    pub sql: Option<String>,
    /// Warehouse that runs a `sql` task; absent means the default warehouse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    /// Task-specific parameters of the other kinds (model path, gate threshold, …).
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

/// When a pipeline runs by itself.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct PipelineSchedule {
    /// Five-field cron expression, for example `0 3 * * *`.
    #[schemars(length(min = 9, max = 100))]
    pub cron: String,
    /// IANA time zone name, for example `Europe/Berlin`. Absent means UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// Retry policy applied to every task.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct PipelineRetries {
    /// Extra attempts after the first failure.
    #[schemars(range(min = 0, max = 10))]
    pub attempts: i32,
    /// Wait between attempts, for example `5m`. Absent means one minute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff: Option<String>,
}

/// What happens when a run is due while the previous one is still running.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
pub enum PipelineConcurrency {
    /// Start the new run next to the old one.
    Allow,
    /// Skip the new run.
    #[default]
    Forbid,
    /// Stop the old run and start the new one.
    Replace,
}

/// A pipeline: tasks with dependencies, an optional schedule, run by one engine.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "Pipeline",
    namespaced,
    status = "PipelineStatus",
    shortname = "pipe",
    doc = "A pipeline: tasks with dependencies and an optional schedule, run on one engine."
)]
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "self.tasks.all(t, self.tasks.exists_one(u, u.name == t.name))",
        "message": "task names must be unique"
    },
    {
        "rule": "self.tasks.all(t, !has(t.dependsOn) || t.dependsOn.all(d, d != t.name && self.tasks.exists(u, u.name == d)))",
        "message": "dependsOn must name other tasks of this pipeline"
    },
    {
        "rule": "self.tasks.all(t, t.kind != 'sql' || has(t.sql))",
        "message": "a sql task needs a sql statement"
    }
]))]
pub struct PipelineSpec {
    /// Tasks of the pipeline; at least one, at most 30.
    #[schemars(length(min = 1, max = 30))]
    pub tasks: Vec<PipelineTask>,
    /// Optional schedule; a pipeline without one runs on demand only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<PipelineSchedule>,
    /// Service principal the tasks run as. Absent means the platform's default pipeline principal.
    #[serde(default, rename = "runAs", skip_serializing_if = "Option::is_none")]
    pub run_as: Option<String>,
    /// Values for `${params.<name>}` in task statements.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<PipelineRetries>,
    /// Longest a whole run may take, for example `2h`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default)]
    pub concurrency: PipelineConcurrency,
    #[serde(default)]
    pub engine: PipelineEngine,
}

impl PipelineSpec {
    /// The rules the schema's CEL validations also enforce, plus the one they cannot
    /// express (no dependency cycles), in one place for the controller and the tests.
    pub fn validate(&self) -> Result<(), String> {
        let mut names = std::collections::BTreeSet::new();
        for task in &self.tasks {
            if !names.insert(task.name.as_str()) {
                return Err(format!("task name `{}` is used twice", task.name));
            }
            if task.kind == TaskKind::Sql && task.sql.is_none() {
                return Err(format!("sql task `{}` needs a sql statement", task.name));
            }
        }
        for task in &self.tasks {
            for dep in &task.depends_on {
                if dep == &task.name || !names.contains(dep.as_str()) {
                    return Err(format!(
                        "task `{}` depends on `{dep}`, which is not another task of this pipeline",
                        task.name
                    ));
                }
            }
        }
        // Kahn's algorithm: whatever cannot be ordered lies on or behind a cycle.
        let mut waiting: std::collections::BTreeMap<&str, usize> = self
            .tasks
            .iter()
            .map(|t| (t.name.as_str(), t.depends_on.len()))
            .collect();
        let mut ready: Vec<&str> = waiting
            .iter()
            .filter(|(_, n)| **n == 0)
            .map(|(name, _)| *name)
            .collect();
        let mut done = 0;
        while let Some(name) = ready.pop() {
            done += 1;
            for task in self
                .tasks
                .iter()
                .filter(|t| t.depends_on.iter().any(|d| d == name))
            {
                let n = waiting.get_mut(task.name.as_str()).expect("known task");
                *n -= 1;
                if *n == 0 {
                    ready.push(task.name.as_str());
                }
            }
        }
        if done != self.tasks.len() {
            return Err("the tasks depend on each other in a circle".to_string());
        }
        Ok(())
    }
}

/// Last-known run of a pipeline. Intentionally minimal: the authoritative history
/// belongs to the engine adapter (Airflow / Spark); this is the operator's view.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct PipelineRun {
    pub id: String,
    /// Free-form until the adapters land (`queued` | `running` | `succeeded` | `failed`).
    pub state: String,
    #[serde(default, rename = "startedAt", skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct PipelineStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub runs: Vec<PipelineRun>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ComponentStatus {
    pub name: String,
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Fingerprint (args + values) of the release that was applied last time a
    /// hash was recorded; lets the next reconcile skip an unchanged component.
    #[serde(
        default,
        rename = "appliedHash",
        skip_serializing_if = "Option::is_none"
    )]
    pub applied_hash: Option<String>,
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
    #[serde(
        default,
        rename = "lastTransitionTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_transition_time: Option<String>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
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
    fn lakehouse_deletion_policy_defaults_to_retain() {
        let lh: Lakehouse = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt" },
            "spec": {}
        }))
        .expect("deserialize Lakehouse");
        assert_eq!(
            lh.spec.deletion_policy,
            DeletionPolicy::Retain,
            "a Lakehouse must keep its data unless deletion is opted into (D18)"
        );
    }

    #[test]
    fn lakehouse_parses_deletion_policy_delete() {
        let lh: Lakehouse = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt" },
            "spec": { "deletionPolicy": "Delete" }
        }))
        .expect("deserialize Lakehouse");
        assert_eq!(lh.spec.deletion_policy, DeletionPolicy::Delete);
        assert_eq!(
            serde_json::to_value(lh.spec.deletion_policy).expect("serialize policy"),
            serde_json::json!("Delete"),
            "the YAML value is PascalCase, like PV reclaim policies"
        );
    }

    #[test]
    fn lakehouse_schema_carries_the_deletion_policy_enum() {
        let crd = Lakehouse::crd();
        let schema = serde_json::to_value(
            crd.spec.versions[0]
                .schema
                .as_ref()
                .and_then(|s| s.open_api_v3_schema.clone())
                .expect("Lakehouse CRD carries an openAPIV3Schema"),
        )
        .expect("serialize the Lakehouse schema");
        let mut values: Vec<&str> =
            schema["properties"]["spec"]["properties"]["deletionPolicy"]["enum"]
                .as_array()
                .expect("deletionPolicy is an enum in the schema")
                .iter()
                .map(|value| value.as_str().expect("enum values are strings"))
                .collect();
        values.sort_unstable();
        assert_eq!(values, ["Delete", "Retain"]);
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
    fn platform_instance_namespace_defaults_to_stardelt() {
        let pi: PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {}
        }))
        .expect("deserialize PlatformInstance");
        assert_eq!(pi.spec.namespace, "stardelt");
    }

    #[test]
    fn platform_instance_namespace_is_configurable() {
        let pi: PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": { "namespace": "lab" }
        }))
        .expect("deserialize PlatformInstance");
        assert_eq!(pi.spec.namespace, "lab");
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

    fn task(name: &str, kind: TaskKind, depends_on: &[&str]) -> PipelineTask {
        PipelineTask {
            name: name.into(),
            kind,
            depends_on: depends_on.iter().map(|d| d.to_string()).collect(),
            sql: (kind == TaskKind::Sql).then(|| "SELECT 1".to_string()),
            warehouse: None,
            config: BTreeMap::new(),
        }
    }

    fn spec_of(tasks: Vec<PipelineTask>) -> PipelineSpec {
        PipelineSpec {
            tasks,
            schedule: None,
            run_as: None,
            params: BTreeMap::new(),
            retries: None,
            timeout: None,
            concurrency: PipelineConcurrency::default(),
            engine: PipelineEngine::default(),
        }
    }

    #[test]
    fn pipeline_defaults_to_airflow_unless_stated() {
        let p: Pipeline = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Pipeline",
            "metadata": { "name": "daily", "namespace": "stardelt" },
            "spec": {
                "tasks": [
                    { "name": "load", "kind": "sql", "sql": "SELECT 1" },
                    { "name": "assert", "kind": "quality", "dependsOn": ["load"], "config": { "gate": "warn" } }
                ]
            }
        }))
        .expect("deserialize Pipeline");
        assert_eq!(p.spec.engine, PipelineEngine::Airflow);
        assert_eq!(p.spec.schedule, None);
        assert_eq!(p.spec.concurrency, PipelineConcurrency::Forbid);
        assert_eq!(p.spec.tasks.len(), 2);
        assert_eq!(p.spec.tasks[0].kind, TaskKind::Sql);
        assert!(p.spec.tasks[0].config.is_empty() && p.spec.tasks[0].depends_on.is_empty());
        assert_eq!(p.spec.tasks[1].kind, TaskKind::Quality);
        assert_eq!(p.spec.tasks[1].depends_on, ["load"]);
        assert_eq!(
            p.spec.tasks[1].config.get("gate").map(String::as_str),
            Some("warn")
        );
        assert_eq!(p.spec.validate(), Ok(()));
    }

    #[test]
    fn pipeline_full_spec_round_trips() {
        let spec = serde_json::json!({
            "tasks": [
                { "name": "load", "kind": "sql", "sql": "INSERT INTO t SELECT 1", "warehouse": "default" },
                { "name": "build", "kind": "dbt", "dependsOn": ["load"], "config": { "selector": "tag:nightly" } }
            ],
            "schedule": { "cron": "0 3 * * *", "timezone": "Europe/Berlin" },
            "runAs": "pipeline-analytics",
            "params": { "days": "1" },
            "retries": { "attempts": 2, "backoff": "5m" },
            "timeout": "2h",
            "concurrency": "Replace",
            "engine": "spark"
        });
        let parsed: PipelineSpec =
            serde_json::from_value(spec.clone()).expect("deserialize PipelineSpec");
        assert_eq!(parsed.engine, PipelineEngine::Spark);
        assert_eq!(parsed.concurrency, PipelineConcurrency::Replace);
        assert_eq!(parsed.run_as.as_deref(), Some("pipeline-analytics"));
        assert_eq!(parsed.tasks[1].kind, TaskKind::Dbt);
        assert_eq!(serde_json::to_value(&parsed).expect("serialize"), spec);
    }

    #[test]
    fn validate_accepts_a_diamond_and_rejects_the_broken_shapes() {
        let diamond = spec_of(vec![
            task("a", TaskKind::Sql, &[]),
            task("b", TaskKind::Sql, &["a"]),
            task("c", TaskKind::Sql, &["a"]),
            task("d", TaskKind::Quality, &["b", "c"]),
        ]);
        assert_eq!(diamond.validate(), Ok(()));

        let twice = spec_of(vec![
            task("a", TaskKind::Sql, &[]),
            task("a", TaskKind::Sql, &[]),
        ]);
        assert!(twice.validate().unwrap_err().contains("used twice"));

        let unknown = spec_of(vec![task("a", TaskKind::Sql, &["ghost"])]);
        assert!(unknown.validate().unwrap_err().contains("ghost"));

        let itself = spec_of(vec![task("a", TaskKind::Sql, &["a"])]);
        assert!(itself.validate().is_err());

        let circle = spec_of(vec![
            task("a", TaskKind::Sql, &["c"]),
            task("b", TaskKind::Sql, &["a"]),
            task("c", TaskKind::Sql, &["b"]),
        ]);
        assert!(circle.validate().unwrap_err().contains("circle"));

        let mut no_sql = task("a", TaskKind::Sql, &[]);
        no_sql.sql = None;
        assert!(
            spec_of(vec![no_sql])
                .validate()
                .unwrap_err()
                .contains("needs a sql")
        );
    }

    #[test]
    fn the_generated_schema_carries_the_cel_rules() {
        let crd = serde_json::to_string(&Pipeline::crd()).expect("serialize CRD");
        assert!(crd.contains("x-kubernetes-validations"));
        assert!(crd.contains("task names must be unique"));
        assert!(crd.contains("a sql task needs a sql statement"));
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
            observed_generation: None,
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

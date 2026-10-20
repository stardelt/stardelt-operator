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

fn default_admin_group() -> String {
    "stardelt-admins".into()
}

/// Which kind of provider an `external` instance talks to; it changes claims, audiences and provider names.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IdentityProvider {
    /// Any OIDC provider.
    #[default]
    Oidc,
    /// Microsoft Entra ID: roles as groups, `oid` as the user id, one API audience, v2 tokens.
    Entra,
}

/// A Secret in the platform namespace and the key that holds a client's secret.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct ClientSecretRef {
    #[schemars(
        length(min = 1, max = 253),
        regex(pattern = r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")
    )]
    pub name: String,
    #[serde(default = "default_client_secret_key")]
    #[schemars(length(min = 1, max = 253))]
    pub key: String,
}

fn default_client_secret_key() -> String {
    "client-secret".into()
}

/// One app registration made by the customer in their provider.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct IdentityClient {
    #[serde(rename = "clientId")]
    #[schemars(length(min = 1, max = 256), regex(pattern = r"^\S+$"))]
    pub client_id: String,
    /// Required for every client except the public `lakekeeper` one. Never the value itself.
    #[serde(
        default,
        rename = "clientSecretRef",
        skip_serializing_if = "Option::is_none"
    )]
    pub client_secret_ref: Option<ClientSecretRef>,
}

/// Identity settings of a platform instance; the operator publishes them to the components as
/// the ConfigMap `stardelt-identity` (see [`crate::identity::contract`]).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "self.mode == 'external' || (self.provider == 'oidc' && !has(self.apiAudience) && (!has(self.clients) || size(self.clients) == 0))",
        "message": "platform mode creates its own clients: leave provider, apiAudience and clients out"
    },
    {
        "rule": "self.mode != 'external' || has(self.issuerUrl)",
        "message": "external mode needs issuerUrl"
    },
    {
        "rule": "self.mode != 'external' || has(self.apiAudience)",
        "message": "external mode needs apiAudience"
    },
    {
        "rule": "self.mode != 'external' || (has(self.clients) && 'nova' in self.clients)",
        "message": "external mode needs a nova client"
    },
    {
        "rule": "!has(self.clients) || self.clients.all(k, k in ['nova','trino','lakekeeper','superset','airflow','jupyterhub','trino-group-sync'])",
        "message": "client names are nova, trino, lakekeeper, superset, airflow, jupyterhub and trino-group-sync"
    },
    {
        "rule": "!has(self.clients) || self.clients.all(k, k == 'lakekeeper' || has(self.clients[k].clientSecretRef))",
        "message": "every client except lakekeeper needs a clientSecretRef"
    }
]))]
pub struct IdentitySpec {
    #[serde(default)]
    pub mode: IdentityMode,
    /// OIDC issuer URL, `https://` only. Required for `external`; for `platform` it is derived
    /// from `baseDomain` and must be left out.
    #[serde(default, rename = "issuerUrl", skip_serializing_if = "Option::is_none")]
    pub issuer_url: Option<String>,
    /// `external` only: which kind of provider this instance talks to.
    #[serde(default)]
    pub provider: IdentityProvider,
    /// `external` only: the audience the engine and the catalog accept.
    #[serde(
        default,
        rename = "apiAudience",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(length(min = 1, max = 256), regex(pattern = r"^\S+$"))]
    pub api_audience: Option<String>,
    /// `external` only: the app registrations the customer made, one entry per client.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub clients: BTreeMap<String, IdentityClient>,
    /// Name of the token claim that carries group (or role) names. Defaults to `roles` for
    /// `entra` and `groups` otherwise.
    #[serde(
        default,
        rename = "groupClaim",
        skip_serializing_if = "Option::is_none"
    )]
    pub group_claim: Option<String>,
    /// Members of this group are platform administrators.
    #[serde(default = "default_admin_group", rename = "adminGroup")]
    pub admin_group: String,
}

impl IdentitySpec {
    /// The group claim to read: the configured one, else `roles` for Entra and `groups` otherwise.
    pub fn effective_group_claim(&self) -> &str {
        self.group_claim.as_deref().unwrap_or(match self.provider {
            IdentityProvider::Entra => "roles",
            IdentityProvider::Oidc => "groups",
        })
    }
}

impl Default for IdentitySpec {
    fn default() -> Self {
        Self {
            mode: IdentityMode::None,
            issuer_url: None,
            provider: IdentityProvider::Oidc,
            api_audience: None,
            clients: BTreeMap::new(),
            group_claim: None,
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

/// Whose credential a connection holds and who may use it (decision D27,
/// internal note `design/secrets-and-connections.md` §8). Personal connections have no
/// resource: they live only in the secret store and never reach a pipeline.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionScope {
    /// Managed and used by the members of `ownerGroup`; they may reveal the value in their own code.
    #[default]
    Group,
    /// Written by administrators and readable by nobody. Only code the platform runs (a
    /// connection test, a federated catalog, an ingestion template) uses it, for the groups
    /// in `usableBy`.
    Managed,
}

/// What a connection reaches. The type decides which settings and secret keys are required.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectionType {
    Postgresql,
    Mysql,
    Sqlserver,
    Oracle,
    RestApi,
    S3,
    Kafka,
    Sftp,
    Generic,
}

/// Where the secret part of a connection lives.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct ConnectionSecret {
    /// Path in the secret store. Absent: derived from the scope, `connections/group/<ownerGroup>/<name>`
    /// or `connections/managed/<name>`. Set it only when the platform reads the customer's own store
    /// (`spec.secrets` mode `external`); in platform mode the controller refuses a value, because
    /// a path chosen by the author could point at another group's secret.
    #[serde(default, rename = "remoteRef", skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = 1, max = 512),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9_./-]*$")
    )]
    pub remote_ref: Option<String>,
    /// Names of the secret fields, for example `user` and `password`. They become the keys of the
    /// synced Kubernetes Secret and the environment variables `CONN_<NAME>_<KEY>`.
    #[schemars(length(min = 1, max = 16), inner(length(min = 1, max = 63)))]
    pub keys: Vec<String>,
}

/// A typed connection to a system outside the platform: non-secret settings here, the secret
/// fields in the secret store (decision D23). No value of a secret is ever part of this resource.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "Connection",
    namespaced,
    status = "ConnectionStatus",
    shortname = "conn",
    doc = "A typed connection to an outside system: settings here, the credentials in the secret store."
)]
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "self.scope != 'group' || (has(self.ownerGroup) && !has(self.usableBy))",
        "message": "a group connection needs ownerGroup and no usableBy"
    },
    {
        "rule": "self.scope != 'managed' || (has(self.usableBy) && !has(self.ownerGroup))",
        "message": "a managed connection needs usableBy and no ownerGroup"
    },
    {
        "rule": "!has(self.usableBy) || self.usableBy.all(g, g.matches('^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'))",
        "message": "usableBy must list group names (letters, digits, dot, underscore, dash)"
    },
    {
        "rule": "!has(self.secret.remoteRef) || (!self.secret.remoteRef.contains('..') && !self.secret.remoteRef.contains('//'))",
        "message": "secret.remoteRef must not contain .. or //"
    },
    {
        "rule": "self.secret.keys.all(k, k.matches('^[A-Za-z][A-Za-z0-9_]{0,62}$') && self.secret.keys.exists_one(j, j == k))",
        "message": "secret.keys must be unique names of letters, digits and underscore"
    },
    {
        "rule": "!has(self.config) || self.config.all(k, k.matches('^[A-Za-z][A-Za-z0-9_.-]{0,62}$') && !k.lowerAscii().matches('(password|passwd|passphrase|secret|token|credential)'))",
        "message": "config holds settings, not secrets: a name like password, secret or token belongs in secret.keys"
    },
    {
        "rule": "!(self.type in ['postgresql', 'mysql', 'sqlserver', 'oracle']) || (has(self.config) && 'host' in self.config && 'user' in self.secret.keys && 'password' in self.secret.keys)",
        "message": "a database connection needs config.host and the secret keys user and password"
    },
    {
        "rule": "self.type != 'rest-api' || (has(self.config) && 'baseUrl' in self.config && ('token' in self.secret.keys || ('clientId' in self.secret.keys && 'clientSecret' in self.secret.keys)))",
        "message": "a rest-api connection needs config.baseUrl and the secret key token, or clientId and clientSecret"
    },
    {
        "rule": "self.type != 's3' || (has(self.config) && 'bucket' in self.config && 'accessKeyId' in self.secret.keys && 'secretAccessKey' in self.secret.keys)",
        "message": "an s3 connection needs config.bucket and the secret keys accessKeyId and secretAccessKey"
    },
    {
        "rule": "self.type != 'sftp' || (has(self.config) && 'host' in self.config && 'user' in self.secret.keys && ('password' in self.secret.keys || 'privateKey' in self.secret.keys))",
        "message": "an sftp connection needs config.host and the secret keys user and password or privateKey"
    },
    {
        "rule": "self.type != 'kafka' || (has(self.config) && 'bootstrapServers' in self.config)",
        "message": "a kafka connection needs config.bootstrapServers"
    }
]))]
pub struct ConnectionSpec {
    #[serde(default)]
    pub scope: ConnectionScope,
    /// Scope `group`: the Keycloak group whose members manage and use the connection.
    #[serde(
        default,
        rename = "ownerGroup",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    pub owner_group: Option<String>,
    /// Scope `managed`: the groups whose platform-run work may use the connection.
    #[serde(default, rename = "usableBy", skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = 20), inner(length(min = 1, max = 64)))]
    pub usable_by: Vec<String>,
    #[serde(rename = "type")]
    pub type_: ConnectionType,
    /// Non-secret settings, for example `host`, `port`, `database`, `bucket`, `baseUrl`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(extend("maxProperties" = 32))]
    pub config: BTreeMap<String, String>,
    pub secret: ConnectionSecret,
    /// How often the synced Secret is refreshed from the store, for example `15m`. Between one minute and 24 hours.
    #[serde(default = "default_refresh_interval", rename = "refreshInterval")]
    #[schemars(regex(pattern = r"^[0-9]{1,4}(m|h)$"))]
    pub refresh_interval: String,
}

fn default_refresh_interval() -> String {
    "15m".to_string()
}

impl ConnectionSpec {
    /// The rules the schema's CEL validations also enforce, plus those a schema cannot express
    /// (the resource name, the refresh range), in one place for the controller and the tests.
    pub fn validate(&self, name: &str) -> Result<(), String> {
        let bytes = name.as_bytes();
        let name_ok = !name.is_empty()
            && name.len() <= 40
            && bytes[0].is_ascii_lowercase()
            && bytes[bytes.len() - 1] != b'-'
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
        if !name_ok {
            return Err(format!(
                "connection name `{name}` must be 1 to 40 lower-case letters, digits and dashes, starting with a letter"
            ));
        }
        let group_ok = |g: &str| {
            !g.is_empty()
                && g.len() <= 64
                && g.as_bytes()[0].is_ascii_alphanumeric()
                && g.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        };
        match self.scope {
            ConnectionScope::Group => match self.owner_group.as_deref() {
                Some(g) if group_ok(g) && self.usable_by.is_empty() => {}
                _ => {
                    return Err(
                        "a group connection needs a valid ownerGroup and no usableBy".to_string(),
                    );
                }
            },
            ConnectionScope::Managed => {
                if self.owner_group.is_some()
                    || self.usable_by.is_empty()
                    || !self.usable_by.iter().all(|g| group_ok(g))
                {
                    return Err(
                        "a managed connection needs usableBy (valid group names) and no ownerGroup"
                            .to_string(),
                    );
                }
            }
        }
        if let Some(path) = &self.secret.remote_ref
            && (path.contains("..") || path.contains("//"))
        {
            return Err("secret.remoteRef must not contain .. or //".to_string());
        }
        let keys = &self.secret.keys;
        let key_ok = |k: &str| {
            !k.is_empty()
                && k.len() <= 63
                && k.as_bytes()[0].is_ascii_alphabetic()
                && k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        };
        let unique: std::collections::BTreeSet<&str> = keys.iter().map(String::as_str).collect();
        if keys.is_empty()
            || keys.len() > 16
            || unique.len() != keys.len()
            || !keys.iter().all(|k| key_ok(k))
        {
            return Err(
                "secret.keys must be 1 to 16 unique names of letters, digits and underscore"
                    .to_string(),
            );
        }
        for key in self.config.keys() {
            let lower = key.to_ascii_lowercase();
            if [
                "password",
                "passwd",
                "passphrase",
                "secret",
                "token",
                "credential",
            ]
            .iter()
            .any(|part| lower.contains(part))
            {
                return Err(format!(
                    "config.{key} looks like a secret: settings go in config, secrets in secret.keys"
                ));
            }
        }
        let has_config = |k: &str| self.config.contains_key(k);
        let has_key = |k: &str| keys.iter().any(|x| x == k);
        let needs = |ok: bool, what: &str| if ok { Ok(()) } else { Err(what.to_string()) };
        match self.type_ {
            ConnectionType::Postgresql
            | ConnectionType::Mysql
            | ConnectionType::Sqlserver
            | ConnectionType::Oracle => needs(
                has_config("host") && has_key("user") && has_key("password"),
                "a database connection needs config.host and the secret keys user and password",
            )?,
            ConnectionType::RestApi => needs(
                has_config("baseUrl")
                    && (has_key("token") || (has_key("clientId") && has_key("clientSecret"))),
                "a rest-api connection needs config.baseUrl and the secret key token, or clientId and clientSecret",
            )?,
            ConnectionType::S3 => needs(
                has_config("bucket") && has_key("accessKeyId") && has_key("secretAccessKey"),
                "an s3 connection needs config.bucket and the secret keys accessKeyId and secretAccessKey",
            )?,
            ConnectionType::Sftp => needs(
                has_config("host")
                    && has_key("user")
                    && (has_key("password") || has_key("privateKey")),
                "an sftp connection needs config.host and the secret keys user and password or privateKey",
            )?,
            ConnectionType::Kafka => needs(
                has_config("bootstrapServers"),
                "a kafka connection needs config.bootstrapServers",
            )?,
            ConnectionType::Generic => {}
        }
        let digits: String = self
            .refresh_interval
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let minutes = match (digits.parse::<u32>(), self.refresh_interval.chars().last()) {
            (Ok(n), Some('m')) => n,
            (Ok(n), Some('h')) => n.saturating_mul(60),
            _ => 0,
        };
        if !(1..=1440).contains(&minutes) {
            return Err("refreshInterval must be between 1m and 24h, for example 15m".to_string());
        }
        Ok(())
    }

    /// The path in the secret store this connection reads, when the platform runs the store:
    /// `connections/group/<ownerGroup>/<name>` or `connections/managed/<name>`.
    pub fn derived_remote_ref(&self, name: &str) -> String {
        match (self.scope, self.owner_group.as_deref()) {
            (ConnectionScope::Group, Some(group)) => format!("connections/group/{group}/{name}"),
            _ => format!("connections/managed/{name}"),
        }
    }
}

/// The Kubernetes Secret a connection is synced into.
pub fn connection_secret_name(name: &str) -> String {
    format!("conn-{name}")
}

/// Prefix of the environment variables a task gets from the Secret: `crm-db` becomes `CONN_CRM_DB_`.
pub fn connection_env_prefix(name: &str) -> String {
    format!("CONN_{}_", name.to_ascii_uppercase().replace('-', "_"))
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct ConnectionStatus {
    /// True when the Secret exists and holds every key in `spec.secret.keys`.
    #[serde(default)]
    pub ready: bool,
    #[serde(
        default,
        rename = "secretName",
        skip_serializing_if = "Option::is_none"
    )]
    pub secret_name: Option<String>,
    /// RFC 3339 time of the last successful sync from the store.
    #[serde(default, rename = "lastSync", skip_serializing_if = "Option::is_none")]
    pub last_sync: Option<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
}

/// What a volume holds. A `libraries` volume is code that every reader executes, so it
/// restricts `write` to groups that also manage it (decision D28, internal note
/// `design/volumes.md` §5).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum VolumePurpose {
    /// Files of any kind: drops, exports, model artifacts, notebook output.
    #[default]
    Data,
    /// Wheels and requirement files, installed into readers' environments.
    Libraries,
}

/// What a grant lets a group do: `read` a file or list a folder, `write` upload, change or
/// delete one, `manage` change the grants and delete the volume.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "lowercase")]
pub enum VolumeVerb {
    Read,
    Write,
    Manage,
}

/// One group's rights on a volume. Subjects are groups, never users.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct VolumeGrant {
    /// The Keycloak group whose members hold these verbs.
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    pub group: String,
    /// At least one verb; each verb at most once.
    #[schemars(length(min = 1, max = 3))]
    pub verbs: Vec<VolumeVerb>,
}

/// Storage the platform owns: the volume gets `volumes/<catalog>/<name>/` in the platform
/// object store. `managed: {}` selects it.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
pub struct ManagedStorage {}

/// A prefix in the customer's own bucket, reached through an `s3` connection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct ExternalStorage {
    /// Name of the `s3` connection that holds the endpoint, bucket and credentials.
    #[schemars(
        length(min = 1, max = 40),
        regex(pattern = r"^[a-z]([-a-z0-9]*[a-z0-9])?$")
    )]
    pub connection: String,
    /// Key prefix inside the bucket; absent means the prefix the connection already scopes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 512), regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9_./-]*$"))]
    pub prefix: Option<String>,
}

/// Where a volume's files live: exactly one of platform-managed storage or an external
/// prefix. Files live in the volume bucket, never in the table bucket (D28).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct VolumeStorage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed: Option<ManagedStorage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external: Option<ExternalStorage>,
}

/// A governed folder of files in the object store with an owner group and grants.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "Volume",
    namespaced,
    status = "VolumeStatus",
    shortname = "vol",
    doc = "A governed folder of files in the object store with an owner group and grants."
)]
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "has(self.storage.managed) != has(self.storage.external)",
        "message": "storage must be either managed or external, not both and not neither"
    },
    {
        "rule": "!has(self.storage.external) || !has(self.storage.external.prefix) || (!self.storage.external.prefix.contains('..') && !self.storage.external.prefix.contains('//'))",
        "message": "an external prefix must not contain .. or //"
    },
    {
        "rule": "self.grants.all(g, self.grants.exists_one(h, h.group == g.group))",
        "message": "each group may appear once in grants"
    },
    {
        "rule": "self.grants.all(g, g.verbs.all(v, g.verbs.exists_one(w, w == v)))",
        "message": "a verb may appear once per grant"
    },
    {
        "rule": "self.purpose != 'libraries' || self.grants.all(g, !('write' in g.verbs) || ('manage' in g.verbs))",
        "message": "on a libraries volume only a group that also manages it may write"
    }
]))]
pub struct VolumeSpec {
    /// The catalog (Lakehouse) this volume belongs to; the owner group is inherited.
    #[schemars(
        length(min = 1, max = 40),
        regex(pattern = r"^[a-z]([-a-z0-9]*[a-z0-9])?$")
    )]
    pub catalog: String,
    #[serde(default)]
    pub purpose: VolumePurpose,
    pub storage: VolumeStorage,
    #[serde(default)]
    #[schemars(length(max = 20))]
    pub grants: Vec<VolumeGrant>,
}

impl VolumeSpec {
    /// The rules the schema's CEL validations also enforce, plus the resource name, in one
    /// place for the controller and the tests.
    pub fn validate(&self, name: &str) -> Result<(), String> {
        let bytes = name.as_bytes();
        let name_ok = !name.is_empty()
            && name.len() <= 40
            && bytes[0].is_ascii_lowercase()
            && bytes[bytes.len() - 1] != b'-'
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
        if !name_ok {
            return Err(format!(
                "volume name `{name}` must be 1 to 40 lower-case letters, digits and dashes, starting with a letter"
            ));
        }
        if self.storage.managed.is_some() == self.storage.external.is_some() {
            return Err(
                "storage must be either managed or external, not both and not neither".to_string(),
            );
        }
        if let Some(external) = &self.storage.external
            && let Some(prefix) = &external.prefix
            && (prefix.contains("..") || prefix.contains("//"))
        {
            return Err("an external prefix must not contain .. or //".to_string());
        }
        let groups: std::collections::BTreeSet<&str> =
            self.grants.iter().map(|g| g.group.as_str()).collect();
        if groups.len() != self.grants.len() {
            return Err("each group may appear once in grants".to_string());
        }
        for grant in &self.grants {
            let verbs: std::collections::BTreeSet<&VolumeVerb> = grant.verbs.iter().collect();
            if verbs.len() != grant.verbs.len() {
                return Err("a verb may appear once per grant".to_string());
            }
        }
        if self.purpose == VolumePurpose::Libraries
            && !self.grants.iter().all(|g| {
                !g.verbs.contains(&VolumeVerb::Write) || g.verbs.contains(&VolumeVerb::Manage)
            })
        {
            return Err(
                "on a libraries volume only a group that also manages it may write".to_string(),
            );
        }
        Ok(())
    }

    /// `volumes/<catalog>/<name>/` for managed storage, or the external prefix (an empty
    /// string when the connection already scopes one).
    pub fn derived_prefix(&self, name: &str) -> String {
        if self.storage.managed.is_some() {
            format!("volumes/{}/{name}/", self.catalog)
        } else {
            self.storage
                .external
                .as_ref()
                .and_then(|external| external.prefix.clone())
                .unwrap_or_default()
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct VolumeStatus {
    /// True when the prefix exists and its grants apply; filled by a later controller.
    #[serde(default)]
    pub ready: bool,
    /// The object-store prefix the volume maps to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, rename = "sizeBytes", skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objects: Option<i64>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
}

/// What a service principal is for. It decides nothing by itself (grants do); it makes the intent
/// visible in `kubectl get` and in Nova.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PrincipalPurpose {
    /// Runs the tasks of scheduled pipelines (`Pipeline.spec.runAs`).
    #[default]
    Pipeline,
    /// Runs table maintenance (compaction, snapshot expiry).
    Maintenance,
    /// Delivers events or metrics into the platform (for example the query-log collector).
    Collector,
}

/// What a principal's tokens are valid for (the `aud` claim of its access token).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PrincipalAudience {
    /// The query engine.
    Trino,
    /// The catalog.
    Lakekeeper,
}

/// A secret that already holds the credentials of an app registration made by the identity
/// provider's administrator (`identity.mode: external`). The Secret must be in the same namespace
/// and have the keys `client-id` and `client-secret`.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct ExistingSecretRef {
    #[schemars(
        length(min = 1, max = 253),
        regex(pattern = r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")
    )]
    pub name: String,
}

/// A non-human identity for jobs, maintenance and collectors (decisions D20 and D21, internal note
/// `design/scheduled-jobs.md`). With the platform identity provider the operator creates a client
/// with a service account and the audiences below and writes its credentials to the Secret
/// `sp-<name>`; with an external provider it only checks the Secret named in `existingSecretRef`.
/// The owner group's members may name the principal as a pipeline's `runAs`; nobody may read the Secret.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "ServicePrincipal",
    namespaced,
    status = "ServicePrincipalStatus",
    shortname = "sp",
    doc = "A non-human identity (client credentials) for pipelines, maintenance and collectors."
)]
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "self.audiences.all(a, self.audiences.exists_one(b, b == a))",
        "message": "each audience may appear once"
    }
]))]
pub struct ServicePrincipalSpec {
    /// The group whose members may use the principal. Members never read its credentials.
    #[serde(rename = "ownerGroup")]
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    pub owner_group: String,
    #[serde(default)]
    pub purpose: PrincipalPurpose,
    /// What the principal's tokens are valid for; at least one, `trino` by default.
    #[serde(default = "default_principal_audiences")]
    #[schemars(length(min = 1, max = 2))]
    pub audiences: Vec<PrincipalAudience>,
    /// Only with an external identity provider: the Secret that holds the app registration's credentials.
    #[serde(
        default,
        rename = "existingSecretRef",
        skip_serializing_if = "Option::is_none"
    )]
    pub existing_secret_ref: Option<ExistingSecretRef>,
}

fn default_principal_audiences() -> Vec<PrincipalAudience> {
    vec![PrincipalAudience::Trino]
}

impl ServicePrincipalSpec {
    /// The rules the schema enforces, repeated for the controller and the tests, plus the resource name.
    pub fn validate(&self, name: &str) -> Result<(), String> {
        let bytes = name.as_bytes();
        let name_ok = !name.is_empty()
            && name.len() <= 40
            && bytes[0].is_ascii_lowercase()
            && bytes[bytes.len() - 1] != b'-'
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
        if !name_ok {
            return Err(format!(
                "service principal name `{name}` must be 1 to 40 lower-case letters, digits and dashes, starting with a letter"
            ));
        }
        let group = self.owner_group.as_bytes();
        if group.is_empty()
            || group.len() > 64
            || !group[0].is_ascii_alphanumeric()
            || !group
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Err("ownerGroup must be a valid group name".to_string());
        }
        let unique: std::collections::BTreeSet<_> =
            self.audiences.iter().map(|a| format!("{a:?}")).collect();
        if self.audiences.is_empty() || unique.len() != self.audiences.len() {
            return Err("audiences must list each audience once, at least one".to_string());
        }
        Ok(())
    }
}

/// The client id of the principal `name` in the identity provider (platform mode) and the name of
/// the Secret that holds its credentials: `client-id` and `client-secret`.
pub fn principal_client_id(name: &str) -> String {
    format!("sp-{name}")
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct ServicePrincipalStatus {
    /// True when the client exists (or the referenced Secret is valid) and the Secret is written.
    #[serde(default)]
    pub ready: bool,
    #[serde(default, rename = "clientId", skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(
        default,
        rename = "secretName",
        skip_serializing_if = "Option::is_none"
    )]
    pub secret_name: Option<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
}

/// What a grant lets a group do with a catalog (decision D21, internal note
/// `design/multitenant-catalogs.md` §5.2). Six verbs exist; `use` belongs to compute (`SqlWarehouse`)
/// and is not a data verb. There is no deny, no `ALL PRIVILEGES` and no per-column verb.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "lowercase")]
pub enum DataVerb {
    /// Metadata and existence only; no rows.
    Browse,
    /// Read data.
    Select,
    /// Insert, update, delete, merge, alter.
    Write,
    /// Create schemas and tables inside the catalog.
    Create,
    /// Grant, revoke, transfer ownership, drop and rename the catalog.
    Manage,
}

/// One group, one verb. Subjects are groups, never people.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct CatalogGrant {
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    pub group: String,
    pub verb: DataVerb,
}

/// The `Lakehouse` that hosts a catalog or a warehouse.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct LakehouseRef {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
    )]
    pub name: String,
}

/// A reference to one key of a Secret in the same namespace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct SecretKeySelector {
    #[schemars(
        length(min = 1, max = 253),
        regex(pattern = r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")
    )]
    pub name: String,
    #[schemars(length(min = 1, max = 253), regex(pattern = r"^[-._a-zA-Z0-9]+$"))]
    pub key: String,
}

/// Where a catalog's data files live. Omitted: the `Lakehouse`'s storage defaults.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
pub struct CatalogStorage {
    /// An isolated bucket per catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = 3, max = 63),
        regex(pattern = r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$")
    )]
    pub bucket: Option<String>,
    /// Required where all catalogs share one bucket (the demo profile); no leading slash, no `..`.
    #[serde(default, rename = "keyPrefix", skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 512), regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9_./-]*$"))]
    pub key_prefix: Option<String>,
    /// The per-catalog storage identity. A reference only: the Secret is never part of this resource.
    #[serde(
        default,
        rename = "credentialSecretRef",
        skip_serializing_if = "Option::is_none"
    )]
    pub credential_secret_ref: Option<SecretKeySelector>,
}

/// One data domain inside the catalog software of a `Lakehouse`: its storage, its owner and its
/// grants. `metadata.name` is the name users type (`finance.orders`) and the name of the
/// catalog's warehouse; there is no alias.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "Catalog",
    namespaced,
    status = "CatalogDomainStatus",
    shortname = "cat",
    doc = "A data domain: a catalog with its storage location, an owner group and grants."
)]
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "self.grants.all(g, self.grants.exists_one(h, h.group == g.group && h.verb == g.verb))",
        "message": "each group may hold each verb once"
    },
    {
        "rule": "!has(self.storage) || !has(self.storage.keyPrefix) || (!self.storage.keyPrefix.contains('..') && !self.storage.keyPrefix.contains('//'))",
        "message": "storage.keyPrefix must not contain .. or //"
    }
]))]
pub struct CatalogDomainSpec {
    #[serde(rename = "lakehouseRef")]
    pub lakehouse_ref: LakehouseRef,
    #[serde(
        default,
        rename = "displayName",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(length(max = 100))]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub description: Option<String>,
    /// Exactly one group. Ownership means `manage` and does not mean `select`.
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<CatalogStorage>,
    #[serde(default)]
    #[schemars(length(max = 50))]
    pub grants: Vec<CatalogGrant>,
    /// True: the owner cannot grant on this catalog; only platform administrators can.
    #[serde(default, rename = "managedAccess")]
    pub managed_access: bool,
}

/// Trino catalog names that are taken by the engine or by the platform.
const RESERVED_CATALOG_NAMES: [&str; 6] = [
    "system",
    "information_schema",
    "memory",
    "tpch",
    "tpcds",
    "jmx",
];

impl CatalogDomainSpec {
    /// The rules the schema enforces, repeated for the controller and the tests, plus the name:
    /// it becomes a Trino catalog and a Lakekeeper warehouse, so it is an unquoted SQL identifier
    /// (lower-case letters and digits, starting with a letter, 1 to 40) and not a reserved name.
    pub fn validate(&self, name: &str) -> Result<(), String> {
        let bytes = name.as_bytes();
        let name_ok = !name.is_empty()
            && name.len() <= 40
            && bytes[0].is_ascii_lowercase()
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
        if !name_ok {
            return Err(format!(
                "catalog name `{name}` must be 1 to 40 lower-case letters and digits, starting with a letter (it is a SQL identifier)"
            ));
        }
        if RESERVED_CATALOG_NAMES.contains(&name) {
            return Err(format!("catalog name `{name}` is reserved"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for grant in &self.grants {
            if !seen.insert((grant.group.as_str(), grant.verb)) {
                return Err(format!(
                    "group `{}` holds `{:?}` twice",
                    grant.group, grant.verb
                ));
            }
        }
        if let Some(prefix) = self.storage.as_ref().and_then(|s| s.key_prefix.as_deref())
            && (prefix.contains("..") || prefix.contains("//"))
        {
            return Err("storage.keyPrefix must not contain .. or //".to_string());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct CatalogDomainStatus {
    #[serde(default)]
    pub phase: String,
    /// The Lakekeeper warehouse id, so Nova and the access bridge never guess it by name.
    #[serde(
        default,
        rename = "warehouseId",
        skip_serializing_if = "Option::is_none"
    )]
    pub warehouse_id: Option<String>,
    /// The Trino catalog name actually rendered.
    #[serde(
        default,
        rename = "catalogName",
        skip_serializing_if = "Option::is_none"
    )]
    pub catalog_name: Option<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
}

/// The platform's compute classes. A class is a name for four numbers (running, queued, memory
/// share, scheduling), not a promise about hardware: the query engine has resource groups, not
/// separate clusters (internal note `design/multitenant-catalogs.md` §4.3).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WarehouseSize {
    Xs,
    #[default]
    Small,
    Medium,
    Large,
}

/// How queued queries are chosen to run.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WarehouseScheduling {
    Fifo,
    Fair,
    Weighted,
    WeightedFair,
}

/// Overrides of the class defaults. An absent field keeps the class value.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Default)]
pub struct WarehouseConcurrency {
    /// Queries that run at the same time (the resource group's hard concurrency limit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 100))]
    pub running: Option<i32>,
    /// Queries that may wait (the resource group's queue limit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0, max = 10000))]
    pub queued: Option<i32>,
    /// Share of the engine's memory the class may use, for example `40%`.
    #[serde(
        default,
        rename = "memoryShare",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(regex(pattern = r"^([1-9][0-9]?|100)%$"))]
    pub memory_share: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<WarehouseScheduling>,
}

/// A group that may send queries into a warehouse (the compute verb `use`).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub struct WarehouseGrant {
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    pub group: String,
}

/// The query-serving plug: one named compute class inside the engine of a `Lakehouse`, with its size,
/// its caps and the groups that may use it (decision D21, internal note `design/multitenant-catalogs.md`
/// §4.3). Exactly one warehouse per `Lakehouse` is the default.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "SqlWarehouse",
    namespaced,
    status = "SqlWarehouseStatus",
    shortname = "wh",
    doc = "A compute class for SQL: size, caps and the groups that may use it."
)]
#[schemars(extend("x-kubernetes-validations" = [
    {
        "rule": "self.grants.all(g, self.grants.exists_one(h, h.group == g.group))",
        "message": "each group may appear once in grants"
    }
]))]
pub struct SqlWarehouseSpec {
    #[serde(rename = "lakehouseRef")]
    pub lakehouse_ref: LakehouseRef,
    #[serde(
        default,
        rename = "displayName",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(length(max = 100))]
    pub display_name: Option<String>,
    #[serde(default)]
    pub size: WarehouseSize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<WarehouseConcurrency>,
    #[serde(default)]
    #[schemars(length(max = 50))]
    pub grants: Vec<WarehouseGrant>,
    /// The class that queries land in when nothing selects another one.
    #[serde(default)]
    pub default: bool,
}

impl SqlWarehouseSpec {
    /// The rules the schema enforces, repeated for the controller and the tests, plus the name: it
    /// becomes the resource group (1 to 40 lower-case letters, digits and dashes, starting with a letter).
    pub fn validate(&self, name: &str) -> Result<(), String> {
        let bytes = name.as_bytes();
        let name_ok = !name.is_empty()
            && name.len() <= 40
            && bytes[0].is_ascii_lowercase()
            && bytes[bytes.len() - 1] != b'-'
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
        if !name_ok {
            return Err(format!(
                "warehouse name `{name}` must be 1 to 40 lower-case letters, digits and dashes, starting with a letter"
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for grant in &self.grants {
            if !seen.insert(grant.group.as_str()) {
                return Err(format!("group `{}` appears twice in grants", grant.group));
            }
        }
        if let Some(c) = &self.concurrency {
            if let Some(n) = c.running
                && !(1..=100).contains(&n)
            {
                return Err("concurrency.running must be between 1 and 100".to_string());
            }
            if let Some(n) = c.queued
                && !(0..=10000).contains(&n)
            {
                return Err("concurrency.queued must be between 0 and 10000".to_string());
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct SqlWarehouseStatus {
    #[serde(default)]
    pub phase: String,
    /// The resource group actually written.
    #[serde(
        default,
        rename = "renderedGroup",
        skip_serializing_if = "Option::is_none"
    )]
    pub rendered_group: Option<String>,
    /// Which engine implements the class, for example `trino@default`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(
        default,
        rename = "observedGeneration",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_generation: Option<i64>,
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

    fn connection(spec: serde_json::Value) -> Connection {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Connection",
            "metadata": { "name": "crm-db", "namespace": "stardelt" },
            "spec": spec
        }))
        .expect("deserialize Connection")
    }

    fn crm(patch: serde_json::Value) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "scope": "group",
            "ownerGroup": "analytics",
            "type": "sqlserver",
            "config": { "host": "crm.example.com", "port": "1433", "database": "sales" },
            "secret": { "keys": ["user", "password"] }
        });
        for (k, v) in patch.as_object().unwrap() {
            if v.is_null() {
                spec.as_object_mut().unwrap().remove(k);
            } else {
                spec[k] = v.clone();
            }
        }
        spec
    }

    #[test]
    fn a_group_connection_parses_and_derives_its_store_path_and_names() {
        let c = connection(crm(serde_json::json!({})));
        assert_eq!(c.spec.scope, ConnectionScope::Group);
        assert_eq!(c.spec.type_, ConnectionType::Sqlserver);
        assert_eq!(c.spec.refresh_interval, "15m");
        assert!(c.spec.secret.remote_ref.is_none());
        assert!(c.spec.validate("crm-db").is_ok());
        assert_eq!(
            c.spec.derived_remote_ref("crm-db"),
            "connections/group/analytics/crm-db"
        );
        assert_eq!(connection_secret_name("crm-db"), "conn-crm-db");
        assert_eq!(connection_env_prefix("crm-db"), "CONN_CRM_DB_");
    }

    #[test]
    fn a_managed_connection_names_the_groups_that_may_use_it() {
        let c = connection(crm(serde_json::json!({
            "scope": "managed", "ownerGroup": null, "usableBy": ["analytics", "finance"]
        })));
        assert!(c.spec.validate("crm-db").is_ok());
        assert_eq!(
            c.spec.derived_remote_ref("crm-db"),
            "connections/managed/crm-db"
        );
    }

    #[test]
    fn the_scope_rules_are_enforced() {
        let bad =
            |patch: serde_json::Value| connection(crm(patch)).spec.validate("crm-db").unwrap_err();
        assert!(bad(serde_json::json!({ "ownerGroup": null })).contains("ownerGroup"));
        assert!(bad(serde_json::json!({ "usableBy": ["finance"] })).contains("usableBy"));
        assert!(bad(serde_json::json!({ "ownerGroup": "../finance" })).contains("ownerGroup"));
        assert!(bad(serde_json::json!({ "ownerGroup": "a/b" })).contains("ownerGroup"));
        assert!(
            bad(serde_json::json!({ "scope": "managed", "ownerGroup": null })).contains("usableBy")
        );
        assert!(
            bad(serde_json::json!({ "scope": "managed", "usableBy": ["x/y"], "ownerGroup": null }))
                .contains("usableBy")
        );
    }

    #[test]
    fn each_type_demands_its_settings_and_secret_keys() {
        let bad =
            |patch: serde_json::Value| connection(crm(patch)).spec.validate("crm-db").unwrap_err();
        assert!(bad(serde_json::json!({ "config": { "port": "1433" } })).contains("config.host"));
        assert!(
            bad(serde_json::json!({ "secret": { "keys": ["user"] } }))
                .contains("user and password")
        );
        assert!(bad(serde_json::json!({ "type": "rest-api", "config": { "baseUrl": "https://x" }, "secret": { "keys": ["clientId"] } })).contains("rest-api"));
        assert!(bad(serde_json::json!({ "type": "s3", "config": { "bucket": "b" }, "secret": { "keys": ["accessKeyId"] } })).contains("s3"));
        assert!(bad(serde_json::json!({ "type": "sftp", "config": { "host": "h" }, "secret": { "keys": ["user"] } })).contains("sftp"));
        assert!(
            bad(serde_json::json!({ "type": "kafka", "config": {}, "secret": { "keys": ["x"] } }))
                .contains("kafka")
        );
        for ok in [
            serde_json::json!({ "type": "rest-api", "config": { "baseUrl": "https://x" }, "secret": { "keys": ["token"] } }),
            serde_json::json!({ "type": "rest-api", "config": { "baseUrl": "https://x" }, "secret": { "keys": ["clientId", "clientSecret"] } }),
            serde_json::json!({ "type": "s3", "config": { "bucket": "b" }, "secret": { "keys": ["accessKeyId", "secretAccessKey"] } }),
            serde_json::json!({ "type": "sftp", "config": { "host": "h" }, "secret": { "keys": ["user", "privateKey"] } }),
            serde_json::json!({ "type": "generic", "config": {}, "secret": { "keys": ["anything"] } }),
        ] {
            assert!(connection(crm(ok)).spec.validate("c").is_ok());
        }
    }

    #[test]
    fn secrets_do_not_hide_in_config_and_keys_stay_clean() {
        let bad =
            |patch: serde_json::Value| connection(crm(patch)).spec.validate("crm-db").unwrap_err();
        assert!(
            bad(serde_json::json!({ "config": { "host": "h", "dbPassword": "x" } }))
                .contains("looks like a secret")
        );
        assert!(
            bad(serde_json::json!({ "config": { "host": "h", "API_TOKEN": "x" } }))
                .contains("looks like a secret")
        );
        assert!(
            bad(serde_json::json!({ "secret": { "keys": ["user", "password", "user"] } }))
                .contains("unique")
        );
        assert!(
            bad(serde_json::json!({ "secret": { "keys": ["user", "pass word"] } }))
                .contains("secret.keys")
        );
    }

    #[test]
    fn a_store_path_cannot_climb_out_of_its_prefix() {
        let ok = connection(crm(
            serde_json::json!({ "secret": { "keys": ["user", "password"], "remoteRef": "kv/team/crm" } }),
        ));
        assert!(ok.spec.validate("crm-db").is_ok());
        for path in ["connections/group/finance/../analytics/x", "a//b"] {
            let bad = connection(crm(
                serde_json::json!({ "secret": { "keys": ["user", "password"], "remoteRef": path } }),
            ));
            assert!(
                bad.spec
                    .validate("crm-db")
                    .unwrap_err()
                    .contains("remoteRef"),
                "{path}"
            );
        }
    }

    #[test]
    fn names_and_refresh_interval_are_checked() {
        let spec = connection(crm(serde_json::json!({}))).spec;
        for name in ["", "Crm", "1crm", "crm_db", "crm-", &"a".repeat(41)] {
            assert!(spec.validate(name).is_err(), "{name}");
        }
        let mut s = spec.clone();
        for (interval, ok) in [
            ("15m", true),
            ("1m", true),
            ("24h", true),
            ("0m", false),
            ("25h", false),
            ("90s", false),
            ("m", false),
        ] {
            s.refresh_interval = interval.to_string();
            assert_eq!(s.validate("c").is_ok(), ok, "{interval}");
        }
    }

    #[test]
    fn the_connection_crd_carries_the_cel_rules_and_the_bounds() {
        let crd = Connection::crd();
        assert_eq!(crd.spec.names.plural, "connections");
        let yaml = serde_yaml::to_string(&crd).unwrap();
        for needle in [
            "x-kubernetes-validations",
            "a group connection needs ownerGroup and no usableBy",
            "a managed connection needs usableBy and no ownerGroup",
            "config holds settings, not secrets",
            "maxProperties: 32",
            "^[A-Za-z0-9][A-Za-z0-9._-]*$",
        ] {
            assert!(yaml.contains(needle), "the CRD lacks `{needle}`");
        }
        assert!(
            !yaml.contains("password:"),
            "no field may be called password"
        );
    }

    fn volume(spec: serde_json::Value) -> Volume {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Volume",
            "metadata": { "name": "raw-landing", "namespace": "stardelt" },
            "spec": spec
        }))
        .expect("deserialize Volume")
    }

    fn volume_spec(patch: serde_json::Value) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "catalog": "analytics",
            "purpose": "data",
            "storage": { "managed": {} },
            "grants": [
                { "group": "analysts", "verbs": ["read"] },
                { "group": "engineers", "verbs": ["read", "write"] }
            ]
        });
        for (k, v) in patch.as_object().unwrap() {
            if v.is_null() {
                spec.as_object_mut().unwrap().remove(k);
            } else {
                spec[k] = v.clone();
            }
        }
        spec
    }

    #[test]
    fn a_managed_volume_parses_and_derives_its_prefix() {
        let v = volume(volume_spec(serde_json::json!({})));
        assert_eq!(v.spec.purpose, VolumePurpose::Data);
        assert!(v.spec.storage.managed.is_some());
        assert!(v.spec.storage.external.is_none());
        assert!(v.spec.validate("raw-landing").is_ok());
        assert_eq!(
            v.spec.derived_prefix("raw-landing"),
            "volumes/analytics/raw-landing/"
        );
    }

    #[test]
    fn an_external_volume_parses_and_keeps_its_prefix() {
        let v = volume(volume_spec(serde_json::json!({
            "storage": { "external": { "connection": "raw-s3", "prefix": "landing/" } }
        })));
        assert_eq!(
            v.spec.storage.external.as_ref().unwrap().connection,
            "raw-s3"
        );
        assert!(v.spec.validate("raw-landing").is_ok());
        assert_eq!(v.spec.derived_prefix("raw-landing"), "landing/");
        let root = volume(volume_spec(serde_json::json!({
            "storage": { "external": { "connection": "raw-s3" } }
        })));
        assert_eq!(root.spec.derived_prefix("raw-landing"), "");
    }

    #[test]
    fn managed_storage_serializes_as_an_empty_object() {
        let v = volume(volume_spec(serde_json::json!({})));
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(json["spec"]["storage"]["managed"], serde_json::json!({}));
        assert_eq!(json["spec"]["storage"].get("external"), None);
    }

    #[test]
    fn the_storage_choice_is_exclusive() {
        let both = volume(volume_spec(serde_json::json!({
            "storage": { "managed": {}, "external": { "connection": "raw-s3" } }
        })));
        assert_eq!(
            both.spec.validate("raw-landing").unwrap_err(),
            "storage must be either managed or external, not both and not neither"
        );
        let neither = volume(volume_spec(serde_json::json!({ "storage": {} })));
        assert_eq!(
            neither.spec.validate("raw-landing").unwrap_err(),
            "storage must be either managed or external, not both and not neither"
        );
    }

    #[test]
    fn an_external_prefix_cannot_climb_out_of_its_volume() {
        for prefix in ["a/../b", "a//b"] {
            let bad = volume(volume_spec(serde_json::json!({
                "storage": { "external": { "connection": "raw-s3", "prefix": prefix } }
            })));
            assert_eq!(
                bad.spec.validate("raw-landing").unwrap_err(),
                "an external prefix must not contain .. or //",
                "{prefix}"
            );
        }
    }

    #[test]
    fn each_group_appears_once_in_grants() {
        let bad = volume(volume_spec(serde_json::json!({
            "grants": [
                { "group": "analysts", "verbs": ["read"] },
                { "group": "analysts", "verbs": ["write"] }
            ]
        })));
        assert_eq!(
            bad.spec.validate("raw-landing").unwrap_err(),
            "each group may appear once in grants"
        );
    }

    #[test]
    fn a_verb_appears_once_per_grant() {
        let bad = volume(volume_spec(serde_json::json!({
            "grants": [ { "group": "analysts", "verbs": ["read", "read"] } ]
        })));
        assert_eq!(
            bad.spec.validate("raw-landing").unwrap_err(),
            "a verb may appear once per grant"
        );
    }

    #[test]
    fn a_libraries_volume_only_lets_managers_write() {
        let ok = volume(volume_spec(serde_json::json!({
            "purpose": "libraries",
            "grants": [
                { "group": "analysts", "verbs": ["read"] },
                { "group": "engineers", "verbs": ["read", "write", "manage"] }
            ]
        })));
        assert!(ok.spec.validate("raw-landing").is_ok());
        let bad = volume(volume_spec(serde_json::json!({
            "purpose": "libraries",
            "grants": [ { "group": "engineers", "verbs": ["write"] } ]
        })));
        assert_eq!(
            bad.spec.validate("raw-landing").unwrap_err(),
            "on a libraries volume only a group that also manages it may write"
        );
    }

    #[test]
    fn verbs_sort_in_ascending_rights() {
        let mut verbs = vec![VolumeVerb::Manage, VolumeVerb::Read, VolumeVerb::Write];
        verbs.sort();
        assert_eq!(
            verbs,
            vec![VolumeVerb::Read, VolumeVerb::Write, VolumeVerb::Manage]
        );
    }

    #[test]
    fn volume_names_are_checked() {
        let spec = volume(volume_spec(serde_json::json!({}))).spec;
        for name in ["", "Vol", "1vol", "raw_landing", "raw-", &"a".repeat(41)] {
            assert!(spec.validate(name).is_err(), "{name}");
        }
        for name in ["raw-landing", "a", &"a".repeat(40)] {
            assert!(spec.validate(name).is_ok(), "{name}");
        }
    }

    #[test]
    fn the_volume_crd_carries_the_cel_rules_and_the_bounds() {
        let crd = Volume::crd();
        assert_eq!(crd.spec.names.plural, "volumes");
        assert_eq!(crd.spec.names.short_names, Some(vec!["vol".to_string()]));
        let yaml = serde_yaml::to_string(&crd).unwrap();
        for needle in [
            "x-kubernetes-validations",
            "storage must be either managed or external, not both and not neither",
            "an external prefix must not contain .. or //",
            "each group may appear once in grants",
            "a verb may appear once per grant",
            "on a libraries volume only a group that also manages it may write",
        ] {
            assert!(yaml.contains(needle), "the CRD lacks `{needle}`");
        }
        for bound in [
            "maxItems: 20",
            "maxItems: 3",
            "maxLength: 512",
            "maxLength: 64",
        ] {
            assert!(yaml.contains(bound), "the CRD lacks `{bound}`");
        }
    }

    fn principal(spec: serde_json::Value) -> ServicePrincipal {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipal",
            "metadata": { "name": "pipeline-analytics", "namespace": "stardelt" },
            "spec": spec
        }))
        .expect("deserialize ServicePrincipal")
    }

    #[test]
    fn a_principal_defaults_to_a_pipeline_identity_for_the_engine() {
        let sp = principal(serde_json::json!({ "ownerGroup": "analytics" }));
        assert_eq!(sp.spec.purpose, PrincipalPurpose::Pipeline);
        assert_eq!(sp.spec.audiences, vec![PrincipalAudience::Trino]);
        assert!(sp.spec.existing_secret_ref.is_none());
        assert!(sp.spec.validate("pipeline-analytics").is_ok());
        assert_eq!(
            principal_client_id("pipeline-analytics"),
            "sp-pipeline-analytics"
        );
    }

    #[test]
    fn a_principal_may_reach_the_catalog_too_and_name_a_secret_for_an_external_provider() {
        let sp = principal(serde_json::json!({
            "ownerGroup": "analytics", "purpose": "maintenance",
            "audiences": ["trino", "lakekeeper"],
            "existingSecretRef": { "name": "sp-maintenance" }
        }));
        assert_eq!(sp.spec.purpose, PrincipalPurpose::Maintenance);
        assert_eq!(sp.spec.audiences.len(), 2);
        assert_eq!(
            sp.spec.existing_secret_ref.as_ref().unwrap().name,
            "sp-maintenance"
        );
        assert!(sp.spec.validate("maintenance").is_ok());
    }

    #[test]
    fn bad_owner_groups_audiences_and_names_are_refused() {
        let bad =
            |spec: serde_json::Value, name: &str| principal(spec).spec.validate(name).unwrap_err();
        assert!(bad(serde_json::json!({ "ownerGroup": "a/b" }), "x").contains("ownerGroup"));
        assert!(bad(serde_json::json!({ "ownerGroup": "../x" }), "x").contains("ownerGroup"));
        assert!(
            bad(
                serde_json::json!({ "ownerGroup": "g", "audiences": ["trino", "trino"] }),
                "x"
            )
            .contains("audience")
        );
        assert!(
            bad(
                serde_json::json!({ "ownerGroup": "g", "audiences": [] }),
                "x"
            )
            .contains("audience")
        );
        for name in ["", "Pipeline", "1p", "p_", "p-", &"a".repeat(41)] {
            assert!(
                bad(serde_json::json!({ "ownerGroup": "g" }), name).contains("name"),
                "{name}"
            );
        }
    }

    #[test]
    fn the_principal_crd_has_the_rule_the_short_name_and_no_secret_field() {
        let crd = ServicePrincipal::crd();
        assert_eq!(crd.spec.names.plural, "serviceprincipals");
        assert_eq!(
            crd.spec.names.short_names.as_deref(),
            Some(&["sp".to_string()][..])
        );
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("each audience may appear once"));
        assert!(
            !yaml.contains("clientSecret") && !yaml.contains("password"),
            "no field holds a credential"
        );
    }

    fn catalog(spec: serde_json::Value) -> Catalog {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Catalog",
            "metadata": { "name": "finance", "namespace": "stardelt" },
            "spec": spec
        }))
        .expect("deserialize Catalog")
    }

    fn finance(patch: serde_json::Value) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "lakehouseRef": { "name": "default" },
            "owner": "stardelt-finops",
            "grants": [
                { "group": "stardelt-finops", "verb": "browse" },
                { "group": "stardelt-analytics", "verb": "browse" },
                { "group": "stardelt-analytics", "verb": "select" }
            ]
        });
        for (k, v) in patch.as_object().unwrap() {
            if v.is_null() {
                spec.as_object_mut().unwrap().remove(k);
            } else {
                spec[k] = v.clone();
            }
        }
        spec
    }

    #[test]
    fn a_catalog_parses_with_its_owner_grants_and_defaults() {
        let c = catalog(finance(serde_json::json!({})));
        assert_eq!(c.spec.owner, "stardelt-finops");
        assert_eq!(c.spec.grants.len(), 3);
        assert_eq!(c.spec.grants[2].verb, DataVerb::Select);
        assert!(
            !c.spec.managed_access,
            "managed access is off unless asked for"
        );
        assert!(c.spec.storage.is_none());
        assert!(c.spec.validate("finance").is_ok());
    }

    #[test]
    fn a_catalog_may_name_its_own_bucket_prefix_and_credential() {
        let c = catalog(finance(serde_json::json!({
            "storage": { "bucket": "finance", "keyPrefix": "warehouse/finance",
                         "credentialSecretRef": { "name": "finance-s3-creds", "key": "identity-entry" } },
            "managedAccess": true
        })));
        let storage = c.spec.storage.as_ref().unwrap();
        assert_eq!(storage.bucket.as_deref(), Some("finance"));
        assert_eq!(
            storage.credential_secret_ref.as_ref().unwrap().key,
            "identity-entry"
        );
        assert!(c.spec.managed_access);
        assert!(c.spec.validate("finance").is_ok());
    }

    #[test]
    fn names_that_are_not_sql_identifiers_or_are_reserved_are_refused() {
        let spec = catalog(finance(serde_json::json!({}))).spec;
        for name in [
            "",
            "Finance",
            "1fin",
            "fin-ance",
            "fin_ance",
            "fin.ance",
            &"a".repeat(41),
        ] {
            assert!(
                spec.validate(name).unwrap_err().contains("SQL identifier"),
                "{name}"
            );
        }
        for name in ["system", "information_schema", "memory", "tpch"] {
            assert!(spec.validate(name).is_err(), "{name}");
        }
    }

    #[test]
    fn a_duplicate_grant_and_a_climbing_prefix_are_refused() {
        let twice = catalog(finance(serde_json::json!({
            "grants": [ { "group": "g", "verb": "select" }, { "group": "g", "verb": "select" } ]
        })));
        assert!(
            twice
                .spec
                .validate("finance")
                .unwrap_err()
                .contains("twice")
        );
        // the same group may hold different verbs
        let two_verbs = catalog(finance(serde_json::json!({
            "grants": [ { "group": "g", "verb": "browse" }, { "group": "g", "verb": "select" } ]
        })));
        assert!(two_verbs.spec.validate("finance").is_ok());
        let climbing = catalog(finance(
            serde_json::json!({ "storage": { "keyPrefix": "a/../b" } }),
        ));
        assert!(
            climbing
                .spec
                .validate("finance")
                .unwrap_err()
                .contains("keyPrefix")
        );
    }

    #[test]
    fn there_is_no_use_verb_no_deny_and_no_credential_value_in_the_catalog_crd() {
        assert!(
            serde_json::from_value::<CatalogGrant>(
                serde_json::json!({ "group": "g", "verb": "use" })
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<CatalogGrant>(
                serde_json::json!({ "group": "g", "verb": "deny" })
            )
            .is_err()
        );
        let crd = Catalog::crd();
        assert_eq!(crd.spec.names.plural, "catalogs");
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("each group may hold each verb once"));
        assert!(yaml.contains("credentialSecretRef"));
        assert!(
            !yaml.contains("secretKey:") && !yaml.contains("password"),
            "no field holds a credential"
        );
    }

    fn warehouse(spec: serde_json::Value) -> SqlWarehouse {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "SqlWarehouse",
            "metadata": { "name": "big", "namespace": "stardelt" },
            "spec": spec
        }))
        .expect("deserialize SqlWarehouse")
    }

    #[test]
    fn a_warehouse_defaults_to_a_small_class_that_is_not_the_default_one() {
        let w = warehouse(serde_json::json!({ "lakehouseRef": { "name": "default" } }));
        assert_eq!(w.spec.size, WarehouseSize::Small);
        assert!(!w.spec.default);
        assert!(w.spec.grants.is_empty() && w.spec.concurrency.is_none());
        assert!(w.spec.validate("big").is_ok());
    }

    #[test]
    fn a_warehouse_parses_overrides_grants_and_scheduling_names() {
        let w = warehouse(serde_json::json!({
            "lakehouseRef": { "name": "default" }, "displayName": "Large (analytics)", "size": "large", "default": true,
            "concurrency": { "running": 4, "queued": 20, "memoryShare": "40%", "scheduling": "weighted_fair" },
            "grants": [ { "group": "stardelt-analytics" }, { "group": "stardelt-admins" } ]
        }));
        assert_eq!(w.spec.size, WarehouseSize::Large);
        let c = w.spec.concurrency.as_ref().unwrap();
        assert_eq!((c.running, c.queued), (Some(4), Some(20)));
        assert_eq!(c.scheduling, Some(WarehouseScheduling::WeightedFair));
        assert_eq!(c.memory_share.as_deref(), Some("40%"));
        assert!(w.spec.default && w.spec.grants.len() == 2);
        assert!(w.spec.validate("big").is_ok());
    }

    #[test]
    fn names_duplicate_groups_and_out_of_range_caps_are_refused() {
        let base = warehouse(serde_json::json!({ "lakehouseRef": { "name": "default" } })).spec;
        for name in ["", "Big", "1big", "big_", "big-", &"a".repeat(41)] {
            assert!(base.validate(name).is_err(), "{name}");
        }
        let twice = warehouse(serde_json::json!({
            "lakehouseRef": { "name": "default" }, "grants": [ { "group": "g" }, { "group": "g" } ]
        }));
        assert!(twice.spec.validate("big").unwrap_err().contains("twice"));
        for (running, queued) in [(0, 1), (101, 1), (1, -1), (1, 10001)] {
            let w = warehouse(serde_json::json!({
                "lakehouseRef": { "name": "default" }, "concurrency": { "running": running, "queued": queued }
            }));
            assert!(w.spec.validate("big").is_err(), "{running}/{queued}");
        }
    }

    #[test]
    fn the_example_manifest_deserialises_and_validates() {
        let yaml = include_str!("../../../examples/platforminstance-external-entra.yaml");
        let instance: PlatformInstance =
            serde_yaml::from_str(yaml).expect("the documentation example parses");
        assert_eq!(instance.spec.identity.mode, IdentityMode::External);
        assert_eq!(instance.spec.identity.provider, IdentityProvider::Entra);
        crate::identity::validate(&instance.spec).expect("the documentation example validates");
    }

    #[test]
    fn unknown_sizes_and_scheduling_names_do_not_parse() {
        for spec in [
            serde_json::json!({ "lakehouseRef": { "name": "d" }, "size": "2x-small" }),
            serde_json::json!({ "lakehouseRef": { "name": "d" }, "concurrency": { "scheduling": "round-robin" } }),
        ] {
            assert!(serde_json::from_value::<SqlWarehouseSpec>(spec).is_err());
        }
        let crd = SqlWarehouse::crd();
        assert_eq!(crd.spec.names.plural, "sqlwarehouses");
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("each group may appear once in grants"));
        assert!(yaml.contains("weighted_fair"));
    }
}

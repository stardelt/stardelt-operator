# stardelt Operator

Kubernetes operator for stardelt. Reconciles declarative CRDs into upstream best-of-breed
projects (Trino, Lakekeeper, Iceberg, SeaweedFS, Airflow, Spark, …) so a whole data platform
comes up from a small set of YAML files.

## Status

**Pre-alpha.** The CRD types, the reconcile *planner* and a controller skeleton are in place
and unit-tested. Applying the upstream charts (the per-plug adapters) is the next milestone —
today the controller computes the component plan and writes a truthful status. The controller
watches `PlatformInstance`, `Lakehouse`, `IngestionSource`, `Pipeline`, `Connection` and
`ServicePrincipal` (phase 1: it validates the principal and checks its credentials Secret; it does
not call the identity provider).

## CRDs

| CRD | Scope | Purpose |
|---|---|---|
| `PlatformInstance` | cluster | which pillars + profile are installed |
| `Lakehouse` | namespaced | a managed catalog + storage (+ query) unit on object storage |
| `IngestionSource` | namespaced | ingestion into Iceberg — one contract, three domains (`saas` / `enterprise` / `cdc`) |
| `Pipeline` | namespaced | ordered `sql` / `python` / `dbt` / `quality` steps on one engine (`airflow` / `spark`) |
| `Connection` | namespaced | a typed connection to an outside system: settings in the resource, credentials in the secret store (types: `postgresql`, `mysql`, `sqlserver`, `oracle`, `rest-api`, `s3`, `kafka`, `sftp`, `generic`); validated at admission and synced into the Secret `conn-<name>` through External Secrets |
| `Volume` | namespaced | a governed folder of files in the object store with an owner group and grants (`read` / `write` / `manage`), on managed or external storage; the resource is validated, nothing reconciles it yet |

`StreamApp` and the ML plugs (`ModelRegistry`, `ModelService`, `ModelEndpoint`) are planned (see
the master spec §5).

### Pillars

`PlatformInstance` carries what belongs to the platform rather than to one lakehouse:

| Pillar | Default | What it plans |
|---|---|---|
| `lakehouse` | `true` | nothing by itself — the storage + catalog + query components hang off each `Lakehouse` |
| `batch` | `false` | reserved for the `Pipeline`/Airflow work |
| `notebooks` | `false` | JupyterHub (chart `jupyterhub/jupyterhub`, pinned like every other component) |
| `streaming` | `false` | reserved for Kafka + Flink |
| `ml` | `false` | reserved for the ML plugs |

```yaml
apiVersion: stardelt.io/v1alpha1
kind: PlatformInstance
metadata: { name: cluster }
spec:
  profile: demo
  pillars: { notebooks: true }      # adds JupyterHub to the cluster-scoped plan
  namespace: stardelt               # where the platform's own components are installed (default)
  baseDomain: data.example.com      # Nova answers here, every other application on a subdomain
  identity:                         # where users sign in (decision D20)
    mode: external                  # none (default) | platform (Keycloak at id.<baseDomain>) | external
    provider: oidc                  # external only: oidc (default) | entra
    issuerUrl: https://login.microsoftonline.com/<tenant-id>/v2.0   # external only, https
    apiAudience: <api-app-client-id> # external only: the audience engine and catalog accept
    clients:                        # external only: app registrations the customer made
      nova: { clientId: <id>, clientSecretRef: { name: idp-nova } }
      lakekeeper: { clientId: <id> } # public, no secret
    groupClaim: groups              # optional: groups, roles for entra
    adminGroup: stardelt-admins     # default
  dryRun: true                      # plan first; flip to apply
```

A complete Entra instance is in
[`examples/platforminstance-external-entra.yaml`](examples/platforminstance-external-entra.yaml):
all ids are fake, and nothing in it is a secret value — `clientSecretRef` names a Secret in the
platform namespace. For `external`, `issuerUrl`, `apiAudience` and a `nova` client are required;
client names are limited to the platform's known clients, and every client except the public
`lakekeeper` one needs a `clientSecretRef`.

`identity` and `baseDomain` are validated by the API server (an `external` provider needs an
`https://` issuer, `platform` derives it and refuses one, both need `baseDomain`). `contract()`
in `src/identity.rs` turns them into the data of the ConfigMap `stardelt-identity` that every
component reads; publishing that ConfigMap from the controller is the next wave step. Until then
`make install-identity` in `stardelt-platform` writes it.

Notebooks are platform-level on purpose: one hub serves every lakehouse, and it is a whole
stack (hub, proxy, single-user pods) rather than a per-catalog concern. The golden values
mirror `stardelt-platform/helm-values/jupyterhub.yaml` — small resources, `ClusterIP`,
ephemeral per-user storage, sign-in through the platform IdP: the hub reads the identity
contract (ConfigMap `stardelt-identity`, Secret `stardelt-oidc-clients`) from its namespace;
install it with stardelt-platform's `make install-identity` first, or nobody can sign in.

### Identity registration checklist

With `identity.mode: external` the operator creates nothing in the customer's provider: the
clients are registered by hand. `identity-checklist` prints exactly what to register, per
client, from the same host functions the platform-mode realm uses, so the two cannot disagree:

```sh
cargo run -q -- identity-checklist examples/platforminstance-external-entra.yaml
cargo run -q -- identity-checklist examples/platforminstance-external-entra.yaml --format json
```

Each block is one app registration: its kind (confidential, public with PKCE, or a service
client), the redirect URIs with the instance's real hosts, the audience its tokens must carry,
and the provider-specific notes — for `entra` the API app's `accessTokenAcceptedVersion`, the
exposed scope, the app roles, the delegated permissions and (for the group sync) the admin
consent; for any other `oidc` provider the group claim. Only clients listed in
`spec.identity.clients` appear, so the output is the complete list for that instance. An
invalid file, or one that is not `external`, exits 1 and prints the reason on stderr.

### Plug selection

The plug matrix is expressed directly in the resource. Defaults are Lakekeeper + SeaweedFS +
the `demo` profile:

```yaml
apiVersion: stardelt.io/v1alpha1
kind: Lakehouse
metadata:
  name: main
  namespace: stardelt
spec:
  catalog:  { implementation: lakekeeper }   # lakekeeper | polaris
  storage:  { implementation: seaweedfs }    # seaweedfs | byoS3
  profile: demo                              # demo | full | airgap
  deletionPolicy: Retain                     # Retain (default) | Delete — what a delete does to the data (D18)
```

`deletionPolicy: Delete` uninstalls the releases and removes the database and object-store volumes; irreversible (a `dryRun` Lakehouse applied nothing, so deleting it removes no data).

A `Lakehouse` also installs Trino with the `warehouse` Iceberg catalog pre-wired: it points at
the Lakehouse's own Lakekeeper (`http://lakekeeper:8181/catalog`) and its SeaweedFS S3 service
(`http://seaweedfs-s3:8333`) — namespace-local names, so the same resource works in any
namespace. Two prerequisites come from the platform install, not from the operator:

- a `warehouse` warehouse in the catalog — `make install` creates it with
  `manifests/lakekeeper-bootstrap.yaml`;
- a `stardelt-s3-creds` Secret (keys `access-key`, `secret-key`) in the Lakehouse's namespace,
  which the catalog's `${ENV:S3_ACCESS_KEY}` / `${ENV:S3_SECRET_KEY}` substitution reads.
  `make install-core` creates the dev default in namespace `stardelt`; for a Lakehouse in any
  other namespace, create the Secret there yourself.

Without the Secret, Trino stays in `CreateContainerConfigError`.

A Lakehouse installs, in this order: the S3 Secrets (stardelt-s3-creds and seaweedfs-s3-config, generated once and never overwritten), the catalog database (a CloudNative-PG Cluster named lakekeeper-pg), SeaweedFS, Lakekeeper (only once the database is Ready), Trino, and a bootstrap Job that creates the warehouse. For byoS3, create stardelt-s3-creds yourself with access-key, secret-key, endpoint, bucket and region. The CloudNative-PG operator must be installed first: create a PlatformInstance.

### Discovery ConfigMap

Once the warehouse is bootstrapped, the operator publishes ConfigMap `<lakehouse>-discovery` in the
Lakehouse's namespace (labels `stardelt.io/lakehouse`, `stardelt.io/component=discovery`; owned by
the Lakehouse, so it is removed with it). It is the connection contract for notebooks, jobs,
dashboards and external tools: read these keys instead of hard-coding hostnames. The URLs are fully
qualified, so they resolve from any namespace. There are no credentials in it; those stay in Secret
`stardelt-s3-creds`.

| Key | Example | Meaning |
|---|---|---|
| `lakehouse` | `main` | name of the Lakehouse |
| `catalogUri` | `http://lakekeeper.data.svc.cluster.local:8181/catalog` | Iceberg REST catalog |
| `warehouse` | `warehouse` | warehouse name in the catalog |
| `s3Endpoint` | `http://seaweedfs-s3.data.svc.cluster.local:8333` | object store (from `stardelt-s3-creds`) |
| `s3Bucket` | `lakehouse` | bucket (from `stardelt-s3-creds`) |
| `s3Region` | `us-east-1` | region (from `stardelt-s3-creds`) |
| `trinoUrl` | `http://trino.data.svc.cluster.local:8080` | Trino coordinator |
| `trinoCatalog` | `warehouse` | Trino catalog that fronts the warehouse |

A pod uses it with `envFrom: [{ configMapRef: { name: main-discovery } }]` (same namespace) or by
reading the keys through the API. The status lists it as component `discovery`.

Ingestion is one contract with three domain defaults (master spec §5, plugs 8–10). `domain`
selects the plug and therefore the components the operator plans:

```yaml
apiVersion: stardelt.io/v1alpha1
kind: IngestionSource
metadata:
  name: customers
  namespace: stardelt
spec:
  domain: cdc            # saas (default) → seatunnel | enterprise → nifi | cdc → kafka + debezium
  target: warehouse.raw.customers
  schedule: "*/5 * * * *"          # optional; cdc follows its own continuous default
  source:                          # connector-specific, opaque to the operator
    connector: postgres
    table: public.customers
```

```yaml
apiVersion: stardelt.io/v1alpha1
kind: Pipeline
metadata:
  name: daily-orders
  namespace: stardelt
spec:
  engine: airflow        # airflow (default) → airflow | spark → spark-operator
  schedule:              # optional; without it the pipeline runs on demand
    cron: "0 3 * * *"
    timezone: Europe/Berlin
  ownerGroup: analytics        # the group whose members see it and run it (optional)
  runAs: pipeline-analytics   # service principal the tasks run as (optional)
  retries: { attempts: 2, backoff: 5m }
  tasks:
    - name: load
      kind: sql          # sql | python | dbt | quality (dbt also pulls in dbt-core)
      sql: INSERT INTO warehouse.analytics.trips SELECT * FROM warehouse.raw.trips
    - name: build
      kind: dbt
      dependsOn: [load]  # a task starts when these have succeeded; no circles
      config: { selector: "tag:nightly" }
    - name: assert
      kind: quality
      dependsOn: [build]
      config: { gate: "warn" }
```

A `Connection` never holds a secret value. `scope: group` is managed and used by the members of `ownerGroup`;
`scope: managed` is written by administrators, readable by nobody, and used only by platform-run code for the
groups in `usableBy`:

```yaml
apiVersion: stardelt.io/v1alpha1
kind: Connection
metadata:
  name: crm-db
  namespace: stardelt
spec:
  scope: group             # group (default) | managed
  ownerGroup: analytics    # scope group; for scope managed use usableBy: [analytics, finance]
  type: sqlserver          # postgresql | mysql | sqlserver | oracle | rest-api | s3 | kafka | sftp | generic
  config:                  # settings only; a name like password or token is refused
    host: crm.example.com
    port: "1433"
    database: sales
  secret:
    keys: [user, password] # the secret fields; the store path is derived from the scope
  refreshInterval: 15m     # 1m to 24h
```

Each `type` requires settings and secret keys (a database: `config.host`, `user`, `password`; `rest-api`: `config.baseUrl`
and `token` or `clientId` and `clientSecret`; `s3`: `config.bucket`, `accessKeyId`, `secretAccessKey`; `sftp`:
`config.host`, `user` and `password` or `privateKey`; `kafka`: `config.bootstrapServers`). The API server enforces the
rules at admission; `ConnectionSpec::validate` repeats them for the controller.

The controller (`connection.rs`) creates one External Secrets `ExternalSecret` `conn-<name>` per connection, owned by the
`Connection`, that reads the fields in `secret.keys` from the store path derived from the scope through the
`SecretStore` of the owner group (`openbao-<group>`) or of managed connections (`openbao-managed`), and keeps the Kubernetes
Secret `conn-<name>` up to date. `kubectl get connection` status: `Ready` is `True` when the Secret is synced; the reason is
`Synced`, `Syncing`, `InvalidSpec`, `RemoteRefNotAllowed` (a path of one's own is refused until an external store is
configured), `GroupNameNotSupported` (the group name cannot name a store: lower-case letters, digits and dashes only) or
`ExternalSecretsNotInstalled`. Deleting the `Connection` removes the sync and the Secret; the value in the store stays.

A `Volume` is a governed folder of files beside the tables. `storage.managed: {}` puts it at
`volumes/<catalog>/<name>/` in the platform object store; `storage.external` names an `s3` connection and a prefix in
the customer's bucket instead. Grants are for groups, never users:

```yaml
apiVersion: stardelt.io/v1alpha1
kind: Volume
metadata:
  name: raw-landing
  namespace: stardelt
spec:
  catalog: analytics            # the Lakehouse this volume belongs to (owner group is inherited)
  purpose: data                 # data (default) | libraries
  storage:
    managed: {}                 # or external: { connection: raw-s3, prefix: landing/ }
  grants:
    - { group: analysts,  verbs: [read] }
    - { group: engineers, verbs: [read, write] }
```

A `purpose: libraries` volume is code in every reader's notebook, so only a group that also holds `manage` may hold
`write` there. The API server enforces that and the rest of the shape at admission (exactly one storage kind, a prefix
without `..` or `//`, one grant per group, one of each verb); `VolumeSpec::validate` repeats the rules for the
controller. `VolumeSpec::derived_prefix` is the path the gateway will use.

## Editor support

`cargo run --bin crdgen` also writes JSON Schemas for every CRD into `schemas/`
(`platforminstance.schema.json`, `lakehouse.schema.json`,
`ingestionsource.schema.json`, `pipeline.schema.json`, `connection.schema.json`,
`volume.schema.json`). Add

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/stardelt/stardelt-operator/main/schemas/pipeline.schema.json
```

as the first line of a file (replace the file name by the kind) to get validation
and completion in editors that use the YAML language server (for example VS Code
with the YAML extension).

## Architecture

Built with [kube-rs](https://kube.rs). The operator does **not** re-implement upstream charts;
it watches CRDs and applies upstream Helm releases via the `helm` binary in its image
(`src/helm.rs`), then reports a truthful per-component status.

- `src/crd.rs` — the plug specification (types + status).
- `src/planner.rs` — pure functions: which components a resource needs, in apply order, plus
  the golden chart pins (`chart_version`).
- `src/values.rs` — pure functions: the golden per-component values, parsing of external
  documents, and the merge order.
- `src/identity_checklist.rs` — pure functions: what to register in an external provider, per
  client (the `identity-checklist` command).
- `src/helm.rs` — the Helm adapter (trait + command runner; recording runner for tests).
- `src/controller.rs` — reconciles `PlatformInstance` → its cluster-scoped plan and `Lakehouse`
  → its component plan.
- `src/bin/crdgen.rs` — emits `chart/crds/` from the types.

### Component values

Every planned component is configurable from the CR — chart values, resource sizing,
storage class, ingress, image registry — without editing code. The operator ships **golden
values** per component (the curated settings that make the stack work, e.g. Trino's
`http-server.process-forwarded=true` behind an ingress, SeaweedFS' PVC-backed volumes) and
layers the CR on top. Lakekeeper's golden values use the operator-created database (lakekeeper-pg, Secret lakekeeper-pg-app) and authz allowall; SeaweedFS reads its S3 identities from Secret seaweedfs-s3-config. Merge order per component, later wins:

1. the operator's golden defaults (`src/values.rs`),
2. `spec.valuesFrom.<component>` — an external YAML document from a ConfigMap or Secret key,
3. `spec.values.<component>` — free-form YAML inline in the CR,
4. `spec.set.<component>.<helm.key>` — passed to Helm as `--set`, which Helm always applies
   last.

The merge is a **deep merge**: objects merge key by key, arrays and scalars replace (Helm's
own `-f` merge semantics). A key you do not set keeps its golden default.

Worked example — pin Trino's resources on my cluster, put SeaweedFS on a storage class, and
keep the per-environment base in GitOps:

```yaml
apiVersion: stardelt.io/v1alpha1
kind: Lakehouse
metadata: { name: main, namespace: stardelt }
spec:
  catalog: { implementation: lakekeeper }
  storage: { implementation: seaweedfs }
  values:
    trino:
      coordinator:
        resources:
          requests: { cpu: 500m, memory: 2Gi }
          limits:   { cpu: "2",  memory: 4Gi }
      worker:
        resources:
          limits: { cpu: "2", memory: 4Gi }
    seaweedfs:
      volume:
        dataDirs:
          - name: data
            type: persistentVolumeClaim
            size: 100Gi
            storageClass: fast-ssd
            maxVolumes: 100
  valuesFrom:
    trino:
      configMapRef: { name: trino-values, key: values.yaml }
  set:
    trino:
      server.workers: "2"      # convenience for a single scalar
```

Notes:

- Only the keys you set change: the example above still carries Trino's golden
  `additionalConfigProperties`, because the override merges into it.
- Arrays replace wholesale, exactly like `helm -f`: the `dataDirs` entry above must stay
  complete (`name`, `type`, `maxVolumes`), not just the `storageClass`.
- `valuesFrom` references exactly one of `configMapRef` / `secretRef`, plus the `key` holding
  the document. `namespace` is optional and defaults to the resource's own namespace; the
  cluster-scoped `PlatformInstance` has none, so it must name one.
- External documents are re-read on every reconcile — nothing is cached that could go stale,
  so a document that changed in GitOps is picked up on the next pass. A missing or malformed
  document is reported per component in `.status.components[].message`, the phase turns
  `Degraded`, and the other components still reconcile (one bad reference never blocks the
  Lakehouse).
- Overrides keyed by a component that is not in the plan (e.g. `polaris` while the catalog
  is Lakekeeper) are ignored, with a warning in the operator log.

### End-to-end test

`tests/e2e/lakehouse.sh` creates a kind cluster, builds the operator image from the checkout, installs
the operator chart, applies one `PlatformInstance` and one `Lakehouse` (both `profile: demo`,
`dryRun: false`), waits for `status.phase: Ready`, and then creates, reads and drops an Iceberg table
through Trino. It needs docker, kind, kubectl and helm, about 12 GB of free memory and up to 30
minutes:

```sh
bash tests/e2e/lakehouse.sh      # prints "e2e OK" on success; always deletes the cluster
```

The workflow `.github/workflows/e2e.yml` runs it nightly and on demand (not on pull requests) and
uploads the failure diagnostics as an artifact.

### Safety: `dryRun`

A new `Lakehouse` **plans only** (`spec.dryRun` defaults to `true`): the controller computes the
component plan, runs `helm --dry-run` for each and writes the plan into `.status.components`.
Set `dryRun: false` to actually apply. `.status.phase` is `Planned` while dry-running, `Ready`
once every component applied, `Progressing` while one still needs to converge, and `Degraded`
when a values source cannot be resolved — an admin error that will not heal by itself.

`Lakehouse` and `PlatformInstance` also write a `Ready` condition and
`.status.observedGeneration` (the generation the status was computed from), so
`kubectl wait --for=condition=Ready` and GitOps health checks work. `lastTransitionTime`
changes only when `Ready` flips.

```yaml
apiVersion: stardelt.io/v1alpha1
kind: Lakehouse
metadata: { name: main, namespace: stardelt }
spec:
  catalog: { implementation: lakekeeper }
  storage: { implementation: seaweedfs }
  dryRun: true     # flip to apply
```

### RBAC

An installer operator is privileged by nature: the chart grants cluster-wide apply rights so
Helm can create whatever the upstream charts contain.

### Component scope — a known correction

`plan_lakehouse` currently includes the **CNPG operator**, which is cluster-scoped and belongs
to the *platform*, not to an individual Lakehouse (two Lakehouses would collide on it). The
split lands with `PlatformInstance` fan-out (WP-23): platform-level prerequisites are
reconciled once, and a Lakehouse only manages its own namespace-scoped resources (SeaweedFS,
the CNPG `Cluster`, Lakekeeper). Until then the `adopt` guard is what keeps an apply from
clobbering an existing install — keep it `false` on clusters installed by the Makefile.

## Dev

```sh
cargo test                 # CRD + planner unit tests (no cluster)
cargo run                  # connects to the cluster, or idles if none is available
cargo run --bin crdgen     # regenerate chart/crds/
helm template chart/       # render the operator manifests
```

## Repo layout

```
Cargo.toml                     workspace
crates/stardelt-operator/
  src/{lib,crd,planner,values,controller}.rs
  src/bin/crdgen.rs
chart/                         Helm chart + generated CRDs
```

## ServicePrincipal

A `ServicePrincipal` is a non-human identity for pipelines, maintenance and collectors. Its owner group's members may name
it as a pipeline's `runAs` and may not read its credentials. With the platform identity provider the operator will create a
client with a service account and write its credentials to the Secret `sp-<name>` (`client-id`, `client-secret`); with an
external provider it only checks the Secret named in `existingSecretRef`. Today the resource is validated and nothing
reconciles it yet.

```yaml
apiVersion: stardelt.io/v1alpha1
kind: ServicePrincipal
metadata:
  name: pipeline-analytics
  namespace: stardelt
spec:
  ownerGroup: analytics        # members may use it as runAs
  purpose: pipeline            # pipeline (default) | maintenance | collector
  audiences: [trino]           # trino (default) and/or lakekeeper
```

## Catalog

A `Catalog` is one data domain inside the catalog software of a `Lakehouse`: its storage location, one owner group and the
grants of other groups. `metadata.name` is the name users type (`finance.orders`), the Trino catalog and the catalog's
warehouse: lower-case letters and digits only, because it is an unquoted SQL identifier. Subjects are groups, never people;
the verbs are `browse`, `select`, `write`, `create` and `manage` (no deny, no per-column grants); ownership means `manage`
and does not mean `select`. Today the resource is validated and nothing reconciles it yet.

```yaml
apiVersion: stardelt.io/v1alpha1
kind: Catalog
metadata:
  name: finance
  namespace: stardelt
spec:
  lakehouseRef: { name: default }
  owner: stardelt-finops
  storage:                      # optional: the Lakehouse's storage defaults apply when omitted
    bucket: finance
    credentialSecretRef: { name: finance-s3-creds, key: identity-entry }
  grants:
    - { group: stardelt-finops, verb: browse }
    - { group: stardelt-analytics, verb: select }
  managedAccess: false          # true: only platform administrators can grant on this catalog
```

## SqlWarehouse

A `SqlWarehouse` is a compute class for SQL inside the engine of a `Lakehouse`: a size (`xs`, `small`, `medium`, `large`),
optional caps and the groups that may send queries into it. A class is a name for four numbers (queries running, queries
queued, memory share, scheduling), because the engine has resource groups and not separate clusters; the size does not
promise hardware. Exactly one warehouse per `Lakehouse` is the default. Today the resource is validated and nothing
reconciles it yet.

```yaml
apiVersion: stardelt.io/v1alpha1
kind: SqlWarehouse
metadata:
  name: big
  namespace: stardelt
spec:
  lakehouseRef: { name: default }
  size: large                  # xs | small (default) | medium | large
  concurrency:                 # optional overrides of the class
    running: 4
    queued: 20
    memoryShare: 40%
    scheduling: weighted_fair  # fifo | fair | weighted | weighted_fair
  grants:
    - { group: stardelt-analytics }
  default: false
```

## Links

- [stardelt.io/roadmap](https://stardelt.io/roadmap)
- [Design — master spec](https://stardelt.io/design/master-spec)

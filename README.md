# stardelt Operator

Kubernetes operator for stardelt. Reconciles declarative CRDs into upstream best-of-breed
projects (Trino, Lakekeeper, Iceberg, SeaweedFS, Airflow, Spark, …) so a whole data platform
comes up from a small set of YAML files.

## Status

**Pre-alpha.** The CRD types, the reconcile *planner* and a controller skeleton are in place
and unit-tested. Applying the upstream charts (the per-plug adapters) is the next milestone —
today the controller computes the component plan and writes a truthful status. The controller
watches `PlatformInstance`, `Lakehouse`, `IngestionSource` and `Pipeline`.

## CRDs

| CRD | Scope | Purpose |
|---|---|---|
| `PlatformInstance` | cluster | which pillars + profile are installed |
| `Tenant` | namespaced | isolation boundary for data and identities |
| `Lakehouse` | namespaced | a managed catalog + storage (+ query) unit on object storage |
| `IngestionSource` | namespaced | ingestion into Iceberg — one contract, three domains (`saas` / `enterprise` / `cdc`) |
| `Pipeline` | namespaced | ordered `sql` / `python` / `dbt` / `quality` steps on one engine (`airflow` / `spark`) |

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
  dryRun: true                      # plan first; flip to apply
```

Notebooks are platform-level on purpose: one hub serves every lakehouse, and it is a whole
stack (hub, proxy, single-user pods) rather than a per-catalog concern. The golden values
mirror `stardelt-platform/helm-values/jupyterhub.yaml` — small resources, `ClusterIP`,
ephemeral per-user storage, sign-in through the platform IdP: the hub reads the identity
contract (ConfigMap `stardelt-identity`, Secret `stardelt-oidc-clients`) from its namespace;
install it with stardelt-platform's `make install-identity` first, or nobody can sign in.

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
```

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
  schedule: "0 3 * * *"  # optional; without it the pipeline runs on demand
  steps:
    - name: build
      kind: dbt          # sql | python | dbt | quality (also pulls in dbt-core)
      config: { selector: "tag:nightly" }
    - name: assert
      kind: quality
      config: { gate: "warn" }
```

## Editor support

`cargo run --bin crdgen` also writes JSON Schemas for every CRD into `schemas/`
(`platforminstance.schema.json`, `tenant.schema.json`, `lakehouse.schema.json`,
`ingestionsource.schema.json`, `pipeline.schema.json`). Add

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
- `src/helm.rs` — the Helm adapter (trait + command runner; recording runner for tests).
- `src/controller.rs` — reconciles `Tenant` → Namespace, `PlatformInstance` → its cluster-scoped
  plan and `Lakehouse` → its component plan.
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

### Safety: `dryRun`

A new `Lakehouse` **plans only** (`spec.dryRun` defaults to `true`): the controller computes the
component plan, runs `helm --dry-run` for each and writes the plan into `.status.components`.
Set `dryRun: false` to actually apply. `.status.phase` is `Planned` while dry-running, `Ready`
once every component applied, `Progressing` while one still needs to converge, and `Degraded`
when a values source cannot be resolved — an admin error that will not heal by itself.

`Tenant`, `Lakehouse` and `PlatformInstance` also write a `Ready` condition and
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
Helm can create whatever the upstream charts contain. Tenants additionally create Namespaces.

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

## Links

- [stardelt.io/roadmap](https://stardelt.io/roadmap)
- [Design — master spec](https://stardelt.io/design/master-spec)

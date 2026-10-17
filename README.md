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

## Architecture

Built with [kube-rs](https://kube.rs). The operator does **not** re-implement upstream charts;
it watches CRDs and applies upstream Helm releases via the `helm` binary in its image
(`src/helm.rs`), then reports a truthful per-component status.

- `src/crd.rs` — the plug specification (types + status).
- `src/planner.rs` — pure functions: which components a resource needs, in apply order, plus
  the golden chart pins (`chart_version`).
- `src/helm.rs` — the Helm adapter (trait + command runner; recording runner for tests).
- `src/controller.rs` — reconciles `Tenant` → Namespace and `Lakehouse` → its component plan.
- `src/bin/crdgen.rs` — emits `chart/crds/` from the types.

### Safety: `dryRun`

A new `Lakehouse` **plans only** (`spec.dryRun` defaults to `true`): the controller computes the
component plan, runs `helm --dry-run` for each and writes the plan into `.status.components`.
Set `dryRun: false` to actually apply.

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
  src/{lib,crd,planner,controller}.rs
  src/bin/crdgen.rs
chart/                         Helm chart + generated CRDs
```

## Links

- [stardelt.io/roadmap](https://stardelt.io/roadmap)
- [Design — master spec](https://stardelt.io/design/master-spec)

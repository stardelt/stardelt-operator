# stardelt Operator

Kubernetes operator for stardelt. Reconciles declarative CRDs into upstream best-of-breed
projects (Trino, Lakekeeper, Iceberg, SeaweedFS, Airflow, Spark, …) so a whole data platform
comes up from a small set of YAML files.

## Status

**Pre-alpha.** The CRD types, the reconcile *planner* and a controller skeleton are in place
and unit-tested. Applying the upstream charts (the per-plug adapters) is the next milestone —
today the controller computes the component plan and writes a truthful status.

## CRDs

| CRD | Scope | Purpose |
|---|---|---|
| `PlatformInstance` | cluster | which pillars + profile are installed |
| `Tenant` | namespaced | isolation boundary for data and identities |
| `Lakehouse` | namespaced | a managed catalog + storage (+ query) unit on object storage |

`Pipeline`, `StreamApp` and `MLWorkspace` are planned (see the master spec §5).

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

## Architecture

Built with [kube-rs](https://kube.rs). The operator does **not** re-implement upstream charts;
it watches CRDs and applies upstream Helm releases and native manifests via server-side apply.

- `src/crd.rs` — the plug specification (types + status).
- `src/planner.rs` — pure functions: which components a resource needs, in apply order.
- `src/controller.rs` — the reconcile loop (watches, writes status).
- `src/bin/crdgen.rs` — emits `chart/crds/` from the types.

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

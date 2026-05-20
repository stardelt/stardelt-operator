# stardelt Operator

Kubernetes operator for stardelt. Reconciles the `Lakehouse`, `PlatformInstance`, `Tenant`,
`Pipeline`, `StreamApp`, and `MLWorkspace` CRDs — composing upstream best-of-breed projects
(Trino, Lakekeeper, Iceberg, Spark, Airflow, RisingWave, KubeRay, …) into a single declarative
experience.

## Status

**Pre-alpha — skeleton only, no reconcile logic yet.** Part of the broader stardelt MVP / vibecoding push; everything here is expected to change fast.

Watch [github.com/stardelt/stardelt-operator/issues/1](https://github.com/stardelt/stardelt-operator/issues/1)
for the implementation milestone.

## Planned CRDs

| CRD | Purpose |
|---|---|
| `PlatformInstance` | Cluster-scoped: declares which stardelt pillars + foundations are installed |
| `Tenant` | Namespaced scope of data + identities; isolation boundary |
| `Lakehouse` | A managed Trino + Lakekeeper + Iceberg unit on object storage |
| `Pipeline` | Batch ETL workload (Spark/Airflow/dbt) bound to a Tenant + Lakehouse |
| `StreamApp` | Streaming workload (Kafka/Flink/RisingWave) |
| `MLWorkspace` | ML/AI environment (Ray/Kubeflow/MLflow/KServe) |

## Architecture

Built with [kube-rs](https://kube.rs) on Tokio. Each CRD has a dedicated controller
(`crates/stardelt-operator/src/controllers/<crd>.rs` — planned). The operator does **not**
re-implement upstream charts; it watches CRDs and applies/reconciles upstream Helm releases
and native manifests via kube-rs server-side apply.

## Dev

```sh
cargo check
cargo run   # logs a stub message and sleeps — no cluster required
```

## Repo layout

```
Cargo.toml                       # workspace
crates/stardelt-operator/
  Cargo.toml
  src/main.rs                    # entry — currently a stub
chart/                           # Helm chart for the operator — TBD
```

## Links

- [stardelt.io/roadmap](https://stardelt.io/roadmap)
- [stardelt.io/architecture/overview](https://stardelt.io/architecture/overview)
- [stardelt.io/design/master-spec](https://stardelt.io/design/master-spec)

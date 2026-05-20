# stardelt Operator

Kubernetes operator for stardelt. Reconciles the `Lakehouse`, `PlatformInstance`, `Tenant`,
`Pipeline`, `StreamApp`, and `MLWorkspace` CRDs — composing upstream best-of-breed projects
(Trino, Lakekeeper, Iceberg, Spark, Airflow, RisingWave, KubeRay, …) into a single declarative
experience.

## Status

**Phase 0 / Stage 4 — skeleton only, no reconcile logic yet.**

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

- [docs.stardelt.io/roadmap](https://docs.stardelt.io/roadmap)
- [docs.stardelt.io/architecture/overview](https://docs.stardelt.io/architecture/overview)
- [docs.stardelt.io/design/master-spec](https://docs.stardelt.io/design/master-spec)

# stardelt Operator

Kubernetes operator for stardelt, built with [kube-rs](https://kube.rs) on Tokio. It
reconciles a single cluster-scoped `PlatformInstance` CRD into the full stardelt **core
stack**, composing upstream best-of-breed projects (SeaweedFS, CloudNative-PG, Lakekeeper,
Trino, Airflow, Superset) plus stardelt Nova into one declarative experience.

## Status

**Phase 1 — core-stack reconciler.** A `PlatformInstance` brings up all nine core
components in dependency order with per-component status conditions. The other planned CRDs
(`Tenant`, `Lakehouse`, `Pipeline`, `StreamApp`, `MLWorkspace`) are not yet implemented.

## How it works

The operator does **not** run Helm itself. For the six upstream charts it emits Flux
`HelmRepository` / `HelmRelease` custom resources and lets Flux do the installs; the native
pieces (the CNPG `Cluster`, the `ozone-s3-creds` Secret, the Lakekeeper bootstrap `Job`, and
Nova's `Deployment` + `Service`) are applied directly via server-side apply.

Reconcile is an ordered walk of the dependency DAG — the same order as
`stardelt-platform/Makefile`. Each pass applies every resource idempotently, then gates on
the next un-ready step (requeuing until it is ready) before proceeding:

```
HelmRepositories → CNPG operator → lakekeeper-pg Cluster → SeaweedFS →
ozone-s3-creds Secret → Lakekeeper → bootstrap Job (warehouse) → Trino →
Airflow → Superset → Nova
```

Flux `dependsOn` is set between HelmReleases as defense-in-depth, but the operator's own
gating is the source of truth — Flux alone cannot order a release behind a native CNPG
`Cluster` becoming Ready or a `Job` succeeding.

## Prerequisites

- A Kubernetes cluster and a kubeconfig (or in-cluster service account).
- **Flux** installed (`flux install`) — required for the HelmRelease installs.

## Dev

```sh
make check                       # fmt --check + clippy -D warnings + cargo check (CI gate)
make crd                         # print the PlatformInstance CRD
make run                         # run the controller against the current kube-context

# End to end against an existing cluster with Flux installed:
cargo run -- crd | kubectl apply -f -
kubectl apply -f examples/platforminstance-dev.yaml
kubectl get platforminstance -w
```

## Configuration (env)

Mirrors Nova's `NOVA_*` convention.

| Var | Default | Meaning |
|---|---|---|
| `STARDELT_OPERATOR_FIELD_MANAGER` | `stardelt-operator` | SSA field manager |
| `STARDELT_OPERATOR_RESYNC_SECS` | `300` | Steady-state resync interval |
| `STARDELT_OPERATOR_POLL_SECS` | `10` | Requeue interval while waiting on a step |
| `RUST_LOG` | `info` | Log filter |

## Repo layout

```
Cargo.toml                       # workspace
crates/stardelt-operator/src/
  main.rs                        # runtime + `crd` subcommand + controller bootstrap
  config.rs                      # env-only config (STARDELT_OPERATOR_*)
  error.rs                       # typed error enum
  readiness.rs                   # status conditions + readiness predicates
  api/platform_instance.rs       # the PlatformInstance CRD type
  controllers/platform_instance.rs  # reconcile() — the ordered DAG engine
  resources/                     # flux / cnpg / secret / bootstrap / nova builders
  values/                        # vendored copies of the 6 helm-values/*.yaml (embedded)
chart/                           # Helm chart: operator Deployment, RBAC, CRD
examples/                        # sample PlatformInstance
Dockerfile                       # multi-stage image build
```

> **Sync note:** chart versions and values are pinned in three places — this operator
> (`api::platform_instance::chart_versions` + `src/values/`), `stardelt-platform/Makefile`,
> and `stardelt-demos/kind/up.sh`. Keep them in sync when bumping (see CLAUDE.md).

## Links

- [stardelt.io/roadmap](https://stardelt.io/roadmap)
- [stardelt.io/architecture/overview](https://stardelt.io/architecture/overview)
- [stardelt.io/design/master-spec](https://stardelt.io/design/master-spec)

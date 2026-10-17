# AGENTS.md — stardelt-operator

Kubernetes operator for stardelt — the control-plane pillar (master spec §5–§6). It
watches the stardelt CRDs and reconciles them into upstream Helm releases (Lakekeeper,
SeaweedFS, Trino, …). **Pre-alpha:** the CRD types, planner and controller are in place and
unit-tested; `Tenant`, `PlatformInstance` and `Lakehouse` have been reconciled successfully
against the dev cluster. New resources default to `spec.dryRun: true`, so they plan first.

## Read first

- `../stardelt-docs/docs/design/master-spec.md` — canonical spec (published at
  https://stardelt.io); when documents disagree, it wins (§5 plug matrix, §6).
- `../stardelt-docs/docs/architecture/licenses.md` — license policy.

The running development environment is the Hetzner k3s cluster
(`alpha.dev.stardelt.io`), provisioned from `stardelt-platform/dev/hetzner-k3s`. The
operator is not yet part of the platform install; point it at that cluster via your
kubeconfig.

## Layout

- `crates/stardelt-operator/src/` — `crd.rs` (plug specification), `planner.rs`
  (component plans + golden chart pins), `helm.rs` (Helm adapter), `controller.rs`
  (reconcilers), `bin/crdgen.rs` (CRD generator).
- `chart/` — Helm chart; `chart/crds/` is generated, `templates/operator.yaml` holds
  ServiceAccount + RBAC + Deployment.
- `image/Dockerfile` — container image (`ghcr.io/stardelt/stardelt-operator`).

## Build / test / run

```bash
cargo test                        # CRD + planner unit tests, no cluster needed
cargo check --all-targets         # CI runs fmt + clippy -D warnings + check
cargo run --bin crdgen            # regenerate chart/crds/ from crd.rs (commit output)
helm template chart/              # render the operator chart
cargo run                         # reconcile against the current kubeconfig (idles without a cluster)
```

## Conventions

- **PR-only.** Never push to `main`; branch off `origin/main` and keep PRs small and
  scoped.
- **License policy: OSI-permissive only** (Apache-2.0, MIT, BSD). The single documented
  exception is OpenBao (MPL-2.0). See
  `../stardelt-docs/docs/architecture/licenses.md`.
- Read the master spec first — the CRDs are the product surface, so schema changes are
  design changes.

## Gotchas

- `chart/crds/` is generated from the Rust types — never edit it by hand. After changing
  `src/crd.rs`, run `cargo run --bin crdgen` (from the repo root) and commit the output.
- A new `Lakehouse` defaults to `spec.dryRun: true`: the controller plans only and runs
  each release with `helm --dry-run`. Flip `dryRun: false` to actually apply; a unit
  test asserts the default.
- Helm releases are invoked with `--repo`, so the operator needs no `helm repo add`
  state; chart versions are pinned in `planner.rs::chart_version` (mirrors the platform
  Makefile).
- Helm installs `chart/crds/` on `helm install`, not on `helm upgrade` — apply CRD
  changes explicitly.

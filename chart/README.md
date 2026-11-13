# Helm chart — stardelt-operator

Installs the operator and its CRDs.

```
chart/
  Chart.yaml
  values.yaml
  crds/                       generated — do not edit by hand
  templates/operator.yaml     ServiceAccount + RBAC + Deployment
```

## CRDs

`crds/` is generated from the Rust types. Regenerate after changing `src/crd.rs`:

```sh
cargo run -p stardelt-operator --bin crdgen
```

Helm installs everything in `crds/` on `helm install`. On `helm upgrade` the operator applies
its own CRDs from its binary at start (server-side, field manager `stardelt-operator`), so an
upgrade needs no manual step.

## Install

```sh
helm upgrade --install stardelt-operator chart/ \
  --namespace stardelt --create-namespace
```

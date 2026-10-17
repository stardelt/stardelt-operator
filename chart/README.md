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

Helm installs everything in `crds/` on `helm install` (not on `helm upgrade`; apply
them explicitly if they change).

## Install

```sh
helm upgrade --install stardelt-operator chart/ \
  --namespace stardelt --create-namespace
```

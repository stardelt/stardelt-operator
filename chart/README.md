# Helm Chart — stardelt-operator

Deploys the stardelt-operator and (by default) installs the `PlatformInstance` CRD.

## Prerequisites

- **Flux** must be installed in the cluster (source-controller + helm-controller).
  The operator emits `HelmRepository` / `HelmRelease` custom resources and relies
  on Flux to perform the actual chart installs:

  ```sh
  flux install
  ```

## Install

```sh
helm install stardelt-operator ./chart --namespace stardelt-system --create-namespace
```

Then create a `PlatformInstance` to bring up the core stack:

```sh
kubectl apply -f ../examples/platforminstance-dev.yaml
kubectl get platforminstance -w
```

## Values

| Key | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/stardelt/operator` | Operator image |
| `image.tag` | `dev` | Image tag |
| `replicaCount` | `1` | Keep at 1 (no leader election yet) |
| `installCRD` | `true` | Install the PlatformInstance CRD with the release |
| `config.resyncSecs` | `300` | Steady-state resync interval |
| `config.pollSecs` | `10` | Requeue interval while waiting on a step |
| `logLevel` | `info` | `RUST_LOG` value |

The CRD template is generated from the Rust type; regenerate with
`make regen-crd` (then re-add the `installCRD` guard).

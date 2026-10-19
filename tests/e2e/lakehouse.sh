#!/usr/bin/env bash
# End-to-end test (operator#60): on an empty kind cluster, one PlatformInstance and one
# Lakehouse produce a stack that answers a Trino query through the Lakehouse's own catalog.
# Needs docker, kind, kubectl and helm on PATH. Run from anywhere: bash tests/e2e/lakehouse.sh
set -euo pipefail

CLUSTER=stardelt-e2e
NS=data
IMAGE=stardelt-operator:e2e
DIAG_DIR="${E2E_DIAG_DIR:-/tmp/stardelt-e2e}"

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
mkdir -p "$DIAG_DIR"

# A private kubeconfig: kind writes its context here, so every kubectl and helm call below
# can only reach the kind cluster, never a cluster of the caller (and the diagnostics
# directory, which CI uploads, never holds credentials).
WORK_DIR="$(mktemp -d)"
export KUBECONFIG="$WORK_DIR/kubeconfig"

log() { printf '\n== %s\n' "$*"; }

# Diagnostics never read a Secret: only the stardelt resources, pods, events and logs.
diagnostics() {
  {
    echo "---- platforminstance and lakehouse ----"
    kubectl get platforminstance,lakehouse -A -o yaml
    echo "---- pods ----"
    kubectl get pods -A -o wide
    echo "---- events in $NS ----"
    kubectl -n "$NS" get events --sort-by=.lastTimestamp
    echo "---- pods in $NS ----"
    kubectl -n "$NS" describe pods
    echo "---- operator log ----"
    kubectl -n stardelt-system logs deploy/stardelt-operator --tail=300
  } >"$DIAG_DIR/diagnostics.txt" 2>&1 || true
  tail -n 120 "$DIAG_DIR/diagnostics.txt"
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    diagnostics
  fi
  kind delete cluster --name "$CLUSTER" || true
  rm -rf "$WORK_DIR"
  exit "$status"
}
trap cleanup EXIT

# retry <attempts> <seconds> <command...>
retry() {
  local attempts=$1 pause=$2
  shift 2
  local n=1
  until "$@"; do
    if [ "$n" -ge "$attempts" ]; then
      return 1
    fi
    n=$((n + 1))
    sleep "$pause"
  done
}

# Same invocation as stardelt-demos/kind/smoke.sh; CSV quotes every value, so strip the quotes.
sql() {
  kubectl -n "$NS" exec deploy/trino-coordinator -- trino --execute "$1" --output-format=CSV | tr -d '"'
}

log "create the kind cluster"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --wait 120s

log "build the operator image and load it into kind"
docker build -f image/Dockerfile -t "$IMAGE" .
kind load docker-image "$IMAGE" --name "$CLUSTER"

log "install the operator (helm installs chart/crds on the first install)"
helm upgrade --install stardelt-operator chart \
  --namespace stardelt-system --create-namespace \
  --set image.repository=stardelt-operator \
  --set image.tag=e2e \
  --set image.pullPolicy=Never \
  --wait --timeout 5m

log "apply the PlatformInstance and wait for CloudNative-PG"
kubectl apply -f - <<'EOF'
apiVersion: stardelt.io/v1alpha1
kind: PlatformInstance
metadata:
  name: cluster
spec:
  profile: demo
  dryRun: false
EOF
retry 60 10 kubectl get crd clusters.postgresql.cnpg.io

log "apply the Lakehouse and wait for Ready"
kubectl create namespace "$NS"
kubectl apply -f - <<'EOF'
apiVersion: stardelt.io/v1alpha1
kind: Lakehouse
metadata:
  name: main
  namespace: data
spec:
  profile: demo
  dryRun: false
EOF
kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Ready lakehouse/main --timeout=20m

log "the discovery ConfigMap is published"
test "$(kubectl -n "$NS" get configmap main-discovery -o jsonpath='{.data.trinoCatalog}')" = "warehouse"

log "query Trino through the Lakehouse's own catalog"
# The first statements can race the worker registering with the coordinator.
catalogs="$(retry 12 10 sql 'SHOW CATALOGS')"
grep -qx 'warehouse' <<<"$catalogs"
sql 'CREATE SCHEMA IF NOT EXISTS warehouse.e2e'
retry 6 10 sql 'CREATE TABLE warehouse.e2e.t AS SELECT 1 AS x'
result="$(sql 'SELECT x FROM warehouse.e2e.t')"
test "$result" = '1'
sql 'DROP TABLE warehouse.e2e.t'
sql 'DROP SCHEMA warehouse.e2e'

echo "e2e OK"

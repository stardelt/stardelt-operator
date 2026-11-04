#!/usr/bin/env bash
# Fail when the operator's golden chart pins (planner.rs `chart_version`) differ
# from the pins in stardelt-platform's Makefile — the bash install path the
# operator replaces. Release names must match too, so the versions are the only
# thing that may drift silently.
#
#   bash scripts/check-platform-pins.sh                     # compares against platform main
#   PLATFORM_MAKEFILE=../stardelt-platform/Makefile bash scripts/check-platform-pins.sh
# The platform Makefile mixes := and ?=; both are read.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

PLANNER=crates/stardelt-operator/src/planner.rs
URL="https://raw.githubusercontent.com/stardelt/stardelt-platform/main/Makefile"

makefile="$(mktemp)"
trap 'rm -f "$makefile"' EXIT
if [[ -n "${PLATFORM_MAKEFILE:-}" ]]; then
  cp "$PLATFORM_MAKEFILE" "$makefile"
else
  curl -fsSL --retry 3 "$URL" -o "$makefile"
fi

# planner chart reference -> Makefile variable prefix
declare -A PREFIX_OF=(
  [cnpg/cloudnative-pg]=CNPG
  [seaweedfs/seaweedfs]=SEAWEEDFS
  [lakekeeper/lakekeeper]=LAKEKEEPER
  [trino/trino]=TRINO
  [jupyterhub/jupyterhub]=JUPYTERHUB
  [apache-airflow/airflow]=AIRFLOW
  [superset/superset]=SUPERSET
  [openfga/openfga]=OPENFGA
  [external-secrets/external-secrets]=EXTERNAL_SECRETS
  [openbao/openbao]=OPENBAO
)

status=0
while IFS= read -r chart; do
  prefix="${PREFIX_OF[$chart]}"
  planner_version="$(sed -n "s|^[[:space:]]*\"${chart}\"[[:space:]]*=>[[:space:]]*Some(\"\([^\"]*\)\").*|\1|p" "$PLANNER" | head -n 1)"
  make_version="$(sed -n "s/^${prefix}_VERSION[[:space:]]*[^[:space:]=]*=[[:space:]]*\([^[:space:]#]*\).*/\1/p" "$makefile")"
  if [[ -z "$planner_version" || -z "$make_version" ]]; then
    echo "✗ $chart: missing pin (planner='${planner_version}', platform Makefile='${make_version}')" >&2
    status=1
  elif [[ "$planner_version" != "$make_version" ]]; then
    echo "✗ $chart: planner pins $planner_version but platform Makefile pins $make_version" >&2
    status=1
  else
    echo "✓ $chart $planner_version"
  fi
done < <(printf '%s\n' "${!PREFIX_OF[@]}" | sort)

# Reverse coverage: every chart `chart_version` pins must also be checked here. A chart added to
# the planner and forgotten in PREFIX_OF would otherwise be skipped without a word.
while IFS= read -r chart; do
  if [[ -z "${PREFIX_OF[$chart]:-}" ]]; then
    echo "✗ $chart: pinned in chart_version but missing from PREFIX_OF in this script" >&2
    status=1
  fi
done < <(awk '/^pub fn chart_version/,/^}/' "$PLANNER" \
         | sed -n 's/^[[:space:]]*\"\([^\"]*\)\"[[:space:]]*=>[[:space:]]*Some(.*/\1/p' | sort -u)

exit "$status"

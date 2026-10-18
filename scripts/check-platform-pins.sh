#!/usr/bin/env bash
# Fail when the operator's golden chart pins (planner.rs `chart_version`) differ
# from the pins in stardelt-platform's Makefile — the bash install path the
# operator replaces. Release names must match too, so the versions are the only
# thing that may drift silently.
#
#   bash scripts/check-platform-pins.sh                     # compares against platform main
#   PLATFORM_MAKEFILE=../stardelt-platform/Makefile bash scripts/check-platform-pins.sh
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
)

status=0
while IFS= read -r chart; do
  prefix="${PREFIX_OF[$chart]}"
  planner_version="$(sed -n "s|^[[:space:]]*\"${chart}\"[[:space:]]*=>[[:space:]]*Some(\"\([^\"]*\)\").*|\1|p" "$PLANNER" | head -n 1)"
  make_version="$(sed -n "s/^${prefix}_VERSION[[:space:]]*:=[[:space:]]*\([^[:space:]#]*\).*/\1/p" "$makefile")"
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
exit "$status"

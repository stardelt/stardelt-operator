#!/usr/bin/env bash
# Fail when parity/steps.tsv is not a complete picture of what stardelt-platform
# installs — the bash install path the operator replaces. Every target that
# `make install` calls and every extra install target the platform offers needs
# a row, and a `built` row must name a symbol this repository really has; a
# `missing` or `not-planned` row must say which ticket or why.
#
#   bash scripts/check-parity.sh                            # compares against platform main
#   PLATFORM_MAKEFILE=../stardelt-platform/Makefile bash scripts/check-parity.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

STEPS=parity/steps.tsv
PLANNER=crates/stardelt-operator/src/planner.rs
SRC=crates/stardelt-operator/src
URL="https://raw.githubusercontent.com/stardelt/stardelt-platform/main/Makefile"

# Steps that are not called from the `install:` recipe but are part of the
# platform's install path all the same.
EXTRA_TARGETS=(install-identity step-notebooks step-spark step-spark-operator step-kafka external-secrets openbao)

makefile="$(mktemp)"
trap 'rm -f "$makefile"' EXIT
if [[ -n "${PLATFORM_MAKEFILE:-}" ]]; then
  cp "$PLATFORM_MAKEFILE" "$makefile"
else
  curl -fsSL --retry 3 "$URL" -o "$makefile"
fi

status=0
fail() { echo "✗ $*" >&2; status=1; }

declare -A STATUS_OF=()
declare -a targets=()
lineno=0
while IFS=$'\t' read -r target row_status symbol reason _ || [[ -n "${target:-}" ]]; do
  lineno=$((lineno + 1))
  [[ $lineno -eq 1 ]] && continue   # header
  [[ -z "${target:-}" ]] && continue
  targets+=("$target")
  STATUS_OF[$target]="$row_status"

  if [[ "$row_status" != built && "$row_status" != missing && "$row_status" != not-planned ]]; then
    fail "$STEPS: row '$target' has status '${row_status}', expected built, missing or not-planned"
    continue
  fi

  if [[ "$row_status" == built ]]; then
    if ! grep -rqE "^[[:space:]]*(pub )?(async )?fn ${symbol}\b" "$SRC" \
       && ! grep -qF "name: \"${symbol}\"" "$PLANNER"; then
      fail "$STEPS: row '$target' is built but '${symbol}' is neither a fn under $SRC nor a name: \"...\" in $PLANNER"
    fi
  elif [[ -z "${reason//[[:space:]]/}" || "${reason//[[:space:]]/}" == "-" ]]; then
    fail "$STEPS: row '$target' is '$row_status' but names no ticket or reason"
  fi
done < "$STEPS"

# Forward coverage: every target the platform's `install:` recipe calls.
install_targets="$(awk '
  /^install:/ { inside = 1 }
  inside && !/^\t/ && !/^install:/ { exit }
  inside { print }
' "$makefile" | grep -oE 'step-[a-z-]+' | sort -u || true)"

while IFS= read -r target; do
  [[ -z "$target" ]] && continue
  if [[ -z "${STATUS_OF[$target]:-}" ]]; then
    fail "$STEPS: the platform's install recipe calls '$target' but there is no row for it"
  fi
done <<< "$install_targets"

for target in "${EXTRA_TARGETS[@]}"; do
  if [[ -z "${STATUS_OF[$target]:-}" ]]; then
    fail "$STEPS: '$target' is an install target of stardelt-platform but there is no row for it"
  fi
done

if [[ $status -ne 0 ]]; then
  exit 1
fi

built=0
missing=0
not_planned=0
for target in "${targets[@]}"; do
  case "${STATUS_OF[$target]}" in
    built) built=$((built + 1)) ;;
    missing) missing=$((missing + 1)) ;;
    not-planned) not_planned=$((not_planned + 1)) ;;
  esac
done

echo "parity: ${built} built, ${missing} missing, ${not_planned} not planned"

#!/usr/bin/env bash
# Compare the operator's golden Nova image (provision.rs NOVA_IMAGE) with the digest
# stardelt-platform pins in manifests/nova-deployment.yaml. The two live in different
# repositories, so no CI sees both (stardelt-platform#759, operator#545).
#
#   bash scripts/check-golden-nova.sh            # 0 equal, 1 differ, 2 unreadable
#   bash scripts/check-golden-nova.sh --report   # print the issue title and body, exit 0
#   PLATFORM_NOVA_FILE=../stardelt-platform/manifests/nova-deployment.yaml bash …   # offline
#
# Needs: bash, grep, curl (curl only when PLATFORM_NOVA_FILE is unset).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROVISION=crates/stardelt-operator/src/provision.rs
PLATFORM_FILE="${PLATFORM_NOVA_FILE:-}"
PLATFORM_URL="https://raw.githubusercontent.com/stardelt/stardelt-platform/main/manifests/nova-deployment.yaml"

die() { echo "check-golden-nova: $*" >&2; exit 2; }

platform_source="$(mktemp)"
trap 'rm -f "$platform_source"' EXIT
if [[ -n "$PLATFORM_FILE" ]]; then
  [[ -f "$PLATFORM_FILE" ]] || die "PLATFORM_NOVA_FILE=$PLATFORM_FILE does not exist"
  cp "$PLATFORM_FILE" "$platform_source"
else
  curl -fsSL --retry 3 "$PLATFORM_URL" -o "$platform_source" || die "cannot read $PLATFORM_URL"
fi

# `|| true`: without a match, grep exits 1 and `set -euo pipefail` aborts the assignment
# before the check below can explain itself (operator#577).
references="$(grep -oE 'ghcr\.io/stardelt/nova:dev@sha256:[0-9a-f]{64}' "$platform_source" | head -n 1 || true)"
[[ "$references" =~ ^ghcr\.io/stardelt/nova:dev@sha256:[0-9a-f]{64}$ ]] \
  || die "no nova digest in the platform manifest"
platform_digest="sha256:${references##*@sha256:}"

operator_digest="$(sed -n 's/^pub const NOVA_IMAGE: &str = "ghcr\.io\/stardelt\/nova:dev@sha256:\([0-9a-f]\{64\}\)";$/\1/p' "$PROVISION")"
[[ "$operator_digest" =~ ^[0-9a-f]{64}$ ]] || die "no NOVA_IMAGE digest in $PROVISION"
operator_digest="sha256:$operator_digest"

if [[ "$platform_digest" == "$operator_digest" ]]; then
  echo "✓ golden nova $operator_digest is the platform pin"
  exit 0
fi

short_platform="${platform_digest#sha256:}"; short_platform="${short_platform:0:8}"
short_operator="${operator_digest#sha256:}"; short_operator="${short_operator:0:8}"

if [[ "${1:-}" == "--report" ]]; then
  cat <<EOF
title: chore(pins): the golden Nova image lags the platform pin ($short_platform)

The operator's golden Nova image is not the digest stardelt-platform pins, so an operator
install runs a Nova one or more builds behind.

## Evidence

\`\`\`
platform  manifests/nova-deployment.yaml                        $platform_digest
operator  crates/stardelt-operator/src/provision.rs NOVA_IMAGE  $operator_digest
\`\`\`

## Required change

1. \`crates/stardelt-operator/src/provision.rs\`, the line
   \`pub const NOVA_IMAGE: &str = "ghcr.io/stardelt/nova:dev@sha256:...";\`:
   replace the digest with \`$platform_digest\`.
2. \`cargo fmt --check && cargo test\` (the test
   \`nova_runs_the_pinned_image_and_reports_it_running\` asserts the constant is a pinned digest).
3. After the merge the \`Image\` workflow publishes a new \`dev\` operator image; the platform's
   pin rollup pins it and the lab rolls it. The architect does that step, not the implementer.

## Open questions

None.
EOF
  exit 0
fi

echo "✗ golden nova differs: platform $platform_digest / operator $operator_digest" >&2
echo "  fix: crates/stardelt-operator/src/provision.rs NOVA_IMAGE" >&2
exit 1

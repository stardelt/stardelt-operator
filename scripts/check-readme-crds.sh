#!/usr/bin/env bash
# Fail when README.md's CRD table and the kinds declared in crd.rs disagree.
# operator#347: the table had lost four served kinds and called StreamApp "planned".
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python3 - <<'PY'
import re
import sys

src = open("crates/stardelt-operator/src/crd.rs").read()
kinds = sorted(set(re.findall(r'kind = "([A-Za-z]+)"', src)))

text = open("README.md").read()
if "## CRDs" not in text:
    sys.exit("README.md has no '## CRDs' section")
section = text.split("## CRDs", 1)[1].split("###", 1)[0]
rows = sorted(set(re.findall(r"^\| `([A-Za-z]+)` \|", section, re.M)))

missing = [k for k in kinds if k not in rows]
extra = [r for r in rows if r not in kinds]
if missing or extra:
    for k in missing:
        print(f"README.md's CRD table is missing `{k}` (declared in crd.rs)")
    for r in extra:
        print(f"README.md's CRD table lists `{r}`, which is not a CRD kind in crd.rs")
    sys.exit(1)

print(f"✓ README lists all {len(kinds)} CRD kinds")
PY

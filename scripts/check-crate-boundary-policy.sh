#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
mode="${1:-}"
if [ "$mode" != "" ] && [ "$mode" != "--expect-blocked" ]; then
  echo "usage: scripts/check-crate-boundary-policy.sh [--expect-blocked]" >&2
  exit 64
fi

cd "$repo_root"
metadata_file="$(mktemp "${TMPDIR:-/tmp}/velorix-crate-boundary.XXXXXX.json")"
trap 'rm -f "$metadata_file"' EXIT
cargo metadata --format-version 1 --no-deps >"$metadata_file"
python3 - "$mode" "$metadata_file" <<'PY'
import json
import sys

mode = sys.argv[1]
metadata_path = sys.argv[2]
with open(metadata_path, "r", encoding="utf-8") as f:
    metadata = json.load(f)
packages = {package["name"]: package for package in metadata["packages"]}

blocked_direct_deps = {
    "velorix-api": {"velorix-k8s", "velorix-meta", "velorix-storage"},
    "velorix-cli": {"velorix-storage"},
    "velorix-k8s": {"velorix-storage"},
    "velorix-runtime": {"velorix-control"},
}
blocked_model_deps = {
    "velorix-core": {"datafusion", "tokio", "object_store", "kube", "k8s-openapi"},
}

violations = []
for crate, blocked in {**blocked_direct_deps, **blocked_model_deps}.items():
    deps = {
        dependency["name"]
        for dependency in packages[crate]["dependencies"]
        if dependency.get("kind") is None
    }
    for dep in sorted(deps & blocked):
        violations.append({"crate": crate, "dependency": dep})

if mode == "--expect-blocked":
    if not violations:
        raise SystemExit("no policy violations found; switch this check to strict CI mode")
    print(json.dumps({"status": "blocked", "violations": violations}, sort_keys=True))
    raise SystemExit(0)

if violations:
    print(json.dumps({"status": "fail", "violations": violations}, indent=2, sort_keys=True), file=sys.stderr)
    raise SystemExit(1)

print(json.dumps({"status": "pass", "violations": []}, sort_keys=True))
PY

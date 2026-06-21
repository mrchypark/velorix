#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate live upgrade/rollback/repair/GC fault-matrix evidence.

Usage:
  scripts/check-upgrade-rollback-repair-gc-fault-matrix-evidence.sh PATH

This validates release evidence kind upgrade_rollback_repair_gc_fault_matrix.
It requires live evidence, not a local smoke or synthesized fixture.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ "$#" -ne 1 ]; then
  usage >&2
  exit 64
fi

helper_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/evidence_ref_validator.py"

python3 - "$1" "$helper_path" <<'PY'
import importlib.util
import json
import sys

path = sys.argv[1]
helper_spec = importlib.util.spec_from_file_location("evidence_ref_validator", sys.argv[2])
helper = importlib.util.module_from_spec(helper_spec)
helper_spec.loader.exec_module(helper)
validate_evidence_ref = helper.validate_evidence_ref
validate_release_identity_fields = helper.validate_release_identity_fields
try:
    with open(path, "r", encoding="utf-8") as f:
        evidence = json.load(f)
except (OSError, json.JSONDecodeError) as exc:
    print(json.dumps({"status": "fail", "errors": [str(exc)]}, indent=2, sort_keys=True))
    raise SystemExit(1)

errors = []
if not isinstance(evidence, dict):
    errors.append("evidence must be a JSON object")
    matrix = {}
else:
    matrix = evidence.get("gc_status") or evidence
    if not isinstance(matrix, dict):
        errors.append("gc_status must be a JSON object")
        matrix = {}

required_true = [
    "live_upgrade_rollback_repair_gc_matrix",
    "upgrade_verified",
    "rollback_verified",
    "repair_verified",
    "gc_reachability_verified",
    "acknowledged_data_preserved",
    "no_source_query_recomputation",
]
required_scenarios = {
    "rolling_upgrade",
    "rollback_after_upgrade",
    "corrupt_latest_checkpoint_repair",
    "gc_concurrent_with_query",
    "gc_concurrent_with_compaction",
    "gc_concurrent_with_recovery",
    "gc_concurrent_with_checkpoint_publication",
    "gc_retains_repair_roots",
}


def valid_scenario_names(scenarios):
    names = set()
    for index, item in enumerate(scenarios):
        if not isinstance(item, dict):
            errors.append(f"scenarios[{index}] must be an object")
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"scenarios[{index}].name must be a non-empty string")
            continue
        names.add(name)
        if item.get("status") != "pass":
            errors.append(f"scenarios[{index}].status must be pass")
        evidence_ref = item.get("evidence")
        if not isinstance(evidence_ref, str) or not evidence_ref.strip():
            errors.append(f"scenarios[{index}].evidence must be a non-empty string")
        else:
            errors.extend(
                validate_evidence_ref(evidence_ref, path, f"scenarios[{index}].evidence")
            )
    return names


forbidden_tokens = {
    "local-only",
    "local_only",
    "local smoke",
    "local_smoke",
    "emulator",
    "fake",
}

if matrix.get("evidence_kind") != "upgrade_rollback_repair_gc_fault_matrix":
    errors.append("evidence_kind must be upgrade_rollback_repair_gc_fault_matrix")

if matrix.get("status") != "pass":
    errors.append("status must be pass")

errors.extend(validate_release_identity_fields(matrix))

deployment_id = matrix.get("deployment_id")
if not isinstance(deployment_id, str) or not deployment_id.strip():
    errors.append("deployment_id must be a non-empty string")

authority_store_id = matrix.get("authority_store_id")
if not isinstance(authority_store_id, str) or not authority_store_id.startswith("s3://"):
    errors.append("authority_store_id must be an s3:// URI")

for field in required_true:
    if matrix.get(field) is not True:
        errors.append(f"{field} must be true")

scenarios = matrix.get("scenarios")
if not isinstance(scenarios, list):
    errors.append("scenarios must be a list")
    scenario_names = set()
else:
    scenario_names = valid_scenario_names(scenarios)
    missing = sorted(required_scenarios - scenario_names)
    if missing:
        errors.append("scenarios missing: " + ", ".join(missing))

text = json.dumps(evidence, sort_keys=True).lower()
for token in sorted(forbidden_tokens):
    if token in text:
        errors.append(f"evidence must not contain {token}")

if errors:
    print(
        json.dumps(
            {
                "status": "fail",
                "evidence_kind": matrix.get("evidence_kind"),
                "errors": errors,
            },
            indent=2,
            sort_keys=True,
        )
    )
    raise SystemExit(1)

print(
    json.dumps(
        {
            "status": "pass",
            "evidence_kind": "upgrade_rollback_repair_gc_fault_matrix",
            "required_scenarios_verified": sorted(required_scenarios),
            "message": "Upgrade/rollback/repair/GC fault-matrix evidence shape is valid",
        },
        sort_keys=True,
    )
)
PY

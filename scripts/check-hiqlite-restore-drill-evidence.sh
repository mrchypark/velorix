#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate live Hiqlite total-voter-loss restore drill evidence.

Usage:
  scripts/check-hiqlite-restore-drill-evidence.sh PATH

This validates standalone drill evidence kind hiqlite_total_voter_loss_restore_drill.
It also accepts readiness compatibility evidence kind hiqlite_no_pvc_three_voter_backup_restore.
Both names require the same live no-PVC, three-voter, object-store-backup
total-voter-loss restore drill.
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
import re
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

allowed_evidence_kinds = {
    "hiqlite_total_voter_loss_restore_drill",
    "hiqlite_no_pvc_three_voter_backup_restore",
}
if evidence.get("evidence_kind") not in allowed_evidence_kinds:
    errors.append(
        "evidence_kind must be one of "
        + ", ".join(sorted(allowed_evidence_kinds))
    )

required = {
    "status": "pass",
    "no_pvc": True,
    "voter_count": 3,
    "total_voter_loss_exercised": True,
    "restored_from_object_store_backup": True,
    "acknowledged_metadata_writes_survived": True,
    "catalog_verified": True,
    "owner_epoch_verified": True,
    "checkpoint_pointer_verified": True,
    "post_restore_ingest_query_verified": True,
    "restore_drill_verified": True,
}
required_evidence_refs = [
    "object_store_backup",
    "total_voter_loss_log",
    "restore_log",
    "metadata_write_survival",
    "post_restore_ingest_query",
]


def validate_evidence_refs(value):
    if not isinstance(value, dict):
        errors.append("evidence_refs must be an object")
        return
    for field in required_evidence_refs:
        ref = value.get(field)
        if not isinstance(ref, str) or not ref.strip():
            errors.append(f"evidence_refs.{field} must be a non-empty string")
        else:
            errors.extend(validate_evidence_ref(ref, path, f"evidence_refs.{field}"))

for field, expected in required.items():
    if evidence.get(field) != expected:
        errors.append(f"{field} must be {expected!r}")

errors.extend(validate_release_identity_fields(evidence))

deployment_id = evidence.get("deployment_id")
if not isinstance(deployment_id, str) or not deployment_id.strip():
    errors.append("deployment_id must be a non-empty string")

authority_store_id = evidence.get("authority_store_id")
if not isinstance(authority_store_id, str) or not authority_store_id.startswith("s3://"):
    errors.append("authority_store_id must be an s3:// URI")

validate_evidence_refs(evidence.get("evidence_refs"))

text = json.dumps(evidence, sort_keys=True).lower()
for forbidden in (
    "persistentvolumeclaim",
    "volumeclaimtemplates",
    "volumeclaim",
    "local-only",
    "local_only",
    "emulator",
):
    if forbidden in text:
        errors.append(f"evidence must not contain {forbidden}")

if re.search(r"\bpvc\b", text):
    errors.append("evidence must not contain pvc")

if errors:
    print(
        json.dumps({"status": "fail", "errors": errors}, indent=2, sort_keys=True),
        file=sys.stderr,
    )
    raise SystemExit(1)

print(
    json.dumps(
        {
            "status": "pass",
            "evidence_kind": "hiqlite_total_voter_loss_restore_drill",
            "restore_drill_verified": True,
            "evidence_refs_verified": required_evidence_refs,
        },
        sort_keys=True,
    )
)
PY

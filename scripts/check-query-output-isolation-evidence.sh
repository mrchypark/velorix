#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate live query output isolation evidence.

Usage:
  scripts/check-query-output-isolation-evidence.sh PATH

This validates release evidence kind query_output_isolation. It requires live
evidence, not a local-only, emulator, fake, or synthetic artifact.
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
    evidence = {}

required = {
    "evidence_kind": "query_output_isolation",
    "status": "pass",
    "live_release_query_isolation": True,
    "query_authority": "published_materialized_output",
    "cold_query_succeeded": True,
    "query_pod_source_ingest_prefix_read_access": False,
    "query_pod_metadata_write_access": False,
    "object_store_audit_no_source_reads": True,
    "object_store_audit_no_source_writes": True,
    "object_store_audit_no_durable_writes": True,
    "materialized_output_read_verified": True,
    "no_source_query_recomputation": True,
}
required_evidence_refs = [
    "query_pod_iam_policy",
    "cold_query_log",
    "object_store_audit_log",
    "materialized_output_read",
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

forbidden_tokens = {
    "local-only",
    "local_only",
    "local only",
    "local smoke",
    "local_smoke",
    "emulator",
    "fake",
    "synthetic",
}
text = json.dumps(evidence, sort_keys=True).lower()
for token in sorted(forbidden_tokens):
    if token in text:
        errors.append(f"evidence must not contain {token}")

if errors:
    print(
        json.dumps(
            {
                "status": "fail",
                "evidence_kind": evidence.get("evidence_kind"),
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
            "evidence_kind": "query_output_isolation",
            "query_authority": "published_materialized_output",
            "live_release_query_isolation": True,
            "evidence_refs_verified": required_evidence_refs,
            "message": "Live query output isolation evidence shape is valid",
        },
        sort_keys=True,
    )
)
PY

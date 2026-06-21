#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate remaining 1.0 release-readiness evidence.

Usage:
  scripts/check-remaining-release-readiness-evidence.sh PATH

This validates release evidence kind remaining_release_readiness. It requires
real live/release evidence, not local-only, emulator, fake, synthetic, mock,
placeholder, TODO, or TBD evidence.
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

required_true = [
    "release_image_contract_tests_passed",
    "versioned_openapi_contract_verified",
    "no_conflicting_accepted_contracts",
    "sql_admission_corpus_generated",
    "sql_admission_corpus_covers_unsupported_datafusion_plan_nodes",
    "sql_admission_corpus_covers_unsupported_datafusion_expression_nodes",
    "unsupported_sql_leaves_no_persisted_view_metadata",
    "unsupported_sql_leaves_no_runtime_binding",
    "sql_admission_mutation_ci_failure_verified",
    "persistent_write_boundary_crash_matrix_passed",
    "crash_matrix_covers_one_view",
    "crash_matrix_covers_multiple_affected_views",
    "crash_matrix_covers_joins",
    "crash_matrix_covers_compaction",
    "replay_duplicate_reordered_gapped_retried_batches_verified",
    "replay_live_crash_clean_outputs_identical",
    "replay_checkpoint_hashes_identical",
    "non_contiguous_input_never_advances_frontier",
    "join_frontier_spec_verified",
    "concurrent_two_input_ingest_crash_leader_handoff_verified",
    "output_manifests_record_exact_input_frontiers",
    "published_limits_verified",
    "multi_day_supported_object_store_soak_passed",
]
required_evidence_refs = [
    "release_image_contract_tests",
    "openapi_contract",
    "sql_admission_corpus",
    "crash_matrix",
    "replay_determinism",
    "join_frontier",
    "scale_soak",
]
forbidden_tokens = {
    "local-only",
    "local_only",
    "local only",
    "emulator",
    "fake",
    "synthetic",
    "mock",
    "placeholder",
    "todo",
    "tbd",
}


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


def walk_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from walk_strings(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from walk_strings(item)


errors = []
if not isinstance(evidence, dict):
    errors.append("evidence must be a JSON object")
    observed_kind = None
else:
    observed_kind = evidence.get("evidence_kind")
    if observed_kind != "remaining_release_readiness":
        errors.append("evidence_kind must be remaining_release_readiness")
    if evidence.get("status") != "pass":
        errors.append("status must be pass")
    errors.extend(validate_release_identity_fields(evidence))
    deployment_id = evidence.get("deployment_id")
    if not isinstance(deployment_id, str) or not deployment_id.strip():
        errors.append("deployment_id must be a non-empty string")
    authority_store_id = evidence.get("authority_store_id")
    if not isinstance(authority_store_id, str) or not authority_store_id.startswith("s3://"):
        errors.append("authority_store_id must be an s3:// URI")
    for field in required_true:
        if evidence.get(field) is not True:
            errors.append(f"{field} must be true")
    validate_evidence_refs(evidence.get("evidence_refs"))
    for value in walk_strings(evidence):
        lower = value.lower()
        for token in sorted(forbidden_tokens):
            if token in lower:
                errors.append(f"evidence string values must not contain {token}")

if errors:
    print(
        json.dumps(
            {
                "status": "fail",
                "evidence_kind": observed_kind,
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
                "evidence_kind": "remaining_release_readiness",
                "verified": required_true,
                "evidence_refs_verified": required_evidence_refs,
                "message": "remaining 1.0 release-readiness evidence is valid",
            },
        sort_keys=True,
    )
)
PY

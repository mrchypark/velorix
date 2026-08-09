#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate live security and release provenance evidence.

Usage:
  scripts/check-security-release-provenance-evidence.sh PATH

This validates an existing release evidence artifact only. It does not create,
fake, or synthesize live security or provenance evidence.
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
required_true = [
    "mandatory_api_auth",
    "mandatory_metadata_auth",
    "tenant_authorization_verified",
    "tls_verified",
    "secret_rotation_verified",
    "body_limits_verified",
    "rate_limits_verified",
    "object_prefix_isolation_verified",
    "negative_cross_tenant_tests_passed",
    "clean_source_revision_verified",
    "exact_deployed_image_digests_verified",
    "sbom_attached",
    "dependency_policy_passed",
    "immutable_test_evidence_attached",
]
required_evidence_refs = [
    "api_auth_test",
    "metadata_auth_test",
    "tenant_authorization_test",
    "tls_attestation",
    "secret_rotation_test",
    "limit_tests",
    "object_prefix_isolation_test",
    "cross_tenant_negative_tests",
    "sbom",
    "dependency_policy",
    "immutable_test_evidence",
]
forbidden_tokens = {
    "127.0.0.1",
    "::1",
    "changeme",
    "dummy",
    "emulator",
    "example.com",
    "example.net",
    "example.org",
    "fake",
    "local-only",
    "local_only",
    "local smoke",
    "local_smoke",
    "localhost",
    "localstack",
    "lorem ipsum",
    "minio",
    "mock",
    "moto",
    "placeholder",
    "replace-me",
    "replace_me",
    "replace_with",
    "synthetic",
    "tbd",
    "todo",
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


if not isinstance(evidence, dict):
    errors.append("evidence must be a JSON object")
    observed_kind = None
else:
    observed_kind = evidence.get("evidence_kind")
    if observed_kind != "security_release_provenance":
        errors.append("evidence_kind must be security_release_provenance")
    if evidence.get("status") != "pass":
        errors.append("status must be pass")
    deployment_id = evidence.get("deployment_id")
    if not isinstance(deployment_id, str) or not deployment_id.strip():
        errors.append("deployment_id must be a non-empty string")
    authority_store_id = evidence.get("authority_store_id")
    if not isinstance(authority_store_id, str) or not authority_store_id.startswith("s3://"):
        errors.append("authority_store_id must be an s3:// URI")
    for field in required_true:
        if evidence.get(field) is not True:
            errors.append(f"{field} must be true")
    errors.extend(validate_release_identity_fields(evidence))
    validate_evidence_refs(evidence.get("evidence_refs"))
    for value in walk_strings(evidence):
        lower = value.lower()
        for token in sorted(forbidden_tokens):
            if token in lower:
                errors.append(f"evidence must not contain {token}")

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
            "evidence_kind": "security_release_provenance",
            "verified": required_true,
            "evidence_refs_verified": required_evidence_refs,
            "message": "security and release provenance evidence is valid",
        },
        sort_keys=True,
    )
)
PY

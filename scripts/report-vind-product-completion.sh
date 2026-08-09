#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
rest_api_smoke_evidence="${VELORIX_REST_API_SMOKE_EVIDENCE:-${product_dir}/rest-api-smoke.json}"
output_file="${VELORIX_PRODUCT_COMPLETION_REPORT:-${product_dir}/product-completion-report.json}"

usage() {
  cat <<'EOF'
Report the current product-complete status for an existing vind product slice.

Usage:
  scripts/report-vind-product-completion.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_VIND_PRODUCT_EVIDENCE=target/velorix-product/product-evidence.json
  VELORIX_REST_API_SMOKE_EVIDENCE=target/velorix-product/rest-api-smoke.json
  VELORIX_PRODUCT_COMPLETION_REPORT=target/velorix-product/product-completion-report.json
  VELORIX_UPGRADE_ROLLBACK_REPAIR_GC_FAULT_MATRIX_EVIDENCE_PATH=target/velorix-product/upgrade-rollback-repair-gc-fault-matrix.json

The script reads product-evidence.json and optional REST smoke evidence, writes a
target-backed JSON report, and prints the remaining gates and next commands. It
does not create product-complete evidence.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ "$#" -ne 0 ]; then
  usage >&2
  exit 64
fi

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

cd "$repo_root"
require python3

if [ ! -f "$product_evidence" ]; then
  mkdir -p "$(dirname "$output_file")"
  python3 - "$product_evidence" "$output_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

product_evidence, output_file = sys.argv[1:]
action = (
    "Run scripts/run-vind-product.sh or set VELORIX_VIND_PRODUCT_EVIDENCE "
    "to an existing product-evidence.json, then rerun "
    "scripts/report-vind-product-completion.sh"
)
payload = {
    "schema_version": 1,
    "report_kind": "velorix_product_completion_report",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_complete": False,
    "local_diagnostic_complete": False,
    "product_evidence": product_evidence,
    "product_complete_blockers": [
        {
            "gate": "product_evidence",
            "status": "missing",
            "summary": "Product evidence is required before completion can be assessed",
        }
    ],
    "gate_summary": {"missing": 1},
    "gates": [
        {
            "id": "product_evidence",
            "status": "missing",
            "summary": "Product evidence is required before completion can be assessed",
            "evidence": {"missing": product_evidence},
            "next_action": action,
        }
    ],
    "completion_plan": {
        "schema_version": 1,
        "derived_from": "missing_product_evidence",
        "complete": False,
        "local_diagnostic_complete": False,
        "steps": [
            {
                "id": "product_evidence",
                "state": "input_required",
                "summary": "Generate or provide product-evidence.json",
                "next_action": action,
            }
        ],
        "runnable_steps": [],
        "input_required_steps": ["product_evidence"],
        "waiting_steps": [],
        "deferred_steps": [],
    },
    "product_completion_source": {
        "derived_from": "missing_product_evidence",
        "product_evidence_product_complete": False,
        "product_evidence_product_complete_blockers": ["missing product evidence"],
        "product_accepted_gate_statuses": ["pass"],
        "local_diagnostic_accepted_gate_statuses": ["pass", "out_of_scope"],
        "all_product_gates_pass": False,
        "all_local_diagnostic_gates_pass_or_out_of_scope": False,
    },
    "next_actions": [action],
}
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
print("product_complete=false")
print(f"product_evidence={product_evidence}")
print(f"report={output_file}")
print("gate_summary=missing:1")
print("blockers:")
print("- product_evidence[missing]: Product evidence is required before completion can be assessed")
print("next_actions:")
print(f"- {action}")
PY
  exit 0
fi

mkdir -p "$(dirname "$output_file")"

python3 - "$product_evidence" "$rest_api_smoke_evidence" "$output_file" "$product_dir" "$repo_root" <<'PY'
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

product_path, rest_smoke_path, output_path, product_dir, repo_root = sys.argv[1:]


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_optional_json(path):
    if not os.path.exists(path):
        return None
    try:
        value = load_json(path)
    except Exception:
        return None
    return value if isinstance(value, dict) else None


def pointer(value, path):
    current = value
    for raw_part in path.strip("/").split("/"):
        if raw_part == "":
            continue
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            current = current[index] if index < len(current) else None
        else:
            return None
    return current


def gate(gate_id, status, summary, evidence=None, next_action=None, blocked_by=None):
    payload = {
        "id": gate_id,
        "status": status,
        "summary": summary,
    }
    if evidence:
        payload["evidence"] = evidence
    if next_action:
        payload["next_action"] = next_action
    if blocked_by:
        payload["blocked_by"] = blocked_by
    return payload


def command(text):
    return text.replace("target/velorix-product", product_dir)


RELEASE_IMAGE_DIGEST_ENVS = {
    "velorix-api": "VELORIX_API_IMAGE_DIGEST",
    "velorix-meta": "VELORIX_META_IMAGE_DIGEST",
}


def release_identity_subject(payload):
    if not isinstance(payload, dict):
        return {}
    for field in ("s3_compatible_test_status", "gc_status"):
        nested = payload.get(field)
        if isinstance(nested, dict):
            return nested
    return payload


def release_identity_binding_errors(payload, label):
    subject = release_identity_subject(payload)
    errors = []
    source_revision = subject.get("source_revision")
    if not release_commit:
        errors.append("VELORIX_RELEASE_COMMIT is required for release evidence binding")
    elif source_revision != release_commit:
        errors.append(f"{label}.source_revision must match VELORIX_RELEASE_COMMIT")

    image_digests = subject.get("deployed_image_digests")
    if not isinstance(image_digests, dict):
        errors.append(f"{label}.deployed_image_digests must be an object")
        image_digests = {}

    for role, env_name in RELEASE_IMAGE_DIGEST_ENVS.items():
        observed = image_digests.get(role)
        product_digest = ((deployed_images.get(role) or {}).get("image_digest"))
        env_digest = os.environ.get(env_name, "").strip()
        if not observed:
            errors.append(f"{label}.deployed_image_digests.{role} is required")
            continue
        if product_digest and observed != product_digest:
            errors.append(
                f"{label}.deployed_image_digests.{role} must match "
                f"product.deployed_images.{role}.image_digest"
            )
        if not product_digest:
            errors.append(f"product.deployed_images.{role}.image_digest is required")
        if env_digest and observed != env_digest:
            errors.append(f"{label}.deployed_image_digests.{role} must match {env_name}")
    return errors


def bind_release_identity(summary, evidence_path, label):
    if summary.get("status") != "pass":
        return False
    payload = load_optional_json(evidence_path) or {}
    errors = release_identity_binding_errors(payload, label)
    if errors:
        summary["status"] = "blocked"
        summary.setdefault("errors", []).extend(errors)
        summary["release_identity_binding"] = "blocked"
        return False
    summary["release_identity_binding"] = "verified"
    return True


PLACEHOLDER_MARKERS = (
    "REPLACE_WITH",
    "PUBLIC_HOST.",
    "INGRESS_CONTROLLER",
    "TLS_SECRET_NAME",
    "S3_OR_OSS_ENDPOINT",
)


def has_placeholder(text):
    return bool(text) and any(marker in text for marker in PLACEHOLDER_MARKERS)


DURABILITY_REVIEW_FIELDS = [
    "versioning_or_object_lock_enabled",
    "server_side_encryption_enabled",
    "backup_or_replication_configured",
    "lifecycle_delete_policy_reviewed",
    "destructive_delete_protection_reviewed",
    "cost_controls_reviewed",
]


def subjects_from_issues(items):
    return sorted(
        {
            item.get("subject")
            for item in items or []
            if isinstance(item, dict) and item.get("subject")
        }
    )


product = load_json(product_path)
rest_smoke = load_json(rest_smoke_path) if os.path.exists(rest_smoke_path) else None
deployed_images = product.get("deployed_images") or {}
release_preflight_path = os.path.join(
    product_dir, "hiqlite-backend-time-release-preflight.json"
)
release_env_report_path = os.path.join(
    product_dir, "hiqlite-backend-time-release-env.json"
)
input_preflight_path = os.path.join(
    product_dir, "complete-vind-product-input-preflight.json"
)
complete_env_report_path = os.path.join(
    product_dir, "complete-vind-product-env.json"
)
complete_execution_plan_path = os.path.join(
    product_dir, "complete-vind-product-plan.json"
)
staged_durability_attestation_path = os.path.join(
    product_dir, "object-store-durability-attestation.json"
)
staged_durability_input_path = os.path.join(
    product_dir, "object-store-durability-input.json"
)
s3_checkpoint_fault_matrix_path = os.environ.get(
    "VELORIX_S3_CHECKPOINT_FAULT_MATRIX_EVIDENCE_PATH",
    os.path.join(product_dir, "s3-checkpoint-fault-matrix.json"),
)
hiqlite_restore_drill_path = os.environ.get(
    "VELORIX_HIQLITE_RESTORE_DRILL_EVIDENCE_PATH",
    os.path.join(product_dir, "hiqlite-restore-drill.json"),
)
upgrade_repair_gc_fault_matrix_path = os.environ.get(
    "VELORIX_UPGRADE_ROLLBACK_REPAIR_GC_FAULT_MATRIX_EVIDENCE_PATH",
    os.path.join(product_dir, "upgrade-rollback-repair-gc-fault-matrix.json"),
)
query_output_isolation_path = os.environ.get(
    "VELORIX_QUERY_OUTPUT_ISOLATION_EVIDENCE_PATH",
    os.path.join(product_dir, "query-output-isolation.json"),
)
security_release_provenance_path = os.environ.get(
    "VELORIX_SECURITY_RELEASE_PROVENANCE_EVIDENCE_PATH",
    os.path.join(product_dir, "security-release-provenance.json"),
)
release_commit = os.environ.get("VELORIX_RELEASE_COMMIT", "").strip()
remaining_release_readiness_path = os.environ.get(
    "VELORIX_REMAINING_RELEASE_READINESS_EVIDENCE_PATH",
    os.path.join(product_dir, "remaining-release-readiness.json"),
)
release_preflight = load_optional_json(release_preflight_path)
release_env_report = load_optional_json(release_env_report_path)
input_preflight = load_optional_json(input_preflight_path)
complete_env_report = load_optional_json(complete_env_report_path)
complete_execution_plan = load_optional_json(complete_execution_plan_path)
staged_durability_attestation = load_optional_json(staged_durability_attestation_path)
staged_durability_input = load_optional_json(staged_durability_input_path)
product_evidence_blockers = product.get("product_complete_blockers") or []
product_evidence_product_complete = product.get("product_complete") is True
external_s3_required = os.environ.get("VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3", "0") == "1"
public_ingress_required = os.environ.get("VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS", "0") == "1"
hiqlite_release_required = os.environ.get("VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE", "0") == "1"
completion_scope_warnings = []
if not public_ingress_required:
    completion_scope_warnings.append(
        "public_ingress_tls_auth_out_of_scope_does_not_prove_public_dns_tls_or_external_client_reachability"
    )
if not external_s3_required:
    completion_scope_warnings.append(
        "object_store_external_authority_out_of_scope_does_not_prove_object_store_durability"
    )
if not hiqlite_release_required:
    completion_scope_warnings.append(
        "hiqlite_backend_time_release_out_of_scope_does_not_prove_sigstore_ci_release_provenance"
    )
gates = []
next_actions = []
handoff_action = command(
    "scripts/write-complete-vind-product-env.sh "
    "--product-evidence target/velorix-product/product-evidence.json && "
    "Replace placeholders in target/velorix-product/complete-vind-product.env, "
    "then VELORIX_COMPLETE_PRODUCT_DRY_RUN=1 scripts/complete-vind-product.sh "
    "--env-file target/velorix-product/complete-vind-product.env && "
    "scripts/complete-vind-product.sh "
    "--env-file target/velorix-product/complete-vind-product.env"
)

architecture_critique_path = os.path.join(repo_root, "docs", "architecture-critique.md")
if os.path.exists(architecture_critique_path):
    with open(architecture_critique_path, "r", encoding="utf-8") as f:
        architecture_critique = f.read()
else:
    architecture_critique = ""
architecture_critique_blocks_1_0 = (
    ("Block 1.0" in architecture_critique and "Top 10 risks" in architecture_critique)
    or "still blocks release readiness" in architecture_critique
)
gates.append(
    gate(
        "architecture_critique_blockers",
        "blocked" if architecture_critique_blocks_1_0 else "pass",
        "Architecture critique blockers must be resolved before product-complete",
        evidence={"architecture_critique": architecture_critique_path},
        next_action="Resolve docs/architecture-critique.md release-readiness blockers and remove the blocking verdict"
        if architecture_critique_blocks_1_0
        else None,
    )
)


def validator_summary(script_name, evidence_path):
    script = os.path.join(repo_root, "scripts", script_name)
    if not os.path.exists(evidence_path):
        return {
            "status": "missing",
            "evidence": evidence_path,
            "errors": [f"missing evidence: {evidence_path}"],
        }
    result = subprocess.run(
        [script, evidence_path],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    raw = result.stdout.strip() or result.stderr.strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {"raw_output": raw}
    payload["validator"] = script_name
    payload["evidence"] = evidence_path
    payload["exit_code"] = result.returncode
    payload["status"] = "pass" if result.returncode == 0 else payload.get("status", "blocked")
    return payload


rest_api_pass = (
    product.get("rest_callable") is True
    and pointer(product, "/api/openapi/catalog_smoke_passed") is True
    and pointer(product, "/api/query_policy/catalog_smoke_passed") is True
)
gates.append(
    gate(
        "rest_api_serving",
        "pass" if rest_api_pass else "blocked",
        "REST serving, promoted API catalog, query policy catalog, and generic query default policy",
        evidence={
            "product_evidence": product_path,
            "promoted_api_path": pointer(product, "/api/openapi/promoted_api_path"),
        },
    )
)

if rest_smoke is None:
    action = command(
        "VELORIX_VIND_PRODUCT_DIR=target/velorix-product scripts/smoke-vind-rest-api.sh"
    )
    gates.append(
        gate(
            "rest_api_live_smoke",
            "missing",
            "Repeatable live REST smoke evidence has not been generated",
            next_action=action,
        )
    )
    next_actions.append(action)
else:
    rest_smoke_pass = (
        rest_smoke.get("status") == "pass"
        and rest_smoke.get("ingested_positive_sum") == 25
        and rest_smoke.get("ingested_positive_count") == 2
    )
    gates.append(
        gate(
            "rest_api_live_smoke",
            "pass" if rest_smoke_pass else "blocked",
            "Live REST smoke for relation admission, ingest, view query, promoted API, OpenAPI, and owner route",
            evidence={"rest_api_smoke": rest_smoke_path},
            next_action=None
            if rest_smoke_pass
            else command(
                "VELORIX_VIND_PRODUCT_DIR=target/velorix-product scripts/smoke-vind-rest-api.sh"
            ),
        )
    )

api_auth_pass = (
    pointer(product, "/api/auth/mode") == "bearer-token"
    and pointer(product, "/api/auth/missing_token_rejected") is True
    and pointer(product, "/api/auth/wrong_token_rejected") is True
    and pointer(product, "/api/auth/data_plane_token_rejected_on_admin_route") is True
)
ingress = pointer(product, "/api/auth/ingress_tls_auth_attestation")
local_tls = pointer(product, "/api/auth/local_tls_auth_smoke") or {}
tls_auth_boundary_pass = (
    api_auth_pass
    and local_tls.get("enabled") is True
    and local_tls.get("passed") is True
    and local_tls.get("public_ingress_attestation") is False
    and local_tls.get("trusted_for_product_complete") is False
)
gates.append(
    gate(
        "tls_auth_boundary",
        "pass" if tls_auth_boundary_pass else "blocked",
        "Local product TLS/auth boundary smoke for REST access",
        evidence={
            "api_auth_passed": api_auth_pass,
            "local_tls_scope": local_tls.get("scope"),
            "local_tls_evidence": local_tls.get("evidence"),
            "local_tls_trusted_for_product_complete": local_tls.get(
                "trusted_for_product_complete"
            ),
        },
        next_action=None
        if tls_auth_boundary_pass
        else command(
            "VELORIX_VIND_PRODUCT_DIR=target/velorix-product "
            "scripts/smoke-vind-rest-api.sh"
        ),
    )
)

if not public_ingress_required:
    action = command(
        "scripts/complete-vind-product-ingress.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product "
        "--validate-only && "
        "scripts/complete-vind-product-ingress.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product"
    )
    gates.append(
        gate(
            "public_ingress_tls_auth",
            "out_of_scope",
            "Public/enterprise ingress, DNS, and TLS auth are excluded from the current product-complete goal",
            evidence={
                "public_ingress_required": public_ingress_required,
                "local_tls_scope": local_tls.get("scope"),
                "local_tls_evidence": local_tls.get("evidence"),
                "ingress_tls_auth_attestation": None
                if not ingress
                else ingress.get("evidence"),
            },
            next_action=action,
        )
    )
elif ingress:
    ingress_pass = (
        ingress.get("public_ingress_attestation") is True
        and ingress.get("trusted_for_product_complete") is True
    )
    gates.append(
        gate(
            "public_ingress_tls_auth",
            "pass" if ingress_pass else "diagnostic",
            "External ingress/TLS/auth product-complete attestation",
            evidence={"ingress_tls_auth_attestation": ingress.get("evidence")},
        )
    )
else:
    action = command(
        "scripts/complete-vind-product-ingress.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product "
        "--validate-only && "
        "scripts/complete-vind-product-ingress.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product"
    )
    gates.append(
        gate(
            "public_ingress_tls_auth",
            "blocked",
            "Bearer-token API auth is present, but public ingress/TLS/auth attestation is missing",
            evidence={
                "api_auth_passed": api_auth_pass,
                "local_tls_scope": local_tls.get("scope"),
                "local_tls_trusted_for_product_complete": local_tls.get(
                    "trusted_for_product_complete"
                ),
            },
            next_action=action,
        )
    )
    next_actions.append(action)

store = product.get("object_store") or {}


def durability_attestation_issues():
    attestation = store.get("durability_policy_attestation")
    issues = []
    if not isinstance(attestation, dict):
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation",
                "detail": "object_store.durability_policy_attestation is required",
            }
        )
        return issues
    if attestation.get("validated") is not True:
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation.validated",
                "detail": "object_store.durability_policy_attestation.validated must be true",
            }
        )
    if attestation.get("schema_version") != 1:
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation.schema_version",
                "detail": "object_store.durability_policy_attestation.schema_version must be 1",
            }
        )
    if (
        attestation.get("evidence_kind")
        != "velorix_object_store_durability_policy_attestation"
    ):
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation.evidence_kind",
                "detail": "object_store.durability_policy_attestation.evidence_kind must be velorix_object_store_durability_policy_attestation",
            }
        )
    for field in ["authority_store_id", "bucket", "s3_prefix"]:
        if attestation.get(field) != store.get(field):
            issues.append(
                {
                    "subject": f"object_store.durability_policy_attestation.{field}",
                    "detail": f"object_store.durability_policy_attestation.{field} must match object_store.{field}",
                }
            )
    for field in DURABILITY_REVIEW_FIELDS:
        if attestation.get(field) is not True:
            issues.append(
                {
                    "subject": f"object_store.durability_policy_attestation.{field}",
                    "detail": f"object_store.durability_policy_attestation.{field} must be true",
                }
            )
    return issues


def staged_durability_attestation_summary():
    attestation = staged_durability_attestation
    if not isinstance(attestation, dict):
        return None
    if (
        attestation.get("evidence_kind")
        != "velorix_object_store_durability_policy_attestation"
    ):
        return {
            "evidence": "object-store-durability-attestation.json",
            "status": "invalid_evidence_kind",
            "creates_product_complete_evidence": False,
        }
    review_flags = {
        field: attestation.get(field) is True for field in DURABILITY_REVIEW_FIELDS
    }
    comparable_fields = [
        field for field in ["authority_store_id", "bucket", "s3_prefix"] if store.get(field)
    ]
    matches_current_authority = bool(comparable_fields) and all(
        attestation.get(field) == store.get(field) for field in comparable_fields
    )
    return {
        "evidence": "object-store-durability-attestation.json",
        "status": "review_ready" if all(review_flags.values()) else "review_incomplete",
        "authority_store_id": attestation.get("authority_store_id"),
        "bucket": attestation.get("bucket"),
        "s3_prefix": attestation.get("s3_prefix"),
        "provider_kind": attestation.get("provider_kind"),
        "review_flags": review_flags,
        "review_ready": all(review_flags.values()),
        "matches_current_authority": matches_current_authority,
        "requires_external_authority_before_attach": True,
        "creates_product_complete_evidence": False,
    }


def staged_durability_input_summary():
    value = staged_durability_input
    if not isinstance(value, dict):
        return None
    if value.get("evidence_kind") != "velorix_object_store_durability_input":
        return {
            "evidence": "object-store-durability-input.json",
            "status": "invalid_evidence_kind",
            "creates_product_complete_evidence": False,
        }
    return {
        "evidence": "object-store-durability-input.json",
        "status": value.get("status"),
        "authority_ready": value.get("authority_ready"),
        "review_flags": value.get("review_flags") or {},
        "missing_subjects": subjects_from_issues(value.get("missing") or []),
        "invalid_subjects": subjects_from_issues(value.get("invalid") or []),
        "creates_product_complete_evidence": False,
    }


object_store_external = (
    store.get("mode") == "external-s3"
    and store.get("external_s3_bucket_validated") is True
    and store.get("external_s3_prefix_validated") is True
)
object_store_real_authority = (
    object_store_external and store.get("local_development_authority") is not True
)
if not external_s3_required:
    external_s3_action = command(
        "scripts/run-vind-product-external-s3.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product "
        "--validate-only && "
        "scripts/run-vind-product-external-s3.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product"
    )
    durability_action = command(
        "Enable VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3=1, prove "
        "object_store_external_authority, then run "
        "scripts/complete-vind-object-store-durability.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product --validate-only && "
        "scripts/complete-vind-object-store-durability.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product"
    )
    gates.append(
        gate(
            "object_store_external_authority",
            "out_of_scope",
            "Actual external S3/OSS authority is excluded from the current product-complete goal",
            evidence={
                "mode": store.get("mode"),
                "authority_store_id": store.get("authority_store_id"),
                "external_s3_required": external_s3_required,
            },
            next_action=external_s3_action,
        )
    )
    gates.append(
        gate(
            "object_store_durability_policy",
            "out_of_scope",
            "Object-store durability attestation is excluded with the actual external S3/OSS authority goal",
            evidence={
                "mode": store.get("mode"),
                "authority_store_id": store.get("authority_store_id"),
                "external_s3_required": external_s3_required,
                "staged_attestation": staged_durability_attestation_summary(),
                "staged_input": staged_durability_input_summary(),
            },
            next_action=durability_action,
            blocked_by=["object_store_external_authority"],
        )
    )
elif not object_store_external:
    action = command(
        "scripts/run-vind-product-external-s3.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product "
        "--validate-only && "
        "scripts/run-vind-product-external-s3.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product"
    )
    gates.append(
        gate(
            "object_store_external_authority",
            "blocked",
            "Product slice is not backed by validated external S3-compatible storage",
            evidence={"mode": store.get("mode")},
            next_action=action,
        )
    )
    next_actions.append(action)
elif not object_store_real_authority:
    action = command(
        "scripts/run-vind-product-external-s3.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product "
        "--validate-only && "
        "scripts/run-vind-product-external-s3.sh "
        "--env-file target/velorix-product/complete-vind-product.env "
        "--output-dir target/velorix-product"
    )
    gates.append(
        gate(
            "object_store_external_authority",
            "blocked",
            "External S3-compatible shape is validated, but the authority is local development RustFS",
            evidence={"authority_store_id": store.get("authority_store_id")},
            next_action=action,
        )
    )
    next_actions.append(action)
else:
    gates.append(
        gate(
            "object_store_external_authority",
            "pass",
            "Nonlocal external S3-compatible authority is validated from inside the cluster",
            evidence={"authority_store_id": store.get("authority_store_id")},
        )
    )

durability = store.get("durability_policy_attestation")
durability_invalid = durability_attestation_issues()
if external_s3_required and object_store_real_authority and durability and not durability_invalid:
    gates.append(
        gate(
            "object_store_durability_policy",
            "pass",
            "Operator-reviewed object-store durability policy attestation is attached",
            evidence={"durability_policy_attestation": durability.get("evidence")},
        )
    )
elif external_s3_required:
    if object_store_real_authority:
        action = command(
            "scripts/complete-vind-object-store-durability.sh "
            "--env-file target/velorix-product/complete-vind-product.env "
            "--output-dir target/velorix-product "
            "--validate-only && "
            "scripts/complete-vind-object-store-durability.sh "
            "--env-file target/velorix-product/complete-vind-product.env "
            "--output-dir target/velorix-product"
        )
        if durability and durability_invalid:
            summary = "Attached object-store durability policy attestation does not match the external authority or required review flags"
        else:
            summary = "Product-complete durability policy attestation is missing for the external authority"
    else:
        action = "Durability attestation is only accepted after the product slice is backed by a nonlocal external S3/OSS authority"
        summary = "Product-complete durability policy attestation cannot be trusted before external object-store authority is proven"
    gates.append(
        gate(
            "object_store_durability_policy",
            "blocked",
            summary,
            evidence={
                "authority_store_id": store.get("authority_store_id"),
                "mode": store.get("mode"),
                "local_development_authority": store.get("local_development_authority"),
                "durability_policy_attestation": durability,
                "durability_policy_attestation_invalid": durability_invalid,
                "durability_policy_attestation_invalid_subjects": subjects_from_issues(
                    durability_invalid
                ),
                "staged_attestation": staged_durability_attestation_summary(),
                "staged_input": staged_durability_input_summary(),
            },
            next_action=action,
            blocked_by=None
            if object_store_real_authority
            else ["object_store_external_authority"],
        )
    )
    if object_store_real_authority:
        next_actions.append(action)

s3_fault_matrix = validator_summary(
    "check-s3-checkpoint-fault-matrix-evidence.sh",
    s3_checkpoint_fault_matrix_path,
)
s3_fault_matrix_pass = bind_release_identity(
    s3_fault_matrix,
    s3_checkpoint_fault_matrix_path,
    "s3_checkpoint_fault_matrix",
)
s3_fault_matrix_action = command(
    "Produce the six live S3 object-store fault scenario evidence files under "
    "target/velorix-product/s3-checkpoint-fault-matrix-scenarios, then run "
    "VELORIX_S3_COMPAT=1 VELORIX_PRODUCT_DEPLOYMENT_ID=REPLACE_WITH_DEPLOYMENT_ID "
    "VELORIX_AUTHORITY_STORE_ID=s3://REPLACE_WITH_BUCKET/REPLACE_WITH_PREFIX "
    "VELORIX_RELEASE_COMMIT=REPLACE_WITH_RELEASE_COMMIT "
    "VELORIX_API_IMAGE_DIGEST=sha256:REPLACE_WITH_API_DIGEST "
    "VELORIX_META_IMAGE_DIGEST=sha256:REPLACE_WITH_META_DIGEST "
    "scripts/run-s3-checkpoint-fault-matrix.sh"
)
gates.append(
    gate(
        "s3_checkpoint_fault_matrix",
        "pass" if s3_fault_matrix_pass else s3_fault_matrix.get("status", "blocked"),
        "Live S3-compatible checkpoint fault matrix proving no mixed checkpoint publication",
        evidence=s3_fault_matrix,
        next_action=None if s3_fault_matrix_pass else s3_fault_matrix_action,
        blocked_by=None
        if object_store_real_authority
        else ["object_store_external_authority"],
    )
)
if not s3_fault_matrix_pass and object_store_real_authority:
    next_actions.append(s3_fault_matrix_action)

backend_time = pointer(product, "/metadata_store/hiqlite_backend_time_attestation")
backend_assessment = pointer(product, "/metadata_store/hiqlite_backend_time_assessment") or {}
backend_time_boundary_pass = (
    backend_assessment.get("validated") is True
    and backend_assessment.get("backend_time_source_kind")
    == "raft_replicated_authority_time"
    and backend_assessment.get("bounded_wall_clock_failover") is True
    and backend_assessment.get("can_generate_product_complete_backend_time_attestation")
    is True
    and isinstance(backend_time, dict)
    and backend_time.get("validated") is True
    and backend_time.get("authoritative_backend_time") is True
    and backend_time.get("time_source_kind") == "raft_replicated_authority_time"
    and backend_time.get("bounded_wall_clock_failover") is True
    and backend_time.get("release_validator_fail_closed") is True
    and backend_time.get("trusted_for_release_validator") is False
    and backend_time.get("trusted_for_product_complete") is False
)
gates.append(
    gate(
        "hiqlite_backend_time_boundary",
        "pass" if backend_time_boundary_pass else "blocked",
        "Local Hiqlite replicated backend-time boundary for owner TTL and failover",
        evidence={
            "assessment": backend_assessment.get("evidence"),
            "attestation": None if backend_time is None else backend_time.get("evidence"),
            "backend_time_source_kind": backend_assessment.get(
                "backend_time_source_kind"
            ),
            "attestation_origin": None
            if backend_time is None
            else backend_time.get("attestation_origin"),
            "source_kind": None if backend_time is None else backend_time.get("source_kind"),
            "release_validator_fail_closed": None
            if backend_time is None
            else backend_time.get("release_validator_fail_closed"),
            "trusted_for_release_validator": None
            if backend_time is None
            else backend_time.get("trusted_for_release_validator"),
        },
        next_action=None
        if backend_time_boundary_pass
        else command(
            "scripts/attest-hiqlite-backend-time.sh "
            "--product-evidence target/velorix-product/product-evidence.json "
            "--output target/velorix-product/hiqlite-backend-time-attestation.json "
            "--update-product-evidence"
        ),
    )
)
if not hiqlite_release_required:
    action = command(
        "scripts/write-hiqlite-backend-time-release-env.sh "
        "--product-evidence target/velorix-product/product-evidence.json && "
        "Replace every REPLACE_WITH_* value in "
        "target/velorix-product/hiqlite-backend-time-release.env, then "
        "scripts/check-hiqlite-backend-time-release-inputs.sh "
        "--env-file target/velorix-product/hiqlite-backend-time-release.env "
        "--product-evidence target/velorix-product/product-evidence.json && "
        "Regenerate Hiqlite backend-time attestation in release CI with "
        "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1"
    )
    gates.append(
        gate(
            "hiqlite_backend_time_release",
            "out_of_scope",
            "Trusted Hiqlite backend-time release CI/Sigstore provenance is excluded from the current product-complete goal",
            evidence={
                "hiqlite_release_required": hiqlite_release_required,
                "diagnostic_attestation": None
                if backend_time is None
                else backend_time.get("evidence"),
                "release_validator_fail_closed": None
                if backend_time is None
                else backend_time.get("release_validator_fail_closed"),
            },
            next_action=action,
        )
    )
elif backend_time and backend_time.get("trusted_for_product_complete") is True:
    gates.append(
        gate(
            "hiqlite_backend_time_release",
            "pass",
            "Trusted Hiqlite backend-time release attestation is attached",
            evidence={"hiqlite_backend_time_attestation": backend_time.get("evidence")},
        )
    )
else:
    if backend_time:
        action = command(
            "scripts/write-hiqlite-backend-time-release-env.sh "
            "--product-evidence target/velorix-product/product-evidence.json && "
            "Replace every REPLACE_WITH_* value in "
            "target/velorix-product/hiqlite-backend-time-release.env, then "
            "scripts/check-hiqlite-backend-time-release-inputs.sh "
            "--env-file target/velorix-product/hiqlite-backend-time-release.env "
            "--product-evidence target/velorix-product/product-evidence.json && "
            "Regenerate Hiqlite backend-time attestation in release CI with "
            "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1, trusted "
            "release wall-clock failover evidence, deployed image digests, "
            "clean source revision, and signing/sigstore provenance; the "
            "current local diagnostic attestation cannot pass release validation"
        )
    else:
        action = command(
            "scripts/attest-hiqlite-backend-time.sh "
            "--product-evidence target/velorix-product/product-evidence.json "
            "--output target/velorix-product/hiqlite-backend-time-attestation.json "
            "--update-product-evidence"
        )
    status = (
        "diagnostic"
        if backend_assessment.get("can_generate_product_complete_backend_time_attestation")
        is True
        else "blocked"
    )
    release_evidence = {}
    if release_preflight:
        missing = release_preflight.get("missing") or []
        invalid = release_preflight.get("invalid") or []
        release_evidence["preflight"] = {
            "evidence": "hiqlite-backend-time-release-preflight.json",
            "status": release_preflight.get("status"),
            "missing_count": len(missing),
            "invalid_count": len(invalid),
            "missing_subjects": [
                item.get("subject") for item in missing if isinstance(item, dict)
            ],
            "invalid_subjects": [
                item.get("subject") for item in invalid if isinstance(item, dict)
            ],
        }
    if release_env_report:
        release_evidence["env_template"] = {
            "evidence": "hiqlite-backend-time-release-env.json",
            "placeholder_count": len(release_env_report.get("placeholders") or []),
            "placeholders": release_env_report.get("placeholders") or [],
            "derived_from_product_evidence": release_env_report.get(
                "derived_from_product_evidence"
            )
            or [],
        }
    gates.append(
        gate(
            "hiqlite_backend_time_release",
            status,
            "Hiqlite backend-time capability is diagnostic until trusted release provenance and product-complete failover evidence are attached",
            evidence={
                "assessment": backend_assessment.get("evidence"),
                "assessment_can_generate": backend_assessment.get(
                    "can_generate_product_complete_backend_time_attestation"
                ),
                "attestation": None
                if backend_time is None
                else backend_time.get("evidence"),
                "trusted_for_product_complete": None
                if backend_time is None
                else backend_time.get("trusted_for_product_complete"),
                "trusted_for_release_validator": None
                if backend_time is None
                else backend_time.get("trusted_for_release_validator"),
                "release_validator_fail_closed": None
                if backend_time is None
                else backend_time.get("release_validator_fail_closed"),
                **release_evidence,
            },
            next_action=action,
        )
    )
    next_actions.append(action)

standing = product.get("standing_runtime_fencing") or {}
standing_pass = (
    standing.get("required_mode") is True
    and standing.get("multi_writer_fencing_safe") is True
    and standing.get("production_bounded_failover_safe") is True
    and pointer(product, "/metadata_store/standing_runtime_adversarial_smoke/status")
    == "pass"
    and pointer(standing, "/multi_replica_fencing_smoke/status") == "pass"
)
gates.append(
    gate(
        "standing_runtime_fencing",
        "pass" if standing_pass else "blocked",
        "Required-mode standing runtime fencing and adversarial deployed smoke",
        evidence={
            "configured_mode": standing.get("configured_mode"),
            "multi_replica_fencing_smoke": pointer(
                standing, "/multi_replica_fencing_smoke/status"
            ),
        },
    )
)

no_pvc_pass = pointer(product, "/no_pvc/namespace_validated") is True
gates.append(
    gate(
        "no_pvc",
        "pass" if no_pvc_pass else "blocked",
        "No PersistentVolumeClaim objects in the product namespace",
        evidence={"no_pvc": pointer(product, "/no_pvc/evidence")},
    )
)

hiqlite_restore_drill = validator_summary(
    "check-hiqlite-restore-drill-evidence.sh",
    hiqlite_restore_drill_path,
)
hiqlite_restore_drill_pass = bind_release_identity(
    hiqlite_restore_drill,
    hiqlite_restore_drill_path,
    "hiqlite_total_voter_loss_restore_drill",
)
hiqlite_restore_drill_action = command(
    "Destroy every Hiqlite voter and node-local disk after an acknowledged "
    "materialized ingest, restore only from object-store backup, write "
    "target/velorix-product/hiqlite-restore-drill.json with deployment_id, "
    "s3:// authority_store_id, evidence_kind=hiqlite_total_voter_loss_restore_drill, "
    "source_revision, deployed_image_digests, "
    "and evidence_refs for object_store_backup, total_voter_loss_log, restore_log, "
    "metadata_write_survival, and post_restore_ingest_query; then validate with "
    "scripts/check-hiqlite-restore-drill-evidence.sh "
    "target/velorix-product/hiqlite-restore-drill.json"
)
gates.append(
    gate(
        "hiqlite_total_voter_loss_restore_drill",
        "pass"
        if hiqlite_restore_drill_pass
        else hiqlite_restore_drill.get("status", "blocked"),
        "Live no-PVC Hiqlite total-voter-loss restore drill preserving acknowledged metadata writes",
        evidence=hiqlite_restore_drill,
        next_action=None if hiqlite_restore_drill_pass else hiqlite_restore_drill_action,
        blocked_by=None if no_pvc_pass else ["no_pvc"],
    )
)
if not hiqlite_restore_drill_pass and no_pvc_pass:
    next_actions.append(hiqlite_restore_drill_action)

upgrade_repair_gc_fault_matrix = validator_summary(
    "check-upgrade-rollback-repair-gc-fault-matrix-evidence.sh",
    upgrade_repair_gc_fault_matrix_path,
)
upgrade_repair_gc_fault_matrix_pass = bind_release_identity(
    upgrade_repair_gc_fault_matrix,
    upgrade_repair_gc_fault_matrix_path,
    "upgrade_rollback_repair_gc_fault_matrix",
)
upgrade_repair_gc_fault_matrix_action = command(
    "Run the live upgrade, rollback, repair, and GC fault matrix, write "
    "target/velorix-product/upgrade-rollback-repair-gc-fault-matrix.json "
    "with deployment_id, s3:// authority_store_id, evidence_kind=upgrade_rollback_repair_gc_fault_matrix, "
    "source_revision, deployed_image_digests, "
    "and scenarios rolling_upgrade, rollback_after_upgrade, corrupt_latest_checkpoint_repair, "
    "gc_concurrent_with_query, gc_concurrent_with_compaction, gc_concurrent_with_recovery, "
    "gc_concurrent_with_checkpoint_publication, and gc_retains_repair_roots; then validate with "
    "scripts/check-upgrade-rollback-repair-gc-fault-matrix-evidence.sh "
    "target/velorix-product/upgrade-rollback-repair-gc-fault-matrix.json"
)
gates.append(
    gate(
        "upgrade_rollback_repair_gc_fault_matrix",
        "pass"
        if upgrade_repair_gc_fault_matrix_pass
        else upgrade_repair_gc_fault_matrix.get("status", "blocked"),
        "Live upgrade, rollback, repair, and GC fault matrix preserving acknowledged data and reachability roots",
        evidence=upgrade_repair_gc_fault_matrix,
        next_action=None
        if upgrade_repair_gc_fault_matrix_pass
        else upgrade_repair_gc_fault_matrix_action,
    )
)
if not upgrade_repair_gc_fault_matrix_pass:
    next_actions.append(upgrade_repair_gc_fault_matrix_action)

query_output_isolation = validator_summary(
    "check-query-output-isolation-evidence.sh",
    query_output_isolation_path,
)
query_output_isolation_pass = bind_release_identity(
    query_output_isolation,
    query_output_isolation_path,
    "query_output_isolation",
)
query_output_isolation_action = command(
    "Run the live query output-isolation drill, write "
    "target/velorix-product/query-output-isolation.json with deployment_id, "
    "s3:// authority_store_id, evidence_kind=query_output_isolation, "
    "source_revision, deployed_image_digests, and "
    "evidence_refs for query_pod_iam_policy, cold_query_log, object_store_audit_log, "
    "and materialized_output_read; then validate with "
    "scripts/check-query-output-isolation-evidence.sh "
    "target/velorix-product/query-output-isolation.json"
)
gates.append(
    gate(
        "query_output_isolation",
        "pass"
        if query_output_isolation_pass
        else query_output_isolation.get("status", "blocked"),
        "Live query pods read published materialized output only, with no source-prefix reads or durable writes",
        evidence=query_output_isolation,
        next_action=None if query_output_isolation_pass else query_output_isolation_action,
    )
)
if not query_output_isolation_pass:
    next_actions.append(query_output_isolation_action)

security_release_provenance = validator_summary(
    "check-security-release-provenance-evidence.sh",
    security_release_provenance_path,
)
security_release_provenance_pass = bind_release_identity(
    security_release_provenance,
    security_release_provenance_path,
    "security_release_provenance",
)
security_release_provenance_action = command(
    "Run the live security and release provenance checks, write "
    "target/velorix-product/security-release-provenance.json with deployment_id, "
    "s3:// authority_store_id, evidence_kind=security_release_provenance, source_revision, "
    "deployed_image_digests, and evidence_refs for auth, TLS, secret rotation, limits, "
    "object-prefix isolation, cross-tenant negatives, SBOM, dependency policy, and "
    "immutable test evidence; then validate with "
    "scripts/check-security-release-provenance-evidence.sh "
    "target/velorix-product/security-release-provenance.json"
)
gates.append(
    gate(
        "security_release_provenance",
        "pass"
        if security_release_provenance_pass
        else security_release_provenance.get("status", "blocked"),
        "Live security controls and release provenance evidence for auth, tenant isolation, TLS, rate/body limits, SBOM, dependency policy, and immutable test evidence",
        evidence=security_release_provenance,
        next_action=None
        if security_release_provenance_pass
        else security_release_provenance_action,
    )
)
if not security_release_provenance_pass:
    next_actions.append(security_release_provenance_action)

remaining_release_readiness = validator_summary(
    "check-remaining-release-readiness-evidence.sh",
    remaining_release_readiness_path,
)
remaining_release_readiness_pass = bind_release_identity(
    remaining_release_readiness,
    remaining_release_readiness_path,
    "remaining_release_readiness",
)
remaining_release_readiness_action = command(
    "Run the remaining live/release readiness checks, write "
    "target/velorix-product/remaining-release-readiness.json with deployment_id, "
    "s3:// authority_store_id, evidence_kind=remaining_release_readiness, "
    "source_revision, deployed_image_digests, and evidence_refs "
    "for release-image contract tests, OpenAPI contract, SQL admission corpus, crash matrix, "
    "replay determinism, join frontier, and scale soak; then validate with "
    "scripts/check-remaining-release-readiness-evidence.sh "
    "target/velorix-product/remaining-release-readiness.json"
)
gates.append(
    gate(
        "remaining_release_readiness",
        "pass"
        if remaining_release_readiness_pass
        else remaining_release_readiness.get("status", "blocked"),
        "Remaining 1.0 release evidence for release-image contract tests, SQL admission corpus/mutation, crash/replay/join-frontier matrix, published limits, and multi-day soak",
        evidence=remaining_release_readiness,
        next_action=None
        if remaining_release_readiness_pass
        else remaining_release_readiness_action,
    )
)
if not remaining_release_readiness_pass:
    next_actions.append(remaining_release_readiness_action)

ingest = product.get("ingest_writer") or {}
lifecycle = ingest.get("lifecycle_attestation") or {}
ingest_pass = (
    ingest.get("job_completed") is True
    and ingest.get("append_outcome") == "appended"
    and lifecycle.get("trusted_for_product_complete") is True
    and lifecycle.get("source") == "generated"
)
gates.append(
    gate(
        "ingest_writer_lifecycle",
        "pass" if ingest_pass else "blocked",
        "Pod-internal ingest-writer append and lifecycle adversarial evidence",
        evidence={
            "job_completed": ingest.get("job_completed"),
            "append_outcome": ingest.get("append_outcome"),
            "lifecycle_source": lifecycle.get("source"),
        },
    )
)

images = deployed_images
if isinstance(images, dict) and images:
    missing_image_digests = [
        role for role, info in images.items() if not (info or {}).get("image_digest")
    ]
else:
    missing_image_digests = ["velorix-api", "velorix-meta"]
if missing_image_digests:
    action = command(
        "VELORIX_VIND_PRODUCT_DIR=target/velorix-product "
        "scripts/refresh-vind-product-deployed-images.sh, or rerun with deployed "
        "image digest env vars if the Deployment has no image-digest annotation: "
        "VELORIX_VIND_PRODUCT_DIR=target/velorix-product "
        "VELORIX_API_IMAGE_DIGEST=sha256:REPLACE_WITH_API_DIGEST "
        "VELORIX_META_IMAGE_DIGEST=sha256:REPLACE_WITH_META_DIGEST "
        "scripts/run-vind-product.sh"
    )
    gates.append(
        gate(
            "deployed_image_digests",
            "blocked",
            "Release product evidence must bind deployed image digests",
            evidence={"missing_roles": missing_image_digests},
            next_action=action,
        )
    )
    next_actions.append(action)
else:
    gates.append(
        gate(
            "deployed_image_digests",
            "pass",
            "Deployed product image digests are recorded",
            evidence={"roles": sorted(images)},
        )
    )

gate_summary = {}
for item in gates:
    status = item.get("status") or "missing"
    gate_summary[status] = gate_summary.get(status, 0) + 1
product_complete = all(item["status"] == "pass" for item in gates)
local_diagnostic_complete = all(
    item["status"] in {"pass", "out_of_scope"} for item in gates
)


def gate_blocker(item):
    payload = {
        "gate": item.get("id"),
        "status": item.get("status"),
        "summary": item.get("summary"),
    }
    if item.get("blocked_by"):
        payload["blocked_by"] = item.get("blocked_by")
    if item.get("next_action"):
        payload["next_action"] = item.get("next_action")
    return payload


blockers = [
    gate_blocker(item)
    for item in gates
    if item.get("status") in {"blocked", "diagnostic", "missing", "out_of_scope"}
]


GATE_INPUT_MAP = {
    "public_ingress_tls_auth": {
        "preflight_steps": ["ingress"],
        "placeholder_groups": ["public_ingress_tls_auth"],
    },
    "object_store_external_authority": {
        "preflight_steps": ["external_s3"],
        "placeholder_groups": ["external_s3"],
    },
    "object_store_durability_policy": {
        "preflight_steps": ["durability"],
        "placeholder_groups": ["object_store_durability_review"],
    },
    "hiqlite_backend_time_release": {
        "preflight_steps": ["hiqlite_backend_time"],
        "placeholder_groups": ["release_identity", "sigstore_provenance"],
    },
}


def placeholder_groups_by_step():
    grouped = {}
    if not complete_env_report:
        return grouped
    for group in complete_env_report.get("placeholder_groups") or []:
        step = group.get("step")
        if step:
            grouped[step] = group
    return grouped


placeholder_groups = placeholder_groups_by_step()
forced_blockers = (input_preflight or {}).get("forced_blockers") or []
forced_blocker_counts = {}
for forced in forced_blockers:
    if not isinstance(forced, dict):
        continue
    step = forced.get("step")
    if step:
        forced_blocker_counts[step] = forced_blocker_counts.get(step, 0) + 1


def redacted_issues(items):
    issues = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        issue = {}
        if item.get("subject"):
            issue["subject"] = item.get("subject")
        if item.get("detail"):
            issue["detail"] = item.get("detail")
        if issue:
            issues.append(issue)
    return issues


def issue_subjects(items):
    return sorted({
        issue.get("subject")
        for issue in redacted_issues(items)
        if issue.get("subject")
    })


def integer_or_zero(value):
    return value if isinstance(value, int) else 0


def release_preflight_summary():
    if not isinstance(release_preflight, dict):
        return None
    missing = redacted_issues(release_preflight.get("missing") or [])
    invalid = redacted_issues(release_preflight.get("invalid") or [])
    return {
        "evidence": "hiqlite-backend-time-release-preflight.json",
        "status": release_preflight.get("status"),
        "missing_count": len(missing),
        "invalid_count": len(invalid),
        "missing": missing,
        "invalid": invalid,
        "missing_subjects": issue_subjects(missing),
        "invalid_subjects": issue_subjects(invalid),
    }


def completion_execution_plan_summary():
    if not isinstance(complete_execution_plan, dict):
        return None
    steps = {}
    for name, step in (complete_execution_plan.get("steps") or {}).items():
        if not isinstance(step, dict):
            continue
        step_summary = {
            "state": step.get("state"),
            "will_run": step.get("will_run"),
            "mode": step.get("mode"),
            "helper": step.get("helper"),
            "waiting_on": step.get("waiting_on") or [],
            "status": step.get("status"),
            "ready": step.get("ready"),
            "missing_count": integer_or_zero(step.get("missing_count")),
            "invalid_count": integer_or_zero(step.get("invalid_count")),
            "missing_subjects": step.get("missing_subjects") or [],
            "invalid_subjects": step.get("invalid_subjects") or [],
        }
        if name == "hiqlite_backend_time":
            release_summary = release_preflight_summary()
            if release_summary:
                step_summary["release_preflight"] = release_summary
                step_summary["release_preflight_status"] = release_summary.get("status")
                step_summary["release_preflight_missing_subjects"] = release_summary.get("missing_subjects") or []
                step_summary["release_preflight_invalid_subjects"] = release_summary.get("invalid_subjects") or []
        steps[name] = step_summary
    return {
        "evidence": "complete-vind-product-plan.json",
        "report_kind": complete_execution_plan.get("report_kind"),
        "dry_run": complete_execution_plan.get("dry_run"),
        "preflight_status": complete_execution_plan.get("preflight_status"),
        "forced_blocker_count": complete_execution_plan.get("forced_blocker_count"),
        "external_s3_current_ready": complete_execution_plan.get(
            "external_s3_current_ready"
        ),
        "run_order": complete_execution_plan.get("run_order") or [],
        "steps": steps,
        "will_run_steps": [
            name for name, step in steps.items() if step.get("will_run") is True
        ],
        "blocked_steps": [
            name for name, step in steps.items() if step.get("state") == "blocked"
        ],
        "waiting_steps": [
            name
            for name, step in steps.items()
            if step.get("state") == "waiting_on_prerequisite"
        ],
        "creates_product_complete_evidence": False,
    }


def input_summary_for_gate(gate_id):
    mapping = GATE_INPUT_MAP.get(gate_id)
    if not mapping:
        return None
    preflight_steps = []
    input_steps = (input_preflight or {}).get("steps") or {}
    for step_name in mapping.get("preflight_steps") or []:
        step = input_steps.get(step_name)
        if not isinstance(step, dict):
            continue
        preflight_step = {
            "step": step_name,
            "status": step.get("status"),
            "ready": step.get("ready"),
            "missing_count": len(step.get("missing") or []),
            "invalid_count": len(step.get("invalid") or []),
            "missing": redacted_issues(step.get("missing") or []),
            "invalid": redacted_issues(step.get("invalid") or []),
            "missing_subjects": issue_subjects(step.get("missing") or []),
            "invalid_subjects": issue_subjects(step.get("invalid") or []),
            "forced_blocker_count": forced_blocker_counts.get(step_name, 0),
            "env": step.get("env") or {},
        }
        if step.get("auth_token_source"):
            preflight_step["auth_token_source"] = step.get("auth_token_source")
        if step.get("env_review_flags"):
            preflight_step["env_review_flags"] = step.get("env_review_flags")
        if "authority_ready" in step:
            preflight_step["authority_ready"] = step.get("authority_ready")
        if step.get("authority"):
            preflight_step["authority"] = step.get("authority")
        preflight_steps.append(preflight_step)

    groups = []
    placeholders = []
    secret_placeholders = []
    for group_name in mapping.get("placeholder_groups") or []:
        group = placeholder_groups.get(group_name)
        if not isinstance(group, dict):
            continue
        group_placeholders = group.get("placeholders") or []
        group_secret_placeholders = group.get("secret_placeholders") or []
        placeholders.extend(group_placeholders)
        secret_placeholders.extend(group_secret_placeholders)
        groups.append(
            {
                "step": group_name,
                "missing_count": group.get("missing_count"),
                "description": group.get("description"),
                "placeholders": group_placeholders,
                "secret_placeholders": group_secret_placeholders,
            }
        )

    staged_attestation = None
    staged_input = None
    if gate_id == "object_store_durability_policy":
        staged_attestation = staged_durability_attestation_summary()
        staged_input = staged_durability_input_summary()

    if not preflight_steps and not groups and not staged_attestation and not staged_input:
        return None
    payload = {
        "preflight_steps": preflight_steps,
        "placeholder_groups": groups,
        "placeholders": sorted(set(placeholders)),
        "secret_placeholders": sorted(set(secret_placeholders)),
        "placeholder_count": len(set(placeholders)),
        "secret_placeholder_count": len(set(secret_placeholders)),
        "descriptions": [
            group.get("description") for group in groups if group.get("description")
        ],
        "creates_product_complete_evidence": False,
    }
    if gate_id == "hiqlite_backend_time_release":
        release_summary = release_preflight_summary()
        if release_summary:
            payload["release_preflight"] = release_summary
    if gate_id == "object_store_durability_policy":
        if staged_attestation:
            payload["staged_attestation"] = staged_attestation
        if staged_input:
            payload["staged_input"] = staged_input
    return payload


def handoff_input_summary():
    groups = []
    placeholders = []
    secret_placeholders = []
    for group in (complete_env_report or {}).get("placeholder_groups") or []:
        if not isinstance(group, dict):
            continue
        group_placeholders = group.get("placeholders") or []
        group_secret_placeholders = group.get("secret_placeholders") or []
        placeholders.extend(group_placeholders)
        secret_placeholders.extend(group_secret_placeholders)
        groups.append(
            {
                "step": group.get("step"),
                "missing_count": group.get("missing_count"),
                "description": group.get("description"),
                "placeholders": group_placeholders,
                "secret_placeholders": group_secret_placeholders,
            }
        )
    return {
        "preflight_status": (input_preflight or {}).get("status"),
        "forced_blocker_count": len(forced_blockers),
        "placeholder_groups": groups,
        "placeholders": sorted(set(placeholders)),
        "secret_placeholders": sorted(set(secret_placeholders)),
        "placeholder_count": len(set(placeholders)),
        "secret_placeholder_count": len(set(secret_placeholders)),
        "creates_product_complete_evidence": False,
    }


def input_summary_requires_input(summary):
    if not isinstance(summary, dict):
        return False
    if summary.get("placeholder_count", 0) > 0:
        return True
    for step in summary.get("preflight_steps") or []:
        if not isinstance(step, dict):
            continue
        if step.get("missing_count", 0) > 0 or step.get("invalid_count", 0) > 0:
            return True
        if step.get("status") in {"blocked", "incomplete"}:
            return True
        if step.get("ready") is False:
            return True
    release = summary.get("release_preflight") or {}
    if isinstance(release, dict) and (
        release.get("missing_count", 0) > 0 or release.get("invalid_count", 0) > 0
    ):
        return True
    return False


def completion_plan_step(item):
    action = item.get("next_action")
    waiting_on = item.get("blocked_by") or []
    action_has_placeholders = has_placeholder(action)
    input_summary = input_summary_for_gate(item.get("id"))
    input_summary_has_required_input = input_summary_requires_input(input_summary)
    if item.get("status") == "pass":
        state = "complete"
    elif item.get("status") == "out_of_scope":
        state = "deferred_product_gate"
    elif waiting_on:
        state = "waiting_on_prerequisite"
    elif action_has_placeholders or input_summary_has_required_input:
        state = "input_required"
    elif action:
        state = "runnable"
    else:
        state = "blocked_without_action"
    payload = {
        "id": item.get("id"),
        "kind": "gate",
        "gate": item.get("id"),
        "status": item.get("status"),
        "state": state,
        "summary": item.get("summary"),
        "waiting_on": waiting_on,
        "action_has_placeholders": action_has_placeholders,
        "input_summary_requires_input": input_summary_has_required_input,
    }
    if action:
        payload["next_action"] = action
    if input_summary:
        payload["input_summary"] = input_summary
    return payload


completion_steps = [
    completion_plan_step(item)
    for item in gates
    if item.get("status") != "pass"
]
handoff_placeholder_count = None
if complete_env_report:
    handoff_placeholder_count = len(complete_env_report.get("placeholders") or [])
handoff_state = (
    "complete"
    if product_complete
    else "input_required"
    if handoff_placeholder_count
    else "runnable"
)
completion_handoff_step = {
    "id": "complete_vind_product_env_handoff",
    "kind": "handoff",
    "state": handoff_state,
    "summary": "Unified env-file handoff for the remaining product-complete gates",
    "placeholder_count": handoff_placeholder_count,
    "input_summary": handoff_input_summary(),
    "next_action": handoff_action,
}
if not product_complete:
    next_actions = [handoff_action] + [
        action for action in next_actions if action != handoff_action
    ]

payload = {
    "schema_version": 1,
    "report_kind": "velorix_product_completion_report",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_evidence": product_path,
    "rest_api_smoke_evidence": rest_smoke_path if rest_smoke is not None else None,
    "input_preflight": {
        "evidence": "complete-vind-product-input-preflight.json",
        "status": input_preflight.get("status"),
        "forced_blocker_count": len(input_preflight.get("forced_blockers") or []),
        "creates_product_complete_evidence": input_preflight.get(
            "creates_product_complete_evidence"
        ),
        "steps": {
            name: {
                "status": step.get("status"),
                "ready": step.get("ready"),
                "missing_count": len(step.get("missing") or []),
                "invalid_count": len(step.get("invalid") or []),
            }
            for name, step in (input_preflight.get("steps") or {}).items()
            if isinstance(step, dict)
        },
    }
    if input_preflight
    else None,
    "completion_handoff": {
        "evidence": "complete-vind-product-env.json",
        "env_file": complete_env_report.get("env_file"),
        "placeholder_count": complete_env_report.get("placeholder_count"),
        "placeholders": complete_env_report.get("placeholders") or [],
        "secret_placeholders": complete_env_report.get("secret_placeholders") or [],
        "placeholder_groups": complete_env_report.get("placeholder_groups") or [],
        "derived_from_product_evidence": complete_env_report.get(
            "derived_from_product_evidence"
        )
        or [],
        "fixed_release_values": complete_env_report.get("fixed_release_values") or [],
        "creates_product_complete_evidence": complete_env_report.get(
            "creates_product_complete_evidence"
        ),
        "next_action": complete_env_report.get("next_action") or handoff_action,
    }
    if complete_env_report
    else {
        "evidence": None,
        "next_action": handoff_action,
        "status": "missing",
    },
    "product_complete": product_complete,
    "local_diagnostic_complete": local_diagnostic_complete,
    "completion_plan": {
        "schema_version": 1,
        "derived_from": "report_gates",
        "complete": product_complete,
        "local_diagnostic_complete": local_diagnostic_complete,
        "excluded_steps": [
            item.get("id") for item in gates if item.get("status") == "out_of_scope"
        ],
        "handoff": completion_handoff_step,
        "steps": completion_steps,
        "runnable_steps": [
            item["id"] for item in completion_steps if item["state"] == "runnable"
        ],
        "input_required_steps": [
            item["id"]
            for item in completion_steps
            if item["state"] == "input_required"
        ],
        "waiting_steps": [
            item["id"]
            for item in completion_steps
            if item["state"] == "waiting_on_prerequisite"
        ],
        "deferred_steps": [
            item["id"]
            for item in completion_steps
            if item["state"] == "deferred_product_gate"
        ],
    },
    "completion_execution_plan": completion_execution_plan_summary(),
    "product_completion_source": {
        "derived_from": "report_gates",
        "product_evidence_product_complete": product_evidence_product_complete,
        "product_evidence_product_complete_blockers": product_evidence_blockers,
        "product_accepted_gate_statuses": ["pass"],
        "local_diagnostic_accepted_gate_statuses": ["pass", "out_of_scope"],
        "all_product_gates_pass": product_complete,
        "all_local_diagnostic_gates_pass_or_out_of_scope": local_diagnostic_complete,
    },
    "completion_scope": {
        "external_s3_required": external_s3_required,
        "public_ingress_required": public_ingress_required,
        "hiqlite_release_required": hiqlite_release_required,
        "excluded_gates": [
            item.get("id") for item in gates if item.get("status") == "out_of_scope"
        ],
        "warnings": completion_scope_warnings,
    },
    "product_complete_blockers": blockers,
    "gate_summary": gate_summary,
    "gates": gates,
    "next_actions": next_actions,
}

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")

print(f"product_complete={str(product_complete).lower()}")
print(f"product_evidence={product_path}")
print(f"report={output_path}")
print(
    "gate_summary="
    f"pass:{payload['gate_summary'].get('pass', 0)} "
    f"blocked:{payload['gate_summary'].get('blocked', 0)} "
    f"diagnostic:{payload['gate_summary'].get('diagnostic', 0)} "
    f"missing:{payload['gate_summary'].get('missing', 0)} "
    f"out_of_scope:{payload['gate_summary'].get('out_of_scope', 0)}"
)
if blockers:
    print("blockers:")
    for blocker in blockers:
        print(
            f"- {blocker.get('gate')}[{blocker.get('status')}]: "
            f"{blocker.get('summary')}"
        )
if next_actions:
    print("next_actions:")
    for item in next_actions:
        print(f"- {item}")
PY

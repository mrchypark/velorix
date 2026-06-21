#!/usr/bin/env bash
set -euo pipefail
umask 077

case "$-" in
  *x*)
    echo "Refusing to run with shell xtrace enabled because completion env files can contain credentials" >&2
    exit 64
    ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
product_evidence="${VELORIX_PRODUCT_EVIDENCE_PATH:-${VELORIX_VIND_PRODUCT_EVIDENCE:-${VELORIX_VIND_PRODUCT_DIR:-${repo_root}/target/velorix-product}/product-evidence.json}}"
output_file="${VELORIX_COMPLETE_PRODUCT_ENV:-}"
report_file="${VELORIX_COMPLETE_PRODUCT_ENV_REPORT:-}"

usage() {
  cat <<'EOF'
Write a single env template for completing a vind product slice.

Usage:
  scripts/write-complete-vind-product-env.sh \
    --product-evidence target/velorix-product/product-evidence.json

Options:
  --product-evidence PATH   Product evidence JSON to inspect.
  --output PATH             Shell env output path.
  --report PATH             JSON report output path.

Environment equivalents:
  VELORIX_PRODUCT_EVIDENCE_PATH or VELORIX_VIND_PRODUCT_EVIDENCE
  VELORIX_COMPLETE_PRODUCT_ENV
  VELORIX_COMPLETE_PRODUCT_ENV_REPORT

The generated env file is a template. It includes every external input needed
by scripts/complete-vind-product.sh for the current product-complete scope,
can optionally embed the Hiqlite backend-time release env template. Actual
external S3/OSS, object-store durability attestation, public/enterprise ingress,
and trusted Hiqlite release/Sigstore provenance are excluded by default; set
VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3=1,
VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS=1, or
VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE=1 before generation to include
those placeholders. It creates no product-complete evidence and no PVCs.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --product-evidence)
      product_evidence="${2:-}"
      shift 2
      ;;
    --output)
      output_file="${2:-}"
      shift 2
      ;;
    --report)
      report_file="${2:-}"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

if [ -z "$product_evidence" ]; then
  echo "--product-evidence or VELORIX_PRODUCT_EVIDENCE_PATH is required" >&2
  exit 64
fi
if [ -z "$output_file" ]; then
  output_file="$(dirname "$product_evidence")/complete-vind-product.env"
fi
if [ -z "$report_file" ]; then
  report_file="$(dirname "$product_evidence")/complete-vind-product-env.json"
fi

cd "$repo_root"

product_dir="$(dirname "$product_evidence")"
hiqlite_env="${product_dir}/hiqlite-backend-time-release.env"
hiqlite_report="${product_dir}/hiqlite-backend-time-release-env.json"

scripts/write-hiqlite-backend-time-release-env.sh \
  --product-evidence "$product_evidence" \
  --output "$hiqlite_env" \
  --report "$hiqlite_report" >/dev/null

python3 - "$product_evidence" "$output_file" "$report_file" "$hiqlite_env" "$hiqlite_report" <<'PY'
import json
import os
import re
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path

product_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
report_path = Path(sys.argv[3])
hiqlite_env_path = Path(sys.argv[4])
hiqlite_report_path = Path(sys.argv[5])

if not product_path.is_file():
    raise SystemExit(f"missing product evidence: {product_path}")
with product_path.open("r", encoding="utf-8") as f:
    product = json.load(f)
if product.get("evidence_kind") != "velorix_product_slice_evidence":
    raise SystemExit("product evidence_kind must be velorix_product_slice_evidence")
hiqlite_env_text = hiqlite_env_path.read_text(encoding="utf-8")
hiqlite_report = json.loads(hiqlite_report_path.read_text(encoding="utf-8"))

product_dir = product_path.parent
prefix = f"product/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-release-handoff"
external_s3_required = os.environ.get("VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3", "0") == "1"
public_ingress_required = os.environ.get("VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS", "0") == "1"
hiqlite_release_required = os.environ.get("VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE", "0") == "1"
scope_warnings = []
if not public_ingress_required:
    scope_warnings.append(
        "public_ingress_tls_auth_out_of_scope_does_not_prove_public_dns_tls_or_external_client_reachability"
    )
if not external_s3_required:
    scope_warnings.append(
        "object_store_external_authority_out_of_scope_does_not_prove_object_store_durability"
    )
if not hiqlite_release_required:
    scope_warnings.append(
        "hiqlite_backend_time_release_out_of_scope_does_not_prove_sigstore_ci_release_provenance"
    )

values = {
    "VELORIX_VIND_PRODUCT_DIR": str(product_dir),
    "VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3": "1" if external_s3_required else "0",
    "VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS": "1" if public_ingress_required else "0",
    "VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE": "1" if hiqlite_release_required else "0",
    "VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE": "1",
    "VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3": "1",
    "VELORIX_COMPLETE_PRODUCT_INGRESS": "1",
    "VELORIX_COMPLETE_PRODUCT_DURABILITY": "1",
    "VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME": "1",
    "AWS_ENDPOINT_URL": "https://S3_OR_OSS_ENDPOINT",
    "AWS_ACCESS_KEY_ID": "REPLACE_WITH_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY": "REPLACE_WITH_SECRET_KEY",
    "AWS_SESSION_TOKEN": "",
    "AWS_REGION": "us-east-1",
    "VELORIX_S3_BUCKET": "REPLACE_WITH_BUCKET",
    "VELORIX_S3_PREFIX": prefix,
    "VELORIX_S3_FORCE_PATH_STYLE": "1",
    "VELORIX_S3_CREDENTIALS_SECRET_NAME": "velorix-s3-credentials",
    "VELORIX_S3_CREDENTIALS_SECRET_MANAGED": "1",
    "VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY": "0",
    "VELORIX_PRODUCT_INGRESS_HOST": "PUBLIC_HOST.example.com",
    "VELORIX_PRODUCT_INGRESS_APPLY": "1",
    "VELORIX_PRODUCT_INGRESS_ATTEST": "1",
    "VELORIX_PRODUCT_INGRESS_ATTACH": "1",
    "VELORIX_PRODUCT_INGRESS_CLASS": "INGRESS_CONTROLLER",
    "VELORIX_PRODUCT_INGRESS_TLS_SECRET": "TLS_SECRET_NAME",
    "VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS": "600",
    "VELORIX_PRODUCT_INGRESS_WAIT_INTERVAL_SECONDS": "5",
    "VELORIX_INGRESS_ENDPOINT_URL": "https://PUBLIC_HOST.example.com",
    "VELORIX_INGRESS_CONTROLLER": "INGRESS_CONTROLLER",
    "VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS": "120",
    "VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS": "5",
    "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED": "REPLACE_WITH_REVIEWED_1",
    "VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED": "REPLACE_WITH_REVIEWED_1",
    "VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED": "REPLACE_WITH_REVIEWED_1",
    "VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED": "REPLACE_WITH_REVIEWED_1",
    "VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED": "REPLACE_WITH_REVIEWED_1",
    "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED": "REPLACE_WITH_REVIEWED_1",
}
if not external_s3_required:
    values["VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3"] = "0"
    values["VELORIX_COMPLETE_PRODUCT_DURABILITY"] = "0"
    for key in [
        "AWS_ENDPOINT_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "VELORIX_S3_BUCKET",
        "VELORIX_S3_PREFIX",
        "VELORIX_S3_FORCE_PATH_STYLE",
        "VELORIX_S3_CREDENTIALS_SECRET_NAME",
        "VELORIX_S3_CREDENTIALS_SECRET_MANAGED",
        "VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY",
        "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED",
        "VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED",
        "VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED",
        "VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED",
        "VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED",
        "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED",
    ]:
        values.pop(key, None)
if not public_ingress_required:
    values["VELORIX_COMPLETE_PRODUCT_INGRESS"] = "0"
    for key in [
        "VELORIX_PRODUCT_INGRESS_HOST",
        "VELORIX_PRODUCT_INGRESS_APPLY",
        "VELORIX_PRODUCT_INGRESS_ATTEST",
        "VELORIX_PRODUCT_INGRESS_ATTACH",
        "VELORIX_PRODUCT_INGRESS_CLASS",
        "VELORIX_PRODUCT_INGRESS_TLS_SECRET",
        "VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS",
        "VELORIX_PRODUCT_INGRESS_WAIT_INTERVAL_SECONDS",
        "VELORIX_INGRESS_ENDPOINT_URL",
        "VELORIX_INGRESS_CONTROLLER",
        "VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS",
        "VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS",
    ]:
        values.pop(key, None)

placeholder_keys = [key for key, value in values.items() if "REPLACE_WITH" in value]
placeholder_keys.extend(
    key
    for key, value in values.items()
    if any(marker in value for marker in ["PUBLIC_HOST.", "INGRESS_CONTROLLER", "TLS_SECRET_NAME", "S3_OR_OSS_ENDPOINT"])
)
for line in hiqlite_env_text.splitlines():
    match = re.match(r"export\s+([A-Z0-9_]+)=(.*)$", line)
    if hiqlite_release_required and match and "REPLACE_WITH" in match.group(2):
        placeholder_keys.append(match.group(1))

placeholder_set = set(placeholder_keys)
secret_placeholders = sorted(
    placeholder_set
    & {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "VELORIX_CI_SIGSTORE_BUNDLE_BASE64",
    }
)

placeholder_groups = [
    {
        "step": "external_s3",
        "description": "Nonlocal S3/OSS endpoint, credentials, and bucket used by scripts/run-vind-product-external-s3.sh.",
        "placeholders": [
            "AWS_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "VELORIX_S3_BUCKET",
        ],
        "secret_placeholders": [
            key
            for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
            if key in placeholder_set
        ],
    },
    {
        "step": "public_ingress_tls_auth",
        "description": "Public HTTPS ingress and TLS/auth route checked by scripts/complete-vind-product-ingress.sh.",
        "placeholders": [
            "VELORIX_PRODUCT_INGRESS_HOST",
            "VELORIX_PRODUCT_INGRESS_CLASS",
            "VELORIX_PRODUCT_INGRESS_TLS_SECRET",
            "VELORIX_INGRESS_ENDPOINT_URL",
            "VELORIX_INGRESS_CONTROLLER",
        ],
        "secret_placeholders": [],
    },
    {
        "step": "object_store_durability_review",
        "description": "Explicit operator review flags required before object-store durability attestation can be trusted.",
        "placeholders": [
            "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED",
            "VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED",
            "VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED",
            "VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED",
            "VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED",
            "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED",
        ],
        "secret_placeholders": [],
    },
    {
        "step": "release_identity",
        "description": "Velorix release/source and CI workflow identity validated by scripts/check-hiqlite-backend-time-release-inputs.sh.",
        "placeholders": [
            "VELORIX_RELEASE_COMMIT",
            "VELORIX_SOURCE_REVISION",
            "VELORIX_CI_WORKFLOW_RUN_ID",
            "VELORIX_CI_JOB_WORKFLOW_REF",
        ],
        "secret_placeholders": [],
    },
    {
        "step": "sigstore_provenance",
        "description": "Sigstore bundle and digest for trusted Hiqlite backend-time release attestation.",
        "placeholders": [
            "VELORIX_CI_SIGSTORE_BUNDLE_BASE64",
            "VELORIX_CI_SIGSTORE_BUNDLE_SHA256",
        ],
        "secret_placeholders": [
            key
            for key in ["VELORIX_CI_SIGSTORE_BUNDLE_BASE64"]
            if key in placeholder_set
        ],
    },
]
for group in placeholder_groups:
    group["placeholders"] = [
        key for key in group["placeholders"] if key in placeholder_set
    ]
    group["missing_count"] = len(group["placeholders"])
placeholder_groups = [group for group in placeholder_groups if group["placeholders"]]

lines = [
    "# Generated by scripts/write-complete-vind-product-env.sh",
    "# Replace every REPLACE_WITH_* value before sourcing.",
    "# This file is 0600 because it may be edited to contain real credentials.",
    "# Actual external S3/OSS is excluded by default. Regenerate with VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3=1 to include S3 and durability-review inputs.",
    "# Public/enterprise ingress is excluded by default. Regenerate with VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS=1 to include public ingress/TLS/auth inputs.",
    "# Trusted Hiqlite release/Sigstore provenance is excluded by default. Regenerate with VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE=1 to include release identity and Sigstore inputs.",
    "# Default local_diagnostic_complete may prove local/internal REST TLS/auth and Hiqlite backend-time boundary only; product_complete remains false until every release/product gate is required and passes.",
    "# When S3 scope is enabled, reuse an existing Kubernetes S3 Secret by setting VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0, setting VELORIX_S3_CREDENTIALS_SECRET_NAME, and blank AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN.",
    "",
]
for key, value in values.items():
    lines.append(f"export {key}={shlex.quote(value)}")

lines.extend(
    [
        "",
        "# Hiqlite backend-time release provenance inputs.",
        "# These exports are generated by scripts/write-hiqlite-backend-time-release-env.sh.",
        "# The embedded template carries release/Sigstore fields such as VELORIX_CI_SIGSTORE_BUNDLE_BASE64.",
        "",
        hiqlite_env_text.rstrip()
        if hiqlite_release_required
        else "# Release/Sigstore env template omitted because VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE=0.",
        "",
        "# Verify inputs without creating product-complete evidence:",
        f"# VELORIX_COMPLETE_PRODUCT_DRY_RUN=1 scripts/complete-vind-product.sh --env-file {output_path}",
        "# Then run the completion sequence:",
        f"# scripts/complete-vind-product.sh --env-file {output_path}",
        "",
    ]
)

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text("\n".join(lines), encoding="utf-8")
os.chmod(output_path, 0o600)

report = {
    "schema_version": 1,
    "report_kind": "velorix_complete_vind_product_env_template",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_evidence": str(product_path),
    "env_file": str(output_path),
    "hiqlite_release_env": str(hiqlite_env_path),
    "hiqlite_release_env_report": str(hiqlite_report_path),
    "hiqlite_release_required": hiqlite_release_required,
    "placeholders": sorted(placeholder_set),
    "placeholder_count": len(placeholder_set),
    "secret_placeholders": secret_placeholders,
    "placeholder_groups": placeholder_groups,
    "derived_from_product_evidence": hiqlite_report.get("derived_from_product_evidence") or [],
    "fixed_release_values": hiqlite_report.get("fixed_release_values") or [],
    "scope_warnings": scope_warnings,
    "creates_product_complete_evidence": False,
    "next_action": (
        f"Replace placeholders in {output_path}, run "
        f"VELORIX_COMPLETE_PRODUCT_DRY_RUN=1 scripts/complete-vind-product.sh --env-file {output_path}, "
        f"then run scripts/complete-vind-product.sh --env-file {output_path}"
    ),
}
report_path.parent.mkdir(parents=True, exist_ok=True)
report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(report_path, 0o600)

print(f"complete_product_env={output_path}")
print(f"complete_product_env_report={report_path}")
print(f"placeholders={report['placeholder_count']}")
PY

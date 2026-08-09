#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
attestation_file="${VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:-${product_dir}/object-store-durability-attestation.json}"
output_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE_OUT:-$product_evidence}"
report="${VELORIX_VIND_PRODUCT_COMPLETION_REPORT:-${product_dir}/product-completion-report.json}"
refresh_report="${VELORIX_ATTACH_DURABILITY_REFRESH_REPORT:-1}"

usage() {
  cat <<'EOF'
Attach operator-reviewed object-store durability evidence to a vind product slice.

Usage:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
  scripts/attach-vind-object-store-durability.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_VIND_PRODUCT_EVIDENCE=target/velorix-product/product-evidence.json
  VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE=target/velorix-product/object-store-durability-attestation.json
  VELORIX_VIND_PRODUCT_EVIDENCE_OUT=target/velorix-product/product-evidence.json
  VELORIX_ATTACH_DURABILITY_REFRESH_REPORT=1

This helper does not create buckets, PVCs, provider policies, or attestation
evidence. It consumes an existing
velorix_object_store_durability_policy_attestation, copies it beside
product-evidence.json as object-store-durability-attestation.json, and updates
object_store.durability_policy_attestation in product-evidence.json.
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

case "$refresh_report" in
  0 | 1) ;;
  *)
    echo "VELORIX_ATTACH_DURABILITY_REFRESH_REPORT must be 0 or 1" >&2
    exit 64
    ;;
esac

if [ ! -f "$product_evidence" ]; then
  echo "missing product evidence: ${product_evidence}" >&2
  exit 66
fi
if [ ! -f "$attestation_file" ]; then
  echo "missing object-store durability attestation: ${attestation_file}" >&2
  exit 66
fi

python3 - "$product_evidence" "$attestation_file" "$output_evidence" <<'PY'
import json
import os
import sys
from datetime import datetime
from pathlib import Path

product_path = Path(sys.argv[1])
attestation_path = Path(sys.argv[2])
output_path = Path(sys.argv[3])
product_dir = output_path.parent
sibling_path = product_dir / "object-store-durability-attestation.json"

with product_path.open("r", encoding="utf-8") as f:
    product = json.load(f)
with attestation_path.open("r", encoding="utf-8") as f:
    attestation = json.load(f)

errors = []
if product.get("evidence_kind") != "velorix_product_slice_evidence":
    errors.append("product evidence_kind must be velorix_product_slice_evidence")
store = product.get("object_store") or {}
if store.get("mode") != "external-s3":
    errors.append("product object_store.mode must be external-s3")
if store.get("local_development_authority") is True:
    errors.append("local development object-store authority cannot receive durability attestation")
if store.get("external_s3_bucket_validated") is not True:
    errors.append("product object_store.external_s3_bucket_validated must be true")
if store.get("external_s3_prefix_validated") is not True:
    errors.append("product object_store.external_s3_prefix_validated must be true")
for field in ["authority_store_id", "bucket", "s3_prefix"]:
    if not isinstance(store.get(field), str):
        errors.append(f"product object_store.{field} must be a string")

if attestation.get("schema_version") != 1:
    errors.append("attestation schema_version must be 1")
if attestation.get("evidence_kind") != "velorix_object_store_durability_policy_attestation":
    errors.append("attestation evidence_kind must be velorix_object_store_durability_policy_attestation")
for field in ["provider_kind", "authority_store_id", "bucket", "s3_prefix", "attested_at", "attester"]:
    if not isinstance(attestation.get(field), str) or (
        field != "s3_prefix" and not attestation[field].strip()
    ):
        errors.append(f"attestation {field} must be a string")
for field in ["authority_store_id", "bucket", "s3_prefix"]:
    if attestation.get(field) != store.get(field):
        errors.append(f"attestation {field} must match product object_store.{field}")
for field in [
    "versioning_or_object_lock_enabled",
    "server_side_encryption_enabled",
    "backup_or_replication_configured",
    "lifecycle_delete_policy_reviewed",
    "destructive_delete_protection_reviewed",
    "cost_controls_reviewed",
]:
    if attestation.get(field) is not True:
        errors.append(f"attestation {field} must be true")
try:
    parsed = datetime.fromisoformat(str(attestation.get("attested_at", "")).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        errors.append("attestation attested_at must include timezone")
except ValueError:
    errors.append("attestation attested_at must be RFC3339")

if errors:
    raise SystemExit(
        "invalid object-store durability attachment:\n- " + "\n- ".join(errors)
    )

product_dir.mkdir(parents=True, exist_ok=True)
if attestation_path.resolve() != sibling_path.resolve():
    sibling_path.write_text(json.dumps(attestation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(sibling_path, 0o600)
else:
    os.chmod(sibling_path, 0o600)

summary = {
    "validated": True,
    "evidence": "object-store-durability-attestation.json",
    "evidence_kind": attestation.get("evidence_kind"),
    "schema_version": attestation.get("schema_version"),
    "provider_kind": attestation.get("provider_kind"),
    "authority_store_id": attestation.get("authority_store_id"),
    "bucket": attestation.get("bucket"),
    "s3_prefix": attestation.get("s3_prefix"),
    "versioning_or_object_lock_enabled": attestation.get("versioning_or_object_lock_enabled"),
    "server_side_encryption_enabled": attestation.get("server_side_encryption_enabled"),
    "backup_or_replication_configured": attestation.get("backup_or_replication_configured"),
    "lifecycle_delete_policy_reviewed": attestation.get("lifecycle_delete_policy_reviewed"),
    "destructive_delete_protection_reviewed": attestation.get("destructive_delete_protection_reviewed"),
    "cost_controls_reviewed": attestation.get("cost_controls_reviewed"),
    "attested_at": attestation.get("attested_at"),
    "attester": attestation.get("attester"),
}
store["durability_policy_attestation"] = summary
product["object_store"] = store
product["product_complete_blockers"] = [
    blocker
    for blocker in product.get("product_complete_blockers", [])
    if blocker != "external S3-compatible authority lacks operator-reviewed durability policy attestation"
]
product["product_complete"] = (
    product.get("product_complete") is True
    and len(product.get("product_complete_blockers", [])) == 0
)
output_path.write_text(json.dumps(product, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(output_path, 0o600)
print(f"product_evidence={output_path}")
print(f"object_store_durability_attestation={sibling_path}")
PY

if [ "$refresh_report" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="$output_evidence" \
    VELORIX_PRODUCT_COMPLETION_REPORT="$report" \
    scripts/report-vind-product-completion.sh
fi

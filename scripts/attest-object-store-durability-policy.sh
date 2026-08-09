#!/usr/bin/env bash
set -euo pipefail
umask 077

case "$-" in
  *x*)
    echo "Refusing to run with shell xtrace enabled because object-store authority details would be logged" >&2
    exit 64
    ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
product_evidence_explicit=0
if [ -n "${VELORIX_VIND_PRODUCT_EVIDENCE:-}" ]; then
  product_evidence_explicit=1
fi
assessment_file="${VELORIX_OBJECT_STORE_DURABILITY_ASSESSMENT_FILE:-}"
output_file="${VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:-${product_dir}/object-store-durability-attestation.json}"
authority_store_id="${VELORIX_OBJECT_STORE_AUTHORITY_STORE_ID:-}"
bucket="${VELORIX_OBJECT_STORE_BUCKET:-${VELORIX_S3_BUCKET:-}}"
s3_prefix="${VELORIX_OBJECT_STORE_S3_PREFIX:-${VELORIX_S3_PREFIX:-}}"
provider_kind="${VELORIX_OBJECT_STORE_PROVIDER_KIND:-s3-compatible}"
attester="${VELORIX_ATTESTER:-$(id -un 2>/dev/null || printf 'operator')}"
versioning_or_object_lock_enabled="${VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED:-}"
server_side_encryption_enabled="${VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED:-}"
backup_or_replication_configured="${VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED:-}"
lifecycle_delete_policy_reviewed="${VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED:-}"
destructive_delete_protection_reviewed="${VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED:-}"
cost_controls_reviewed="${VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED:-}"

usage() {
  cat <<'EOF'
Generate operator-reviewed product-complete object-store durability evidence.

Usage:
  scripts/attest-object-store-durability-policy.sh \
    --product-evidence target/velorix-product/product-evidence.json \
    --output target/velorix-product/object-store-durability-attestation.json \
    --versioning-or-object-lock-enabled \
    --server-side-encryption-enabled \
    --backup-or-replication-configured \
    --lifecycle-delete-policy-reviewed \
    --destructive-delete-protection-reviewed \
    --cost-controls-reviewed

Environment equivalents:
  VELORIX_VIND_PRODUCT_EVIDENCE
  VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE
  VELORIX_OBJECT_STORE_DURABILITY_ASSESSMENT_FILE
  VELORIX_OBJECT_STORE_AUTHORITY_STORE_ID
  VELORIX_OBJECT_STORE_BUCKET or VELORIX_S3_BUCKET
  VELORIX_OBJECT_STORE_S3_PREFIX or VELORIX_S3_PREFIX
  VELORIX_OBJECT_STORE_PROVIDER_KIND
  VELORIX_ATTESTER
  VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED=1
  VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED=1
  VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED=1
  VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED=1
  VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED=1
  VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED=1

The product evidence must describe an external S3-compatible authority. When
generating attestation before a product run, omit --product-evidence and pass
--authority-store-id, --bucket, and --s3-prefix instead. Local development
authorities are intentionally rejected by product-evidence mode and by the
product runner when the attestation is attached.
EOF
}

mark_true() {
  printf '1'
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --product-evidence)
      product_evidence="${2:-}"
      product_evidence_explicit=1
      shift 2
      ;;
    --assessment)
      assessment_file="${2:-}"
      shift 2
      ;;
    --authority-store-id)
      authority_store_id="${2:-}"
      shift 2
      ;;
    --bucket)
      bucket="${2:-}"
      shift 2
      ;;
    --s3-prefix)
      s3_prefix="${2:-}"
      shift 2
      ;;
    --output)
      output_file="${2:-}"
      shift 2
      ;;
    --provider-kind)
      provider_kind="${2:-}"
      shift 2
      ;;
    --attester)
      attester="${2:-}"
      shift 2
      ;;
    --versioning-or-object-lock-enabled)
      versioning_or_object_lock_enabled="$(mark_true)"
      shift
      ;;
    --server-side-encryption-enabled)
      server_side_encryption_enabled="$(mark_true)"
      shift
      ;;
    --backup-or-replication-configured)
      backup_or_replication_configured="$(mark_true)"
      shift
      ;;
    --lifecycle-delete-policy-reviewed)
      lifecycle_delete_policy_reviewed="$(mark_true)"
      shift
      ;;
    --destructive-delete-protection-reviewed)
      destructive_delete_protection_reviewed="$(mark_true)"
      shift
      ;;
    --cost-controls-reviewed)
      cost_controls_reviewed="$(mark_true)"
      shift
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

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

require python3

if [ -z "$output_file" ]; then
  echo "--output or VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE is required" >&2
  exit 64
fi
if [ -z "$provider_kind" ]; then
  echo "--provider-kind or VELORIX_OBJECT_STORE_PROVIDER_KIND is required" >&2
  exit 64
fi
if [ -z "$attester" ]; then
  echo "--attester or VELORIX_ATTESTER is required" >&2
  exit 64
fi

mkdir -p "$(dirname "$output_file")"

python3 - \
  "$product_evidence" \
  "$product_evidence_explicit" \
  "$assessment_file" \
  "$output_file" \
  "$authority_store_id" \
  "$bucket" \
  "$s3_prefix" \
  "$provider_kind" \
  "$attester" \
  "$versioning_or_object_lock_enabled" \
  "$server_side_encryption_enabled" \
  "$backup_or_replication_configured" \
  "$lifecycle_delete_policy_reviewed" \
  "$destructive_delete_protection_reviewed" \
  "$cost_controls_reviewed" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    product_evidence,
    product_evidence_explicit,
    assessment_file,
    output_file,
    authority_store_id,
    bucket,
    s3_prefix,
    provider_kind,
    attester,
    versioning_or_object_lock_enabled,
    server_side_encryption_enabled,
    backup_or_replication_configured,
    lifecycle_delete_policy_reviewed,
    destructive_delete_protection_reviewed,
    cost_controls_reviewed,
) = sys.argv[1:]


def bool_from_flag(name: str, raw: str) -> bool:
    if raw == "1":
        return True
    if raw in {"", "0"}:
        raise SystemExit(f"{name}=1 or the matching CLI flag is required")
    raise SystemExit(f"{name} must be 1 when supplied")


use_direct_authority = bool(authority_store_id or bucket or s3_prefix) and product_evidence_explicit != "1"
product_path = None if use_direct_authority else Path(product_evidence) if product_evidence else None
store = {}
if product_path and product_path.is_file():
    with open(product_path, "r", encoding="utf-8") as f:
        product = json.load(f)
    if product.get("evidence_kind") != "velorix_product_slice_evidence":
        raise SystemExit(
            f"product evidence_kind must be velorix_product_slice_evidence: {product_evidence}"
        )
    store = product.get("object_store") or {}
    if store.get("mode") != "external-s3":
        raise SystemExit("object-store durability attestation requires object_store.mode=external-s3")
    if store.get("local_development_authority") is True:
        raise SystemExit("local development object-store authority cannot receive durability attestation")
    for field in ["authority_store_id", "bucket", "s3_prefix"]:
        if not isinstance(store.get(field), str):
            raise SystemExit(f"product evidence object_store.{field} must be a string")
    if not store.get("authority_store_id") or not store.get("bucket"):
        raise SystemExit("product evidence object_store authority_store_id and bucket are required")
    if authority_store_id and authority_store_id != store["authority_store_id"]:
        raise SystemExit("authority_store_id does not match product evidence")
    if bucket and bucket != store["bucket"]:
        raise SystemExit("bucket does not match product evidence")
    if s3_prefix and s3_prefix != store["s3_prefix"]:
        raise SystemExit("s3_prefix does not match product evidence")
else:
    store = {
        "authority_store_id": authority_store_id,
        "bucket": bucket,
        "s3_prefix": s3_prefix,
    }
    for field in ["authority_store_id", "bucket", "s3_prefix"]:
        if not isinstance(store.get(field), str) or (field != "s3_prefix" and not store[field].strip()):
            raise SystemExit(
                f"--{field.replace('_', '-')} or matching environment variable is required without product evidence"
            )

if assessment_file:
    assessment_path = Path(assessment_file)
    if not assessment_path.is_file():
        raise SystemExit(f"assessment file does not exist: {assessment_file}")
    with open(assessment_path, "r", encoding="utf-8") as f:
        assessment = json.load(f)
    if assessment.get("evidence_kind") != "velorix_object_store_durability_policy_assessment":
        raise SystemExit("assessment evidence_kind must be velorix_object_store_durability_policy_assessment")
    for field in ["authority_store_id", "bucket", "s3_prefix"]:
        if assessment.get(field) != store.get(field):
            raise SystemExit(f"assessment {field} does not match product evidence")
    if assessment.get("authority_class") == "local_single_node_docker_volume":
        raise SystemExit("local single-node Docker-volume assessment cannot receive product-complete attestation")

payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_object_store_durability_policy_attestation",
    "provider_kind": provider_kind,
    "authority_store_id": store["authority_store_id"],
    "bucket": store["bucket"],
    "s3_prefix": store["s3_prefix"],
    "versioning_or_object_lock_enabled": bool_from_flag(
        "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED",
        versioning_or_object_lock_enabled,
    ),
    "server_side_encryption_enabled": bool_from_flag(
        "VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED",
        server_side_encryption_enabled,
    ),
    "backup_or_replication_configured": bool_from_flag(
        "VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED",
        backup_or_replication_configured,
    ),
    "lifecycle_delete_policy_reviewed": bool_from_flag(
        "VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED",
        lifecycle_delete_policy_reviewed,
    ),
    "destructive_delete_protection_reviewed": bool_from_flag(
        "VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED",
        destructive_delete_protection_reviewed,
    ),
    "cost_controls_reviewed": bool_from_flag(
        "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED",
        cost_controls_reviewed,
    ),
    "attested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "attester": attester,
}

Path(output_file).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(output_file)
PY

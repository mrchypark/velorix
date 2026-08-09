#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
assess="${VELORIX_OBJECT_STORE_DURABILITY_ASSESS:-1}"
attest="${VELORIX_OBJECT_STORE_DURABILITY_ATTEST:-1}"
attach="${VELORIX_OBJECT_STORE_DURABILITY_ATTACH:-1}"
env_file="${VELORIX_OBJECT_STORE_DURABILITY_ENV:-}"
validate_only=0
input_evidence="${VELORIX_OBJECT_STORE_DURABILITY_INPUT_EVIDENCE:-}"
product_dir_cli=0
input_evidence_cli=0

usage() {
  cat <<'EOF'
Complete the object-store durability evidence path for an external S3 product slice.

Usage:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
  scripts/complete-vind-object-store-durability.sh \
    --versioning-or-object-lock-enabled \
    --server-side-encryption-enabled \
    --backup-or-replication-configured \
    --lifecycle-delete-policy-reviewed \
    --destructive-delete-protection-reviewed \
    --cost-controls-reviewed

Or use the generated product-completion env file:
  scripts/complete-vind-object-store-durability.sh \
    --env-file target/velorix-product/complete-vind-product.env \
    --output-dir target/velorix-product

Validate inputs without assessing, attesting, attaching, or editing evidence:
  scripts/complete-vind-object-store-durability.sh \
    --env-file target/velorix-product/complete-vind-product.env \
    --output-dir target/velorix-product \
    --validate-only

Step toggles:
  VELORIX_OBJECT_STORE_DURABILITY_ASSESS=1
  VELORIX_OBJECT_STORE_DURABILITY_ATTEST=1
  VELORIX_OBJECT_STORE_DURABILITY_ATTACH=1

Options:
  --env-file PATH        Source durability review inputs from a shell env file.
  --output-dir PATH      Product evidence directory.
  --input-evidence PATH  Where to write object-store-durability-input.json.
  --validate-only        Validate authority/review inputs only.

The helper creates no buckets, provider policies, or PVCs. It assesses the
current product evidence, generates an operator-reviewed durability attestation
from explicit review flags, attaches it to product-evidence.json, and refreshes
product-completion-report.json.
EOF
}

durability_args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --env-file)
      env_file="${2:-}"
      if [ -z "$env_file" ]; then
        echo "--env-file requires a path" >&2
        exit 64
      fi
      shift 2
      ;;
    --output-dir)
      product_dir="${2:-}"
      if [ -z "$product_dir" ]; then
        echo "--output-dir requires a path" >&2
        exit 64
      fi
      product_dir_cli=1
      shift 2
      ;;
    --input-evidence)
      input_evidence="${2:-}"
      if [ -z "$input_evidence" ]; then
        echo "--input-evidence requires a path" >&2
        exit 64
      fi
      input_evidence_cli=1
      shift 2
      ;;
    --validate-only)
      validate_only=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      durability_args+=("$1")
      shift
      ;;
  esac
done

case "$assess" in
  0 | 1) ;;
  *)
    echo "VELORIX_OBJECT_STORE_DURABILITY_ASSESS must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$attest" in
  0 | 1) ;;
  *)
    echo "VELORIX_OBJECT_STORE_DURABILITY_ATTEST must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$attach" in
  0 | 1) ;;
  *)
    echo "VELORIX_OBJECT_STORE_DURABILITY_ATTACH must be 0 or 1" >&2
    exit 64
    ;;
esac

cd "$repo_root"

source_env_file_preserving_overrides() {
  local env_path="$1"
  shift
  local name flag_var value_var
  for name in "$@"; do
    flag_var="__velorix_env_override_${name}"
    value_var="__velorix_env_override_value_${name}"
    if [ "${!name+x}" = x ]; then
      printf -v "$flag_var" '%s' 1
      printf -v "$value_var" '%s' "${!name}"
    else
      printf -v "$flag_var" '%s' 0
    fi
  done
  # shellcheck disable=SC1090
  source "$env_path"
  for name in "$@"; do
    flag_var="__velorix_env_override_${name}"
    value_var="__velorix_env_override_value_${name}"
    if [ "${!flag_var}" = "1" ]; then
      export "$name=${!value_var}"
    fi
    unset "$flag_var" "$value_var"
  done
}

if [ -n "$env_file" ]; then
  if [ ! -f "$env_file" ]; then
    echo "--env-file does not exist: ${env_file}" >&2
    exit 66
  fi
  source_env_file_preserving_overrides "$env_file" \
    VELORIX_VIND_PRODUCT_DIR \
    VELORIX_OBJECT_STORE_DURABILITY_INPUT_EVIDENCE \
    VELORIX_OBJECT_STORE_DURABILITY_ASSESS \
    VELORIX_OBJECT_STORE_DURABILITY_ATTEST \
    VELORIX_OBJECT_STORE_DURABILITY_ATTACH \
    VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED \
    VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED \
    VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED \
    VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED \
    VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED \
    VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED
  if [ "$product_dir_cli" = "0" ]; then
    product_dir="${VELORIX_VIND_PRODUCT_DIR:-$product_dir}"
  fi
  assess="${VELORIX_OBJECT_STORE_DURABILITY_ASSESS:-$assess}"
  attest="${VELORIX_OBJECT_STORE_DURABILITY_ATTEST:-$attest}"
  attach="${VELORIX_OBJECT_STORE_DURABILITY_ATTACH:-$attach}"
fi
case "$assess" in
  0 | 1) ;;
  *)
    echo "VELORIX_OBJECT_STORE_DURABILITY_ASSESS must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$attest" in
  0 | 1) ;;
  *)
    echo "VELORIX_OBJECT_STORE_DURABILITY_ATTEST must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$attach" in
  0 | 1) ;;
  *)
    echo "VELORIX_OBJECT_STORE_DURABILITY_ATTACH must be 0 or 1" >&2
    exit 64
    ;;
esac
if [ "$input_evidence_cli" = "0" ]; then
  input_evidence="${VELORIX_OBJECT_STORE_DURABILITY_INPUT_EVIDENCE:-${product_dir}/object-store-durability-input.json}"
fi

mkdir -p "$product_dir"
python3 - \
  "$input_evidence" \
  "${product_dir}/product-evidence.json" \
  "$validate_only" \
  "$assess" \
  "$attest" \
  "$attach" \
  "${VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED:-}" \
  "${VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED:-}" \
  "${VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED:-}" \
  "${VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED:-}" \
  "${VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED:-}" \
  "${VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED:-}" \
  "${#durability_args[@]}" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    output_path,
    product_evidence,
    validate_only,
    assess,
    attest,
    attach,
    versioning,
    encryption,
    backup,
    lifecycle,
    delete_protection,
    cost,
    cli_arg_count,
) = sys.argv[1:]

review_flags = {
    "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED": versioning,
    "VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED": encryption,
    "VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED": backup,
    "VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED": lifecycle,
    "VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED": delete_protection,
    "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED": cost,
}
missing = []
invalid = []
for name, value in review_flags.items():
    if value not in {"", "0", "1"}:
        invalid.append({"subject": name, "detail": f"{name} must be 0 or 1"})
    elif int(cli_arg_count) == 0 and value != "1":
        missing.append({"subject": name, "detail": f"{name}=1 or explicit durability CLI flag is required"})

authority_ready = False
authority = {}
path = Path(product_evidence)
if path.is_file():
    try:
        product = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        product = {}
    store = product.get("object_store") or {}
    authority = {
        "mode": store.get("mode"),
        "authority_store_id": store.get("authority_store_id"),
        "bucket": store.get("bucket"),
        "s3_prefix": store.get("s3_prefix"),
        "local_development_authority": store.get("local_development_authority"),
        "external_s3_bucket_validated": store.get("external_s3_bucket_validated"),
        "external_s3_prefix_validated": store.get("external_s3_prefix_validated"),
    }
    authority_ready = (
        store.get("mode") == "external-s3"
        and store.get("local_development_authority") is not True
        and store.get("external_s3_bucket_validated") is True
        and store.get("external_s3_prefix_validated") is True
    )
else:
    missing.append({"subject": "product_evidence", "detail": f"product evidence is required: {product_evidence}"})

if not authority_ready:
    invalid.append(
        {
            "subject": "object_store_external_authority",
            "detail": "validated nonlocal external S3/OSS authority is required before durability attestation",
        }
    )

payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_object_store_durability_input",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_evidence": product_evidence,
    "validate_only": validate_only == "1",
    "assess": assess == "1",
    "attest": attest == "1",
    "attach": attach == "1",
    "authority_ready": authority_ready,
    "authority": authority,
    "review_flags": {name: value == "1" for name, value in review_flags.items()},
    "cli_args_count": int(cli_arg_count),
    "missing": missing,
    "invalid": invalid,
    "status": "blocked" if missing or invalid else "pass",
    "creates_product_complete_evidence": False,
}
out = Path(output_path)
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(out, 0o600)
print(f"object_store_durability_input={out}")
print(f"object_store_durability_input_status={payload['status']}")
if missing or invalid:
    raise SystemExit(
        "invalid object-store durability inputs:\n"
        + "\n".join(
            f"- {issue['subject']}: {issue['detail']}" for issue in missing + invalid
        )
    )
PY

if [ "$validate_only" = "1" ]; then
  echo "validate_only=1"
  exit 0
fi

if [ "$assess" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    scripts/assess-object-store-durability-policy.sh
fi

if [ "$attest" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    scripts/attest-object-store-durability-policy.sh \
      --product-evidence "${product_dir}/product-evidence.json" \
      --assessment "${product_dir}/object-store-durability-assessment.json" \
      --output "${product_dir}/object-store-durability-attestation.json" \
      "${durability_args[@]}"
fi

if [ "$attach" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    scripts/attach-vind-object-store-durability.sh
fi

echo "object_store_durability_complete_dir=${product_dir}"

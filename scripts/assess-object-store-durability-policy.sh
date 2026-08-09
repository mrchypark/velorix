#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
output_file="${VELORIX_OBJECT_STORE_DURABILITY_ASSESSMENT_FILE:-${product_dir}/object-store-durability-assessment.json}"
external_rustfs_env="${VELORIX_EXTERNAL_RUSTFS_ENV:-${product_dir}/external-rustfs.env}"
aws_cli_image="${VELORIX_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
probe="${VELORIX_OBJECT_STORE_DURABILITY_ASSESS_PROBE:-1}"

usage() {
  cat <<'EOF'
Assess whether the product object-store authority can truthfully receive the
product-complete durability policy attestation.

Usage:
  scripts/assess-object-store-durability-policy.sh

Main overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_VIND_PRODUCT_EVIDENCE=target/velorix-product/product-evidence.json
  VELORIX_OBJECT_STORE_DURABILITY_ASSESSMENT_FILE=target/velorix-product/object-store-durability-assessment.json
  VELORIX_EXTERNAL_RUSTFS_ENV=target/velorix-product/external-rustfs.env
  VELORIX_OBJECT_STORE_DURABILITY_ASSESS_PROBE=1

This writes an assessment artifact. It deliberately does not write
object-store-durability-attestation.json, because that file is product-complete
evidence and must come from a real operator review of a real durable authority.
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
  echo "missing product evidence: ${product_evidence}" >&2
  exit 66
fi

if [ -f "$external_rustfs_env" ]; then
  # shellcheck disable=SC1090
  source "$external_rustfs_env"
fi

IFS=$'\t' read -r mode endpoint bucket s3_prefix authority_store_id region < <(
  python3 - "$product_evidence" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    product = json.load(f)
store = product.get("object_store") or {}
print(
    "\t".join(
        str(store.get(key) or "")
        for key in [
            "mode",
            "endpoint",
            "bucket",
            "s3_prefix",
            "authority_store_id",
            "region",
        ]
    )
)
PY
)

if [ "$mode" != "external-s3" ]; then
  echo "object-store durability assessment requires product evidence object_store.mode=external-s3" >&2
  exit 64
fi
if [ -z "$bucket" ] || [ -z "$authority_store_id" ]; then
  echo "product evidence is missing object-store bucket or authority_store_id" >&2
  exit 66
fi
if [ -z "$region" ]; then
  region="${AWS_REGION:-us-east-1}"
fi

work_dir="${product_dir}/object-store-durability-assessment-work"
mkdir -p "$work_dir"
versioning_json="${work_dir}/bucket-versioning.json"
versioning_err="${work_dir}/bucket-versioning.err"
encryption_json="${work_dir}/bucket-encryption.json"
encryption_err="${work_dir}/bucket-encryption.err"
lifecycle_json="${work_dir}/bucket-lifecycle.json"
lifecycle_err="${work_dir}/bucket-lifecycle.err"

printf '{}\n' >"$versioning_json"
: >"$versioning_err"
printf '{}\n' >"$encryption_json"
: >"$encryption_err"
printf '{}\n' >"$lifecycle_json"
: >"$lifecycle_err"

probe_endpoint="${VELORIX_OBJECT_STORE_DURABILITY_ASSESS_ENDPOINT:-${AWS_ENDPOINT_URL:-$endpoint}}"
docker_network="${VELORIX_OBJECT_STORE_DURABILITY_ASSESS_DOCKER_NETWORK:-}"
if [ -z "$docker_network" ] && [ -n "${VELORIX_EXTERNAL_RUSTFS_CONTAINER:-}" ]; then
  docker_network="$VELORIX_EXTERNAL_RUSTFS_CONTAINER"
  probe_endpoint="http://${VELORIX_EXTERNAL_RUSTFS_CONTAINER}:9000"
fi

run_aws_s3api() {
  local stdout_file="$1"
  local stderr_file="$2"
  shift 2
  if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    echo "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required for live durability probing" >"$stderr_file"
    return 125
  fi
  if [ -n "$docker_network" ]; then
    docker run --rm \
      --network "$docker_network" \
      -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
      -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "$probe_endpoint" \
      s3api "$@" >"$stdout_file" 2>"$stderr_file"
  else
    docker run --rm \
      -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
      -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "$probe_endpoint" \
      s3api "$@" >"$stdout_file" 2>"$stderr_file"
  fi
}

if [ "$probe" = "1" ]; then
  require docker
  run_aws_s3api "$versioning_json" "$versioning_err" \
    get-bucket-versioning --bucket "$bucket" || true
  run_aws_s3api "$encryption_json" "$encryption_err" \
    get-bucket-encryption --bucket "$bucket" || true
  run_aws_s3api "$lifecycle_json" "$lifecycle_err" \
    get-bucket-lifecycle-configuration --bucket "$bucket" || true
fi

mkdir -p "$(dirname "$output_file")"
python3 - \
  "$output_file" \
  "$product_evidence" \
  "$mode" \
  "$endpoint" \
  "$probe_endpoint" \
  "$bucket" \
  "$s3_prefix" \
  "$authority_store_id" \
  "$region" \
  "$versioning_json" \
  "$versioning_err" \
  "$encryption_json" \
  "$encryption_err" \
  "$lifecycle_json" \
  "$lifecycle_err" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    output_file,
    product_evidence,
    mode,
    endpoint,
    probe_endpoint,
    bucket,
    s3_prefix,
    authority_store_id,
    region,
    versioning_json,
    versioning_err,
    encryption_json,
    encryption_err,
    lifecycle_json,
    lifecycle_err,
) = sys.argv[1:]


def read_json(path: str) -> dict:
    try:
        text = Path(path).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return {}
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def read_error(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


versioning = read_json(versioning_json)
encryption = read_json(encryption_json)
lifecycle = read_json(lifecycle_json)
versioning_error = read_error(versioning_err)
encryption_error = read_error(encryption_err)
lifecycle_error = read_error(lifecycle_err)

versioning_enabled = versioning.get("Status") == "Enabled"
encryption_enabled = bool(encryption.get("ServerSideEncryptionConfiguration", {}).get("Rules"))
lifecycle_rules = lifecycle.get("Rules")
lifecycle_present = isinstance(lifecycle_rules, list) and bool(lifecycle_rules)

required_truths = {
    "versioning_or_object_lock_enabled": versioning_enabled,
    "server_side_encryption_enabled": encryption_enabled,
    "backup_or_replication_configured": False,
    "lifecycle_delete_policy_reviewed": False,
    "destructive_delete_protection_reviewed": False,
    "cost_controls_reviewed": False,
}
missing = [name for name, passed in required_truths.items() if not passed]

attested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_object_store_durability_policy_assessment",
    "generated_at": attested_at,
    "product_evidence": product_evidence,
    "authority_store_id": authority_store_id,
    "bucket": bucket,
    "s3_prefix": s3_prefix,
    "region": region,
    "endpoint_from_product_evidence": endpoint,
    "probe_endpoint": probe_endpoint,
    "object_store_mode": mode,
    "authority_class": "local_single_node_docker_volume"
    if "rustfs" in probe_endpoint.lower()
    or "127.0.0.1" in probe_endpoint
    or "192.168." in probe_endpoint
    else "external_s3_compatible",
    "reason": "local Docker-volume RustFS is useful for API compatibility smoke tests but is not an externally durable production object-store authority",
    "trusted_for_product_complete": False,
    "can_generate_product_complete_attestation": not missing,
    "required_truths": required_truths,
    "missing_for_product_complete": missing,
    "observed_provider_api": {
        "bucket_versioning": {
            "enabled": versioning_enabled,
            "response": versioning,
            "error": versioning_error or None,
        },
        "bucket_encryption": {
            "enabled": encryption_enabled,
            "response": encryption,
            "error": encryption_error or None,
        },
        "bucket_lifecycle": {
            "configured": lifecycle_present,
            "response": lifecycle,
            "error": lifecycle_error or None,
        },
    },
    "operator_attestation_template": {
        "schema_version": 1,
        "evidence_kind": "velorix_object_store_durability_policy_attestation",
        "provider_kind": "s3-compatible",
        "authority_store_id": authority_store_id,
        "bucket": bucket,
        "s3_prefix": s3_prefix,
        "versioning_or_object_lock_enabled": required_truths[
            "versioning_or_object_lock_enabled"
        ],
        "server_side_encryption_enabled": required_truths[
            "server_side_encryption_enabled"
        ],
        "backup_or_replication_configured": False,
        "lifecycle_delete_policy_reviewed": False,
        "destructive_delete_protection_reviewed": False,
        "cost_controls_reviewed": False,
        "attested_at": attested_at,
        "attester": "operator-required",
    },
    "notes": [
        "This assessment is not product-complete evidence.",
        "Only object-store-durability-attestation.json is accepted by the release validator.",
        "Do not mark missing fields true unless the backing provider and operator review really satisfy them.",
    ],
}

Path(output_file).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(output_file)
PY

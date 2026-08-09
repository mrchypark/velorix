#!/usr/bin/env bash
set -euo pipefail
umask 077

case "$-" in
  *x*)
    echo "Refusing to run with shell xtrace enabled because S3 credentials would be logged" >&2
    exit 64
    ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
output_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product-external-s3}"
env_file="${VELORIX_EXTERNAL_S3_ENV:-}"
endpoint="${AWS_ENDPOINT_URL:-}"
access_key="${AWS_ACCESS_KEY_ID:-}"
secret_key="${AWS_SECRET_ACCESS_KEY:-}"
session_token="${AWS_SESSION_TOKEN:-}"
region="${AWS_REGION:-us-east-1}"
bucket="${VELORIX_S3_BUCKET:-}"
prefix="${VELORIX_S3_PREFIX:-}"
authority_store_id="${VELORIX_AUTHORITY_STORE_ID:-}"
durability_attestation="${VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:-}"
allow_local_endpoint="${VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT:-0}"
run_product="${VELORIX_EXTERNAL_S3_RUN_PRODUCT:-1}"
input_evidence="${VELORIX_EXTERNAL_S3_INPUT_EVIDENCE:-}"
s3_force_path_style="${VELORIX_S3_FORCE_PATH_STYLE:-1}"
s3_credentials_secret_name="${VELORIX_S3_CREDENTIALS_SECRET_NAME:-velorix-s3-credentials}"
s3_credentials_secret_managed="${VELORIX_S3_CREDENTIALS_SECRET_MANAGED:-1}"
output_dir_cli=0
input_evidence_cli=0
run_product_cli=0

usage() {
  cat <<'EOF'
Run the vind product slice against a nonlocal S3/OSS-compatible authority.

Usage:
  AWS_ENDPOINT_URL=https://oss.example.com \
  AWS_ACCESS_KEY_ID=... \
  AWS_SECRET_ACCESS_KEY=... \
  AWS_SESSION_TOKEN=... \
  AWS_REGION=us-east-1 \
  VELORIX_S3_BUCKET=velorix-product \
  VELORIX_S3_PREFIX=product/manual-run \
  scripts/run-vind-product-external-s3.sh

Or use an env file:
  scripts/run-vind-product-external-s3.sh \
    --env-file target/velorix-product/complete-vind-product.env

Validate inputs without deploying:
  scripts/run-vind-product-external-s3.sh \
    --env-file target/velorix-product/complete-vind-product.env \
    --validate-only

Optional:
  VELORIX_EXTERNAL_S3_ENV=path/to/env-file
  --env-file path/to/env-file
  --output-dir target/velorix-product
  --input-evidence target/.../external-s3-product-input.json
  --validate-only
  VELORIX_S3_PREFIX=product/<run-id>
  VELORIX_S3_FORCE_PATH_STYLE=1
  VELORIX_S3_CREDENTIALS_SECRET_NAME=velorix-s3-credentials
  VELORIX_S3_CREDENTIALS_SECRET_MANAGED=1
  VELORIX_AUTHORITY_STORE_ID=s3://external/<bucket>/<prefix>
  VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE=target/.../object-store-durability-attestation.json
  VELORIX_EXTERNAL_S3_RUN_PRODUCT=0

This wrapper is for real external object-store authorities, not local Docker
RustFS. It rejects localhost-style endpoints by default, writes a target-backed
input evidence file, then delegates to scripts/run-vind-product.sh with
VELORIX_OBJECT_STORE_MODE=external-s3 and
VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=0.
EOF
}

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
      output_dir="${2:-}"
      if [ -z "$output_dir" ]; then
        echo "--output-dir requires a path" >&2
        exit 64
      fi
      output_dir_cli=1
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
      run_product=0
      run_product_cli=1
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

contains_placeholder() {
  case "$1" in
    *REPLACE_WITH* | *S3_OR_OSS_ENDPOINT* | *PUBLIC_HOST.* | *INGRESS_CONTROLLER* | *TLS_SECRET_NAME*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_default_secret_value() {
  local lowered
  lowered="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    rustfsadmin | minioadmin | changeme | password)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

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

cd "$repo_root"
require python3

if [ -n "$env_file" ]; then
  if [ ! -f "$env_file" ]; then
    echo "--env-file/VELORIX_EXTERNAL_S3_ENV does not exist: ${env_file}" >&2
    exit 66
  fi
  source_env_file_preserving_overrides "$env_file" \
    VELORIX_VIND_PRODUCT_DIR \
    AWS_ENDPOINT_URL \
    AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY \
    AWS_SESSION_TOKEN \
    AWS_REGION \
    VELORIX_S3_BUCKET \
    VELORIX_S3_PREFIX \
    VELORIX_S3_FORCE_PATH_STYLE \
    VELORIX_S3_CREDENTIALS_SECRET_NAME \
    VELORIX_S3_CREDENTIALS_SECRET_MANAGED \
    VELORIX_AUTHORITY_STORE_ID \
    VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE \
    VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT \
    VELORIX_EXTERNAL_S3_RUN_PRODUCT
  if [ "$output_dir_cli" = "0" ]; then
    output_dir="${VELORIX_VIND_PRODUCT_DIR:-$output_dir}"
  fi
  endpoint="${AWS_ENDPOINT_URL:-$endpoint}"
  access_key="${AWS_ACCESS_KEY_ID:-$access_key}"
  secret_key="${AWS_SECRET_ACCESS_KEY:-$secret_key}"
  session_token="${AWS_SESSION_TOKEN:-$session_token}"
  region="${AWS_REGION:-$region}"
  bucket="${VELORIX_S3_BUCKET:-$bucket}"
  prefix="${VELORIX_S3_PREFIX:-$prefix}"
  s3_force_path_style="${VELORIX_S3_FORCE_PATH_STYLE:-$s3_force_path_style}"
  s3_credentials_secret_name="${VELORIX_S3_CREDENTIALS_SECRET_NAME:-$s3_credentials_secret_name}"
  s3_credentials_secret_managed="${VELORIX_S3_CREDENTIALS_SECRET_MANAGED:-$s3_credentials_secret_managed}"
  authority_store_id="${VELORIX_AUTHORITY_STORE_ID:-$authority_store_id}"
  durability_attestation="${VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:-$durability_attestation}"
  allow_local_endpoint="${VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT:-$allow_local_endpoint}"
  if [ "$run_product_cli" = "0" ]; then
    run_product="${VELORIX_EXTERNAL_S3_RUN_PRODUCT:-$run_product}"
  fi
fi
if [ "$input_evidence_cli" = "0" ]; then
  input_evidence="${VELORIX_EXTERNAL_S3_INPUT_EVIDENCE:-${output_dir}/external-s3-product-input.json}"
fi

case "$allow_local_endpoint" in
  0 | 1) ;;
  *)
    echo "VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$run_product" in
  0 | 1) ;;
  *)
    echo "VELORIX_EXTERNAL_S3_RUN_PRODUCT must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$s3_force_path_style" in
  0 | 1) ;;
  *)
    echo "VELORIX_S3_FORCE_PATH_STYLE must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$s3_credentials_secret_managed" in
  0 | 1) ;;
  *)
    echo "VELORIX_S3_CREDENTIALS_SECRET_MANAGED must be 0 or 1" >&2
    exit 64
    ;;
esac
python3 - "$s3_credentials_secret_name" <<'PY'
import re
import sys

name = sys.argv[1]
if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", name):
    raise SystemExit("VELORIX_S3_CREDENTIALS_SECRET_NAME must be a valid Kubernetes Secret name")
PY

if [ -z "$endpoint" ]; then
  echo "AWS_ENDPOINT_URL is required" >&2
  exit 64
fi
if contains_placeholder "$endpoint"; then
  echo "AWS_ENDPOINT_URL still contains a placeholder" >&2
  exit 64
fi
if [ "$s3_credentials_secret_managed" = "0" ]; then
  if [ -n "$access_key" ] || [ -n "$secret_key" ] || [ -n "$session_token" ]; then
    echo "VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0 uses an existing Kubernetes Secret; unset AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN" >&2
    exit 64
  fi
elif [ -z "$access_key" ] || [ -z "$secret_key" ]; then
  echo "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required" >&2
  exit 64
elif contains_placeholder "$access_key" || is_default_secret_value "$access_key"; then
  echo "AWS_ACCESS_KEY_ID is placeholder or known development default" >&2
  exit 64
elif contains_placeholder "$secret_key" || is_default_secret_value "$secret_key"; then
  echo "AWS_SECRET_ACCESS_KEY is placeholder or known development default" >&2
  exit 64
elif contains_placeholder "$session_token"; then
  echo "AWS_SESSION_TOKEN still contains a placeholder" >&2
  exit 64
fi
if [ -z "$bucket" ]; then
  echo "VELORIX_S3_BUCKET is required" >&2
  exit 64
fi
if contains_placeholder "$bucket"; then
  echo "VELORIX_S3_BUCKET still contains a placeholder" >&2
  exit 64
fi
if [ -z "$prefix" ]; then
  if [ "$run_product" = "0" ]; then
    echo "VELORIX_S3_PREFIX is required for --validate-only so validation and execution use the same authority" >&2
    exit 64
  fi
  prefix="product/${run_id}"
fi
if contains_placeholder "$prefix"; then
  echo "VELORIX_S3_PREFIX still contains a placeholder" >&2
  exit 64
fi
if [ "${VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY:-0}" = "1" ]; then
  echo "run-vind-product-external-s3.sh refuses VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1" >&2
  exit 64
fi
if [ -n "$durability_attestation" ] && [ ! -f "$durability_attestation" ]; then
  echo "object-store durability attestation does not exist: ${durability_attestation}" >&2
  exit 66
fi

IFS=$'\t' read -r normalized_endpoint normalized_bucket normalized_prefix normalized_authority < <(
  python3 - "$endpoint" "$bucket" "$prefix" "$authority_store_id" "$allow_local_endpoint" <<'PY'
import ipaddress
import re
import sys
from urllib.parse import urlparse

endpoint, bucket, prefix, authority, allow_local = sys.argv[1:]
allow_local = allow_local == "1"
parsed = urlparse(endpoint)
if parsed.scheme not in {"http", "https"} or not parsed.netloc:
    raise SystemExit("AWS_ENDPOINT_URL must be an http(s) URL with a host")
if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
    raise SystemExit("AWS_ENDPOINT_URL must be the S3/OSS service endpoint only, without bucket, prefix, query, or fragment")
host = parsed.hostname or ""
local_names = {"localhost", "host.docker.internal", "kubernetes.docker.internal"}
is_local = host.lower() in local_names
try:
    ip = ipaddress.ip_address(host)
    if (
        ip.is_loopback
        or ip.is_link_local
        or ip.is_private
        or ip.is_unspecified
        or ip.is_multicast
        or ip.is_reserved
    ):
        is_local = True
except ValueError:
    pass
if is_local and not allow_local:
    raise SystemExit(
        "AWS_ENDPOINT_URL looks like a local development endpoint; use "
        "scripts/run-vind-product-external-rustfs.sh for local RustFS or set "
        "VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT=1 only for diagnostics"
    )
if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
    raise SystemExit("VELORIX_S3_BUCKET must be a valid S3 bucket name")
prefix = prefix.strip("/")
if not prefix or prefix.startswith(".") or ".." in prefix.split("/"):
    raise SystemExit("VELORIX_S3_PREFIX must be a nonempty safe object prefix")
if any(part == "" for part in prefix.split("/")):
    raise SystemExit("VELORIX_S3_PREFIX must not contain empty path segments")
if not authority:
    authority = f"s3://external/{bucket}/{prefix}"
expected = f"s3://external/{bucket}/{prefix}"
if authority != expected:
    raise SystemExit(
        f"VELORIX_AUTHORITY_STORE_ID must equal {expected} for this wrapper"
    )
print(f"{endpoint.rstrip('/')}\t{bucket}\t{prefix}\t{authority}")
PY
)

mkdir -p "$output_dir"
python3 - \
  "$input_evidence" \
  "$normalized_endpoint" \
  "$normalized_bucket" \
  "$normalized_prefix" \
  "$normalized_authority" \
  "$region" \
  "$s3_force_path_style" \
  "$s3_credentials_secret_name" \
  "$s3_credentials_secret_managed" \
  "$access_key" \
  "$secret_key" \
  "$session_token" \
  "$durability_attestation" \
  "$run_product" <<'PY'
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    path,
    endpoint,
    bucket,
    prefix,
    authority,
    region,
    s3_force_path_style,
    s3_credentials_secret_name,
    s3_credentials_secret_managed,
    access_key,
    secret_key,
    session_token,
    durability_attestation,
    run_product,
) = sys.argv[1:]
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_external_s3_product_input",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "endpoint": endpoint,
    "bucket": bucket,
    "s3_prefix": prefix,
    "authority_store_id": authority,
    "region": region,
    "force_path_style": s3_force_path_style == "1",
    "credentials_source": "managed-env-secret" if s3_credentials_secret_managed == "1" else "existing-kubernetes-secret",
    "credentials_secret_name": s3_credentials_secret_name,
    "credentials_secret_managed": s3_credentials_secret_managed == "1",
    "credentials_sha256": hashlib.sha256(
        f"{access_key}\n{secret_key}\n{session_token}".encode("utf-8")
    ).hexdigest()
    if s3_credentials_secret_managed == "1"
    else None,
    "local_development_authority": False,
    "durability_attestation_file": durability_attestation or None,
    "delegates_to": "scripts/run-vind-product.sh",
    "run_product": run_product == "1",
}
path = Path(path)
path.parent.mkdir(parents=True, exist_ok=True)
with path.open("w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY

if [ "$run_product" != "1" ]; then
  cat <<EOF
external S3 product input validated
input_evidence=${input_evidence}
authority_store_id=${normalized_authority}
run_product=0
EOF
  exit 0
fi

env \
  VELORIX_VIND_PRODUCT_DIR="$output_dir" \
  VELORIX_OBJECT_STORE_MODE=external-s3 \
  VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=0 \
  VELORIX_AUTHORITY_STORE_ID="$normalized_authority" \
  VELORIX_S3_BUCKET="$normalized_bucket" \
  VELORIX_S3_PREFIX="$normalized_prefix" \
  VELORIX_S3_FORCE_PATH_STYLE="$s3_force_path_style" \
  VELORIX_S3_CREDENTIALS_SECRET_NAME="$s3_credentials_secret_name" \
  VELORIX_S3_CREDENTIALS_SECRET_MANAGED="$s3_credentials_secret_managed" \
  AWS_ENDPOINT_URL="$normalized_endpoint" \
  AWS_ACCESS_KEY_ID="$access_key" \
  AWS_SECRET_ACCESS_KEY="$secret_key" \
  AWS_SESSION_TOKEN="$session_token" \
  AWS_REGION="$region" \
  VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE="$durability_attestation" \
  scripts/run-vind-product.sh

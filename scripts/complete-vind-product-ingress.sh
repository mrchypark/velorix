#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
auth_env_file="${VELORIX_API_AUTH_ENV:-${product_dir}/api-auth.env}"
apply_ingress="${VELORIX_PRODUCT_INGRESS_APPLY:-1}"
attest_ingress="${VELORIX_PRODUCT_INGRESS_ATTEST:-1}"
attach_ingress="${VELORIX_PRODUCT_INGRESS_ATTACH:-1}"
env_file="${VELORIX_PRODUCT_INGRESS_ENV:-}"
validate_only=0
input_evidence="${VELORIX_PRODUCT_INGRESS_INPUT_EVIDENCE:-}"
product_dir_cli=0

usage() {
  cat <<'EOF'
Complete the public ingress/TLS/auth evidence path for a vind product slice.

Usage:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
  VELORIX_PRODUCT_INGRESS_HOST=velorix.example.com \
  VELORIX_PRODUCT_INGRESS_CLASS=nginx \
  VELORIX_PRODUCT_INGRESS_TLS_SECRET=velorix-api-public-tls \
  VELORIX_INGRESS_ENDPOINT_URL=https://velorix.example.com \
  VELORIX_INGRESS_CONTROLLER=nginx \
  scripts/complete-vind-product-ingress.sh

Or use the generated product-completion env file:
  scripts/complete-vind-product-ingress.sh \
    --env-file target/velorix-product/complete-vind-product.env \
    --output-dir target/velorix-product

Validate inputs without applying Ingress, calling HTTPS, or attaching evidence:
  scripts/complete-vind-product-ingress.sh \
    --env-file target/velorix-product/complete-vind-product.env \
    --output-dir target/velorix-product \
    --validate-only

Step toggles:
  VELORIX_PRODUCT_INGRESS_APPLY=1
  VELORIX_PRODUCT_INGRESS_ATTEST=1
  VELORIX_PRODUCT_INGRESS_ATTACH=1

Set VELORIX_PRODUCT_INGRESS_APPLY=0 when the public/enterprise Ingress,
DNS, and TLS Secret are managed outside this helper. In that mode the helper
attests and attaches the supplied HTTPS endpoint without requiring an Ingress
class or TLS Secret name.

Options:
  --env-file PATH        Source ingress inputs from a shell env file.
  --output-dir PATH      Product evidence directory.
  --input-evidence PATH  Where to write the redacted ingress input evidence.
  --validate-only        Validate inputs and write product-ingress-input.json only.

The helper creates no PVCs. It can apply the networking.k8s.io/v1 Ingress,
generate public ingress/TLS/auth attestation from the external HTTPS endpoint,
attach that attestation to product-evidence.json, and refresh
product-completion-report.json.
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
      echo "unknown option: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

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
    VELORIX_API_AUTH_ENV \
    VELORIX_API_BEARER_TOKEN \
    VELORIX_ADMIN_BEARER_TOKEN \
    VELORIX_API_AUTH_HEADER \
    VELORIX_ADMIN_AUTH_HEADER \
    VELORIX_PRODUCT_INGRESS_HOST \
    VELORIX_PRODUCT_INGRESS_CLASS \
    VELORIX_PRODUCT_INGRESS_TLS_SECRET \
    VELORIX_PRODUCT_INGRESS_APPLY \
    VELORIX_PRODUCT_INGRESS_ATTEST \
    VELORIX_PRODUCT_INGRESS_ATTACH \
    VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS \
    VELORIX_PRODUCT_INGRESS_WAIT_INTERVAL_SECONDS \
    VELORIX_INGRESS_ENDPOINT_URL \
    VELORIX_INGRESS_CONTROLLER \
    VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS \
    VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS
  if [ "$product_dir_cli" = "0" ]; then
    product_dir="${VELORIX_VIND_PRODUCT_DIR:-$product_dir}"
  fi
  apply_ingress="${VELORIX_PRODUCT_INGRESS_APPLY:-$apply_ingress}"
  attest_ingress="${VELORIX_PRODUCT_INGRESS_ATTEST:-$attest_ingress}"
  attach_ingress="${VELORIX_PRODUCT_INGRESS_ATTACH:-$attach_ingress}"
fi
auth_env_file="${VELORIX_API_AUTH_ENV:-${product_dir}/api-auth.env}"
if [ -z "$input_evidence" ]; then
  input_evidence="${product_dir}/product-ingress-input.json"
fi

case "$apply_ingress" in
  0 | 1) ;;
  *)
    echo "VELORIX_PRODUCT_INGRESS_APPLY must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$attest_ingress" in
  0 | 1) ;;
  *)
    echo "VELORIX_PRODUCT_INGRESS_ATTEST must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$attach_ingress" in
  0 | 1) ;;
  *)
    echo "VELORIX_PRODUCT_INGRESS_ATTACH must be 0 or 1" >&2
    exit 64
    ;;
esac

mkdir -p "$product_dir"
python3 - \
  "$input_evidence" \
  "$product_dir" \
  "${VELORIX_PRODUCT_INGRESS_HOST:-}" \
  "${VELORIX_PRODUCT_INGRESS_CLASS:-${VELORIX_INGRESS_CONTROLLER:-}}" \
  "${VELORIX_PRODUCT_INGRESS_TLS_SECRET:-}" \
  "${VELORIX_INGRESS_ENDPOINT_URL:-}" \
  "${VELORIX_INGRESS_CONTROLLER:-}" \
  "$apply_ingress" \
  "$attest_ingress" \
  "$attach_ingress" \
  "$validate_only" \
  "$auth_env_file" \
  "${VELORIX_API_BEARER_TOKEN:-}" \
  "${VELORIX_ADMIN_BEARER_TOKEN:-}" \
  "${VELORIX_API_AUTH_HEADER:-}" \
  "${VELORIX_ADMIN_AUTH_HEADER:-}" <<'PY'
import json
import os
import re
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

(
    output_path,
    product_dir,
    host,
    ingress_class,
    tls_secret,
    endpoint_url,
    ingress_controller,
    apply_ingress,
    attest_ingress,
    attach_ingress,
    validate_only,
    auth_env_file,
    api_token,
    admin_token,
    api_auth_header,
    admin_auth_header,
) = sys.argv[1:]
PLACEHOLDER_MARKERS = (
    "REPLACE_WITH",
    "PUBLIC_HOST.",
    "INGRESS_CONTROLLER",
    "TLS_SECRET_NAME",
)
HOSTNAME_PATTERN = re.compile(
    r"^(?=.{1,253}$)([A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
)


def has_placeholder(value):
    return any(marker in (value or "") for marker in PLACEHOLDER_MARKERS)


def add_issue(items, subject, detail):
    items.append({"subject": subject, "detail": detail})


def bearer_from_header(header):
    match = re.match(r"^\s*authorization\s*:\s*Bearer\s+(.+?)\s*$", header or "", re.IGNORECASE)
    return match.group(1) if match else ""


def parse_env_file(path):
    values = {}
    if not path:
        return values
    env_path = Path(path)
    if not env_path.is_file():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Z0-9_]+", key):
            continue
        try:
            parsed = shlex.split(value, posix=True)
        except ValueError:
            continue
        values[key] = parsed[0] if parsed else ""
    return values


auth_env_values = parse_env_file(auth_env_file)
api_token_available = bool(api_token or bearer_from_header(api_auth_header))
admin_token_available = bool(admin_token or bearer_from_header(admin_auth_header))
auth_env_api_token_available = bool(
    auth_env_values.get("VELORIX_API_BEARER_TOKEN")
    or bearer_from_header(auth_env_values.get("VELORIX_API_AUTH_HEADER", ""))
)
auth_env_admin_token_available = bool(
    auth_env_values.get("VELORIX_ADMIN_BEARER_TOKEN")
    or bearer_from_header(auth_env_values.get("VELORIX_ADMIN_AUTH_HEADER", ""))
)

missing = []
invalid = []
required_inputs = [
    ("VELORIX_PRODUCT_INGRESS_HOST", host),
    ("VELORIX_INGRESS_ENDPOINT_URL", endpoint_url),
    ("VELORIX_INGRESS_CONTROLLER", ingress_controller),
]
if apply_ingress == "1":
    required_inputs.extend(
        [
            ("VELORIX_PRODUCT_INGRESS_CLASS", ingress_class),
            ("VELORIX_PRODUCT_INGRESS_TLS_SECRET", tls_secret),
        ]
    )

for name, value in required_inputs:
    if not value:
        add_issue(missing, name, f"{name} is required for public ingress completion")
    elif has_placeholder(value):
        add_issue(invalid, name, f"{name} still contains a placeholder")

if host and ("://" in host or "/" in host):
    add_issue(invalid, "VELORIX_PRODUCT_INGRESS_HOST", "host must not include scheme or path")
elif host and not has_placeholder(host) and not HOSTNAME_PATTERN.fullmatch(host):
    add_issue(invalid, "VELORIX_PRODUCT_INGRESS_HOST", "host must be a valid DNS hostname")

parsed = urlparse(endpoint_url)
if endpoint_url and (parsed.scheme != "https" or not parsed.netloc):
    add_issue(invalid, "VELORIX_INGRESS_ENDPOINT_URL", "ingress endpoint must be an https URL")
if endpoint_url and (parsed.query or parsed.fragment):
    add_issue(invalid, "VELORIX_INGRESS_ENDPOINT_URL", "ingress endpoint must not include query parameters or a fragment")
if endpoint_url and host and parsed.hostname and parsed.hostname != host:
    add_issue(invalid, "VELORIX_INGRESS_ENDPOINT_URL", "ingress endpoint host must match VELORIX_PRODUCT_INGRESS_HOST")

if attest_ingress == "1":
    if not api_token_available and not auth_env_api_token_available:
        add_issue(
            missing,
            "VELORIX_API_BEARER_TOKEN",
            "public ingress attestation requires VELORIX_API_BEARER_TOKEN, VELORIX_API_AUTH_HEADER, or api-auth.env with a data-plane bearer token",
        )
    if not admin_token_available and not auth_env_admin_token_available:
        add_issue(
            missing,
            "VELORIX_ADMIN_BEARER_TOKEN",
            "public ingress attestation requires VELORIX_ADMIN_BEARER_TOKEN, VELORIX_ADMIN_AUTH_HEADER, or api-auth.env with an admin bearer token",
        )

payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_product_ingress_input",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_dir": product_dir,
    "host": host or None,
    "ingress_class": ingress_class or None,
    "tls_secret": tls_secret or None,
    "endpoint_url": endpoint_url or None,
    "ingress_controller": ingress_controller or None,
    "apply_ingress": apply_ingress == "1",
    "existing_ingress_mode": apply_ingress == "0",
    "attest_ingress": attest_ingress == "1",
    "attach_ingress": attach_ingress == "1",
    "validate_only": validate_only == "1",
    "auth_env_file": auth_env_file or None,
    "auth_token_source": {
        "api_token_from_environment": api_token_available,
        "admin_token_from_environment": admin_token_available,
        "api_token_from_auth_env": auth_env_api_token_available,
        "admin_token_from_auth_env": auth_env_admin_token_available,
        "auth_env_exists": Path(auth_env_file).is_file() if auth_env_file else False,
    },
    "missing": missing,
    "invalid": invalid,
    "status": "blocked" if missing or invalid else "pass",
    "creates_product_complete_evidence": False,
}
path = Path(output_path)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(path, 0o600)
print(f"product_ingress_input={path}")
print(f"product_ingress_input_status={payload['status']}")
if missing or invalid:
    raise SystemExit(
        "invalid public ingress inputs:\n"
        + "\n".join(
            f"- {issue['subject']}: {issue['detail']}" for issue in missing + invalid
        )
    )
PY

if [ "$validate_only" = "1" ]; then
  echo "validate_only=1"
  exit 0
fi

if [ "$apply_ingress" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    scripts/apply-vind-product-ingress.sh
fi

if [ "$attest_ingress" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    scripts/attest-vind-product-ingress.sh
fi

if [ "$attach_ingress" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    scripts/attach-vind-product-ingress.sh
fi

echo "product_ingress_complete_dir=${product_dir}"

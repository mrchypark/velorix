#!/usr/bin/env bash
set -euo pipefail
umask 077

case "$-" in
  *x*)
    echo "Refusing to run with shell xtrace enabled because auth secrets would be logged" >&2
    exit 64
    ;;
esac

endpoint_url="${VELORIX_INGRESS_ENDPOINT_URL:-}"
api_token="${VELORIX_API_BEARER_TOKEN:-}"
admin_token="${VELORIX_ADMIN_BEARER_TOKEN:-}"
ingress_controller="${VELORIX_INGRESS_CONTROLLER:-}"
external_hostname="${VELORIX_INGRESS_EXTERNAL_HOSTNAME:-}"
attester="${VELORIX_ATTESTER:-$(id -un 2>/dev/null || printf 'operator')}"
output_file="${VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE:-target/velorix-product/ingress-tls-auth-attestation.json}"
local_scratch_dir="${VELORIX_LOCAL_SCRATCH_DIR:-target/velorix-product/scratch}"
ready_timeout_seconds="${VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS:-120}"
ready_interval_seconds="${VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS:-5}"

usage() {
  cat <<'EOF'
Generate product-complete ingress/TLS/auth attestation evidence.

Usage:
  scripts/attest-ingress-tls-auth.sh --endpoint https://velorix.example.com \
    --api-token <data-plane-token> \
    --admin-token <admin-token> \
    --ingress-controller <name> \
    --output target/velorix-product/ingress-tls-auth-attestation.json

Environment equivalents:
  VELORIX_INGRESS_ENDPOINT_URL
  VELORIX_API_BEARER_TOKEN
  VELORIX_ADMIN_BEARER_TOKEN
  VELORIX_INGRESS_CONTROLLER
  VELORIX_INGRESS_EXTERNAL_HOSTNAME
  VELORIX_ATTESTER
  VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE
  VELORIX_LOCAL_SCRATCH_DIR
  VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS
  VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS

The endpoint must be an HTTPS external/public or enterprise ingress boundary.
Localhost and Kubernetes service DNS are intentionally rejected by
scripts/run-vind-product.sh when this evidence is consumed.

The admin token is required as an operator input to attest that the deployment
has a separate admin credential. This script proves separation by verifying
that the data-plane token is rejected on the standing-runtime admin route.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --endpoint)
      endpoint_url="${2:-}"
      shift 2
      ;;
    --api-token)
      api_token="${2:-}"
      shift 2
      ;;
    --admin-token)
      admin_token="${2:-}"
      shift 2
      ;;
    --ingress-controller)
      ingress_controller="${2:-}"
      shift 2
      ;;
    --external-hostname)
      external_hostname="${2:-}"
      shift 2
      ;;
    --attester)
      attester="${2:-}"
      shift 2
      ;;
    --output)
      output_file="${2:-}"
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

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

require curl
require openssl
require python3

if [ -z "$endpoint_url" ]; then
  echo "--endpoint or VELORIX_INGRESS_ENDPOINT_URL is required" >&2
  exit 64
fi
if [ -z "$api_token" ]; then
  echo "--api-token or VELORIX_API_BEARER_TOKEN is required" >&2
  exit 64
fi
if [ -z "$admin_token" ]; then
  echo "--admin-token or VELORIX_ADMIN_BEARER_TOKEN is required" >&2
  exit 64
fi
if [ -z "$ingress_controller" ]; then
  echo "--ingress-controller or VELORIX_INGRESS_CONTROLLER is required" >&2
  exit 64
fi
case "$ready_timeout_seconds" in
  '' | *[!0-9]*)
    echo "VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS must be a non-negative integer" >&2
    exit 64
    ;;
esac
case "$ready_interval_seconds" in
  '' | *[!0-9]* | 0)
    echo "VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS must be a positive integer" >&2
    exit 64
    ;;
esac

parsed_endpoint="$(
  python3 - "$endpoint_url" "$external_hostname" <<'PY'
import ipaddress
import sys
from urllib.parse import urlparse

endpoint, external_hostname = sys.argv[1:]
parsed = urlparse(endpoint)
errors = []
if parsed.scheme != "https" or not parsed.hostname:
    errors.append("endpoint must be an https URL with a hostname")
else:
    host = parsed.hostname.lower()
    if host in {"localhost"} or host.endswith(".svc") or host.endswith(".svc.cluster.local"):
        errors.append("endpoint must not point at localhost or Kubernetes service DNS")
    try:
        if ipaddress.ip_address(host).is_loopback:
            errors.append("endpoint must not point at a loopback IP")
    except ValueError:
        pass
if parsed.query or parsed.fragment:
    errors.append("endpoint must not include query parameters or a fragment")
if errors:
    sys.stderr.write("; ".join(errors) + "\n")
    raise SystemExit(64)
port = parsed.port or 443
base = f"https://{parsed.hostname}"
if parsed.port:
    base += f":{parsed.port}"
path = parsed.path.rstrip("/")
if path:
    base += path
hostname = external_hostname or parsed.hostname
print(f"{base}\t{parsed.hostname}\t{port}\t{hostname}")
PY
)"
IFS=$'\t' read -r normalized_endpoint host port inferred_hostname <<<"$parsed_endpoint"

tmpdir="${local_scratch_dir}/ingress-tls-auth-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$tmpdir"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

cert_file="${tmpdir}/ingress-cert.pem"
fetch_certificate() {
  local candidate="${cert_file}.tmp"
  if printf '' | openssl s_client -connect "${host}:${port}" -servername "$host" -showcerts 2>/dev/null \
    | openssl x509 -outform PEM >"$candidate" 2>/dev/null; then
    mv "$candidate" "$cert_file"
    return 0
  fi
  rm -f "$candidate"
  return 1
}

wait_for_certificate() {
  local deadline=$((SECONDS + ready_timeout_seconds))
  while ! fetch_certificate; do
    if [ "$ready_timeout_seconds" = "0" ] || [ "$SECONDS" -ge "$deadline" ]; then
      echo "failed to fetch a TLS certificate from ${host}:${port} within ${ready_timeout_seconds}s" >&2
      return 1
    fi
    sleep "$ready_interval_seconds"
  done
}

wait_for_certificate
cert_der="${tmpdir}/ingress-cert.der"
openssl x509 -in "$cert_file" -outform DER >"$cert_der"
tls_certificate_sha256="$(python3 - "$cert_der" <<'PY'
import hashlib
import sys

with open(sys.argv[1], "rb") as f:
    print("sha256:" + hashlib.sha256(f.read()).hexdigest())
PY
)"
tls_certificate_issuer="$(openssl x509 -in "$cert_file" -noout -issuer | sed 's/^issuer=//')"

check_status() {
  local expected="$1"
  local label="$2"
  local output="$3"
  shift 3
  local status
  status="$(curl -sS -o "$output" -w '%{http_code}' "$@")"
  if [ "$status" != "$expected" ]; then
    echo "expected ${label} to return ${expected}, got ${status}" >&2
    cat "$output" >&2 || true
    exit 1
  fi
}

wait_for_status() {
  local expected="$1"
  local label="$2"
  local output="$3"
  shift 3
  local deadline=$((SECONDS + ready_timeout_seconds))
  local status
  while :; do
    status="$(curl -sS -o "$output" -w '%{http_code}' "$@" || printf 'curl-failed')"
    if [ "$status" = "$expected" ]; then
      return 0
    fi
    if [ "$ready_timeout_seconds" = "0" ] || [ "$SECONDS" -ge "$deadline" ]; then
      echo "expected ${label} to return ${expected}, got ${status} within ${ready_timeout_seconds}s" >&2
      cat "$output" >&2 || true
      return 1
    fi
    sleep "$ready_interval_seconds"
  done
}

wait_for_status 401 "missing bearer token" "${tmpdir}/missing-token.json" \
  -X POST "${normalized_endpoint}/v1/relations/scores-default"
check_status 401 "wrong bearer token" "${tmpdir}/wrong-token.json" \
  -X POST "${normalized_endpoint}/v1/relations/scores-default" \
  -H "authorization: Bearer definitely-wrong-token"
check_status 401 "missing bearer token on admin route" "${tmpdir}/missing-token-admin.json" \
  "${normalized_endpoint}/v1/standing-runtime/owners"
check_status 401 "wrong bearer token on admin route" "${tmpdir}/wrong-token-admin.json" \
  "${normalized_endpoint}/v1/standing-runtime/owners" \
  -H "authorization: Bearer definitely-wrong-token"
check_status 401 "data-plane token on admin route" "${tmpdir}/data-token-admin.json" \
  "${normalized_endpoint}/v1/standing-runtime/owners" \
  -H "authorization: Bearer ${api_token}"
check_status 200 "admin token on admin route" "${tmpdir}/admin-token-admin.json" \
  "${normalized_endpoint}/v1/standing-runtime/owners" \
  -H "authorization: Bearer ${admin_token}"
curl -fsS \
  -H "authorization: Bearer ${api_token}" \
  "${normalized_endpoint}/v1/openapi.json" \
  >"${tmpdir}/openapi.json"

mkdir -p "$(dirname "$output_file")"
python3 - \
  "$output_file" \
  "$normalized_endpoint" \
  "$inferred_hostname" \
  "$ingress_controller" \
  "$tls_certificate_sha256" \
  "$tls_certificate_issuer" \
  "$attester" \
  "$ready_timeout_seconds" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

(
    output_file,
    endpoint_url,
    external_hostname,
    ingress_controller,
    tls_certificate_sha256,
    tls_certificate_issuer,
    attester,
    ready_timeout_seconds,
) = sys.argv[1:]

attestation = {
    "schema_version": 1,
    "evidence_kind": "velorix_ingress_tls_auth_attestation",
    "endpoint_url": endpoint_url,
    "external_hostname": external_hostname,
    "ingress_controller": ingress_controller,
    "transport_security": "external-or-enterprise-tls",
    "public_ingress_attestation": True,
    "trusted_for_product_complete": True,
    "tls_enabled": True,
    "tls_certificate_sha256": tls_certificate_sha256,
    "tls_certificate_issuer": tls_certificate_issuer,
    "auth_enforced": True,
    "missing_token_rejected": True,
    "wrong_token_rejected": True,
    "admin_auth_separate": True,
    "admin_route_missing_token_rejected": True,
    "admin_route_wrong_token_rejected": True,
    "admin_token_accepted_on_admin_route": True,
    "data_plane_token_rejected_on_admin_route": True,
    "attested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "attester": attester,
    "ready_timeout_seconds": int(ready_timeout_seconds),
}

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(attestation, f, indent=2, sort_keys=True)
    f.write("\n")
os.chmod(output_file, 0o600)
PY

echo "wrote ${output_file}"

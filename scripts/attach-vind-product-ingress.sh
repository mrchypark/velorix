#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
attestation_file="${VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE:-${product_dir}/ingress-tls-auth-attestation.json}"
output_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE_OUT:-$product_evidence}"
report="${VELORIX_VIND_PRODUCT_COMPLETION_REPORT:-${product_dir}/product-completion-report.json}"
refresh_report="${VELORIX_ATTACH_INGRESS_REFRESH_REPORT:-1}"

usage() {
  cat <<'EOF'
Attach public ingress/TLS/auth attestation to an existing vind product slice.

Usage:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
  scripts/attach-vind-product-ingress.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_VIND_PRODUCT_EVIDENCE=target/velorix-product/product-evidence.json
  VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE=target/velorix-product/ingress-tls-auth-attestation.json
  VELORIX_VIND_PRODUCT_EVIDENCE_OUT=target/velorix-product/product-evidence.json
  VELORIX_ATTACH_INGRESS_REFRESH_REPORT=1

This helper does not create Ingress, TLS Secret, DNS, PVC, or attestation
evidence. It consumes an existing velorix_ingress_tls_auth_attestation generated
by scripts/attest-vind-product-ingress.sh or scripts/attest-ingress-tls-auth.sh,
copies it beside product-evidence.json as ingress-tls-auth-attestation.json, and
updates api.auth.ingress_tls_auth_attestation in product-evidence.json.
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
    echo "VELORIX_ATTACH_INGRESS_REFRESH_REPORT must be 0 or 1" >&2
    exit 64
    ;;
esac

if [ ! -f "$product_evidence" ]; then
  echo "missing product evidence: ${product_evidence}" >&2
  exit 66
fi
if [ ! -f "$attestation_file" ]; then
  echo "missing ingress/TLS/auth attestation: ${attestation_file}" >&2
  exit 66
fi

python3 - "$product_evidence" "$attestation_file" "$output_evidence" <<'PY'
import ipaddress
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

product_path = Path(sys.argv[1])
attestation_path = Path(sys.argv[2])
output_path = Path(sys.argv[3])
product_dir = output_path.parent
sibling_path = product_dir / "ingress-tls-auth-attestation.json"

with product_path.open("r", encoding="utf-8") as f:
    product = json.load(f)
with attestation_path.open("r", encoding="utf-8") as f:
    attestation = json.load(f)

errors = []
if product.get("evidence_kind") != "velorix_product_slice_evidence":
    errors.append("product evidence_kind must be velorix_product_slice_evidence")
if (((product.get("api") or {}).get("auth") or {}).get("mode")) != "bearer-token":
    errors.append("product api.auth.mode must be bearer-token")
if attestation.get("schema_version") != 1:
    errors.append("attestation schema_version must be 1")
if attestation.get("evidence_kind") != "velorix_ingress_tls_auth_attestation":
    errors.append("attestation evidence_kind must be velorix_ingress_tls_auth_attestation")
for field in ["endpoint_url", "ingress_controller", "external_hostname", "transport_security"]:
    if not isinstance(attestation.get(field), str) or not attestation[field].strip():
        errors.append(f"attestation {field} must be a nonempty string")
endpoint = urlparse(str(attestation.get("endpoint_url", "")))
if endpoint.scheme != "https" or not endpoint.hostname:
    errors.append("attestation endpoint_url must be an https URL with a hostname")
else:
    host = endpoint.hostname.lower()
    if host in {"localhost"} or host.endswith(".svc") or host.endswith(".svc.cluster.local"):
        errors.append("attestation endpoint_url must not point at localhost or Kubernetes service DNS")
    try:
        if ipaddress.ip_address(host).is_loopback:
            errors.append("attestation endpoint_url must not point at a loopback IP")
    except ValueError:
        pass
external_hostname = str(attestation.get("external_hostname", "")).lower()
if external_hostname in {"localhost"} or external_hostname.endswith(".svc") or external_hostname.endswith(".svc.cluster.local"):
    errors.append("attestation external_hostname must not be localhost or Kubernetes service DNS")
transport_security = str(attestation.get("transport_security", "")).lower()
if any(marker in transport_security for marker in ["self-signed", "generated-local", "local-only"]):
    errors.append("attestation transport_security must describe an external/public or enterprise TLS boundary")
issuer = str(attestation.get("tls_certificate_issuer", "")).lower()
if any(marker in issuer for marker in ["self-signed", "generated-local", "velorix-api.local"]):
    errors.append("attestation tls_certificate_issuer must not describe the generated local smoke certificate")
for field in [
    "public_ingress_attestation",
    "trusted_for_product_complete",
    "tls_enabled",
    "auth_enforced",
    "missing_token_rejected",
    "wrong_token_rejected",
    "admin_auth_separate",
    "admin_route_missing_token_rejected",
    "admin_route_wrong_token_rejected",
    "data_plane_token_rejected_on_admin_catalog_route",
    "admin_token_accepted_on_admin_route",
    "data_plane_token_rejected_on_admin_route",
]:
    if attestation.get(field) is not True:
        errors.append(f"attestation {field} must be true")
if attestation.get("tls_enabled") is True and not (
    attestation.get("tls_certificate_sha256") or attestation.get("tls_certificate_issuer")
):
    errors.append("attestation tls_certificate_sha256 or tls_certificate_issuer is required when tls_enabled=true")
attested_at_raw = attestation.get("attested_at")
if not (attested_at_raw and attestation.get("attester")):
    errors.append("attestation attested_at and attester are required")
else:
    try:
        attested_at = datetime.fromisoformat(str(attested_at_raw).replace("Z", "+00:00"))
        if attested_at.tzinfo is None:
            errors.append("attestation attested_at must include timezone")
        else:
            now = datetime.now(timezone.utc)
            if attested_at > now + timedelta(minutes=15):
                errors.append("attestation attested_at must not be more than 15 minutes in the future")
            if now - attested_at > timedelta(hours=24):
                errors.append("attestation attested_at must be no older than 24 hours")
    except ValueError:
        errors.append("attestation attested_at must be RFC3339")

if errors:
    raise SystemExit(
        "invalid ingress/TLS/auth attachment:\n- " + "\n- ".join(errors)
    )

product_dir.mkdir(parents=True, exist_ok=True)
if attestation_path.resolve() != sibling_path.resolve():
    sibling_path.write_text(json.dumps(attestation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(sibling_path, 0o600)
else:
    os.chmod(sibling_path, 0o600)

summary = {
    "validated": True,
    "evidence": "ingress-tls-auth-attestation.json",
    "evidence_kind": attestation.get("evidence_kind"),
    "schema_version": attestation.get("schema_version"),
    "endpoint_url": attestation.get("endpoint_url"),
    "external_hostname": attestation.get("external_hostname"),
    "ingress_controller": attestation.get("ingress_controller"),
    "transport_security": attestation.get("transport_security"),
    "public_ingress_attestation": attestation.get("public_ingress_attestation"),
    "trusted_for_product_complete": attestation.get("trusted_for_product_complete"),
    "tls_enabled": attestation.get("tls_enabled"),
    "tls_certificate_sha256": attestation.get("tls_certificate_sha256"),
    "tls_certificate_issuer": attestation.get("tls_certificate_issuer"),
    "auth_enforced": attestation.get("auth_enforced"),
    "missing_token_rejected": attestation.get("missing_token_rejected"),
    "wrong_token_rejected": attestation.get("wrong_token_rejected"),
    "admin_auth_separate": attestation.get("admin_auth_separate"),
    "admin_route_missing_token_rejected": attestation.get("admin_route_missing_token_rejected"),
    "admin_route_wrong_token_rejected": attestation.get("admin_route_wrong_token_rejected"),
    "data_plane_token_rejected_on_admin_catalog_route": attestation.get("data_plane_token_rejected_on_admin_catalog_route"),
    "admin_token_accepted_on_admin_route": attestation.get("admin_token_accepted_on_admin_route"),
    "data_plane_token_rejected_on_admin_route": attestation.get("data_plane_token_rejected_on_admin_route"),
    "attested_at": attestation.get("attested_at"),
    "attester": attestation.get("attester"),
}
product.setdefault("api", {}).setdefault("auth", {})["ingress_tls_auth_attestation"] = summary
product["product_complete_blockers"] = [
    blocker
    for blocker in product.get("product_complete_blockers", [])
    if blocker != "local vind TLS/auth smoke passed, but public ingress/TLS/auth attestation is missing"
]
product["product_complete"] = (
    product.get("product_complete") is True
    and len(product.get("product_complete_blockers", [])) == 0
)
output_path.write_text(json.dumps(product, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(output_path, 0o600)
print(f"product_evidence={output_path}")
print(f"ingress_tls_auth_attestation={sibling_path}")
PY

if [ "$refresh_report" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="$output_evidence" \
    VELORIX_PRODUCT_COMPLETION_REPORT="$report" \
    scripts/report-vind-product-completion.sh
fi

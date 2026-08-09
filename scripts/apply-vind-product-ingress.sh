#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
host="${VELORIX_PRODUCT_INGRESS_HOST:-}"
ingress_class="${VELORIX_PRODUCT_INGRESS_CLASS:-${VELORIX_INGRESS_CONTROLLER:-}}"
tls_secret="${VELORIX_PRODUCT_INGRESS_TLS_SECRET:-}"
backend_protocol="${VELORIX_PRODUCT_INGRESS_BACKEND_PROTOCOL:-http}"
backend_service_port="${VELORIX_PRODUCT_INGRESS_BACKEND_SERVICE_PORT:-}"
annotations_json="${VELORIX_PRODUCT_INGRESS_ANNOTATIONS_JSON:-{}}"
manifest_file="${VELORIX_PRODUCT_INGRESS_MANIFEST:-${product_dir}/product-ingress.json}"
observed_file="${VELORIX_PRODUCT_INGRESS_OBSERVED:-${product_dir}/product-ingress-observed.json}"
dry_run="${VELORIX_PRODUCT_INGRESS_DRY_RUN:-0}"
wait_timeout_seconds="${VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS:-600}"
wait_interval_seconds="${VELORIX_PRODUCT_INGRESS_WAIT_INTERVAL_SECONDS:-5}"

usage() {
  cat <<'EOF'
Apply a Kubernetes Ingress for an existing vind product slice.

Usage:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
  VELORIX_PRODUCT_INGRESS_HOST=velorix.example.com \
  VELORIX_PRODUCT_INGRESS_CLASS=nginx \
  VELORIX_PRODUCT_INGRESS_TLS_SECRET=velorix-api-public-tls \
  scripts/apply-vind-product-ingress.sh

This helper writes product-ingress.json and product-ingress-observed.json under
the target-backed product directory and applies only a networking.k8s.io/v1
Ingress. It does not create PVCs, TLS Secrets, DNS records, or product-complete
attestation evidence. After DNS points at the ingress controller, run
scripts/attest-vind-product-ingress.sh against the public HTTPS endpoint.
Set VELORIX_PRODUCT_INGRESS_DRY_RUN=1 to render the manifest without applying it.
Set VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS=0 to skip the bounded wait
for the Ingress load balancer address after apply.
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
require kubectl
require python3

if [ -z "$host" ]; then
  echo "VELORIX_PRODUCT_INGRESS_HOST is required" >&2
  exit 64
fi
if [ -z "$ingress_class" ]; then
  echo "VELORIX_PRODUCT_INGRESS_CLASS or VELORIX_INGRESS_CONTROLLER is required" >&2
  exit 64
fi
if [ -z "$tls_secret" ]; then
  echo "VELORIX_PRODUCT_INGRESS_TLS_SECRET is required" >&2
  exit 64
fi
case "$backend_protocol" in
  http)
    backend_service_port="${backend_service_port:-8080}"
    ;;
  https)
    backend_service_port="${backend_service_port:-8443}"
    ;;
  *)
    echo "VELORIX_PRODUCT_INGRESS_BACKEND_PROTOCOL must be http or https" >&2
    exit 64
    ;;
esac
case "$backend_service_port" in
  '' | *[!0-9]*)
    echo "VELORIX_PRODUCT_INGRESS_BACKEND_SERVICE_PORT must be a positive integer" >&2
    exit 64
    ;;
esac
case "$dry_run" in
  0 | 1) ;;
  *)
    echo "VELORIX_PRODUCT_INGRESS_DRY_RUN must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$wait_timeout_seconds" in
  '' | *[!0-9]*)
    echo "VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS must be a non-negative integer" >&2
    exit 64
    ;;
esac
case "$wait_interval_seconds" in
  '' | *[!0-9]* | 0)
    echo "VELORIX_PRODUCT_INGRESS_WAIT_INTERVAL_SECONDS must be a positive integer" >&2
    exit 64
    ;;
esac

IFS=$'\t' read -r context namespace < <(
  python3 - "$product_evidence" "${VELORIX_K8S_CONTEXT:-}" "${VELORIX_K8S_NAMESPACE:-}" <<'PY'
import json
import sys
from pathlib import Path

product_path = Path(sys.argv[1])
context = sys.argv[2]
namespace = sys.argv[3]
if product_path.is_file():
    with product_path.open("r", encoding="utf-8") as f:
        product = json.load(f)
    context = product.get("context") or context
    namespace = product.get("namespace") or namespace
if not context:
    raise SystemExit("product evidence is missing context and VELORIX_K8S_CONTEXT is unset")
if not namespace:
    raise SystemExit("product evidence is missing namespace and VELORIX_K8S_NAMESPACE is unset")
print(f"{context}\t{namespace}")
PY
)

mkdir -p "$product_dir"
python3 - \
  "$manifest_file" \
  "$namespace" \
  "$host" \
  "$ingress_class" \
  "$tls_secret" \
  "$backend_protocol" \
  "$backend_service_port" \
  "$annotations_json" <<'PY'
import json
import sys
from pathlib import Path

manifest_file, namespace, host, ingress_class, tls_secret, backend_protocol, backend_service_port, annotations_json = sys.argv[1:]
try:
    annotations = json.loads(annotations_json)
except json.JSONDecodeError as exc:
    raise SystemExit(f"VELORIX_PRODUCT_INGRESS_ANNOTATIONS_JSON must be a JSON object: {exc}") from exc
if not isinstance(annotations, dict) or any(
    not isinstance(k, str) or not isinstance(v, str)
    for k, v in annotations.items()
):
    raise SystemExit("VELORIX_PRODUCT_INGRESS_ANNOTATIONS_JSON must be a JSON object of string values")
annotations = {
    **annotations,
    "velorix.dev/backend-protocol": backend_protocol,
}
if backend_protocol == "https" and ingress_class.lower() == "nginx":
    annotations.setdefault("nginx.ingress.kubernetes.io/backend-protocol", "HTTPS")
manifest = {
    "apiVersion": "networking.k8s.io/v1",
    "kind": "Ingress",
    "metadata": {
        "name": "velorix-api",
        "namespace": namespace,
        "labels": {
            "app": "velorix-api",
            "velorix.dev/component": "product-ingress",
        },
        "annotations": annotations,
    },
    "spec": {
        "ingressClassName": ingress_class,
        "tls": [{"hosts": [host], "secretName": tls_secret}],
        "rules": [
            {
                "host": host,
                "http": {
                    "paths": [
                        {
                            "path": "/",
                            "pathType": "Prefix",
                            "backend": {
                                "service": {
                                    "name": "velorix-api",
                                    "port": {"number": int(backend_service_port)},
                                }
                            },
                        }
                    ]
                },
            }
        ],
    },
}
path = Path(manifest_file)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
chmod 600 "$manifest_file"

if [ "$dry_run" = "1" ]; then
  echo "product_ingress_manifest=${manifest_file}"
  echo "dry_run=1"
  exit 0
fi

kubectl --context "$context" apply -f "$manifest_file"
kubectl --context "$context" -n "$namespace" get ingress velorix-api -o json >"$observed_file"
if [ "$wait_timeout_seconds" != "0" ]; then
  ingress_has_load_balancer_address() {
    python3 - "$observed_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    ingress = json.load(f)
addresses = (
    ingress.get("status", {})
    .get("loadBalancer", {})
    .get("ingress", [])
)
if any(item.get("ip") or item.get("hostname") for item in addresses if isinstance(item, dict)):
    raise SystemExit(0)
raise SystemExit(1)
PY
  }

  deadline=$((SECONDS + wait_timeout_seconds))
  while ! ingress_has_load_balancer_address; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "Ingress velorix-api did not report status.loadBalancer.ingress within ${wait_timeout_seconds}s" >&2
      exit 1
    fi
    sleep "$wait_interval_seconds"
    kubectl --context "$context" -n "$namespace" get ingress velorix-api -o json >"$observed_file"
  done
fi
chmod 600 "$observed_file"

echo "product_ingress_manifest=${manifest_file}"
echo "product_ingress_observed=${observed_file}"
echo "product_ingress_wait_timeout_seconds=${wait_timeout_seconds}"
echo "attest_with=VELORIX_VIND_PRODUCT_DIR=${product_dir} VELORIX_INGRESS_ENDPOINT_URL=https://${host} VELORIX_INGRESS_CONTROLLER=${ingress_class} scripts/attest-vind-product-ingress.sh"

#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
evidence_file="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
auth_env_file="${VELORIX_API_AUTH_ENV:-${product_dir}/api-auth.env}"
attach_evidence_file="${VELORIX_API_ATTACH_EVIDENCE:-${product_dir}/rest-attach-evidence.json}"
failover_evidence_file="${VELORIX_STANDING_RUNTIME_FAILOVER_EVIDENCE:-${product_dir}/standing-runtime-failover-smoke.json}"
api_local_port="${VELORIX_API_LOCAL_PORT:-}"
attach_timeout_seconds="${VELORIX_API_ATTACH_TIMEOUT_SECONDS:-30}"
keep_attach="${VELORIX_STANDING_RUNTIME_FAILOVER_KEEP_ATTACH:-1}"
update_product_evidence="${VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE:-1}"
release_attest="${VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST:-0}"

usage() {
  cat <<'EOF'
Smoke-test standing-runtime API pod failover for an existing vind product slice.

Usage:
  scripts/smoke-vind-standing-runtime-failover.sh

The product slice must already have owner-aware REST attach evidence from
scripts/run-vind-product.sh or scripts/attach-vind-product-rest.sh. If that
evidence is missing or stale, the smoke first performs an owner-aware attach.
It deletes that owner pod, waits for deployment/velorix-api to become
available, reattaches to a current writer-owner pod, then proves REST ingest
and promoted API query still work.

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_API_LOCAL_PORT=<defaults to api-auth.env URL port>
  VELORIX_STANDING_RUNTIME_FAILOVER_EVIDENCE=target/velorix-product/standing-runtime-failover-smoke.json
  VELORIX_STANDING_RUNTIME_FAILOVER_KEEP_ATTACH=1
  VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE=1
  VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST=0
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

require curl
require kubectl
require python3

cd "$repo_root"

if [ ! -f "$evidence_file" ]; then
  echo "missing product evidence: $evidence_file" >&2
  exit 66
fi
if [ ! -f "$auth_env_file" ]; then
  echo "missing API auth env file: $auth_env_file" >&2
  exit 66
fi

# shellcheck disable=SC1090
source "$auth_env_file"
if [ -z "${VELORIX_API_URL:-}" ] || [ -z "${VELORIX_API_AUTH_HEADER:-}" ] || [ -z "${VELORIX_ADMIN_AUTH_HEADER:-}" ]; then
  echo "api auth env must define VELORIX_API_URL, VELORIX_API_AUTH_HEADER, and VELORIX_ADMIN_AUTH_HEADER" >&2
  exit 66
fi
if [ -z "$api_local_port" ]; then
  api_local_port="${VELORIX_API_URL##*:}"
  api_local_port="${api_local_port%%/*}"
fi
case "$api_local_port" in
  '' | *[!0-9]*)
    echo "VELORIX_API_LOCAL_PORT must be a TCP port number" >&2
    exit 64
    ;;
esac
case "$keep_attach" in
  0 | 1) ;;
  *)
    echo "VELORIX_STANDING_RUNTIME_FAILOVER_KEEP_ATTACH must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$update_product_evidence" in
  0 | 1) ;;
  *)
    echo "VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$release_attest" in
  0 | 1) ;;
  *)
    echo "VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST must be 0 or 1" >&2
    exit 64
    ;;
esac

cleanup_attach_if_requested() {
  if [ "$keep_attach" = "1" ]; then
    return 0
  fi
  local pid_file="${product_dir}/port-forward.attach.pid"
  local tmux_session_file="${product_dir}/port-forward.attach.tmux-session"
  local pid=""
  local tmux_session=""
  if [ -f "$pid_file" ]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  fi
  if [ -f "$tmux_session_file" ] && command -v tmux >/dev/null 2>&1; then
    tmux_session="$(cat "$tmux_session_file" 2>/dev/null || true)"
    if [ -n "$tmux_session" ]; then
      tmux kill-session -t "$tmux_session" >/dev/null 2>&1 || true
    fi
  fi
}

VELORIX_VIND_PRODUCT_DIR="$product_dir" \
  VELORIX_VIND_PRODUCT_EVIDENCE="$evidence_file" \
  VELORIX_API_AUTH_ENV="$auth_env_file" \
  VELORIX_API_ATTACH_EVIDENCE="$attach_evidence_file" \
  VELORIX_API_LOCAL_PORT="$api_local_port" \
  VELORIX_API_ATTACH_TIMEOUT_SECONDS="$attach_timeout_seconds" \
  VELORIX_API_ATTACH_HOLD=1 \
  VELORIX_API_ATTACH_BACKGROUND=1 \
  VELORIX_API_ATTACH_WRITER_OWNER=1 \
  scripts/attach-vind-product-rest.sh >/dev/null

# shellcheck disable=SC1090
source "$auth_env_file"

IFS=$'\t' read -r context namespace cluster initial_target < <(
  python3 - "$evidence_file" "$attach_evidence_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    product = json.load(f)
with open(sys.argv[2], "r", encoding="utf-8") as f:
    attach = json.load(f)

context = product.get("context")
namespace = product.get("namespace")
cluster = product.get("cluster") or ""
target = attach.get("port_forward_target")
if not isinstance(context, str) or not context:
    raise SystemExit("product evidence is missing context")
if not isinstance(namespace, str) or not namespace:
    raise SystemExit("product evidence is missing namespace")
if not isinstance(target, str) or not target.startswith("pod/"):
    raise SystemExit("REST attach evidence does not point at a pod target")
print(f"{context}\t{namespace}\t{cluster}\t{target}")
PY
)

initial_pod="${initial_target#pod/}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
pre_owner_report="${product_dir}/standing-runtime-failover-pre-owner-report.json"
post_owner_report="${product_dir}/standing-runtime-failover-post-owner-report.json"
post_ingest_response="${product_dir}/standing-runtime-failover-ingest-response.json"
post_query_response="${product_dir}/standing-runtime-failover-promoted-api-query.json"
pod_before_file="${product_dir}/standing-runtime-failover-pods-before.json"
pod_after_file="${product_dir}/standing-runtime-failover-pods-after.json"

curl -fsS --max-time 5 \
  "$VELORIX_API_URL/v1/standing-runtime/owners" \
  -H "$VELORIX_ADMIN_AUTH_HEADER" >"$pre_owner_report"
kubectl --context "$context" -n "$namespace" get pods -l app=velorix-api -o json >"$pod_before_file"

start_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
kubectl --context "$context" -n "$namespace" delete pod "$initial_pod" --wait=true >/dev/null
kubectl --context "$context" -n "$namespace" rollout status deployment/velorix-api --timeout=120s >/dev/null

VELORIX_VIND_PRODUCT_DIR="$product_dir" \
  VELORIX_VIND_PRODUCT_EVIDENCE="$evidence_file" \
  VELORIX_API_AUTH_ENV="$auth_env_file" \
  VELORIX_API_ATTACH_EVIDENCE="$attach_evidence_file" \
  VELORIX_API_LOCAL_PORT="$api_local_port" \
  VELORIX_API_ATTACH_TIMEOUT_SECONDS="$attach_timeout_seconds" \
  VELORIX_API_ATTACH_HOLD=1 \
  VELORIX_API_ATTACH_BACKGROUND=1 \
  VELORIX_API_ATTACH_WRITER_OWNER=1 \
  scripts/attach-vind-product-rest.sh >/dev/null

# shellcheck disable=SC1090
source "$auth_env_file"
curl -fsS --max-time 5 \
  "$VELORIX_API_URL/v1/standing-runtime/owners" \
  -H "$VELORIX_ADMIN_AUTH_HEADER" >"$post_owner_report"

stream_id="standing-failover-${run_id}"
user_id="standing-failover-${run_id}"
expected_sum=23
expected_count=2
curl -fsS --max-time 10 \
  -X POST "$VELORIX_API_URL/v1/relations/scores/ingest" \
  -H "$VELORIX_API_AUTH_HEADER" \
  -H 'content-type: application/json' \
  -d "{\"relation_version\":\"2026-05-24.v1\",\"stream_id\":\"${stream_id}\",\"partition_id\":0,\"start_offset_inclusive\":0,\"rows\":[{\"user_id\":\"${user_id}\",\"score\":19,\"delta\":1},{\"user_id\":\"${user_id}\",\"score\":4,\"delta\":1}]}" \
  >"$post_ingest_response"
curl -fsS --max-time 10 \
  "$VELORIX_API_URL/v1/api/scores/positive" \
  -H "$VELORIX_API_AUTH_HEADER" >"$post_query_response"
kubectl --context "$context" -n "$namespace" get pods -l app=velorix-api -o json >"$pod_after_file"
end_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"

scripts/write-standing-runtime-failover-evidence.py \
  "$failover_evidence_file" \
  "$evidence_file" \
  "$run_id" \
  "$context" \
  "$namespace" \
  "$cluster" \
  "$initial_target" \
  "$attach_evidence_file" \
  "$pre_owner_report" \
  "$post_owner_report" \
  "$post_ingest_response" \
  "$post_query_response" \
  "$pod_before_file" \
  "$pod_after_file" \
  "$start_ms" \
  "$end_ms" \
  "$user_id" \
  "$expected_sum" \
  "$expected_count" \
  "$release_attest"

if [ "$update_product_evidence" = "1" ]; then
  python3 - "$evidence_file" "$failover_evidence_file" <<'PY'
import json
import sys

product_path, failover_path = sys.argv[1:]
with open(product_path, "r", encoding="utf-8") as f:
    product = json.load(f)
with open(failover_path, "r", encoding="utf-8") as f:
    failover = json.load(f)

if product.get("evidence_kind") != "velorix_product_slice_evidence":
    raise SystemExit(f"product evidence has wrong evidence_kind: {product_path}")
if failover.get("evidence_kind") != "velorix_standing_runtime_failover_smoke":
    raise SystemExit(f"failover evidence has wrong evidence_kind: {failover_path}")
if failover.get("status") != "pass":
    raise SystemExit(f"failover evidence is not pass: {failover_path}")
standing = product.setdefault("standing_runtime_fencing", {})
standing["local_api_pod_failover_smoke"] = {
    "status": "pass",
    "enabled": True,
    "evidence": "standing-runtime-failover-smoke.json",
    "scope": failover.get("scope") or "local vind product API pod deletion and owner reacquire smoke",
    "trusted_for_product_complete": failover.get("trusted_for_product_complete") is True,
    "production_wall_clock_failover_attestation": failover.get("production_wall_clock_failover_attestation") is True,
    "observed_failover_ms": failover.get("observed_failover_ms"),
    "initial_port_forward_target": failover.get("initial_port_forward_target"),
    "post_failover_port_forward_target": failover.get("post_failover_port_forward_target"),
    "post_failover_query_row": failover.get("post_failover_query_row"),
}
for field in [
    "evidence_scope",
    "failover_probe_kind",
    "backend_time_source_kind",
    "authority_time_observed",
    "owner_ttl_ms",
    "failover_time_bound_ms",
    "pre_failover_owner_epoch",
    "post_failover_owner_epoch",
    "affected_api_pods",
]:
    if field in failover:
        standing["local_api_pod_failover_smoke"][field] = failover.get(field)

with open(product_path, "w", encoding="utf-8") as f:
    json.dump(product, f, indent=2, sort_keys=True)
    f.write("\n")
PY
fi

cleanup_attach_if_requested

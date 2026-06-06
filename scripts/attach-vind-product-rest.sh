#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
evidence_file="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
auth_env_file="${VELORIX_API_AUTH_ENV:-${product_dir}/api-auth.env}"
attach_evidence_file="${VELORIX_API_ATTACH_EVIDENCE:-${product_dir}/rest-attach-evidence.json}"
port="${VELORIX_API_LOCAL_PORT:-}"
startup_timeout_seconds="${VELORIX_API_ATTACH_TIMEOUT_SECONDS:-30}"
hold="${VELORIX_API_ATTACH_HOLD:-1}"
background="${VELORIX_API_ATTACH_BACKGROUND:-0}"
writer_owner_attach="${VELORIX_API_ATTACH_WRITER_OWNER:-auto}"

usage() {
  cat <<'EOF'
Attach local REST access to an existing vind/vCluster Velorix product slice.

Usage:
  scripts/attach-vind-product-rest.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_VIND_PRODUCT_EVIDENCE=target/velorix-product/product-evidence.json
  VELORIX_API_AUTH_ENV=target/velorix-product/api-auth.env
  VELORIX_API_ATTACH_EVIDENCE=target/velorix-product/rest-attach-evidence.json
  VELORIX_API_LOCAL_PORT=8080
  VELORIX_API_ATTACH_TIMEOUT_SECONDS=30
  VELORIX_API_ATTACH_HOLD=1
  VELORIX_API_ATTACH_BACKGROUND=0
  VELORIX_API_ATTACH_WRITER_OWNER=auto

The script reuses the context and namespace recorded by product-evidence.json,
starts kubectl port-forward to service/velorix-api or a selected writer-owner
pod, validates /healthz and /readyz with the saved bearer-token header, and
refreshes api-auth.env.
It does not create a new vCluster, build images, or deploy Kubernetes resources.
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
  echo "run scripts/run-vind-product.sh first or set VELORIX_VIND_PRODUCT_EVIDENCE" >&2
  exit 66
fi
if [ ! -f "$auth_env_file" ]; then
  echo "missing API auth env file: $auth_env_file" >&2
  echo "run scripts/run-vind-product.sh first or set VELORIX_API_AUTH_ENV" >&2
  exit 66
fi

IFS=$'\t' read -r context namespace cluster < <(
  python3 - "$evidence_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    evidence = json.load(f)

context = evidence.get("context")
namespace = evidence.get("namespace")
cluster = evidence.get("cluster")
if not isinstance(context, str) or not context:
    raise SystemExit("product evidence is missing context")
if not isinstance(namespace, str) or not namespace:
    raise SystemExit("product evidence is missing namespace")
if not isinstance(cluster, str) or not cluster:
    cluster = ""
print(f"{context}\t{namespace}\t{cluster}")
PY
)

if [ -z "$port" ]; then
  # shellcheck disable=SC1090
  source "$auth_env_file"
  port="${VELORIX_API_URL##*:}"
  port="${port%%/*}"
fi
case "$port" in
  '' | *[!0-9]*)
    echo "VELORIX_API_LOCAL_PORT must be a TCP port number" >&2
    exit 64
    ;;
esac
case "$startup_timeout_seconds" in
  '' | *[!0-9]*)
    echo "VELORIX_API_ATTACH_TIMEOUT_SECONDS must be a non-negative integer" >&2
    exit 64
    ;;
esac
case "$hold" in
  0 | 1) ;;
  *)
    echo "VELORIX_API_ATTACH_HOLD must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$background" in
  0 | 1) ;;
  *)
    echo "VELORIX_API_ATTACH_BACKGROUND must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$writer_owner_attach" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_API_ATTACH_WRITER_OWNER must be auto, 0, or 1" >&2
    exit 64
    ;;
esac

if ! kubectl config get-contexts "$context" >/dev/null 2>&1; then
  echo "recorded Kubernetes context does not exist: $context" >&2
  exit 66
fi

if ! kubectl --context "$context" -n "$namespace" get service velorix-api >/dev/null; then
  echo "service/velorix-api is missing in ${context}/${namespace}" >&2
  exit 66
fi

mkdir -p "$product_dir"
pods_file="${product_dir}/attach-velorix-api-pods.json"
deployment_file="${product_dir}/attach-velorix-api-deployment.json"
kubectl --context "$context" -n "$namespace" get pods -l app=velorix-api -o json >"$pods_file" || true
kubectl --context "$context" -n "$namespace" get deploy velorix-api -o json >"$deployment_file" || true

available_replicas="$(
  kubectl --context "$context" -n "$namespace" get deploy velorix-api \
    -o jsonpath='{.status.availableReplicas}' 2>/dev/null || true
)"
if [ -z "$available_replicas" ]; then
  available_replicas=0
fi
if [ "$available_replicas" = "0" ]; then
  python3 - "$attach_evidence_file" "$evidence_file" "$context" "$namespace" "$cluster" "$port" "$deployment_file" "$pods_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

evidence_path, product_evidence_path, context, namespace, cluster, port, deployment_file, pods_file = sys.argv[1:]
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_rest_attach_evidence",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "blocked",
    "blocker_kind": "api_deployment_unavailable",
    "context": context,
    "namespace": namespace,
    "cluster": cluster,
    "api_url": f"http://127.0.0.1:{port}",
    "product_evidence": product_evidence_path,
    "available_replicas": 0,
    "trusted_for_product_complete": False,
    "evidence_files": {
        "api_deployment": deployment_file,
        "api_pods": pods_file,
    },
    "remediation": [
        "inspect the recorded vCluster pod status and events",
        "free local Docker/vCluster capacity if pods are pending or evicted",
        "rerun scripts/run-vind-product.sh after local resource pressure is resolved",
    ],
}
with open(evidence_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  echo "deployment/velorix-api has no available replicas in ${context}/${namespace}" >&2
  echo "wrote REST attach blocker evidence to ${attach_evidence_file}" >&2
  kubectl --context "$context" -n "$namespace" get pods -l app=velorix-api -o wide >&2 || true
  exit 75
fi

# shellcheck disable=SC1090
source "$auth_env_file"
if [ -z "${VELORIX_API_AUTH_HEADER:-}" ]; then
  echo "auth env file is missing VELORIX_API_AUTH_HEADER" >&2
  exit 66
fi

secret_value() {
  local secret_name="$1"
  local key="$2"
  local secret_json
  secret_json="$(kubectl --context "$context" -n "$namespace" get secret "$secret_name" -o json 2>/dev/null)" || return 1
  python3 - "$key" "$secret_json" <<'PY'
import base64
import json
import sys

key = sys.argv[1]
body = json.loads(sys.argv[2])
encoded = (body.get("data") or {}).get(key)
if not encoded:
    raise SystemExit(1)
print(base64.b64decode(encoded).decode("utf-8"))
PY
}

if api_secret_token="$(secret_value velorix-api-auth bearer-token)"; then
  VELORIX_API_AUTH_HEADER="authorization: Bearer ${api_secret_token}"
fi
if admin_secret_token="$(secret_value velorix-admin-auth admin-bearer-token)"; then
  VELORIX_ADMIN_AUTH_HEADER="authorization: Bearer ${admin_secret_token}"
fi

log_file="${product_dir}/port-forward.attach.log"
pid_file="${product_dir}/port-forward.attach.pid"
tmux_session_file="${product_dir}/port-forward.attach.tmux-session"
port_forward_command_file="${product_dir}/port-forward.attach.command.sh"
target_ref="service/velorix-api"
writer_owner_attach_status="disabled"

if [ "$writer_owner_attach" != "0" ] && [ -n "${VELORIX_ADMIN_AUTH_HEADER:-}" ]; then
  writer_owner_attach_status="not_found"
  owner_probe_deadline=$((SECONDS + startup_timeout_seconds))
  while true; do
    kubectl --context "$context" -n "$namespace" get pods -l app=velorix-api -o json >"$pods_file" || true
    while read -r pod_name; do
      [ -n "$pod_name" ] || continue
      probe_port=$((20000 + RANDOM % 20000))
      probe_log="${product_dir}/writer-owner-probe-${pod_name}.log"
      probe_json="${product_dir}/writer-owner-probe-${pod_name}.json"
      acquire_json="${product_dir}/writer-owner-acquire-${pod_name}.json"
      kubectl --context "$context" -n "$namespace" port-forward "pod/${pod_name}" \
        "${probe_port}:8080" >"$probe_log" 2>&1 &
      probe_pid="$!"
      probe_ready_deadline=$((SECONDS + 10))
      while true; do
        if curl -fsS --max-time 2 "http://127.0.0.1:${probe_port}/healthz" >/dev/null 2>&1; then
          break
        fi
        if ! kill -0 "$probe_pid" >/dev/null 2>&1; then
          break
        fi
        if [ "$SECONDS" -ge "$probe_ready_deadline" ]; then
          break
        fi
        sleep 1
      done
      curl -fsS --max-time 5 \
        -X POST "http://127.0.0.1:${probe_port}/v1/standing-runtime/owners" \
        -H "$VELORIX_ADMIN_AUTH_HEADER" >"$acquire_json" 2>/dev/null || true
      if curl -fsS --max-time 5 \
        "http://127.0.0.1:${probe_port}/v1/standing-runtime/owners" \
        -H "$VELORIX_ADMIN_AUTH_HEADER" >"$probe_json" 2>/dev/null; then
        if python3 - "$probe_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    report = json.load(f)
owners = report.get("owners") or []
if owners and all(owner.get("current_owner_matches_local_process") is True for owner in owners):
    raise SystemExit(0)
raise SystemExit(1)
PY
        then
          target_ref="pod/${pod_name}"
          writer_owner_attach_status="selected"
          kill "$probe_pid" >/dev/null 2>&1 || true
          wait "$probe_pid" >/dev/null 2>&1 || true
          break
        fi
      fi
      kill "$probe_pid" >/dev/null 2>&1 || true
      wait "$probe_pid" >/dev/null 2>&1 || true
    done < <(
      python3 - "$pods_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
for item in body.get("items") or []:
    metadata = item.get("metadata") or {}
    if metadata.get("deletionTimestamp"):
        continue
    status = item.get("status") or {}
    conditions = {
        condition.get("type"): condition.get("status")
        for condition in status.get("conditions") or []
    }
    if status.get("phase") == "Running" and conditions.get("Ready") == "True":
        print(metadata.get("name") or "")
PY
    )
    if [ "$writer_owner_attach_status" = "selected" ]; then
      break
    fi
    if [ "$SECONDS" -ge "$owner_probe_deadline" ]; then
      break
    fi
    sleep 1
  done

  if [ "$writer_owner_attach" = "1" ] && [ "$writer_owner_attach_status" != "selected" ]; then
    echo "could not find a standing-runtime writer owner pod" >&2
    exit 75
  fi
elif [ "$writer_owner_attach" != "0" ]; then
  writer_owner_attach_status="missing_admin_header"
fi

if [ -f "$pid_file" ]; then
  old_pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [ -n "$old_pid" ] && kill -0 "$old_pid" >/dev/null 2>&1; then
    kill "$old_pid" >/dev/null 2>&1 || true
  fi
fi
if [ -f "$tmux_session_file" ]; then
  old_tmux_session="$(cat "$tmux_session_file" 2>/dev/null || true)"
  if [ -n "$old_tmux_session" ] && command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$old_tmux_session" >/dev/null 2>&1 || true
  fi
fi

if [ "$background" = "1" ] && command -v tmux >/dev/null 2>&1; then
  tmux_session="velorix-rest-${namespace}-${port}"
  tmux kill-session -t "$tmux_session" >/dev/null 2>&1 || true
  {
    printf '#!/usr/bin/env bash\n'
    printf 'set -euo pipefail\n'
    printf 'exec'
    printf ' %q' kubectl --context "$context" -n "$namespace" port-forward "$target_ref" "${port}:8080"
    printf ' >%q 2>&1\n' "$log_file"
  } >"$port_forward_command_file"
  chmod 700 "$port_forward_command_file"
  tmux new-session -d -s "$tmux_session" "$port_forward_command_file"
  printf '%s\n' "$tmux_session" >"$tmux_session_file"
  port_forward_pid="$(tmux display-message -p -t "$tmux_session" '#{pane_pid}')"
else
  nohup kubectl --context "$context" -n "$namespace" port-forward "$target_ref" \
    "${port}:8080" >"$log_file" 2>&1 </dev/null &
  port_forward_pid="$!"
  : >"$tmux_session_file"
fi
printf '%s\n' "$port_forward_pid" >"$pid_file"

cleanup() {
  if [ "$hold" != "1" ] && [ "$background" != "1" ]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

deadline=$((SECONDS + startup_timeout_seconds))
api_url="http://127.0.0.1:${port}"
while true; do
  if curl -fsS --max-time 2 "${api_url}/healthz" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    echo "kubectl port-forward exited before healthz became reachable" >&2
    cat "$log_file" >&2 || true
    exit 75
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for ${api_url}/healthz" >&2
    cat "$log_file" >&2 || true
    exit 75
  fi
  sleep 1
done

readyz_file="${product_dir}/readyz.attach.json"
openapi_file="${product_dir}/openapi.attach.json"
curl -fsS --max-time 5 "${api_url}/readyz" -H "$VELORIX_API_AUTH_HEADER" >"$readyz_file"
curl -fsS --max-time 5 "${api_url}/v1/openapi.json" -H "$VELORIX_API_AUTH_HEADER" >"$openapi_file"

python3 - "$attach_evidence_file" "$evidence_file" "$context" "$namespace" "$cluster" "$api_url" "$port_forward_pid" "$log_file" "$readyz_file" "$openapi_file" "$deployment_file" "$pods_file" "$target_ref" "$writer_owner_attach_status" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    evidence_path,
    product_evidence_path,
    context,
    namespace,
    cluster,
    api_url,
    pid,
    log_file,
    readyz_file,
    openapi_file,
    deployment_file,
    pods_file,
    target_ref,
    writer_owner_attach_status,
) = sys.argv[1:]

with open(readyz_file, "r", encoding="utf-8") as f:
    readyz = json.load(f)
with open(openapi_file, "r", encoding="utf-8") as f:
    openapi = json.load(f)

payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_rest_attach_evidence",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "pass",
    "context": context,
    "namespace": namespace,
    "cluster": cluster,
    "api_url": api_url,
    "port_forward_target": target_ref,
    "writer_owner_attach_status": writer_owner_attach_status,
    "product_evidence": product_evidence_path,
    "port_forward_pid": int(pid),
    "healthz_passed": True,
    "readyz_passed": readyz.get("status") == "ready",
    "protected_openapi_passed": str(openapi.get("openapi", "")).startswith("3."),
    "trusted_for_product_complete": False,
    "evidence_files": {
        "readyz": readyz_file,
        "protected_openapi": openapi_file,
        "port_forward_log": log_file,
        "api_deployment": deployment_file,
        "api_pods": pods_file,
    },
}
with open(evidence_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY

cat >"$auth_env_file" <<EOF
export VELORIX_API_URL=${api_url}
export VELORIX_API_BEARER_TOKEN='${api_secret_token:-}'
export VELORIX_ADMIN_BEARER_TOKEN='${admin_secret_token:-}'
export VELORIX_API_AUTH_HEADER='${VELORIX_API_AUTH_HEADER}'
export VELORIX_ADMIN_AUTH_HEADER='${VELORIX_ADMIN_AUTH_HEADER:-}'
export VELORIX_PRODUCT_CONTEXT='${context}'
export VELORIX_PRODUCT_NAMESPACE='${namespace}'
export VELORIX_PRODUCT_CLUSTER='${cluster}'
EOF

cat <<EOF
attached Velorix REST API
api_url=${api_url}
target=${target_ref}
writer_owner_attach_status=${writer_owner_attach_status}
context=${context}
namespace=${namespace}
pid=${port_forward_pid}
log=${log_file}
readyz=${readyz_file}
openapi=${openapi_file}
evidence=${attach_evidence_file}
EOF

if [ "$hold" = "1" ] && [ "$background" != "1" ]; then
  wait "$port_forward_pid"
fi

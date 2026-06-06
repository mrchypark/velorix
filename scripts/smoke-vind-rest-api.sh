#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
auth_env_file="${VELORIX_API_AUTH_ENV:-${product_dir}/api-auth.env}"
attach_evidence_file="${VELORIX_API_ATTACH_EVIDENCE:-${product_dir}/rest-attach-evidence.json}"
output_dir="${VELORIX_REST_API_SMOKE_DIR:-${product_dir}/rest-api-smoke}"
summary_file="${VELORIX_REST_API_SMOKE_EVIDENCE:-${product_dir}/rest-api-smoke.json}"
auto_attach="${VELORIX_REST_API_SMOKE_ATTACH:-auto}"
query_wait_seconds="${VELORIX_REST_API_SMOKE_QUERY_WAIT_SECONDS:-20}"

usage() {
  cat <<'EOF'
Smoke-test the REST API of an existing vind product slice.

Usage:
  scripts/smoke-vind-rest-api.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_API_AUTH_ENV=target/velorix-product/api-auth.env
  VELORIX_REST_API_SMOKE_DIR=target/velorix-product/rest-api-smoke
  VELORIX_REST_API_SMOKE_EVIDENCE=target/velorix-product/rest-api-smoke.json
  VELORIX_REST_API_SMOKE_ATTACH=auto
  VELORIX_REST_API_SMOKE_QUERY_WAIT_SECONDS=20

The script uses the already deployed product REST API. If healthz is not
reachable and VELORIX_REST_API_SMOKE_ATTACH is auto or 1, it reuses
scripts/attach-vind-product-rest.sh to recreate the local port-forward. It does
not create a vCluster, deploy workloads, build images, or create PVCs.
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
require python3

cd "$repo_root"

case "$auto_attach" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_REST_API_SMOKE_ATTACH must be auto, 0, or 1" >&2
    exit 64
    ;;
esac
case "$query_wait_seconds" in
  '' | *[!0-9]*)
    echo "VELORIX_REST_API_SMOKE_QUERY_WAIT_SECONDS must be a non-negative integer" >&2
    exit 64
    ;;
esac

if [ ! -f "$auth_env_file" ]; then
  echo "missing API auth env file: $auth_env_file" >&2
  echo "run scripts/run-vind-product.sh first, or reattach with scripts/attach-vind-product-rest.sh" >&2
  exit 66
fi

mkdir -p "$output_dir"

# shellcheck disable=SC1090
source "$auth_env_file"
if [ -z "${VELORIX_API_URL:-}" ] || [ -z "${VELORIX_API_AUTH_HEADER:-}" ]; then
  echo "api auth env must define VELORIX_API_URL and VELORIX_API_AUTH_HEADER" >&2
  exit 66
fi
VELORIX_API_URL="${VELORIX_API_URL%/}"

attach_rest_api() {
  local attach_port
  attach_port="$(
    python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
  )"
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_API_AUTH_ENV="$auth_env_file" \
    VELORIX_API_ATTACH_EVIDENCE="$attach_evidence_file" \
    VELORIX_API_LOCAL_PORT="$attach_port" \
    VELORIX_API_ATTACH_BACKGROUND=1 \
    VELORIX_API_ATTACH_HOLD=1 \
    scripts/attach-vind-product-rest.sh >/dev/null
  # shellcheck disable=SC1090
  source "$auth_env_file"
  if [ -z "${VELORIX_API_URL:-}" ] || [ -z "${VELORIX_API_AUTH_HEADER:-}" ]; then
    echo "api auth env must define VELORIX_API_URL and VELORIX_API_AUTH_HEADER after attach" >&2
    exit 66
  fi
  VELORIX_API_URL="${VELORIX_API_URL%/}"
}

if ! curl -fsS --max-time 3 "$VELORIX_API_URL/healthz" >/dev/null 2>&1; then
  case "$auto_attach" in
    auto | 1)
      attach_rest_api
      ;;
    0)
      echo "REST API is not reachable: $VELORIX_API_URL/healthz" >&2
      echo "run scripts/attach-vind-product-rest.sh or set VELORIX_REST_API_SMOKE_ATTACH=auto" >&2
      exit 75
      ;;
  esac
fi

auth_precheck_file="${output_dir}/openapi-auth-precheck.json"
if ! curl -fsS --max-time 5 "$VELORIX_API_URL/v1/openapi.json" -H "$VELORIX_API_AUTH_HEADER" >"$auth_precheck_file" 2>/dev/null; then
  case "$auto_attach" in
    auto | 1)
      attach_rest_api
      if ! curl -fsS --max-time 5 "$VELORIX_API_URL/v1/openapi.json" -H "$VELORIX_API_AUTH_HEADER" >"$auth_precheck_file"; then
        echo "authenticated REST API is not reachable after reattach: $VELORIX_API_URL/v1/openapi.json" >&2
        cat "$auth_precheck_file" >&2 || true
        exit 75
      fi
      ;;
    0)
      echo "authenticated REST API is not reachable: $VELORIX_API_URL/v1/openapi.json" >&2
      echo "run scripts/attach-vind-product-rest.sh or set VELORIX_REST_API_SMOKE_ATTACH=auto" >&2
      cat "$auth_precheck_file" >&2 || true
      exit 75
      ;;
  esac
fi

curl_api() {
  curl -fsS --max-time 15 "$@" -H "$VELORIX_API_AUTH_HEADER"
}

curl_admin_api() {
  if [ -z "${VELORIX_ADMIN_AUTH_HEADER:-}" ]; then
    echo "api auth env is missing VELORIX_ADMIN_AUTH_HEADER" >&2
    return 66
  fi
  curl -fsS --max-time 15 "$@" -H "$VELORIX_ADMIN_AUTH_HEADER"
}

curl_api_status() {
  local output_file="$1"
  shift
  curl -sS --max-time 15 -o "$output_file" -w '%{http_code}' "$@" -H "$VELORIX_API_AUTH_HEADER"
}

run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
user_id="rest-smoke-${run_id}"
stream_id="rest-smoke-${run_id}"

healthz_file="${output_dir}/healthz.json"
readyz_file="${output_dir}/readyz.json"
relation_file="${output_dir}/scores-relation.json"
policy_file="${output_dir}/query-policy-interactive.json"
policy_create_file="${output_dir}/query-policy-interactive-create.json"
view_file="${output_dir}/positive-scores-view.json"
view_create_file="${output_dir}/positive-scores-view-create.json"
views_file="${output_dir}/views.json"
openapi_file="${output_dir}/openapi.json"
generic_query_file="${output_dir}/generic-query-disabled.json"
owner_acquire_file="${output_dir}/standing-runtime-owner-acquire.json"
owner_report_file="${output_dir}/standing-runtime-owner-report.json"
ingest_file="${output_dir}/scores-ingest.json"
view_query_file="${output_dir}/positive-scores-view-query.json"
api_query_file="${output_dir}/positive-scores-api-query.json"

curl -fsS --max-time 10 "$VELORIX_API_URL/healthz" >"$healthz_file"
curl_api "$VELORIX_API_URL/readyz" >"$readyz_file"

relation_status="$(curl_api_status "$relation_file" \
  -X POST "$VELORIX_API_URL/v1/relations/scores-default")"
case "$relation_status" in
  200 | 201 | 409) ;;
  *)
    echo "expected scores default relation creation to return 200, 201, or duplicate 409; got ${relation_status}" >&2
    cat "$relation_file" >&2 || true
    exit 1
    ;;
esac

policy_status="$(curl_api_status "$policy_file" "$VELORIX_API_URL/v1/query-policies/interactive")"
if [ "$policy_status" != "200" ]; then
  policy_create_status="$(curl_api_status "$policy_create_file" \
    -X POST "$VELORIX_API_URL/v1/query-policies" \
    -H 'content-type: application/json' \
    -d '{"query_policy_id":"interactive","policy":{"max_sql_bytes":4096,"planning_timeout_ms":1000,"execution_timeout_ms":5000,"max_output_rows":1000,"max_output_bytes":1048576,"max_scan_files":100,"max_scan_bytes":134217728,"max_object_requests":100,"max_concurrent_queries":4,"memory_limit_bytes":536870912,"spill_limit_bytes":1073741824}}')"
  case "$policy_create_status" in
    200 | 201 | 409)
      curl_api "$VELORIX_API_URL/v1/query-policies/interactive" >"$policy_file"
      ;;
    *)
      echo "expected interactive query policy create to return 200, 201, or duplicate 409; got ${policy_create_status}" >&2
      cat "$policy_create_file" >&2 || true
      exit 1
      ;;
  esac
else
  cp "$policy_file" "$policy_create_file"
fi

view_status="$(curl_api_status "$view_file" "$VELORIX_API_URL/v1/views/positive_scores_by_user")"
if [ "$view_status" != "200" ]; then
  view_create_status="$(curl_api_status "$view_create_file" \
    -X POST "$VELORIX_API_URL/v1/views" \
    -H 'content-type: application/json' \
    -d '{"view_id":"positive_scores_by_user","urlPath":"/scores/positive","input_relation_id":"scores","input_relation_version":"2026-05-24.v1","sql":"select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id","response_formats":["json"],"query_policy_id":"interactive"}')"
  case "$view_create_status" in
    200 | 201 | 409)
      curl_api "$VELORIX_API_URL/v1/views/positive_scores_by_user" >"$view_file"
      ;;
    *)
      echo "expected positive scores view create to return 200, 201, or duplicate 409; got ${view_create_status}" >&2
      cat "$view_create_file" >&2 || true
      exit 1
      ;;
  esac
else
  cp "$view_file" "$view_create_file"
fi

generic_query_status="$(curl_api_status "$generic_query_file" \
  -X POST "$VELORIX_API_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{"relation_id":"scores","relation_version":"2026-05-24.v1","sql":"select key, value, weight from input"}')"
if [ "$generic_query_status" != "409" ]; then
  echo "expected generic /v1/query to fail with 409, got ${generic_query_status}" >&2
  cat "$generic_query_file" >&2 || true
  exit 1
fi

if [ -n "${VELORIX_ADMIN_AUTH_HEADER:-}" ]; then
  curl_admin_api -X POST "$VELORIX_API_URL/v1/standing-runtime/owners" >"$owner_acquire_file"
  curl_admin_api "$VELORIX_API_URL/v1/standing-runtime/owners" >"$owner_report_file"
else
  python3 - "$owner_acquire_file" "$owner_report_file" <<'PY'
import json
import sys

payload = {"status": "skipped", "reason": "VELORIX_ADMIN_AUTH_HEADER missing"}
for path in sys.argv[1:]:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
PY
fi

curl_api -X POST "$VELORIX_API_URL/v1/ingest" \
  -H 'content-type: application/json' \
  -d "{\"relation_id\":\"scores\",\"relation_version\":\"2026-05-24.v1\",\"stream_id\":\"${stream_id}\",\"partition_id\":0,\"start_offset_inclusive\":0,\"rows\":[{\"user_id\":\"${user_id}\",\"score\":10,\"delta\":1},{\"user_id\":\"${user_id}\",\"score\":15,\"delta\":1},{\"user_id\":\"${user_id}\",\"score\":-7,\"delta\":1}]}" \
  >"$ingest_file"

deadline=$((SECONDS + query_wait_seconds))
while true; do
  curl_api "$VELORIX_API_URL/v1/views/positive_scores_by_user/query?max_rows=1000" >"$view_query_file"
  curl_api "$VELORIX_API_URL/v1/api/scores/positive?max_rows=1000" >"$api_query_file"
  if python3 - "$view_query_file" "$api_query_file" "$user_id" <<'PY'
import json
import sys

for path in sys.argv[1:3]:
    with open(path, "r", encoding="utf-8") as f:
        body = json.load(f)
    rows = {row.get("user_id"): row for row in body.get("rows") or []}
    row = rows.get(sys.argv[3]) or {}
    if row.get("sum") != 25 or row.get("count") != 2:
        raise SystemExit(1)
PY
  then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for ingested row ${user_id} to appear in view and promoted API" >&2
    cat "$api_query_file" >&2 || true
    exit 1
  fi
  sleep 1
done

curl_api "$VELORIX_API_URL/v1/views" >"$views_file"
curl_api "$VELORIX_API_URL/v1/openapi.json" >"$openapi_file"

python3 - \
  "$summary_file" \
  "$run_id" \
  "$user_id" \
  "$stream_id" \
  "$VELORIX_API_URL" \
  "$healthz_file" \
  "$readyz_file" \
  "$relation_file" \
  "$policy_file" \
  "$view_file" \
  "$views_file" \
  "$openapi_file" \
  "$generic_query_file" \
  "$owner_acquire_file" \
  "$owner_report_file" \
  "$ingest_file" \
  "$view_query_file" \
  "$api_query_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    summary_path,
    run_id,
    user_id,
    stream_id,
    api_url,
    healthz_path,
    readyz_path,
    relation_path,
    policy_path,
    view_path,
    views_path,
    openapi_path,
    generic_query_path,
    owner_acquire_path,
    owner_report_path,
    ingest_path,
    view_query_path,
    api_query_path,
) = sys.argv[1:]

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

readyz = read_json(readyz_path)
if readyz.get("status") != "ready":
    raise SystemExit(f"readyz is not ready: {readyz}")
if readyz.get("generic_query_enabled") is not False:
    raise SystemExit(f"generic query must stay disabled in product smoke: {readyz}")
if readyz.get("legacy_recovered_sql_views_allowed") is not False:
    raise SystemExit(f"legacy recovered SQL views must stay disabled: {readyz}")
if not (readyz.get("metadata_store") or {}).get("configured"):
    raise SystemExit(f"metadata store is not configured: {readyz}")

policy = read_json(policy_path)
if policy.get("query_policy_id") != "interactive":
    raise SystemExit(f"interactive query policy mismatch: {policy}")
policy_body = policy.get("policy") or {}
for field in (
    "max_sql_bytes",
    "planning_timeout_ms",
    "execution_timeout_ms",
    "max_output_rows",
    "max_output_bytes",
    "max_scan_files",
    "max_scan_bytes",
    "max_object_requests",
    "max_concurrent_queries",
    "memory_limit_bytes",
    "spill_limit_bytes",
):
    if policy_body.get(field) is None:
        raise SystemExit(f"interactive query policy missing {field}: {policy}")

view = read_json(view_path)
if view.get("view_id") != "positive_scores_by_user":
    raise SystemExit(f"positive_scores_by_user view mismatch: {view}")
if view.get("execution_mode") != "standing_runtime":
    raise SystemExit(f"positive_scores_by_user is not a standing runtime view: {view}")
if view.get("query_enabled") is not True:
    raise SystemExit(f"positive_scores_by_user query is not enabled: {view}")
if view.get("query_policy_id") != "interactive":
    raise SystemExit(f"positive_scores_by_user is not linked to interactive policy: {view}")

views = read_json(views_path)
view_ids = {item.get("view_id") for item in views.get("views") or []}
if "positive_scores_by_user" not in view_ids:
    raise SystemExit(f"views catalog does not include positive_scores_by_user: {views}")

openapi = read_json(openapi_path)
if not str(openapi.get("openapi", "")).startswith("3."):
    raise SystemExit(f"OpenAPI document is not 3.x: {openapi.get('openapi')}")
paths = openapi.get("paths") or {}
if "/v1/query" in paths:
    raise SystemExit("generic /v1/query unexpectedly appears in OpenAPI")
if "/v1/api/scores/positive" not in paths:
    raise SystemExit("promoted /v1/api/scores/positive path missing from OpenAPI")
positive_get = paths["/v1/api/scores/positive"].get("get") or {}
if positive_get.get("x-velorix-query-policy-id") != "interactive":
    raise SystemExit(f"promoted API is not linked to interactive policy: {positive_get}")
if positive_get.get("x-velorix-view-id") != "positive_scores_by_user":
    raise SystemExit(f"promoted API does not point at positive_scores_by_user: {positive_get}")

generic_query = read_json(generic_query_path)
if "generic_query_disabled" not in generic_query.get("error", ""):
    raise SystemExit(f"generic query rejection did not prove disabled policy: {generic_query}")

owner = read_json(owner_report_path)
owner_status = "skipped"
owner_matches = None
if owner.get("status") != "skipped":
    owners = owner.get("owners") or []
    owner_status = "pass"
    owner_matches = all(item.get("current_owner_matches_local_process") is True for item in owners)
    if not owners or not owner_matches:
        raise SystemExit(f"standing runtime owner report does not prove local writer ownership: {owner}")

for path in (view_query_path, api_query_path):
    body = read_json(path)
    rows = {row.get("user_id"): row for row in body.get("rows") or []}
    row = rows.get(user_id) or {}
    if row.get("sum") != 25 or row.get("count") != 2:
        raise SystemExit(f"query response does not include expected smoke row in {path}: {body}")

payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_rest_api_smoke",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "pass",
    "run_id": run_id,
    "api_url": api_url,
    "relation_id": "scores",
    "relation_version": "2026-05-24.v1",
    "stream_id": stream_id,
    "user_id": user_id,
    "view_id": "positive_scores_by_user",
    "promoted_api_path": "/v1/api/scores/positive",
    "generic_query_disabled": True,
    "interactive_query_policy_verified": True,
    "standing_runtime_owner_status": owner_status,
    "standing_runtime_owner_matches_local_process": owner_matches,
    "ingested_positive_sum": 25,
    "ingested_positive_count": 2,
    "trusted_for_product_complete": False,
    "evidence_files": {
        "healthz": healthz_path,
        "readyz": readyz_path,
        "relation": relation_path,
        "query_policy": policy_path,
        "view": view_path,
        "views": views_path,
        "openapi": openapi_path,
        "generic_query_disabled": generic_query_path,
        "standing_runtime_owner_acquire": owner_acquire_path,
        "standing_runtime_owner_report": owner_report_path,
        "ingest": ingest_path,
        "view_query": view_query_path,
        "promoted_api_query": api_query_path,
    },
}
with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY

cat <<EOF
REST API smoke passed
api_url=${VELORIX_API_URL}
user_id=${user_id}
evidence=${summary_file}
details=${output_dir}
EOF

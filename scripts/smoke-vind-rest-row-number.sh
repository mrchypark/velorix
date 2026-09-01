#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
base_url="${BASE_URL:-${VELORIX_API_URL:-http://127.0.0.1:8080}}"
auth_header="${VELORIX_API_AUTH_HEADER:-${AUTH_HEADER:-}}"
output_dir="${VELORIX_ROW_NUMBER_REST_API_SMOKE_DIR:-target/velorix-product/row-number-rest-api-smoke}"
summary_file="${VELORIX_ROW_NUMBER_REST_API_SMOKE_EVIDENCE:-target/velorix-product/row-number-rest-api-smoke.json}"
query_wait_seconds="${VELORIX_ROW_NUMBER_REST_API_SMOKE_QUERY_WAIT_SECONDS:-20}"

usage() {
  cat <<'EOF'
Smoke-test an internal experimental ROW_NUMBER materialized view against an
existing non-public Velorix test API.

Usage:
  BASE_URL=http://127.0.0.1:8080 scripts/smoke-vind-rest-row-number.sh

Main environment overrides:
  BASE_URL=http://127.0.0.1:8080
  VELORIX_API_AUTH_HEADER='authorization: Bearer ...'
  VELORIX_ROW_NUMBER_REST_API_SMOKE_DIR=target/velorix-product/row-number-rest-api-smoke
  VELORIX_ROW_NUMBER_REST_API_SMOKE_EVIDENCE=target/velorix-product/row-number-rest-api-smoke.json
  VELORIX_ROW_NUMBER_REST_API_SMOKE_QUERY_WAIT_SECONDS=20
  VELORIX_ROW_NUMBER_REST_API_SMOKE_EXPERIMENTAL=1

The script uses an already running product REST API. It does not start local
build, deploy, or port-forward infrastructure.
ROW_NUMBER is not part of the public 1.0 default contract; this smoke requires
an internal test API that explicitly enables experimental advanced view
features in-process.
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

if [ "${VELORIX_ROW_NUMBER_REST_API_SMOKE_EXPERIMENTAL:-0}" != "1" ]; then
  echo "ROW_NUMBER smoke is experimental and not part of the public 1.0 API; set VELORIX_ROW_NUMBER_REST_API_SMOKE_EXPERIMENTAL=1 and run it only against an internal test API" >&2
  exit 64
fi

case "$query_wait_seconds" in
  '' | *[!0-9]*)
    echo "VELORIX_ROW_NUMBER_REST_API_SMOKE_QUERY_WAIT_SECONDS must be a non-negative integer" >&2
    exit 64
    ;;
esac

base_url="${base_url%/}"
mkdir -p "$output_dir"
mkdir -p "$(dirname "$summary_file")"

auth_args=()
if [ -n "$auth_header" ]; then
  auth_args=(-H "$auth_header")
fi

healthz_file="${output_dir}/healthz.json"
openapi_precheck_file="${output_dir}/openapi-auth-precheck.json"

if ! curl -fsS --max-time 3 "$base_url/healthz" >"$healthz_file" 2>"${output_dir}/healthz.stderr"; then
  echo "REST API is not reachable: ${base_url}/healthz" >&2
  cat "${output_dir}/healthz.stderr" >&2 || true
  exit 75
fi

if ! curl -fsS --max-time 5 ${auth_args[@]+"${auth_args[@]}"} "$base_url/v1/openapi.json" >"$openapi_precheck_file" 2>"${output_dir}/openapi-auth-precheck.stderr"; then
  echo "authenticated REST API is not reachable: ${base_url}/v1/openapi.json" >&2
  echo "set BASE_URL and, if required, VELORIX_API_AUTH_HEADER" >&2
  cat "${output_dir}/openapi-auth-precheck.stderr" >&2 || true
  exit 75
fi

curl_api() {
  curl -fsS --max-time 15 ${auth_args[@]+"${auth_args[@]}"} "$@"
}

curl_api_status() {
  local output_file="$1"
  shift
  curl -sS --max-time 15 -o "$output_file" -w '%{http_code}' ${auth_args[@]+"${auth_args[@]}"} "$@"
}

run_id="$(date -u +%Y%m%dT%H%M%SZ)_$$"
relation_id="rest_row_number_accounts_${run_id}"
view_id="rest_row_number_accounts_by_tier_${run_id}"
relation_version="2026-06-20.v1"
stream_id="${relation_id}_stream"
api_path="/rest-row-number/${run_id}"

readyz_file="${output_dir}/readyz.json"
relation_request_file="${output_dir}/accounts-relation-request.json"
relation_file="${output_dir}/accounts-relation.json"
view_request_file="${output_dir}/row-number-view-request.json"
view_file="${output_dir}/row-number-view.json"
backfill_file="${output_dir}/row-number-backfill.json"
ingest_request_file="${output_dir}/accounts-ingest-request.json"
ingest_file="${output_dir}/accounts-ingest.json"
view_query_file="${output_dir}/row-number-view-query.json"

python3 - \
  "$relation_request_file" \
  "$view_request_file" \
  "$ingest_request_file" \
  "$relation_id" \
  "$view_id" \
  "$relation_version" \
  "$stream_id" \
  "$api_path" <<'PY'
import hashlib
import json
import sys

(
    relation_request_path,
    view_request_path,
    ingest_request_path,
    relation_id,
    view_id,
    relation_version,
    stream_id,
    api_path,
) = sys.argv[1:]


def column(column_id, kind, ordinal, role):
    return {
        "column_id": column_id,
        "name": column_id,
        "logical_type": {"kind": kind},
        "physical_arrow_type": {"kind": "utf8" if kind == "utf8" else kind},
        "nullable": False,
        "ordinal": ordinal,
        "semantic_role": role,
    }


relation_schema = {
    "relation_id": relation_id,
    "relation_name": relation_id,
    "relation_version": relation_version,
    "columns": [
        column("account_id", "utf8", 0, "primary_key"),
        column("tier", "utf8", 1, "metadata"),
        column("score", "int64", 2, "value"),
        column("delta", "int64", 3, "weight"),
    ],
    "primary_key_column_ids": ["account_id"],
    "weight_column_id": "delta",
    "allowed_operations": ["insert", "delete"],
    "event_time_column_id": None,
}
canonical = json.dumps(relation_schema, separators=(",", ":"), ensure_ascii=False).encode()
schema_fingerprint = "sha256:" + hashlib.sha256(
    b"velorix-relation-schema-v1\0" + canonical
).hexdigest()
catalog = {
    "schema_version": 1,
    "relation_schema": relation_schema,
    "schema_fingerprint": schema_fingerprint,
    "datafusion_registration": {"name": relation_id, "mode": "table"},
    "incremental_relation": {
        "relation_id": relation_id,
        "schema_fingerprint": schema_fingerprint,
    },
    "incremental_adapter": {
        "adapter_id": "incremental-adapter-single-key-sum-count-v1",
    },
}
sql = (
    f"select account_id, row_number() over "
    f"(partition by tier order by score desc, account_id asc) as tier_rank "
    f"from {relation_id} where score > 0"
)
files = {
    relation_request_path: {
        "catalog": catalog,
        "default_orders_sum_count": False,
    },
    view_request_path: {
        "view_id": view_id,
        "urlPath": api_path,
        "inputRelationRefs": [
            {"relation_id": relation_id, "relation_version": relation_version}
        ],
        "sql": sql,
        "description": "standalone REST smoke for ROW_NUMBER materialized view ranking",
        "response_formats": ["json"],
    },
    ingest_request_path: {
        "relation_version": relation_version,
        "stream_id": stream_id,
        "partition_id": 0,
        "start_offset_inclusive": 0,
        "rows": [
            {"account_id": "acct-a", "tier": "gold", "score": 120, "delta": 1},
            {"account_id": "acct-b", "tier": "gold", "score": 120, "delta": 1},
            {"account_id": "acct-c", "tier": "gold", "score": 90, "delta": 1},
            {"account_id": "acct-d", "tier": "silver", "score": 70, "delta": 1},
            {"account_id": "acct-e", "tier": "gold", "score": -1, "delta": 1},
        ],
    },
}
for path, payload in files.items():
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=False)
        f.write("\n")
PY

curl_api "$base_url/readyz" >"$readyz_file"

relation_status="$(curl_api_status "$relation_file" \
  -X POST "$base_url/v1/relations" \
  -H 'content-type: application/json' \
  -d @"$relation_request_file")"
case "$relation_status" in
  200 | 201) ;;
  *)
    echo "expected relation creation to return 200 or 201; got ${relation_status}" >&2
    cat "$relation_file" >&2 || true
    exit 1
    ;;
esac

view_status="$(curl_api_status "$view_file" \
  -X POST "$base_url/v1/views" \
  -H 'content-type: application/json' \
  -d @"$view_request_file")"
case "$view_status" in
  200 | 201) ;;
  *)
    python3 - \
      "$summary_file" \
      "$run_id" \
      "$base_url" \
      "$relation_id" \
      "$relation_version" \
      "$view_id" \
      "$api_path" \
      "$view_status" \
      "$relation_file" \
      "$view_file" \
      "$view_request_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    summary_path,
    run_id,
    api_url,
    relation_id,
    relation_version,
    view_id,
    api_path,
    view_status,
    relation_path,
    view_path,
    view_request_path,
) = sys.argv[1:]


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


view_request = read_json(view_request_path)
try:
    view_response = read_json(view_path)
except json.JSONDecodeError:
    view_response = {"raw": open(view_path, "r", encoding="utf-8").read()}

payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_row_number_rest_api_smoke",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "blocked",
    "blocker_kind": "row_number_view_admission_failed",
    "http_status": int(view_status),
    "api_url": api_url,
    "run_id": run_id,
    "relation_id": relation_id,
    "relation_version": relation_version,
    "view_id": view_id,
    "promoted_api_path": api_path,
    "sql": view_request["sql"],
    "view_response": view_response,
    "trusted_for_product_complete": False,
    "evidence_files": {
        "relation": relation_path,
        "view": view_path,
        "view_request": view_request_path,
    },
}
with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
    echo "expected ROW_NUMBER view creation to return 200 or 201; got ${view_status}" >&2
    cat "$view_file" >&2 || true
    printf '\n' >&2
    echo "wrote ROW_NUMBER REST smoke blocker evidence to ${summary_file}" >&2
    exit 1
    ;;
esac

curl_api -X POST "$base_url/v1/relations/${relation_id}/ingest" \
  -H 'content-type: application/json' \
  -d @"$ingest_request_file" \
  >"$ingest_file"

backfill_status="$(curl_api_status "$backfill_file" \
  -X POST "$base_url/v1/views/${view_id}/backfill" \
  -H 'content-type: application/json' \
  -d '{}')"
case "$backfill_status" in
  200 | 201) ;;
  *)
    echo "expected ROW_NUMBER view backfill to return 200 or 201; got ${backfill_status}" >&2
    cat "$backfill_file" >&2 || true
    exit 1
    ;;
esac

deadline=$((SECONDS + query_wait_seconds))
while true; do
  query_status="$(curl_api_status "$view_query_file" \
    -X POST "$base_url/v1/views/${view_id}/query" \
    -H 'content-type: application/json' \
    -d '{"max_rows":1000}')"
  if [ "$query_status" = "200" ] && python3 - "$view_query_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)

rows = {row.get("account_id"): row for row in body.get("rows") or []}
expected = {
    "acct-a": 1,
    "acct-b": 2,
    "acct-c": 3,
    "acct-d": 1,
}
for account_id, expected_rank in expected.items():
    row = rows.get(account_id) or {}
    if row.get("tier_rank") != expected_rank:
        raise SystemExit(1)
if "acct-e" in rows:
    raise SystemExit(1)
PY
  then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for ROW_NUMBER view ${view_id} to materialize expected ranks" >&2
    cat "$view_query_file" >&2 || true
    exit 1
  fi
  sleep 1
done

python3 - \
  "$summary_file" \
  "$run_id" \
  "$base_url" \
  "$relation_id" \
  "$relation_version" \
  "$stream_id" \
  "$view_id" \
  "$api_path" \
  "$relation_file" \
  "$view_file" \
  "$ingest_file" \
  "$view_query_file" \
  "$view_request_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    summary_path,
    run_id,
    api_url,
    relation_id,
    relation_version,
    stream_id,
    view_id,
    api_path,
    relation_path,
    view_path,
    ingest_path,
    query_path,
    view_request_path,
) = sys.argv[1:]


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


view_request = read_json(view_request_path)
query = read_json(query_path)
rows = {row.get("account_id"): row for row in query.get("rows") or []}
verified = {
    account_id: rows[account_id]
    for account_id in ("acct-a", "acct-b", "acct-c", "acct-d")
}
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_row_number_rest_api_smoke",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "pass",
    "run_id": run_id,
    "api_url": api_url,
    "relation_id": relation_id,
    "relation_version": relation_version,
    "stream_id": stream_id,
    "view_id": view_id,
    "promoted_api_path": api_path,
    "sql": view_request["sql"],
    "row_number_rows_verified": verified,
    "filtered_account_absent": "acct-e",
    "trusted_for_product_complete": False,
    "evidence_files": {
        "relation": relation_path,
        "view": view_path,
        "ingest": ingest_path,
        "view_query": query_path,
    },
}
with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY

cat <<EOF
REST ROW_NUMBER smoke passed
api_url=${base_url}
relation_id=${relation_id}
view_id=${view_id}
evidence=${summary_file}
details=${output_dir}
EOF

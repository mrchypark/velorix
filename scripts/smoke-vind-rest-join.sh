#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
auth_env_file="${VELORIX_API_AUTH_ENV:-${product_dir}/api-auth.env}"
attach_evidence_file="${VELORIX_API_ATTACH_EVIDENCE:-${product_dir}/rest-attach-evidence.json}"
output_dir="${VELORIX_JOIN_REST_API_SMOKE_DIR:-${product_dir}/join-rest-api-smoke}"
summary_file="${VELORIX_JOIN_REST_API_SMOKE_EVIDENCE:-${product_dir}/join-rest-api-smoke.json}"
summary_public_file="${summary_file%.json}.public.json"
auto_attach="${VELORIX_JOIN_REST_API_SMOKE_ATTACH:-auto}"
query_wait_seconds="${VELORIX_JOIN_REST_API_SMOKE_QUERY_WAIT_SECONDS:-20}"
authoritative_relation_ingest="${VELORIX_API_AUTHORITATIVE_RELATION_INGEST:-0}"

usage() {
  cat <<'EOF'
Smoke-test a two-relation REST join against an existing Velorix product API.
Uses one-batch relation-scoped ingest in authoritative mode; legacy mode uses
multi-relation `/v1/relations/ingest` as the ordered join frontier path.

Usage:
  scripts/smoke-vind-rest-join.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_API_AUTH_ENV=target/velorix-product/api-auth.env
  VELORIX_JOIN_REST_API_SMOKE_DIR=target/velorix-product/join-rest-api-smoke
  VELORIX_JOIN_REST_API_SMOKE_EVIDENCE=target/velorix-product/join-rest-api-smoke.json
  VELORIX_JOIN_REST_API_SMOKE_ATTACH=auto
  VELORIX_JOIN_REST_API_SMOKE_QUERY_WAIT_SECONDS=20

The script uses an already running product REST API. If healthz is not
reachable and VELORIX_JOIN_REST_API_SMOKE_ATTACH is auto or 1, it reuses
scripts/attach-vind-product-rest.sh to recreate the local port-forward.
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

write_public_evidence() {
  local path="$1"
  if [ -z "$path" ] || [ ! -f "$path" ]; then
    echo "public join evidence requires an existing private JSON artifact" >&2
    return 66
  fi
  chmod 600 "$path"
  VELORIX_EVIDENCE_REDACT_ONLY=1 \
    VELORIX_EVIDENCE_REDACT_ONLY_FILE="$path" \
    "$repo_root/scripts/run-vind-product.sh" >/dev/null
}

case "$authoritative_relation_ingest" in
  0 | 1) ;;
  *)
    echo "VELORIX_API_AUTHORITATIVE_RELATION_INGEST must be 0 or 1" >&2
    exit 64
    ;;
esac
if [ "$authoritative_relation_ingest" = "1" ]; then
  require jq
fi

cd "$repo_root"

case "$auto_attach" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_JOIN_REST_API_SMOKE_ATTACH must be auto, 0, or 1" >&2
    exit 64
    ;;
esac
case "$query_wait_seconds" in
  '' | *[!0-9]*)
    echo "VELORIX_JOIN_REST_API_SMOKE_QUERY_WAIT_SECONDS must be a non-negative integer" >&2
    exit 64
    ;;
esac

if [ ! -f "$auth_env_file" ]; then
  echo "missing API auth environment" >&2
  echo "run scripts/run-vind-product.sh first, or reattach with scripts/attach-vind-product-rest.sh" >&2
  exit 66
fi

mkdir -p "$output_dir"
chmod 700 "$output_dir"

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
      echo "REST API health check failed" >&2
      echo "run scripts/attach-vind-product-rest.sh or set VELORIX_JOIN_REST_API_SMOKE_ATTACH=auto" >&2
      exit 75
      ;;
  esac
fi

auth_precheck_file="${output_dir}/openapi-auth-precheck.json"
if ! curl -fsS --max-time 5 "$VELORIX_API_URL/v1/openapi.json" -H "$VELORIX_API_AUTH_HEADER" >"$auth_precheck_file" 2>/dev/null; then
  case "$auto_attach" in
    auto | 1)
      attach_rest_api
      if ! curl -fsS --max-time 5 "$VELORIX_API_URL/v1/openapi.json" -H "$VELORIX_API_AUTH_HEADER" >"$auth_precheck_file" 2>/dev/null; then
        echo "authenticated REST API precheck failed after reattach" >&2
        exit 75
      fi
      ;;
    0)
      echo "authenticated REST API precheck failed" >&2
      echo "run scripts/attach-vind-product-rest.sh or set VELORIX_JOIN_REST_API_SMOKE_ATTACH=auto" >&2
      exit 75
      ;;
  esac
fi

curl_api() {
  curl -H "Accept: application/json" -fsS --max-time 15 "$@" -H "$VELORIX_API_AUTH_HEADER" 2>/dev/null
}

curl_api_status() {
  local output_file="$1"
  shift
  curl -H "Accept: application/json" -sS --max-time 15 -o "$output_file" -w '%{http_code}' "$@" -H "$VELORIX_API_AUTH_HEADER" 2>/dev/null
}

run_id="$(date -u +%Y%m%dT%H%M%SZ)_$$"
left_relation_id="rest_join_left_${run_id}"
right_relation_id="rest_join_right_${run_id}"
view_id="rest_join_left_right_${run_id}"
relation_version="2026-05-24.v1"
left_stream_id="${left_relation_id}_stream"
right_stream_id="${right_relation_id}_stream"
api_path="/rest-join/${run_id}"

healthz_file="${output_dir}/healthz.json"
readyz_file="${output_dir}/readyz.json"
left_relation_request_file="${output_dir}/left-relation-request.json"
right_relation_request_file="${output_dir}/right-relation-request.json"
left_relation_file="${output_dir}/left-relation.json"
right_relation_file="${output_dir}/right-relation.json"
view_request_file="${output_dir}/join-view-request.json"
view_file="${output_dir}/join-view.json"
backfill_file="${output_dir}/join-backfill.json"
relations_ingest_file="${output_dir}/relations-ingest.json"
left_ingest_file="${output_dir}/left-ingest.json"
right_ingest_file="${output_dir}/right-ingest.json"
view_query_file="${output_dir}/join-view-query.json"

python3 - \
  "$left_relation_request_file" \
  "$right_relation_request_file" \
  "$view_request_file" \
  "$left_relation_id" \
  "$right_relation_id" \
  "$view_id" \
  "$relation_version" \
  "$api_path" <<'PY'
import hashlib
import json
import sys

(
    left_request_path,
    right_request_path,
    view_request_path,
    left_relation_id,
    right_relation_id,
    view_id,
    relation_version,
    api_path,
) = sys.argv[1:]


def logical(kind):
    return {"kind": kind}


def physical(kind):
    return {"kind": kind}


def column(column_id, kind, ordinal, role):
    return {
        "column_id": column_id,
        "name": column_id,
        "logical_type": logical(kind),
        "physical_arrow_type": physical("utf8" if kind == "utf8" else kind),
        "nullable": False,
        "ordinal": ordinal,
        "semantic_role": role,
    }


def schema(relation_id, columns, primary_key):
    return {
        "relation_id": relation_id,
        "relation_name": relation_id,
        "relation_version": relation_version,
        "columns": columns,
        "primary_key_column_ids": [primary_key],
        "weight_column_id": "delta",
        "allowed_operations": ["insert", "delete"],
        "event_time_column_id": None,
    }


def fingerprint(relation_schema):
    canonical = json.dumps(relation_schema, separators=(",", ":"), ensure_ascii=False).encode()
    digest = hashlib.sha256(b"velorix-relation-schema-v1\0" + canonical).hexdigest()
    return f"sha256:{digest}"


def catalog(relation_schema):
    schema_fingerprint = fingerprint(relation_schema)
    relation_id = relation_schema["relation_id"]
    return {
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


left_schema = schema(
    left_relation_id,
    [
        column("key_col", "utf8", 0, "primary_key"),
        column("value_col", "int64", 1, "metadata"),
        column("delta", "int64", 2, "weight"),
    ],
    "key_col",
)
right_schema = schema(
    right_relation_id,
    [
        column("key_col", "utf8", 0, "primary_key"),
        column("value_col", "int64", 1, "value"),
        column("category", "utf8", 2, "metadata"),
        column("delta", "int64", 3, "weight"),
    ],
    "key_col",
)

sql = (
    "select r.key_col, sum(l.value_col) as total_value, "
    "count(*) as row_count, min(l.value_col) as min_value, "
    f"max(l.value_col) as max_value from {left_relation_id} l "
    f"join {right_relation_id} r on l.key_col = r.key_col group by r.key_col"
)

files = {
    left_request_path: {
        "catalog": catalog(left_schema),
        "default_orders_sum_count": False,
    },
    right_request_path: {
        "catalog": catalog(right_schema),
        "default_orders_sum_count": False,
    },
    view_request_path: {
        "view_id": view_id,
        "urlPath": api_path,
        "inputRelationRefs": [
            {"relation_id": left_relation_id, "relation_version": relation_version},
            {"relation_id": right_relation_id, "relation_version": relation_version},
        ],
        "sql": sql,
        "description": "standalone REST smoke for a two-relation materialized join via multi-relation relation ingest",
        "response_formats": ["json"],
    },
}

for path, payload in files.items():
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=False)
        f.write("\n")
PY

curl -fsS --max-time 10 "$VELORIX_API_URL/healthz" >"$healthz_file"
curl_api "$VELORIX_API_URL/readyz" >"$readyz_file"
if [ "$authoritative_relation_ingest" = "1" ] && ! jq -e '
  .relation_ingest.mode == "authoritative"
  and .relation_ingest.authoritative == true
  and .relation_ingest.owner_id_configured == true
' "$readyz_file" >/dev/null; then
  echo "readyz relation ingest capability did not confirm authoritative mode" >&2
  exit 1
fi

left_relation_status="$(curl_api_status "$left_relation_file" \
  -X POST "$VELORIX_API_URL/v1/relations" \
  -H 'content-type: application/json' \
  -d @"$left_relation_request_file")"
case "$left_relation_status" in
  200 | 201) ;;
  *)
    echo "expected left relation creation to return 200 or 201; got ${left_relation_status}" >&2
    echo "left relation creation failed; raw response remains in private evidence" >&2
    exit 1
    ;;
esac

right_relation_status="$(curl_api_status "$right_relation_file" \
  -X POST "$VELORIX_API_URL/v1/relations" \
  -H 'content-type: application/json' \
  -d @"$right_relation_request_file")"
case "$right_relation_status" in
  200 | 201) ;;
  *)
    echo "expected right relation creation to return 200 or 201; got ${right_relation_status}" >&2
    echo "right relation creation failed; raw response remains in private evidence" >&2
    exit 1
    ;;
esac

view_status="$(curl_api_status "$view_file" \
  -X POST "$VELORIX_API_URL/v1/views" \
  -H 'content-type: application/json' \
  -d @"$view_request_file")"
case "$view_status" in
  200 | 201 | 409) ;;
  *)
    python3 - \
      "$summary_file" \
      "$run_id" \
      "$VELORIX_API_URL" \
      "$left_relation_id" \
      "$right_relation_id" \
      "$relation_version" \
      "$view_id" \
      "$api_path" \
      "$view_status" \
      "$authoritative_relation_ingest" \
      "$left_relation_file" \
      "$right_relation_file" \
      "$view_file" \
      "$view_request_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    summary_path,
    run_id,
    api_url,
    left_relation_id,
    right_relation_id,
    relation_version,
    view_id,
    api_path,
    view_status,
    authoritative_relation_ingest,
    left_relation_path,
    right_relation_path,
    view_path,
    view_request_path,
) = sys.argv[1:]

with open(view_request_path, "r", encoding="utf-8") as f:
    view_request = json.load(f)
with open(view_path, "r", encoding="utf-8") as f:
    view_response = json.load(f)

payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_join_rest_api_smoke",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "blocked",
    "blocker_kind": "join_view_admission_failed",
    "http_status": int(view_status),
    "relation_ingest_mode": "authoritative-single-batch" if authoritative_relation_ingest == "1" else "legacy-multi-relation",
    "api_url": api_url,
    "run_id": run_id,
    "relation_ids": [left_relation_id, right_relation_id],
    "relation_version": relation_version,
    "view_id": view_id,
    "promoted_api_path": api_path,
    "sql": view_request["sql"],
    "join_frontier_path": "/v1/relations/{relation_id}/ingest" if authoritative_relation_ingest == "1" else "/v1/relations/ingest",
    "join_frontier_contract": "sequential_relation_ingest_frontier_vector",
    "view_response": view_response,
    "trusted_for_product_complete": False,
    "evidence_files": {
        "left_relation": left_relation_path,
        "right_relation": right_relation_path,
        "view": view_path,
    },
}
with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
    write_public_evidence "$summary_file"
    echo "expected join view creation to return 200, 201, or duplicate 409; got ${view_status}" >&2
    echo "join view admission failed; raw response remains in private evidence" >&2
    echo "wrote join REST smoke blocker public evidence to ${summary_public_file}" >&2
    exit 1
    ;;
esac

python3 - "$output_dir/left-ingest-request.json" "$output_dir/right-ingest-request.json" "$output_dir/relations-ingest-request.json" "$left_relation_id" "$right_relation_id" "$relation_version" "$left_stream_id" "$right_stream_id" <<'PY'
import json
import sys

left_path, right_path, relations_path, left_relation_id, right_relation_id, relation_version, left_stream_id, right_stream_id = sys.argv[1:]
payloads = {
    left_path: {
        "relation_id": left_relation_id,
        "relation_version": relation_version,
        "stream_id": left_stream_id,
        "partition_id": 0,
        "start_offset_inclusive": 0,
        "rows": [
            {"key_col": "key-a", "value_col": 42, "delta": 1},
            {"key_col": "key-a", "value_col": 40, "delta": 1},
            {"key_col": "key-b", "value_col": 15, "delta": 1},
        ],
    },
    right_path: {
        "relation_id": right_relation_id,
        "relation_version": relation_version,
        "stream_id": right_stream_id,
        "partition_id": 0,
        "start_offset_inclusive": 0,
        "rows": [
            {"key_col": "key-a", "value_col": -2, "category": "group-a", "delta": 1},
            {"key_col": "key-b", "value_col": 3, "category": "group-b", "delta": 1},
        ],
    },
}
for path, payload in payloads.items():
    with open(path, "w", encoding="utf-8") as f:
        single_relation_payload = {k: v for k, v in payload.items() if k != "relation_id"}
        json.dump(single_relation_payload, f, indent=2, sort_keys=False)
        f.write("\n")
with open(relations_path, "w", encoding="utf-8") as f:
    json.dump({"batches": list(payloads.values())}, f, indent=2, sort_keys=False)
    f.write("\n")
PY

if [ "$authoritative_relation_ingest" = "1" ]; then
  curl_api -X POST "$VELORIX_API_URL/v1/relations/${left_relation_id}/ingest" \
    -H 'content-type: application/json' \
    -d @"$output_dir/left-ingest-request.json" \
    >"$left_ingest_file"
  curl_api -X POST "$VELORIX_API_URL/v1/relations/${right_relation_id}/ingest" \
    -H 'content-type: application/json' \
    -d @"$output_dir/right-ingest-request.json" \
    >"$right_ingest_file"
  python3 - "$relations_ingest_file" "$left_ingest_file" "$right_ingest_file" <<'PY'
import json
import sys

output_path, left_path, right_path = sys.argv[1:]
with open(left_path, "r", encoding="utf-8") as f:
    left = json.load(f)
with open(right_path, "r", encoding="utf-8") as f:
    right = json.load(f)
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(
        {
            "mode": "authoritative-single-batch",
            "batches": [left, right],
        },
        f,
        indent=2,
        sort_keys=True,
    )
    f.write("\n")
PY
else
  curl_api -X POST "$VELORIX_API_URL/v1/relations/ingest" \
    -H 'content-type: application/json' \
    -d @"$output_dir/relations-ingest-request.json" \
    >"$relations_ingest_file"
fi

backfill_status="$(curl_api_status "$backfill_file" \
  -X POST "$VELORIX_API_URL/v1/views/${view_id}/backfill" \
  -H 'content-type: application/json' \
  -d '{}')"
case "$backfill_status" in
  200 | 201) ;;
  *)
    echo "expected join view backfill to return 200 or 201; got ${backfill_status}" >&2
    echo "join view backfill failed; raw response remains in private evidence" >&2
    exit 1
    ;;
esac

deadline=$((SECONDS + query_wait_seconds))
while true; do
  query_status="$(curl_api_status "$view_query_file" \
    -X POST "$VELORIX_API_URL/v1/views/${view_id}/query" \
    -H 'content-type: application/json' \
    -d '{"max_rows":1000}')"
  if [ "$query_status" = "200" ] && python3 - "$view_query_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)

rows = {row.get("key_col"): row for row in body.get("rows") or []}
key_a = rows.get("key-a") or {}
key_b = rows.get("key-b") or {}
if key_a.get("total_value") != 82 or key_a.get("row_count") != 2:
    raise SystemExit(1)
if key_a.get("min_value") != 40 or key_a.get("max_value") != 42:
    raise SystemExit(1)
if key_b.get("total_value") != 15 or key_b.get("row_count") != 1:
    raise SystemExit(1)
if key_b.get("min_value") != 15 or key_b.get("max_value") != 15:
    raise SystemExit(1)
PY
  then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for join view materialization" >&2
    exit 1
  fi
  sleep 1
done

python3 - \
  "$summary_file" \
  "$run_id" \
  "$VELORIX_API_URL" \
  "$left_relation_id" \
  "$right_relation_id" \
  "$relation_version" \
  "$view_id" \
  "$api_path" \
  "$authoritative_relation_ingest" \
  "$left_relation_file" \
  "$right_relation_file" \
  "$view_file" \
  "$relations_ingest_file" \
  "$view_query_file" \
  "$view_request_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    summary_path,
    run_id,
    api_url,
    left_relation_id,
    right_relation_id,
    relation_version,
    view_id,
    api_path,
    authoritative_relation_ingest,
    left_relation_path,
    right_relation_path,
    view_path,
    relations_ingest_path,
    query_path,
    view_request_path,
) = sys.argv[1:]


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


view_request = read_json(view_request_path)
query = read_json(query_path)
rows = {row.get("key_col"): row for row in query.get("rows") or []}
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_join_rest_api_smoke",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "pass",
    "run_id": run_id,
    "api_url": api_url,
    "relation_ingest_mode": "authoritative-single-batch" if authoritative_relation_ingest == "1" else "legacy-multi-relation",
    "relation_ids": [left_relation_id, right_relation_id],
    "relation_version": relation_version,
    "view_id": view_id,
    "promoted_api_path": api_path,
    "sql": view_request["sql"],
    "join_frontier_path": "/v1/relations/{relation_id}/ingest" if authoritative_relation_ingest == "1" else "/v1/relations/ingest",
    "join_frontier_contract": "sequential_relation_ingest_frontier_vector",
    "join_rows_verified": {
        "key-a": rows["key-a"],
        "key-b": rows["key-b"],
    },
    "trusted_for_product_complete": False,
    "evidence_files": {
        "left_relation": left_relation_path,
        "right_relation": right_relation_path,
        "view": view_path,
        "relations_ingest_frontier_vector": relations_ingest_path,
        "view_query": query_path,
    },
}
with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
write_public_evidence "$summary_file"

cat <<EOF
REST join smoke passed
identifier_redaction=enabled
evidence_public=${summary_public_file}
join_rows_verified=key-a,key-b
EOF

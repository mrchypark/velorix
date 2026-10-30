#!/bin/sh
set -eu

# Public HTTP semantic probes used by the Dory Chaos Mesh runner. The recovery
# probe is safe to repeat: offset 1 must append once and then remain duplicate.

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
mode=${1:-}
api_url=${VELORIX_CHAOS_API_URL:-}
api_token=${VELORIX_CHAOS_API_TOKEN:-}
evidence=${VELORIX_CHAOS_EVIDENCE_DIR:-}
relation_id=${VELORIX_CHAOS_RELATION_ID:-samples}
sum_view_id=${VELORIX_CHAOS_SUM_VIEW_ID:-sample_sum}
count_view_id=${VELORIX_CHAOS_COUNT_VIEW_ID:-sample_count}
stream_id=${VELORIX_CHAOS_STREAM_ID:-samples-stream}

die() { echo "rhiza Dory chaos HTTP probe: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }
need curl
need jq
need shasum
need mktemp
need stat
need id

case "$mode" in baseline|baseline-check|recovery|during-s3) ;; *) die 'usage: check-rhiza-dory-chaos-http.sh baseline|baseline-check|recovery|during-s3' ;; esac
[ -n "$api_url" ] || die 'set VELORIX_CHAOS_API_URL to the local port-forwarded public API URL'
[ -n "$api_token" ] || die 'set VELORIX_CHAOS_API_TOKEN'
[ -n "$evidence" ] || die 'set VELORIX_CHAOS_EVIDENCE_DIR'
case "$api_url" in http://127.0.0.1:*|http://localhost:*) ;; *) die 'API URL must be a local loopback port-forward' ;; esac
[ -d "$evidence" ] || die 'evidence directory must already exist'
[ ! -L "$evidence" ] || die 'evidence directory must not be a symbolic link'
evidence=$(CDPATH='' cd -- "$evidence" && pwd -P) || die 'could not resolve evidence directory'
umask 077
evidence_owner=$(stat -c '%u' "$evidence" 2>/dev/null || stat -f '%u' "$evidence" 2>/dev/null) || die 'could not inspect evidence directory owner'
evidence_mode=$(stat -c '%a' "$evidence" 2>/dev/null || stat -f '%Lp' "$evidence" 2>/dev/null) || die 'could not inspect evidence directory permissions'
[ "$evidence_owner" = "$(id -u)" ] || die 'evidence directory must be owned by the current user'
[ "$evidence_mode" = 700 ] || die 'evidence directory must have mode 700'
phase_dir=$(mktemp -d "$evidence/$mode.XXXXXX") || die 'could not create a fresh private phase output directory'
evidence=$phase_dir
auth_header=$(mktemp "$evidence/.authorization-header.XXXXXX") || die 'could not create a private authorization header file'
trap 'rm -f "$auth_header"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'Authorization: Bearer %s\n' "$api_token" >"$auth_header"
unset api_token
unset VELORIX_CHAOS_API_TOKEN
echo "Evidence: $evidence"
mkdir "$evidence/payloads"
. "$repo_root/scripts/rhiza-service-recovery-payloads.sh"
rhiza_service_recovery_write_payloads "$evidence/payloads"
# Use a fresh relation identity when requested. Compute the schema fingerprint
# with the same domain-separated canonical JSON contract as SchemaFingerprintV1.
jq --arg relation "$relation_id" '
  .catalog.relation_schema.relation_id = $relation |
  .catalog.relation_schema.relation_name = $relation |
  .catalog.datafusion_registration.name = $relation |
  .catalog.incremental_relation.relation_id = $relation
' "$evidence/payloads/relation.json" >"$evidence/payloads/relation-schema.tmp"
mv "$evidence/payloads/relation-schema.tmp" "$evidence/payloads/relation.json"
schema=$(jq -c '.catalog.relation_schema' "$evidence/payloads/relation.json")
fingerprint=$(printf 'velorix-relation-schema-v1\000%s' "$schema" | shasum -a 256 | awk '{print $1}')
jq --arg fingerprint "sha256:$fingerprint" '
  .catalog.schema_fingerprint = $fingerprint |
  .catalog.incremental_relation.schema_fingerprint = $fingerprint
' "$evidence/payloads/relation.json" >"$evidence/payloads/relation.tmp"
mv "$evidence/payloads/relation.tmp" "$evidence/payloads/relation.json"
# The public CreateViewRequest is explicitly standing_view; split roles use
# authoritative demand ingest and never fall back to all-role legacy execution.
for view_payload in "$evidence/payloads/view-sum.json" "$evidence/payloads/view-count.json"; do
  jq --arg relation "$relation_id" --arg sum_view "$sum_view_id" --arg count_view "$count_view_id" '
    .input_relation_id = $relation |
    .view_id = (if .view_id == "sample_sum" then $sum_view else $count_view end) |
    .sql |= gsub("samples"; $relation) |
    . + {"source_kind":"standing_view"}
  ' "$view_payload" >"$view_payload.tmp"
  mv "$view_payload.tmp" "$view_payload"
done
jq --arg stream "$stream_id" '.stream_id = $stream' \
  "$evidence/payloads/ingest.json" >"$evidence/payloads/ingest.tmp"
mv "$evidence/payloads/ingest.tmp" "$evidence/payloads/ingest.json"

request() {
  method=$1 path=$2 payload=$3 output=$4
  code=$(curl --silent --show-error --max-time 60 -o "$output" -w '%{http_code}' \
    -X "$method" -H "@$auth_header" \
    -H 'Content-Type: application/json' -H 'Accept: application/json' \
    --data-binary "@$payload" "$api_url$path") || die "HTTP request failed: $method $path"
  printf '%s %s %s\n' "$method" "$path" "$code" >>"$evidence/http-status.log"
  case "$code" in 2*) ;; *) die "$method $path returned HTTP $code (see $output)" ;; esac
}

query() {
  view=$1 output=$2
  tries=0
  while [ "$tries" -lt 90 ]; do
    code=$(curl --silent --show-error --max-time 30 -o "$output" -w '%{http_code}' \
      -H "@$auth_header" -H 'Content-Type: application/json' \
      -H 'Accept: application/json' --data-binary "@$evidence/payloads/query.json" \
      "$api_url/v1/views/$view/query") || die "query transport failed: $view"
    printf 'POST /v1/views/%s/query %s\n' "$view" "$code" >>"$evidence/http-status.log"
    if [ "$code" = 200 ]; then
      jq -e '(.rows | type == "array") and (.rows | length > 0) and (has("materialization") | not)' "$output" >/dev/null \
        || die "query returned no materialized result rows: $view"
      return 0
    fi
    [ "$code" = 202 ] || die "query returned HTTP $code: $view (see $output)"
    sleep 1
    tries=$((tries + 1))
  done
  die "timed out waiting for materialized query: $view"
}

assert_values() {
  expected_sum=$1 expected_count=$2 label=$3
  query "$sum_view_id" "$evidence/$label-sum.json"
  query "$count_view_id" "$evidence/$label-count.json"
  jq -e --argjson v "$expected_sum" '.rows[0].result == $v' "$evidence/$label-sum.json" >/dev/null \
    || die "$label sum did not equal $expected_sum"
  jq -e --argjson v "$expected_count" '.rows[0].result == $v' "$evidence/$label-count.json" >/dev/null \
    || die "$label count did not equal $expected_count"
}

if [ "$mode" = baseline-check ]; then
  expected_sum=${VELORIX_CHAOS_EXPECT_SUM:-2.5}
  expected_count=${VELORIX_CHAOS_EXPECT_COUNT:-1}
  assert_values "$expected_sum" "$expected_count" baseline-check
  echo "PASS read-only public baseline query: sum=$expected_sum count=$expected_count"
  exit 0
fi

if [ "$mode" = baseline ]; then
  request POST /v1/relations "$evidence/payloads/relation.json" "$evidence/register-response.json"
  request POST /v1/views "$evidence/payloads/view-sum.json" "$evidence/view-sum-response.json"
  request POST /v1/views "$evidence/payloads/view-count.json" "$evidence/view-count-response.json"
  request POST "/v1/relations/$relation_id/ingest" "$evidence/payloads/ingest.json" "$evidence/ingest-initial-response.json"
  outcome=$(jq -r '.outcome // empty' "$evidence/ingest-initial-response.json")
  case "$outcome" in appended|duplicate) ;; *) die "initial ingest had unexpected outcome: $outcome" ;; esac
  request POST "/v1/relations/$relation_id/ingest" "$evidence/payloads/ingest.json" "$evidence/ingest-initial-retry-response.json"
  jq -e '.outcome == "duplicate"' "$evidence/ingest-initial-retry-response.json" >/dev/null \
    || die 'same-offset baseline retry was not reported as duplicate'
  assert_values 2.5 1 baseline
  echo "PASS public HTTP authoritative-demand baseline: registered relation/views, ingest=$outcome, same-offset retry=duplicate, sum=2.5 count=1"
  exit 0
fi

if [ "$mode" = during-s3 ]; then
  jq --arg stream "$stream_id" '.stream_id=$stream | .start_offset_inclusive=4 | .rows=[{"reading":9,"delta":1}]' \
    "$evidence/payloads/ingest.json" >"$evidence/payloads/ingest-offset-4.json"
  set +e
  code=$(curl --silent --show-error --max-time 8 -o "$evidence/during-s3-response.json" \
    -w '%{http_code}' -X POST -H "@$auth_header" \
    -H 'Content-Type: application/json' -H 'Accept: application/json' \
    --data-binary "@$evidence/payloads/ingest-offset-4.json" \
    "$api_url/v1/relations/$relation_id/ingest" 2>"$evidence/during-s3-curl.stderr")
  curl_rc=$?
  set -e
  printf '%s\n' "$code" >"$evidence/during-s3-http-status.txt"
  if [ "$curl_rc" -ne 0 ]; then
    die "during-S3 mutation transport timed out before an application failure response (curl exit $curl_rc)"
  fi
  if [ "$curl_rc" -eq 0 ]; then
    case "$code" in 5??) ;; 2*) die 'fresh offset-4 mutation unexpectedly succeeded during S3 partition' ;; *) die "during-S3 mutation returned HTTP $code, not a storage failure" ;; esac
    jq -e '(.error // .message // tostring) | test("s3|object|storage|checkpoint|metadata|transport|timeout|connection"; "i")' \
      "$evidence/during-s3-response.json" >/dev/null \
      || die 'during-S3 HTTP failure did not identify a storage/transport error'
  fi
  printf 'observed_failed_fresh_mutation=true http_status=%s curl_exit=%s\n' "$code" "$curl_rc"
  exit 0
fi

query "$sum_view_id" "$evidence/recovery-before-sum.json"
query "$count_view_id" "$evidence/recovery-before-count.json"
sum=$(jq -r '.rows[0].result' "$evidence/recovery-before-sum.json")
count=$(jq -r '.rows[0].result' "$evidence/recovery-before-count.json")
case "$sum/$count" in
  2.5/1)
    offset=1 reading=1.5 expected_sum=4 expected_count=2
    ;;
  4/2|4.0/2)
    offset=2 reading=7 expected_sum=11 expected_count=3
    ;;
  11/3|11.0/3)
    offset=3 reading=8 expected_sum=19 expected_count=4
    ;;
  19/4|19.0/4)
    offset=4 reading=9 expected_sum=28 expected_count=5
    ;;
  28/5|28.0/5)
    offset=4 reading=9 expected_sum=28 expected_count=5
    ;;
  *) die "unexpected materialized values after recovery: sum=$sum count=$count" ;;
esac

jq --arg stream "$stream_id" --argjson offset "$offset" --argjson reading "$reading" \
  '.stream_id=$stream | .start_offset_inclusive=$offset | .rows=[{"reading":$reading,"delta":1}]' \
  "$evidence/payloads/ingest.json" >"$evidence/payloads/ingest-next.json"
request POST "/v1/relations/$relation_id/ingest" "$evidence/payloads/ingest-next.json" "$evidence/ingest-next-first.json"
outcome=$(jq -r '.outcome // empty' "$evidence/ingest-next-first.json")
case "$outcome" in appended|duplicate) ;; *) die "fresh offset-$offset request had unexpected outcome: $outcome" ;; esac
request POST "/v1/relations/$relation_id/ingest" "$evidence/payloads/ingest-next.json" "$evidence/ingest-next-retry.json"
jq -e '.outcome == "duplicate"' "$evidence/ingest-next-retry.json" >/dev/null \
  || die "same-offset $offset retry was not reported as duplicate"
assert_values "$expected_sum" "$expected_count" recovery
echo "PASS public HTTP recovery: sum=$expected_sum count=$expected_count; offset-$offset retry is duplicate with no double count (first=$outcome)"

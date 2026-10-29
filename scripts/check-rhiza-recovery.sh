#!/bin/sh
set -eu

# Run native Rhiza recovery drills against an isolated local source-pinned
# Versity Gateway. Set RHIZA_RECOVERY_TEST_FILTER=single-node or local to run a
# focused one-voter or Config::local probe; this is not a Floci integration.
#
# Scope: this drill starts three local Rhiza processes and therefore exercises
# no Kubernetes at all. It proves the Rhiza-level guarantees below (quorum loss
# fails closed, an emptied working directory fails closed, a cold restart
# recovers from the original disks plus the shared object store) and nothing
# about a deployment shape. The emptyDir/no-PVC Kubernetes proof belongs to
# scripts/run-rhiza-kv-k8s-fixture.sh and scripts/run-rhiza-kv-k8s-gate.sh;
# this evidence kind must never be cited as it. The suite also probes a true
# one-voter native cluster: retained-directory restart must preserve values,
# while empty-directory startup either proves native restore or records the
# engine's explicit continuity refusal. This is not Kubernetes operator work.

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
evidence_root="$repo_root/target/rhiza-recovery-evidence"
run_id=$(date -u +%Y%m%dT%H%M%SZ)-$$
evidence_dir="$evidence_root/$run_id"
mkdir -p "$evidence_dir"

gateway_pid=
s3_port=${RHIZA_RECOVERY_S3_PORT:-29000}
base_port=${RHIZA_RECOVERY_BASE_PORT:-28100}
bucket="velorix-rhiza-recovery-$(date -u +%Y%m%d%H%M%S)-$$"
prefix=${RHIZA_RECOVERY_S3_PREFIX:-recovery-$run_id}
access_key=${RHIZA_RECOVERY_S3_ACCESS_KEY:-$(openssl rand -hex 16)}
secret_key=${RHIZA_RECOVERY_S3_SECRET_KEY:-$(openssl rand -hex 32)}
test_filter=${RHIZA_RECOVERY_TEST_FILTER:-all}
# Official Versity Gateway v1.8.0.
gateway_commit=fd04bc1df2656298577b82667a4195c77f8c7563
fixture_bin="$evidence_dir/bin"
mkdir -p "$fixture_bin" "$evidence_dir/posix-data"

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ -n "$gateway_pid" ]; then
        # Signal only the timeout supervisor we started; it forwards TERM to
        # its own gateway child and enforces the kill-after bound.
        kill "$gateway_pid" 2>/dev/null || true
        wait "$gateway_pid" 2>/dev/null || true
    fi
    if [ "$status" -eq 0 ]; then
        rm -r "$evidence_dir/work" "$evidence_dir/posix-data" "$fixture_bin"
    fi
    if [ "$status" -ne 0 ]; then
        printf '%s\n' "Rhiza recovery failed; evidence retained at $evidence_dir" >&2
    fi
    exit "$status"
}
on_signal() {
    exit 1
}
trap cleanup EXIT
trap on_signal HUP INT TERM

command -v xmllint >/dev/null
command -v nc >/dev/null
if timeout 2s nc -z -w 1 127.0.0.1 "$s3_port"; then
    printf '%s\n' "fixture port is already occupied: $s3_port" >&2
    exit 1
fi
GOBIN="$fixture_bin" timeout --kill-after=10s 600s go install "github.com/versity/versitygw/cmd/versitygw@$gateway_commit" \
    >"$evidence_dir/gateway-build.log" 2>&1
go version -m "$fixture_bin/versitygw" >"$evidence_dir/fixture-versions.log"

ROOT_ACCESS_KEY_ID="$access_key" ROOT_SECRET_ACCESS_KEY="$secret_key" \
timeout --kill-after=10s 900s "$fixture_bin/versitygw" --port "127.0.0.1:$s3_port" \
    --region us-east-1 posix "$evidence_dir/posix-data" >"$evidence_dir/gateway.log" 2>&1 &
gateway_pid=$!
printf '%s\n' "$gateway_pid" >"$evidence_dir/gateway-supervisor.pid"

s3() {
    curl --silent --show-error --max-time 10 --noproxy '*' \
        --aws-sigv4 'aws:amz:us-east-1:s3' --user "$access_key:$secret_key" "$@"
}
endpoint="http://127.0.0.1:$s3_port"

healthy=0
for _ in $(seq 1 30); do
    if ! kill -0 "$gateway_pid" 2>/dev/null; then
        break
    fi
    if s3 --max-time 1 --fail "$endpoint/" >/dev/null 2>&1 \
        && kill -0 "$gateway_pid" 2>/dev/null; then
        healthy=1
        break
    fi
    sleep 1
done
if [ "$healthy" -ne 1 ]; then
    printf '%s\n' "local Versity Gateway did not become healthy" >&2
    exit 1
fi

s3 --fail -X PUT "$endpoint/$bucket" \
    >"$evidence_dir/bucket-create.log" 2>&1 || {
    cat "$evidence_dir/bucket-create.log" >&2
    exit 1
}

# These sequential probes cover conditional requests, not general S3 atomicity.
probe="$endpoint/$bucket/fixture-conditional-probe"
printf 'original\n' >"$evidence_dir/original.txt"
printf 'replacement\n' >"$evidence_dir/replacement.txt"
s3 --fail -X PUT -H 'If-None-Match: *' --data-binary "@$evidence_dir/original.txt" \
    -D "$evidence_dir/create-headers.log" "$probe" >"$evidence_dir/create.log"
etag=$(awk 'tolower($1) == "etag:" { sub(/\r$/, "", $2); print $2 }' "$evidence_dir/create-headers.log")
[ -n "$etag" ]
for condition in 'If-None-Match: *' 'If-Match: "stale-etag"'; do
    status=$(s3 -X PUT -H "$condition" --data-binary "@$evidence_dir/replacement.txt" \
        -o "$evidence_dir/condition-response.log" -w '%{http_code}' "$probe")
    printf '%s -> %s\n' "$condition" "$status" >>"$evidence_dir/conditional-probes.log"
    [ "$status" = 412 ]
    s3 --fail "$probe" >"$evidence_dir/readback.txt"
    cmp "$evidence_dir/original.txt" "$evidence_dir/readback.txt"
done
s3 --fail -X PUT -H "If-Match: $etag" --data-binary "@$evidence_dir/replacement.txt" \
    "$probe" >"$evidence_dir/update.log"
s3 --fail "$probe" >"$evidence_dir/readback.txt"
cmp "$evidence_dir/replacement.txt" "$evidence_dir/readback.txt"
s3 --fail -X DELETE "$probe" >"$evidence_dir/delete.log"
printf '%s\n' 'matching ETag update and readback passed' >>"$evidence_dir/conditional-probes.log"

case "$test_filter" in
    all) test_name= ;;
    single-node) test_name=rhiza_single_node_retained_restart_and_empty_state_recovery_probe ;;
    local) test_name=rhiza_local_mode_s3_before_ack_and_empty_state_restore ;;
    *) printf '%s\n' "unsupported RHIZA_RECOVERY_TEST_FILTER: $test_filter" >&2; exit 2 ;;
esac

if [ -n "$test_name" ]; then
    env \
        RHIZA_RECOVERY_BASE_PORT="$base_port" \
        RHIZA_RECOVERY_S3_ENDPOINT="127.0.0.1:$s3_port" \
        RHIZA_RECOVERY_S3_BUCKET="$bucket" \
        RHIZA_RECOVERY_S3_PREFIX="$prefix" \
        RHIZA_RECOVERY_S3_ACCESS_KEY="$access_key" \
        RHIZA_RECOVERY_S3_SECRET_KEY="$secret_key" \
        RHIZA_RECOVERY_WORKDIR="$evidence_dir/work" \
        timeout --kill-after=10s 540s cargo test -p velorix-meta --features rhiza-backend --test rhiza_recovery "$test_name" -- --exact --ignored --nocapture \
        >"$evidence_dir/rhiza-recovery.log" 2>&1 || {
        cat "$evidence_dir/rhiza-recovery.log"
        exit 1
    }
else
    env \
        RHIZA_RECOVERY_BASE_PORT="$base_port" \
        RHIZA_RECOVERY_S3_ENDPOINT="127.0.0.1:$s3_port" \
        RHIZA_RECOVERY_S3_BUCKET="$bucket" \
        RHIZA_RECOVERY_S3_PREFIX="$prefix" \
        RHIZA_RECOVERY_S3_ACCESS_KEY="$access_key" \
        RHIZA_RECOVERY_S3_SECRET_KEY="$secret_key" \
        RHIZA_RECOVERY_WORKDIR="$evidence_dir/work" \
        timeout --kill-after=10s 540s cargo test -p velorix-meta --features rhiza-backend --test rhiza_recovery -- --ignored --nocapture \
        >"$evidence_dir/rhiza-recovery.log" 2>&1 || {
        cat "$evidence_dir/rhiza-recovery.log"
        exit 1
    }
fi
cat "$evidence_dir/rhiza-recovery.log"

if [ "$test_filter" = single-node ] || [ "$test_filter" = local ]; then
    cargo tree -p velorix-meta --features rhiza-backend -i rhizadb \
        >"$evidence_dir/rhiza-version.log" 2>&1
fi

s3 --fail --get --data-urlencode 'list-type=2' --data-urlencode "prefix=$prefix/" \
    --data-urlencode 'max-keys=1000' "$endpoint/$bucket" \
    >"$evidence_dir/objects.log" 2>"$evidence_dir/objects.stderr.log"
# Reject malformed XML and incomplete listings rather than undercount evidence.
truncated=$(xmllint --nonet --xpath 'string(/*[local-name()="ListBucketResult"]/*[local-name()="IsTruncated"])' "$evidence_dir/objects.log")
[ "$truncated" = false ]
object_count=$(xmllint --nonet --xpath 'count(/*[local-name()="ListBucketResult"]/*[local-name()="Contents"])' "$evidence_dir/objects.log")
if [ "$object_count" -eq 0 ]; then
    printf '%s\n' "before-ack recovery drill produced no shared checkpoint/archive objects" >&2
    exit 1
fi

if [ "$test_filter" = single-node ]; then
    if rg -q '^empty_state_restore=restored$' "$evidence_dir/rhiza-recovery.log"; then
        empty_state_restore=restored
    elif rg -q '^empty_state_restore=unsupported$' "$evidence_dir/rhiza-recovery.log"; then
        empty_state_restore=unsupported
    else
        printf '%s\n' "single-node test did not report an empty-state restore outcome" >&2
        exit 1
    fi
    jq -n \
        --arg rhiza_version "$(sed -n 's/.*rhizadb v\([^ ]*\).*/\1/p' "$evidence_dir/rhiza-version.log" | head -n 1)" \
        --arg versitygw_commit "$gateway_commit" \
        --arg empty_state_restore "$empty_state_restore" \
        --argjson remote_object_count "$object_count" \
        '{evidence_kind:"rhiza_single_native_node_local_recovery_probe",scope:"native Rhiza runtime with local VersityGW S3 fixture; no Kubernetes operator",rhiza_version:$rhiza_version,object_store_fixture:"VersityGW v1.8.0 source-pinned POSIX driver",versitygw_source_commit:$versitygw_commit,retained_directory_restart:"passed; test asserted catalog and checkpoint values",post_restart_write:"passed; test asserted catalog value",empty_local_state_restore:$empty_state_restore,empty_state_is_recovery_pass:($empty_state_restore == "restored"),remote_s3_objects:$remote_object_count}' \
        >"$evidence_dir/rhiza-recovery.json"
elif [ "$test_filter" = local ]; then
    jq -n \
        --arg rhiza_version "$(sed -n 's/.*rhizadb v\([^ ]*\).*/\1/p' "$evidence_dir/rhiza-version.log" | head -n 1)" \
        --arg versitygw_commit "$gateway_commit" \
        --argjson remote_object_count "$object_count" \
        '{evidence_kind:"rhiza_config_local_s3_recovery",scope:"native Rhiza Config::local with local VersityGW S3 fixture; no Kubernetes operator",rhiza_version:$rhiza_version,object_store_fixture:"VersityGW v1.8.0 source-pinned POSIX driver",versitygw_source_commit:$versitygw_commit,before_ack_write:"passed; test read back acknowledged catalog",retained_directory_restart:"passed",empty_directory_s3_restore:"passed",post_restore_write_and_repeat:"passed",remote_s3_objects:$remote_object_count}' \
        >"$evidence_dir/rhiza-recovery.json"
else
{
    printf '%s\n' '{'
    printf '  "evidence_kind": "rhiza_three_native_node_local_recovery",\n'
    printf '  "scope": "native_local_processes_without_kubernetes",\n'
    printf '  "no_pvc_deployment_proven": false,\n'
    printf '  "three_native_nodes": true,\n'
    printf '  "single_native_node_test": "rhiza_single_node_retained_restart_and_empty_state_recovery_probe",\n'
    printf '  "single_node_operator_recovery": false,\n'
    printf '  "cross_node_linearizable_read_and_cas": true,\n'
    printf '  "one_node_loss_retains_quorum": true,\n'
    printf '  "quorum_loss_fails_closed": true,\n'
    printf '  "emptied_working_directory_fails_closed": true,\n'
    printf '  "restart_recovers_from_disk_and_object_store": true,\n'
    printf '  "shutdown_mode": "graceful_cold_restart",\n'
    printf '  "abrupt_crash_tested": false,\n'
    printf '  "object_store_fixture": "isolated_versitygw_posix_not_provider_loss",\n'
    printf '  "versitygw_source_commit": "%s",\n' "$gateway_commit"
    printf '  "sequential_s3_conditional_probes": true,\n'
    printf '  "before_ack_shared_objects": %s\n' "$object_count"
    printf '%s\n' '}'
} >"$evidence_dir/rhiza-recovery.json"
fi
if [ "$test_filter" = single-node ]; then
    printf '%s\n' "Rhiza single-node native probe completed: empty_local_state_restore=$empty_state_restore (evidence: $evidence_dir)"
elif [ "$test_filter" = local ]; then
    printf '%s\n' "Rhiza Config::local S3 recovery completed (evidence: $evidence_dir)"
else
printf '%s\n' "Rhiza recovery drill completed ($test_filter; evidence: $evidence_dir)"
fi

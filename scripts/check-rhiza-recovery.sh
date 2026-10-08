#!/bin/sh
set -eu

# Run the native three-voter recovery drill against an isolated local
# source-pinned Versity Gateway. Failure logs are retained under target/ for
# diagnosis.
#
# Scope: this drill starts three local Rhiza processes and therefore exercises
# no Kubernetes at all. It proves the Rhiza-level guarantees below (quorum loss
# fails closed, an emptied working directory fails closed, a cold restart
# recovers from the original disks plus the shared object store) and nothing
# about a deployment shape. The emptyDir/no-PVC Kubernetes proof belongs to
# scripts/run-rhiza-kv-k8s-fixture.sh and scripts/run-rhiza-kv-k8s-gate.sh;
# this evidence kind must never be cited as it.

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
evidence_root="$repo_root/target/rhiza-recovery-evidence"
run_id=$(date -u +%Y%m%dT%H%M%SZ)-$$
evidence_dir="$evidence_root/$run_id"
mkdir -p "$evidence_dir"

gateway_pid=
s3_port=${RHIZA_RECOVERY_S3_PORT:-29000}
base_port=${RHIZA_RECOVERY_BASE_PORT:-28100}
bucket=${RHIZA_RECOVERY_S3_BUCKET:-velorix-rhiza-recovery}
prefix=${RHIZA_RECOVERY_S3_PREFIX:-recovery-$run_id}
access_key=${RHIZA_RECOVERY_S3_ACCESS_KEY:-velorix-test-access}
secret_key=${RHIZA_RECOVERY_S3_SECRET_KEY:-velorix-test-secret}
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

RHIZA_RECOVERY_BASE_PORT="$base_port" \
RHIZA_RECOVERY_S3_ENDPOINT="127.0.0.1:$s3_port" \
RHIZA_RECOVERY_S3_BUCKET="$bucket" \
RHIZA_RECOVERY_S3_PREFIX="$prefix" \
RHIZA_RECOVERY_S3_ACCESS_KEY="$access_key" \
RHIZA_RECOVERY_S3_SECRET_KEY="$secret_key" \
RHIZA_RECOVERY_WORKDIR="$evidence_dir/work" \
timeout --kill-after=10s 540s cargo test -p velorix-meta --features rhiza-backend --test rhiza_recovery -- \
    --ignored --nocapture >"$evidence_dir/rhiza-recovery.log" 2>&1 || {
    cat "$evidence_dir/rhiza-recovery.log"
    exit 1
}
cat "$evidence_dir/rhiza-recovery.log"

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

{
    printf '%s\n' '{'
    printf '  "evidence_kind": "rhiza_three_native_node_local_recovery",\n'
    printf '  "scope": "native_local_processes_without_kubernetes",\n'
    printf '  "no_pvc_deployment_proven": false,\n'
    printf '  "three_native_nodes": true,\n'
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
printf '%s\n' "Rhiza three-node recovery drill passed (evidence: $evidence_dir)"

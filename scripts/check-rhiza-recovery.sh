#!/bin/sh
set -eu

# Run the native three-voter/no-PVC recovery drill against an isolated local
# source-pinned MinIO process. Failure logs are retained under target/ for diagnosis.

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
evidence_root="$repo_root/target/rhiza-recovery-evidence"
run_id=$(date -u +%Y%m%dT%H%M%SZ)-$$
evidence_dir="$evidence_root/$run_id"
mkdir -p "$evidence_dir"

minio_pid=
s3_port=${RHIZA_RECOVERY_S3_PORT:-29000}
base_port=${RHIZA_RECOVERY_BASE_PORT:-28100}
bucket=${RHIZA_RECOVERY_S3_BUCKET:-velorix-rhiza-recovery}
prefix=${RHIZA_RECOVERY_S3_PREFIX:-recovery-$run_id}
access_key=${RHIZA_RECOVERY_S3_ACCESS_KEY:-velorix-test-access}
secret_key=${RHIZA_RECOVERY_S3_SECRET_KEY:-velorix-test-secret}
# Official commits matching the former images; public image pulls now fail.
minio_commit=07c3a429bfed433e49018cb0f78a52145d4bedeb
mc_commit=7394ce0dd2a80935aded936b09fa12cbb3cb8096
fixture_bin="$evidence_dir/bin"
mkdir -p "$fixture_bin" "$evidence_dir/minio-data" "$evidence_dir/mc-config"

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ -n "$minio_pid" ]; then
        # Signal only the timeout supervisor we started; it forwards TERM to
        # its own MinIO child and enforces the kill-after bound.
        kill "$minio_pid" 2>/dev/null || true
        wait "$minio_pid" 2>/dev/null || true
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

GOBIN="$fixture_bin" timeout --kill-after=10s 600s go install "github.com/minio/minio@$minio_commit" \
    >"$evidence_dir/minio-build.log" 2>&1
GOBIN="$fixture_bin" timeout --kill-after=10s 600s go install "github.com/minio/mc@$mc_commit" \
    >"$evidence_dir/mc-build.log" 2>&1
go version -m "$fixture_bin/minio" "$fixture_bin/mc" >"$evidence_dir/fixture-versions.log"

MINIO_ROOT_USER="$access_key" MINIO_ROOT_PASSWORD="$secret_key" \
timeout --kill-after=10s 660s "$fixture_bin/minio" server "$evidence_dir/minio-data" \
    --address "127.0.0.1:$s3_port" --console-address "127.0.0.1:$((s3_port + 1))" \
    >"$evidence_dir/minio.log" 2>&1 &
minio_pid=$!
printf '%s\n' "$minio_pid" >"$evidence_dir/minio-supervisor.pid"

healthy=0
for _ in $(seq 1 30); do
    if ! kill -0 "$minio_pid" 2>/dev/null; then
        break
    fi
    if curl --max-time 1 -fsS "http://127.0.0.1:$s3_port/minio/health/live" >/dev/null 2>&1 \
        && kill -0 "$minio_pid" 2>/dev/null; then
        healthy=1
        break
    fi
    sleep 1
done
if [ "$healthy" -ne 1 ]; then
    printf '%s\n' "local MinIO did not become healthy" >&2
    exit 1
fi

export MC_HOST_local="http://$access_key:$secret_key@127.0.0.1:$s3_port"
export MC_CONFIG_DIR="$evidence_dir/mc-config"
timeout --kill-after=5s 30s "$fixture_bin/mc" mb --ignore-existing "local/$bucket" \
    >"$evidence_dir/bucket-create.log" 2>&1

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

timeout --kill-after=5s 30s "$fixture_bin/mc" ls --recursive "local/$bucket/$prefix" \
    >"$evidence_dir/objects.log" 2>"$evidence_dir/objects.stderr.log"
object_count=$(awk 'NF { count++ } END { print count + 0 }' "$evidence_dir/objects.log")
if [ "$object_count" -eq 0 ]; then
    printf '%s\n' "before-ack recovery drill produced no shared checkpoint/archive objects" >&2
    exit 1
fi

{
    printf '%s\n' '{'
    printf '  "evidence_kind": "rhiza_three_node_no_pvc_recovery",\n'
    printf '  "three_native_nodes": true,\n'
    printf '  "cross_node_linearizable_read_and_cas": true,\n'
    printf '  "one_node_loss_retains_quorum": true,\n'
    printf '  "quorum_loss_fails_closed": true,\n'
    printf '  "empty_working_directory_recovery": true,\n'
    printf '  "shutdown_mode": "graceful_cold_restart",\n'
    printf '  "abrupt_crash_tested": false,\n'
    printf '  "object_store_fixture": "isolated_minio_not_provider_loss",\n'
    printf '  "minio_source_commit": "%s",\n' "$minio_commit"
    printf '  "mc_source_commit": "%s",\n' "$mc_commit"
    printf '  "before_ack_shared_objects": %s\n' "$object_count"
    printf '%s\n' '}'
} >"$evidence_dir/rhiza-recovery.json"
printf '%s\n' "Rhiza three-node recovery drill passed (evidence: $evidence_dir)"

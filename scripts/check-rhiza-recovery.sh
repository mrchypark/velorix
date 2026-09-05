#!/bin/sh
set -eu

# Run the native three-voter/no-PVC recovery drill against an isolated local
# MinIO container. Failure logs are retained under target/ for diagnosis.

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
evidence_root="$repo_root/target/rhiza-recovery-evidence"
run_id=$(date -u +%Y%m%dT%H%M%SZ)-$$
evidence_dir="$evidence_root/$run_id"
mkdir -p "$evidence_dir"

container="velorix-rhiza-recovery-$run_id"
s3_port=${RHIZA_RECOVERY_S3_PORT:-29000}
base_port=${RHIZA_RECOVERY_BASE_PORT:-28100}
bucket=${RHIZA_RECOVERY_S3_BUCKET:-velorix-rhiza-recovery}
prefix=${RHIZA_RECOVERY_S3_PREFIX:-recovery-$run_id}
access_key=${RHIZA_RECOVERY_S3_ACCESS_KEY:-velorix-test-access}
secret_key=${RHIZA_RECOVERY_S3_SECRET_KEY:-velorix-test-secret}
minio_image=minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e
mc_image=minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        docker logs "$container" >"$evidence_dir/minio.log" 2>&1 || true
        printf '%s\n' "Rhiza recovery failed; evidence retained at $evidence_dir" >&2
    fi
    docker rm -f "$container" >/dev/null 2>&1 || true
    exit "$status"
}
on_signal() {
    exit 1
}
trap cleanup EXIT
trap on_signal HUP INT TERM

docker run -d --name "$container" \
    -p "127.0.0.1:$s3_port:9000" -p "127.0.0.1:$((s3_port + 1)):9001" \
    -e MINIO_ROOT_USER="$access_key" \
    -e MINIO_ROOT_PASSWORD="$secret_key" \
    "$minio_image" server /data --console-address :9001 \
    >"$evidence_dir/minio-container-id"

healthy=0
for _ in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:$s3_port/minio/health/live" >/dev/null 2>&1; then
        healthy=1
        break
    fi
    sleep 1
done
if [ "$healthy" -ne 1 ]; then
    printf '%s\n' "local MinIO did not become healthy" >&2
    exit 1
fi

docker run --rm --network host \
    -e "MC_HOST_local=http://$access_key:$secret_key@127.0.0.1:$s3_port" \
    "$mc_image" mb --ignore-existing "local/$bucket" \
    >"$evidence_dir/bucket-create.log"

RHIZA_RECOVERY_BASE_PORT="$base_port" \
RHIZA_RECOVERY_S3_ENDPOINT="127.0.0.1:$s3_port" \
RHIZA_RECOVERY_S3_BUCKET="$bucket" \
RHIZA_RECOVERY_S3_PREFIX="$prefix" \
RHIZA_RECOVERY_S3_ACCESS_KEY="$access_key" \
RHIZA_RECOVERY_S3_SECRET_KEY="$secret_key" \
RHIZA_RECOVERY_WORKDIR="$evidence_dir/work" \
cargo test -p velorix-meta --features rhiza-backend --test rhiza_recovery -- \
    --ignored --nocapture >"$evidence_dir/rhiza-recovery.log" 2>&1 || {
    cat "$evidence_dir/rhiza-recovery.log"
    exit 1
}
cat "$evidence_dir/rhiza-recovery.log"

object_count=$(docker run --rm --network host \
    -e "MC_HOST_local=http://$access_key:$secret_key@127.0.0.1:$s3_port" \
    "$mc_image" ls --recursive "local/$bucket/$prefix" | awk 'NF { count++ } END { print count + 0 }')
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
    printf '  "before_ack_shared_objects": %s\n' "$object_count"
    printf '%s\n' '}'
} >"$evidence_dir/rhiza-recovery.json"
printf '%s\n' "Rhiza three-node recovery drill passed (evidence: $evidence_dir)"

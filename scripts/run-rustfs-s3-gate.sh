#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
container="${VELORIX_RUSTFS_CONTAINER:-velorix-rustfs-s3-${run_id}}"
network="${VELORIX_RUSTFS_NETWORK:-velorix-rustfs-s3-${run_id}}"
volume="${VELORIX_RUSTFS_VOLUME:-velorix-rustfs-s3-${run_id}}"
image="${VELORIX_RUSTFS_IMAGE:-rustfs/rustfs:latest}"
aws_cli_image="${VELORIX_AWS_CLI_IMAGE:-amazon/aws-cli:latest}"
port="${VELORIX_RUSTFS_PORT:-9000}"
region="${AWS_REGION:-us-east-1}"
bucket="${VELORIX_S3_BUCKET:-velorix-rustfs}"
prefix="${VELORIX_S3_PREFIX:-rustfs-s3-gate/${run_id}}"
run_benchmark="${VELORIX_RUSTFS_RUN_BENCHMARK:-1}"
cleanup="${VELORIX_RUSTFS_CLEANUP:-1}"
evidence_path="${VELORIX_RUSTFS_EVIDENCE_PATH:-target/velorix-s3/rustfs-s3-gate-evidence.json}"
benchmark_path="${VELORIX_RUSTFS_BENCHMARK_PATH:-target/velorix-bench/rustfs-s3-nightly.json}"
created_container=0
created_network=0
created_volume=0

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

preflight_docker_networks() {
  local probe_network="${network}-preflight"
  local probe_container="${probe_network}-container"
  local output

  if docker network inspect "$probe_network" >/dev/null 2>&1; then
    echo "docker preflight network already exists: ${probe_network}" >&2
    echo "remove it or set VELORIX_RUSTFS_NETWORK to a fresh name" >&2
    exit 1
  fi
  if docker container inspect "$probe_container" >/dev/null 2>&1; then
    echo "docker preflight container already exists: ${probe_container}" >&2
    echo "remove it or set VELORIX_RUSTFS_NETWORK to a fresh name" >&2
    exit 1
  fi

  if ! output="$(docker network create "$probe_network" 2>&1)"; then
    echo "docker cannot create bridge networks required by the RustFS S3 gate" >&2
    echo "$output" >&2
    echo "repair or restart Docker, then rerun scripts/run-rustfs-s3-gate.sh" >&2
    exit 1
  fi

  if ! output="$(
    docker run --rm \
      --name "$probe_container" \
      --network "$probe_network" \
      "$aws_cli_image" \
      --version 2>&1
  )"; then
    docker rm -f "$probe_container" >/dev/null 2>&1 || true
    docker network rm "$probe_network" >/dev/null 2>&1 || true
    echo "docker cannot run containers on bridge networks required by the RustFS S3 gate" >&2
    echo "$output" >&2
    echo "repair or restart Docker, then rerun scripts/run-rustfs-s3-gate.sh" >&2
    exit 1
  fi

  if ! output="$(docker network rm "$probe_network" 2>&1)"; then
    echo "docker created the preflight network but could not remove it: ${probe_network}" >&2
    echo "$output" >&2
    echo "remove the probe network manually before rerunning the RustFS S3 gate" >&2
    exit 1
  fi
}

cleanup_rustfs() {
  if [ "$cleanup" = "1" ]; then
    if [ "$created_container" = "1" ]; then
      docker rm -f "$container" >/dev/null 2>&1 || true
    fi
    if [ "$created_network" = "1" ]; then
      docker network rm "$network" >/dev/null 2>&1 || true
    fi
    if [ "$created_volume" = "1" ]; then
      docker volume rm "$volume" >/dev/null 2>&1 || true
    fi
  fi
}

wait_for_rustfs() {
  for _ in $(seq 1 120); do
    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID=rustfsadmin \
      -e AWS_SECRET_ACCESS_KEY=rustfsadmin \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api list-buckets >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  docker logs "$container" >&2 || true
  echo "rustfs did not become ready on http://127.0.0.1:${port}" >&2
  exit 1
}

ensure_bucket() {
  for _ in $(seq 1 120); do
    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID=rustfsadmin \
      -e AWS_SECRET_ACCESS_KEY=rustfsadmin \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      return 0
    fi

    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID=rustfsadmin \
      -e AWS_SECRET_ACCESS_KEY=rustfsadmin \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api create-bucket --bucket "$bucket" --region "$region" >/dev/null 2>&1; then
      return 0
    fi

    sleep 1
  done

  docker logs "$container" >&2 || true
  echo "rustfs S3 API did not become ready for bucket ${bucket}" >&2
  exit 1
}

require cargo
require docker
require python3
preflight_docker_networks

cd "$repo_root"
trap cleanup_rustfs EXIT
mkdir -p "$(dirname "$evidence_path")" "$(dirname "$benchmark_path")"
rm -f "$evidence_path" "$benchmark_path"

if docker container inspect "$container" >/dev/null 2>&1; then
  echo "docker container already exists: ${container}" >&2
  exit 1
fi

if docker network inspect "$network" >/dev/null 2>&1; then
  created_network=0
else
  docker network create "$network" >/dev/null
  created_network=1
fi

if docker volume inspect "$volume" >/dev/null 2>&1; then
  created_volume=0
else
  docker volume create "$volume" >/dev/null
  created_volume=1
fi

docker run -d \
  --name "$container" \
  --network "$network" \
  -p "${port}:9000" \
  -e RUSTFS_ADDRESS=:9000 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin \
  -e RUSTFS_SECRET_KEY=rustfsadmin \
  -v "${volume}:/data" \
  "$image" \
  /data >/dev/null
created_container=1

wait_for_rustfs
ensure_bucket

export VELORIX_S3_COMPAT=1
export AWS_ENDPOINT_URL="http://127.0.0.1:${port}"
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_REGION="$region"
export VELORIX_S3_BUCKET="$bucket"
export VELORIX_S3_PREFIX="$prefix"
export VELORIX_BENCHMARK_EVIDENCE_SCOPE=live_or_native

cargo test -p velorix-storage --test s3_compat --features s3-compat-tests -- --nocapture --test-threads=1
cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests -- --nocapture --test-threads=1
cargo test -p velorix-runtime --test s3_compat_query --features s3-compat-tests -- --nocapture --test-threads=1

benchmark_ran=false
if [ "$run_benchmark" = "1" ]; then
  cargo bench -p velorix-runtime --bench s3_incremental --features s3-compat-tests > "$benchmark_path"
  cargo run -p velorix-cli -- benchmark-validate --result "$benchmark_path"
  benchmark_ran=true
fi

python3 - "$evidence_path" "$benchmark_path" "$benchmark_ran" "$container" "$image" "$volume" "$port" "$bucket" "$prefix" "$region" <<'PY'
import json
import subprocess
import sys
from datetime import datetime, timezone


def run(command):
    return subprocess.check_output(command, text=True).strip().splitlines()


(
    evidence_path,
    benchmark_path,
    benchmark_ran,
    container,
    image,
    volume,
    port,
    bucket,
    prefix,
    region,
) = sys.argv[1:]

evidence = {
    "schema_version": 1,
    "evidence_kind": "rustfs_s3_compatible_gate",
    "readiness_evidence_kind": [
        "s3_compatible",
        "s3_compatible_integration_harness",
    ],
    "endpoint": f"http://127.0.0.1:{port}",
    "bucket": bucket,
    "prefix": prefix,
    "region": region,
    "rustfs_container": container,
    "rustfs_image": image,
    "rustfs_volume": volume,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "docker_version": run(["docker", "version", "--format", "{{.Server.Version}}"])[0],
    "live_tests": [
        "cargo test -p velorix-storage --test s3_compat --features s3-compat-tests",
        "cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests",
        "cargo test -p velorix-runtime --test s3_compat_query --features s3-compat-tests",
    ],
    "benchmark": {
        "ran": benchmark_ran == "true",
        "result_path": benchmark_path if benchmark_ran == "true" else None,
        "validation": "velorix-cli benchmark-validate --result" if benchmark_ran == "true" else None,
    },
    "backend_evidence_scope": "live_or_native",
    "scope": "RustFS S3-compatible live evidence through the S3 API; release benchmark closure still requires the benchmark-gate artifact for the selected gate level",
}

with open(evidence_path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY

echo "wrote rustfs S3-compatible gate evidence to ${evidence_path}"

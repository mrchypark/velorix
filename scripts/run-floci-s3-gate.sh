#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
container="${VELORIX_FLOCI_CONTAINER:-velorix-floci-s3-${run_id}}"
network="${VELORIX_FLOCI_NETWORK:-velorix-floci-s3-${run_id}}"
image="${VELORIX_FLOCI_IMAGE:-floci/floci:latest}"
aws_cli_image="${VELORIX_AWS_CLI_IMAGE:-amazon/aws-cli:latest}"
port="${VELORIX_FLOCI_PORT:-4566}"
region="${AWS_REGION:-us-east-1}"
bucket="${VELORIX_S3_BUCKET:-velorix-floci}"
prefix="${VELORIX_S3_PREFIX:-floci-s3-gate/${run_id}}"
run_benchmark="${VELORIX_FLOCI_RUN_BENCHMARK:-1}"
cleanup="${VELORIX_FLOCI_CLEANUP:-1}"
evidence_path="${VELORIX_FLOCI_EVIDENCE_PATH:-target/velorix-s3/floci-s3-gate-evidence.json}"
benchmark_path="${VELORIX_FLOCI_BENCHMARK_PATH:-target/velorix-bench/floci-s3-nightly.json}"
created_container=0
created_network=0

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup_floci() {
  if [ "$cleanup" = "1" ]; then
    if [ "$created_container" = "1" ]; then
      docker rm -f "$container" >/dev/null 2>&1 || true
    fi
    if [ "$created_network" = "1" ]; then
      docker network rm "$network" >/dev/null 2>&1 || true
    fi
  fi
}

wait_for_floci() {
  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:${port}/_localstack/health" >/dev/null 2>&1; then
      return 0
    fi
    if curl -fsS "http://127.0.0.1:${port}/" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  docker logs "$container" >&2 || true
  echo "floci did not become ready on http://127.0.0.1:${port}" >&2
  exit 1
}

ensure_bucket() {
  for _ in $(seq 1 120); do
    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID=test \
      -e AWS_SECRET_ACCESS_KEY=test \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:4566" \
      s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      return 0
    fi

    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID=test \
      -e AWS_SECRET_ACCESS_KEY=test \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:4566" \
      s3api create-bucket --bucket "$bucket" --region "$region" >/dev/null 2>&1; then
      return 0
    fi

    sleep 1
  done

  docker logs "$container" >&2 || true
  echo "floci S3 API did not become ready for bucket ${bucket}" >&2
  exit 1
}

require cargo
require curl
require docker
require python3

cd "$repo_root"
trap cleanup_floci EXIT
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

docker run -d \
  --name "$container" \
  --network "$network" \
  -p "${port}:4566" \
  -e FLOCI_DEFAULT_REGION="$region" \
  -e FLOCI_STORAGE_MODE=memory \
  "$image" >/dev/null
created_container=1

wait_for_floci
ensure_bucket

export VELORIX_S3_COMPAT=1
export AWS_ENDPOINT_URL="http://127.0.0.1:${port}"
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION="$region"
export VELORIX_S3_BUCKET="$bucket"
export VELORIX_S3_PREFIX="$prefix"

cargo test -p velorix-storage --test s3_compat --features s3-compat-tests -- --nocapture --test-threads=1
cargo test -p velorix-runtime --test s3_compat_query --features s3-compat-tests -- --nocapture --test-threads=1

benchmark_ran=false
if [ "$run_benchmark" = "1" ]; then
  cargo bench -p velorix-runtime --bench s3_incremental --features s3-compat-tests > "$benchmark_path"
  cargo run -p velorix-cli -- benchmark-validate --result "$benchmark_path"
  benchmark_ran=true
fi

python3 - "$evidence_path" "$benchmark_path" "$benchmark_ran" "$container" "$image" "$port" "$bucket" "$prefix" "$region" <<'PY'
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
    port,
    bucket,
    prefix,
    region,
) = sys.argv[1:]

evidence = {
    "schema_version": 1,
    "evidence_kind": "floci_s3_compatible_gate",
    "readiness_evidence_kind": [
        "s3_compatible",
        "s3_compatible_integration_harness",
    ],
    "endpoint": f"http://127.0.0.1:{port}",
    "bucket": bucket,
    "prefix": prefix,
    "region": region,
    "floci_container": container,
    "floci_image": image,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "docker_version": run(["docker", "version", "--format", "{{.Server.Version}}"])[0],
    "live_tests": [
        "cargo test -p velorix-storage --test s3_compat --features s3-compat-tests",
        "cargo test -p velorix-runtime --test s3_compat_query --features s3-compat-tests",
    ],
    "benchmark": {
        "ran": benchmark_ran == "true",
        "result_path": benchmark_path if benchmark_ran == "true" else None,
        "validation": "velorix-cli benchmark-validate --result" if benchmark_ran == "true" else None,
    },
    "scope": "local floci S3-compatible emulator evidence; not a substitute for release S3-compatible baseline evidence",
}

with open(evidence_path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY

echo "wrote floci S3-compatible gate evidence to ${evidence_path}"

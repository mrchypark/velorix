#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
container="${VELORIX_RUSTFS_CONTAINER:-velorix-rustfs-s3-${run_id}}"
network="${VELORIX_RUSTFS_NETWORK:-velorix-rustfs-s3-${run_id}}"
volume="${VELORIX_RUSTFS_VOLUME:-velorix-rustfs-s3-${run_id}}"
image="${VELORIX_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.4}"
allow_mutable_rustfs_image="${VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE:-0}"
aws_cli_image="${VELORIX_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
allow_mutable_aws_cli_image="${VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE:-0}"
rustfs_access_key="${VELORIX_RUSTFS_ACCESS_KEY:-velorix-rustfs-gate}"
rustfs_secret_key="${VELORIX_RUSTFS_SECRET_KEY:-velorix-rustfs-gate-${run_id}}"
port="${VELORIX_RUSTFS_PORT:-9000}"
region="${AWS_REGION:-us-east-1}"
bucket="${VELORIX_S3_BUCKET:-velorix-rustfs}"
prefix="${VELORIX_S3_PREFIX:-rustfs-s3-gate/${run_id}}"
run_benchmark="${VELORIX_RUSTFS_RUN_BENCHMARK:-1}"
cleanup="${VELORIX_RUSTFS_CLEANUP:-1}"
min_free_kib="${VELORIX_RUSTFS_MIN_FREE_KIB:-4194304}"
cargo_target_dir="${VELORIX_RUSTFS_CARGO_TARGET_DIR:-${repo_root}/target/rustfs-s3-gate}"
evidence_path="${VELORIX_RUSTFS_EVIDENCE_PATH:-target/velorix-s3/rustfs-s3-gate-evidence.json}"
benchmark_path="${VELORIX_RUSTFS_BENCHMARK_PATH:-target/velorix-bench/rustfs-s3-nightly.json}"
production_gc_seed_path="${VELORIX_RUSTFS_PRODUCTION_GC_SEED_PATH:-target/release-evidence/rustfs-production-gc-seed.json}"
production_gc_run_path="${VELORIX_RUSTFS_PRODUCTION_GC_RUN_PATH:-target/release-evidence/rustfs-production-gc-run.json}"
production_gc_path="${VELORIX_RUSTFS_PRODUCTION_GC_PATH:-target/release-evidence/rustfs-production-gc.json}"
production_gc_validation_path="${VELORIX_RUSTFS_PRODUCTION_GC_VALIDATION_PATH:-target/release-evidence/rustfs-production-gc-validation.json}"
production_gc_prefix="${VELORIX_RUSTFS_PRODUCTION_GC_PREFIX:-${prefix}/production-gc}"
production_gc_run_id="${VELORIX_RUSTFS_PRODUCTION_GC_RUN_ID:-rustfs-production-gc-${run_id}}"
production_gc_deployment_id="${VELORIX_RUSTFS_PRODUCTION_GC_DEPLOYMENT_ID:-rustfs-s3-gate}"
production_gc_authority_store_id="${VELORIX_RUSTFS_PRODUCTION_GC_AUTHORITY_STORE_ID:-s3://rustfs/${bucket}/${production_gc_prefix}}"
run_production_gc_evidence="${VELORIX_RUSTFS_RUN_PRODUCTION_GC_EVIDENCE:-1}"
production_gc_retain_latest_manifests="${VELORIX_RUSTFS_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS:-1}"
created_container=0
created_network=0
created_volume=0

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

preflight_docker_daemon() {
  local output
  local context_name
  context_name="$(docker context show 2>/dev/null || true)"
  if output="$(docker info 2>&1 >/dev/null)"; then
    return 0
  fi

  echo "docker daemon is not reachable; cannot run the RustFS S3 gate" >&2
  if [ -n "$context_name" ]; then
    echo "docker_context=${context_name}" >&2
  fi
  echo "$output" >&2
  if command -v colima >/dev/null 2>&1; then
    echo "colima status:" >&2
    colima status >&2 || true
    echo "If this context is Colima-backed, repair or start Colima before rerunning scripts/run-rustfs-s3-gate.sh." >&2
  else
    echo "Start or repair Docker before rerunning scripts/run-rustfs-s3-gate.sh." >&2
  fi
  exit 1
}

is_mutable_image_reference() {
  python3 - "$1" <<'PY'
import sys

image = sys.argv[1]
if "@sha256:" in image:
    raise SystemExit(1)
name = image.rsplit("/", 1)[-1]
if ":" not in name:
    raise SystemExit(0)
tag = name.rsplit(":", 1)[-1]
if tag in {"latest", "latest-glibc", "beta", "beta-glibc"}:
    raise SystemExit(0)
raise SystemExit(1)
PY
}

case "$allow_mutable_rustfs_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$allow_mutable_aws_cli_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$production_gc_retain_latest_manifests" in
  '' | *[!0-9]*)
    echo "VELORIX_RUSTFS_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS must be 1 for the fixed two-checkpoint release smoke fixture" >&2
    exit 64
    ;;
  1) ;;
  *)
    echo "VELORIX_RUSTFS_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS must be 1 for the fixed two-checkpoint release smoke fixture" >&2
    exit 64
    ;;
esac

case "$min_free_kib" in
  '' | *[!0-9]*)
    echo "VELORIX_RUSTFS_MIN_FREE_KIB must be a positive integer" >&2
    exit 64
    ;;
  0)
    echo "VELORIX_RUSTFS_MIN_FREE_KIB must be greater than zero" >&2
    exit 64
    ;;
esac

if [ -z "$rustfs_access_key" ] || [ -z "$rustfs_secret_key" ]; then
  echo "VELORIX_RUSTFS_ACCESS_KEY and VELORIX_RUSTFS_SECRET_KEY must be non-empty" >&2
  exit 64
fi

preflight_disk_space() {
  local available_kib
  available_kib="$(df -k "$repo_root" | awk 'NR == 2 { print $4 }')"
  if [ -z "$available_kib" ]; then
    echo "could not determine available disk space for ${repo_root}" >&2
    exit 1
  fi
  if [ "$available_kib" -lt "$min_free_kib" ]; then
    echo "insufficient disk space for RustFS S3 gate: available_kib=${available_kib} required_kib=${min_free_kib}" >&2
    echo "Free disk space or set VELORIX_RUSTFS_MIN_FREE_KIB to an explicitly reviewed lower value before rerunning." >&2
    exit 75
  fi
}

preflight_cargo_target_dir() {
  mkdir -p "$cargo_target_dir"
  if [ ! -d "$cargo_target_dir" ] || [ ! -w "$cargo_target_dir" ]; then
    echo "CARGO_TARGET_DIR for RustFS S3 gate is not writable: ${cargo_target_dir}" >&2
    exit 1
  fi
}

if [ "$rustfs_access_key" = "rustfsadmin" ] || [ "$rustfs_secret_key" = "rustfsadmin" ]; then
  echo "RustFS S3 gate refuses the rustfsadmin default credentials; set VELORIX_RUSTFS_ACCESS_KEY and VELORIX_RUSTFS_SECRET_KEY to non-default values" >&2
  exit 64
fi

if [ "$allow_mutable_rustfs_image" != "1" ] && is_mutable_image_reference "$image"; then
  echo "VELORIX_RUSTFS_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE=1 to use ${image}" >&2
  exit 64
fi

if [ "$allow_mutable_aws_cli_image" != "1" ] && is_mutable_image_reference "$aws_cli_image"; then
  echo "VELORIX_AWS_CLI_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE=1 to use ${aws_cli_image}" >&2
  exit 64
fi

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
      -e AWS_ACCESS_KEY_ID="$rustfs_access_key" \
      -e AWS_SECRET_ACCESS_KEY="$rustfs_secret_key" \
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
      -e AWS_ACCESS_KEY_ID="$rustfs_access_key" \
      -e AWS_SECRET_ACCESS_KEY="$rustfs_secret_key" \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      return 0
    fi

    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID="$rustfs_access_key" \
      -e AWS_SECRET_ACCESS_KEY="$rustfs_secret_key" \
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
preflight_disk_space
preflight_cargo_target_dir
preflight_docker_daemon
preflight_docker_networks

cd "$repo_root"
trap cleanup_rustfs EXIT
mkdir -p "$(dirname "$evidence_path")" "$(dirname "$benchmark_path")" "$(dirname "$production_gc_seed_path")" "$(dirname "$production_gc_run_path")" "$(dirname "$production_gc_path")" "$(dirname "$production_gc_validation_path")"
rm -f "$evidence_path" "$benchmark_path" "$production_gc_seed_path" "$production_gc_run_path" "$production_gc_path" "$production_gc_validation_path"

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
  -e RUSTFS_ACCESS_KEY="$rustfs_access_key" \
  -e RUSTFS_SECRET_KEY="$rustfs_secret_key" \
  -v "${volume}:/data" \
  "$image" \
  /data >/dev/null
created_container=1

wait_for_rustfs
ensure_bucket

export VELORIX_S3_COMPAT=1
export AWS_ENDPOINT_URL="http://127.0.0.1:${port}"
export AWS_ACCESS_KEY_ID="$rustfs_access_key"
export AWS_SECRET_ACCESS_KEY="$rustfs_secret_key"
export AWS_REGION="$region"
export VELORIX_S3_BUCKET="$bucket"
export VELORIX_S3_PREFIX="$prefix"
export VELORIX_BENCHMARK_EVIDENCE_SCOPE=live_or_native
export CARGO_TARGET_DIR="$cargo_target_dir"
if [ "$run_production_gc_evidence" = "1" ]; then
  export VELORIX_S3_GC_PREFIX="${production_gc_prefix}/harness-precheck"
  export VELORIX_S3_GC_RUN_ID="${production_gc_run_id}-harness-precheck"
fi

cargo test -p velorix-storage --test s3_compat --features s3-compat-tests -- --nocapture --test-threads=1
cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests -- --nocapture --test-threads=1
cargo test -p velorix-runtime --test s3_compat_query --features s3-compat-tests -- --nocapture --test-threads=1

benchmark_ran=false
if [ "$run_benchmark" = "1" ]; then
  cargo bench -p velorix-runtime --bench s3_incremental --features s3-compat-tests > "$benchmark_path"
  cargo run -p velorix-cli -- benchmark-validate --result "$benchmark_path"
  benchmark_ran=true
fi

production_gc_generated=false
if [ "$run_production_gc_evidence" = "1" ]; then
  VELORIX_S3_PREFIX="$production_gc_prefix" cargo run -p velorix-cli -- \
    gc-seed-s3-compatible-fixture \
    --authority-store-id "$production_gc_authority_store_id" \
    --seed-id "$production_gc_run_id" \
    --json > "$production_gc_seed_path"
  VELORIX_S3_PREFIX="$production_gc_prefix" cargo run -p velorix-cli -- \
    gc-execute-s3-compatible \
    --authority-store-id "$production_gc_authority_store_id" \
    --retain-latest-manifests "$production_gc_retain_latest_manifests" \
    --run-id "$production_gc_run_id" \
    --json > "$production_gc_run_path"
  VELORIX_S3_PREFIX="$production_gc_prefix" cargo run -p velorix-cli -- \
    gc-production-evidence \
    --deployment-id "$production_gc_deployment_id" \
    --authority-store-id "$production_gc_authority_store_id" \
    --gc-run-id "$production_gc_run_id" \
    --json > "$production_gc_path"
  production_gc_generated=true
fi

python3 - "$evidence_path" "$benchmark_path" "$benchmark_ran" "$container" "$image" "$volume" "$port" "$bucket" "$prefix" "$region" "$cargo_target_dir" "$production_gc_seed_path" "$production_gc_run_path" "$production_gc_path" "$production_gc_validation_path" "$production_gc_generated" "$production_gc_prefix" "$production_gc_run_id" "$production_gc_deployment_id" "$production_gc_authority_store_id" "$production_gc_retain_latest_manifests" <<'PY'
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
    cargo_target_dir,
    production_gc_seed_path,
    production_gc_run_path,
    production_gc_path,
    production_gc_validation_path,
    production_gc_generated,
    production_gc_prefix,
    production_gc_run_id,
    production_gc_deployment_id,
    production_gc_authority_store_id,
    production_gc_retain_latest_manifests,
) = sys.argv[1:]

evidence = {
    "schema_version": 1,
    "evidence_kind": "rustfs_s3_compatible_gate",
    "readiness_evidence_kind": [
        "s3_compatible",
        "s3_compatible_integration_harness",
    ],
    "gate_detail_kind": [
        "s3_compatible_ingest_admission_crash_restart",
        "s3_compatible_gc_execution_retention",
    ],
    "endpoint": f"http://127.0.0.1:{port}",
    "bucket": bucket,
    "prefix": prefix,
    "region": region,
    "rustfs_container": container,
    "rustfs_image": image,
    "rustfs_volume": volume,
    "cargo_target_dir": cargo_target_dir,
    "credentials_redacted": True,
    "credential_policy": "run-local non-default RustFS root credentials; override with VELORIX_RUSTFS_ACCESS_KEY and VELORIX_RUSTFS_SECRET_KEY",
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

if production_gc_generated == "true":
    evidence["production_gc_artifact"] = {
        "generated": True,
        "evidence_kind": "production_gc_run_evidence",
        "fixture_kind": "release_smoke_gc_fixture",
        "seed_artifact_path": production_gc_seed_path,
        "execute_artifact_path": production_gc_run_path,
        "artifact_path": production_gc_path,
        "validation_artifact_path": production_gc_validation_path,
        "deployment_id": production_gc_deployment_id,
        "authority_store_id": production_gc_authority_store_id,
        "gc_run_id": production_gc_run_id,
        "prefix": production_gc_prefix,
        "retain_latest_manifests": int(production_gc_retain_latest_manifests),
        "expected_min_deleted_candidates": 1,
        "seed_command": "cargo run -p velorix-cli -- gc-seed-s3-compatible-fixture --json",
        "execute_command": "cargo run -p velorix-cli -- gc-execute-s3-compatible --json",
        "verify_command": "cargo run -p velorix-cli -- gc-production-evidence --json",
        "validation_command": "cargo run -p velorix-cli -- rustfs-production-gc-evidence-validate --json",
        "scope": "separate production GC release evidence artifact generated by seeding a live retired-checkpoint fixture, executing the Velorix CLI S3-compatible GC path, and then verifying that non-empty persisted run; readiness remains blocked until the full release readiness report validates all required artifacts",
    }

with open(evidence_path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY

echo "wrote rustfs S3-compatible gate evidence to ${evidence_path}"
if [ "$production_gc_generated" = "true" ]; then
  cargo run -p velorix-cli -- rustfs-production-gc-evidence-validate \
    --gate-evidence "$evidence_path" \
    --seed-evidence "$production_gc_seed_path" \
    --execute-evidence "$production_gc_run_path" \
    --production-evidence "$production_gc_path" \
    --json > "$production_gc_validation_path"
  echo "wrote RustFS production GC seed evidence to ${production_gc_seed_path}"
  echo "wrote RustFS production GC run evidence to ${production_gc_run_path}"
  echo "wrote RustFS production GC evidence to ${production_gc_path}"
  echo "wrote RustFS production GC validation evidence to ${production_gc_validation_path}"
fi

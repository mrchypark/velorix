#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
container="${VELORIX_VERSITYGW_CONTAINER:-velorix-versitygw-s3-${run_id}}"
network="${VELORIX_VERSITYGW_NETWORK:-velorix-versitygw-s3-${run_id}}"
volume="${VELORIX_VERSITYGW_VOLUME:-velorix-versitygw-s3-${run_id}}"
image="${VELORIX_VERSITYGW_IMAGE:-versity/versitygw:v1.8.0}"
allow_mutable_versitygw_image="${VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE:-0}"
aws_cli_image="${VELORIX_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
allow_mutable_aws_cli_image="${VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE:-0}"
versitygw_access_key="${VELORIX_VERSITYGW_ACCESS_KEY:-velorix-versitygw-gate}"
versitygw_secret_key="${VELORIX_VERSITYGW_SECRET_KEY:-velorix-versitygw-gate-${run_id}}"
port="${VELORIX_VERSITYGW_PORT:-9000}"
region="${AWS_REGION:-us-east-1}"
bucket="${VELORIX_S3_BUCKET:-velorix-versitygw}"
prefix="${VELORIX_S3_PREFIX:-versitygw-s3-gate/${run_id}}"
cleanup="${VELORIX_VERSITYGW_CLEANUP:-1}"
min_free_kib="${VELORIX_VERSITYGW_MIN_FREE_KIB:-4194304}"
cargo_target_dir="${VELORIX_VERSITYGW_CARGO_TARGET_DIR:-${repo_root}/target/versitygw-s3-gate}"
evidence_path="${VELORIX_VERSITYGW_EVIDENCE_PATH:-target/velorix-s3/versitygw-s3-gate-evidence.json}"
production_gc_seed_path="${VELORIX_VERSITYGW_PRODUCTION_GC_SEED_PATH:-target/release-evidence/versitygw-production-gc-seed.json}"
production_gc_run_path="${VELORIX_VERSITYGW_PRODUCTION_GC_RUN_PATH:-target/release-evidence/versitygw-production-gc-run.json}"
production_gc_path="${VELORIX_VERSITYGW_PRODUCTION_GC_PATH:-target/release-evidence/versitygw-production-gc.json}"
production_gc_validation_path="${VELORIX_VERSITYGW_PRODUCTION_GC_VALIDATION_PATH:-target/release-evidence/versitygw-production-gc-validation.json}"
run_production_gc_evidence="${VELORIX_VERSITYGW_RUN_PRODUCTION_GC_EVIDENCE:-0}"
production_gc_retain_latest_manifests="${VELORIX_VERSITYGW_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS:-1}"
created_container=0
created_network=0
created_volume=0

case "$run_production_gc_evidence" in
  0) ;;
  1)
    echo "Versity Gateway production GC is unavailable: durable cross-process coordinator is required" >&2
    exit 75
    ;;
  *)
    echo "VELORIX_VERSITYGW_RUN_PRODUCTION_GC_EVIDENCE must be 0 or 1" >&2
    exit 64
    ;;
esac

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

  echo "docker daemon is not reachable; cannot run the Versity Gateway S3 gate" >&2
  if [ -n "$context_name" ]; then
    echo "docker_context=${context_name}" >&2
  fi
  echo "$output" >&2
  if command -v colima >/dev/null 2>&1; then
    echo "colima status:" >&2
    colima status >&2 || true
    echo "If this context is Colima-backed, repair or start Colima before rerunning scripts/run-versitygw-s3-gate.sh." >&2
  else
    echo "Start or repair Docker before rerunning scripts/run-versitygw-s3-gate.sh." >&2
  fi
  exit 1
}

is_mutable_image_reference() {
  case "$1" in *@sha256:*) return 1 ;; esac
  local name="${1##*/}"
  case "$name" in
    *:latest | *:latest-glibc | *:beta | *:beta-glibc) return 0 ;;
    *:*) return 1 ;;
    *) return 0 ;;
  esac
}

case "$allow_mutable_versitygw_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE must be 0 or 1" >&2
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
    echo "VELORIX_VERSITYGW_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS must be 1 for the fixed two-checkpoint release smoke fixture" >&2
    exit 64
    ;;
  1) ;;
  *)
    echo "VELORIX_VERSITYGW_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS must be 1 for the fixed two-checkpoint release smoke fixture" >&2
    exit 64
    ;;
esac

case "$min_free_kib" in
  '' | *[!0-9]*)
    echo "VELORIX_VERSITYGW_MIN_FREE_KIB must be a positive integer" >&2
    exit 64
    ;;
  0)
    echo "VELORIX_VERSITYGW_MIN_FREE_KIB must be greater than zero" >&2
    exit 64
    ;;
esac

if [ -z "$versitygw_access_key" ] || [ -z "$versitygw_secret_key" ]; then
  echo "VELORIX_VERSITYGW_ACCESS_KEY and VELORIX_VERSITYGW_SECRET_KEY must be non-empty" >&2
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
    echo "insufficient disk space for Versity Gateway S3 gate: available_kib=${available_kib} required_kib=${min_free_kib}" >&2
    echo "Free disk space or set VELORIX_VERSITYGW_MIN_FREE_KIB to an explicitly reviewed lower value before rerunning." >&2
    exit 75
  fi
}

preflight_cargo_target_dir() {
  mkdir -p "$cargo_target_dir"
  if [ ! -d "$cargo_target_dir" ] || [ ! -w "$cargo_target_dir" ]; then
    echo "CARGO_TARGET_DIR for Versity Gateway S3 gate is not writable: ${cargo_target_dir}" >&2
    exit 1
  fi
}

if [ "$versitygw_access_key" = "admin" ] || [ "$versitygw_secret_key" = "admin" ]; then
  echo "Versity Gateway S3 gate refuses example admin credentials; set VELORIX_VERSITYGW_ACCESS_KEY and VELORIX_VERSITYGW_SECRET_KEY to non-default values" >&2
  exit 64
fi

if [ "$allow_mutable_versitygw_image" != "1" ] && is_mutable_image_reference "$image"; then
  echo "VELORIX_VERSITYGW_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE=1 to use ${image}" >&2
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
    echo "remove it or set VELORIX_VERSITYGW_NETWORK to a fresh name" >&2
    exit 1
  fi
  if docker container inspect "$probe_container" >/dev/null 2>&1; then
    echo "docker preflight container already exists: ${probe_container}" >&2
    echo "remove it or set VELORIX_VERSITYGW_NETWORK to a fresh name" >&2
    exit 1
  fi

  if ! output="$(docker network create "$probe_network" 2>&1)"; then
    echo "docker cannot create bridge networks required by the Versity Gateway S3 gate" >&2
    echo "$output" >&2
    echo "repair or restart Docker, then rerun scripts/run-versitygw-s3-gate.sh" >&2
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
    echo "docker cannot run containers on bridge networks required by the Versity Gateway S3 gate" >&2
    echo "$output" >&2
    echo "repair or restart Docker, then rerun scripts/run-versitygw-s3-gate.sh" >&2
    exit 1
  fi

  if ! output="$(docker network rm "$probe_network" 2>&1)"; then
    echo "docker created the preflight network but could not remove it: ${probe_network}" >&2
    echo "$output" >&2
    echo "remove the probe network manually before rerunning the Versity Gateway S3 gate" >&2
    exit 1
  fi
}

cleanup_versitygw() {
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

wait_for_versitygw() {
  for _ in $(seq 1 120); do
    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID="$versitygw_access_key" \
      -e AWS_SECRET_ACCESS_KEY="$versitygw_secret_key" \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api list-buckets >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  docker logs "$container" >&2 || true
  echo "versitygw did not become ready on http://127.0.0.1:${port}" >&2
  exit 1
}

ensure_bucket() {
  for _ in $(seq 1 120); do
    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID="$versitygw_access_key" \
      -e AWS_SECRET_ACCESS_KEY="$versitygw_secret_key" \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      return 0
    fi

    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID="$versitygw_access_key" \
      -e AWS_SECRET_ACCESS_KEY="$versitygw_secret_key" \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api create-bucket --bucket "$bucket" --region "$region" >/dev/null 2>&1; then
      return 0
    fi

    sleep 1
  done

  docker logs "$container" >&2 || true
  echo "versitygw S3 API did not become ready for bucket ${bucket}" >&2
  exit 1
}

require cargo
require docker
require jq
preflight_disk_space
preflight_cargo_target_dir
preflight_docker_daemon
preflight_docker_networks

cd "$repo_root"
trap cleanup_versitygw EXIT
mkdir -p "$(dirname "$evidence_path")" "$(dirname "$production_gc_seed_path")" "$(dirname "$production_gc_run_path")" "$(dirname "$production_gc_path")" "$(dirname "$production_gc_validation_path")"
rm -f "$evidence_path" "$production_gc_seed_path" "$production_gc_run_path" "$production_gc_path" "$production_gc_validation_path"

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
  if [ "$(docker volume inspect --format '{{ index .Labels "velorix.dev/object-store" }}' "$volume")" != "versitygw-posix" ]; then
    echo "refusing existing volume ${volume}: requires label velorix.dev/object-store=versitygw-posix; use a fresh POSIX volume" >&2
    exit 64
  fi
  created_volume=0
else
  docker volume create --label velorix.dev/object-store=versitygw-posix "$volume" >/dev/null
  created_volume=1
fi

docker run -d \
  --name "$container" \
  --network "$network" \
  -p "${port}:9000" \
  -e ROOT_ACCESS_KEY="$versitygw_access_key" \
  -e ROOT_SECRET_KEY="$versitygw_secret_key" \
  -v "${volume}:/data" \
  "$image" \
  --port :9000 posix /data >/dev/null
created_container=1

# Resolve the running container's image, not a tag that could move after launch.
image_id="$(docker container inspect --format '{{.Image}}' "$container")"
if ! image_digest="$(docker image inspect --format '{{json .RepoDigests}}' "$image_id" |
  jq -er '[.[] | select(test("^(docker.io/)?versity/versitygw@sha256:[0-9a-f]{64}$")) | split("@")[1]][0] // error("missing Versity Gateway RepoDigest")')"; then
  echo "cannot resolve an immutable Versity Gateway image digest from the running container" >&2
  exit 1
fi

wait_for_versitygw
ensure_bucket

export VELORIX_S3_COMPAT=1
export AWS_ENDPOINT_URL="http://127.0.0.1:${port}"
export AWS_ACCESS_KEY_ID="$versitygw_access_key"
export AWS_SECRET_ACCESS_KEY="$versitygw_secret_key"
export AWS_REGION="$region"
export VELORIX_S3_BUCKET="$bucket"
export VELORIX_S3_PREFIX="$prefix"
export VELORIX_BENCHMARK_EVIDENCE_SCOPE=live_or_native
export CARGO_TARGET_DIR="$cargo_target_dir"
cargo test -p velorix-storage --test s3_compat --features s3-compat-tests -- --nocapture --test-threads=1
cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests -- --nocapture --test-threads=1

# Production GC remains blocked above; never manufacture a deletion artifact.
jq -n \
  --arg endpoint "$AWS_ENDPOINT_URL" \
  --arg bucket "$bucket" \
  --arg prefix "$prefix" \
  --arg region "$region" \
  --arg container "$container" \
  --arg image "versity/versitygw@${image_digest}" \
  --arg image_digest "$image_digest" \
  --arg volume "$volume" \
  --arg cargo_target_dir "$cargo_target_dir" \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" \
  --arg docker_version "$(docker version --format '{{.Server.Version}}')" \
  '{
    schema_version: 1,
    evidence_kind: "versitygw_s3_compatible_gate",
    provider: "versitygw",
    readiness_evidence_kind: ["s3_compatible", "s3_compatible_integration_harness"],
    gate_detail_kind: ["s3_compatible_ingest_admission_crash_restart", "s3_compatible_gc_execution_unavailable"],
    endpoint: $endpoint,
    bucket: $bucket,
    prefix: $prefix,
    region: $region,
    versitygw_container: $container,
    versitygw_image: $image,
    versitygw_image_digest: $image_digest,
    versitygw_volume: $volume,
    cargo_target_dir: $cargo_target_dir,
    credentials_redacted: true,
    credential_policy: "run-local non-default Versity Gateway root credentials; override with VELORIX_VERSITYGW_ACCESS_KEY and VELORIX_VERSITYGW_SECRET_KEY",
    generated_at: $generated_at,
    docker_version: $docker_version,
    live_tests: [
      "cargo test -p velorix-storage --test s3_compat --features s3-compat-tests",
      "cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests"
    ],
    benchmark: {ran: false, result_path: null, validation: null},
    backend_evidence_scope: "live_or_native",
    scope: "Versity Gateway S3-compatible live evidence through the S3 API; release benchmark closure still requires the benchmark-gate artifact for the selected gate level"
  }' > "$evidence_path"

echo "wrote Versity Gateway S3-compatible gate evidence to ${evidence_path}"

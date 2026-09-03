#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"

evidence_dir="${VELORIX_FIRST_E2E_EVIDENCE_DIR:-target/release-evidence}"
dependency_dir="${VELORIX_FIRST_E2E_DEPENDENCY_DIR:-target/dependency-governance}"
k8s_dir="${VELORIX_FIRST_E2E_K8S_DIR:-target/velorix-k8s}"
bench_dir="${VELORIX_FIRST_E2E_BENCH_DIR:-target/velorix-bench}"

cargo_deny_jsonl="${VELORIX_FIRST_E2E_CARGO_DENY_JSONL:-${dependency_dir}/cargo-deny.jsonl}"
dependency_evidence="${VELORIX_FIRST_E2E_DEPENDENCY_EVIDENCE:-${dependency_dir}/local-dependency-governance-evidence.json}"
rustfs_benchmark_result="${VELORIX_FIRST_E2E_RUSTFS_BENCHMARK_RESULT:-${bench_dir}/rustfs-s3-release.json}"
s3_benchmark_gate_evidence="${VELORIX_FIRST_E2E_S3_BENCHMARK_GATE_EVIDENCE:-${evidence_dir}/s3-release-benchmark-gate.json}"
production_gc_seed_evidence="${VELORIX_FIRST_E2E_PRODUCTION_GC_SEED_EVIDENCE:-${evidence_dir}/rustfs-production-gc-seed.json}"
production_gc_run_evidence="${VELORIX_FIRST_E2E_PRODUCTION_GC_RUN_EVIDENCE:-${evidence_dir}/rustfs-production-gc-run.json}"
production_gc_evidence="${VELORIX_FIRST_E2E_PRODUCTION_GC_EVIDENCE:-${evidence_dir}/rustfs-production-gc.json}"
production_gc_validation_evidence="${VELORIX_FIRST_E2E_PRODUCTION_GC_VALIDATION_EVIDENCE:-${evidence_dir}/rustfs-production-gc-validation.json}"
vind_evidence="${VELORIX_FIRST_E2E_VIND_EVIDENCE:-${k8s_dir}/vind-k8s-gate-evidence.json}"
ingest_writer_lifecycle_evidence_default="${k8s_dir}/ingest-writer-lifecycle-attestation.json"
ingest_writer_lifecycle_evidence="${VELORIX_FIRST_E2E_INGEST_WRITER_LIFECYCLE_EVIDENCE:-$ingest_writer_lifecycle_evidence_default}"
ingest_writer_lifecycle_evidence_explicit=0
if [ -n "${VELORIX_FIRST_E2E_INGEST_WRITER_LIFECYCLE_EVIDENCE:-}" ]; then
  ingest_writer_lifecycle_evidence_explicit=1
fi
product_evidence="${VELORIX_FIRST_E2E_PRODUCT_EVIDENCE:-}"
run_product="${VELORIX_FIRST_E2E_RUN_PRODUCT:-0}"
product_profile="${VELORIX_FIRST_E2E_PRODUCT_PROFILE:-default}"
product_evidence_level="${VELORIX_FIRST_E2E_PRODUCT_EVIDENCE_LEVEL:-${VELORIX_PRODUCT_EVIDENCE_LEVEL:-}}"
product_output_dir="${VELORIX_FIRST_E2E_PRODUCT_OUTPUT_DIR:-target/velorix-product}"
product_object_store_durability_attestation_file="${VELORIX_FIRST_E2E_PRODUCT_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:-${VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:-}}"
product_ingress_tls_auth_attestation_file="${VELORIX_FIRST_E2E_PRODUCT_INGRESS_TLS_AUTH_ATTESTATION_FILE:-${VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE:-}}"
product_ingress_endpoint_url="${VELORIX_FIRST_E2E_PRODUCT_INGRESS_ENDPOINT_URL:-${VELORIX_INGRESS_ENDPOINT_URL:-}}"
product_ingress_controller="${VELORIX_FIRST_E2E_PRODUCT_INGRESS_CONTROLLER:-${VELORIX_INGRESS_CONTROLLER:-}}"
product_ingress_external_hostname="${VELORIX_FIRST_E2E_PRODUCT_INGRESS_EXTERNAL_HOSTNAME:-${VELORIX_INGRESS_EXTERNAL_HOSTNAME:-}}"
product_ingress_tls_auth_auto="${VELORIX_FIRST_E2E_PRODUCT_INGRESS_TLS_AUTH_AUTO:-${VELORIX_INGRESS_TLS_AUTH_AUTO:-}}"
readiness_evidence="${VELORIX_FIRST_E2E_READINESS_EVIDENCE:-${evidence_dir}/first-e2e-readiness-evidence.json}"
readiness_report="${VELORIX_FIRST_E2E_READINESS_REPORT:-${evidence_dir}/first-e2e-readiness-report.json}"
local_environment_blocker="${VELORIX_FIRST_E2E_LOCAL_ENVIRONMENT_BLOCKER:-${evidence_dir}/first-e2e-local-environment-blocker.json}"
local_disk_preflight="${VELORIX_LOCAL_DISK_PREFLIGHT:-1}"
local_min_free_disk_gib="${VELORIX_FIRST_E2E_MIN_FREE_DISK_GIB:-${VELORIX_LOCAL_MIN_FREE_DISK_GIB:-20}}"

api_image="${VELORIX_API_IMAGE:-}"
meta_image="${VELORIX_META_IMAGE:-}"
hiqlite_deploy="${VELORIX_HIQLITE_DEPLOY:-0}"
hiqlite_image="${VELORIX_HIQLITE_IMAGE:-velorix-hiqlite:e2e}"
ingest_writer_image="${VELORIX_INGEST_WRITER_IMAGE:-velorix-ingest-writer:e2e}"
hiqlite_local_source_dir="${VELORIX_HIQLITE_LOCAL_SOURCE_DIR:-${repo_root}/../hiqlite}"
max_regression_fraction="${VELORIX_FIRST_E2E_MAX_REGRESSION_FRACTION:-0.35}"

skip_rustfs="${VELORIX_FIRST_E2E_SKIP_RUSTFS:-0}"
skip_docker_build="${VELORIX_FIRST_E2E_SKIP_DOCKER_BUILD:-0}"
skip_vind="${VELORIX_FIRST_E2E_SKIP_VIND:-0}"

rustfs_cleanup_requested="${VELORIX_RUSTFS_CLEANUP:-1}"
rustfs_cleanup_after_product=0
rustfs_product_container="${VELORIX_RUSTFS_CONTAINER:-velorix-first-e2e-rustfs-${run_id}}"
rustfs_product_network="${VELORIX_RUSTFS_NETWORK:-velorix-first-e2e-rustfs-${run_id}}"
rustfs_product_volume="${VELORIX_RUSTFS_VOLUME:-velorix-first-e2e-rustfs-${run_id}}"
rustfs_access_key="${VELORIX_RUSTFS_ACCESS_KEY:-velorix-first-e2e-rustfs}"
rustfs_secret_key="${VELORIX_RUSTFS_SECRET_KEY:-velorix-first-e2e-rustfs-${run_id}}"
rustfs_credentials_explicit=0
if [ -n "${VELORIX_RUSTFS_ACCESS_KEY:-}" ] || [ -n "${VELORIX_RUSTFS_SECRET_KEY:-}" ]; then
  rustfs_credentials_explicit=1
fi

cleanup_first_e2e_rustfs() {
  if [ "$rustfs_cleanup_after_product" != "1" ]; then
    return 0
  fi
  docker rm -f "$rustfs_product_container" >/dev/null 2>&1 || true
  docker network rm "$rustfs_product_network" >/dev/null 2>&1 || true
  docker volume rm "$rustfs_product_volume" >/dev/null 2>&1 || true
}

trap cleanup_first_e2e_rustfs EXIT

usage() {
  cat <<'EOF'
Run the local first-E2E readiness profile.

Usage:
  scripts/run-first-e2e-readiness.sh

Main environment overrides:
  VELORIX_FIRST_E2E_MAX_REGRESSION_FRACTION=0.35
  VELORIX_API_IMAGE=<required with RUN_PRODUCT=1 and SKIP_DOCKER_BUILD=1>
  VELORIX_META_IMAGE=<required with RUN_PRODUCT=1 and SKIP_DOCKER_BUILD=1>
  VELORIX_HIQLITE_DEPLOY=0  # set 1 to let the product slice deploy managed no-PVC Hiqlite
  VELORIX_HIQLITE_IMAGE=velorix-hiqlite:e2e
  VELORIX_INGEST_WRITER_IMAGE=velorix-ingest-writer:e2e
  VELORIX_FIRST_E2E_SKIP_RUSTFS=1
  VELORIX_FIRST_E2E_SKIP_DOCKER_BUILD=1
  VELORIX_FIRST_E2E_SKIP_VIND=1
  VELORIX_FIRST_E2E_RUN_PRODUCT=1
  VELORIX_FIRST_E2E_PRODUCT_PROFILE=logical-fencing
  VELORIX_FIRST_E2E_PRODUCT_EVIDENCE=target/velorix-product/product-evidence.json
  VELORIX_FIRST_E2E_PRODUCT_AWS_ENDPOINT_URL=http://host.docker.internal:9000
  VELORIX_RUSTFS_CLEANUP=1  # with RUN_PRODUCT=1, cleanup happens after product evidence
  VELORIX_LOCAL_DISK_PREFLIGHT=1
  VELORIX_FIRST_E2E_MIN_FREE_DISK_GIB=20

Default output:
  target/dependency-governance/local-dependency-governance-evidence.json
  target/release-evidence/s3-release-benchmark-gate.json
  target/release-evidence/rustfs-production-gc.json
  target/release-evidence/rustfs-production-gc-validation.json
  target/velorix-k8s/vind-k8s-gate-evidence.json
  target/velorix-product/ingest-writer-lifecycle-attestation.json via RUN_PRODUCT=1,
    or target/velorix-k8s/ingest-writer-lifecycle-attestation.json / explicit override
  target/velorix-product/product-evidence.json via RUN_PRODUCT=1 or VELORIX_FIRST_E2E_PRODUCT_EVIDENCE
  target/release-evidence/first-e2e-readiness-evidence.json
  target/release-evidence/first-e2e-readiness-report.json

This is not the 1.0 release gate. It intentionally accepts local cargo-deny
governance evidence and does not require cargo-vet external audit attestation.
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

require_file() {
  if [ ! -f "$1" ]; then
    echo "missing required artifact: $1" >&2
    echo "rerun the producing gate or set the matching VELORIX_FIRST_E2E_* path override" >&2
    exit 66
  fi
}

step() {
  printf '\n==> %s\n' "$*"
}

require_env() {
  if [ -z "${!1:-}" ]; then
    echo "$1 must be set" >&2
    exit 64
  fi
}

require_local_docker_image() {
  if ! docker image inspect "$1" >/dev/null 2>&1; then
    echo "missing required local Docker image: $1" >&2
    echo "build it first or unset VELORIX_FIRST_E2E_SKIP_DOCKER_BUILD" >&2
    exit 66
  fi
}

write_local_disk_capacity_blocker() {
  local df_file="${evidence_dir}/first-e2e-local-host-df.txt"
  df -Pk "$repo_root" >"$df_file"
  python3 - \
    "$local_environment_blocker" \
    "$df_file" \
    "$local_min_free_disk_gib" \
    "$repo_root" <<'PY'
import json
import sys
from datetime import datetime, timezone

blocker_path, df_path, required_gib, repo_root = sys.argv[1:]
with open(df_path, "r", encoding="utf-8", errors="replace") as f:
    df_text = f.read()

line = [line for line in df_text.splitlines() if line.strip()][1]
parts = line.split()
available_kib = int(parts[3])
required_kib = int(required_gib) * 1024 * 1024
blocker = {
    "schema_version": 1,
    "evidence_kind": "velorix_local_environment_blocker",
    "blocker_kind": "local_host_disk_capacity",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "repo_root": repo_root,
    "required_free_gib": int(required_gib),
    "available_free_gib": round(available_kib / 1024 / 1024, 2),
    "available_kib": available_kib,
    "required_kib": required_kib,
    "trusted_for_product_complete": False,
    "product_failure": False,
    "evidence_files": {"host_df": df_path},
    "remediation": [
        "free local Docker/Colima/vCluster capacity before running first-E2E",
        "if appropriate, delete Docker build cache with scripts/doctor-vind-local.sh --prune-build-cache --yes",
        "increase local Docker or host disk capacity",
        "or rerun with VELORIX_FIRST_E2E_MIN_FREE_DISK_GIB=<lower bound> when you intentionally accept the risk",
        "do not add PVCs to bypass this no-PVC product path",
    ],
}
with open(blocker_path, "w", encoding="utf-8") as f:
    json.dump(blocker, f, indent=2, sort_keys=True)
    f.write("\n")
print(
    f"local host free disk is {blocker['available_free_gib']}GiB, below required {required_gib}GiB",
    file=sys.stderr,
)
print(f"wrote local environment blocker evidence to {blocker_path}", file=sys.stderr)
PY
}

check_local_disk_preflight() {
  case "$local_disk_preflight" in
    0 | 1) ;;
    *)
      echo "VELORIX_LOCAL_DISK_PREFLIGHT must be 0 or 1" >&2
      exit 64
      ;;
  esac
  case "$local_min_free_disk_gib" in
    '' | *[!0-9]*)
      echo "VELORIX_FIRST_E2E_MIN_FREE_DISK_GIB must be a non-negative integer" >&2
      exit 64
      ;;
  esac
  if [ "$local_disk_preflight" != "1" ]; then
    return 0
  fi
  local available_kib
  local required_kib
  available_kib="$(df -Pk "$repo_root" | awk 'NR == 2 {print $4}')"
  required_kib=$((local_min_free_disk_gib * 1024 * 1024))
  if [ "$available_kib" -lt "$required_kib" ]; then
    write_local_disk_capacity_blocker
    exit 75
  fi
}

case "$run_product" in
  0 | 1) ;;
  *)
    echo "VELORIX_FIRST_E2E_RUN_PRODUCT must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$product_profile" in
  default | logical-fencing | required) ;;
  *)
    echo "VELORIX_FIRST_E2E_PRODUCT_PROFILE must be default, logical-fencing, or required" >&2
    exit 64
    ;;
esac
if [ "$run_product" = "0" ] && [ -z "$product_evidence" ]; then
  cat >&2 <<'EOF'
first-E2E readiness schema v5 requires deployed standing-runtime fencing product evidence.

Set VELORIX_FIRST_E2E_RUN_PRODUCT=1 with
VELORIX_FIRST_E2E_PRODUCT_PROFILE=logical-fencing, or provide an existing
VELORIX_FIRST_E2E_PRODUCT_EVIDENCE artifact that includes a passing
two-replica standing-runtime fencing smoke.
EOF
  exit 64
fi
if [ "$run_product" = "1" ] && [ "$product_profile" = "default" ]; then
  cat >&2 <<'EOF'
VELORIX_FIRST_E2E_RUN_PRODUCT=1 now requires
VELORIX_FIRST_E2E_PRODUCT_PROFILE=logical-fencing or required.

The default product profile is useful for local REST smoke, but it does not
produce the multi-replica standing-runtime fencing evidence required by
first-E2E readiness schema v5.
EOF
  exit 64
fi
case "$rustfs_cleanup_requested" in
  0 | 1) ;;
  *)
    echo "VELORIX_RUSTFS_CLEANUP must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$hiqlite_deploy" in
  0 | 1) ;;
  *)
    echo "VELORIX_HIQLITE_DEPLOY must be 0 or 1" >&2
    exit 64
    ;;
esac
if [ "$rustfs_credentials_explicit" = "1" ]; then
  if [ -z "${VELORIX_RUSTFS_ACCESS_KEY:-}" ] || [ -z "${VELORIX_RUSTFS_SECRET_KEY:-}" ]; then
    echo "VELORIX_RUSTFS_ACCESS_KEY and VELORIX_RUSTFS_SECRET_KEY must be set together" >&2
    exit 64
  fi
fi
if [ "$rustfs_access_key" = "rustfsadmin" ] || [ "$rustfs_secret_key" = "rustfsadmin" ]; then
  echo "RustFS default credentials are not allowed for first-E2E readiness" >&2
  exit 64
fi

require cargo
require docker
require python3

cd "$repo_root"
mkdir -p "$evidence_dir" "$dependency_dir" "$k8s_dir" "$bench_dir"
check_local_disk_preflight

if [ "$skip_docker_build" = "1" ]; then
  require_local_docker_image "$ingest_writer_image"
  if [ "$run_product" = "1" ]; then
    if [ -z "$api_image" ] || [ -z "$meta_image" ]; then
      echo "VELORIX_API_IMAGE and VELORIX_META_IMAGE are required when VELORIX_FIRST_E2E_RUN_PRODUCT=1 and VELORIX_FIRST_E2E_SKIP_DOCKER_BUILD=1" >&2
      exit 64
    fi
    require_local_docker_image "$api_image"
    require_local_docker_image "$meta_image"
    if [ "$hiqlite_deploy" = "1" ]; then
      require_local_docker_image "$hiqlite_image"
    fi
  fi
fi

step "Checking dependency governance with cargo-deny"
cargo metadata --format-version 1 --locked --all-features >"${dependency_dir}/cargo-metadata.json"
cargo deny --color never --locked --metadata-path "${dependency_dir}/cargo-metadata.json" -f json check -W unmaintained 2>"$cargo_deny_jsonl"
cargo run -p velorix-cli -- dependency-governance-validate \
  --manifest dependency-governance.json \
  --cargo-deny-json "$cargo_deny_jsonl" \
  --json >"$dependency_evidence"

if [ "$skip_rustfs" = "1" ]; then
  step "Skipping RustFS S3 gate and reusing existing artifacts"
  require_file "$rustfs_benchmark_result"
  require_file "$production_gc_seed_evidence"
  require_file "$production_gc_run_evidence"
  require_file "$production_gc_evidence"
  require_file "$production_gc_validation_evidence"
else
  step "Running RustFS S3-compatible gate"
  rustfs_env=(
    "VELORIX_BENCHMARK_GATE_LEVEL=release"
    "VELORIX_RUSTFS_BENCHMARK_PATH=$rustfs_benchmark_result"
    "VELORIX_RUSTFS_PRODUCTION_GC_SEED_PATH=$production_gc_seed_evidence"
    "VELORIX_RUSTFS_PRODUCTION_GC_RUN_PATH=$production_gc_run_evidence"
    "VELORIX_RUSTFS_PRODUCTION_GC_PATH=$production_gc_evidence"
    "VELORIX_RUSTFS_PRODUCTION_GC_VALIDATION_PATH=$production_gc_validation_evidence"
    "VELORIX_RUSTFS_ACCESS_KEY=$rustfs_access_key"
    "VELORIX_RUSTFS_SECRET_KEY=$rustfs_secret_key"
  )
  if [ "$run_product" = "1" ]; then
    rustfs_env+=(
      "VELORIX_RUSTFS_CLEANUP=0"
      "VELORIX_RUSTFS_CONTAINER=$rustfs_product_container"
      "VELORIX_RUSTFS_NETWORK=$rustfs_product_network"
      "VELORIX_RUSTFS_VOLUME=$rustfs_product_volume"
    )
    if [ "$rustfs_cleanup_requested" = "1" ]; then
      rustfs_cleanup_after_product=1
    fi
  fi
  env "${rustfs_env[@]}" scripts/run-rustfs-s3-gate.sh
fi

step "Generating first-E2E S3 release benchmark gate evidence"
cargo run -p velorix-cli -- benchmark-gate \
  --gate-level release \
  --backend s3-compatible \
  --baseline baselines/benchmark/s3/release.json \
  --result "$rustfs_benchmark_result" \
  --max-regression-fraction "$max_regression_fraction" \
  --json >"$s3_benchmark_gate_evidence"

if [ "$skip_docker_build" = "1" ]; then
  step "Skipping ingest-writer image build"
else
  step "Building ingest-writer image ${ingest_writer_image}"
  if [ ! -f "${hiqlite_local_source_dir}/hiqlite/Cargo.toml" ]; then
    echo "VELORIX_HIQLITE_LOCAL_SOURCE_DIR must point to a hiqlite checkout containing hiqlite/Cargo.toml: ${hiqlite_local_source_dir}" >&2
    exit 64
  fi
  DOCKER_BUILDKIT=1 docker build \
    --build-context "velorix-hiqlite-source=${hiqlite_local_source_dir}" \
    -f Dockerfile.ingest-writer \
    -t "$ingest_writer_image" \
    .
fi

if [ "$skip_vind" = "1" ]; then
  step "Skipping vind Kubernetes gate and reusing existing artifact"
  require_file "$vind_evidence"
else
  step "Running vind Kubernetes gate with ingest-writer image"
  VELORIX_K8S_INGEST_WRITER_IMAGE="$ingest_writer_image" \
    VELORIX_VIND_EVIDENCE_PATH="$vind_evidence" \
    scripts/run-vind-k8s-gate.sh
fi
if [ "$run_product" != "1" ] || [ "$ingest_writer_lifecycle_evidence_explicit" = "1" ]; then
  require_file "$ingest_writer_lifecycle_evidence"
fi

if [ "$run_product" = "1" ]; then
  step "Running manually callable vind product slice"
  require_file "$production_gc_evidence"
  IFS=$'\t' read -r product_deployment_id product_authority_store_id product_authority_kind product_s3_bucket product_s3_prefix < <(
    python3 - "$production_gc_evidence" <<'PY'
import json
import sys
from urllib.parse import urlparse

with open(sys.argv[1], "r", encoding="utf-8") as f:
    production_gc = json.load(f)

deployment_id = production_gc.get("deployment_id")
authority_store_id = production_gc.get("authority_store_id")
if not isinstance(deployment_id, str) or not deployment_id.strip():
    raise SystemExit("production GC evidence is missing deployment_id")
if not isinstance(authority_store_id, str) or not authority_store_id.strip():
    raise SystemExit("production GC evidence is missing authority_store_id")

parsed = urlparse(authority_store_id)
parts = [part for part in parsed.path.split("/") if part]
if parsed.scheme != "s3" or parsed.netloc not in {"rustfs", "external"} or len(parts) < 2:
    raise SystemExit(
        "production GC authority_store_id must be s3://rustfs/<bucket>/<prefix> "
        "or s3://external/<bucket>/<prefix> when VELORIX_FIRST_E2E_RUN_PRODUCT=1"
    )
bucket = parts[0]
prefix = "/".join(parts[1:])
print(f"{deployment_id}\t{authority_store_id}\t{parsed.netloc}\t{bucket}\t{prefix}")
PY
  )
  product_aws_endpoint_url="${AWS_ENDPOINT_URL:-}"
  product_aws_access_key_id="${AWS_ACCESS_KEY_ID:-}"
  product_aws_secret_access_key="${AWS_SECRET_ACCESS_KEY:-}"
  product_aws_region="${AWS_REGION:-}"
  if [ "$product_authority_kind" = "rustfs" ]; then
    product_aws_endpoint_url="${VELORIX_FIRST_E2E_PRODUCT_AWS_ENDPOINT_URL:-http://host.docker.internal:${VELORIX_RUSTFS_PORT:-9000}}"
    if [ -n "${VELORIX_FIRST_E2E_PRODUCT_AWS_ACCESS_KEY_ID:-}" ] || [ -n "${VELORIX_FIRST_E2E_PRODUCT_AWS_SECRET_ACCESS_KEY:-}" ]; then
      if [ -z "${VELORIX_FIRST_E2E_PRODUCT_AWS_ACCESS_KEY_ID:-}" ] || [ -z "${VELORIX_FIRST_E2E_PRODUCT_AWS_SECRET_ACCESS_KEY:-}" ]; then
        echo "VELORIX_FIRST_E2E_PRODUCT_AWS_ACCESS_KEY_ID and VELORIX_FIRST_E2E_PRODUCT_AWS_SECRET_ACCESS_KEY must be set together" >&2
        exit 64
      fi
      product_aws_access_key_id="$VELORIX_FIRST_E2E_PRODUCT_AWS_ACCESS_KEY_ID"
      product_aws_secret_access_key="$VELORIX_FIRST_E2E_PRODUCT_AWS_SECRET_ACCESS_KEY"
    elif [ -n "${AWS_ACCESS_KEY_ID:-}" ] || [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
      if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
        echo "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together for RustFS product evidence" >&2
        exit 64
      fi
      product_aws_access_key_id="$AWS_ACCESS_KEY_ID"
      product_aws_secret_access_key="$AWS_SECRET_ACCESS_KEY"
    elif [ "$skip_rustfs" = "0" ] || [ "$rustfs_credentials_explicit" = "1" ]; then
      product_aws_access_key_id="$rustfs_access_key"
      product_aws_secret_access_key="$rustfs_secret_key"
    else
      cat >&2 <<'EOF'
VELORIX_FIRST_E2E_SKIP_RUSTFS=1 with RustFS product evidence requires explicit matching S3 credentials.

Set VELORIX_FIRST_E2E_PRODUCT_AWS_ACCESS_KEY_ID and
VELORIX_FIRST_E2E_PRODUCT_AWS_SECRET_ACCESS_KEY, or AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY, or VELORIX_RUSTFS_ACCESS_KEY and
VELORIX_RUSTFS_SECRET_KEY to match the reused RustFS backend.
EOF
      exit 64
    fi
    product_aws_region="${AWS_REGION:-us-east-1}"
  fi
  product_env=(
    "VELORIX_VIND_PRODUCT_DIR=$product_output_dir"
    "VELORIX_INGEST_WRITER_IMAGE=$ingest_writer_image"
    "VELORIX_BUILD_INGEST_WRITER_IMAGE=0"
    "VELORIX_LOAD_EXISTING_IMAGES=1"
    "VELORIX_API_HOLD_PORT_FORWARD=0"
    "VELORIX_LOCAL_DISK_PREFLIGHT=$local_disk_preflight"
    "VELORIX_LOCAL_MIN_FREE_DISK_GIB=$local_min_free_disk_gib"
    "VELORIX_PRODUCT_DEPLOYMENT_ID=$product_deployment_id"
    "VELORIX_AUTHORITY_STORE_ID=$product_authority_store_id"
    "VELORIX_OBJECT_STORE_MODE=external-s3"
    "VELORIX_S3_BUCKET=$product_s3_bucket"
    "VELORIX_S3_PREFIX=$product_s3_prefix"
    "AWS_ENDPOINT_URL=$product_aws_endpoint_url"
    "AWS_ACCESS_KEY_ID=$product_aws_access_key_id"
    "AWS_SECRET_ACCESS_KEY=$product_aws_secret_access_key"
    "AWS_REGION=$product_aws_region"
  )
  if [ -n "$product_evidence_level" ]; then
    product_env+=("VELORIX_PRODUCT_EVIDENCE_LEVEL=$product_evidence_level")
  fi
  if [ "$product_authority_kind" = "rustfs" ]; then
    if [ -n "$product_object_store_durability_attestation_file" ]; then
      cat >&2 <<'EOF'
VELORIX_FIRST_E2E_PRODUCT_OBJECT_STORE_DURABILITY_ATTESTATION_FILE cannot be used with
the internally generated first-E2E RustFS authority. That authority is local
development evidence and must not receive product-complete durability attestation.
EOF
      exit 64
    fi
    product_env+=("VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1")
  fi
  if [ -n "$product_object_store_durability_attestation_file" ]; then
    require_file "$product_object_store_durability_attestation_file"
    product_env+=(
      "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE=$product_object_store_durability_attestation_file"
    )
  fi
  if [ -n "$product_ingress_tls_auth_attestation_file" ]; then
    require_file "$product_ingress_tls_auth_attestation_file"
    product_env+=(
      "VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE=$product_ingress_tls_auth_attestation_file"
    )
  fi
  if [ -n "$product_ingress_endpoint_url" ]; then
    product_env+=("VELORIX_INGRESS_ENDPOINT_URL=$product_ingress_endpoint_url")
  fi
  if [ -n "$product_ingress_controller" ]; then
    product_env+=("VELORIX_INGRESS_CONTROLLER=$product_ingress_controller")
  fi
  if [ -n "$product_ingress_external_hostname" ]; then
    product_env+=("VELORIX_INGRESS_EXTERNAL_HOSTNAME=$product_ingress_external_hostname")
  fi
  if [ -n "$product_ingress_tls_auth_auto" ]; then
    product_env+=("VELORIX_INGRESS_TLS_AUTH_AUTO=$product_ingress_tls_auth_auto")
  fi
  if [ "$hiqlite_deploy" = "1" ]; then
    product_env+=(
      "VELORIX_HIQLITE_DEPLOY=1"
      "VELORIX_HIQLITE_IMAGE=$hiqlite_image"
    )
  fi
  if [ -n "$api_image" ]; then
    product_env+=("VELORIX_API_IMAGE=$api_image")
  fi
  if [ -n "$meta_image" ]; then
    product_env+=("VELORIX_META_IMAGE=$meta_image")
  fi
  if [ "$skip_docker_build" = "1" ]; then
    product_env+=(
      "VELORIX_BUILD_API_IMAGE=0"
      "VELORIX_BUILD_META_IMAGE=0"
    )
    if [ "$hiqlite_deploy" = "1" ]; then
      product_env+=("VELORIX_BUILD_HIQLITE_IMAGE=0")
    fi
  fi
  if [ "$product_authority_kind" = "external" ]; then
    require_env AWS_ENDPOINT_URL
    require_env AWS_ACCESS_KEY_ID
    require_env AWS_SECRET_ACCESS_KEY
    require_env AWS_REGION
  fi
	  case "$product_profile" in
	    default)
	      ;;
	    logical-fencing)
	      if [ "$hiqlite_deploy" != "1" ]; then
        require_env VELORIX_HIQLITE_NODES
        require_env VELORIX_HIQLITE_API_SECRET
      fi
      product_env+=(
        "VELORIX_STANDING_RUNTIME_FENCING=logical-fencing"
        "VELORIX_API_REPLICA_COUNT=2"
        "VELORIX_META_ENABLED=1"
	        "VELORIX_META_BACKEND=hiqlite"
	      )
	      ;;
	    required)
	      if [ "$hiqlite_deploy" != "1" ]; then
	        require_env VELORIX_HIQLITE_NODES
	        require_env VELORIX_HIQLITE_API_SECRET
	      fi
	      product_env+=(
	        "VELORIX_STANDING_RUNTIME_FENCING=required"
	        "VELORIX_API_REPLICA_COUNT=2"
	        "VELORIX_META_ENABLED=1"
	        "VELORIX_META_BACKEND=hiqlite"
	        "VELORIX_REQUIRE_HIQLITE_BACKEND_TIME=1"
	      )
	      ;;
	  esac
  env "${product_env[@]}" scripts/run-vind-product.sh
  product_evidence="${product_output_dir}/product-evidence.json"
  if [ "$ingest_writer_lifecycle_evidence_explicit" != "1" ] \
    && [ "$ingest_writer_lifecycle_evidence" = "$ingest_writer_lifecycle_evidence_default" ]; then
    ingest_writer_lifecycle_evidence="${product_output_dir}/ingest-writer-lifecycle-attestation.json"
  fi
fi
if [ -n "$product_evidence" ]; then
  require_file "$product_evidence"
fi
require_file "$ingest_writer_lifecycle_evidence"
require_file "$production_gc_validation_evidence"

step "Generating first-E2E readiness evidence"
python3 - "$production_gc_evidence" "$production_gc_validation_evidence" "$vind_evidence" "$ingest_writer_lifecycle_evidence" "$product_evidence" "$product_profile" "$readiness_evidence" <<'PY'
import json
import os
import sys
from urllib.parse import urlparse

production_gc_path, production_gc_validation_path, vind_path, ingest_writer_lifecycle_path, product_path, requested_product_profile, readiness_path = sys.argv[1:]

def require_sibling_evidence_file(artifact_path, filename, label):
    if not isinstance(filename, str) or not filename.strip() or "/" in filename or "\\" in filename:
        raise SystemExit(f"{label} has invalid evidence filename {filename!r}: {artifact_path}")
    sibling = os.path.join(os.path.dirname(artifact_path), filename)
    if not os.path.isfile(sibling):
        raise SystemExit(f"{label} requires sibling evidence file {sibling}: {artifact_path}")

with open(production_gc_path, "r", encoding="utf-8") as f:
    production_gc = json.load(f)
with open(production_gc_validation_path, "r", encoding="utf-8") as f:
    production_gc_validation = json.load(f)
with open(vind_path, "r", encoding="utf-8") as f:
    vind = json.load(f)
with open(ingest_writer_lifecycle_path, "r", encoding="utf-8") as f:
    ingest_writer_lifecycle = json.load(f)
product = None
if product_path:
    with open(product_path, "r", encoding="utf-8") as f:
        product = json.load(f)
if product is None:
    raise SystemExit(
        "first-E2E readiness schema v5 requires product evidence; set "
        "VELORIX_FIRST_E2E_RUN_PRODUCT=1 with "
        "VELORIX_FIRST_E2E_PRODUCT_PROFILE=logical-fencing or provide "
        "VELORIX_FIRST_E2E_PRODUCT_EVIDENCE"
    )

if production_gc.get("status") != "pass":
    raise SystemExit(f"production GC evidence is not pass: {production_gc_path}")
if production_gc.get("evidence_kind") != "production_gc_run_evidence":
    raise SystemExit(f"production GC evidence has wrong evidence_kind: {production_gc_path}")
if not str(production_gc.get("verified_gc_run_digest", "")).startswith("sha256:"):
    raise SystemExit(f"production GC evidence is missing verified GC run digest: {production_gc_path}")
if production_gc_validation.get("status") != "pass":
    raise SystemExit(f"production GC validation evidence is not pass: {production_gc_validation_path}")
if production_gc_validation.get("evidence_kind") != "rustfs_production_gc_evidence_family_validated":
    raise SystemExit(f"production GC validation evidence has wrong evidence_kind: {production_gc_validation_path}")
if production_gc_validation.get("deployment_id") != production_gc["deployment_id"]:
    raise SystemExit("production GC validation deployment_id does not match production GC evidence")
if production_gc_validation.get("authority_store_id") != production_gc["authority_store_id"]:
    raise SystemExit("production GC validation authority_store_id does not match production GC evidence")
if production_gc_validation.get("gc_run_id") != production_gc["gc_run_id"]:
    raise SystemExit("production GC validation gc_run_id does not match production GC evidence")
required_gc_validation_checks = {
    "rustfs_s3_compatible_gate_present",
    "seed_fixture_created_retired_checkpoint_state",
    "s3_gc_execute_deleted_seeded_candidate",
    "production_gc_evidence_verified_listing_retention_and_transition",
    "artifact_family_paths_and_identity_bound",
}
missing_gc_validation_checks = required_gc_validation_checks - set(production_gc_validation.get("checks") or [])
if missing_gc_validation_checks:
    raise SystemExit(
        "production GC validation evidence missing checks "
        + ",".join(sorted(missing_gc_validation_checks))
    )
if vind.get("evidence_kind") != "kubernetes_vind_gate":
    raise SystemExit(f"vind evidence has wrong evidence_kind: {vind_path}")
if ingest_writer_lifecycle.get("schema_version") != 1:
    raise SystemExit(f"ingest-writer lifecycle evidence has wrong schema_version: {ingest_writer_lifecycle_path}")
if ingest_writer_lifecycle.get("evidence_kind") != "velorix_ingest_writer_lifecycle_attestation":
    raise SystemExit(f"ingest-writer lifecycle evidence has wrong evidence_kind: {ingest_writer_lifecycle_path}")
for field in [
    "pod_internal_append_completed",
    "multi_pod_overlap_conflict_rejected",
    "adjacent_append_succeeded",
    "crash_restart_reconstruction_checked",
    "kubernetes_lease_handoff_checked",
    "lease_held_through_append_checked",
    "commit_guard_checked",
    "admission_commit_guard_bound_checked",
    "lease_loss_during_reservation_checked",
    "no_pvc_created_by_vind",
]:
    if ingest_writer_lifecycle.get(field) is not True:
        raise SystemExit(f"ingest-writer lifecycle evidence requires {field}=true: {ingest_writer_lifecycle_path}")
if ingest_writer_lifecycle.get("deployment_id") != production_gc["deployment_id"]:
    raise SystemExit("ingest-writer lifecycle evidence deployment_id does not match production GC evidence")
if ingest_writer_lifecycle.get("authority_store_id") != production_gc["authority_store_id"]:
    raise SystemExit("ingest-writer lifecycle evidence authority_store_id does not match production GC evidence")
required_ingest_writer_lifecycle_files = {
    "pod_internal_job": "velorix-ingest-writer-smoke-log.json",
    "overlap_job": "velorix-ingest-lifecycle-overlap-log.json",
    "adjacent_job": "velorix-ingest-lifecycle-adjacent-log.json",
    "restart_job": "velorix-ingest-lifecycle-restart-log.json",
    "lease_loss_job": "velorix-ingest-lifecycle-lease-loss-log.json",
    "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json",
}
ingest_writer_lifecycle_files = ingest_writer_lifecycle.get("evidence_files") or {}
for key, expected in required_ingest_writer_lifecycle_files.items():
    if ingest_writer_lifecycle_files.get(key) != expected:
        raise SystemExit(f"ingest-writer lifecycle evidence file {key} must be {expected}: {ingest_writer_lifecycle_path}")
    require_sibling_evidence_file(
        ingest_writer_lifecycle_path,
        expected,
        "ingest-writer lifecycle evidence",
    )

gate_detail_kind = set(vind.get("gate_detail_kind") or [])
kubernetes_evidence = "vind/vCluster Kubernetes gate evidence for live Kubernetes coordination paths"
if vind.get("ingest_writer_image_configured") and "kubernetes_ingest_writer_pod_topology_preflight" in gate_detail_kind:
    kubernetes_evidence += " including ingest-writer Pod topology preflight"
ingest_evidence = (
    "catalog-backed deployed ingest admission plus ingest-writer lifecycle evidence "
    f"from {ingest_writer_lifecycle.get('deployed_topology')}"
)
ownership_evidence = "durable ownership epoch record evidence from Kubernetes/vind gate and storage authority tests"
checkpoint_evidence = "published checkpoint lifecycle and recovery transition evidence from storage/runtime gates"
if product is not None:
    if product.get("evidence_kind") != "velorix_product_slice_evidence":
        raise SystemExit(f"product evidence has wrong evidence_kind: {product_path}")
    if product.get("deployment_id") != production_gc["deployment_id"]:
        raise SystemExit(
            f"product evidence deployment_id does not match production GC evidence: {product_path}"
        )
    product_object_store = product.get("object_store") or {}
    if product_object_store.get("authority_store_id") != production_gc["authority_store_id"]:
        raise SystemExit(
            f"product evidence authority_store_id does not match production GC evidence: {product_path}"
        )
    production_authority = urlparse(production_gc["authority_store_id"])
    production_authority_parts = [part for part in production_authority.path.split("/") if part]
    if production_authority.scheme != "s3" or production_authority.netloc not in {"rustfs", "external"} or len(production_authority_parts) < 2:
        raise SystemExit(f"production GC authority_store_id is not a supported S3 authority: {production_gc_path}")
    expected_s3_bucket = production_authority_parts[0]
    expected_s3_prefix = "/".join(production_authority_parts[1:])
    if product_object_store.get("mode") != "external-s3":
        raise SystemExit(f"product evidence must run through external-s3 mode against the production authority: {product_path}")
    if product_object_store.get("bucket") != expected_s3_bucket:
        raise SystemExit(f"product evidence bucket does not match production GC authority: {product_path}")
    if product_object_store.get("s3_prefix") != expected_s3_prefix:
        raise SystemExit(f"product evidence s3_prefix does not match production GC authority: {product_path}")
    if product_object_store.get("external_s3_validate_enabled") is not True:
        raise SystemExit(f"product evidence must enable external S3 validation: {product_path}")
    if product_object_store.get("external_s3_bucket_validated") is not True:
        raise SystemExit(f"product evidence must prove external S3 bucket validation: {product_path}")
    if product_object_store.get("external_s3_prefix_validated") is not True:
        raise SystemExit(f"product evidence must prove external S3 prefix read/write/list/delete validation: {product_path}")
    validation_key = product_object_store.get("external_s3_validation_key")
    expected_validation_prefix = (
        f"{expected_s3_prefix.rstrip('/')}/_velorix_external_s3_validation/"
        if expected_s3_prefix
        else "_velorix_external_s3_validation/"
    )
    if not isinstance(validation_key, str) or not validation_key.startswith(expected_validation_prefix):
        raise SystemExit(f"product evidence external S3 validation key is outside the authority prefix: {product_path}")
    validation_evidence = product_object_store.get("external_s3_validation_evidence") or {}
    if validation_evidence.get("job") != "external-s3-validate-job.json" or validation_evidence.get("log") != "external-s3-validate.log":
        raise SystemExit(f"product evidence must attach external S3 validation job/log evidence: {product_path}")
    require_sibling_evidence_file(product_path, "external-s3-validate-job.json", "product external S3 validation evidence")
    require_sibling_evidence_file(product_path, "external-s3-validate.log", "product external S3 validation evidence")
    if product.get("rest_callable") is not True:
        raise SystemExit(f"product evidence does not prove REST callability: {product_path}")
    api = product.get("api") or {}
    auth = api.get("auth") or {}
    if auth.get("mode") != "bearer-token":
        raise SystemExit(f"product evidence must use bearer-token API auth: {product_path}")
    if auth.get("secret_name") != "velorix-api-auth":
        raise SystemExit(f"product evidence must use the velorix-api-auth Secret: {product_path}")
    if auth.get("admin_secret_name") != "velorix-admin-auth":
        raise SystemExit(f"product evidence must use the velorix-admin-auth Secret: {product_path}")
    for field in [
        "missing_token_rejected",
        "wrong_token_rejected",
        "correct_token_smoke_passed",
        "data_plane_token_rejected_on_admin_route",
        "healthz_unauthenticated",
        "readyz_unauthenticated",
        "deployment_env_verified",
    ]:
        if auth.get(field) is not True:
            raise SystemExit(f"product evidence API auth smoke requires {field}=true: {product_path}")
    local_tls = auth.get("local_tls_auth_smoke") or {}
    if local_tls.get("enabled") is not True:
        raise SystemExit(f"product evidence must enable local TLS/auth smoke: {product_path}")
    if local_tls.get("passed") is not True:
        raise SystemExit(f"product evidence must prove local TLS/auth smoke passed: {product_path}")
    if local_tls.get("evidence") != "tls-auth-smoke.json":
        raise SystemExit(f"product evidence must attach tls-auth-smoke.json evidence: {product_path}")
    require_sibling_evidence_file(product_path, "tls-auth-smoke.json", "product local TLS/auth evidence")
    if local_tls.get("public_ingress_attestation") is not False:
        raise SystemExit(f"product evidence local TLS smoke must not claim public ingress attestation: {product_path}")
    if local_tls.get("trusted_for_product_complete") is not False:
        raise SystemExit(f"product evidence local TLS smoke must not be trusted for product_complete: {product_path}")
    no_pvc = product.get("no_pvc") or {}
    if no_pvc.get("namespace_validated") is not True:
        raise SystemExit(f"product evidence must prove no-PVC namespace validation: {product_path}")
    if no_pvc.get("evidence") != "no-pvc-namespace.json":
        raise SystemExit(f"product evidence must attach no-pvc-namespace.json evidence: {product_path}")
    require_sibling_evidence_file(product_path, "no-pvc-namespace.json", "product no-PVC namespace evidence")
    if no_pvc.get("contract") != "no PersistentVolumeClaim objects in the Velorix product namespace":
        raise SystemExit(f"product evidence no-PVC contract mismatch: {product_path}")
    openapi = api.get("openapi") or {}
    if openapi.get("catalog_smoke_passed") is not True:
        raise SystemExit(f"product evidence must prove OpenAPI catalog smoke passed: {product_path}")
    if openapi.get("evidence_file") != "openapi.json":
        raise SystemExit(f"product evidence must attach openapi.json evidence: {product_path}")
    require_sibling_evidence_file(product_path, "openapi.json", "product OpenAPI evidence")
    if openapi.get("promoted_api_path") != "/v1/api/scores/positive":
        raise SystemExit(f"product evidence OpenAPI smoke must use the default promoted API path: {product_path}")
    for field in [
        "promoted_api_path_present",
        "generic_query_path_absent",
        "legacy_parameterized_path_absent",
        "query_policy_extension_present",
        "response_schema_checked",
    ]:
        if openapi.get(field) is not True:
            raise SystemExit(f"product evidence OpenAPI catalog smoke requires {field}=true: {product_path}")
    if openapi.get("linked_view_policy_id") != "interactive":
        raise SystemExit(f"product evidence OpenAPI catalog must bind the interactive query policy: {product_path}")
    query_policy = api.get("query_policy") or {}
    if query_policy.get("catalog_smoke_passed") is not True:
        raise SystemExit(f"product evidence must prove query policy catalog smoke passed: {product_path}")
    if query_policy.get("production_bounds_required") is not True:
        raise SystemExit(f"product evidence must prove production query policy bounds are required: {product_path}")
    if query_policy.get("weak_policy_rejected") is not True:
        raise SystemExit(f"product evidence must prove weak query policy rejection: {product_path}")
    if query_policy.get("missing_policy_rejected") is not True:
        raise SystemExit(f"product evidence must prove missing query policy rejection: {product_path}")
    if query_policy.get("linked_view_policy_id") != "interactive":
        raise SystemExit(f"product evidence must prove default view query_policy_id linkage: {product_path}")
    query_policy_files = query_policy.get("evidence_files") or {}
    for key, expected in {
        "created": "query-policy-interactive.json",
        "read_back": "query-policy-interactive-read.json",
        "weak_policy_rejection": "query-policy-weak-rejection.json",
        "missing_policy_rejection": "query-policy-missing-view.json",
    }.items():
        if query_policy_files.get(key) != expected:
            raise SystemExit(f"product evidence query policy evidence file {key} must be {expected}: {product_path}")
        require_sibling_evidence_file(product_path, expected, "product query-policy evidence")
    product_ingest_writer = product.get("ingest_writer") or {}
    if product_ingest_writer.get("pod_internal_append_verified") is not True:
        raise SystemExit(f"product evidence must prove Pod-internal ingest-writer append: {product_path}")
    product_ingest_writer_files = product_ingest_writer.get("evidence_files") or {}
    for key, expected in {
        "job_log": "ingest-writer-job-log.json",
        "job": "ingest-writer-job.json",
        "pods": "ingest-writer-pods.json",
    }.items():
        if product_ingest_writer_files.get(key) != expected:
            raise SystemExit(f"product ingest-writer evidence file {key} must be {expected}: {product_path}")
        require_sibling_evidence_file(product_path, expected, "product ingest-writer append evidence")
    lifecycle_attestation = product_ingest_writer.get("lifecycle_attestation") or {}
    if lifecycle_attestation.get("validated") is not True:
        raise SystemExit(f"product evidence must include validated ingest-writer lifecycle attestation: {product_path}")
    if lifecycle_attestation.get("source") != "generated":
        raise SystemExit(f"product evidence must use script-generated ingest-writer lifecycle attestation: {product_path}")
    if lifecycle_attestation.get("trusted_for_product_complete") is not True:
        raise SystemExit(f"product evidence lifecycle attestation must be trusted for product-complete: {product_path}")
    if lifecycle_attestation.get("deployment_id") != production_gc["deployment_id"]:
        raise SystemExit(
            f"product evidence lifecycle deployment_id does not match production GC evidence: {product_path}"
        )
    if lifecycle_attestation.get("authority_store_id") != production_gc["authority_store_id"]:
        raise SystemExit(
            f"product evidence lifecycle authority_store_id does not match production GC evidence: {product_path}"
        )
    for field in [
        "pod_internal_append_completed",
        "multi_pod_overlap_conflict_rejected",
        "adjacent_append_succeeded",
        "crash_restart_reconstruction_checked",
        "kubernetes_lease_handoff_checked",
        "lease_held_through_append_checked",
        "commit_guard_checked",
        "admission_commit_guard_bound_checked",
        "lease_loss_during_reservation_checked",
        "no_pvc_created_by_vind",
    ]:
        if lifecycle_attestation.get(field) is not True:
            raise SystemExit(
                f"product evidence lifecycle attestation requires {field}=true: {product_path}"
            )
    provenance = lifecycle_attestation.get("evidence_provenance") or {}
    for key in [
        "pod_internal_job",
        "overlap_job",
        "adjacent_job",
        "restart_job",
        "lease_loss_job",
        "handoff_owner_a_job",
        "handoff_owner_b_job",
        "handoff_stale_owner_job",
    ]:
        item = provenance.get(key) or {}
        for field in [
            "job_uid",
            "pod_uid",
            "pod_name",
            "container_image",
            "container_image_id",
        ]:
            if not isinstance(item.get(field), str) or not item[field].strip():
                raise SystemExit(
                    f"product evidence lifecycle provenance requires {key}.{field}: {product_path}"
                )
    lifecycle_files = lifecycle_attestation.get("evidence_files") or {}
    for key, expected in required_ingest_writer_lifecycle_files.items():
        if lifecycle_files.get(key) != expected:
            raise SystemExit(f"product evidence lifecycle evidence file {key} must be {expected}: {product_path}")
        require_sibling_evidence_file(product_path, expected, "product lifecycle evidence")
    standing = product.get("standing_runtime_fencing") or {}
    capability = standing.get("capability") or {}
    configured_mode = standing.get("configured_mode")
    if requested_product_profile != "default" and configured_mode != requested_product_profile:
        raise SystemExit(
            f"product evidence configured_mode={configured_mode!r} does not match "
            f"VELORIX_FIRST_E2E_PRODUCT_PROFILE={requested_product_profile!r}: {product_path}"
        )
    replica_count = int(api.get("replica_count") or 0)
    if configured_mode == "unsafe-dev-only":
        raise SystemExit(
            f"first-E2E readiness schema v5 requires non-dev standing-runtime fencing: {product_path}"
        )
    if replica_count < 2:
        raise SystemExit(
            f"first-E2E readiness schema v5 requires at least two API replicas in product evidence: {product_path}"
        )
    adversarial = (product.get("metadata_store") or {}).get("standing_runtime_adversarial_smoke") or {}
    if adversarial.get("status") != "pass":
        raise SystemExit(
            f"product evidence missing metadata standing-runtime adversarial smoke pass: {product_path}"
        )
    assertions = adversarial.get("assertions") or {}
    for field in [
        "logical_owner_expiry_checked",
        "new_owner_epoch_fences_old_owner",
        "stale_owner_checkpoint_publish_rejected",
        "latest_checkpoint_remains_metadata_authoritative",
    ]:
        if assertions.get(field) is not True:
            raise SystemExit(
                f"product evidence adversarial smoke requires {field}=true: {product_path}"
            )
    if capability.get("multi_writer_fencing_safe") is not True:
        raise SystemExit(f"product evidence must prove multi_writer_fencing_safe=true: {product_path}")
    smoke = standing.get("multi_replica_fencing_smoke") or {}
    if smoke.get("status") != "pass":
        raise SystemExit(f"product evidence missing multi-replica fencing smoke pass: {product_path}")
    local_failover = standing.get("local_api_pod_failover_smoke") or {}
    if local_failover.get("status") != "pass":
        raise SystemExit(f"product evidence missing local API pod failover smoke pass: {product_path}")
    if local_failover.get("evidence") != "standing-runtime-failover-smoke.json":
        raise SystemExit(
            f"product evidence must attach standing-runtime-failover-smoke.json evidence: {product_path}"
        )
    require_sibling_evidence_file(
        product_path,
        "standing-runtime-failover-smoke.json",
        "product local API pod failover evidence",
    )
    if local_failover.get("trusted_for_product_complete") is not False:
        raise SystemExit(
            f"local API pod failover smoke must not be trusted for product_complete: {product_path}"
        )
    if local_failover.get("production_wall_clock_failover_attestation") is not False:
        raise SystemExit(
            f"local API pod failover smoke must not claim production wall-clock failover attestation: {product_path}"
        )
    if configured_mode == "logical-fencing":
        if capability.get("lease_expiry_semantics") != "operation_driven_logical":
            raise SystemExit(
                f"logical-fencing product evidence must report operation_driven_logical lease expiry: {product_path}"
            )
        if capability.get("bounded_wall_clock_failover") is not False:
            raise SystemExit(
                f"logical-fencing product evidence must not claim bounded wall-clock failover: {product_path}"
            )
    if configured_mode == "required":
        for field in [
            "bounded_wall_clock_failover",
            "production_bounded_failover_safe",
            "production_multi_writer_safe",
        ]:
            if capability.get(field) is not True:
                raise SystemExit(f"required product evidence requires {field}=true: {product_path}")
        if capability.get("backend_time_source_kind") != "raft_replicated_authority_time":
            raise SystemExit(
                f"required product evidence requires raft_replicated_authority_time backend time: {product_path}"
            )
    kubernetes_evidence += "; product slice REST/API evidence validated from run-vind-product.sh"
    ownership_evidence += "; metadata adversarial owner epoch fencing validated in deployed product slice"
    checkpoint_evidence += "; metadata adversarial checkpoint publish fencing validated in deployed product slice"
    ingest_evidence += "; product slice REST ingest/query path validated"

standing_runtime_evidence_kind = [
    "standing_runtime_fencing_capability",
    "multi_replica_standing_runtime_fencing_smoke",
    "local_api_pod_failover_smoke",
]
standing_runtime_evidence = (
    "standing-runtime fencing capability from product slice readyz; "
    "deployed multi-replica fencing smoke passed; "
    "local API pod failover smoke passed"
)

readiness = {
    "schema_version": 8,
    "deployment_id": production_gc["deployment_id"],
    "authority_store_id": production_gc["authority_store_id"],
    "capability_status": {
        "status": "pass",
        "evidence": "RustFS S3-compatible capability probe through configured S3 API",
        "evidence_kind": ["s3_compatible"],
    },
    "s3_compatible_test_status": {
        "status": "pass",
        "evidence": "RustFS S3-compatible integration harness passed through scripts/run-rustfs-s3-gate.sh",
        "evidence_kind": ["s3_compatible_integration_harness"],
    },
    "ownership_status": {
        "status": "pass",
        "evidence": ownership_evidence,
        "evidence_kind": ["durable_ownership_epoch_record"],
    },
    "checkpoint_status": {
        "status": "pass",
        "evidence": checkpoint_evidence,
        "evidence_kind": [
            "published_checkpoint_lifecycle_record",
            "checkpoint_recovery_transition_record",
        ],
    },
    "ingest_status": {
        "status": "pass",
        "evidence": ingest_evidence,
        "evidence_kind": [
            "catalog_backed_ingest_admission",
            "deployed_ingest_admission",
            "ingest_writer_lifecycle_attestation",
        ],
    },
    "standing_runtime_status": {
        "status": "pass",
        "evidence": standing_runtime_evidence,
        "evidence_kind": standing_runtime_evidence_kind,
    },
    "relation_catalog_status": {
        "status": "pass",
        "evidence": "relation catalog record, registry, closed adapter scope, and unsupported adapter fail-closed evidence",
        "evidence_kind": [
            "relation_catalog_record",
            "relation_catalog_registry",
            "relation_catalog_closed_adapter_scope",
            "relation_catalog_unsupported_adapter_fail_closed",
        ],
    },
    "state_status": {
        "status": "pass",
        "evidence": "SlateDB checkpoint ref and checked recovery evidence from RustFS runtime recovery harness",
        "evidence_kind": ["slate_db_checkpoint_ref", "slate_db_checked_recovery"],
    },
    "query_policy_status": {
        "status": "pass",
        "evidence": "query policy catalog evidence through production DataFusion table scan path",
        "evidence_kind": ["query_policy_catalog"],
    },
    "table_catalog_status": {
        "status": "pass",
        "evidence": "registry-backed table catalog evidence through RustFS S3-compatible query harness",
        "evidence_kind": ["registry_backed_table_catalog"],
    },
    "dependency_governance_status": {
        "status": "pass",
        "evidence": "cargo-deny diagnostics checked by dependency-governance-validate for local first E2E",
        "evidence_kind": ["dependency_governance_validated"],
    },
    "benchmark_gate_status": {
        "status": "pass",
        "evidence": "RustFS S3-compatible benchmark gate evidence generated for local first E2E",
        "evidence_kind": ["s3_compatible_benchmark_gate"],
    },
    "gc_status": {
        "status": "pass",
        "evidence": "RustFS production GC run evidence with retention and checkpoint GC transition checks",
        "evidence_kind": [
            "gc_run_evidence",
            "production_gc_run_evidence",
            "rustfs_production_gc_evidence_family_validated",
            "checkpoint_retention_record",
        ],
    },
    "kubernetes_status": {
        "status": "pass",
        "evidence": kubernetes_evidence,
        "evidence_kind": ["kubernetes_lease_client"],
    },
}

with open(readiness_path, "w", encoding="utf-8") as f:
    json.dump(readiness, f, indent=2, sort_keys=True)
    f.write("\n")
PY

step "Generating first-E2E readiness report"
cargo run -p velorix-cli -- readiness-report \
  --evidence "$readiness_evidence" \
  --first-e2e-artifacts \
  --dependency-governance-evidence "$dependency_evidence" \
  --s3-release-benchmark-gate-evidence "$s3_benchmark_gate_evidence" \
  --production-gc-run-evidence "$production_gc_evidence" \
  --rustfs-production-gc-validation-evidence "$production_gc_validation_evidence" \
  --ingest-writer-lifecycle-evidence "$ingest_writer_lifecycle_evidence" \
  --standing-runtime-product-evidence "$product_evidence" \
  --json >"$readiness_report"

step "First-E2E readiness summary"
python3 - "$readiness_report" "$dependency_evidence" "$s3_benchmark_gate_evidence" "$production_gc_evidence" "$production_gc_validation_evidence" "$vind_evidence" "$ingest_writer_lifecycle_evidence" "$product_evidence" <<'PY'
import json
import sys

report_path, dependency_path, s3_gate_path, production_gc_path, production_gc_validation_path, vind_path, ingest_writer_lifecycle_path, product_path = sys.argv[1:]
with open(report_path, "r", encoding="utf-8") as f:
    report = json.load(f)

print(f"production_ready={str(report.get('production_ready')).lower()}")
print(f"blocking_reasons={len(report.get('blocking_reasons') or [])}")
print(f"deployment_id={report.get('deployment_id')}")
print(f"authority_store_id={report.get('authority_store_id')}")
print(f"readiness_report={report_path}")
print(f"dependency_governance_evidence={dependency_path}")
print(f"s3_benchmark_gate_evidence={s3_gate_path}")
print(f"production_gc_evidence={production_gc_path}")
print(f"production_gc_validation_evidence={production_gc_validation_path}")
print(f"vind_kubernetes_evidence={vind_path}")
print(f"ingest_writer_lifecycle_evidence={ingest_writer_lifecycle_path}")
if product_path:
    print(f"product_evidence={product_path}")

if report.get("production_ready") is not True:
    raise SystemExit(1)
PY

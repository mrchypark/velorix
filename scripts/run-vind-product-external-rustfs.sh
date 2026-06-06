#!/usr/bin/env bash
set -euo pipefail
umask 077

case "$-" in
  *x*)
    echo "Refusing to run with shell xtrace enabled because auth secrets would be logged" >&2
    exit 64
    ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
output_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
container="${VELORIX_EXTERNAL_RUSTFS_CONTAINER:-velorix-product-external-rustfs-${run_id}}"
network="${VELORIX_EXTERNAL_RUSTFS_NETWORK:-velorix-product-external-rustfs-${run_id}}"
volume="${VELORIX_EXTERNAL_RUSTFS_VOLUME:-velorix-product-external-rustfs-${run_id}}"
image="${VELORIX_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.4}"
aws_cli_image="${VELORIX_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
allow_mutable_rustfs_image="${VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE:-0}"
allow_mutable_aws_cli_image="${VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE:-0}"
port="${VELORIX_EXTERNAL_RUSTFS_PORT:-${VELORIX_RUSTFS_PORT:-9000}}"
bucket="${VELORIX_S3_BUCKET:-velorix-product}"
prefix="${VELORIX_S3_PREFIX:-product/${run_id}}"
region="${AWS_REGION:-us-east-1}"
access_key="${VELORIX_EXTERNAL_RUSTFS_ACCESS_KEY:-}"
secret_key="${VELORIX_EXTERNAL_RUSTFS_SECRET_KEY:-}"
cleanup="${VELORIX_EXTERNAL_RUSTFS_CLEANUP:-0}"
pod_endpoint_explicit=0
if [ -n "${VELORIX_EXTERNAL_RUSTFS_POD_ENDPOINT+x}" ]; then
  pod_endpoint="$VELORIX_EXTERNAL_RUSTFS_POD_ENDPOINT"
  pod_endpoint_explicit=1
else
  pod_endpoint="http://host.docker.internal:${port}"
fi
local_endpoint="http://127.0.0.1:${port}"
evidence_file="${VELORIX_EXTERNAL_RUSTFS_EVIDENCE:-${output_dir}/external-rustfs-authority.json}"
env_file="${VELORIX_EXTERNAL_RUSTFS_ENV:-${output_dir}/external-rustfs.env}"
created_container=0
created_network=0
created_volume=0

usage() {
  cat <<'EOF'
Run the vind product slice with an external S3-compatible RustFS authority.

Usage:
  scripts/run-vind-product-external-rustfs.sh

This starts RustFS as a local Docker container with a Docker volume, creates the
configured bucket, writes target/velorix-product/external-rustfs.env, then runs
scripts/run-vind-product.sh with VELORIX_OBJECT_STORE_MODE=external-s3.

Main overrides:
  VELORIX_EXTERNAL_RUSTFS_PORT=9000
  VELORIX_EXTERNAL_RUSTFS_POD_ENDPOINT=http://host.docker.internal:9000
  VELORIX_EXTERNAL_RUSTFS_CLEANUP=0
  VELORIX_S3_BUCKET=velorix-product
  VELORIX_S3_PREFIX=product/<run-id>
  VELORIX_META_BACKEND=hiqlite
  VELORIX_STANDING_RUNTIME_FENCING=logical-fencing
  VELORIX_API_REPLICA_COUNT=2

The external RustFS container is local development infrastructure, not public
ingress/TLS/auth evidence and not Hiqlite backend-time proof.
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

random_token() {
  python3 - <<'PY'
import secrets

print(secrets.token_urlsafe(32))
PY
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

validate_token() {
  local name="$1"
  local value="$2"
  python3 - "$name" "$value" <<'PY'
import re
import sys

name, value = sys.argv[1:]
if not value:
    raise SystemExit(f"{name} must be nonempty")
if value.strip() != value:
    raise SystemExit(f"{name} must not have leading or trailing whitespace")
if not value.isascii():
    raise SystemExit(f"{name} must be ASCII")
if any(ch.isspace() for ch in value):
    raise SystemExit(f"{name} must not contain whitespace")
if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
    raise SystemExit(f"{name} must not contain control characters")
if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", value):
    raise SystemExit(f"{name} must contain only URL/header-safe token characters")
PY
}

validate_bucket() {
  python3 - "$bucket" <<'PY'
import re
import sys

bucket = sys.argv[1]
if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
    raise SystemExit("VELORIX_S3_BUCKET must be a DNS-compatible S3 bucket name")
if ".." in bucket or ".-" in bucket or "-." in bucket:
    raise SystemExit("VELORIX_S3_BUCKET must not contain adjacent dots or dot-hyphen sequences")
if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", bucket):
    raise SystemExit("VELORIX_S3_BUCKET must not look like an IPv4 address")
PY
}

wait_for_rustfs() {
  for _ in $(seq 1 120); do
    if docker run --rm \
      --network "$network" \
      -e AWS_ACCESS_KEY_ID="$access_key" \
      -e AWS_SECRET_ACCESS_KEY="$secret_key" \
      -e AWS_DEFAULT_REGION="$region" \
      "$aws_cli_image" \
      --endpoint-url "http://${container}:9000" \
      s3api list-buckets >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker logs "$container" >&2 || true
  echo "RustFS did not become reachable through Docker network ${network}" >&2
  exit 75
}

ensure_bucket() {
  if docker run --rm \
    --network "$network" \
    -e AWS_ACCESS_KEY_ID="$access_key" \
    -e AWS_SECRET_ACCESS_KEY="$secret_key" \
    -e AWS_DEFAULT_REGION="$region" \
    "$aws_cli_image" \
    --endpoint-url "http://${container}:9000" \
    s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    return 0
  fi

  docker run --rm \
    --network "$network" \
    -e AWS_ACCESS_KEY_ID="$access_key" \
    -e AWS_SECRET_ACCESS_KEY="$secret_key" \
    -e AWS_DEFAULT_REGION="$region" \
    "$aws_cli_image" \
    --endpoint-url "http://${container}:9000" \
    s3api create-bucket --bucket "$bucket" >/dev/null
}

resolve_pod_endpoint() {
  if [ "$pod_endpoint_explicit" = "1" ]; then
    return 0
  fi
  if [ "${VELORIX_VIND_CLUSTER_DRIVER:-docker-vcluster}" != "existing-context" ]; then
    return 0
  fi

  local context="${VELORIX_K8S_CONTEXT:-}"
  case "$context" in
    k3d-*) ;;
    *) return 0 ;;
  esac

  local cluster_name="${context#k3d-}"
  local node=""
  local suffix=""
  for suffix in server-0 agent-0 agent-1; do
    if docker container inspect "k3d-${cluster_name}-${suffix}" >/dev/null 2>&1; then
      node="k3d-${cluster_name}-${suffix}"
      break
    fi
  done
  if [ -z "$node" ]; then
    return 0
  fi

  local host_ip=""
  host_ip="$(
    docker exec "$node" sh -c \
      'ping -c 1 -W 1 host.docker.internal 2>/dev/null | sed -n "s/^PING [^(]*(\([^)]*\)).*/\1/p" | head -1' \
      2>/dev/null || true
  )"
  if [ -z "$host_ip" ]; then
    host_ip="$(
      docker exec "$node" sh -c \
        'ip route | sed -n "s/^default via \([^ ]*\).*/\1/p" | head -1' \
        2>/dev/null || true
    )"
  fi
  if [ -n "$host_ip" ]; then
    pod_endpoint="http://${host_ip}:${port}"
    echo "resolved k3d pod endpoint for external RustFS: ${pod_endpoint}"
  fi
}

write_external_rustfs_evidence() {
  mkdir -p "$output_dir"
  python3 - \
    "$evidence_file" \
    "$run_id" \
    "$container" \
    "$network" \
    "$volume" \
    "$image" \
    "$bucket" \
    "$prefix" \
    "$region" \
    "$local_endpoint" \
    "$pod_endpoint" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    path,
    run_id,
    container,
    network,
    volume,
    image,
    bucket,
    prefix,
    region,
    local_endpoint,
    pod_endpoint,
) = sys.argv[1:]
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_external_rustfs_product_authority",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "run_id": run_id,
    "container": container,
    "docker_network": network,
    "docker_volume": volume,
    "image": image,
    "bucket": bucket,
    "s3_prefix": prefix,
    "region": region,
    "host_endpoint": local_endpoint,
    "pod_endpoint": pod_endpoint,
    "object_store_mode_for_product": "external-s3",
    "uses_kubernetes_pvc": False,
    "trusted_for_product_complete": False,
    "trusted_scope": "local Docker RustFS authority for manual vind product execution",
    "remaining_product_complete_gates": [
        "public ingress/TLS/auth attestation",
        "Hiqlite backend-authoritative bounded wall-clock failover",
        "operator-reviewed external object-store durability policy",
    ],
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

write_env_file() {
  mkdir -p "$output_dir"
  cat >"$env_file" <<EOF
export VELORIX_OBJECT_STORE_MODE=external-s3
export VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1
export AWS_ENDPOINT_URL='${pod_endpoint}'
export AWS_ACCESS_KEY_ID='${access_key}'
export AWS_SECRET_ACCESS_KEY='${secret_key}'
export AWS_REGION='${region}'
export VELORIX_S3_BUCKET='${bucket}'
export VELORIX_S3_PREFIX='${prefix}'
export VELORIX_AUTHORITY_STORE_ID='s3://external/${bucket}/${prefix}'
export VELORIX_EXTERNAL_RUSTFS_LOCAL_ENDPOINT='${local_endpoint}'
export VELORIX_EXTERNAL_RUSTFS_CONTAINER='${container}'
export VELORIX_EXTERNAL_RUSTFS_VOLUME='${volume}'
EOF
}

cleanup_rustfs() {
  if [ "$cleanup" != "1" ]; then
    return 0
  fi
  if [ "$created_container" = "1" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [ "$created_network" = "1" ]; then
    docker network rm "$network" >/dev/null 2>&1 || true
  fi
  if [ "$created_volume" = "1" ]; then
    docker volume rm "$volume" >/dev/null 2>&1 || true
  fi
}

case "$cleanup" in
  0 | 1) ;;
  *)
    echo "VELORIX_EXTERNAL_RUSTFS_CLEANUP must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$port" in
  '' | *[!0-9]*)
    echo "VELORIX_EXTERNAL_RUSTFS_PORT must be a TCP port number" >&2
    exit 64
    ;;
esac
if [ "$allow_mutable_rustfs_image" != "1" ] && is_mutable_image_reference "$image"; then
  echo "VELORIX_RUSTFS_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE=1 to use ${image}" >&2
  exit 64
fi
if [ "$allow_mutable_aws_cli_image" != "1" ] && is_mutable_image_reference "$aws_cli_image"; then
  echo "VELORIX_AWS_CLI_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE=1 to use ${aws_cli_image}" >&2
  exit 64
fi

require docker
require python3
validate_bucket

if [ -z "$access_key" ]; then
  access_key="vlx$(python3 - <<'PY'
import secrets

print(secrets.token_hex(16))
PY
)"
fi
if [ -z "$secret_key" ]; then
  secret_key="$(random_token)"
fi
if [ "$access_key" = "rustfsadmin" ] || [ "$secret_key" = "rustfsadmin" ]; then
  echo "RustFS default credentials are not allowed" >&2
  exit 64
fi
validate_token VELORIX_EXTERNAL_RUSTFS_ACCESS_KEY "$access_key"
validate_token VELORIX_EXTERNAL_RUSTFS_SECRET_KEY "$secret_key"

cd "$repo_root"
trap cleanup_rustfs EXIT

if docker container inspect "$container" >/dev/null 2>&1; then
  echo "docker container already exists: ${container}" >&2
  exit 66
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
  -e RUSTFS_ACCESS_KEY="$access_key" \
  -e RUSTFS_SECRET_KEY="$secret_key" \
  -v "${volume}:/data" \
  "$image" \
  /data >/dev/null
created_container=1

wait_for_rustfs
ensure_bucket
resolve_pod_endpoint
write_external_rustfs_evidence
write_env_file

echo "external RustFS authority is running"
echo "local_endpoint=${local_endpoint}"
echo "pod_endpoint=${pod_endpoint}"
echo "bucket=${bucket}"
echo "prefix=${prefix}"
echo "evidence=${evidence_file}"
echo "env=${env_file}"

env \
  VELORIX_OBJECT_STORE_MODE=external-s3 \
  VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1 \
  AWS_ENDPOINT_URL="$pod_endpoint" \
  AWS_ACCESS_KEY_ID="$access_key" \
  AWS_SECRET_ACCESS_KEY="$secret_key" \
  AWS_REGION="$region" \
  VELORIX_S3_BUCKET="$bucket" \
  VELORIX_S3_PREFIX="$prefix" \
  VELORIX_AUTHORITY_STORE_ID="s3://external/${bucket}/${prefix}" \
  scripts/run-vind-product.sh

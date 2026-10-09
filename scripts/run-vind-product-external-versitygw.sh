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
container="${VELORIX_EXTERNAL_VERSITYGW_CONTAINER:-velorix-product-external-versitygw-${run_id}}"
network="${VELORIX_EXTERNAL_VERSITYGW_NETWORK:-velorix-product-external-versitygw-${run_id}}"
volume="${VELORIX_EXTERNAL_VERSITYGW_VOLUME:-velorix-product-external-versitygw-${run_id}}"
image="${VELORIX_VERSITYGW_IMAGE:-versity/versitygw:v1.8.0}"
aws_cli_image="${VELORIX_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
allow_mutable_versitygw_image="${VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE:-0}"
allow_mutable_aws_cli_image="${VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE:-0}"
port="${VELORIX_EXTERNAL_VERSITYGW_PORT:-${VELORIX_VERSITYGW_PORT:-9000}}"
bucket="${VELORIX_S3_BUCKET:-velorix-product}"
prefix="${VELORIX_S3_PREFIX:-product/${run_id}}"
region="${AWS_REGION:-us-east-1}"
access_key="${VELORIX_EXTERNAL_VERSITYGW_ACCESS_KEY:-}"
secret_key="${VELORIX_EXTERNAL_VERSITYGW_SECRET_KEY:-}"
cleanup="${VELORIX_EXTERNAL_VERSITYGW_CLEANUP:-0}"
pod_endpoint_explicit=0
if [ -n "${VELORIX_EXTERNAL_VERSITYGW_POD_ENDPOINT+x}" ]; then
  pod_endpoint="$VELORIX_EXTERNAL_VERSITYGW_POD_ENDPOINT"
  pod_endpoint_explicit=1
else
  pod_endpoint="http://host.docker.internal:${port}"
fi
local_endpoint="http://127.0.0.1:${port}"
evidence_file="${VELORIX_EXTERNAL_VERSITYGW_EVIDENCE:-${output_dir}/external-versitygw-authority.json}"
env_file="${VELORIX_EXTERNAL_VERSITYGW_ENV:-${output_dir}/external-versitygw.env}"
created_container=0
created_network=0
created_volume=0

usage() {
  cat <<'EOF'
Run the vind product slice with an external S3-compatible Versity Gateway authority.

Usage:
  scripts/run-vind-product-external-versitygw.sh

This starts Versity Gateway as a local Docker container with a Docker volume, creates the
configured bucket, writes target/velorix-product/external-versitygw.env, then runs
scripts/run-vind-product.sh with VELORIX_OBJECT_STORE_MODE=external-s3.

Main overrides:
  VELORIX_EXTERNAL_VERSITYGW_PORT=9000
  VELORIX_EXTERNAL_VERSITYGW_POD_ENDPOINT=http://host.docker.internal:9000
  VELORIX_EXTERNAL_VERSITYGW_CLEANUP=0
  VELORIX_S3_BUCKET=velorix-product
  VELORIX_S3_PREFIX=product/<run-id>
  VELORIX_META_BACKEND=oss
  VELORIX_STANDING_RUNTIME_FENCING=logical-fencing
  VELORIX_API_REPLICA_COUNT=2

The external Versity Gateway container is local development infrastructure, not public
ingress/TLS/auth evidence and not metadata-authority failover proof.

Existing volumes must be labeled velorix.dev/object-store=versitygw-posix.
Never mount RustFS data directories: migrate objects through S3 copy instead.
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
  openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n'
}

is_mutable_image_reference() {
  case "$1" in
    *@sha256:*) return 1 ;;
  esac
  local name="${1##*/}"
  case "$name" in
    *:latest | *:latest-glibc | *:beta | *:beta-glibc) return 0 ;;
    *:*) return 1 ;;
    *) return 0 ;;
  esac
}

validate_token() {
  local name="$1"
  local value="$2"
  local error
  error="$(jq -nr --arg value "$value" '
    def whitespace: "[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]";
    $value |
    if length == 0 then "must be nonempty"
    elif test("^" + whitespace + "|" + whitespace + "$") then "must not have leading or trailing whitespace"
    elif test("[^\u0000-\u007f]") then "must be ASCII"
    elif test(whitespace) then "must not contain whitespace"
    elif test("[\u0000-\u001f\u007f]") then "must not contain control characters"
    elif test("^[A-Za-z0-9._~+/=-]+$") | not then "must contain only URL/header-safe token characters"
    else empty end
  ')" || return
  if [ -n "$error" ]; then
    echo "$name $error" >&2
    return 1
  fi
}

validate_bucket() {
  local LC_ALL=C
  if [ "${#bucket}" -lt 3 ] || [ "${#bucket}" -gt 63 ] ||
    [[ ! "$bucket" =~ ^[a-z0-9][a-z0-9.-]*[a-z0-9]$ ]]; then
    echo "VELORIX_S3_BUCKET must be a DNS-compatible S3 bucket name" >&2
    return 1
  fi
  case "$bucket" in
    *..* | *.-* | *-.*)
      echo "VELORIX_S3_BUCKET must not contain adjacent dots or dot-hyphen sequences" >&2
      return 1 ;;
  esac
  if [[ "$bucket" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "VELORIX_S3_BUCKET must not look like an IPv4 address" >&2
    return 1
  fi
}

wait_for_versitygw() {
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
  echo "Versity Gateway did not become reachable through Docker network ${network}" >&2
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
    echo "resolved k3d pod endpoint for external Versity Gateway: ${pod_endpoint}"
  fi
}

write_external_versitygw_evidence() {
  mkdir -p "$output_dir"
  jq -nS \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg run_id "$run_id" \
    --arg container "$container" \
    --arg network "$network" \
    --arg volume "$volume" \
    --arg image "$image" \
    --arg bucket "$bucket" \
    --arg prefix "$prefix" \
    --arg region "$region" \
    --arg local_endpoint "$local_endpoint" \
    --arg pod_endpoint "$pod_endpoint" '
    {
      schema_version: 1,
      evidence_kind: "velorix_external_versitygw_product_authority",
      generated_at: $generated_at,
      run_id: $run_id,
      container: $container,
      docker_network: $network,
      docker_volume: $volume,
      image: $image,
      bucket: $bucket,
      s3_prefix: $prefix,
      region: $region,
      host_endpoint: $local_endpoint,
      pod_endpoint: $pod_endpoint,
      object_store_mode_for_product: "external-s3",
      uses_kubernetes_pvc: false,
      trusted_for_product_complete: false,
      trusted_scope: "local Docker Versity Gateway authority for manual vind product execution",
      remaining_product_complete_gates: [
        "public ingress/TLS/auth attestation",
        "metadata-authority bounded wall-clock failover",
        "operator-reviewed external object-store durability policy"
      ]
    }' >"$evidence_file"
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
export VELORIX_EXTERNAL_VERSITYGW_LOCAL_ENDPOINT='${local_endpoint}'
export VELORIX_EXTERNAL_VERSITYGW_CONTAINER='${container}'
export VELORIX_EXTERNAL_VERSITYGW_VOLUME='${volume}'
EOF
}

cleanup_versitygw() {
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
    echo "VELORIX_EXTERNAL_VERSITYGW_CLEANUP must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$port" in
  '' | *[!0-9]*)
    echo "VELORIX_EXTERNAL_VERSITYGW_PORT must be a TCP port number" >&2
    exit 64
    ;;
esac
if [ "$allow_mutable_versitygw_image" != "1" ] && is_mutable_image_reference "$image"; then
  echo "VELORIX_VERSITYGW_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE=1 to use ${image}" >&2
  exit 64
fi
if [ "$allow_mutable_aws_cli_image" != "1" ] && is_mutable_image_reference "$aws_cli_image"; then
  echo "VELORIX_AWS_CLI_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE=1 to use ${aws_cli_image}" >&2
  exit 64
fi

require docker
require jq
require openssl
validate_bucket

if [ -z "$access_key" ]; then
  access_key="vlx$(openssl rand -hex 16)"
fi
if [ -z "$secret_key" ]; then
  secret_key="$(random_token)"
fi
if [ "$access_key" = "rustfsadmin" ] || [ "$secret_key" = "rustfsadmin" ]; then
  echo "Versity Gateway default credentials are not allowed" >&2
  exit 64
fi
validate_token VELORIX_EXTERNAL_VERSITYGW_ACCESS_KEY "$access_key"
validate_token VELORIX_EXTERNAL_VERSITYGW_SECRET_KEY "$secret_key"

cd "$repo_root"
trap cleanup_versitygw EXIT

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
  if [ "$(docker volume inspect --format '{{ index .Labels "velorix.dev/object-store" }}' "$volume")" != "versitygw-posix" ]; then
    echo "Refusing an unverified POSIX volume: use a new Versity Gateway volume; migrate RustFS objects through S3 copy" >&2
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
  -e ROOT_ACCESS_KEY="$access_key" \
  -e ROOT_SECRET_KEY="$secret_key" \
  -v "${volume}:/data" \
  "$image" \
  --port :9000 posix /data >/dev/null
created_container=1

wait_for_versitygw
ensure_bucket
resolve_pod_endpoint
write_external_versitygw_evidence
write_env_file

echo "external Versity Gateway authority is running"
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

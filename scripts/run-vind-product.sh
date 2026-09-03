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
cluster_driver="${VELORIX_VIND_CLUSTER_DRIVER:-docker-vcluster}"
cluster="${VELORIX_VIND_CLUSTER:-velorix-product-${run_id}}"
product_deployment_id="${VELORIX_PRODUCT_DEPLOYMENT_ID:-${cluster}/${run_id}}"
namespace="${VELORIX_K8S_NAMESPACE:-velorix-product}"
context="vcluster-docker_${cluster}"
existing_k8s_context="${VELORIX_K8S_CONTEXT:-}"
existing_context_allow_remote="${VELORIX_EXISTING_CONTEXT_ALLOW_REMOTE:-0}"
image_load_mode="${VELORIX_IMAGE_LOAD_MODE:-auto}"
k3d_cluster="${VELORIX_K3D_CLUSTER:-}"
cleanup="${VELORIX_VIND_CLEANUP:-0}"
reuse_existing="${VELORIX_VIND_REUSE_EXISTING:-0}"
vcluster_create_retries="${VELORIX_VCLUSTER_CREATE_RETRIES:-2}"
preserve_state="${VELORIX_VIND_PRESERVE_STATE:-0}"
api_image="${VELORIX_API_IMAGE:-velorix-api:product-${run_id}}"
api_image_digest="${VELORIX_API_IMAGE_DIGEST:-}"
build_api_image="${VELORIX_BUILD_API_IMAGE:-1}"
meta_enabled="${VELORIX_META_ENABLED:-1}"
meta_image="${VELORIX_META_IMAGE:-velorix-meta:product-${run_id}}"
meta_image_digest="${VELORIX_META_IMAGE_DIGEST:-}"
build_meta_image="${VELORIX_BUILD_META_IMAGE:-1}"
meta_mode="${VELORIX_META_MODE:-development}"
meta_backend="${VELORIX_META_BACKEND:-memory}"
hiqlite_deploy="${VELORIX_HIQLITE_DEPLOY:-0}"
managed_persistence="${VELORIX_MANAGED_PERSISTENCE:-0}"
managed_storage_class="${VELORIX_MANAGED_STORAGE_CLASS:-}"
managed_hiqlite_storage_size="${VELORIX_MANAGED_HIQLITE_STORAGE_SIZE:-10Gi}"
managed_rustfs_storage_size="${VELORIX_MANAGED_RUSTFS_STORAGE_SIZE:-10Gi}"
hiqlite_image="${VELORIX_HIQLITE_IMAGE:-velorix-hiqlite:product-${run_id}}"
hiqlite_image_digest="${VELORIX_HIQLITE_IMAGE_DIGEST:-}"
build_hiqlite_image="${VELORIX_BUILD_HIQLITE_IMAGE:-1}"
ingest_writer_image="${VELORIX_INGEST_WRITER_IMAGE:-velorix-ingest-writer:product-${run_id}}"
ingest_writer_image_digest="${VELORIX_INGEST_WRITER_IMAGE_DIGEST:-}"
build_ingest_writer_image="${VELORIX_BUILD_INGEST_WRITER_IMAGE:-1}"
image_pull_secret="${VELORIX_IMAGE_PULL_SECRET:-}"
docker_build_no_cache="${VELORIX_DOCKER_BUILD_NO_CACHE:-0}"
hiqlite_local_source_dir="${VELORIX_HIQLITE_LOCAL_SOURCE_DIR:-${repo_root}/../hiqlite}"
load_existing_images="${VELORIX_LOAD_EXISTING_IMAGES:-0}"
ingest_writer_smoke="${VELORIX_INGEST_WRITER_SMOKE:-1}"
multi_replica_fencing_smoke="${VELORIX_MULTI_REPLICA_FENCING_SMOKE:-1}"
standing_runtime_failover_smoke="${VELORIX_STANDING_RUNTIME_FAILOVER_SMOKE:-auto}"
hiqlite_backend_time_assess="${VELORIX_HIQLITE_BACKEND_TIME_ASSESS:-auto}"
object_store_mode="${VELORIX_OBJECT_STORE_MODE:-rustfs}"
object_store_local_development_authority="${VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY:-0}"
external_s3_validate="${VELORIX_EXTERNAL_S3_VALIDATE:-1}"
s3_force_path_style="${VELORIX_S3_FORCE_PATH_STYLE:-1}"
rustfs_image="${VELORIX_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.4}"
allow_mutable_rustfs_image="${VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE:-0}"
api_local_port="${VELORIX_API_LOCAL_PORT:-8080}"
api_tls_enabled="${VELORIX_API_TLS_ENABLED:-1}"
api_tls_local_port="${VELORIX_API_TLS_LOCAL_PORT:-8443}"
hold_port_forward="${VELORIX_API_HOLD_PORT_FORWARD:-1}"
final_owner_aware_attach="${VELORIX_API_FINAL_OWNER_AWARE_ATTACH:-1}"
product_smoke="${VELORIX_VIND_PRODUCT_SMOKE:-1}"
rest_api_smoke="${VELORIX_VIND_REST_API_SMOKE:-auto}"
product_completion_report="${VELORIX_VIND_PRODUCT_COMPLETION_REPORT:-auto}"
product_evidence_level="${VELORIX_PRODUCT_EVIDENCE_LEVEL:-local-vind-only}"
api_replica_count="${VELORIX_API_REPLICA_COUNT:-1}"
standing_runtime_fencing="${VELORIX_STANDING_RUNTIME_FENCING:-unsafe-dev-only}"
standing_runtime_owner_ttl_ms="${VELORIX_STANDING_RUNTIME_OWNER_TTL_MS:-5000}"
output_compaction_interval_epochs="${VELORIX_OUTPUT_COMPACTION_INTERVAL_EPOCHS:-0}"
output_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
local_disk_preflight="${VELORIX_LOCAL_DISK_PREFLIGHT:-1}"
local_min_free_disk_gib="${VELORIX_LOCAL_MIN_FREE_DISK_GIB:-20}"
bucket="${VELORIX_S3_BUCKET:-velorix-product}"
if [ -n "${VELORIX_S3_PREFIX+x}" ]; then
  s3_prefix="${VELORIX_S3_PREFIX}"
elif [ "$preserve_state" = "1" ]; then
  s3_prefix="product"
else
  s3_prefix="product/${run_id}"
fi
meta_s3_prefix="${VELORIX_META_S3_PREFIX:-${s3_prefix}/meta}"
aws_access_key_id="${AWS_ACCESS_KEY_ID:-}"
aws_secret_access_key="${AWS_SECRET_ACCESS_KEY:-}"
aws_session_token="${AWS_SESSION_TOKEN:-}"
aws_region="${AWS_REGION:-us-east-1}"
aws_endpoint_url="${AWS_ENDPOINT_URL:-}"
s3_credentials_secret_name="${VELORIX_S3_CREDENTIALS_SECRET_NAME:-velorix-s3-credentials}"
s3_credentials_secret_managed="${VELORIX_S3_CREDENTIALS_SECRET_MANAGED:-1}"
s3_credentials_source="supplied"
s3_endpoint=""
s3_authority_store_id=""
s3_backend_label=""
s3_durability_label=""
api_bearer_token="${VELORIX_API_BEARER_TOKEN:-}"
admin_bearer_token="${VELORIX_ADMIN_BEARER_TOKEN:-}"
api_allow_unauthenticated_dev="${VELORIX_API_ALLOW_UNAUTHENTICATED_DEV:-0}"
api_auth_mode="bearer-token"
api_bearer_token_source="generated"
admin_bearer_token_source="generated"
ingress_tls_auth_attestation_file="${VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE:-}"
ingress_tls_auth_endpoint_url="${VELORIX_INGRESS_ENDPOINT_URL:-}"
ingress_tls_auth_external_hostname="${VELORIX_INGRESS_EXTERNAL_HOSTNAME:-}"
ingress_tls_auth_controller="${VELORIX_INGRESS_CONTROLLER:-}"
ingress_tls_auth_auto="${VELORIX_INGRESS_TLS_AUTH_AUTO:-1}"
generated_ingress_tls_auth_attestation="${VELORIX_INGRESS_TLS_AUTH_GENERATED_ATTESTATION:-${output_dir}/ingress-tls-auth-attestation.json}"
ingress_tls_auth_sibling_attestation="${output_dir}/ingress-tls-auth-attestation.json"
ingress_tls_auth_attestation_validated=0
product_ingress_apply="${VELORIX_PRODUCT_INGRESS_APPLY:-0}"
object_store_durability_attestation_file="${VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:-}"
object_store_durability_sibling_attestation="${output_dir}/object-store-durability-attestation.json"
object_store_durability_attestation_validated=0
ingest_writer_lifecycle_attestation_file="${VELORIX_INGEST_WRITER_LIFECYCLE_ATTESTATION_FILE:-}"
ingest_writer_lifecycle_attestation_validated=0
ingest_writer_lifecycle_attestation_source="none"
ingest_writer_lifecycle_generated_by_script=0
ingest_writer_lifecycle_auto="${VELORIX_INGEST_WRITER_LIFECYCLE_AUTO:-1}"
generated_ingest_writer_lifecycle_attestation="${VELORIX_INGEST_WRITER_LIFECYCLE_GENERATED_ATTESTATION:-${output_dir}/ingest-writer-lifecycle-attestation.json}"
no_pvc_namespace_validate="${VELORIX_NO_PVC_NAMESPACE_VALIDATE:-1}"
no_pvc_namespace_validated=0
api_auth_observed_readyz_mode=""
api_auth_missing_token_rejected=0
api_auth_wrong_token_rejected=0
api_auth_correct_token_smoke_passed=0
api_auth_data_plane_token_rejected_on_admin_route=0
api_healthz_unauthenticated=0
api_readyz_unauthenticated=0
api_deployment_env_verified=0
api_deployment_observed_file=""
meta_deployment_observed_file=""
api_tls_auth_smoke_passed=0
api_tls_certificate_sha256=""
api_tls_evidence_file=""
api_final_rest_attach_evidence_file=""
rest_api_smoke_status="not_run"
rest_api_smoke_evidence_file=""
product_completion_report_status="not_run"
product_completion_report_file=""
api_query_policy_smoke_passed=0
api_query_policy_missing_policy_rejected=0
api_query_policy_weak_policy_rejected=0
api_openapi_catalog_smoke_passed=0
object_store_namespace_count=0
object_store_artifact_catalog_conditional_update=0
external_s3_bucket_validated=0
external_s3_prefix_validated=0
external_s3_validation_prefix=""
external_s3_validation_key=""
ingest_writer_job_completed=0
ingest_writer_append_outcome=""
ingest_writer_object_key=""
ingest_writer_job_name="velorix-ingest-writer-smoke"
meta_fencing_adversarial_smoke_passed=0
meta_fencing_adversarial_smoke_log=""
meta_smoke_invocation=0
multi_replica_fencing_smoke_passed=0
multi_replica_fencing_smoke_evidence_file=""
standing_runtime_failover_smoke_passed=0
standing_runtime_failover_smoke_evidence_file=""
hiqlite_backend_time_assessment_file="${VELORIX_HIQLITE_BACKEND_TIME_ASSESSMENT_PATH:-${output_dir}/hiqlite-backend-time-assessment.json}"
hiqlite_backend_time_assessment_validated=0
hiqlite_backend_time_attest="${VELORIX_HIQLITE_BACKEND_TIME_ATTEST:-auto}"
hiqlite_backend_time_attestation_file="${output_dir}/hiqlite-backend-time-attestation.json"
hiqlite_backend_time_attestation_validated=0
meta_bearer_token="${VELORIX_META_BEARER_TOKEN:-}"
hiqlite_nodes="${VELORIX_HIQLITE_NODES:-}"
hiqlite_api_secret="${VELORIX_HIQLITE_API_SECRET:-}"
hiqlite_raft_secret="${VELORIX_HIQLITE_RAFT_SECRET:-}"
hiqlite_enc_key_active="${VELORIX_HIQLITE_ENC_KEY_ACTIVE:-}"
hiqlite_enc_keys="${VELORIX_HIQLITE_ENC_KEYS:-}"
hiqlite_backup_cron="${VELORIX_HIQLITE_BACKUP_CRON:-0 30 2 * * * *}"
hiqlite_backup_keep_days="${VELORIX_HIQLITE_BACKUP_KEEP_DAYS:-30}"
hiqlite_backup_keep_days_local="${VELORIX_HIQLITE_BACKUP_KEEP_DAYS_LOCAL:-3}"
hiqlite_with_proxy="${VELORIX_HIQLITE_WITH_PROXY:-0}"
hiqlite_authority_attestation_file="${VELORIX_HIQLITE_AUTHORITY_ATTESTATION_FILE:-}"
generated_hiqlite_authority_attestation="${VELORIX_HIQLITE_AUTHORITY_GENERATED_ATTESTATION:-${output_dir}/hiqlite-authority-attestation.json}"
hiqlite_authority_sibling_attestation="${output_dir}/hiqlite-authority-attestation.json"
hiqlite_authority_attestation_validated=0
created_cluster=0
created_namespace=0
previous_context=""
port_forward_pid=""
api_tls_port_forward_pid=""

case "$cluster_driver" in
  docker-vcluster) ;;
  existing-context)
    if [ -z "$existing_k8s_context" ]; then
      existing_k8s_context="$(kubectl config current-context 2>/dev/null || true)"
    fi
    if [ -z "$existing_k8s_context" ]; then
      echo "VELORIX_VIND_CLUSTER_DRIVER=existing-context requires VELORIX_K8S_CONTEXT or a current kubectl context" >&2
      exit 64
    fi
    context="$existing_k8s_context"
    cluster="${VELORIX_VIND_CLUSTER:-${context}}"
    product_deployment_id="${VELORIX_PRODUCT_DEPLOYMENT_ID:-${cluster}/${run_id}}"
    ;;
  *)
    echo "VELORIX_VIND_CLUSTER_DRIVER must be docker-vcluster or existing-context" >&2
    exit 64
    ;;
esac

usage() {
  cat <<'EOF'
Deploy the runnable Velorix product slice to vind/vCluster.

Usage:
  scripts/run-vind-product.sh

After it finishes, call the REST API through the local port-forward:
  source target/velorix-product/api-auth.env
  curl "$VELORIX_API_URL/healthz"
  curl -X POST "$VELORIX_API_URL/v1/relations/scores-default" -H "$VELORIX_API_AUTH_HEADER"
  curl -X POST "$VELORIX_API_URL/v1/views" -H "$VELORIX_API_AUTH_HEADER" -H 'content-type: application/json' -d '{"view_id":"positive_scores_by_user","urlPath":"/scores/positive","input_relation_id":"scores","input_relation_version":"2026-05-24.v1","sql":"select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id","response_formats":["json"]}'
  curl -X POST "$VELORIX_API_URL/v1/relations/scores/ingest" -H "$VELORIX_API_AUTH_HEADER" -H 'content-type: application/json' -d '{"relation_version":"2026-05-24.v1","stream_id":"scores","partition_id":0,"start_offset_inclusive":0,"rows":[{"user_id":"u1","score":5,"delta":1},{"user_id":"u1","score":7,"delta":1},{"user_id":"u2","score":-1,"delta":1}]}'
  curl "$VELORIX_API_URL/v1/views/positive_scores_by_user/query" -H "$VELORIX_API_AUTH_HEADER"

Main overrides:
  VELORIX_VIND_CLUSTER=velorix-product
  VELORIX_VIND_CLUSTER_DRIVER=docker-vcluster
  VELORIX_K8S_CONTEXT=<required when VELORIX_VIND_CLUSTER_DRIVER=existing-context>
  VELORIX_IMAGE_LOAD_MODE=auto  # auto, vcluster-docker, k3d, kind, none
  VELORIX_PRODUCT_DEPLOYMENT_ID=<defaults to VELORIX_VIND_CLUSTER/current-run-id>
  VELORIX_VIND_REUSE_EXISTING=1
  VELORIX_VIND_PRESERVE_STATE=0
  VELORIX_VIND_CLEANUP=1
  VELORIX_API_IMAGE=velorix-api:product
  VELORIX_BUILD_API_IMAGE=1
  VELORIX_META_ENABLED=1
  VELORIX_META_IMAGE=velorix-meta:product
  VELORIX_BUILD_META_IMAGE=1
  VELORIX_META_MODE=development
  VELORIX_META_BACKEND=memory
  VELORIX_HIQLITE_DEPLOY=0  # set 1 with VELORIX_META_BACKEND=hiqlite to deploy a no-PVC 3-voter authority
  VELORIX_MANAGED_PERSISTENCE=0  # set 1 only for an existing cluster with provisioned PVC storage
  VELORIX_MANAGED_STORAGE_CLASS=<optional existing StorageClass>
  VELORIX_MANAGED_HIQLITE_STORAGE_SIZE=10Gi
  VELORIX_MANAGED_RUSTFS_STORAGE_SIZE=10Gi
  VELORIX_HIQLITE_IMAGE=velorix-hiqlite:product
  VELORIX_HIQLITE_IMAGE_DIGEST=<optional sha256 digest for managed hiqlite attestation>
  VELORIX_BUILD_HIQLITE_IMAGE=1
  VELORIX_HIQLITE_AUTHORITY_ATTESTATION_FILE=<optional JSON for external hiqlite authority evidence>
  VELORIX_INGEST_WRITER_IMAGE=velorix-ingest-writer:product
  VELORIX_INGEST_WRITER_IMAGE_DIGEST=<required sha256 digest when externally pulling the ingest-writer image>
  VELORIX_BUILD_INGEST_WRITER_IMAGE=1
  VELORIX_LOAD_EXISTING_IMAGES=0  # set 1 with BUILD_*_IMAGE=0 to load local images into the selected product cluster
  VELORIX_IMAGE_PULL_SECRET=<optional namespace-scoped imagePullSecret for immutable external images>
  VELORIX_INGEST_WRITER_SMOKE=1
  VELORIX_OBJECT_STORE_MODE=rustfs  # or external-s3
  VELORIX_EXTERNAL_S3_VALIDATE=1
  VELORIX_META_BEARER_TOKEN=<generated when unset>
  VELORIX_RUSTFS_IMAGE=rustfs/rustfs:1.0.0-beta.4
  VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE=0
  VELORIX_MULTI_REPLICA_FENCING_SMOKE=1
  AWS_ENDPOINT_URL=<required when VELORIX_OBJECT_STORE_MODE=external-s3>
  AWS_ACCESS_KEY_ID=<required with AWS_SECRET_ACCESS_KEY for external-s3; unset both for local RustFS generation>
  AWS_SECRET_ACCESS_KEY=<required with AWS_ACCESS_KEY_ID for external-s3; unset both for local RustFS generation>
  VELORIX_API_LOCAL_PORT=18080
  VELORIX_API_TLS_ENABLED=1
  VELORIX_API_TLS_LOCAL_PORT=18443
  VELORIX_API_HOLD_PORT_FORWARD=0
  VELORIX_API_FINAL_OWNER_AWARE_ATTACH=1
  VELORIX_VIND_PRODUCT_SMOKE=1
  VELORIX_PRODUCT_EVIDENCE_LEVEL=local-vind-only
  VELORIX_LOCAL_DISK_PREFLIGHT=1
  VELORIX_LOCAL_MIN_FREE_DISK_GIB=20
  VELORIX_API_BEARER_TOKEN=<unset means generate a random local bearer token>
  VELORIX_ADMIN_BEARER_TOKEN=<unset means generate a distinct random local admin token>
  VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE=<optional JSON for product ingress/TLS/auth evidence>
  VELORIX_INGRESS_ENDPOINT_URL=<optional external https endpoint; auto-generates ingress/TLS/auth evidence when set>
  VELORIX_INGRESS_CONTROLLER=<required with VELORIX_INGRESS_ENDPOINT_URL>
  VELORIX_INGRESS_EXTERNAL_HOSTNAME=<optional hostname override for generated ingress/TLS/auth evidence>
  VELORIX_INGRESS_TLS_AUTH_AUTO=1
  VELORIX_PRODUCT_INGRESS_APPLY=0  # set 1 to apply a networking.k8s.io/v1 Ingress for velorix-api
  VELORIX_PRODUCT_INGRESS_HOST=<required when VELORIX_PRODUCT_INGRESS_APPLY=1>
  VELORIX_PRODUCT_INGRESS_CLASS=<required when VELORIX_PRODUCT_INGRESS_APPLY=1>
  VELORIX_PRODUCT_INGRESS_TLS_SECRET=<required when VELORIX_PRODUCT_INGRESS_APPLY=1>
  VELORIX_INGEST_WRITER_LIFECYCLE_ATTESTATION_FILE=<optional JSON for deployed ingest-writer lifecycle evidence>
  VELORIX_INGEST_WRITER_LIFECYCLE_AUTO=1
  VELORIX_NO_PVC_NAMESPACE_VALIDATE=1
  VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1  # explicit local dev opt-out only
  VELORIX_API_REPLICA_COUNT=1
  VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only  # or logical-fencing / required
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

require curl
require docker
require kubectl
require python3
require vcluster

preflight_docker_daemon() {
  local output
  local context_name
  context_name="$(docker context show 2>/dev/null || true)"
  if output="$(docker info 2>&1 >/dev/null)"; then
    return 0
  fi

  echo "docker daemon is not reachable; cannot run the vind product deployment" >&2
  if [ -n "$context_name" ]; then
    echo "docker_context=${context_name}" >&2
  fi
  echo "$output" >&2
  if command -v colima >/dev/null 2>&1; then
    echo "colima status:" >&2
    colima status >&2 || true
    echo "If this context is Colima-backed, repair or start Colima before rerunning scripts/run-vind-product.sh." >&2
  else
    echo "Start or repair Docker before rerunning scripts/run-vind-product.sh." >&2
  fi
  exit 1
}

write_local_disk_capacity_blocker() {
  local blocker_json="${output_dir}/local-environment-blocker.json"
  local df_file="${output_dir}/local-host-df.txt"
  df -Pk "$repo_root" >"$df_file"
  python3 - \
    "$blocker_json" \
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
        "free local Docker/Colima/vCluster capacity before running the product slice",
        "if appropriate, delete Docker build cache with scripts/doctor-vind-local.sh --prune-build-cache --yes",
        "increase local Docker or host disk capacity",
        "or rerun with VELORIX_LOCAL_MIN_FREE_DISK_GIB=<lower bound> when you intentionally accept the risk",
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
      echo "VELORIX_LOCAL_MIN_FREE_DISK_GIB must be a non-negative integer" >&2
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

base64_value() {
  python3 -c 'import base64, sys; print(base64.b64encode(sys.stdin.buffer.read().rstrip(b"\n")).decode("ascii"))' <<<"$1"
}

base64_file() {
  python3 - "$1" <<'PY'
import base64
import sys

with open(sys.argv[1], "rb") as f:
    print(base64.b64encode(f.read()).decode("ascii"))
PY
}

ingest_writer_run_offset_base() {
  python3 - "$run_id" <<'PY'
import hashlib
import sys

run_id = sys.argv[1].encode("utf-8")
offset = 1_000_000_000 + (int(hashlib.sha256(run_id).hexdigest()[:12], 16) * 10)
print(offset)
PY
}

sha256_value() {
  python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.stdin.buffer.read().rstrip(b"\n")).hexdigest())' <<<"$1"
}

random_token() {
  python3 - <<'PY'
import secrets

print(secrets.token_urlsafe(32))
PY
}

random_alnum() {
  python3 - <<'PY'
import secrets
import string

alphabet = string.ascii_letters + string.digits
print("".join(secrets.choice(alphabet) for _ in range(12)))
PY
}

random_base64_key() {
  python3 - <<'PY'
import base64
import secrets

print(base64.b64encode(secrets.token_bytes(32)).decode("ascii"))
PY
}

validate_hiqlite_enc_keys() {
  local active="$1"
  local keys="$2"
  python3 - "$active" "$keys" <<'PY'
import base64
import re
import sys

active = sys.argv[1]
raw_keys = sys.argv[2]
key_ids = []
for line in raw_keys.splitlines():
    line = line.strip()
    if not line:
        continue
    if "/" not in line:
        continue
    key_id, encoded = line.split("/", 1)
    key_id = key_id.strip()
    encoded = encoded.strip()
    if not key_id or not encoded:
        raise SystemExit("VELORIX_HIQLITE_ENC_KEYS entries must use <id>/<base64-key>")
    if not re.fullmatch(r"[a-zA-Z0-9:_-]{2,20}", key_id):
        raise SystemExit("VELORIX_HIQLITE_ENC_KEYS key IDs must match ^[a-zA-Z0-9:_-]{2,20}$")
    try:
        key = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise SystemExit(f"VELORIX_HIQLITE_ENC_KEYS key {key_id} is not valid base64: {exc}")
    if len(key) != 32:
        raise SystemExit(
            f"VELORIX_HIQLITE_ENC_KEYS key {key_id} must decode to exactly 32 bytes, got {len(key)}"
        )
    key_ids.append(key_id)

if not key_ids:
    raise SystemExit("VELORIX_HIQLITE_ENC_KEYS must contain at least one <id>/<base64-key> entry")
if active not in key_ids:
    raise SystemExit("VELORIX_HIQLITE_ENC_KEY_ACTIVE must match one VELORIX_HIQLITE_ENC_KEYS key ID")
PY
}

validate_hiqlite_nodes_for_remote_client() {
  local nodes="$1"
  if [[ "$nodes" == *"://"* ]]; then
    echo "VELORIX_HIQLITE_NODES must use Hiqlite remote client addresses without URL schemes, for example host:8200,host2:8200" >&2
    exit 64
  fi
}

sha256_file() {
  python3 - "$1" <<'PY'
import hashlib
import sys

with open(sys.argv[1], "rb") as f:
    print("sha256:" + hashlib.sha256(f.read()).hexdigest())
PY
}

validate_hiqlite_authority_attestation() {
  if [ -z "$hiqlite_authority_attestation_file" ]; then
    return 0
  fi
  if [ "$meta_backend" != "hiqlite" ]; then
    echo "VELORIX_HIQLITE_AUTHORITY_ATTESTATION_FILE requires VELORIX_META_BACKEND=hiqlite" >&2
    exit 64
  fi
  if ! python3 - "$hiqlite_authority_attestation_file" "$hiqlite_nodes" <<'PY'
import json
import sys
from datetime import datetime

path, configured_nodes = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    attestation = json.load(f)

errors = []
if attestation.get("schema_version") != 1:
    errors.append("schema_version must be 1")
if attestation.get("authority_kind") not in {"external_hiqlite", "velorix_managed_hiqlite"}:
    errors.append("authority_kind must be external_hiqlite or velorix_managed_hiqlite")
nodes = attestation.get("nodes")
if not isinstance(nodes, list) or not nodes or not all(isinstance(node, str) and node for node in nodes):
    errors.append("nodes must be a nonempty string array")
else:
    if len(nodes) != 3:
        errors.append("nodes must contain exactly three Hiqlite voter endpoints")
    if len(set(nodes)) != len(nodes):
        errors.append("nodes must contain unique Hiqlite voter endpoints")
    expected_nodes = [node for node in configured_nodes.split(",") if node]
    if expected_nodes and sorted(nodes) != sorted(expected_nodes):
        errors.append("nodes must match VELORIX_HIQLITE_NODES")
if attestation.get("expected_voter_count") != 3:
    errors.append("expected_voter_count must be 3")
for field in [
    "no_pvc_created_by_vind",
    "metadata_authority_no_pvc_used",
    "voters_learner_only_disabled",
    "api_auth_configured",
    "raft_auth_configured",
    "backup_restore_configured",
]:
    if attestation.get(field) is not True:
        errors.append(f"{field} must be true")
if attestation.get("metadata_authority_storage_mode") != "object-store-backup-restore-with-ephemeral-node-disk":
    errors.append("metadata_authority_storage_mode must be object-store-backup-restore-with-ephemeral-node-disk")
transport_security = attestation.get("transport_security")
if not isinstance(transport_security, str) or transport_security.strip().lower() in {"", "none", "plaintext", "local-only", "generated-local-self-signed"}:
    errors.append("transport_security must describe non-local TLS, service mesh, or equivalent boundary")
image_digest = attestation.get("image_digest")
if not (image_digest or attestation.get("source_revision")):
    errors.append("image_digest or source_revision is required")
if image_digest and (not isinstance(image_digest, str) or not image_digest.startswith("sha256:")):
    errors.append("image_digest must be a sha256 digest")
if attestation.get("authority_kind") == "velorix_managed_hiqlite" and not image_digest:
    errors.append("managed Hiqlite authority requires image_digest")
attested_at = attestation.get("attested_at")
if not isinstance(attested_at, str):
    errors.append("attested_at must be an RFC3339 UTC timestamp")
else:
    try:
        if not attested_at.endswith("Z"):
            raise ValueError("timestamp must use UTC Z suffix")
        datetime.fromisoformat(attested_at.replace("Z", "+00:00"))
    except ValueError:
        errors.append("attested_at must be an RFC3339 UTC timestamp")
if errors:
    raise SystemExit(
        "invalid VELORIX_HIQLITE_AUTHORITY_ATTESTATION_FILE:\n- " + "\n- ".join(errors)
    )
PY
  then
    exit 64
  fi
  mkdir -p "$output_dir"
  if [ "$hiqlite_authority_attestation_file" != "$hiqlite_authority_sibling_attestation" ]; then
    cp "$hiqlite_authority_attestation_file" "$hiqlite_authority_sibling_attestation"
    chmod 600 "$hiqlite_authority_sibling_attestation"
    hiqlite_authority_attestation_file="$hiqlite_authority_sibling_attestation"
  fi
  hiqlite_authority_attestation_validated=1
}

validate_ingress_tls_auth_attestation() {
  if [ -z "$ingress_tls_auth_attestation_file" ]; then
    return 0
  fi
  if [ "$api_auth_mode" != "bearer-token" ]; then
    echo "VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE requires bearer-token API auth" >&2
    exit 64
  fi
  if ! python3 - "$ingress_tls_auth_attestation_file" <<'PY'
import json
import ipaddress
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    attestation = json.load(f)

errors = []
if attestation.get("schema_version") != 1:
    errors.append("schema_version must be 1")
if attestation.get("evidence_kind") != "velorix_ingress_tls_auth_attestation":
    errors.append("evidence_kind must be velorix_ingress_tls_auth_attestation")
for field in ["endpoint_url", "ingress_controller", "external_hostname", "transport_security"]:
    if not isinstance(attestation.get(field), str) or not attestation[field].strip():
        errors.append(f"{field} must be a nonempty string")
endpoint = urlparse(attestation.get("endpoint_url", ""))
if endpoint.scheme != "https" or not endpoint.hostname:
    errors.append("endpoint_url must be an https URL with a hostname")
else:
    host = endpoint.hostname.lower()
    if host in {"localhost"} or host.endswith(".svc") or host.endswith(".svc.cluster.local"):
        errors.append("endpoint_url must not point at localhost or Kubernetes service DNS")
    try:
        if ipaddress.ip_address(host).is_loopback:
            errors.append("endpoint_url must not point at a loopback IP")
    except ValueError:
        pass
external_hostname = str(attestation.get("external_hostname", "")).lower()
if external_hostname in {"localhost"} or external_hostname.endswith(".svc") or external_hostname.endswith(".svc.cluster.local"):
    errors.append("external_hostname must not be localhost or Kubernetes service DNS")
transport_security = str(attestation.get("transport_security", "")).lower()
if any(marker in transport_security for marker in ["self-signed", "generated-local", "local-only"]):
    errors.append("transport_security must describe an external/public or enterprise TLS boundary, not local self-signed smoke")
issuer = str(attestation.get("tls_certificate_issuer", "")).lower()
if any(marker in issuer for marker in ["self-signed", "generated-local", "velorix-api.local"]):
    errors.append("tls_certificate_issuer must not describe the generated local smoke certificate")
for field in [
    "public_ingress_attestation",
    "trusted_for_product_complete",
    "tls_enabled",
    "auth_enforced",
    "missing_token_rejected",
    "wrong_token_rejected",
    "admin_auth_separate",
    "admin_route_missing_token_rejected",
    "admin_route_wrong_token_rejected",
    "admin_token_accepted_on_admin_route",
    "data_plane_token_rejected_on_admin_route",
]:
    if attestation.get(field) is not True:
        errors.append(f"{field} must be true")
if attestation.get("tls_enabled") is True and not (
    attestation.get("tls_certificate_sha256") or attestation.get("tls_certificate_issuer")
):
    errors.append("tls_certificate_sha256 or tls_certificate_issuer is required when tls_enabled=true")
attested_at_raw = attestation.get("attested_at")
if not (attested_at_raw and attestation.get("attester")):
    errors.append("attested_at and attester are required")
else:
    try:
        attested_at = datetime.fromisoformat(str(attested_at_raw).replace("Z", "+00:00"))
        if attested_at.tzinfo is None:
            errors.append("attested_at must include timezone")
        else:
            now = datetime.now(timezone.utc)
            if attested_at > now + timedelta(minutes=15):
                errors.append("attested_at must not be more than 15 minutes in the future")
            if now - attested_at > timedelta(hours=24):
                errors.append("attested_at must be no older than 24 hours")
    except ValueError:
        errors.append("attested_at must be RFC3339")
if errors:
    raise SystemExit(
        "invalid VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE:\n- " + "\n- ".join(errors)
    )
PY
  then
    exit 64
  fi
  mkdir -p "$output_dir"
  if [ "$ingress_tls_auth_attestation_file" != "$ingress_tls_auth_sibling_attestation" ]; then
    cp "$ingress_tls_auth_attestation_file" "$ingress_tls_auth_sibling_attestation"
    chmod 600 "$ingress_tls_auth_sibling_attestation"
    ingress_tls_auth_attestation_file="$ingress_tls_auth_sibling_attestation"
  fi
  ingress_tls_auth_attestation_validated=1
}

validate_object_store_durability_attestation() {
  if [ -z "$object_store_durability_attestation_file" ]; then
    return 0
  fi
  if [ "$object_store_local_development_authority" = "1" ]; then
    echo "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE cannot be used with VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1" >&2
    exit 64
  fi
  if [ "$object_store_mode" != "external-s3" ]; then
    echo "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE requires VELORIX_OBJECT_STORE_MODE=external-s3" >&2
    exit 64
  fi
  if ! python3 - \
    "$object_store_durability_attestation_file" \
    "$s3_authority_store_id" \
    "$bucket" \
    "$s3_prefix" <<'PY'
import json
import sys
from datetime import datetime

path, authority_store_id, bucket, s3_prefix = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    attestation = json.load(f)

errors = []
if attestation.get("schema_version") != 1:
    errors.append("schema_version must be 1")
if attestation.get("evidence_kind") != "velorix_object_store_durability_policy_attestation":
    errors.append("evidence_kind must be velorix_object_store_durability_policy_attestation")
for field in ["provider_kind", "authority_store_id", "bucket", "s3_prefix", "attested_at", "attester"]:
    if not isinstance(attestation.get(field), str) or not attestation[field].strip() and field != "s3_prefix":
        errors.append(f"{field} must be a string")
if attestation.get("authority_store_id") != authority_store_id:
    errors.append("authority_store_id must match the current object-store authority")
if attestation.get("bucket") != bucket:
    errors.append("bucket must match the current object-store bucket")
if attestation.get("s3_prefix") != s3_prefix:
    errors.append("s3_prefix must match the current object-store prefix")
for field in [
    "versioning_or_object_lock_enabled",
    "server_side_encryption_enabled",
    "backup_or_replication_configured",
    "lifecycle_delete_policy_reviewed",
    "destructive_delete_protection_reviewed",
    "cost_controls_reviewed",
]:
    if attestation.get(field) is not True:
        errors.append(f"{field} must be true")
try:
    parsed = datetime.fromisoformat(str(attestation.get("attested_at", "")).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        errors.append("attested_at must include timezone")
except ValueError:
    errors.append("attested_at must be RFC3339")
if errors:
    raise SystemExit(
        "invalid VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE:\n- " + "\n- ".join(errors)
    )
PY
  then
    exit 64
  fi
  mkdir -p "$output_dir"
  if [ "$object_store_durability_attestation_file" != "$object_store_durability_sibling_attestation" ]; then
    cp "$object_store_durability_attestation_file" "$object_store_durability_sibling_attestation"
    chmod 600 "$object_store_durability_sibling_attestation"
    object_store_durability_attestation_file="$object_store_durability_sibling_attestation"
  fi
  object_store_durability_attestation_validated=1
}

generate_ingress_tls_auth_attestation() {
  if [ "$ingress_tls_auth_auto" != "1" ]; then
    return 0
  fi
  if [ -n "$ingress_tls_auth_attestation_file" ]; then
    return 0
  fi
  if [ -z "$ingress_tls_auth_endpoint_url" ]; then
    return 0
  fi
  if [ "$api_auth_mode" != "bearer-token" ]; then
    echo "VELORIX_INGRESS_ENDPOINT_URL requires bearer-token API auth" >&2
    exit 64
  fi
  if [ -z "$ingress_tls_auth_controller" ]; then
    echo "VELORIX_INGRESS_CONTROLLER is required when VELORIX_INGRESS_ENDPOINT_URL is set" >&2
    exit 64
  fi

  echo "generating ingress/TLS/auth attestation from ${ingress_tls_auth_endpoint_url}"
  VELORIX_INGRESS_ENDPOINT_URL="$ingress_tls_auth_endpoint_url" \
    VELORIX_API_BEARER_TOKEN="$api_bearer_token" \
    VELORIX_ADMIN_BEARER_TOKEN="$admin_bearer_token" \
    VELORIX_INGRESS_CONTROLLER="$ingress_tls_auth_controller" \
    VELORIX_INGRESS_EXTERNAL_HOSTNAME="$ingress_tls_auth_external_hostname" \
    VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE="$generated_ingress_tls_auth_attestation" \
    "$repo_root/scripts/attest-ingress-tls-auth.sh"
  ingress_tls_auth_attestation_file="$generated_ingress_tls_auth_attestation"
  validate_ingress_tls_auth_attestation
}

validate_ingest_writer_lifecycle_attestation() {
  if [ -z "$ingest_writer_lifecycle_attestation_file" ]; then
    return 0
  fi
  if ! python3 - \
    "$ingest_writer_lifecycle_attestation_file" \
    "$product_deployment_id" \
    "$s3_authority_store_id" <<'PY'
import json
import sys
from datetime import datetime, timezone, timedelta

path, expected_deployment_id, expected_authority_store_id = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    attestation = json.load(f)

errors = []
if not expected_deployment_id.strip():
    errors.append("VELORIX_PRODUCT_DEPLOYMENT_ID must be a nonempty string")
if not expected_authority_store_id.strip():
    errors.append("current authority_store_id must be a nonempty string")
if attestation.get("schema_version") != 1:
    errors.append("schema_version must be 1")
if attestation.get("evidence_kind") != "velorix_ingest_writer_lifecycle_attestation":
    errors.append("evidence_kind must be velorix_ingest_writer_lifecycle_attestation")
for field in ["deployment_id", "authority_store_id", "deployed_topology"]:
    if not isinstance(attestation.get(field), str) or not attestation[field].strip():
        errors.append(f"{field} must be a nonempty string")
if attestation.get("deployment_id") != expected_deployment_id:
    errors.append(
        "deployment_id must match current VELORIX_PRODUCT_DEPLOYMENT_ID "
        f"({expected_deployment_id})"
    )
if attestation.get("authority_store_id") != expected_authority_store_id:
    errors.append(
        "authority_store_id must match current object_store.authority_store_id "
        f"({expected_authority_store_id})"
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
    if attestation.get(field) is not True:
        errors.append(f"{field} must be true")
topology = str(attestation.get("deployed_topology", ""))
if topology not in {"kubernetes_jobs", "kubernetes_operator", "replicated_controller"}:
    errors.append("deployed_topology must be kubernetes_jobs, kubernetes_operator, or replicated_controller")
if attestation.get("leader_handoff_checked") is True and topology == "kubernetes_jobs":
    errors.append("kubernetes_jobs attestation must not claim leader_handoff_checked=true")
provenance = attestation.get("evidence_provenance")
if not isinstance(provenance, dict):
    errors.append("evidence_provenance must be an object")
else:
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
        item = provenance.get(key)
        if not isinstance(item, dict):
            errors.append(f"evidence_provenance.{key} must be an object")
            continue
        for field in [
            "job_uid",
            "pod_uid",
            "pod_name",
            "container_image",
            "container_image_id",
        ]:
            if not isinstance(item.get(field), str) or not item[field].strip():
                errors.append(f"evidence_provenance.{key}.{field} must be a nonempty string")
evidence_files = attestation.get("evidence_files")
if not isinstance(evidence_files, dict):
    errors.append("evidence_files must be an object")
else:
    for key, expected in {
        "pod_internal_job": "velorix-ingest-writer-smoke-log.json",
        "overlap_job": "velorix-ingest-lifecycle-overlap-log.json",
        "adjacent_job": "velorix-ingest-lifecycle-adjacent-log.json",
        "restart_job": "velorix-ingest-lifecycle-restart-log.json",
        "lease_loss_job": "velorix-ingest-lifecycle-lease-loss-log.json",
        "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json",
    }.items():
        if evidence_files.get(key) != expected:
            errors.append(f"evidence_files.{key} must be {expected}")
attested_at_raw = attestation.get("attested_at")
if not (attested_at_raw and attestation.get("attester")):
    errors.append("attested_at and attester are required")
else:
    try:
        attested_at = datetime.fromisoformat(str(attested_at_raw).replace("Z", "+00:00"))
        if attested_at.tzinfo is None:
            errors.append("attested_at must include timezone")
        else:
            now = datetime.now(timezone.utc)
            if attested_at > now + timedelta(minutes=15):
                errors.append("attested_at must not be more than 15 minutes in the future")
            if now - attested_at > timedelta(hours=24):
                errors.append("attested_at must be no older than 24 hours")
    except ValueError:
        errors.append("attested_at must be RFC3339")
if errors:
    raise SystemExit(
        "invalid VELORIX_INGEST_WRITER_LIFECYCLE_ATTESTATION_FILE:\n- "
        + "\n- ".join(errors)
    )
PY
  then
    exit 64
  fi
  ingest_writer_lifecycle_attestation_validated=1
  if [ "$ingest_writer_lifecycle_generated_by_script" = "1" ]; then
    ingest_writer_lifecycle_attestation_source="generated"
  else
    ingest_writer_lifecycle_attestation_source="external"
  fi
}

normalized_meta_backend() {
  case "$1" in
    memory | in-memory) printf '%s\n' "in-memory" ;;
    oss | object-store) printf '%s\n' "oss" ;;
    hiqlite) printf '%s\n' "hiqlite" ;;
    *) return 1 ;;
  esac
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

immutable_image_reference() {
  local image="$1"
  local digest="$2"
  if [ -z "$digest" ] && [[ "$image" == *@sha256:* ]]; then
    digest="${image##*@}"
  fi
  if [[ ! "$digest" =~ ^sha256:[0-9a-fA-F]{64}$ ]]; then
    echo "external image digest must be sha256:<64 hex characters>" >&2
    exit 64
  fi
  if [[ "$image" == *@sha256:* ]]; then
    if [[ "$image" != *"@${digest}" ]]; then
      echo "external image reference digest must match its supplied digest" >&2
      exit 64
    fi
    printf '%s\n' "$image"
  else
    printf '%s@%s\n' "$image" "$digest"
  fi
}

image_pull_secrets_yaml() {
  if [ -n "$image_pull_secret" ]; then
    printf '%s\n' "      imagePullSecrets:" "        - name: ${image_pull_secret}"
  fi
}

service_account_image_pull_secrets_yaml() {
  if [ -n "$image_pull_secret" ]; then
    printf '%s\n' "imagePullSecrets:" "  - name: ${image_pull_secret}"
  fi
}

managed_storage_class_yaml() {
  if [ -n "$managed_storage_class" ]; then
    printf '%s\n' "      storageClassName: ${managed_storage_class}"
  fi
}

hiqlite_data_volume_claim_templates_yaml() {
  if [ "$managed_persistence" = "1" ]; then
    printf '%s\n' \
      '  volumeClaimTemplates:' \
      '    - metadata:' \
      '        name: data' \
      '      spec:' \
      '        accessModes: ["ReadWriteOnce"]' \
      '        resources:' \
      '          requests:' \
      "            storage: ${managed_hiqlite_storage_size}"
    if [ -n "$managed_storage_class" ]; then
      printf '%s\n' "        storageClassName: ${managed_storage_class}"
    fi
  fi
}

hiqlite_data_volume_yaml() {
  if [ "$managed_persistence" = "0" ]; then
    printf '%s\n' '        - name: data' '          emptyDir: {}'
  fi
}

rustfs_pvc_yaml() {
  if [ "$managed_persistence" = "1" ]; then
    printf '%s\n' \
      'apiVersion: v1' \
      'kind: PersistentVolumeClaim' \
      'metadata:' \
      '  name: velorix-rustfs-data' \
      "  namespace: ${namespace}" \
      'spec:' \
      '  accessModes: ["ReadWriteOnce"]' \
      '  resources:' \
      '    requests:' \
      "      storage: ${managed_rustfs_storage_size}"
    if [ -n "$managed_storage_class" ]; then
      printf '%s\n' "  storageClassName: ${managed_storage_class}"
    fi
    printf '%s\n' '---'
  fi
}

rustfs_data_volume_yaml() {
  if [ "$managed_persistence" = "1" ]; then
    printf '%s\n' '        - name: data' '          persistentVolumeClaim:' '            claimName: velorix-rustfs-data'
  else
    printf '%s\n' '        - name: data' '          emptyDir: {}'
  fi
}

rustfs_strategy_yaml() {
  if [ "$managed_persistence" = "1" ]; then
    printf '%s\n' '  strategy:' '    type: Recreate'
  fi
}

case "$meta_enabled" in
  0 | 1) ;;
  *)
    echo "VELORIX_META_ENABLED must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$build_api_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_BUILD_API_IMAGE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$build_meta_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_BUILD_META_IMAGE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$build_ingest_writer_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_BUILD_INGEST_WRITER_IMAGE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$docker_build_no_cache" in
  0 | 1) ;;
  *)
    echo "VELORIX_DOCKER_BUILD_NO_CACHE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$load_existing_images" in
  0 | 1) ;;
  *)
    echo "VELORIX_LOAD_EXISTING_IMAGES must be 0 or 1" >&2
    exit 64
    ;;
esac

if [ -n "$image_pull_secret" ] && [[ ! "$image_pull_secret" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "VELORIX_IMAGE_PULL_SECRET must be a lowercase DNS label" >&2
  exit 64
fi

api_image_pull_policy="Never"
meta_image_pull_policy="Never"
hiqlite_image_pull_policy="Never"
ingest_writer_image_pull_policy="Never"
if [ "$image_load_mode" = "none" ]; then
  if [ "$build_api_image" = "0" ]; then
    if [ -z "$api_image_digest" ] && [[ "$api_image" == *@sha256:* ]]; then
      api_image_digest="${api_image##*@}"
    fi
    api_image="$(immutable_image_reference "$api_image" "$api_image_digest")"
    api_image_pull_policy="IfNotPresent"
  fi
  if [ "$meta_enabled" = "1" ] && [ "$build_meta_image" = "0" ]; then
    if [ -z "$meta_image_digest" ] && [[ "$meta_image" == *@sha256:* ]]; then
      meta_image_digest="${meta_image##*@}"
    fi
    meta_image="$(immutable_image_reference "$meta_image" "$meta_image_digest")"
    meta_image_pull_policy="IfNotPresent"
  fi
  if [ "$hiqlite_deploy" = "1" ] && [ "$build_hiqlite_image" = "0" ]; then
    if [ -z "$hiqlite_image_digest" ] && [[ "$hiqlite_image" == *@sha256:* ]]; then
      hiqlite_image_digest="${hiqlite_image##*@}"
    fi
    hiqlite_image="$(immutable_image_reference "$hiqlite_image" "$hiqlite_image_digest")"
    hiqlite_image_pull_policy="IfNotPresent"
  fi
  if [ "$ingest_writer_smoke" = "1" ] && [ "$build_ingest_writer_image" = "0" ]; then
    if [ -z "$ingest_writer_image_digest" ] && [[ "$ingest_writer_image" == *@sha256:* ]]; then
      ingest_writer_image_digest="${ingest_writer_image##*@}"
    fi
    ingest_writer_image="$(immutable_image_reference "$ingest_writer_image" "$ingest_writer_image_digest")"
    ingest_writer_image_pull_policy="IfNotPresent"
  fi
fi

case "$product_smoke" in
  0 | 1) ;;
  *)
    echo "VELORIX_VIND_PRODUCT_SMOKE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$rest_api_smoke" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_VIND_REST_API_SMOKE must be auto, 0, or 1" >&2
    exit 64
    ;;
esac

case "$product_completion_report" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_VIND_PRODUCT_COMPLETION_REPORT must be auto, 0, or 1" >&2
    exit 64
    ;;
esac

case "$api_tls_enabled" in
  0 | 1) ;;
  *)
    echo "VELORIX_API_TLS_ENABLED must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$object_store_mode" in
  rustfs | local-rustfs)
    object_store_mode="rustfs"
    s3_endpoint="http://rustfs:9000"
    s3_backend_label="rustfs"
    s3_durability_label="ephemeral-emptyDir"
    s3_authority_store_id="${VELORIX_AUTHORITY_STORE_ID:-s3://rustfs/${bucket}/${s3_prefix}}"
    ;;
  external-s3 | external-s3-compatible | external-oss | oss)
    object_store_mode="external-s3"
    if [ -z "$aws_endpoint_url" ]; then
      echo "AWS_ENDPOINT_URL is required when VELORIX_OBJECT_STORE_MODE=external-s3" >&2
      exit 64
    fi
    s3_endpoint="$aws_endpoint_url"
    s3_backend_label="external-s3-compatible"
    s3_durability_label="external"
    s3_authority_store_id="${VELORIX_AUTHORITY_STORE_ID:-s3://external/${bucket}/${s3_prefix}}"
    ;;
  *)
    echo "VELORIX_OBJECT_STORE_MODE must be rustfs or external-s3" >&2
    exit 64
    ;;
esac

case "$ingest_writer_smoke" in
  0 | 1) ;;
  *)
    echo "VELORIX_INGEST_WRITER_SMOKE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$ingest_writer_lifecycle_auto" in
  0 | 1) ;;
  *)
    echo "VELORIX_INGEST_WRITER_LIFECYCLE_AUTO must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$multi_replica_fencing_smoke" in
  0 | 1) ;;
  *)
    echo "VELORIX_MULTI_REPLICA_FENCING_SMOKE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$external_s3_validate" in
  0 | 1) ;;
  *)
    echo "VELORIX_EXTERNAL_S3_VALIDATE must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$s3_force_path_style" in
  0 | 1) ;;
  *)
    echo "VELORIX_S3_FORCE_PATH_STYLE must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$s3_credentials_secret_managed" in
  0 | 1) ;;
  *)
    echo "VELORIX_S3_CREDENTIALS_SECRET_MANAGED must be 0 or 1" >&2
    exit 64
    ;;
esac
python3 - "$s3_credentials_secret_name" <<'PY'
import re
import sys

name = sys.argv[1]
if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", name):
    raise SystemExit("VELORIX_S3_CREDENTIALS_SECRET_NAME must be a valid Kubernetes Secret name")
PY
if [ "$s3_force_path_style" = "1" ]; then
  s3_force_path_style_bool="true"
else
  s3_force_path_style_bool="false"
fi
case "$object_store_local_development_authority" in
  0 | 1) ;;
  *)
    echo "VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$api_allow_unauthenticated_dev" in
  0 | 1) ;;
  *)
    echo "VELORIX_API_ALLOW_UNAUTHENTICATED_DEV must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$no_pvc_namespace_validate" in
  0 | 1) ;;
  *)
    echo "VELORIX_NO_PVC_NAMESPACE_VALIDATE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$managed_persistence" in
  0 | 1) ;;
  *)
    echo "VELORIX_MANAGED_PERSISTENCE must be 0 or 1" >&2
    exit 64
    ;;
esac
for managed_size in "$managed_hiqlite_storage_size" "$managed_rustfs_storage_size"; do
  if [[ ! "$managed_size" =~ ^[1-9][0-9]*([KMGTPE]i)?$ ]]; then
    echo "managed PVC storage sizes must be positive Kubernetes quantities such as 10Gi" >&2
    exit 64
  fi
done
if [ -n "$managed_storage_class" ] && { [ "${#managed_storage_class}" -gt 253 ] || [[ ! "$managed_storage_class" =~ ^([a-z0-9]([-a-z0-9]*[a-z0-9])?)(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$ ]]; }; then
  echo "VELORIX_MANAGED_STORAGE_CLASS must be a lowercase Kubernetes DNS subdomain" >&2
  exit 64
fi
if [ "$managed_persistence" = "1" ] && [ "$cleanup" = "1" ]; then
  echo "VELORIX_MANAGED_PERSISTENCE=1 rejects VELORIX_VIND_CLEANUP=1 to avoid deleting a persistent deployment without explicit acknowledgement" >&2
  exit 64
fi

case "$final_owner_aware_attach" in
  0 | 1) ;;
  *)
    echo "VELORIX_API_FINAL_OWNER_AWARE_ATTACH must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$standing_runtime_failover_smoke" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_STANDING_RUNTIME_FAILOVER_SMOKE must be auto, 0, or 1" >&2
    exit 64
    ;;
esac

case "$hiqlite_backend_time_assess" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_HIQLITE_BACKEND_TIME_ASSESS must be auto, 0, or 1" >&2
    exit 64
    ;;
esac

case "$ingress_tls_auth_auto" in
  0 | 1) ;;
  *)
    echo "VELORIX_INGRESS_TLS_AUTH_AUTO must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$product_ingress_apply" in
  0 | 1) ;;
  *)
    echo "VELORIX_PRODUCT_INGRESS_APPLY must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$product_evidence_level" in
  local-vind | local-vind-only)
    product_evidence_level="local-vind-only"
    ;;
  product-complete)
    if [ -n "$ingress_tls_auth_attestation_file" ]; then
      validate_ingress_tls_auth_attestation
    fi
    if [ -n "$object_store_durability_attestation_file" ]; then
      validate_object_store_durability_attestation
    fi
    if [ -n "$ingest_writer_lifecycle_attestation_file" ]; then
      validate_ingest_writer_lifecycle_attestation
    fi
    if [ -n "$hiqlite_authority_attestation_file" ]; then
      validate_hiqlite_authority_attestation
    fi
    ;;
  *)
    echo "VELORIX_PRODUCT_EVIDENCE_LEVEL must be local-vind-only or product-complete" >&2
    exit 64
    ;;
esac

case "$preserve_state" in
  0 | 1) ;;
  *)
    echo "VELORIX_VIND_PRESERVE_STATE must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$vcluster_create_retries" in
  '' | *[!0-9]*)
    echo "VELORIX_VCLUSTER_CREATE_RETRIES must be a non-negative integer" >&2
    exit 64
    ;;
esac

case "$allow_mutable_rustfs_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE must be 0 or 1" >&2
    exit 64
    ;;
esac

if [ "$object_store_mode" = "rustfs" ] && [ "$allow_mutable_rustfs_image" != "1" ] && is_mutable_image_reference "$rustfs_image"; then
  echo "VELORIX_RUSTFS_IMAGE must use a version tag or digest; set VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE=1 to use ${rustfs_image}" >&2
  exit 64
fi

if [ "$s3_credentials_secret_managed" = "0" ]; then
  if [ "$object_store_mode" != "external-s3" ]; then
    echo "VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0 is only supported with VELORIX_OBJECT_STORE_MODE=external-s3" >&2
    exit 64
  fi
  if [ -n "$aws_access_key_id" ] || [ -n "$aws_secret_access_key" ] || [ -n "$aws_session_token" ]; then
    echo "VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0 uses an existing Kubernetes Secret; unset AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN" >&2
    exit 64
  fi
  s3_credentials_source="existing-kubernetes-secret"
elif [ -z "$aws_access_key_id" ] && [ -z "$aws_secret_access_key" ] && [ "$object_store_mode" = "rustfs" ]; then
  aws_access_key_id="$(
    python3 - <<'PY'
import secrets

print("vlx" + secrets.token_hex(16))
PY
  )"
  aws_secret_access_key="$(
    python3 - <<'PY'
import secrets

print(secrets.token_urlsafe(32))
PY
  )"
  s3_credentials_source="generated"
elif [ -z "$aws_access_key_id" ] || [ -z "$aws_secret_access_key" ]; then
  if [ "$object_store_mode" = "external-s3" ]; then
echo "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required together when VELORIX_OBJECT_STORE_MODE=external-s3 unless VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0 uses an existing Kubernetes Secret" >&2
  else
    echo "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be supplied together, or both left unset for generated RustFS credentials" >&2
  fi
  exit 64
fi

if [ "$s3_credentials_secret_managed" = "1" ]; then
  python3 - "$aws_access_key_id" "$aws_secret_access_key" "$aws_session_token" "$object_store_mode" <<'PY'
import re
import sys

access_key, secret_key, session_token, object_store_mode = sys.argv[1:]
known_default_pairs = {
    ("rustfsadmin", "rustfsadmin"),
    ("minioadmin", "minioadmin"),
}
if (access_key, secret_key) in known_default_pairs:
    hint = (
        "leave both unset to generate local RustFS credentials"
        if object_store_mode == "rustfs"
        else "supply non-default external object-store credentials"
    )
    raise SystemExit(
        f"AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY must not use known local object-store defaults; {hint}"
    )
for name, value in {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
}.items():
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
if not re.fullmatch(r"[A-Za-z0-9._+=,@-]{3,128}", access_key):
    raise SystemExit("AWS_ACCESS_KEY_ID must contain only S3-compatible access-key characters")
if len(secret_key) < 16:
    raise SystemExit("AWS_SECRET_ACCESS_KEY must be at least 16 characters")
if session_token:
    if session_token.strip() != session_token:
        raise SystemExit("AWS_SESSION_TOKEN must not have leading or trailing whitespace")
    if not session_token.isascii():
        raise SystemExit("AWS_SESSION_TOKEN must be ASCII")
    if any(ch.isspace() for ch in session_token):
        raise SystemExit("AWS_SESSION_TOKEN must not contain whitespace")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in session_token):
        raise SystemExit("AWS_SESSION_TOKEN must not contain control characters")
PY
fi

case "$api_replica_count" in
  '' | *[!0-9]*)
    echo "VELORIX_API_REPLICA_COUNT must be a positive integer" >&2
    exit 64
    ;;
  *)
    if [ "$api_replica_count" -lt 1 ]; then
      echo "VELORIX_API_REPLICA_COUNT must be a positive integer" >&2
      exit 64
    fi
    ;;
esac

case "$api_tls_local_port" in
  '' | *[!0-9]*)
    echo "VELORIX_API_TLS_LOCAL_PORT must be a positive integer" >&2
    exit 64
    ;;
  *)
    if [ "$api_tls_local_port" -lt 1 ] || [ "$api_tls_local_port" -gt 65535 ]; then
      echo "VELORIX_API_TLS_LOCAL_PORT must be between 1 and 65535" >&2
      exit 64
    fi
    ;;
esac
if [ "$api_tls_enabled" = "1" ] && [ "$api_tls_local_port" = "$api_local_port" ]; then
  echo "VELORIX_API_TLS_LOCAL_PORT must differ from VELORIX_API_LOCAL_PORT" >&2
  exit 64
fi

case "$meta_backend" in
  memory | in-memory | oss | object-store | hiqlite) ;;
  *)
    echo "VELORIX_META_BACKEND must be memory, oss, or hiqlite" >&2
    exit 64
    ;;
esac

case "$meta_mode" in
  production | prod | development | dev) ;;
  *)
    echo "VELORIX_META_MODE must be production or development" >&2
    exit 64
    ;;
esac
if [ "$meta_mode" = "production" ] || [ "$meta_mode" = "prod" ]; then
  echo "VELORIX_META_MODE=production is unsupported by this local runner until validated transport configuration exists" >&2
  exit 64
fi
case "$hiqlite_deploy" in
  0 | 1) ;;
  *)
    echo "VELORIX_HIQLITE_DEPLOY must be 0 or 1" >&2
    exit 64
    ;;
esac
if [ "$hiqlite_deploy" = "1" ] && { [ "$meta_enabled" != "1" ] || [ "$meta_backend" != "hiqlite" ]; }; then
  echo "VELORIX_HIQLITE_DEPLOY=1 requires VELORIX_META_ENABLED=1 and VELORIX_META_BACKEND=hiqlite" >&2
  exit 64
fi
if [ "$hiqlite_deploy" = "1" ]; then
  if [ -z "$hiqlite_nodes" ]; then
    hiqlite_nodes="velorix-hiqlite-0.velorix-hiqlite-headless:8200,velorix-hiqlite-1.velorix-hiqlite-headless:8200,velorix-hiqlite-2.velorix-hiqlite-headless:8200"
  fi
  if [ -z "$hiqlite_api_secret" ]; then
    hiqlite_api_secret="$(random_token)"
  fi
  if [ -z "$hiqlite_raft_secret" ]; then
    hiqlite_raft_secret="$(random_token)"
  fi
  if [ -z "$hiqlite_enc_key_active" ]; then
    hiqlite_enc_key_active="$(random_alnum)"
  fi
  if [ -z "$hiqlite_enc_keys" ]; then
    hiqlite_enc_keys="${hiqlite_enc_key_active}/$(random_base64_key)"
  fi
fi

case "$standing_runtime_fencing" in
  unsafe-dev-only | logical-fencing | required) ;;
  *)
    echo "VELORIX_STANDING_RUNTIME_FENCING must be unsafe-dev-only, logical-fencing, or required" >&2
    exit 64
    ;;
esac

case "$standing_runtime_owner_ttl_ms" in
  '' | *[!0-9]*)
    echo "VELORIX_STANDING_RUNTIME_OWNER_TTL_MS must be a positive integer" >&2
    exit 64
    ;;
  *)
    if [ "$standing_runtime_owner_ttl_ms" -lt 1 ]; then
      echo "VELORIX_STANDING_RUNTIME_OWNER_TTL_MS must be a positive integer" >&2
      exit 64
    fi
    ;;
esac

case "$output_compaction_interval_epochs" in
  '' | *[!0-9]*)
    echo "VELORIX_OUTPUT_COMPACTION_INTERVAL_EPOCHS must be a non-negative integer" >&2
    exit 64
    ;;
esac

if [ "$meta_enabled" = "1" ] && [ -z "$meta_bearer_token" ]; then
  meta_bearer_token="$(
    python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
  )"
fi

if [ -n "$api_bearer_token" ] && [ "$api_allow_unauthenticated_dev" = "1" ]; then
  echo "VELORIX_API_BEARER_TOKEN and VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1 are mutually exclusive" >&2
  exit 64
fi
if [ -n "$admin_bearer_token" ] && [ "$api_allow_unauthenticated_dev" = "1" ]; then
  echo "VELORIX_ADMIN_BEARER_TOKEN and VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1 are mutually exclusive" >&2
  exit 64
fi

if [ -n "$api_bearer_token" ]; then
  api_auth_mode="bearer-token"
  api_bearer_token_source="supplied"
elif [ "$api_allow_unauthenticated_dev" = "1" ]; then
  api_auth_mode="unauthenticated-dev"
  api_bearer_token_source="none"
else
  api_auth_mode="bearer-token"
  api_bearer_token="$(
    python3 - <<'PY'
import secrets

print(secrets.token_urlsafe(32))
PY
  )"
  api_bearer_token_source="generated"
fi

if [ "$api_auth_mode" = "bearer-token" ]; then
  if [ -n "$admin_bearer_token" ]; then
    admin_bearer_token_source="supplied"
  else
    admin_bearer_token="$(
      python3 - <<'PY'
import secrets

print(secrets.token_urlsafe(32))
PY
    )"
    admin_bearer_token_source="generated"
  fi
  if [ "$admin_bearer_token" = "$api_bearer_token" ]; then
    echo "VELORIX_ADMIN_BEARER_TOKEN must be distinct from VELORIX_API_BEARER_TOKEN" >&2
    exit 64
  fi
else
  admin_bearer_token_source="none"
fi
validate_ingress_tls_auth_attestation
validate_object_store_durability_attestation
validate_ingest_writer_lifecycle_attestation

if [ "$meta_enabled" = "1" ]; then
  python3 - "$meta_bearer_token" <<'PY'
import sys

token = sys.argv[1]
if not token:
    raise SystemExit("VELORIX_META_BEARER_TOKEN must be nonempty")
if token.strip() != token:
    raise SystemExit("VELORIX_META_BEARER_TOKEN must not have leading or trailing whitespace")
if not token.isascii():
    raise SystemExit("VELORIX_META_BEARER_TOKEN must be ASCII")
if any(ch.isspace() for ch in token):
    raise SystemExit("VELORIX_META_BEARER_TOKEN must not contain whitespace")
if any(ord(ch) < 32 or ord(ch) == 127 for ch in token):
    raise SystemExit("VELORIX_META_BEARER_TOKEN must not contain control characters")
PY
fi

if [ "$api_auth_mode" = "bearer-token" ]; then
  python3 - "$api_bearer_token" <<'PY'
import re
import sys

token = sys.argv[1]
if not token:
    raise SystemExit("VELORIX_API_BEARER_TOKEN must be nonempty")
if token.strip() != token:
    raise SystemExit("VELORIX_API_BEARER_TOKEN must not have leading or trailing whitespace")
if not token.isascii():
    raise SystemExit("VELORIX_API_BEARER_TOKEN must be ASCII")
if any(ch.isspace() for ch in token):
    raise SystemExit("VELORIX_API_BEARER_TOKEN must not contain whitespace")
if any(ord(ch) < 32 or ord(ch) == 127 for ch in token):
    raise SystemExit("VELORIX_API_BEARER_TOKEN must not contain control characters")
if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
    raise SystemExit("VELORIX_API_BEARER_TOKEN must contain only URL/header-safe token characters")
PY
  python3 - "$admin_bearer_token" <<'PY'
import re
import sys

token = sys.argv[1]
if not token:
    raise SystemExit("VELORIX_ADMIN_BEARER_TOKEN must be nonempty")
if token.strip() != token:
    raise SystemExit("VELORIX_ADMIN_BEARER_TOKEN must not have leading or trailing whitespace")
if not token.isascii():
    raise SystemExit("VELORIX_ADMIN_BEARER_TOKEN must be ASCII")
if any(ch.isspace() for ch in token):
    raise SystemExit("VELORIX_ADMIN_BEARER_TOKEN must not contain whitespace")
if any(ord(ch) < 32 or ord(ch) == 127 for ch in token):
    raise SystemExit("VELORIX_ADMIN_BEARER_TOKEN must not contain control characters")
if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
    raise SystemExit("VELORIX_ADMIN_BEARER_TOKEN must contain only URL/header-safe token characters")
PY
fi

meta_bearer_token_hash="disabled"
if [ "$meta_enabled" = "1" ]; then
  meta_bearer_token_hash="$(sha256_value "$meta_bearer_token")"
fi
if [ "$s3_credentials_secret_managed" = "1" ]; then
  s3_credentials_hash="$(sha256_value "${aws_access_key_id}:${aws_secret_access_key}:${aws_session_token}")"
else
  s3_credentials_hash="$(sha256_value "existing-kubernetes-secret:${s3_credentials_secret_name}")"
fi
hiqlite_api_secret_hash="disabled"
if [ -n "$hiqlite_api_secret" ]; then
  hiqlite_api_secret_hash="$(sha256_value "$hiqlite_api_secret")"
fi
hiqlite_raft_secret_hash="disabled"
if [ -n "$hiqlite_raft_secret" ]; then
  hiqlite_raft_secret_hash="$(sha256_value "$hiqlite_raft_secret")"
fi

if [ "$meta_enabled" = "1" ] && [ "$meta_backend" = "hiqlite" ]; then
  if [ -z "$hiqlite_nodes" ] || [ -z "$hiqlite_api_secret" ]; then
    echo "VELORIX_META_BACKEND=hiqlite requires VELORIX_HIQLITE_NODES and VELORIX_HIQLITE_API_SECRET" >&2
    exit 64
  fi
  validate_hiqlite_nodes_for_remote_client "$hiqlite_nodes"
fi
if [ "$hiqlite_deploy" = "1" ]; then
  if [ -z "$hiqlite_raft_secret" ] || [ -z "$hiqlite_enc_key_active" ] || [ -z "$hiqlite_enc_keys" ]; then
    echo "VELORIX_HIQLITE_DEPLOY=1 requires generated or supplied raft secret and encryption keys" >&2
    exit 64
  fi
  validate_hiqlite_enc_keys "$hiqlite_enc_key_active" "$hiqlite_enc_keys"
fi
validate_hiqlite_authority_attestation

if [ "$standing_runtime_fencing" = "unsafe-dev-only" ] && [ "$api_replica_count" -gt 1 ]; then
  echo "VELORIX_API_REPLICA_COUNT>1 requires VELORIX_STANDING_RUNTIME_FENCING=logical-fencing or required" >&2
  exit 64
fi

if [ "$standing_runtime_fencing" != "unsafe-dev-only" ] && [ "$meta_enabled" != "1" ]; then
  echo "standing-runtime fencing mode ${standing_runtime_fencing} requires VELORIX_META_ENABLED=1" >&2
  exit 64
fi

if ! python3 - "$bucket" <<'PY'
import re
import sys

bucket = sys.argv[1]
if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
    raise SystemExit(
        "VELORIX_S3_BUCKET must be a DNS-compatible S3 bucket name "
        "(3-63 lowercase letters, digits, dots, or hyphens)"
    )
if ".." in bucket or ".-" in bucket or "-." in bucket:
    raise SystemExit("VELORIX_S3_BUCKET must not contain adjacent dots or dot-hyphen sequences")
if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", bucket):
    raise SystemExit("VELORIX_S3_BUCKET must not look like an IPv4 address")
PY
then
  exit 64
fi

write_diagnostics() {
  mkdir -p "$output_dir"
  {
    echo "generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "cluster=${cluster}"
    echo "cluster_driver=${cluster_driver}"
    echo "product_deployment_id=${product_deployment_id}"
    echo "context=${context}"
    echo "namespace=${namespace}"
    echo "object_store_mode=${object_store_mode}"
    echo "object_store_endpoint=${s3_endpoint}"
    echo "authority_store_id=${s3_authority_store_id}"
    echo "s3_bucket=${bucket}"
    echo "s3_prefix=${s3_prefix}"
    echo "aws_region=${aws_region}"
    echo "external_s3_validate=${external_s3_validate}"
    echo "external_s3_bucket_validated=${external_s3_bucket_validated}"
    echo "hiqlite_authority_attestation_file=${hiqlite_authority_attestation_file:-}"
    echo "hiqlite_authority_attestation_validated=${hiqlite_authority_attestation_validated}"
    echo "ingress_tls_auth_attestation_file=${ingress_tls_auth_attestation_file:-}"
    echo "ingress_tls_auth_attestation_validated=${ingress_tls_auth_attestation_validated}"
    echo "ingest_writer_lifecycle_attestation_file=${ingest_writer_lifecycle_attestation_file:-}"
    echo "ingest_writer_lifecycle_attestation_validated=${ingest_writer_lifecycle_attestation_validated}"
    echo
    if [ "$cluster_driver" = "docker-vcluster" ]; then
      echo "== vcluster list =="
      vcluster list --driver docker || true
      echo
    fi
    echo "== kubectl contexts =="
    kubectl config get-contexts || true
    echo
    echo "== pods =="
    kubectl --context "$context" get pods -n "$namespace" -o wide || true
    echo
    echo "== services =="
    kubectl --context "$context" get svc -n "$namespace" -o wide || true
    echo
    echo "== nodes =="
    kubectl --context "$context" get nodes -o wide || true
    echo
    echo "== node conditions and taints =="
    kubectl --context "$context" describe nodes || true
    echo
    echo "== recent events =="
    kubectl --context "$context" get events -A --sort-by=.lastTimestamp || true
    echo
    echo "== api logs =="
    kubectl --context "$context" logs -n "$namespace" deploy/velorix-api --tail=200 || true
    echo
    echo "== meta logs =="
    kubectl --context "$context" logs -n "$namespace" deploy/velorix-meta --tail=200 || true
    if [ "$object_store_mode" = "rustfs" ]; then
      echo
      echo "== rustfs logs =="
      kubectl --context "$context" logs -n "$namespace" deploy/rustfs --tail=200 || true
    fi
  } >"${output_dir}/diagnostics.txt" 2>&1
}

cleanup_vind() {
  status="$1"

  if [ "$status" != "0" ]; then
    write_diagnostics
    echo "wrote diagnostics to ${output_dir}/diagnostics.txt" >&2
  fi

  if [ "$cleanup" = "1" ] && [ "$created_cluster" = "1" ] && [ "$cluster_driver" = "docker-vcluster" ]; then
    vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || true
  fi
  if [ "$cleanup" = "1" ] && [ "$cluster_driver" = "existing-context" ] && [ "$created_namespace" = "1" ]; then
    kubectl --context "$context" delete namespace "$namespace" --ignore-not-found >/dev/null 2>&1 || true
  fi

  if [ -n "$port_forward_pid" ] && kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$api_tls_port_forward_pid" ] && kill -0 "$api_tls_port_forward_pid" >/dev/null 2>&1; then
    kill "$api_tls_port_forward_pid" >/dev/null 2>&1 || true
  fi

  if [ -n "$previous_context" ]; then
    kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  fi
}

vcluster_exists() {
  local clusters
  clusters="$(vcluster list --driver docker --output json)" || {
    echo "failed to list docker vClusters" >&2
    exit 1
  }

  python3 - "$cluster" "$clusters" <<'PY'
import json
import sys

cluster = sys.argv[1]
clusters = json.loads(sys.argv[2])
for item in clusters:
    if item.get("Name") == cluster or item.get("name") == cluster:
        sys.exit(0)
sys.exit(1)
PY
}

wait_for_kubernetes() {
  for _ in $(seq 1 120); do
    if kubectl --context "$context" get --raw=/readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "vind Kubernetes API did not become ready for ${context}" >&2
  return 1
}

wait_for_kubernetes_scheduling_ready() {
  local nodes_json
  local deadline=$((SECONDS + 120))
  nodes_json="${output_dir}/k8s-nodes-scheduling-ready.json"
  while true; do
    if kubectl --context "$context" get nodes -o json >"$nodes_json" 2>/dev/null \
      && python3 - "$nodes_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    nodes = json.load(f)

items = nodes.get("items") or []
if not items:
    sys.exit(1)

blocking_taints = {
    "node.kubernetes.io/disk-pressure",
    "node.kubernetes.io/memory-pressure",
    "node.kubernetes.io/network-unavailable",
    "node.kubernetes.io/not-ready",
    "node.kubernetes.io/pid-pressure",
    "node.kubernetes.io/unreachable",
}

for node in items:
    status = node.get("status") or {}
    spec = node.get("spec") or {}
    ready = False
    for condition in status.get("conditions") or []:
        if condition.get("type") == "Ready":
            ready = condition.get("status") == "True"
            break
    if not ready:
        sys.exit(1)
    for taint in spec.get("taints") or []:
        if taint.get("effect") not in {"NoSchedule", "NoExecute"}:
            continue
        if taint.get("key") in blocking_taints:
            sys.exit(1)

sys.exit(0)
PY
    then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "vind Kubernetes node scheduling did not become ready for ${context}" >&2
      return 1
    fi
    sleep 2
  done
}

validate_managed_storage_class() {
  if [ "$managed_persistence" != "1" ] || [ -z "$managed_storage_class" ]; then
    return 0
  fi
  if ! kubectl --context "$context" get storageclass "$managed_storage_class" >/dev/null 2>&1; then
    echo "VELORIX_MANAGED_STORAGE_CLASS=${managed_storage_class} is unavailable in the selected Kubernetes context" >&2
    exit 66
  fi
}

validate_local_vcluster_context() {
  local current_context
  local server
  current_context="$(kubectl config current-context)"
  if [ "$current_context" != "$context" ]; then
    echo "refusing to continue: current kube context is ${current_context}, expected ${context}" >&2
    return 1
  fi
  server="$(kubectl --context "$context" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
  case "$server" in
    https://127.0.0.1:* | https://localhost:*) ;;
    *)
      echo "refusing to continue: ${context} does not point at a local vCluster API server (${server})" >&2
      return 1
      ;;
  esac
}

validate_existing_kubernetes_context() {
  local server
  if ! kubectl config get-contexts "$context" >/dev/null 2>&1; then
    echo "existing Kubernetes context does not exist: ${context}" >&2
    return 1
  fi
  server="$(kubectl --context "$context" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
  if [ "$existing_context_allow_remote" != "1" ]; then
    case "$server" in
      https://127.0.0.1:* | https://localhost:* | https://0.0.0.0:*) ;;
      *)
        echo "refusing existing-context driver for non-local Kubernetes API server ${server}; set VELORIX_EXISTING_CONTEXT_ALLOW_REMOTE=1 to override" >&2
        return 1
        ;;
    esac
  fi
}

vcluster_container() {
  local id
  id="$(docker ps --filter "name=${cluster}" --format '{{.ID}} {{.Names}}' \
    | awk -v c="$cluster" '$2 == "vcluster-" c || $2 == c || index($2, c) > 0 { print $1; exit }')"
  if [ -z "$id" ]; then
    echo "could not find Docker container for vCluster ${cluster}" >&2
    docker ps --format '{{.ID}} {{.Names}}' >&2
    exit 1
  fi
  printf '%s\n' "$id"
}

infer_k3d_cluster_name() {
  if [ -n "$k3d_cluster" ]; then
    printf '%s\n' "$k3d_cluster"
    return 0
  fi
  case "$context" in
    k3d-*)
      printf '%s\n' "${context#k3d-}"
      return 0
      ;;
  esac
  return 1
}

k3d_node_containers() {
  local detected_cluster
  detected_cluster="$(infer_k3d_cluster_name)" || return 1
  docker ps \
    --filter "label=app=k3d" \
    --filter "label=k3d.cluster=${detected_cluster}" \
    --format '{{.Names}} {{.Label "k3d.role"}}' \
    | awk '$2 == "server" || $2 == "agent" { print $1 }'
}

infer_kind_cluster_name() {
  case "$context" in
    kind-*)
      printf '%s\n' "${context#kind-}"
      return 0
      ;;
  esac
  return 1
}

load_image_into_kind() {
  local image="$1"
  local detected_cluster
  detected_cluster="$(infer_kind_cluster_name)" || {
    echo "could not infer kind cluster from Kubernetes context ${context}; set VELORIX_IMAGE_LOAD_MODE=none if images are already pullable" >&2
    exit 66
  }
  echo "loading ${image} into kind cluster ${detected_cluster}"
  kind load docker-image "$image" --name "$detected_cluster"
}

load_image_into_k3d() {
  local image="$1"
  local nodes
  nodes="$(k3d_node_containers)"
  if [ -z "$nodes" ]; then
    echo "could not find k3d server/agent containers for Kubernetes context ${context}; set VELORIX_K3D_CLUSTER or VELORIX_IMAGE_LOAD_MODE=none" >&2
    exit 66
  fi
  while IFS= read -r node; do
    [ -n "$node" ] || continue
    echo "loading ${image} into k3d node ${node}"
    docker save "$image" | docker exec -i "$node" ctr -n k8s.io images import -
  done <<EOF
${nodes}
EOF
}

load_image_into_vcluster() {
  local image="$1"
  local container="$2"
  echo "loading ${image} into vCluster container ${container}"
  docker save "$image" | docker exec -i "$container" sh -c \
    'ctr -n k8s.io images import - || k3s ctr images import -'
}

load_image_into_product_cluster() {
  local image="$1"
  case "$cluster_driver:${image_load_mode}" in
    docker-vcluster:auto | docker-vcluster:vcluster-docker)
      load_image_into_vcluster "$image" "$(vcluster_container)"
      ;;
    existing-context:auto)
      if infer_k3d_cluster_name >/dev/null 2>&1; then
        load_image_into_k3d "$image"
      elif infer_kind_cluster_name >/dev/null 2>&1; then
        load_image_into_kind "$image"
      else
        echo "VELORIX_IMAGE_LOAD_MODE=auto cannot infer an image loader for context ${context}; set VELORIX_IMAGE_LOAD_MODE=none if images are already pullable" >&2
        exit 66
      fi
      ;;
    existing-context:k3d)
      load_image_into_k3d "$image"
      ;;
    existing-context:kind)
      load_image_into_kind "$image"
      ;;
    *:none)
      echo "skipping image load for ${image}; assuming it is already pullable by ${context}"
      ;;
    *)
      echo "unsupported VELORIX_IMAGE_LOAD_MODE=${image_load_mode} for VELORIX_VIND_CLUSTER_DRIVER=${cluster_driver}" >&2
      exit 64
      ;;
  esac
}

load_existing_image_into_product_cluster() {
  local image="$1"
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "VELORIX_LOAD_EXISTING_IMAGES=1 requires local Docker image ${image}" >&2
    exit 66
  fi
  load_image_into_product_cluster "$image"
}

resolve_local_image_digest() {
  local image="$1"
  docker image inspect "$image" --format '{{.Id}}' 2>/dev/null || true
}

observed_pod_image_digest() {
  local pods_file="$1"
  local container_name="$2"
  python3 - "$pods_file" "$container_name" <<'PY'
import json
import re
import sys

pods_file, container_name = sys.argv[1:]
sha256_pattern = re.compile(r"sha256:[0-9a-fA-F]{64}")
with open(pods_file, "r", encoding="utf-8") as f:
    pods = json.load(f)
digests = set()
for pod in pods.get("items") or []:
    for status in pod.get("status", {}).get("containerStatuses") or []:
        if status.get("name") != container_name:
            continue
        image_id = status.get("imageID") or ""
        match = sha256_pattern.search(image_id)
        if not match:
            raise SystemExit(
                f"pod {pod.get('metadata', {}).get('name')} {container_name} imageID lacks sha256 digest"
            )
        digests.add(match.group(0).lower())
if not digests:
    raise SystemExit(f"pods evidence has no container status for {container_name}")
if len(digests) != 1:
    raise SystemExit(f"pods for {container_name} use multiple image digests: {sorted(digests)}")
print(next(iter(digests)))
PY
}

wait_for_rollout() {
  local deployment="$1"
  local deadline=$((SECONDS + 300))
  while true; do
    if kubectl --context "$context" -n "$namespace" rollout status "deployment/${deployment}" --timeout=5s >/dev/null 2>&1; then
      return 0
    fi
    check_kubernetes_scheduling_health "rollout-${deployment}"
    if [ "$SECONDS" -ge "$deadline" ]; then
      kubectl --context "$context" -n "$namespace" rollout status "deployment/${deployment}" --timeout=1s >&2 || true
      echo "deployment/${deployment} did not roll out in ${context}/${namespace}" >&2
      exit 1
    fi
  done
}

sync_deployed_image_digest_annotation() {
  local deployment="$1"
  local app_label="$2"
  local container_name="$3"
  local current_digest="$4"
  local deployment_file="$5"
  local pods_file="$6"
  local observed_digest
  local refreshed_digest
  local patch_json

  observed_digest="$(observed_pod_image_digest "$pods_file" "$container_name")"
  if [ "$current_digest" != "$observed_digest" ]; then
    echo "updating ${deployment} image digest from deployed pod imageID: ${observed_digest}" >&2
  fi
  patch_json="$(
    python3 - "$observed_digest" <<'PY'
import json
import sys

print(json.dumps({
    "spec": {
        "template": {
            "metadata": {
                "annotations": {
                    "velorix.dev/image-digest": sys.argv[1],
                    "velorix.dev/image-digest-source": "observed-pod-imageid-after-rollout",
                }
            }
        }
    }
}))
PY
  )"
  kubectl --context "$context" -n "$namespace" patch deployment "$deployment" \
    --type merge -p "$patch_json" >/dev/null
  wait_for_rollout "$deployment"
  kubectl --context "$context" -n "$namespace" get deployment "$deployment" -o json >"$deployment_file"
  kubectl --context "$context" -n "$namespace" get pods \
    -l "app=${app_label},velorix.dev/run-id=${run_id}" -o json >"$pods_file"
  refreshed_digest="$(observed_pod_image_digest "$pods_file" "$container_name")"
  if [ "$refreshed_digest" != "$observed_digest" ]; then
    echo "${deployment} pod image digest changed after annotation sync: ${observed_digest} -> ${refreshed_digest}" >&2
    exit 1
  fi
  echo "$observed_digest"
}

remove_service_run_id_selector() {
  local service="$1"
  kubectl --context "$context" -n "$namespace" patch "service/${service}" \
    --type=json \
    -p='[{"op":"remove","path":"/spec/selector/velorix.dev~1run-id"}]' \
    >/dev/null 2>&1 || true
}

wait_for_statefulset_rollout() {
  local statefulset="$1"
  local deadline=$((SECONDS + 240))
  while true; do
    if kubectl --context "$context" -n "$namespace" rollout status "statefulset/${statefulset}" --timeout=5s >/dev/null 2>&1; then
      return 0
    fi
    check_kubernetes_scheduling_health "rollout-${statefulset}"
    if [ "$SECONDS" -ge "$deadline" ]; then
      kubectl --context "$context" -n "$namespace" rollout status "statefulset/${statefulset}" --timeout=1s >&2 || true
      echo "statefulset/${statefulset} did not roll out in ${context}/${namespace}" >&2
      exit 1
    fi
  done
}

wait_for_job_complete() {
  local job="$1"
  local deadline=$((SECONDS + 180))
  while true; do
    if kubectl --context "$context" -n "$namespace" wait --for=condition=complete "job/${job}" --timeout=5s >/dev/null 2>&1; then
      return 0
    fi
    check_kubernetes_scheduling_health "job-${job}"
    if [ "$SECONDS" -ge "$deadline" ]; then
      kubectl --context "$context" -n "$namespace" logs "job/${job}" --tail=200 >&2 || true
      echo "job/${job} did not complete in ${context}/${namespace}" >&2
      exit 1
    fi
  done
}

wait_for_job_failed() {
  local job="$1"
  local deadline=$((SECONDS + 180))
  while true; do
    if kubectl --context "$context" -n "$namespace" wait --for=condition=failed "job/${job}" --timeout=5s >/dev/null 2>&1; then
      return 0
    fi
    check_kubernetes_scheduling_health "job-${job}"
    if [ "$SECONDS" -ge "$deadline" ]; then
      kubectl --context "$context" -n "$namespace" logs "job/${job}" --tail=200 >&2 || true
      echo "job/${job} did not fail as expected in ${context}/${namespace}" >&2
      exit 1
    fi
  done
}

check_kubernetes_scheduling_health() {
  local stage="$1"
  local safe_stage
  local nodes_json
  local pods_json=""
  local blocker_json
  safe_stage="$(printf '%s' "$stage" | tr -c 'A-Za-z0-9_.-' '-')"
  nodes_json="${output_dir}/k8s-nodes-${safe_stage}.json"
  blocker_json="${output_dir}/local-environment-blocker.json"
  if ! kubectl --context "$context" get nodes -o json >"$nodes_json"; then
    echo "could not inspect Kubernetes nodes for scheduling health at ${stage}" >&2
    return 0
  fi
  if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
    pods_json="${output_dir}/k8s-pods-${safe_stage}.json"
    kubectl --context "$context" -n "$namespace" get pods -o json >"$pods_json" || pods_json=""
  fi
  if ! python3 - "$nodes_json" "${pods_json:-}" "$stage" "$blocker_json" <<'PY'
import json
import sys
from datetime import datetime, timezone

nodes_path, pods_path, stage, blocker_path = sys.argv[1:]
with open(nodes_path, "r", encoding="utf-8") as f:
    body = json.load(f)

pressure_or_readiness = {
    "DiskPressure",
    "MemoryPressure",
    "PIDPressure",
    "Ready",
}
blocking_taints = {
    "node.kubernetes.io/disk-pressure",
    "node.kubernetes.io/memory-pressure",
    "node.kubernetes.io/pid-pressure",
    "node.kubernetes.io/not-ready",
    "node.kubernetes.io/unreachable",
}
problems = []
evidence_files = {"nodes": nodes_path}
for node in body.get("items") or []:
    metadata = node.get("metadata") or {}
    status = node.get("status") or {}
    spec = node.get("spec") or {}
    name = metadata.get("name", "<unnamed>")
    if spec.get("unschedulable") is True:
        problems.append(f"{name}: node is marked unschedulable")
    for condition in status.get("conditions") or []:
        condition_type = condition.get("type")
        condition_status = condition.get("status")
        reason = condition.get("reason")
        message = condition.get("message")
        if condition_type not in pressure_or_readiness:
            continue
        if condition_type == "Ready":
            if condition_status != "True":
                problems.append(f"{name}: Ready={condition_status} reason={reason} message={message}")
        elif condition_status == "True":
            problems.append(f"{name}: {condition_type}=True reason={reason} message={message}")
    for taint in spec.get("taints") or []:
        if taint.get("effect") not in {"NoSchedule", "NoExecute"}:
            continue
        key = taint.get("key")
        if key in blocking_taints:
            problems.append(f"{name}: blocking taint {key}:{taint.get('effect')}")

if pods_path:
    evidence_files["pods"] = pods_path
    with open(pods_path, "r", encoding="utf-8") as f:
        pods = json.load(f)
    markers = [
        "disk-pressure",
        "noschedule",
        "insufficient ephemeral-storage",
        "had taint",
        "evicted",
        "unschedulable",
    ]
    for pod in pods.get("items") or []:
        metadata = pod.get("metadata") or {}
        status = pod.get("status") or {}
        name = metadata.get("name", "<unnamed>")
        phase = status.get("phase")
        reason = status.get("reason")
        message = status.get("message")
        if phase == "Failed" and reason == "Evicted":
            problems.append(f"{name}: pod was Evicted message={message}")
        for condition in status.get("conditions") or []:
            if condition.get("type") == "PodScheduled" and condition.get("status") == "False":
                text = " ".join(
                    str(part or "")
                    for part in [
                        condition.get("reason"),
                        condition.get("message"),
                    ]
                )
                lowered = text.lower()
                if condition.get("reason") == "Unschedulable" or any(marker in lowered for marker in markers):
                    problems.append(f"{name}: PodScheduled=False {text}".strip())

if problems:
    blocker = {
        "schema_version": 1,
        "evidence_kind": "velorix_local_environment_blocker",
        "blocker_kind": "local_vind_kubernetes_scheduling",
        "status": "blocked",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "stage": stage,
        "context": "local vind/vCluster",
        "trusted_for_product_complete": False,
        "no_pvc_workaround_allowed": True,
        "problems": problems,
        "evidence_files": evidence_files,
        "remediation": [
            "inspect the local environment with scripts/doctor-vind-local.sh",
            "free Docker/Colima/vCluster ephemeral storage",
            "if appropriate, delete Docker build cache with scripts/doctor-vind-local.sh --prune-build-cache --yes",
            "remove stale local containers/images/build cache if appropriate",
            "increase local Docker disk capacity",
            "or recreate the reused local vCluster after capacity is available",
            "do not add PVCs to bypass this no-PVC product path",
        ],
    }
    with open(blocker_path, "w", encoding="utf-8") as f:
        json.dump(blocker, f, indent=2, sort_keys=True)
        f.write("\n")
    print(
        f"local vind Kubernetes scheduling is unhealthy at {stage}; this is an environment blocker, not product evidence:",
        file=sys.stderr,
    )
    for problem in problems:
        print(f"- {problem}", file=sys.stderr)
    print(
        "Run scripts/doctor-vind-local.sh to inspect the local environment. "
        "Free Docker/Colima/vCluster ephemeral storage or recreate the local vCluster, then rerun. "
        "The product path remains no-PVC; do not add PVCs to work around this local pressure.",
        file=sys.stderr,
    )
    print(f"wrote local environment blocker evidence to {blocker_path}", file=sys.stderr)
    raise SystemExit(75)
PY
  then
    kubectl --context "$context" get nodes -o wide >&2 || true
    kubectl --context "$context" -n "$namespace" get pods -o wide >&2 || true
    kubectl --context "$context" get events -A --sort-by=.lastTimestamp | tail -60 >&2 || true
    exit 75
  fi
  rm -f "$blocker_json"
}

write_local_vcluster_bootstrap_blocker() {
  local stage="$1"
  local log_file="$2"
  local blocker_json="${output_dir}/local-environment-blocker.json"
  local doctor_json
  doctor_json="$(write_local_environment_doctor_snapshot || true)"
  python3 - "$stage" "$log_file" "$blocker_json" "$doctor_json" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

stage, log_path, blocker_path, doctor_path = sys.argv[1:]
with open(log_path, "r", encoding="utf-8", errors="replace") as f:
    log = f.read()

problems = []
blocker_detail_kind = []
lowered = log.lower()
if "unknown service runtime.v1.runtimeservice" in lowered:
    blocker_detail_kind.append("missing_cri_v1_runtime_service")
    problems.append(
        "vCluster standalone node join failed because the container runtime does not expose the CRI v1 RuntimeService"
    )
if "too many open files" in lowered:
    blocker_detail_kind.append("local_open_file_limit")
    problems.append("vCluster control-plane hit the local open-file limit")
if "exit status 137" in lowered:
    blocker_detail_kind.append("local_runtime_exit_137")
    problems.append(
        "vCluster standalone process exited with status 137, consistent with local runtime resource pressure or forced termination"
    )
if (
    "procready not received" in lowered
    or "cannot exec in a stopped container" in lowered
    or "cannot exec in a stopped state" in lowered
    or "init process is not running" in lowered
    or "failedprecondition" in lowered
):
    blocker_detail_kind.append("vm_container_systemd_exit")
    problems.append(
        "vCluster standalone vm-container exited before the CLI could exec the install step"
    )
if "load balancer type services are not supported" in lowered and "insufficient privileges" in lowered:
    blocker_detail_kind.append("load_balancer_privilege_warning")
if "node couldn't join" in lowered:
    blocker_detail_kind.append("vcluster_node_join_failed")
    problems.append("vCluster standalone node could not join")
if "context was not found" in lowered:
    blocker_detail_kind.append("vcluster_context_missing")
    problems.append("vCluster kube context was not created")
if not problems:
    blocker_detail_kind.append("vcluster_bootstrap_unknown")
    tail = "\n".join(log.splitlines()[-20:])
    problems.append(f"vCluster bootstrap failed; last log lines:\n{tail}")

evidence_files = {"vcluster_bootstrap_log": log_path}
doctor = None
if doctor_path and os.path.exists(doctor_path):
    evidence_files["local_environment_doctor"] = doctor_path
    with open(doctor_path, "r", encoding="utf-8") as f:
        doctor = json.load(f)

blocker = {
    "schema_version": 1,
    "evidence_kind": "velorix_local_environment_blocker",
    "blocker_kind": "local_vind_vcluster_bootstrap",
    "blocker_detail_kind": blocker_detail_kind,
    "status": "blocked",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "stage": stage,
    "context": "local vind/vCluster",
    "trusted_for_product_complete": False,
    "no_pvc_workaround_allowed": True,
    "problems": problems,
    "evidence_files": evidence_files,
    "remediation_commands": doctor.get("remediation_commands") if isinstance(doctor, dict) else None,
    "remediation": [
        "inspect the local environment with scripts/doctor-vind-local.sh",
        "ensure the local Docker/vCluster runtime supports CRI v1 for standalone node join",
        "free local file descriptors/processes if the log reports too many open files",
        "free local Docker/Colima memory or disk capacity if the log reports exit status 137",
        "increase Docker/Colima resource limits if vCluster standalone is killed during startup",
        "delete failed local vClusters before retrying when appropriate",
        "if appropriate, delete Docker build cache with scripts/doctor-vind-local.sh --prune-build-cache --yes",
        "do not add PVCs to bypass this no-PVC product path",
    ],
}
with open(blocker_path, "w", encoding="utf-8") as f:
    json.dump(blocker, f, indent=2, sort_keys=True)
    f.write("\n")

print(
    f"local vind vCluster bootstrap is unhealthy at {stage}; this is an environment blocker, not product evidence:",
    file=sys.stderr,
)
for problem in problems:
    print(f"- {problem}", file=sys.stderr)
print(f"wrote local environment blocker evidence to {blocker_path}", file=sys.stderr)
PY
}

write_local_environment_doctor_snapshot() {
  local doctor_json="${output_dir}/local-environment-doctor.json"
  VELORIX_VIND_CLUSTER="$cluster" \
    VELORIX_K8S_NAMESPACE="$namespace" \
    VELORIX_VIND_PRODUCT_DIR="$output_dir" \
    VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE=1 \
    "$repo_root/scripts/doctor-vind-local.sh" \
      --cluster "$cluster" \
      --namespace "$namespace" \
      --output "$doctor_json" >/dev/null 2>&1 || true
  if [ -f "$doctor_json" ]; then
    printf '%s\n' "$doctor_json"
  fi
}

vcluster_bootstrap_log_is_retryable() {
  local log_file="$1"
  python3 - "$log_file" <<'PY'
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8", errors="replace") as f:
    lowered = f.read().lower()

retryable_markers = [
    "procready not received",
    "failed to start vcluster standalone: exit status 128",
    "exit status 137",
    "oci runtime exec failed",
    "node couldn't join",
]
raise SystemExit(0 if any(marker in lowered for marker in retryable_markers) else 1)
PY
}

cleanup_failed_vcluster_create_attempt() {
  vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || true
  docker rm -f "vcluster.cp.${cluster}" >/dev/null 2>&1 || true
  docker network rm "vcluster.${cluster}" >/dev/null 2>&1 || true
}

create_vcluster_with_retry() {
  local attempt=0
  local max_attempts=$((vcluster_create_retries + 1))
  local attempt_log

  while [ "$attempt" -lt "$max_attempts" ]; do
    attempt=$((attempt + 1))
    attempt_log="${output_dir}/vcluster-create-attempt-${attempt}.log"
    if vcluster create "$cluster" --driver docker --kube-config-context-name "$context" >"$attempt_log" 2>&1; then
      cp "$attempt_log" "${output_dir}/vcluster-create.log"
      return 0
    fi

    cp "$attempt_log" "${output_dir}/vcluster-create.log"
    if [ "$attempt" -ge "$max_attempts" ] || ! vcluster_bootstrap_log_is_retryable "$attempt_log"; then
      cat "$attempt_log" >&2 || true
      cleanup_failed_vcluster_create_attempt
      write_local_vcluster_bootstrap_blocker "vcluster-create" "$attempt_log"
      return 75
    fi

    cat "$attempt_log" >&2 || true
    echo "retrying vCluster create after local bootstrap transient (${attempt}/${vcluster_create_retries})" >&2
    cleanup_failed_vcluster_create_attempt
    sleep 2
  done

  cleanup_failed_vcluster_create_attempt
  write_local_vcluster_bootstrap_blocker "vcluster-create" "${output_dir}/vcluster-create.log"
  return 75
}

validate_no_pvc_namespace() {
  if [ "$managed_persistence" = "1" ]; then
    no_pvc_namespace_validated=0
    return 0
  fi
  if [ "$no_pvc_namespace_validate" != "1" ]; then
    return 0
  fi
  local pvc_json="${output_dir}/no-pvc-namespace.json"
  kubectl --context "$context" -n "$namespace" get pvc -o json >"$pvc_json"
  python3 - "$pvc_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
items = body.get("items") or []
if items:
    names = [item.get("metadata", {}).get("name", "<unnamed>") for item in items]
    raise SystemExit("no-PVC product contract violated; PVCs exist in namespace: " + ", ".join(names))
PY
  if [ "$ingest_writer_smoke" = "1" ]; then
    for service_account in velorix-ingest-writer-append velorix-ingest-writer-lease-probe; do
      if kubectl --context "$context" auth can-i create persistentvolumeclaims \
        --namespace "$namespace" \
        --as "system:serviceaccount:${namespace}:${service_account}" | grep -qx "yes"; then
        echo "no-PVC product contract violated; service account ${service_account} can create PVCs" >&2
        exit 1
      fi
    done
    for verb in get create update patch; do
      if ! kubectl --context "$context" auth can-i "$verb" leases.coordination.k8s.io \
        --namespace "$namespace" \
        --as "system:serviceaccount:${namespace}:velorix-ingest-writer-lease-probe" | grep -qx "yes"; then
        echo "lease-guarded ingest-writer service account must be able to ${verb} Kubernetes Leases" >&2
        exit 1
      fi
    done
    for service_account in velorix-ingest-writer-append velorix-ingest-writer-lease-probe; do
      for verb in get list watch; do
        if kubectl --context "$context" auth can-i "$verb" secrets \
          --namespace "$namespace" \
          --as "system:serviceaccount:${namespace}:${service_account}" | grep -qx "yes"; then
          echo "ingest-writer service account ${service_account} must not be able to ${verb} Kubernetes Secrets" >&2
          exit 1
        fi
      done
    done
  fi
  if [ "$hiqlite_deploy" = "1" ]; then
    local hiqlite_statefulset_json="${output_dir}/no-pvc-hiqlite-statefulset.json"
    kubectl --context "$context" -n "$namespace" get statefulset velorix-hiqlite -o json >"$hiqlite_statefulset_json"
    python3 - "$hiqlite_statefulset_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    statefulset = json.load(f)
spec = statefulset.get("spec") or {}
if spec.get("replicas") != 3:
    raise SystemExit("managed Hiqlite authority must run exactly three voters")
if spec.get("volumeClaimTemplates"):
    raise SystemExit("managed Hiqlite authority must not define StatefulSet volumeClaimTemplates")
template_spec = ((spec.get("template") or {}).get("spec") or {})
if template_spec.get("serviceAccountName") != "velorix-hiqlite":
    raise SystemExit("managed Hiqlite authority must use the locked-down velorix-hiqlite service account")
volumes = {volume.get("name"): volume for volume in template_spec.get("volumes") or []}
data_volume = volumes.get("data") or {}
if "emptyDir" not in data_volume:
    raise SystemExit("managed Hiqlite authority data volume must be emptyDir, not PVC-backed")
for volume in volumes.values():
    if "persistentVolumeClaim" in volume:
        raise SystemExit("managed Hiqlite authority pod template must not mount persistentVolumeClaim volumes")
containers = template_spec.get("containers") or []
if len(containers) != 1 or containers[0].get("name") != "hiqlite":
    raise SystemExit("managed Hiqlite authority must run a single hiqlite container per voter pod")
env = {item.get("name"): item for item in containers[0].get("env") or []}
if str(env.get("HQL_LEARNER_ONLY", {}).get("value", "")).lower() == "true":
    raise SystemExit("managed Hiqlite voter StatefulSet must not set HQL_LEARNER_ONLY=true")
if "HQL_SECRET_API" not in env or "HQL_SECRET_RAFT" not in env:
    raise SystemExit("managed Hiqlite authority must configure both API and Raft authentication secrets")
if "ENC_KEY_ACTIVE" not in env or "ENC_KEYS" not in env:
    raise SystemExit("managed Hiqlite authority must configure backup encryption keys")
PY
    if kubectl --context "$context" auth can-i create persistentvolumeclaims \
      --namespace "$namespace" \
      --as "system:serviceaccount:${namespace}:velorix-hiqlite" | grep -qx "yes"; then
      echo "no-PVC product contract violated; service account velorix-hiqlite can create PVCs" >&2
      exit 1
    fi
    for verb in get list watch; do
      if kubectl --context "$context" auth can-i "$verb" secrets \
        --namespace "$namespace" \
        --as "system:serviceaccount:${namespace}:velorix-hiqlite" | grep -qx "yes"; then
        echo "managed Hiqlite service account must not be able to ${verb} Kubernetes Secrets" >&2
        exit 1
      fi
    done
  fi
  no_pvc_namespace_validated=1
}

wait_for_api() {
  local url="http://127.0.0.1:${api_local_port}/healthz"
  for _ in $(seq 1 120); do
    if [ -n "$port_forward_pid" ] && ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
      echo "kubectl port-forward exited before ${url} became ready" >&2
      if [ -f "${output_dir}/port-forward.log" ]; then
        tail -50 "${output_dir}/port-forward.log" >&2 || true
      fi
      exit 1
    fi
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "velorix-api did not answer ${url}" >&2
  exit 1
}

curl_api() {
  if [ -n "$api_bearer_token" ]; then
    curl -fsS -H "authorization: Bearer ${api_bearer_token}" "$@"
  else
    curl -fsS "$@"
  fi
}

curl_admin_api() {
  if [ -n "$admin_bearer_token" ]; then
    curl -fsS -H "authorization: Bearer ${admin_bearer_token}" "$@"
  else
    curl -fsS "$@"
  fi
}

curl_api_status() {
  local output_file="$1"
  shift
  if [ -n "$api_bearer_token" ]; then
    curl -sS -o "$output_file" -w '%{http_code}' -H "authorization: Bearer ${api_bearer_token}" "$@"
  else
    curl -sS -o "$output_file" -w '%{http_code}' "$@"
  fi
}

wait_for_api_status() {
  local output_file="$1"
  local expected_status="$2"
  local label="$3"
  shift 3
  local status=""
  for _ in $(seq 1 60); do
    status="$(curl_api_status "$output_file" "$@")"
    if [ "$status" = "$expected_status" ]; then
      return 0
    fi
    sleep 1
  done
  echo "${label} did not return ${expected_status}; last status was ${status}" >&2
  cat "$output_file" >&2 || true
  return 1
}

write_api_auth_helper() {
  local auth_env="${output_dir}/api-auth.env"
  local tmp_auth_env="${auth_env}.tmp.$$"

  python3 - "$tmp_auth_env" "$api_bearer_token" "$admin_bearer_token" "$api_local_port" "$api_tls_enabled" "$api_tls_local_port" <<'PY'
import os
import shlex
import sys

path, token, admin_token, port, tls_enabled, tls_port = sys.argv[1:]
api_url = f"http://127.0.0.1:{port}"
with open(path, "w", encoding="utf-8") as f:
    f.write(f"export VELORIX_API_BEARER_TOKEN={shlex.quote(token)}\n")
    f.write(f"export VELORIX_ADMIN_BEARER_TOKEN={shlex.quote(admin_token)}\n")
    f.write(f"export VELORIX_API_URL={shlex.quote(api_url)}\n")
    if tls_enabled == "1":
        f.write(f"export VELORIX_API_TLS_URL={shlex.quote(f'https://127.0.0.1:{tls_port}')}\n")
        f.write("export VELORIX_API_TLS_CACERT=target/velorix-product/api-tls.crt\n")
    f.write('export VELORIX_API_AUTH_HEADER="authorization: Bearer ${VELORIX_API_BEARER_TOKEN}"\n')
    f.write('export VELORIX_ADMIN_AUTH_HEADER="authorization: Bearer ${VELORIX_ADMIN_BEARER_TOKEN}"\n')
os.chmod(path, 0o600)
PY
  mv "$tmp_auth_env" "$auth_env"
  chmod 600 "$auth_env"
}

check_api_auth_rejection() {
  local output_file="$1"
  local expected="$2"
  shift 2
  local status
  status="$(curl -sS -o "$output_file" -w '%{http_code}' "$@")"
  if [ "$status" != "401" ]; then
    echo "expected ${expected} to return 401, got ${status}" >&2
    cat "$output_file" >&2 || true
    exit 1
  fi
}

verify_api_auth_deployment() {
  local observed="${output_dir}/velorix-api-deployment-observed.json"
  kubectl --context "$context" -n "$namespace" get deployment velorix-api -o json >"$observed"
  python3 - "$observed" "$api_auth_mode" <<'PY'
import json
import sys

path, mode = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    deployment = json.load(f)
containers = deployment["spec"]["template"]["spec"].get("containers") or []
api = next((container for container in containers if container.get("name") == "api"), None)
if api is None:
    raise SystemExit("velorix-api Deployment has no api container")
env = {item.get("name"): item for item in api.get("env") or []}
if mode == "bearer-token":
    token = env.get("VELORIX_API_BEARER_TOKEN")
    if token is None:
        raise SystemExit("VELORIX_API_BEARER_TOKEN is missing from Deployment")
    secret_ref = ((token.get("valueFrom") or {}).get("secretKeyRef") or {})
    if secret_ref.get("name") != "velorix-api-auth" or secret_ref.get("key") != "bearer-token":
        raise SystemExit(f"VELORIX_API_BEARER_TOKEN does not reference velorix-api-auth/bearer-token: {token}")
    admin = env.get("VELORIX_ADMIN_BEARER_TOKEN")
    if admin is None:
        raise SystemExit("VELORIX_ADMIN_BEARER_TOKEN is missing from bearer-token Deployment")
    admin_secret_ref = ((admin.get("valueFrom") or {}).get("secretKeyRef") or {})
    if admin_secret_ref.get("name") != "velorix-admin-auth" or admin_secret_ref.get("key") != "admin-bearer-token":
        raise SystemExit(f"VELORIX_ADMIN_BEARER_TOKEN does not reference velorix-admin-auth/admin-bearer-token: {admin}")
    if "VELORIX_API_ALLOW_UNAUTHENTICATED_DEV" in env:
        raise SystemExit("bearer-token Deployment must not include VELORIX_API_ALLOW_UNAUTHENTICATED_DEV")
else:
    dev = env.get("VELORIX_API_ALLOW_UNAUTHENTICATED_DEV")
    if (dev or {}).get("value") != "1":
        raise SystemExit("unauthenticated-dev Deployment must include VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1")
    if "VELORIX_API_BEARER_TOKEN" in env:
        raise SystemExit("unauthenticated-dev Deployment must not include VELORIX_API_BEARER_TOKEN")
    if "VELORIX_ADMIN_BEARER_TOKEN" in env:
        raise SystemExit("unauthenticated-dev Deployment must not include VELORIX_ADMIN_BEARER_TOKEN")
PY
  api_deployment_env_verified=1
}

stop_api_port_forward() {
  if [ -n "$port_forward_pid" ] && kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
  fi
  port_forward_pid=""
}

stop_api_tls_port_forward() {
  if [ -n "$api_tls_port_forward_pid" ] && kill -0 "$api_tls_port_forward_pid" >/dev/null 2>&1; then
    kill "$api_tls_port_forward_pid" >/dev/null 2>&1 || true
    wait "$api_tls_port_forward_pid" >/dev/null 2>&1 || true
  fi
  api_tls_port_forward_pid=""
}

ensure_local_api_port_free() {
  ensure_local_port_free "$api_local_port"
}

ensure_local_port_free() {
  python3 - "$1" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(0.2)
try:
    occupied = sock.connect_ex(("127.0.0.1", port)) == 0
finally:
    sock.close()
if occupied:
    raise SystemExit(f"127.0.0.1:{port} is already accepting connections; refusing to smoke-test a potentially stale API")
PY
}

wait_for_forward_url() {
  local pid="$1"
  local url="$2"
  local log_file="$3"
  shift 3
  for _ in $(seq 1 120); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "kubectl port-forward exited before ${url} became ready" >&2
      tail -50 "$log_file" >&2 || true
      exit 1
    fi
    if curl -fsS "$@" "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "port-forwarded endpoint did not answer ${url}" >&2
  tail -50 "$log_file" >&2 || true
  exit 1
}

select_two_ready_api_pods() {
  python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
ready = []
for item in body.get("items") or []:
    conditions = {
        condition.get("type"): condition.get("status")
        for condition in item.get("status", {}).get("conditions") or []
    }
    if item.get("status", {}).get("phase") == "Running" and conditions.get("Ready") == "True":
        ready.append(item.get("metadata", {}).get("name"))
if len(ready) < 2:
    raise SystemExit(f"need at least two ready velorix-api pods, got {ready}")
print(ready[0], ready[1])
PY
}

is_api_port_forward_pid() {
  local pid="$1"
  local command
  case "$pid" in
    '' | *[!0-9]*) return 1 ;;
  esac
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  case "$command" in
    *kubectl*port-forward*"svc/velorix-api"*"$api_local_port:8080"*) return 0 ;;
    *kubectl*port-forward*"pod/velorix-api-"*"$api_local_port:8080"*) return 0 ;;
    *) return 1 ;;
  esac
}

stop_existing_api_port_forward_pid_file() {
  local pid_file="${output_dir}/port-forward.pid"
  local old_pid
  if [ ! -f "$pid_file" ]; then
    return 0
  fi
  old_pid="$(cat "$pid_file")"
  if is_api_port_forward_pid "$old_pid" && kill -0 "$old_pid" >/dev/null 2>&1; then
    kill "$old_pid" >/dev/null 2>&1 || true
  else
    echo "ignoring stale port-forward pid file: ${pid_file}" >&2
  fi
  rm -f "$pid_file"
}

generate_api_tls_secret() {
  if [ "$api_tls_enabled" != "1" ]; then
    return 0
  fi
  if ! command -v openssl >/dev/null 2>&1; then
    echo "VELORIX_API_TLS_ENABLED=1 requires openssl to generate the local vind TLS certificate" >&2
    exit 64
  fi

  local cert="${output_dir}/api-tls.crt"
  local key="${output_dir}/api-tls.key"
  local config="${output_dir}/api-tls-openssl.cnf"
  cat >"$config" <<EOF
[req]
default_bits = 2048
prompt = no
distinguished_name = dn
x509_extensions = v3_req

[dn]
CN = velorix-api.local

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = velorix-api
DNS.3 = velorix-api.${namespace}.svc
IP.1 = 127.0.0.1
EOF
  openssl req -x509 -newkey rsa:2048 -nodes -days 7 \
    -keyout "$key" -out "$cert" -config "$config" >/dev/null 2>&1
  chmod 600 "$key" "$cert"
  api_tls_certificate_sha256="$(sha256_file "$cert")"

  local cert_b64
  local key_b64
  cert_b64="$(base64_file "$cert")"
  key_b64="$(base64_file "$key")"
  cat >"${output_dir}/velorix-api-tls.yaml" <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: velorix-api-tls
  namespace: ${namespace}
type: kubernetes.io/tls
data:
  tls.crt: ${cert_b64}
  tls.key: ${key_b64}
EOF
  kubectl --context "$context" apply -f "${output_dir}/velorix-api-tls.yaml"
}

run_meta_smoke_job() {
  local expected_meta_backend
  local expected_meta_production_safe
  local standing_runtime_adversarial_arg=""
  local catalog_probe_id
  meta_smoke_invocation=$((meta_smoke_invocation + 1))
  catalog_probe_id="${run_id}-meta-smoke-${meta_smoke_invocation}"
  expected_meta_backend="$(normalized_meta_backend "$meta_backend")"
  expected_meta_production_safe="false"
  if [ "$standing_runtime_fencing" = "required" ]; then
    expected_meta_production_safe="true"
  fi
  if [ "$standing_runtime_fencing" != "unsafe-dev-only" ]; then
    standing_runtime_adversarial_arg="            - --run-standing-runtime-fencing-adversarial"
  fi

  cat >"${output_dir}/velorix-meta-smoke.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: velorix-meta-smoke
  namespace: ${namespace}
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: velorix-meta-smoke
        velorix.dev/run-id: "${run_id}"
    spec:
$(image_pull_secrets_yaml)
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      restartPolicy: Never
      containers:
        - name: smoke
          image: ${meta_image}
          imagePullPolicy: ${meta_image_pull_policy}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_META_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: velorix-meta-auth
                  key: bearer-token
          args:
            - smoke
            - --endpoint
            - http://velorix-meta:9090
            - --expect-backend
            - "${expected_meta_backend}"
            - --expect-auth-enforced
            - "true"
            - --expect-production-multi-writer-safe
            - "${expected_meta_production_safe}"
            - --catalog-probe-id
            - "${catalog_probe_id}"
${standing_runtime_adversarial_arg}
EOF
  kubectl --context "$context" -n "$namespace" delete job velorix-meta-smoke --ignore-not-found
  kubectl --context "$context" apply -f "${output_dir}/velorix-meta-smoke.yaml"
  wait_for_job_complete velorix-meta-smoke
  meta_fencing_adversarial_smoke_log="${output_dir}/velorix-meta-smoke.log"
  kubectl --context "$context" -n "$namespace" logs job/velorix-meta-smoke \
    >"$meta_fencing_adversarial_smoke_log"
  if [ "$standing_runtime_fencing" != "unsafe-dev-only" ]; then
    if grep -q "velorix-meta standing runtime adversarial smoke ok: .*stale_checkpoint_pointer_publish_conflicted=true" "$meta_fencing_adversarial_smoke_log"; then
      meta_fencing_adversarial_smoke_passed=1
    else
      echo "metadata standing runtime adversarial smoke did not produce pass evidence" >&2
      cat "$meta_fencing_adversarial_smoke_log" >&2 || true
      exit 1
    fi
  fi
}

run_external_s3_validation_job() {
  if [ "$object_store_mode" != "external-s3" ] || [ "$external_s3_validate" != "1" ]; then
    return 0
  fi

  external_s3_validation_prefix="${s3_prefix%/}"
  if [ -n "$external_s3_validation_prefix" ]; then
    external_s3_validation_key="${external_s3_validation_prefix}/_velorix_external_s3_validation/${run_id}.txt"
  else
    external_s3_validation_prefix="_velorix_external_s3_validation"
    external_s3_validation_key="${external_s3_validation_prefix}/${run_id}.txt"
  fi
  cat >"${output_dir}/external-s3-validate.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: velorix-external-s3-validate
  namespace: ${namespace}
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: velorix-external-s3-validate
        velorix.dev/run-id: "${run_id}"
    spec:
      restartPolicy: Never
      containers:
        - name: aws
          image: amazon/aws-cli:2.17.36
          imagePullPolicy: IfNotPresent
          volumeMounts:
            - name: work
              mountPath: /work
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: AWS_DEFAULT_REGION
              value: "${aws_region}"
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -eu
              if [ "${s3_force_path_style}" = "1" ]; then
                aws configure set default.s3.addressing_style path
              fi
              validation_file=/work/velorix-external-s3-validation.txt
              printf '%s\n' 'velorix external-s3 validation ${run_id}' > "\${validation_file}"
              aws --endpoint-url "${s3_endpoint}" s3api head-bucket --bucket "${bucket}"
              aws --endpoint-url "${s3_endpoint}" s3api put-object --bucket "${bucket}" --key "${external_s3_validation_key}" --body "\${validation_file}" >/work/put-object.json
              aws --endpoint-url "${s3_endpoint}" s3api get-object --bucket "${bucket}" --key "${external_s3_validation_key}" /work/read-back.txt >/work/get-object.json
              cmp "\${validation_file}" /work/read-back.txt
              aws --endpoint-url "${s3_endpoint}" s3api list-objects-v2 --bucket "${bucket}" --prefix "${external_s3_validation_key}" --max-keys 1 --query 'Contents[].Key' --output text | tr '\t' '\n' | grep -Fx "${external_s3_validation_key}" >/dev/null
              aws --endpoint-url "${s3_endpoint}" s3api delete-object --bucket "${bucket}" --key "${external_s3_validation_key}" >/work/delete-object.json
              echo "velorix external-s3 validation ok bucket=${bucket} prefix=${external_s3_validation_prefix} key=${external_s3_validation_key}"
      volumes:
        - name: work
          emptyDir: {}
EOF

  kubectl --context "$context" -n "$namespace" delete job velorix-external-s3-validate --ignore-not-found
  kubectl --context "$context" apply -f "${output_dir}/external-s3-validate.yaml"
  wait_for_job_complete velorix-external-s3-validate
  kubectl --context "$context" -n "$namespace" logs job/velorix-external-s3-validate --tail=200 >"${output_dir}/external-s3-validate.log" || true
  kubectl --context "$context" -n "$namespace" get job velorix-external-s3-validate -o json >"${output_dir}/external-s3-validate-job.json"
  external_s3_bucket_validated=1
  external_s3_prefix_validated=1
}

run_ingest_writer_smoke_job() {
  local schema_fingerprint
  local payload_file="${output_dir}/ingest-writer-scores-payload.vlxingest"
  local payload_b64
  local rows_json
  local start_offset
  local job_log="${output_dir}/ingest-writer-job-log.json"
  local job_json="${output_dir}/ingest-writer-job.json"
  local pods_json="${output_dir}/ingest-writer-pods.json"

  schema_fingerprint="$(python3 - "${output_dir}/scores-relation.json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
print(body["schema_fingerprint"])
PY
)"
  rows_json='[{"user_id":"u3","score":11,"delta":1},{"user_id":"u2","score":-4,"delta":1}]'
  start_offset="$(ingest_writer_run_offset_base)"
  cargo run --locked -p velorix-ingest-writer --quiet -- encode-default-scores-payload \
    --output "$payload_file" \
    --schema-fingerprint "$schema_fingerprint" \
    --stream-id scores \
    --partition-id 0 \
    --start-offset-inclusive "$start_offset" \
    --rows-json "$rows_json"
  payload_b64="$(base64_file "$payload_file")"

  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: velorix-ingest-writer-smoke-payload
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-smoke
    velorix.dev/run-id: "${run_id}"
binaryData:
  payload.vlxingest: ${payload_b64}
EOF

  cat >"${output_dir}/ingest-writer-job.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${ingest_writer_job_name}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-smoke
    velorix.dev/run-id: "${run_id}"
  annotations:
    velorix.dev/run-id: "${run_id}"
    velorix.dev/image-tag: "${ingest_writer_image}"
    velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: velorix-ingest-writer-smoke
        velorix.dev/run-id: "${run_id}"
    spec:
      serviceAccountName: velorix-ingest-writer-lease-probe
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      restartPolicy: Never
      containers:
        - name: ingest-writer
          image: ${ingest_writer_image}
          imagePullPolicy: ${ingest_writer_image_pull_policy}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID
              value: "${s3_authority_store_id}"
            - name: VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE
              value: "velorix"
            - name: VELORIX_INGEST_WRITER_OPERATOR_ID
              value: "velorix-product-script"
            - name: VELORIX_INGEST_WRITER_ID
              value: "smoke-${run_id}"
            - name: VELORIX_INGEST_WRITER_PAYLOAD_FILE
              value: "/var/lib/velorix-ingest-writer/payload.vlxingest"
            - name: VELORIX_INGEST_WRITER_NAMESPACE
              value: "${namespace}"
            - name: VELORIX_INGEST_WRITER_LEASE_VIEW_ID
              value: "positive_scores_by_user"
            - name: VELORIX_INGEST_WRITER_LEASE_STREAM_ID
              value: "scores"
            - name: VELORIX_INGEST_WRITER_LEASE_PARTITION_ID
              value: "0"
            - name: VELORIX_INGEST_WRITER_LEASE_OWNER_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.uid
            - name: VELORIX_INGEST_WRITER_LEASE_TTL_MS
              value: "60000"
            - name: VELORIX_S3_COMPAT
              value: "1"
            - name: VELORIX_S3_FORCE_PATH_STYLE
              value: "${s3_force_path_style}"
            - name: AWS_ENDPOINT_URL
              value: "${s3_endpoint}"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: VELORIX_S3_BUCKET
              value: "${bucket}"
            - name: VELORIX_S3_PREFIX
              value: "${s3_prefix}"
          volumeMounts:
            - name: payload
              mountPath: /var/lib/velorix-ingest-writer
              readOnly: true
      volumes:
        - name: payload
          configMap:
            name: velorix-ingest-writer-smoke-payload
EOF

  kubectl --context "$context" -n "$namespace" delete job "$ingest_writer_job_name" --ignore-not-found
  kubectl --context "$context" apply -f "${output_dir}/ingest-writer-job.yaml"
  wait_for_job_complete "$ingest_writer_job_name"

  kubectl --context "$context" -n "$namespace" logs "job/${ingest_writer_job_name}" >"$job_log"
  kubectl --context "$context" -n "$namespace" get "job/${ingest_writer_job_name}" -o json >"$job_json"
  kubectl --context "$context" -n "$namespace" get pods -l "job-name=${ingest_writer_job_name}" -o json >"$pods_json"
  cp "$job_log" "${output_dir}/${ingest_writer_job_name}-log.json"
  cp "$job_json" "${output_dir}/${ingest_writer_job_name}.json"
  cp "$pods_json" "${output_dir}/${ingest_writer_job_name}-pods.json"
  python3 - "$job_log" "$job_json" "$pods_json" "$ingest_writer_image" <<'PY'
import json
import sys

log_path, job_path, pods_path, expected_image = sys.argv[1:]
with open(log_path, "r", encoding="utf-8") as f:
    artifact = json.load(f)
if artifact.get("evidence_kind") != "ingest_writer_lease_guarded_append_probe":
    raise SystemExit(f"unexpected ingest-writer evidence kind: {artifact}")
if artifact.get("status") != "pass":
    raise SystemExit(f"ingest-writer evidence did not pass: {artifact}")
if artifact.get("outcome") != "appended":
    raise SystemExit(f"unexpected ingest-writer append outcome: {artifact}")
if artifact.get("lease_held_through_append") is not True:
    raise SystemExit(f"ingest-writer append did not prove lease ownership through append: {artifact}")
if artifact.get("commit_guard_enforced") is not True:
    raise SystemExit(f"ingest-writer append did not enforce lease ownership inside the commit path: {artifact}")
if artifact.get("admission_commit_guard_bound") is not True:
    raise SystemExit(f"ingest-writer append did not bind admission reservation to the commit guard: {artifact}")
binding = artifact.get("admission_commit_guard_binding") or {}
if binding.get("binding_kind") != "kubernetes_partition_lease" or not binding.get("subject"):
    raise SystemExit(f"ingest-writer append did not report a Kubernetes lease admission binding: {artifact}")
with open(job_path, "r", encoding="utf-8") as f:
    job = json.load(f)
if job.get("status", {}).get("succeeded") != 1:
    raise SystemExit(f"ingest-writer job did not record one success: {job.get('status')}")
with open(pods_path, "r", encoding="utf-8") as f:
    pods = json.load(f)
items = pods.get("items") or []
if len(items) != 1:
    raise SystemExit(f"expected exactly one ingest-writer pod, got {len(items)}")
container = (items[0].get("spec", {}).get("containers") or [{}])[0]
if container.get("image") != expected_image:
    raise SystemExit(f"ingest-writer pod image mismatch: {container.get('image')} != {expected_image}")
if items[0].get("status", {}).get("phase") != "Succeeded":
    raise SystemExit(f"ingest-writer pod did not succeed: {items[0].get('status')}")
PY

  ingest_writer_job_completed=1
  ingest_writer_append_outcome="$(python3 - "$job_log" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    print(json.load(f)["outcome"])
PY
)"
  ingest_writer_object_key="$(python3 - "$job_log" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    print(json.load(f)["descriptor"]["object_key"])
PY
  )"
}

run_ingest_writer_lifecycle_append_job() {
  local job_name="$1"
  local writer_id="$2"
  local start_offset="$3"
  local rows_json="$4"
  local expected_outcome="$5"
  local schema_fingerprint="$6"
  local payload_file="${output_dir}/${job_name}.vlxingest"
  local payload_b64
  local config_map="${job_name}-payload"
  local job_log="${output_dir}/${job_name}-log.json"
  local job_json="${output_dir}/${job_name}.json"
  local pods_json="${output_dir}/${job_name}-pods.json"

  cargo run --locked -p velorix-ingest-writer --quiet -- encode-default-scores-payload \
    --output "$payload_file" \
    --schema-fingerprint "$schema_fingerprint" \
    --stream-id scores \
    --partition-id 0 \
    --start-offset-inclusive "$start_offset" \
    --rows-json "$rows_json"
  payload_b64="$(base64_file "$payload_file")"

  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${config_map}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
binaryData:
  payload.vlxingest: ${payload_b64}
EOF

  cat >"${output_dir}/${job_name}.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
  annotations:
    velorix.dev/run-id: "${run_id}"
    velorix.dev/image-tag: "${ingest_writer_image}"
    velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: velorix-ingest-writer-lifecycle
        velorix.dev/run-id: "${run_id}"
    spec:
      serviceAccountName: velorix-ingest-writer-lease-probe
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      restartPolicy: Never
      containers:
        - name: ingest-writer
          image: ${ingest_writer_image}
          imagePullPolicy: ${ingest_writer_image_pull_policy}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID
              value: "${s3_authority_store_id}"
            - name: VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE
              value: "velorix"
            - name: VELORIX_INGEST_WRITER_OPERATOR_ID
              value: "velorix-product-script"
            - name: VELORIX_INGEST_WRITER_ID
              value: "${writer_id}"
            - name: VELORIX_INGEST_WRITER_PAYLOAD_FILE
              value: "/var/lib/velorix-ingest-writer/payload.vlxingest"
            - name: VELORIX_INGEST_WRITER_NAMESPACE
              value: "${namespace}"
            - name: VELORIX_INGEST_WRITER_LEASE_VIEW_ID
              value: "positive_scores_by_user"
            - name: VELORIX_INGEST_WRITER_LEASE_STREAM_ID
              value: "scores"
            - name: VELORIX_INGEST_WRITER_LEASE_PARTITION_ID
              value: "0"
            - name: VELORIX_INGEST_WRITER_LEASE_OWNER_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.uid
            - name: VELORIX_INGEST_WRITER_LEASE_TTL_MS
              value: "60000"
            - name: VELORIX_S3_COMPAT
              value: "1"
            - name: VELORIX_S3_FORCE_PATH_STYLE
              value: "${s3_force_path_style}"
            - name: AWS_ENDPOINT_URL
              value: "${s3_endpoint}"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: VELORIX_S3_BUCKET
              value: "${bucket}"
            - name: VELORIX_S3_PREFIX
              value: "${s3_prefix}"
          volumeMounts:
            - name: payload
              mountPath: /var/lib/velorix-ingest-writer
              readOnly: true
      volumes:
        - name: payload
          configMap:
            name: ${config_map}
EOF

  kubectl --context "$context" -n "$namespace" delete job "$job_name" --ignore-not-found
  kubectl --context "$context" apply -f "${output_dir}/${job_name}.yaml"
  case "$expected_outcome" in
    conflict)
      wait_for_job_failed "$job_name"
      ;;
    appended)
      wait_for_job_complete "$job_name"
      ;;
    *)
      echo "unknown ingest-writer lifecycle expected outcome: ${expected_outcome}" >&2
      exit 64
      ;;
  esac

  kubectl --context "$context" -n "$namespace" logs "job/${job_name}" >"$job_log" || true
  kubectl --context "$context" -n "$namespace" get "job/${job_name}" -o json >"$job_json"
  kubectl --context "$context" -n "$namespace" get pods -l "job-name=${job_name}" -o json >"$pods_json"

  python3 - "$job_log" "$job_json" "$pods_json" "$ingest_writer_image" "$expected_outcome" "$writer_id" "$start_offset" "$rows_json" <<'PY'
import json
import sys
from datetime import datetime, timezone

log_path, job_path, pods_path, expected_image, expected_outcome, writer_id, start_offset, rows_json = sys.argv[1:]
with open(job_path, "r", encoding="utf-8") as f:
    job = json.load(f)
with open(pods_path, "r", encoding="utf-8") as f:
    pods = json.load(f)
items = pods.get("items") or []
if len(items) != 1:
    raise SystemExit(f"expected exactly one lifecycle ingest-writer pod, got {len(items)}")
container = (items[0].get("spec", {}).get("containers") or [{}])[0]
if container.get("image") != expected_image:
    raise SystemExit(f"ingest-writer lifecycle pod image mismatch: {container.get('image')} != {expected_image}")
env = {item.get("name"): item.get("value") for item in container.get("env") or []}
if env.get("VELORIX_INGEST_WRITER_ID") != writer_id:
    raise SystemExit(f"ingest-writer lifecycle writer id mismatch: {env.get('VELORIX_INGEST_WRITER_ID')} != {writer_id}")
if expected_outcome == "conflict":
    if job.get("status", {}).get("failed") != 1:
        raise SystemExit(f"expected failed lifecycle conflict job, got {job.get('status')}")
    with open(log_path, "r", encoding="utf-8") as f:
        log = f.read()
    if "conflicted before append" not in log and "fresh append outcome, got conflict" not in log:
        raise SystemExit(f"expected conflict log, got: {log}")
    rows = json.loads(rows_json)
    if not isinstance(rows, list) or not rows:
        raise SystemExit(f"overlap conflict probe requires nonempty rows_json: {rows_json}")
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "schema_version": 1,
                "evidence_kind": "ingest_writer_lifecycle_overlap_conflict_probe",
                "status": "pass",
                "outcome": "conflict-rejected",
                "writer_id": writer_id,
                "stream_id": "scores",
                "partition_id": 0,
                "start_offset_inclusive": int(start_offset),
                "attempted_row_count": len(rows),
                "multi_pod_overlap_conflict_rejected": True,
                "conflicting_append_rejected_before_append": True,
                "append_completed": False,
                "conflict_log_observed": True,
                "raw_conflict_log": log,
                "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            },
            f,
            sort_keys=True,
        )
        f.write("\n")
else:
    if job.get("status", {}).get("succeeded") != 1:
        raise SystemExit(f"expected successful lifecycle append job, got {job.get('status')}")
    with open(log_path, "r", encoding="utf-8") as f:
        artifact = json.load(f)
    if artifact.get("evidence_kind") != "ingest_writer_lease_guarded_append_probe":
        raise SystemExit(f"unexpected lifecycle evidence kind: {artifact}")
    if artifact.get("status") != "pass":
        raise SystemExit(f"lifecycle append evidence did not pass: {artifact}")
    if artifact.get("outcome") != expected_outcome:
        raise SystemExit(f"expected lifecycle append outcome {expected_outcome}, got: {artifact}")
    if artifact.get("lease_held_through_append") is not True:
        raise SystemExit(f"lifecycle append did not prove lease ownership through append: {artifact}")
    if artifact.get("commit_guard_enforced") is not True:
        raise SystemExit(f"lifecycle append did not enforce lease ownership inside the commit path: {artifact}")
    if artifact.get("admission_commit_guard_bound") is not True:
        raise SystemExit(f"lifecycle append did not bind admission reservation to the commit guard: {artifact}")
    binding = artifact.get("admission_commit_guard_binding") or {}
    if binding.get("binding_kind") != "kubernetes_partition_lease" or not binding.get("subject"):
        raise SystemExit(f"lifecycle append did not report a Kubernetes lease admission binding: {artifact}")
PY
}

release_ingest_writer_smoke_partition_lease() {
  local leases_json="${output_dir}/ingest-writer-smoke-leases-before-lifecycle.json"
  local lease_name
  kubectl --context "$context" -n "$namespace" get leases -o json >"$leases_json"
  while IFS= read -r lease_name; do
    if [ -n "$lease_name" ]; then
      kubectl --context "$context" -n "$namespace" delete lease "$lease_name" --ignore-not-found >/dev/null
    fi
  done < <(python3 - "$leases_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
for item in body.get("items") or []:
    annotations = item.get("metadata", {}).get("annotations") or {}
    if (
        annotations.get("control.velorix.io/view-id") == "positive_scores_by_user"
        and annotations.get("control.velorix.io/stream-id") == "scores"
        and annotations.get("control.velorix.io/partition-id") == "0"
    ):
        print(item.get("metadata", {}).get("name", ""))
PY
)
}

run_ingest_writer_lifecycle_crash_restart_probe_job() {
  local job_name="$1"
  local writer_id="$2"
  local start_offset="$3"
  local rows_json="$4"
  local schema_fingerprint="$5"
  local payload_file="${output_dir}/${job_name}.vlxingest"
  local payload_b64
  local config_map="${job_name}-payload"
  local job_log="${output_dir}/${job_name}-log.json"
  local job_json="${output_dir}/${job_name}.json"
  local pods_json="${output_dir}/${job_name}-pods.json"

  cargo run --locked -p velorix-ingest-writer --quiet -- encode-default-scores-payload \
    --output "$payload_file" \
    --schema-fingerprint "$schema_fingerprint" \
    --stream-id scores \
    --partition-id 0 \
    --start-offset-inclusive "$start_offset" \
    --rows-json "$rows_json"
  payload_b64="$(base64_file "$payload_file")"

  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${config_map}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
binaryData:
  payload.vlxingest: ${payload_b64}
EOF

  cat >"${output_dir}/${job_name}.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
  annotations:
    velorix.dev/run-id: "${run_id}"
    velorix.dev/image-tag: "${ingest_writer_image}"
    velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: velorix-ingest-writer-lifecycle
        velorix.dev/run-id: "${run_id}"
    spec:
      serviceAccountName: velorix-ingest-writer-append
      automountServiceAccountToken: false
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      restartPolicy: Never
      containers:
        - name: ingest-writer
          image: ${ingest_writer_image}
          imagePullPolicy: ${ingest_writer_image_pull_policy}
          args:
            - probe-ingest-admission-crash-restart
            - --payload-file
            - /var/lib/velorix-ingest-writer/payload.vlxingest
            - --authority-store-id
            - "${s3_authority_store_id}"
            - --authority-namespace
            - velorix
            - --operator-id
            - velorix-product-script
            - --writer-id
            - "${writer_id}"
            - --json
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_S3_COMPAT
              value: "1"
            - name: VELORIX_S3_FORCE_PATH_STYLE
              value: "${s3_force_path_style}"
            - name: AWS_ENDPOINT_URL
              value: "${s3_endpoint}"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: VELORIX_S3_BUCKET
              value: "${bucket}"
            - name: VELORIX_S3_PREFIX
              value: "${s3_prefix}"
          volumeMounts:
            - name: payload
              mountPath: /var/lib/velorix-ingest-writer
              readOnly: true
      volumes:
        - name: payload
          configMap:
            name: ${config_map}
EOF

  kubectl --context "$context" -n "$namespace" delete job "$job_name" --ignore-not-found
  kubectl --context "$context" apply -f "${output_dir}/${job_name}.yaml"
  wait_for_job_complete "$job_name"

  kubectl --context "$context" -n "$namespace" logs "job/${job_name}" >"$job_log"
  kubectl --context "$context" -n "$namespace" get "job/${job_name}" -o json >"$job_json"
  kubectl --context "$context" -n "$namespace" get pods -l "job-name=${job_name}" -o json >"$pods_json"

  python3 - "$job_log" "$job_json" "$pods_json" "$ingest_writer_image" "$writer_id" "$start_offset" "$rows_json" <<'PY'
import json
import sys

log_path, job_path, pods_path, expected_image, writer_id, start_offset, rows_json = sys.argv[1:]
start_offset = int(start_offset)
expected_end_offset = start_offset + len(json.loads(rows_json))
with open(job_path, "r", encoding="utf-8") as f:
    job = json.load(f)
with open(pods_path, "r", encoding="utf-8") as f:
    pods = json.load(f)
items = pods.get("items") or []
if len(items) != 1:
    raise SystemExit(f"expected exactly one crash/restart probe pod, got {len(items)}")
container = (items[0].get("spec", {}).get("containers") or [{}])[0]
if container.get("image") != expected_image:
    raise SystemExit(f"crash/restart probe image mismatch: {container.get('image')} != {expected_image}")
if job.get("status", {}).get("succeeded") != 1:
    raise SystemExit(f"expected successful crash/restart probe job, got {job.get('status')}")
with open(log_path, "r", encoding="utf-8") as f:
    artifact = json.load(f)
if artifact.get("evidence_kind") != "ingest_writer_admission_crash_restart_probe":
    raise SystemExit(f"unexpected crash/restart evidence kind: {artifact}")
if artifact.get("status") != "pass":
    raise SystemExit(f"crash/restart probe did not pass: {artifact}")
if artifact.get("writer_id") != writer_id:
    raise SystemExit(f"crash/restart writer id mismatch: {artifact}")
if artifact.get("reserve_outcome") != "reserved":
    raise SystemExit(f"crash/restart probe did not create a fresh orphan admission: {artifact}")
if artifact.get("append_outcome") != "appended":
    raise SystemExit(f"crash/restart recovered append did not commit a fresh batch: {artifact}")
if artifact.get("orphan_admission_created") is not True:
    raise SystemExit(f"crash/restart probe did not report orphan admission creation: {artifact}")
if artifact.get("restart_reconstructed_active_admission") is not True:
    raise SystemExit(f"crash/restart probe did not report startup reconstruction: {artifact}")
if artifact.get("recovered_append_completed") is not True:
    raise SystemExit(f"crash/restart probe did not report recovered append completion: {artifact}")
if artifact.get("committed_admission_not_expirable") is not True:
    raise SystemExit(f"crash/restart probe allowed committed admission expiry: {artifact}")
if artifact.get("after_restart_active_admission_records", 0) <= artifact.get("before_restart_active_admission_records", 0):
    raise SystemExit(f"crash/restart startup reconstruction did not observe the orphan admission: {artifact}")
descriptor = artifact.get("descriptor") or {}
if descriptor.get("stream_id") != "scores" or descriptor.get("partition_id") != 0:
    raise SystemExit(f"crash/restart descriptor used an unexpected stream/partition: {artifact}")
if descriptor.get("start_offset_inclusive") != start_offset or descriptor.get("end_offset_exclusive") != expected_end_offset:
    raise SystemExit(f"crash/restart descriptor used an unexpected offset range: {artifact}")
PY
}

run_ingest_writer_lifecycle_lease_loss_probe_job() {
  local job_name="$1"
  local writer_id="$2"
  local start_offset="$3"
  local rows_json="$4"
  local schema_fingerprint="$5"
  local payload_file="${output_dir}/${job_name}.vlxingest"
  local payload_b64
  local config_map="${job_name}-payload"
  local job_log="${output_dir}/${job_name}-log.json"
  local job_json="${output_dir}/${job_name}.json"
  local pods_json="${output_dir}/${job_name}-pods.json"

  cargo run --locked -p velorix-ingest-writer --quiet -- encode-default-scores-payload \
    --output "$payload_file" \
    --schema-fingerprint "$schema_fingerprint" \
    --stream-id scores \
    --partition-id 0 \
    --start-offset-inclusive "$start_offset" \
    --rows-json "$rows_json"
  payload_b64="$(base64_file "$payload_file")"

  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${config_map}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
binaryData:
  payload.vlxingest: ${payload_b64}
EOF

  cat >"${output_dir}/${job_name}.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
  annotations:
    velorix.dev/run-id: "${run_id}"
    velorix.dev/image-tag: "${ingest_writer_image}"
    velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: velorix-ingest-writer-lifecycle
        velorix.dev/run-id: "${run_id}"
    spec:
      serviceAccountName: velorix-ingest-writer-lease-probe
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      restartPolicy: Never
      containers:
        - name: ingest-writer
          image: ${ingest_writer_image}
          imagePullPolicy: ${ingest_writer_image_pull_policy}
          args:
            - probe-lease-loss-during-reservation
            - --payload-file
            - /var/lib/velorix-ingest-writer/payload.vlxingest
            - --authority-store-id
            - "${s3_authority_store_id}"
            - --authority-namespace
            - velorix
            - --operator-id
            - velorix-product-script
            - --writer-id
            - "${writer_id}"
            - --lease-namespace
            - "${namespace}"
            - --lease-view-id
            - positive_scores_by_user
            - --lease-stream-id
            - scores
            - --lease-partition-id
            - "0"
            - --owner-id
            - "${writer_id}"
            - --ttl-ms
            - "60000"
            - --json
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_S3_COMPAT
              value: "1"
            - name: VELORIX_S3_FORCE_PATH_STYLE
              value: "${s3_force_path_style}"
            - name: AWS_ENDPOINT_URL
              value: "${s3_endpoint}"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: VELORIX_S3_BUCKET
              value: "${bucket}"
            - name: VELORIX_S3_PREFIX
              value: "${s3_prefix}"
          volumeMounts:
            - name: payload
              mountPath: /var/lib/velorix-ingest-writer
              readOnly: true
      volumes:
        - name: payload
          configMap:
            name: ${config_map}
EOF

  kubectl --context "$context" -n "$namespace" delete job "$job_name" --ignore-not-found
  kubectl --context "$context" apply -f "${output_dir}/${job_name}.yaml"
  wait_for_job_complete "$job_name"

  kubectl --context "$context" -n "$namespace" logs "job/${job_name}" >"$job_log"
  kubectl --context "$context" -n "$namespace" get "job/${job_name}" -o json >"$job_json"
  kubectl --context "$context" -n "$namespace" get pods -l "job-name=${job_name}" -o json >"$pods_json"

  python3 - "$job_log" "$job_json" "$pods_json" "$ingest_writer_image" "$writer_id" "$start_offset" "$rows_json" <<'PY'
import json
import sys

log_path, job_path, pods_path, expected_image, writer_id, start_offset, rows_json = sys.argv[1:]
start_offset = int(start_offset)
expected_end_offset = start_offset + len(json.loads(rows_json))
with open(job_path, "r", encoding="utf-8") as f:
    job = json.load(f)
with open(pods_path, "r", encoding="utf-8") as f:
    pods = json.load(f)
items = pods.get("items") or []
if len(items) != 1:
    raise SystemExit(f"expected exactly one lease-loss probe pod, got {len(items)}")
container = (items[0].get("spec", {}).get("containers") or [{}])[0]
if container.get("image") != expected_image:
    raise SystemExit(f"lease-loss probe image mismatch: {container.get('image')} != {expected_image}")
if job.get("status", {}).get("succeeded") != 1:
    raise SystemExit(f"expected successful lease-loss probe job, got {job.get('status')}")
with open(log_path, "r", encoding="utf-8") as f:
    artifact = json.load(f)
if artifact.get("evidence_kind") != "ingest_writer_lease_loss_during_reservation_probe":
    raise SystemExit(f"unexpected lease-loss evidence kind: {artifact}")
if artifact.get("status") != "pass":
    raise SystemExit(f"lease-loss probe did not pass: {artifact}")
if artifact.get("writer_id") != writer_id:
    raise SystemExit(f"lease-loss writer id mismatch: {artifact}")
for field in [
    "before_admission_lease_verified",
    "lease_released_before_commit",
    "commit_guard_rejected_before_batch_commit",
    "batch_object_absent_after_rejection",
    "admission_commit_guard_bound",
    "restart_reconstructed_active_admission",
    "target_admission_rejected_overlapping_reservation_before_expiry",
    "orphan_expired",
    "expired_target_rejected_original_retry",
]:
    if artifact.get(field) is not True:
        raise SystemExit(f"lease-loss probe requires {field}=true: {artifact}")
binding = artifact.get("admission_commit_guard_binding") or {}
if binding.get("binding_kind") != "kubernetes_partition_lease":
    raise SystemExit(f"lease-loss probe did not bind admission to Kubernetes lease: {artifact}")
if binding.get("owner_id") != artifact.get("owner_id") or binding.get("owner_epoch") != artifact.get("owner_epoch"):
    raise SystemExit(f"lease-loss admission binding is not bound to the rejected owner epoch: {artifact}")
descriptor = artifact.get("descriptor") or {}
if descriptor.get("stream_id") != "scores" or descriptor.get("partition_id") != 0:
    raise SystemExit(f"lease-loss descriptor used an unexpected stream/partition: {artifact}")
if descriptor.get("start_offset_inclusive") != start_offset or descriptor.get("end_offset_exclusive") != expected_end_offset:
    raise SystemExit(f"lease-loss descriptor used an unexpected offset range: {artifact}")
PY
}

run_ingest_writer_lifecycle_handoff_probe_job() {
  local job_name="$1"
  local schema_fingerprint="$2"
  local job_log="${output_dir}/${job_name}-log.json"
  local owner_a="lifecycle-handoff-a-${run_id}"
  local owner_b="lifecycle-handoff-b-${run_id}"
  local lease_stream_id="scores"
  local acquire_job="${job_name}-owner-a"
  local owner_b_job="${job_name}-owner-b"
  local stale_job="${job_name}-stale-a"
  local payload_file="${output_dir}/${job_name}.vlxingest"
  local payload_b64
  local config_map="${job_name}-payload"
  local start_offset

  start_offset="$(($(ingest_writer_run_offset_base) + 5))"
  cargo run --locked -p velorix-ingest-writer --quiet -- encode-default-scores-payload \
    --output "$payload_file" \
    --schema-fingerprint "$schema_fingerprint" \
    --stream-id scores \
    --partition-id 0 \
    --start-offset-inclusive "$start_offset" \
    --rows-json '[{"user_id":"lifecycle-handoff","score":4,"delta":1}]'
  payload_b64="$(base64_file "$payload_file")"

  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${config_map}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
binaryData:
  payload.vlxingest: ${payload_b64}
EOF

  cat >"${output_dir}/${acquire_job}.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${acquire_job}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: velorix-ingest-writer-lifecycle
        velorix.dev/run-id: "${run_id}"
    spec:
      serviceAccountName: velorix-ingest-writer-lease-probe
      restartPolicy: Never
      containers:
        - name: ingest-writer
          image: ${ingest_writer_image}
          imagePullPolicy: ${ingest_writer_image_pull_policy}
          args:
            - probe-kubernetes-lease-acquire
            - --namespace
            - "${namespace}"
            - --view-id
            - positive_scores_by_user
            - --stream-id
            - "${lease_stream_id}"
            - --partition-id
            - "0"
            - --owner-id
            - "${owner_a}"
            - --ttl-ms
            - "2000"
            - --json
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
EOF

  kubectl --context "$context" -n "$namespace" delete job "$acquire_job" --ignore-not-found
  kubectl --context "$context" apply -f "${output_dir}/${acquire_job}.yaml"
  wait_for_job_complete "$acquire_job"
  kubectl --context "$context" -n "$namespace" logs "job/${acquire_job}" >"${output_dir}/${acquire_job}-log.json"
  kubectl --context "$context" -n "$namespace" get "job/${acquire_job}" -o json >"${output_dir}/${acquire_job}.json"
  kubectl --context "$context" -n "$namespace" get pods -l "job-name=${acquire_job}" -o json >"${output_dir}/${acquire_job}-pods.json"
  local owner_a_epoch
  owner_a_epoch="$(python3 - "${output_dir}/${acquire_job}-log.json" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    print(json.load(f)["owner_epoch"])
PY
)"
  sleep 3

  for guarded_job in "$owner_b_job" "$stale_job"; do
    local guarded_owner="$owner_b"
    local expected_outcome="appended"
    local acquire_flag='            - --acquire-lease'
    if [ "$guarded_job" = "$stale_job" ]; then
      guarded_owner="$owner_a"
      expected_outcome="stale-owner-rejected"
      acquire_flag=""
    fi
    local expected_epoch_args=""
    if [ "$guarded_job" = "$stale_job" ]; then
      expected_epoch_args="            - --expected-owner-epoch
            - \"${owner_a_epoch}\""
    fi
    cat >"${output_dir}/${guarded_job}.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${guarded_job}
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer-lifecycle
    velorix.dev/run-id: "${run_id}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: velorix-ingest-writer-lifecycle
        velorix.dev/run-id: "${run_id}"
    spec:
      serviceAccountName: velorix-ingest-writer-lease-probe
      restartPolicy: Never
      containers:
        - name: ingest-writer
          image: ${ingest_writer_image}
          imagePullPolicy: ${ingest_writer_image_pull_policy}
          args:
            - lease-guarded-append
            - --payload-file
            - /var/lib/velorix-ingest-writer/payload.vlxingest
            - --authority-store-id
            - "${s3_authority_store_id}"
            - --authority-namespace
            - velorix
            - --operator-id
            - velorix-product-script
            - --writer-id
            - "${guarded_owner}"
            - --lease-namespace
            - "${namespace}"
            - --lease-view-id
            - positive_scores_by_user
            - --lease-stream-id
            - "${lease_stream_id}"
            - --lease-partition-id
            - "0"
            - --owner-id
            - "${guarded_owner}"
${expected_epoch_args}
            - --ttl-ms
            - "60000"
${acquire_flag}
            - --expected-outcome
            - "${expected_outcome}"
            - --json
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_S3_COMPAT
              value: "1"
            - name: VELORIX_S3_FORCE_PATH_STYLE
              value: "${s3_force_path_style}"
            - name: AWS_ENDPOINT_URL
              value: "${s3_endpoint}"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: VELORIX_S3_BUCKET
              value: "${bucket}"
            - name: VELORIX_S3_PREFIX
              value: "${s3_prefix}"
          volumeMounts:
            - name: payload
              mountPath: /var/lib/velorix-ingest-writer
              readOnly: true
      volumes:
        - name: payload
          configMap:
            name: ${config_map}
EOF
    kubectl --context "$context" -n "$namespace" delete job "$guarded_job" --ignore-not-found
    kubectl --context "$context" apply -f "${output_dir}/${guarded_job}.yaml"
    wait_for_job_complete "$guarded_job"
    kubectl --context "$context" -n "$namespace" logs "job/${guarded_job}" >"${output_dir}/${guarded_job}-log.json"
    kubectl --context "$context" -n "$namespace" get "job/${guarded_job}" -o json >"${output_dir}/${guarded_job}.json"
    kubectl --context "$context" -n "$namespace" get pods -l "job-name=${guarded_job}" -o json >"${output_dir}/${guarded_job}-pods.json"
  done

  python3 - \
    "$job_log" \
    "${output_dir}/${acquire_job}-log.json" \
    "${output_dir}/${owner_b_job}-log.json" \
    "${output_dir}/${stale_job}-log.json" \
    "$owner_a" \
    "$owner_b" <<'PY'
import json
import sys
from datetime import datetime, timezone

output_path, acquire_path, owner_b_path, stale_path, owner_a, owner_b = sys.argv[1:]
with open(acquire_path, "r", encoding="utf-8") as f:
    acquired_a = json.load(f)
with open(owner_b_path, "r", encoding="utf-8") as f:
    owner_b_append = json.load(f)
with open(stale_path, "r", encoding="utf-8") as f:
    stale_attempt = json.load(f)
if acquired_a.get("evidence_kind") != "ingest_writer_kubernetes_lease_acquire_probe":
    raise SystemExit(f"unexpected owner A acquire evidence: {acquired_a}")
if acquired_a.get("owner_id") != owner_a or acquired_a.get("released") is not False:
    raise SystemExit(f"owner A did not hold an unreleased lease before simulated death: {acquired_a}")
if owner_b_append.get("evidence_kind") != "ingest_writer_lease_guarded_append_probe":
    raise SystemExit(f"unexpected owner B append evidence: {owner_b_append}")
if owner_b_append.get("owner_id") != owner_b or owner_b_append.get("outcome") != "appended":
    raise SystemExit(f"owner B did not append after handoff: {owner_b_append}")
if owner_b_append.get("append_completed") is not True:
    raise SystemExit(f"owner B guarded append did not report append completion: {owner_b_append}")
if owner_b_append.get("lease_held_through_append") is not True:
    raise SystemExit(f"owner B did not prove lease ownership through append completion: {owner_b_append}")
if owner_b_append.get("commit_guard_enforced") is not True:
    raise SystemExit(f"owner B did not enforce lease ownership inside the commit path: {owner_b_append}")
if owner_b_append.get("admission_commit_guard_bound") is not True:
    raise SystemExit(f"owner B did not bind admission reservation to the commit guard: {owner_b_append}")
owner_b_epoch = ((owner_b_append.get("acquired_grant") or {}).get("owner_epoch"))
if not isinstance(owner_b_epoch, int) or owner_b_epoch <= acquired_a.get("owner_epoch", 0):
    raise SystemExit(f"owner B did not acquire a higher lease epoch: owner_a={acquired_a} owner_b={owner_b_append}")
owner_b_binding = owner_b_append.get("admission_commit_guard_binding") or {}
if owner_b_binding.get("binding_kind") != "kubernetes_partition_lease":
    raise SystemExit(f"owner B did not report a Kubernetes lease admission binding: {owner_b_append}")
if owner_b_binding.get("owner_id") != owner_b or owner_b_binding.get("owner_epoch") != owner_b_epoch:
    raise SystemExit(f"owner B admission binding is not bound to owner B epoch: {owner_b_append}")
descriptor = owner_b_append.get("descriptor") or {}
if descriptor.get("stream_id") != "scores" or descriptor.get("partition_id") != 0:
    raise SystemExit(f"owner B append did not use the expected data stream/partition: {owner_b_append}")
if stale_attempt.get("evidence_kind") != "ingest_writer_lease_guarded_append_probe":
    raise SystemExit(f"unexpected stale owner evidence: {stale_attempt}")
if stale_attempt.get("owner_id") != owner_a:
    raise SystemExit(f"stale attempt did not use owner A: {stale_attempt}")
if stale_attempt.get("expected_owner_epoch") != acquired_a.get("owner_epoch"):
    raise SystemExit(f"stale attempt was not bound to owner A epoch: {stale_attempt}")
if stale_attempt.get("outcome") != "stale-owner-rejected":
    raise SystemExit(f"stale owner append was not rejected: {stale_attempt}")
if stale_attempt.get("stale_owner_rejected") is not True or stale_attempt.get("append_completed") is not False:
    raise SystemExit(f"stale owner evidence did not prove pre-append rejection: {stale_attempt}")
current_owner = stale_attempt.get("current_owner") or {}
if current_owner.get("owner_id") != owner_b or current_owner.get("owner_epoch") != owner_b_epoch:
    raise SystemExit(f"stale attempt did not observe owner B as current holder: {stale_attempt}")
artifact = {
    "schema_version": 1,
    "evidence_kind": "ingest_writer_two_pod_lease_handoff_probe",
    "status": "pass",
    "leader_handoff_checked": False,
    "kubernetes_lease_handoff_checked": True,
    "commit_guard_checked": True,
    "admission_commit_guard_bound_checked": True,
    "owner_a": owner_a,
    "owner_a_epoch": acquired_a["owner_epoch"],
    "owner_b": owner_b,
    "owner_b_epoch": owner_b_epoch,
    "owner_a_pod_terminated_before_release": True,
    "owner_b_append_completed": True,
    "owner_b_lease_held_through_append": True,
    "stale_owner_rejected": True,
    "same_stream_partition_lease_checked": True,
    "lease_identity": owner_b_append.get("lease_identity"),
    "descriptor": descriptor,
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "evidence_files": {
        "owner_a_acquire": acquire_path,
        "owner_b_guarded_append": owner_b_path,
        "stale_owner_attempt": stale_path,
    },
}
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(artifact, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

generate_ingest_writer_lifecycle_attestation() {
  if [ "$ingest_writer_lifecycle_auto" != "1" ]; then
    return 0
  fi
  if [ -n "$ingest_writer_lifecycle_attestation_file" ]; then
    return 0
  fi
  if [ "$ingest_writer_job_completed" != "1" ]; then
    return 0
  fi
  if [ "$no_pvc_namespace_validate" != "1" ]; then
    echo "skipping auto ingest-writer lifecycle attestation because VELORIX_NO_PVC_NAMESPACE_VALIDATE=0" >&2
    return 0
  fi
  if [ "$no_pvc_namespace_validated" != "1" ]; then
    validate_no_pvc_namespace
  fi

  local schema_fingerprint
  local overlap_job="velorix-ingest-lifecycle-overlap"
  local adjacent_job="velorix-ingest-lifecycle-adjacent"
  local restart_job="velorix-ingest-lifecycle-restart"
  local lease_loss_job="velorix-ingest-lifecycle-lease-loss"
  local handoff_job="velorix-ingest-lifecycle-handoff"
  local offset_base
  schema_fingerprint="$(python3 - "${output_dir}/scores-relation.json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
print(body["schema_fingerprint"])
PY
)"
  offset_base="$(ingest_writer_run_offset_base)"
  release_ingest_writer_smoke_partition_lease

  run_ingest_writer_lifecycle_append_job \
    "$overlap_job" \
    "lifecycle-overlap-${run_id}" \
    "$((offset_base + 1))" \
    '[{"user_id":"lifecycle-overlap","score":1,"delta":1}]' \
    conflict \
    "$schema_fingerprint"
  release_ingest_writer_smoke_partition_lease
  run_ingest_writer_lifecycle_append_job \
    "$adjacent_job" \
    "lifecycle-adjacent-${run_id}" \
    "$((offset_base + 2))" \
    '[{"user_id":"lifecycle-adjacent","score":2,"delta":1}]' \
    appended \
    "$schema_fingerprint"
  release_ingest_writer_smoke_partition_lease
  run_ingest_writer_lifecycle_crash_restart_probe_job \
    "$restart_job" \
    "lifecycle-restart-${run_id}" \
    "$((offset_base + 3))" \
    '[{"user_id":"lifecycle-restart","score":3,"delta":1}]' \
    "$schema_fingerprint"
  release_ingest_writer_smoke_partition_lease
  run_ingest_writer_lifecycle_lease_loss_probe_job \
    "$lease_loss_job" \
    "lifecycle-lease-loss-${run_id}" \
    "$((offset_base + 4))" \
    '[{"user_id":"lifecycle-lease-loss","score":0,"delta":0}]' \
    "$schema_fingerprint"
  release_ingest_writer_smoke_partition_lease
  run_ingest_writer_lifecycle_handoff_probe_job "$handoff_job" "$schema_fingerprint"

  python3 - \
    "$generated_ingest_writer_lifecycle_attestation" \
    "$product_deployment_id" \
    "$s3_authority_store_id" \
    "$ingest_writer_job_name" \
    "$overlap_job" \
    "$adjacent_job" \
    "$restart_job" \
    "$lease_loss_job" \
    "$handoff_job" \
    "$output_dir" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    output_path,
    deployment_id,
    authority_store_id,
    pod_internal_job,
    overlap_job,
    adjacent_job,
    restart_job,
    lease_loss_job,
    handoff_job,
    output_dir,
) = sys.argv[1:]
output_dir = Path(output_dir)

def read_json(name):
    with open(output_dir / name, "r", encoding="utf-8") as f:
        return json.load(f)

def job_provenance(job_name):
    job = read_json(f"{job_name}.json")
    pods = read_json(f"{job_name}-pods.json")
    items = pods.get("items") or []
    if len(items) != 1:
        raise SystemExit(f"expected exactly one provenance pod for {job_name}, got {len(items)}")
    pod = items[0]
    containers = pod.get("spec", {}).get("containers") or []
    statuses = pod.get("status", {}).get("containerStatuses") or []
    if not containers:
        raise SystemExit(f"provenance pod for {job_name} has no container spec: {pod}")
    if not statuses:
        raise SystemExit(f"provenance pod for {job_name} has no container status: {pod}")
    item = {
        "job_uid": (job.get("metadata") or {}).get("uid"),
        "pod_uid": (pod.get("metadata") or {}).get("uid"),
        "pod_name": (pod.get("metadata") or {}).get("name"),
        "container_image": containers[0].get("image"),
        "container_image_id": statuses[0].get("imageID"),
    }
    missing = [key for key, value in item.items() if not isinstance(value, str) or not value.strip()]
    if missing:
        raise SystemExit(f"provenance for {job_name} is missing {missing}: {item}")
    return item

pod_internal = read_json(f"{pod_internal_job}-log.json")
adjacent = read_json(f"{adjacent_job}-log.json")
restart = read_json(f"{restart_job}-log.json")
lease_loss = read_json(f"{lease_loss_job}-log.json")
handoff = read_json("velorix-ingest-lifecycle-handoff-log.json")
overlap = read_json(f"{overlap_job}-log.json")
if pod_internal.get("evidence_kind") != "ingest_writer_lease_guarded_append_probe":
    raise SystemExit(f"pod-internal evidence kind is not a lease-guarded append probe: {pod_internal}")
if pod_internal.get("status") != "pass" or pod_internal.get("outcome") != "appended":
    raise SystemExit(f"pod-internal append did not produce a fresh guarded append: {pod_internal}")
if pod_internal.get("lease_held_through_append") is not True:
    raise SystemExit(f"pod-internal append did not prove lease ownership through append: {pod_internal}")
if overlap.get("evidence_kind") != "ingest_writer_lifecycle_overlap_conflict_probe":
    raise SystemExit(f"overlap evidence kind is not a lifecycle overlap conflict probe: {overlap}")
if overlap.get("status") != "pass" or overlap.get("outcome") != "conflict-rejected":
    raise SystemExit(f"overlap probe did not produce a rejected overlap conflict: {overlap}")
for field in [
    "multi_pod_overlap_conflict_rejected",
    "conflicting_append_rejected_before_append",
    "conflict_log_observed",
]:
    if overlap.get(field) is not True:
        raise SystemExit(f"overlap probe did not prove {field}: {overlap}")
if overlap.get("append_completed") is not False:
    raise SystemExit(f"overlap probe must prove append_completed=false: {overlap}")
raw_overlap_log = overlap.get("raw_conflict_log") or ""
if (
    "conflicted before append" not in raw_overlap_log
    and "fresh append outcome, got conflict" not in raw_overlap_log
):
    raise SystemExit(f"overlap probe did not retain expected raw conflict evidence: {overlap}")
if adjacent.get("evidence_kind") != "ingest_writer_lease_guarded_append_probe":
    raise SystemExit(f"adjacent evidence kind is not a lease-guarded append probe: {adjacent}")
if adjacent.get("status") != "pass" or adjacent.get("outcome") != "appended":
    raise SystemExit(f"adjacent append did not produce a fresh guarded append: {adjacent}")
if adjacent.get("lease_held_through_append") is not True:
    raise SystemExit(f"adjacent append did not prove lease ownership through append: {adjacent}")
if restart.get("evidence_kind") != "ingest_writer_admission_crash_restart_probe":
    raise SystemExit(f"restart probe evidence kind is not an ingest admission crash/restart probe: {restart}")
if restart.get("orphan_admission_created") is not True:
    raise SystemExit(f"restart probe did not create a controlled orphan admission: {restart}")
if restart.get("restart_reconstructed_active_admission") is not True:
    raise SystemExit(f"restart probe did not reconstruct active admission after restart: {restart}")
if restart.get("recovered_append_completed") is not True:
    raise SystemExit(f"restart probe did not complete recovered append: {restart}")
if restart.get("committed_admission_not_expirable") is not True:
    raise SystemExit(f"restart probe did not protect committed admission from orphan expiry: {restart}")
if lease_loss.get("evidence_kind") != "ingest_writer_lease_loss_during_reservation_probe":
    raise SystemExit(f"lease-loss probe evidence kind is not a lease-loss probe: {lease_loss}")
for field in [
    "before_admission_lease_verified",
    "lease_released_before_commit",
    "commit_guard_rejected_before_batch_commit",
    "batch_object_absent_after_rejection",
    "admission_commit_guard_bound",
    "restart_reconstructed_active_admission",
    "target_admission_rejected_overlapping_reservation_before_expiry",
    "orphan_expired",
    "expired_target_rejected_original_retry",
]:
    if lease_loss.get(field) is not True:
        raise SystemExit(f"lease-loss probe did not prove {field}: {lease_loss}")
if handoff.get("evidence_kind") != "ingest_writer_two_pod_lease_handoff_probe":
    raise SystemExit(f"handoff probe evidence kind is not a two-Pod Lease handoff probe: {handoff}")
if handoff.get("kubernetes_lease_handoff_checked") is not True:
    raise SystemExit(f"handoff probe did not check Kubernetes Lease handoff: {handoff}")
if handoff.get("owner_b_epoch", 0) <= handoff.get("owner_a_epoch", 0):
    raise SystemExit(f"handoff probe did not advance lease epoch: {handoff}")
if handoff.get("owner_b_append_completed") is not True:
    raise SystemExit(f"handoff probe did not prove owner B guarded append: {handoff}")
if handoff.get("owner_b_lease_held_through_append") is not True:
    raise SystemExit(f"handoff probe did not prove owner B held the lease through append: {handoff}")
if handoff.get("stale_owner_rejected") is not True:
    raise SystemExit(f"handoff probe did not reject stale owner A before append: {handoff}")

attestation = {
    "schema_version": 1,
    "evidence_kind": "velorix_ingest_writer_lifecycle_attestation",
    "deployment_id": deployment_id,
    "authority_store_id": authority_store_id,
    "deployed_topology": "kubernetes_jobs",
    "pod_internal_append_completed": True,
    "multi_pod_overlap_conflict_rejected": True,
    "adjacent_append_succeeded": True,
    "crash_restart_reconstruction_checked": True,
    "leader_handoff_checked": False,
    "kubernetes_lease_handoff_checked": True,
    "lease_held_through_append_checked": True,
    "commit_guard_checked": True,
    "admission_commit_guard_bound_checked": True,
    "lease_loss_during_reservation_checked": True,
    "no_pvc_created_by_vind": True,
    "attested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "attester": "scripts/run-vind-product.sh",
    "evidence_provenance": {
        "pod_internal_job": job_provenance(pod_internal_job),
        "overlap_job": job_provenance(overlap_job),
        "adjacent_job": job_provenance(adjacent_job),
        "restart_job": job_provenance(restart_job),
        "lease_loss_job": job_provenance(lease_loss_job),
        "handoff_owner_a_job": job_provenance(f"{handoff_job}-owner-a"),
        "handoff_owner_b_job": job_provenance(f"{handoff_job}-owner-b"),
        "handoff_stale_owner_job": job_provenance(f"{handoff_job}-stale-a"),
    },
    "evidence_files": {
        "pod_internal_job": f"{pod_internal_job}-log.json",
        "overlap_job": f"{overlap_job}-log.json",
        "adjacent_job": f"{adjacent_job}-log.json",
        "restart_job": f"{restart_job}-log.json",
        "lease_loss_job": f"{lease_loss_job}-log.json",
        "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json",
    },
}
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(attestation, f, indent=2, sort_keys=True)
    f.write("\n")
PY

  ingest_writer_lifecycle_attestation_file="$generated_ingest_writer_lifecycle_attestation"
  ingest_writer_lifecycle_generated_by_script=1
  validate_ingest_writer_lifecycle_attestation
}

run_multi_replica_fencing_smoke() {
  if [ "$multi_replica_fencing_smoke" != "1" ]; then
    return 0
  fi
  if [ "$api_replica_count" -lt 2 ] || [ "$standing_runtime_fencing" = "unsafe-dev-only" ]; then
    return 0
  fi
  if ! python3 - "${output_dir}/readyz.json" "$standing_runtime_fencing" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    readyz = json.load(f)
mode = sys.argv[2]
capability = (readyz.get("metadata_store") or {}).get("standing_runtime_fencing") or {}
if mode == "required":
    if capability.get("production_multi_writer_safe") is not True:
        raise SystemExit(1)
    if capability.get("backend_time_source_kind") != "raft_replicated_authority_time":
        raise SystemExit(1)
elif mode == "logical-fencing":
    if capability.get("multi_writer_fencing_safe") is not True:
        raise SystemExit(1)
    if capability.get("lease_authority_kind") not in {
        "hiqlite_raft_serialized",
        "raft_replicated_time",
    }:
        raise SystemExit(1)
    if capability.get("lease_expiry_semantics") not in {
        "operation_driven_logical",
        "backend_wall_clock_ttl",
    }:
        raise SystemExit(1)
else:
    raise SystemExit(1)
PY
  then
    return 0
  fi

  local pods_json="${output_dir}/multi-replica-api-pods.json"
  local evidence_file="${output_dir}/multi-replica-fencing-smoke.json"
  local pod_a
  local pod_b
  local port_a
  local port_b
  local pid_a=""
  local pid_b=""
  local log_a="${output_dir}/multi-replica-pod-a-port-forward.log"
  local log_b="${output_dir}/multi-replica-pod-b-port-forward.log"
  local view_id="multi_replica_positive_scores_by_user"
  local data_stream_id="multi-replica-${run_id}"
  local user_id="multi-replica-${run_id}"
  local owner_ingest_status
  local fenced_ingest_status
  local owner_retry_status
  local view_status
  local previous_trap

  kubectl --context "$context" -n "$namespace" get pods \
    -l "app=velorix-api,velorix.dev/run-id=${run_id}" -o json >"$pods_json"
  read -r pod_a pod_b < <(select_two_ready_api_pods "$pods_json")

  port_a=$((api_local_port + 10))
  port_b=$((api_local_port + 11))
  ensure_local_port_free "$port_a"
  ensure_local_port_free "$port_b"

  previous_trap="$(trap -p EXIT || true)"
  cleanup_multi_replica_forwards() {
    if [ -n "${pid_a:-}" ]; then
      kill "$pid_a" >/dev/null 2>&1 || true
      wait "$pid_a" >/dev/null 2>&1 || true
    fi
    if [ -n "${pid_b:-}" ]; then
      kill "$pid_b" >/dev/null 2>&1 || true
      wait "$pid_b" >/dev/null 2>&1 || true
    fi
  }
  trap 'status=$?; cleanup_multi_replica_forwards; cleanup_vind "$status"; exit "$status"' EXIT

  nohup kubectl --context "$context" -n "$namespace" port-forward "pod/${pod_a}" "${port_a}:8080" \
    >"$log_a" 2>&1 &
  pid_a="$!"
  nohup kubectl --context "$context" -n "$namespace" port-forward "pod/${pod_b}" "${port_b}:8080" \
    >"$log_b" 2>&1 &
  pid_b="$!"
  wait_for_forward_url "$pid_a" "http://127.0.0.1:${port_a}/healthz" "$log_a"
  wait_for_forward_url "$pid_b" "http://127.0.0.1:${port_b}/healthz" "$log_b"

  view_status="$(curl_api_status "${output_dir}/multi-replica-view.json" \
    -X POST "http://127.0.0.1:${port_a}/v1/views" \
    -H 'content-type: application/json' \
    -d "{\"view_id\":\"${view_id}\",\"urlPath\":\"/multi-replica/scores/positive\",\"input_relation_id\":\"scores\",\"input_relation_version\":\"2026-05-24.v1\",\"sql\":\"select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id\",\"response_formats\":[\"json\"]}")"
  case "$view_status" in
    200 | 201) ;;
    *)
      echo "multi-replica view creation failed with ${view_status}" >&2
      cat "${output_dir}/multi-replica-view.json" >&2 || true
      exit 1
      ;;
  esac

  for _ in $(seq 1 10); do
    owner_ingest_status="$(curl_api_status "${output_dir}/multi-replica-owner-ingest.json" \
      -X POST "http://127.0.0.1:${port_a}/v1/relations/scores/ingest" \
      -H 'content-type: application/json' \
      -d "{\"relation_version\":\"2026-05-24.v1\",\"stream_id\":\"${data_stream_id}\",\"partition_id\":0,\"start_offset_inclusive\":0,\"rows\":[{\"user_id\":\"${user_id}\",\"score\":5,\"delta\":1}]}")"
    case "$owner_ingest_status" in
      200 | 201) break ;;
      409) sleep 1 ;;
      *) break ;;
    esac
  done
  case "$owner_ingest_status" in
    200 | 201) ;;
    *)
      echo "multi-replica owner ingest failed with ${owner_ingest_status}" >&2
      cat "${output_dir}/multi-replica-owner-ingest.json" >&2 || true
      exit 1
      ;;
  esac

  wait_for_api_status \
    "${output_dir}/multi-replica-owner-materialize-query.json" \
    200 \
    "multi-replica owner materialize query" \
    "http://127.0.0.1:${port_a}/v1/views/${view_id}/query"

  wait_for_api_status \
    "${output_dir}/multi-replica-read-replica-query.json" \
    200 \
    "multi-replica read-replica query" \
    "http://127.0.0.1:${port_b}/v1/views/${view_id}/query"

  fenced_ingest_status="$(curl_api_status "${output_dir}/multi-replica-fenced-ingest.json" \
    -X POST "http://127.0.0.1:${port_b}/v1/relations/scores/ingest" \
    -H 'content-type: application/json' \
    -d "{\"relation_version\":\"2026-05-24.v1\",\"stream_id\":\"${data_stream_id}\",\"partition_id\":0,\"start_offset_inclusive\":1,\"rows\":[{\"user_id\":\"${user_id}\",\"score\":9,\"delta\":1}]}")"
  if [ "$fenced_ingest_status" != "409" ]; then
    echo "expected non-owner replica ingest to be fenced with 409, got ${fenced_ingest_status}" >&2
    cat "${output_dir}/multi-replica-fenced-ingest.json" >&2 || true
    exit 1
  fi

  owner_retry_status="$(curl_api_status "${output_dir}/multi-replica-owner-retry-ingest.json" \
    -X POST "http://127.0.0.1:${port_a}/v1/relations/scores/ingest" \
    -H 'content-type: application/json' \
    -d "{\"relation_version\":\"2026-05-24.v1\",\"stream_id\":\"${data_stream_id}\",\"partition_id\":0,\"start_offset_inclusive\":1,\"rows\":[{\"user_id\":\"${user_id}\",\"score\":9,\"delta\":1}]}")"
  case "$owner_retry_status" in
    200 | 201) ;;
    *)
      echo "multi-replica owner retry ingest failed with ${owner_retry_status}" >&2
      cat "${output_dir}/multi-replica-owner-retry-ingest.json" >&2 || true
      exit 1
      ;;
  esac

  for _ in $(seq 1 60); do
    wait_for_api_status \
      "${output_dir}/multi-replica-final-query.json" \
      200 \
      "multi-replica final query" \
      "http://127.0.0.1:${port_b}/v1/views/${view_id}/query"
    if python3 - "${output_dir}/multi-replica-final-query.json" "$user_id" <<'PY'
import json
import sys

path, user_id = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    body = json.load(f)
rows = body.get("rows") or []
matching = [row for row in rows if row.get("user_id") == user_id]
if matching != [{"user_id": user_id, "sum": 14, "count": 2}]:
    raise SystemExit(f"unexpected multi-replica final rows for {user_id}: {body}")
PY
    then
      break
    fi
    sleep 1
  done
  python3 - "${output_dir}/multi-replica-final-query.json" "$user_id" <<'PY'
import json
import sys

path, user_id = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    body = json.load(f)
rows = body.get("rows") or []
matching = [row for row in rows if row.get("user_id") == user_id]
if matching != [{"user_id": user_id, "sum": 14, "count": 2}]:
    raise SystemExit(f"unexpected multi-replica final rows for {user_id}: {body}")
PY

  python3 - "$evidence_file" "$pod_a" "$pod_b" "$port_a" "$port_b" "$view_id" "$user_id" <<'PY'
import json
import sys
from datetime import datetime, timezone

path, pod_a, pod_b, port_a, port_b, view_id, user_id = sys.argv[1:]
evidence = {
    "schema_version": 1,
    "evidence_kind": "velorix_deployed_multi_replica_fencing_smoke",
    "status": "pass",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "owner_pod": pod_a,
    "read_replica_pod": pod_b,
    "owner_port_forward": int(port_a),
    "read_replica_port_forward": int(port_b),
    "view_id": view_id,
    "user_id": user_id,
    "artifacts": {
        "pods": "multi-replica-api-pods.json",
        "view": "multi-replica-view.json",
        "owner_ingest": "multi-replica-owner-ingest.json",
        "owner_materialize_query": "multi-replica-owner-materialize-query.json",
        "read_replica_query": "multi-replica-read-replica-query.json",
        "fenced_ingest": "multi-replica-fenced-ingest.json",
        "owner_retry_ingest": "multi-replica-owner-retry-ingest.json",
        "final_query": "multi-replica-final-query.json",
    },
    "assertions": {
        "distinct_api_pods": pod_a != pod_b,
        "read_replica_served_query": True,
        "non_owner_ingest_rejected": True,
        "owner_retry_converged": True,
    },
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  multi_replica_fencing_smoke_passed=1
  multi_replica_fencing_smoke_evidence_file="$evidence_file"

  cleanup_multi_replica_forwards
  pid_a=""
  pid_b=""
  if [ -n "$previous_trap" ]; then
    eval "$previous_trap"
  else
    trap - EXIT
  fi
}

start_api_port_forward() {
  stop_api_port_forward
  ensure_local_api_port_free
  nohup kubectl --context "$context" -n "$namespace" port-forward "svc/velorix-api" "${api_local_port}:8080" \
    >"${output_dir}/port-forward.log" 2>&1 &
  port_forward_pid="$!"
  echo "$port_forward_pid" >"${output_dir}/port-forward.pid"
  wait_for_api
}

start_api_writer_owner_port_forward_for_smoke() {
  if [ "$api_auth_mode" != "bearer-token" ]; then
    return 0
  fi
  if [ "$api_replica_count" -lt 2 ]; then
    return 0
  fi
  if [ "$standing_runtime_fencing" = "unsafe-dev-only" ]; then
    return 0
  fi
  if [ -z "$admin_bearer_token" ]; then
    echo "cannot run multi-replica product smoke without an admin bearer token for writer-owner attach" >&2
    exit 66
  fi

  local pods_json="${output_dir}/smoke-attach-api-pods.json"
  local evidence_file="${output_dir}/smoke-owner-rest-attach.json"
  local selected_pod=""
  local deadline=$((SECONDS + 60))
  local probe_port=""
  local probe_pid=""
  local probe_log=""
  local probe_json=""
  local acquire_json=""

  while true; do
    kubectl --context "$context" -n "$namespace" get pods \
      -l "app=velorix-api,velorix.dev/run-id=${run_id}" -o json >"$pods_json"
    while read -r pod_name; do
      [ -n "$pod_name" ] || continue
      probe_port=$((20000 + RANDOM % 20000))
      probe_log="${output_dir}/smoke-owner-probe-${pod_name}.log"
      probe_json="${output_dir}/smoke-owner-probe-${pod_name}.json"
      acquire_json="${output_dir}/smoke-owner-acquire-${pod_name}.json"
      kubectl --context "$context" -n "$namespace" port-forward "pod/${pod_name}" \
        "${probe_port}:8080" >"$probe_log" 2>&1 &
      probe_pid="$!"
      for _ in $(seq 1 10); do
        if curl -fsS --max-time 2 "http://127.0.0.1:${probe_port}/healthz" >/dev/null 2>&1; then
          break
        fi
        if ! kill -0 "$probe_pid" >/dev/null 2>&1; then
          break
        fi
        sleep 1
      done
      curl -fsS --max-time 5 \
        -X POST "http://127.0.0.1:${probe_port}/v1/standing-runtime/owners" \
        -H "authorization: Bearer ${admin_bearer_token}" >"$acquire_json" 2>/dev/null || true
      if curl -fsS --max-time 5 \
        "http://127.0.0.1:${probe_port}/v1/standing-runtime/owners" \
        -H "authorization: Bearer ${admin_bearer_token}" >"$probe_json" 2>/dev/null; then
        if python3 - "$probe_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    report = json.load(f)
owners = report.get("owners") or []
if owners and all(owner.get("current_owner_matches_local_process") is True for owner in owners):
    raise SystemExit(0)
raise SystemExit(1)
PY
        then
          selected_pod="$pod_name"
          kill "$probe_pid" >/dev/null 2>&1 || true
          wait "$probe_pid" >/dev/null 2>&1 || true
          break
        fi
      fi
      kill "$probe_pid" >/dev/null 2>&1 || true
      wait "$probe_pid" >/dev/null 2>&1 || true
    done < <(
      python3 - "$pods_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
for item in body.get("items") or []:
    metadata = item.get("metadata") or {}
    if metadata.get("deletionTimestamp"):
        continue
    status = item.get("status") or {}
    conditions = {
        condition.get("type"): condition.get("status")
        for condition in status.get("conditions") or []
    }
    if status.get("phase") == "Running" and conditions.get("Ready") == "True":
        print(metadata.get("name") or "")
PY
    )
    if [ -n "$selected_pod" ]; then
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      break
    fi
    sleep 1
  done

  if [ -z "$selected_pod" ]; then
    echo "could not find a standing-runtime writer owner pod for product smoke" >&2
    exit 75
  fi

  stop_api_port_forward
  ensure_local_api_port_free
  nohup kubectl --context "$context" -n "$namespace" port-forward "pod/${selected_pod}" "${api_local_port}:8080" \
    >"${output_dir}/port-forward.log" 2>&1 &
  port_forward_pid="$!"
  echo "$port_forward_pid" >"${output_dir}/port-forward.pid"
  wait_for_api

  python3 - "$evidence_file" "$selected_pod" "$pods_json" "$probe_json" "$acquire_json" <<'PY'
import json
import sys
from datetime import datetime, timezone

path, selected_pod, pods_json, probe_json, acquire_json = sys.argv[1:]
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_product_smoke_owner_rest_attach",
    "status": "pass",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "selected_pod": selected_pod,
    "reason": "multi-replica fenced product smoke writes must target the standing-runtime writer owner",
    "trusted_for_product_complete": False,
    "evidence_files": {
        "api_pods": pods_json,
        "owner_probe": probe_json,
        "owner_acquire": acquire_json,
    },
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

attach_final_rest_to_writer_owner() {
  api_final_rest_attach_evidence_file=""
  if [ "$hold_port_forward" != "1" ]; then
    return 0
  fi
  if [ "$final_owner_aware_attach" != "1" ]; then
    return 0
  fi
  if [ "$api_auth_mode" != "bearer-token" ]; then
    return 0
  fi
  if [ "$api_replica_count" -lt 2 ]; then
    return 0
  fi
  if [ "$standing_runtime_fencing" = "unsafe-dev-only" ]; then
    return 0
  fi
  if [ -z "$admin_bearer_token" ]; then
    echo "skipping final owner-aware REST attach because no admin bearer token is available" >&2
    return 0
  fi

  stop_api_port_forward
  VELORIX_VIND_PRODUCT_DIR="$output_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="${output_dir}/product-evidence.json" \
    VELORIX_API_AUTH_ENV="${output_dir}/api-auth.env" \
    VELORIX_API_ATTACH_EVIDENCE="${output_dir}/rest-attach-evidence.json" \
    VELORIX_API_LOCAL_PORT="$api_local_port" \
    VELORIX_API_ATTACH_HOLD=1 \
    VELORIX_API_ATTACH_BACKGROUND=1 \
    VELORIX_API_ATTACH_WRITER_OWNER=1 \
    bash "${repo_root}/scripts/attach-vind-product-rest.sh"

  if [ ! -f "${output_dir}/port-forward.attach.pid" ]; then
    echo "owner-aware REST attach did not write ${output_dir}/port-forward.attach.pid" >&2
    exit 1
  fi
  port_forward_pid="$(cat "${output_dir}/port-forward.attach.pid")"
  case "$port_forward_pid" in
    '' | *[!0-9]*)
      echo "owner-aware REST attach wrote an invalid pid: ${port_forward_pid}" >&2
      exit 1
      ;;
  esac
  if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    echo "owner-aware REST attach port-forward is not running: ${port_forward_pid}" >&2
    exit 1
  fi
  printf '%s\n' "$port_forward_pid" >"${output_dir}/port-forward.pid"
  api_final_rest_attach_evidence_file="${output_dir}/rest-attach-evidence.json"
}

run_rest_api_smoke() {
  local should_run=0
  rest_api_smoke_status="not_run"
  rest_api_smoke_evidence_file=""
  case "$rest_api_smoke" in
    0)
      rest_api_smoke_status="disabled"
      return 0
      ;;
    1)
      should_run=1
      ;;
    auto)
      if [ "$product_smoke" = "1" ] && [ "$api_auth_mode" = "bearer-token" ]; then
        should_run=1
      else
        rest_api_smoke_status="skipped"
        return 0
      fi
      ;;
  esac
  if [ "$should_run" != "1" ]; then
    return 0
  fi
  if [ "$api_auth_mode" != "bearer-token" ]; then
    echo "VELORIX_VIND_REST_API_SMOKE=1 requires bearer-token API auth" >&2
    exit 64
  fi
  VELORIX_VIND_PRODUCT_DIR="$output_dir" \
    VELORIX_API_AUTH_ENV="${output_dir}/api-auth.env" \
    VELORIX_API_ATTACH_EVIDENCE="${output_dir}/rest-attach-evidence.json" \
    VELORIX_REST_API_SMOKE_DIR="${output_dir}/rest-api-smoke" \
    VELORIX_REST_API_SMOKE_EVIDENCE="${output_dir}/rest-api-smoke.json" \
    VELORIX_REST_API_SMOKE_ATTACH=0 \
    scripts/smoke-vind-rest-api.sh >/dev/null
  rest_api_smoke_status="pass"
  rest_api_smoke_evidence_file="${output_dir}/rest-api-smoke.json"
}

run_product_completion_report() {
  local should_run=0
  product_completion_report_status="not_run"
  product_completion_report_file=""
  case "$product_completion_report" in
    0)
      product_completion_report_status="disabled"
      return 0
      ;;
    1)
      should_run=1
      ;;
    auto)
      if [ -f "${output_dir}/product-evidence.json" ]; then
        should_run=1
      else
        product_completion_report_status="skipped"
        return 0
      fi
      ;;
  esac
  if [ "$should_run" != "1" ]; then
    return 0
  fi
  VELORIX_VIND_PRODUCT_DIR="$output_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="${output_dir}/product-evidence.json" \
    VELORIX_REST_API_SMOKE_EVIDENCE="${output_dir}/rest-api-smoke.json" \
    VELORIX_PRODUCT_COMPLETION_REPORT="${output_dir}/product-completion-report.json" \
    scripts/report-vind-product-completion.sh
  product_completion_report_status="pass"
  product_completion_report_file="${output_dir}/product-completion-report.json"
}

run_standing_runtime_failover_smoke() {
  local should_run=0
  case "$standing_runtime_failover_smoke" in
    0)
      return 0
      ;;
    1)
      should_run=1
      ;;
    auto)
      if [ "$product_smoke" = "1" ] \
        && [ "$api_auth_mode" = "bearer-token" ] \
        && [ "$api_replica_count" -ge 2 ] \
        && [ "$standing_runtime_fencing" != "unsafe-dev-only" ] \
        && [ "$multi_replica_fencing_smoke_passed" = "1" ] \
        && [ -n "$admin_bearer_token" ]; then
        should_run=1
      fi
      ;;
  esac
  if [ "$should_run" != "1" ]; then
    return 0
  fi
  if [ ! -f "${output_dir}/product-evidence.json" ]; then
    echo "standing-runtime failover smoke requires ${output_dir}/product-evidence.json" >&2
    exit 66
  fi

  VELORIX_VIND_PRODUCT_DIR="$output_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="${output_dir}/product-evidence.json" \
    VELORIX_API_AUTH_ENV="${output_dir}/api-auth.env" \
    VELORIX_API_ATTACH_EVIDENCE="${output_dir}/rest-attach-evidence.json" \
    VELORIX_API_LOCAL_PORT="$api_local_port" \
    VELORIX_API_ATTACH_TIMEOUT_SECONDS=60 \
    VELORIX_STANDING_RUNTIME_FAILOVER_EVIDENCE="${output_dir}/standing-runtime-failover-smoke.json" \
    VELORIX_STANDING_RUNTIME_FAILOVER_KEEP_ATTACH="$hold_port_forward" \
    bash "${repo_root}/scripts/smoke-vind-standing-runtime-failover.sh" \
    | tee "${output_dir}/standing-runtime-failover-smoke.out" >/dev/null

  standing_runtime_failover_smoke_passed=1
  standing_runtime_failover_smoke_evidence_file="${output_dir}/standing-runtime-failover-smoke.json"
}

run_hiqlite_backend_time_assessment() {
  local should_run=0
  case "$hiqlite_backend_time_assess" in
    0)
      return 0
      ;;
    1)
      if [ "$meta_enabled" != "1" ] || [ "$meta_backend" != "hiqlite" ]; then
        echo "VELORIX_HIQLITE_BACKEND_TIME_ASSESS=1 requires VELORIX_META_ENABLED=1 and VELORIX_META_BACKEND=hiqlite" >&2
        exit 64
      fi
      should_run=1
      ;;
    auto)
      if [ "$meta_enabled" = "1" ] && [ "$meta_backend" = "hiqlite" ]; then
        should_run=1
      fi
      ;;
  esac
  if [ "$should_run" != "1" ]; then
    return 0
  fi
  if [ ! -f "${output_dir}/product-evidence.json" ]; then
    echo "Hiqlite backend-time assessment requires ${output_dir}/product-evidence.json" >&2
    exit 66
  fi

  local require_backend_time=0
  if [ "$product_evidence_level" = "product-complete" ]; then
    require_backend_time=1
  fi

  VELORIX_PRODUCT_EVIDENCE_PATH="${output_dir}/product-evidence.json" \
    VELORIX_HIQLITE_BACKEND_TIME_ASSESSMENT_PATH="$hiqlite_backend_time_assessment_file" \
    VELORIX_HIQLITE_BACKEND_TIME_UPDATE_PRODUCT_EVIDENCE=1 \
    VELORIX_REQUIRE_HIQLITE_BACKEND_TIME="${VELORIX_REQUIRE_HIQLITE_BACKEND_TIME:-${require_backend_time}}" \
    bash "${repo_root}/scripts/assess-hiqlite-backend-time.sh" "$output_dir" \
    | tee "${output_dir}/hiqlite-backend-time-assessment.out" >/dev/null

  hiqlite_backend_time_assessment_validated=1
}

run_hiqlite_backend_time_attestation() {
  local should_run=0
  case "$hiqlite_backend_time_attest" in
    0)
      return 0
      ;;
    1)
      if [ "$meta_enabled" != "1" ] || [ "$meta_backend" != "hiqlite" ] || [ "$standing_runtime_fencing" != "required" ]; then
        echo "VELORIX_HIQLITE_BACKEND_TIME_ATTEST=1 requires VELORIX_META_ENABLED=1, VELORIX_META_BACKEND=hiqlite, and VELORIX_STANDING_RUNTIME_FENCING=required" >&2
        exit 64
      fi
      should_run=1
      ;;
    auto)
      if [ "$meta_enabled" = "1" ] \
        && [ "$meta_backend" = "hiqlite" ] \
        && [ "$standing_runtime_fencing" = "required" ] \
        && [ "$hiqlite_backend_time_assessment_validated" = "1" ] \
        && [ "$meta_fencing_adversarial_smoke_passed" = "1" ] \
        && [ "$multi_replica_fencing_smoke_passed" = "1" ] \
        && [ "$standing_runtime_failover_smoke_passed" = "1" ]; then
        should_run=1
      fi
      ;;
    *)
      echo "VELORIX_HIQLITE_BACKEND_TIME_ATTEST must be 0, 1, or auto" >&2
      exit 64
      ;;
  esac
  if [ "$should_run" != "1" ]; then
    return 0
  fi
  if [ ! -f "${output_dir}/product-evidence.json" ]; then
    echo "Hiqlite backend-time attestation requires ${output_dir}/product-evidence.json" >&2
    exit 66
  fi

  VELORIX_PRODUCT_EVIDENCE_PATH="${output_dir}/product-evidence.json" \
    VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_FILE="$hiqlite_backend_time_attestation_file" \
    VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_UPDATE_PRODUCT_EVIDENCE=1 \
    VELORIX_ATTESTER="scripts/run-vind-product.sh" \
    bash "${repo_root}/scripts/attest-hiqlite-backend-time.sh" \
    | tee "${output_dir}/hiqlite-backend-time-attestation.out" >/dev/null

  hiqlite_backend_time_attestation_validated=1
}

start_api_tls_port_forward() {
  if [ "$api_tls_enabled" != "1" ]; then
    return 0
  fi
  stop_api_tls_port_forward
  ensure_local_port_free "$api_tls_local_port"
  nohup kubectl --context "$context" -n "$namespace" port-forward "svc/velorix-api" "${api_tls_local_port}:8443" \
    >"${output_dir}/port-forward-tls.log" 2>&1 &
  api_tls_port_forward_pid="$!"
  echo "$api_tls_port_forward_pid" >"${output_dir}/port-forward-tls.pid"
  wait_for_forward_url "$api_tls_port_forward_pid" "https://127.0.0.1:${api_tls_local_port}/healthz" "${output_dir}/port-forward-tls.log" \
    --cacert "${output_dir}/api-tls.crt"
}

run_api_tls_auth_smoke() {
  if [ "$api_tls_enabled" != "1" ]; then
    return 0
  fi
  if [ "$api_auth_mode" != "bearer-token" ]; then
    echo "VELORIX_API_TLS_ENABLED=1 requires bearer-token API auth for TLS/auth smoke" >&2
    exit 64
  fi

  local tls_url="https://127.0.0.1:${api_tls_local_port}"
  local observed_cert="${output_dir}/api-tls-observed.crt"
  printf '' | openssl s_client -connect "127.0.0.1:${api_tls_local_port}" -servername localhost -showcerts 2>/dev/null \
    | openssl x509 -outform PEM >"$observed_cert"
  local observed_sha256
  observed_sha256="$(sha256_file "$observed_cert")"
  if [ "$observed_sha256" != "$api_tls_certificate_sha256" ]; then
    echo "observed TLS certificate fingerprint ${observed_sha256} did not match generated certificate ${api_tls_certificate_sha256}" >&2
    exit 1
  fi
  check_api_auth_rejection "${output_dir}/tls-auth-missing-response.json" "missing TLS bearer token" \
    --cacert "${output_dir}/api-tls.crt" \
    -X POST "${tls_url}/v1/relations/scores-default"
  check_api_auth_rejection "${output_dir}/tls-auth-wrong-response.json" "wrong TLS bearer token" \
    --cacert "${output_dir}/api-tls.crt" \
    -X POST "${tls_url}/v1/relations/scores-default" \
    -H "authorization: Bearer definitely-wrong-token"
  check_api_auth_rejection "${output_dir}/tls-admin-auth-data-token-response.json" "TLS data-plane token on admin route" \
    --cacert "${output_dir}/api-tls.crt" \
    "${tls_url}/v1/standing-runtime/owners" \
    -H "authorization: Bearer ${api_bearer_token}"
  curl -fsS --cacert "${output_dir}/api-tls.crt" \
    -H "authorization: Bearer ${api_bearer_token}" \
    "${tls_url}/v1/openapi.json" \
    >"${output_dir}/tls-auth-correct-token-openapi.json"

  api_tls_evidence_file="${output_dir}/tls-auth-smoke.json"
  python3 - "$api_tls_evidence_file" "$tls_url" "$api_tls_certificate_sha256" "$observed_sha256" <<'PY'
import json
import sys
from datetime import datetime, timezone

path, tls_url, cert_sha256, observed_sha256 = sys.argv[1:]
evidence = {
    "schema_version": 1,
    "evidence_kind": "velorix_local_vind_tls_auth_smoke",
    "status": "pass",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "endpoint_url": tls_url,
    "transport_security": "self-signed TLS trusted by smoke via --cacert",
    "tls_enabled": True,
    "tls_certificate_sha256": cert_sha256,
    "observed_tls_certificate_sha256": observed_sha256,
    "cert_authority": "generated-self-signed-local",
    "verification_mode": "verified-with-generated-cacert",
    "auth_enforced": True,
    "missing_token_rejected": True,
    "wrong_token_rejected": True,
    "admin_auth_separate": True,
    "data_plane_token_rejected_on_admin_route": True,
    "scope": "local port-forwarded vind/vCluster service",
    "public_ingress_attestation": False,
    "trusted_for_product_complete": False,
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  api_tls_auth_smoke_passed=1
}

write_product_evidence() {
  python3 - \
    "${output_dir}/product-evidence.json" \
    "$run_id" \
    "$product_deployment_id" \
    "$cluster" \
    "$namespace" \
    "$context" \
    "$product_evidence_level" \
    "$api_image" \
    "$api_image_digest" \
    "$meta_enabled" \
    "$meta_backend" \
    "$meta_image" \
    "$meta_image_digest" \
    "$api_replica_count" \
    "$standing_runtime_fencing" \
    "$api_auth_mode" \
    "$api_bearer_token_source" \
    "$admin_bearer_token_source" \
    "$api_auth_observed_readyz_mode" \
    "$api_auth_missing_token_rejected" \
    "$api_auth_wrong_token_rejected" \
    "$api_auth_correct_token_smoke_passed" \
    "$api_auth_data_plane_token_rejected_on_admin_route" \
    "$api_healthz_unauthenticated" \
    "$api_readyz_unauthenticated" \
    "$api_deployment_env_verified" \
    "$api_query_policy_smoke_passed" \
    "$api_query_policy_missing_policy_rejected" \
    "$api_query_policy_weak_policy_rejected" \
    "$api_openapi_catalog_smoke_passed" \
    "$ingest_writer_smoke" \
    "$ingest_writer_image" \
    "$ingest_writer_job_completed" \
    "$ingest_writer_append_outcome" \
    "$ingest_writer_object_key" \
    "$ingest_writer_lifecycle_attestation_file" \
    "$ingest_writer_lifecycle_attestation_validated" \
    "$ingest_writer_lifecycle_attestation_source" \
    "$meta_fencing_adversarial_smoke_passed" \
    "$meta_fencing_adversarial_smoke_log" \
    "$multi_replica_fencing_smoke" \
    "$multi_replica_fencing_smoke_passed" \
    "$multi_replica_fencing_smoke_evidence_file" \
    "$standing_runtime_failover_smoke" \
    "$standing_runtime_failover_smoke_passed" \
    "$standing_runtime_failover_smoke_evidence_file" \
    "$preserve_state" \
    "$s3_prefix" \
    "$meta_s3_prefix" \
    "$s3_credentials_source" \
    "$s3_credentials_secret_name" \
    "$s3_credentials_secret_managed" \
    "$object_store_namespace_count" \
    "$object_store_artifact_catalog_conditional_update" \
    "$object_store_mode" \
    "$object_store_local_development_authority" \
    "$s3_backend_label" \
    "$s3_durability_label" \
    "$s3_endpoint" \
    "$s3_authority_store_id" \
    "$bucket" \
    "$aws_region" \
    "$s3_force_path_style" \
    "$s3_credentials_hash" \
    "$external_s3_validate" \
    "$external_s3_bucket_validated" \
    "$external_s3_prefix_validated" \
    "$external_s3_validation_key" \
    "$object_store_durability_attestation_file" \
    "$object_store_durability_attestation_validated" \
    "$no_pvc_namespace_validate" \
    "$no_pvc_namespace_validated" \
    "${output_dir}/readyz.json" \
    "$hiqlite_authority_attestation_file" \
    "$hiqlite_authority_attestation_validated" \
    "$hiqlite_backend_time_assessment_file" \
    "$hiqlite_backend_time_attestation_file" \
    "$hiqlite_backend_time_attestation_validated" \
    "$api_tls_enabled" \
    "$api_tls_auth_smoke_passed" \
    "$api_tls_certificate_sha256" \
    "$api_tls_evidence_file" \
    "$ingress_tls_auth_attestation_file" \
    "$ingress_tls_auth_attestation_validated" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    path,
    run_id,
    product_deployment_id,
    cluster,
    namespace,
    context,
    evidence_level,
    api_image,
    api_image_digest,
    meta_enabled,
    meta_backend,
    meta_image,
    meta_image_digest,
    api_replica_count,
    standing_runtime_fencing,
    api_auth_mode,
    api_bearer_token_source,
    admin_bearer_token_source,
    api_auth_observed_readyz_mode,
    api_auth_missing_token_rejected,
    api_auth_wrong_token_rejected,
    api_auth_correct_token_smoke_passed,
    api_auth_data_plane_token_rejected_on_admin_route,
    api_healthz_unauthenticated,
    api_readyz_unauthenticated,
    api_deployment_env_verified,
    api_query_policy_smoke_passed,
    api_query_policy_missing_policy_rejected,
    api_query_policy_weak_policy_rejected,
    api_openapi_catalog_smoke_passed,
    ingest_writer_smoke,
    ingest_writer_image,
    ingest_writer_job_completed,
    ingest_writer_append_outcome,
    ingest_writer_object_key,
    ingest_writer_lifecycle_attestation_file,
    ingest_writer_lifecycle_attestation_validated,
    ingest_writer_lifecycle_attestation_source,
    meta_fencing_adversarial_smoke_passed,
    meta_fencing_adversarial_smoke_log,
    multi_replica_fencing_smoke,
    multi_replica_fencing_smoke_passed,
    multi_replica_fencing_smoke_evidence_file,
    standing_runtime_failover_smoke,
    standing_runtime_failover_smoke_passed,
    standing_runtime_failover_smoke_evidence_file,
    preserve_state,
    s3_prefix,
    meta_s3_prefix,
    s3_credentials_source,
    s3_credentials_secret_name,
    s3_credentials_secret_managed,
    object_store_namespace_count,
    object_store_artifact_catalog_conditional_update,
    object_store_mode,
    object_store_local_development_authority,
    s3_backend_label,
    s3_durability_label,
    s3_endpoint,
    s3_authority_store_id,
    bucket,
    aws_region,
    s3_force_path_style,
    s3_credentials_hash,
    external_s3_validate,
    external_s3_bucket_validated,
    external_s3_prefix_validated,
    external_s3_validation_key,
    object_store_durability_attestation_file,
    object_store_durability_attestation_validated,
    no_pvc_namespace_validate,
    no_pvc_namespace_validated,
    readyz_path,
    hiqlite_authority_attestation_file,
    hiqlite_authority_attestation_validated,
    hiqlite_backend_time_assessment_file,
    hiqlite_backend_time_attestation_file,
    hiqlite_backend_time_attestation_validated,
    api_tls_enabled,
    api_tls_auth_smoke_passed,
    api_tls_certificate_sha256,
    api_tls_evidence_file,
    ingress_tls_auth_attestation_file,
    ingress_tls_auth_attestation_validated,
) = sys.argv[1:]

readyz = {}
try:
    with open(readyz_path, "r", encoding="utf-8") as f:
        readyz = json.load(f)
except FileNotFoundError:
    readyz = {}
metadata_store_readyz = readyz.get("metadata_store") or {}
standing_capability = metadata_store_readyz.get("standing_runtime_fencing")
production_multi_writer_safe = bool(
    isinstance(standing_capability, dict)
    and standing_capability.get("production_multi_writer_safe") is True
)
multi_writer_fencing_safe = bool(
    isinstance(standing_capability, dict)
    and standing_capability.get("multi_writer_fencing_safe") is True
)
bounded_wall_clock_failover = bool(
    isinstance(standing_capability, dict)
    and standing_capability.get("bounded_wall_clock_failover") is True
)
production_bounded_failover_safe = bool(
    isinstance(standing_capability, dict)
    and standing_capability.get("production_bounded_failover_safe") is True
)
authoritative_backend_time = bool(
    isinstance(standing_capability, dict)
    and standing_capability.get("authoritative_backend_time") is True
)
if multi_replica_fencing_smoke_passed == "1":
    multi_replica_fencing_smoke_status = "pass"
elif multi_replica_fencing_smoke != "1":
    multi_replica_fencing_smoke_status = "disabled"
elif standing_runtime_fencing == "required" and not production_multi_writer_safe:
    multi_replica_fencing_smoke_status = "blocked_by_capability"
elif standing_runtime_fencing == "logical-fencing" and not multi_writer_fencing_safe:
    multi_replica_fencing_smoke_status = "blocked_by_capability"
else:
    multi_replica_fencing_smoke_status = "not_run"
if standing_runtime_failover_smoke_passed == "1":
    standing_runtime_failover_smoke_status = "pass"
elif standing_runtime_failover_smoke == "0":
    standing_runtime_failover_smoke_status = "disabled"
else:
    standing_runtime_failover_smoke_status = "not_run"

blocked_reason = None
backend_time_source = None
if isinstance(standing_capability, dict):
    backend_time_source = standing_capability.get("backend_time_source_kind") or (
        "metadata_authority" if authoritative_backend_time else "not_authoritative"
    )
    if not production_multi_writer_safe:
        blocked_reason = standing_capability.get("backend_time_blocked_reason")
        if not blocked_reason and metadata_store_readyz.get("standing_runtime_fencing", {}).get("backend_name") == "hiqlite" and not authoritative_backend_time:
            blocked_reason = "hiqlite_authoritative_backend_time_false"
        elif not blocked_reason:
            blocked_reason = "metadata_backend_not_production_multi_writer_safe"
elif meta_enabled == "1":
    blocked_reason = "metadata_capability_missing_from_readyz"
else:
    blocked_reason = "metadata_store_disabled"

hiqlite_authority_attestation = None
if hiqlite_authority_attestation_file:
    with open(hiqlite_authority_attestation_file, "r", encoding="utf-8") as f:
        raw_attestation = json.load(f)
    hiqlite_authority_attestation = {
        "validated": hiqlite_authority_attestation_validated == "1",
        "evidence": "hiqlite-authority-attestation.json"
        if hiqlite_authority_attestation_validated == "1"
        else None,
        "authority_kind": raw_attestation.get("authority_kind"),
        "schema_version": raw_attestation.get("schema_version"),
        "nodes": raw_attestation.get("nodes"),
        "expected_voter_count": raw_attestation.get("expected_voter_count"),
        "no_pvc_created_by_vind": raw_attestation.get("no_pvc_created_by_vind"),
        "metadata_authority_no_pvc_used": raw_attestation.get("metadata_authority_no_pvc_used"),
        "metadata_authority_storage_mode": raw_attestation.get("metadata_authority_storage_mode"),
        "voters_learner_only_disabled": raw_attestation.get("voters_learner_only_disabled"),
        "api_auth_configured": raw_attestation.get("api_auth_configured"),
        "raft_auth_configured": raw_attestation.get("raft_auth_configured"),
        "transport_security": raw_attestation.get("transport_security"),
        "backup_restore_configured": raw_attestation.get("backup_restore_configured"),
        "image_digest": raw_attestation.get("image_digest"),
        "source_revision": raw_attestation.get("source_revision"),
        "raft_secret_sha256": raw_attestation.get("raft_secret_sha256"),
        "no_pvc_evidence_files": raw_attestation.get("no_pvc_evidence_files"),
        "attested_at": raw_attestation.get("attested_at"),
        "attester": raw_attestation.get("attester"),
    }

hiqlite_backend_time_assessment = None
if hiqlite_backend_time_assessment_file:
    try:
        with open(hiqlite_backend_time_assessment_file, "r", encoding="utf-8") as f:
            raw_assessment = json.load(f)
    except FileNotFoundError:
        raw_assessment = None
    if raw_assessment:
        hiqlite_backend_time_assessment = {
            "validated": True,
            "evidence": "hiqlite-backend-time-assessment.json",
            "schema_version": raw_assessment.get("schema_version"),
            "evidence_kind": raw_assessment.get("evidence_kind"),
            "required_mode_supported": raw_assessment.get("required_mode_supported"),
            "can_generate_product_complete_backend_time_attestation": raw_assessment.get(
                "can_generate_product_complete_backend_time_attestation"
            ),
            "backend_time_source_kind": raw_assessment.get("backend_time_source_kind"),
            "backend_time_blocked_reason": raw_assessment.get("backend_time_blocked_reason"),
            "lease_authority_kind": raw_assessment.get("lease_authority_kind"),
            "lease_expiry_semantics": raw_assessment.get("lease_expiry_semantics"),
            "bounded_wall_clock_failover": raw_assessment.get("bounded_wall_clock_failover"),
            "trusted_for_product_complete": False,
        }

hiqlite_backend_time_attestation = None
if hiqlite_backend_time_attestation_file and hiqlite_backend_time_attestation_validated == "1":
    try:
        with open(hiqlite_backend_time_attestation_file, "r", encoding="utf-8") as f:
            raw_attestation = json.load(f)
    except FileNotFoundError:
        raw_attestation = None
    if raw_attestation:
        hiqlite_backend_time_attestation = {
            "validated": hiqlite_backend_time_attestation_validated == "1",
            "evidence": "hiqlite-backend-time-attestation.json"
            if hiqlite_backend_time_attestation_validated == "1"
            else None,
            "schema_version": raw_attestation.get("schema_version"),
            "evidence_kind": raw_attestation.get("evidence_kind"),
            "backend_name": raw_attestation.get("backend_name"),
            "time_source_kind": raw_attestation.get("time_source_kind"),
            "lease_authority_kind": raw_attestation.get("lease_authority_kind"),
            "lease_expiry_semantics": raw_attestation.get("lease_expiry_semantics"),
            "authoritative_backend_time": raw_attestation.get("authoritative_backend_time"),
            "bounded_wall_clock_failover": raw_attestation.get("bounded_wall_clock_failover"),
            "production_bounded_failover_safe": raw_attestation.get("production_bounded_failover_safe"),
            "authority_sampled_unix_time_ms_in_raft_operation": raw_attestation.get(
                "authority_sampled_unix_time_ms_in_raft_operation"
            ),
            "owner_expiry_bound_to_authority_time": raw_attestation.get(
                "owner_expiry_bound_to_authority_time"
            ),
            "checkpoint_publish_rejects_expired_owner_with_authority_time": raw_attestation.get(
                "checkpoint_publish_rejects_expired_owner_with_authority_time"
            ),
            "bounded_failover_probe_passed": raw_attestation.get("bounded_failover_probe_passed"),
            "failover_time_bound_ms": raw_attestation.get("failover_time_bound_ms"),
            "observed_max_failover_ms": raw_attestation.get("observed_max_failover_ms"),
            "metrics_time_source_rejected": raw_attestation.get("metrics_time_source_rejected"),
            "raft_log_index_time_source_rejected": raw_attestation.get(
                "raft_log_index_time_source_rejected"
            ),
            "distributed_lock_ttl_source_rejected": raw_attestation.get(
                "distributed_lock_ttl_source_rejected"
            ),
            "trusted_for_product_complete": raw_attestation.get("trusted_for_product_complete"),
            "trusted_for_release_validator": raw_attestation.get("trusted_for_release_validator"),
            "release_validator_fail_closed": raw_attestation.get("release_validator_fail_closed"),
            "attested_at": raw_attestation.get("attested_at"),
            "attester": raw_attestation.get("attester"),
        }

local_tls_auth_smoke = None
if api_tls_enabled == "1":
    local_tls_auth_smoke = {
        "enabled": True,
        "passed": api_tls_auth_smoke_passed == "1",
        "evidence": "tls-auth-smoke.json" if api_tls_auth_smoke_passed == "1" else None,
        "tls_certificate_sha256": api_tls_certificate_sha256 or None,
        "cert_authority": "generated-self-signed-local" if api_tls_auth_smoke_passed == "1" else None,
        "scope": "local port-forwarded vind/vCluster service",
        "public_ingress_attestation": False,
        "trusted_for_product_complete": False,
    }
else:
    local_tls_auth_smoke = {
        "enabled": False,
        "passed": False,
        "evidence": None,
        "scope": "disabled",
        "public_ingress_attestation": False,
        "trusted_for_product_complete": False,
    }

ingress_tls_auth_attestation = None
if ingress_tls_auth_attestation_file:
    with open(ingress_tls_auth_attestation_file, "r", encoding="utf-8") as f:
        raw_attestation = json.load(f)
    ingress_tls_auth_attestation = {
        "validated": ingress_tls_auth_attestation_validated == "1",
        "evidence": "ingress-tls-auth-attestation.json"
        if ingress_tls_auth_attestation_validated == "1"
        else None,
        "evidence_kind": raw_attestation.get("evidence_kind"),
        "schema_version": raw_attestation.get("schema_version"),
        "endpoint_url": raw_attestation.get("endpoint_url"),
        "external_hostname": raw_attestation.get("external_hostname"),
        "ingress_controller": raw_attestation.get("ingress_controller"),
        "transport_security": raw_attestation.get("transport_security"),
        "public_ingress_attestation": raw_attestation.get("public_ingress_attestation"),
        "trusted_for_product_complete": raw_attestation.get("trusted_for_product_complete"),
        "tls_enabled": raw_attestation.get("tls_enabled"),
        "tls_certificate_sha256": raw_attestation.get("tls_certificate_sha256"),
        "tls_certificate_issuer": raw_attestation.get("tls_certificate_issuer"),
        "auth_enforced": raw_attestation.get("auth_enforced"),
        "missing_token_rejected": raw_attestation.get("missing_token_rejected"),
        "wrong_token_rejected": raw_attestation.get("wrong_token_rejected"),
        "admin_auth_separate": raw_attestation.get("admin_auth_separate"),
        "admin_route_missing_token_rejected": raw_attestation.get(
            "admin_route_missing_token_rejected"
        ),
        "admin_route_wrong_token_rejected": raw_attestation.get(
            "admin_route_wrong_token_rejected"
        ),
        "admin_token_accepted_on_admin_route": raw_attestation.get(
            "admin_token_accepted_on_admin_route"
        ),
        "data_plane_token_rejected_on_admin_route": raw_attestation.get(
            "data_plane_token_rejected_on_admin_route"
        ),
        "attested_at": raw_attestation.get("attested_at"),
        "attester": raw_attestation.get("attester"),
    }

object_store_durability_attestation = None
if object_store_durability_attestation_file:
    with open(object_store_durability_attestation_file, "r", encoding="utf-8") as f:
        raw_attestation = json.load(f)
    object_store_durability_attestation = {
        "validated": object_store_durability_attestation_validated == "1",
        "evidence": "object-store-durability-attestation.json"
        if object_store_durability_attestation_validated == "1"
        else None,
        "evidence_kind": raw_attestation.get("evidence_kind"),
        "schema_version": raw_attestation.get("schema_version"),
        "provider_kind": raw_attestation.get("provider_kind"),
        "authority_store_id": raw_attestation.get("authority_store_id"),
        "bucket": raw_attestation.get("bucket"),
        "s3_prefix": raw_attestation.get("s3_prefix"),
        "versioning_or_object_lock_enabled": raw_attestation.get("versioning_or_object_lock_enabled"),
        "server_side_encryption_enabled": raw_attestation.get("server_side_encryption_enabled"),
        "backup_or_replication_configured": raw_attestation.get("backup_or_replication_configured"),
        "lifecycle_delete_policy_reviewed": raw_attestation.get("lifecycle_delete_policy_reviewed"),
        "destructive_delete_protection_reviewed": raw_attestation.get("destructive_delete_protection_reviewed"),
        "cost_controls_reviewed": raw_attestation.get("cost_controls_reviewed"),
        "attested_at": raw_attestation.get("attested_at"),
        "attester": raw_attestation.get("attester"),
    }

ingest_writer_lifecycle_attestation = None
if ingest_writer_lifecycle_attestation_file:
    with open(ingest_writer_lifecycle_attestation_file, "r", encoding="utf-8") as f:
        raw_attestation = json.load(f)
    ingest_writer_lifecycle_attestation = {
        "validated": ingest_writer_lifecycle_attestation_validated == "1",
        "source": ingest_writer_lifecycle_attestation_source,
        "trusted_for_product_complete": ingest_writer_lifecycle_attestation_source == "generated",
        "evidence_kind": raw_attestation.get("evidence_kind"),
        "schema_version": raw_attestation.get("schema_version"),
        "deployment_id": raw_attestation.get("deployment_id"),
        "authority_store_id": raw_attestation.get("authority_store_id"),
        "deployed_topology": raw_attestation.get("deployed_topology"),
        "pod_internal_append_completed": raw_attestation.get("pod_internal_append_completed"),
        "multi_pod_overlap_conflict_rejected": raw_attestation.get(
            "multi_pod_overlap_conflict_rejected"
        ),
        "adjacent_append_succeeded": raw_attestation.get("adjacent_append_succeeded"),
        "crash_restart_reconstruction_checked": raw_attestation.get(
            "crash_restart_reconstruction_checked"
        ),
        "leader_handoff_checked": raw_attestation.get("leader_handoff_checked"),
        "kubernetes_lease_handoff_checked": raw_attestation.get(
            "kubernetes_lease_handoff_checked"
        ),
        "lease_held_through_append_checked": raw_attestation.get(
            "lease_held_through_append_checked"
        ),
        "commit_guard_checked": raw_attestation.get("commit_guard_checked"),
        "admission_commit_guard_bound_checked": raw_attestation.get(
            "admission_commit_guard_bound_checked"
        ),
        "lease_loss_during_reservation_checked": raw_attestation.get(
            "lease_loss_during_reservation_checked"
        ),
        "no_pvc_created_by_vind": raw_attestation.get("no_pvc_created_by_vind"),
        "evidence_provenance": raw_attestation.get("evidence_provenance"),
        "evidence_files": raw_attestation.get("evidence_files"),
        "attested_at": raw_attestation.get("attested_at"),
        "attester": raw_attestation.get("attester"),
    }

product_complete_blockers = []
if not production_multi_writer_safe:
    if multi_writer_fencing_safe and not production_bounded_failover_safe:
        product_complete_blockers.append(
            "metadata backend proves multi-writer fencing, but bounded wall-clock failover is not proven"
        )
    elif blocked_reason in {
        "hiqlite_authoritative_backend_time_false",
        "hiqlite_raft_replicated_authority_time_primitive_missing",
    }:
        product_complete_blockers.append(
            "Hiqlite metadata authority shape may be attested, but backend-time lease semantics are not proven"
        )
    else:
        product_complete_blockers.append(
            f"metadata backend is not production multi-writer safe: {blocked_reason or 'unknown'}"
        )
if (
    meta_enabled == "1"
    and meta_backend == "hiqlite"
    and standing_runtime_fencing == "required"
    and backend_time_source == "raft_replicated_authority_time"
):
    if not hiqlite_backend_time_attestation:
        product_complete_blockers.append(
            "Hiqlite backend-time attestation was not generated from deployed product smoke"
        )
    elif hiqlite_backend_time_attestation.get("trusted_for_release_validator") is not True:
        product_complete_blockers.append(
            "Hiqlite backend-time attestation is diagnostic and release validator remains fail-closed"
        )
if standing_runtime_fencing != "unsafe-dev-only" and meta_fencing_adversarial_smoke_passed != "1":
    product_complete_blockers.append(
        "metadata standing-runtime stale-owner/checkpoint adversarial smoke was not proven"
    )
if multi_replica_fencing_smoke_status == "blocked_by_capability":
    product_complete_blockers.append(
        "multi-replica adversarial ingest/fencing smoke is blocked by metadata fencing capability"
    )
elif multi_replica_fencing_smoke_status == "not_run":
    product_complete_blockers.append(
        "multi-replica adversarial ingest/fencing smoke was not run"
    )
elif multi_replica_fencing_smoke_status == "disabled":
    product_complete_blockers.append(
        "multi-replica adversarial ingest/fencing smoke was disabled"
    )
if ingress_tls_auth_attestation_validated != "1":
    if api_tls_auth_smoke_passed == "1":
        product_complete_blockers.append(
            "local vind TLS/auth smoke passed, but public ingress/TLS/auth attestation is missing"
        )
    else:
        product_complete_blockers.append(
            "REST API bearer auth exists, but product-complete ingress/TLS/auth evidence is outside this script"
        )
if object_store_mode != "external-s3":
    product_complete_blockers.append("RustFS is deployed with emptyDir, so object-store state is ephemeral across pod loss")
elif external_s3_bucket_validated != "1":
    product_complete_blockers.append("external S3-compatible bucket reachability was not validated from inside the cluster")
elif external_s3_prefix_validated != "1":
    product_complete_blockers.append("external S3-compatible authority prefix read/write/list/delete was not validated from inside the cluster")
elif object_store_local_development_authority == "1":
    product_complete_blockers.append(
        "external S3-compatible authority is local development RustFS and cannot prove production durability policy"
    )
elif object_store_durability_attestation_validated != "1":
    product_complete_blockers.append("external S3-compatible authority lacks operator-reviewed durability policy attestation")
if no_pvc_namespace_validated != "1":
    product_complete_blockers.append("no-PVC namespace validation was not performed")
if ingest_writer_job_completed != "1":
    product_complete_blockers.append("deployed ingest-writer Pod-internal checked append was not proven")
elif ingest_writer_append_outcome != "appended":
    product_complete_blockers.append(
        f"deployed ingest-writer Pod-internal append did not create a fresh append outcome: {ingest_writer_append_outcome or 'unknown'}"
    )
if ingest_writer_lifecycle_attestation_validated != "1":
    product_complete_blockers.append(
        "deployed ingest-writer multi-pod overlap/adjacent/crash/restart/lease-loss/Kubernetes-Lease-handoff evidence is missing"
    )
elif ingest_writer_lifecycle_attestation_source != "generated":
    product_complete_blockers.append(
        "deployed ingest-writer lifecycle attestation was externally supplied; only script-generated Kubernetes Job evidence can clear product-complete"
    )
if not api_image_digest:
    product_complete_blockers.append("velorix-api deployed image digest was not recorded")
if meta_enabled == "1" and not meta_image_digest:
    product_complete_blockers.append("velorix-meta deployed image digest was not recorded")

deployed_images = {
    "velorix-api": {
        "image": api_image,
        "image_digest": api_image_digest or None,
        "evidence_files": {
            "manifest": "velorix-api.yaml",
            "deployment": "velorix-api-deployment-observed.json",
            "pods": "velorix-api-pods.json",
        },
    }
}
if meta_enabled == "1":
    deployed_images["velorix-meta"] = {
        "image": meta_image,
        "image_digest": meta_image_digest or None,
        "evidence_files": {
            "manifest": "velorix-meta.yaml",
            "deployment": "velorix-meta-deployment-observed.json",
            "pods": "velorix-meta-pods.json",
        },
    }

evidence = {
    "schema_version": 1,
    "evidence_kind": "velorix_product_slice_evidence",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "run_id": run_id,
    "deployment_id": product_deployment_id,
    "cluster": cluster,
    "namespace": namespace,
    "context": context,
    "evidence_level": evidence_level,
    "product_complete": len(product_complete_blockers) == 0,
    "product_complete_blockers": product_complete_blockers,
    "rest_callable": True,
    "rest_scope": "local port-forwarded vind/vCluster service",
    "deployed_images": deployed_images,
    "object_store": {
        "mode": object_store_mode,
        "backend": s3_backend_label,
        "durability": s3_durability_label,
        "local_development_authority": object_store_local_development_authority == "1",
        "endpoint": s3_endpoint,
        "authority_store_id": s3_authority_store_id,
        "bucket": bucket,
        "region": aws_region,
        "force_path_style": s3_force_path_style == "1",
        "s3_prefix": s3_prefix,
        "preserve_state_requested": preserve_state == "1",
        "credentials_source": s3_credentials_source,
        "credentials_secret_name": s3_credentials_secret_name,
        "credentials_secret_managed": s3_credentials_secret_managed == "1",
        "credentials_sha256": s3_credentials_hash,
        "external_s3_validate_enabled": object_store_mode == "external-s3"
        and external_s3_validate == "1",
        "external_s3_bucket_validated": object_store_mode == "external-s3"
        and external_s3_bucket_validated == "1",
        "external_s3_prefix_validated": object_store_mode == "external-s3"
        and external_s3_prefix_validated == "1",
        "external_s3_validation_key": external_s3_validation_key or None,
        "external_s3_validation_evidence": {
            "job": "external-s3-validate-job.json",
            "log": "external-s3-validate.log",
        }
        if object_store_mode == "external-s3"
        and external_s3_bucket_validated == "1"
        and external_s3_prefix_validated == "1"
        else None,
        "durability_policy_attestation": object_store_durability_attestation,
        "active_view_cas": {
            "readyz_namespace_count": int(object_store_namespace_count),
            "artifact_catalog_conditional_update": object_store_artifact_catalog_conditional_update == "1",
            "evidence": "readyz object_store.artifact_catalog.conditional_update",
        },
    },
    "no_pvc": {
        "namespace_validation_enabled": no_pvc_namespace_validate == "1",
        "namespace_validated": no_pvc_namespace_validated == "1",
        "evidence": "no-pvc-namespace.json" if no_pvc_namespace_validated == "1" else None,
        "contract": "no PersistentVolumeClaim objects in the Velorix product namespace",
        "managed_hiqlite_authority_validated": bool(
            hiqlite_authority_attestation
            and hiqlite_authority_attestation.get("authority_kind") == "velorix_managed_hiqlite"
            and hiqlite_authority_attestation.get("no_pvc_created_by_vind") is True
            and hiqlite_authority_attestation.get("metadata_authority_no_pvc_used") is True
        ),
    },
    "metadata_store": {
        "enabled": meta_enabled == "1",
        "backend": meta_backend,
        "meta_s3_prefix": meta_s3_prefix if meta_enabled == "1" else None,
        "hiqlite_authority_attestation": hiqlite_authority_attestation,
        "hiqlite_backend_time_assessment": hiqlite_backend_time_assessment,
        "hiqlite_backend_time_attestation": hiqlite_backend_time_attestation,
        "standing_runtime_adversarial_smoke": {
            "status": "pass" if meta_fencing_adversarial_smoke_passed == "1" else (
                "not_required" if standing_runtime_fencing == "unsafe-dev-only" else "not_run"
            ),
            "evidence": "velorix-meta-smoke.log"
            if meta_fencing_adversarial_smoke_passed == "1"
            else None,
            "assertions": {
                "logical_owner_expiry_checked": meta_fencing_adversarial_smoke_passed == "1",
                "new_owner_epoch_fences_old_owner": meta_fencing_adversarial_smoke_passed == "1",
                "stale_owner_checkpoint_publish_rejected": meta_fencing_adversarial_smoke_passed == "1",
                "stale_checkpoint_pointer_publish_conflicted": meta_fencing_adversarial_smoke_passed == "1",
                "latest_checkpoint_remains_metadata_authoritative": meta_fencing_adversarial_smoke_passed == "1",
            },
        },
    },
    "standing_runtime_fencing": {
        "configured_mode": standing_runtime_fencing,
        "required_mode": standing_runtime_fencing == "required",
        "logical_fencing_mode": standing_runtime_fencing == "logical-fencing",
        "metadata_fencing_enforced": standing_runtime_fencing != "unsafe-dev-only",
        "metadata_backend": meta_backend if meta_enabled == "1" else None,
        "capability": standing_capability,
        "backend_time_source": backend_time_source,
        "multi_writer_fencing_safe": multi_writer_fencing_safe,
        "bounded_wall_clock_failover": bounded_wall_clock_failover,
        "production_bounded_failover_safe": production_bounded_failover_safe,
        "blocked_reason": blocked_reason,
        "multi_replica_fencing_smoke": {
            "status": multi_replica_fencing_smoke_status,
            "enabled": multi_replica_fencing_smoke == "1",
            "evidence": "multi-replica-fencing-smoke.json"
            if multi_replica_fencing_smoke_passed == "1"
            else None,
        },
        "local_api_pod_failover_smoke": {
            "status": standing_runtime_failover_smoke_status,
            "enabled": standing_runtime_failover_smoke != "0",
            "evidence": "standing-runtime-failover-smoke.json"
            if standing_runtime_failover_smoke_passed == "1"
            else None,
            "scope": "local vind product API pod deletion and owner reacquire smoke",
            "trusted_for_product_complete": False,
            "production_wall_clock_failover_attestation": False,
        },
    },
    "api": {
        "replica_count": int(api_replica_count),
        "standing_runtime_fencing": standing_runtime_fencing,
        "openapi": {
            "catalog_smoke_passed": api_openapi_catalog_smoke_passed == "1",
            "evidence_file": "openapi.json"
            if api_openapi_catalog_smoke_passed == "1"
            else None,
            "promoted_api_path": "/v1/api/scores/positive",
            "promoted_api_path_present": api_openapi_catalog_smoke_passed == "1",
            "generic_query_path_absent": api_openapi_catalog_smoke_passed == "1",
            "legacy_parameterized_path_absent": api_openapi_catalog_smoke_passed == "1",
            "query_policy_extension_present": api_openapi_catalog_smoke_passed == "1",
            "linked_view_policy_id": "interactive"
            if api_openapi_catalog_smoke_passed == "1"
            else None,
            "response_schema_checked": api_openapi_catalog_smoke_passed == "1",
        },
        "query_policy": {
            "catalog_smoke_passed": api_query_policy_smoke_passed == "1",
            "production_bounds_required": api_query_policy_weak_policy_rejected == "1",
            "weak_policy_rejected": api_query_policy_weak_policy_rejected == "1",
            "missing_policy_rejected": api_query_policy_missing_policy_rejected == "1",
            "linked_view_policy_id": "interactive"
            if api_query_policy_smoke_passed == "1"
            else None,
            "evidence_files": {
                "created": "query-policy-interactive.json",
                "read_back": "query-policy-interactive-read.json",
                "weak_policy_rejection": "query-policy-weak-rejection.json",
                "missing_policy_rejection": "query-policy-missing-view.json",
            }
            if api_query_policy_smoke_passed == "1"
            else None,
        },
        "auth": {
            "mode": api_auth_mode,
            "token_source": api_bearer_token_source,
            "secret_name": "velorix-api-auth" if api_auth_mode == "bearer-token" else None,
            "admin_token_source": admin_bearer_token_source,
            "admin_secret_name": "velorix-admin-auth" if api_auth_mode == "bearer-token" else None,
            "observed_readyz_mode": api_auth_observed_readyz_mode or None,
            "missing_token_rejected": api_auth_missing_token_rejected == "1",
            "wrong_token_rejected": api_auth_wrong_token_rejected == "1",
            "correct_token_smoke_passed": api_auth_correct_token_smoke_passed == "1",
            "data_plane_token_rejected_on_admin_route": api_auth_data_plane_token_rejected_on_admin_route == "1",
            "healthz_unauthenticated": api_healthz_unauthenticated == "1",
            "readyz_unauthenticated": api_readyz_unauthenticated == "1",
            "deployment_env_verified": api_deployment_env_verified == "1",
            "local_tls_auth_smoke": local_tls_auth_smoke,
            "ingress_tls_auth_attestation": ingress_tls_auth_attestation,
        },
    },
    "ingest_writer": {
        "smoke_enabled": ingest_writer_smoke == "1",
        "image": ingest_writer_image,
        "default_entrypoint_mode": "lease-guarded-append",
        "raw_append_command_scope": "diagnostic-explicit-cli-only",
        "job_completed": ingest_writer_job_completed == "1",
        "append_outcome": ingest_writer_append_outcome or None,
        "object_key": ingest_writer_object_key or None,
        "pod_internal_append_verified": ingest_writer_job_completed == "1"
        and ingest_writer_append_outcome == "appended",
        "lifecycle_attestation": ingest_writer_lifecycle_attestation,
        "evidence_files": {
            "job_log": "ingest-writer-job-log.json",
            "job": "ingest-writer-job.json",
            "pods": "ingest-writer-pods.json",
        }
        if ingest_writer_job_completed == "1"
        else None,
    },
}

with open(path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

assert_product_complete_evidence() {
  python3 - "${output_dir}/product-evidence.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    evidence = json.load(f)
if evidence.get("product_complete") is True:
    raise SystemExit(0)
blockers = evidence.get("product_complete_blockers") or ["unknown blocker"]
print("VELORIX_PRODUCT_EVIDENCE_LEVEL=product-complete failed; blockers:", file=sys.stderr)
for blocker in blockers:
    print(f"- {blocker}", file=sys.stderr)
raise SystemExit(65)
PY
}

cd "$repo_root"
mkdir -p "$output_dir"
preflight_docker_daemon
check_local_disk_preflight
trap 'status=$?; cleanup_vind "$status"; exit "$status"' EXIT

previous_context="$(kubectl config current-context 2>/dev/null || true)"

if [ "$cluster_driver" = "docker-vcluster" ]; then
  vcluster use driver docker >/dev/null

  if vcluster_exists; then
    if [ "$reuse_existing" != "1" ]; then
      echo "vcluster already exists: ${cluster}; set VELORIX_VIND_REUSE_EXISTING=1 or choose another name" >&2
      exit 1
    fi
  else
    created_cluster=1
    if ! create_vcluster_with_retry; then
      exit 75
    fi
  fi

  if ! kubectl config use-context "$context" >/dev/null 2>"${output_dir}/vcluster-context.log"; then
    cat "${output_dir}/vcluster-context.log" >&2 || true
    write_local_vcluster_bootstrap_blocker "vcluster-context" "${output_dir}/vcluster-context.log"
    exit 75
  fi
  if ! validate_local_vcluster_context 2>"${output_dir}/vcluster-context-validate.log"; then
    cat "${output_dir}/vcluster-context-validate.log" >&2 || true
    write_local_vcluster_bootstrap_blocker "vcluster-context-validate" "${output_dir}/vcluster-context-validate.log"
    exit 75
  fi
else
  if ! validate_existing_kubernetes_context 2>"${output_dir}/existing-context-validate.log"; then
    cat "${output_dir}/existing-context-validate.log" >&2 || true
    exit 75
  fi
fi
if ! wait_for_kubernetes 2>"${output_dir}/vcluster-readyz.log"; then
  cat "${output_dir}/vcluster-readyz.log" >&2 || true
  write_local_vcluster_bootstrap_blocker "vcluster-readyz" "${output_dir}/vcluster-readyz.log"
  exit 75
fi
if ! wait_for_kubernetes_scheduling_ready 2>"${output_dir}/vcluster-scheduling-ready.log"; then
  cat "${output_dir}/vcluster-scheduling-ready.log" >&2 || true
fi
validate_managed_storage_class
check_kubernetes_scheduling_health "before-image-build"

docker_build_args=()
if [ "$docker_build_no_cache" = "1" ]; then
  docker_build_args+=(--no-cache)
fi
if [ "$build_api_image" = "1" ] || \
  { [ "$meta_enabled" = "1" ] && [ "$build_meta_image" = "1" ]; } || \
  { [ "$hiqlite_deploy" = "1" ] && [ "$build_hiqlite_image" = "1" ]; } || \
  { [ "$ingest_writer_smoke" = "1" ] && [ "$build_ingest_writer_image" = "1" ]; }; then
  if [ ! -f "${hiqlite_local_source_dir}/hiqlite/Cargo.toml" ]; then
    echo "VELORIX_HIQLITE_LOCAL_SOURCE_DIR must point to a hiqlite checkout containing hiqlite/Cargo.toml: ${hiqlite_local_source_dir}" >&2
    exit 64
  fi
  docker_build_args+=(--build-context "velorix-hiqlite-source=${hiqlite_local_source_dir}")
fi

if [ "$build_api_image" = "1" ]; then
  echo "building ${api_image}"
  DOCKER_BUILDKIT=1 docker build "${docker_build_args[@]}" -f Dockerfile.api -t "$api_image" .
  load_image_into_product_cluster "$api_image"
else
  if [ "$load_existing_images" = "1" ]; then
    echo "skipping api image build for ${api_image}; loading existing local image into the selected product cluster"
    load_existing_image_into_product_cluster "$api_image"
  else
    echo "skipping api image build/load for ${api_image}; assuming it is already available in the selected product cluster"
  fi
fi

if [ "$meta_enabled" = "1" ]; then
  if [ "$build_meta_image" = "1" ]; then
    echo "building ${meta_image}"
    DOCKER_BUILDKIT=1 docker build "${docker_build_args[@]}" -f Dockerfile.meta -t "$meta_image" .
    load_image_into_product_cluster "$meta_image"
  else
    if [ "$load_existing_images" = "1" ]; then
      echo "skipping meta image build for ${meta_image}; loading existing local image into the selected product cluster"
      load_existing_image_into_product_cluster "$meta_image"
    else
      echo "skipping meta image build/load for ${meta_image}; assuming it is already available in the selected product cluster"
    fi
  fi
fi

if [ -z "$api_image_digest" ]; then
  api_image_digest="$(resolve_local_image_digest "$api_image")"
fi
if [ "$meta_enabled" = "1" ] && [ -z "$meta_image_digest" ]; then
  meta_image_digest="$(resolve_local_image_digest "$meta_image")"
fi

if [ "$hiqlite_deploy" = "1" ]; then
  if [ "$build_hiqlite_image" = "1" ]; then
    echo "building ${hiqlite_image}"
    DOCKER_BUILDKIT=1 docker build "${docker_build_args[@]}" -f Dockerfile.hiqlite -t "$hiqlite_image" .
    load_image_into_product_cluster "$hiqlite_image"
  else
    if [ "$load_existing_images" = "1" ]; then
      echo "skipping hiqlite image build for ${hiqlite_image}; loading existing local image into the selected product cluster"
      load_existing_image_into_product_cluster "$hiqlite_image"
    else
      echo "skipping hiqlite image build/load for ${hiqlite_image}; assuming it is already available in the selected product cluster"
    fi
  fi
fi

if [ "$ingest_writer_smoke" = "1" ]; then
  if [ "$build_ingest_writer_image" = "1" ]; then
    echo "building ${ingest_writer_image}"
    DOCKER_BUILDKIT=1 docker build "${docker_build_args[@]}" -f Dockerfile.ingest-writer -t "$ingest_writer_image" .
    load_image_into_product_cluster "$ingest_writer_image"
  else
    if [ "$load_existing_images" = "1" ]; then
      echo "skipping ingest-writer image build for ${ingest_writer_image}; loading existing local image into the selected product cluster"
      load_existing_image_into_product_cluster "$ingest_writer_image"
    else
      echo "skipping ingest-writer image build/load for ${ingest_writer_image}; assuming it is already available in the selected product cluster"
    fi
  fi
fi

check_kubernetes_scheduling_health "after-image-load"

if ! kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  created_namespace=1
fi
kubectl --context "$context" create namespace "$namespace" --dry-run=client -o yaml \
  | kubectl --context "$context" apply -f -

if [ "$ingest_writer_smoke" = "1" ]; then
  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: velorix-ingest-writer-append
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer
    velorix.dev/run-id: "${run_id}"
automountServiceAccountToken: false
$(service_account_image_pull_secrets_yaml)
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: velorix-ingest-writer-lease-probe
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer
    velorix.dev/run-id: "${run_id}"
$(service_account_image_pull_secrets_yaml)
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: velorix-ingest-writer-lease-probe
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer
    velorix.dev/run-id: "${run_id}"
rules:
  - apiGroups:
      - coordination.k8s.io
    resources:
      - leases
    verbs:
      - get
      - create
      - update
      - patch
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: velorix-ingest-writer-lease-probe
  namespace: ${namespace}
  labels:
    app: velorix-ingest-writer
    velorix.dev/run-id: "${run_id}"
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: velorix-ingest-writer-lease-probe
subjects:
  - kind: ServiceAccount
    name: velorix-ingest-writer-lease-probe
    namespace: ${namespace}
EOF
fi

if [ "$s3_credentials_secret_managed" = "1" ]; then
  s3_access_key_id_b64="$(base64_value "$aws_access_key_id")"
  s3_secret_access_key_b64="$(base64_value "$aws_secret_access_key")"
  s3_session_token_data=""
  if [ -n "$aws_session_token" ]; then
    s3_session_token_b64="$(base64_value "$aws_session_token")"
    s3_session_token_data="  session-token: ${s3_session_token_b64}"
  fi
  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: ${s3_credentials_secret_name}
  namespace: ${namespace}
type: Opaque
data:
  access-key-id: ${s3_access_key_id_b64}
  secret-access-key: ${s3_secret_access_key_b64}
${s3_session_token_data}
EOF
else
  existing_s3_credentials_secret_json="${output_dir}/s3-credentials-secret.json"
  if ! kubectl --context "$context" -n "$namespace" get secret "$s3_credentials_secret_name" -o json >"$existing_s3_credentials_secret_json"; then
    echo "VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0 requires existing Secret ${s3_credentials_secret_name} in namespace ${namespace}" >&2
    exit 66
  fi
  s3_credentials_hash="$(
    python3 - "$existing_s3_credentials_secret_json" <<'PY'
import base64
import hashlib
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    secret = json.load(f)
data = secret.get("data") or {}
missing = [key for key in ["access-key-id", "secret-access-key"] if key not in data]
if missing:
    raise SystemExit("existing S3 credentials Secret is missing keys: " + ", ".join(missing))
try:
    access_key = base64.b64decode(data["access-key-id"], validate=True).decode("utf-8")
    secret_key = base64.b64decode(data["secret-access-key"], validate=True).decode("utf-8")
    session_token = (
        base64.b64decode(data["session-token"], validate=True).decode("utf-8")
        if data.get("session-token")
        else ""
    )
except Exception as exc:
    raise SystemExit(f"existing S3 credentials Secret contains invalid base64/UTF-8 data: {exc}")
print(hashlib.sha256(f"{access_key}:{secret_key}:{session_token}".encode("utf-8")).hexdigest())
PY
  )"
fi

if [ "$api_auth_mode" = "bearer-token" ]; then
  api_bearer_token_b64="$(base64_value "$api_bearer_token")"
  admin_bearer_token_b64="$(base64_value "$admin_bearer_token")"
  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: velorix-api-auth
  namespace: ${namespace}
type: Opaque
data:
  bearer-token: ${api_bearer_token_b64}
EOF
  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: velorix-admin-auth
  namespace: ${namespace}
type: Opaque
data:
  admin-bearer-token: ${admin_bearer_token_b64}
EOF
  write_api_auth_helper
  api_auth_env="$(cat <<'EOF'
            - name: VELORIX_API_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: velorix-api-auth
                  key: bearer-token
            - name: VELORIX_ADMIN_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: velorix-admin-auth
                  key: admin-bearer-token
EOF
)"
else
  kubectl --context "$context" -n "$namespace" delete secret velorix-api-auth --ignore-not-found >/dev/null
  kubectl --context "$context" -n "$namespace" delete secret velorix-admin-auth --ignore-not-found >/dev/null
  rm -f "${output_dir}/api-auth.env"
  api_auth_env='            - name: VELORIX_API_ALLOW_UNAUTHENTICATED_DEV
              value: "1"'
fi

hiqlite_api_secret_ref_name="velorix-hiqlite-auth"
if [ "$meta_enabled" = "1" ] && [ -n "$hiqlite_api_secret" ]; then
  hiqlite_api_secret_b64="$(base64_value "$hiqlite_api_secret")"
  if [ "$hiqlite_deploy" = "1" ]; then
    hiqlite_raft_secret_b64="$(base64_value "$hiqlite_raft_secret")"
    hiqlite_enc_key_active_b64="$(base64_value "$hiqlite_enc_key_active")"
    hiqlite_enc_keys_b64="$(base64_value "$hiqlite_enc_keys")"
    cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: velorix-hiqlite-auth
  namespace: ${namespace}
type: Opaque
data:
  api-secret: ${hiqlite_api_secret_b64}
  raft-secret: ${hiqlite_raft_secret_b64}
  enc-key-active: ${hiqlite_enc_key_active_b64}
  enc-keys: ${hiqlite_enc_keys_b64}
EOF
  else
    hiqlite_api_secret_ref_name="velorix-meta-hiqlite-auth"
    cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: velorix-meta-hiqlite-auth
  namespace: ${namespace}
type: Opaque
data:
  api-secret: ${hiqlite_api_secret_b64}
EOF
  fi
fi

deploy_hiqlite_authority() {
cat >"${output_dir}/velorix-hiqlite.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: velorix-hiqlite
  namespace: ${namespace}
  labels:
    app: velorix-hiqlite
    velorix.dev/run-id: "${run_id}"
automountServiceAccountToken: false
$(service_account_image_pull_secrets_yaml)
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: velorix-hiqlite-config
  namespace: ${namespace}
  labels:
    app: velorix-hiqlite
    velorix.dev/run-id: "${run_id}"
data:
  hiqlite.toml: |
    [hiqlite]
    node_id_from = "k8s"
    nodes = [
      "1 velorix-hiqlite-0.velorix-hiqlite-headless:8100 velorix-hiqlite-0.velorix-hiqlite-headless:8200",
      "2 velorix-hiqlite-1.velorix-hiqlite-headless:8100 velorix-hiqlite-1.velorix-hiqlite-headless:8200",
      "3 velorix-hiqlite-2.velorix-hiqlite-headless:8100 velorix-hiqlite-2.velorix-hiqlite-headless:8200",
    ]
    listen_addr_api = "0.0.0.0"
    listen_addr_raft = "0.0.0.0"
    data_dir = "/data"
    filename_db = "velorix-hiqlite.db"
    log_statements = false
    read_pool_size = 4
    log_sync = "interval_200"
    wal_size = 2097152
    cache_storage_disk = true
    logs_until_snapshot = 10000
    health_check_delay_secs = 30
    backup_cron = "${hiqlite_backup_cron}"
    backup_keep_days = ${hiqlite_backup_keep_days}
    backup_keep_days_local = ${hiqlite_backup_keep_days_local}
---
apiVersion: v1
kind: Service
metadata:
  name: velorix-hiqlite-headless
  namespace: ${namespace}
  labels:
    app: velorix-hiqlite
    velorix.dev/run-id: "${run_id}"
spec:
  clusterIP: None
  selector:
    app: velorix-hiqlite
  ports:
    - name: raft
      port: 8100
      targetPort: 8100
    - name: api
      port: 8200
      targetPort: 8200
---
apiVersion: v1
kind: Service
metadata:
  name: velorix-hiqlite
  namespace: ${namespace}
  labels:
    app: velorix-hiqlite
    velorix.dev/run-id: "${run_id}"
spec:
  selector:
    app: velorix-hiqlite
  ports:
    - name: api
      port: 8200
      targetPort: 8200
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: velorix-hiqlite
  namespace: ${namespace}
  labels:
    app: velorix-hiqlite
    velorix.dev/run-id: "${run_id}"
spec:
  serviceName: velorix-hiqlite-headless
  replicas: 3
  selector:
    matchLabels:
      app: velorix-hiqlite
  template:
    metadata:
      labels:
        app: velorix-hiqlite
        velorix.dev/run-id: "${run_id}"
      annotations:
        velorix.dev/run-id: "${run_id}"
        velorix.dev/image-tag: "${hiqlite_image}"
        velorix.dev/hiqlite-api-secret-sha256: "${hiqlite_api_secret_hash}"
        velorix.dev/hiqlite-raft-secret-sha256: "${hiqlite_raft_secret_hash}"
        velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
        velorix.dev/managed-persistence: "${managed_persistence}"
    spec:
      serviceAccountName: velorix-hiqlite
      automountServiceAccountToken: false
$(image_pull_secrets_yaml)
      securityContext:
        seccompProfile:
          type: RuntimeDefault
        fsGroup: 65532
        fsGroupChangePolicy: OnRootMismatch
      containers:
        - name: hiqlite
          image: ${hiqlite_image}
          imagePullPolicy: ${hiqlite_image_pull_policy}
          args: ["serve", "-c", "/etc/hiqlite/hiqlite.toml"]
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: HQL_SECRET_API
              valueFrom:
                secretKeyRef:
                  name: velorix-hiqlite-auth
                  key: api-secret
            - name: HQL_SECRET_RAFT
              valueFrom:
                secretKeyRef:
                  name: velorix-hiqlite-auth
                  key: raft-secret
            - name: ENC_KEY_ACTIVE
              valueFrom:
                secretKeyRef:
                  name: velorix-hiqlite-auth
                  key: enc-key-active
            - name: ENC_KEYS
              valueFrom:
                secretKeyRef:
                  name: velorix-hiqlite-auth
                  key: enc-keys
            - name: HQL_S3_URL
              value: "${s3_endpoint}"
            - name: HQL_S3_BUCKET
              value: "${bucket}"
            - name: HQL_S3_REGION
              value: "${aws_region}"
            - name: HQL_S3_PATH_STYLE
              value: "${s3_force_path_style_bool}"
            - name: HQL_S3_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: HQL_S3_SECRET
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
          ports:
            - name: raft
              containerPort: 8100
            - name: api
              containerPort: 8200
          readinessProbe:
            httpGet:
              path: /health
              port: 8200
            periodSeconds: 2
            failureThreshold: 60
          livenessProbe:
            httpGet:
              path: /health
              port: 8200
            periodSeconds: 10
            failureThreshold: 12
          volumeMounts:
            - name: config
              mountPath: /etc/hiqlite
              readOnly: true
            - name: data
              mountPath: /data
      volumes:
        - name: config
          configMap:
            name: velorix-hiqlite-config
$(hiqlite_data_volume_yaml)
$(hiqlite_data_volume_claim_templates_yaml)
EOF

  kubectl --context "$context" apply -f "${output_dir}/velorix-hiqlite.yaml"
  remove_service_run_id_selector velorix-hiqlite-headless
  remove_service_run_id_selector velorix-hiqlite
  wait_for_statefulset_rollout velorix-hiqlite
  if [ -z "$hiqlite_image_digest" ]; then
    hiqlite_image_digest="$(docker image inspect "$hiqlite_image" --format '{{.Id}}' 2>/dev/null || true)"
  fi
  if [ -z "$hiqlite_image_digest" ]; then
    echo "managed Hiqlite authority attestation requires VELORIX_HIQLITE_IMAGE_DIGEST or a locally inspectable image" >&2
    exit 64
  fi
  if [ "$managed_persistence" = "1" ]; then
    echo "managed PVC persistence is storage-local only; no backup durability attestation is generated" >&2
    return 0
  fi
  hiqlite_source_revision="local-source-no-git"
  if git -C "$hiqlite_local_source_dir" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    hiqlite_source_head="$(git -C "$hiqlite_local_source_dir" rev-parse --short HEAD)"
    hiqlite_source_remote="$(git -C "$hiqlite_local_source_dir" config --get remote.origin.url || true)"
    if [ -z "$hiqlite_source_remote" ]; then
      hiqlite_source_remote="local"
    fi
    hiqlite_source_dirty=""
    if [ -n "$(git -C "$hiqlite_local_source_dir" status --porcelain)" ]; then
      hiqlite_source_dirty="+dirty"
    fi
    hiqlite_source_revision="${hiqlite_source_remote}@${hiqlite_source_head}${hiqlite_source_dirty}"
  fi
  python3 - \
    "$generated_hiqlite_authority_attestation" \
    "$hiqlite_nodes" \
    "$hiqlite_image_digest" \
    "$hiqlite_raft_secret_hash" \
    "$hiqlite_source_revision" <<'PY'
import json
import sys
from datetime import datetime, timezone

path, nodes_csv, image_digest, raft_secret_hash, source_revision = sys.argv[1:]
nodes = [node for node in nodes_csv.split(",") if node]
attestation = {
    "schema_version": 1,
    "authority_kind": "velorix_managed_hiqlite",
    "nodes": nodes,
    "expected_voter_count": 3,
    "no_pvc_created_by_vind": True,
    "metadata_authority_no_pvc_used": True,
    "metadata_authority_storage_mode": "object-store-backup-restore-with-ephemeral-node-disk",
    "voters_learner_only_disabled": True,
    "api_auth_configured": True,
    "raft_auth_configured": True,
    "transport_security": "cluster-internal-authenticated-plaintext",
    "backup_restore_configured": True,
    "image_digest": image_digest,
    "source_revision": source_revision,
    "raft_secret_sha256": raft_secret_hash,
    "no_pvc_evidence_files": {
        "namespace_pvc_list": "no-pvc-namespace.json",
        "hiqlite_statefulset": "no-pvc-hiqlite-statefulset.json",
        "manifest": "velorix-hiqlite.yaml",
    },
    "attested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "attester": "scripts/run-vind-product.sh",
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(attestation, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  hiqlite_authority_attestation_file="$generated_hiqlite_authority_attestation"
  validate_hiqlite_authority_attestation
}

if [ "$object_store_mode" = "rustfs" ]; then
cat >"${output_dir}/rustfs.yaml" <<EOF
$(rustfs_pvc_yaml)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs
  namespace: ${namespace}
spec:
  replicas: 1
$(rustfs_strategy_yaml)
  selector:
    matchLabels:
      app: rustfs
  template:
    metadata:
      labels:
        app: rustfs
        velorix.dev/run-id: "${run_id}"
      annotations:
        velorix.dev/run-id: "${run_id}"
        velorix.dev/image-tag: "${rustfs_image}"
        velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
    spec:
      containers:
        - name: rustfs
          image: ${rustfs_image}
          imagePullPolicy: IfNotPresent
          args: ["/data"]
          env:
            - name: RUSTFS_ADDRESS
              value: ":9000"
            - name: RUSTFS_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: RUSTFS_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
          ports:
            - containerPort: 9000
          readinessProbe:
            tcpSocket:
              port: 9000
            periodSeconds: 2
            failureThreshold: 30
          livenessProbe:
            tcpSocket:
              port: 9000
            periodSeconds: 10
            failureThreshold: 6
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
$(rustfs_data_volume_yaml)
---
apiVersion: v1
kind: Service
metadata:
  name: rustfs
  namespace: ${namespace}
spec:
  selector:
    app: rustfs
    velorix.dev/run-id: "${run_id}"
  ports:
    - name: s3
      port: 9000
      targetPort: 9000
EOF

kubectl --context "$context" apply -f "${output_dir}/rustfs.yaml"
wait_for_rollout rustfs

cat >"${output_dir}/bucket-job.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: rustfs-create-bucket
  namespace: ${namespace}
spec:
  backoffLimit: 6
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: aws
          image: amazon/aws-cli:2.17.36
          imagePullPolicy: IfNotPresent
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: AWS_DEFAULT_REGION
              value: "${aws_region}"
          command: ["/bin/sh", "-c"]
          args:
            - >
              aws --endpoint-url ${s3_endpoint} s3api head-bucket --bucket ${bucket}
              || aws --endpoint-url ${s3_endpoint} s3api create-bucket --bucket ${bucket}
EOF

kubectl --context "$context" -n "$namespace" delete job rustfs-create-bucket --ignore-not-found
kubectl --context "$context" apply -f "${output_dir}/bucket-job.yaml"
wait_for_job_complete rustfs-create-bucket
else
  echo "using external S3-compatible object store endpoint ${s3_endpoint}; skipping RustFS deployment and bucket creation"
fi
run_external_s3_validation_job
if [ "$hiqlite_deploy" = "1" ]; then
  deploy_hiqlite_authority
fi

api_meta_env=""
if [ "$meta_enabled" = "1" ]; then
  meta_bearer_token_b64="$(base64_value "$meta_bearer_token")"
  cat <<EOF | kubectl --context "$context" apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: velorix-meta-auth
  namespace: ${namespace}
type: Opaque
data:
  bearer-token: ${meta_bearer_token_b64}
EOF

  cat >"${output_dir}/velorix-meta.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velorix-meta
  namespace: ${namespace}
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: velorix-meta
  template:
    metadata:
      labels:
        app: velorix-meta
        velorix.dev/run-id: "${run_id}"
      annotations:
        velorix.dev/run-id: "${run_id}"
        velorix.dev/image-tag: "${meta_image}"
        velorix.dev/image-digest: "${meta_image_digest:-unknown}"
        velorix.dev/meta-token-sha256: "${meta_bearer_token_hash}"
        velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
        velorix.dev/hiqlite-secret-sha256: "${hiqlite_api_secret_hash}"
    spec:
$(image_pull_secrets_yaml)
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: meta
          image: ${meta_image}
          imagePullPolicy: ${meta_image_pull_policy}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_META_MODE
              value: "${meta_mode}"
            - name: VELORIX_META_BIND
              value: "0.0.0.0:9090"
            - name: VELORIX_META_BACKEND
              value: "${meta_backend}"
            - name: VELORIX_META_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: velorix-meta-auth
                  key: bearer-token
            - name: VELORIX_S3_COMPAT
              value: "1"
            - name: VELORIX_S3_FORCE_PATH_STYLE
              value: "${s3_force_path_style}"
            - name: AWS_ENDPOINT_URL
              value: "${s3_endpoint}"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: VELORIX_S3_BUCKET
              value: "${bucket}"
            - name: VELORIX_S3_PREFIX
              value: "${meta_s3_prefix}"
            - name: VELORIX_HIQLITE_NODES
              value: "${hiqlite_nodes}"
            - name: VELORIX_HIQLITE_API_SECRET
              valueFrom:
                secretKeyRef:
                  name: ${hiqlite_api_secret_ref_name}
                  key: api-secret
                  optional: true
            - name: VELORIX_HIQLITE_WITH_PROXY
              value: "${hiqlite_with_proxy}"
          ports:
            - containerPort: 9090
          readinessProbe:
            tcpSocket:
              port: 9090
            periodSeconds: 2
            failureThreshold: 120
          startupProbe:
            tcpSocket:
              port: 9090
            periodSeconds: 2
            failureThreshold: 150
          livenessProbe:
            tcpSocket:
              port: 9090
            periodSeconds: 10
            initialDelaySeconds: 30
            failureThreshold: 6
---
apiVersion: v1
kind: Service
metadata:
  name: velorix-meta
  namespace: ${namespace}
spec:
  selector:
    app: velorix-meta
  ports:
    - name: grpc
      port: 9090
      targetPort: 9090
EOF

  kubectl --context "$context" apply -f "${output_dir}/velorix-meta.yaml"
  remove_service_run_id_selector velorix-meta
  wait_for_rollout velorix-meta
  meta_deployment_observed_file="${output_dir}/velorix-meta-deployment-observed.json"
  kubectl --context "$context" -n "$namespace" get deployment velorix-meta -o json >"$meta_deployment_observed_file"
  meta_pods_file="${output_dir}/velorix-meta-pods.json"
  kubectl --context "$context" -n "$namespace" get pods \
    -l "app=velorix-meta,velorix.dev/run-id=${run_id}" -o json >"$meta_pods_file"
  meta_image_digest="$(
    sync_deployed_image_digest_annotation \
      velorix-meta \
      velorix-meta \
      meta \
      "$meta_image_digest" \
      "$meta_deployment_observed_file" \
      "$meta_pods_file"
  )"
  run_meta_smoke_job

  api_meta_env="$(cat <<'EOF'
            - name: VELORIX_META_GRPC_ENDPOINT
              value: "http://velorix-meta:9090"
            - name: VELORIX_META_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: velorix-meta-auth
                  key: bearer-token
EOF
  )"
fi

generate_api_tls_secret
api_tls_env=""
api_tls_ports=""
api_tls_volume_mounts=""
api_tls_volumes=""
api_tls_service_port=""
if [ "$api_tls_enabled" = "1" ]; then
  api_tls_env="$(cat <<'EOF'
            - name: VELORIX_API_TLS_BIND
              value: "0.0.0.0:8443"
            - name: VELORIX_API_TLS_CERT_PATH
              value: "/var/run/velorix-api-tls/tls.crt"
            - name: VELORIX_API_TLS_KEY_PATH
              value: "/var/run/velorix-api-tls/tls.key"
EOF
)"
  api_tls_ports="$(cat <<'EOF'
            - containerPort: 8443
EOF
)"
  api_tls_volume_mounts="$(cat <<'EOF'
          volumeMounts:
            - name: api-tls
              mountPath: /var/run/velorix-api-tls
              readOnly: true
EOF
)"
  api_tls_volumes="$(cat <<'EOF'
      volumes:
        - name: api-tls
          secret:
            secretName: velorix-api-tls
EOF
)"
  api_tls_service_port="$(cat <<'EOF'
    - name: https
      port: 8443
      targetPort: 8443
EOF
)"
fi

cat >"${output_dir}/velorix-api.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velorix-api
  namespace: ${namespace}
spec:
  replicas: ${api_replica_count}
  selector:
    matchLabels:
      app: velorix-api
  template:
    metadata:
      labels:
        app: velorix-api
        velorix.dev/run-id: "${run_id}"
      annotations:
        velorix.dev/run-id: "${run_id}"
        velorix.dev/image-tag: "${api_image}"
        velorix.dev/image-digest: "${api_image_digest:-unknown}"
        velorix.dev/api-auth-mode: "${api_auth_mode}"
        velorix.dev/api-auth-source: "${api_bearer_token_source}"
        velorix.dev/api-auth-rollout-id: "${run_id}"
        velorix.dev/api-tls-cert-sha256: "${api_tls_certificate_sha256:-disabled}"
        velorix.dev/meta-token-sha256: "${meta_bearer_token_hash}"
        velorix.dev/s3-credentials-sha256: "${s3_credentials_hash}"
    spec:
$(image_pull_secrets_yaml)
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: api
          image: ${api_image}
          imagePullPolicy: ${api_image_pull_policy}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
            capabilities:
              drop:
                - ALL
          env:
            - name: VELORIX_S3_COMPAT
              value: "1"
            - name: VELORIX_S3_FORCE_PATH_STYLE
              value: "${s3_force_path_style}"
            - name: AWS_ENDPOINT_URL
              value: "${s3_endpoint}"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: secret-access-key
            - name: AWS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${s3_credentials_secret_name}
                  key: session-token
                  optional: true
            - name: AWS_REGION
              value: "${aws_region}"
            - name: VELORIX_S3_BUCKET
              value: "${bucket}"
            - name: VELORIX_S3_PREFIX
              value: "${s3_prefix}"
            - name: VELORIX_AUTHORITY_STORE_ID
              value: "${s3_authority_store_id}"
            - name: VELORIX_AUTHORITY_NAMESPACE
              value: "velorix"
            - name: VELORIX_OPERATOR_ID
              value: "velorix-api"
            - name: VELORIX_STATE_PATH
              value: "v1/state/slatedb"
            - name: VELORIX_API_BIND
              value: "0.0.0.0:8080"
${api_tls_env}
${api_auth_env}
            - name: VELORIX_API_REPLICA_COUNT
              value: "${api_replica_count}"
            - name: VELORIX_STANDING_RUNTIME_FENCING
              value: "${standing_runtime_fencing}"
            - name: VELORIX_STANDING_RUNTIME_OWNER_TTL_MS
              value: "${standing_runtime_owner_ttl_ms}"
            - name: VELORIX_OUTPUT_COMPACTION_INTERVAL_EPOCHS
              value: "${output_compaction_interval_epochs}"
${api_meta_env}
          ports:
            - containerPort: 8080
${api_tls_ports}
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
            periodSeconds: 2
            failureThreshold: 30
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
            periodSeconds: 10
            failureThreshold: 6
${api_tls_volume_mounts}
${api_tls_volumes}
---
apiVersion: v1
kind: Service
metadata:
  name: velorix-api
  namespace: ${namespace}
spec:
  selector:
    app: velorix-api
  ports:
    - name: http
      port: 8080
      targetPort: 8080
${api_tls_service_port}
EOF

kubectl --context "$context" apply -f "${output_dir}/velorix-api.yaml"
remove_service_run_id_selector velorix-api
wait_for_rollout velorix-api
api_deployment_observed_file="${output_dir}/velorix-api-deployment-observed.json"
kubectl --context "$context" -n "$namespace" get deployment velorix-api -o json >"$api_deployment_observed_file"
api_pods_file="${output_dir}/velorix-api-pods.json"
kubectl --context "$context" -n "$namespace" get pods \
  -l "app=velorix-api,velorix.dev/run-id=${run_id}" -o json >"$api_pods_file"
api_image_digest="$(
  sync_deployed_image_digest_annotation \
    velorix-api \
    velorix-api \
    api \
    "$api_image_digest" \
    "$api_deployment_observed_file" \
    "$api_pods_file"
)"
verify_api_auth_deployment
if [ "$product_ingress_apply" = "1" ]; then
  VELORIX_VIND_PRODUCT_DIR="$output_dir" \
    VELORIX_K8S_CONTEXT="$context" \
    VELORIX_K8S_NAMESPACE="$namespace" \
    scripts/apply-vind-product-ingress.sh
fi

stop_existing_api_port_forward_pid_file

start_api_port_forward

curl -fsS "http://127.0.0.1:${api_local_port}/healthz" | tee "${output_dir}/healthz.json" >/dev/null
api_healthz_unauthenticated=1
curl -fsS "http://127.0.0.1:${api_local_port}/readyz" | tee "${output_dir}/readyz.json" >/dev/null
api_readyz_unauthenticated=1
IFS=$'\t' read -r api_auth_observed_readyz_mode object_store_namespace_count object_store_artifact_catalog_conditional_update < <(python3 - "${output_dir}/readyz.json" "$meta_enabled" "$meta_backend" "$standing_runtime_fencing" "$api_auth_mode" <<'PY'
import json
import sys

path, meta_enabled, meta_backend, fencing, api_auth_mode = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    readyz = json.load(f)
if readyz.get("status") != "ready":
    raise SystemExit(f"readyz status is not ready: {readyz}")
api_auth = readyz.get("api_auth") or {}
if api_auth.get("mode") != api_auth_mode:
    raise SystemExit(f"api auth mode mismatch: expected {api_auth_mode}, got {api_auth}")
admin_auth = readyz.get("admin_auth") or {}
if admin_auth.get("mode") != api_auth_mode:
    raise SystemExit(f"admin auth mode mismatch: expected {api_auth_mode}, got {admin_auth}")
if readyz.get("standing_runtime_fencing_mode") != fencing:
    raise SystemExit(
        f"standing runtime fencing mode mismatch: expected {fencing}, got {readyz}"
    )
object_store = readyz.get("object_store") or {}
if object_store.get("schema_version") != 1:
    raise SystemExit(f"object_store readyz schema missing or unsupported: {readyz}")
artifact_catalog = object_store.get("artifact_catalog") or {}
if artifact_catalog.get("conditional_update") is not True:
    raise SystemExit(f"artifact catalog does not prove conditional_update for active view CAS: {readyz}")
namespace_count = object_store.get("authoritative_namespace_count")
if not isinstance(namespace_count, int) or namespace_count <= 0:
    raise SystemExit(f"object_store authoritative_namespace_count is invalid: {readyz}")
if meta_enabled == "1":
    metadata_store = readyz.get("metadata_store") or {}
    if metadata_store.get("configured") is not True:
        raise SystemExit(f"metadata_store is not configured in readyz: {readyz}")
    if metadata_store.get("endpoint") != "http://velorix-meta:9090":
        raise SystemExit(f"metadata endpoint mismatch: {readyz}")
    capability = metadata_store.get("standing_runtime_fencing") or {}
    expected_backend = {
        "memory": "in-memory",
        "in-memory": "in-memory",
        "oss": "oss",
        "object-store": "oss",
        "hiqlite": "hiqlite",
    }[meta_backend]
    if capability.get("backend_name") != expected_backend:
        raise SystemExit(f"metadata backend mismatch: expected {expected_backend}, got {capability}")
    if capability.get("capability_schema_version") != 2:
        raise SystemExit(f"unsupported metadata capability schema: {capability}")
    if capability.get("control_plane_auth_enforced") is not True:
        raise SystemExit(f"metadata auth is not enforced: {capability}")
    if fencing != "unsafe-dev-only":
        if readyz.get("standing_runtime_fencing_required") is not True:
            raise SystemExit(f"required fencing mode was not reported as required: {readyz}")
        logical_required_true = [
            "linearizable_owner_lease",
            "durable_monotonic_owner_epoch",
            "owner_validated_checkpoint_publish",
            "publish_checks_owner_and_latest_atomically",
            "publish_rejects_expired_owner",
            "latest_read_linearizable",
            "publish_rejects_scope_mismatch",
            "multi_writer_fencing_safe",
        ]
        missing = [name for name in logical_required_true if capability.get(name) is not True]
        if missing:
            raise SystemExit(
                f"metadata backend does not satisfy logical fencing fields {missing}: {capability}"
            )
        if capability.get("lease_authority_kind") not in {
            "hiqlite_raft_serialized",
            "raft_replicated_time",
        }:
            raise SystemExit(
                f"metadata backend does not report a recognized lease authority: {capability}"
            )
        if capability.get("lease_expiry_semantics") not in {
            "operation_driven_logical",
            "backend_wall_clock_ttl",
        }:
            raise SystemExit(
                f"metadata backend does not report recognized lease expiry semantics: {capability}"
            )
        if fencing == "required":
            required_true = [
                "authoritative_backend_time",
                "multi_writer_fencing_safe",
                "bounded_wall_clock_failover",
                "production_bounded_failover_safe",
                "production_multi_writer_safe",
            ]
            missing = [name for name in required_true if capability.get(name) is not True]
            if missing:
                raise SystemExit(
                    f"metadata backend does not satisfy production fencing fields {missing}: {capability}"
                )
            if capability.get("failover_time_bound_ms", 0) <= 0:
                raise SystemExit(
                    f"metadata backend does not report a bounded failover time: {capability}"
                )
            if capability.get("backend_time_source_kind") != "raft_replicated_authority_time":
                raise SystemExit(
                    f"metadata backend does not report a raft-replicated authority time source: {capability}"
                )
else:
    if (readyz.get("metadata_store") or {}).get("configured") is not False:
        raise SystemExit(f"metadata_store should not be configured: {readyz}")
if fencing == "unsafe-dev-only" and readyz.get("standing_runtime_fencing_required") is not False:
    raise SystemExit(f"unsafe-dev-only should not require production fencing: {readyz}")
print(f"{api_auth.get('mode', '')}\t{namespace_count}\t1")
PY
)

if [ "$api_auth_mode" = "bearer-token" ]; then
  check_api_auth_rejection "${output_dir}/auth-missing-response.json" "missing bearer token" \
    -X POST "http://127.0.0.1:${api_local_port}/v1/relations/scores-default"
  api_auth_missing_token_rejected=1
  check_api_auth_rejection "${output_dir}/auth-wrong-response.json" "wrong bearer token" \
    -X POST "http://127.0.0.1:${api_local_port}/v1/relations/scores-default" \
    -H "authorization: Bearer definitely-wrong-token"
  api_auth_wrong_token_rejected=1
  check_api_auth_rejection "${output_dir}/admin-auth-data-token-response.json" "data-plane token on admin route" \
    "http://127.0.0.1:${api_local_port}/v1/standing-runtime/owners" \
    -H "authorization: Bearer ${api_bearer_token}"
  api_auth_data_plane_token_rejected_on_admin_route=1
  fi
  start_api_tls_port_forward
  run_api_tls_auth_smoke

  if [ "$product_smoke" = "1" ]; then
  curl_api -X POST "http://127.0.0.1:${api_local_port}/v1/relations/scores-default" \
    | tee "${output_dir}/scores-relation.json" >/dev/null
  interactive_policy_status="$(curl_api_status "${output_dir}/query-policy-interactive-create.json" \
    -X POST "http://127.0.0.1:${api_local_port}/v1/query-policies" \
    -H 'content-type: application/json' \
    -d '{"query_policy_id":"interactive","policy":{"max_sql_bytes":4096,"planning_timeout_ms":1000,"execution_timeout_ms":5000,"max_output_rows":1000,"max_output_bytes":1048576,"max_scan_files":100,"max_scan_bytes":134217728,"max_object_requests":100,"max_concurrent_queries":4,"memory_limit_bytes":536870912,"spill_limit_bytes":1073741824}}')"
  case "$interactive_policy_status" in
    200 | 201)
      cp "${output_dir}/query-policy-interactive-create.json" "${output_dir}/query-policy-interactive.json"
      ;;
    409)
      curl_api "http://127.0.0.1:${api_local_port}/v1/query-policies/interactive" \
        | tee "${output_dir}/query-policy-interactive.json" >/dev/null
      ;;
    *)
      echo "expected interactive query policy create to return 200, 201, or duplicate 409; got ${interactive_policy_status}" >&2
      cat "${output_dir}/query-policy-interactive-create.json" >&2 || true
      exit 1
      ;;
  esac
  curl_api "http://127.0.0.1:${api_local_port}/v1/query-policies/interactive" \
    | tee "${output_dir}/query-policy-interactive-read.json" >/dev/null
  weak_policy_status="$(curl_api_status "${output_dir}/query-policy-weak-rejection.json" \
    -X POST "http://127.0.0.1:${api_local_port}/v1/query-policies" \
    -H 'content-type: application/json' \
    -d '{"query_policy_id":"weak","policy":{"max_output_rows":1,"max_concurrent_queries":1}}')"
  if [ "$weak_policy_status" != "400" ]; then
    echo "expected weak query policy to fail creation with 400, got ${weak_policy_status}" >&2
    cat "${output_dir}/query-policy-weak-rejection.json" >&2 || true
    exit 1
  fi
  missing_policy_status="$(curl_api_status "${output_dir}/query-policy-missing-view.json" \
    -X POST "http://127.0.0.1:${api_local_port}/v1/views" \
    -H 'content-type: application/json' \
    -d '{"view_id":"missing_policy_scores_by_user","urlPath":"/missing-policy/scores/by-user","input_relation_id":"scores","input_relation_version":"2026-05-24.v1","sql":"select user_id, sum(score) as sum, count(*) as count from scores group by user_id","query_policy_id":"missing_policy"}')"
  if [ "$missing_policy_status" != "400" ]; then
    echo "expected missing query policy to fail view creation with 400, got ${missing_policy_status}" >&2
    cat "${output_dir}/query-policy-missing-view.json" >&2 || true
    exit 1
  fi
  python3 - "${output_dir}/query-policy-interactive.json" "${output_dir}/query-policy-interactive-read.json" "${output_dir}/query-policy-weak-rejection.json" "${output_dir}/query-policy-missing-view.json" <<'PY'
import json
import sys

for path in sys.argv[1:3]:
    with open(path, "r", encoding="utf-8") as f:
        body = json.load(f)
    if body.get("tenant_id") != "default":
        raise SystemExit(f"query policy tenant mismatch in {path}: {body}")
    if body.get("query_policy_id") != "interactive":
        raise SystemExit(f"query policy id mismatch in {path}: {body}")
    policy = body.get("policy") or {}
    if policy.get("max_output_rows") != 1000:
        raise SystemExit(f"query policy max_output_rows mismatch in {path}: {body}")
    if policy.get("max_concurrent_queries") != 4:
        raise SystemExit(f"query policy max_concurrent_queries mismatch in {path}: {body}")
    for field in (
        "max_sql_bytes",
        "planning_timeout_ms",
        "execution_timeout_ms",
        "max_output_bytes",
        "max_scan_files",
        "max_scan_bytes",
        "max_object_requests",
        "memory_limit_bytes",
        "spill_limit_bytes",
    ):
        if policy.get(field) is None:
            raise SystemExit(f"query policy missing production field {field} in {path}: {body}")
with open(sys.argv[3], "r", encoding="utf-8") as f:
    weak = json.load(f)
if "production table scans require query policy field max_sql_bytes" not in weak.get("error", ""):
    raise SystemExit(f"weak query policy rejection did not mention production bounds: {weak}")
with open(sys.argv[4], "r", encoding="utf-8") as f:
    missing = json.load(f)
if "query policy" not in missing.get("error", "").lower():
    raise SystemExit(f"missing query policy rejection did not mention query policy: {missing}")
PY
  api_query_policy_weak_policy_rejected=1
  api_query_policy_missing_policy_rejected=1
  positive_scores_view_status="$(curl_api_status "${output_dir}/positive-scores-view-create.json" \
    -X POST "http://127.0.0.1:${api_local_port}/v1/views" \
    -H 'content-type: application/json' \
    -d '{"view_id":"positive_scores_by_user","urlPath":"/scores/positive","input_relation_id":"scores","input_relation_version":"2026-05-24.v1","sql":"select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id","response_formats":["json"],"query_policy_id":"interactive"}')"
  case "$positive_scores_view_status" in
    200 | 201)
      cp "${output_dir}/positive-scores-view-create.json" "${output_dir}/positive-scores-view.json"
      ;;
    409)
      curl_api "http://127.0.0.1:${api_local_port}/v1/views/positive_scores_by_user" \
        | tee "${output_dir}/positive-scores-view.json" >/dev/null
      ;;
    *)
      echo "expected positive scores view create to return 200, 201, or duplicate 409; got ${positive_scores_view_status}" >&2
      cat "${output_dir}/positive-scores-view-create.json" >&2 || true
      exit 1
      ;;
  esac
  start_api_writer_owner_port_forward_for_smoke
  curl_api -X POST "http://127.0.0.1:${api_local_port}/v1/relations/scores/ingest" \
    -H 'content-type: application/json' \
    -d '{"relation_version":"2026-05-24.v1","stream_id":"product-smoke-'"${run_id}"'","partition_id":0,"start_offset_inclusive":0,"rows":[{"user_id":"u1","score":5,"delta":1},{"user_id":"u1","score":7,"delta":1},{"user_id":"u2","score":-1,"delta":1},{"user_id":"u3","score":0,"delta":1}]}' \
    | tee "${output_dir}/scores-ingest.json" >/dev/null
  curl_api "http://127.0.0.1:${api_local_port}/v1/views/positive_scores_by_user/query" \
    | tee "${output_dir}/positive-scores-query.json" >/dev/null
  curl_api "http://127.0.0.1:${api_local_port}/v1/api/scores/positive" \
    | tee "${output_dir}/positive-scores-api.json" >/dev/null
  curl_api "http://127.0.0.1:${api_local_port}/v1/openapi.json" \
    | tee "${output_dir}/openapi.json" >/dev/null
  python3 - "${output_dir}/positive-scores-query.json" "${output_dir}/positive-scores-api.json" "${output_dir}/openapi.json" "${output_dir}/positive-scores-view.json" <<'PY'
import json
import sys

for path in sys.argv[1:3]:
    with open(path, "r", encoding="utf-8") as f:
        body = json.load(f)
    rows = {row.get("user_id"): row for row in body.get("rows") or []}
    u1 = rows.get("u1") or {}
    if u1.get("sum", 0) < 12 or u1.get("count", 0) < 2:
        raise SystemExit(f"unexpected query rows in {path}: {body}")
with open(sys.argv[3], "r", encoding="utf-8") as f:
    openapi = json.load(f)
paths = openapi.get("paths") or {}
if "/v1/query" in paths:
    raise SystemExit("generic /v1/query unexpectedly appears in OpenAPI")
if "/v1/api/scores/positive" not in paths:
    raise SystemExit(f"default promoted scores API path is missing from OpenAPI: {paths.keys()}")
if "/v1/api/scores/positive/{user_id}" in paths:
    raise SystemExit("default promoted scores API unexpectedly requires a user_id path parameter")
positive_get = paths["/v1/api/scores/positive"].get("get") or {}
if positive_get.get("x-velorix-query-policy-id") != "interactive":
    raise SystemExit(f"default promoted scores API is not linked to query policy in OpenAPI: {positive_get}")
with open(sys.argv[4], "r", encoding="utf-8") as f:
    view = json.load(f)
if view.get("query_policy_id") != "interactive":
    raise SystemExit(f"positive scores view is not linked to query policy: {view}")
PY
  api_openapi_catalog_smoke_passed=1
  api_query_policy_smoke_passed=1
  if [ "$ingest_writer_smoke" = "1" ]; then
    run_ingest_writer_smoke_job
    generate_ingest_writer_lifecycle_attestation
  fi
  run_multi_replica_fencing_smoke
  kubectl --context "$context" -n "$namespace" rollout restart deployment/velorix-api >/dev/null
  wait_for_rollout velorix-api
  start_api_port_forward
  curl_api "http://127.0.0.1:${api_local_port}/v1/views/positive_scores_by_user/query" \
    | tee "${output_dir}/positive-scores-query-after-api-restart.json" >/dev/null
  python3 - "${output_dir}/positive-scores-query-after-api-restart.json" "$ingest_writer_smoke" "$ingest_writer_lifecycle_attestation_validated" "$multi_replica_fencing_smoke_passed" "$run_id" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
expected = [{"user_id": "u1", "sum": 12, "count": 2}]
if sys.argv[2] == "1":
    expected.append({"user_id": "u3", "sum": 11, "count": 1})
if sys.argv[3] == "1":
    expected.extend(
        [
            {"user_id": "lifecycle-adjacent", "sum": 2, "count": 1},
            {"user_id": "lifecycle-restart", "sum": 3, "count": 1},
            {"user_id": "lifecycle-handoff", "sum": 4, "count": 1},
        ]
    )
if sys.argv[4] == "1":
    expected.append({"user_id": f"multi-replica-{sys.argv[5]}", "sum": 14, "count": 2})
actual = {row.get("user_id"): row for row in body.get("rows") or []}
for row in expected:
    got = actual.get(row["user_id"]) or {}
    if got.get("sum", 0) < row["sum"] or got.get("count", 0) < row["count"]:
        raise SystemExit(f"missing expected post-restart row {row}: {body}")
if (actual.get("u1") or {}).get("sum", 0) < 12 or (actual.get("u1") or {}).get("count", 0) < 2:
    raise SystemExit(f"unexpected post-restart query rows: {body}")
PY
  if [ "$meta_enabled" = "1" ]; then
    case "$meta_backend" in
      oss | object-store | hiqlite)
        kubectl --context "$context" -n "$namespace" rollout restart deployment/velorix-meta >/dev/null
        wait_for_rollout velorix-meta
        run_meta_smoke_job
        kubectl --context "$context" -n "$namespace" rollout restart deployment/velorix-api >/dev/null
        wait_for_rollout velorix-api
        start_api_port_forward
        curl_api "http://127.0.0.1:${api_local_port}/v1/views/positive_scores_by_user/query" \
          | tee "${output_dir}/positive-scores-query-after-meta-api-restart.json" >/dev/null
        python3 - "${output_dir}/positive-scores-query-after-meta-api-restart.json" "$ingest_writer_smoke" "$ingest_writer_lifecycle_attestation_validated" "$multi_replica_fencing_smoke_passed" "$run_id" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    body = json.load(f)
expected = [{"user_id": "u1", "sum": 12, "count": 2}]
if sys.argv[2] == "1":
    expected.append({"user_id": "u3", "sum": 11, "count": 1})
if sys.argv[3] == "1":
    expected.extend(
        [
            {"user_id": "lifecycle-adjacent", "sum": 2, "count": 1},
            {"user_id": "lifecycle-restart", "sum": 3, "count": 1},
            {"user_id": "lifecycle-handoff", "sum": 4, "count": 1},
        ]
    )
if sys.argv[4] == "1":
    expected.append({"user_id": f"multi-replica-{sys.argv[5]}", "sum": 14, "count": 2})
actual = {row.get("user_id"): row for row in body.get("rows") or []}
for row in expected:
    got = actual.get(row["user_id"]) or {}
    if got.get("sum", 0) < row["sum"] or got.get("count", 0) < row["count"]:
        raise SystemExit(f"missing expected post-meta-restart row {row}: {body}")
if (actual.get("u1") or {}).get("sum", 0) < 12 or (actual.get("u1") or {}).get("count", 0) < 2:
    raise SystemExit(f"unexpected post-meta-restart query rows: {body}")
PY
        ;;
    esac
  fi
  api_auth_correct_token_smoke_passed=1
fi

generate_ingress_tls_auth_attestation
validate_no_pvc_namespace
write_product_evidence
run_standing_runtime_failover_smoke
if [ "$standing_runtime_failover_smoke_passed" = "1" ]; then
  write_product_evidence
fi
run_hiqlite_backend_time_assessment
run_hiqlite_backend_time_attestation
write_product_evidence
if [ "$product_evidence_level" = "product-complete" ]; then
  assert_product_complete_evidence
fi
attach_final_rest_to_writer_owner
run_rest_api_smoke
run_product_completion_report

echo "velorix-api is running at http://127.0.0.1:${api_local_port}"
echo "cluster=${cluster}"
echo "product_deployment_id=${product_deployment_id}"
echo "namespace=${namespace}"
echo "context=${context}"
echo "product_evidence_level=${product_evidence_level}"
echo "product_evidence=${output_dir}/product-evidence.json"
echo "meta_enabled=${meta_enabled}"
if [ "$meta_enabled" = "1" ]; then
  echo "meta_backend=${meta_backend}"
  echo "meta_service=velorix-meta.${namespace}.svc:9090"
  if [ "$meta_backend" = "hiqlite" ]; then
    echo "hiqlite_authority_attestation_validated=${hiqlite_authority_attestation_validated}"
  fi
fi
echo "product_smoke=${product_smoke}"
echo "rest_api_smoke=${rest_api_smoke}"
echo "rest_api_smoke_status=${rest_api_smoke_status}"
if [ -n "$rest_api_smoke_evidence_file" ]; then
  echo "rest_api_smoke_evidence=${rest_api_smoke_evidence_file}"
fi
echo "product_completion_report=${product_completion_report}"
echo "product_completion_report_status=${product_completion_report_status}"
if [ -n "$product_completion_report_file" ]; then
  echo "product_completion_report_file=${product_completion_report_file}"
fi
echo "ingest_writer_smoke=${ingest_writer_smoke}"
echo "ingest_writer_lifecycle_attestation_validated=${ingest_writer_lifecycle_attestation_validated}"
echo "multi_replica_fencing_smoke=${multi_replica_fencing_smoke}"
echo "multi_replica_fencing_smoke_passed=${multi_replica_fencing_smoke_passed}"
echo "standing_runtime_failover_smoke=${standing_runtime_failover_smoke}"
echo "standing_runtime_failover_smoke_passed=${standing_runtime_failover_smoke_passed}"
echo "hiqlite_backend_time_assess=${hiqlite_backend_time_assess}"
echo "hiqlite_backend_time_assessment_validated=${hiqlite_backend_time_assessment_validated}"
echo "hiqlite_backend_time_attest=${hiqlite_backend_time_attest}"
echo "hiqlite_backend_time_attestation_validated=${hiqlite_backend_time_attestation_validated}"
if [ "$ingest_writer_smoke" = "1" ]; then
  echo "ingest_writer_image=${ingest_writer_image}"
  echo "ingest_writer_job_completed=${ingest_writer_job_completed}"
  if [ -n "$ingest_writer_object_key" ]; then
    echo "ingest_writer_object_key=${ingest_writer_object_key}"
  fi
fi
echo "preserve_state=${preserve_state}"
echo "object_store_mode=${object_store_mode}"
echo "object_store_endpoint=${s3_endpoint}"
echo "authority_store_id=${s3_authority_store_id}"
if [ "$object_store_mode" = "external-s3" ]; then
  echo "external_s3_validate=${external_s3_validate}"
  echo "external_s3_bucket_validated=${external_s3_bucket_validated}"
fi
echo "s3_prefix=${s3_prefix}"
echo "s3_credentials_source=${s3_credentials_source}"
if [ "$meta_enabled" = "1" ]; then
  echo "meta_s3_prefix=${meta_s3_prefix}"
fi
echo "api_replica_count=${api_replica_count}"
echo "standing_runtime_fencing=${standing_runtime_fencing}"
echo "api_auth_mode=${api_auth_mode}"
echo "api_auth_token_source=${api_bearer_token_source}"
echo "admin_auth_token_source=${admin_bearer_token_source}"
echo "ingress_tls_auth_attestation_validated=${ingress_tls_auth_attestation_validated}"
echo "no_pvc_namespace_validated=${no_pvc_namespace_validated}"
if [ "$api_auth_mode" = "bearer-token" ]; then
  echo "api_auth_env=${output_dir}/api-auth.env"
fi
echo "final_owner_aware_attach=${final_owner_aware_attach}"
if [ -n "$api_final_rest_attach_evidence_file" ]; then
  echo "rest_attach_evidence=${api_final_rest_attach_evidence_file}"
fi
echo "port_forward_pid=${port_forward_pid}"
echo
echo "Try:"
if [ "$api_auth_mode" = "bearer-token" ]; then
  echo "  source ${output_dir}/api-auth.env"
  # shellcheck disable=SC2016
  echo '  curl "$VELORIX_API_URL/healthz"'
  # shellcheck disable=SC2016
  echo '  curl -X POST "$VELORIX_API_URL/v1/relations/scores/ingest" -H "$VELORIX_API_AUTH_HEADER" -H '\''content-type: application/json'\'' -d '\''{"relation_version":"2026-05-24.v1","stream_id":"scores","partition_id":0,"start_offset_inclusive":0,"rows":[{"user_id":"u1","score":5,"delta":1},{"user_id":"u1","score":7,"delta":1},{"user_id":"u2","score":-1,"delta":1}]}'\'''
  # shellcheck disable=SC2016
  echo '  curl "$VELORIX_API_URL/v1/views/positive_scores_by_user/query" -H "$VELORIX_API_AUTH_HEADER"'
  # shellcheck disable=SC2016
  echo '  curl "$VELORIX_API_URL/v1/api/scores/positive" -H "$VELORIX_API_AUTH_HEADER"'
  # shellcheck disable=SC2016
  echo '  curl "$VELORIX_API_URL/v1/standing-runtime/owners" -H "$VELORIX_ADMIN_AUTH_HEADER"'
  echo "  VELORIX_VIND_PRODUCT_DIR=${output_dir} scripts/smoke-vind-rest-api.sh"
  echo "  VELORIX_VIND_PRODUCT_DIR=${output_dir} scripts/report-vind-product-completion.sh"
else
  echo "  curl http://127.0.0.1:${api_local_port}/healthz"
  echo "  curl -X POST http://127.0.0.1:${api_local_port}/v1/relations/scores/ingest -H 'content-type: application/json' -d '{\"relation_version\":\"2026-05-24.v1\",\"stream_id\":\"scores\",\"partition_id\":0,\"start_offset_inclusive\":0,\"rows\":[{\"user_id\":\"u1\",\"score\":5,\"delta\":1},{\"user_id\":\"u1\",\"score\":7,\"delta\":1},{\"user_id\":\"u2\",\"score\":-1,\"delta\":1}]}'"
  echo "  curl http://127.0.0.1:${api_local_port}/v1/views/positive_scores_by_user/query"
  echo "  curl http://127.0.0.1:${api_local_port}/v1/api/scores/positive"
fi

if [ "$hold_port_forward" = "1" ]; then
  echo
  echo "Keeping port-forward open. Press Ctrl-C when you are done testing."
  if ! wait "$port_forward_pid" 2>/dev/null; then
    while kill -0 "$port_forward_pid" >/dev/null 2>&1; do
      sleep 1
    done
  fi
fi

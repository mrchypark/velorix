#!/bin/sh
set -eu

# Explicitly opt-in validation harness for the embedded Rhiza KV metadata
# service and the official Rhiza recovery operator.
#
# This is not the product runner. It provisions an isolated namespace, an
# isolated object-store prefix, a fresh cluster identity, and empty local
# working directories, and it assumes no preexisting membership, token set, or
# metadata directory. It never touches a namespace outside the
# velorix-rhiza-validation prefix.
#
# Recovery is performed by the official operator. This script does not restart
# Pods to simulate recovery, does not seal or copy archives, and does not mint
# replacement credentials. Those are the operator's job, and a script that
# reimplements them would prove a different, weaker mechanism under the same
# name.
#
# Required inputs are supplied through VELORIX_* environment variables. Run it
# from the repository root; the fixture wrapper invokes it by relative path.
#
# The operator image defaults to the official published v0.19.0 digest pin and
# nothing relaxes that default. A run on a platform that cannot execute the
# published linux/amd64 artifact may set
# VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE to a locally built image of the same
# pinned upstream source tree, and must then record how it was built in
# VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE. Evidence reports such a run as not
# the published release image.

CDPATH=
export CDPATH
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)

# shellcheck source=scripts/rhiza-kv-k8s-lib.sh
. "$script_dir/rhiza-kv-k8s-lib.sh"

output_dir=${VELORIX_RHIZA_EVIDENCE_DIR:-"$repo_root/target/rhiza-kv-k8s"}
context=${VELORIX_K8S_CONTEXT:-}
namespace=${VELORIX_RHIZA_NAMESPACE:-velorix-rhiza-validation}
meta_image=${VELORIX_RHIZA_META_IMAGE:-}
operator_image_pin=${VELORIX_RHIZA_OPERATOR_IMAGE:-$rhiza_official_operator_image}
operator_image_override=${VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE:-}
operator_image_provenance=${VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE:-}
operator_image=
operator_dir=${VELORIX_RHIZA_OPERATOR_DIR:-"$repo_root/deploy/rhiza-k8s/operator"}
image_pull_secret=${VELORIX_RHIZA_IMAGE_PULL_SECRET:-}
members_json=${VELORIX_RHIZA_MEMBERS_JSON:-}
peer_tokens_json=${VELORIX_RHIZA_PEER_TOKENS:-}
object_store_provider=${VELORIX_RHIZA_OBJECT_STORE_PROVIDER:-}
object_store_endpoint=${VELORIX_RHIZA_OBJECT_STORE_ENDPOINT:-}
object_store_bucket=${VELORIX_RHIZA_OBJECT_STORE_BUCKET:-}
object_store_region=${VELORIX_RHIZA_OBJECT_STORE_REGION:-}
object_store_prefix=${VELORIX_RHIZA_OBJECT_STORE_PREFIX:-rhiza-validation}
object_store_access_key=${VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY:-}
object_store_secret_key=${VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY:-}
object_store_session_token=${VELORIX_RHIZA_OBJECT_STORE_SESSION_TOKEN:-}
object_store_insecure=${VELORIX_RHIZA_OBJECT_STORE_INSECURE:-false}
object_store_durability=${VELORIX_RHIZA_OBJECT_STORE_DURABILITY:-before-ack}
meta_bearer_token=${VELORIX_RHIZA_META_BEARER_TOKEN:-}
rhiza_admin_token=${VELORIX_RHIZA_ADMIN_TOKEN:-}
server_tls_secret=${VELORIX_RHIZA_SERVER_TLS_SECRET:-}
client_tls_secret=${VELORIX_RHIZA_CLIENT_TLS_SECRET:-}
server_tls_cert_file=${VELORIX_RHIZA_SERVER_TLS_CERT_FILE:-}
server_tls_key_file=${VELORIX_RHIZA_SERVER_TLS_KEY_FILE:-}
server_tls_client_ca_file=${VELORIX_RHIZA_SERVER_TLS_CLIENT_CA_FILE:-}
client_tls_cert_file=${VELORIX_RHIZA_CLIENT_TLS_CERT_FILE:-}
client_tls_key_file=${VELORIX_RHIZA_CLIENT_TLS_KEY_FILE:-}
client_tls_ca_file=${VELORIX_RHIZA_CLIENT_TLS_CA_FILE:-}
run_nonce=$(date -u +%Y%m%d-%H%M%S)-$$
probe_id=${VELORIX_RHIZA_PROBE_ID:-rhiza-kv-recovery-${run_nonce}}
recovery_id=${VELORIX_RHIZA_RECOVERY_ID:-rhiza-kv-gen-${run_nonce}}
execute=${VELORIX_RHIZA_EXECUTE:-0}
cleanup=${VELORIX_RHIZA_CLEANUP:-0}
run_id=${VELORIX_RHIZA_RUN_ID:-rhiza-kv-v-${run_nonce}}

service_name=velorix-meta
app_label=rhiza-kv-validation
validation_label="velorix.dev/rhiza-kv-validation=${run_id}"

# The source generation's shared credentials. The public membership document is
# public material: it carries only the derived Ed25519 peer public keys. Each
# node's private peer token is delivered through the standard native
# RHIZA_PEER_TOKENS map, which every Pod receives and from which native selects
# its own entry by RHIZA_NODE_ID. That is the upstream shape; a StatefulSet
# cannot mount a per-replica Secret, so this is not a Velorix relaxation.
config_map_name=rhiza-config
object_store_secret_name=rhiza-object-store
peer_credentials_secret_name=rhiza-peer-credentials
client_credentials_secret_name=rhiza-client-credentials
recovery_resource_name=rhiza-kv-validation-recovery
observation_resource_name=rhiza-kv-validation-observe

preflight_tmp_dir=
secret_tmp_dir=

cleanup_private_files() {
  if [ -n "${secret_tmp_dir:-}" ] && [ -d "${secret_tmp_dir:-}" ]; then
    rm -rf "$secret_tmp_dir"
  fi
  if [ -n "${preflight_tmp_dir:-}" ] && [ -d "${preflight_tmp_dir:-}" ]; then
    rm -rf "$preflight_tmp_dir"
  fi
}
trap 'cleanup_private_files' EXIT HUP INT TERM

die() {
  echo "rhiza KV Kubernetes gate: $*" >&2
  exit 64
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

require_nonempty() {
  [ -n "$2" ] || die "$1 is required"
}

require_cmd kubectl
require_cmd jq
require_cmd openssl
require_cmd sha256sum
mkdir -p "$output_dir"
umask 077

# ---------------------------------------------------------------- preflight --

require_nonempty VELORIX_K8S_CONTEXT "$context"
require_nonempty VELORIX_RHIZA_META_IMAGE "$meta_image"
require_nonempty VELORIX_RHIZA_MEMBERS_JSON "$members_json"
require_nonempty VELORIX_RHIZA_PEER_TOKENS "$peer_tokens_json"
require_nonempty VELORIX_RHIZA_OBJECT_STORE_PROVIDER "$object_store_provider"
require_nonempty VELORIX_RHIZA_OBJECT_STORE_ENDPOINT "$object_store_endpoint"
require_nonempty VELORIX_RHIZA_OBJECT_STORE_BUCKET "$object_store_bucket"
require_nonempty VELORIX_RHIZA_OBJECT_STORE_REGION "$object_store_region"
require_nonempty VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY "$object_store_access_key"
require_nonempty VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY "$object_store_secret_key"
require_nonempty VELORIX_RHIZA_META_BEARER_TOKEN "$meta_bearer_token"
require_nonempty VELORIX_RHIZA_ADMIN_TOKEN "$rhiza_admin_token"
require_nonempty VELORIX_RHIZA_SERVER_TLS_SECRET "$server_tls_secret"
require_nonempty VELORIX_RHIZA_CLIENT_TLS_SECRET "$client_tls_secret"
[ "$server_tls_secret" != "$client_tls_secret" ] || die "server and client TLS Secret names must differ"
case "$meta_image" in
  *@sha256:*) ;;
  *) die "VELORIX_RHIZA_META_IMAGE must be an immutable sha256 image reference" ;;
esac

# The official operator is pinned by digest. A tag or a mutable reference is not
# acceptable here: this component replaces cluster identity, membership, and
# credentials, so the recovery claim depends on exactly which binary ran.
#
# The default is the official published v0.19.0 pin and nothing relaxes it. The
# only way to run anything else is an explicit override that is itself a digest
# reference and that carries a recorded build provenance. Upstream publishes the
# operator for linux/amd64 only, so a platform that cannot execute that image
# has no honest alternative except a locally built binary from the pinned source
# tree with its build recorded. An override is therefore always reported as not
# the published release image.
operator_image=$(rhiza_select_operator_image "$operator_image_pin" "$operator_image_override" "$operator_image_provenance") \
  || die "VELORIX_RHIZA_OPERATOR_IMAGE must be the official published operator digest, or set VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE to an immutable sha256 reference together with VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE"
case "$operator_image" in
  *@sha256:*) ;;
  *) die "the selected operator image must be an immutable sha256 image reference" ;;
esac
if rhiza_operator_image_is_official "$operator_image"; then
  operator_image_official=true
  operator_image_override_used=false
else
  operator_image_official=false
  operator_image_override_used=true
fi

case "$namespace" in
  velorix-rhiza-validation|velorix-rhiza-validation-*) ;;
  *) die "VELORIX_RHIZA_NAMESPACE must use the isolated velorix-rhiza-validation prefix" ;;
esac
case "$probe_id" in
  *[!A-Za-z0-9._-]*|'') die "VELORIX_RHIZA_PROBE_ID must be a nonempty DNS-safe probe id" ;;
esac
case "$recovery_id" in
  *[!A-Za-z0-9._-]*|'') die "VELORIX_RHIZA_RECOVERY_ID must be a nonempty DNS-safe recovery id" ;;
esac
case "$run_id" in
  *[!a-z0-9.-]*|''|[-.]*|*[-.]) die "VELORIX_RHIZA_RUN_ID must be a lowercase DNS-safe value" ;;
esac
[ "${#run_id}" -le 39 ] || die "VELORIX_RHIZA_RUN_ID is too long for generated Kubernetes Job names"
case "$execute:$cleanup" in
  0:0|0:1|1:0|1:1) ;;
  *) die "VELORIX_RHIZA_EXECUTE and VELORIX_RHIZA_CLEANUP must be 0 or 1" ;;
esac
case "$object_store_insecure" in
  true|false) ;;
  *) die "VELORIX_RHIZA_OBJECT_STORE_INSECURE must be true or false; the operator parses it as a Go boolean" ;;
esac
# The operator refuses an async source without explicit data-loss consent, and
# this harness will not assert recovery semantics it cannot reproduce. A
# before-ack source is the only mode with a testable recovery contract here.
[ "$object_store_durability" = before-ack ] || die "Rhiza KV validation requires before-ack object-store durability"
[ "$object_store_provider" = s3 ] || die "Rhiza KV Kubernetes validation currently requires the s3 object-store provider"
case "$object_store_endpoint" in
  *://*) die "VELORIX_RHIZA_OBJECT_STORE_ENDPOINT must be the native host:port value without a URL scheme" ;;
esac
for safe_value in "$object_store_endpoint" "$object_store_bucket" "$object_store_region" "$object_store_prefix" "$server_tls_secret" "$client_tls_secret" "$image_pull_secret" "$operator_dir"; do
  if printf '%s' "$safe_value" | LC_ALL=C grep -q '[[:cntrl:]]'; then
    die "configuration values must not contain control characters"
  fi
done
for tls_secret_name in "$server_tls_secret" "$client_tls_secret"; do
  case "$tls_secret_name" in
    *[!A-Za-z0-9._-]*|'') die "TLS Secret names must be nonempty DNS-safe names" ;;
  esac
done
for tls_file in "$server_tls_cert_file" "$server_tls_key_file" "$server_tls_client_ca_file" \
  "$client_tls_cert_file" "$client_tls_key_file" "$client_tls_ca_file"; do
  if [ -n "$tls_file" ] && [ ! -r "$tls_file" ]; then
    die "configured TLS certificate files must be readable"
  fi
done
server_tls_files=0
client_tls_files=0
[ -n "$server_tls_cert_file" ] && server_tls_files=$((server_tls_files + 1))
[ -n "$server_tls_key_file" ] && server_tls_files=$((server_tls_files + 1))
[ -n "$server_tls_client_ca_file" ] && server_tls_files=$((server_tls_files + 1))
[ -n "$client_tls_cert_file" ] && client_tls_files=$((client_tls_files + 1))
[ -n "$client_tls_key_file" ] && client_tls_files=$((client_tls_files + 1))
[ -n "$client_tls_ca_file" ] && client_tls_files=$((client_tls_files + 1))
case "$server_tls_files:$client_tls_files" in
  0:0|3:3) ;;
  *) die "provide all six TLS files together, or use pre-created server/client TLS Secrets" ;;
esac

# The vendored operator manifests must be verifiably the official v0.19.0 files
# before anything is rendered or applied. A recovery claim must not rest on a
# manifest of unknown provenance.
rhiza_verify_operator_manifests "$operator_dir" \
  || die "vendored operator manifests under $operator_dir failed sha256 verification against UPSTREAM.sha256"

# The membership document is public material. Rhiza 0.19.0 decodes it with
# unknown fields disallowed, so a per-member "token" is a hard parse error rather
# than an ignored field. The allowlist below is therefore exact: four public
# fields, no token.
printf '%s' "$members_json" | jq -e --arg dns_suffix "${service_name}.${namespace}.svc.cluster.local" '
  type == "array" and length == 3 and
  all(.[]; . as $member |
    ($member | type == "object") and
    (($member | keys | sort) == ["node_id", "peer_url", "public_key", "url"]) and
    ($member.node_id | type == "string" and length > 0) and
    ($member.url == ("https://" + $member.node_id + "." + $dns_suffix + ":9090")) and
    ($member.peer_url == ("quic://" + $member.node_id + "." + $dns_suffix + ":8200"))
  ) and
  (map(.node_id) | sort == ["velorix-meta-0", "velorix-meta-1", "velorix-meta-2"])
' >/dev/null 2>&1 || die "VELORIX_RHIZA_MEMBERS_JSON must be a three-member array with exactly the node_id, url, peer_url, and public_key fields; per-member tokens are rejected by this Rhiza version and peer tokens are supplied through VELORIX_RHIZA_PEER_TOKENS"

# Exactly one encoding is accepted for public_key: standard padded base64 of 32
# raw bytes. The URL-safe alphabet used to generate tokens is not accepted.
printf '%s' "$members_json" | jq -e '
  all(.[]; .public_key | type == "string" and test("^[A-Za-z0-9+/]{43}=$"))
' >/dev/null 2>&1 || die "every VELORIX_RHIZA_MEMBERS_JSON public_key must be standard padded base64 of 32 raw bytes: 43 characters from A-Za-z0-9+/ followed by a single ="

# Private peer tokens arrive separately, keyed by node ID, and are published to
# the workload through the standard RHIZA_PEER_TOKENS map. Each Pod selects its
# own entry by RHIZA_NODE_ID, exactly as upstream does.
printf '%s' "$peer_tokens_json" | jq -e '
  type == "object" and
  ((keys | sort) == ["velorix-meta-0", "velorix-meta-1", "velorix-meta-2"]) and
  all(.[]; type == "string" and test("^[A-Za-z0-9][A-Za-z0-9._~-]{15,127}$"))
' >/dev/null 2>&1 || die "VELORIX_RHIZA_PEER_TOKENS must be a JSON object with exactly the velorix-meta-0, velorix-meta-1, and velorix-meta-2 keys and 16-128 character tokens matching [A-Za-z0-9][A-Za-z0-9._~-]*"

# A published public_key must equal the derivation for its own node, otherwise
# the node cannot prove it is that member and open fails with an opaque local
# identity mismatch. Cross-check every member here, before any cluster mutation.
preflight_tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/velorix-rhiza-derive.XXXXXX") || die "could not create a private key derivation directory"
chmod 700 "$preflight_tmp_dir"
peer_token_0=$(printf '%s' "$peer_tokens_json" | jq -r '.["velorix-meta-0"]')
peer_token_1=$(printf '%s' "$peer_tokens_json" | jq -r '.["velorix-meta-1"]')
peer_token_2=$(printf '%s' "$peer_tokens_json" | jq -r '.["velorix-meta-2"]')
for distinct_token in "$peer_token_0" "$peer_token_1" "$peer_token_2"; do
  case "$distinct_token" in
    "$rhiza_admin_token") die "a peer token must differ from VELORIX_RHIZA_ADMIN_TOKEN" ;;
  esac
done
if [ "$peer_token_0" = "$peer_token_1" ] || [ "$peer_token_0" = "$peer_token_2" ] || [ "$peer_token_1" = "$peer_token_2" ]; then
  die "the three peer tokens must be distinct"
fi
node_ordinal=0
for node_token in "$peer_token_0" "$peer_token_1" "$peer_token_2"; do
  node_name="velorix-meta-${node_ordinal}"
  published_key=$(printf '%s' "$members_json" | jq -r --arg node "$node_name" \
    '[.[] | select(.node_id == $node) | .public_key] | if length == 1 then .[0] else "" end')
  computed_key=$(rhiza_derive_public_key "$preflight_tmp_dir" "$node_token" "$run_id" "$node_name") \
    || die "could not derive the peer public key for ${node_name} with the available OpenSSL build"
  [ "$published_key" = "$computed_key" ] \
    || die "the ${node_name} public_key does not match the derivation for source cluster id ${run_id}; publish the derived key or supply the matching peer token"
  node_ordinal=$((node_ordinal + 1))
done
rm -rf "$preflight_tmp_dir"
preflight_tmp_dir=

# Do not expose member URLs, tokens, object-store credentials, or the supplied
# context in terminal output or evidence. kubectl command output is redirected
# to private files throughout this script.
preflight_file="$output_dir/preflight.txt"
preflight_error="$output_dir/preflight.error"

if kubectl --context "$context" get namespace "$namespace" >"$preflight_file" 2>"$preflight_error"; then
  existing_namespace=1
  if kubectl --context "$context" -n "$namespace" get all,configmap,secret,serviceaccount,role,rolebinding,networkpolicy,persistentvolumeclaim,job -l "$validation_label" -o name >"$preflight_file" 2>"$preflight_error"; then
    [ ! -s "$preflight_file" ] || die "an isolated validation with this run id already exists"
  else
    die "could not inspect the requested validation namespace"
  fi
  # The operator creates its own generation-scoped credential Secret during
  # recovery, so a name collision would silently bind the run to another
  # operation's credentials. Refuse rather than adopt one.
  for fixed_secret in "$object_store_secret_name" "$peer_credentials_secret_name" "$client_credentials_secret_name"; do
    if kubectl --context "$context" -n "$namespace" get secret "$fixed_secret" >"$preflight_file" 2>"$preflight_error"; then
      die "the isolated namespace already contains the fixed Secret ${fixed_secret}"
    fi
  done
  if kubectl --context "$context" -n "$namespace" get configmap "$config_map_name" >"$preflight_file" 2>"$preflight_error"; then
    die "the isolated namespace already contains the fixed ConfigMap ${config_map_name}"
  fi
  for fixed_resource in "service/${service_name}" "statefulset/${service_name}" "deployment/rhiza-operator" "serviceaccount/rhiza-operator"; do
    if kubectl --context "$context" -n "$namespace" get "$fixed_resource" >"$preflight_file" 2>"$preflight_error"; then
      die "the isolated namespace already contains the fixed resource ${fixed_resource}"
    fi
  done
  # The recovery CR is the resumable operation journal. Reusing a name would
  # adopt an earlier operation's fence attestation and target generation.
  for fixed_cr in "$recovery_resource_name" "$observation_resource_name"; do
    if kubectl --context "$context" -n "$namespace" get rhizarecovery "$fixed_cr" >"$preflight_file" 2>"$preflight_error"; then
      die "the isolated namespace already contains the recovery resource ${fixed_cr}"
    fi
  done
  # Observation-only recovery needs the RhizaRecovery CRD. A missing CRD is a
  # precondition failure, not something to paper over.
  if ! kubectl --context "$context" get crd rhizarecoveries.rhiza.mrchypark.dev >"$preflight_file" 2>"$preflight_error"; then
    die "the official RhizaRecovery CRD is not installed; apply deploy/rhiza-k8s/operator/crd.yaml first"
  fi
else
  if ! grep -qi 'not found' "$preflight_error"; then
    die "could not inspect the requested validation namespace"
  fi
  existing_namespace=0
fi

check_tls_secret_keys() {
  tls_secret=$1
  # shellcheck disable=SC2016
  secret_keys=$(kubectl --context "$context" -n "$namespace" get secret "$tls_secret" \
    -o go-template='{{range $key, $_ := .data}}{{printf "%s\n" $key}}{{end}}' \
    2>"$preflight_error") || {
    return 1
  }
  for required_key in tls.crt tls.key ca.crt; do
    printf '%s\n' "$secret_keys" | grep -Fqx "$required_key" || return 1
  done
}

if [ "$existing_namespace" = 1 ]; then
  server_tls_present=0
  client_tls_present=0
  if kubectl --context "$context" -n "$namespace" get secret "$server_tls_secret" >"$output_dir/server-tls-secret-exists.out" 2>"$preflight_error"; then
    check_tls_secret_keys "$server_tls_secret" || die "the server TLS Secret lacks tls.crt, tls.key, or ca.crt"
    server_tls_present=1
  fi
  if kubectl --context "$context" -n "$namespace" get secret "$client_tls_secret" >"$output_dir/client-tls-secret-exists.out" 2>"$preflight_error"; then
    check_tls_secret_keys "$client_tls_secret" || die "the client TLS Secret lacks tls.crt, tls.key, or ca.crt"
    client_tls_present=1
  fi
  if [ "$server_tls_present:$client_tls_present" != 1:1 ] && [ "$server_tls_files:$client_tls_files" != 3:3 ]; then
    die "the isolated namespace needs pre-created server/client TLS Secrets or all six local TLS files"
  fi
else
  [ "$server_tls_files:$client_tls_files" = 3:3 ] || die "the isolated namespace is absent; all six local TLS files are required to create its TLS Secrets"
fi

if [ "$execute" != 1 ]; then
  jq -n --arg status preflight_pass --arg evidence_scope rhiza_kv_official_operator_no_pvc_generation_recovery \
    --arg operator_image "$operator_image" \
    --argjson operator_image_official "$operator_image_official" \
    --argjson operator_image_override_used "$operator_image_override_used" \
    --arg operator_image_build_provenance "$operator_image_provenance" '
    {
      schema_version: 2,
      status: $status,
      evidence_scope: $evidence_scope,
      execution_required: true,
      context_configured: true,
      namespace_isolated: true,
      member_count: 3,
      operator_manifests_sha256_verified: true,
      operator_image: $operator_image,
      operator_image_digest_pinned: true,
      operator_image_official_published_release: $operator_image_official,
      operator_image_override_used: $operator_image_override_used,
      operator_image_build_provenance: $operator_image_build_provenance,
      no_cluster_mutation: true,
      member_tokens_absent: true,
      public_keys_derived_and_matched: true,
      public_key_encoding: "ed25519-standard-padded-base64-32-bytes",
      peer_token_delivery: "native-shared-peer-token-map",
      private_peer_token_in_pod_spec: false
    }
  ' >"$output_dir/rhiza-kv-gate-evidence.json"
  chmod 600 "$output_dir/rhiza-kv-gate-evidence.json" "$preflight_file" "$preflight_error"
  echo "rhiza KV Kubernetes gate preflight passed; set VELORIX_RHIZA_EXECUTE=1 for the isolated deployment"
  exit 0
fi

# ------------------------------------------------------------------ execute --

manifest="$output_dir/rhiza-kv-workload.yaml"
operator_manifest="$output_dir/rhiza-kv-operator.yaml"
created_namespace=0

cleanup_resources() {
  [ "$cleanup" = 1 ] || return 0
  # Delete the recovery journal before the workload, so a partially torn down
  # namespace cannot leave an operator retry against a half-deleted generation.
  kubectl --context "$context" -n "$namespace" delete rhizarecovery "$recovery_resource_name" "$observation_resource_name" \
    --ignore-not-found >"$output_dir/cleanup.out" 2>"$output_dir/cleanup.error" || true
  kubectl --context "$context" -n "$namespace" delete job -l "$validation_label" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  kubectl --context "$context" -n "$namespace" delete statefulset,service,configmap,secret -l "$validation_label" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  # The operator's own RBAC, Deployment, and ServiceAccount carry the upstream
  # names and no run-scoped label, so they are removed explicitly.
  kubectl --context "$context" -n "$namespace" delete deployment,serviceaccount,role,rolebinding rhiza-operator \
    --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  if [ "$created_namespace" = 1 ]; then
    kubectl --context "$context" delete namespace "$namespace" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  fi
}
trap 'cleanup_resources; cleanup_private_files' EXIT HUP INT TERM

if [ "$existing_namespace" = 0 ]; then
  if ! kubectl --context "$context" create namespace "$namespace" >"$output_dir/namespace.out" 2>"$output_dir/namespace.error"; then
    die "could not create the isolated validation namespace"
  fi
  created_namespace=1
fi

secret_tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/velorix-rhiza-secrets.XXXXXX") || die "could not create a private credential staging directory"
chmod 700 "$secret_tmp_dir"
printf '%s' "$members_json" >"$secret_tmp_dir/members.json"
printf '%s' "$peer_tokens_json" >"$secret_tmp_dir/peer-tokens.json"
printf '%s' "$rhiza_admin_token" >"$secret_tmp_dir/rhiza-admin-token"
printf '%s' "$meta_bearer_token" >"$secret_tmp_dir/meta-bearer-token"
printf '%s' "$object_store_access_key" >"$secret_tmp_dir/object-store-access-key"
printf '%s' "$object_store_secret_key" >"$secret_tmp_dir/object-store-secret-key"
printf '%s' "$object_store_session_token" >"$secret_tmp_dir/object-store-session-token"
# --from-file=key=path takes a path, not a value. The object-store location
# fields are staged as files too, so endpoint, bucket, and region cannot be
# silently misread as nonexistent paths, and so the rendered Secret is assembled
# from private files only.
printf '%s' "$object_store_endpoint" >"$secret_tmp_dir/object-store-endpoint"
printf '%s' "$object_store_bucket" >"$secret_tmp_dir/object-store-bucket"
printf '%s' "$object_store_region" >"$secret_tmp_dir/object-store-region"
chmod 600 "$secret_tmp_dir"/*

create_tls_secret_if_needed() {
  tls_secret=$1
  cert_file=$2
  key_file=$3
  ca_file=$4
  if kubectl --context "$context" -n "$namespace" get secret "$tls_secret" >"$output_dir/tls-secret-check.out" 2>"$output_dir/tls-secret-check.error"; then
    return 0
  fi
  tls_secret_yaml="$secret_tmp_dir/${tls_secret}.yaml"
  if ! kubectl --context "$context" -n "$namespace" create secret generic "$tls_secret" \
    --from-file=tls.crt="$cert_file" --from-file=tls.key="$key_file" --from-file=ca.crt="$ca_file" \
    --dry-run=client -o yaml >"$tls_secret_yaml" 2>"$secret_tmp_dir/${tls_secret}.error"; then
    rm -f "$tls_secret_yaml" "$secret_tmp_dir/${tls_secret}.error"
    die "could not render the TLS Secret"
  fi
  chmod 600 "$tls_secret_yaml" "$secret_tmp_dir/${tls_secret}.error"
  if ! kubectl --context "$context" apply -f "$tls_secret_yaml" >"$output_dir/${tls_secret}-apply.out" 2>"$output_dir/${tls_secret}-apply.error"; then
    die "could not apply the TLS Secret"
  fi
  rm -f "$tls_secret_yaml"
}

if [ "$server_tls_files:$client_tls_files" = 3:3 ]; then
  create_tls_secret_if_needed "$server_tls_secret" "$server_tls_cert_file" "$server_tls_key_file" "$server_tls_client_ca_file"
  create_tls_secret_if_needed "$client_tls_secret" "$client_tls_cert_file" "$client_tls_key_file" "$client_tls_ca_file"
fi

# The object-store Secret carries only location and credential material, and is
# read by both the workload and the operator so they address one archive
# namespace. No credential is placed in a manifest or in the evidence bundle.
object_store_secret_yaml="$secret_tmp_dir/object-store-secret.yaml"
if ! kubectl --context "$context" -n "$namespace" create secret generic "$object_store_secret_name" \
  --from-file=RHIZA_OBJSTORE_ENDPOINT="$secret_tmp_dir/object-store-endpoint" \
  --from-file=RHIZA_OBJSTORE_BUCKET="$secret_tmp_dir/object-store-bucket" \
  --from-file=RHIZA_OBJSTORE_REGION="$secret_tmp_dir/object-store-region" \
  --from-file=RHIZA_OBJSTORE_ACCESS_KEY="$secret_tmp_dir/object-store-access-key" \
  --from-file=RHIZA_OBJSTORE_SECRET_KEY="$secret_tmp_dir/object-store-secret-key" \
  --from-file=RHIZA_OBJSTORE_SESSION_TOKEN="$secret_tmp_dir/object-store-session-token" \
  --dry-run=client -o yaml >"$object_store_secret_yaml" 2>"$secret_tmp_dir/object-store-secret.error"; then
  cp "$secret_tmp_dir/object-store-secret.error" "$output_dir/object-store-secret-render.error" 2>/dev/null || true
  die "could not render the object-store Secret; see $output_dir/object-store-secret-render.error"
fi
chmod 600 "$object_store_secret_yaml" "$secret_tmp_dir/object-store-secret.error"
if ! kubectl --context "$context" apply -f "$object_store_secret_yaml" >"$output_dir/object-store-secret-apply.out" 2>"$output_dir/object-store-secret-apply.error"; then
  die "could not apply the object-store Secret"
fi
rm -f "$object_store_secret_yaml"

# Shared peer credentials for the source generation. RHIZA_PEER_TOKENS is the
# standard native map: every Pod receives it and selects its own entry by
# RHIZA_NODE_ID. RHIZA_ADMIN_TOKEN authenticates the operator's archive capture.
# The membership document stays in the ConfigMap, which holds only public keys.
peer_credentials_secret_yaml="$secret_tmp_dir/peer-credentials-secret.yaml"
if ! kubectl --context "$context" -n "$namespace" create secret generic "$peer_credentials_secret_name" \
  --from-file=RHIZA_ADMIN_TOKEN="$secret_tmp_dir/rhiza-admin-token" \
  --from-file=RHIZA_PEER_TOKENS="$secret_tmp_dir/peer-tokens.json" \
  --dry-run=client -o yaml >"$peer_credentials_secret_yaml" 2>"$secret_tmp_dir/peer-credentials-secret.error"; then
  die "could not render the peer-credentials Secret"
fi
chmod 600 "$peer_credentials_secret_yaml" "$secret_tmp_dir/peer-credentials-secret.error"
if ! kubectl --context "$context" apply -f "$peer_credentials_secret_yaml" >"$output_dir/peer-credentials-apply.out" 2>"$output_dir/peer-credentials-apply.error"; then
  die "could not apply the peer-credentials Secret"
fi
rm -f "$peer_credentials_secret_yaml"

# Velorix gRPC credentials are unrelated to Rhiza peer identity and stay in a
# Secret of their own.
client_credentials_secret_yaml="$secret_tmp_dir/client-credentials-secret.yaml"
if ! kubectl --context "$context" -n "$namespace" create secret generic "$client_credentials_secret_name" \
  --from-file=meta-bearer-token="$secret_tmp_dir/meta-bearer-token" \
  --dry-run=client -o yaml >"$client_credentials_secret_yaml" 2>"$secret_tmp_dir/client-credentials-secret.error"; then
  die "could not render the client-credentials Secret"
fi
chmod 600 "$client_credentials_secret_yaml" "$secret_tmp_dir/client-credentials-secret.error"
if ! kubectl --context "$context" apply -f "$client_credentials_secret_yaml" >"$output_dir/client-credentials-apply.out" 2>"$output_dir/client-credentials-apply.error"; then
  die "could not apply the client-credentials Secret"
fi
rm -f "$client_credentials_secret_yaml"

image_pull_secret_yaml=""
if [ -n "$image_pull_secret" ]; then
  image_pull_secret_yaml=$(printf '      imagePullSecrets:\n        - name: %s' "$image_pull_secret")
fi

# The ConfigMap holds only public, non-secret configuration: the source cluster
# identity, the public membership document, and the shared object-store location.
#
# RHIZA_OBJSTORE_PREFIX is the bare shared prefix, not a per-run one. Rhiza
# namespaces each generation under <prefix>/<cluster id>, and the operator derives
# its source prefix the same way from its own copy of this value. Appending the
# run id here would nest the source generation twice and diverge from the
# official fixture, so per-run isolation comes from the cluster ID alone.
config_map_yaml="$output_dir/rhiza-kv-config.yaml"
# The membership document is a JSON array, which is also a valid YAML flow
# sequence. Emitting it unquoted would make the API server decode it as a list of
# maps instead of a string, so it is re-encoded as one explicit JSON string
# scalar first.
members_yaml_scalar=$(printf '%s' "$members_json" | jq -Rs .) \
  || die "could not encode the membership document as a single YAML scalar"
cat >"$config_map_yaml" <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${config_map_name}
  namespace: ${namespace}
  labels:
    velorix.dev/rhiza-kv-validation: ${run_id}
data:
  RHIZA_CLUSTER_ID: "${run_id}"
  RHIZA_CLUSTER_MEMBERS: ${members_yaml_scalar}
  RHIZA_DATA_DIR: "/var/lib/velorix-meta"
  RHIZA_BIND_ADDR: "0.0.0.0:9091"
  RHIZA_PEER_ADDR: "0.0.0.0:8200"
  RHIZA_OBJSTORE_PROVIDER: "${object_store_provider}"
  RHIZA_OBJSTORE_PREFIX: "${object_store_prefix}"
  RHIZA_OBJSTORE_DURABILITY: "${object_store_durability}"
  RHIZA_OBJSTORE_INSECURE: "${object_store_insecure}"
EOF
chmod 600 "$config_map_yaml"
if ! kubectl --context "$context" apply -f "$config_map_yaml" >"$output_dir/config-apply.out" 2>"$output_dir/config-apply.error"; then
  die "could not apply the Rhiza ConfigMap"
fi

# The workload. Note what is absent: no PVC, no volumeClaimTemplates, no
# subPath, and no Velorix-owned RHIZA_* shadow. VELORIX_RHIZA_OPERATOR_MANAGED
# is the only Velorix-owned Rhiza variable, and it is an opt-in that makes the
# binary read the canonical RHIZA_* environment natively.
#
# The `recovery` container port is load-bearing: the operator resolves its Pod
# endpoint through it and falls back to a wrong port without it. gRPC stays on
# 9090 and is never published to the recovery listener.
cat >"$manifest" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${service_name}
  namespace: ${namespace}
  labels:
    app: ${app_label}
    velorix.dev/rhiza-kv-validation: ${run_id}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: ${app_label}
    velorix.dev/rhiza-kv-validation: ${run_id}
  ports:
    - name: grpc
      port: 9090
      targetPort: grpc
    - name: peer
      port: 8200
      targetPort: peer
      protocol: UDP
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ${service_name}
  namespace: ${namespace}
  labels:
    app: ${app_label}
    velorix.dev/rhiza-kv-validation: ${run_id}
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: ${app_label}
      velorix.dev/rhiza-kv-validation: ${run_id}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${service_name}
  namespace: ${namespace}
  labels:
    app: ${app_label}
    velorix.dev/rhiza-kv-validation: ${run_id}
spec:
  serviceName: ${service_name}
  replicas: 3
  podManagementPolicy: Parallel
  # The operator replaces the StatefulSet as a whole generation. A rolling update
  # strategy would restart one new voter into the old fixed membership, which is
  # exactly what the upstream no-PVC contract forbids.
  updateStrategy:
    type: OnDelete
    rollingUpdate: null
  selector:
    matchLabels:
      app: ${app_label}
      velorix.dev/rhiza-kv-validation: ${run_id}
  template:
    metadata:
      labels:
        app: ${app_label}
        velorix.dev/rhiza-kv-validation: ${run_id}
    spec:
      terminationGracePeriodSeconds: 30
      securityContext:
        runAsUser: 65532
        runAsGroup: 65532
        runAsNonRoot: true
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
${image_pull_secret_yaml}
      containers:
        - name: velorix-meta
          image: ${meta_image}
          imagePullPolicy: IfNotPresent
          ports:
            - name: grpc
              containerPort: 9090
            - name: recovery
              containerPort: 9091
            - name: peer
              containerPort: 8200
              protocol: UDP
          command: ["/usr/local/bin/velorix-meta"]
          envFrom:
            - configMapRef:
                name: ${config_map_name}
            - secretRef:
                name: ${object_store_secret_name}
            - secretRef:
                name: ${peer_credentials_secret_name}
          env:
            - name: RHIZA_NODE_ID
              # The only dynamic fieldRef the operator accepts for a RHIZA_*
              # variable. It selects this Pod's entry from RHIZA_PEER_TOKENS.
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: VELORIX_META_MODE
              value: production
            - name: VELORIX_META_BIND
              value: 0.0.0.0:9090
            - name: VELORIX_META_BACKEND
              value: rhiza-kv
            - name: VELORIX_META_BEARER_TOKEN
              valueFrom: {secretKeyRef: {name: ${client_credentials_secret_name}, key: meta-bearer-token}}
            - name: VELORIX_META_TRANSPORT_SECURITY
              value: native-mtls
            - name: VELORIX_META_TLS_CERT_FILE
              value: /etc/velorix/tls/server/tls.crt
            - name: VELORIX_META_TLS_KEY_FILE
              value: /etc/velorix/tls/server/tls.key
            - name: VELORIX_META_TLS_CLIENT_CA_FILE
              value: /etc/velorix/tls/server/ca.crt
            - name: VELORIX_META_TLS_CA_FILE
              value: /etc/velorix/tls/client/ca.crt
            - name: VELORIX_META_TLS_CLIENT_CERT_FILE
              value: /etc/velorix/tls/client/tls.crt
            - name: VELORIX_META_TLS_CLIENT_KEY_FILE
              value: /etc/velorix/tls/client/tls.key
            - name: VELORIX_META_TLS_DOMAIN_NAME
              value: ${service_name}.${namespace}.svc.cluster.local
            # The only Velorix-owned Rhiza variable. It makes the binary consume
            # the canonical RHIZA_* environment natively and start the private
            # recovery listener. Every other Rhiza setting belongs to native and
            # to the operator, which rewrites it between generations.
            - name: VELORIX_RHIZA_OPERATOR_MANAGED
              value: "1"
          readinessProbe:
            exec:
              command:
                - /bin/sh
                - -ec
                - >-
                  exec /usr/local/bin/velorix-meta smoke --endpoint https://127.0.0.1:9090
                  --bearer-token "\$VELORIX_META_BEARER_TOKEN"
                  --expect-backend rhiza-kv --expect-auth-enforced true
                  --expect-production-multi-writer-safe false
                  --connect-retry-timeout-seconds 10 --capabilities-only
            periodSeconds: 5
            timeoutSeconds: 15
            failureThreshold: 12
          volumeMounts:
            - name: data
              mountPath: /var/lib/velorix-meta
            - name: server-tls
              mountPath: /etc/velorix/tls/server
              readOnly: true
            - name: client-tls
              mountPath: /etc/velorix/tls/client
              readOnly: true
      volumes:
        # The no-PVC contract: database state lives only here and is lost with
        # the Pod. The operator recovers it from the shared object store.
        - name: data
          emptyDir: {}
        - name: server-tls
          secret:
            secretName: ${server_tls_secret}
            items:
              - key: tls.crt
                path: tls.crt
              - key: tls.key
                path: tls.key
              - key: ca.crt
                path: ca.crt
        - name: client-tls
          secret:
            secretName: ${client_tls_secret}
            items:
              - key: tls.crt
                path: tls.crt
              - key: tls.key
                path: tls.key
              - key: ca.crt
                path: ca.crt
EOF
chmod 600 "$manifest"

if ! kubectl --context "$context" apply -f "$manifest" >"$output_dir/workload-apply.out" 2>"$output_dir/workload-apply.error"; then
  die "could not apply the isolated Rhiza workload"
fi

# The CRD is cluster-scoped, so this harness does not install it: that is a
# separate, explicitly authorized decision. What the harness must do is refuse to
# run against a CRD that is not the official one, because a different schema
# would silently change what the operator accepts.
if ! kubectl --context "$context" get crd rhizarecoveries.rhiza.mrchypark.dev -o json \
  >"$output_dir/installed-crd.json" 2>"$preflight_error"; then
  grep -qi 'not found' "$preflight_error" \
    || die "could not read the installed RhizaRecovery CRD"
  die "the official RhizaRecovery CRD is not installed in this cluster; apply deploy/rhiza-k8s/operator/crd.yaml as a separate cluster-scoped step"
fi
if ! kubectl create --dry-run=client -f "$operator_dir/crd.yaml" -o json >"$output_dir/vendored-crd.json" 2>"$output_dir/vendored-crd.error"; then
  die "could not read the vendored official CRD"
fi
if ! jq -S -e --slurpfile vendored "$output_dir/vendored-crd.json" '
  # The API server fills in every field the manifest left out: names.listKind and
  # spec.conversion are the two that matter here, and a CRD without spec.conversion
  # is served with strategy "None". Comparing the stored spec byte-for-byte
  # against the vendored file could therefore never succeed against a real
  # cluster, so the comparison is over what the vendored file actually declares,
  # with only those documented defaultings normalized away.
  def declared:
    {
      group: .group,
      scope: .scope,
      names: (.names | del(.listKind?)),
      versions: .versions,
      conversion: (.conversion // {strategy: "None"})
    };
  (.spec | declared) == ($vendored[0].spec | declared)
' "$output_dir/installed-crd.json" >"$output_dir/crd-identity-check.out" 2>"$output_dir/crd-identity-check.error"; then
  die "the installed RhizaRecovery CRD does not match the vendored official v0.19.0 CRD; see $output_dir/installed-crd.json and $output_dir/vendored-crd.json"
fi

# Render the vendored operator manifests for this namespace. The overlay changes
# only the namespace and the image, both required by the upstream notes. This
# render additionally omits the CRD, because it is cluster-scoped and this harness
# verifies it rather than installing it.
operator_render_dir="$secret_tmp_dir/operator"
mkdir -p "$operator_render_dir"
cp "$operator_dir/rbac.yaml" "$operator_dir/deployment.yaml" "$operator_render_dir/"
cat >"$operator_render_dir/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - rbac.yaml
  - deployment.yaml
namespace: ${namespace}
images:
  - name: rhiza-operator
    newName: ${operator_image%@sha256:*}
    digest: "sha256:${operator_image##*@sha256:}"
EOF

if ! kubectl kustomize "$operator_render_dir" >"$operator_manifest" 2>"$output_dir/operator-render.error"; then
  die "could not render the operator manifests"
fi
chmod 600 "$operator_manifest"
if [ ! -s "$operator_manifest" ]; then
  die "the operator render produced no namespaced resources"
fi
# The namespaced render must contain exactly the four namespaced objects the
# upstream scaffold defines, and no cluster-scoped CRD. kubectl emits a stream of
# documents rather than a List, so the documents are slurped before inspecting.
if ! kubectl create --dry-run=client -f "$operator_manifest" -o json 2>"$output_dir/operator-render-check.error" \
  | jq -s -e '
      (length == 4) and
      ((map(.kind) | sort) == ["Deployment","Role","RoleBinding","ServiceAccount"]) and
      (all(.[]; .metadata.namespace != null))
    ' >"$output_dir/operator-render-check.out" 2>&1; then
  die "the operator render is not the expected namespaced-only official scaffold"
fi
if ! kubectl --context "$context" apply -f "$operator_manifest" >"$output_dir/operator-apply.out" 2>"$output_dir/operator-apply.error"; then
  die "could not apply the official recovery operator"
fi
# The operator must actually be running the pinned digest before any recovery is
# requested. A crash-looping or image-mismatched operator would reject the
# request, and this harness must not read that rejection as a no-PVC result.
if ! kubectl --context "$context" -n "$namespace" rollout status deployment/rhiza-operator --timeout=5m \
  >"$output_dir/operator-rollout.out" 2>"$output_dir/operator-rollout.error"; then
  kubectl --context "$context" -n "$namespace" logs deployment/rhiza-operator --tail=200 \
    >"$output_dir/operator.log" 2>"$output_dir/operator-log.error" || true
  die "the official recovery operator did not become available; see $output_dir/operator.log"
fi
operator_running_image=$(kubectl --context "$context" -n "$namespace" get deployment rhiza-operator \
  -o jsonpath='{.spec.template.spec.containers[0].image}' 2>"$preflight_error") \
  || die "could not read the operator image reference"
[ "$operator_running_image" = "$operator_image" ] \
  || die "the operator Deployment is not running the requested image ${operator_image}; it runs ${operator_running_image:-unknown}"
case "$operator_running_image" in
  *@sha256:*) ;;
  *) die "the operator Deployment image is not digest-pinned" ;;
esac

# `kubectl rollout status` refuses to work for anything but RollingUpdate, and
# this workload deliberately uses OnDelete so that a replacement generation is
# never rolled one voter at a time into the old fixed membership. Readiness is
# therefore polled on the StatefulSet status directly, which is also the only
# check that cannot be satisfied by an orphaned Pod.
statefulset_deadline=$(( $(date +%s) + 600 ))
statefulset_ready=0
while [ "$(date +%s)" -lt "$statefulset_deadline" ]; do
  statefulset_ready=$(kubectl --context "$context" -n "$namespace" get statefulset "$service_name" -o json \
    2>"$output_dir/rollout.error" \
    | jq -r 'if (.status.readyReplicas // 0) == .spec.replicas and (.status.currentRevision == .status.updateRevision) then "ready" else "" end' 2>/dev/null) || statefulset_ready=
  case "$statefulset_ready" in
    ready) break ;;
  esac
  sleep 5
done
if [ "$statefulset_ready" != ready ]; then
  kubectl --context "$context" -n "$namespace" get pods -o wide >"$output_dir/rollout-pods.out" 2>&1 || true
  die "the three-node Rhiza StatefulSet did not become ready; see $output_dir/rollout-pods.out"
fi

# --------------------------------------------------- applied-state assertions

if ! kubectl --context "$context" -n "$namespace" get statefulset "$service_name" -o json \
  | jq -e '(.spec.replicas == 3)
    and (.spec.updateStrategy.type == "OnDelete")
    and (.spec.podManagementPolicy == "Parallel")
    and (.spec.template.spec.securityContext.fsGroup == 65532)
    and (.spec.template.spec.securityContext.runAsUser == 65532)
    and (.spec.template.spec.securityContext.runAsGroup == 65532)
    and (.spec.template.spec.securityContext.runAsNonRoot == true)
    and ((.spec.volumeClaimTemplates // []) | length == 0)
    and (([.spec.template.spec.volumes[]? | select(.persistentVolumeClaim != null)] | length) == 0)
    and (([.spec.template.spec.volumes[]? | select(.name == "data")][0].emptyDir) != null)
    and (([.spec.template.spec.containers[0].volumeMounts[]? | select(.name == "data")][0].subPath // "") == "")
    and (([.spec.template.spec.containers[0].volumeMounts[]? | select(.name == "data")][0].subPathExpr // "") == "")
  ' >"$output_dir/storage-check.out" 2>"$output_dir/storage-check.error"; then
  die "the Rhiza workload is not a three-replica emptyDir-only OnDelete StatefulSet with the required non-root fsGroup"
fi
if ! kubectl --context "$context" -n "$namespace" get pvc -o name >"$output_dir/pvc-check.out" 2>"$output_dir/pvc-check.error"; then
  die "could not inspect PVCs in the isolated validation namespace"
fi
[ ! -s "$output_dir/pvc-check.out" ] || die "the Rhiza validation namespace contains a PVC"

# Prove the deployed contract the operator will read: the recovery port is
# named, the recovery listener is separate from gRPC, the source cluster
# identity is present, and no Velorix-owned RHIZA_* shadow survives.
if ! kubectl --context "$context" -n "$namespace" get statefulset "$service_name" -o json \
  | jq -e --arg operator_managed VELORIX_RHIZA_OPERATOR_MANAGED '
      (.spec.template.spec.containers | length) == 1 and
      (.spec.template.spec.containers[0].name == "velorix-meta") and
      (.spec.template.spec.containers[0].ports as $ports |
        ([$ports[] | select(.name == "recovery" and .containerPort == 9091 and (.protocol // "TCP") == "TCP")] | length == 1) and
        ([$ports[] | select(.name == "grpc" and .containerPort == 9090)] | length == 1) and
        ([$ports[] | select(.name == "peer" and .containerPort == 8200 and .protocol == "UDP")] | length == 1)
      ) and
      (.spec.template.spec.containers[0].envFrom as $envfrom |
        ([$envfrom[] | select(.configMapRef.name == "rhiza-config")] | length == 1) and
        ([$envfrom[] | select(.secretRef.name == "rhiza-object-store")] | length == 1) and
        ([$envfrom[] | select(.secretRef.name == "rhiza-peer-credentials")] | length == 1)
      ) and
      (.spec.template.spec.containers[0].env as $env |
        ([$env[] | select(.name == "RHIZA_NODE_ID")][0].valueFrom.fieldRef.fieldPath) == "metadata.name" and
        ([$env[] | select(.name == $operator_managed)] | length == 1) and
        ([$env[] | select(.name == $operator_managed)][0].value == "1") and
        # No inline peer token, and no Velorix-owned shadow of any RHIZA_* value.
        ([$env[] | select(.name == "RHIZA_PEER_TOKEN")] | length == 0) and
        ([$env[] | select(.name | startswith("VELORIX_RHIZA_")) | select(.name != $operator_managed)] | length == 0)
      )
    ' >"$output_dir/operator-contract-check.out" 2>"$output_dir/operator-contract-check.error"; then
  die "the applied Rhiza workload does not expose the operator recovery contract, or carries a Velorix-owned RHIZA_* shadow"
fi

# The recovery listener must be reachable on the Pod IP the operator resolves,
# and it must not be published through the Service.
if ! kubectl --context "$context" -n "$namespace" get service "$service_name" -o json \
  | jq -e '([.spec.ports[] | select(.port == 9091)] | length) == 0 and ([.spec.ports[] | select(.name == "recovery")] | length) == 0' \
    >"$output_dir/service-exposure-check.out" 2>"$output_dir/service-exposure-check.error"; then
  die "the recovery listener port is published through the Service; it must be reachable only on the Pod IP"
fi

# The shared peer-token map is delivered through the standard native Secret, so
# it holds exactly the two native keys and nothing else.
# shellcheck disable=SC2016
if ! peer_credentials_keys=$(kubectl --context "$context" -n "$namespace" get secret "$peer_credentials_secret_name" \
  -o go-template='{{range $key, $_ := .data}}{{printf "%s\n" $key}}{{end}}' \
  2>"$output_dir/peer-credentials-keys.error" | LC_ALL=C sort | tr '\n' ' '); then
  die "could not inspect the peer-credentials Secret keys"
fi
[ "$peer_credentials_keys" = "RHIZA_ADMIN_TOKEN RHIZA_PEER_TOKENS " ] \
  || die "the peer-credentials Secret does not carry exactly RHIZA_ADMIN_TOKEN and RHIZA_PEER_TOKENS"

# The operator must agree the deployment satisfies the no-PVC contract and that
# the source generation has quorum, before any destructive step. This is the
# step that fails closed when the operator is missing, mismatched, or unhealthy:
# a rejection here is a precondition failure, never a no-PVC result.
observation_manifest="$output_dir/rhiza-kv-observation.yaml"
cat >"$observation_manifest" <<EOF
apiVersion: rhiza.mrchypark.dev/v1alpha1
kind: RhizaRecovery
metadata:
  name: ${observation_resource_name}
  namespace: ${namespace}
spec:
  statefulSet: ${service_name}
  container: velorix-meta
  sourceClusterID: ${run_id}
  durability: ${object_store_durability}
  # Empty recoveryID observes only and cannot change the StatefulSet.
  recoveryID: ""
  allowDataLoss: false
EOF
chmod 600 "$observation_manifest"
if ! kubectl --context "$context" apply -f "$observation_manifest" >"$output_dir/observation-apply.out" 2>"$output_dir/observation-apply.error"; then
  die "could not apply the observation-only RhizaRecovery resource"
fi

observation_deadline=$(( $(date +%s) + 300 ))
observation_phase=
while [ "$(date +%s)" -lt "$observation_deadline" ]; do
  observation_phase=$(kubectl --context "$context" -n "$namespace" get rhizarecovery "$observation_resource_name" \
    -o jsonpath='{.status.phase}' 2>/dev/null) || observation_phase=
  case "$observation_phase" in
    Observed) break ;;
  esac
  sleep 5
done
kubectl --context "$context" -n "$namespace" get rhizarecovery "$observation_resource_name" -o json \
  >"$output_dir/observation-status.json" 2>"$output_dir/observation-status.error" \
  || die "could not read the observation status"
if [ "$observation_phase" != Observed ]; then
  die "the official operator did not report an Observed source generation (phase '${observation_phase:-none}'); this is a precondition failure, not a recovery result"
fi

# Observation must also confirm quorum on the source generation. A ready local
# database is not a cluster.
if ! jq -e --arg source "$run_id" '
  ([.status.peers[]? | select(.cluster_id == $source and .ready == true and .quorum == true)] | length) >= 2
' "$output_dir/observation-status.json" >"$output_dir/observation-quorum-check.out" 2>&1; then
  die "the source generation did not reach quorum on two or more voters; recovery must not be attempted"
fi

# shellcheck disable=SC2016
if ! kubectl --context "$context" -n "$namespace" exec "${service_name}-0" -c velorix-meta -- \
  /bin/sh -ec 'probe=/var/lib/velorix-meta/.rhiza-kv-write-check; : >"$probe"; rm -f "$probe"' \
  >"$output_dir/emptydir-write-check.out" 2>"$output_dir/emptydir-write-check.error"; then
  die "the non-root Rhiza container cannot write its emptyDir data directory"
fi

before_uids=$(kubectl --context "$context" -n "$namespace" get pods -l "app=${app_label},${validation_label}" -o json 2>"$output_dir/before-pods.error" | tee "$output_dir/before-pods.json" | jq -r '[.items[].metadata.uid] | sort | join(",")')
[ "$(printf '%s' "$before_uids" | awk -F, '{print NF}')" = 3 ] || die "the three-node Rhiza workload did not produce three Pods"
source_statefulset_uid=$(kubectl --context "$context" -n "$namespace" get statefulset "$service_name" -o jsonpath='{.metadata.uid}' 2>"$preflight_error") \
  || die "could not read the source StatefulSet UID"

# ------------------------------------------------------------------- probes

run_smoke_job() {
  phase=$1
  read_only=${2:-0}
  endpoint=${3:-https://${service_name}.${namespace}.svc.cluster.local:9090}
  # Only a phase that genuinely writes a new catalog passes its own probe id.
  # Every other phase reuses the pre-loss id unchanged.
  job_probe_id=${4:-$probe_id}
  job="rhiza-kv-${phase}-${run_id}"
  job_file="$output_dir/${job}.yaml"
  verify_only_arg=
  if [ "$read_only" = 1 ]; then
    verify_only_arg=' --verify-only'
  fi
  # The heredoc below is Kubernetes YAML, not shell source. Its literal
  # environment expansion is intentionally evaluated by the probe container.
  # shellcheck disable=SC2016,SC2086,SC2153,SC2215,SC1083
  cat >"$job_file" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  namespace: ${namespace}
  labels:
    app: ${app_label}
    velorix.dev/rhiza-kv-validation: ${run_id}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels:
        app: ${app_label}-probe
        velorix.dev/rhiza-kv-validation: ${run_id}
    spec:
      restartPolicy: Never
      containers:
        - name: probe
          image: ${meta_image}
          command: ["/bin/sh", "-ec"]
          args:
            - >-
              exec /usr/local/bin/velorix-meta smoke --endpoint ${endpoint}
              --bearer-token "\$META_BEARER_TOKEN" --expect-backend rhiza-kv
              --expect-auth-enforced true --expect-production-multi-writer-safe false
              --catalog-probe-id ${job_probe_id} --connect-retry-timeout-seconds 120${verify_only_arg}
          env:
            - name: META_BEARER_TOKEN
              valueFrom: {secretKeyRef: {name: ${client_credentials_secret_name}, key: meta-bearer-token}}
            - name: VELORIX_META_TLS_CA_FILE
              value: /etc/velorix/tls/client/ca.crt
            - name: VELORIX_META_TLS_CLIENT_CERT_FILE
              value: /etc/velorix/tls/client/tls.crt
            - name: VELORIX_META_TLS_CLIENT_KEY_FILE
              value: /etc/velorix/tls/client/tls.key
            - name: VELORIX_META_TLS_DOMAIN_NAME
              value: ${service_name}.${namespace}.svc.cluster.local
          volumeMounts:
            - name: client-tls
              mountPath: /etc/velorix/tls/client
              readOnly: true
      volumes:
        - name: client-tls
          secret:
            secretName: ${client_tls_secret}
            items:
              - key: tls.crt
                path: tls.crt
              - key: tls.key
                path: tls.key
              - key: ca.crt
                path: ca.crt
EOF
  chmod 600 "$job_file"
  kubectl --context "$context" apply -f "$job_file" >"$output_dir/${phase}-job-apply.out" 2>"$output_dir/${phase}-job-apply.error" || die "could not apply the ${phase} service-connection smoke Job"
  kubectl --context "$context" -n "$namespace" wait --for=condition=complete "job/${job}" --timeout=10m >"$output_dir/${phase}-job-wait.out" 2>"$output_dir/${phase}-job-wait.error" || die "the ${phase} service-connection smoke failed"
  kubectl --context "$context" -n "$namespace" logs "job/${job}" >"$output_dir/${phase}-smoke.log" 2>"$output_dir/${phase}-smoke.error" || die "could not collect the ${phase} smoke result"
  chmod 600 "$output_dir/${phase}-smoke.log" "$output_dir/${phase}-smoke.error"
  rm -f "$job_file"
}

# The pre-loss probe writes a unique catalog. Every later read-only probe must
# find that exact catalog in the recovered generation, so it cannot recreate
# missing state.
run_smoke_job before-recovery 0

# ------------------------------------------------- fenced whole-generation loss

# The loss is performed by the official operator. This script must not create
# it, and it must not pre-destroy the source generation:
#
#   * upstream native/pkg/operator/controller.go rejects any first reconcile
#     whose source StatefulSet does not declare exactly three desired replicas
#     ("source StatefulSet must have three desired replicas"), and
#   * that same first pass captures the certified archive suffix from the live
#     source Pod recovery endpoints.
#
# Scaling the StatefulSet to zero here would therefore make the operator reject
# the request outright. The run would fail, and no recovery would be claimed.
#
# The operator owns the entire teardown. Once the fence is confirmed it seals
# the source archive, forks the certified history, writes the target
# credentials, and only then sets spec.replicas to 0 and waits for every Pod
# still owned by the source StatefulSet UID to terminate. It refuses to activate
# the target while spec.replicas is anything other than 0, or while any source
# Pod remains. A Complete phase from the pinned official digest is therefore
# itself proof that the whole source generation, emptyDir state included, was
# destroyed by the official recovery path rather than by this harness.
#
# What this does NOT establish: an external production fence. A single-namespace
# harness that owns the only workload and the only object-store writer has no
# residual old voter, client, or archive writer by construction, so the run is
# explicitly fixture-scoped. A production run needs credential revocation,
# storage quiescing, and an attested external fence.
#
# The fence attestation binds this operation, the source cluster, and the live
# StatefulSet UID. It is a trusted administrator claim about an external
# authority, and the operator deliberately never infers it from Pod
# disappearance.
recovery_request_epoch=$(date -u +%s)
recovery_manifest="$output_dir/rhiza-kv-recovery.yaml"
cat >"$recovery_manifest" <<EOF
apiVersion: rhiza.mrchypark.dev/v1alpha1
kind: RhizaRecovery
metadata:
  name: ${recovery_resource_name}
  namespace: ${namespace}
spec:
  statefulSet: ${service_name}
  container: velorix-meta
  sourceClusterID: ${run_id}
  durability: ${object_store_durability}
  # A unique explicit ID. This resource is the resumable operation journal:
  # after a successor is reserved, do not edit its source, mode, or ID, and do
  # not delete it.
  recoveryID: ${recovery_id}
  # The source is before-ack, so the operator requires a certified archive
  # rather than data-loss consent. allowDataLoss stays false.
  allowDataLoss: false
  fence:
    recoveryID: ${recovery_id}
    clusterID: ${run_id}
    statefulSetUID: ${source_statefulset_uid}
    confirmed: true
    evidence: >-
      Isolated validation namespace ${namespace} run ${run_id}. The harness owns
      the only Rhiza workload and the only object-store writer in this namespace,
      so no source voter, client, or archive writer from this generation retains
      residual authority. The official operator then stops the source StatefulSet
      to zero replicas, requires every source Pod to terminate, and refuses to
      activate the target until that holds. This is a fixture-scoped attestation
      of harness ownership; it is not a production infrastructure fence.
EOF
chmod 600 "$recovery_manifest"
if ! kubectl --context "$context" apply -f "$recovery_manifest" >"$output_dir/recovery-apply.out" 2>"$output_dir/recovery-apply.error"; then
  die "could not apply the whole-generation recovery request"
fi

recovery_deadline=$(( $(date +%s) + 900 ))
recovery_phase=
recovery_target=
while [ "$(date +%s)" -lt "$recovery_deadline" ]; do
  recovery_status_json=$(kubectl --context "$context" -n "$namespace" get rhizarecovery "$recovery_resource_name" -o json 2>/dev/null) || recovery_status_json=
  if [ -n "$recovery_status_json" ]; then
    printf '%s' "$recovery_status_json" >"$output_dir/recovery-status.json"
    recovery_phase=$(printf '%s' "$recovery_status_json" | jq -r '.status.phase // ""')
    recovery_target=$(printf '%s' "$recovery_status_json" | jq -r '.status.target // ""')
  fi
  case "$recovery_phase" in
    Complete) break ;;
    Blocked|AwaitingFence) break ;;
  esac
  sleep 10
done
printf '%s' "${recovery_status_json:-}" >"$output_dir/recovery-status.json"
if [ "$recovery_phase" != Complete ]; then
  kubectl --context "$context" -n "$namespace" logs deployment/rhiza-operator --tail=200 \
    >"$output_dir/operator.log" 2>"$output_dir/operator-log.error" || true
  die "the official operator did not complete generation recovery (phase '${recovery_phase:-none}'); see $output_dir/recovery-status.json and $output_dir/operator.log"
fi

# The whole point of the operator path: a new generation with new identity, not
# the same cluster ID restarted against a blank disk.
[ -n "$recovery_target" ] || die "the operator reported Complete without a target cluster ID"
[ "$recovery_target" != "$run_id" ] || die "the operator reported the source cluster ID as its target; this is not a generation replacement"

# The operator's own durable journal. Because the upstream controller refuses to
# activate a target while spec.replicas is not 0 or while any Pod owned by the
# source StatefulSet UID survives, a Complete stage is proof that the official
# operator performed the whole-generation teardown. These assertions make that
# reasoning auditable instead of implicit: the journal must show the same source
# StatefulSet, a certified archive captured from live source Pods, a non-zero
# recovered tip, and the operator's own completion message.
if ! jq -e \
    --arg source "$run_id" \
    --arg target "$recovery_target" \
    --arg sts_uid "$source_statefulset_uid" \
    --arg durability "$object_store_durability" \
    --arg recovery_id "$recovery_id" '
    (.status.stage == "Complete") and
    (.status.source == $source) and
    (.status.target == $target) and
    (.status.statefulSetUID == $sts_uid) and
    (.status.sourceDurability == $durability) and
    (.status.recoveryID == $recovery_id) and
    (.status.recoveredTip > 0) and
    ((.status.manifestHash // "") | length) > 0 and
    # An "unavailable: ..." prefix means the operator could not reach the source
    # Pod recovery endpoints, i.e. no certified suffix was captured.
    ((.status.archiveCapture // "") | startswith("certified suffix captured")) and
    (.status.message | contains("verified recovered quorum"))
  ' "$output_dir/recovery-status.json" >"$output_dir/recovery-journal-check.out" 2>"$output_dir/recovery-journal-check.error"; then
  die "the operator's recovery journal does not show a completed, identity-bound generation replacement"
fi

if ! kubectl --context "$context" -n "$namespace" get statefulset "$service_name" -o json \
  | jq -e --arg target "$recovery_target" --arg source "$run_id" \
    --arg source_peer_secret "$peer_credentials_secret_name" \
    --arg operator_managed VELORIX_RHIZA_OPERATOR_MANAGED '
      (.spec.replicas == 3) and
      (.spec.template.spec.containers[0].env as $env |
        ([$env[] | select(.name == "RHIZA_CLUSTER_ID")][0].value) == $target and
        ([$env[] | select(.name == "RHIZA_CLUSTER_ID")][0].value) != $source and
        # The operator clears any inherited scalar peer token by publishing a
        # name-only entry. EnvVar.Value is omitempty, so an explicit empty
        # override reads back as an absent `value` field. Accept either, but
        # require the entry to exist: its absence would leave a scalar token
        # alongside the token map, which fails the next process start.
        (([$env[] | select(.name == "RHIZA_PEER_TOKEN")] | length) == 1) and
        (([$env[] | select(.name == "RHIZA_PEER_TOKEN")][0].value) // "") == "" and
        ([$env[] | select(.name == "RHIZA_PEER_TOKENS")][0].valueFrom.secretKeyRef.name) != $source_peer_secret and
        ([$env[] | select(.name == "VELORIX_RHIZA_OPERATOR_MANAGED")][0].value) == "1" and
        ([$env[] | select(.name | startswith("VELORIX_RHIZA_")) | select(.name != "VELORIX_RHIZA_OPERATOR_MANAGED")] | length == 0)
      )
    ' >"$output_dir/generation-check.out" 2>"$output_dir/generation-check.error"; then
  die "the recovered generation did not adopt the operator-published target identity in the canonical RHIZA_* environment"
fi

# The operator's own credential Secret must exist and be immutable, and must not
# be the source generation's credentials.
recovery_secret_name=$(jq -r '.status.secretName // ""' "$output_dir/recovery-status.json")
[ -n "$recovery_secret_name" ] || die "the operator did not report a target credential Secret"
[ "$recovery_secret_name" != "$peer_credentials_secret_name" ] \
  || die "the operator reused the source generation's peer credentials"
if ! kubectl --context "$context" -n "$namespace" get secret "$recovery_secret_name" -o json \
  | jq -e '(.immutable == true) and (((.data // {}) | keys | sort) == ["admin", "members", "peer_tokens"])' \
  >"$output_dir/recovery-secret-check.out" 2>"$output_dir/recovery-secret-check.error"; then
  die "the operator's target credential Secret is missing, mutable, or not the expected native shape"
fi

after_uids=$(kubectl --context "$context" -n "$namespace" get pods -l "app=${app_label},${validation_label}" -o json 2>"$output_dir/after-pods.error" | tee "$output_dir/after-pods.json" | jq -r '[.items[].metadata.uid] | sort | join(",")')
[ "$(printf '%s' "$after_uids" | awk -F, '{print NF}')" = 3 ] || die "the recovered generation did not produce three Pods"
[ "$before_uids" != "$after_uids" ] || die "generation recovery did not create new Pod identities"

# New Pod identities alone would also be produced by an ordinary restart. Tie the
# replacement to the source StatefulSet object and to the moment recovery was
# requested, so this is a generation replacement performed after the fence, not
# a rolling restart that happened to coincide with it.
if ! jq -e --arg sts_uid "$source_statefulset_uid" --argjson requested "$recovery_request_epoch" '
    (.items | length) == 3 and
    (all(.items[];
      (([.metadata.ownerReferences[]? | select(.uid == $sts_uid)] | length) == 1) and
      (((.metadata.creationTimestamp | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601)) > $requested)
    ))
  ' "$output_dir/after-pods.json" >"$output_dir/recovered-pod-provenance-check.out" 2>"$output_dir/recovered-pod-provenance-check.error"; then
  die "the recovered Pods are not newly created members of the source StatefulSet after the recovery request"
fi

# Each new voter must report the recovered generation, not the source.
if ! jq -e --arg target "$recovery_target" '
  ([.status.peers[]? | select(.cluster_id == $target and .ready == true and .quorum == true)] | length) >= 2
' "$output_dir/recovery-status.json" >"$output_dir/recovery-quorum-check.out" 2>&1; then
  die "the recovered generation did not reach quorum on two or more voters"
fi

# Restored pre-loss state, read-only, against the exact catalog written before
# the Pod loss. This is the substantive recovery assertion: the new generation
# serves the old generation's metadata rather than an empty database.
run_smoke_job after-recovery 1
for node_ordinal in 0 1 2; do
  run_smoke_job "recovered-node-${node_ordinal}" 1 "https://${service_name}-${node_ordinal}.${service_name}.${namespace}.svc.cluster.local:9090"
done

# The recovered generation must accept new writes; a read-only artifact proves
# nothing about whether it is a live voter set. The probe id carries its own
# suffix, because reusing the pre-loss id would report Duplicate and would show
# only that the recovered generation kept an old record, not that it stored
# anything. A new catalog must report Created.
run_smoke_job after-recovery-write 0 "" "${probe_id}-post-recovery-write"
after_recovery_write_outcome=$(sed -n 's/.*catalog_store_outcome=\([A-Za-z]*\).*/\1/p' "$output_dir/after-recovery-write-smoke.log" | head -n 1)
printf '%s\n' "$after_recovery_write_outcome" >"$output_dir/after-recovery-write-catalog-outcome.out"
[ "$after_recovery_write_outcome" = "Created" ] \
  || die "the recovered generation reported catalog_store_outcome=${after_recovery_write_outcome:-none} instead of Created for a new catalog write"

jq -n \
  --arg status pass \
  --arg evidence_scope rhiza_kv_official_operator_no_pvc_generation_recovery \
  --arg provider "$object_store_provider" \
  --arg durability "$object_store_durability" \
  --arg operator_image "$operator_image" \
  --argjson operator_image_official "$operator_image_official" \
  --argjson operator_image_override_used "$operator_image_override_used" \
  --arg operator_image_build_provenance "$operator_image_provenance" \
  --arg image_digest "${meta_image#*@}" \
  --arg recovery_id "$recovery_id" \
  --arg source_cluster "$run_id" \
  --arg target_cluster "$recovery_target" \
  --arg recovery_secret "$recovery_secret_name" \
  --arg post_recovery_write_outcome "$after_recovery_write_outcome" \
  --arg fence_scope "isolated-fixture-harness-ownership" '
  {
    schema_version: 2,
    status: $status,
    evidence_scope: $evidence_scope,
    member_count: 3,
    statefulset_replicas: 3,
    no_pvc: true,
    empty_dir_node_disk: true,
    update_strategy: "OnDelete",
    publish_not_ready_addresses: true,
    service_connection: true,
    recovery_performed_by: "official-rhiza-operator",
    custom_recovery_orchestration: false,
    operator_manifests_sha256_verified: true,
    operator_image: $operator_image,
    operator_image_digest_pinned: true,
    # A locally built operator from the pinned upstream source tree is a real
    # binary, but it is not the published release artifact. Both facts are
    # reported, so a consumer never has to guess which one ran.
    operator_image_official_published_release: $operator_image_official,
    operator_image_override_used: $operator_image_override_used,
    operator_image_build_provenance: $operator_image_build_provenance,
    observation_only_phase_confirmed: true,
    source_generation_quorum_before_recovery: true,
    # The harness does not create the loss. The official operator seals the
    # archive, stops spec.replicas to 0, and requires every source Pod to
    # terminate before it will activate the target, so a Complete phase is
    # proof of a whole-generation teardown by the operator itself.
    whole_generation_loss_performed_by_operator: true,
    operator_journal_stage_complete: true,
    operator_refused_source_replicas_nonzero: true,
    operator_refused_surviving_source_pods: true,
    certified_archive_captured_before_teardown: true,
    recovered_tip_positive: true,
    source_generation_statefulset_uid_preserved: true,
    recovered_pods_created_after_recovery_request: true,
    recovery_id: $recovery_id,
    source_cluster_id: $source_cluster,
    target_cluster_id: $target_cluster,
    generation_identity_rotated: true,
    recovery_phase_complete: true,
    target_credential_secret: $recovery_secret,
    target_credentials_immutable: true,
    target_credentials_distinct_from_source: true,
    recovered_generation_quorum: true,
    pod_identity_changed: true,
    pre_loss_catalog_restored_read_only: true,
    each_recovered_pod_read_only_verification: true,
    recovered_generation_accepts_writes: true,
    recovered_generation_new_catalog_store_outcome: $post_recovery_write_outcome,
    recovery_listener_named_port: 9091,
    recovery_listener_exposed_by_service: false,
    member_tokens_absent: true,
    public_keys_derived_and_matched: true,
    public_key_encoding: "ed25519-standard-padded-base64-32-bytes",
    peer_token_delivery: "native-shared-peer-token-map",
    private_peer_token_in_pod_spec: false,
    velorix_rhiza_shadow_variables_present: false,
    object_store_provider: $provider,
    object_store_durability: $durability,
    image_digest: $image_digest,
    fence_scope: $fence_scope,
    production_fence_attested: false,
    trusted_for_production: false
  }
' >"$output_dir/rhiza-kv-gate-evidence.json"
chmod 600 "$output_dir/rhiza-kv-gate-evidence.json"
echo "rhiza KV Kubernetes gate passed: the official operator replaced the lost no-PVC generation and the pre-loss metadata was restored"
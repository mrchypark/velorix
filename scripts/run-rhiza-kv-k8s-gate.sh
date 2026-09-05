#!/bin/sh
set -eu

# This is an explicitly opt-in validation harness. It is intentionally separate
# from the product runner: a Rhiza KV pod owns its own emptyDir WAL while the
# configured object store is the shared recovery authority.

CDPATH=
export CDPATH
repo_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
output_dir=${VELORIX_RHIZA_EVIDENCE_DIR:-"$repo_root/target/rhiza-kv-k8s"}
context=${VELORIX_K8S_CONTEXT:-}
namespace=${VELORIX_RHIZA_NAMESPACE:-velorix-rhiza-validation}
meta_image=${VELORIX_RHIZA_META_IMAGE:-}
image_pull_secret=${VELORIX_RHIZA_IMAGE_PULL_SECRET:-}
members_json=${VELORIX_RHIZA_MEMBERS_JSON:-}
object_store_provider=${VELORIX_RHIZA_OBJECT_STORE_PROVIDER:-}
object_store_endpoint=${VELORIX_RHIZA_OBJECT_STORE_ENDPOINT:-}
object_store_bucket=${VELORIX_RHIZA_OBJECT_STORE_BUCKET:-}
object_store_region=${VELORIX_RHIZA_OBJECT_STORE_REGION:-}
object_store_prefix=${VELORIX_RHIZA_OBJECT_STORE_PREFIX:-rhiza-validation}
object_store_access_key=${VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY:-}
object_store_secret_key=${VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY:-}
object_store_session_token=${VELORIX_RHIZA_OBJECT_STORE_SESSION_TOKEN:-}
object_store_insecure=${VELORIX_RHIZA_OBJECT_STORE_INSECURE:-0}
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
execute=${VELORIX_RHIZA_EXECUTE:-0}
cleanup=${VELORIX_RHIZA_CLEANUP:-0}
run_id=${VELORIX_RHIZA_RUN_ID:-rhiza-kv-v-${run_nonce}}
service_name=velorix-meta

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
mkdir -p "$output_dir"
umask 077

require_nonempty VELORIX_K8S_CONTEXT "$context"
require_nonempty VELORIX_RHIZA_META_IMAGE "$meta_image"
require_nonempty VELORIX_RHIZA_MEMBERS_JSON "$members_json"
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

case "$namespace" in
  velorix-rhiza-validation|velorix-rhiza-validation-*) ;;
  *) die "VELORIX_RHIZA_NAMESPACE must use the isolated velorix-rhiza-validation prefix" ;;
esac
case "$probe_id" in
  *[!A-Za-z0-9._-]*|'') die "VELORIX_RHIZA_PROBE_ID must be a nonempty DNS-safe probe id" ;;
esac
case "$run_id" in
  *[!a-z0-9.-]*|''|[-.]*|*[-.]) die "VELORIX_RHIZA_RUN_ID must be a lowercase DNS-safe value" ;;
esac
[ "${#run_id}" -le 39 ] || die "VELORIX_RHIZA_RUN_ID is too long for generated Kubernetes Job names"
case "$execute:$cleanup" in
  0:0|0:1|1:0|1:1) ;;
  *) die "VELORIX_RHIZA_EXECUTE and VELORIX_RHIZA_CLEANUP must be 0 or 1" ;;
esac
case "$object_store_insecure" in 0|1) ;; *) die "VELORIX_RHIZA_OBJECT_STORE_INSECURE must be 0 or 1" ;; esac
[ "$object_store_durability" = before-ack ] || die "Rhiza KV validation requires before-ack object-store durability"
[ "$object_store_provider" = s3 ] || die "Rhiza KV Kubernetes validation currently requires the s3 object-store provider"
case "$object_store_endpoint" in *://*) die "VELORIX_RHIZA_OBJECT_STORE_ENDPOINT must be the native host:port value without a URL scheme" ;; esac
for safe_value in "$object_store_endpoint" "$object_store_bucket" "$object_store_region" "$object_store_prefix" "$server_tls_secret" "$client_tls_secret" "$image_pull_secret"; do
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
printf '%s' "$members_json" | jq -e --arg dns_suffix "${service_name}.${namespace}.svc.cluster.local" '
  type == "array" and length == 3 and
  all(.[]; . as $member | type == "object" and
    ($member.node_id | type == "string" and length > 0) and
    ($member.url == ("https://" + $member.node_id + "." + $dns_suffix + ":9090")) and
    ($member.peer_url == ("quic://" + $member.node_id + "." + $dns_suffix + ":8200")) and
    ($member.token | type == "string" and length > 0)) and
  (map(.node_id) | sort == ["velorix-meta-0", "velorix-meta-1", "velorix-meta-2"])
' >/dev/null 2>&1 || die "VELORIX_RHIZA_MEMBERS_JSON must be a three-member array with quic peer URLs and nonempty voter tokens"

# Do not expose member URLs, tokens, object-store credentials, or the supplied
# context in terminal output or evidence. kubectl command output is redirected
# to private files throughout this script.
preflight_file="$output_dir/preflight.txt"
preflight_error="$output_dir/preflight.error"
if kubectl --context "$context" get namespace "$namespace" >"$preflight_file" 2>"$preflight_error"; then
  existing_namespace=1
  if kubectl --context "$context" -n "$namespace" get all,configmap,secret,serviceaccount,role,rolebinding,networkpolicy,persistentvolumeclaim,job -l "velorix.dev/rhiza-kv-validation=$run_id" -o name >"$preflight_file" 2>"$preflight_error"; then
    [ ! -s "$preflight_file" ] || die "an isolated validation with this run id already exists"
  else
    die "could not inspect the requested validation namespace"
  fi
  if kubectl --context "$context" -n "$namespace" get secret rhiza-kv-validation-secrets >"$preflight_file" 2>"$preflight_error"; then
    die "the isolated validation Secret name is already present"
  fi
  for fixed_resource in "service/${service_name}" "statefulset/${service_name}"; do
    if kubectl --context "$context" -n "$namespace" get "$fixed_resource" >"$preflight_file" 2>"$preflight_error"; then
      die "the isolated namespace already contains the fixed Rhiza validation resource"
    fi
  done
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
  jq -n --arg status preflight_pass --arg evidence_scope rhiza_kv_no_pvc_recovery_validation \
    '{schema_version: 1, status: $status, evidence_scope: $evidence_scope, execution_required: true, context_configured: true, namespace_isolated: true, member_count: 3, no_cluster_mutation: true}' \
    >"$output_dir/rhiza-kv-gate-evidence.json"
  chmod 600 "$output_dir/rhiza-kv-gate-evidence.json" "$preflight_file" "$preflight_error"
  echo "rhiza KV Kubernetes gate preflight passed; set VELORIX_RHIZA_EXECUTE=1 for the isolated deployment"
  exit 0
fi

manifest="$output_dir/rhiza-kv-workload.yaml"
secret_name="rhiza-kv-validation-secrets"
members_mount="/etc/velorix/rhiza"
server_tls_mount="/etc/velorix/tls/server"
client_tls_mount="/etc/velorix/tls/client"
app_label="rhiza-kv-validation"
created_namespace=0
secret_applied=0
server_tls_created=0
client_tls_created=0
secret_tmp_dir=
image_pull_secret_yaml=""
if [ -n "$image_pull_secret" ]; then
  image_pull_secret_yaml=$(printf '      imagePullSecrets:\n        - name: %s' "$image_pull_secret")
fi

cleanup_resources() {
  [ "$cleanup" = 1 ] || return 0
  kubectl --context "$context" -n "$namespace" delete job -l "velorix.dev/rhiza-kv-validation=$run_id" --ignore-not-found >"$output_dir/cleanup.out" 2>"$output_dir/cleanup.error" || true
  kubectl --context "$context" -n "$namespace" delete statefulset,service,secret -l "velorix.dev/rhiza-kv-validation=$run_id" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  if [ "$secret_applied" = 1 ]; then
    kubectl --context "$context" -n "$namespace" delete secret "$secret_name" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  fi
  if [ "$server_tls_created" = 1 ]; then
    kubectl --context "$context" -n "$namespace" delete secret "$server_tls_secret" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  fi
  if [ "$client_tls_created" = 1 ]; then
    kubectl --context "$context" -n "$namespace" delete secret "$client_tls_secret" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  fi
  if [ "$created_namespace" = 1 ]; then
    kubectl --context "$context" delete namespace "$namespace" --ignore-not-found >>"$output_dir/cleanup.out" 2>>"$output_dir/cleanup.error" || true
  fi
}
cleanup_private_files() {
  if [ -n "$secret_tmp_dir" ] && [ -d "$secret_tmp_dir" ]; then
    rm -rf "$secret_tmp_dir"
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
printf '%s' "$members_json" >"$secret_tmp_dir/members"
printf '%s' "$meta_bearer_token" >"$secret_tmp_dir/meta-bearer-token"
printf '%s' "$rhiza_admin_token" >"$secret_tmp_dir/rhiza-admin-token"
printf '%s' "$object_store_access_key" >"$secret_tmp_dir/object-store-access-key"
printf '%s' "$object_store_secret_key" >"$secret_tmp_dir/object-store-secret-key"
printf '%s' "$object_store_session_token" >"$secret_tmp_dir/object-store-session-token"
chmod 600 "$secret_tmp_dir"/*

create_tls_secret_if_needed() {
  tls_secret=$1
  cert_file=$2
  key_file=$3
  ca_file=$4
  created_flag=$5
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
  case "$created_flag" in
    server) server_tls_created=1 ;;
    client) client_tls_created=1 ;;
  esac
}

if [ "$server_tls_files:$client_tls_files" = 3:3 ]; then
  create_tls_secret_if_needed "$server_tls_secret" "$server_tls_cert_file" "$server_tls_key_file" "$server_tls_client_ca_file" server
  create_tls_secret_if_needed "$client_tls_secret" "$client_tls_cert_file" "$client_tls_key_file" "$client_tls_ca_file" client
fi

# Keep credentials in a short-lived Kubernetes Secret. No credential is placed
# in the workload manifest or in the evidence bundle.
validation_secret_yaml="$secret_tmp_dir/validation-secret.yaml"
if ! kubectl --context "$context" -n "$namespace" create secret generic "$secret_name" \
  --from-file=members="$secret_tmp_dir/members" \
  --from-file=meta-bearer-token="$secret_tmp_dir/meta-bearer-token" \
  --from-file=rhiza-admin-token="$secret_tmp_dir/rhiza-admin-token" \
  --from-file=object-store-access-key="$secret_tmp_dir/object-store-access-key" \
  --from-file=object-store-secret-key="$secret_tmp_dir/object-store-secret-key" \
  --from-file=object-store-session-token="$secret_tmp_dir/object-store-session-token" \
  --dry-run=client -o yaml >"$validation_secret_yaml" 2>"$secret_tmp_dir/validation-secret.error"; then
  die "could not render validation Secret"
fi
chmod 600 "$validation_secret_yaml" "$secret_tmp_dir/validation-secret.error"
if ! kubectl --context "$context" apply -f "$validation_secret_yaml" >"$output_dir/secret-apply.out" 2>"$output_dir/secret-apply.error"; then
  die "could not apply validation Secret"
fi
rm -f "$validation_secret_yaml"
secret_applied=1
if ! kubectl --context "$context" -n "$namespace" label secret "$secret_name" \
  "velorix.dev/rhiza-kv-validation=${run_id}" --overwrite >"$output_dir/secret-label.out" 2>"$output_dir/secret-label.error"; then
  die "could not label validation Secret"
fi

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
            - name: peer
              containerPort: 8200
              protocol: UDP
          command: ["/usr/local/bin/velorix-meta"]
          env:
            - name: VELORIX_META_MODE
              value: production
            - name: VELORIX_META_BIND
              value: 0.0.0.0:9090
            - name: VELORIX_META_BACKEND
              value: rhiza-kv
            - name: VELORIX_META_BEARER_TOKEN
              valueFrom: {secretKeyRef: {name: ${secret_name}, key: meta-bearer-token}}
            - name: VELORIX_META_TRANSPORT_SECURITY
              value: native-mtls
            - name: VELORIX_META_TLS_CERT_FILE
              value: ${server_tls_mount}/tls.crt
            - name: VELORIX_META_TLS_KEY_FILE
              value: ${server_tls_mount}/tls.key
            - name: VELORIX_META_TLS_CLIENT_CA_FILE
              value: ${server_tls_mount}/ca.crt
            - name: VELORIX_META_TLS_CA_FILE
              value: ${client_tls_mount}/ca.crt
            - name: VELORIX_META_TLS_CLIENT_CERT_FILE
              value: ${client_tls_mount}/tls.crt
            - name: VELORIX_META_TLS_CLIENT_KEY_FILE
              value: ${client_tls_mount}/tls.key
            - name: VELORIX_META_TLS_DOMAIN_NAME
              value: ${service_name}.${namespace}.svc.cluster.local
            - name: VELORIX_RHIZA_DATA_DIR
              value: /var/lib/velorix-meta
            - name: VELORIX_RHIZA_NODE_ID
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: VELORIX_RHIZA_CLUSTER_ID
              value: ${run_id}
            - name: VELORIX_RHIZA_BIND_ADDR
              value: 0.0.0.0:8100
            - name: VELORIX_RHIZA_PEER_ADDR
              # PeerAddr is the native bare bind address. The membership
              # document carries the externally advertised quic:// URLs.
              value: 0.0.0.0:8200
            - name: VELORIX_RHIZA_MEMBERS_FILE
              value: ${members_mount}/members.json
            - name: VELORIX_RHIZA_ADMIN_TOKEN
              valueFrom: {secretKeyRef: {name: ${secret_name}, key: rhiza-admin-token}}
            - name: VELORIX_RHIZA_OBJECT_STORE_PROVIDER
              value: ${object_store_provider}
            - name: VELORIX_RHIZA_OBJECT_STORE_ENDPOINT
              value: ${object_store_endpoint}
            - name: VELORIX_RHIZA_OBJECT_STORE_BUCKET
              value: ${object_store_bucket}
            - name: VELORIX_RHIZA_OBJECT_STORE_REGION
              value: ${object_store_region}
            - name: VELORIX_RHIZA_OBJECT_STORE_PREFIX
              value: ${object_store_prefix}/${run_id}
            - name: VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY
              valueFrom: {secretKeyRef: {name: ${secret_name}, key: object-store-access-key}}
            - name: VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY
              valueFrom: {secretKeyRef: {name: ${secret_name}, key: object-store-secret-key}}
            - name: VELORIX_RHIZA_OBJECT_STORE_SESSION_TOKEN
              valueFrom: {secretKeyRef: {name: ${secret_name}, key: object-store-session-token, optional: true}}
            - name: VELORIX_RHIZA_OBJECT_STORE_INSECURE
              value: "${object_store_insecure}"
            - name: VELORIX_RHIZA_OBJECT_STORE_DURABILITY
              value: ${object_store_durability}
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
            - name: members
              mountPath: ${members_mount}
              readOnly: true
            - name: server-tls
              mountPath: ${server_tls_mount}
              readOnly: true
            - name: client-tls
              mountPath: ${client_tls_mount}
              readOnly: true
      volumes:
        - name: data
          emptyDir: {}
        - name: members
          secret:
            secretName: ${secret_name}
            items:
              - key: members
                path: members.json
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
if ! kubectl --context "$context" -n "$namespace" rollout status statefulset/${service_name} --timeout=10m >"$output_dir/rollout.out" 2>"$output_dir/rollout.error"; then
  die "the three-node Rhiza StatefulSet did not become ready"
fi

if ! kubectl --context "$context" -n "$namespace" get statefulset "$service_name" -o json \
  | jq -e '(.spec.replicas == 3) and (.spec.template.spec.securityContext.fsGroup == 65532) and (.spec.template.spec.securityContext.runAsUser == 65532) and (.spec.template.spec.securityContext.runAsGroup == 65532) and (.spec.template.spec.securityContext.runAsNonRoot == true) and ((.spec.volumeClaimTemplates // []) | length == 0) and (([.spec.template.spec.volumes[]? | select(.persistentVolumeClaim != null)] | length) == 0)' \
  >"$output_dir/storage-check.out" 2>"$output_dir/storage-check.error"; then
  die "the Rhiza workload is not a three-replica emptyDir-only StatefulSet with the required non-root fsGroup"
fi
if ! kubectl --context "$context" -n "$namespace" get pvc -o name >"$output_dir/pvc-check.out" 2>"$output_dir/pvc-check.error"; then
  die "could not inspect PVCs in the isolated validation namespace"
fi
[ ! -s "$output_dir/pvc-check.out" ] || die "the Rhiza validation namespace contains a PVC"

# shellcheck disable=SC2016
if ! kubectl --context "$context" -n "$namespace" exec "${service_name}-0" -c velorix-meta -- \
  /bin/sh -ec 'probe=/var/lib/velorix-meta/.rhiza-kv-write-check; : >"$probe"; rm -f "$probe"' \
  >"$output_dir/emptydir-write-check.out" 2>"$output_dir/emptydir-write-check.error"; then
  die "the non-root Rhiza container cannot write its emptyDir data directory"
fi

before_uids=$(kubectl --context "$context" -n "$namespace" get pods -l "app=${app_label},velorix.dev/rhiza-kv-validation=${run_id}" -o json 2>"$output_dir/before-pods.error" | tee "$output_dir/before-pods.json" | jq -r '[.items[].metadata.uid] | sort | join(",")')
[ "$(printf '%s' "$before_uids" | awk -F, '{print NF}')" = 3 ] || die "the three-node Rhiza workload did not produce three Pods"

run_smoke_job() {
  phase=$1
  read_only=${2:-0}
  endpoint=${3:-https://${service_name}.${namespace}.svc.cluster.local:9090}
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
              --catalog-probe-id ${probe_id} --connect-retry-timeout-seconds 120${verify_only_arg}
          env:
            - name: META_BEARER_TOKEN
              valueFrom: {secretKeyRef: {name: ${secret_name}, key: meta-bearer-token}}
            - name: VELORIX_META_TLS_CA_FILE
              value: ${client_tls_mount}/ca.crt
            - name: VELORIX_META_TLS_CLIENT_CERT_FILE
              value: ${client_tls_mount}/tls.crt
            - name: VELORIX_META_TLS_CLIENT_KEY_FILE
              value: ${client_tls_mount}/tls.key
            - name: VELORIX_META_TLS_DOMAIN_NAME
              value: ${service_name}.${namespace}.svc.cluster.local
          volumeMounts:
            - name: client-tls
              mountPath: ${client_tls_mount}
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

# The first probe writes a unique catalog. The post-restart probe is strictly
# read-only; it must find the exact prior catalog in recovered metadata and may
# not recreate it if the emptyDir state was lost.
run_smoke_job before-restart 0
# Force every selected StatefulSet Pod through a completed scale-down before
# scaling back up. This prevents rollout status from succeeding while an old
# UID is still serving the recovery probe.
kubectl --context "$context" -n "$namespace" scale statefulset "$service_name" --replicas=0 >"$output_dir/pod-restart.out" 2>"$output_dir/pod-restart.error" || die "could not scale down the isolated StatefulSet"
kubectl --context "$context" -n "$namespace" wait --for=delete pod -l "app=${app_label},velorix.dev/rhiza-kv-validation=${run_id}" --timeout=10m >>"$output_dir/pod-restart.out" 2>>"$output_dir/pod-restart.error" || die "old isolated Rhiza Pods did not terminate"
kubectl --context "$context" -n "$namespace" scale statefulset "$service_name" --replicas=3 >>"$output_dir/pod-restart.out" 2>>"$output_dir/pod-restart.error" || die "could not scale up the isolated StatefulSet"
kubectl --context "$context" -n "$namespace" rollout status statefulset/${service_name} --timeout=10m >"$output_dir/recovery-rollout.out" 2>"$output_dir/recovery-rollout.error" || die "replacement Rhiza pods did not become ready"
after_uids=$(kubectl --context "$context" -n "$namespace" get pods -l "app=${app_label},velorix.dev/rhiza-kv-validation=${run_id}" -o json 2>"$output_dir/after-pods.error" | tee "$output_dir/after-pods.json" | jq -r '[.items[].metadata.uid] | sort | join(",")')
[ "$(printf '%s' "$after_uids" | awk -F, '{print NF}')" = 3 ] || die "the replacement Rhiza workload did not produce three Pods"
[ "$before_uids" != "$after_uids" ] || die "pod replacement did not create new Pod identities"
for node_ordinal in 0 1 2; do
  run_smoke_job "node-${node_ordinal}" 1 "https://${service_name}-${node_ordinal}.${service_name}.${namespace}.svc.cluster.local:9090"
done

jq -n \
  --arg status pass \
  --arg evidence_scope rhiza_kv_no_pvc_recovery_validation \
  --arg provider "$object_store_provider" \
  --arg image_digest "${meta_image#*@}" \
  '{schema_version: 1, status: $status, evidence_scope: $evidence_scope, member_count: 3, statefulset_replicas: 3, no_pvc: true, empty_dir_node_disk: true, publish_not_ready_addresses: true, service_connection: true, pre_restart_catalog_round_trip: true, pods_recreated: true, pod_identity_changed: true, post_restart_catalog_read_only_verification: true, post_restart_each_pod_read_only_verification: true, post_restart_catalog_round_trip: true, object_store_provider: $provider, object_store_durability: "before-ack", image_digest: $image_digest, trusted_for_production: false}' \
  >"$output_dir/rhiza-kv-gate-evidence.json"
chmod 600 "$output_dir/rhiza-kv-gate-evidence.json"
echo "rhiza KV Kubernetes gate passed: three-node service connection and no-PVC pod-replacement recovery verified"

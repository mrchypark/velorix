#!/bin/sh
# shellcheck disable=SC2129,SC2153
set -eu

# TEST-ONLY fallback for a cluster without an approved external S3 endpoint.
# This fixture proves Meta recovery against a separate ephemeral object store; it
# is not evidence of provider loss, production object-store durability, or a
# production cutover.
#
# The object store is the official Versity Gateway, on its POSIX driver with an
# emptyDir root. Versity is the gateway this repository already builds from
# source for `scripts/check-rhiza-recovery.sh`, so the Kubernetes fixture and
# the local recovery drill speak S3 to the same implementation rather than to
# two different emulations. The bucket is a directory created directly in that
# POSIX root, which is how the POSIX driver represents a bucket, so no separate
# provisioning client and no `mc` image are needed.
#
# It changes nothing about the recovery mechanism. The official Rhiza operator
# still performs the whole-generation recovery; this wrapper only supplies a
# throwaway object store and generates the source generation's peer identities.

CDPATH=
export CDPATH
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)

# shellcheck source=scripts/rhiza-kv-k8s-lib.sh
. "$script_dir/rhiza-kv-k8s-lib.sh"

context=${VELORIX_K8S_CONTEXT:-}
meta_image=${VELORIX_RHIZA_META_IMAGE:-}
operator_image_pin=${VELORIX_RHIZA_OPERATOR_IMAGE:-$rhiza_official_operator_image}
operator_image_override=${VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE:-}
operator_image_provenance=${VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE:-}
namespace=${VELORIX_RHIZA_NAMESPACE:-}
run_nonce=$(date -u +%Y%m%d-%H%M%S)-$$
run_id=${VELORIX_RHIZA_RUN_ID:-rhiza-kv-f-${run_nonce}}
probe_id=${VELORIX_RHIZA_PROBE_ID:-rhiza-kv-fixture-probe-${run_nonce}}
recovery_id=${VELORIX_RHIZA_RECOVERY_ID:-rhiza-kv-fixture-gen-${run_nonce}}
execute=${VELORIX_RHIZA_FIXTURE_EXECUTE:-0}
cleanup=${VELORIX_RHIZA_FIXTURE_CLEANUP:-0}
versity_image=${VELORIX_RHIZA_FIXTURE_VERSITY_IMAGE:-}
evidence_dir=${VELORIX_RHIZA_EVIDENCE_DIR:-"$repo_root/target/rhiza-kv-k8s-fixture"}
fixture_label=rhiza-kv-fixture
versity_service=versity
versity_port=7070
bucket="rhiza-${run_id}"

die() {
  echo "rhiza KV Kubernetes fixture: $*" >&2
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
mkdir -p "$evidence_dir"
umask 077

require_nonempty VELORIX_K8S_CONTEXT "$context"
require_nonempty VELORIX_RHIZA_META_IMAGE "$meta_image"
require_nonempty VELORIX_RHIZA_FIXTURE_VERSITY_IMAGE "$versity_image"
# The gate owns the operator image decision, including the digest pin and any
# explicit override. The fixture only requires that at least one of its inputs
# is set, and forwards all three unchanged.
if [ -z "$operator_image_pin" ] && [ -z "$operator_image_override" ]; then
  die "set VELORIX_RHIZA_OPERATOR_IMAGE, or VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE with VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE"
fi
case "$meta_image:$versity_image" in
  *@sha256:*:*@sha256:*) ;;
  *) die "Meta and Versity Gateway fixture images must be immutable sha256 references" ;;
esac
case "$execute:$cleanup" in
  0:0|0:1|1:0|1:1) ;;
  *) die "VELORIX_RHIZA_FIXTURE_EXECUTE and VELORIX_RHIZA_FIXTURE_CLEANUP must be 0 or 1" ;;
esac
case "$run_id" in
  *[!a-z0-9.-]*|''|[-.]*|*[-.]) die "VELORIX_RHIZA_RUN_ID must be a lowercase DNS-safe value" ;;
esac
[ "${#run_id}" -le 39 ] || die "VELORIX_RHIZA_RUN_ID is too long for generated Kubernetes names"
case "$probe_id" in
  *[!A-Za-z0-9._-]*|'') die "VELORIX_RHIZA_PROBE_ID must be a nonempty DNS-safe value" ;;
esac
case "$recovery_id" in
  *[!A-Za-z0-9._-]*|'') die "VELORIX_RHIZA_RECOVERY_ID must be a nonempty DNS-safe recovery id" ;;
esac
if [ -z "$namespace" ]; then
  namespace="velorix-rhiza-validation-fixture-${run_nonce}"
fi
case "$namespace" in
  velorix-rhiza-validation-fixture-*) ;;
  *) die "VELORIX_RHIZA_NAMESPACE must use the fixture validation prefix" ;;
esac
[ "${#namespace}" -le 63 ] || die "VELORIX_RHIZA_NAMESPACE is too long for Kubernetes"
for safe_value in "$namespace" "$run_id" "$probe_id" "$recovery_id" "$bucket"; do
  if printf '%s' "$safe_value" | LC_ALL=C grep -q '[[:cntrl:]]'; then
    die "fixture identifiers must not contain control characters"
  fi
done

# Do not create or alter anything when the namespace already exists. This also
# prevents a fixture from sharing an existing namespace's object store/data.
if kubectl --context "$context" get namespace "$namespace" >"$evidence_dir/namespace-check.out" 2>"$evidence_dir/namespace-check.error"; then
  die "fixture namespace already exists; choose a fresh namespace"
elif ! grep -qi 'not found' "$evidence_dir/namespace-check.error"; then
  die "could not inspect the requested fixture namespace"
fi

private_dir=$(mktemp -d "${TMPDIR:-/tmp}/velorix-rhiza-fixture.XXXXXX") || die "could not create a private fixture staging directory"
chmod 700 "$private_dir"
created_namespace=0

cleanup_fixture() {
  [ "$cleanup" = 1 ] || return 0
  if [ "$created_namespace" = 1 ]; then
    kubectl --context "$context" delete namespace "$namespace" --ignore-not-found >"$evidence_dir/fixture-cleanup.out" 2>"$evidence_dir/fixture-cleanup.error" || true
  fi
}
cleanup_private() {
  if [ -d "$private_dir" ]; then
    rm -rf "$private_dir"
  fi
}
trap 'cleanup_fixture; cleanup_private' EXIT HUP INT TERM

ca_key="$private_dir/ca.key"
ca_cert="$private_dir/ca.crt"
server_key="$private_dir/server.key"
server_csr="$private_dir/server.csr"
server_cert="$private_dir/server.crt"
client_key="$private_dir/client.key"
client_csr="$private_dir/client.csr"
client_cert="$private_dir/client.crt"
server_ext="$private_dir/server-ext.cnf"
client_ext="$private_dir/client-ext.cnf"

cat >"$server_ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:velorix-meta.${namespace}.svc.cluster.local,DNS:velorix-meta
EOF
cat >"$client_ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
chmod 600 "$server_ext" "$client_ext"

# Keep all OpenSSL diagnostics in the private staging directory.
run_openssl() {
  if ! "$@" >>"$private_dir/openssl.out" 2>>"$private_dir/openssl.error"; then
    die "could not generate fixture certificate material"
  fi
}
run_openssl openssl genrsa -out "$ca_key" 3072
run_openssl openssl req -x509 -new -nodes -key "$ca_key" -sha256 -days 2 \
  -subj "/CN=velorix-rhiza-fixture-${run_id}" -out "$ca_cert"
run_openssl openssl genrsa -out "$server_key" 2048
run_openssl openssl req -new -key "$server_key" -subj "/CN=velorix-meta" \
  -out "$server_csr"
run_openssl openssl x509 -req -in "$server_csr" -CA "$ca_cert" -CAkey "$ca_key" -CAcreateserial \
  -out "$server_cert" -days 2 -sha256 -extfile "$server_ext"
run_openssl openssl genrsa -out "$client_key" 2048
run_openssl openssl req -new -key "$client_key" -subj "/CN=rhiza-fixture-client" \
  -out "$client_csr"
run_openssl openssl x509 -req -in "$client_csr" -CA "$ca_cert" -CAkey "$ca_key" -CAcreateserial \
  -out "$client_cert" -days 2 -sha256 -extfile "$client_ext"
chmod 600 "$private_dir"/*

# Versity Gateway takes its root credentials from these two variables. Random
# per-run values only: the fixture is disposable and shares no credential with
# any other environment.
if ! openssl rand -hex 16 >"$private_dir/versity-access-key" 2>"$private_dir/rand-access.error"; then
  die "could not generate fixture credentials"
fi
if ! openssl rand -hex 32 >"$private_dir/versity-secret-key" 2>"$private_dir/rand.error"; then
  die "could not generate fixture credentials"
fi
versity_access_key=$(cat "$private_dir/versity-access-key")
versity_secret_key=$(cat "$private_dir/versity-secret-key")
printf '%s' "$bucket" >"$private_dir/bucket"
if ! openssl rand -hex 16 >"$private_dir/peer-token-seed" 2>>"$private_dir/rand.error"; then
  die "could not generate fixture peer tokens"
fi
peer_token_seed=$(cat "$private_dir/peer-token-seed")

# Generate the source generation's peer identities. Three random peer tokens,
# the derived public key for each node against this run's source cluster ID, a
# membership document carrying only those public keys, and the node-keyed
# peer-token map. Both are handed to the gate, so the fixture exercises the same
# preflight derivation check as an operator-supplied document.
fixture_token_0="${peer_token_seed}-0"
fixture_token_1="${peer_token_seed}-1"
fixture_token_2="${peer_token_seed}-2"
fixture_key_0=$(rhiza_derive_public_key "$private_dir" "$fixture_token_0" "$run_id" velorix-meta-0) || die "could not derive the velorix-meta-0 fixture peer public key"
fixture_key_1=$(rhiza_derive_public_key "$private_dir" "$fixture_token_1" "$run_id" velorix-meta-1) || die "could not derive the velorix-meta-1 fixture peer public key"
fixture_key_2=$(rhiza_derive_public_key "$private_dir" "$fixture_token_2" "$run_id" velorix-meta-2) || die "could not derive the velorix-meta-2 fixture peer public key"
# The membership document carries public material only. Private peer tokens are
# handed to the gate separately as a node-keyed object and reach the workload
# only through the standard native RHIZA_PEER_TOKENS map.
members_json=$(jq -cn --arg ns "$namespace" --arg k0 "$fixture_key_0" --arg k1 "$fixture_key_1" --arg k2 "$fixture_key_2" '
  [range(0;3) as $i | {
    node_id: ("velorix-meta-" + ($i|tostring)),
    url: ("https://velorix-meta-" + ($i|tostring) + ".velorix-meta." + $ns + ".svc.cluster.local:9090"),
    peer_url: ("quic://velorix-meta-" + ($i|tostring) + ".velorix-meta." + $ns + ".svc.cluster.local:8200"),
    public_key: ([$k0, $k1, $k2][$i])
  }]
')
peer_tokens_json=$(jq -cn --arg t0 "$fixture_token_0" --arg t1 "$fixture_token_1" --arg t2 "$fixture_token_2" '{"velorix-meta-0": $t0, "velorix-meta-1": $t1, "velorix-meta-2": $t2}')

server_tls_secret="rhiza-${run_id}-server-tls"
client_tls_secret="rhiza-${run_id}-client-tls"
fixture_secret="rhiza-${run_id}-versity"
server_tls_yaml="$private_dir/server-tls.yaml"
client_tls_yaml="$private_dir/client-tls.yaml"
fixture_secret_yaml="$private_dir/fixture-secret.yaml"
versity_endpoint="${versity_service}.${namespace}.svc.cluster.local:${versity_port}"

# Every input the gate needs, including the three operator-image inputs, is
# passed through unchanged. The gate remains the only place that decides which
# operator image runs, so the fixture cannot silently diverge from it.
run_gate() {
  VELORIX_K8S_CONTEXT="$context" \
  VELORIX_RHIZA_NAMESPACE="$namespace" \
  VELORIX_RHIZA_RUN_ID="$run_id" \
  VELORIX_RHIZA_PROBE_ID="$probe_id" \
  VELORIX_RHIZA_RECOVERY_ID="$recovery_id" \
  VELORIX_RHIZA_META_IMAGE="$meta_image" \
  VELORIX_RHIZA_OPERATOR_IMAGE="$operator_image_pin" \
  VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE="$operator_image_override" \
  VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE="$operator_image_provenance" \
  VELORIX_RHIZA_MEMBERS_JSON="$members_json" \
  VELORIX_RHIZA_PEER_TOKENS="$peer_tokens_json" \
  VELORIX_RHIZA_OBJECT_STORE_PROVIDER=s3 \
  VELORIX_RHIZA_OBJECT_STORE_ENDPOINT="$versity_endpoint" \
  VELORIX_RHIZA_OBJECT_STORE_BUCKET="$bucket" \
  VELORIX_RHIZA_OBJECT_STORE_REGION=us-east-1 \
  VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY="$versity_access_key" \
  VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY="$versity_secret_key" \
  VELORIX_RHIZA_OBJECT_STORE_INSECURE=true \
  VELORIX_RHIZA_META_BEARER_TOKEN="fixture-meta-${run_id}" \
  VELORIX_RHIZA_ADMIN_TOKEN="fixture-admin-${run_id}" \
  VELORIX_RHIZA_SERVER_TLS_SECRET="$server_tls_secret" \
  VELORIX_RHIZA_CLIENT_TLS_SECRET="$client_tls_secret" \
  VELORIX_RHIZA_SERVER_TLS_CERT_FILE="$server_cert" \
  VELORIX_RHIZA_SERVER_TLS_KEY_FILE="$server_key" \
  VELORIX_RHIZA_SERVER_TLS_CLIENT_CA_FILE="$ca_cert" \
  VELORIX_RHIZA_CLIENT_TLS_CERT_FILE="$client_cert" \
  VELORIX_RHIZA_CLIENT_TLS_KEY_FILE="$client_key" \
  VELORIX_RHIZA_CLIENT_TLS_CA_FILE="$ca_cert" \
  VELORIX_RHIZA_EVIDENCE_DIR="$evidence_dir" \
  VELORIX_RHIZA_EXECUTE="$1" \
  VELORIX_RHIZA_CLEANUP="$cleanup" \
  "$script_dir/run-rhiza-kv-k8s-gate.sh"
}

if [ "$execute" = 0 ]; then
  # Let the generic gate perform its normal read-only contract checks with the
  # generated fixture inputs, but do not create a namespace or cluster object.
  run_gate 0
  jq -n --arg run_id "$run_id" --arg object_store_fixture versitygw_posix_emptydir \
    '{schema_version: 2, status: "fixture_preflight_pass", fixture_only: true, run_id: $run_id, recovery_performed_by: "official-rhiza-operator", object_store_fixture: $object_store_fixture, no_cluster_mutation: true, production_durability_evidence: false}' >"$evidence_dir/fixture-evidence.json"
  chmod 600 "$evidence_dir/fixture-evidence.json"
  echo "rhiza KV Kubernetes fixture preflight passed; set VELORIX_RHIZA_FIXTURE_EXECUTE=1 for the test-only fixture"
  exit 0
fi

kubectl --context "$context" create namespace "$namespace" >"$evidence_dir/namespace-create.out" 2>"$evidence_dir/namespace-create.error"
created_namespace=1

if ! kubectl --context "$context" -n "$namespace" create secret generic "$server_tls_secret" \
  --from-file=tls.crt="$server_cert" --from-file=tls.key="$server_key" --from-file=ca.crt="$ca_cert" \
  --dry-run=client -o yaml >"$server_tls_yaml" 2>"$private_dir/server-tls.error"; then
  die "could not render fixture server TLS Secret"
fi
if ! kubectl --context "$context" apply -f "$server_tls_yaml" >"$evidence_dir/server-tls-apply.out" 2>"$evidence_dir/server-tls-apply.error"; then
  die "could not apply fixture server TLS Secret"
fi
if ! kubectl --context "$context" -n "$namespace" create secret generic "$client_tls_secret" \
  --from-file=tls.crt="$client_cert" --from-file=tls.key="$client_key" --from-file=ca.crt="$ca_cert" \
  --dry-run=client -o yaml >"$client_tls_yaml" 2>"$private_dir/client-tls.error"; then
  die "could not render fixture client TLS Secret"
fi
if ! kubectl --context "$context" apply -f "$client_tls_yaml" >"$evidence_dir/client-tls-apply.out" 2>"$evidence_dir/client-tls-apply.error"; then
  die "could not apply fixture client TLS Secret"
fi

printf '%s' "$versity_access_key" >"$private_dir/access-key"
printf '%s' "$versity_secret_key" >"$private_dir/secret-key"
if ! kubectl --context "$context" -n "$namespace" create secret generic "$fixture_secret" \
  --from-file=access-key="$private_dir/access-key" --from-file=secret-key="$private_dir/secret-key" --from-file=bucket="$private_dir/bucket" \
  --dry-run=client -o yaml >"$fixture_secret_yaml" 2>"$private_dir/fixture-secret.error"; then
  die "could not render fixture object-store Secret"
fi
if ! kubectl --context "$context" apply -f "$fixture_secret_yaml" >"$evidence_dir/fixture-secret-apply.out" 2>"$evidence_dir/fixture-secret-apply.error"; then
  die "could not apply fixture object-store Secret"
fi

# The official Versity Gateway on its POSIX driver. The bucket is created as a
# directory in that root before the server starts, because a directory is how
# the POSIX driver represents a bucket; that is why this fixture needs no
# provisioning client and no separate bucket Job.
#
# The gateway runs as a non-root user with a RuntimeDefault seccomp profile and
# an emptyDir at /data; this Pod spec deliberately does not set
# readOnlyRootFilesystem. Its own credentials come from the fixture Secret, so
# no credential appears in this manifest.
fixture_manifest="$private_dir/fixture.yaml"
cat >"$fixture_manifest" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${versity_service}
  namespace: ${namespace}
  labels:
    velorix.dev/rhiza-kv-fixture: ${run_id}
spec:
  selector:
    app: ${fixture_label}
    velorix.dev/rhiza-kv-fixture: ${run_id}
  ports:
    - name: s3
      port: ${versity_port}
      targetPort: s3
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${versity_service}
  namespace: ${namespace}
  labels:
    app: ${fixture_label}
    velorix.dev/rhiza-kv-fixture: ${run_id}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${fixture_label}
      velorix.dev/rhiza-kv-fixture: ${run_id}
  template:
    metadata:
      labels:
        app: ${fixture_label}
        velorix.dev/rhiza-kv-fixture: ${run_id}
    spec:
      terminationGracePeriodSeconds: 15
      securityContext:
        runAsUser: 65532
        runAsGroup: 65532
        runAsNonRoot: true
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: versity
          image: ${versity_image}
          imagePullPolicy: IfNotPresent
          command: ["/bin/sh", "-ec"]
          args:
            - >-
              mkdir -p "/data/\$VERSITY_BUCKET";
              exec /usr/local/bin/versitygw --port :${versity_port}
              --region us-east-1 posix /data
          env:
            - name: ROOT_ACCESS_KEY_ID
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: access-key}}
            - name: ROOT_SECRET_ACCESS_KEY
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: secret-key}}
            - name: VERSITY_BUCKET
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: bucket}}
          ports:
            - name: s3
              containerPort: ${versity_port}
          readinessProbe:
            # An unauthenticated list is rejected with 403, which still proves
            # the listener is serving; only a closed port fails this probe.
            exec:
              command: ["/bin/sh", "-ec", "wget -q -O /dev/null http://127.0.0.1:${versity_port}/ || [ \$? -eq 1 ]"]
            periodSeconds: 3
            timeoutSeconds: 5
            failureThreshold: 30
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
EOF
chmod 600 "$fixture_manifest"
if ! kubectl --context "$context" apply -f "$fixture_manifest" >"$evidence_dir/fixture-apply.out" 2>"$evidence_dir/fixture-apply.error"; then
  die "could not apply the test-only Versity Gateway fixture"
fi
if ! kubectl --context "$context" -n "$namespace" rollout status deployment/${versity_service} --timeout=5m >"$evidence_dir/versity-rollout.out" 2>"$evidence_dir/versity-rollout.error"; then
  die "the test-only Versity Gateway fixture did not become ready"
fi
if ! kubectl --context "$context" -n "$namespace" get pvc -o name >"$evidence_dir/pvc-check.out" 2>"$evidence_dir/pvc-check.error"; then
  die "could not inspect fixture PVCs"
fi
[ ! -s "$evidence_dir/pvc-check.out" ] || die "the test-only fixture namespace contains a PVC"

run_gate 1

jq -n --arg run_id "$run_id" --arg object_store_fixture versitygw_posix_emptydir \
  '{schema_version: 2, status: "fixture_pass", fixture_only: true, run_id: $run_id, recovery_performed_by: "official-rhiza-operator", object_store_fixture: $object_store_fixture, external_provider_failure_evidence: false, production_durability_evidence: false}' >"$evidence_dir/fixture-evidence.json"
chmod 600 "$evidence_dir/fixture-evidence.json"
echo "rhiza KV Kubernetes TEST-ONLY fixture passed: the official operator replaced the lost no-PVC generation against an ephemeral Versity Gateway"
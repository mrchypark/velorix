#!/bin/sh
# shellcheck disable=SC2129,SC2153
set -eu

# TEST-ONLY fallback for a cluster without an approved external S3 endpoint.
# This fixture proves Meta recovery against a separate ephemeral S3 service; it
# is not evidence of provider loss, production object-store durability, or a
# production cutover.

CDPATH=
export CDPATH
repo_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
context=${VELORIX_K8S_CONTEXT:-}
meta_image=${VELORIX_RHIZA_META_IMAGE:-}
namespace=${VELORIX_RHIZA_NAMESPACE:-}
run_nonce=$(date -u +%Y%m%d-%H%M%S)-$$
run_id=${VELORIX_RHIZA_RUN_ID:-rhiza-kv-f-${run_nonce}}
probe_id=${VELORIX_RHIZA_PROBE_ID:-rhiza-kv-fixture-probe-${run_nonce}}
execute=${VELORIX_RHIZA_FIXTURE_EXECUTE:-0}
cleanup=${VELORIX_RHIZA_FIXTURE_CLEANUP:-0}
minio_image=${VELORIX_RHIZA_FIXTURE_MINIO_IMAGE:-}
mc_image=${VELORIX_RHIZA_FIXTURE_MC_IMAGE:-}
evidence_dir=${VELORIX_RHIZA_EVIDENCE_DIR:-"$repo_root/target/rhiza-kv-k8s-fixture"}
fixture_label="rhiza-kv-fixture"
minio_service=minio
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
require_nonempty VELORIX_RHIZA_FIXTURE_MINIO_IMAGE "$minio_image"
require_nonempty VELORIX_RHIZA_FIXTURE_MC_IMAGE "$mc_image"
case "$meta_image:$minio_image:$mc_image" in
  *@sha256:*:*@sha256:*:*@sha256:*) ;;
  *) die "Meta, MinIO, and mc fixture images must be immutable sha256 references" ;;
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
if [ -z "$namespace" ]; then
  namespace="velorix-rhiza-validation-fixture-${run_nonce}"
fi
case "$namespace" in
  velorix-rhiza-validation-fixture-*) ;;
  *) die "VELORIX_RHIZA_NAMESPACE must use the fixture validation prefix" ;;
esac
[ "${#namespace}" -le 63 ] || die "VELORIX_RHIZA_NAMESPACE is too long for Kubernetes"
for safe_value in "$namespace" "$run_id" "$probe_id" "$bucket"; do
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

if ! openssl rand -hex 16 >"$private_dir/minio-access-key" 2>"$private_dir/rand-access.error"; then
  die "could not generate fixture credentials"
fi
if ! openssl rand -hex 32 >"$private_dir/minio-secret-key" 2>"$private_dir/rand.error"; then
  die "could not generate fixture credentials"
fi
minio_access_key=$(cat "$private_dir/minio-access-key")
minio_secret_key=$(cat "$private_dir/minio-secret-key")
printf '%s' "$bucket" >"$private_dir/bucket"
if ! openssl rand -hex 16 >"$private_dir/voter-seed" 2>>"$private_dir/rand.error"; then
  die "could not generate fixture voter tokens"
fi
voter_seed=$(cat "$private_dir/voter-seed")
members_json=$(jq -cn --arg ns "$namespace" --arg seed "$voter_seed" '[range(0;3) as $i | {node_id:("velorix-meta-"+($i|tostring)), url:("https://velorix-meta-"+($i|tostring)+".velorix-meta."+$ns+".svc.cluster.local:9090"), peer_url:("quic://velorix-meta-"+($i|tostring)+".velorix-meta."+$ns+".svc.cluster.local:8200"), token:($seed+"-"+($i|tostring))}]')

server_tls_secret="rhiza-${run_id}-server-tls"
client_tls_secret="rhiza-${run_id}-client-tls"
fixture_secret="rhiza-${run_id}-minio"
server_tls_yaml="$private_dir/server-tls.yaml"
client_tls_yaml="$private_dir/client-tls.yaml"
fixture_secret_yaml="$private_dir/fixture-secret.yaml"

if [ "$execute" = 0 ]; then
  # Let the generic gate perform its normal read-only contract checks with the
  # generated fixture inputs, but do not create a namespace or cluster object.
  VELORIX_K8S_CONTEXT="$context" \
  VELORIX_RHIZA_NAMESPACE="$namespace" \
  VELORIX_RHIZA_RUN_ID="$run_id" \
  VELORIX_RHIZA_PROBE_ID="$probe_id" \
  VELORIX_RHIZA_META_IMAGE="$meta_image" \
  VELORIX_RHIZA_MEMBERS_JSON="$members_json" \
  VELORIX_RHIZA_OBJECT_STORE_PROVIDER=s3 \
  VELORIX_RHIZA_OBJECT_STORE_ENDPOINT="${minio_service}.${namespace}.svc.cluster.local:9000" \
  VELORIX_RHIZA_OBJECT_STORE_BUCKET="$bucket" \
  VELORIX_RHIZA_OBJECT_STORE_REGION=us-east-1 \
  VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY="$minio_access_key" \
  VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY="$minio_secret_key" \
  VELORIX_RHIZA_OBJECT_STORE_INSECURE=1 \
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
  VELORIX_RHIZA_EXECUTE=0 \
  scripts/run-rhiza-kv-k8s-gate.sh
  jq -n --arg run_id "$run_id" '{schema_version: 1, status: "fixture_preflight_pass", fixture_only: true, run_id: $run_id, no_cluster_mutation: true, production_durability_evidence: false}' >"$evidence_dir/fixture-evidence.json"
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

printf '%s' "$minio_access_key" >"$private_dir/access-key"
printf '%s' "$minio_secret_key" >"$private_dir/secret-key"
if ! kubectl --context "$context" -n "$namespace" create secret generic "$fixture_secret" \
  --from-file=access-key="$private_dir/access-key" --from-file=secret-key="$private_dir/secret-key" --from-file=bucket="$private_dir/bucket" \
  --dry-run=client -o yaml >"$fixture_secret_yaml" 2>"$private_dir/fixture-secret.error"; then
  die "could not render fixture object-store Secret"
fi
if ! kubectl --context "$context" apply -f "$fixture_secret_yaml" >"$evidence_dir/fixture-secret-apply.out" 2>"$evidence_dir/fixture-secret-apply.error"; then
  die "could not apply fixture object-store Secret"
fi

fixture_manifest="$private_dir/fixture.yaml"
cat >"$fixture_manifest" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${minio_service}
  namespace: ${namespace}
  labels:
    velorix.dev/rhiza-kv-fixture: ${run_id}
spec:
  selector:
    app: ${fixture_label}
    velorix.dev/rhiza-kv-fixture: ${run_id}
  ports:
    - name: s3
      port: 9000
      targetPort: s3
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${minio_service}
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
      containers:
        - name: minio
          image: ${minio_image}
          args: ["server", "/data", "--address", ":9000", "--console-address", ":9001"]
          env:
            - name: MINIO_ROOT_USER
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: access-key}}
            - name: MINIO_ROOT_PASSWORD
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: secret-key}}
          ports:
            - name: s3
              containerPort: 9000
            - name: console
              containerPort: 9001
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: s3
            periodSeconds: 3
            timeoutSeconds: 5
            failureThreshold: 20
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-bucket-${run_id}
  namespace: ${namespace}
  labels:
    app: ${fixture_label}
    velorix.dev/rhiza-kv-fixture: ${run_id}
spec:
  backoffLimit: 3
  template:
    metadata:
      labels:
        app: ${fixture_label}-bucket
        velorix.dev/rhiza-kv-fixture: ${run_id}
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: ${mc_image}
          command: ["/bin/sh", "-ec"]
          args:
            - >-
              mc alias set fixture http://${minio_service}.${namespace}.svc.cluster.local:9000
              "\$MINIO_ACCESS_KEY" "\$MINIO_SECRET_KEY";
              mc mb --ignore-existing "fixture/\$MINIO_BUCKET"
          env:
            - name: MINIO_ACCESS_KEY
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: access-key}}
            - name: MINIO_SECRET_KEY
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: secret-key}}
            - name: MINIO_BUCKET
              valueFrom: {secretKeyRef: {name: ${fixture_secret}, key: bucket}}
EOF
chmod 600 "$fixture_manifest"
if ! kubectl --context "$context" apply -f "$fixture_manifest" >"$evidence_dir/fixture-apply.out" 2>"$evidence_dir/fixture-apply.error"; then
  die "could not apply the test-only MinIO fixture"
fi
if ! kubectl --context "$context" -n "$namespace" rollout status deployment/${minio_service} --timeout=5m >"$evidence_dir/minio-rollout.out" 2>"$evidence_dir/minio-rollout.error"; then
  die "the test-only MinIO fixture did not become ready"
fi
bucket_job="minio-bucket-${run_id}"
if ! kubectl --context "$context" -n "$namespace" wait --for=condition=complete "job/${bucket_job}" --timeout=5m >"$evidence_dir/bucket-job.out" 2>"$evidence_dir/bucket-job.error"; then
  die "the test-only MinIO bucket setup failed"
fi
if ! kubectl --context "$context" -n "$namespace" get pvc -o name >"$evidence_dir/pvc-check.out" 2>"$evidence_dir/pvc-check.error"; then
  die "could not inspect fixture PVCs"
fi
[ ! -s "$evidence_dir/pvc-check.out" ] || die "the test-only fixture namespace contains a PVC"

VELORIX_K8S_CONTEXT="$context" \
VELORIX_RHIZA_NAMESPACE="$namespace" \
VELORIX_RHIZA_RUN_ID="$run_id" \
VELORIX_RHIZA_PROBE_ID="$probe_id" \
VELORIX_RHIZA_META_IMAGE="$meta_image" \
VELORIX_RHIZA_MEMBERS_JSON="$members_json" \
VELORIX_RHIZA_OBJECT_STORE_PROVIDER=s3 \
VELORIX_RHIZA_OBJECT_STORE_ENDPOINT="${minio_service}.${namespace}.svc.cluster.local:9000" \
VELORIX_RHIZA_OBJECT_STORE_BUCKET="$bucket" \
VELORIX_RHIZA_OBJECT_STORE_REGION=us-east-1 \
VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY="$minio_access_key" \
VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY="$minio_secret_key" \
VELORIX_RHIZA_OBJECT_STORE_INSECURE=1 \
VELORIX_RHIZA_META_BEARER_TOKEN="fixture-meta-${run_id}" \
VELORIX_RHIZA_ADMIN_TOKEN="fixture-admin-${run_id}" \
VELORIX_RHIZA_SERVER_TLS_SECRET="$server_tls_secret" \
VELORIX_RHIZA_CLIENT_TLS_SECRET="$client_tls_secret" \
VELORIX_RHIZA_EVIDENCE_DIR="$evidence_dir" \
VELORIX_RHIZA_EXECUTE=1 \
VELORIX_RHIZA_CLEANUP="$cleanup" \
scripts/run-rhiza-kv-k8s-gate.sh

jq -n --arg run_id "$run_id" '{schema_version: 1, status: "fixture_pass", fixture_only: true, run_id: $run_id, external_provider_failure_evidence: false, production_durability_evidence: false}' >"$evidence_dir/fixture-evidence.json"
chmod 600 "$evidence_dir/fixture-evidence.json"
echo "rhiza KV Kubernetes TEST-ONLY fixture passed: no-PVC metadata recovery verified against ephemeral MinIO"

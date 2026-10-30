#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
manifest_dir="$repo_root/deploy/rhiza-k8s/local"
command -v jq >/dev/null 2>&1 || {
  echo "jq is required for the local Rhiza Kubernetes contract" >&2
  exit 1
}
command -v yq >/dev/null 2>&1 || {
  echo "Mike Farah yq v4 is required for the local Rhiza Kubernetes contract" >&2
  exit 1
}

command -v kustomize >/dev/null 2>&1 || {
  echo "kustomize is required to render the local Rhiza manifests" >&2
  exit 1
}
render() { kustomize build "$manifest_dir"; }

# Kustomize and Mike Farah yq parse local files only. No kubectl API command is
# used, so this check cannot perform discovery or contact an ambient cluster.
render | yq -o=json -I=0 '.' | jq -s -e '
  def resource($kind; $name): first(.[] | select(.kind == $kind and .metadata.name == $name));
  def config: resource("ConfigMap"; "velorix-meta-local-config").data;
  def deployment: resource("Deployment"; "velorix-meta-local").spec;
  def pod: deployment.template.spec;
  def init: pod.initContainers[0];
  def main: pod.containers[0];

  (resource("Deployment"; "velorix-meta-local") != null) and
  (deployment.replicas == 1) and
  (deployment.strategy.type == "Recreate") and
  (init.name == "rhiza-init") and
  (init.args == ["init-local"]) and
  (main.args == ["serve"]) and
  (init.image == main.image) and
  ((init.image == "velorix-meta:PIN_REQUIRED") or (init.image | test("@sha256:[a-f0-9]{64}$"))) and
  (init.imagePullPolicy == "IfNotPresent") and
  (main.imagePullPolicy == "IfNotPresent") and
  (init.envFrom == main.envFrom) and
  (init.env == main.env) and
  (init.resources.requests == main.resources.requests) and
  (init.resources.limits == main.resources.limits) and
  (any((init.volumeMounts // [])[]; .name == "data" and .mountPath == "/var/lib/velorix-meta")) and
  (any((init.volumeMounts // [])[]; .name == "tls" and .mountPath == "/etc/velorix-meta/tls")) and
  (any((main.volumeMounts // [])[]; .name == "data" and .mountPath == "/var/lib/velorix-meta")) and
  (any((main.volumeMounts // [])[]; .name == "tls" and .mountPath == "/etc/velorix-meta/tls")) and
  (pod.volumes[] | select(.name == "data") | .emptyDir.sizeLimit == "10Gi") and
  ([.[] | select(.kind == "PersistentVolumeClaim")] | length == 0) and
  (config.VELORIX_META_MODE == "production") and
  (config.VELORIX_META_BACKEND == "rhiza-kv") and
  (config.VELORIX_META_TRANSPORT_SECURITY == "native-mtls") and
  (config.VELORIX_RHIZA_MODE == "local") and
  (config.VELORIX_RHIZA_CLUSTER_ID == "velorix-meta-local") and
  (config.VELORIX_RHIZA_DATA_DIR == "/var/lib/velorix-meta") and
  (config.VELORIX_RHIZA_OBJECT_STORE_PROVIDER == "s3") and
  (config.VELORIX_RHIZA_OBJECT_STORE_DURABILITY == "before-ack") and
  (config.VELORIX_RHIZA_OBJECT_STORE_INSECURE == "0") and
  (config.VELORIX_RHIZA_OBJECT_STORE_ENDPOINT | length > 0) and
  (config.VELORIX_RHIZA_OBJECT_STORE_BUCKET | length > 0) and
  (config.VELORIX_RHIZA_OBJECT_STORE_PREFIX | length > 0) and
  (any(init.env[]; .name == "VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY" and .valueFrom.secretKeyRef.name == "velorix-meta-local-s3")) and
  (any(init.env[]; .name == "VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY" and .valueFrom.secretKeyRef.name == "velorix-meta-local-s3")) and
  (any(init.env[]; .name == "VELORIX_META_BEARER_TOKEN" and .valueFrom.secretKeyRef.name == "velorix-meta-local-auth")) and
  (resource("Service"; "velorix-meta-local").spec.type == "ClusterIP") and
  (resource("Service"; "velorix-meta-local").spec.ports[0].targetPort == "grpc") and
  (pod.volumes[] | select(.name == "tls") | .secret.secretName == "velorix-meta-local-tls") and
  (all([init, main][]; .resources.limits.cpu != null and .resources.limits.memory != null))
' >/dev/null || {
  echo "local Rhiza Kubernetes manifest contract failed" >&2
  exit 1
}

echo "Local Rhiza Kubernetes manifests passed the offline structural contract"

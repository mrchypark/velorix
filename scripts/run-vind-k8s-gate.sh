#!/usr/bin/env bash
set -euo pipefail

cluster="${VELORIX_VIND_CLUSTER:-velorix-vind}"
namespace="${VELORIX_K8S_NAMESPACE:-velorix-live}"
context="vcluster-docker_${cluster}"
repo_root="$(git rev-parse --show-toplevel)"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require cargo
require docker
require kubectl
require vcluster

cd "$repo_root"

vcluster use driver docker >/dev/null

if ! vcluster list --driver docker --output json | tr -d '[:space:]' | grep -q "\"Name\":\"${cluster}\""; then
  vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster"
else
  kubectl config use-context "$context" >/dev/null
fi

mkdir -p target/velorix-k8s
cargo run -p velorix-k8s --example print_crds > target/velorix-k8s/crds.json
kubectl apply -f target/velorix-k8s/crds.json
kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply -f -

export VELORIX_K8S_INTEGRATION=1
export VELORIX_K8S_NAMESPACE="$namespace"

cargo test -p velorix-k8s --test live_crd_round_trip -- --nocapture --test-threads=1
cargo test -p velorix-k8s --test live_lease -- --nocapture --test-threads=1
cargo test -p velorix-k8s --test live_worker_shard -- --nocapture --test-threads=1

kubectl get crd | grep velorix
kubectl get leases -n "$namespace"
kubectl get velorixdatabases,velorixstreams,velorixworkershards -n "$namespace"

if [ "${VELORIX_VIND_CLEANUP:-0}" = "1" ]; then
  vcluster delete "$cluster" --driver docker
fi

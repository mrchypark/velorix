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
require python3
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

evidence_path="target/velorix-k8s/vind-k8s-gate-evidence.json"
python3 - "$evidence_path" "$cluster" "$context" "$namespace" <<'PY'
import json
import subprocess
import sys
from datetime import datetime, timezone


def run(command):
    return subprocess.check_output(command, text=True).strip().splitlines()


path, cluster, context, namespace = sys.argv[1:]
evidence = {
    "schema_version": 1,
    "evidence_kind": "kubernetes_vind_gate",
    "readiness_evidence_kind": ["kubernetes_lease_client"],
    "cluster": cluster,
    "context": context,
    "namespace": namespace,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "vcluster_version": run(["vcluster", "--version"])[0],
    "kubectl_client_version": run(["kubectl", "version", "--client=true", "--output=yaml"]),
    "applied_crds": run(["kubectl", "get", "crd", "-o", "name"]),
    "live_tests": [
        "cargo test -p velorix-k8s --test live_crd_round_trip",
        "cargo test -p velorix-k8s --test live_lease",
        "cargo test -p velorix-k8s --test live_worker_shard",
    ],
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY
echo "wrote vind Kubernetes gate evidence to ${evidence_path}"

if [ "${VELORIX_VIND_CLEANUP:-0}" = "1" ]; then
  vcluster delete "$cluster" --driver docker
fi

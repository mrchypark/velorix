#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
cluster="${VELORIX_VIND_CLUSTER:-velorix-vind-${run_id}}"
namespace="${VELORIX_K8S_NAMESPACE:-velorix-live}"
context="vcluster-docker_${cluster}"
cleanup="${VELORIX_VIND_CLEANUP:-1}"
reuse_existing="${VELORIX_VIND_REUSE_EXISTING:-0}"
evidence_path="${VELORIX_VIND_EVIDENCE_PATH:-target/velorix-k8s/vind-k8s-gate-evidence.json}"
diagnostics_path="${VELORIX_VIND_DIAGNOSTICS_PATH:-target/velorix-k8s/vind-k8s-gate-diagnostics.txt}"
created_cluster=0
previous_context=""

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

write_diagnostics() {
  mkdir -p "$(dirname "$diagnostics_path")"
  {
    echo "generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "cluster=${cluster}"
    echo "context=${context}"
    echo "namespace=${namespace}"
    echo "reuse_existing=${reuse_existing}"
    echo
    echo "== vcluster list =="
    vcluster list --driver docker || true
    echo
    echo "== kubectl contexts =="
    kubectl config get-contexts || true
    echo
    echo "== readyz =="
    kubectl --context "$context" get --raw=/readyz || true
    echo
    echo "== pods =="
    kubectl --context "$context" get pods -A -o wide || true
    echo
    echo "== leases =="
    kubectl --context "$context" get leases -n "$namespace" -o wide || true
    echo
    echo "== velorix resources =="
    kubectl --context "$context" get velorixdatabases,velorixstreams,velorixworkershards -n "$namespace" -o wide || true
  } >"$diagnostics_path" 2>&1
}

cleanup_vind() {
  status="$1"

  if [ "$status" != "0" ]; then
    write_diagnostics
  fi

  if [ "$cleanup" = "1" ] && [ "$created_cluster" = "1" ]; then
    vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || true
  fi

  if [ -n "$previous_context" ]; then
    kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  fi
}

vcluster_exists() {
  local clusters
  local status
  clusters="$(vcluster list --driver docker --output json)" || {
    echo "failed to list docker vClusters" >&2
    exit 1
  }

  set +e
  python3 - "$cluster" "$clusters" <<'PY'
import json
import sys

cluster = sys.argv[1]
try:
    clusters = json.loads(sys.argv[2])
except json.JSONDecodeError as exc:
    print(f"failed to parse vcluster list JSON: {exc}", file=sys.stderr)
    sys.exit(2)

if not isinstance(clusters, list):
    print("failed to parse vcluster list JSON: expected a list", file=sys.stderr)
    sys.exit(2)

for item in clusters:
    if not isinstance(item, dict):
        print("failed to parse vcluster list JSON: expected object entries", file=sys.stderr)
        sys.exit(2)
    if item.get("Name") == cluster or item.get("name") == cluster:
        sys.exit(0)
sys.exit(1)
PY
  status=$?
  set -e

  case "$status" in
    0) return 0 ;;
    1) return 1 ;;
    *) exit "$status" ;;
  esac
}

wait_for_kubernetes() {
  for _ in $(seq 1 120); do
    if kubectl --context "$context" get --raw=/readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  write_diagnostics
  echo "vind Kubernetes API did not become ready for context ${context}" >&2
  exit 1
}

cd "$repo_root"
trap 'status=$?; cleanup_vind "$status"; exit "$status"' EXIT

previous_context="$(kubectl config current-context 2>/dev/null || true)"

vcluster use driver docker >/dev/null

if vcluster_exists; then
  if [ "$reuse_existing" != "1" ]; then
    echo "vcluster already exists: ${cluster}; choose a unique VELORIX_VIND_CLUSTER, delete the existing cluster, or set VELORIX_VIND_REUSE_EXISTING=1" >&2
    exit 1
  fi
  cluster_exists=1
else
  cluster_exists=0
fi

contexts="$(kubectl config get-contexts -o name)" || {
  echo "failed to list kubectl contexts" >&2
  exit 1
}

if printf '%s\n' "$contexts" | grep -Fxq "$context"; then
  context_exists=1
else
  context_exists=0
fi

if [ "$cluster_exists" = "1" ]; then
  if [ "$context_exists" != "1" ]; then
    echo "vcluster exists but kubectl context is missing: ${context}" >&2
    exit 1
  fi
else
  if [ "$context_exists" = "1" ]; then
    echo "kubectl context already exists: ${context}; choose a unique VELORIX_VIND_CLUSTER or remove the stale context" >&2
    exit 1
  fi
  created_cluster=1
  vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster"
fi

kubectl config use-context "$context" >/dev/null
wait_for_kubernetes

mkdir -p "$(dirname "$evidence_path")" target/velorix-k8s
cargo run -p velorix-k8s --example print_crds > target/velorix-k8s/crds.json
kubectl --context "$context" apply -f target/velorix-k8s/crds.json
kubectl --context "$context" create namespace "$namespace" --dry-run=client -o yaml \
  | kubectl --context "$context" apply -f -

export VELORIX_K8S_INTEGRATION=1
export VELORIX_K8S_NAMESPACE="$namespace"

cargo test -p velorix-k8s --test live_crd_round_trip -- --nocapture --test-threads=1
cargo test -p velorix-k8s --test live_lease -- --nocapture --test-threads=1
cargo test -p velorix-k8s --test live_ingest_admission -- --nocapture --test-threads=1
cargo test -p velorix-k8s --test live_worker_shard -- --nocapture --test-threads=1

kubectl --context "$context" get crd | grep velorix
kubectl --context "$context" get leases -n "$namespace"
kubectl --context "$context" get velorixdatabases,velorixstreams,velorixworkershards -n "$namespace"

python3 - "$evidence_path" "$diagnostics_path" "$cluster" "$context" "$namespace" "$cleanup" "$created_cluster" "$reuse_existing" <<'PY'
import json
import subprocess
import sys
from datetime import datetime, timezone


def run(command):
    return subprocess.check_output(command, text=True).strip().splitlines()


(
    path,
    diagnostics_path,
    cluster,
    context,
    namespace,
    cleanup,
    created_cluster,
    reuse_existing,
) = sys.argv[1:]
evidence = {
    "schema_version": 1,
    "evidence_kind": "kubernetes_vind_gate",
    "readiness_evidence_kind": [
        "kubernetes_lease_client",
        "kubernetes_worker_shard_live_pod_executor",
        "kubernetes_worker_shard_local_filesystem_durable_epoch_restart_read_back",
        "kubernetes_ingest_admission_startup_preflight",
        "kubernetes_ingest_admission_run_local_expiry_restart"
    ],
    "cluster": cluster,
    "context": context,
    "namespace": namespace,
    "cleanup_requested": cleanup == "1",
    "created_cluster": created_cluster == "1",
    "reused_existing_cluster": reuse_existing == "1",
    "diagnostics_path": diagnostics_path,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "docker_version": run(["docker", "version", "--format", "{{.Server.Version}}"])[0],
    "vcluster_version": run(["vcluster", "--version"])[0],
    "kubectl_current_context": run(["kubectl", "config", "current-context"])[0],
    "kubectl_client_version": run(["kubectl", "version", "--client=true", "--output=yaml"]),
    "applied_crds": run(["kubectl", "--context", context, "get", "crd", "-o", "name"]),
    "live_tests": [
        "cargo test -p velorix-k8s --test live_crd_round_trip",
        "cargo test -p velorix-k8s --test live_lease",
        "cargo test -p velorix-k8s --test live_ingest_admission",
        "cargo test -p velorix-k8s --test live_worker_shard",
    ],
    "scope": "local vind Docker Kubernetes evidence with run-local filesystem object-store authority; not 1.0 completion evidence; not multi-pod production ingest-admission readiness",
    "limitations": [
        "ingest-admission startup preflight and run-local expiry/restart use a run-local object-store authority",
        "worker-shard live Pod runtime uses a run-local filesystem-backed object-store authority for epoch records",
        "worker-shard restart read-back recreates checked startup components over the same local filesystem authority root",
        "does not exercise distributed or multi-pod admission races",
        "does not prove worker-shard epoch durability on a production S3-compatible authority, multi-pod restart, or broader operator lifecycle management",
    ],
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2, sort_keys=True)
    f.write("\n")
PY
echo "wrote vind Kubernetes gate evidence to ${evidence_path}"

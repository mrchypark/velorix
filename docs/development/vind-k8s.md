# Local Kubernetes Testing With vind

`vind` is vCluster's Docker driver mode. It gives Velorix a disposable local
Kubernetes API for CRD apply and k8s crate smoke testing without making Kubernetes
the database authority.

Prerequisites:

```bash
docker version
vcluster upgrade --version v0.34.0
vcluster use driver docker
kubectl version --client=true
```

Create and connect a local cluster:

```bash
vcluster create velorix-vind --driver docker --kube-config-context-name velorix-vind
kubectl config current-context
kubectl get nodes
```

Generate and apply the Velorix CRDs:

```bash
mkdir -p target/velorix-k8s
cargo run -p velorix-k8s --example print_crds > target/velorix-k8s/crds.json
kubectl apply -f target/velorix-k8s/crds.json
kubectl get crd | grep velorix
```

Run the local k8s crate checks:

```bash
cargo test -p velorix-k8s -- --nocapture --test-threads=1
```

Run the env-gated live Lease check against the active vind context:

```bash
VELORIX_K8S_INTEGRATION=1 VELORIX_K8S_NAMESPACE=velorix-live \
  cargo test -p velorix-k8s --test live_lease -- --nocapture --test-threads=1
```

Run the env-gated live CRD and reconciled-status round-trip check against the active vind context:

```bash
VELORIX_K8S_INTEGRATION=1 VELORIX_K8S_NAMESPACE=velorix-live \
  cargo test -p velorix-k8s --test live_crd_round_trip -- --nocapture --test-threads=1
```

Run the env-gated live worker-shard ownership, Pod executor, and watch-loop bridge checks against the active vind context. The Pod checks create and read a worker Pod through the Pod-backed executor, including the real `VelorixWorkerShard` watch loop path; they use `registry.k8s.io/pause:3.10` by default and can be overridden with `VELORIX_K8S_WORKER_IMAGE`.

```bash
VELORIX_K8S_INTEGRATION=1 VELORIX_K8S_NAMESPACE=velorix-live \
  cargo test -p velorix-k8s --test live_worker_shard -- --nocapture --test-threads=1
```

Run the env-gated ingest-admission startup preflight against the active vind
context:

```bash
VELORIX_K8S_INTEGRATION=1 VELORIX_K8S_NAMESPACE=velorix-live \
  cargo test -p velorix-k8s --test live_ingest_admission -- --nocapture --test-threads=1
```

This checks Kubernetes API reachability and exercises
`IngestAdmissionCoordinatorProvider::startup()` against a run-local object-store
authority. It does not exercise distributed or multi-pod admission races.

Run the full local vind gate:

```bash
scripts/run-vind-k8s-gate.sh
```

By default, the gate creates a run-owned `velorix-vind-*` vCluster, deletes
that cluster on exit, and writes
`target/velorix-k8s/vind-k8s-gate-evidence.json` with the cluster context,
namespace, applied CRDs, tool versions, and live k8s test set. The artifact is
local Kubernetes evidence only; it is not 1.0 completion evidence and does not
claim multi-pod production ingest-admission readiness. On failure, the gate
writes `target/velorix-k8s/vind-k8s-gate-diagnostics.txt` before cleaning up
owned resources.

Useful overrides:

```bash
VELORIX_VIND_CLUSTER=velorix-vind \
VELORIX_VIND_REUSE_EXISTING=1 \
VELORIX_VIND_CLEANUP=0 \
VELORIX_VIND_EVIDENCE_PATH=target/velorix-k8s/local-evidence.json \
VELORIX_VIND_DIAGNOSTICS_PATH=target/velorix-k8s/local-diagnostics.txt \
  scripts/run-vind-k8s-gate.sh
```

If the requested vCluster name or generated kube context already exists, the
gate fails before applying CRDs or running tests unless
`VELORIX_VIND_REUSE_EXISTING=1` is set. Reused clusters are never deleted by the
gate.

Run the same gate in GitHub Actions with the `Kubernetes vind Gate` manual
workflow. The workflow always attempts to upload JSON evidence and TXT
diagnostics from `target/velorix-k8s` as the `kubernetes-vind-evidence` artifact
so successful evidence or failure context is preserved even when later cleanup
fails.

If you set `VELORIX_VIND_CLEANUP=0`, clean up the cluster when done:

```bash
vcluster delete velorix-vind --driver docker
```

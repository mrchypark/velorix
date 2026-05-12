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

Run the env-gated live worker-shard ownership bridge check against the active vind context:

```bash
VELORIX_K8S_INTEGRATION=1 VELORIX_K8S_NAMESPACE=velorix-live \
  cargo test -p velorix-k8s --test live_worker_shard -- --nocapture --test-threads=1
```

Run the full local vind gate:

```bash
scripts/run-vind-k8s-gate.sh
```

Run the same gate in GitHub Actions with the `Kubernetes vind Gate` manual
workflow.

Clean up the cluster when done:

```bash
vcluster delete velorix-vind
```

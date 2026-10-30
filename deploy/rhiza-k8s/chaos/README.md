# Rhiza Kubernetes Chaos drills

These scripts run one bounded Chaos Mesh experiment against the private Dory
cluster. They never invoke `kubectl` directly. The runner calls the Dory wrapper
(`DORY_BIN`, or `dory` found on `PATH`), resolves the explicitly expected
context from Dory's private kubeconfig, and pins every Kubernetes call to that
context. The current Dory private kubeconfig names it `default`; set
`VELORIX_CHAOS_KUBE_CONTEXT=default` explicitly. The runner also requires that
context's API endpoint to be exactly `https://127.0.0.1:6443` and records both
values. It does not read or change the global kubectl context.

## Preconditions

- The `velorix-chaos` namespace already exists, is labeled
  `velorix.dev/chaos-owner=dory-chaos-drill`, and has annotation
  `velorix.dev/run-id` matching the supplied run ID.
- Choose a new run ID of up to 20 lowercase letters, digits, or hyphens. Set
  `VELORIX_CHAOS_RUN_ID` to that value.
- Choose a new, nonexistent `VELORIX_CHAOS_EVIDENCE_DIR`. The runner creates it
  with mode 700 and refuses to reuse an existing path. Probe output may include
  application responses; keep and share the evidence directory accordingly.
  The HTTP probe accepts only a nonsymlink, current-user-owned mode-700
  directory, creates a unique phase subdirectory, and removes its temporary
  authorization header on exit. Do not place API tokens in logs or evidence.
- Supply exact single-label selectors, a baseline semantic probe, and a
  recovery semantic probe. The commands run with `sh -c` and their output is
  captured in the private evidence directory.
- For `s3-network-loss`, also supply a selector for exactly one Versity Gateway
  Pod and a during-fault probe that succeeds only when a fresh mutation receives
  an actual storage or network failure.

The public HTTP probe is
[`scripts/check-rhiza-dory-chaos-http.sh`](../../../scripts/check-rhiza-dory-chaos-http.sh).
It accepts a loopback port-forward URL, an API token from the environment, and
the run evidence directory. Each invocation writes to a unique mode-700 phase
subdirectory, so baseline and recovery can share the run directory without
overwriting prior outputs. Run `baseline` once before the fault and `recovery`
after rollback. Its baseline registers a schema-bound relation and two views,
ingests one row, and checks materialized sum/count results. Recovery checks the
materialized results, appends the next offset, retries that offset, and requires
the retry to be reported as a duplicate without double counting. Keep tokens
out of logs and evidence files.

Invoke the HTTP probe through `sh` so the documented command works whether or
not the checkout marks the script executable:

```sh
sh scripts/check-rhiza-dory-chaos-http.sh baseline
sh scripts/check-rhiza-dory-chaos-http.sh recovery
```

## Running a drill

Run [`scripts/check-rhiza-local-chaos.sh`](../../../scripts/check-rhiza-local-chaos.sh)
for one experiment at a time. Set the variables below for each run:

```sh
export VELORIX_CHAOS_KUBE_CONTEXT=default
export VELORIX_CHAOS_NAMESPACE=velorix-chaos
export VELORIX_CHAOS_RUN_ID=your-new-run-id
export VELORIX_CHAOS_EVIDENCE_DIR="$PWD/target/rhiza-dory-chaos-$VELORIX_CHAOS_RUN_ID"
export VELORIX_CHAOS_SCENARIO=api-pod-kill
export VELORIX_CHAOS_SELECTOR='app=velorix-api'
export VELORIX_CHAOS_BASELINE_CMD='your application baseline probe'
export VELORIX_CHAOS_RECOVERY_CMD='your application recovery probe'
export VELORIX_CHAOS_DURATION=20s
sh scripts/check-rhiza-local-chaos.sh
```

Set `DORY_BIN` when the wrapper is not named `dory` on `PATH`. Do not replace
the wrapper with `kubectl`. Supported scenarios are `meta-pod-kill`,
`api-pod-kill`, `worker-pod-kill`, and `s3-network-loss`.

## Acceptance and gaps

Every scenario must show `Selected=True` and `AllInjected=True`; mere custom
resource creation is not injection evidence. PodChaos uses the `pod-kill`
one-shot action, so `AllRecovered=True` is not a required recovery signal and
may remain false. A PodChaos pass requires all of the following: observed
injection, the original Pod UID is absent, exactly one replacement Pod with a
new UID is Running and Ready, and the replacement has the same controlling
owner UID as the original Pod. The owned PodChaos resource must be deleted with
NotFound confirmed before the application recovery probe passes. Meta Pod
recovery also restarts the API and worker Deployments before the probe to avoid
relying on a live-process query cache.

NetworkChaos is reversible. A pass requires observed injection, the
during-fault semantic probe to establish an actual failed mutation, an observed
`AllRecovered=True` rollback condition, the original target Pod still Running
and Ready under its original controller, the owned NetworkChaos resource
deleted with NotFound confirmed, and a passing recovery probe. A kernel or
runtime that cannot apply the fault does
not pass: if Chaos Mesh cannot report injection or recovery, the script exits
nonzero and preserves evidence. Record unavailable kernel/runtime support as a
verification gap; do not call it a pass or silently skip it.

Pod readiness alone is not application recovery. The semantic probe must check
the relevant materialized query and ingest invariants. Do not force-delete Meta
or modify held-token locks. If recovery fails closed, preserve the evidence and
report the gap; do not add an unlock or fencing bypass. These drills establish
only the behavior of the specific isolated run, not production availability or
general failover guarantees.

The runner validates namespace ownership label and run annotation before it
creates an experiment. It uses atomic creation and refuses name collisions.
The generated Kubernetes manifest is retained as `experiment.json`. The
network scenario uses a bidirectional partition of the exact workload/S3 Pod
pair, which disconnects that pair completely.
Cleanup requires the UID returned by that creation, checks the live UID, and
uses the Kubernetes DeleteOptions UID precondition in the raw DELETE request,
then waits for NotFound. Keep the request body, URL, and cleanup logs for review.
The offline contract test uses a mock Dory wrapper and does not contact a
Kubernetes cluster:

```sh
sh scripts/check-rhiza-chaos-contract.sh
```

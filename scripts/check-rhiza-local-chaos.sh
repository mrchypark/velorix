#!/bin/sh
set -eu

# Bounded Chaos Mesh drill in Dory's private Kubernetes cluster. All Kubernetes
# access goes through its kubeconfig and an explicit, validated context.
# Workload deployment and semantic probes are supplied by the caller.

die() { echo "rhiza local chaos: $*" >&2; exit 64; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }
need jq
need sleep

expected_context=${VELORIX_CHAOS_KUBE_CONTEXT:-}
namespace=${VELORIX_CHAOS_NAMESPACE:-}
run_id=${VELORIX_CHAOS_RUN_ID:-}
scenario=${VELORIX_CHAOS_SCENARIO:-}
selector=${VELORIX_CHAOS_SELECTOR:-}
s3_selector=${VELORIX_CHAOS_S3_SELECTOR:-}
baseline=${VELORIX_CHAOS_BASELINE_CMD:-}
recovery=${VELORIX_CHAOS_RECOVERY_CMD:-}
during=${VELORIX_CHAOS_DURING_CMD:-}
duration=${VELORIX_CHAOS_DURATION:-20s}
evidence=${VELORIX_CHAOS_EVIDENCE_DIR:-}

[ -n "$expected_context" ] || die 'set VELORIX_CHAOS_KUBE_CONTEXT to the expected context in the Dory private kubeconfig (currently default)'
case "$expected_context" in *[!A-Za-z0-9._-]*|'') die 'Kubernetes context contains unsupported characters' ;; esac
[ -n "$namespace" ] || die 'set VELORIX_CHAOS_NAMESPACE explicitly'
case "$namespace" in velorix-chaos) ;; *) die 'namespace must be the exact owned namespace velorix-chaos' ;; esac
[ -n "$run_id" ] || die 'set VELORIX_CHAOS_RUN_ID to the run ID recorded on the owned namespace'
case "$run_id" in *[!a-z0-9-]*|'') die 'run ID must contain only lowercase letters, digits, and hyphens' ;; esac
[ "${#run_id}" -le 20 ] || die 'run ID must be at most 20 characters'
[ -n "$scenario" ] || die 'set VELORIX_CHAOS_SCENARIO to meta-pod-kill, api-pod-kill, worker-pod-kill, or s3-network-loss'
case "$scenario" in
  meta-pod-kill|api-pod-kill|worker-pod-kill|s3-network-loss) ;;
  *) die 'unsupported scenario; choose meta-pod-kill, api-pod-kill, worker-pod-kill, or s3-network-loss' ;;
esac
[ -n "$selector" ] || die 'set VELORIX_CHAOS_SELECTOR to an exact target label selector'
case "$selector" in *[!A-Za-z0-9._/:=-]*|*=*=*) die 'selector must be one exact key=value label' ;; *=*) ;; *) die 'selector must be one exact key=value label' ;; esac
[ -n "$baseline" ] || die 'set VELORIX_CHAOS_BASELINE_CMD to the application-level pre-fault probe'
[ -n "$recovery" ] || die 'set VELORIX_CHAOS_RECOVERY_CMD to the application-level post-fault probe'
[ "$scenario" != s3-network-loss ] || [ -n "$during" ] || die 's3-network-loss requires VELORIX_CHAOS_DURING_CMD to verify an actual S3 failure'
if [ "$scenario" = s3-network-loss ]; then
  [ -n "$s3_selector" ] || die 's3-network-loss requires VELORIX_CHAOS_S3_SELECTOR for the Versity Gateway Pod'
  case "$s3_selector" in *[!A-Za-z0-9._/:=-]*|*=*=*) die 'S3 selector must be one exact key=value label' ;; *=*) ;; *) die 'S3 selector must be one exact key=value label' ;; esac
fi
[ -n "$evidence" ] || die 'set VELORIX_CHAOS_EVIDENCE_DIR to a new evidence directory'
case "$duration" in 10s|15s|20s|30s) ;; *) die 'duration must be one of 10s, 15s, 20s, 30s' ;; esac
umask 077
[ ! -e "$evidence" ] || die 'evidence directory must not already exist; choose a fresh run directory'
mkdir -m 700 -p "$evidence"
resource=''
chaos_name=''
experiment_uid=''
delete_owned_experiment() {
  delete_log=$1
  delete_url="/apis/chaos-mesh.org/v1alpha1/namespaces/${namespace}/${resource}/${chaos_name}"
  if ! k -n "$namespace" get "$resource" "$chaos_name" -o json >"$evidence/experiment-current-before-delete.json" 2>"$evidence/experiment-current-before-delete.error"; then
    case "$(cat "$evidence/experiment-current-before-delete.error")" in
      *NotFound*|*not\ found*) experiment_uid=''; return 0 ;;
      *) return 1 ;;
    esac
  fi
  jq -e --arg uid "$experiment_uid" '.metadata.uid == $uid' "$evidence/experiment-current-before-delete.json" >/dev/null || return 1
  printf '%s\n' "$delete_url" >"$evidence/experiment-delete-url.txt"
  jq -n --arg uid "$experiment_uid" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' \
    >"$evidence/experiment-delete-options.json"
  k delete --raw="$delete_url" -f "$evidence/experiment-delete-options.json" >"$delete_log" 2>&1 || return 1
  delete_tries=0
  while [ "$delete_tries" -lt 30 ]; do
    delete_tries=$((delete_tries + 1))
    if k -n "$namespace" get "$resource" "$chaos_name" -o json \
      >"$evidence/experiment-after-delete.json" 2>"$evidence/experiment-after-delete.error"; then
      sleep 2
    else
      case "$(cat "$evidence/experiment-after-delete.error")" in
        *NotFound*|*not\ found*) experiment_uid=''; return 0 ;;
        *) return 1 ;;
      esac
    fi
  done
  return 1
}
cleanup_experiment() {
  cleanup_rc=0
  if [ -n "$experiment_uid" ] && [ -n "$resource" ] && [ -n "$chaos_name" ]; then
    delete_owned_experiment "$evidence/experiment-cleanup-trap.log" || cleanup_rc=1
    if [ "$cleanup_rc" -ne 0 ]; then
      echo "rhiza local chaos: FAILURE removing owned experiment $chaos_name; see cleanup evidence under $evidence" >&2
      printf '%s\n' "cleanup_failed experiment=$chaos_name uid=$experiment_uid" >"$evidence/CLEANUP_FAILED"
    fi
  fi
  return "$cleanup_rc"
}
trap cleanup_experiment EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
if [ -n "${DORY_BIN:-}" ]; then
  dory_bin=$(command -v "$DORY_BIN" 2>/dev/null || true)
  [ -n "$dory_bin" ] || die 'DORY_BIN must name an executable Dory wrapper or command on PATH'
else
  dory_bin=$(command -v dory 2>/dev/null || true)
  [ -n "$dory_bin" ] || die 'Dory wrapper not found; set DORY_BIN or add dory to PATH'
fi
resolved_context=$("$dory_bin" k8s --context "$expected_context" config view --minify -o jsonpath='{.contexts[0].name}') || die 'could not resolve the expected context from the Dory private kubeconfig'
[ "$resolved_context" = "$expected_context" ] || die "Dory kubeconfig resolved context '$resolved_context', expected '$expected_context'"
k() { "$dory_bin" k8s --context "$resolved_context" "$@"; }
server=$(k config view --minify -o jsonpath='{.clusters[0].cluster.server}') || die 'Dory Kubernetes API is unavailable'
[ "$server" = https://127.0.0.1:6443 ] || die 'selected Dory context does not point to the verified private loopback API endpoint'
printf '%s\n' "$server" >"$evidence/api-server.txt"
printf '%s\n' "$resolved_context" >"$evidence/context.txt"
k get namespace "$namespace" >/dev/null 2>&1 || die 'owned test namespace must already exist'
k get namespace "$namespace" -o json >"$evidence/namespace-before.json"
jq -e '.metadata.labels["velorix.dev/chaos-owner"] == "dory-chaos-drill"' "$evidence/namespace-before.json" >/dev/null || die 'namespace ownership label is missing or does not identify this drill'
jq -e --arg run_id "$run_id" '.metadata.annotations["velorix.dev/run-id"] == $run_id' "$evidence/namespace-before.json" >/dev/null || die 'namespace run annotation does not match VELORIX_CHAOS_RUN_ID'
k -n "$namespace" get pods -l "$selector" -o json >"$evidence/pods-before.json"
jq -e '.items | length == 1' "$evidence/pods-before.json" >/dev/null || die 'selector must identify exactly one target Pod'
pod=$(jq -r '.items[0].metadata.name' "$evidence/pods-before.json")
uid=$(jq -r '.items[0].metadata.uid' "$evidence/pods-before.json")
controller_uid=$(jq -r '[.items[0].metadata.ownerReferences[]? | select(.controller == true) | .uid] | if length == 1 then .[0] else empty end' "$evidence/pods-before.json")
[ -n "$controller_uid" ] || die 'target Pod must have exactly one controlling owner UID'
printf '%s\n' "$controller_uid" >"$evidence/target-controller-uid.txt"
printf '%s\n' "$namespace" >"$evidence/namespace.txt"
printf '%s\n' "$scenario" >"$evidence/scenario.txt"
printf '%s\n' "$duration" >"$evidence/duration.txt"
printf '%s\n' "$pod $uid" >"$evidence/target-pod.txt"

# Baseline and recovery commands run in a shell and must themselves verify
# durable query results / checkpoint invariants. Their output is retained.
sh -c "$baseline" >"$evidence/baseline.log" 2>&1 || die 'baseline application probe failed'
chaos_name="velorix-chaos-${scenario}-${run_id}"
case "$scenario" in
  meta-pod-kill|api-pod-kill|worker-pod-kill)
    jq -n --arg name "$chaos_name" --arg namespace "$namespace" \
      --arg duration "$duration" --arg key "${selector%%=*}" --arg value "${selector#*=}" '
      {apiVersion:"chaos-mesh.org/v1alpha1",kind:"PodChaos",
       metadata:{name:$name,namespace:$namespace},
       spec:{action:"pod-kill",mode:"one",duration:$duration,
             selector:{namespaces:[$namespace],labelSelectors:{($key):$value}}}}
    ' >"$evidence/experiment.json"
    resource=podchaos
    ;;
  s3-network-loss)
    jq -n --arg name "$chaos_name" --arg namespace "$namespace" \
      --arg duration "$duration" --arg key "${selector%%=*}" --arg value "${selector#*=}" \
      --arg s3_key "${s3_selector%%=*}" --arg s3_value "${s3_selector#*=}" '
      {apiVersion:"chaos-mesh.org/v1alpha1",kind:"NetworkChaos",
       metadata:{name:$name,namespace:$namespace},
       spec:{action:"partition",mode:"one",duration:$duration,direction:"both",
             target:{mode:"one",selector:{namespaces:[$namespace],labelSelectors:{($s3_key):$s3_value}}},
             selector:{namespaces:[$namespace],labelSelectors:{($key):$value}}}}
    ' >"$evidence/experiment.json"
    resource=networkchaos
    ;;
  *) die 'unsupported scenario name' ;;
esac

if [ "$scenario" = s3-network-loss ]; then
  k -n "$namespace" get pods -l "$s3_selector" -o json >"$evidence/s3-target-before.json"
  jq -e '.items | length == 1' "$evidence/s3-target-before.json" >/dev/null || die 'S3 selector must identify exactly one Versity Gateway Pod'
  jq -r '.items[0].metadata.name + " " + .items[0].metadata.uid' "$evidence/s3-target-before.json" >"$evidence/s3-target-pod.txt"
fi

k -n "$namespace" create -f "$evidence/experiment.json" -o json >"$evidence/experiment-created.json" 2>"$evidence/create.error" \
  || die 'could not atomically create the Chaos Mesh experiment (name collision or admission error; no existing experiment was changed)'
if ! experiment_uid=$(jq -er '.metadata.uid | select(type == "string" and length > 0)' "$evidence/experiment-created.json"); then
  printf '%s\n' 'create succeeded but response omitted UID; inspect the Dory context and remove only after verifying ownership' >"$evidence/CLEANUP_FAILED"
  die 'created experiment response did not include a UID; automatic cleanup is withheld for safety'
fi
printf '%s\n' "$chaos_name" >"$evidence/experiment-name.txt"
# Injection evidence is the CR's observed injected condition, not mere creation.
injected=0
tries=0
while [ "$tries" -lt 30 ]; do
  tries=$((tries + 1))
  k -n "$namespace" get "$resource" "$chaos_name" -o json >"$evidence/experiment-status.json" 2>"$evidence/experiment-status.error" || true
  if jq -e '[.status.conditions[]? | select((.type == "AllInjected") and (.status == "True"))] | length > 0' "$evidence/experiment-status.json" >/dev/null 2>&1; then injected=1; break; fi
  sleep 2
done
[ "$injected" -eq 1 ] || die 'Chaos Mesh did not report an injected condition; no recovery claim made'
jq -e '[.status.conditions[]? | select(.type == "Selected" and .status == "True")] | length > 0' \
  "$evidence/experiment-status.json" >/dev/null || die 'Chaos Mesh did not report Selected=True for the requested target'
sleep 2
if [ "$scenario" != s3-network-loss ]; then
  k -n "$namespace" get pods -o json >"$evidence/pods-in-namespace-after-kill.json"
  jq -e --arg uid "$uid" '[.items[]? | select(.metadata.uid == $uid)] | length == 0' \
    "$evidence/pods-in-namespace-after-kill.json" >/dev/null || die 'original Pod UID is still present after PodChaos'
  k -n "$namespace" get pods -l "$selector" -o json >"$evidence/pods-during.json"
  jq -e --arg uid "$uid" --arg controller "$controller_uid" '
    .items | length == 1 and .[0].metadata.uid != $uid and
    ([.[0].metadata.ownerReferences[]? | select(.controller == true and .uid == $controller)] | length == 1)
  ' "$evidence/pods-during.json" >/dev/null || die 'replacement Pod is not controlled by the original Pod controller'
else
  cp "$evidence/experiment-status.json" "$evidence/network-injection-confirmed.json"
  sh -c "$during" >"$evidence/during-fault-s3-probe.log" 2>&1 || die 'during-fault probe could not verify the expected S3 failure'
  printf '%s\n' 'The probe exited successfully because S3 failure was observed as expected.' >"$evidence/during-fault-s3-probe.result"
fi

# PodChaos pod-kill is a one-shot action: duration/pause do not undo the kill.
# Its acceptance is injection, old UID gone, a Ready replacement, owned CR
# deletion/absence, then an application-level recovery probe. NetworkChaos is
# reversible and still requires AllRecovered=True before semantic recovery.
if [ "$scenario" != s3-network-loss ]; then
  k -n "$namespace" get "$resource" "$chaos_name" -o json >"$evidence/experiment-final-before-delete.json"
  jq -r '[.status.conditions[]? | select(.type == "AllRecovered") | .status] | first // "not-observed"' \
    "$evidence/experiment-final-before-delete.json" >"$evidence/all-recovered-before-delete.txt"
  delete_owned_experiment "$evidence/experiment-cleanup.log" \
    || die 'could not safely delete the owned one-shot PodChaos experiment and confirm its absence'
  resource=''
  chaos_name=''
  k -n "$namespace" get pods -l "$selector" -o json >"$evidence/pods-after-delete.json"
  k -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=180s >"$evidence/recovery-ready.log" 2>&1 \
    || die 'replacement Pod did not return Ready after one-shot cleanup'
  k -n "$namespace" get pods -l "$selector" -o json >"$evidence/pods-after-recovery.json"
  jq -e --arg uid "$uid" --arg controller "$controller_uid" '
    .items | length == 1 and .[0].status.phase == "Running" and .[0].metadata.uid != $uid and
    ([.[0].status.conditions[]? | select(.type == "Ready" and .status == "True")] | length > 0) and
    ([.[0].metadata.ownerReferences[]? | select(.controller == true and .uid == $controller)] | length == 1)
  ' "$evidence/pods-after-recovery.json" >/dev/null || die 'Ready replacement Pod does not belong to the original Pod controller'
  jq -r '.items[0].metadata.name + " " + .items[0].metadata.uid' \
    "$evidence/pods-after-recovery.json" >"$evidence/recovery-pod-uid.txt"
  if [ "$scenario" = meta-pod-kill ]; then
    k -n "$namespace" rollout restart deployment/velorix-api deployment/velorix-worker >"$evidence/api-worker-cold-restart.log" 2>&1 \
      || die 'could not restart owned API and worker Deployments after Meta replacement'
    k -n "$namespace" rollout status deployment/velorix-api --timeout=180s >"$evidence/api-cold-ready.log" 2>&1 \
      || die 'API did not return Ready after Meta restart'
    k -n "$namespace" rollout status deployment/velorix-worker --timeout=180s >"$evidence/worker-cold-ready.log" 2>&1 \
      || die 'worker did not return Ready after Meta restart'
    k -n "$namespace" get pods -l 'velorix.dev/chaos-role in (api,worker)' -o json >"$evidence/api-worker-after-cold-restart.json"
  fi
  sh -c "$recovery" >"$evidence/recovery.log" 2>&1 || die 'application-level recovery probe failed (possible fail-closed/orphan-held-token condition; no unlock attempted)'
  echo "PASS application recovery after one-shot PodChaos; AllRecovered=$(cat "$evidence/all-recovered-before-delete.txt") (condition gap recorded) scenario=$scenario evidence=$evidence"
  exit 0
fi

recovered=0
tries=0
while [ "$tries" -lt 90 ]; do
  tries=$((tries + 1))
  k -n "$namespace" get "$resource" "$chaos_name" -o json >"$evidence/experiment-recovery-status.json" 2>"$evidence/experiment-recovery-status.error" || true
  if jq -e '[.status.conditions[]? | select((.type|ascii_downcase == "allrecovered") and (.status|ascii_downcase == "true"))] | length > 0' "$evidence/experiment-recovery-status.json" >/dev/null 2>&1; then recovered=1; break; fi
  sleep 2
done
[ "$recovered" -eq 1 ] || die 'Chaos Mesh did not report AllRecovered=True within 180s'
k -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=180s >"$evidence/recovery-ready.log" 2>&1 || die 'target Pod did not return Ready within 180s'
k -n "$namespace" get pods -l "$selector" -o json >"$evidence/pods-after-recovery.json"
jq -e --arg uid "$uid" --arg controller "$controller_uid" '
  .items | length == 1 and .[0].metadata.uid == $uid and .[0].status.phase == "Running" and
  ([.[0].status.conditions[]? | select(.type == "Ready" and .status == "True")] | length > 0) and
  ([.[0].metadata.ownerReferences[]? | select(.controller == true and .uid == $controller)] | length == 1)
' "$evidence/pods-after-recovery.json" >/dev/null || die 'NetworkChaos target did not return Ready under its original controller'
jq -r '.items[0].metadata.name + " " + .items[0].metadata.uid' \
  "$evidence/pods-after-recovery.json" >"$evidence/recovery-pod-uid.txt"
k -n "$namespace" get "$resource" "$chaos_name" -o json >"$evidence/experiment-final.json" 2>&1 || true
delete_owned_experiment "$evidence/experiment-cleanup.log" \
  || die 'could not safely delete the owned NetworkChaos experiment and confirm its absence'
resource=''
chaos_name=''
sh -c "$recovery" >"$evidence/recovery.log" 2>&1 || die 'application-level recovery probe failed (possible fail-closed/orphan-held-token condition; no unlock attempted)'
echo "PASS scenario=$scenario target=$pod evidence=$evidence"

#!/bin/sh
set -eu

# Offline Dory mock covers context pinning, PodChaos and NetworkChaos criteria,
# atomic name collision handling, UID-conditioned cleanup, and HTTP secrets.
repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
command -v jq >/dev/null 2>&1 || { echo 'jq is required for the Chaos contract' >&2; exit 1; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/velorix-chaos-contract.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir "$tmp/bin"
cat >"$tmp/bin/dory" <<'MOCK'
#!/bin/sh
set -eu
[ "${1:-}" = k8s ] || exit 90
shift
[ "${1:-}" = --context ] || { echo 'Dory call omitted explicit context' >&2; exit 90; }
context=$2
shift 2
printf 'context=%s command=%s\n' "$context" "$*" >>"$MOCK_LOG"
case " $* " in *' --preconditions='*) echo 'unsupported delete flag --preconditions' >&2; exit 93 ;; esac
resource=podchaos
chaos_name=velorix-chaos-api-pod-kill-contract-run
case "${MOCK_SCENARIO:-api-pod-kill}" in
  s3-network-loss) resource=networkchaos; chaos_name=velorix-chaos-s3-network-loss-contract-run ;;
esac
case "${1:-}" in
  delete)
    raw=${2:-}
    expected_raw="--raw=/apis/chaos-mesh.org/v1alpha1/namespaces/velorix-chaos/$resource/$chaos_name"
    [ "$raw" = "$expected_raw" ] || { echo "unexpected raw delete URL: $raw" >&2; exit 94; }
    shift 2
    [ "${1:-}" = -f ] || { echo 'raw delete must carry a DeleteOptions file' >&2; exit 95; }
    delete_options=$2
    shift 2
    [ "$#" -eq 0 ] || { echo "unexpected raw delete flags: $*" >&2; exit 96; }
    jq -e '.apiVersion == "v1" and .kind == "DeleteOptions" and .preconditions.uid == "chaos-uid"' "$delete_options" >/dev/null || exit 97
    printf 'context=%s delete raw=%s uid=%s\n' "$context" "$raw" "$(jq -r '.preconditions.uid' "$delete_options")" >>"$MOCK_LOG"
    : >"$MOCK_DELETED"
    exit 0 ;;
esac
case "$*" in
  'config view --minify -o jsonpath={.contexts[0].name}')
    printf '%s\n' "${MOCK_CONFIG_CONTEXT:-default}" ;;
  'config view --minify -o jsonpath={.clusters[0].cluster.server}')
    printf '%s\n' 'https://127.0.0.1:6443' ;;
  'get namespace velorix-chaos')
    exit 0 ;;
  'get namespace velorix-chaos -o json')
    printf '%s\n' '{"metadata":{"labels":{"velorix.dev/chaos-owner":"dory-chaos-drill"},"annotations":{"velorix.dev/run-id":"contract-run"}}}' ;;
  '-n velorix-chaos get pods -l app=gateway -o json')
    printf '%s\n' '{"items":[{"metadata":{"name":"gateway","uid":"gateway-uid"}}]}' ;;
  '-n velorix-chaos get pods -l app=meta -o json')
    count=0
    [ ! -f "$MOCK_COUNT" ] || count=$(cat "$MOCK_COUNT")
    count=$((count + 1)); printf '%s\n' "$count" >"$MOCK_COUNT"
    owner=controller-uid
    [ "$count" -lt 2 ] || owner=${MOCK_REPLACEMENT_CONTROLLER:-controller-uid}
    if [ "${MOCK_SCENARIO:-api-pod-kill}" = s3-network-loss ] || [ "$count" -eq 1 ]; then name=meta-old; uid=old-uid; else name=meta-new; uid=new-uid; fi
    printf '{"items":[{"metadata":{"name":"%s","uid":"%s","ownerReferences":[{"uid":"%s","controller":true}]},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}]}\n' "$name" "$uid" "$owner" ;;
  '-n velorix-chaos get pods -o json')
    printf '%s\n' '{"items":[{"metadata":{"name":"meta-new","uid":"new-uid"}}]}' ;;
  '-n velorix-chaos create -f '*'-o json')
    [ "$#" -eq 7 ] && [ "$4" = -f ] && [ "$6" = -o ] && [ "$7" = json ] || exit 98
    manifest=$5
    jq -e --arg name "$chaos_name" '
      .apiVersion == "chaos-mesh.org/v1alpha1" and
      .metadata.namespace == "velorix-chaos" and .metadata.name == $name and
      .spec.mode == "one" and .spec.duration == "10s" and
      .spec.selector.namespaces == ["velorix-chaos"] and
      .spec.selector.labelSelectors == {app:"meta"}
    ' "$manifest" >/dev/null || { echo 'invalid experiment scope, selector, or duration' >&2; exit 98; }
    if [ "$resource" = podchaos ]; then
      jq -e '.kind == "PodChaos" and .spec.action == "pod-kill" and
        (.spec | has("scheduler") | not) and (.spec | has("target") | not)' \
        "$manifest" >/dev/null || { echo 'invalid one-shot PodChaos fault' >&2; exit 98; }
    else
      # A bidirectional partition disconnects the pair completely; loss.loss
      # belongs to the distinct packet-loss action, not this partition spec.
      jq -e '.kind == "NetworkChaos" and .spec.action == "partition" and
        .spec.direction == "both" and .spec.target.mode == "one" and
        .spec.target.selector.namespaces == ["velorix-chaos"] and
        .spec.target.selector.labelSelectors == {app:"gateway"} and
        (.spec | has("loss") | not)
      ' "$manifest" >/dev/null || { echo 'invalid complete NetworkChaos partition or S3 target' >&2; exit 98; }
    fi
    if [ "${MOCK_COLLISION:-0}" = 1 ]; then echo 'Error from server (AlreadyExists): podchaos already exists' >&2; exit 1; fi
    printf '%s\n' '{"metadata":{"uid":"chaos-uid"}}' ;;
  '-n velorix-chaos get podchaos velorix-chaos-api-pod-kill-contract-run -o json'|'-n velorix-chaos get networkchaos velorix-chaos-s3-network-loss-contract-run -o json')
    if [ -f "$MOCK_DELETED" ]; then echo 'Error from server (NotFound): podchaos not found' >&2; exit 1; fi
    jq -cn --arg injected "${MOCK_ALL_INJECTED:-True}" --arg recovered "${MOCK_ALL_RECOVERED:-False}" \
      '{metadata:{uid:"chaos-uid"},status:{conditions:[{type:"Selected",status:"True"},{type:"AllInjected",status:$injected},{type:"AllRecovered",status:$recovered}]}}' ;;
  '-n velorix-chaos wait --for=condition=Ready pod -l app=meta --timeout=180s')
    exit 0 ;;
  *) echo "unexpected mock Dory command: $*" >&2; exit 91 ;;
esac
MOCK
cat >"$tmp/bin/curl" <<'MOCK'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$MOCK_CURL_ARGS"
output=
url=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) output=$2; shift 2 ;;
    -H)
      case "$2" in
        @*) cat "${2#@}" >>"$MOCK_CURL_HEADERS" ;;
        *) printf '%s\n' "$2" >>"$MOCK_CURL_HEADERS" ;;
      esac
      shift 2 ;;
    *) url=$1; shift ;;
  esac
done
case "$url" in
  */sample_sum/query) printf '%s\n' '{"rows":[{"result":2.5}]}' >"$output" ;;
  */sample_count/query) printf '%s\n' '{"rows":[{"result":1}]}' >"$output" ;;
  *) echo "unexpected mock curl URL: $url" >&2; exit 92 ;;
esac
printf '200'
MOCK
cat >"$tmp/bin/sleep" <<'MOCK'
#!/bin/sh
exit 0
MOCK
chmod 700 "$tmp/bin/dory" "$tmp/bin/curl" "$tmp/bin/sleep"
export MOCK_COUNT="$tmp/count" MOCK_DELETED="$tmp/deleted" MOCK_LOG="$tmp/dory.log"
export MOCK_CURL_ARGS="$tmp/curl-args.log" MOCK_CURL_HEADERS="$tmp/curl-headers.log"

reset_mock() { rm -f "$MOCK_COUNT" "$MOCK_DELETED" "$MOCK_LOG"; : >"$MOCK_LOG"; }
run_drill() {
  expected_context=$1 config_context=$2 replacement_owner=$3 collision=$4 scenario=$5 injected=$6 recovered=$7 evidence=$8
  reset_mock
  PATH="$tmp/bin:$PATH" MOCK_CONFIG_CONTEXT="$config_context" \
    MOCK_REPLACEMENT_CONTROLLER="$replacement_owner" MOCK_COLLISION="$collision" \
    MOCK_SCENARIO="$scenario" MOCK_ALL_INJECTED="$injected" MOCK_ALL_RECOVERED="$recovered" \
    VELORIX_CHAOS_KUBE_CONTEXT="$expected_context" VELORIX_CHAOS_NAMESPACE=velorix-chaos \
    VELORIX_CHAOS_RUN_ID=contract-run VELORIX_CHAOS_SCENARIO="$scenario" \
    VELORIX_CHAOS_SELECTOR=app=meta VELORIX_CHAOS_BASELINE_CMD=true \
    VELORIX_CHAOS_RECOVERY_CMD=true VELORIX_CHAOS_EVIDENCE_DIR="$evidence" \
    VELORIX_CHAOS_S3_SELECTOR=app=gateway VELORIX_CHAOS_DURING_CMD=true \
    VELORIX_CHAOS_DURATION=10s sh "$repo_root/scripts/check-rhiza-local-chaos.sh" \
    >"$tmp/stdout" 2>"$tmp/stderr"
}

if run_drill default default controller-uid 0 api-pod-kill True False "$tmp/evidence-success"; then :; else cat "$tmp/stderr" >&2; exit 1; fi
grep -q 'PASS application recovery after one-shot PodChaos' "$tmp/stdout" || { cat "$tmp/stdout" >&2; exit 1; }
[ "$(cat "$tmp/evidence-success/all-recovered-before-delete.txt")" = False ] || { echo 'mock did not exercise AllRecovered=False' >&2; exit 1; }
[ "$(cat "$tmp/evidence-success/recovery-pod-uid.txt")" = 'meta-new new-uid' ] || { echo 'replacement Pod evidence missing' >&2; exit 1; }
[ "$(cat "$tmp/evidence-success/target-controller-uid.txt")" = controller-uid ] || { echo 'controller UID evidence missing' >&2; exit 1; }
grep -q 'NotFound' "$tmp/evidence-success/experiment-after-delete.error" || { echo 'CR absence was not confirmed' >&2; exit 1; }
[ "$(cat "$tmp/evidence-success/context.txt")" = default ] || { echo 'resolved context was not recorded' >&2; exit 1; }
[ "$(cat "$tmp/evidence-success/api-server.txt")" = https://127.0.0.1:6443 ] || { echo 'loopback endpoint was not recorded' >&2; exit 1; }
if grep -v '^context=default ' "$MOCK_LOG"; then echo 'a Dory Kubernetes call used a different context' >&2; exit 1; fi
mode=$(stat -c '%a' "$tmp/evidence-success" 2>/dev/null || stat -f '%Lp' "$tmp/evidence-success")
[ "$mode" = 700 ] || { echo 'evidence directory is not private' >&2; exit 1; }

if run_drill expected-other default controller-uid 0 api-pod-kill True False "$tmp/evidence-context"; then echo 'context mismatch unexpectedly passed' >&2; exit 1; fi
grep -q 'expected.*expected-other' "$tmp/stderr" || { cat "$tmp/stderr" >&2; exit 1; }
if grep -q 'get namespace' "$MOCK_LOG" || grep -q 'create -f' "$MOCK_LOG"; then echo 'context mismatch reached workload operations' >&2; exit 1; fi

if run_drill default default unrelated-controller 0 api-pod-kill True False "$tmp/evidence-controller"; then echo 'controller mismatch unexpectedly passed' >&2; exit 1; fi
grep -q 'replacement Pod is not controlled' "$tmp/stderr" || { cat "$tmp/stderr" >&2; exit 1; }
grep -q 'context=default delete raw=--raw=/apis/chaos-mesh.org/v1alpha1/namespaces/velorix-chaos/podchaos/velorix-chaos-api-pod-kill-contract-run uid=chaos-uid' "$MOCK_LOG" || { cat "$MOCK_LOG" >&2; echo 'owned CR delete did not carry the pinned context, raw URL, and server-side UID condition' >&2; exit 1; }

if run_drill default default controller-uid 1 api-pod-kill True False "$tmp/evidence-collision"; then echo 'name collision unexpectedly passed' >&2; exit 1; fi
grep -q 'atomically create' "$tmp/stderr" || { cat "$tmp/stderr" >&2; exit 1; }
if grep -q 'delete raw=.*podchaos' "$MOCK_LOG"; then echo 'collision path attempted to delete a pre-existing CR' >&2; exit 1; fi

if run_drill default default controller-uid 0 api-pod-kill False False "$tmp/evidence-no-injection"; then echo 'missing AllInjected unexpectedly passed' >&2; exit 1; fi
grep -q 'did not report an injected condition' "$tmp/stderr" || { cat "$tmp/stderr" >&2; exit 1; }
if grep -q 'PASS' "$tmp/stdout"; then echo 'missing AllInjected emitted a PASS result' >&2; exit 1; fi
grep -q 'context=default delete raw=--raw=/apis/chaos-mesh.org/v1alpha1/namespaces/velorix-chaos/podchaos/velorix-chaos-api-pod-kill-contract-run uid=chaos-uid' "$MOCK_LOG" || { echo 'missing-injection path did not clean up its owned PodChaos CR' >&2; exit 1; }
grep -q 'NotFound' "$tmp/evidence-no-injection/experiment-after-delete.error" || { echo 'missing-injection cleanup did not confirm NotFound' >&2; exit 1; }

if run_drill default default controller-uid 0 s3-network-loss True True "$tmp/evidence-network-pass"; then :; else cat "$tmp/stderr" >&2; exit 1; fi
grep -q 'PASS scenario=s3-network-loss' "$tmp/stdout" || { cat "$tmp/stdout" >&2; exit 1; }
[ "$(cat "$tmp/evidence-network-pass/recovery-pod-uid.txt")" = 'meta-old old-uid' ] || { echo 'NetworkChaos did not preserve its original target Pod UID' >&2; exit 1; }
grep -q 'context=default delete raw=--raw=/apis/chaos-mesh.org/v1alpha1/namespaces/velorix-chaos/networkchaos/velorix-chaos-s3-network-loss-contract-run uid=chaos-uid' "$MOCK_LOG" || { echo 'NetworkChaos cleanup did not use its UID-conditioned raw DELETE' >&2; exit 1; }

if run_drill default default controller-uid 0 s3-network-loss True False "$tmp/evidence-network-no-recovery"; then echo 'NetworkChaos AllRecovered=False unexpectedly passed' >&2; exit 1; fi
grep -q 'did not report AllRecovered=True' "$tmp/stderr" || { cat "$tmp/stderr" >&2; exit 1; }
if grep -q 'PASS' "$tmp/stdout"; then echo 'NetworkChaos without rollback emitted a PASS result' >&2; exit 1; fi
grep -q 'context=default delete raw=--raw=/apis/chaos-mesh.org/v1alpha1/namespaces/velorix-chaos/networkchaos/velorix-chaos-s3-network-loss-contract-run uid=chaos-uid' "$MOCK_LOG" || { echo 'failed NetworkChaos recovery did not clean up its owned CR' >&2; exit 1; }
grep -q 'NotFound' "$tmp/evidence-network-no-recovery/experiment-after-delete.error" || { echo 'failed NetworkChaos cleanup did not confirm NotFound' >&2; exit 1; }

http_evidence="$tmp/http-evidence"
mkdir -m 700 "$http_evidence"
rm -f "$MOCK_CURL_ARGS" "$MOCK_CURL_HEADERS"
if PATH="$tmp/bin:$PATH" VELORIX_CHAOS_API_URL=http://127.0.0.1:18080 \
  VELORIX_CHAOS_API_TOKEN=contract-secret VELORIX_CHAOS_EVIDENCE_DIR="$http_evidence" \
  sh "$repo_root/scripts/check-rhiza-dory-chaos-http.sh" baseline-check >"$tmp/http.stdout" 2>"$tmp/http.stderr"; then
  :
else
  cat "$tmp/http.stderr" >&2
  exit 1
fi
if grep -q 'contract-secret' "$MOCK_CURL_ARGS"; then echo 'API token appeared in curl arguments' >&2; exit 1; fi
grep -q 'Authorization: Bearer contract-secret' "$MOCK_CURL_HEADERS" || { echo 'curl did not read authorization from the private header file' >&2; exit 1; }
if find "$http_evidence" -name '.authorization-header.*' | grep -q .; then echo 'authorization header file was not removed' >&2; exit 1; fi
phase_count=0
for phase in "$http_evidence"/*; do
  [ -d "$phase" ] || continue
  phase_count=$((phase_count + 1))
done
[ "$phase_count" = 1 ] || { echo 'HTTP probe did not create one unique phase directory' >&2; exit 1; }

mkdir -m 755 "$tmp/http-unprivate"
if PATH="$tmp/bin:$PATH" VELORIX_CHAOS_API_URL=http://127.0.0.1:18080 \
  VELORIX_CHAOS_API_TOKEN=contract-secret VELORIX_CHAOS_EVIDENCE_DIR="$tmp/http-unprivate" \
  sh "$repo_root/scripts/check-rhiza-dory-chaos-http.sh" baseline-check >"$tmp/unprivate.stdout" 2>"$tmp/unprivate.stderr"; then
  echo 'HTTP probe accepted a nonprivate evidence directory' >&2; exit 1
fi
grep -q 'mode 700' "$tmp/unprivate.stderr" || { cat "$tmp/unprivate.stderr" >&2; exit 1; }
ln -s "$http_evidence" "$tmp/http-symlink"
if PATH="$tmp/bin:$PATH" VELORIX_CHAOS_API_URL=http://127.0.0.1:18080 \
  VELORIX_CHAOS_API_TOKEN=contract-secret VELORIX_CHAOS_EVIDENCE_DIR="$tmp/http-symlink" \
  sh "$repo_root/scripts/check-rhiza-dory-chaos-http.sh" baseline-check >"$tmp/symlink.stdout" 2>"$tmp/symlink.stderr"; then
  echo 'HTTP probe accepted a symlink evidence directory' >&2; exit 1
fi
grep -q 'symbolic link' "$tmp/symlink.stderr" || { cat "$tmp/symlink.stderr" >&2; exit 1; }

grep -q 'AllRecovered=True within 180s' "$repo_root/scripts/check-rhiza-local-chaos.sh" || { echo 'NetworkChaos rollback gate missing' >&2; exit 1; }
echo 'Rhiza Chaos offline mock contract passed'

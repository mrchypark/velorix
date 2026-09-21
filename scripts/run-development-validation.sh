#!/bin/sh
# Local evidence only; deliberately independent of release/security certification.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
usage() {
    echo 'Usage: sh scripts/run-development-validation.sh [--output NEW_DIRECTORY] [--repeats 1..10]'
    echo 'Runs exact API-router functional tests, one benchmark warmup, then measured release runs.'
}
repeats=3
out=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --output|--repeats)
            [ "$#" -ge 2 ] || { usage >&2; exit 64; }
            case "$1" in --output) out=$2 ;; --repeats) repeats=$2 ;; esac
            shift 2 ;;
        *) usage >&2; exit 64 ;;
    esac
done
case "$repeats" in 1|2|3|4|5|6|7|8|9|10) ;; *) usage >&2; exit 64 ;; esac
cd "$root"
for tool in cargo rustc jq git shasum; do command -v "$tool" >/dev/null || exit 69; done
if [ -z "$out" ]; then
    mkdir -p target/development-validation
    out=$(mktemp -d "$root/target/development-validation/run.XXXXXX")
else
    # Reject even empty existing directories: evidence must belong to this invocation.
    mkdir -- "$out" || exit 73
    out=$(CDPATH= cd -- "$out" && pwd)
fi
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
started=$(date +%s)
stage=initialization
commit=$(git rev-parse HEAD)
git status --porcelain > "$out/worktree.txt"
git status --porcelain -- crates Cargo.toml Cargo.lock rust-toolchain.toml .cargo > "$out/build-inputs.txt"
git status --porcelain -- scripts/run-development-validation.sh scripts/development-functional-cases.tsv > "$out/validation-inputs.txt"
baseline=baselines/benchmark/local/pr-smoke.json
git status --porcelain -- "$baseline" > "$out/baseline-status.txt"
baseline_sha=$(shasum -a 256 "$baseline" | cut -d ' ' -f 1)
fingerprint() {
    { git rev-parse HEAD; git diff HEAD -- crates Cargo.toml Cargo.lock rust-toolchain.toml .cargo scripts/run-development-validation.sh scripts/development-functional-cases.tsv baselines/benchmark/local/pr-smoke.json;
      git ls-files --others --exclude-standard -- crates Cargo.toml Cargo.lock rust-toolchain.toml .cargo;
      shasum -a 256 "$root/scripts/run-development-validation.sh" "$root/scripts/development-functional-cases.tsv";
    } | shasum -a 256 | cut -d ' ' -f 1
}
before=$(fingerprint)
rustc --version > "$out/rustc.txt"
cargo --version > "$out/cargo.txt"
uname -sm > "$out/platform.txt"
printf '[]\n' > "$out/functional.json"
printf '[]\n' > "$out/performance.json"
printf 'null\n' > "$out/metric-statistics.json"
finish() {
    code=$?
    trap - EXIT
    after=$(fingerprint)
    if [ "$before" != "$after" ]; then code=1; stage=source_changed; fi
    ended=$(date +%s)
    status=failed
    [ "$code" -ne 0 ] || status=passed
    jq -n --arg status "$status" --arg stage "$stage" --arg commit "$commit" \
        --argjson exit_code "$code" --argjson elapsed "$((ended-started))" \
        --argjson repeats "$repeats" --arg output "$out" \
        --arg before "$before" --arg after "$after" --rawfile build_inputs "$out/build-inputs.txt" \
        --rawfile validation_inputs "$out/validation-inputs.txt" \
        --arg baseline "$baseline" --arg baseline_sha "$baseline_sha" --rawfile baseline_status "$out/baseline-status.txt" \
        --rawfile worktree "$out/worktree.txt" --rawfile rustc "$out/rustc.txt" \
        --rawfile cargo "$out/cargo.txt" --rawfile platform "$out/platform.txt" \
        --slurpfile functional "$out/functional.json" --slurpfile performance "$out/performance.json" \
        --slurpfile statistics "$out/metric-statistics.json" '
        {schema_version:1,status:$status,stage:$stage,exit_code:$exit_code,commit:$commit,
         dirty:($worktree != ""),worktree:$worktree,elapsed_seconds:$elapsed,output_directory:$output,
         build_inputs_dirty:($build_inputs != ""),build_inputs:$build_inputs,
         validation_inputs_dirty:($validation_inputs != ""),validation_inputs:$validation_inputs,
         source_fingerprint_before:$before,source_fingerprint_after:$after,
         baseline:{path:$baseline,sha256:$baseline_sha,dirty:($baseline_status != ""),worktree:$baseline_status},
         comparable_to_baseline:($build_inputs == "" and $validation_inputs == "" and $baseline_status == "" and $before==$after),
         latency_throughput_status:"diagnostic_only",cost_gate:"unchanged_local_pr_smoke_25_percent",
         environment:{rustc:$rustc,cargo:$cargo,platform:$platform,debug_info:0,incremental:false},
         evidence_scope:{functional:"in_process_public_router_functional",performance:"local_runtime_storage",
           live_rest:"not_run",live_s3:"not_run",deployment_recovery:"not_run",security:"deferred"},
         release_certified:false,requested_warmup_runs:1,
         completed_warmup_runs:([$performance[0][] | select(.warmup)]|length),
         requested_measured_runs:$repeats,
         completed_measured_runs:([$performance[0][] | select(.warmup|not)]|length),
         metric_statistics:{artifact:"metric-statistics.json",scope:"measured_runs_excluding_warmup",values:$statistics[0]},
         metric_semantics:{rows_per_second:"runtime apply only: 4096 rows across 256 batches; excludes ingest, checkpoint, recovery and HTTP",
           peak_rss_bytes:"terminal RSS sample, not measured high-water peak",
           scan_bytes:"top-level hardcoded zero, not instrumented",
           percentile_summary:"summary of per-run percentiles, not pooled percentiles"},
         functional:$functional[0],performance:$performance[0]}' > "$out/summary.json"
    echo "Validation $status: $out/summary.json"
    exit "$code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
stage=build_input_preflight
if [ -n "$(git ls-files --others --exclude-standard -- crates Cargo.toml Cargo.lock rust-toolchain.toml .cargo)" ]; then
    echo 'Untracked build inputs must be reviewed and tracked before evidence collection.' >&2
    exit 65
fi
stage=functional_manifest
jq -Rse '
  sub("\n$"; "") | split("\n") | map(split("\t")) |
  if length>0 and all(.[]; length==2 and (.[0]|test("^[a-z][a-z0-9_]*$")) and (.[1]|test("^tests::[A-Za-z_][A-Za-z0-9_]*$")))
     and ((map(.[0])|unique|length)==length) and ((map(.[1])|unique|length)==length)
  then . else error("functional manifest requires nonempty unique safe labels and exact test names") end
' "$root/scripts/development-functional-cases.tsv" > "$out/functional-cases.json"
jq -r '.[] | @tsv' "$out/functional-cases.json" > "$out/functional-cases.tsv"
stage=functional_build
cargo test --locked -p velorix-api --lib --no-run --message-format=json > "$out/functional-build.jsonl" 2> "$out/functional-build.log"
api=$(jq -rs '[.[] | select(.reason=="compiler-artifact" and .target.name=="velorix_api" and .profile.test==true and .executable!=null) | .executable] | unique | if length==1 then .[0] else error("expected one API test executable") end' "$out/functional-build.jsonl")
tab=$(printf '\t')
while IFS="$tab" read -r label test; do
    [ -n "$label" ] || continue
    stage="functional:$label"
    "$api" --list --exact "$test" > "$out/$label.list"
    grep -Fx "$test: test" "$out/$label.list" >/dev/null
    begin=$(date +%s)
    "$api" --exact "$test" --test-threads=1 > "$out/$label.log" 2>&1
    grep -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$out/$label.log" >/dev/null
    jq --arg label "$label" --arg test "$test" --arg log "$label.log" \
        --argjson seconds "$(($(date +%s)-begin))" \
        '. + [{case:$label,test:$test,status:"passed",seconds:$seconds,log:$log}]' \
        "$out/functional.json" > "$out/functional.next.json"
    mv "$out/functional.next.json" "$out/functional.json"
done < "$out/functional-cases.tsv"
stage=benchmark_build
cargo bench --locked -p velorix-runtime --bench local_incremental --no-run --message-format=json > "$out/benchmark-build.jsonl" 2> "$out/benchmark-build.log"
bench=$(jq -rs '[.[] | select(.reason=="compiler-artifact" and .target.name=="local_incremental" and .executable!=null) | .executable] | unique | if length==1 then .[0] else error("expected one benchmark executable") end' "$out/benchmark-build.jsonl")
cargo build --locked -p velorix-cli --release --message-format=json > "$out/cli-build.jsonl" 2> "$out/cli-build.log"
cli=$(jq -rs '[.[] | select(.reason=="compiler-artifact" and (.target.kind|index("bin"))!=null and .executable!=null) | .executable] | unique | if length==1 then .[0] else error("expected one CLI executable") end' "$out/cli-build.jsonl")
i=0
while [ "$i" -le "$repeats" ]; do
    stage="benchmark:$i"
    begin=$(date +%s)
    "$bench" > "$out/benchmark-$i.json" 2> "$out/benchmark-$i.log"
    jq -e 'type=="object" and (.metrics|type=="object")' "$out/benchmark-$i.json" >/dev/null
    "$cli" benchmark-validate --result "$out/benchmark-$i.json" > "$out/validate-$i.log" 2>&1
    "$cli" benchmark-gate --gate-level pr-smoke --backend local \
        --baseline "$root/baselines/benchmark/local/pr-smoke.json" --result "$out/benchmark-$i.json" \
        --max-regression-fraction 0.25 --json > "$out/gate-$i.json" 2> "$out/gate-$i.log"
    jq -e 'type=="object"' "$out/gate-$i.json" >/dev/null
    jq --argjson run "$i" --argjson seconds "$(($(date +%s)-begin))" \
        '. + [{run:$run,warmup:($run==0),status:"passed",seconds:$seconds,
          result:("benchmark-"+($run|tostring)+".json"),gate:("gate-"+($run|tostring)+".json")}]' \
        "$out/performance.json" > "$out/performance.next.json"
    mv "$out/performance.next.json" "$out/performance.json"
    i=$((i+1))
done
stage=statistics
set --
i=1
while [ "$i" -le "$repeats" ]; do set -- "$@" "$out/benchmark-$i.json"; i=$((i+1)); done
jq -s '
  def median: sort | length as $n | if $n%2==1 then .[($n/2|floor)] else (.[($n/2)-1]+.[$n/2])/2 end;
  [.[0].metrics | paths(numbers)] as $paths |
  . as $runs | reduce $paths[] as $p ({};
    ($runs | map(.metrics|getpath($p))) as $values |
    .[($p|join("."))] = {min:($values|min),median:($values|median),max:($values|max)})
' "$@" > "$out/metric-statistics.json"
stage=complete

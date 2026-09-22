#!/bin/sh
set -eu
# Re-enter this script through fake executable names; never invoke real Cargo.
case "$(basename "$0")" in
    cargo)
        case "$1" in
            --version) echo 'cargo contract-fixture'; exit 0 ;;
            test) name=velorix_api; kind=lib; binary=api ;;
            bench) name=local_incremental; kind=bench; binary=bench ;;
            build) name=velorix; kind=bin; binary=cli ;;
            *) exit 99 ;;
        esac
        case " $* " in *' --locked '*) ;; *) echo 'Missing --locked' >&2; exit 98 ;; esac
        jq -n --arg name "$name" --arg kind "$kind" --arg exe "$VALIDATION_FAKE_BIN/$binary" \
          '{reason:"compiler-artifact",target:{name:$name,kind:[$kind]},profile:{test:true},executable:$exe}'
        exit 0 ;;
    git)
        if [ "$1" = status ]; then
            case "$VALIDATION_FAKE_MODE:$*" in
                baseline_dirty:*pr-smoke.json*) echo ' M baselines/benchmark/local/pr-smoke.json'; exit 0 ;;
                build_dirty:*crates*) echo ' M crates/velorix-api/src/lib.rs'; exit 0 ;;
                validation_dirty:*scripts/run-development-validation.sh*) echo '?? scripts/development-functional-cases.tsv'; exit 0 ;;
            esac
        fi
        exec "$VALIDATION_REAL_GIT" -C "$VALIDATION_REAL_ROOT" "$@" ;;
    api)
        if [ "$1" = --list ]; then
            [ "$VALIDATION_FAKE_MODE" != missing ] || exit 0
            printf '%s: test\n' "$3"
            exit 0
        fi
        [ "$VALIDATION_FAKE_MODE" != failure ] || exit 1
        if [ "$VALIDATION_FAKE_MODE" = zero ]; then
            echo 'test result: ok. 0 passed; 0 failed; 0 ignored;'
        else
            echo 'test result: ok. 1 passed; 0 failed; 0 ignored;'
        fi
        exit 0 ;;
    bench)
        case "$VALIDATION_FAKE_MODE" in gate_first|gate_middle)
            count=0
            [ ! -f "$VALIDATION_FAKE_COUNTER" ] || count=$(cat "$VALIDATION_FAKE_COUNTER")
            printf '%s\n' "$((count+1))" > "$VALIDATION_FAKE_COUNTER"
            value=$((count*10))
            if [ "$VALIDATION_FAKE_MODE:$count" = gate_middle:2 ]; then value=200; fi
            printf '{"metrics":{"rows_per_second":%s,"object_requests":{"put_count":2}}}\n' "$value"
            exit 0 ;;
        esac
        if [ "$VALIDATION_FAKE_MODE" = invalid ]; then echo invalid; else
            printf '{"metrics":{"rows_per_second":10,"object_requests":{"put_count":2}}}\n'
        fi
        exit 0 ;;
    cli)
        [ "$VALIDATION_FAKE_MODE:$1" != validator:benchmark-validate ] || exit 1
        if [ "$1" = benchmark-gate ]; then
            rejected=false
            case "$VALIDATION_FAKE_MODE:$*" in
                gate:*) rejected=true ;;
                gate_first:*benchmark-0.json*) rejected=true ;;
                gate_middle:*benchmark-2.json*) rejected=true ;;
                gate_error:*) echo 'failed to validate benchmark baseline' >&2; exit 1 ;;
            esac
            if [ "$rejected" = true ]; then
                echo 'Error: benchmark result exceeds gate' >&2
                echo 'benchmark workload fixture metric put_count regressed by 0.500, over budget 0.250' >&2
                exit 1
            fi
            echo '{}'
        fi
        exit 0 ;;
esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/velorix-validation-contract.XXXXXX")
echo "Contract artifacts: $tmp"
mkdir "$tmp/bin"
export VALIDATION_REAL_GIT=$(command -v git)
export VALIDATION_REAL_ROOT="$root"
for name in cargo api bench cli git; do ln -s "$root/scripts/check-development-validation-contract.sh" "$tmp/bin/$name"; done
export VALIDATION_FAKE_BIN="$tmp/bin"
export PATH="$tmp/bin:$PATH"
export VALIDATION_FAKE_MODE=success
sh "$root/scripts/run-development-validation.sh" --help > "$tmp/help.log"
if sh "$root/scripts/run-development-validation.sh" --repeats 0 > "$tmp/invalid-args.log" 2>&1; then exit 1; fi
sh "$root/scripts/run-development-validation.sh" --output "$tmp/success" --repeats 2 > "$tmp/success.log" 2>&1
jq -e '.status=="passed" and .release_certified==false and (.functional|length)==17 and (.performance|length)==3' "$tmp/success/summary.json" >/dev/null
jq -e '.rows_per_second=={min:10,median:10,max:10}' "$tmp/success/metric-statistics.json" >/dev/null
jq -e '.completed_warmup_runs==1 and .completed_measured_runs==2 and .metric_statistics.values.rows_per_second.median==10 and .latency_throughput_status=="diagnostic_only" and .evidence_scope.live_rest=="not_run" and .evidence_scope.security=="deferred" and (.baseline.sha256|length)==64' "$tmp/success/summary.json" >/dev/null
if sh "$root/scripts/run-development-validation.sh" --output "$tmp/success" > "$tmp/reuse.log" 2>&1; then exit 1; fi
for mode in missing failure zero invalid validator gate_error gate; do
    export VALIDATION_FAKE_MODE=$mode
    if sh "$root/scripts/run-development-validation.sh" --output "$tmp/$mode" --repeats 1 > "$tmp/$mode.log" 2>&1; then
        echo "Expected rejection: $mode" >&2; exit 1
    fi
    jq -e '.status=="failed" and .exit_code!=0 and .release_certified==false' "$tmp/$mode/summary.json" >/dev/null
    case "$mode" in missing|failure|zero)
        [ ! -e "$tmp/$mode/benchmark-build.jsonl" ]
        jq -e '.completed_warmup_runs==0 and .metric_statistics.values==null' "$tmp/$mode/summary.json" >/dev/null ;;
    esac
done
for mode in gate_first gate_middle; do
    export VALIDATION_FAKE_MODE=$mode
    export VALIDATION_FAKE_COUNTER="$tmp/$mode.count"
    if sh "$root/scripts/run-development-validation.sh" --output "$tmp/$mode" --repeats 3 > "$tmp/$mode.log" 2>&1; then exit 1; fi
    jq -e '.status=="failed" and .failed_gate_runs==1 and .completed_measured_runs==3 and (.performance|length)==4 and .performance[3].status=="passed"' "$tmp/$mode/summary.json" >/dev/null
done
jq -e '.stage=="cost_gate:0" and .passed_measured_runs==3 and .performance[0].gate_exit_code==1 and .metric_statistics.values.rows_per_second=={min:10,median:20,max:30}' "$tmp/gate_first/summary.json" >/dev/null
jq -e '.stage=="cost_gate:2" and .passed_measured_runs==2 and .performance[2].gate_exit_code==1 and .metric_statistics.values.rows_per_second=={min:10,median:30,max:200}' "$tmp/gate_middle/summary.json" >/dev/null
jq -e '.failed_gate_runs==2 and .completed_measured_runs==1 and .passed_measured_runs==0 and .metric_statistics.values.rows_per_second.median==10' "$tmp/gate/summary.json" >/dev/null
for mode in invalid validator gate_error; do [ ! -e "$tmp/$mode/benchmark-1.json" ]; done
for mode in baseline_dirty build_dirty validation_dirty; do
    export VALIDATION_FAKE_MODE=$mode
    sh "$root/scripts/run-development-validation.sh" --output "$tmp/$mode" --repeats 1 > "$tmp/$mode.log" 2>&1
    jq -e '.status=="passed" and .comparable_to_baseline==false' "$tmp/$mode/summary.json" >/dev/null
done
jq -e '.baseline.dirty==true' "$tmp/baseline_dirty/summary.json" >/dev/null
jq -e '.build_inputs_dirty==true' "$tmp/build_dirty/summary.json" >/dev/null
jq -e '.validation_inputs_dirty==true' "$tmp/validation_dirty/summary.json" >/dev/null
mkdir -p "$tmp/fixture/scripts" "$tmp/fixture/baselines/benchmark/local"
cp "$root/scripts/run-development-validation.sh" "$tmp/fixture/scripts/"
cp "$root/baselines/benchmark/local/pr-smoke.json" "$tmp/fixture/baselines/benchmark/local/"
export VALIDATION_FAKE_MODE=success
for mode in empty_manifest duplicate_manifest unsafe_manifest; do
    case "$mode" in
        empty_manifest) printf '' ;;
        duplicate_manifest) printf 'same\ttests::first\nsame\ttests::second\n' ;;
        unsafe_manifest) printf '../escape\ttests::first\n' ;;
    esac > "$tmp/fixture/scripts/development-functional-cases.tsv"
    if sh "$tmp/fixture/scripts/run-development-validation.sh" --output "$tmp/$mode" --repeats 1 > "$tmp/$mode.log" 2>&1; then exit 1; fi
    jq -e '.status=="failed" and .stage=="functional_manifest"' "$tmp/$mode/summary.json" >/dev/null
    [ ! -e "$tmp/$mode/functional-build.jsonl" ]
done
echo 'development validation contract: passed'

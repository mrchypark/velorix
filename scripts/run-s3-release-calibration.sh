#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel)
cd "$repo_root"

fixture_mode_expected=docker
regression_budget=0.10
active_pid=
baseline_worktree=
worktree_created=0
output_dir=
run_id="$(date -u +%Y%m%dt%H%M%Sz)-$$"
status=preflight
release_passed=false
cleanup_status=not_needed
measurement_count=0
candidate_gate_count=0
cleanup_failed=0
all_candidate_gates_pass=false
candidate_commit=
baseline_parent=
historical_baseline=
historical_baseline_sha256=
cargo_target_dir=
instrumentation_commit=
instrumentation_tree=
instrumentation_author=
instrumentation_committer=
instrumentation_patch_sha256=
harness_source_sha256=
target_disk_free_kib=
gate_results='[]'
runner_name=${RUNNER_NAME:-}
runner_arch=${RUNNER_ARCH:-}
github_run_id=${GITHUB_RUN_ID:-}
github_run_attempt=${GITHUB_RUN_ATTEMPT:-}
rustc_identity=
runner_uname=
cpu_count=
fixture_identity=

usage() {
  cat <<'EOF'
Usage:
  scripts/run-s3-release-calibration.sh --candidate-commit SHA \
    --baseline-parent SHA \
    --historical-baseline ABS --output-dir ABS-EMPTY --cargo-target-dir ABS-SHARED
  scripts/run-s3-release-calibration.sh --self-test

Runs three paired predecessor/candidate release measurements sequentially on
one GitHub Actions Linux runner, derives a predecessor-only conservative
baseline, then applies the unchanged 10% CLI gate to all candidate samples.
EOF
}

die() {
  printf '%s\n' "$*" >&2
  status=failed
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

absolute_path() {
  case "$1" in
    /*) return 0 ;;
    *) return 1 ;;
  esac
}

checkout_is_clean() {
  [ -z "$(git status --porcelain --untracked-files=all)" ]
}

write_summary() {
  [ -n "$output_dir" ] && [ -d "$output_dir" ] || return 0
  jq -n \
    --arg run_id "$run_id" \
    --arg status "$status" \
    --arg candidate_commit "$candidate_commit" \
    --arg baseline_parent "$baseline_parent" \
    --arg instrumentation_commit "$instrumentation_commit" \
    --arg instrumentation_tree "$instrumentation_tree" \
    --arg instrumentation_author "$instrumentation_author" \
    --arg instrumentation_committer "$instrumentation_committer" \
    --arg instrumentation_patch_sha256 "$instrumentation_patch_sha256" \
    --arg harness_source_sha256 "$harness_source_sha256" \
    --arg historical_baseline "$historical_baseline" \
    --arg historical_baseline_sha256 "$historical_baseline_sha256" \
    --arg output_dir "$output_dir" \
    --arg cargo_target_dir "$cargo_target_dir" \
    --arg runner_name "$runner_name" \
    --arg runner_arch "$runner_arch" \
    --arg github_run_id "$github_run_id" \
    --arg github_run_attempt "$github_run_attempt" \
    --arg rustc_identity "$rustc_identity" \
    --arg runner_uname "$runner_uname" \
    --arg cpu_count "$cpu_count" \
    --arg fixture_identity "$fixture_identity" \
    --argjson target_disk_free_kib "${target_disk_free_kib:-0}" \
    --argjson measurement_count "$measurement_count" \
    --argjson candidate_gate_count "$candidate_gate_count" \
    --argjson release_passed "$release_passed" \
    --arg cleanup_status "$cleanup_status" \
    --argjson gate_results "$gate_results" \
    '{schema_version:1,run_id:$run_id,status:$status,releasebenchmark_passed:$release_passed,
      candidate_commit:$candidate_commit,baseline_production_parent:$baseline_parent,
      baseline_instrumentation_commit:$instrumentation_commit,
      baseline_instrumentation_tree:$instrumentation_tree,
      baseline_instrumentation_author:$instrumentation_author,
      baseline_instrumentation_committer:$instrumentation_committer,
      instrumentation_patch_sha256:$instrumentation_patch_sha256,
      harness_source_sha256:$harness_source_sha256,
      historical_baseline:{path:$historical_baseline,sha256:$historical_baseline_sha256},output_dir:$output_dir,
      cargo_target_dir:$cargo_target_dir,cargo_incremental:"0",
      fixture_mode:"docker",runner_platform:{github_actions:true,runner_os:"Linux",runner_name:$runner_name,
        runner_arch:$runner_arch,github_run_id:$github_run_id,github_run_attempt:$github_run_attempt,
        rustc_identity:$rustc_identity,uname:$runner_uname,cpu_count:$cpu_count},
      fixture_identity:($fixture_identity | fromjson?),target_disk_free_kib:$target_disk_free_kib,
      measurements_validated:$measurement_count,candidate_gates_evaluated:$candidate_gate_count,
      candidate_gate_results:$gate_results,cleanup_status:$cleanup_status}' \
    >"$output_dir/calibration-summary.json.tmp"
  mv "$output_dir/calibration-summary.json.tmp" "$output_dir/calibration-summary.json"
}

finish() {
  exit_code=$?
  trap - EXIT HUP INT TERM
  if [ -n "$active_pid" ]; then
    kill -TERM "$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
    active_pid=
  fi
  if [ "$worktree_created" -eq 1 ]; then
    if git worktree remove "$baseline_worktree" >/dev/null 2>&1; then
      worktree_created=0
      cleanup_status=cleaned
    else
      cleanup_failed=1
      cleanup_status=failed
      printf 'cleanup failed: could not remove owned baseline worktree %s\n' "$baseline_worktree" >&2
    fi
  elif [ "$cleanup_status" = not_needed ]; then
    cleanup_status=cleaned
  fi
  if [ "$cleanup_failed" -eq 1 ]; then
    status=cleanup_failed
    exit_code=1
  fi
  compute_release_passed "$all_candidate_gates_pass" "$measurement_count" \
    "$candidate_gate_count" "$cleanup_status"
  if [ "$release_passed" != true ]; then
    [ "$status" = cleanup_failed ] || status=candidate_gate_failed
    exit_code=1
  fi
  write_summary || exit_code=1
  exit "$exit_code"
}

compute_release_passed() {
  release_passed=false
  if [ "$1" = true ] && [ "$2" -eq 6 ] && [ "$3" -eq 3 ] && [ "$4" = cleaned ]; then
    release_passed=true
  fi
}

calibration_exit_code() {
  [ "$release_passed" = true ]
}

raw_collection_report_valid() {
  report_path=$1
  expected_commit=$2
  jq -e --arg commit "$expected_commit" '
    .commit == $commit and .measurement_complete == true and
    .result_pair_complete == true and .result_validated == true and
    .gate_evaluated == false and .gate_status == "collected" and
    .cleanup_status == "cleaned" and .release_gate_passed == false and
    .runner_exit_code == 0 and
    (.versitygw_image_digest | type == "string" and test("^sha256:[0-9a-f]{64}$")) and
    (.versitygw_image_platform | type == "string" and length > 0) and
    (.docker_host | type == "string" and length > 0)
  ' "$report_path" >/dev/null
}

forward_signal() {
  signal_name=$1
  signal_status=$2
  if [ -n "$active_pid" ]; then
    kill -"$signal_name" "$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
    active_pid=
  fi
  status=interrupted
  exit "$signal_status"
}

run_logged() {
  label=$1
  checkout=$2
  shift 2
  log_path="$output_dir/$label.log"
  (cd "$checkout" && exec env CARGO_TARGET_DIR="$cargo_target_dir" CARGO_INCREMENTAL=0 "$@") >"$log_path" 2>&1 &
  active_pid=$!
  if wait "$active_pid"; then
    active_pid=
    return 0
  else
    command_status=$?
    active_pid=
    printf '%s exited with status %s; see %s\n' "$label" "$command_status" "$log_path" >&2
    return "$command_status"
  fi
}

replace_dev_dependencies() {
  manifest=$1
  temporary="$manifest.calibration-tmp"
  awk '
    /^\[/ {
      if ($0 == "[dev-dependencies]") in_dev = 1
      else in_dev = 0
    }
    in_dev && /^libc[[:space:]]*=/ { bad = 1 }
    in_dev && /^object_store = \{ workspace = true \}$/ {
      print "libc = \"0.2\""
      print "object_store = { workspace = true, features = [\"aws\"] }"
      replaced++
      next
    }
    { print }
    END { if (replaced != 1 || bad) exit 1 }
  ' "$manifest" >"$temporary" || die 'baseline manifest dev-dependency shape differed from the approved patch'
  mv "$temporary" "$manifest"
}

prepare_minimal_lockfile() {
  lockfile="$baseline_worktree/Cargo.lock"
  expected="$output_dir/baseline-expected-Cargo.lock"
  awk '
    /^\[\[package\]\]$/ { in_runtime = 0; in_dependencies = 0 }
    /^name = "velorix-runtime"$/ { in_runtime = 1 }
    /^dependencies = \[$/ && in_runtime { in_dependencies = 1 }
    in_runtime && in_dependencies && /^ "object_store 0\.14\.2",$/ {
      print " \"libc\","; inserted++
    }
    { print }
    END { if (inserted != 1) exit 1 }
  ' "$lockfile" >"$expected" || die 'v0.5.0 lockfile did not have the expected locked runtime dependency entry'
  cargo metadata --format-version 1 --manifest-path "$baseline_worktree/Cargo.toml" \
    >"$output_dir/baseline-cargo-metadata.json" 2>"$output_dir/baseline-cargo-metadata.stderr.log" ||
    die 'cargo metadata failed while resolving the minimal baseline dev-dependency change'
  cmp -s "$expected" "$lockfile" ||
    die 'baseline Cargo.lock changed beyond adding libc to velorix-runtime; refusing dependency drift'
  jq -e '
    any(.packages[]; .name == "velorix-runtime" and
      any(.dependencies[]; .name == "libc") and
      any(.dependencies[]; .name == "object_store" and (.features | index("aws"))))
  ' "$output_dir/baseline-cargo-metadata.json" >/dev/null ||
    die 'resolved baseline metadata lacks the approved libc or object_store aws dev-dependency'
}

copy_instrumentation_sources() {
  for relative in \
    crates/velorix-runtime/benches/local_incremental.rs \
    crates/velorix-runtime/benches/materialized_output_workloads.rs \
    crates/velorix-runtime/benches/phase8_family_workloads.rs \
    crates/velorix-runtime/benches/scale_group_key_workloads.rs \
    crates/velorix-runtime/benches/scale_join_key_workloads.rs \
    crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json \
    scripts/run-s3-release-benchmark.sh \
    scripts/run-process-group.sh \
    scripts/run-process-group-src/go.mod \
    scripts/run-process-group-src/main.go \
    scripts/run-process-group-src/main_test.go; do
    mkdir -p "$baseline_worktree/$(dirname "$relative")"
    git show "$candidate_commit:$relative" >"$baseline_worktree/$relative" ||
      die "candidate commit lacks required frozen instrumentation file $relative"
    if ! cmp -s "$repo_root/$relative" "$baseline_worktree/$relative"; then
      die "candidate source checkout and candidate commit differ for $relative"
    fi
  done
  chmod 755 "$baseline_worktree/scripts/run-s3-release-benchmark.sh" \
    "$baseline_worktree/scripts/run-process-group.sh"
}

verify_allowed_baseline_diff() {
  git -C "$baseline_worktree" diff --name-only "$baseline_parent" >"$output_dir/baseline-changed-paths.txt"
  while IFS= read -r changed; do
    case "$changed" in
      Cargo.lock|crates/velorix-runtime/Cargo.toml|\
      crates/velorix-runtime/benches/local_incremental.rs|\
      crates/velorix-runtime/benches/materialized_output_workloads.rs|\
      crates/velorix-runtime/benches/phase8_family_workloads.rs|\
      crates/velorix-runtime/benches/scale_group_key_workloads.rs|\
      crates/velorix-runtime/benches/scale_join_key_workloads.rs|\
      crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json|\
      scripts/run-s3-release-benchmark.sh|scripts/run-process-group.sh|\
      scripts/run-process-group-src/go.mod|scripts/run-process-group-src/main.go|\
      scripts/run-process-group-src/main_test.go) ;;
      '') ;;
      *) die "baseline instrumentation diff contains disallowed path $changed" ;;
    esac
  done <"$output_dir/baseline-changed-paths.txt"
  git -C "$baseline_worktree" diff --quiet "$baseline_parent" -- \
    crates/velorix-core crates/velorix-storage crates/velorix-runtime/src ||
    die 'baseline contains production-source changes'
}

commit_baseline_instrumentation() {
  verify_allowed_baseline_diff
  git -C "$baseline_worktree" add Cargo.lock crates/velorix-runtime/Cargo.toml \
    crates/velorix-runtime/benches/local_incremental.rs \
    crates/velorix-runtime/benches/materialized_output_workloads.rs \
    crates/velorix-runtime/benches/phase8_family_workloads.rs \
    crates/velorix-runtime/benches/scale_group_key_workloads.rs \
    crates/velorix-runtime/benches/scale_join_key_workloads.rs \
    crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json \
    scripts/run-s3-release-benchmark.sh scripts/run-process-group.sh \
    scripts/run-process-group-src/go.mod scripts/run-process-group-src/main.go \
    scripts/run-process-group-src/main_test.go
  git -C "$baseline_worktree" diff --cached --quiet && die 'baseline instrumentation patch is unexpectedly empty'
  git -C "$baseline_worktree" diff --cached --name-only >"$output_dir/baseline-staged-paths.txt"
  git -C "$baseline_worktree" -c user.name='Velorix S3 Calibration' \
    -c user.email='velorix-s3-calibration@example.invalid' \
    commit -m 'bench: instrument v0.5.0 for paired S3 calibration' \
    >"$output_dir/baseline-instrumentation-commit.log" 2>&1 || die 'baseline instrumentation commit failed'
  instrumentation_commit=$(git -C "$baseline_worktree" rev-parse HEAD)
  instrumentation_tree=$(git -C "$baseline_worktree" rev-parse HEAD^{tree})
  instrumentation_author=$(git -C "$baseline_worktree" show -s --format='%an <%ae> %aI' HEAD)
  instrumentation_committer=$(git -C "$baseline_worktree" show -s --format='%cn <%ce> %cI' HEAD)
  git -C "$baseline_worktree" diff --binary "$baseline_parent" "$instrumentation_commit" \
    >"$output_dir/baseline-instrumentation.patch"
  instrumentation_patch_sha256=$(sha256_file "$output_dir/baseline-instrumentation.patch")
  git -C "$baseline_worktree" show "$instrumentation_commit" \
    --format= --name-only >"$output_dir/baseline-commit-paths.txt"
  if [ -n "$(git -C "$baseline_worktree" status --porcelain --untracked-files=all)" ]; then
    git -C "$baseline_worktree" status --porcelain --untracked-files=all >"$output_dir/baseline-dirty-after-commit.txt"
    die 'baseline worktree is dirty after instrumentation commit'
  fi
  git -C "$baseline_worktree" diff --quiet "$baseline_parent" "$instrumentation_commit" -- \
    crates/velorix-core crates/velorix-storage crates/velorix-runtime/src ||
    die 'committed baseline instrumentation changed production sources'
  {
    for relative in \
      crates/velorix-runtime/benches/local_incremental.rs \
      crates/velorix-runtime/benches/materialized_output_workloads.rs \
      crates/velorix-runtime/benches/phase8_family_workloads.rs \
      crates/velorix-runtime/benches/scale_group_key_workloads.rs \
      crates/velorix-runtime/benches/scale_join_key_workloads.rs \
      crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json; do
      printf '%s ' "$relative"
      sha256_file "$baseline_worktree/$relative"
    done
  } >"$output_dir/harness-source-sha256.txt"
  jq -n \
    --arg baseline_parent "$baseline_parent" \
    --arg candidate_commit "$candidate_commit" \
    --arg instrumentation_commit "$instrumentation_commit" \
    --arg instrumentation_tree "$instrumentation_tree" \
    --arg author "$instrumentation_author" \
    --arg committer "$instrumentation_committer" \
    --arg patch_sha "$instrumentation_patch_sha256" \
    --arg source_hashes "$output_dir/harness-source-sha256.txt" \
    --arg patch "$output_dir/baseline-instrumentation.patch" \
    --arg historical_baseline "$historical_baseline" \
    --arg historical_baseline_sha256 "$historical_baseline_sha256" \
    --argjson allowed_paths "$(jq -R -s 'split("\n")|map(select(length>0))' "$output_dir/baseline-commit-paths.txt")" \
    '{schema_version:1,baseline_production_parent:$baseline_parent,candidate_source_commit:$candidate_commit,
      instrumentation_commit:$instrumentation_commit,instrumentation_tree:$instrumentation_tree,
      instrumentation_author:$author,instrumentation_committer:$committer,
      instrumentation_patch:$patch,instrumentation_patch_sha256:$patch_sha,
      harness_source_hashes:$source_hashes,
      fixture_mode:"docker",allowed_changed_paths:$allowed_paths,
      historical_baseline:{path:$historical_baseline,sha256:$historical_baseline_sha256},
      production_sources_changed:false,dependencies:{dev_only:["libc","object_store aws feature"],
        lockfile_delta:"only libc added to velorix-runtime dependency list"}}' \
    >"$output_dir/baseline-provenance.json"
}

write_envelope_provenance() {
  jq --arg baseline_parent "$baseline_parent" \
    --arg instrumentation_commit "$instrumentation_commit" \
    --argjson fixture_identity "$fixture_identity" \
    --argjson result_hashes "$(jq -R -s 'split("\n")|map(select(length>0))' "$output_dir/predecessor-result-sha256.txt")" \
    '.candidate_git_head=$instrumentation_commit |
     .fixture_identity=$fixture_identity |
     .calibration={method:"componentwise conservative envelope of exactly three predecessor raw samples",
       predecessor_production_parent:$baseline_parent,
       predecessor_instrumentation_commit:$instrumentation_commit,
       predecessor_samples:$result_hashes}' \
    "$output_dir/baseline-01.json.provenance.json" >"$output_dir/predecessor-envelope.json.provenance.json"
}

timing_diagnostics_valid() {
  jq -e --slurpfile result "$1" '
    def percentile($fraction): sort | .[(((length - 1) * $fraction) | ceil)];
    .timing_diagnostics.samples_ms as $samples |
    ($samples | type == "object" and length > 0) and
    ($samples.checkpoint_publish | length == 9) and
    ($samples.checkpoint_recovery | length == 9) and
    all($samples | to_entries[];
      .key as $name | .value as $values |
      ([$result[0].workload_metrics[] | select(.name == $name)] | .[0]) as $metric |
      ($values | type == "array" and length > 0) and
      all($values[]; type == "number" and . >= 0) and
      $metric != null and
      ((($values | percentile(0.50)) - $metric.p50_ms) | fabs) < 0.000000001 and
      ((($values | percentile(0.95)) - $metric.p95_ms) | fabs) < 0.000000001)
  ' "$2" >/dev/null
}

validate_collected_measurement() {
  label=$1
  expected_commit=$2
  result="$output_dir/$label.json"
  provenance="$result.provenance.json"
  report="$result.run.json"
  [ -s "$result" ] || die "$label did not produce a result JSON"
  [ -s "$provenance" ] || die "$label did not produce provenance"
  [ -s "$report" ] || die "$label did not produce a run report"
  jq -e --arg commit "$expected_commit" '
    .schema_version == 1 and .commit == $commit and .gate_level == "release" and
    .backend == "s3_compatible" and .backend_evidence_scope == "live_or_native" and
    .workload == "s3_incremental" and
    ([.workload_metrics[].name] | length > 0)
  ' "$result" >/dev/null || die "$label result failed shape or source-commit validation"
  raw_collection_report_valid "$report" "$expected_commit" ||
    die "$label did not produce a complete validated collect-only result with clean fixture cleanup"
  jq -e --arg commit "$expected_commit" '
    .candidate_git_head == $commit and .workload == "s3_incremental" and
    (.harness_source_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.workload_profile_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
  ' "$provenance" >/dev/null || die "$label provenance lacks the expected source or fixture identity"
  timing_diagnostics_valid "$result" "$provenance" ||
    die "$label raw timing samples do not reproduce the reported percentiles"
  measurement_count=$((measurement_count + 1))
}

run_measurement() {
  label=$1
  checkout=$2
  expected_commit=$3
  result="$output_dir/$label.json"
  runner="$checkout/scripts/run-s3-release-benchmark.sh"
  [ -x "$runner" ] || die "missing executable benchmark runner in $checkout"
  (cd "$checkout" && exec env \
    CARGO_INCREMENTAL=0 \
    CARGO_TARGET_DIR="$cargo_target_dir" \
    VELORIX_S3_BENCHMARK_CARGO_TARGET_DIR="$cargo_target_dir" \
    VELORIX_S3_BENCHMARK_FIXTURE_MODE=docker \
    VELORIX_BENCHMARK_RESULT_PATH="$result" \
    "$runner" --collect-only) >"$output_dir/$label-orchestrator.log" 2>&1 &
  active_pid=$!
  if wait "$active_pid"; then runner_status=0; else runner_status=$?; fi
  active_pid=
  [ "$runner_status" -eq 0 ] || die "$label collect-only measurement failed; see $output_dir/$label-orchestrator.log"
  validate_collected_measurement "$label" "$expected_commit"
}

request_max_filter='def request_max($items):
  if $items[0] == null then null else
    reduce ($items[0] | keys[]) as $key ({};
      .[$key] = ([$items[] | .[$key]] | max))
  end;
  '

build_envelope() {
  output_path=$1
  baseline_commit=$2
  shift 2
  jq -s --arg commit "$baseline_commit" "$request_max_filter
    if length != 3 then error(\"expected exactly three predecessor samples\") else . end |
    .[0] as \$first |
    . as \$samples |
    \$first |
    .commit = \$commit |
    .metrics.rows_per_second = ([\$samples[].metrics.rows_per_second] | min) |
    .metrics.bytes_per_row = ([\$samples[].metrics.bytes_per_row] | max) |
    .metrics.put_per_gib = ([\$samples[].metrics.put_per_gib] | max) |
    .metrics.object_requests = request_max([\$samples[].metrics.object_requests]) |
    .metrics.checkpoint_p50_ms = ([\$samples[].metrics.checkpoint_p50_ms] | max) |
    .metrics.checkpoint_p95_ms = ([\$samples[].metrics.checkpoint_p95_ms] | max) |
    .metrics.recovery_p95_ms = ([\$samples[].metrics.recovery_p95_ms] | max) |
    .metrics.peak_rss_bytes = ([\$samples[].metrics.peak_rss_bytes] | max) |
    .metrics.spill_bytes = ([\$samples[].metrics.spill_bytes] | max) |
    .metrics.scan_bytes = ([\$samples[].metrics.scan_bytes] | max) |
    .workload_metrics = [\$first.workload_metrics[] as \$workload |
      \$workload + {
        p50_ms: ([\$samples[].workload_metrics[] | select(.name == \$workload.name) | .p50_ms] | max),
        p95_ms: ([\$samples[].workload_metrics[] | select(.name == \$workload.name) | .p95_ms] | max),
        object_requests: request_max([\$samples[].workload_metrics[] | select(.name == \$workload.name) | .object_requests]),
        scan_bytes: ([\$samples[].workload_metrics[] | select(.name == \$workload.name) | .scan_bytes] | max)
      }]
  " "$@" >"$output_path"
}

validate_six_source_sets() {
  first_result="$output_dir/baseline-01.json"
  first_provenance="$first_result.provenance.json"
  reference_names=$(jq -c '[.workload_metrics[].name] | sort' "$first_result")
  reference_identity=$(jq -cn \
    --slurpfile p "$first_provenance" \
    --slurpfile r "$output_dir/baseline-01.json.run.json" \
    '$p[0] | {workload_profile_sha256,harness_source_sha256} +
      {fixture_identity:{mode:"docker",image_digest:$r[0].versitygw_image_digest,
        image_platform:$r[0].versitygw_image_platform,docker_host:$r[0].docker_host}}')
  fixture_identity=$(printf '%s\n' "$reference_identity" | jq -c '.fixture_identity')
  for label in baseline-01 candidate-01 baseline-02 candidate-02 baseline-03 candidate-03; do
    result="$output_dir/$label.json"
    provenance="$result.provenance.json"
    [ "$(jq -c '[.workload_metrics[].name] | sort' "$result")" = "$reference_names" ] ||
      die "$label workload metric set differs from predecessor/candidate samples"
    jq -e '[.workload_metrics[].name] as $names | ($names | unique | length) == ($names | length)' \
      "$result" >/dev/null || die "$label contains duplicate workload metric names"
    jq -e --argjson expected "$reference_identity" --slurpfile report "$output_dir/$label.json.run.json" \
      '({workload_profile_sha256,harness_source_sha256} +
       {fixture_identity:{mode:"docker",image_digest:$report[0].versitygw_image_digest,
         image_platform:$report[0].versitygw_image_platform,docker_host:$report[0].docker_host}}) == $expected' \
      "$provenance" >/dev/null || die "$label harness, workload profile, or fixture identity differs across samples"
  done
  for pair in 01 02 03; do
    baseline_report="$output_dir/baseline-$pair.json.run.json"
    candidate_report="$output_dir/candidate-$pair.json.run.json"
    baseline_provenance="$output_dir/baseline-$pair.json.provenance.json"
    candidate_provenance="$output_dir/candidate-$pair.json.provenance.json"
    jq -e --argjson expected "$reference_identity" --slurpfile p "$baseline_provenance" \
      --slurpfile r "$baseline_report" \
      '$p[0].workload_profile_sha256 == $expected.workload_profile_sha256 and
       $p[0].harness_source_sha256 == $expected.harness_source_sha256 and
       $r[0].versitygw_image_digest == $expected.fixture_identity.image_digest and
       $r[0].versitygw_image_platform == $expected.fixture_identity.image_platform and
       $r[0].docker_host == $expected.fixture_identity.docker_host' \
      "$baseline_report" >/dev/null || die "baseline-$pair report fixture identity differs"
    jq -e --argjson expected "$reference_identity" --slurpfile p "$candidate_provenance" \
      --slurpfile r "$candidate_report" \
      '$p[0].workload_profile_sha256 == $expected.workload_profile_sha256 and
       $p[0].harness_source_sha256 == $expected.harness_source_sha256 and
       $r[0].versitygw_image_digest == $expected.fixture_identity.image_digest and
       $r[0].versitygw_image_platform == $expected.fixture_identity.image_platform and
       $r[0].docker_host == $expected.fixture_identity.docker_host' \
      "$candidate_report" >/dev/null || die "candidate-$pair report fixture identity differs"
  done
}

record_raw_result_hashes() {
  manifest="$output_dir/raw-result-sha256.txt"
  : >"$manifest"
  for label in baseline-01 candidate-01 baseline-02 candidate-02 baseline-03 candidate-03; do
    for suffix in '' '.provenance.json' '.run.json'; do
      path="$output_dir/$label.json$suffix"
      [ -f "$path" ] || die "missing retained evidence $path"
      printf '%s  %s\n' "$(sha256_file "$path")" "${path##*/}" >>"$manifest"
    done
    path="$output_dir/$label-orchestrator.log"
    [ -f "$path" ] || die "missing retained orchestrator log $path"
    printf '%s  %s\n' "$(sha256_file "$path")" "${path##*/}" >>"$manifest"
  done
  : >"$output_dir/predecessor-result-sha256.txt"
  for label in baseline-01 baseline-02 baseline-03; do
    path="$output_dir/$label.json"
    printf '%s  %s\n' "$(sha256_file "$path")" "${path##*/}" >>"$output_dir/predecessor-result-sha256.txt"
  done
}

self_test() {
  require_command jq
  require_command mktemp
  require_command shasum || require_command sha256sum
  selftest_dir=$(mktemp -d "${TMPDIR:-/tmp}/velorix-s3-calibration-selftest.XXXXXX")
  trap 'case "$selftest_dir" in "${TMPDIR:-/tmp}"/velorix-s3-calibration-selftest.*) rm -rf "$selftest_dir" ;; esac' EXIT HUP INT TERM
  for index in 1 2 3; do
    rows=$((100 - index * 10))
    bytes=$((index * 10))
    jq -n --argjson rows "$rows" --argjson bytes "$bytes" --argjson index "$index" '
      {commit:"baseline-instrumentation",gate_level:"release",backend:"s3_compatible",
       backend_evidence_scope:"live_or_native",workload:"s3_incremental",
       metrics:{rows_per_second:$rows,bytes_per_row:$bytes,put_per_gib:$bytes,
         object_requests:{put_count:$index,get_count:$index,list_count:$index,range_read_count:$index,bytes_written:$bytes,bytes_read:$bytes},
         checkpoint_p50_ms:$index,checkpoint_p95_ms:$index,recovery_p95_ms:$index,
         peak_rss_bytes:$bytes,spill_bytes:$bytes,scan_bytes:$bytes},
       workload_metrics:[{name:"sample",p50_ms:$index,p95_ms:$index,
         object_requests:{put_count:$index,get_count:$index,list_count:$index,range_read_count:$index,bytes_written:$bytes,bytes_read:$bytes},scan_bytes:$bytes}]}
    ' >"$selftest_dir/sample-$index.json"
  done
  build_envelope "$selftest_dir/envelope.json" baseline-instrumentation \
    "$selftest_dir/sample-1.json" "$selftest_dir/sample-2.json" "$selftest_dir/sample-3.json"
  jq -e '
    .metrics.rows_per_second == 70 and .metrics.bytes_per_row == 30 and
    .metrics.object_requests.put_count == 3 and
    .workload_metrics[0].p95_ms == 3 and .workload_metrics[0].scan_bytes == 30
  ' "$selftest_dir/envelope.json" >/dev/null || die 'self-test failed: predecessor componentwise envelope'
  output_dir=$selftest_dir
  baseline_parent=predecessor-production
  instrumentation_commit=baseline-instrumentation
  fixture_identity='{"mode":"docker","image_platform":"linux/amd64"}'
  printf '%s\n' '{"harness_source_sha256":"retained-harness"}' >"$output_dir/baseline-01.json.provenance.json"
  printf '%s\n' 'hash1  baseline-01.json' 'hash2  baseline-02.json' 'hash3  baseline-03.json' >"$output_dir/predecessor-result-sha256.txt"
  write_envelope_provenance
  jq -e '
    .candidate_git_head == "baseline-instrumentation" and
    .harness_source_sha256 == "retained-harness" and
    .fixture_identity.image_platform == "linux/amd64" and
    .calibration.predecessor_production_parent == "predecessor-production" and
    .calibration.predecessor_instrumentation_commit == "baseline-instrumentation" and
    (.calibration.predecessor_samples | length) == 3
  ' "$output_dir/predecessor-envelope.json.provenance.json" >/dev/null ||
    die 'self-test failed: predecessor envelope provenance'
  jq -n '{workload_metrics:[{name:"checkpoint_publish",p50_ms:5,p95_ms:9},
    {name:"checkpoint_recovery",p50_ms:5,p95_ms:9}]}' >"$selftest_dir/timing-result.json"
  jq -n '{timing_diagnostics:{samples_ms:{checkpoint_publish:[range(1;10)],
    checkpoint_recovery:[range(1;10)]}}}' >"$selftest_dir/timing-provenance.json"
  timing_diagnostics_valid "$selftest_dir/timing-result.json" "$selftest_dir/timing-provenance.json" ||
    die 'self-test failed: matching raw timing samples rejected'
  jq '.timing_diagnostics.samples_ms.checkpoint_publish[8]=10' \
    "$selftest_dir/timing-provenance.json" >"$selftest_dir/invalid-timing.json"
  if timing_diagnostics_valid "$selftest_dir/timing-result.json" "$selftest_dir/invalid-timing.json"; then
    die 'self-test failed: mismatched raw timing samples accepted'
  fi
  jq -n '{commit:"candidate",measurement_complete:true,result_pair_complete:true,
    result_validated:true,gate_evaluated:false,gate_status:"collected",
    cleanup_status:"cleaned",release_gate_passed:false,runner_exit_code:0,
    versitygw_image_digest:("sha256:" + ("a" * 64)),
    versitygw_image_platform:"linux/amd64",docker_host:"linux/amd64"}' \
    >"$selftest_dir/collected-report.json"
  raw_collection_report_valid "$selftest_dir/collected-report.json" candidate ||
    die 'self-test failed: valid collect-only report was rejected'
  for invalid_report in evaluated released dirty; do
    case "$invalid_report" in
      evaluated) jq '.gate_evaluated=true' "$selftest_dir/collected-report.json" ;;
      released) jq '.release_gate_passed=true' "$selftest_dir/collected-report.json" ;;
      dirty) jq '.cleanup_status="failed"' "$selftest_dir/collected-report.json" ;;
    esac >"$selftest_dir/invalid-report.json"
    if raw_collection_report_valid "$selftest_dir/invalid-report.json" candidate; then
      die "self-test failed: unsafe raw collection report ($invalid_report) was accepted"
    fi
  done
  compute_release_passed false 6 0 cleaned
  if [ "$release_passed" != false ] || calibration_exit_code; then
    die 'self-test failed: all-failing candidate gates did not yield a false release decision and nonzero exit'
  fi
  compute_release_passed true 5 3 cleaned
  [ "$release_passed" = false ] || die 'self-test failed: incomplete measurements yielded release success'
  compute_release_passed true 6 3 failed
  [ "$release_passed" = false ] || die 'self-test failed: cleanup failure yielded release success'
  compute_release_passed true 6 3 cleaned
  [ "$release_passed" = true ] && calibration_exit_code ||
    die 'self-test failed: three passing gates and clean complete evidence did not yield success'
  printf '%s\n' 'offline calibration self-test passed: conservative envelope and fail-closed collection/gates'
}

if [ "${1:-}" = --self-test ]; then
  [ "$#" -eq 1 ] || { usage >&2; exit 64; }
  self_test
  exit 0
fi

while [ "$#" -gt 0 ]; do
  case "$1" in
    --candidate-commit) [ "$#" -ge 2 ] || die '--candidate-commit requires a full SHA'; candidate_commit=$2; shift 2 ;;
    --baseline-parent) [ "$#" -ge 2 ] || die '--baseline-parent requires a full SHA'; baseline_parent=$2; shift 2 ;;
    --historical-baseline) [ "$#" -ge 2 ] || die '--historical-baseline requires an absolute path'; historical_baseline=$2; shift 2 ;;
    --output-dir) [ "$#" -ge 2 ] || die '--output-dir requires an absolute empty path'; output_dir=$2; shift 2 ;;
    --cargo-target-dir) [ "$#" -ge 2 ] || die '--cargo-target-dir requires an absolute shared path'; cargo_target_dir=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument $1" ;;
  esac
done

[ -n "$candidate_commit" ] && [ -n "$baseline_parent" ] && [ -n "$historical_baseline" ] &&
  [ -n "$output_dir" ] && [ -n "$cargo_target_dir" ] || { usage >&2; die 'all five arguments are required'; }
case "$candidate_commit" in *[!0-9a-f]*|'') die 'candidate commit must be full lowercase hexadecimal SHA-1' ;; esac
[ "${#candidate_commit}" -eq 40 ] || die 'candidate commit must be a full 40-character SHA-1'
case "$baseline_parent" in *[!0-9a-f]*|'') die 'baseline parent must be a full lowercase hexadecimal SHA-1' ;; esac
[ "${#baseline_parent}" -eq 40 ] || die 'baseline parent must be a full 40-character SHA-1'
[ "$baseline_parent" != "$candidate_commit" ] || die 'baseline parent must differ from candidate commit'
absolute_path "$historical_baseline" || die 'historical baseline path must be absolute'
absolute_path "$output_dir" || die 'output directory must be absolute'
absolute_path "$cargo_target_dir" || die 'Cargo target directory must be absolute'

require_command git
require_command cargo
require_command jq
require_command awk
require_command grep
require_command cmp
require_command find
if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  die 'missing sha256sum or shasum'
fi
[ "${GITHUB_ACTIONS:-}" = true ] && [ "${RUNNER_OS:-}" = Linux ] ||
  die 'calibration measurements require one GitHub Actions Linux runner'
[ -f "$historical_baseline" ] || die "historical baseline does not exist: $historical_baseline"
historical_baseline_sha256=$(sha256_file "$historical_baseline")
mkdir -p "$cargo_target_dir"
[ -d "$cargo_target_dir" ] || die "shared Cargo target directory does not exist: $cargo_target_dir"

head_commit=$(git rev-parse HEAD)
[ "$head_commit" = "$candidate_commit" ] || die 'current checkout HEAD does not match --candidate-commit'
checkout_is_clean || die 'candidate checkout must be clean before calibration'
git cat-file -e "$baseline_parent^{commit}" || die 'approved predecessor commit is unavailable locally'
git cat-file -e "$candidate_commit^{commit}" || die 'candidate commit is unavailable locally'
git merge-base --is-ancestor "$baseline_parent" "$candidate_commit" ||
  die 'baseline parent must be an ancestor of the candidate commit'

if [ -e "$output_dir" ] || [ -L "$output_dir" ]; then
  [ -d "$output_dir" ] && [ ! -L "$output_dir" ] || die 'output path must be a real directory, not a symlink'
  [ -z "$(find "$output_dir" -mindepth 1 -print -quit)" ] || die 'output directory must be empty; refusing to overwrite evidence'
else
  mkdir -p "$output_dir"
fi
trap finish EXIT
trap 'forward_signal HUP 129' HUP
trap 'forward_signal INT 130' INT
trap 'forward_signal TERM 143' TERM
cleanup_status=not_needed

target_disk_free_kib=$(df -k "$cargo_target_dir" | awk 'NR == 2 {print $4}')
case "$target_disk_free_kib" in ''|*[!0-9]*) die 'could not determine free disk space for the shared Cargo target' ;; esac
[ "$target_disk_free_kib" -ge 4194304 ] || die 'calibration requires at least 4 GiB free; refusing to lower the fixture runner guard'
rustc_identity=$(rustc --version --verbose 2>&1) || die 'could not record rustc identity'
runner_uname=$(uname -a) || die 'could not record runner platform'
cpu_count=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || printf unknown)

status=baseline_worktree
baseline_worktree="$output_dir/.baseline-worktree-$run_id"
[ ! -e "$baseline_worktree" ] && [ ! -L "$baseline_worktree" ] || die 'owned baseline worktree path already exists'
git worktree add --detach "$baseline_worktree" "$baseline_parent" >"$output_dir/worktree-create.log" 2>&1 ||
  die 'could not create detached predecessor worktree'
worktree_created=1
copy_instrumentation_sources
replace_dev_dependencies "$baseline_worktree/crates/velorix-runtime/Cargo.toml"
prepare_minimal_lockfile
verify_allowed_baseline_diff
commit_baseline_instrumentation

status=precompile
run_logged baseline-bench-precompile "$baseline_worktree" cargo bench -p velorix-runtime --bench local_incremental --no-run ||
  die 'predecessor benchmark precompile failed'
run_logged baseline-cli-precompile "$baseline_worktree" cargo build -p velorix-cli ||
  die 'predecessor CLI precompile failed'
run_logged candidate-bench-precompile "$repo_root" cargo bench -p velorix-runtime --bench local_incremental --no-run ||
  die 'candidate benchmark precompile failed'
run_logged candidate-cli-precompile "$repo_root" cargo build -p velorix-cli ||
  die 'candidate CLI precompile failed'
if [ -n "$(git -C "$baseline_worktree" status --porcelain --untracked-files=all)" ]; then
  die 'predecessor checkout changed during precompile'
fi
checkout_is_clean || die 'candidate checkout changed during precompile'

status=measurements
for pair in 01 02 03; do
  run_measurement "baseline-$pair" "$baseline_worktree" "$instrumentation_commit"
  run_measurement "candidate-$pair" "$repo_root" "$candidate_commit"
done

validate_six_source_sets
record_raw_result_hashes
harness_source_sha256=$(jq -r '.harness_source_sha256' "$output_dir/baseline-01.json.provenance.json")
jq --arg harness_sha "$harness_source_sha256" \
  --argjson fixture_identity "$fixture_identity" \
  '.harness_source_sha256=$harness_sha | .fixture_identity=$fixture_identity' "$output_dir/baseline-provenance.json" \
  >"$output_dir/baseline-provenance.json.tmp"
mv "$output_dir/baseline-provenance.json.tmp" "$output_dir/baseline-provenance.json"

status=baseline_envelope
build_envelope "$output_dir/predecessor-envelope.json" "$instrumentation_commit" \
  "$output_dir/baseline-01.json" "$output_dir/baseline-02.json" "$output_dir/baseline-03.json"
write_envelope_provenance

status=candidate_gates
all_candidate_gates_pass=true
for pair in 01 02 03; do
  log="$output_dir/candidate-$pair-calibrated-gate.log"
  if run_logged "candidate-$pair-calibrated-gate" "$repo_root" cargo run --quiet -p velorix-cli -- \
    benchmark-gate --gate-level release --backend s3-compatible \
    --baseline "$output_dir/predecessor-envelope.json" \
    --result "$output_dir/candidate-$pair.json" \
    --max-regression-fraction "$regression_budget" --json; then
    gate_status=pass
    candidate_gate_count=$((candidate_gate_count + 1))
  else
    gate_status=fail
    all_candidate_gates_pass=false
  fi
  if [ -f "$output_dir/candidate-$pair-calibrated-gate.log" ]; then
    gate_log_file="$output_dir/candidate-$pair-calibrated-gate.log"
  else
    gate_log_file="$output_dir/candidate-$pair-calibrated-gate.log"
  fi
  gate_results=$(jq -cn --argjson existing "$gate_results" --arg sample "candidate-$pair" \
    --arg status "$gate_status" --arg log "$gate_log_file" \
    '$existing + [{sample:$sample,status:$status,log:$log}]')
done

if ! checkout_is_clean; then
  all_candidate_gates_pass=false
  status=source_changed
fi

if [ "$all_candidate_gates_pass" = true ] && [ "$candidate_gate_count" -eq 3 ] && [ "$measurement_count" -eq 6 ]; then
  status=complete
else
  status=candidate_gate_failed
fi
if [ "$all_candidate_gates_pass" = true ] && [ "$candidate_gate_count" -eq 3 ] && [ "$measurement_count" -eq 6 ]; then
  exit 0
else
  exit 1
fi


#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
script_path="$script_dir/$(basename "$0")"
process_group_launcher="$script_dir/run-process-group.sh"
repo_root=$(git -C "$(dirname "$script_path")" rev-parse --show-toplevel)
cd "$repo_root"

versitygw_image='versity/versitygw:v1.8.0@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499'
aws_cli_image='amazon/aws-cli:2.17.36@sha256:b9a5694d2c5fea14c9d5d3c5ba221e8c74914960afce322aad251d83ead6f8b7'
default_baseline_path="$repo_root/baselines/benchmark/s3/release.json"
baseline_path=$default_baseline_path
run_id="$(date -u +%Y%m%dt%H%M%Sz)-$$"
resource_label="velorix-s3-release-benchmark-${run_id}"
resource_prefix="velorix-s3-release-${run_id}"
container_name="$resource_prefix"
network_name="$resource_prefix"
volume_name="$resource_prefix"
access_key=
secret_key=
bucket="velorix-${run_id}-bench"
prefix="release/${run_id}"
output_dir="${VELORIX_S3_RELEASE_BENCHMARK_DIR:-$repo_root/target/velorix-bench}"
result_path="${VELORIX_BENCHMARK_RESULT_PATH:-$output_dir/s3-release-${run_id}.json}"
provenance_path=
gate_path=
gate_log_path=
run_report_path=
runner_log_path=
runner_architecture=
active_child_pid=
run_phase=preflight
gate_evaluated=0
baseline_sha256=
baseline_companion_sha256=
baseline_companion_present=0
baseline_companion_valid=1
checkout_clean_initial=0
gate_status=not_run
created_container=0
created_network=0
created_volume=0
cleanup_failed=0
finished=0

usage() {
  cat <<'EOF'
Usage:
  scripts/run-s3-release-benchmark.sh [--baseline PATH]
  scripts/run-s3-release-benchmark.sh --self-test

Run a release-level S3-compatible benchmark against a disposable, pinned local
Versity Gateway fixture. Result and run evidence are retained under
target/velorix-bench by default. The historical baseline is never modified.

Self-tests use jq and the existing Velorix CLI; they do not start Docker.
The default baseline is historical baselines/benchmark/s3/release.json. A
calibrated baseline may be selected explicitly; the CLI still compares the
complete workload sets and applies its existing regression budget.
EOF
}

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

run_owned_command() {
  "$process_group_launcher" "$@" &
  active_child_pid=$!
  if wait "$active_child_pid"; then
    command_status=0
  else
    command_status=$?
  fi
  active_child_pid=
  return "$command_status"
}

forward_signal() {
  signal_name=$1
  signal_status=$2
  if [ -n "$active_child_pid" ]; then
    kill -"$signal_name" "$active_child_pid" 2>/dev/null || true
    wait "$active_child_pid" 2>/dev/null || true
    active_child_pid=
  fi
  exit "$signal_status"
}

checkout_is_clean() {
  git diff --quiet --ignore-submodules HEAD -- || return 1
  untracked_paths=$(git ls-files --others --exclude-standard) || return 1
  [ -z "$untracked_paths" ]
}

source_checkout_matches_head() {
  expected_head=$1
  current_head=$(git rev-parse HEAD) || return 1
  [ "$current_head" = "$expected_head" ] || return 1
  checkout_is_clean
}

file_sha256() {
  target_file=$1
  if command -v sha256sum >/dev/null 2>&1; then
    hash_output=$(sha256sum "$target_file" | awk '{print $1}') || return 1
  elif command -v shasum >/dev/null 2>&1; then
    hash_output=$(shasum -a 256 "$target_file" | awk '{print $1}') || return 1
  else
    return 1
  fi
  case "$hash_output" in
    *[!0-9a-f]* | '') return 1 ;;
  esac
  [ "${#hash_output}" -eq 64 ] || return 1
  printf '%s\n' "$hash_output"
}

baseline_inputs_unchanged() {
  [ "$(file_sha256 "$baseline_path")" = "$baseline_sha256" ] || return 1
  if [ "$baseline_companion_present" -eq 1 ]; then
    [ -f "$baseline_companion_path" ] || return 1
    [ "$(file_sha256 "$baseline_companion_path")" = "$baseline_companion_sha256" ]
  else
    [ ! -e "$baseline_companion_path" ] && [ ! -L "$baseline_companion_path" ]
  fi
}

baseline_companion_allowed() {
  candidate_baseline=$1
  candidate_companion="${candidate_baseline}.provenance.json"
  if [ -e "$candidate_companion" ] || [ -L "$candidate_companion" ]; then
    [ -f "$candidate_companion" ]
  else
    [ "$candidate_baseline" = "$default_baseline_path" ]
  fi
}

result_shape_is_valid() {
  candidate_file=$1
  expected_head=$2
  jq -e -s --arg head "$expected_head" '
    length == 1 and
    .[0].schema_version == 1 and
    .[0].commit == $head and
    .[0].gate_level == "release" and
    .[0].backend == "s3_compatible" and
    .[0].backend_evidence_scope == "live_or_native" and
    .[0].workload == "s3_incremental" and
    (.[0].metrics | type == "object") and
    (.[0].metrics.object_requests | type == "object") and
    ([.[0].workload_metrics[]?.name] as $names |
      ([
        "object_store_capability_probe",
        "ingest_envelope_validation",
        "checkpoint_publish",
        "checkpoint_recovery",
        "materialized_output_segment_pruning",
        "materialized_output_recent_k",
        "materialized_output_compaction_equivalence",
        "materialized_output_compaction_debt",
        "materialized_output_delete_vector",
        "materialized_output_ttl_vector",
        "materialized_output_late_materialization",
        "gc_dry_run_planning"
      ] - $names) as $missing |
      ($missing | length) == 0)
  ' "$candidate_file" >/dev/null 2>&1
}

provenance_matches_baseline_companion() {
  candidate_provenance=$1
  baseline_companion=$2
  jq -e -s '
    length == 2 and
    (.[0].workload_profile_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.[0].harness_source_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.[1].workload_profile_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.[1].harness_source_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    .[0].workload_profile_sha256 == .[1].workload_profile_sha256 and
    .[0].harness_source_sha256 == .[1].harness_source_sha256
  ' "$candidate_provenance" "$baseline_companion" >/dev/null 2>&1
}

require_result_shape() {
  candidate_file=$1
  expected_head=$2
  if ! result_shape_is_valid "$candidate_file" "$expected_head"; then
    jq -r --arg head "$expected_head" '
      if .commit != $head then "result commit does not match checkout HEAD" 
      elif .backend != "s3_compatible" then "result backend must be s3_compatible"
      elif .backend_evidence_scope != "live_or_native" then "result evidence scope must be live_or_native"
      elif .gate_level != "release" then "result gate level must be release"
      elif .workload != "s3_incremental" then "result workload must be s3_incremental"
      else ([
        "object_store_capability_probe",
        "ingest_envelope_validation",
        "checkpoint_publish",
        "checkpoint_recovery",
        "materialized_output_segment_pruning",
        "materialized_output_recent_k",
        "materialized_output_compaction_equivalence",
        "materialized_output_compaction_debt",
        "materialized_output_delete_vector",
        "materialized_output_ttl_vector",
        "materialized_output_late_materialization",
        "gc_dry_run_planning"
      ] - [.workload_metrics[]?.name] | "missing required S3 workload metrics: " + join(", ")) end
    ' "$candidate_file" >&2 || printf 'result JSON is invalid or lacks required release fields\n' >&2
    return 1
  fi
  run_owned_command cargo run --quiet -p velorix-cli -- benchmark-validate --result "$candidate_file"
}

run_self_tests() {
  require_command jq
  require_command cargo
  require_command go
  (cd "$script_dir/run-process-group-src" && go test ./...)
  selftest_dir=$(mktemp -d "${TMPDIR:-/tmp}/velorix-s3-release-benchmark.XXXXXX")
  trap 'rm -rf "$selftest_dir"' EXIT HUP INT TERM
  child_pid_path="$selftest_dir/child.pid"
  "$process_group_launcher" sh -c 'sleep 60 & echo $! >"$1"; wait' sh "$child_pid_path" \
    >/dev/null 2>&1 &
  group_runner_pid=$!
  attempt=0
  while [ ! -s "$child_pid_path" ] && [ "$attempt" -lt 10 ]; do
    sleep 1
    attempt=$((attempt + 1))
  done
  if [ ! -s "$child_pid_path" ]; then
    kill -TERM "$group_runner_pid" 2>/dev/null || true
    wait "$group_runner_pid" 2>/dev/null || true
    die 'self-test failed: process-group stub descendant did not start'
  fi
  child_pid=$(cat "$child_pid_path")
  kill -TERM "$group_runner_pid"
  if wait "$group_runner_pid"; then
    signal_exit=0
  else
    signal_exit=$?
  fi
  if [ "$signal_exit" -ne 143 ]; then
    die "self-test failed: process-group runner exit status after SIGTERM was $signal_exit"
  fi
  attempt=0
  while kill -0 "$child_pid" 2>/dev/null && [ "$attempt" -lt 20 ]; do
    sleep 0.1
    attempt=$((attempt + 1))
  done
  if kill -0 "$child_pid" 2>/dev/null; then
    die "self-test failed: owned process-group descendant $child_pid survived SIGTERM"
  fi

  head_commit=$(git rev-parse HEAD)
  selftest_baseline_path=$default_baseline_path
  jq --arg commit "$head_commit" '
    .commit = $commit |
    .gate_level = "release" |
    .backend = "s3_compatible" |
    .backend_evidence_scope = "live_or_native" |
    .workload = "s3_incremental" |
    .workload_metrics |= map(select(.name != "datafusion_table_scan"))
  ' "$selftest_baseline_path" >"$selftest_dir/valid-shape.json"
  require_result_shape "$selftest_dir/valid-shape.json" "$head_commit" >/dev/null

  jq -n --arg profile "$(printf '%064d' 0 | tr 0 a)" --arg harness "$(printf '%064d' 0 | tr 0 b)" \
    '{workload_profile_sha256:$profile,harness_source_sha256:$harness}' \
    >"$selftest_dir/candidate.provenance.json"
  cp "$selftest_baseline_path" "$selftest_dir/custom-baseline.json"
  if baseline_companion_allowed "$selftest_dir/custom-baseline.json"; then
    die 'self-test failed: calibrated baseline without a companion was accepted'
  fi
  cp "$selftest_dir/candidate.provenance.json" \
    "$selftest_dir/custom-baseline.json.provenance.json"
  if ! baseline_companion_allowed "$selftest_dir/custom-baseline.json"; then
    die 'self-test failed: calibrated baseline with a companion was rejected'
  fi
  baseline_path="$selftest_dir/custom-baseline.json"
  baseline_companion_path="${baseline_path}.provenance.json"
  baseline_sha256=$(file_sha256 "$baseline_path")
  baseline_companion_sha256=$(file_sha256 "$baseline_companion_path")
  baseline_companion_present=1
  if ! baseline_inputs_unchanged; then
    die 'self-test failed: unchanged baseline inputs were rejected'
  fi
  printf '\n' >>"$baseline_companion_path"
  if baseline_inputs_unchanged; then
    die 'self-test failed: changed baseline companion was accepted'
  fi
  cp "$selftest_dir/candidate.provenance.json" "$selftest_dir/matching-baseline.provenance.json"
  if ! provenance_matches_baseline_companion "$selftest_dir/candidate.provenance.json" \
    "$selftest_dir/matching-baseline.provenance.json"; then
    die 'self-test failed: matching baseline companion fingerprints were rejected'
  fi
  jq --arg harness "$(printf '%064d' 0 | tr 0 c)" \
    '.harness_source_sha256 = $harness' "$selftest_dir/candidate.provenance.json" \
    >"$selftest_dir/mismatching-baseline.provenance.json"
  if provenance_matches_baseline_companion "$selftest_dir/candidate.provenance.json" \
    "$selftest_dir/mismatching-baseline.provenance.json"; then
    die 'self-test failed: mismatched baseline companion harness fingerprint was accepted'
  fi

  mkdir "$selftest_dir/checkout"
  git -C "$selftest_dir/checkout" init -q
  git -C "$selftest_dir/checkout" config user.email runner-selftest@example.invalid
  git -C "$selftest_dir/checkout" config user.name runner-selftest
  : >"$selftest_dir/checkout/tracked"
  printf 'target/\n' >"$selftest_dir/checkout/.gitignore"
  git -C "$selftest_dir/checkout" add tracked .gitignore
  git -C "$selftest_dir/checkout" commit -qm initial
  checkout_head=$(git -C "$selftest_dir/checkout" rev-parse HEAD)
  mkdir "$selftest_dir/checkout/target"
  : >"$selftest_dir/checkout/target/evidence.json"
  if ! (cd "$selftest_dir/checkout" && source_checkout_matches_head "$checkout_head"); then
    die 'self-test failed: clean temporary checkout was rejected'
  fi
  : >"$selftest_dir/checkout/untracked"
  if (cd "$selftest_dir/checkout" && checkout_is_clean); then
    die 'self-test failed: untracked checkout content was accepted'
  fi
  rm -f "$selftest_dir/checkout/untracked"
  git -C "$selftest_dir/checkout" commit --allow-empty -qm changed-head
  if (cd "$selftest_dir/checkout" && source_checkout_matches_head "$checkout_head"); then
    die 'self-test failed: changed checkout HEAD was accepted'
  fi

  jq '.commit = ""' "$selftest_dir/valid-shape.json" >"$selftest_dir/missing-commit.json"
  if result_shape_is_valid "$selftest_dir/missing-commit.json" "$head_commit"; then
    die "self-test failed: missing result commit was accepted"
  fi
  jq '.commit = "not-the-current-head"' "$selftest_dir/valid-shape.json" >"$selftest_dir/bad-commit.json"
  if result_shape_is_valid "$selftest_dir/bad-commit.json" "$head_commit"; then
    die "self-test failed: mismatched result commit was accepted"
  fi
  jq '.backend = "local"' "$selftest_dir/valid-shape.json" >"$selftest_dir/local-as-s3.json"
  if result_shape_is_valid "$selftest_dir/local-as-s3.json" "$head_commit"; then
    die "self-test failed: local backend masquerading as S3 was accepted"
  fi
  jq '.workload_metrics |= map(select(.name != "checkpoint_recovery"))' \
    "$selftest_dir/valid-shape.json" >"$selftest_dir/missing-workload.json"
  if result_shape_is_valid "$selftest_dir/missing-workload.json" "$head_commit"; then
    die "self-test failed: missing current S3 workload was accepted"
  fi

  selftest_baseline_commit=$(jq -r '.commit' "$selftest_baseline_path")
  jq --arg commit "$selftest_baseline_commit" '.commit = $commit' \
    "$selftest_dir/valid-shape.json" >"$selftest_dir/matching-current-baseline.json"
  if ! cargo run --quiet -p velorix-cli -- benchmark-gate \
    --gate-level release \
    --backend s3-compatible \
    --baseline "$selftest_dir/matching-current-baseline.json" \
    --result "$selftest_dir/valid-shape.json" \
    --max-regression-fraction 0.10 \
    --json >"$selftest_dir/positive-gate.log" 2>&1; then
    cat "$selftest_dir/positive-gate.log" >&2
    die 'self-test failed: matching current-workload baseline did not pass the CLI gate'
  fi

  if cargo run --quiet -p velorix-cli -- benchmark-gate \
    --gate-level release \
    --backend s3-compatible \
    --baseline "$selftest_baseline_path" \
    --result "$selftest_dir/valid-shape.json" \
    --max-regression-fraction 0.10 \
    --json >"$selftest_dir/historical-baseline-mismatch.log" 2>&1; then
    die "self-test failed: comparison unexpectedly accepted the historical S3 baseline workload set"
  fi
  if ! grep -F 'datafusion_table_scan' "$selftest_dir/historical-baseline-mismatch.log" >/dev/null; then
    cat "$selftest_dir/historical-baseline-mismatch.log" >&2
    die "self-test failed: historical baseline rejection did not identify datafusion_table_scan"
  fi
  printf '%s\n' \
    'S3 release benchmark runner self-tests passed:' \
    '  missing result commit rejected' \
    '  mismatched result commit rejected' \
    '  local backend labeled as S3 rejected' \
    '  missing current S3 workload rejected' \
    '  untracked checkout content rejected' \
    '  ignored target evidence allowed' \
    '  checkout HEAD changes rejected' \
    '  SIGTERM reaped an owned process-group stub descendant' \
    '  custom calibrated baseline requires and matches a provenance companion' \
    '  baseline JSON and companion hashes detect changed inputs' \
    '  baseline companion workload and harness fingerprints checked' \
    '  matching current-workload baseline passed the CLI gate' \
    '  historical datafusion_table_scan baseline mismatch surfaced by the unmodified CLI gate'
}

finish() {
  exit_code=$?
  if [ "$finished" -eq 1 ]; then
    return
  fi
  finished=1
  trap - EXIT HUP INT TERM

  if [ "$created_container" -eq 1 ]; then
    if [ "$(docker container inspect --format '{{ index .Config.Labels "velorix.s3-release-benchmark.run" }}' "$container_name" 2>/dev/null || true)" = "$resource_label" ]; then
      if docker logs "$container_name" >"$versitygw_log_tmp" 2>&1; then
        mv "$versitygw_log_tmp" "$versitygw_log_path"
      else
        rm -f "$versitygw_log_tmp"
      fi
      if ! docker rm -f "$container_name" >/dev/null 2>&1; then
        printf 'cleanup failed: could not remove owned container %s\n' "$container_name" >&2
        cleanup_failed=1
      fi
    elif docker container inspect "$container_name" >/dev/null 2>&1; then
      printf 'cleanup refused: container %s no longer has this run ownership label\n' "$container_name" >&2
      cleanup_failed=1
    fi
  fi
  if [ "$created_network" -eq 1 ]; then
    if [ "$(docker network inspect --format '{{ index .Labels "velorix.s3-release-benchmark.run" }}' "$network_name" 2>/dev/null || true)" = "$resource_label" ]; then
      if ! docker network rm "$network_name" >/dev/null 2>&1; then
        printf 'cleanup failed: could not remove owned network %s\n' "$network_name" >&2
        cleanup_failed=1
      fi
    elif docker network inspect "$network_name" >/dev/null 2>&1; then
      printf 'cleanup refused: network %s no longer has this run ownership label\n' "$network_name" >&2
      cleanup_failed=1
    fi
  fi
  if [ "$created_volume" -eq 1 ]; then
    if [ "$(docker volume inspect --format '{{ index .Labels "velorix.s3-release-benchmark.run" }}' "$volume_name" 2>/dev/null || true)" = "$resource_label" ]; then
      if ! docker volume rm "$volume_name" >/dev/null 2>&1; then
        printf 'cleanup failed: could not remove owned data volume %s\n' "$volume_name" >&2
        cleanup_failed=1
      fi
    elif docker volume inspect "$volume_name" >/dev/null 2>&1; then
      printf 'cleanup refused: volume %s no longer has this run ownership label\n' "$volume_name" >&2
      cleanup_failed=1
    fi
  fi

  if [ -f "$result_path" ] && [ ! -e "$provenance_path" ]; then
    if ! mv "$result_path" "$partial_result_path"; then
      printf 'cleanup failed: incomplete result pair could not be moved to partial evidence\n' >&2
      cleanup_failed=1
    fi
  fi
  if [ -f "$provenance_path" ] && [ ! -e "$result_path" ]; then
    if ! mv "$provenance_path" "$partial_provenance_path"; then
      printf 'cleanup failed: incomplete provenance pair could not be moved to partial evidence\n' >&2
      cleanup_failed=1
    fi
  fi

  if [ -f "$bench_stage_path" ]; then
    if ! mv "$bench_stage_path" "$partial_result_path"; then
      printf 'cleanup failed: could not preserve partial benchmark output at %s\n' "$bench_stage_path" >&2
      cleanup_failed=1
    fi
  fi
  if [ -f "$provenance_stage_path" ]; then
    if ! mv "$provenance_stage_path" "$partial_provenance_path"; then
      printf 'cleanup failed: could not preserve partial provenance at %s\n' "$provenance_stage_path" >&2
      cleanup_failed=1
    fi
  fi

  if [ ! -f "$run_report_path" ]; then
    jq -n \
      --arg run_id "$run_id" \
      --arg commit "$head_commit" \
      --arg result "$result_path" \
      --arg provenance "$provenance_path" \
      --arg baseline "$baseline_path" \
      --arg baseline_sha256 "$baseline_sha256" \
      --arg baseline_companion "$baseline_companion_path" \
      --arg baseline_companion_sha256 "$baseline_companion_sha256" \
      --arg phase "$run_phase" \
      --arg gate_status "$gate_status" \
      --argjson gate_evaluated "$gate_evaluated" \
      --argjson exit_code "$exit_code" \
      '{schema_version:1,run_id:$run_id,commit:$commit,benchmark_result:$result,
        provenance:$provenance,baseline:{path:$baseline,sha256:$baseline_sha256,
          companion_path:$baseline_companion,companion_sha256:(if $baseline_companion_sha256 == "" then null else $baseline_companion_sha256 end)},
        measurement_complete:false,result_pair_complete:false,result_validated:false,
        gate_status:$gate_status,gate_evaluated:($gate_evaluated == 1),release_gate_passed:false,
        phase:$phase,runner_exit_code:$exit_code,credentials_redacted:true,
        cleanup_status:"pending"}' >"$run_report_tmp" &&
      mv "$run_report_tmp" "$run_report_path"
  fi

  if [ -f "$run_report_path" ]; then
    jq --arg status "$([ "$cleanup_failed" -eq 0 ] && printf cleaned || printf failed)" \
      --argjson exit_code "$exit_code" \
      --arg phase "$run_phase" \
      --arg gate_status "$gate_status" \
      --argjson gate_evaluated "$gate_evaluated" \
      '.cleanup_status = $status | .runner_exit_code = $exit_code | .last_phase = $phase |
       .gate_status = $gate_status | .gate_evaluated = ($gate_evaluated == 1) |
       .release_gate_passed = ($gate_status == "pass" and ($gate_evaluated == 1))' \
      "$run_report_path" >"$run_report_tmp" &&
      mv "$run_report_tmp" "$run_report_path"
  fi
  if [ "$cleanup_failed" -ne 0 ]; then
    exit_code=1
    printf 'owned resource cleanup incomplete; run report: %s\n' "$run_report_path" >&2
  fi
  if [ -f "$run_report_path" ]; then
    printf 'retained run report: %s\n' "$run_report_path"
    [ ! -f "$result_path" ] || printf 'retained benchmark result: %s\n' "$result_path"
    [ ! -f "$provenance_path" ] || printf 'retained benchmark provenance: %s\n' "$provenance_path"
    [ ! -f "$partial_result_path" ] || printf 'retained partial benchmark result: %s\n' "$partial_result_path"
    [ ! -f "$partial_provenance_path" ] || printf 'retained partial benchmark provenance: %s\n' "$partial_provenance_path"
    if [ -f "$gate_log_path" ]; then
      printf 'retained full gate output: %s (status %s)\n' "$gate_log_path" "$gate_status"
    fi
  fi
  exit "$exit_code"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --self-test)
      [ "$#" -eq 1 ] || { usage >&2; exit 64; }
      run_self_tests
      exit 0
      ;;
    --baseline)
      [ "$#" -ge 2 ] || { echo '--baseline requires a path' >&2; exit 64; }
      baseline_path=$2
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *) usage >&2; exit 64 ;;
  esac
done

head_commit=$(git rev-parse HEAD)
if checkout_is_clean; then
  checkout_clean_initial=1
fi
case "$head_commit" in
  [0-9a-f][0-9a-f]*) [ "${#head_commit}" -eq 40 ] || die "HEAD is not a full 40-character commit: $head_commit" ;;
  *) die "HEAD is not a full hexadecimal commit: $head_commit" ;;
esac

require_command docker
require_command cargo
require_command go
require_command jq
require_command openssl
require_command awk
require_command grep
require_command date
case "$versitygw_image" in
  versity/versitygw:v1.8.0@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499) ;;
  *) die 'internal Versity Gateway image pin mismatch' ;;
esac

case "$result_path" in
  /*) ;;
  *) result_path="$repo_root/$result_path" ;;
esac
case "$baseline_path" in
  /*) ;;
  *) baseline_path="$repo_root/$baseline_path" ;;
esac
[ -f "$baseline_path" ] || die "benchmark baseline does not exist or is not a regular file: $baseline_path"
baseline_companion_path="${baseline_path}.provenance.json"
baseline_companion_present=0
baseline_companion_allowed "$baseline_path" || baseline_companion_valid=0
if [ -e "$baseline_companion_path" ] || [ -L "$baseline_companion_path" ]; then
  baseline_companion_present=1
  if [ -f "$baseline_companion_path" ]; then
    baseline_companion_sha256=$(file_sha256 "$baseline_companion_path") || baseline_companion_valid=0
  else
    baseline_companion_valid=0
  fi
fi
baseline_sha256=$(file_sha256 "$baseline_path") || die "could not compute SHA-256 for baseline: $baseline_path"
output_dir=$(dirname -- "$result_path")
provenance_path="${result_path}.provenance.json"
gate_path="${result_path}.gate.json"
gate_copy_tmp="${gate_path}.tmp"
gate_log_path="${result_path}.gate.log"
gate_log_tmp="${gate_log_path}.tmp"
run_report_path="${result_path}.run.json"
runner_log_path="${result_path}.runner.log"
versitygw_log_path="${result_path}.versitygw.log"
runner_log_tmp="${runner_log_path}.tmp"
run_report_tmp="${run_report_path}.tmp"
versitygw_log_tmp="${versitygw_log_path}.tmp"
bench_stage_path="${result_path}.staging.${run_id}"
provenance_stage_path="${bench_stage_path}.provenance.json"
partial_result_path="${result_path}.partial.${run_id}"
partial_provenance_path="${partial_result_path}.provenance.json"
case "$result_path" in
  /*) ;;
  *) die 'benchmark result path did not resolve to an absolute path' ;;
esac
if [ -n "${VELORIX_BENCHMARK_RESULT_PATH:-}" ]; then
  case "$VELORIX_BENCHMARK_RESULT_PATH" in
    /*) ;;
    *) die 'VELORIX_BENCHMARK_RESULT_PATH must be absolute when supplied' ;;
  esac
fi
mkdir -p "$output_dir"
for artifact_path in "$result_path" "${result_path}.bench.stderr.log" "${result_path}.validate.log" \
  "$provenance_path" "$gate_path" "$gate_copy_tmp" "$gate_log_path" "$run_report_path" "$runner_log_path" \
  "$versitygw_log_path" "$runner_log_tmp" "$run_report_tmp" "$versitygw_log_tmp" "$gate_log_tmp" \
  "$bench_stage_path" "$provenance_stage_path" "$partial_result_path" "$partial_provenance_path"; do
  [ ! -e "$artifact_path" ] && [ ! -L "$artifact_path" ] || die "refusing to overwrite existing benchmark artifact: $artifact_path"
done

trap finish EXIT
trap 'forward_signal HUP 129' HUP
trap 'forward_signal INT 130' INT
trap 'forward_signal TERM 143' TERM

jq -n \
  --arg run_id "$run_id" \
  --arg commit "$head_commit" \
  --arg result "$result_path" \
  --arg provenance "$provenance_path" \
  --arg baseline "$baseline_path" \
  --arg baseline_sha256 "$baseline_sha256" \
  --arg baseline_companion "$baseline_companion_path" \
  --arg baseline_companion_sha256 "$baseline_companion_sha256" \
  --arg gate_log "$gate_log_path" \
  --arg fixture_log "$versitygw_log_path" \
  --arg validation_log "${result_path}.validate.log" \
  --arg benchmark_stderr "${result_path}.bench.stderr.log" \
  --arg runner_log "$runner_log_path" \
  --arg phase "$run_phase" \
  --argjson checkout_clean "$checkout_clean_initial" \
  '{schema_version:1,run_id:$run_id,commit:$commit,benchmark_result:$result,
    provenance:$provenance,baseline:{path:$baseline,sha256:$baseline_sha256,
      companion_path:$baseline_companion,companion_sha256:(if $baseline_companion_sha256 == "" then null else $baseline_companion_sha256 end)},
    artifacts:{gate_log:$gate_log,fixture_log:$fixture_log,validation_log:$validation_log,
      benchmark_stderr:$benchmark_stderr,runner_log:$runner_log},
    measurement_complete:false,result_pair_complete:false,result_validated:false,
    gate_evaluated:false,gate_status:"not_run",phase:$phase,
    checkout_clean_at_start:($checkout_clean == 1),credentials_redacted:true,
    cleanup_status:"pending"}' >"$run_report_tmp"
mv "$run_report_tmp" "$run_report_path"

[ "$checkout_clean_initial" -eq 1 ] || die 'release S3 benchmark requires no tracked changes or nonignored untracked files; ignored target evidence is allowed'
[ "$baseline_companion_valid" -eq 1 ] || die "custom or malformed baseline requires a regular provenance companion: $baseline_companion_path"

available_kib=$(df -k "$repo_root" | awk 'NR == 2 {print $4}')
case "$available_kib" in
  '' | *[!0-9]*) die 'could not determine free disk space for benchmark run' ;;
esac
if [ "$available_kib" -lt 4194304 ]; then
  die "release benchmark requires at least 4 GiB free; current free KiB=${available_kib}"
fi

run_phase=fixture_preflight
runner_architecture=$(docker info --format '{{.OSType}}/{{.Architecture}}' 2>/dev/null || true)
[ -n "$runner_architecture" ] || die 'Docker daemon is unavailable; no S3 benchmark fixture started'
access_key="vlx$(openssl rand -hex 16)"
secret_key="$(openssl rand -hex 32)"

if docker container inspect "$container_name" >/dev/null 2>&1 ||
  docker network inspect "$network_name" >/dev/null 2>&1 ||
  docker volume inspect "$volume_name" >/dev/null 2>&1; then
  die "refusing pre-existing Docker resource with this run name: $resource_prefix"
fi
if [ "${#access_key}" -lt 24 ] || [ "${#secret_key}" -lt 48 ]; then
  die 'failed to generate unique per-run Versity Gateway credentials'
fi

printf '%s\n' "run_id=$run_id" "commit=$head_commit" "docker_host=$runner_architecture" \
  "image=$versitygw_image" "bucket=$bucket" "prefix=$prefix" \
  "result=$result_path" >"$runner_log_tmp"
mv "$runner_log_tmp" "$runner_log_path"

docker network create --internal \
  --label "velorix.s3-release-benchmark.run=$resource_label" \
  "$network_name" >/dev/null
created_network=1
docker volume create \
  --label "velorix.s3-release-benchmark.run=$resource_label" \
  "$volume_name" >/dev/null
created_volume=1
docker run -d \
  --name "$container_name" \
  --label "velorix.s3-release-benchmark.run=$resource_label" \
  --network "$network_name" \
  -p '127.0.0.1::9000' \
  -e "ROOT_ACCESS_KEY=$access_key" \
  -e "ROOT_SECRET_KEY=$secret_key" \
  -v "$volume_name:/data" \
  "$versitygw_image" --port :9000 posix /data >/dev/null
created_container=1
run_phase=fixture_started

actual_image_digest=$(docker container inspect --format '{{.Image}}' "$container_name" |
  xargs docker image inspect --format '{{json .RepoDigests}}' |
  jq -er '[.[] | select(test("^(docker.io/)?versity/versitygw@sha256:[0-9a-f]{64}$")) | split("@")[1]][0] // error("running image has no official immutable Versity Gateway RepoDigest")')
[ "$actual_image_digest" = 'sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499' ] ||
  die "running Versity Gateway digest differs from approved pin: $actual_image_digest"
actual_image_platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$(docker container inspect --format '{{.Image}}' "$container_name")")

mapped_endpoint=$(docker port "$container_name" 9000/tcp | awk 'NR == 1 {print $1}')
case "$mapped_endpoint" in
  127.0.0.1:*) ;;
  *) die "Versity Gateway port is not bound only to loopback: $mapped_endpoint" ;;
esac
mapped_port=${mapped_endpoint##*:}
case "$mapped_port" in
  '' | *[!0-9]*) die "invalid ephemeral Versity Gateway host port: $mapped_port" ;;
esac
endpoint="http://127.0.0.1:${mapped_port}"
container_endpoint="http://${container_name}:9000"

aws_cli() {
  docker run --rm --network "$network_name" \
    -e "AWS_ACCESS_KEY_ID=$access_key" \
    -e "AWS_SECRET_ACCESS_KEY=$secret_key" \
    -e AWS_DEFAULT_REGION=us-east-1 \
    "$aws_cli_image" \
    --endpoint-url "$container_endpoint" "$@"
}

ready=0
attempt=0
while [ "$attempt" -lt 90 ]; do
  if aws_cli s3api list-buckets >/dev/null 2>&1; then
    ready=1
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done
[ "$ready" -eq 1 ] || die 'pinned local Versity Gateway did not become ready on the isolated Docker network'
if ! aws_cli s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
  aws_cli s3api create-bucket --bucket "$bucket" >/dev/null
fi

unset AWS_PROFILE AWS_SESSION_TOKEN AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_DEFAULT_PROFILE 2>/dev/null || true
export VELORIX_BENCHMARK_BACKEND=s3-compatible
export VELORIX_BENCHMARK_GATE_LEVEL=release
export VELORIX_BENCHMARK_RESULT_PATH="$bench_stage_path"
export VELORIX_BENCHMARK_EXPECTED_COMMIT="$head_commit"
export VELORIX_BENCHMARK_PROVENANCE_PATH="$provenance_stage_path"
export AWS_ENDPOINT_URL="$endpoint"
export AWS_ACCESS_KEY_ID="$access_key"
export AWS_SECRET_ACCESS_KEY="$secret_key"
export AWS_REGION=us-east-1
export VELORIX_S3_BUCKET="$bucket"
export VELORIX_S3_PREFIX="$prefix"
export CARGO_TARGET_DIR="${VELORIX_S3_BENCHMARK_CARGO_TARGET_DIR:-$repo_root/target}"

run_phase=measurement
if ! run_owned_command cargo bench -p velorix-runtime --bench local_incremental >"$bench_stage_path" 2>"${result_path}.bench.stderr.log"; then
  printf 'benchmark producer failed; stderr retained at %s\n' "${result_path}.bench.stderr.log" >&2
  exit 1
fi
require_result_shape "$bench_stage_path" "$head_commit" >"${result_path}.validate.log" 2>&1 || {
  cat "${result_path}.validate.log" >&2
  exit 1
}
test -s "$provenance_stage_path" || die "producer did not write required provenance sidecar: $provenance_stage_path"
jq -e --arg commit "$head_commit" --arg endpoint "$endpoint" --arg bucket "$bucket" --arg prefix "$prefix" '
  .schema_version == 1 and .candidate_git_head == $commit and
  .backend == "s3_compatible" and .gate_level == "release" and
  .workload == "s3_incremental" and .endpoint == $endpoint and
  .bucket == $bucket and .prefix == $prefix and
  (.adapter | type == "string" and length > 0) and
  (.workload_profile_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
  (.harness_source_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
  (.object_request_metric_scope | type == "string" and length > 0) and
  (.peak_rss_definition | type == "string" and length > 0)
' "$provenance_stage_path" >/dev/null || die "producer provenance does not match this isolated run: $provenance_stage_path"

mv "$bench_stage_path" "$result_path"
mv "$provenance_stage_path" "$provenance_path"
jq '.measurement_complete = true | .result_pair_complete = true | .result_validated = true | .phase = "measurement_validated"' \
  "$run_report_path" >"$run_report_tmp"
mv "$run_report_tmp" "$run_report_path"
run_phase=measurement_validated

gate_status=not_run
if ! source_checkout_matches_head "$head_commit"; then
  gate_status=source_changed_after_measurement
  cat >"$gate_log_tmp" <<EOF
candidate source checkout changed during measurement or no longer matches HEAD $head_commit.
No release comparison was performed. Measured result and provenance are retained for diagnosis.
EOF
  mv "$gate_log_tmp" "$gate_log_path"
  cat "$gate_log_path" >&2
elif ! baseline_inputs_unchanged; then
  gate_status=baseline_changed_during_measurement
  cat >"$gate_log_tmp" <<EOF
baseline inputs changed during measurement or no longer match their recorded SHA-256.
No release comparison was performed; baseline and companion hashes are retained in the run report.
EOF
  mv "$gate_log_tmp" "$gate_log_path"
  cat "$gate_log_path" >&2
elif [ "$baseline_companion_present" -eq 1 ] &&
  ! provenance_matches_baseline_companion "$provenance_path" "$baseline_companion_path"; then
  gate_status=baseline_incompatible
  cat >"$gate_log_tmp" <<EOF
calibrated baseline provenance is incompatible with this candidate: $baseline_companion_path
Required matching fields: workload_profile_sha256 and harness_source_sha256 (both lowercase SHA-256 hex).
No benchmark comparison was performed; no baseline workload or regression requirement was changed.
EOF
  mv "$gate_log_tmp" "$gate_log_path"
  cat "$gate_log_path" >&2
else
  run_phase=gate
  gate_evaluated=1
  if run_owned_command cargo run --quiet -p velorix-cli -- benchmark-gate \
    --gate-level release \
    --backend s3-compatible \
    --baseline "$baseline_path" \
    --result "$result_path" \
    --max-regression-fraction 0.10 \
    --json >"$gate_log_tmp" 2>&1; then
    gate_status=pass
    mv "$gate_log_tmp" "$gate_log_path"
    cp "$gate_log_path" "$gate_copy_tmp"
    mv "$gate_copy_tmp" "$gate_path"
  else
    gate_exit=$?
    gate_status="failed:$gate_exit"
    mv "$gate_log_tmp" "$gate_log_path"
    cat "$gate_log_path" >&2
  fi
fi

run_phase=gate_complete
jq \
  --arg runner_arch "$runner_architecture" \
  --arg image "$versitygw_image" \
  --arg image_digest "$actual_image_digest" \
  --arg image_platform "$actual_image_platform" \
  --arg endpoint "$endpoint" \
  --arg bucket "$bucket" \
  --arg prefix "$prefix" \
  --arg gate_status "$gate_status" \
  --arg phase "$run_phase" \
  --argjson gate_evaluated "$gate_evaluated" \
  --arg created_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '.docker_host=$runner_arch | .versitygw_image=$image |
    .versitygw_image_digest=$image_digest | .versitygw_image_platform=$image_platform |
    .endpoint=$endpoint | .bucket=$bucket | .prefix=$prefix |
    .gate_status=$gate_status | .gate_evaluated=($gate_evaluated == 1) | .phase=$phase |
    .release_gate_passed=($gate_status == "pass" and ($gate_evaluated == 1)) | .generated_at=$created_at |
    .docker_resources={container:"'"$container_name"'",network:"'"$network_name"'",volume:"'"$volume_name"'",owned_by_run:true}' \
  "$run_report_path" >"$run_report_tmp"
mv "$run_report_tmp" "$run_report_path"

if [ "$gate_status" != pass ]; then
  printf 'release run did not pass (status %s); report: %s\n' "$gate_status" "$run_report_path" >&2
  exit 1
fi
printf 'release S3 benchmark passed and was recorded in %s\n' "$gate_path"

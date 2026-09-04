#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
workflow_path="${repo_root}/.github/workflows/nightly.yml"
output_validator="${repo_root}/scripts/validate-nightly-output.sh"

test -f "$workflow_path"
test -x "$output_validator"

# The local gate is independent of optional S3 configuration. Keep these
# assertions deliberately textual so this contract can run without a YAML
# parser on the GitHub runner.
grep -Eq '^  local-nightly:$' "$workflow_path"
grep -Eq 'cargo test --workspace' "$workflow_path"
grep -Eq 'cargo bench -p velorix-runtime --bench local_incremental' "$workflow_path"
grep -Fq -- '--backend local' "$workflow_path"
grep -Eq -- '--baseline baselines/benchmark/local/pr-smoke\.json' "$workflow_path"
if awk '
    /^  local-nightly:/ { in_local = 1; next }
    /^  s3-nightly:/ { exit }
    in_local && /^    if:/ { found = 1 }
    in_local && /^        if:/ && $0 !~ /if: always\(\)/ { found = 1 }
    END { exit found }
' "$workflow_path"; then
    :
else
    echo "local-nightly must remain unconditional" >&2
    exit 1
fi
if awk '
    /^  s3-nightly:/ { exit }
    /secrets\./ { found = 1 }
    END { exit found }
' "$workflow_path"; then
    :
else
    echo "AWS secrets must not be exposed to local-nightly or workflow-level env" >&2
    exit 1
fi
if grep -Eq 'run:.*\$\{\{' "$workflow_path"; then
    echo "workflow run commands must not interpolate GitHub expressions directly" >&2
    exit 1
fi
grep -Eq '^      AWS_ACCESS_KEY_ID: \$\{\{ secrets\.AWS_ACCESS_KEY_ID \}\}$' "$workflow_path"
grep -Eq '^      AWS_SECRET_ACCESS_KEY: \$\{\{ secrets\.AWS_SECRET_ACCESS_KEY \}\}$' "$workflow_path"
if grep -Ein 'gcloud|cloud[- ]build|google-github-actions' "$workflow_path"; then
    echo "nightly workflow must not use Cloud Build or gcloud" >&2
    exit 1
fi

# A scheduled run with no S3 path and no opt-in is a successful, explicit
# skip; it must not prevent the local correctness/performance gate from run.
grep -Eq 'echo "s3_gate_skipped=true"' "$workflow_path"
grep -Eq '^      - name: Mark optional S3-compatible gate skipped$' "$workflow_path"
grep -Eq "if: steps.config.outputs.s3_gate_skipped == 'true'" "$workflow_path"
grep -Eq 'echo "live_s3_skipped=true"' "$workflow_path"
grep -Eq '^      - name: Mark live S3-compatible tests skipped$' "$workflow_path"
if grep -Eq 'nightly gate requires S3_BENCHMARK_RESULT_PATH or explicit live S3-compatible test opt-in' "$workflow_path"; then
    echo "nightly workflow still fails when the optional S3 gate is unconfigured" >&2
    exit 1
fi

# Existing benchmark evidence remains a validated input, and live execution
# remains protected by the complete credential set.
grep -Eq 'scripts/validate-nightly-output\.sh benchmark_result_path "\$S3_BENCHMARK_RESULT_PATH"' "$workflow_path"
grep -Eq 'BENCHMARK_RESULT_PATH: \$\{\{ steps.config.outputs.benchmark_result_path \}\}' "$workflow_path"
grep -Eq 'test -f "\$BENCHMARK_RESULT_PATH"' "$workflow_path"
grep -Fq -- '--result "$BENCHMARK_RESULT_PATH"' "$workflow_path"
if awk '
    /^        run: \|$/ { in_run = 1; next }
    in_run && /^        [^ ]/ { in_run = 0 }
    in_run && /\$\{\{/ { found = 1 }
    END { exit found }
' "$workflow_path"; then
    :
else
    echo "benchmark step must not interpolate a step output directly into run" >&2
    exit 1
fi
grep -Eq 'newline or carriage return' "$output_validator"
grep -Eq 'NUL or control character' "$output_validator"
grep -Eq 'for name in AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION VELORIX_S3_BUCKET;' "$workflow_path"
grep -Eq 'live S3-compatible tests requested but missing required env:' "$workflow_path"
grep -Eq "if: steps.config.outputs.live_s3_configured == 'true'" "$workflow_path"

# Exercise the validator with both valid path preservation and payloads that
# would otherwise break the line-oriented GITHUB_OUTPUT protocol.
scratch_dir="$(mktemp -d "${TMPDIR:-/tmp}/velorix-nightly-contract.XXXXXX")"
trap 'rm -rf "$scratch_dir"' EXIT
valid_output="$scratch_dir/valid.output"
GITHUB_OUTPUT="$valid_output" "$output_validator" benchmark_result_path 'target/benchmark result.json'
test "$(<"$valid_output")" = 'benchmark_result_path=target/benchmark result.json'

malicious_values=(
    $'target/benchmark\nresult.json'
    $'target/benchmark\rresult.json'
    $'target/benchmark\tresult.json'
)
for malicious_value in "${malicious_values[@]}"; do
    malicious_output="$scratch_dir/malicious.output"
    if GITHUB_OUTPUT="$malicious_output" "$output_validator" benchmark_result_path "$malicious_value"; then
        echo "nightly output validator accepted a control-byte payload" >&2
        exit 1
    fi
    test ! -e "$malicious_output"
done

echo "Nightly workflow contract passed"

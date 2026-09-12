#!/bin/sh
set -eu

repo_root=$(git rev-parse --show-toplevel)
script_path=$repo_root/scripts/run-rustfs-s3-gate.sh
doc_path=$repo_root/docs/release/1.0-readiness-checklist.md
status_path=$repo_root/docs/architecture/production-readiness-status.md
benchmark_path=$repo_root/docs/architecture/benchmark-gate-v1.md

require_text() {
    file=$1
    text=$2
    if ! grep -F -- "$text" "$file" >/dev/null; then
        echo "RustFS S3 gate contract check failed: $file lacks: $text" >&2
        exit 1
    fi
}

require_text "$script_path" 'production_gc_seed_path="${VELORIX_RUSTFS_PRODUCTION_GC_SEED_PATH:-'
require_text "$script_path" 'production_gc_run_path="${VELORIX_RUSTFS_PRODUCTION_GC_RUN_PATH:-'
require_text "$script_path" 'production_gc_validation_path="${VELORIX_RUSTFS_PRODUCTION_GC_VALIDATION_PATH:-'
require_text "$script_path" 'run_production_gc_evidence="${VELORIX_RUSTFS_RUN_PRODUCTION_GC_EVIDENCE:-0}"'
require_text "$script_path" 'RustFS production GC is unavailable: durable cross-process coordinator is required'
require_text "$script_path" 'exit 75'
require_text "$script_path" 'cargo test -p velorix-storage --test s3_compat --features s3-compat-tests'
require_text "$script_path" 'cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests'
require_text "$script_path" 'export AWS_ACCESS_KEY_ID="$rustfs_access_key"'
require_text "$script_path" 'export AWS_SECRET_ACCESS_KEY="$rustfs_secret_key"'
require_text "$script_path" 'docker run'
require_text "$script_path" 'docker rm -f'
require_text "$script_path" 's3_compatible_gc_execution_unavailable'
require_text "$script_path" 'gc-execute-s3-compatible'
require_text "$doc_path" 'blocked pending a durable cross-process'
require_text "$doc_path" 'cannot create a live run'
require_text "$status_path" 'currently blocked'
require_text "$status_path" 'no live `GcRunV1` deletion evidence is claimed'
require_text "$benchmark_path" 'gc_execution_denied'
require_text "$benchmark_path" 'must not be presented as deletion, retention, or'

if grep -F 'run_production_gc_evidence" = "2"' "$script_path" >/dev/null; then
    echo "RustFS S3 gate contract check failed: hidden GC execution mode remains" >&2
    exit 1
fi
if grep -F 'can create the live `GcRunV1`' "$status_path" >/dev/null; then
    echo "RustFS S3 gate contract check failed: stale live-GC status claim" >&2
    exit 1
fi

echo "RustFS S3 gate contract check passed"

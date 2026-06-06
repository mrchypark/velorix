#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
script_path="${repo_root}/scripts/run-rustfs-s3-gate.sh"
cli_path="${repo_root}/crates/velorix-cli/src/main.rs"
doc_path="${repo_root}/docs/release/1.0-readiness-checklist.md"
status_path="${repo_root}/docs/architecture/production-readiness-status.md"

python3 - "$script_path" "$cli_path" "$doc_path" "$status_path" <<'PY'
import sys

script_path, cli_path, doc_path, status_path = sys.argv[1:]
with open(script_path, "r", encoding="utf-8") as f:
    script = f.read()
with open(cli_path, "r", encoding="utf-8") as f:
    cli = f.read()
with open(doc_path, "r", encoding="utf-8") as f:
    doc = f.read()
with open(status_path, "r", encoding="utf-8") as f:
    status = f.read()

seed_index = script.find("gc-seed-s3-compatible-fixture")
execute_index = script.find("gc-execute-s3-compatible")
verify_index = script.find("gc-production-evidence")
validation_index = script.find("rustfs-production-gc-evidence-validate")

checks = {
    "defines production GC run artifact path": (
        "production_gc_seed_path=" in script
        and "production_gc_run_path=" in script
        and "production_gc_validation_path=" in script
        and "VELORIX_RUSTFS_PRODUCTION_GC_SEED_PATH" in script
        and "VELORIX_RUSTFS_PRODUCTION_GC_RUN_PATH" in script
        and "VELORIX_RUSTFS_PRODUCTION_GC_VALIDATION_PATH" in script
        and 'rm -f "$evidence_path" "$benchmark_path" "$production_gc_seed_path" "$production_gc_run_path" "$production_gc_path" "$production_gc_validation_path"'
        in script
    ),
    "validates fixed retain-latest policy before running GC": (
        "production_gc_retain_latest_manifests=" in script
        and "VELORIX_RUSTFS_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS must be 1 for the fixed two-checkpoint release smoke fixture"
        in script
    ),
    "fails early when local disk is too low": (
        "min_free_kib=" in script
        and "VELORIX_RUSTFS_MIN_FREE_KIB" in script
        and "preflight_disk_space" in script
        and "insufficient disk space for RustFS S3 gate" in script
        and "exit 75" in script
        and script.find("\npreflight_disk_space\npreflight_cargo_target_dir\npreflight_docker_daemon") >= 0
    ),
    "uses isolated cargo target dir for live gate builds": (
        "cargo_target_dir=" in script
        and "VELORIX_RUSTFS_CARGO_TARGET_DIR" in script
        and '${repo_root}/target/rustfs-s3-gate' in script
        and "preflight_cargo_target_dir" in script
        and 'export CARGO_TARGET_DIR="$cargo_target_dir"' in script
        and '"cargo_target_dir": cargo_target_dir' in script
        and script.find("\npreflight_disk_space\npreflight_cargo_target_dir\npreflight_docker_daemon") >= 0
    ),
    "uses non-default RustFS credentials": (
        "rustfs_access_key=" in script
        and "rustfs_secret_key=" in script
        and "refuses the rustfsadmin default credentials" in script
        and "-e RUSTFS_ACCESS_KEY=\"$rustfs_access_key\"" in script
        and "-e RUSTFS_SECRET_KEY=\"$rustfs_secret_key\"" in script
        and "export AWS_ACCESS_KEY_ID=\"$rustfs_access_key\"" in script
        and "export AWS_SECRET_ACCESS_KEY=\"$rustfs_secret_key\"" in script
        and '"credentials_redacted": True' in script
    ),
    "seeds live S3-compatible GC fixture before execute and production evidence": (
        seed_index >= 0
        and execute_index >= 0
        and verify_index >= 0
        and seed_index < execute_index < verify_index
        and 'VELORIX_S3_PREFIX="$production_gc_prefix" cargo run -p velorix-cli -- \\' in script
        and "--seed-id \"$production_gc_run_id\"" in script
        and '--json > "$production_gc_seed_path"' in script
        and "harness-precheck" not in script[seed_index:execute_index]
    ),
    "runs explicit S3-compatible GC execute before production evidence": (
        execute_index >= 0
        and verify_index >= 0
        and execute_index < verify_index
        and 'VELORIX_S3_PREFIX="$production_gc_prefix" cargo run -p velorix-cli -- \\' in script
        and "--authority-store-id \"$production_gc_authority_store_id\"" in script
        and "--retain-latest-manifests \"$production_gc_retain_latest_manifests\"" in script
        and "--run-id \"$production_gc_run_id\"" in script
        and '--json > "$production_gc_run_path"' in script
    ),
    "keeps storage harness GC precheck separate from release GC run id": (
        'export VELORIX_S3_GC_PREFIX="${production_gc_prefix}/harness-precheck"' in script
        and 'export VELORIX_S3_GC_RUN_ID="${production_gc_run_id}-harness-precheck"' in script
    ),
    "records execute and verify commands in RustFS evidence": (
        '"seed_artifact_path": production_gc_seed_path' in script
        and '"fixture_kind": "release_smoke_gc_fixture"' in script
        and '"execute_artifact_path": production_gc_run_path' in script
        and '"validation_artifact_path": production_gc_validation_path' in script
        and '"expected_min_deleted_candidates": 1' in script
        and '"seed_command": "cargo run -p velorix-cli -- gc-seed-s3-compatible-fixture --json"'
        in script
        and '"execute_command": "cargo run -p velorix-cli -- gc-execute-s3-compatible --json"'
        in script
        and '"verify_command": "cargo run -p velorix-cli -- gc-production-evidence --json"'
        in script
        and '"validation_command": "cargo run -p velorix-cli -- rustfs-production-gc-evidence-validate --json"'
        in script
        and '"retain_latest_manifests": int(production_gc_retain_latest_manifests)' in script
    ),
    "validates RustFS production GC artifact family after gate evidence is written": (
        verify_index >= 0
        and validation_index >= 0
        and verify_index < validation_index
        and '--gate-evidence "$evidence_path"' in script
        and '--seed-evidence "$production_gc_seed_path"' in script
        and '--execute-evidence "$production_gc_run_path"' in script
        and '--production-evidence "$production_gc_path"' in script
        and '--json > "$production_gc_validation_path"' in script
    ),
    "production evidence rejects no-op GC runs": (
        "gc-production-evidence requires a live GC run with at least one deleted candidate"
        in cli
        and "verified.report.deleted.is_empty()" in cli
        and "gc_production_evidence_rejects_empty_live_gc_run" in cli
    ),
    "production evidence carries persisted GC run digest for family binding": (
        "verified_gc_run_digest" in cli
        and "garbage_collection_run_digest(&verified)" in cli
        and "verified_gc_run_deleted_object_keys" in cli
        and "rustfs_production_gc_evidence_validate_rejects_stale_execute_digest" in cli
        and "rustfs_production_gc_evidence_validate_rejects_seed_id_substring_key" in cli
        and '"expected_deleted_object_keys"' in cli
        and "deleted keys do not match seeded expectation" in cli
    ),
    "release checklist documents execute then verify": (
        "gc-seed-s3-compatible-fixture" in doc
        and "gc-execute-s3-compatible" in doc
        and "rustfs-production-gc-evidence-validate" in doc
        and "This creates the live run that `gc-production-evidence` must verify" in doc
        and "at least one deleted candidate" in doc
    ),
    "status matrix documents execute then production evidence": (
        "`gc-seed-s3-compatible-fixture`" in status
        and "`gc-execute-s3-compatible`" in status
        and "`rustfs-production-gc-evidence-validate`" in status
        and "can create the live `GcRunV1`" in status
        and "`gc-production-evidence` separately emits" in status
    ),
}

failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise SystemExit(
        "RustFS S3 gate contract check failed:\n- " + "\n- ".join(failed)
    )

print("RustFS S3 gate contract check passed")
PY

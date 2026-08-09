#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
script_path="${repo_root}/scripts/run-first-e2e-readiness.sh"
doc_path="${repo_root}/docs/release/1.0-readiness-checklist.md"
status_doc_path="${repo_root}/docs/architecture/production-readiness-status.md"
workflow_path="${repo_root}/.github/workflows/release-gate.yml"

python3 - "$script_path" "$doc_path" "$status_doc_path" "$workflow_path" <<'PY'
import re
import sys

script_path, doc_path, status_doc_path, workflow_path = sys.argv[1:]
with open(script_path, "r", encoding="utf-8") as f:
    script = f.read()
with open(doc_path, "r", encoding="utf-8") as f:
    doc = f.read()
with open(status_doc_path, "r", encoding="utf-8") as f:
    status_doc = f.read()
with open(workflow_path, "r", encoding="utf-8") as f:
    workflow = f.read()

checks = {
    "release readiness completion is evidence-bound, not static matrix-bound": (
        "release-status-validate" not in doc
        and "release-status-validate" not in workflow
        and "--require-release-artifacts" in doc
        and "--require-release-artifacts" in workflow
        and "static Markdown matrix" in status_doc
        and "does not certify release readiness" in status_doc
        and "generated from the readiness report" in status_doc
    ),
    "tracks explicit lifecycle override": (
        'ingest_writer_lifecycle_evidence_explicit=0' in script
        and 'VELORIX_FIRST_E2E_INGEST_WRITER_LIFECYCLE_EVIDENCE:-' in script
    ),
    "does not require default standalone lifecycle before product run": bool(
        re.search(
            r'if \[ "\$run_product" != "1" \] \|\| \[ "\$ingest_writer_lifecycle_evidence_explicit" = "1" \]; then\s+require_file "\$ingest_writer_lifecycle_evidence"\s+fi',
            script,
        )
    ),
    "first-E2E Docker build includes local Hiqlite source context": (
        "VELORIX_HIQLITE_LOCAL_SOURCE_DIR" in script
        and "velorix-hiqlite-source=${hiqlite_local_source_dir}" in script
        and "DOCKER_BUILDKIT=1 docker build" in script
    ),
    "switches default lifecycle evidence to product output after product run": (
        'ingest_writer_lifecycle_evidence="${product_output_dir}/ingest-writer-lifecycle-attestation.json"'
        in script
    ),
    "requires resolved lifecycle evidence before report generation": bool(
        re.search(
            r'if \[ -n "\$product_evidence" \]; then\s+require_file "\$product_evidence"\s+fi\s+require_file "\$ingest_writer_lifecycle_evidence"\s+require_file "\$production_gc_validation_evidence"\s+step "Generating first-E2E readiness evidence"',
            script,
        )
    ),
    "passes resolved lifecycle evidence to readiness-report": (
        '--ingest-writer-lifecycle-evidence "$ingest_writer_lifecycle_evidence"'
        in script
    ),
    "first-E2E requires local API pod failover smoke evidence": (
        "local_api_pod_failover_smoke" in script
        and "standing-runtime-failover-smoke.json" in script
        and "product local API pod failover evidence" in script
        and "trusted_for_product_complete" in script
        and "production_wall_clock_failover_attestation" in script
        and "local_api_pod_failover_smoke" in doc
        and "standing-runtime-failover-smoke.json" in doc
    ),
    "first-E2E required product profile is enabled for Hiqlite authority time": (
        'default | logical-fencing | required)' in script
        and 'VELORIX_FIRST_E2E_PRODUCT_PROFILE=required is not supported' not in script
        and 'VELORIX_STANDING_RUNTIME_FENCING=required' in script
        and 'VELORIX_REQUIRE_HIQLITE_BACKEND_TIME=1' in script
        and 'VELORIX_FIRST_E2E_PRODUCT_PROFILE=required' in doc
        and 'backend_time_source_kind=raft_replicated_authority_time' in doc
        and 'production_bounded_failover_safe=true' in doc
    ),
    "first-E2E can pass external object-store durability attestation but rejects local RustFS attestation": (
        "VELORIX_FIRST_E2E_PRODUCT_OBJECT_STORE_DURABILITY_ATTESTATION_FILE" in script
        and "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE" in script
        and "product_object_store_durability_attestation_file" in script
        and "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE=$product_object_store_durability_attestation_file" in script
        and "cannot be used with" in script
        and "internally generated first-E2E RustFS authority" in script
        and "VELORIX_FIRST_E2E_PRODUCT_OBJECT_STORE_DURABILITY_ATTESTATION_FILE" in doc
        and "object-store-durability-attestation.json" in doc
    ),
    "first-E2E forwards ingress TLS auth attestation and product-complete evidence level": (
        "VELORIX_FIRST_E2E_PRODUCT_INGRESS_TLS_AUTH_ATTESTATION_FILE" in script
        and "VELORIX_FIRST_E2E_PRODUCT_INGRESS_ENDPOINT_URL" in script
        and "VELORIX_FIRST_E2E_PRODUCT_INGRESS_CONTROLLER" in script
        and "VELORIX_FIRST_E2E_PRODUCT_EVIDENCE_LEVEL" in script
        and "VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE=$product_ingress_tls_auth_attestation_file" in script
        and "VELORIX_INGRESS_ENDPOINT_URL=$product_ingress_endpoint_url" in script
        and "VELORIX_PRODUCT_EVIDENCE_LEVEL=$product_evidence_level" in script
        and "VELORIX_FIRST_E2E_PRODUCT_INGRESS_TLS_AUTH_ATTESTATION_FILE" in doc
        and "VELORIX_FIRST_E2E_PRODUCT_INGRESS_ENDPOINT_URL" in doc
        and "VELORIX_FIRST_E2E_PRODUCT_EVIDENCE_LEVEL=product-complete" in doc
        and "ingress-tls-auth-attestation.json" in doc
    ),
    "keeps RustFS production GC artifact family together": (
        "production_gc_seed_evidence=" in script
        and "production_gc_run_evidence=" in script
        and "production_gc_validation_evidence=" in script
        and "VELORIX_FIRST_E2E_PRODUCTION_GC_SEED_EVIDENCE" in script
        and "VELORIX_FIRST_E2E_PRODUCTION_GC_RUN_EVIDENCE" in script
        and "VELORIX_FIRST_E2E_PRODUCTION_GC_VALIDATION_EVIDENCE" in script
        and "VELORIX_RUSTFS_PRODUCTION_GC_SEED_PATH=$production_gc_seed_evidence" in script
        and "VELORIX_RUSTFS_PRODUCTION_GC_RUN_PATH=$production_gc_run_evidence" in script
        and "VELORIX_RUSTFS_PRODUCTION_GC_VALIDATION_PATH=$production_gc_validation_evidence" in script
        and 'require_file "$production_gc_seed_evidence"' in script
        and 'require_file "$production_gc_run_evidence"' in script
        and 'require_file "$production_gc_validation_evidence"' in script
        and '--rustfs-production-gc-validation-evidence "$production_gc_validation_evidence"'
        in script
        and "production_gc_validation_evidence={production_gc_validation_path}" in script
        and "rustfs_production_gc_evidence_family_validated" in script
    ),
    "shares non-default RustFS credentials with product slice": (
        "rustfs_access_key=" in script
        and "rustfs_secret_key=" in script
        and "rustfs_credentials_explicit=0" in script
        and "VELORIX_RUSTFS_ACCESS_KEY and VELORIX_RUSTFS_SECRET_KEY must be set together"
        in script
        and "VELORIX_RUSTFS_ACCESS_KEY=$rustfs_access_key" in script
        and "VELORIX_RUSTFS_SECRET_KEY=$rustfs_secret_key" in script
        and 'product_aws_access_key_id="$rustfs_access_key"'
        in script
        and 'product_aws_secret_access_key="$rustfs_secret_key"'
        in script
        and "RustFS default credentials are not allowed for first-E2E readiness"
        in script
    ),
    "fails fast when reusing RustFS product backend without matching credentials": (
        "VELORIX_FIRST_E2E_SKIP_RUSTFS=1 with RustFS product evidence requires explicit matching S3 credentials"
        in script
        and "VELORIX_FIRST_E2E_PRODUCT_AWS_ACCESS_KEY_ID and VELORIX_FIRST_E2E_PRODUCT_AWS_SECRET_ACCESS_KEY must be set together"
        in script
        and "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together for RustFS product evidence"
        in script
        and 'elif [ "$skip_rustfs" = "0" ] || [ "$rustfs_credentials_explicit" = "1" ]; then'
        in script
    ),
    "release checklist documents product lifecycle default": (
        "target/velorix-product/ingest-writer-lifecycle-attestation.json" in doc
        and "VELORIX_FIRST_E2E_INGEST_WRITER_LIFECYCLE_EVIDENCE" in doc
        and "target/release-evidence/rustfs-production-gc-seed.json" in doc
        and "target/release-evidence/rustfs-production-gc-run.json" in doc
        and "target/release-evidence/rustfs-production-gc-validation.json" in doc
        and "--rustfs-production-gc-validation-evidence" in doc
        and "non-default RustFS credentials" in doc
        and "requires explicit matching S3 credentials" in doc
    ),
}

failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise SystemExit(
        "first-E2E readiness contract check failed:\n- " + "\n- ".join(failed)
    )

print("first-E2E readiness contract check passed")
PY

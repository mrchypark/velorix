# Production Readiness Status

This document does not certify release readiness. It exists to point readers at
the evidence-bound release gate.

Release readiness is generated from the readiness report produced by:

```bash
cargo run -p velorix-cli -- readiness-report \
  --evidence "$READINESS_EVIDENCE_PATH" \
  --require-release-artifacts \
  --dependency-governance-evidence target/dependency-governance/local-dependency-governance-evidence.json \
  --dependency-governance-manifest dependency-governance.json \
  --release-commit "$RELEASE_COMMIT" \
  --s3-release-benchmark-gate-evidence target/release-evidence/s3-release-benchmark-gate.json \
  --production-gc-run-evidence "$PRODUCTION_GC_RELEASE_PATH" \
  --rustfs-production-gc-validation-evidence "$RUSTFS_PRODUCTION_GC_RECHECKED_PATH" \
  --ingest-writer-lifecycle-evidence "$INGEST_WRITER_LIFECYCLE_RELEASE_PATH" \
  --standing-runtime-product-evidence "$STANDING_RUNTIME_PRODUCT_RELEASE_PATH" \
  --s3-checkpoint-fault-matrix-evidence "$VELORIX_S3_CHECKPOINT_FAULT_MATRIX_EVIDENCE_PATH" \
  --hiqlite-restore-drill-evidence "$VELORIX_HIQLITE_RESTORE_DRILL_EVIDENCE_PATH" \
  --upgrade-rollback-repair-gc-fault-matrix-evidence "$VELORIX_UPGRADE_ROLLBACK_REPAIR_GC_FAULT_MATRIX_EVIDENCE_PATH" \
  --query-output-isolation-evidence "$VELORIX_QUERY_OUTPUT_ISOLATION_EVIDENCE_PATH" \
  --security-release-provenance-evidence "$VELORIX_SECURITY_RELEASE_PROVENANCE_EVIDENCE_PATH" \
  --remaining-release-readiness-evidence "$VELORIX_REMAINING_RELEASE_READINESS_EVIDENCE_PATH" \
  --json
```

The static Markdown matrix was removed because it could say `complete` while
required artifacts were missing, local-only, stale, or failed their artifact
requirements. The release decision must come from the generated readiness report
and its concrete evidence artifacts, not from this file.

For RustFS production GC evidence, `gc-seed-s3-compatible-fixture` prepares
the retired-checkpoint fixture and `gc-execute-s3-compatible` can create the live `GcRunV1`
on the same authority. `gc-production-evidence` separately emits the
verification artifact, and `rustfs-production-gc-evidence-validate` binds
the seed, execute, and production evidence to one authority, run id, retention
policy, and persisted-run digest.

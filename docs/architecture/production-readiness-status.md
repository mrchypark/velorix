# Production Readiness Status

Current CI baseline (2026-10-30): [run 41020731224](https://github.com/mrchypark/velorix/actions/runs/41020731224)
completed all eight reported jobs: formatting, script contracts, MSRV,
clippy, workspace tests, Rhiza backend, dependency deny, and PR benchmark smoke.
This is CI evidence for baseline `c6222abd054858109c90a63a061db698fa9fc9e8`
only. It does not establish complete SQL-family coverage, production readiness,
or the representative S3 release benchmark gate. No release-readiness report
or live release evidence is asserted by this document.

The current machine-generated readiness contract is implemented by
`crates/velorix-cli/src/main.rs` (`readiness-report`) and
`.github/workflows/release-gate.yml`; local CI success does not satisfy those
artifact gates. Canonical product requirements: [specification.md](../specification.md).

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
  --versitygw-production-gc-validation-evidence "$VERSITYGW_PRODUCTION_GC_RECHECKED_PATH" \
  --ingest-writer-lifecycle-evidence "$INGEST_WRITER_LIFECYCLE_RELEASE_PATH" \
  --standing-runtime-product-evidence "$STANDING_RUNTIME_PRODUCT_RELEASE_PATH" \
  --s3-checkpoint-fault-matrix-evidence "$VELORIX_S3_CHECKPOINT_FAULT_MATRIX_EVIDENCE_PATH" \
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

Fresh Versity Gateway production evidence is pending; historical RustFS evidence
must not be relabeled as a successful Versity Gateway run.

Versity Gateway production GC evidence is currently blocked: `gc-execute-s3-compatible`
must not be treated as available while the durable cross-process coordinator is
absent, and no live `GcRunV1` deletion evidence is claimed. The seed/planning
fixture, historical evidence reads, and retention-debt accounting remain
available. Once coordination is implemented and accepted,
`gc-production-evidence` can emit the verification artifact and
`versitygw-production-gc-evidence-validate` can bind seed, execute, and production
evidence to one authority, run id, retention policy, and persisted-run digest.

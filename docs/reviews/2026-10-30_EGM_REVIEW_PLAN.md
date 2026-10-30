# EGM review plan — Velorix current implementation specification

## Review target and readiness

- **Target:** [`docs/specification.md`](../specification.md), current implementation draft as of `c6222abd054858109c90a63a061db698fa9fc9e8` (2026-10-30).
- **Feature range:** the full document, lines 1 through end.
- **Detailed implementation range:** N/A. The specification records as-built behavior, gaps, and future owner decisions; it contains no detailed implementation plan.
- **Implementation-readiness review:** N/A until owners select a future design and create a detailed implementation plan. Do not invent tasks or mark readiness from this as-built draft.
- **Execution status:** user selected **Preset 2 — Consolidated report with all review rubrics (uninterrupted)**. Stages 5–8 completed, including all three Stage 7 reviewers and a fresh Stage 8 retry. Read-manifest follow-ups completed. Stage 9 is N/A, not a pass. See the [consolidated report](egm-2026-10-30/consolidated_review_report.md) for findings, repairs, execution failures, and evidence limits. No review-loop convergence is claimed.
- **Frozen pre-review target:** [`specification-before-review.md`](egm-2026-10-30/specification-before-review.md), byte-for-byte snapshot, SHA-256 `1b5ed4d6d2a1302fe4dbb75cd7235094c499d053459958904b463ea038e58065`, lines 1–144, baseline `c6222abd054858109c90a63a061db698fa9fc9e8`.
- **Report directory:** [`docs/reviews/egm-2026-10-30/`](egm-2026-10-30/README.md). Actual reviewer reports and implementation-readiness applicability are recorded here.

## Four EGM execution-mode choices

The user selected option 2 below. The other choices are retained to document the available EGM presets:

1. **Interactive review with all review rubrics** — run zoom-out, comprehension, critique, intermediate zoom-out, then implementation-readiness only if a detailed plan exists; pause at the required stage gates.
2. **Consolidated report with all review rubrics** — run the complete applicable flow without stage pauses and consolidate findings.
3. **Feature spec only** — review the entire specification through feature-spec stages and stop before implementation readiness.
4. **Implementation plan only** — evaluate a detailed implementation range. It is currently unavailable because this target has no implementation plan; readiness remains N/A until owners select a design and add that plan.

Use the selected EGM workflow's required fresh-context Goldfish isolation. A same-context self-review must not be represented as a Goldfish review. If the runtime cannot provide the required isolated reviewers, record that limitation and do not simulate them.

## Reviewer personas and standards

Paths below refer to the installed EGM plugin bundle. Each reviewer must read its persona and the assigned standard before reporting.

- Zoom-out stages: `agents/zoom_out.md`, applying `resources/document_standards/zoom-out-standard.md`.
- Comprehension stage: `agents/comprehension.md`; test whether a fresh reader can accurately explain the specification without importing unstated context.
- Critique stage: `agents/design_critic.md` and `agents/structural.md` with `resources/document_standards/feature-spec-standard.md`; use `agents/style.md` for style findings.
- Synthesis: `agents/synthesizer.md` after the relevant isolated reviewer reports are complete.
- Implementation readiness: only after a detailed plan exists, use `agents/implementation_readiness.md`, `agents/codebase_details.md`, and `agents/style.md` with `resources/document_standards/implementation-readiness-standard.md` for the applicable reviewers.

## Reading list for reviewers

Read the target and its linked source/docs. Add these baseline documents to establish boundary context:

- [`docs/architecture/supported-sql.md`](../architecture/supported-sql.md)
- [`docs/development/generic-view-pipeline.md`](../development/generic-view-pipeline.md)
- [`docs/architecture/ingest-admission-contract.md`](../architecture/ingest-admission-contract.md)
- [`docs/architecture/storage-contract.md`](../architecture/storage-contract.md)
- [`docs/architecture/relation-contract-v1.md`](../architecture/relation-contract-v1.md)
- [`docs/architecture/ingest-envelope-v1.md`](../architecture/ingest-envelope-v1.md)
- [`docs/architecture/checkpoint-recovery-index-v1.md`](../architecture/checkpoint-recovery-index-v1.md)
- [`docs/architecture/checkpoint-state-arrow-v1.md`](../architecture/checkpoint-state-arrow-v1.md)
- [`docs/architecture/datafusion-resource-policy-v1.md`](../architecture/datafusion-resource-policy-v1.md)
- [`docs/development/materialized-query-cache.md`](../development/materialized-query-cache.md)
- [`docs/architecture/production-readiness-status.md`](../architecture/production-readiness-status.md)
- [`docs/release/1.0-readiness-checklist.md`](../release/1.0-readiness-checklist.md)
- [`docs/reviews/2026-10-30_DRIFT_REPORT.md`](2026-10-30_DRIFT_REPORT.md)

Core code entry points include `crates/velorix-api/src/lib.rs`, `view_admission.rs`, `ingest_epoch.rs`, `demand_materialization.rs`, `query_serving.rs`, `view_dependencies.rs`, `crates/velorix-core/src/relation.rs`, `view_plan/mod.rs`, `arrow_state.rs`, `crates/velorix-runtime/src/runtime_contract.rs`, `materialized_view_runtime.rs`, `crates/velorix-storage/src/ingest_envelope.rs`, `log.rs`, `manifest.rs`, `checkpoint_index.rs`, `crates/velorix-api/src/checkpoint_publication.rs`, and Meta configuration/snapshot code under `crates/velorix-meta/src/`.

## Review questions to stress-test

- Does the document distinguish the product requirement excluding public input DELETE from the as-built catalog-gated DELETE/UPDATE/UPSERT capabilities, while leaving UPDATE/UPSERT product policy unselected?
- Can readers distinguish the fixed `ack_mode: materialized` wire label, the `materialization.status` response, demand-managed view progress, and eager view completion?
- Are public view admission, query-time SQL over published output, and internal/dev table scans separate in every section?
- Are all four checkpoint/snapshot protocols assigned separate bodies and authorities, without implying Meta is the authority for storage manifests or that a cache is durable?
- Do every numeric default, SQL family, experimental flag, recovery claim, and CI statement have a source-backed scope and a clear evidence limit?
- Are open owner decisions explicit, with no proposed implementation plan or unapproved architecture choice hidden in the prose?

## Expected outputs after mode selection

Write reports from parent raw results under `docs/reviews/egm-2026-10-30/` using the filenames listed in that directory's README. Preserve the raw reviewer wording and provenance, exact target ranges, incomplete/failed runs, and evidence limits. Do not mark a stage complete until its reviewer result is received and verified. Implementation readiness remains N/A without a detailed plan. The integrated source-drift report is not a substitute for any Goldfish stage.

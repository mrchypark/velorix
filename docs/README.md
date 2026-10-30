# Velorix documentation index

This index covers the 68 existing documentation files in the 2026-10-30 reconciliation scope. “Current” means source-compared at baseline `c6222abd054858109c90a63a061db698fa9fc9e8`; it does not mean production-qualified or freshly test-run. Historical reports retain the date/revision of their evidence. Proposed designs remain proposals except for explicitly linked narrow implementations.

Canonical entry points:

- [Current implementation specification](specification.md) — product behavior, implementation boundaries, gaps, and open owner decisions.
- [Public schema/input validation](reviews/2026-10-30_PUBLIC_RELATION_INPUT_VALIDATION.md) — business-only declarations, automatic internal multiplicity, and local regression results.
- [Supported SQL admission contract](architecture/supported-sql.md) — exact view SQL family grammar and admission evidence.
- [Repository README](../README.md) — project overview and local navigation.
- [How-to guide](how-to.md) — local development startup and examples.
- [Integrated drift report, 2026-10-30](reviews/2026-10-30_DRIFT_REPORT.md) — differences recorded across all 68 scoped documents.
- [EGM review plan](reviews/2026-10-30_EGM_REVIEW_PLAN.md) — target, reading set, reviewer rubrics, and selected execution mode.
- [Completed EGM consolidated review](reviews/egm-2026-10-30/consolidated_review_report.md) — applicable stage results, documentation repairs, remaining product work, and evidence limits.

The index includes document paths from the A/B/C/D inventories. “Disposition” records whether the existing document was reconciled, compared without drift, retained as historical, or remains partly/unverified. “Role” identifies how to use it; neither column claims a formal Goldfish review.

| Document | Group | Role | Disposition |
| --- | --- | --- | --- |
| [README.md](../README.md) | A | Project overview | Source-reconciled or compared |
| [docs/architecture/advanced-capabilities-design-v1.md](architecture/advanced-capabilities-design-v1.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/api-serving-scope-v1.md](architecture/api-serving-scope-v1.md) | A | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/external-table-surface-contract.md](architecture/external-table-surface-contract.md) | A | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/incremental-key-semantics-v1.md](architecture/incremental-key-semantics-v1.md) | A | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/incremental-processing-semantics-v1.md](architecture/incremental-processing-semantics-v1.md) | A | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/incremental-sql-gap-plan.md](architecture/incremental-sql-gap-plan.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/materialized-view-runtime-roadmap.md](architecture/materialized-view-runtime-roadmap.md) | A | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/materialized-view-runtime.md](architecture/materialized-view-runtime.md) | A | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/operator-edge-contract-v1.md](architecture/operator-edge-contract-v1.md) | A | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/phase-4-8-execution-plan.md](architecture/phase-4-8-execution-plan.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/phase-8-cross-join-design.md](architecture/phase-8-cross-join-design.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/phase-8-interval-join-design.md](architecture/phase-8-interval-join-design.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/phase-8-percentile-design.md](architecture/phase-8-percentile-design.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/phase-8-recursive-cte-design.md](architecture/phase-8-recursive-cte-design.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/phase-8-udf-design.md](architecture/phase-8-udf-design.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/phase-8-window-frames-design.md](architecture/phase-8-window-frames-design.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/query-rewrite-design-v1.md](architecture/query-rewrite-design-v1.md) | A | Design proposal with bounded implemented subsets | Source-reconciled or compared |
| [docs/architecture/supported-sql.md](architecture/supported-sql.md) | A | Canonical SQL admission matrix | Source-reconciled or compared |
| [docs/development/generic-view-pipeline.md](development/generic-view-pipeline.md) | A | Operational or validation guide | Source-reconciled or compared |
| [docs/how-to.md](how-to.md) | A | Operational or validation guide | Source-reconciled or compared |
| [docs/architecture/ingest-admission-contract.md](architecture/ingest-admission-contract.md) | B | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/ingest-envelope-v1.md](architecture/ingest-envelope-v1.md) | B | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/ingest-storage-format-decision.md](architecture/ingest-storage-format-decision.md) | B | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/legacy-json-deltabatch-removal.md](architecture/legacy-json-deltabatch-removal.md) | B | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/meta-service.md](architecture/meta-service.md) | B | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/partition-ownership-protocol-v1.md](architecture/partition-ownership-protocol-v1.md) | B | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/relation-contract-v1.md](architecture/relation-contract-v1.md) | B | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/rhiza-migration.md](architecture/rhiza-migration.md) | B | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/schema-fingerprint-v1.md](architecture/schema-fingerprint-v1.md) | B | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/state-substrate-contract.md](architecture/state-substrate-contract.md) | B | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/storage-contract.md](architecture/storage-contract.md) | B | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/type-inventory-v1.md](architecture/type-inventory-v1.md) | B | Architecture or product contract | Historical / partial / unverified |
| [docs/development/rhiza-kv-k8s.md](development/rhiza-kv-k8s.md) | B | Operational or validation guide | Historical / partial / unverified |
| [docs/architecture/benchmark-gate-v1.md](architecture/benchmark-gate-v1.md) | C | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/checkpoint-recovery-index-v1.md](architecture/checkpoint-recovery-index-v1.md) | C | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/checkpoint-state-arrow-v1.md](architecture/checkpoint-state-arrow-v1.md) | C | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/datafusion-resource-policy-v1.md](architecture/datafusion-resource-policy-v1.md) | C | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/materialized-output-segment-index-v1.md](architecture/materialized-output-segment-index-v1.md) | C | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/object-store-capabilities-v1.md](architecture/object-store-capabilities-v1.md) | C | Architecture or product contract | Source-reconciled or compared |
| [docs/architecture/s3-compatible-test-harness.md](architecture/s3-compatible-test-harness.md) | C | Architecture or product contract | Source-reconciled or compared |
| [docs/development/functional-performance-validation.md](development/functional-performance-validation.md) | C | Operational or validation guide | Source-reconciled or compared |
| [docs/development/materialized-query-cache.md](development/materialized-query-cache.md) | C | Operational or validation guide | Source-reconciled or compared |
| [docs/development/query-performance-validation-2026-09-22.md](development/query-performance-validation-2026-09-22.md) | C | Operational or validation guide | Historical / partial / unverified |
| [docs/development/runtime-arrow-snapshot-verification.md](development/runtime-arrow-snapshot-verification.md) | C | Operational or validation guide | Historical / partial / unverified |
| [docs/development/versitygw-s3.md](development/versitygw-s3.md) | C | Operational or validation guide | Source-reconciled or compared |
| [docs/architecture-critique.md](architecture-critique.md) | D | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/dependency-governance.md](architecture/dependency-governance.md) | D | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/package-review-opendata.md](architecture/package-review-opendata.md) | D | Architecture or product contract | Historical / partial / unverified |
| [docs/architecture/production-readiness-status.md](architecture/production-readiness-status.md) | D | Architecture or product contract | Source-reconciled or compared |
| [docs/consultation/phase-4-design-review-packet.md](consultation/phase-4-design-review-packet.md) | D | Architecture or product contract | Historical / partial / unverified |
| [docs/development/dependency-refresh-2026-09.md](development/dependency-refresh-2026-09.md) | D | Operational or validation guide | Historical / partial / unverified |
| [docs/development/vind-k8s.md](development/vind-k8s.md) | D | Operational or validation guide | Source-reconciled or compared |
| [docs/development/vind-product.md](development/vind-product.md) | D | Operational or validation guide | Historical / partial / unverified |
| [docs/issue-24-aggregate-group-liveness.md](issue-24-aggregate-group-liveness.md) | D | Narrow semantic contract / issue record | Historical / partial / unverified |
| [docs/issue-26-interval-join-semantics.md](issue-26-interval-join-semantics.md) | D | Narrow semantic contract / issue record | Compared; no drift recorded |
| [docs/issue-27-temporal-asof-contract.md](issue-27-temporal-asof-contract.md) | D | Narrow semantic contract / issue record | Compared; no drift recorded |
| [docs/issue-28-recursive-predicate-direction.md](issue-28-recursive-predicate-direction.md) | D | Narrow semantic contract / issue record | Compared; no drift recorded |
| [docs/issue-29-recursive-cte-reachability.md](issue-29-recursive-cte-reachability.md) | D | Narrow semantic contract / issue record | Compared; no drift recorded |
| [docs/issue-31-scalar-aggregate-modifiers.md](issue-31-scalar-aggregate-modifiers.md) | D | Narrow semantic contract / issue record | Compared; no drift recorded |
| [docs/open-issue-priority-2026-09-12.md](open-issue-priority-2026-09-12.md) | D | Historical readiness, release, or review evidence | Historical / partial / unverified |
| [docs/release/0.1-native-runtime-migration.md](release/0.1-native-runtime-migration.md) | D | Historical readiness, release, or review evidence | Historical / partial / unverified |
| [docs/release/0.6.0-preview.md](release/0.6.0-preview.md) | D | Historical readiness, release, or review evidence | Historical / partial / unverified |
| [docs/release/1.0-readiness-checklist.md](release/1.0-readiness-checklist.md) | D | Historical readiness, release, or review evidence | Source-reconciled or compared |
| [docs/reviews/2026-08-28-comprehensive-review.md](reviews/2026-08-28-comprehensive-review.md) | D | Historical readiness, release, or review evidence | Historical / partial / unverified |
| [docs/reviews/2026-08-29-phase3-5-plan.md](reviews/2026-08-29-phase3-5-plan.md) | D | Historical readiness, release, or review evidence | Historical / partial / unverified |
| [docs/reviews/2026-08-31-remaining-gaps.md](reviews/2026-08-31-remaining-gaps.md) | D | Historical readiness, release, or review evidence | Historical / partial / unverified |
| [docs/reviews/2026-09-04-main-review-reassessment.md](reviews/2026-09-04-main-review-reassessment.md) | D | Historical readiness, release, or review evidence | Historical / partial / unverified |

The table contains the exact 68 paths in the reconciliation scope. For current behavior, follow links from the specification and supported-SQL matrix to repository source and tests. The reconciliation did not execute tests and did not complete formal EGM Goldfish review.

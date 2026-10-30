## What this proposes

The specification describes Velorix as a jarless materialized-view database: callers register schema-bound relations, ingest rows, define views that pass admission into a native incremental runtime, and query the materialized outputs. It distinguishes public view admission, read-time SQL over published outputs, and internal or development table scans; unsupported view shapes fail admission instead of falling back to source recomputation. The document also records current behavior, implementation gaps, evidence limits, and open owner decisions, but selects no implementation plan.

## How the existing system works, per this document

Configured API startup defaults to authoritative relation ingest, so new views are demand-managed: ingest publishes source changes, while queries or explicit backfill advance bounded materialization work. Legacy behavior and existing eager checkpoints remain distinct; enabling authoritative mode does not migrate existing checkpoints or replay all history. The system admits only specified SQL shapes into typed plans and the native runtime, reads queries from published output, and persists state and progress through distinct storage and metadata authorities. The specification presents these as source-backed descriptions, not proof of release readiness, production-scale behavior, or complete test qualification.

## Findings

No comprehension findings. The specification explains the key mode distinctions and limits inline, and links to the supporting SQL matrix, pipeline guide, design proposals, and readiness status. The linked material clarifies that proposed advanced capabilities are not the current contract and that cited source or test evidence does not, by itself, establish release qualification.

## Scope and files read

FeatureRange: full specification (144 lines). DetailedRange: N/A.

- EGM agents/comprehension.md — full.
- docs/specification.md — full, 144 lines.
- docs/development/generic-view-pipeline.md — full.
- docs/architecture/supported-sql.md — full.
- docs/architecture/advanced-capabilities-design-v1.md — full.
- docs/architecture/production-readiness-status.md — full.

No partial or inaccessible reads.

## Provenance

- Reviewer: Mendel
- Reviewer ID: `01a18be9-af1d-7e53-bebb-6598db5f89f2`
- Model: `p78f8fc/gpt-6-luna`
- Context: fresh fork, `fork_context: false`
- Stage: 6 — Comprehension Review
- Target: `docs/specification.md`, full `FeatureRange` (144 lines); `DetailedRange`: N/A

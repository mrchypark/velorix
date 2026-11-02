## docs/specification.md

- Scope: focused review at `4751ef51718ac7a041072ad70404d0907648cb12`, not an exhaustive rerun of the historical 68-document audit. SQL, demand, query/resource, and checkpoint sections were compared with linked source by a separate Luna reviewer; no concrete drift was reported in that inspected scope. No runtime tests were run for this review.
- Line 111 states production Rhiza KV requires three voters without the Local exception. `crates/velorix-meta/src/main.rs:701` applies the membership check only when `!config.local`; lines 706–719 permit production Local with explicit durable storage and `before-ack`. The specification should distinguish clustered KV requirements from single-writer Local requirements explicitly.
- The product document remains an as-built draft, not a decision-complete future feature specification: lines 130–140 leave mutation policy, supported DAG breadth, resource promises, and migration open. Goals, alternatives, acceptance criteria, and owner decisions must be supplied before claiming implementation readiness.

## docs/reviews/egm-2026-10-30/consolidated_review_report.md

- The frozen before/after snapshot hashes match the recorded values. However, the completed review applies to its frozen baseline, not later public-input changes or current benchmark work. The report itself marks these limits. Treating this record as a current full-product EGM approval would exceed its evidence.
- The historical open-work list includes input DELETE removal; the current implementation and `docs/reviews/2026-10-30_PUBLIC_RELATION_INPUT_VALIDATION.md:13` already record DELETE rejection. Keep the historical report intact and use a current decision ledger rather than presenting this item as unresolved.

## docs/architecture/benchmark-gate-v1.md

- Line 257 says identical-source controls execute before release comparison. `scripts/run-s3-release-calibration.sh:864` invokes these controls after the three candidate comparisons; they still block final qualification on failure. Describe controls as required before qualification, or change execution order in separate implementation work.

## docs/release/0.6.0-preview.md

- This file explicitly identifies itself as a dated draft, but its statement that there is no in-repository S3 result producer is obsolete at the inspected HEAD. `scripts/run-s3-release-benchmark.sh` and `scripts/run-s3-release-calibration.sh` now produce and validate measured results. The current blocker is failed regression/stability qualification, not a missing producer. Do not use this historical draft as current release status.

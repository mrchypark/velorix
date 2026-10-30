# Public input DELETE restriction — implementation validation

Date: 2026-10-30. Validation of the worktree changes based on `c6222abd054858109c90a63a061db698fa9fc9e8`. This validation is separate from the earlier frozen EGM specification review and records the earlier DELETE-policy phase.

This records the earlier DELETE-restriction phase. The subsequent [public relation/input validation](2026-10-30_PUBLIC_RELATION_INPUT_VALIDATION.md) hides multiplicity entirely from public declarations and input; use that report for the current wire contract. The results below remain the historical evidence for this phase.

## Behavior

- Public HTTP relation ingest rejects DELETE operation envelopes with HTTP 400, even when catalog metadata lists Delete.
- Caller-supplied negative values in the catalog-declared weight column are rejected before envelope classification and source publication. Ordinary negative measurement values are unaffected.
- Catalog-authorized UPDATE/UPSERT still normalize into internal signed retractions/replacements. Native signed-weight processing, persisted input replay, and derived-view delta publication remain intact.
- Schema-bound data fields named operation, row, before, or after remain usable. They do not create a negative-weight bypass. The restriction does not reserve a business value such as operation=delete in an ordinary positive-weight data row.
- A mixed invalid request does not persist an earlier valid batch. Existing stored data is not deleted or migrated by this change.

Implementation entry point: [operation normalization](../../crates/velorix-api/src/lib.rs). Public schema descriptions: [OpenAPI](../../crates/velorix-api/src/openapi.rs). Current policy: [specification](../specification.md#public-input-deletion-policy).

## Final local results

| Command | Result |
| --- | --- |
| cargo fmt; cargo fmt --check | Passed |
| cargo clippy --workspace --all-targets -- -D warnings | Passed |
| cargo test -p velorix-runtime --lib | 86 passed |
| cargo test -p velorix-runtime --test materialized_view_runtime --quiet | 263 passed |
| cargo test -p velorix-api --lib --quiet -- --test-threads=2 | 465 passed, 0 failed, 1 ignored; 57.24 s |
| git diff --check | Passed |

The ignored API case is the pre-existing `normalization_benchmark` marked explicit performance measurement; no test was newly disabled. Clippy passed with warnings denied. No performance conclusion is drawn from this correctness run.

## Coverage retained and added

The API regressions cover DELETE rejection, raw negative-weight rejection, mixed-batch rejection before publication, operation-like data columns, generated UPDATE/UPSERT retractions, and downstream output recovery after restart. Existing public negative-input tests were converted to supported updates while retaining exact aggregate, join, Top-K, paging cursor, residency eviction, dependency-cut, late-correction, duplicate-retry, and restart assertions. Primitive-key publication retains an empty bootstrap using a non-negative zero-weight seed, followed by a real value replacement.

The far-late test changes amount 10 to 11 and verifies corrected total/count/min/max/average after restart and duplicate retry. The correction-configuration tests also make nonzero replacements and retain the 2 MiB stack checkpoint case. Native runtime tests continue exercising signed retractions and fresh-process Arrow checkpoint recovery.

## Failed runs and corrections

The first full API run returned 452 passed, 13 failed, 1 ignored. Remaining test modules still submitted public negative weights, and several converted tests had incorrect normalized offsets or result expectations. The next run returned 461 passed, 4 failed, 1 ignored. Three failures shared an outdated correction-result expectation; the fourth used a filtered aggregate shape inappropriate for that fixture. The test now builds a filtered parent materialized view and its original aggregate consumers. Its focused rerun passed, followed by the successful full API run above. Production SQL support was not expanded to make tests pass.

## Limits and cleanup

These are local API/runtime tests, not a live deployment, real-data, provider-contract, or performance qualification. No object-storage adapter, Meta backend, dependency, or deployment configuration changed. Shared internal operation types and trusted signed-delta interfaces were retained.

No service or container was started for this validation phase. Cargo/test processes completed; test artifacts use the existing target directory. Existing user data and frozen EGM review snapshots were preserved. No commit or push occurred during this validation phase; this statement records that phase only and does not describe the repository's current PR or publication status.

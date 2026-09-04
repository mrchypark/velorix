# Reassessment of the 2026-09-03 main-branch review

**Reassessed 2026-09-05 at HEAD `4954d57`.** The supplied review was a static
assessment of `1ffbca70`. This is a source-and-test reassessment, not a claim
that live object storage, a cluster, scale benchmarks, or disaster recovery was
run here. Status: **fixed** means committed code plus focused repository
evidence address the finding; **partial** means a bounded mitigation exists;
**confirmed** means the concern remains; **stale** means the old premise is no
longer current; and **false** means current evidence contradicts it.

## Item-by-item assessment (29 technical findings)

| # | Historical finding | Status | Severity now | Current evidence and remaining work |
| ---: | --- | --- | --- | --- |
| 1 | Product image builders use a Rust version below the declared MSRV. | Fixed | P0 resolved | `642b848` and `6e3ec95` align/pin builder and workspace toolchains; `4954d57` keeps the contract checker portable. Actual image builds remain CI evidence. |
| 2 | Lease expiry uses caller-local time and lease loss can leave a worker able to commit. | Partial | P0 | `1fd32e5`, `891cb7f`, `f91dcb7`, and `e6b075b` improve authority/server-time fencing, but worker-shard caller-clock use and non-durable fencing remain. Best-effort local cleanup is not inherited-worker or commit-boundary self-fencing; retain skew, renewal-loss, and crash tests. |
| 3 | Checkpoint fencing is check-then-write rather than a final authoritative CAS. | Partial | P0 | `3ef542c` and `5177a80` add authority/checkpoint pieces, but the product writer, storage, read, and GC paths are not yet proven as one connected atomic-publication protocol. Retain competing-owner, orphan-GC, and live recovery tests. |
| 4 | Specialized runtimes can leave an epoch partially applied after a later failure. | Partial | P0 | `618821b`, `700448f`, `122ae2b`, and `fc020f8` improve selected families. The common-DAG comparison path is differential-only, non-public, and two-input; it does not establish product atomicity. Family-wide failpoint coverage and remaining runtime atomicity are open. |
| 5 | Invalid JSON values can become Arrow NULL instead of an admission error. | Fixed | P0 resolved | `61842c5` rejects invalid JSON/Arrow values; malformed-input regression coverage is required whenever conversion types expand. |
| 6 | A durable duplicate ingest retry is reported as failure. | Fixed | P0 resolved | `642b848` accepts and verifies duplicate/appended-or-duplicate outcomes; `9044723` publishes ranges under authority. Lost-response testing remains release evidence. |
| 7 | Epochs clone whole state for rollback. | Partial | P1 | Atomicity changes reduce correctness risk, but no current evidence proves an O(delta) state/write-set cost model at scale. |
| 8 | `net_rows`, diff, and combine repeat consolidation/sorting/cloning. | Confirmed | P1 | No benchmark or implementation evidence in the post-review changes establishes canonical-row reuse or removes repeated consolidation. |
| 9 | Join/window indexes do not demonstrate affected-state incremental cost. | Confirmed | P1 | Runtime families are present, but no committed scale proof bounds work for skewed joins or windows. |
| 10 | Expression evaluation is JSON-recursive rather than admission-compiled/vectorized. | Partial | P1 | `c8ee553` tightens multi-argument semantics; it is not an Arrow-native/vectorized execution conversion. |
| 11 | Transient state holds multiple logical-state copies. | Confirmed | P1 | No measured peak-RSS or structural-sharing proof closes this memory finding. |
| 12 | Recovery buffers too much replay payload. | Confirmed | P1 | The intended checkpoint/replay architecture remains; streaming replay and byte-budget evidence are still needed. |
| 13 | Query page size does not bound full materialization work. | Partial | P1 | Published-output page mechanisms exist, but no large-output/page-limit measurement proves bounded decode/consolidation. |
| 14 | Ingest acknowledgement is synchronously coupled to downstream view backlog. | Stale | P1 | The public contract now exposes materialization/backfill/lag state rather than treating query serving as source catch-up. Tail-latency behavior still requires measurement. |
| 15 | A process-wide Meta RPC mutex serializes independent operations. | Fixed | P1 resolved | `90fe757` removes Meta gRPC client serialization. Verify concurrency benefit with representative load rather than inferring it. |
| 16 | Query serving rebuilds DataFusion state and collects complete results. | Confirmed | P1 | Published-output-only policy is correct but does not prove shared planning, streaming, or process-wide memory admission. |
| 17 | Capability probing repeats on every product append. | Fixed | P1 resolved | `abc5d18` adds evidence that capability probes are startup-only. Reconfirm operation budgets against a live compatible backend. |
| 18 | Admission reconstruction grows with ingest history. | Partial | P1 | `9044723` publishes ingest ranges under authoritative control; measure hot-path history independence and recovery behavior. |
| 19 | API route lookup grows with the number of views. | Partial | P1 | No current measurement establishes direct route lookup cost. Retain as a request-path performance validation item. |
| 20 | Stream control can scan streams times checkpoint history. | Partial | P1 | No new public scale trace was inspected; preserve this as a controller scalability test requirement. |
| 21 | Checkpoint/lineage inspection and repair are flat-scan heavy. | Partial | P1 | Authority publication improved, but no evidence here proves tree/chunk indexing or bounded repair traversal. |
| 22 | SQL corpus evidence says only one representative shape passes. | Partial | P1 | The old corpus conclusion is obsolete for default-public paths now covered by plan/runtime/API tests. Cross, recursive, interval, and temporal paths are code-reachable through default admission, but API admission-to-materialization-to-restart evidence remains absent; see the canonical SQL matrix. |
| 23 | Expression validation misses variadic arguments. | Fixed | P1 resolved | `c8ee553` adds multi-argument semantic validation, including the visible `CONCAT` argument checks. This is distinct from JSON/Arrow coercion. |
| 24 | Window/join null and retraction semantics need hardening. | Partial | P1 | Atomic epoch work reduces one failure mode, but property/failpoint coverage across navigation, retractions, and multiplicity remains necessary. |
| 25 | Collision audit does not record/report collisions. | Fixed | P1 resolved | `5be704f` bounds and persists collision audits. Keep adversarial collision and retention tests in the release gate. |
| 26 | Binary key encoding claims ordering properties it may not provide. | Confirmed | P1 | No reviewed post-baseline change separates equality, ordering, and display codecs or supplies ordering-property evidence. |
| 27 | Source-cut construction scans broad reservation history. | Partial | P1 | New partition-authority/range-publication work narrows authority handling, but no query-plan/scale evidence proves a bounded source-cut read. |
| 28 | Duplicate view bootstrap can advance graph revision incorrectly. | Fixed | P1 resolved | `642b848` adds idempotent control-path changes and bootstrap tests. Cross-process contention remains an important release test. |
| 29 | CI lacks required distributed, image, and scale safety evidence. | Partial | P1 | Current workflows and image controls improved, but live storage, ownership handoff, crash injection, and scale evidence must remain mandatory/verified in the hosting configuration. |

## Product capability and operations reconciliation

The canonical materialized-view contract is
[Supported materialized-view SQL](../architecture/supported-sql.md). It
distinguishes default-public SQL, experimental ranking, internal-only runtime
families, and fail-closed rejection. Parser acceptance, a runtime unit test, or
query-time DataFusion syntax never independently creates a public view feature.

The intended product recovery architecture remains jarless and no-PVC. A
replacement pod must use durable remote object storage plus metadata; local
storage supports only same-host restart and cannot be treated as replacement-pod
durability. That is an architecture and product constraint, not a completed
production claim. Production/adversarial proof—including authoritative metadata
loss, delayed visibility, retry, owner handoff, and recovery equivalence—remains
required. Do not add PVC state, package-loaded runtimes, or a source-query
fallback to paper over missing evidence.

GitHub Actions supplies repository gates. The GHCR workflow records
digest-pinned references, while SHA-named tags remain mutable and provenance is
disabled. Treat workflow results and tag spelling as delivery metadata, not
immutable provenance or a substitute for the runtime SQL matrix and outstanding
adversarial tests. This document intentionally contains no environment,
endpoint, or cluster identifier.

## Focused validation commands

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p velorix-core --test view_plan
cargo test -p velorix-runtime --lib
cargo test -p velorix-runtime --test materialized_view_runtime
cargo test -p velorix-api --lib
```

## Evidence sources

- Task-supplied historical review dated 2026-09-03.
- Current baseline: `git log 1ffbca70..4954d57` and the committed source/tests.
- Public admission: `crates/velorix-api/src/view_admission.rs` and `lib.rs`.
- Runtime: `crates/velorix-runtime/src/materialized_view_runtime/` and its
  integration test.
- Operating constraints: README, `docs/development/vind-product.md`, and the
  repository workflow definitions.

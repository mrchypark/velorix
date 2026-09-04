# Reassessment of the 2026-09-03 main-branch review

**As assessed 2026-09-04 against `main` at `642b848` plus the visible working
tree.** The supplied review was static and pinned to `1ffbca70`. This is also a
source/code reassessment, not a claim that live storage, cluster, benchmark, or
disaster-recovery tests were run. Status: **confirmed** (still evidenced),
**fixed** (a committed change directly addresses it), **partial** (mitigated or
uncommitted), **stale** (premise no longer current), or **false** (contradicted
by current code).

| Review item | Status | Severity now | Current evidence / remaining work |
| --- | --- | --- | --- |
| Official image builders use an incompatible Rust version | Fixed | P0 resolved | `642b848` changes all five product Dockerfiles to the workspace toolchain. Still run actual image builds in CI. |
| Kubernetes lease trusts local caller time; renewal loss does not fence work | Partial | P0 | The visible `worker_shard.rs` work adds best-effort cleanup of workers started by that local runtime and stops a planned worker after renewal error. It does not establish durable partition authority, inherited-worker fencing, or self-fencing at every commit boundary; skew, renewal-loss, and no-post-loss-commit tests remain required. |
| Object-store checkpoint fencing is check-then-write | Partial | P0 | Ownership/checkpoint controls exist, but no competing-owner fault test was executed in this assessment. Retain authoritative-CAS and orphan-GC evidence as release work. |
| Epoch failure atomicity varies by specialized runtime | Confirmed | P0 | Specialized modules remain under `crates/velorix-runtime/src/materialized_view_runtime/`; restore tests do not prove common staged atomicity under all failpoints. |
| JSON-to-Arrow type mismatch becomes NULL | Confirmed | P0 | The visible `typed_expr.rs` change expands `CONCAT` argument type checks; it is unrelated to JSON-to-Arrow coercion. No inspected committed change or focused malformed-input evidence closes the conversion concern. |
| Duplicate ingest retry is a product failure | Fixed | P0 resolved | `642b848` adds verified `duplicate` / `appended-or-duplicate` outcomes in `crates/velorix-ingest-writer/src/main.rs`. Remaining risk: lost-response fault injection. |
| Per-epoch whole-state clone/diff/rebuild cost | Confirmed | P1 | No committed evidence establishes an O(delta) write-set runtime. Measure large-state and peak-RSS gates. |
| JSON-heavy expression evaluation and repeated canonicalization | Confirmed | P1 | No committed Arrow-native replacement was identified during this pass. |
| Join/window indexes do not prove incremental cost | Confirmed | P1 | Implementations exist; no current benchmark proves bounded affected-state work. |
| Recovery/paging materialize too much data | Partial | P1 | Published-output paging/checkpoint paths exist, but no large-replay byte-budget evidence was run. |
| Ingest ACK is coupled to view progress | Stale | P1 | Present public code/documents expose materialized output availability with explicit backfill/lag behavior; validate tail latency independently. |
| Meta RPC client serializes calls | Confirmed | P1 | Needs a targeted concurrency benchmark or code change; no proof of removal was found. |
| Per-query DataFusion setup/full collection | Confirmed | P1 | Published-output-only policy does not prove streaming execution or process-wide memory control. |
| Capability probes and history-wide LIST operations dominate hot paths | Partial | P1 | The concern remains; exact historical call counts were not re-measured. Establish an instrumented object-operation budget. |
| Route lookup/controller scans scale with history | Partial | P1 | Treat as an operational measurement gap unless a current trace reproduces it; do not repeat old numeric claims as measured fact. |
| SQL support is only one corpus case | False | P1 | Current runtime/API tests cover filters, projections, distinct set operations, bounded CTE/derived sources, aggregates, joins, windows, recursive, interval, temporal, and percentile families. Exact public scope, including the percentile API-E2E gap, is in `docs/architecture/supported-sql.md`. |
| Support is shape-specific, not arbitrary SQL composition | Confirmed | P1 | `crates/velorix-core/src/view_plan/mod.rs` retains family validators and execution variants. This is deliberate fail-closed scope with a maintenance cost. |
| Variadic typing, window null semantics, join/retraction helpers | Partial | P1 | The original report gives hypotheses, but this pass did not reproduce each; retain property/failpoint tests as closure evidence. |
| Collision audit and key encoding claims | Partial | P1 | No current validation was run; recheck named helpers before treating each as a release blocker. |
| Hiqlite source-cut scans and duplicate bootstrap revision | Partial | P1 | `642b848` changes `crates/velorix-meta/src/lib.rs` and bootstrap tests. Review cross-process CAS behavior before labeling fixed. |
| PR CI omits live distributed safety and Docker build | Partial | P1 | GitHub Actions/GHCR workflows exist, but are not equivalent to mandatory live storage, skew, handoff, crash-injection, and all-image-build evidence. |
| Benchmark scale/metrics miss target bottlenecks | Confirmed | P2 | No new measured scale evidence was supplied; add large-state, peak-RSS, and object-operation gates. |
| Oversized specialized modules/prototype exposure | Confirmed | P2 | Family-specific modules and large plan code remain; this is a maintainability risk. |
| No-PVC authority disaster recovery is unproven | Confirmed | P1 | README and `docs/development/vind-product.md` describe the intended no-PVC checkpoint/metadata recovery architecture. Production and adversarial authority-loss proof remains pending; do not add PVC state as a shortcut. |
| Main protection/required checks are absent | Partial | P1 | Hosting configuration cannot be proven from this checkout. Verify required GitHub Actions checks and immutable GHCR provenance in the hosting service. |

## What changed since the reviewed commit

`642b848` postdates the review baseline and fixes the Rust-builder mismatch and
verified duplicate append handling; related metadata changes are relevant to
bootstrap idempotency. These are committed code changes, not live-system
evidence. Concurrent `worker_shard` work is recorded only as best-effort local
cleanup, not as durable partition authority or self-fencing. The concurrent
typed-expression change is a `CONCAT` type-check adjustment, not an ingest
coercion fix.

## Required closure evidence

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p velorix-runtime --lib
cargo test -p velorix-runtime --test materialized_view_runtime
cargo test -p velorix-core --test view_plan
cargo test -p velorix-api --lib
```

For release, run the configured image, storage-compatibility, and deployment
gates in GitHub Actions; publish only workflow-produced immutable GHCR images.
Keep environment names, endpoints, and cluster identifiers out of public
evidence.

## Sources

- Task-supplied historical review, dated 2026-09-03.
- `git log`, `git show 642b848`, and the visible working tree on 2026-09-04.
- Public admission: `crates/velorix-api/src/view_admission.rs` and `lib.rs`.
- Runtime evidence: `crates/velorix-runtime/tests/materialized_view_runtime.rs`.
- Product/recovery policy: README, `docs/development/vind-product.md`, and
  repository GitHub Actions workflow definitions.

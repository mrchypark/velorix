# Query coverage and performance validation — 2026-09-22

Base: `f050c8dbe6fe6870b99af8ee0db64b7cb1ff1867`. Measurements include the
working-tree changes on `feature/query-performance-coverage`; they are not
measurements of a clean released commit. Each run records source fingerprints,
toolchain, dirty paths and baseline hash.

## Functional changes

- Corrected the public scalar-aggregate-filter and grouped median documentation.
- Expanded the exact-test functional manifest from 7 to 17 cases; all 17 passed
  both before and after the storage change.
- Added public cross/interval join admission, materialization, API-state restore
  and post-restore retraction coverage. Strict interval endpoints and disjoint
  intervals are explicitly checked.
- Added real loopback HTTP registration/ingest/query validation: two writers,
  64 requests, 512 rows, 87.5% hot-key skew, and exact final SUM/COUNT. The initial
  run passed with zero errors (ingest p50 33.831 ms, p95 41.455 ms).
- Storage regression validation passed: 56 ingest-envelope tests and 10
  multi-process admission tests.
- Final checks passed: workspace/all-targets Clippy with warnings denied,
  runtime unit tests (54), runtime integration tests (249), API library tests
  (209), and the validation-script contract. The targeted ingest failure/retry
  subset also passed (6 tests). The API test linker emitted the existing macOS
  large-unwind-section warning; tests completed successfully.

The [SQL support matrix](../architecture/supported-sql.md) describes admitted
shapes and exclusions. Representative tests are not an exhaustive SQL coverage
percentage. Loopback uses an in-memory store and is not deployed-service or S3
performance evidence.

## Controlled local storage comparison

Same machine, fixture, toolchain and unchanged cost baseline; each side used one
warmup followed by three measured runs. All results, including failed gates,
were retained. There were no selective retries. Evidence directories:

- `target/development-validation/query-perf-before`
- `target/development-validation/query-perf-after`

Runs were sequential before/after, not randomized or CPU-isolated; thermal and
background-system effects are not excluded. Request counts are the strongest
evidence here, while wall-clock changes remain diagnostic.

The change shares one admission-body listing between indexed-state construction
and orphan-expiry classification **within a single load**. Active-state
reconstruction remains fresh; committed-payload validation, existence checks,
cross-process CAS and conflict handling are unchanged. No cross-call cache was
introduced. This reduces duplicate work, not the asymptotic history-scan cost.

| Ingest-envelope workload | Before | After |
| --- | ---: | ---: |
| GET requests (every measured run) | 263,425 | 230,529 |
| LIST requests (every measured run) | 2,314 | 2,057 |
| Bytes read (every measured run) | 254,964,587 | 233,054,659 |
| PUT requests (every measured run) | 771 | 771 |
| Bytes written (every measured run) | 864,388 | 864,388 |
| Per-run p50 latency, median | 65.356 ms | 56.600 ms |
| Per-run p95 latency, median | 153.399 ms | 124.991 ms |

GETs decreased about 12.5%, LISTs 11.1%, and bytes read 8.6%. Latencies are
diagnostic medians of three per-run percentiles, not pooled percentiles or an
SLA. Overall runtime-only throughput median changed from 91,493 to 95,502
rows/s; its range overlaps and that metric excludes admission and HTTP. It
must not be attributed to the admission optimization.

## Separate native SQL diagnostic

Evidence: `target/development-validation/sql-family-diagnostics/run-0.json`
(warmup), then `run-1.json` through `run-3.json` (all retained). Every output
field and every snapshot page matched the independent oracle in all runs.
Inputs are non-null; timing covers runtime apply only, not input construction,
output validation, storage or HTTP.

| Family | Input rows | Output rows | Median rows/s | Median per-run p95 |
| --- | ---: | ---: | ---: | ---: |
| Filter + calculated projection | 4,096 | 2,087 | 154,894 | 6.089 ms |
| Filter + calculated projection | 65,536 | 33,735 | 7,872 | 122.403 ms |
| Grouped SUM/COUNT/MIN/MAX/AVG | 4,096 | 4 | 10,747 | 50.745 ms |
| Grouped SUM/COUNT/MIN/MAX/AVG | 65,536 | 4 | 7,828 | 150.856 ms |

The much slower filter/projection at larger retained state is a scaling concern,
not evidence of a new regression: these are different-size fixtures with no
previous revision comparator. Profile state copying, retained-state traversal
and epoch publication before promising production-scale throughput. The input
uses 90% composite-group skew, but aggregation collapses categories to four
customer groups; this is not a 256-output-group aggregate measurement.

## Open performance gate

The full cost gate did **not** pass consistently: two measured runs before and
one after failed because `slatedb_state_reopen` issued 12 LISTs versus baseline
9 (33.3% above baseline; allowed regression 25%). The failure existed before
the storage change. Baselines, thresholds and production maintenance defaults
were not changed. Lifecycle phase/prefix tracing is needed before choosing a
fix; a successful request-count optimization does not make this gate green.
Source inspection confirms SlateDB's first ticker tick fires immediately, so
short open/close samples can include different amounts of startup GC/compactor
work. Existing deferred LIST trace artifacts attribute calls to startup manifest
loading, compactor fencing and GC. This identifies a plausible source of
variation, not a completed deterministic-gate fix.

No production deployment, abrupt-crash recovery or provider-loss certification
is claimed by this report.

## Isolated three-node recovery

`scripts/check-rhiza-recovery.sh` passed against its own local MinIO container
and three native Rhiza nodes (17.93 seconds test runtime). Evidence:
`target/rhiza-recovery-evidence/20260922T071445Z-54049/rhiza-recovery.json`.
The test checks cross-node read/CAS, retaining quorum after one node loss,
failing closed after quorum loss, and recovery with empty working directories.
Shutdown/quorum-loss warnings remain in the retained log; the recovery oracle
passed. The disposable MinIO container was removed by the script after the run.
This is graceful cold restart, not SIGKILL/power-loss or Kubernetes/provider
failure evidence.

## Follow-up: retained-state filter/projection

The full-snapshot path no longer clones/consolidates unchanged output merely to
derive its visible delta. It still returns the complete epoch snapshot, and its
work therefore remains proportional to retained output. A separate explicit
`apply_changes_delta_only` contract preserves deltas/frontiers/idempotency but
omits those snapshots. Plain key-preserving filter/projection stages only the
touched rows before committing an ordered map update. DISTINCT and Top-K retain
their existing algorithms. Checkpoint format and ordinary callers are preserved.
The API ingest caller now uses the delta-only hook because it consumes deltas,
not epoch snapshots. It still takes full checkpoints for rollback/durability;
no end-to-end O(batch-size) API claim is made.

Initial fixed schedules (one warmup + three measured runs, all retained):

| Filter/projection mode | Input rows | Median rows/s | Median per-run p95 |
| --- | ---: | ---: | ---: |
| Optimized full snapshot | 4,096 | 404,957 | 1.909 ms |
| Optimized full snapshot | 65,536 | 27,830 | 36.511 ms |
| Delta only | 4,096 | 853,504 | 0.836 ms |
| Delta only | 65,536 | 915,264 | 0.686 ms |

Evidence directories are `sql-family-filter-optimized` and
`sql-family-delta-only` under `target/development-validation`. These are sequential
local diagnostics, not CPU-isolated trials or an SLA. The modes have different
output contracts: do not report their ratio as a like-for-like speedup. Every
final output value/page matched the independent oracle. These initial measurements
precede the final dependency pin; final integrated evidence is recorded separately.

Regression tests compare full-snapshot and delta-only deltas/checkpoints for plain,
computed, DISTINCT and Top-K queries, including mixed retractions, restore,
duplicate epochs, rejected ordinary-mode calls, failed-epoch retry and switching
back to snapshots. Plain restore rejects mismatched full/published state.

## Follow-up: SlateDB startup maintenance

The fork now pins `4b0a95a86c68b94d2fd368db76a0b75791951d5f`. Its change defers only
the first positive-minimum-age background GC tick for a freshly created database
using the default builder. Existing databases still perform immediate catch-up;
custom collectors, zero-age cleanup and manual collection retain their behavior.
Recurring intervals and deletion policies are unchanged. Fresh databases have no
clone parent to detach. Failed-bootstrap orphan objects may wait until the next
interval or reopen, which is an explicit tradeoff, not eliminated maintenance cost.

This changes production startup scheduling; it is not a benchmark-only switch.
The dependency's GC tests (63), full library tests (2,015 passed, one ignored),
library Clippy, and a builder test covering fresh/next-interval/reopen behavior
passed. The builder test initially used an incorrect metric label and failed;
correcting `op=list` to the actual `op=get, api=list` made the measurement valid.
No cost baseline or threshold was changed.

### Integrated gate result

`target/development-validation/query-perf-final/summary.json` passed all 17
functional cases, one warmup and all three measured runs. SlateDB reopen LIST
counts were **5, 4, 5, 7** (warmup first), versus the unchanged baseline of 9 and
25% allowed regression. Source fingerprints matched before/after; baseline
SHA-256 remained `3f93de1454275ec493b076f2f26d77ba6b30a7c9033a70824cc582df32cb7e96`.
The local gate failure was not reproduced in this complete fixed schedule.
Background maintenance still creates count variation; four passes are not proof
of invariant call counts under every scheduler or workload. This is a dirty-tree
diagnostic result, not release certification or production latency evidence.

### Final integrated native SQL measurements

Evidence: `target/development-validation/sql-family-final`, using the exact
benchmark executable built by `query-perf-final`. Each mode independently ran
one warmup and three measurements; all outputs/pages matched the oracle.
`provenance.json` links the source fingerprint, toolchain and benchmark artifact.

| Mode / family | Input rows | Median rows/s | Median per-run p95 |
| --- | ---: | ---: | ---: |
| Full snapshot / filter-projection | 4,096 | 434,027 | 1.832 ms |
| Full snapshot / filter-projection | 65,536 | 27,899 | 35.128 ms |
| Delta only / filter-projection | 4,096 | 972,460 | 0.632 ms |
| Delta only / filter-projection | 65,536 | 1,010,770 | 0.536 ms |
| Full snapshot / grouped aggregates | 4,096 | 11,298 | 48.591 ms |
| Full snapshot / grouped aggregates | 65,536 | 9,831 | 115.382 ms |
| Delta only / grouped aggregates | 4,096 | 11,522 | 47.186 ms |
| Delta only / grouped aggregates | 65,536 | 9,877 | 102.425 ms |

Grouped aggregates use the trait's fallback implementation, which computes the
snapshot and then discards it; they did not receive the indexed plain-filter
optimization. Their timing differences are diagnostic variation, not evidence
of an aggregate algorithm improvement. These separate schedules are not
randomized or CPU-isolated. The output-contract and API-checkpoint limitations
above also apply to this final table.

### Review and residual boundaries

Luna reviewers completed fix confirmation and a full re-review of the runtime
API/state changes, GC lifecycle change and evidence claims. Layers covered
intent/scope, correctness, operability, complexity, API compatibility, capacity
and data integrity. No material finding remained. One internal representation
transition was moved to the successful commit point so rejected ordinary calls
also leave the delta-only map untouched. Historical trace claims were labeled
as preceding startup deferral.

API checkpoint serialization remains proportional to retained state; grouped
aggregation and admission-history scanning are not asymptotically optimized by
this patch. No security, provider-loss or abrupt-crash certification is implied.

### Final regression and recovery checks

- Runtime: 54 unit and 251 integration tests passed; API: 209 library tests passed.
- Storage: checkpoint publication 108, ingest envelope 56, multi-process admission
  10, SlateDB durability 4 and SlateDB state 10 tests passed (188 total).
- Workspace/all-targets Clippy with `-D warnings`, `cargo fmt --check`, diff
  whitespace checks and the development-validation script contract passed.
- The integrated three-node no-PVC drill passed in 18.32 seconds, with 30 shared
  checkpoint/archive objects. Evidence:
  `target/rhiza-recovery-evidence/20260922T090844Z-96934/rhiza-recovery.json`.
  It covered cross-node read/CAS, one-node loss, quorum-loss rejection and empty
  working-directory recovery. Archive-head-change and shutdown/quorum warnings
  remain in the log; the oracle passed. This remains a graceful cold-restart
  experiment using isolated local MinIO, not an abrupt-crash/provider-loss test.

At the end of local validation, the SlateDB dependency commit had been pushed
to the existing fork branch, while Velorix changes were still uncommitted.
These validation results do not themselves certify a merge or deployment.
Existing unrelated edits in the two product-runner
scripts were preserved byte-for-byte.

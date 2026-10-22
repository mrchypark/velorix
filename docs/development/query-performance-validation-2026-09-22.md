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
- Storage: checkpoint publication (108), ingest envelope (56), and
  multi-process admission (10) tests passed.
- Workspace/all-targets Clippy with `-D warnings`, `cargo fmt --check`, diff
  whitespace checks and the development-validation script contract passed.
- The integrated three-node no-PVC drill passed in 18.32 seconds, with 30 shared
  checkpoint/archive objects. Evidence:
  `target/rhiza-recovery-evidence/20260922T090844Z-96934/rhiza-recovery.json`.
  It covered cross-node read/CAS, one-node loss, quorum-loss rejection and empty
  working-directory recovery. Archive-head-change and shutdown/quorum warnings
  remain in the log; the oracle passed. This remains a graceful cold-restart
  experiment using isolated local MinIO, not an abrupt-crash/provider-loss test.

These validation results do not themselves certify a merge or deployment.
Existing unrelated edits in the two product-runner
scripts were preserved byte-for-byte.

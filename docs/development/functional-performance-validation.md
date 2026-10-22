# Local functional and performance validation

Run from the repository root:

```sh
sh scripts/check-development-validation-contract.sh
sh scripts/run-development-validation.sh
# Optional: destination must not exist, and its parent must exist.
sh scripts/run-development-validation.sh --output target/my-validation --repeats 3
```

This development profile deliberately defers security remediation and does not
replace release, dependency, S3, or recovery gates. It never enables disabled GC,
changes an acceptance threshold, creates a bucket, or deploys a service. Do not
run another Cargo command concurrently. Disk space must accommodate debug tests
and release benchmark/CLI artifacts; this command never deletes build caches.

## Functional evidence

The checked-in `scripts/development-functional-cases.tsv` maps coverage labels to
17 exact API library test names. The runner builds the API test executable
once, verifies each exact test is listed, and requires exactly one passing test
with no failures or ignores. Renaming a test therefore cannot silently produce a
successful zero-test run. These tests exercise in-process API routers and their
test fixtures, not a separately deployed HTTP service:

| Case | Evidence |
| --- | --- |
| filter_projection | Registered schema, ingest, filtered/projected materialized output |
| sum_count | Relation-scoped ingest automatically updates grouped sum/count |
| nullable_aggregates_retract_restart | Nullable aggregates, retraction and API-state restart |
| composite_global_retract_restart | Composite/global aggregates, restart and final retraction |
| two_relation_join_restart | Two-table materialization and API-state restart |
| unsupported_single_admission | Unsupported single-input SQL fails admission |
| unsupported_join_admission | Unsupported join SQL fails admission |
| latest_by_key_restart | Nullable latest value and restored output |
| event_time_windows | Hopping/session window materialization |
| scalar_aggregate_filter | Public scalar aggregate filter and rejection matrix |
| semi_anti_restart | EXISTS/NOT EXISTS match transitions and restart |
| three_input_join_restart | Three-input composite-key join and restart |
| percentile_restart | Grouped median/percentiles and restart |
| recursive_cte_restart | Reachable recursive closure and restart |
| temporal_join_restart | Narrow ASOF match, restart and retraction |
| cross_interval_join_restart | Cartesian pairs, strict interval boundaries, restart and retraction |
| unsupported_three_input_admission | Unsupported three-input SQL fails admission |

This curated sample is not exhaustive SQL coverage. In-process restart is not
proof of process crash, three-node recovery, or durable no-PVC recovery.

### Separate loopback HTTP diagnostic

Run `cargo test -p velorix-api --lib
rest_http_two_writers_materialize_exact_aggregate_with_latency_diagnostics --
--nocapture` as one command. It starts an ephemeral loopback TCP listener and
uses real HTTP requests for registration, view admission, ingest and query.
Two independent writers each send 32 batches of eight rows; 87.5% of rows share
one hot key. Every request must succeed and the final sum/count must match all
512 rows exactly. The test emits per-request p50/p95 ingest latency, total ingest
throughput and query latency. It is intentionally separate from the curated
router manifest and has no wall-clock pass threshold.

This uses an in-memory object store and a same-process server. It does not
measure remote object storage, process restart, pod replacement or a production
network. A passing run certifies the bounded fixture's correctness, not an SLA.

## Performance evidence

For separate filter/projection and SUM/COUNT/MIN/MAX/AVG diagnostics, run:

```sh
VELORIX_SQL_FAMILY_DIAGNOSTIC=1 cargo bench --locked -p velorix-runtime --bench local_incremental
# Explicitly omit full epoch snapshots, as the API ingest caller does:
VELORIX_SQL_FAMILY_DIAGNOSTIC=1 VELORIX_SQL_FAMILY_DELTA_ONLY=1 cargo bench --locked -p velorix-runtime --bench local_incremental
```

This opt-in mode emits a separate JSON document, not a cost-gate result. It
checks all output values across snapshot pages at 4,096 and 65,536 input rows.
Each apply batch has 512 non-null rows (8 or 128 timing samples); p50 is the
lower median and p95 uses nearest rank. Input generation puts 90% in one
composite `(customer_id, category)` group. The aggregate SQL groups only by
customer, collapsing the 256 composite groups to four output customers, so its
actual hottest customer exceeds 90%. The filter keys remain unique order IDs.
Object storage, checkpoint/recovery, HTTP and nullable semantics are not measured
by this mode. The default benchmark corpus and cost baseline remain unchanged.
The JSON `epoch_output` field distinguishes `full_snapshot` from `delta_only`.
These modes perform different work and must not be presented as identical
throughput contracts. Plain key-preserving filter/projection uses an indexed
delta-only update path; DISTINCT and Top-K retain the existing snapshot path.
API ingest still serializes full checkpoints for rollback and durability, so
runtime-only delta throughput is not end-to-end API throughput.

### Interpretation and next optimization boundary

The local PR smoke gate compares storage-cost invariants, not wall-clock
throughput or latency. A passing gate must not be described as a performance
certification. Compare latency only with fixed warmup/repetition schedules on
the same host, fixture, toolchain, and configuration, and retain every result.

The ingest-envelope workload currently reloads historical admission state on
each append. `load_range_admission_index_state_for_partition` independently
loads expired keys, indexed admissions, and active admissions; the active path
reconstructs the namespace again. Repeated object GETs therefore reflect
admission validation, not necessarily materialized-runtime source replay.
Deduplicating reads within one admission may reduce the constant factor but
does not by itself remove quadratic cumulative history scanning. Any cache
across admissions needs an explicit cross-writer invalidation/fencing design.
Do not skip committed-object validation or weaken range-conflict checks to
improve the request-count metric.

The existing `local_incremental` benchmark and CLI are built in release mode.
One warmup and three measured executions run sequentially by default; `--repeats`
accepts 1–10. Every execution, including warmup, must pass the existing benchmark
schema validator and unchanged local PR smoke cost gate at a 25% regression
threshold against `baselines/benchmark/local/pr-smoke.json`.
An explicit cost regression is recorded as a failed run, but the remaining fixed
warmup/measurement schedule still executes. Statistics include rejected measured
runs, and any rejected gate makes the final summary failed and exit nonzero.
There are no retries or selective samples. Functional/build failures, malformed
benchmark data, validator failures and other gate errors still stop immediately.
Completed measurement counts are separate from passed counts; rejected runs retain
their gate exit code and log (the CLI produces no gate JSON on rejection).
This is a cost gate, not a latency/throughput gate; speed measurements remain
`diagnostic_only`. Dirty Rust/build inputs set `comparable_to_baseline: false`,
even when the diagnostic cost gate passes. A dirty baseline independently also
prevents comparability; its path and SHA-256 are recorded. Untracked build inputs
are rejected, and all Cargo builds use `--locked` to prevent lockfile updates.
Dirty or untracked validation runner/manifest files are recorded separately and
also set `comparable_to_baseline: false`.

`metric-statistics.json` reports min/median/max of each numeric aggregate metric
across measured runs, excluding warmup. A summarized p95 is a median/min/max of
per-run p95 values, not a pooled p95. The historical `peak_rss_bytes` field is a
terminal RSS sample, not a measured high-water peak. These results do not measure
HTTP throughput, network latency, production S3 performance or multi-node scale.
Build time is not included in benchmark metrics; overall elapsed time includes
builds, while per-run seconds include benchmark validation and gate overhead.
`rows_per_second` measures runtime application of 4,096 rows across 256 batches,
excluding ingest, checkpoint, recovery and HTTP. The top-level `scan_bytes` value
is currently hardcoded zero, not an instrumented total. These limitations are
also machine-readable in `metric_semantics`.

## Evidence and failures

Each invocation exclusively creates a fresh directory (default under
`target/development-validation`). Existing output directories are rejected.
`summary.json` records commit, dirty-worktree listing, toolchain/platform,
elapsed time, completed cases and runs, scope exclusions, failure stage/exit code,
and `release_certified: false`. Logs, Cargo artifact records, raw benchmark JSON
and source fingerprints are retained. A changed source fingerprint during the
run fails the result; an unrelated dirty script is recorded without claiming
that it changed compiled Rust inputs. Gate JSON remains beside it. Requested and
completed warmup/measurement counts are separate; statistics are null until
available and are linked and included in the summary. A failed case, missing test, malformed benchmark,
or rejected gate returns nonzero and produces a failed summary. Environment
prerequisite/argument/output-directory failures occur before evidence creation.

Live REST, live S3 and deployment recovery are explicitly `not_run`; security is
`deferred`. Do not interpret a successful local summary as production readiness.
For live recovery acceptance, separately provide an authorized durable object
store and run the existing deployment/recovery workflows. Keep private cluster
identifiers and credentials out of tracked files and GitHub reports.

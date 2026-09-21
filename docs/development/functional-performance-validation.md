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
seven exact API library test names. The runner builds the API test executable
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

This curated sample is not exhaustive SQL coverage. In-process restart is not
proof of process crash, three-node recovery, or durable no-PVC recovery.

## Performance evidence

The existing `local_incremental` benchmark and CLI are built in release mode.
One warmup and three measured executions run sequentially by default; `--repeats`
accepts 1–10. Every execution, including warmup, must pass the existing benchmark
schema validator and unchanged local PR smoke cost gate at a 25% regression
threshold against `baselines/benchmark/local/pr-smoke.json`.
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

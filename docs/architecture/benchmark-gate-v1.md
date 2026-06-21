# Benchmark Gate V1

Status: Accepted
Applies to: performance, cost, and release readiness.

Benchmarks are merge and release gates, not observational notes. Velorix cannot
claim object-storage-first, low-cost streaming readiness without
machine-readable benchmark regression checks.

## Gate Levels

| Gate | Scope | Backend | Purpose |
| --- | --- | --- | --- |
| PR smoke | Ingest, replay, query critical-path changes. | Local object store. | Catch obvious regressions. |
| Nightly integration | Storage, runtime, query. | S3-compatible backend. | Catch object-store cost and latency regressions. |
| Release | Release candidate. | Representative S3-compatible backend. | Block unsafe releases. |

## Required Metrics

Benchmark output must be JSON and include commit, backend, workload,
rows/second, bytes/row, PUT per GiB, GET/list/range-read counts, checkpoint p50
and p95 latency, recovery p95 latency, peak RSS, spill bytes, and scan bytes.
It must also include a non-empty `workload_metrics` array. Each entry names a
measured production-readiness workload and records p50/p95 latency, scan bytes,
and object request metrics for object-backed workloads.

Initial gates should use regression budgets rather than absolute targets. Local
filesystem and S3-compatible baselines are separate and not interchangeable.
The committed local PR-smoke baseline is a non-placeholder, conservative
threshold derived from a measured local run. It checks schema, required workload
coverage, non-placeholder commit provenance, and gate wiring, not stable local
machine performance. S3-compatible nightly and release baselines require live
measured S3-compatible evidence and must not use placeholders. The committed
S3-compatible baselines were refreshed from the RustFS-backed live S3 API gate
on 2026-05-18 at commit `1d477916d9341a478473e4cee0c4d63eb84e968b`; the
nightly and release gate artifacts both compare the generated result against
the matching committed baseline with `backend_evidence_scope=live_or_native`.

`local_incremental` is a bootstrap harness, not production readiness evidence.
It currently emits real local workload details for the authoritative
object-store capability probe, catalog-aware ingest envelope admission,
checkpoint publication, checkpoint recovery, and a bounded DataFusion Parquet
table scan. The DataFusion workload is instrumentation evidence that the local
query path lists and reads Parquet objects under policy; it is not production
scan latency evidence. The SlateDB workload writes a small checkpoint state
payload through `SlateDbStateStore`, closes and drops the store, reopens the
same object-store path, reads the state back, and records elapsed latency plus
the local metered object-store requests visible through the harness wrapper.
This does not expose SlateDB internals or make object-request metering part of
the state-store API. The GC dry-run workload prepares a small retained/orphan
state-output set, calls `CheckpointPublisher::plan_garbage_collection`, asserts
the retained checkpoint and candidates, and records local metered object-store
requests. The local GC execution workload reuses that fixture, calls
`execute_garbage_collection_plan_with_evidence`, reads back the persisted
`GcRunV1`, verifies checkpoint-retention evidence for the released checkpoint
object, and records local object-store requests. It does not test
listing-consistency failure modes or provide S3-compatible evidence.

## V1 Gate-Enforced Workload Metrics

The V1 CLI gate requires these `workload_metrics.name` values for local
benchmark results:

- `object_store_capability_probe`
- `ingest_envelope_validation`
- `checkpoint_publish`
- `checkpoint_recovery`
- `datafusion_table_scan`
- `materialized_output_segment_pruning`
- `materialized_output_recent_k`
- `materialized_output_compaction_equivalence`
- `materialized_output_compaction_debt`
- `materialized_output_delete_vector`
- `materialized_output_ttl_vector`
- `materialized_output_late_materialization`
- `slatedb_state_reopen`
- `gc_dry_run_planning`
- `gc_execution_evidence`

The current `local_incremental` and `s3_incremental` benchmark workloads are
storage/runtime primitive evidence, not public REST product-path evidence. In
particular, `ingest_envelope_validation` measures direct
`IngestAdmissionCoordinator` admission and the incremental state path uses
`PrototypeIncrementalEngine`; these labels must not be cited as full relation
ingest API to materialized-output evidence. Product-path ingest evidence should
use a separate benchmark/workload label once it measures public relation ingest
and materialized-output reads through the product API path.

S3-compatible benchmark gates do not require `gc_execution_evidence`, because
live GC deletion is a separate release artifact path rather than a timing
workload. Use `velorix-cli gc-execute-s3-compatible` to create the live
S3-compatible `GcRunV1`, then `gc-production-evidence` to emit the release-bound
production GC evidence artifact.

## Broader Benchmark Coverage

- Small batch high-QPS ingest.
- Object-store capability probe latency and request counts.
- Large batch throughput ingest.
- Many-partition replay.
- High-cardinality aggregate.
- Recovery after many checkpoints.
- Checkpoint publication latency.
- DataFusion bounded Parquet scan.
- SlateDB state write/read/reopen latency.
- GC dry-run planning latency.
- Local-only GC execution evidence latency.
- Many-small-file scan.
- Duplicate retry latency.
- Corrupt payload detection.
- SlateDB checkpoint recovery path when enabled.

## Materialized Output Segment Gates

SmithDB's public design is useful to Velorix as a feature benchmark, not as a
latency target. The gate is deliberately limited to materialized-output read
evidence:

- `materialized_output_segment_pruning`
- `materialized_output_recent_k`
- `materialized_output_compaction_equivalence`
- `materialized_output_compaction_debt`
- `materialized_output_delete_vector`
- `materialized_output_ttl_vector`
- `materialized_output_late_materialization`

Each workload compares an oracle over materialized output pages with a bounded
or optimized materialized-output read. Results must match exactly, selected
pages or vectors must be content-hash verified, source relation batches must
not be read, object request counts and bytes read must be recorded, and the
optimized path must re-read selected objects through the object store path.
These gates do not
measure the production compaction scheduler itself or claim general Top-K
support.

## Verification

- Benchmark JSON validates against schema.
- `velorix-cli benchmark-validate` validates a single benchmark output file.
- `velorix-cli benchmark-gate --gate-level <level> --backend <backend>`
  compares a result against a matching baseline with a caller-supplied
  regression budget and requires the current V1 workload detail names.
- Synthetic regression over budget fails the gate.
- Local and S3 baselines cannot be mixed.
- Missing object request metrics invalidates the result.
- PR smoke writes `target/velorix-bench/local-pr-smoke.json`, gates it against
  `baselines/benchmark/local/pr-smoke.json`, and uploads the result artifact.
- Nightly S3-compatible gating fails closed when no S3-compatible result path is
  configured or when the S3-compatible result regresses against the live
  baseline.
- Release gating fails closed when no S3-compatible result path is provided or
  when the S3-compatible result regresses against the live baseline.
- `s3_incremental` fails closed unless `VELORIX_S3_COMPAT=1` is set. With the
  flag set, it still requires real S3-compatible object-store configuration.
  When explicitly enabled, it emits S3-compatible benchmark JSON for the
  authoritative object-store capability probe, ingest envelope validation,
  checkpoint publication, checkpoint recovery, bounded DataFusion Parquet scan,
  materialized output read gates, SlateDB state reopen, and GC dry-run planning.
  Set
  `VELORIX_BENCHMARK_GATE_LEVEL=release` to emit release-level benchmark JSON;
  the default is nightly integration.

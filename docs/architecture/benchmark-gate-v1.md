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

Initial gates should use regression budgets rather than absolute targets. Local
filesystem and S3-compatible baselines are separate and not interchangeable.

`local_incremental` is a bootstrap harness, not production readiness evidence.

## Required Workloads

- Small batch high-QPS ingest.
- Large batch throughput ingest.
- Many-partition replay.
- High-cardinality aggregate.
- Recovery after many checkpoints.
- Checkpoint publication latency.
- DataFusion bounded Parquet scan.
- Many-small-file scan.
- Duplicate retry latency.
- Corrupt payload detection.
- SlateDB checkpoint recovery path when enabled.

## Verification

- Benchmark JSON validates against schema.
- Synthetic regression over budget fails the gate.
- Local and S3 baselines cannot be mixed.
- Missing object request metrics invalidates the result.
- Release workflow requires S3-compatible benchmark artifacts.

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
The initial committed baselines are conservative placeholders for wiring the
gate; they are not production performance claims.

`local_incremental` is a bootstrap harness, not production readiness evidence.
It currently emits real local workload details for ingest envelope validation,
checkpoint publication, checkpoint recovery, and a bounded DataFusion Parquet
table scan. The DataFusion workload is instrumentation evidence that the local
query path lists and reads Parquet objects under policy; it is not production
scan latency evidence. The SlateDB workload writes a small checkpoint state
payload through `SlateDbStateStore`, closes and drops the store, reopens the
same object-store path, reads the state back, and records elapsed latency plus
the local metered object-store requests visible through the harness wrapper.
This does not expose SlateDB internals or make object-request metering part of
the state-store API. GC dry-run planning workload details remain pending until
that path is measured directly.

## Required Workloads

- Small batch high-QPS ingest.
- Large batch throughput ingest.
- Many-partition replay.
- High-cardinality aggregate.
- Recovery after many checkpoints.
- Checkpoint publication latency.
- DataFusion bounded Parquet scan.
- SlateDB state write/read/reopen latency.
- Many-small-file scan.
- Duplicate retry latency.
- Corrupt payload detection.
- SlateDB checkpoint recovery path when enabled.

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
  configured.
- Release gating fails closed when no S3-compatible result path is provided or
  when the release baseline is still a placeholder.
- `s3_incremental` fails closed unless `VELORIX_S3_COMPAT=1` is set. With the
  flag set, it still requires real S3-compatible object-store configuration and
  refuses to emit benchmark JSON until the live workload is implemented.

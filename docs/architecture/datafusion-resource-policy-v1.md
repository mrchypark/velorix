# DataFusion Resource Policy V1

Status: Accepted
Applies to: ad hoc SQL, persisted query execution, persisted table scans, and
persisted view access.

Velorix treats DataFusion SQL as untrusted code. SQL text size and output row
caps are bootstrap controls only; they are not a production resource boundary.

## QueryExecutionPolicyV1

Production query admission requires:

- `max_sql_bytes`.
- planning timeout.
- execution timeout.
- `max_output_rows` and `max_output_bytes`.
- per-query and tenant/global memory limits.
- spill enablement and spill byte quota.
- scan byte limit.
- object request limit.
- file and partition limits.
- tenant/global concurrent query limit at the shared production runtime boundary.
- optional join and cross-join policy.

Policy is an execution admission contract, not just metadata stored with a
query spec. Runtime query paths build `SessionContext` instances through the
Velorix DataFusion session factory so `batch_size`, `target_partitions`,
`memory_limit_bytes`, and `spill_limit_bytes` have one mapping point.
Generic query policy catalog `create`/`get` remains bootstrap-compatible and
admits `QueryExecutionPolicyV1::default()`. Production table-scan admission uses
the explicit catalog production methods, which fail closed unless
`QueryExecutionPolicyV1::validate_production_table_scan` sees all required
single-query production boundary fields. Tenant/global concurrency admission
remains a shared-runtime responsibility because setting `max_concurrent_queries`
without a shared limiter intentionally fails query execution.

Current DataFusion 53 wiring:

- `batch_size` maps to `SessionConfig::with_batch_size`.
- `target_partitions` maps to `SessionConfig::with_target_partitions`.
- `memory_limit_bytes` maps to
  `RuntimeEnvBuilder::with_memory_limit(max_memory, 1.0)`.
- `spill_limit_bytes` maps to
  `RuntimeEnvBuilder::with_max_temp_directory_size`.
- Runtime environment build failures are returned through the existing
  DataFusion query error path.

DataFusion documents that its memory limit is not respected in all cases.
Velorix therefore treats this as a centralized DataFusion configuration
boundary, not complete memory or spill enforcement.

Object-backed query execution uses one small metered DataFusion object-store
wrapper for the whole query when `max_object_requests` is set. Scan preflight
and DataFusion execution share that same meter, so preflight list operations
debit the same request budget used by runtime file access. The wrapper counts
trait-level object-store operations before delegation and rejects the next
operation that would exceed the request budget. It records bytes from
successful `get_ranges` results and wraps `get_opts` response streams so
`bytes_read` advances as DataFusion consumes chunks instead of when a stream is
opened. Local filesystem payloads are converted through the same stream path
before metering.

DataFusion 53 memory exhaustion now has version-specific runtime evidence for
a grouped aggregate over the public object-backed Parquet query path.
Object-backed scan preflight timeout evidence also proves that cancellation
drops the stalled list stream and releases the shared query limiter permit.
The same `QueryExecutionLimiter` instance now has local evidence across two
public runtime query surfaces: an object-backed query can hold the only permit
while a production recovered materialized-view query fails immediately with
`ConcurrencyLimitExceeded` before recovery setup runs.
Version-specific spill failure tests, broader tenant/global shared runtime
semantics, and Velorix-owned typed memory/spill errors remain future work.

## Typed Errors

Policy violations should return typed errors where Velorix enforces the limit
directly, such as `PlanningTimeout`, `ExecutionTimeout`, `ScanBytesExceeded`,
`ObjectRequestLimitExceeded`, and `FileCountLimitExceeded`. Memory and spill
limits currently flow through DataFusion runtime configuration, so their exact
error shape is DataFusion-version-dependent.

## Verification

- Large joins, sorts, high-cardinality aggregations, many-file scans, and large
  Parquet scans are bounded by policy where the current DataFusion version
  honors the configured memory and spill limits.
- DataFusion 53 returns `ResourcesExhausted` for an object-backed grouped
  aggregate that exceeds `memory_limit_bytes`.
- `LIMIT 1` does not bypass scan byte or object request limits.
- Concurrent queries cannot exceed concurrency pools; shared memory semantics
  require a future shared runtime boundary.

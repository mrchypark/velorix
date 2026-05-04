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
- concurrent query limit.
- optional join and cross-join policy.

Policy is an execution admission contract, not just metadata stored with a
query spec. DataFusion `SessionConfig`, memory pool, spill manager,
cancellation, object-store scan instrumentation, and timeout handling must be
connected to this policy.

## Typed Errors

Policy violations should return typed errors such as `PlanningTimeout`,
`ExecutionTimeout`, `MemoryLimitExceeded`, `SpillLimitExceeded`,
`ScanBytesExceeded`, `ObjectRequestLimitExceeded`, and `FileCountLimitExceeded`.

## Verification

- Large joins, sorts, high-cardinality aggregations, many-file scans, and large
  Parquet scans are bounded by policy.
- `LIMIT 1` does not bypass scan byte or object request limits.
- Concurrent queries cannot exceed shared memory or concurrency pools.

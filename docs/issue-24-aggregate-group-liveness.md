# Issue 24: aggregate group liveness

Status: partial implementation and focused validation. This note does not
close Issue 24 or claim production, cluster, or external-object-store proof.

## Contract

The existence of a grouped output row is determined by the source group's
positive input multiplicity, not by the current value of any aggregate.

`WHERE` removes rows before grouping. If it removes every source row for a
key, that key does not exist in the grouped result. `FILTER` applies only to
one aggregate's contribution; a source group remains visible when another
aggregate filter excludes all of its rows. For a live group:

- `SUM`, `AVG`, `MIN`, and `MAX` are SQL `NULL` when their qualifying input
  set is empty.
- `COUNT` and `COUNT DISTINCT` are `0` when their qualifying input set is
  empty.
- A qualifying row whose numeric value is zero produces a numeric `SUM` of
  `0`, which is distinct from `NULL`.

When source multiplicity retracts to zero, the grouped row is removed. A later
positive insertion may create it again with the normal aggregate semantics.

## State and checkpoint shape

The single-key aggregate runtime keeps source group multiplicity separate from
filtered aggregate contribution state. Its generic checkpoint payload (schema
version `1`, runtime kind `single_key_sum_count`) contains:

- `filtered_aggregate_state`: aggregate contribution state, including the
  internal qualifying-count metadata needed to distinguish an empty filtered
  `SUM` from a numeric zero.
- `group_input_counts`: an optional `DeltaBatch` for runtime aggregate plans.
  Each net row has the grouped key, weight `1`, and a value object of the form
  `{"count": <positive int64>}`.

The restore path must not default a missing `group_input_counts` field to an
empty state. A checkpoint from before this state existed requires a deliberate
replay or rebuild of the affected view; there is no claimed seamless legacy
restore or migration procedure here.

The API-generated schema for generic aggregate views now marks `SUM`, `AVG`,
`MIN`, and `MAX` outputs nullable while keeping `COUNT` and `COUNT DISTINCT`
non-nullable. This permits the public schema to represent the SQL result above.
The change is intentionally limited to the generic single-key aggregate
schema; window and join schema validators retain their existing family-specific
nullability contracts.

## Evidence currently available

The API regression test
`rest_filtered_aggregate_retains_zero_and_all_filtered_groups_across_restart_retraction`
uses the public relation and view endpoints and checks this sequence:

1. Register a relation and a grouped view with filtered `SUM` and `COUNT`.
2. Ingest one zero-valued qualifying row and one row excluded by both filters.
3. Assert both groups, including `filtered_sum = 0` for the first and
   `filtered_sum = NULL, filtered_count = 0` for the second.
4. Restore from the same in-memory object store and assert the exact rows.
5. Retract both source rows and assert no groups remain.
6. Reinsert both rows and assert the exact original rows.

The following focused API checks pass on the current working tree:

```text
cargo test -p velorix-api --lib \
  rest_filtered_aggregate_retains_zero_and_all_filtered_groups_across_restart_retraction
# 1 passed

cargo test -p velorix-api --lib \
  rest_nullable_numeric_aggregates_preserve_nulls_and_retractions_across_restart
# 1 passed

cargo test -p velorix-api --lib \
  single_key_output_schema_fingerprint_changes_with_aggregate_projection
# 1 passed

cargo test -p velorix-api --lib \
  rest_tumbling_window_filtered_nullable_column_count_view_materializes_outputs
# 1 passed
```

The nullable-source regression
`rest_nullable_numeric_aggregates_preserve_nulls_and_retractions_across_restart`
also passes. It proves the live all-NULL group, the mixed `NULL + 0` group, and
the single zero-valued group with all five aggregate outputs, then verifies the
exact rows after restart, full retraction, and reinsertion.

On 2026-09-08, the coordinator independently reran the full materialized-view
suite (244/244), core planner suite (348/348), runtime unit tests (54),
formatting, and workspace all-target clippy with `-D warnings`: all passed.
Core unit tests (74) passed in the preceding group-liveness validation run.
The API test does not weaken the expected
rows or add a fallback execution path.

Focused restore tests reject missing legacy group counts and mismatched group
keys. Older shared-FILTER plans also differ from the normalized current plan
and require replay/rebuild; no automatic migration is implemented.
The signed-delta regression also verifies a net-zero group with both filters
false in positive-first and negative-first input order.

The first regression uses a non-nullable source value column to isolate
aggregate-filter group liveness. The nullable-source regression exercises
nullable Int64 directly, including an all-NULL source group; this is the proven
scope for `SUM/AVG/MIN/MAX/COUNT`. Raw nullable Decimal128
`SUM/AVG/MIN/MAX` is explicitly rejected during single-key admission until a
NULL-aware decimal runtime is supported. A planner regression checks each
rejection and preserves admission for the corresponding non-nullable forms.

## Proof boundaries and remaining work

The evidence above is local, in-memory API/runtime evidence. It does not prove
an external object-store failure, Kubernetes replacement-pod recovery, or
production cutover. Legacy restore rejection is tested, but a replay/rebuild
procedure against an existing deployment is not. Nullable source aggregate
coverage beyond the tested Int64 shape, including Decimal128, remains outside
this proof. No unverified legacy restore command or production durability claim
is provided here.

Nullable aggregate Top-K now has explicit default-order regressions:
ascending uses NULLS LAST and descending uses NULLS FIRST, consistent with the
query engine's default NullsMax behavior. Omitting ASC/DESC now means ASC.
The earlier NULL-last comment was incorrect for descending order; the old NULL
position itself was consistent with this default. A typed comparator now
preserves exact Int64 ordering instead of converting integral values to f64.
Tests cover NULL versus zero, ties, LIMIT/OFFSET, restart/retraction, and adjacent
large Int64 values. Explicit NULLS FIRST/LAST clauses remain admission-rejected;
exact Decimal Top-K ordering is not established by these Int64 tests.
Legacy nullable COUNT-only checkpoints may also need replay because these plans
now use the runtime aggregate state and require persisted group counts.

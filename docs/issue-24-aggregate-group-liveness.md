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
replay or rebuild of the affected view; it is not a seamless legacy restore.

An explicit, startup-only rebuild path now exists for the one supported legacy
shape. It is enabled only when `VELORIX_LEGACY_REBUILD_VIEW_ID` exactly names
the affected view; ordinary restore calls and ingest-triggered runtime
initialization leave it disabled. The path is fail-closed unless all of these
conditions hold:

- the checkpoint has already passed identity, current-plan, and schema
  validation and failed specifically with the known missing-group-counts
  classifier;
- the binding is the native materialized `SingleKeySumCount` runtime over
  direct source relations (published-view inputs and other runtime families
  are rejected);
- metadata returns one atomic relation-wide source cut for every input
  relation in the configured namespace, with generation/schema identity
  intact, contiguous publications, no retention gap, and coverage from offset
  zero through every old frontier;
- every staged publication is revalidated against its immutable envelope and
  digest before a fresh empty runtime replays it; the captured cut is reused
  unchanged for the replacement coverage;
- the existing runtime-owner lease is acquired/renewed and the new checkpoint
  is published with the old pointer as its strict CAS predecessor.

The old checkpoint and all staged objects are retained. A source-cut,
replay, lease, frontier, or pointer-CAS failure leaves the old authoritative
pointer unchanged and does not silently fall back to ordinary replay. The
guarded publication now carries the complete captured cuts into the metadata
CAS transaction. A source-cut change returns a distinct fail-closed error; a
generic predecessor conflict on a guarded migration does not invoke pointer
rehydration or write an intermediate predecessor record. If the predecessor is
missing or changed, migration remains manual/fail-closed. The local API proof
covers the exact captured-cut/frontier guard and an authoritative in-memory
startup fixture that reaches the typed legacy error, rejects a source commit
after the cut, then retries against the fresh cut while preserving old objects
and avoiding double application. External-object-store fault injection and a
competing-writer CAS failure remain required before this path can be treated as
production migration evidence.

The migration does not use a `LocalIngestLog` listing as a substitute for the
authoritative relation-wide cut. Backends that cannot evaluate the guarded
source-cut predicate atomically (including the current Hiqlite adapter) reject
the guarded publication as unsupported; they must not silently downgrade to an
unguarded pointer write.
Historical shared-FILTER plan mismatches remain manual/fail-closed.

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

The guarded migration continuation also passed `cargo test --workspace --quiet`
and workspace all-target clippy with `-D warnings` on 2026-09-08. This includes
202 API tests, 244 materialized-view runtime tests, and 348 planner tests.
The migration race fixture publishes a real source batch after capture and
before checkpoint publication; it verifies rejection with the old pointer
unchanged, successful replay against the next captured cut, and idempotent retry.

Additional guard regressions exercise the remote gRPC source-change outcome,
an old-server `UNIMPLEMENTED` response with exactly one guarded RPC and no
ordinary-publication fallback, and stale-cut rejection after a local Rhiza
store reopen. The Rhiza test is local persistence evidence, not a three-node
network-failure or external-object-store recovery test.

Focused restore tests reject missing legacy group counts and mismatched group
keys. Older shared-FILTER plans also differ from the normalized current plan
and require an explicit replay/rebuild; they are not covered by the targeted
single-key migration path.
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
production cutover. The replay/rebuild proof is in-memory and does not claim
an existing deployment migration. The tested migration shape is a current
plan-valid filtered single-key aggregate with the legacy group-count field
removed; historical folded/shared-FILTER plans and schema-incompatible
checkpoints remain outside it. Nullable source aggregate
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

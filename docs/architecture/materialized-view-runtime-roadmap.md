# Materialized View Runtime Roadmap

Status: First complete milestone and window foundation implemented and locally
verified; follow-up SQL-family expansion remains tracked here.
Applies to: view admission, logical planning, incremental operators,
materialized output serving, checkpoint/recovery, and window SQL.

This roadmap expands the accepted
[Materialized View Runtime](materialized-view-runtime.md) decision into an
implementation plan. The product target is a jarless Velorix-native
materialized view database/runtime:

- users register relations
- users ingest committed epochs
- users define views over registered relations
- supported views are admitted into a Velorix-owned logical plan
- ingest advances materialized output automatically
- queries read materialized output, not a source full scan
- restart recovers from metadata plus durable checkpoints and replay data

## Research Summary

Arroyo models streaming SQL as a stateful dataflow. SQL is parsed and planned
with DataFusion, then compiled into a streaming dataflow. Its window model is
event-time and watermark aware, with tumbling, sliding, and session windows.
Stateful operators keep state and checkpoints are written to durable storage for
recovery.

Relevant references:

- [Arroyo streaming windows](https://doc.arroyo.dev/sql/windows/)
- [Arroyo concepts](https://doc.arroyo.dev/concepts/)

Flink exposes windowing through table-valued functions. `TUMBLE`, `HOP`,
`CUMULATE`, and `SESSION` produce a new relation with `window_start`,
`window_end`, and `window_time`. Flink's correctness boundary is keyed state plus
checkpoint barriers. Recovery restores operator state and resumes sources from
the checkpointed position.

Relevant references:

- [Flink window TVF](https://nightlies.apache.org/flink/flink-docs-master/docs/sql/reference/queries/window-tvf/)
- [Flink stateful stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)

Materialize maintains views as dataflows over update collections. Updates carry
data, logical time, and diff. It uses arrangements, which are maintained indexes
over input, output, and intermediate collections, so repeated views can share
indexed state.

Relevant references:

- [Materialize arrangements](https://materialize.com/docs/get-started/arrangements/)
- [Materialize reaction time](https://materialize.com/docs/concepts/reaction-time/)

RisingWave treats `CREATE MATERIALIZED VIEW AS SELECT ...` as a streaming job.
The system backfills historical data, continuously applies upstream updates, and
uses metadata, barriers, object storage, and checkpoint recovery to maintain a
consistent materialized result.

Relevant references:

- [RisingWave CREATE MATERIALIZED VIEW](https://docs.risingwave.com/sql/commands/sql-create-mv)
- [RisingWave architecture](https://docs.risingwave.com/get-started/architecture)

ksqlDB uses materialized tables for request/response pull queries. The pull
query reads the current materialized state; the expensive streaming work has
already been performed by a persistent query.

Relevant references:

- [ksqlDB materialized views](https://docs.confluent.io/platform/current/ksqldb/concepts/materialized-views.html)
- [ksqlDB pull queries](https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/select-pull-query.html)

## Design Lessons

The systems above converge on the same boundary:

- SQL is the user interface, not the runtime contract.
- Admission must lower SQL into a versioned incremental plan.
- Runtime dispatch must use the admitted plan, not SQL text, input arity, or
  relation-specific heuristics.
- Operators must own explicit state layouts, checkpoint codecs, and output
  delta semantics.
- Query serving must read a published materialized output surface.
- Window SQL is primarily a watermark and recovery problem, not a parser
  feature.

## Target Architecture

Velorix should introduce `VelorixLogicalViewPlanV1` as the only runtime
admission artifact for materialized views.

The plan should contain:

- plan version
- deterministic plan hash
- view id and view spec hash
- dialect version
- node ids
- relation ids and schema fingerprints
- resolved column ids and logical types
- output schema
- expression tree with typed operators
- operator capability requirements
- state layout ids
- checkpoint codec versions
- materialized output codec versions

The first closed operator capability table should include:

- `RelationScan`
- `Filter`
- `Project`
- `Aggregate`
- `InnerEquiJoin`
- `LatestByKey`
- `Output`

Window operators should be added only after the non-window path has a durable
state and output publication boundary.

## Admission Flow

1. Parse SQL with the existing SQL parser/DataFusion front-end.
2. Resolve relation names against the relation catalog.
3. Resolve column references to stable column ids.
4. Type-check expressions against relation schemas.
5. Lower supported SQL into `VelorixLogicalViewPlanV1`.
6. Validate the plan against the static runtime capability table.
7. Persist the view spec and admitted plan metadata.
8. Construct runtime state only from the admitted plan.

Unsupported SQL must fail closed during admission. It must not fall back to a
source full-scan query path or a best-effort runtime branch.

## Runtime Flow

For each committed ingest epoch:

1. Read the durable epoch manifest.
2. Verify relation id, schema fingerprint, offset range, and content hashes.
3. Apply source deltas to the relevant plan inputs.
4. Route deltas through the operator graph.
5. Update keyed operator state.
6. Emit materialized output deltas.
7. Write operator state checkpoint objects when checkpointing.
8. Write output delta/page manifests.
9. Verify content hashes.
10. Atomically advance metadata pointers in hiqlite.

The query path must read the materialized output table/page index. It must not
reconstruct query results by scanning source relation batches or by rebuilding
rows from live operator accumulator state.

## Durable State Model

Hiqlite should store small metadata:

- relation definitions
- view specs
- admitted plan hashes
- active plan pointer
- runtime status
- committed epoch pointers
- checkpoint manifest pointers
- output manifest pointers

Object or local storage should store durable data:

- source ingest batches
- epoch manifests
- operator state checkpoints
- materialized output delta manifests
- materialized output page/checkpoint manifests
- replayable epochs after the latest checkpoint

Foyer and in-memory state may cache hot pages, state blocks, and checkpoint
objects. They are never the correctness boundary.

## First Complete Milestone

The first complete implementation should intentionally exclude user-facing
window SQL. It should prove the generic materialized view pipeline first.

Required SQL families:

- filter
- projection
- group aggregate
- `sum`
- `count`
- `min`
- `max`
- `avg`
- two-relation inner equi-join

Required runtime behavior:

- relation schemas do not need prototype semantic roles
- SQL lowers into one logical plan format
- all supported operators execute through the same plan executor
- output changes are proportional to changed output keys
- query reads materialized output
- restart restores checkpoint and replays only later epochs
- unsupported SQL fails during admission

Current implementation evidence:

- Single-relation aggregate views lower into `VelorixLogicalViewPlanV1` and run
  through the built-in materialized view runtime for filters, projections,
  `sum`, `count`, `min`, `max`, and `avg`.
- Runtime tests now execute a filtered single-relation aggregate view and verify
  the materialized output contains only rows accepted by the admitted `Filter`
  node before aggregation.
- Runtime and plan tests now execute a filtered aggregate with projected output
  aliases and verify the aggregate accumulators publish the projected output
  columns. For aggregate views, SQL projection is represented by accumulator
  output projection; latest-by-key views use an explicit `Project` node.
- Latest-by-key views lower into `LatestByKey` and support the current
  `arg_max(value, ordering)` shape, including boolean latest state.
- Two-relation inner primary-key equi-join views lower into `InnerEquiJoin`
  plus aggregate state for the supported sum/count family.
- Single-relation aggregates, latest-by-key, and two-relation joins now apply
  live epoch changes through one private plan-executor boundary while preserving
  their existing checkpoint and output publication formats.
- Single-relation aggregates and the supported two-relation join family can use
  the SQL-selected sum column without requiring a prototype `Value` semantic
  role on the input relation schema.
- Latest-by-key views use the SQL-selected `arg_max(value, ordering)` columns
  without depending on `Value` semantic-role cardinality.
- Relation catalog admission no longer rejects scalar materialized-view inputs
  because of multiple prototype `Value` roles, and generic ingest validates the
  configured `weight_column_id` by type rather than by a `Weight` semantic role.
- Runtime creation and restore fail closed when admitted SQL, plan, and logical
  plan metadata are missing; role-synthesized default SQL is no longer used as a
  fallback.
- Active runtime bindings persist the admitted `VelorixLogicalViewPlanV1`, and
  runtime creation uses that stored plan instead of reparsing a fallback SQL
  shape at activation or restart.
- Admission tests now cover unsupported one-input and join SQL families,
  including window aggregates, distinct aggregates, HAVING, ORDER BY, CTEs, LEFT
  JOIN, non-equality joins, and join WHERE clauses.
- Runtime commit tests now assert ingest emits signed materialized output
  deltas in the commit result, including a changed-key-only delta with the old
  row retracted and the new row inserted. Snapshot output batches remain
  available separately for compatibility.
- API checkpoint persistence writes signed output delta records to durable
  object storage using content-addressed `standing-runtime-output-deltas` keys
  and records `standing-runtime-output-delta:` refs beside snapshot output
  manifest refs in the checkpoint pointer.
- Runtime checkpoints publish operator state payload objects and output page
  objects; query serving reads the published materialized output surface rather
  than reconstructing rows from source batches.
- Query serving no longer falls back to a live in-memory runtime when no
  published output manifest exists; it fails closed until an ingest/checkpoint
  has durably published materialized output.
- Query serving fails closed when the standing-runtime output manifest object is
  missing or corrupt, and separately when a referenced output page object is
  missing or corrupt.
- Checkpoint recovery fails closed when the standing-runtime checkpoint record
  itself is corrupt, and separately when the referenced state payload object is
  missing or corrupt.
- Checkpoint recovery also fails closed when metadata contains a latest
  checkpoint pointer but the referenced checkpoint object is missing.
- Runtime input frontiers reject non-contiguous offset ranges instead of
  silently advancing by `max(end_offset_exclusive)`, so a replay or ingest gap
  fails closed before the materialized view frontier can move.
- API-level restart evidence now covers:
  - relation registration, view registration, ingest, query, restart restore,
    and post-restart query for latest bool views
  - a crash-window batch committed after the latest checkpoint and replayed
    during restart restore
  - relation registration, multi-relation join view registration, ingest on both
    input relations, query, restart restore, and post-restart query for the
    supported two-relation join family
- A source guard test covers active product runtime source and fails if external
  compiler, JAR, pipeline-manager, DBSP/Feldera, or PVC-dependent execution
  references re-enter the runtime path.

Verification commands for this evidence:

```bash
cargo test -p velorix-api --lib
cargo test -p velorix-runtime --test materialized_view_runtime
cargo test -p velorix-api materialized_runtime_binding_persists_admitted_logical_plan
cargo test -p velorix-runtime --test materialized_view_runtime runtime_commit_publishes_materialized_output_batch_after_ingest
cargo test -p velorix-runtime --test materialized_view_runtime runtime_commit_publishes_signed_output_delta_for_changed_keys_only
cargo test -p velorix-api standing_runtime_checkpoint_persistence_writes_output_delta_manifest_ref
cargo test -p velorix-api rest_view_query_fails_closed_without_published_output_manifest
cargo test -p velorix-api standing_runtime_checkpoint_read_fails_closed_when_checkpoint_object_is_corrupt
cargo test -p velorix-api standing_runtime_checkpoint_read_fails_closed_when_meta_pointer_checkpoint_object_is_missing
cargo test -p velorix-storage object_key::tests::standing_runtime_output_delta_keys_are_deterministic_and_parseable
cargo test -p velorix-meta --test meta_store standing_runtime_checkpoint_pointer
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_filtered_single_relation_aggregate_view
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_filtered_projected_aggregate_view
cargo test -p velorix-core --test view_plan filtered_projected_single_key_aggregate_sql_lowers_to_projected_accumulators
cargo test -p velorix-runtime --test materialized_view_runtime runtime_rejects_non_contiguous_input_offsets_without_advancing_frontier
cargo test -p velorix-runtime --test no_external_runtime_dependencies
cargo test -p velorix-core --test view_plan
cargo test -p velorix-core --test relation
cargo test -p velorix-storage --test relation_catalog_registry
```

Full workspace verification for the first complete milestone passed locally on
2026-06-15:

```bash
cargo test --workspace
cargo fmt --all --check
git diff --check --
```

## Window Foundation Milestone

Before exposing window SQL, Velorix needs durable event-time semantics:

- event-time column binding
- per source-partition watermark frontier
- epoch records that can carry watermark advances
- allowed lateness policy
- late-data action
- window close semantics
- retention policy
- deterministic replay of data and watermark epochs
- output correction or retraction semantics where needed

The first window operator should be a tumbling event-time aggregate with a strict
late-data policy.

Current implementation evidence:

- `IngestRowsRequest` accepts optional `event_time_watermark` metadata for
  relations that declare `relation_schema.event_time_column_id`.
- Ingest admission rejects watermark metadata when the relation has no declared
  event-time column, when the column id does not match, when the physical type
  is not `Int64`, `Date32`, or `TimestampNanosecond`, or when
  `watermark_ns > max_observed_event_time_ns`.
- REST and storage-level catalog admission reject watermark metadata when
  `max_observed_event_time_ns` is below the actual maximum event-time value in
  the admitted record batches.
- `IngestEnvelopeHeader`, durable ingest admission records, and epoch manifest
  batch records carry the same optional watermark metadata.
- The ingest envelope payload digest covers present watermark metadata while
  omitting absent metadata from the canonical digest for old-envelope
  compatibility.
- Runtime `RelationInputBatch` carries watermark metadata into the logical plan
  executor. `RuntimeCheckpoint` stores `input_event_time_frontiers` by
  `(relation_id, relation_version, schema_fingerprint, stream_id,
  partition_id)`.
- Runtime execution rejects non-monotonic watermark or observed max event-time
  movement for the same source partition.
- Runtime checkpoint restore validates event-time frontiers in the checkpoint
  and runtime payload match, rejects malformed frontier contents, and verifies
  each frontier is bound to a declared catalog event-time column before
  restoring state.
- The first tumbling event-time aggregate slice is admitted through
  `VelorixLogicalViewPlanV1` for:

  ```sql
  select
    user_id,
    window_start,
    window_end,
    sum(amount) as total_amount,
    count(*) as event_count
  from tumble(purchases, event_time, interval '60 seconds')
  group by user_id, window_start, window_end
  ```

- The tumbling event-time aggregate family now supports `sum`, `count`, `min`,
  `max`, and `avg` over the same `Int64` value column. `min` and `max` keep
  checkpointed per-window value multisets so retractions can recompute extrema;
  `avg` is published from checkpointed weighted sum and count state.
- The tumbling runtime keeps explicit checkpointed window state, publishes only
  windows whose `window_end` is at or behind the current source-partition
  watermark frontier, and rejects late rows for already closed windows under the
  strict late-data policy.
- Tumbling output schemas use the composite materialized output primary key
  `[group_key, window_start, window_end]`; query serving reads the published
  materialized output surface rather than source batches.
- Tumbling runtime restore validates the admitted SQL, logical plan, checkpoint
  payload, event-time frontiers, and published output before accepting the
  checkpoint.
- API-level restart evidence now covers relation registration, tumbling window
  view registration, watermark-bearing ingest, query, crash-window replay after
  restart, and post-restart query for the first strict tumbling event-time
  aggregate family.

Verification commands for this evidence:

```bash
cargo test -p velorix-storage --test ingest_envelope ingest_envelope_preserves_event_time_watermark_and_covers_it_in_digest
cargo test -p velorix-storage --test ingest_envelope watermark
cargo test -p velorix-runtime --test materialized_view_runtime runtime_checkpoints_event_time_frontiers_by_source_partition
cargo test -p velorix-runtime --test materialized_view_runtime event_time
cargo test -p velorix-api event_time_watermark
cargo test -p velorix-api rest_latest_bool_view_materialized_output_replays_later_ingest_after_restart
cargo test -p velorix-core --test view_plan tumbling
cargo test -p velorix-core --test view_plan tumbling_event_time_aggregate_sql_lowers_min_max_avg_outputs
cargo test -p velorix-runtime --test materialized_view_runtime tumbling
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_tumbling_event_time_min_max_avg_and_restores_state
cargo test -p velorix-api materialized_runtime_output_schema_supports_tumbling_event_time_window
cargo test -p velorix-api rest_tumbling_window_view_materialized_output_replays_later_ingest_after_restart
```

Example target syntax:

```sql
select
  user_id,
  window_start,
  window_end,
  count(*) as event_count,
  sum(amount) as total_amount
from tumble(events, event_time, interval '1 minute')
group by user_id, window_start, window_end
```

This syntax should be admitted only after the logical plan, checkpoint, replay,
and output publication paths can prove deterministic results across restart.

## Explicit Non-Goals For The First Complete Implementation

- full Arroyo/Flink-compatible window SQL
- session windows
- hopping windows with pane sharing
- window joins
- temporal joins
- processing-time triggers
- early and late firing policies
- custom trigger policies
- arbitrary SQL
- CTEs
- nested subqueries
- set operations
- outer, semi, and anti joins
- non-equi joins
- UDFs
- arbitrary JSON, map, or struct expression evaluation
- multi-output view graphs
- distributed scheduling
- dynamic operator loading
- runtime Rust compilation
- external package deployment for user-defined views
- PVC-dependent execution
- source full-scan repair as the normal serving path
- fake fallback execution

## Acceptance Criteria

The roadmap is complete when these checks pass:

- a relation with non-prototype schema can be registered
- a second relation with a different schema can be registered
- both relations can ingest committed epochs
- a filter/project/aggregate view can be admitted and executed
- a two-relation inner equi-join view can be admitted and executed
- supported SQL is represented by `VelorixLogicalViewPlanV1`
- runtime construction uses the admitted plan
- unsupported SQL produces a clear admission error
- ingest publishes materialized output deltas
- query reads the materialized output table/page index
- restart restores state from checkpoint metadata and durable objects
- replay applies only epochs after the checkpoint
- a missing or corrupted checkpoint object fails closed
- a missing or corrupted output manifest fails closed
- a non-contiguous epoch range does not silently advance the view frontier
- no runtime path requires external package deployment, runtime compilation, or
  PVCs

## Implementation Order

1. Define `VelorixLogicalViewPlanV1`.
2. Add deterministic plan hashing and plan validation.
3. Replace SQL-shape recognizers with SQL-to-plan admission for the first SQL
   families.
4. Replace runtime dispatch by input arity with plan-based dispatch.
5. Implement generic filter/project/aggregate operators.
6. Implement keyed inner equi-join state.
7. Add materialized output delta/page manifests.
8. Add durable epoch manifests and contiguous frontier validation.
9. Add checkpoint object roots and fail-closed recovery.
10. Add end-to-end relation, ingest, view, query, restart tests.
11. Add event-time and watermark metadata without exposing window SQL.
12. Add tumbling event-time aggregate admission and runtime support.

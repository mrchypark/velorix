# Materialized View Runtime Roadmap

Status: First local-development materialized-view runtime milestone implemented
and locally verified. This is local development evidence only, not
release/product-complete evidence. Window, analytic, scoped backfill, and
background scheduling work remains internal or experimental.
Applies to: view admission, logical planning, incremental operators,
materialized output serving, checkpoint/recovery, and experimental window SQL.

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

Late-created views additionally follow the persisted snapshot/tail bootstrap
barrier defined in
[Incremental Processing Semantics V1](incremental-processing-semantics-v1.md#view-bootstrap-frontier).
The current boolean backfill-required scan is compatibility scaffolding and is
not sufficient completion evidence for concurrent view creation and ingest.

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

Materialized output segment and page metadata are planning indexes, not progress
authority. See
[Materialized Output Segment Index V1](materialized-output-segment-index-v1.md).
The checkpoint manifest remains authoritative, and every selected output page
must still be verified against manifest-bound object refs and content hashes.

## First Local-Development Milestone

The first local-development implementation milestone should intentionally
exclude user-facing window SQL. It should prove the generic materialized view
pipeline first.

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
- A narrow one-relation self-join lowers to two canonically identified scans
  (`scan_left` and `scan_right`) and one `InnerEquiJoin`. Its atomic fanout
  runtime supports one non-primary, non-null scalar equality feeding global
  `COUNT(*)`; wider self-join SQL fails closed.
- A bounded three-relation family lowers to three scans and exactly two
  left-deep `InnerEquiJoin` nodes. It accepts complete, type-identical,
  non-null composite-PK equalities and root-PK-grouped `COUNT(*)` only. Runtime
  execution is the existing `NativeOperatorGraph` with two binary joins,
  project, and aggregate; restore re-derives and fully matches the logical plan
  and rejects torn graph/output/frontier/idempotency epochs.
- Three-input join order is a durable planner policy. New schema-v2 plans keep
  the output-key root fixed and order the two non-root roles by stable relation
  ID. Field-absent schema-v1 plans retain legacy SQL encounter-order semantics.
  Restore re-lowers with the stored policy, not the current default, and
  rejects unknown policy IDs; different SQL hashes remain different programs.
- Phase 2 join-distribution evidence now runs five deterministic non-primary
  inner-join profiles through production SQL admission and native incremental
  execution: one-to-one, one-to-many, many-to-many, 80% hot-key skew, and
  fully unmatched inputs. Each sample checks an independently derived per-key
  `SUM`/`COUNT` oracle and checkpoint-visible left/right multiset cardinality.
  The release benchmark, result validator, and archived PR-smoke gate pass;
  its five-sample p95 values are descriptive workload evidence, not an SLO.
- Phase 3A fixes outer joins as dynamic SQL-bag tables whose signed output
  is the consolidated difference between committed snapshots. First/last match
  transitions retract or restore null-extended rows, the non-preserved schema
  is nullable, unproved keys are dropped, and both input bags are checkpointed.
  Left/right and the bounded scalar-PK full-join family now prove this
  definition across the completed payload/key-update matrix; broader
  outer-join shapes remain deliberately fail-closed.
- Unbounded outer-join output cannot cross an append-only/final consumer edge:
  the capability validator rejects the changelog mismatch. V1 has no implicit
  finalization escape hatch; any later bounded or watermark-based exception
  must be represented by an explicit closing operator and output guarantee.
- Left-join match multiplicity is derived from the checkpoint-authoritative
  right multiset for each touched key. Partial match deletion remains matched;
  only deletion of the final occurrence restores the null-extended row, with no
  independently cached counter that can drift during recovery.
- Admitted grouped left joins can now retain nullable right-side values as
  aggregate and filter inputs. Right-referencing top-level `WHERE` predicates
  run after the join, empty/all-NULL right aggregates publish SQL NULL, counts
  preserve SQL NULL rules, and extended state restores exactly. Raw joined-row
  projection and provenance-sensitive right source filters remain fail-closed.
- SQL `RIGHT JOIN` has no independent logical or runtime operator. Admission
  swaps operands and key direction into the same canonical `LeftEquiJoin`; the
  checkpoint persists `join_kind = left` and the swapped relation identities,
  and restore reproduces unmatched preserved-side rows through that path.
- Bounded `FULL OUTER JOIN` admits one scalar primary-key equality only, with
  an explicit `COALESCE(left_key, right_key)` output/group key. It lowers to
  `FullEquiJoin`, retains both input bags, publishes symmetric null-extended
  rows, and retracts/restores them exactly at each side's zero/nonzero match
  boundary. Native exact-delta, public SQL, checkpoint/restart, and independent
  common-DAG equivalence tests cover duplicate multiplication and partial/final
  deletes. Residual `ON`, composite/non-primary keys, and source-filter
  rewrites remain fail-closed.
- The Phase 3A row matrix now covers checked duplicate multiplication, a real
  nullable payload, same-epoch key changes represented as old-key retract plus
  new-key insert, and partial/final deletes from either side. Native and public
  checkpoints restore simultaneous left-only/right-only state before later
  match transitions, then continue through another restore and tail changes.
- Phase 3A benchmark coverage includes a 97.5%-unmatched FULL JOIN snapshot
  workload and a transition workload that moves 1,024 unmatched occurrences
  into 512 matches by replacing the complete right key set. Both run five
  samples through production SQL admission/native execution and verify the
  complete materialized snapshot per sample; their local-debug latency is
  descriptive evidence rather than an SLO.
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
- Admission tests cover unsupported one-input and join SQL families, including
  non-equality joins and SQL outside the narrow family-specific scopes. The
  supported aggregate runtime includes `COUNT(DISTINCT ...)`; the supported
  join runtime includes a constrained `LEFT JOIN` that preserves unmatched left
  rows. The exact contract, including left-join restrictions and experimental
  SQL, is documented in [Supported materialized-view SQL](supported-sql.md).
- Identity CTE source filters are admitted for the supported one-input,
  window, latest-by-key, and two-relation join shapes; supported two-relation
  join aggregate views can also apply admitted join `WHERE` predicates within
  their family-specific scope.
- Single-relation and two-relation inner-join aggregate views support a simple
  `HAVING` comparison against a projected aggregate output.
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
  - public REST creation of the narrow self-join, durable plan/instance identity,
    singleton counts `10`, `5`, and `0`, service restart, forced checkpoint
    publication failure, authoritative-pointer recovery, durable-tail replay,
    and duplicate-offset idempotency
  - public REST creation of the bounded three-input composite-PK join, durable
    two-join DAG identity, multiplicity `24`, publisher-manifest query, service
    restart, post-restart retract to `18`, and duplicate-offset idempotency
- A source guard test covers active product runtime source and fails if external
  compiler, JAR, pipeline-manager, DBSP/Feldera, or PVC-dependent execution
  references re-enter the runtime path.

Verification commands for this internal experimental-only evidence. These
commands do not make window SQL part of the public 1.0 contract; the product
API admission path rejects window SQL.

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
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_single_relation_aggregate_having_view
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_two_relation_join_having_view
cargo test -p velorix-core --test view_plan filtered_projected_single_key_aggregate_sql_lowers_to_projected_accumulators
cargo test -p velorix-core --test view_plan single_key_aggregate_sql_lowers_having_to_post_aggregate_filter
cargo test -p velorix-core --test view_plan two_input_join_sql_lowers_having_to_post_aggregate_filter
cargo test -p velorix-api rest_aggregate_having_view_materializes_outputs
cargo test -p velorix-api rest_two_relation_join_having_view_materializes_outputs
cargo test -p velorix-core --test view_plan self_join_uses_canonical_scan_instances_independent_of_sql_aliases
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_atomic_self_join_fanout_across_retract_and_restart
cargo test -p velorix-api rest_self_join_atomic_fanout_survives_restart_replay_and_final_retract
cargo test -p velorix-core --test view_plan three_input_composite_pk_sql_lowers_to_validated_binary_dag
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_three_input_composite_pk_join_through_binary_dag
cargo test -p velorix-runtime --test materialized_view_runtime three_input_join_epoch_rolls_back_on_overflow_and_restore_rejects_torn_checkpoint
cargo test -p velorix-runtime --test materialized_view_runtime three_input_join_order_policy_preserves_results_state_and_legacy_restore
cargo test -p velorix-api rest_three_input_composite_pk_join_uses_binary_dag_and_survives_restart
cargo test -p velorix-runtime --test materialized_view_runtime runtime_rejects_non_contiguous_input_offsets_without_advancing_frontier
cargo test -p velorix-runtime --test no_external_runtime_dependencies
cargo test -p velorix-core --test view_plan
cargo test -p velorix-core --test relation
cargo test -p velorix-storage --test relation_catalog_registry
```

Full workspace verification for the first local-development milestone passed
locally on 2026-06-15. This is not release evidence:

```bash
cargo test --workspace
cargo fmt --all --check
git diff --check --
```

## Experimental Window Foundation Milestone

Window SQL is not part of the public 1.0 default contract. The implementation
evidence below is retained as internal/experimental groundwork. Before exposing
window SQL by default, Velorix needs durable event-time semantics:

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
- Hopping and session event-time aggregate views are admitted into the same
  window runtime family and now have runtime plus REST relation/view/ingest/query
  evidence.

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
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_hopping_event_time_windows
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_session_event_time_windows
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_tumbling_event_time_min_max_avg_and_restores_state
cargo test -p velorix-api materialized_runtime_output_schema_supports_tumbling_event_time_window
cargo test -p velorix-api rest_tumbling_window_view_materialized_output_replays_later_ingest_after_restart
cargo test -p velorix-api rest_hopping_and_session_window_views_materialize_outputs
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

## Explicit Non-Goals For The First Local-Development Implementation

- full Arroyo/Flink-compatible window SQL
- window families beyond the currently admitted tumbling, hopping, and session
  aggregate shapes
- window joins
- temporal joins
- processing-time triggers
- early and late firing policies
- custom trigger policies
- arbitrary SQL
- CTE shapes beyond the currently admitted identity source-filter forms
- nested or undecorrelated subqueries beyond the bounded complete-PK-correlated
  `EXISTS`/`NOT EXISTS` forms
- set operations
- join shapes beyond the currently admitted inner, bounded outer, and bounded
  complete-PK-correlated semi/anti families
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

## Ingest Optimization Contract

The default REST ingest acknowledgement is `materialized`: an ingest request
returns only after the durable ingest append has been applied to active
materialized views and the checkpoint pointer has been published. This preserves
the first-complete product contract that a successful ingest has automatically
advanced materialized output.

The public 1.0 ingest API does not expose an async append-only acknowledgement
or an `ack_mode` request field. Stale clients that send `ack_mode` fail closed
as unknown-field requests before materialization starts.
Queries read published materialized output only; they do not replay source data
or perform catch-up materialization on the read path. If a late-created view
still needs historical input, queries return a materialization lag error until
the fenced materializer/operator replay path advances and publishes the required
output.

Users ingest rows through `/v1/relations/{relation_id}/ingest` for one relation,
or `/v1/relations/ingest` as a convenience surface for an ordered list of
relation ingests. Active materialized views that depend on each relation update
are updated automatically before that update's `materialized` ack returns.
Internally both paths use the epoch materialization machinery so relation
updates and view checkpoint publication share the same coalesced write path.
`/v1/ingest/epoch` remains internal/test-only, not the product ingest API.

### Public Join Frontier Contract

Public 1.0 join consistency is a per-relation frontier-vector contract, not an
atomic grouped multi-relation transaction contract. Each relation ingest advances
only that relation's stream/partition frontier. For a two-relation join,
sequential left/right ingests may publish and expose intermediate materialized
join output at vectors such as `{left: N, right: M}` followed by
`{left: N + 1, right: M}` and later `{left: N + 1, right: M + 1}`.

`/v1/relations/ingest` must not be described as an atomic multi-relation
transaction API. If it accepts multiple relation batches, the public contract is
equivalent to a deterministic sequence of relation ingests with materialized
acknowledgement per accepted relation update. It does not provide all-or-nothing
rollback across relation batches, a single global epoch visible to clients, or a
hidden guarantee that a join only becomes visible after both sides advance.

Every published runtime checkpoint for a join must carry the complete
per-relation input frontier vector it represents. Materialized output manifests
must either carry the same vector directly or be readable only through a
checkpoint/pointer that carries that vector. A standalone output manifest that
can be selected without the checkpoint-bound input frontier vector is not
sufficient product evidence.

The internal ingest epoch remains an implementation batch unit only. It may
coalesce relation updates and checkpoint/state writes behind the public relation
ingest URLs, but it is not a public atomicity boundary and must not be used to
claim atomic multi-relation join semantics.

Relation ingest paths always use the synchronous `materialized`
acknowledgement. The internal epoch is an implementation batch unit only; it
does not expose acknowledgement negotiation to clients.

Ingest checkpoints publish signed output delta refs and durable state payloads
that include the checkpoint-bound published output. Query serving reads only
that durable checkpoint-bound materialized output or a compacted output manifest
bound to the same checkpoint. It does not read live in-process accumulator
state. Full compacted materialized-output snapshots are internal maintenance
artifacts for public 1.0, not a public endpoint or response mode. Public 1.0
does not republish checkpoint pointers from compaction until immutable
manifest-keyed checkpoint compaction is available.

Backfill replays committed ingest evidence through the same materialized-view
runtime path. Public 1.0 does not expose request-scope, range, predicate, or
background backfill. Operator-triggered full replay may be used to make a
late-created view queryable; disabled scopes fail admission instead of
pretending to have materialized only a narrower request range.

Background output compaction and scheduling are internal/experimental only. The
product knob `VELORIX_OUTPUT_COMPACTION_INTERVAL_EPOCHS` is rejected by the
public 1.0 API. Durable checkpoint writes are coalesced into
ingest/materialization commits with output delta refs and state payloads rather
than produced by a separate background checkpoint daemon.

Every ingest response includes stable workload and materialization counters:

- `timings.total_ms`, `timings.total_us`, `timings.batch_count`,
  `timings.row_count`, `timings.avg_batch_us`, `timings.avg_row_us`,
  and `timings.rows_per_second`
- detailed per-stage timings are intentionally not part of the public 1.0
  response contract; publish them through traces or metrics
- `materialization.status`
- `materialization.active_views`
- `materialization.applied_batches`
- `materialization.checkpoint_writes`
- `materialization.applied_batches_per_checkpoint_write`
- `materialization.output_delta_writes`
- `materialization.state_payload_writes`
- `materialization.checkpoint_record_writes`
- `materialization.checkpoint_pointer_writes`
- `materialization.latest_cache_writes`
- `materialization.checkpoint_publication_writes`

Internal background output compaction work must still be deduplicated per view,
but public 1.0 responses must not expose background task names or scheduling
state.

These fields are diagnostic evidence, not correctness authority. Durable
correctness still comes from the ingest log, signed output deltas, checkpoint
state records, and Hiqlite checkpoint pointer authority.

## Incremental View Dependencies

View-on-view work uses the existing native runtime and recovery model. Admission
now persists `PublishedRelationBindingV1` for each output in the active runtime
record. The binding fixes the producer generation, logical-plan hash, public
relation schema, key descriptor, schema/key hashes, output stream, signed-delta
codec, and producer-commit frontier kind. Public relation schemas never acquire
a hidden delta-weight column.

This metadata is only the first verified slice; it does not yet make view output
consumable. The remaining dependency path must use direct typed signed deltas,
consumer cursors, and one authoritative `CausalCutV1` over direct source and view
inputs. Durable producer commit records are now implemented: every new published
epoch, including an empty delta, seals the binding identity, checkpoint/state,
direct-source coverage, consolidated delta, and a canonical commit digest behind
the authoritative checkpoint pointer. Legacy delta records remain readable. The
transitional direct-source coverage seal has been replaced in new producer
commits by a domain-separated `CausalCutV1` digest. The checkpoint cut
canonically records separate direct source frontiers and direct view cursors;
the old `producer_input_coverage_hash` commit field no longer exists. Bootstrap
input coverage remains temporarily co-encoded and must exactly match the cut's
source portion, so it cannot become a second progress truth. `CausalCutV1` is
the recovery authority; `input_coverage` is only a bootstrap-compatibility
mirror.

The commit object is never authority by existence alone. Publication authority
is the single metadata-pointer chain through its checkpoint reference, so an
orphan written before a failed pointer CAS must be ignored. A follow-up strict
review accepted this producer-commit slice on that bounded basis and retained
canonical mixed source/view `CausalCutV1` plus fail-closed orphan recovery as the
next correctness boundary. That implementation and its local recovery evidence
now pass, and a strict follow-up review returned bounded `GO` with no P0/P1 for
the cut replacement itself. Production cursor resolution/consumption is still
unimplemented and remains the next fail-closed boundary.

A tenant DAG revision must bind edges to immutable generations. Producer deltas
may be garbage-collected only after every live dependent's durable cursor, and a
missing segment fails closed rather than triggering an implicit snapshot
recomputation. See the Phase 4 ledger in the gap-closure plan for checkable
evidence.

## Acceptance Criteria

The local-development runtime milestone is satisfied when these checks pass:

- a relation with non-prototype schema can be registered
- a second relation with a different schema can be registered
- both relations can ingest committed epochs
- a filter/project/aggregate view can be admitted and executed
- a two-relation inner equi-join view can be admitted and executed
- supported SQL is represented by `VelorixLogicalViewPlanV1`
- runtime construction uses the admitted plan
- unsupported SQL produces a clear admission error
- ingest publishes materialized output deltas and durable state checkpoints
- query reads checkpoint-bound published materialized output, with compacted
  output snapshots as the maintenance/read-optimized surface
- restart restores state from checkpoint metadata and durable objects
- replay applies only epochs after the checkpoint
- a missing or corrupted checkpoint object fails closed
- a missing or corrupted compacted output manifest fails closed when that
  manifest is used
- a non-contiguous epoch range does not silently advance the view frontier
- no runtime path requires external package deployment, runtime compilation, or
  PVCs

## Implementation Order

The completed foundation below is followed by the checkable, capability-focused
[Incremental SQL Gap-Closure Plan](incremental-sql-gap-plan.md). Use that plan for
new SQL breadth work and keep this section as the historical construction order.

1. Define `VelorixLogicalViewPlanV1`.
2. Add deterministic plan hashing and plan validation.
3. Replace SQL-shape recognizers with SQL-to-plan admission for the first SQL
   families.
4. Replace runtime dispatch by input arity with plan-based dispatch.
5. Implement generic filter/project/aggregate operators.
6. Implement keyed inner equi-join state.
7. Add materialized output delta refs and checkpoint state payloads.
8. Add durable epoch manifests and contiguous frontier validation.
9. Add checkpoint object roots and fail-closed recovery.
10. Add end-to-end relation, ingest, view, query, restart tests.
11. Add event-time and watermark metadata without exposing window SQL.
12. Add tumbling event-time aggregate admission and runtime support.
13. Add materialized output segment/page pruning.

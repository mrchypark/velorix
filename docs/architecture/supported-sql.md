# Supported materialized-view SQL

**Status: 2026-09-05; contract verified against committed code/tests through
`677c125` (not a moving-HEAD claim).** The verification range includes the
latest authoritative, no-PVC product deployment gate in
`scripts/run-vind-product.sh`. This is the canonical contract for
`POST /v1/views`.
It is deliberately narrower than parser acceptance and than SQL accepted by a
read-only query over an already materialized output. A view is admitted only
when registered input catalogs resolve, output-schema derivation succeeds, a
typed `VelorixLogicalViewPlanV1` is built, the native runtime accepts it, and
public-policy checks pass. Unsupported SQL or view shapes fail closed with a
clear 4xx admission error; there is no source-recomputation fallback.

## Product flow

1. `POST /v1/relations` registers an explicit relation schema and key.
2. An ingest endpoint validates schema-bound rows and commits an ingest epoch.
3. `POST /v1/views` resolves registered schemas and admits a typed native plan.
4. The standing runtime applies committed deltas and persists materialized output.
5. The view query endpoint reads that published output, never source batches.

A late-created view is `backfill_required` until its backfill completes. That
state is materialization progress, not an alternate query implementation.

## Authoritative relation-ingest operational mode

The SQL capability table below is unchanged by authoritative relation ingest.
When `VELORIX_API_AUTHORITATIVE_RELATION_INGEST=1`, the API requires a metadata
service with the relation-ingest capability and a non-empty, stable
`VELORIX_RELATION_INGEST_OWNER_ID`. For each accepted relation batch, the
feature-gated path obtains relation-scoped authority, reserves the range,
writes a bounded staging object, and publishes it through metadata. The native
runtime then applies the published relation batch. During checkpoint
persistence, validated input coverage is persisted. On restart recovery, the
replay frontier and checkpoints drive a fresh capture and validation of the
relation source cut from Meta.

This authoritative mode intentionally accepts exactly one relation batch per
request. A multi-batch request fails closed; atomic multi-batch publication is
not claimed. With the gate off, the legacy object-store admission path remains
in effect. This is implementation and focused-test evidence, not evidence of a
current live K8s deployment.

## Admission matrix

“Default public” means reachable through the default public API configuration.
“Experimental-gated” requires `experimental_advanced_view_features=true`.
“Internal but publicly unreachable” means a runtime test exists but public
schema derivation/admission does not expose it; it is not a product capability.
“Default public path; API E2E/restart verification pending” means the default
API can reach the validator and runtime, but an API
admission-to-materialization-to-restart test has not yet supplied end-to-end
evidence for that family.

| SQL family | Status | Exact bounded scope / evidence |
| --- | --- | --- |
| Filters and projections | Default public | One registered relation, key-preserving direct projection plus bounded `WHERE` predicates. `filter_project_*` plan tests and REST tests in `crates/velorix-api/src/tests.rs`. |
| Computed Int64 projections | Default public | Registered Int64 columns/literals and the admitted deterministic arithmetic, casts, `abs`, `greatest`/`least`, `coalesce`, `CASE`, and `if` forms. Output key, type, and nullability must match the derived schema. `filter_project_sql_accepts_computed_int64_projection`. |
| `SELECT DISTINCT` | Default public | Only when the output has a valid, non-duplicated output key; `DISTINCT ON` is rejected for filter/project views. The plan tests at `view_plan.rs:775` through `:887` cover admitted and rejected key shapes. |
| Same-relation distinct set operations | Default public | `UNION DISTINCT`, `INTERSECT DISTINCT`, and `EXCEPT DISTINCT` only for validated filter/project branches over one relation with compatible direct projections; `ALL`, cross-relation branches, and unsupported computed branches fail closed. `filter_project_union_distinct_same_relation_lowers_to_filter_project_plan`, `filter_project_intersect_distinct_same_relation_lowers_to_filter_project_plan`, `filter_project_except_distinct_filtered_left_lowers_to_left_and_not_right`, and `rest_filter_project_union_distinct_view_materializes_outputs`. |
| Bounded CTE / derived sources | Default public | Identity or single-source filter/direct-projection CTE and derived-table forms only, when required key/value/order/predicate columns remain traceable to catalog columns. They are not general, recursive, or multi-source subqueries. `filter_project_sql_accepts_identity_cte_source_filters`, `filter_project_sql_accepts_derived_table_source_filters`, and `rest_filter_project_derived_table_view_materializes_outputs`. |
| Grouping and basic aggregates | Default public | Typed group keys plus `SUM`, `COUNT(*)`, `COUNT(column)`, `MIN`, `MAX`, `AVG`; global aggregation is limited to the admitted count shape. `single_key_aggregate_*` and `rest_composite_and_global_aggregates_survive_restart_and_final_retraction`. |
| `COUNT(DISTINCT column)` | Default public | One supported aggregate input, including documented join restrictions; no multi-column or other distinct aggregates. `rest_two_relation_join_count_distinct_view_materializes_outputs`. |
| `HAVING` and aggregate `FILTER` | Default public | Must bind exactly to a projected aggregate/admitted input. `rest_aggregate_having_view_materializes_outputs` and `rest_two_relation_join_having_view_materializes_outputs`. |
| Latest / arg extrema | Default public | One `arg_min(value, ordering)` or `arg_max(value, ordering)`, grouped by the input primary key. `latest_by_key_*` plan tests. |
| Top-K | Default public | `ORDER BY` plus literal positive `LIMIT`/`FETCH`, optional literal non-negative `OFFSET`; public limit is 1,000. |
| Inner join | Default public | Two registered inputs with validated equality/key restrictions and admitted aggregate/project shapes. `rest_two_relation_join_view_materialized_output_survives_api_restart`. |
| Outer joins | Default public | Narrow left/right grouped forms and a narrow full join with the required coalesced key; raw/general outer joins are rejected. `rest_left_join_left_group_key_view_materializes_unmatched_left_rows` and `rest_right_join_swaps_operands_and_materializes_unmatched_right_rows`. |
| Self join | Default public | Two aliases of one relation, one non-primary scalar equality, global `COUNT(*)` only. `rest_self_join_atomic_fanout_survives_restart_replay_and_final_retract`. |
| Semi / anti join | Default public | Direct correlated `EXISTS`/`NOT EXISTS` equality over two single, non-null scalar primary keys; not general subquery support. `correlated_exists_*` and `rest_exists_and_not_exists_views_survive_restart_and_match_transitions`. |
| Three-way join | Default public | Exactly three inputs, left-deep inner joins, complete composite-PK equalities, root-PK projection/grouping, and one `COUNT(*)`. `rest_three_input_composite_pk_join_uses_binary_dag_and_survives_restart`. |
| Cross join | Default public | Specifically validated two-input cross-join projection (`validate_supported_cross_join_sql`), not general join composition. `rest_join_families_materialize_retract_and_restart` checks all Cartesian pairs, API-state restore and post-restore retraction. |
| Event-time windows | Default public | `TUMBLE`, `HOP`, and `SESSION` over the validated aggregate shape and declared event-time/watermark contract. `tumbling_event_time_aggregate_sql_accepts_subsecond_interval_units` and `rest_hopping_window_advanced_aggregate_view_survives_api_restart`. |
| Recursive CTE | Default public | The default API reaches the narrow positive `UNION DISTINCT` fixpoint grammar; arbitrary recursive SQL is rejected. Two-CTE definitions are admission-validated, but execution follows the first CTE because the outer query selects only it. Restore recomputes and validates that reachable closure. Runtime evidence: `recursive_cte_materializes_closure_exactly_across_retract_restart_and_fail_closed`; API admission-to-materialization-to-restart evidence: `rest_recursive_cte_outer_first_ignores_disjoint_second_cte_across_restore` (disjoint second-CTE input is excluded from output). |
| Interval join | Default public | Two-input inner overlap with exact strict endpoint comparisons and bounded projection, with no grouping or `HAVING`. Runtime evidence: `interval_join_materializes_overlap_retraction_and_restart`; public API evidence: `rest_join_families_materialize_retract_and_restart`, including disjoint/touching intervals, API-state restore and post-restore retraction. |
| Temporal/as-of join | Default public | The default API can reach the bounded two-input temporal/equality/projection validator/runtime. Runtime evidence: `temporal_join_materializes_asof_match_and_retracts`; API restart/retraction evidence: `rest_temporal_asof_join_materializes_retracts_and_restores`. |
| Percentile and median | Default public | Grouped `median`, `percentile_disc`, and `percentile_cont` use direct Int64 input columns and validated numeric literal percentiles in `[0, 1]`; global median, string/Decimal128 inputs, and invalid percentile shapes/types fail closed during admission. They are not supported in join output-schema construction. Public factory evidence: `rest_grouped_median_and_percentiles_materialize_and_survive_api_restart`; rejection evidence: `rest_percentile_admission_rejects_global_invalid_and_non_int64_inputs`; runtime evidence: `percentile_aggregates_are_exact_across_retract_and_restart`. |
| `ROW_NUMBER`, `RANK`, `DENSE_RANK` | Experimental-gated | One relation with validated partition/order/tie-breaker and bounded rank-filter form. Default admission returns an explicit experimental-disabled error; `public_1_0_rejects_experimental_view_surfaces_by_default`. |
| Scalar aggregate subquery filter | Default public | Narrow two-input outer comparison against one unfiltered inner global `SUM`, `COUNT`, `MIN`, `MAX`, or `AVG` (including `COUNT(*)`), not arbitrary subqueries. The public factory dispatches `ScalarAggregateFilter`; API evidence: `rest_scalar_aggregate_modifier_admission_and_plain_matrix`. This does not broaden standalone global aggregate views beyond `COUNT(*)`. |
| Analytic navigation frames | Internal but publicly unreachable | `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE` runtime coverage exists (`analytic_window_frames_navigation_materializes_and_restores`), but public schema derivation does not expose it. |

<a id="event-time-windows-and-bounded-corrections"></a>

## Event-time windows and unlimited lateness

The following describes the verified core/runtime/API window contract; this is
not a live deployment claim. See the example requests in
[the local guide](../how-to.md#event-time-windows-with-unlimited-lateness).

Both the existing table-function form and the projected window-alias form use
the same native admission/runtime path. For a registered `purchases` relation
with primary key `user_id`, declared Int64 nanosecond event time `event_time`,
Int64 value `amount`, and signed weight `delta`, the alias form is:

```sql
SELECT TUMBLE(INTERVAL '60 seconds') AS window, user_id,
       SUM(amount) AS total_amount, COUNT(*) AS event_count
FROM purchases
GROUP BY window, user_id
```

`HOP(INTERVAL '30 seconds', INTERVAL '60 seconds')` replaces `TUMBLE(...)`
for 30-second slides over 60-second windows. Normal mode also accepts
`SESSION(INTERVAL '30 seconds')` as the projected window alias; it uses the
declared event-time column and a 30-second session gap. The projected window and key must
precede the aggregates, and `GROUP BY` must reference the window alias and key
exactly once. The alias normalizes to flat `user_id`, `window_start`,
`window_end`, and aggregate output columns. It does **not** return a `window`
struct or support `window.start`/`window.end` access. The equivalent legacy
form explicitly projects and groups `window_start`/`window_end` from
`TUMBLE(purchases, event_time, INTERVAL '60 seconds')`.

Window grouping currently requires the registered relation's single scalar
primary key. An arbitrary business column, such as `category` when the key is
`order_id`, is not supported as the grouping key. The admitted aggregate shape
supports `SUM`, `COUNT(*)`, `COUNT(column)`, `MIN`, `MAX`, and `AVG`; value
aggregates and `COUNT(column)` must use the same admitted input value column.
The Int64 example above fits that shape; the general aggregate matrix does not
imply arbitrary types, expressions, or multiple value columns in windows.

`POST /v1/views` accepts optional `correction_horizon_ns` (also
`correctionHorizonNs`), a strictly positive signed 64-bit nanosecond duration.
It maps to `LateRowPolicy::CorrectWithinHorizon { horizon_ns }` in the typed
plan. Despite the retained names, this duration controls hot-state retention,
not an admission cutoff. For new public compatible `TUMBLE`/`HOP` views,
omission/null selects this policy with `horizon_ns = window_size_ns` (one
window width, not the HOP slide). Correction mode is limited to `TUMBLE` and `HOP`
without Top-K; `SESSION`, Top-K, and non-window correction requests fail closed
during admission. Ordinary `SESSION` and window Top-K keep plan policy `None`:
they accept valid late rows with all window state retained. Cold-state correction
configuration remains unsupported for those families. An internal explicit
`Some(LateRowPolicy::Reject)` opts into strict rejection; the enum's `Default`
does not define the meaning of plan `None`.

For effective watermark `W`, hot retention `H`, and a fixed window end `end`,
the runtime behavior is:

| Condition | Behavior in correction mode |
| --- | --- |
| `end > W` | Keep the window open; no published row yet. |
| `W - H < end <= W` | Publish the aggregate and retain correction state; signed inserts/retractions can change the published row. |
| `end <= W - H` | Move correction state out of hot retention only with durable cold state available; keep published output. Load only affected windows to accept valid late corrections. |

Window-end age controls state placement, not correction eligibility. Valid
late rows remain accepted beyond hot retention, including HOP rows affecting
both hot and cold targets, with atomic updates across their affected windows.
The accepted batch's watermark advances publication and hot-state retention.
Queries read materialized snapshots whose historical rows can change at any
age. This differs from `AdmitWithinAllowance`, which
delays finalization rather than correcting already published fixed windows.

Every window input batch must carry explicit `event_time_watermark` metadata
matching the relation's declared event-time column. Per-partition watermarks
must be monotonic, and `max_observed_event_time_ns` must cover the batch's
actual maximum event time. The effective watermark is the minimum across
tracked input partitions. Correction mode also requires that this effective
global watermark never fall below its previously committed value. An unseen
partition must not introduce a watermark below the committed global watermark;
such an input fails closed during preflight before source writes. Per-partition
monotonicity alone does not satisfy this global requirement. Missing progress
from a tracked partition can pin closure/expiry; this does not make the
required watermark field optional on window input batches.
There is no automatic wall-clock/idle-partition advance promised here.

The current public ingest API uses synchronous materialized acknowledgement:
success follows runtime application, checkpoint publication, and dependency
draining. There is no public asynchronous acknowledgement mode in this path.

Correction preflight reads live plans and previously committed frontiers under
per-view guards, without cloning historical state. View creation, ingest, and
backfill share a per-relation admission mutex within one API process to order
activation against source publication within one shared `ApiState`. This new fence is process-local; it
does not establish distributed admission safety across a second API process.
Cross-process safety depends on the existing metadata protocol and is not
verified by this extension's local fence.

An internal retention contract may keep hot state longer; it must not introduce
a lateness cutoff. Hot-state eviction is not an output TTL: materialized historical rows
remain queryable after their state moves to cold storage. Hot retention therefore
does not bound total output storage, open-window cardinality, or all retained
state under a stalled watermark. The legacy `StateRetentionContractV1` does
not by itself prove that every closed window's state is released; correction
mode requires durable affected-window recovery before hot eviction. Policy,
retained state, and published output belong to checkpoint recovery; no source
full-recomputation or DataFusion fallback is added. Older stored explicit
correction policies retain their wire identity but acquire unlimited-lateness
semantics; state discarded by an older runtime needs recovery evidence before
that compatibility path is considered complete. Stored `None` keeps its absent
representation and now accepts late rows with retained state.

The optimization targets affected-window state and output deltas. `EpochCommit`
still includes published output batches, and full published-snapshot
construction/serialization may remain necessary. Do not infer whole-epoch
O(affected windows), bounded total snapshot cost, or benchmark improvement from
this contract alone.
Published output remains in memory and subject to the existing 8 MiB snapshot
cap. That output-cardinality limit is not a lateness limit.

Immutable cold objects use a key namespace intentionally excluded from legacy
garbage collection. Archive versions remain retained without GC until traversal
of references from retained standing checkpoints is implemented and verified.
Disk/object storage therefore grows with window history and corrections. This
does not guarantee source retention under an external TTL or storage lifecycle
policy; external deletion remains outside this retention contract. The full
snapshot and published-output memory baseline remains unchanged.

Cold-state recovery fails closed on availability or integrity errors: missing
cold state returns HTTP 503, and corrupt cold state returns HTTP 400. Neither
failure is based on the age of an otherwise valid late row.

Source evidence: `normalize_projected_event_time_window` and
`validate_supported_tumbling_window_sql_with_policy` in
`crates/velorix-core/src/view_plan/mod.rs`, `CreateViewRequest` in
`crates/velorix-api/src/lib.rs`, admission in
`crates/velorix-api/src/view_admission.rs`, and the correction path in
`crates/velorix-runtime/src/materialized_view_runtime/event_time_window.rs`.
Focused planner fixtures are
`projected_event_time_window_alias_uses_flat_boundary_schema`,
`projected_event_time_window_alias_fails_closed`, and
`correction_policy_requires_positive_hot_retention_and_fixed_windows_only` and
`correction_policy_lowering_preserves_sql_identity_and_absent_policy`.
Core planner verification passed 363 tests. Runtime verification passed 261
integration tests and 55 library tests, with clippy `-D warnings` and formatting
checks passing. Runtime evidence includes:

- `late_correction_hop_hydrates_only_expired_fanout_atomically`: targeted cold hydration and atomic HOP fanout.
- `cold_correction_exports_bounded_chunks_and_keeps_pending_rows_in_rollback_checkpoint`: bounded chunk export and rollback state.
- `cold_correction_expired_extrema_avg_missing_corrupt_and_fault_rollback` and `late_correction_boundary_noop_last_retraction_and_checkpoint_validation`: cold-state integrity, failure rollback, and checkpoint validation.
- `cold_correction_legacy_expired_auxiliary_state_replays_only_requested_window`: legacy replay scoped to the requested window.
- `legacy_none_accepts_unlimited_late_tumble_hop_session_and_top_k`: absent-policy late acceptance across existing families.

Cold restore validates hash, object key, epoch, and program identity before
using archived state. These checks establish affected-window recovery without
a full-source recomputation fallback.

API library verification passed 221/221 tests. Workspace formatting checks and
clippy `--workspace --all-targets -- -D warnings` passed; storage library and
storage test targets also passed. API fixtures
`rest_default_window_far_late_extrema_and_avg_load_only_affected_state` and
`authoritative_default_window_far_late_extrema_and_avg_load_only_affected_state`
verify new-range HTTP 201, idempotent retry HTTP 200, and HTTP 200 query results
with the corrected old window, without an age cutoff. They cover missing cold
state (503), corrupt cold state (400), and identical retry after archive failure
without restarting the API. `rest_far_late_legacy_checkpoint_reconstructs_affected_window`
and `authoritative_far_late_legacy_checkpoint_reconstructs_affected_window`
verify affected-window reconstruction for legacy checkpoints through both
legacy and authoritative ingress. Recovery uses affected window state without
a full-source recomputation fallback.
The historic committed-code contract date above remains unchanged;
this contract establishes no new multi-API guarantee or benchmark result.

## Explicit rejection boundary

Everything outside the table is rejected during admission: arbitrary subqueries,
general navigation frames, unbounded analytic windows, recursive forms outside
the validated grammar, unsupported join compositions, four-or-more-input views,
unsupported distinct aggregates, `UNION ALL`, `ROLLUP`/`CUBE`/`GROUPING SETS`,
DDL/DML, multiple statements, and parser-only syntax. Query-time SQL is a
separate read-only DataFusion surface over one published output table; it never
expands materialization support.

The subsequent public API evidence is
`rest_temporal_asof_join_materializes_retracts_and_restores`. It covers only the
narrow [issue #27 ASOF contract](https://github.com/mrchypark/velorix/issues/27):
single UTF8 primary keys, non-NULL supported event-time/output columns, exact
left/right primary-key equality, and the mandatory `WHERE right_pk IS NOT NULL`
guard, including requery after restore and right-side retraction. This completed
evidence supersedes the earlier pending API-fixture note, but does not claim full
ASOF or general temporal INNER JOIN support. These in-process API tests do not
claim a network load test or a cluster rollout.

## Operational boundaries

Recovery is intentionally jarless and no-PVC. A replacement pod must recover
from durable remote object storage plus metadata; node-local storage can support
only a same-host restart and is not replacement-pod durability. The committed
materialized output/checkpoint must be recovered and queried without scanning
source ingest. Production and adversarial proof of that contract remains
pending. Do not introduce PVC-backed view state, package-loaded runtimes, or a
source-query fallback as a shortcut.

GitHub Actions provides build/test/release gates. The GHCR workflow records
digest-pinned image references; SHA-named tags remain mutable and provenance is
disabled, so neither tag spelling nor workflow completion alone is immutable
provenance evidence. Those delivery controls are not evidence that an internal
runtime is a public SQL capability; this matrix and its named tests are the
capability authority. Do not add a Cloud Build path.

## Focused validation

```sh
cargo test -p velorix-core --test view_plan
cargo test -p velorix-runtime --test materialized_view_runtime
cargo test -p velorix-api --lib
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Authoritative paths: `crates/velorix-api/src/view_admission.rs`,
`crates/velorix-api/src/lib.rs` (`MaterializedViewRuntimeFactory`), and
`crates/velorix-core/src/view_plan/mod.rs`. Promote an internal runtime only
after an admission-to-materialization-to-restart API test exists.

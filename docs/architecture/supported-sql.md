# Supported materialized-view SQL

**Status: 2026-09-05 (HEAD `4954d57`).** This is the canonical contract for
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
| Cross join | Default public path; API E2E/restart verification pending | The default API can reach the specifically validated two-input cross-join projection path (`validate_supported_cross_join_sql`), but it lacks API admission-to-materialization-to-restart evidence. It is not a general join-composition escape hatch. |
| Event-time windows | Default public | `TUMBLE`, `HOP`, and `SESSION` over the validated aggregate shape and declared event-time/watermark contract. `tumbling_event_time_aggregate_sql_accepts_subsecond_interval_units` and `rest_hopping_window_advanced_aggregate_view_survives_api_restart`. |
| Recursive CTE | Default public path; API E2E/restart verification pending | The default API can reach the validated positive `UNION DISTINCT` fixpoint grammar; arbitrary recursive SQL is rejected. Runtime evidence: `recursive_cte_materializes_closure_exactly_across_retract_restart_and_fail_closed`; API admission-to-restart evidence is still required. |
| Interval join | Default public path; API E2E/restart verification pending | The default API can reach the two-input inner overlap validator/runtime for exact strict endpoint comparisons and bounded projection, with no grouping or `HAVING`. Runtime evidence: `interval_join_materializes_overlap_retraction_and_restart`; API admission-to-restart coverage is still required. |
| Temporal/as-of join | Default public path; API E2E/restart verification pending | The default API can reach the bounded two-input temporal/equality/projection validator/runtime. Runtime evidence: `temporal_join_materializes_asof_match_and_retracts`; API admission-to-restart coverage is still required. |
| Percentile and median | Default public | Grouped `median`, `percentile_disc`, and `percentile_cont` use direct Int64 input columns and validated numeric literal percentiles in `[0, 1]`; global median, string/Decimal128 inputs, and invalid percentile shapes/types fail closed during admission. They are not supported in join output-schema construction. Public factory evidence: `rest_grouped_median_and_percentiles_materialize_and_survive_api_restart`; rejection evidence: `rest_percentile_admission_rejects_global_invalid_and_non_int64_inputs`; runtime evidence: `percentile_aggregates_are_exact_across_retract_and_restart`. |
| `ROW_NUMBER`, `RANK`, `DENSE_RANK` | Experimental-gated | One relation with validated partition/order/tie-breaker and bounded rank-filter form. Default admission returns an explicit experimental-disabled error; `public_1_0_rejects_experimental_view_surfaces_by_default`. |
| Scalar aggregate subquery filter | Internal but publicly unreachable | `ScalarAggregateFilter` runtime coverage exists (`scalar_aggregate_filter_materializes_and_restores`), but the public output-schema factory has no corresponding branch. |
| Analytic navigation frames | Internal but publicly unreachable | `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE` runtime coverage exists (`analytic_window_frames_navigation_materializes_and_restores`), but public schema derivation does not expose it. |

## Explicit rejection boundary

Everything outside the table is rejected during admission: arbitrary subqueries,
general navigation frames, unbounded analytic windows, recursive forms outside
the validated grammar, unsupported join compositions, four-or-more-input views,
unsupported distinct aggregates, `UNION ALL`, `ROLLUP`/`CUBE`/`GROUPING SETS`,
DDL/DML, multiple statements, and parser-only syntax. Query-time SQL is a
separate read-only DataFusion surface over one published output table; it never
expands materialization support.

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
capability authority.

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

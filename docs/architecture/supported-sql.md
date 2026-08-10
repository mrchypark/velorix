# Supported materialized-view SQL

This is the capability contract for the production `POST /v1/views` admission
path. It describes SQL which is lowered by
`velorix_core::view_plan::lower_supported_sql_to_logical_plan` and run by the
built-in materialized-view runtime. It is deliberately narrower than the SQL
accepted by the parser or by query-time DataFusion.

Every admitted view has one output relation and either one or two registered
input relations. Its input schemas, output schema, keys, nullability, and
types are part of admission; a syntactically similar statement can therefore
be rejected for a schema-specific reason. The output schema must match the
admitted projection. Examples below show the SQL shape, not a complete DDL
contract.

## Production admission (default API configuration)

| Family | Admitted shape and scope |
| --- | --- |
| Filter and projection | One relation; a primary-key projection plus one or more values. `WHERE` accepts the predicate forms validated by the plan (column/literal comparisons, Boolean combinations, `IN`, `BETWEEN`, `LIKE`, null and distinctness checks, and the supported Int64 scalar expressions). `SELECT DISTINCT`, `UNION DISTINCT`, `INTERSECT DISTINCT`, and `EXCEPT DISTINCT` are supported for this family. |
| Computed Int64 projection | Integer literals and registered Int64 columns; `+`, `-`, `*`, `/`, `%`, `abs`, `greatest`, `least`, `coalesce(column, int_literal)`, searched `CASE`, simple `CASE column WHEN literal THEN ... ELSE ... END`, and `if(condition, then, else)`, subject to the validator's type/nullability rules. These expressions are also usable as supported aggregate inputs and predicates where the relevant validator admits them. |
| Single-relation aggregate | One or more ordered group keys made from registered columns or supported deterministic Int64 scalar expressions, with `sum`, `count(*)`, `count(column)`, `count(DISTINCT column)`, `min`, `max`, and `avg`. A global aggregate with no `GROUP BY` is admitted only for `count(*)` or its normalized count of a registered non-null column. Aggregate `FILTER (WHERE ...)`, a supported aggregate-output `HAVING` predicate, `DISTINCT ON`, and Top-K are admitted only within their validator restrictions. |
| Latest by key | One relation keyed by its primary key: `arg_max(value, ordering)` or `arg_min(value, ordering)`, optionally with supported source/filter predicates and Top-K. |
| Two-relation join aggregate | Exactly two distinct registered relations. `INNER JOIN` accepts one equality between non-null scalar columns outside both primary keys, one primary-key equality, or a canonical conjunction/`USING` list that covers every column of both composite primary keys exactly once. Narrow outer joins remain single-key primary-key joins only. Aggregate outputs use the supported aggregate family, with supported qualified `WHERE`, `FILTER`, `HAVING`, and Top-K forms. A relation must be referred to unambiguously by its table alias when the join validator requires it. |
| Three-relation composite-PK count | Exactly three distinct registered relations with explicit aliases and two left-deep `INNER JOIN` steps. Each step must add one relation and equate every root composite-PK position bijectively to every PK position of the new relation, using non-null columns with exact Arrow types. Projection and `GROUP BY` are the canonical root PK followed by one `COUNT(*)`. The root role is fixed and non-root roles use durable relation-ID order, so swapping the two JOIN clauses does not change the canonical join DAG or result. No residual predicate, `WHERE`, aggregate `FILTER`, `HAVING`, Top-K, CTE/derived source, outer join, or non-PK key is admitted. The runtime is two ordinary Foundation 0B binary joins, not a three-table state machine. |
| Narrow self-join count | One registered relation scanned twice with two explicit aliases, one `INNER JOIN` equality between the same non-primary, non-null supported scalar column, and exactly one global `COUNT(*)` output. The runtime maintains two canonical role indexes from one physical input frontier and always publishes one row, including `count = 0`. |
| Narrow left join | `LEFT [OUTER] JOIN` is admitted only when grouping by the left primary key. It preserves unmatched left rows and permits nullable right-side aggregate/filter inputs in the bounded grouped family. Right-referencing top-level `WHERE` runs after null extension. Residual `ON`, shared aggregate filters, right-side grouping, raw joined-row projection, and right-side CTE/derived-source filters remain unsupported. |
| Narrow right join | `RIGHT [OUTER] JOIN` has the mirror restrictions: group by the SQL-right primary key and preserve its unmatched rows, while the bounded grouped family may aggregate/filter nullable values from either SQL side. Admission swaps operands and column bindings, then uses the narrow-left logical node and runtime state machine. |
| Narrow full join | `FULL [OUTER] JOIN` admits exactly two relations joined by one complete scalar primary-key equality. The first output and grouping key must be `COALESCE(left_key, right_key)` (or its projection alias/ordinal for grouping). Both bags are retained and either side can contribute nullable aggregate/filter inputs. |
| Top-K | `ORDER BY` plus positive integer `LIMIT` or `FETCH FIRST`, optionally non-negative integer `OFFSET`, for the supported family. Public API limits `LIMIT` to 1,000. `LIMIT` without `ORDER BY`, `LIMIT BY`, and simultaneous `LIMIT`/`FETCH` are rejected. |
| CTE / derived source | The identity or single-source filter/projection forms admitted by the individual family validators. They are not general recursive or multi-source subqueries. |

## Complete production shape matrix

The table above names the runtime families. The matrix below lists every SQL
feature admitted inside those families. A query feature or combination not
listed here is unsupported, even when the SQL parser accepts it.

| Area | Supported forms | Required restrictions |
| --- | --- | --- |
| Statement | One parenthesized or unparenthesized `SELECT` | Exactly one statement and one output relation. |
| Source | A registered table, an identity CTE, or an identity/single-source filter and direct-projection CTE or derived table | CTEs are non-recursive. Required key, value, ordering, and predicate columns must remain directly traceable to catalog columns. Source projection aliases are allowed when unambiguous. |
| Projection | Direct columns, `*`, qualified `alias.*`, supported Int64 computed expressions, and aliases | `*` expands in catalog order and only over an identity source. Output columns, types, nullability, and output key must match the requested output schema. The weight column cannot be projected as an ordinary runtime value. |
| Distinct projection | Plain `SELECT DISTINCT` | The resulting output must have a valid, non-duplicated output key. `DISTINCT ON` is not supported for filter/project views. |
| Comparisons | `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=` between a supported column/expression and a compatible literal or expression | Columns must be runtime-visible and types must match. Expression-to-expression comparison is limited to supported Int64 expressions. |
| Boolean predicates | `AND`, `OR`, parentheses, `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `LIKE`, `NOT LIKE`, `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, and `IS NOT DISTINCT FROM` | Predicate literals and columns must satisfy the family validator. A source weight column is never predicate-visible. |
| Int64 scalar expressions | Integer literals and registered Int64 columns; unary sign; `+`, `-`, `*`, `/`, `%`; `CAST`, `TRY_CAST`, `SAFE_CAST`, and `::` to Int64; `abs`; `greatest`; `least`; `coalesce(nullable_column, integer_literal)`; searched and simple `CASE`; `if(condition, then, else)` | Branches/arguments must resolve to the supported Int64/nullability contract. Arithmetic is same-row only; join aggregate inputs cannot combine columns from both sides in one scalar expression. |
| Set operations | `UNION DISTINCT`, `INTERSECT DISTINCT`, and `EXCEPT DISTINCT` for filter/project branches over the same relation | Both branches must preserve the same supported direct projection and compatible schema. Only the tested branch-filter combinations are admitted; `ALL`, cross-relation branches, and computed branch projections are rejected. |
| Grouping | One or more ordered registered columns or supported deterministic Int64 scalar expressions; direct expressions, projected aliases, and ordinals are admitted in the validated forms. The legacy primary-key shape also accepts its supported `GROUP BY ALL` equivalent. No `GROUP BY` is admitted only for the global count shape. | Every grouping expression must be projected exactly once and bind unambiguously; the weight column, duplicate grouping expressions, volatile/unknown functions, `ROLLUP`, `CUBE`, and `GROUPING SETS` are rejected. Nullable direct columns use SQL NULL grouping semantics. The ordered projected grouping columns form the output primary key; global count has no public key column. |
| Aggregates | `SUM(expr)`, `COUNT(*)`, `COUNT(column)`, `COUNT(DISTINCT column)`, `MIN(expr)`, `MAX(expr)`, and `AVG(expr)` | `expr` is a supported direct or computed input for that family. Other distinct aggregates are rejected. Decimal `AVG` has the validated Float64 output contract. |
| Aggregate filters | Per-aggregate `FILTER (WHERE predicate)` | The predicate must use columns visible to that aggregate/family. Different filters may be used by different aggregates. Left joins impose the stricter left-only rule described below. |
| `HAVING` | One supported Boolean predicate over a projected aggregate alias or an aggregate function exactly matching a projected aggregate, including its input and filter | An unprojected, ambiguous, or merely similar aggregate expression is rejected. `AND`/`OR` combinations are admitted only when every atom binds successfully. |
| Latest by key | Exactly one `arg_max(value, ordering)` or `arg_min(value, ordering)` grouped by the primary key | Ordering must be a supported non-null column. Source/outer predicates, aliases, the standard grouping forms, and Top-K are supported. |
| Inner join | Exactly two distinct registered relations; `INNER JOIN`/`JOIN` with equalities in `ON` or `USING` | Primary-key pairs must cover each input's complete primary key exactly once. Alternatively, one equality may join non-null, non-weight scalar columns outside both primary keys; duplicate matches retain SQL bag multiplicity. Every corresponding pair must have the exact same Arrow physical type; cross-type coercion is not admitted. Nested, nullable, composite non-primary, and partial-primary-key equality are rejected. Pairs are canonicalized deterministically; repeating the same pair is deduplicated. Composite PK keys use `velorix-composite-pk-positional-json-array-join-key-v1`; non-primary scalar keys use the separate `velorix-non-primary-non-null-scalar-join-key-v1` domain. Both identities are bound to execution and checkpoint restore. Supported residual `ON` predicates and qualified `WHERE` predicates may refer to either side, but an `ON` residual cannot be an `OR`, and a single scalar residual expression cannot mix sides. |
| Three-relation inner join | Exactly three distinct registered relations; two explicit-alias, left-deep `INNER JOIN`/`JOIN` steps; complete composite-PK equalities; canonical root-PK projection and `GROUP BY`; one `COUNT(*)` | Every step adds one new relation and maps every root PK position bijectively to the new relation's complete PK with exact non-null Arrow types. Relation roles, permutations, `velorix-composite-pk-positional-json-array-join-key-v1`, and the versioned root-fixed/right-relation-ID order policy are durable plan identity. Field-absent schema-v1 plans retain legacy encounter-order restore semantics. Residuals, predicates, other aggregates, outer joins, non-PK keys, and source rewrites are rejected. |
| Self join | One physical relation with two explicit aliases; `INNER JOIN`/`JOIN` on one equality between the same non-primary, non-null, non-weight supported scalar column; global `COUNT(*)` only | The binder emits canonical `scan_left` and `scan_right` input-instance identities independent of SQL alias spelling. One physical delta is applied atomically to both role indexes under `velorix-self-join-left-then-right-atomic-fanout-v1`, preserving SQL bag multiplicity. Primary-key or composite equality, residual `ON`, `WHERE`, grouping, projections, aggregate filters, `HAVING`, Top-K, CTE/derived sources, outer joins, and aggregates other than `COUNT(*)` are rejected. |
| Inner-join aggregation | Group by the admitted lexicographically first join-key pair's component and use the supported aggregates over allowed left or right inputs | Qualified references must be unambiguous. Cross-side scalar aggregate inputs are rejected. Nullable and distinct counts are accepted only for the side/input combinations explicitly validated by the join plan. Composite grouping of the join result is not yet admitted. |
| Left join | `LEFT [OUTER] JOIN` with the same single-key equality, grouped by the left primary key | Left- and right-side aggregate inputs are supported for the admitted grouped family. Right-referencing `WHERE` is evaluated after the join; aggregate null/empty behavior follows SQL. No right grouping, residual `ON`, shared aggregate filter, raw joined-row projection, or right-side CTE/derived-source filter. |
| Right join | `RIGHT [OUTER] JOIN` with the same single-key equality, grouped by the SQL right primary key | The bounded grouped family mirrors left-join aggregate/filter support after swapping operands. Non-preserved SQL-left `WHERE` is evaluated after null extension; corresponding source filters, residual `ON`, shared filters, and raw projection remain unsupported. No right-join state machine exists. |
| Full join | `FULL [OUTER] JOIN` with one complete scalar primary-key equality, projected/grouped by `COALESCE(left_key, right_key)` | Both unmatched sides are maintained with general-retract bag semantics. Composite/non-primary keys, residual `ON`, shared aggregate filters, and CTE/derived filters on either null-extending input are rejected. |
| Semi/anti join | One direct outer `WHERE EXISTS (SELECT non_null_literal FROM right WHERE right.pk = left.pk)` or `WHERE NOT EXISTS (...)`, with either correlation orientation | Exactly two distinct registered relations with one non-null scalar primary-key column each; logical and Arrow key types must match exactly. The outer projection follows the keyed filter/project contract and must include at least one value column. `DISTINCT`, grouping, residual predicates, explicit joins, derived/CTE inputs, nonliteral inner projections, composite/non-primary/nullable keys, `IN`/`NOT IN` subqueries, and all other subquery shapes fail closed. The forms lower to ordinary `SemiEquiJoin`/`AntiEquiJoin` nodes and use the separate `velorix-native-semi-join-v1` and `velorix-native-anti-join-v1` checkpoint codecs. |
| Ordering without a bound | A supported trailing `ORDER BY` | It does not change materialization unless paired with a supported bound. |
| Top-K | One metric/order expression, optionally followed by the deterministic key tie-breaker; `LIMIT positive_integer` or `FETCH FIRST positive_integer ROWS ONLY`; optional `OFFSET non_negative_integer` | The order expression must be a projected output or an exactly bindable supported function/expression; hidden order columns are supported only for non-null direct filter/project inputs. Public admission caps the limit at 1,000. |

Representative admission evidence is named directly in
`crates/velorix-core/tests/view_plan.rs`: `filter_project_*`,
`single_key_aggregate_*`, `latest_by_key_*`, `two_input_join_*`, `self_join_*`,
`left_join_*`, and `correlated_exists_*`. Those tests, not parser behavior, are
authoritative. Public REST recovery evidence for both existence forms is
`rest_exists_and_not_exists_views_survive_restart_and_match_transitions`.

The aggregate implementation has explicit incremental state for
`COUNT(DISTINCT ...)`; it is not implemented as a source re-scan. In joins,
the precise side/type restrictions are intentionally enforced by admission,
especially for nullable `COUNT(column)` and distinct inputs.

Global aggregation is intentionally narrower than grouped aggregation. Only
`COUNT(*)` (including the admitted normalized count of a registered non-null
column) has the required empty-input representation today. It always publishes
one row, including `count = 0` for empty input, and exposes no synthetic SQL key
column. Global `SUM`, `MIN`, `MAX`, and `AVG` remain rejected until their
empty-set NULL state is represented by the runtime and public schema.

## Experimental-gated materialized views

The runtime has implementations and API integration coverage for the following
families, but `POST /v1/views` rejects them by default with "advanced view SQL
is experimental and disabled for the public 1.0 API". They require the server
configuration that enables `experimental_advanced_view_features`:

| Family | Narrow admitted scope |
| --- | --- |
| Event-time aggregate windows | `TUMBLE`, `HOP`, and `SESSION` over one relation, with the same supported aggregate/filter/HAVING/Top-K concepts where their window validator permits them. Intervals and event-time columns are validated; this is not arbitrary window SQL. |
| Analytic ranking | `ROW_NUMBER`, `RANK`, and `DENSE_RANK` with the required single partition column, sortable non-null order column, deterministic primary-key ascending tie-breaker, and optional bounded rank filter (`QUALIFY` or the supported wrapper form). |

The window aggregate table-function forms admitted by the experimental plan
are `TUMBLE`, `HOP`, and `SESSION`. Their event-time argument must resolve to
the catalog's declared event-time column and their intervals must be positive,
supported interval literals. Ranking admits one relation, no grouping or set
operation, one non-null partition column, one non-null sortable order column,
and the primary key as an ascending deterministic tie-breaker. A rank bound is
`rank = 1` or `rank <= positive_integer` in the supported `QUALIFY` or wrapper
shape; arbitrary window frames, named windows, and other rank predicates are
rejected.

Enabling the flag does not turn parser acceptance into support. These shapes
still go through the same typed logical-plan admission and runtime capability
checks.

## Query-time SQL is a separate surface

After a view has materialized its published output snapshot, a caller may provide
raw SQL to the view-query API (or a registered API SQL template). That SQL is
executed by DataFusion **against the one materialized output table**, under the
view query policy. Raw SQL and templates are rejected if an explicit page bound
or the policy cap would truncate their materialized input; they never execute
against a partial page while presenting the result as complete. The optimized
query plan is rejected if it scans any other table. This read-only query surface
is not a way to register a new incremental view, and its DataFusion syntax
support must not be read as materialization support.

## Rejected rather than emulated

Admission is fail-closed: parsing or planning failures become a 400 response;
there is no full-source recomputation fallback. The rejected space is
exhaustively defined as **everything outside the preceding matrices**. The
following table makes the major rejection boundaries explicit.

| Area | Rejected forms |
| --- | --- |
| Statements | `INSERT`, `UPDATE`, `DELETE`, DDL, commands, multiple statements, and any non-`SELECT` statement. |
| Sources | Zero inputs, four or more inputs, every three-input shape outside the bounded composite-PK count family, recursive CTEs, correlated or arbitrary subqueries, multi-source CTEs/derived tables, comma joins, and sources whose required catalog columns are hidden or computed. |
| Projection/key | A non-distinct filter/project output without its primary key; duplicate/ambiguous key outputs; unsupported `DISTINCT ON`; a projection or output schema whose names, order, types, nullability, or key do not match the logical plan. |
| Predicates/expressions | Weight-column predicates, non-runtime-visible columns, arbitrary functions, nondeterministic functions, non-Int64 computed-expression families, incompatible literals/types, invalid nullable expressions, `IN`/`NOT IN` over nullable expressions or lists containing `NULL`, subquery `IN`/`NOT IN`, and unsupported mixed-side join scalar expressions. |
| Sets | `UNION ALL`, `INTERSECT ALL`, `EXCEPT ALL`; cross-relation branches; incompatible projections; computed branch projections; and branch-filter arrangements outside the admitted same-relation forms. |
| Grouping | An unprojected, duplicated, ambiguous, volatile, unknown, or weight-column grouping expression; mismatched projection/group expressions; an unsupported `GROUP BY ALL` expansion; `ROLLUP`, `CUBE`, `GROUPING SETS`, and `GROUP BY ALL WITH ROLLUP`. No group key is rejected except for the admitted global count shape. |
| Aggregates | Every function outside `SUM`, `COUNT`, `MIN`, `MAX`, `AVG`, and latest-by-key `arg_min`/`arg_max`; global aggregates other than the admitted count shape; `SUM(DISTINCT ...)`, `AVG(DISTINCT ...)`, `MIN/MAX(DISTINCT ...)`, multi-column distinct counts, and unsupported aggregate inputs. |
| `HAVING`/aggregate `FILTER` | References that do not exactly bind to a projected aggregate; mismatched function input/filter/distinctness; unsupported predicate columns; right/shared filters in a left join. |
| Joins | `CROSS` and `NATURAL` joins; four or more tables; every three-table join outside the bounded composite-PK count family; non-equality keys; partial-primary-key equality; nullable, nested, or multi-column non-primary keys; incompatible corresponding key types; composite outer joins; full joins without the exact coalesced output/group key; `OR` in `ON`; unsupported residuals; every self-join outside the narrow global-count shape above; and outer-join source/filter/projection shapes outside the bounded contracts above. |
| Top-K | `LIMIT`/`FETCH` without `ORDER BY`; zero, negative, non-literal, or over-policy limits; negative/non-literal offsets; MySQL `LIMIT offset,count`; `WITH TIES`; percent fetch; `LIMIT BY`; simultaneous `LIMIT` and `FETCH`; multiple non-key order metrics; ambiguous, nullable hidden, unmatched, or unprojected order expressions. |
| Analytic/window SQL | All analytic functions except the gated `ROW_NUMBER`, `RANK`, and `DENSE_RANK` shapes; arbitrary frames and named windows; nullable partition/order columns; descending/missing primary-key tie-breaker; grouping, set operations, or joins around ranking; unsupported rank bounds; all event-time windows when the experimental flag is off. |
| Runtime capability | Any syntactically valid plan whose input/output schema, key, type, nullability, adapter, or incremental state requirement is not supported by the selected runtime family. |

`VelorixLogicalViewPlanV1` is the only materialized-view admission and runtime
plan surface. Product SQL capability claims derive solely from that plan and
its validators.

## Evidence and maintenance

The authoritative checks are the plan lowerer and public API gates:

- `crates/velorix-core/src/view_plan.rs`: logical plan families and their
  family-specific validators;
- `crates/velorix-api/src/lib.rs`: `create_view`,
  `lower_materialized_view_runtime_sql_to_logical_plan`, and
  `validate_public_*_admission`;
- `crates/velorix-runtime/tests/materialized_view_runtime.rs`: incremental,
  retraction, and restore coverage for the admitted runtime families;
- `crates/velorix-api/src/lib.rs` tests: REST creation, ingest, query, and
  restart coverage, including composite/global aggregate keys,
  `COUNT(DISTINCT)`, left-join unmatched rows, and two-relation `WHERE`
  filtering.

When adding a SQL shape, update this document only after it has a typed plan,
runtime implementation, and admission-to-restart test coverage. When removing
one, update this document in the same change so callers do not infer a parser
feature is executable incrementally.

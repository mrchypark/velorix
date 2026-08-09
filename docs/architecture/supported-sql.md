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
| Single-relation aggregate | One group key (the relation primary key), `GROUP BY` that key (or the supported `GROUP BY ALL`/equivalent form), with `sum`, `count(*)`, `count(column)`, `count(DISTINCT column)`, `min`, `max`, and `avg`. Aggregate `FILTER (WHERE ...)`, a single supported aggregate-output `HAVING` comparison, `DISTINCT ON` restricted to the group key, and Top-K are admitted within their validator restrictions. |
| Latest by key | One relation keyed by its primary key: `arg_max(value, ordering)` or `arg_min(value, ordering)`, optionally with supported source/filter predicates and Top-K. |
| Two-relation join aggregate | Exactly two distinct registered relations and exactly one equi-key relation (`INNER JOIN` or the narrow `LEFT JOIN` described below). Aggregate outputs use the supported aggregate family, with supported qualified `WHERE`, `FILTER`, `HAVING`, and Top-K forms. A relation must be referred to unambiguously by its table alias when the join validator requires it. |
| Narrow left join | `LEFT [OUTER] JOIN` is admitted only when grouping by the left primary key. It preserves unmatched left rows, but does not admit ON residual predicates, right-side aggregate inputs, right-side `WHERE` predicates, or shared aggregate filters; per-aggregate filters must be left-only. |
| Top-K | `ORDER BY` plus positive integer `LIMIT` or `FETCH FIRST`, optionally non-negative integer `OFFSET`, for the supported family. Public API limits `LIMIT` to 1,000. `LIMIT` without `ORDER BY`, `LIMIT BY`, and simultaneous `LIMIT`/`FETCH` are rejected. |
| CTE / derived source | The identity or single-source filter/projection forms admitted by the individual family validators. They are not general recursive or multi-source subqueries. |

The aggregate implementation has explicit incremental state for
`COUNT(DISTINCT ...)`; it is not implemented as a source re-scan. In joins,
the precise side/type restrictions are intentionally enforced by admission,
especially for nullable `COUNT(column)` and distinct inputs.

## Experimental-gated materialized views

The runtime has implementations and API integration coverage for the following
families, but `POST /v1/views` rejects them by default with "advanced view SQL
is experimental and disabled for the public 1.0 API". They require the server
configuration that enables `experimental_advanced_view_features`:

| Family | Narrow admitted scope |
| --- | --- |
| Event-time aggregate windows | `TUMBLE`, `HOP`, and `SESSION` over one relation, with the same supported aggregate/filter/HAVING/Top-K concepts where their window validator permits them. Intervals and event-time columns are validated; this is not arbitrary window SQL. |
| Analytic ranking | `ROW_NUMBER`, `RANK`, and `DENSE_RANK` with the required single partition column, sortable non-null order column, deterministic primary-key ascending tie-breaker, and optional bounded rank filter (`QUALIFY` or the supported wrapper form). |

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
there is no full-source recomputation fallback. Notable rejected classes are:

- non-`SELECT` statements, multiple statements, and more than two input
  relations;
- arbitrary subqueries/CTEs, recursive CTEs, comma joins, cross joins, and
  join types other than the constrained inner/left forms;
- non-equality join keys, more than one join key equality, and join shapes
  outside the family-specific predicate and aggregate restrictions;
- arbitrary SQL functions, arbitrary expression types, arbitrary window
  frames/functions, and aggregates outside the admitted list;
- aggregate `DISTINCT` except `COUNT(DISTINCT one supported column)`;
- materialized Top-K without an order, unbounded ranking, or output/key/schema
  layouts that cannot be maintained incrementally.

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
  restart coverage, including `COUNT(DISTINCT)`, left-join unmatched rows, and
  two-relation `WHERE` filtering.

When adding a SQL shape, update this document only after it has a typed plan,
runtime implementation, and admission-to-restart test coverage. When removing
one, update this document in the same change so callers do not infer a parser
feature is executable incrementally.

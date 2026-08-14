# Query Rewrite Design V1

This document outlines the design for Phase 7: Relational Rewrites and
Subqueries. It describes how to admit common SQL syntax by lowering it
to already proven operators.

## Current State

### Supported CTEs
- Identity CTEs: `WITH cte AS (SELECT * FROM t) SELECT * FROM cte`
- Simple filter/project CTEs: `WITH cte AS (SELECT col1, col2 FROM t WHERE ...) SELECT * FROM cte`

### Supported Subqueries
- Correlated EXISTS/NOT EXISTS (Phase 3B): Limited to complete non-null scalar PK equality
- Two-relation only

### Rejected Forms
- Complex CTEs with aggregation/join
- Multi-source CTEs
- Uncorrelated scalar subqueries
- IN/NOT IN with nullable inputs
- Broader EXISTS/NOT EXISTS correlations

## Design Goals

1. **Lower to proven operators**: All rewrites must use existing filter,
   project, aggregate, join, and semi/anti join operators.
2. **Fail closed**: Unsupported rewrites must fail at admission with clear
   error messages.
3. **Deterministic**: Rewrites must produce identical plan semantics and
   output deltas.
4. **Checkpoint compatible**: Rewrites must use existing checkpoint codecs.

## Proposed Rewrites

### 7.1 CTE and Derived Table Normalization

#### Approach
- Non-recursive CTEs with aggregation/join are normalized into the general
  logical plan by inlining the CTE definition.
- Multi-source CTEs are lowered to joins when the CTE definition is a
  join between registered relations.
- Derived tables with complex expressions are normalized to filter/project
  nodes.

#### Implementation
- Extend `lower_supported_sql_to_logical_plan` to handle CTE definitions
  that contain aggregation or join.
- Add `normalize_cte_definition` function that checks if the CTE is a
  supported shape (filter/project/join/aggregate).
- Validate CTE dependency ordering to prevent circular references.

#### Evidence
- `complex_cte_with_aggregation_normalizes_to_logical_plan`
- `multi_source_cte_with_join_normalizes_to_logical_plan`

### 7.2 Uncorrelated Scalar Subqueries

#### Approach
- `WHERE x > (SELECT MAX(y) FROM t)` is lowered to:
  1. Aggregate: `SELECT MAX(y) AS max_y FROM t`
  2. Cross-join: Join the aggregate result with the main query
  3. Filter: Apply the comparison predicate

#### Requirements
- Subquery must have no correlated references
- Cardinality must be statically determinable (exactly one row)
- Subquery must not contain nondeterministic functions

#### Implementation
- Add `lower_uncorrelated_scalar_subquery` function
- Validate subquery cardinality
- Generate aggregate + cross-join + filter plan

#### Evidence
- `scalar_subquery_lowering_to_aggregate_cross_join`

### 7.3 IN/NOT IN Decorrelation

#### Approach
- `IN` is decorrelated to semi-join with correct null semantics
- `NOT IN` is decorrelated to anti-join with null-aware comparison

#### Requirements
- Subquery must return exactly one column
- Subquery must not contain aggregate functions
- Null semantics must be preserved

#### Implementation
- Add `decorrelate_in_subquery` function
- Generate semi-join for `IN`, anti-join for `NOT IN`
- Handle empty subquery results correctly

#### Evidence
- `in_subquery_decorrelates_to_null_aware_semi_join`

### 7.4 Broader EXISTS/NOT EXISTS

#### Approach
- Extend Phase 3B to support non-PK equality keys
- Support multiple join conditions
- Support multi-relation correlations

#### Requirements
- All join conditions must be equality predicates
- Keys must be non-null and supported types
- Null semantics must be preserved

#### Implementation
- Extend `validate_supported_semi_anti_join_sql` to support broader forms
- Add support for multiple equality conditions
- Add support for multi-relation correlations

#### Evidence
- `broader_exists_not_exists_admitted_and_restored`

### 7.5 Rewritten Query Verification

#### Approach
- Build test framework for query equivalence verification
- Verify identical plan semantics across rewrites
- Verify identical output deltas across rewrites
- Verify identical checkpoint state across rewrites

#### Implementation
- Add `verify_query_equivalence` helper function
- Generate test cases for each rewrite
- Compare plan hashes, output deltas, and checkpoint state

#### Evidence
- `rewritten_query_equivalence_framework_proves_identical_deltas`

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Complex CTEs create unbounded plan size | Medium | Limit CTE nesting depth |
| Scalar subqueries produce incorrect cardinality | High | Validate cardinality at admission |
| IN/NOT IN null semantics incorrect | High | Test with NULL values extensively |
| Rewrites change plan semantics | High | Verify equivalence across all test cases |
| Checkpoint compatibility broken | High | Use existing checkpoint codecs |

## Verification Commands

```bash
# Phase 7
cargo test -p velorix-core --test view_plan cte_normalization
cargo test -p velorix-core --test view_plan scalar_subquery
cargo test -p velorix-core --test view_plan in_not_in
cargo test -p velorix-core --test view_plan broader_exists
```

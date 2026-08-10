# Phase 4-8 Execution Plan

This document defines the execution plan for completing Phases 4 through 8 of the
[Incremental SQL Gap-Closure Plan](incremental-sql-gap-plan.md). It provides a
structured roadmap for advancing Velorix from its current verified baseline to a
comprehensive materialized-view database/runtime.

## Current Status

### Verified Baseline (2026-08-09)

- [x] Foundation 0A: Processing, key, bag, and recovery semantics
- [x] Foundation 0B: Native relational DAG and edge capabilities
- [x] Phase 1: General group keys
- [x] Phase 2: General inner joins
- [x] Phase 3A: Complete outer-join state
- [x] Phase 3B: Semi/anti joins

### Test Evidence

| Suite | Documented | Current |
| --- | --- | --- |
| `velorix-api --lib` | 154 passed | **169 passed** |
| `velorix-core --test view_plan` | 309 passed | **333 passed** |
| `velorix-runtime --test materialized_view_runtime` | 166 passed | **189 passed** |
| `velorix-core --test relation` | 42 passed | **42 passed** |
| `velorix-storage --test relation_catalog_registry` | 12 passed | **12 passed** |
| Workspace total | ~1,200 passed | **1,475 passed** |

## Phase 4: View-on-View Incremental Dependencies

**Goal**: Let a materialized output act as a typed input to another standing view.

### 4.1 Schema, Key, and Frontier Contract for Published View Output

- [x] Persist `PublishedRelationBindingV1` in active runtime record
- [x] Back `producer_commit_epoch` with durable output commit records
- [x] Replace direct-source coverage with canonical `CausalCutV1` digest
- [ ] **Validate view-produced catalogs as inputs during admission**
- [ ] **Bind cursor edges to immutable view generations under tenant graph revision**

**Current state**: Metadata and commit envelope infrastructure complete.
Consumer-side resolution of producer output as input catalog is not implemented.

### 4.2 Acyclic Dependency Graph Validation

- [ ] **Build dependency graph during view admission**
- [ ] **Detect and reject cycles with clear error messages**
- [ ] **Validate topological ordering for execution scheduling**
- [ ] **Reject missing or unavailable dependencies at admission time**

**Current state**: No dependency graph validation exists.
Views can only ingest from direct source relations.

### 4.3 Delta Propagation to Dependent Views

- [ ] **Forward signed output deltas from producer to consumer input**
- [ ] **Propagate without reading full materialized snapshots**
- [ ] **Maintain signed bag semantics across the dependency chain**
- [ ] **Handle retractions correctly across dependent views**

**Current state**: No delta propagation mechanism exists.
All views ingest from source relations only.

### 4.4 Frontier and Progress Contract Reuse

- [ ] **Chain `RelationFrontier` across dependent views**
- [ ] **Reuse Foundation 0A processing/output/dependency frontier contract**
- [ ] **Prevent queries from observing output beyond input frontier**
- [ ] **Maintain deterministic progress tracking across the chain**

**Current state**: Frontier structures exist but no chaining mechanism.

### 4.5 Checkpoint and Recovery for Dependency Chains

- [ ] **Restore multi-level dependency chains from checkpoints**
- [ ] **Replay only epochs after consistent frontier**
- [ ] **Fail closed on cycles, missing dependencies, incompatible schemas**
- [ ] **Fail closed on checkpoint generation mismatches**

**Current state**: Single-view checkpoint/recovery complete.
Multi-view chain recovery not implemented.

### 4.6 Exit Gate

- [ ] Three-level filter → aggregate → Top-K chain remains exact across
  insert, retract, restart, and replay.

---

## Phase 5: Public Event-Time Semantics

**Goal**: Promote window support only after its observable time behavior is a
stable product contract.

### 5.1 Event-Time Semantics Documentation

- [ ] **Document event-time extraction semantics**
- [ ] **Document per-partition watermark behavior**
- [ ] **Document window closure rules**
- [ ] **Document allowed lateness and late-row handling**
- [ ] **Publish as part of supported-sql.md**

**Current state**: Experimental implementation exists.
No public documentation of time semantics.

### 5.2 Multi-Input Watermark Combination

- [ ] **Define watermark combination strategy for multi-input windows**
- [ ] **Handle idle-partition behavior explicitly**
- [ ] **Implement watermark advance logic for join-with-window scenarios**
- [ ] **Validate watermark monotonicity across inputs**

**Current state**: Single-input watermark propagation implemented.
Multi-input combination not implemented.

### 5.3 Late-Row Handling Policy

- [ ] **Decide policy: reject, drop with evidence, or admit within allowance**
- [ ] **Make policy explicit and configurable**
- [ ] **Persist late-row handling state for recovery**
- [ ] **Add late-row workload to benchmark corpus**

**Current state**: Strict late-data policy (reject late rows).
No configurable policy or evidence tracking.

### 5.4 State Boundedness and Retention

- [ ] **Define explicit retention contract for watermark-bounded state**
- [ ] **Bound state growth without silently changing SQL semantics**
- [ ] **Persist retention policy in operator contract**
- [ ] **Add state boundedness tests for window operators**

**Current state**: `StateBoundednessV1::WatermarkBounded` exists.
No explicit retention contract or state growth bounds.

### 5.5 Window Retraction Verification

- [ ] **Verify TUMBLE retraction before and after window closure**
- [ ] **Verify HOP retraction for sliding windows**
- [ ] **Verify SESSION merge and bridge retraction**
- [ ] **Verify recovery behavior for closed windows**

**Current state**: Basic window closure tests exist.
Comprehensive retraction verification not complete.

### 5.6 Experimental Gate Removal

- [ ] **Remove `experimental_advanced_view_features` gate for window SQL**
- [ ] **Update public admission path to accept window SQL**
- [ ] **Update supported-sql.md with window families**
- [ ] **Add window-specific admission tests**

**Current state**: Windows gated behind experimental flag.
Public admission rejects window SQL.

### 5.7 Exit Gate

- [ ] TUMBLE, HOP, and SESSION have documented deterministic outcomes for
  in-order, out-of-order, late, restart, and replay cases.

---

## Phase 6: Types and Deterministic Expressions

**Goal**: Broaden common SQL without weakening replay correctness.

### 6.1 Type Inventory and Requirements

- [ ] **Inventory scalar and aggregate state requirements by type**
- [ ] **Document type-specific overflow and NaN handling rules**
- [ ] **Define type promotion and coercion rules**
- [ ] **Version expression encoding in checkpoint state**

**Current state**: Int64 fully implemented.
No formal type inventory or versioning.

### 6.2 Decimal Type Support

- [ ] **Add checked arithmetic for Decimal128/256**
- [ ] **Define precision, scale, and overflow rules**
- [ ] **Implement Decimal as group key and join key**
- [ ] **Add Decimal aggregate output rules**
- [ ] **Prove canonical encoding, equality, hashing, ordering for Decimal**

**Current state**: Decimal128 supported for values and aggregates.
Not supported as group key or join key.

### 6.3 String Expressions

- [ ] **Add CONCAT expression**
- [ ] **Add SUBSTRING expression**
- [ ] **Add UPPER/LOWER expressions**
- [ ] **Add TRIM expression**
- [ ] **Add LENGTH/CHAR_LENGTH expressions**
- [ ] **Add string comparison predicates**

**Current state**: No string expressions implemented.
String literals supported in predicates.

### 6.4 Temporal Expressions

- [ ] **Add EXTRACT expression (year, month, day, etc.)**
- [ ] **Add DATE_TRUNC expression**
- [ ] **Add AGE expression**
- [ ] **Add date/time arithmetic (date + interval)**
- [ ] **Add temporal comparison predicates**

**Current state**: No temporal expressions implemented.
Event-time column bound as Int64 nanoseconds.

### 6.5 Float and Numeric Expressions

- [ ] **Add checked arithmetic for Float32/Float64**
- [ ] **Define NaN, infinity, and zero-division handling rules**
- [ ] **Add ABS, CEIL, FLOOR, ROUND expressions**
- [ ] **Add GREATEST/LEAST for Float types**

**Current state**: Int64 arithmetic only.
GREATEST/LEAST implemented for Int64 only.

### 6.6 Type-Specific Tests

- [ ] **Add null handling tests per type family**
- [ ] **Add overflow and boundary tests per type family**
- [ ] **Add restart and recovery tests per type family**
- [ ] **Add type coercion tests**

**Current state**: Int64 null/overflow tests exist.
No tests for other type families.

### 6.7 Exit Gate

- [ ] Each newly admitted type has exact SQL, Arrow, delta, checkpoint, and query
  representations with no lossy implicit conversion.

---

## Phase 7: Relational Rewrites and Subqueries

**Goal**: Admit common SQL syntax by lowering it to already proven operators.

### 7.1 CTE and Derived Table Normalization

- [ ] **Normalize non-recursive CTEs with aggregation/join**
- [ ] **Normalize multi-source CTEs into general logical plan**
- [ ] **Normalize derived tables with complex expressions**
- [ ] **Validate CTE dependency ordering**

**Current state**: Identity CTEs and simple filter/project CTEs supported.
Complex CTEs with aggregation/join rejected.

### 7.2 Uncorrelated Scalar Subqueries

- [ ] **Lower `WHERE x > (SELECT MAX(y) FROM t)` to aggregate + cross-join**
- [ ] **Handle subqueries with no correlated references**
- [ ] **Validate cardinality is statically determinable**
- [ ] **Reject subqueries with nondeterministic functions**

**Current state**: Scalar subqueries rejected at admission.
No lowering mechanism exists.

### 7.3 IN/NOT IN Decorrelation

- [ ] **Decorrelate `IN` to semi-join with correct null semantics**
- [ ] **Decorrelate `NOT IN` to anti-join with null-aware comparison**
- [ ] **Handle empty subquery results correctly**
- [ ] **Handle duplicate values in subquery results**

**Current state**: IN/NOT IN rejected for nullable inputs.
Phase 3B EXISTS/NOT EXISTS implemented for narrow case.

### 7.4 Broader EXISTS/NOT EXISTS

- [ ] **Extend to non-PK equality keys**
- [ ] **Extend to multiple join conditions**
- [ ] **Extend to multi-relation correlations**
- [ ] **Maintain correct null semantics**

**Current state**: Limited to complete non-null scalar PK equality.
Two-relation only.

### 7.5 Rewritten Query Verification

- [ ] **Build test framework for query equivalence verification**
- [ ] **Verify identical plan semantics across rewrites**
- [ ] **Verify identical output deltas across rewrites**
- [ ] **Verify identical checkpoint state across rewrites**

**Current state**: No equivalence verification framework.
Manual testing only.

### 7.6 Exit Gate

- [ ] The selected subquery corpus lowers to existing operators and remains exact
  after retractions and restart.

---

## Phase 8: Deferred Advanced Capabilities

**Goal**: Implement capabilities requiring separate design decisions, not implied
by completing earlier phases.

### 8.1 Analytic Window Frames and Navigation Functions

- [ ] **Design window frame specification (ROWS/RANGE/GROUPS)**
- [ ] **Implement LAG/LEAD navigation functions**
- [ ] **Implement FIRST_VALUE/LAST_VALUE/NTH_VALUE**
- [ ] **Define frame boundary semantics for retractions**
- [ ] **Add checkpoint codec for window frame state**

**Current state**: Ranking functions (ROW_NUMBER, RANK, DENSE_RANK) implemented.
No window frame or navigation functions.

### 8.2 Exact Percentile, Median, and Ordered-Set Aggregates

- [ ] **Design approximate vs exact percentile tradeoffs**
- [ ] **Implement PERCENTILE_CONT (continuous)**
- [ ] **Implement PERCENTILE_DISC (discrete)**
- [ ] **Implement MEDIAN as convenience alias**
- [ ] **Define state boundedness for ordered-set aggregates**

**Current state**: No percentile or median aggregates.
SUM/COUNT/MIN/MAX/AVG implemented.

### 8.3 Non-Equality, Interval, and Temporal Joins

- [ ] **Design CROSS JOIN semantics for incremental systems**
- [ ] **Implement interval-based joins (overlap predicates)**
- [ ] **Implement temporal join (as-of, temporal containment)**
- [ ] **Define state boundedness for non-equi joins**
- [ ] **Add retraction semantics for non-equi matches**

**Current state**: Only equi-joins supported.
All non-equi joins rejected at admission.

### 8.4 Deterministic User-Defined Functions

- [ ] **Design UDF serialization and identity contract**
- [ ] **Implement deterministic UDF registration**
- [ ] **Validate UDF determinism at admission time**
- [ ] **Persist UDF identity in plan and checkpoint**
- [ ] **Define UDF versioning and upgrade path**

**Current state**: No UDF mechanism.
All functions are built-in.

### 8.5 Recursive and Mutually Recursive CTEs

- [ ] **Design fixpoint computation semantics**
- [ ] **Define termination guarantees**
- [ ] **Implement recursive CTE admission and validation**
- [ ] **Add checkpoint codec for fixpoint state**
- [ ] **Define retraction semantics for recursive results**

**Current state**: Recursive CTEs explicitly rejected at admission.

### 8.6 Pre-Implementation Requirements

Each Phase 8 item requires before implementation:

- [ ] Worst-case state growth specification
- [ ] Retraction algorithm specification
- [ ] Replay determinism proof
- [ ] Checkpoint compatibility proof
- [ ] Real workload demonstration
- [ ] Dedicated design document
- [ ] Benchmark budget approval

**Current state**: No Phase 8 items have pre-implementation artifacts.

### 8.7 Exit Gate

- [ ] Each Phase 8 capability has a dedicated design document, worst-case state
  analysis, retraction algorithm, replay determinism proof, checkpoint
  compatibility evidence, real workload benchmark, and public admission test.

---

## Execution Order

### Step 1: Phase 4 Completion (View Dependencies)

1. **Dependency graph validation during admission**
   - File: `crates/velorix-api/src/view_admission.rs`
   - Action: Add `validate_acyclic_dependency_graph()` function
   - Evidence: `view_dependency_graph_rejects_cycles_at_admission`

2. **Delta propagation runtime path**
   - File: `crates/velorix-control/src/general_circuit_runtime.rs`
   - Action: Add `propagate_view_delta_to_consumers()` function
   - Evidence: `view_delta_propagation_forwards_signed_deltas_without_snapshot`

3. **Frontier chaining across dependency chain**
   - File: `crates/velorix-core/src/standing_program.rs`
   - Action: Add `chain_frontiers_across_dependencies()` function
   - Evidence: `dependency_chain_frontier_propagation_is_deterministic`

4. **Multi-level chain checkpoint/restore**
   - File: `crates/velorix-control/src/general_circuit_runtime.rs`
   - Action: Extend checkpoint to include dependency chain state
   - Evidence: `three_level_dependency_chain_survives_restart`

5. **Exit gate test**
   - File: `crates/velorix-api/src/tests.rs`
   - Action: Add `rest_three_level_filter_aggregate_topk_chain_exact`
   - Evidence: Test passes across insert, retract, restart, replay

### Step 2: Phase 5 Completion (Event-Time Semantics)

1. **Documentation**
   - File: `docs/architecture/supported-sql.md`
   - Action: Add "Event-Time Semantics" section
   - Evidence: Documentation complete and reviewed

2. **Multi-input watermark combination**
   - File: `crates/velorix-runtime/src/materialized_view_runtime/event_time_window.rs`
   - Action: Add `combine_multi_input_watermarks()` function
   - Evidence: `multi_input_watermark_combination_is_monotonic`

3. **Late-row handling policy**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `LateRowPolicy` enum and admission validation
   - Evidence: `late_row_policy_rejects_drop_admit_with_correctness`

4. **State retention contract**
   - File: `crates/velorix-core/src/operator_contract.rs`
   - Action: Add `StateRetentionContractV1` struct
   - Evidence: `state_retention_contract_bounds_watermark_state_growth`

5. **Experimental gate removal**
   - File: `crates/velorix-api/src/view_admission.rs`
   - Action: Remove `experimental_advanced_view_features` gate
   - Evidence: `window_sql_admitted_through_public_api_path`

### Step 3: Phase 6 Completion (Type Extensions)

1. **Decimal type as key/order**
   - File: `crates/velorix-core/src/delta.rs`
   - Action: Extend `DeltaKey` codec for Decimal128
   - Evidence: `decimal128_key_encoding_is_canonical_and_deterministic`

2. **String expressions**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Extend `SupportedProjectionExpr` with string variants
   - Evidence: `string_expression_sql_lowers_to_validated_logical_plan`

3. **Temporal expressions**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Extend `SupportedProjectionExpr` with temporal variants
   - Evidence: `temporal_expression_sql_lowers_to_validated_logical_plan`

4. **Float/Decimal arithmetic**
   - File: `crates/velorix-runtime/src/materialized_view_runtime.rs`
   - Action: Add checked arithmetic for Float and Decimal
   - Evidence: `float_decimal_checked_arithmetic_handles_overflow_and_nan`

5. **Type-specific tests**
   - File: `crates/velorix-core/tests/view_plan.rs`
   - Action: Add type family test matrix
   - Evidence: `type_family_test_matrix_covers_null_overflow_boundary_restart`

### Step 4: Phase 7 Completion (Query Rewrites)

1. **Complex CTE normalization**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Extend `lower_supported_sql_to_logical_plan` for complex CTEs
   - Evidence: `complex_cte_with_aggregation_normalizes_to_logical_plan`

2. **Scalar subquery lowering**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `lower_uncorrelated_scalar_subquery()` function
   - Evidence: `scalar_subquery_lowering_to_aggregate_cross_join`

3. **IN/NOT IN decorrelation**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `decorrelate_in_subquery()` function
   - Evidence: `in_subquery_decorrelates_to_null_aware_semi_join`

4. **Query equivalence framework**
   - File: `crates/velorix-core/tests/view_plan.rs`
   - Action: Add `verify_query_equivalence()` helper
   - Evidence: `rewritten_query_equivalence_framework_proves_identical_deltas`

### Step 5: Phase 8 Completion (Advanced Capabilities)

1. **Design documents** (one per capability)
   - File: `docs/architecture/phase-8-{capability}-design.md`
   - Action: Write design document for each capability
   - Evidence: Design documents reviewed and approved

2. **Window frames and navigation**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `SupportedWindowFrameSpec` and navigation expressions
   - Evidence: `window_frame_navigation_functions_admitted_and_restored`

3. **Percentile/Median aggregates**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `CircuitAggFunc::Percentile` variant
   - Evidence: `percentile_aggregate_exact_across_retract_and_restart`

4. **Non-equi joins**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `SupportedJoinKind::Interval` variant
   - Evidence: `interval_join_admitted_and_materialized_correctly`

5. **UDF framework**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `SupportedProjectionExpr::UserDefinedFunction` variant
   - Evidence: `deterministic_udf_persisted_and_restored_across_restart`

6. **Recursive CTEs**
   - File: `crates/velorix-core/src/view_plan.rs`
   - Action: Add `SupportedRecursiveCTE` plan node
   - Evidence: `recursive_cte_terminates_and_materializes_correctly`

## Verification Commands

```bash
# Phase 4
cargo test -p velorix-api --lib rest_three_level_filter_aggregate_topk_chain_exact
cargo test -p velorix-core --test view_plan view_dependency_graph
cargo test -p velorix-runtime --test materialized_view_runtime dependency_chain

# Phase 5
cargo test -p velorix-api --lib window_sql
cargo test -p velorix-runtime --test materialized_view_runtime watermark_combination
cargo test -p velorix-core --test view_plan event_time

# Phase 6
cargo test -p velorix-core --test view_plan decimal_key
cargo test -p velorix-core --test view_plan string_expression
cargo test -p velorix-core --test view_plan temporal_expression
cargo test -p velorix-runtime --test materialized_view_runtime float_decimal

# Phase 7
cargo test -p velorix-core --test view_plan cte_normalization
cargo test -p velorix-core --test view_plan scalar_subquery
cargo test -p velorix-core --test view_plan in_not_in

# Phase 8
cargo test -p velorix-core --test view_plan window_frame
cargo test -p velorix-core --test view_plan percentile
cargo test -p velorix-core --test view_plan non_equi_join
cargo test -p velorix-core --test view_plan udf
cargo test -p velorix-core --test view_plan recursive_cte

# Full verification
cargo test --workspace
cargo fmt --all --check
```

## Progress Record

| Date | Phase | Capability | Evidence | Status |
| --- | --- | --- | --- | --- |
| 2026-08-10 | 4 | PublishedRelationBindingV1 | Core contract tests | Complete |
| 2026-08-10 | 4 | CausalCutV1 canonical digest | Causal cut tests | Complete |
| 2026-08-10 | 4 | Producer commit records | Commit fence tests | Complete |
| 2026-08-10 | 5 | Experimental window implementations | Runtime tests | Experimental |
| 2026-08-10 | 6 | Int64 expressions | View plan tests | Complete |
| 2026-08-10 | 7 | PK Correlated EXISTS/NOT EXISTS | View plan tests | Complete |
| 2026-08-10 | 8 | Ranking functions (ROW_NUMBER, RANK) | Runtime tests | Experimental |

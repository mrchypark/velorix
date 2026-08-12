# Phase 4-8 Execution Plan

This document defines the execution plan for completing Phases 4 through 8 of the
[Incremental SQL Gap-Closure Plan](incremental-sql-gap-plan.md). It provides a
structured roadmap for advancing Velorix from its current verified baseline to a
comprehensive materialized-view database/runtime.

## Current Status

### Verified Baseline (2026-08-12)

- [x] Foundation 0A: Processing, key, bag, and recovery semantics
- [x] Foundation 0B: Native relational DAG and edge capabilities
- [x] Phase 1: General group keys
- [x] Phase 2: General inner joins
- [x] Phase 3A: Complete outer-join state
- [x] Phase 3B: Semi/anti joins
- [x] Phase 4: View-on-View Incremental Dependencies (4.1-4.6 verified, see below)

### Test Evidence

| Suite | Documented | Current |
| --- | --- | --- |
| `velorix-api --lib` | 154 passed | **172 passed** |
| `velorix-core --test view_plan` | 309 passed | **333 passed** |
| `velorix-runtime --test materialized_view_runtime` | 166 passed | **191 passed** |
| `velorix-core --test relation` | 42 passed | **42 passed** |
| `velorix-storage --test relation_catalog_registry` | 12 passed | **12 passed** |
| Workspace total | ~1,200 passed | **1,488 passed** |

## Phase 4: View-on-View Incremental Dependencies

**Goal**: Let a materialized output act as a typed input to another standing view.

### 4.1 Schema, Key, and Frontier Contract for Published View Output

- [x] Persist `PublishedRelationBindingV1` in active runtime record
- [x] Back `producer_commit_epoch` with durable output commit records
- [x] Replace direct-source coverage with canonical `CausalCutV1` digest
- [x] **Validate view-produced catalogs as inputs during admission**
      (`resolve_standing_inputs_for_view_request`, view_dependencies.rs:82; missing,
      ambiguous, and source/published overlap all fail closed)
- [x] **Bind cursor edges to immutable view generations under tenant graph revision**
      (`input_bindings_for_resolved_inputs` captures the producer head cursor;
      `StandingInputBindingV1::validate` cross-checks cursor against the binding;
      meta-store graph-revision CAS fences concurrent admissions)

**Current state**: Complete. Consumer-side resolution of producer output as input
catalog is implemented and covered by
`view_on_view_admission_rejects_cycles_missing_producers_and_ambiguity`
(tests.rs:17396) and `published_view_input_binding_validation_is_generation_fenced`
(view_contract.rs:1372).

### 4.2 Acyclic Dependency Graph Validation

- [x] **Build dependency graph during view admission**
      (`dependency_edges_from_input_bindings` + `validate_view_dependency_graph_with_candidate`,
      view_admission.rs:46)
- [x] **Detect and reject cycles with clear error messages**
      (`validate_view_dependency_graph` DFS with on-stack set, view_contract.rs:874;
      covered by `view_dependency_graph_rejects_self_two_node_and_three_node_cycles`
      and `view_on_view_chain_rejects_cycles_at_admission`)
- [x] **Validate topological ordering for execution scheduling**
      (graph validation returns producer-first topological order; drain applies
      commits in that order, view_dependencies.rs:971)
- [x] **Reject missing or unavailable dependencies at admission time**
      (missing producer / no authoritative checkpoint / ambiguity all fail closed)

**Current state**: Complete. In-process admissions are serialized by a graph mutex
and the authoritative meta-store graph-revision CAS extends the fence across
processes.

### 4.3 Delta Propagation to Dependent Views

- [x] **Forward signed output deltas from producer to consumer input**
      (`drain_published_view_dependencies`, view_dependencies.rs:957)
- [x] **Propagate without reading full materialized snapshots**
      (durable producer commits are the only propagation source; in-memory deltas
      are never forwarded; each commit carries the signed delta)
- [x] **Maintain signed bag semantics across the dependency chain**
      (weights preserved verbatim, including negative and |weight| > 1)
- [x] **Handle retractions correctly across dependent views**
      (covered by `rest_three_level_filter_aggregate_topk_chain_exact` insert +
      retract assertions)

**Current state**: Complete. Drain is triggered after consumer bootstrap
(view_admission.rs:240), after durable producer checkpoint publication on ingest
(ingest_epoch.rs:447), and after restart restore (lib.rs:1215).

### 4.4 Frontier and Progress Contract Reuse

- [x] **Chain `RelationFrontier` across dependent views**
      (consumer `RelationInputBatch` binds `[previous_epoch, commit_epoch)`; cursors
      advance per applied commit)
- [x] **Reuse Foundation 0A processing/output/dependency frontier contract**
- [x] **Prevent queries from observing output beyond input frontier**
      (`consumer_edge_cursor` + causal-cut validation fail closed on missing or
      mismatched cursors)
- [x] **Maintain deterministic progress tracking across the chain**
      (producer commit digest chained through `CausalViewCursorV1`)

**Current state**: Complete.

### 4.5 Checkpoint and Recovery for Dependency Chains

- [x] **Restore multi-level dependency chains from checkpoints**
      (`restore_standing_program_runtimes_from_active_views` restores all chain
      members and then drains; `rest_three_level_filter_aggregate_topk_chain_exact`
      verifies 3 restored views)
- [x] **Replay only epochs after consistent frontier**
      (replay reads validated commits strictly after the checkpoint cursor)
- [x] **Fail closed on cycles, missing dependencies, incompatible schemas**
      (admission and restore both re-validate)
- [x] **Fail closed on checkpoint generation mismatches**
      (checkpoint identity revalidation on restore; generation-fenced bindings)

**Current state**: Complete.

### 4.6 Exit Gate

- [x] Three-level filter → aggregate → Top-K chain remains exact across
      insert, retract, restart, and replay.
      Evidence: `rest_three_level_filter_aggregate_topk_chain_exact`
      (tests.rs:17040) — exact output asserted after first ingest, retracting
      ingest, crash-window replay, restart restore, and live ingest after restart.

---

## Contract Boundaries (verified 2026-08-12)

These boundaries are deliberate product decisions confirmed by the ChatGPT Pro
design review; they are enforced by tests, not just documentation.

1. **Phase 5–8 remain gated / future scope.** TUMBLE/HOP/SESSION and
   ROW_NUMBER/RANK/DENSE_RANK are implemented but rejected on the public 1.0
   admission path unless `experimental_advanced_view_features` is enabled.
   Evidence: `public_1_0_rejects_experimental_view_surfaces_by_default`
   (tests.rs:16244) — all window/ranking SQL returns `BAD_REQUEST` with an
   "experimental" error on the default public path. Phase 8 items additionally
   require per-item design documents, state-boundedness specs, retraction
   algorithms, replay determinism proofs, checkpoint compatibility proofs, and
   benchmark budgets before implementation (see Phase 8.6).

2. **Catalog namespace authority boundary.** The meta store is the
   authoritative source-catalog namespace for view admission: a meta-store
   `RelationCatalogNotFound` is a definitive missing dependency and never falls
   back to the object-store registry, so a stale or deleted catalog cannot
   resurrect a source during admission (`try_read_source_catalog`,
   view_dependencies.rs). The explicit recovery read path
   (`read_relation_catalog`, recovery.rs) keeps the object-store fallback for
   the not-yet-populated-meta migration scenario. Evidence:
   - Admission authoritative: `view_admission_does_not_resurrect_stale_object_store_catalog_when_meta_is_authoritative`
     (BAD_REQUEST "not a registered relation" even though the object store
     holds the catalog)
   - Recovery fallback kept: `relation_catalog_read_falls_back_to_object_store_when_meta_is_empty_after_recovery`
   - Checkpoint authority (no fallback, both paths):
     `standing_runtime_checkpoint_read_ignores_object_store_when_meta_pointer_is_empty_after_recovery`

3. **Graph revision gate.** `read_view_dependency_graph_revision` is a required
   `MetaStore` trait method (no silent `Ok(0)` default); the `Arc<T>` forwarding
   impl forwards it; capability-limited backends fail closed with
   `UnsupportedCapability`. Evidence: `arc_dyn_meta_store_forwarding_reads_live_graph_revision`,
   `view_dependency_graph_revision_cas_fences_only_view_input_admissions`.

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

**Current state**: **COMPLETE (2026-08-12).** Windows are public 1.0
(`PublicViewFeaturePolicyV1` splits the gate; event-time enabled by default,
analytic gated). Public contract documented in supported-sql.md (extraction,
per-partition watermark, min-over-inputs combination, finalization frontier
F = W - allowance, late-row policies, retention, determinism). LateRowPolicy
(strict/drop-with-evidence/admit-within-allowance) with durable evidence
counter; StateRetentionContractV1; retraction verification matrix (TUMBLE
pre/post closure, HOP fanout, SESSION merge-split + restart).

## Phase 6: Types and Deterministic Expressions

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

**Current state**: Int64 fully implemented; Decimal128 supported for values and
aggregates (not as group/join key); Utf8/Float64 pass through filter/project and
latest-by-key. No formal type inventory or versioning.

### 6.2 Decimal Type Support

- [ ] **Add checked arithmetic for Decimal128/256**
- [ ] **Define precision, scale, and overflow rules**
- [ ] **Implement Decimal as group key and join key**
- [ ] **Add Decimal aggregate output rules** (Decimal128 sum/avg input admitted;
      output projection exists via `project_aggregate_value`)
- [ ] **Prove canonical encoding, equality, hashing, ordering for Decimal**

**Current state**: Decimal128 supported for values and aggregates.
Not supported as group key or join key.

### 6.3 String Expressions

- [ ] **Add CONCAT expression**
- [ ] **Add SUBSTRING expression**
- [ ] **Add UPPER/LOWER expressions**
- [ ] **Add TRIM expression**
- [ ] **Add LENGTH/CHAR_LENGTH expressions**
- [x] **Add string comparison predicates** (LIKE/NOT LIKE, Eq/NotEq, IS NULL on
      Utf8 columns implemented in `PredicateOp`)

**Current state**: No string scalar expressions implemented.
String literals supported in predicates.

### 6.4 Temporal Expressions

- [ ] **Add EXTRACT expression (year, month, day, etc.)**
- [ ] **Add DATE_TRUNC expression**
- [ ] **Add AGE expression**
- [ ] **Add date/time arithmetic (date + interval)**
- [x] **Add temporal comparison predicates** (event-time columns bound as Int64
      nanoseconds/Date32/TimestampNanosecond are comparable in predicates and
      window SQL)

**Current state**: No temporal expressions implemented.
Event-time column bound as Int64 nanoseconds.

### 6.5 Float and Numeric Expressions

- [ ] **Add checked arithmetic for Float32/Float64**
- [ ] **Define NaN, infinity, and zero-division handling rules**
- [ ] **Add ABS, CEIL, FLOOR, ROUND expressions**
- [ ] **Add GREATEST/LEAST for Float types** (Float64 pass-through in
      filter/project; computed expressions Int64-only)

**Current state**: Int64 arithmetic only.
GREATEST/LEAST implemented for Int64 only.

### 6.6 Type-Specific Tests

- [ ] **Add null handling tests per type family**
- [ ] **Add overflow and boundary tests per type family**
- [ ] **Add restart and recovery tests per type family**
- [ ] **Add type coercion tests**

**Current state**: Int64 null/overflow tests exist; Decimal128/Utf8/Float64
value-path coverage exists in view_plan and runtime tests. No expression-family
test matrix for the unimplemented string/temporal/float expressions.

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

**Current state**: Partially complete. Identity/filter/project CTEs plus
Phase 7.4: correlated EXISTS/NOT EXISTS on identical non-null scalar columns
(non-PK equality) admitted and materialized exactly with restart. Remaining:
complex CTE normalization with aggregation, uncorrelated scalar subquery
lowering, IN/NOT IN subquery decorrelation, query equivalence harness.

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
- [x] **Handle duplicate values in subquery results** (literal IN/NOT IN
      expands to OR/AND equality chains, tested)

**Current state**: IN/NOT IN rejected for nullable inputs; literal-list IN/NOT IN
on non-nullable columns supported. Phase 3B EXISTS/NOT EXISTS implemented for
narrow case.

### 7.4 Broader EXISTS/NOT EXISTS

- [ ] **Extend to non-PK equality keys**
- [ ] **Extend to multiple join conditions**
- [ ] **Extend to multi-relation correlations**
- [x] **Maintain correct null semantics** (narrow correlated form requires
      identical non-null scalar PK equality, `validate_supported_semi_anti_join_sql`)

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
  after retractions and restart. (Admission invariant enforced today:
  `subquery_admission_uses_existing_relational_nodes_or_fails_closed`)

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

**Current state**: Ranking functions (ROW_NUMBER, RANK, DENSE_RANK) implemented
and gated behind `experimental_advanced_view_features`; verified by
`row_number_sql_*`/`analytic_*` view_plan tests and API restart tests.
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

## Execution Order (Revised after Oracle Pro Review 2026-08-11)

### Step 1: Phase 4 Completion (View Dependencies) — DONE

1. **Dependency graph validation during admission**
   - File: `crates/velorix-api/src/view_admission.rs` + `view_dependencies.rs`
   - Action: `validate_view_dependency_graph_with_candidate` + `dependency_edges_from_input_bindings`
   - Evidence: `view_on_view_chain_rejects_cycles_at_admission` (tests.rs:17529),
     `view_dependency_graph_rejects_self_two_node_and_three_node_cycles`
     (view_contract.rs:1579)

2. **Delta propagation runtime path**
   - File: `crates/velorix-api/src/view_dependencies.rs`
   - Action: `drain_published_view_dependencies` (durable-commit only)
   - Evidence: `rest_three_level_filter_aggregate_topk_chain_exact` drain-on-ingest
     assertions; `view_delta_propagation` covered by API chain tests

3. **Frontier chaining across dependency chain**
   - File: `crates/velorix-core/src/standing_program.rs` + `view_dependencies.rs`
   - Action: cursor-bounded `RelationInputBatch` offsets; `CausalViewCursorV1` chaining
   - Evidence: chain exactness across ingest/restart in
     `rest_three_level_filter_aggregate_topk_chain_exact`

4. **Multi-level chain checkpoint/restore**
   - File: `crates/velorix-api/src/lib.rs` (restore) + `view_dependencies.rs`
   - Action: `restore_standing_program_runtimes_from_active_views` restores all
     chain members then drains
   - Evidence: `rest_three_level_filter_aggregate_topk_chain_exact` restores 3 views
     and replays the crash-window ingest

#### 0.2 Dependency Edge Binding in Program Identity

- **File**: `crates/velorix-core/src/standing_program.rs`
- **Action**: Add `DependencyEdgeBindingV1` struct and `dependency_binding_digest`
  to `StandingProgramIdentity`. Tenant from authenticated request scope (not
  "default"). Output stream and object key namespaced by tenant/program/generation.
- **File**: `crates/velorix-core/src/view_contract.rs`
- **Action**: Add `graph_revision` and `dependency_binding_digest` to
  `PublishedRelationBindingV1`.
- **Evidence**: `different_producer_generations_yield_different_program_identities`,
  `producer_replacement_requires_explicit_consumer_migration`

#### 0.3 Typed View-Delta Input API

- **File**: `crates/velorix-core/src/standing_program.rs`
- **Action**: Replace source-only input with tagged enum:
  ```rust
  enum StandingInputChangeV1 {
      Source(RelationInputBatch),
      View(ViewInputDeltaV1),
  }
  struct ViewInputDeltaV1 {
      edge_binding_digest: String,
      producer_cursor: CausalViewCursorV1,
      commit_ref: String,
      delta: DeltaBatch,
  }
  ```
- **File**: `crates/velorix-core/src/standing_program.rs`
- **Action**: `StandingProgramRuntime::apply_changes()` accepts `Vec<StandingInputChangeV1>`.
  View deltas are NOT converted to source offsets. Empty-delta commits advance cursors.
- **Evidence**: `view_delta_preserves_signed_bag_weights_across_chain`,
  `empty_delta_commit_advances_cursor_without_row_change`,
  `tampered_edge_digest_rejects_before_state_mutation`

#### 0.4 View Bootstrap + Retention/GC Protocol

- **File**: `crates/velorix-meta/src/view_bootstrap.rs`
- **Action**: Extend `BeginViewBootstrapRequest` with `view_base: Option<ViewBootstrapBaseV1>`
  that captures authoritative producer checkpoint P, base snapshot ref, generation/edge
  binding, and retention pin. Tail replay from P+1.
- **File**: `crates/velorix-meta/src/view_bootstrap.rs`
- **Action**: Add `DeltaRetentionProtocol` with:
  - Per-edge durable consumed epoch (from consumer checkpoint pointer CAS)
  - GC low watermark = min(active edge cursors)
  - Edge tombstone grace period + failed consumer transition
- **File**: `crates/velorix-api/src/ingest_epoch.rs`
- **Action**: GC only retained deltas above low watermark. Retention budget excess
  triggers consumer failure (not silent drop or snapshot fallback).
- **Evidence**: `producer_gc_preserves_deltas_for_slow_consumer`,
  `edge_deletion_requires_consumer_unrecoverable_transition`,
  `retention_budget_excess_fails_closed_without_snapshot_fallback`

#### 0.5 Dependency Checkpoint Mandatory Causal Validation

- **File**: `crates/velorix-core/src/standing_program.rs`
- **Action**: For dependency-capable plans, `causal_cut` is mandatory in checkpoint.
  Validate: cursor set == admitted edge set exactly; each cursor passes full
  authority chain (pointer → checkpoint → commit → digest). Legacy source-only
  checkpoints use separate schema/version path.
- **File**: `crates/velorix-api/src/checkpoint_publication.rs`
- **Action**: Recovery coordinator loads committed graph revision, validates all
  view checkpoints and dependency edges, confirms each causal parent via authority
  chain, restores runtime state individually, catch-up replay in topological order.
  No global atomic snapshot; causal parent set is the consistency unit.
- **Evidence**: `checkpoint_without_causal_cut_rejects_for_dependency_plan`,
  `orphan_cursor_rejects_authority_chain_validation`,
  `three_level_chain_survives_fresh_process_restore`

#### 0.6 Fan-in Scheduling + Backpressure

- **File**: `crates/velorix-api/src/ingest_epoch.rs`
- **Action**: Producer writes immutable commit once; consumers pull independently.
  Notification is non-authoritative wake-up only. Each consumer apply produces
  `(previous pointer, edge set, consumed commits, CausalCutV1 digest)` for
  idempotent CAS. Bounded worker concurrency per tenant.
- **File**: `crates/velorix-meta/src/view_bootstrap.rs`
- **Action**: Consumer lag quota; exceeded → explicit degraded/failed state with
  backpressure (not silent drop).
- **Evidence**: `fan_in_idempotent_across_all_delivery_orders`,
  `notification_loss_does_not_affect_correctness`,
  `consumer_lag_quota_exceeds_fails_closed`

### Step 1: Phase 4 Feature Implementation (after Step 0)

Oracle planner-integration review (2026-08-11) decided the exact seam:

- **Do NOT** add a generic `lower_supported_sql_to_logical_plan_with_inputs`
  dispatcher. Reusing the fallback chain would let out-of-slice families be
  admitted accidentally.
- **Add** `lower_published_single_key_sum_count_sql(sql, input, output)` which:
  1. verifies `PlannerChangeEncoding::PublishedDelta`
  2. verifies `input.schema_fingerprint == input.relation.schema_fingerprint`
  3. verifies edge codec/frontier matches `change_encoding`
  4. calls only the `SingleKeySumCount` validator
  5. fails immediately, no fallback
- **Split** `validate_supported_view_sql` into
  `validate_supported_view_sql_with_input(sql, input, registration_name)`.
  Existing source lowerer validates catalog/adapter first then calls the core;
  published-view lowerer calls it with `registration_name: None`.
- **weight_column_id: None** means "no reserved weight column", not "reject any
  column named weight". Use `is_weight_column(input, id)`. A published output's
  real `"weight"` data column is a normal column.
- **event_time_column_id: None** means window SQL is fail-closed. The
  view-input dispatcher never calls window families.
- **Runtime factory**: add
  `create_standing_runtime_with_logical_plan_and_input_schemas(plan, input_schemas)`;
  keep the catalogs-based fn as a wrapper. View path passes the persisted
  producer relation from `resolve_view_input_relation_v1` — never re-reads a
  catalog and never builds a synthetic catalog.
- **First admitted family**: `SingleKeySumCount` (not FilterProject) so signed
  producer retraction is verified against stateful aggregate state. Slice shape:
  exactly 1 `PublishedDelta` input, direct single PK key, `SUM(non-null Int64)`
  and/or `COUNT(*)`; exclude Top-K, window, CTE/derived, DISTINCT, HAVING,
  aggregate FILTER, computed group key. Failure rejects, no family fallback.

Oracle runtime-execution review (2026-08-11) decided the input-boundary shape:

- **Do NOT make `catalog: Option<_>`.** A missing catalog for a Source binding
  must fail recovery, while a PublishedView binding legitimately has no catalog.
  Store an explicit tagged enum on the runtime:
  ```rust
  enum SingleKeyRuntimeInputV1 {
      Source { catalog: VelorixRelationCatalogV1 },
      PublishedView { binding: PublishedRelationBindingV1, primary_key_column_id: String },
  }
  ```
- **Remove `source_input_batches` from `apply_changes`.** Dispatch the original
  `StandingInputChangeV1` after idempotency checking:
  - `Source` → `validate_input_matches_schema` + `aggregate_group_input_delta_batch`
  - `View` → `validate_view_input_matches_binding` + use `input.delta` directly
  - both → `filter` → `rekey` → `combine` → aggregate apply
- **Never call `aggregate_group_input_delta_batch` for View inputs** (already a
  DeltaBatch). Do not fabricate a catalog. `weight_column_id` is irrelevant for
  View inputs; `DeltaRecord.weight` is the signed multiplicity.
- **View validation** must check admitted producer/view identity, relation
  id/version, schema fingerprint, expected change encoding, and cursor
  continuity before any state mutation.
- **Narrow slice guard**: single PK, published `DeltaBatch.key` is that PK,
  direct column GROUP BY, non-null aggregate inputs, direct SUM/COUNT, no
  predicate/aggregate-FILTER/expression (or RelationSchema-based evaluator).
  Reject anything wider at runtime construction.
- **`GenericCheckpointPayload`** stores mandatory `catalog`; replace with
  `SingleKeyRuntimeInputV1` and migrate legacy catalog payloads to the `Source`
  variant. View cursors persist in the same checkpoint, never masquerading as
  Source `RelationFrontier`s.

Oracle create_view output-schema review (2026-08-11) decided:

- **Use option 1**: a planner-owned common inference helper
  `infer_single_key_sum_count_output_schema(sql, input: &PlannerRelationInput,
  output) -> RelationSchema` that derives group-key, SUM, COUNT output
  column ids/names/types, SQL projection order and aliases, and schema
  fingerprint. It must NOT be published-view-specific; extract the existing
  catalog-based single-key inference so both paths share one function.
- **Do NOT use client-provided output schema as authority** (option 2). The
  server must infer it anyway to validate. **Do NOT introduce
  plan-returns-schema (option 3)** in this slice.
- The single inferred `output_schema` instance must be reused identically for
  the logical plan, `ViewSpec`, `PublishedRelationBindingV1`, runtime factory,
  and create response.
- **`view_spec_from_request` takes `&[ResolvedAdmissionInput]`** (slice), and
  `resolved_input_relation_schema(input)` picks `catalog_input_relation_schema`
  for Source and the verified `PublishedRelationBindingV1.relation` for View.
  Never fabricate a `VelorixRelationCatalogV1` or fill missing physical fields.
- Slice boundary: exactly one resolved view input and supported single-key
  SUM/COUNT; other view-input SQL families stay fail-closed.

1. **Three-level exit gate test**
   - File: `crates/velorix-api/src/tests.rs`
   - Action: `rest_three_level_filter_aggregate_topk_chain_exact`
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
# Phase 4 (implemented; evidence tests)
cargo test -p velorix-api --lib rest_three_level_filter_aggregate_topk_chain_exact
cargo test -p velorix-api --lib view_on_view_admission_rejects_cycles_missing_producers_and_ambiguity
cargo test -p velorix-api --lib view_on_view_chain_rejects_cycles_at_admission
cargo test -p velorix-core --test view_plan subquery_admission_uses_existing_relational_nodes_or_fails_closed
cargo test -p velorix-runtime --test materialized_view_runtime runtime_materializes_filter_project_view_from_published_relation_delta_input

# Phase 5 (experimental-gated)
cargo test -p velorix-api --lib window_sql
cargo test -p velorix-runtime --test materialized_view_runtime event_time_watermark
cargo test -p velorix-core --test view_plan event_time

# Phase 6 (current type surface)
cargo test -p velorix-core --test view_plan decimal
cargo test -p velorix-core --test view_plan string
cargo test -p velorix-core --test view_plan latest_by_key
cargo test -p velorix-runtime --test materialized_view_runtime float

# Phase 7 (current rewrite surface)
cargo test -p velorix-core --test view_plan cte
cargo test -p velorix-core --test view_plan subquery
cargo test -p velorix-core --test view_plan in_list

# Phase 8 (current gated surface)
cargo test -p velorix-core --test view_plan row_number
cargo test -p velorix-core --test view_plan rank

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
| 2026-08-12 | 4 | Consumer input resolution (missing/ambiguous fail-closed) | `view_on_view_admission_rejects_cycles_missing_producers_and_ambiguity` | Complete |
| 2026-08-12 | 4 | Generation-fenced binding + cursor cross-validation | `published_view_input_binding_validation_is_generation_fenced` | Complete |
| 2026-08-12 | 4 | Graph revision CAS (fenced admission) | `begin_view_bootstrap` revision CAS + chain cycle tests | Complete |
| 2026-08-12 | 4 | Authoritative commit/cursor validation | `authoritative_view_cursor_resolution_fails_closed_at_every_trust_boundary` | Complete |
| 2026-08-12 | 4 | Aggregate output projection to public columns | `project_aggregate_value` + chain exactness tests | Complete |
| 2026-08-12 | 4 | Durable-commit-only delta propagation | `drain_published_view_dependencies` call sites | Complete |
| 2026-08-12 | 4 | Three-level chain exact across insert/retract/restart/replay | `rest_three_level_filter_aggregate_topk_chain_exact` | Complete |
| 2026-08-12 | 4 | Graph revision read is a REQUIRED trait method (no silent `Ok(0)`) | `view_dependency_graph_revision_cas_fences_only_view_input_admissions`, `arc_dyn_meta_store_forwarding_reads_live_graph_revision` | Complete |
| 2026-08-12 | 4 | Admission uses meta store as authoritative source namespace | `try_read_source_catalog` fails closed on meta `NotFound` | Complete |
| 2026-08-12 | 4 | Public relation API rejects internal `PublishedViewOutput` kind | `rest_relation_admission_rejects_internal_published_view_output_source_kind` | Complete |

## Design-Goal Evidence Mapping (AGENTS.md)

| AGENTS.md criterion | Evidence (test → admission/runtime path) |
| --- | --- |
| Multiple relation schemas | Scores/purchases/orders catalogs across API tests; `VelorixRelationCatalogV1` fingerprinting in view_plan tests |
| Filters | `rest_three_level_filter_aggregate_topk_chain_exact` (filter view `filtered_scores`); `filter_project_sql_*` lowering tests |
| Projections | `filter_project_sql_accepts_*`; `SingleKeySumCount` projected accumulators tests |
| Group by | `single_key_aggregate_sql_*` group key lowering; chain aggregate view `score_totals` |
| sum/count/min/max/avg | `LogicalPlanAggregateFunctionV1` full set; `apply_filtered_single_key_aggregate_delta` runtime dispatch; tumbling aggregate tests |
| Two-table join | `TwoInputJoinSumCount` execution; `join` view_plan tests; two-catalog `lower_supported_join_view_sql_to_logical_plan` |
| Same admission + runtime path | `create_view` (view_admission.rs) → `lower_materialized_view_runtime_sql_to_logical_plan` → factory `create_with_catalogs_plan_and_spec` → `apply_changes`/checkpoint |
| Unsupported SQL fails closed | `rest_unsupported_single_input_sql_admission_fails_closed_without_creating_view`, `rest_unsupported_join_sql_admission_fails_closed_without_creating_view`, `rest_unsupported_three_table_join_admission_fails_closed_without_active_view` |
| Restart/replay identical output | `rest_three_level_filter_aggregate_topk_chain_exact` (crash window + restart + replay), standing_runtime restart tests |
| View-on-view chains | Phase 4 rows above |

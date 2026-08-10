# Incremental SQL Gap-Closure Plan

This document is the executable checklist for closing the gap between
Velorix's native materialized-view runtime and broader incremental systems such
as RisingWave, Materialize, and Feldera. GreptimeDB Flow is the comparison point
for time-series continuous aggregation, not the target for general relational
SQL compatibility.

The current SQL contract remains
[Supported materialized-view SQL](supported-sql.md). Checking an item here does
not change the public contract until that document and the public admission path
are updated in the same change.

## Working Rules

- Keep the runtime jarless and native. Do not add an external compiler, generated
  runtime package, Feldera/DBSP runtime, or source-recomputation fallback.
- Add one independently useful SQL capability at a time. Do not claim product or
  dialect compatibility with another system.
- Unsupported shapes must continue to fail during admission with a clear error.
- A parser-only or logical-plan-only implementation is not complete.
- Prefer generalizing the existing typed plan and operators over adding another
  family-specific recognizer.
- Preserve signed input and output deltas, deterministic replay, checkpoint
  compatibility, and materialized-output query isolation.
- Record benchmark comparisons only when the tested engines, versions, schemas,
  inputs, and correctness checks are identical.
- Use the generic relational path as the correctness oracle. A specialized path
  may remain only when its consolidated deltas, state, and recovery are proven
  equivalent.
- Never aggregate performance across unsupported or semantically different
  workloads.

## Definition of Done for Every Capability

Check a capability only after all applicable items below are complete:

- [ ] The supported and rejected SQL shapes are explicit.
- [ ] SQL lowers into a typed, versioned logical plan.
- [ ] Invalid schema, key, type, nullability, and state requirements fail closed.
- [ ] Inserts update only affected operator and output state.
- [ ] Retractions or equivalent updates remove old results correctly.
- [ ] Checkpoint and restart preserve exact operator and output state.
- [ ] The public relation/view/ingest/query path has an end-to-end test.
- [ ] The focused admission and runtime suites pass.
- [ ] A representative workload is present in the benchmark corpus.
- [ ] `supported-sql.md` and user-facing examples match the implementation.

Evidence must name tests, benchmark artifacts, or durable design documents. A
checkbox without evidence is not complete.

## Verified Baseline

Baseline verified on 2026-08-09:

- [x] Filters and direct projection
- [x] Supported Int64 computed projection and predicates
- [x] One-key `SUM`, `COUNT`, `COUNT(DISTINCT)`, `MIN`, `MAX`, and `AVG`
- [x] Aggregate `FILTER` and constrained `HAVING`
- [x] Latest-by-key with `arg_min` and `arg_max`
- [x] Two-relation, one-key inner aggregate join
- [x] Narrow left aggregate join preserving unmatched left rows
- [x] Deterministic bounded Top-K
- [x] Signed output deltas, retractions, checkpoint restore, and published-output
  reads
- [x] Experimental-gated `TUMBLE`, `HOP`, `SESSION`, `ROW_NUMBER`, `RANK`, and
  `DENSE_RANK` implementations
- [x] Fail-closed admission outside the documented SQL families

Baseline evidence:

- `cargo test -p velorix-core --test view_plan`: 309 passed
- `cargo test -p velorix-runtime --test materialized_view_runtime`: 166 passed

## Phase Status

- [x] Foundation 0A: Processing, key, bag, and recovery semantics
- [x] Foundation 0B: Native relational DAG and edge capabilities
- [ ] Foundation 0C: Comparable correctness and benchmark evidence
- [x] Phase 1: General group keys
- [x] Phase 2: General inner joins
- [x] Phase 3A: Complete outer-join state
- [x] Phase 3B: Semi/anti joins
- [ ] Phase 4: View-on-view incremental dependencies
- [ ] Phase 5: Public event-time semantics
- [ ] Phase 6: Types and deterministic expressions
- [ ] Phase 7: Relational rewrites and subqueries
- [ ] Phase 8: Advanced capabilities selected individually when justified

## Foundation 0A: Processing, Key, Bag, and Recovery Semantics

Goal: freeze the shared semantics that composite state, joins, dependencies, and
recovery must reuse.

Existing evidence to preserve:

- [x] Public joins expose a checkpoint-bound per-relation input frontier vector,
  not a false atomic multi-relation transaction. Evidence: the Public Join
  Frontier Contract in `materialized-view-runtime-roadmap.md`.
- [x] Processing frontiers and event-time frontiers are distinct checkpoint
  fields and restore validates both. Evidence:
  `runtime_checkpoints_event_time_frontiers_by_source_partition` and the runtime
  checkpoint frontier validators.

Required work:

- [x] Specify input and output weight ranges, epoch consolidation, zero-weight
  removal, committed negative-multiplicity handling, and checked join-weight
  multiplication. Evidence:
  `incremental-processing-semantics-v1.md` and the checked delta/join arithmetic
  tests.
- [x] Specify canonical key encoding, equality, hashing, ordering, NULL grouping
  versus join semantics, coercion, and overflow for every currently key-safe
  type. Evidence: `incremental-key-semantics-v1.md` and the finite/negative-zero
  key adapter tests.
- [x] Bind key/type semantics and state codec versions into the plan/checkpoint
  identity and fail closed on incompatible restore. Evidence: logical plans hash
  the explicit key/bag semantics versions, and standing-program checkpoint
  identity includes the same runtime capabilities plus the state codec.
- [x] Specify monotonic output identity and idempotent publication across state,
  output object, checkpoint manifest, and authoritative pointer writes.
  Evidence: the Durable Output Identity and Publication section of
  `incremental-processing-semantics-v1.md` plus meta/API publication tests.
- [x] Define view bootstrap as a consistent frontier snapshot plus subsequent
  delta replay, including concurrent ingest. Evidence: the View Bootstrap
  Frontier section of `incremental-processing-semantics-v1.md`, including the
  distinction between committed-set cuts and in-flight range reservations;
  implementation of the persisted barrier and race tests remains required by
  the exit gate.
  - [x] Persist the bootstrapping view generation and committed-set cut through
    the same multi-writer metadata authority that serializes ingest admission.
    Evidence: `BeginViewBootstrapRequest` writes the versioned control record,
    ordered input identities, and an immutable reservation/commit snapshot in
    one Hiqlite Raft SQL transaction; the API persists the admitted plan hash
    and view spec before registry publication. In-memory atomicity, gRPC
    round-trip, durable-append ordering, and API admission tests cover the same
    contract.
  - [x] Capture sealed contiguous frontiers plus relation, input-catalog,
    stream, and partition generations; stop before unresolved reservation holes
    and reject commits below a sealed frontier.
    Evidence: `IngestSourceCutV1` persists the complete identity vector and
    fixed generation-1 identities (numeric identity reuse is not supported),
    `ingest_source_cut_stops_before_uncommitted_reservation_hole` proves the
    contiguous seal, and
    `view_bootstrap_sealed_partition_base_rejects_late_lower_range` proves that
    later admission cannot insert a range below a sealed partition base. Exact
    commits still require their matching reservation, so no new commit can be
    introduced inside the sealed prefix.
  - [x] Treat identities first discovered after the cut and committed ranges
    above its sealed frontiers as log-discoverable tail. Evidence:
    `rest_view_admission_persists_plan_spec_and_source_cut_in_metadata` freezes
    partition 0 at offset 1, then commits both a higher partition-0 range and a
    newly discovered partition 1; native log replay materializes all three
    batches while the persisted bootstrap cut remains unchanged.
  - [x] Fence lifecycle promotion on an activation cut and an authoritative
    checkpoint pointer that covers it in one control-record CAS.
    - [x] Bind the pointer to canonical catalog/relation/schema/stream/partition
      generations, lower bounds, processed frontiers, plan hash, and bootstrap
      generation. Evidence: canonical coverage/hash tests and
      `checkpoint_coverage_requires_every_cut_identity_and_frontier`.
    - [x] Enforce the owner lease, fixed activation cut, full authoritative
      pointer equality, and lifecycle mutation in the Hiqlite authority path.
      Evidence:
      `hiqlite_activation_cas_fences_expired_owner_pointer_change_and_concurrent_workers`
      runs a real single-node Hiqlite authority and proves lease expiry/takeover,
      stale-owner rejection, old-pointer rejection after a pointer advance, and
      one-winner concurrent promotion.
    - [x] Fail query readiness closed on the authoritative control and recover
      an active checkpoint after API restart. Evidence:
      `rest_view_admission_persists_plan_spec_and_source_cut_in_metadata` forces
      a locally-running/authoritatively-bootstrapping mismatch, then verifies
      restart reads the same active materialized rows.
    - [x] Validate the OCC proof boundary without exposing an artificial
      product failpoint. Evidence: the real Hiqlite test fixes `A` with `P1`,
      advances authority to `P2`, and rejects `Promote(P1)`; because the SQL CAS
      freezes the same `A` and complete pointer commitment used by the pure
      coverage proof, a pointer move at any point before the CAS is the same
      semantic conflict. The 2026-08-09 Pro follow-up agreed this closes the
      activation-CAS item under the non-Byzantine writer model. Authority
      disconnect, post-commit response loss, manifest corruption, and
      source-cut scheduling remain P1/recovery or broader deterministic-race
      evidence tracked by the still-open neighboring items.
  - [x] Pin source-log retention for every range required by a bootstrapping
    generation, or fail closed when coverage is unavailable. Evidence: the
    current storage contract treats `v1/ingest` as immutable, unbounded source
    history; `CheckpointPublisher::plan_garbage_collection` only scans state
    and output namespaces, and
    `garbage_collection_never_reclaims_immutable_ingest_log_ranges` proves an
    ingest range is neither planned nor deleted. If a required range is missing
    outside that supported GC contract,
    `rest_view_admission_persists_plan_spec_and_source_cut_in_metadata` proves
    activation fails with a checkpoint-coverage conflict and the authoritative
    lifecycle remains `Bootstrapping`. A future bounded ingest-log GC must add
    explicit generation pins before this equivalent unbounded-retention
    contract may be weakened.
  - [x] Add deterministic scan/register, replay/promote, in-flight reservation,
    new-partition, concurrent worker, and restart race tests.
    Evidence: `view_bootstrap_atomically_freezes_source_cut_and_is_idempotent`
    fixes scan/register ordering;
    `view_bootstrap_new_partition_reserved_before_snapshot_and_committed_after_is_tail`
    fixes the reserve/snapshot/commit interleaving; source-cut hole and sealed
    base tests cover unresolved reservations;
    `view_bootstrap_activation_cut_and_promotion_fail_closed_across_tail_race`
    covers same-partition/new-partition replay, full-pointer mutation rejection,
    and concurrent one-winner promotion; the real-Hiqlite
    `hiqlite_activation_cas_fences_expired_owner_pointer_change_and_concurrent_workers`
    repeats reserve/snapshot/commit, lease expiry with and without takeover,
    pointer advancement, and concurrent promotion on the Raft authority; and
    `rest_view_admission_persists_plan_spec_and_source_cut_in_metadata` proves
    tail rows appear exactly once, missing retained input fails closed, and the
    active rows survive API restart. The CAS freezes the proof inputs, so a
    pointer move before its linearization point is covered by the stale-pointer
    conflict rather than requiring a product-only scheduler hook.
- [x] Add crash injection before and after state write, output write, checkpoint
  manifest publication, and authoritative pointer publication.
  - [x] Inject output-delta, state-payload, and immutable-checkpoint write
    failures and prove the previous metadata pointer remains authoritative.
    Evidence:
    `standing_runtime_checkpoint_crash_matrix_keeps_previous_authoritative_pointer`.
  - [x] Prove a fully written checkpoint object without a metadata-pointer
    advance remains an invisible orphan. Evidence:
    `standing_runtime_checkpoint_read_keeps_old_meta_pointer_when_new_checkpoint_is_orphaned`.
  - [x] Prove a stale post-pointer latest-cache object cannot override metadata
    authority. Evidence:
    `standing_runtime_checkpoint_read_uses_meta_pointer_when_latest_cache_is_stale`.
  - [x] Inject failures before a compacted output page write and after the page
    write but before its manifest, then prove an idempotent retry and restart
    expose only the complete snapshot. Evidence:
    `standing_runtime_output_compaction_crash_windows_publish_only_complete_snapshots`.
  - [x] Inject a metadata-pointer conflict in the complete persistence path and
    a latest-cache failure after a successful pointer advance, then restart.
    Evidence:
    `standing_runtime_checkpoint_pointer_conflict_keeps_winning_checkpoint_after_restart`
    and
    `standing_runtime_latest_cache_failure_after_pointer_publish_recovers_from_metadata`.
- [x] Replay the same input epoch repeatedly and prove no missing or duplicate
  durable output. Evidence:
  `runtime_repeated_epoch_is_idempotent_before_and_after_restore`.
- [x] Randomize same-epoch input order and prove identical consolidated output,
  materialized state, and restored state. Evidence:
  `runtime_same_epoch_input_permutations_have_identical_state_output_and_restore`;
  checkpoint processing and event-time frontier vectors are canonically sorted.
- [x] Add key codec properties, NULL composite keys, duplicates, key-changing
  updates, final-duplicate deletion, multiplicity overflow, and codec fixture
  tests. Evidence: the `velorix-core` `delta` property/unit tests, including
  `delta_key_json_codec_round_trip_is_lossless`,
  `delta_key_codec_fixture_round_trips_composite_null_key`, duplicate/key-change/
  final-delete cases, and the existing checked-overflow operator tests.

Exit gate:

- [x] The crash/replay matrix and key/bag semantics tests pass for the existing
  aggregate, join, Top-K, window, and ranking families without changing their
  documented results. Evidence: all 165 materialized-runtime tests, all 154 API
  library tests, 23 core operator tests, 42 core relation tests, and 18 core
  delta/library tests pass; the real Hiqlite activation CAS test also passes.

## Foundation 0B: Native Relational DAG and Edge Capabilities

Goal: prevent new SQL breadth from creating more family-specific state machines.

- [x] Define one versioned operator/edge contract carrying changelog mode
  (`append_only`, `upsert`, or `general_retract`), candidate keys, uniqueness,
  nullability, determinism, frontier requirements, state boundedness, watermark
  requirements, and checkpoint codec identity. Evidence:
  `velorix_core::operator_contract` and
  [Operator and Edge Capability Contract V1](operator-edge-contract-v1.md).
- [x] Make admission validate compatibility across the complete DAG before
  runtime construction. Evidence: logical plan wire/capability version 2 binds
  the derived `OperatorDagContractV1` into the plan hash; validation re-derives
  the complete contract, checks every port edge, rejects disconnected nodes and
  legacy/missing contracts, and
  `runtime_rejects_tampered_operator_contract_before_construction` proves the
  runtime boundary fails closed.
- [x] Represent filter, project, aggregate, Top-K, and binary join as composable
  native operators with one checkpoint/replay contract. Evidence:
  `velorix_core::native_operator` runs named-port operator DAGs under monotonic
  logical epochs and one versioned graph checkpoint envelope; pair/triple-node
  replay, typed Top-K ordering, wire round-trip, codec rejection, duplicate-node
  rejection, and graph-wide apply/restore rollback tests pass.
- [x] Lower N-way joins to a binary join DAG instead of adding an N-way runtime.
  Evidence: `lower_join_chain_to_binary_dag` folds arbitrary ordered join steps
  into ordinary left-deep binary nodes, the admitted two-input path uses that
  fold, and `n_way_join_chain_lowers_to_a_left_deep_binary_dag` proves a
  three-relation shape. This is not a public N-way SQL support claim; catalog
  binding and runtime admission remain in Phase 2.
- [x] Lower `RIGHT JOIN` by operand swap and column remapping instead of adding a
  right-join state machine. Evidence: admission normalizes narrow
  `RIGHT [OUTER] JOIN` to `SupportedJoinKind::Left`, swaps catalog/alias/source
  and key bindings, and emits `LeftEquiJoin`; focused plan, runtime restore, and
  public relation/view/ingest/query tests prove unmatched SQL-right rows without
  any right-join operator or checkpoint codec.
- [x] Require decorrelated subqueries to lower only to existing relational
  operators. Evidence: the logical node enum has no subquery operator;
  identity/direct-projection/filter CTE and derived sources inline to existing
  scan/filter/project nodes, while scalar, correlated, aggregate-derived, and
  otherwise undecorrelated forms fail closed in
  `subquery_admission_uses_existing_relational_nodes_or_fails_closed`.
- [x] Re-express the existing aggregate-join and narrow-left-join paths through
  the generic DAG, or prove their differential equivalence as explicit
  specializations.
  - [x] Persist a fail-closed, versioned implementation selector and physical
    DAG hash in the admitted plan/checkpoint. The planner distinguishes keyed
    inner aggregate join, general aggregate join, and narrow-left join; plan
    validation rejects missing or tampered identities. The version-2 execution
    contract also binds the state codec, checkpoint manifest, output codec, and
    durable-output-publication protocol into the physical identity, so changing
    a persistence or visibility boundary invalidates prior equivalence evidence.
  - [x] Differentially verify the keyed inner sum/count specialization against
    `NativeBinaryJoinOperator -> NativeAggregateOperator` at every tested epoch,
    including consolidated output delta, join-side state, aggregate/published
    state, independent checkpoints, restore, and a shared tail. The harness has
    a deliberate delta mutant test.
  - [x] Differentially verify the left-only sum/count narrow-left specialization
    against `NativeLeftJoinOperator -> NativeAggregateOperator`, including
    unmatched/matched right transitions, consolidated deltas, canonical logical
    state, independent checkpoints, restore, and a shared tail. The retained
    specialization intentionally omits right state because admission proves a
    unique right key and no right-dependent output.
  - [x] Cover the retained general aggregate-join variants: aggregate filters,
    input expressions, right-side aggregate inputs, count-distinct, and
    min/max/avg state. `JoinSpecializationComparisonGraph` is an isolated,
    non-authoritative comparison target with independent join, aggregate, and
    HAVING/Top-K publication nodes and checkpoint state. Tests cover initial and
    changed epochs, independent restore/continued-tail equivalence for the
    right-side distinct/statistics family, and a HAVING/Top-K winner change.
- [x] Add pairwise and three-operator composition coverage, including rejection
  of incompatible changelog edges during admission. Evidence:
  `binary_join_and_top_k_share_checkpoint_and_replay_contract` covers a
  stateful pair; `filter_project_aggregate_composes_and_restores_through_one_checkpoint`
  covers a three-node chain; the general join comparison DAG additionally runs
  join -> aggregate -> publication. Admission rejects a crafted
  general-retract -> append-only edge in
  `logical_view_plan_admission_rejects_incompatible_changelog_edge` before plan
  hash validation or runtime construction.
- [x] Record typed plan fingerprint and physical operator DAG identity in
  comparison evidence. Evidence: Velorix passed outcomes in the version-2
  `IncrementalSqlComparisonResultV2` wire contract carry both the admitted
  `velorix-logical-view-plan-sha256-v1` fingerprint and the
  `velorix-physical-operator-dag-sha256-v1` identity. The cross-engine contract
  represents each native identity as either `available` with a validated,
  engine-namespaced digest or `unavailable` with a machine-readable reason;
  malformed identities fail validation instead of being downgraded to absent.
- [x] Classify every stateful plan as statically bounded, retention/watermark
  bounded, or unbounded; expose unbounded state explicitly and fail closed on a
  hard state quota rather than publishing partial results. Evidence: every
  stateful logical node derives a serialized `StateBoundednessV1` plus checkpoint
  codec; current aggregate, Top-K, join, latest-by-key, projection, and ranking
  nodes conservatively declare `unbounded`, while event-time windows declare
  their watermark column and lateness bound. The API enforces the configured
  maximum serialized checkpoint-state bytes before persistence. A rejected
  apply now restores the pre-epoch checkpoint under the same runtime lock, so
  neither an advanced epoch nor a partial output can leak into a retry;
  `standing_runtime_state_quota_rejection_rolls_back_before_publication` proves
  rejection, exact rollback, and successful same-epoch retry.

Exit gate:

- [x] The versioned supported-SQL surface passes through the common DAG
  contract. For each retained join specialization, construction-time binding of
  the same admitted logical DAG to either the selected implementation or the
  internal common-DAG reference produces identical consolidated deltas,
  canonically equivalent checkpoints and restart behavior, and identical
  authoritative/query-visible durable output. Checkpoint equivalence means the
  same logical epoch/frontiers, published relation, independent same-mode
  restore, and shared-tail observations; it does not require byte-identical
  state layouts or live cross-mode switching. Evidence:
  - `JoinExecutionBindingV1` is fixed before state creation, records the common
    logical-DAG hash, and derives distinct truthful implementation IDs, state
    codecs, and physical DAG hashes while preserving the production output
    codec and publication protocol. Wrong-mode restore fails closed, while the
    checkpoint runtime kind reconstructs the bound mode during ordinary
    rollback/recovery. The reference backend is injectable only through the
    internal runtime-factory seam and is not exposed through SQL, API, CLI, or
    release configuration.
  - `retained_join_specializations_toggle_to_common_dag_with_equivalent_recovery`
    covers keyed-inner, narrow-left, and general aggregate joins through two
    independent checkpoints, full teardown, restore, a shared state-sensitive
    tail, per-epoch consolidated deltas, canonical frontiers/published state,
    and explicit cross-mode restore rejection. Existing specialization state
    assertions additionally compare join, aggregate, multiplicity, and
    publisher state; the narrow-left projection is tied to admitted uniqueness
    and right-observational-independence.
  - `retained_join_specializations_match_common_dag_through_durable_api_restart`
    runs the same three paired cases in separate object-store namespaces through
    public relation/view admission, production apply/checkpoint publication,
    output-delta objects, authoritative checkpoint pointers, complete API
    restart, query serving, a shared tail, and duplicate-ingest retry. Both
    modes produce the same canonical checkpoint observations, output-delta
    refs, and query-visible rows.
  - Durable output deltas are consolidated before hashing and publication, so
    operator scheduling/order differences cannot create different object refs.
    `standing_runtime_output_delta_publication_is_canonical_and_detects_a_mutant`
    proves equivalent reorder/split deltas converge and a missing-row mutant is
    rejected by the comparison boundary.

## Foundation 0C: Comparable Correctness and Benchmark Evidence

Goal: make future parity and performance claims reproducible before broadening
the runtime.

- [x] Define shared `orders`, `customers`, and `products` relation schemas with
  explicit change identities and event time. Evidence:
  `crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json`.
- [x] Add expected-result fixtures for initial load, insert, retract/update,
  delete, checkpoint, restart, and replay. Evidence:
  `incremental_sql_corpus` validates every expected relation snapshot.
- [x] Cover filter/project, aggregate, distinct aggregate, inner join, left join,
  Top-K, fixed window, ranking, and chained-view workloads. Evidence: the shared
  corpus defines canonical SQL and expected final rows for all nine workloads.
- [x] Separate correctness results from throughput and latency results. Evidence:
  `IncrementalSqlComparisonResultV2` stores correctness outcomes and performance
  measurements in distinct collections.
- [x] Record engine name, exact version, configuration, durability mode, input
  semantics, warm-up, data volume, change mix, and state-retention policy.
- [x] Add a result format that can represent unsupported, semantically different,
  failed correctness, and measured outcomes without converting them to zeroes.
  Evidence: `incremental_sql_comparison` validates status-specific evidence and
  permits performance measurements only for correctness-passing workloads.
- [x] Compare each committed frontier against an independent batch SQL oracle,
  checking both the consolidated delta and materialized bag snapshot. Evidence:
  `FrontierConformanceVerifierV1` derives the expected signed delta from
  consecutive oracle bags, checks the observed consolidated delta, proves that
  delta produces the separately read materialized bag, and then compares the
  bag to the oracle without advancing on failure. The
  `frontier_conformance` integration test runs admitted Velorix SQL against an
  independent DataFusion batch recomputation at insert, update, and
  delete-after-restart frontiers; focused mutants distinguish a missing
  retraction from a wrong snapshot.
- [x] Verify durable output identity/acknowledgement separately so a correct final
  snapshot cannot hide duplicate publication or a missing retraction. Evidence:
  the version-2 ingest-epoch convergence record binds the materialized ACK to
  the exact authoritative checkpoint, output object refs, and
  `velorix-durable-output-publication-v1` protocol. Reads compare those refs to
  the local or metadata-backed authority before accepting a retry. The REST
  ingest test rejects a tampered ACK/output-ref binding, then proves an
  identical retry returns the existing materialized ACK with zero checkpoint or
  output-delta writes; the checkpoint crash matrix separately proves failures
  before authority publication do not advance the acknowledged checkpoint.
- [x] Split conformance/recovery workloads from the semantically equivalent
  performance suite. Evidence: corpus V1 declares distinct `conformance`,
  `recovery`, and `performance` suite manifests. Recovery owns only
  `checkpoint_restart` and `replay_tail`; those phases are forbidden from the
  performance suite, whose manifest explicitly requires semantic equivalence.
  Corpus validation rejects unknown/duplicate suite members and any accidental
  checkpoint/restart performance phase.
- [x] Exclude cells with different SQL, durability, output acknowledgement,
  watermark/lateness, retention, or restart-success semantics from performance
  comparison automatically. Evidence: every `PerformanceMeasurementV1` carries
  required and observed `PerformanceCellSemanticsV1`; result validation rejects
  the cell before publication if any of the six named dimensions differs. A
  table-driven contract test mutates every dimension independently and proves
  each is classified as `IncomparablePerformanceCell`, never as a zero or a
  measured result.
- [x] Report input rate, output rate/amplification, state bytes, checkpoint
  bytes/time, and restore time by feature family; do not produce one composite
  score. Evidence: `PerformanceMeasurementV1` requires a feature family,
  input/output rates, output-change count and derived amplification, state and
  checkpoint bytes, checkpoint time, and restore time. Validation recomputes
  amplification and rejects inconsistent evidence; the deny-unknown-fields wire
  contract has a focused test proving a `composite_score` cannot be published.
- [x] Run the corpus against Velorix and archive the baseline artifact.
  Evidence: `scripts/run-incremental-sql-baseline.sh` executes the versioned
  runtime example over all nine corpus cells and writes
  `baselines/incremental-sql/velorix-v0.1.0.json`. The admitted filter/project
  cell passes all six phases against independent DataFusion delta and bag
  snapshots, including checkpoint/restore; the other eight cells preserve their
  actual fail-closed admission reasons. Two consecutive runs produced the same
  SHA-256 artifact, and an integration test validates the archived contract,
  workload coverage, plan/DAG evidence, and 1-pass/8-unsupported classification.
- [ ] Complete edition-scoped evidence collection for GreptimeDB Flow,
  RisingWave, Materialize, and Feldera without weakening their default
  correctness semantics. Every engine artifact must retain all nine workload
  records. SQL/admission limitations are `unsupported`, observed oracle
  mismatches are `failed`, and a declared edition-level semantic limitation is
  `semantic_difference`; none may be omitted or converted into a pass.
  - [x] GreptimeDB Flow 1.1.4. Evidence:
    `scripts/run-greptimedb-flow-baseline.sh` verifies the official package and
    runs all six frontiers, including an actual standalone process restart;
    `scripts/incremental_sql_greptimedb.py` compares every admitted sink with an
    independent DuckDB batch recomputation. The deterministic archived artifact
    `baselines/incremental-sql/greptimedb-flow-v1.1.4.json` records five passes
    (aggregate, distinct aggregate, inner join, left join, chained view), three
    observed correctness failures (filter/project, Top-K, fixed window), and
    one admission rejection (ranking). Two consecutive runs produced SHA-256
    `5932bc53fd3b7b4e009e2c3e576a9e28abad5705efdddc400cce21a21519b069`;
    an integration test validates coverage and preserves the failures.
  - [ ] RisingWave 3.0.2 single-node durable recovery evidence collection.
  - [ ] Materialize Emulator edition-scoped evidence collection. This item may
    record the required durable fresh-process restart as
    `semantic_difference`; it does not assert recovery parity because the
    official Emulator provides neither persistence nor fault tolerance. See
    [Materialize Emulator limitations](https://materialize.com/docs/get-started/install-materialize-emulator/).
    Prepared evidence path: `scripts/run-materialize-emulator-baseline.sh`
    pins the v26.34.0 image index and platform digests, while
    `scripts/incremental_sql_materialize.py` requires all nine workload records,
    executes and oracle-checks the four pre-restart phases, and emits explicit
    blocked records for `checkpoint_restart` and `replay_tail`. Static Python,
    ShellCheck, image-digest, and result-contract checks pass; the first actual
    container pull currently stops at the unhealthy Dory engine, so this item
    remains unchecked until two complete clean runs produce one deterministic
    archived artifact.
  - [ ] Feldera Community edition-scoped evidence collection. This item may
    record checkpoint/fresh-process recovery as `semantic_difference`; it does
    not assert recovery parity because official checkpoint and fault-tolerance
    support is Enterprise-only. See
    [Feldera fault tolerance](https://docs.feldera.com/pipelines/fault-tolerance-overview/).
    Prepared evidence path: `scripts/run-feldera-community-baseline.sh` pins the
    0.330.0 official image index and platform digests, while
    `scripts/incremental_sql_feldera.py` compiles each of the nine workloads in
    an isolated Community pipeline, waits for ingress completion tokens, and
    checks all source snapshots and materialized views against DuckDB after each
    pre-restart phase. Compiler rejections remain `unsupported`, oracle
    mismatches remain `failed`, and passing pre-restart workloads record the two
    Enterprise-only recovery phases as structured `semantic_difference`
    evidence. The ad-hoc DataFusion endpoint is used only to inspect maintained
    state and is explicitly identified as non-incremental in the artifact.
    Python, ShellCheck, formatting, and pinned-image manifest checks pass; the
    first actual image pull reaches the same unhealthy Dory engine and returns
    EOF, so this item remains unchecked until two complete clean runs produce
    one deterministic archived artifact.

An edition-scoped item is complete only after two clean deterministic runs,
artifact validation, and explicit phase coverage. A `semantic_difference`
record must carry a stable reason code, scope, expected result digest, verified
and blocked phases, and explicit false values for recovery-parity and
performance-comparability claims. Recovery parity requires a separate capable
deployment and actual fresh-process restore evidence; evidence collection does
not imply it.

Exit gate:

- [ ] The same nine logical workloads, expected final rows, and phase schedules
  can be inspected for all engines; every phase has either actual output or an
  explicit admission/semantic diagnostic, and exceptions such as append-only
  input, TTL, unavailable durable restart, or partition recomputation are
  visible in the result. This is an evidence-completeness gate, not a claim of
  cross-engine recovery parity or performance comparability.
- [x] Injected duplicate publication, missing retraction, and wrong final snapshot
  failures are caught by different conformance checks. Evidence: the durable
  ACK retry test rejects a forged output binding and proves an idempotent retry
  performs zero output writes; `FrontierConformanceVerifierV1` reports a missing
  retraction as `DeltaMismatch` and a wrong materialized bag as
  `DeltaSnapshotMismatch` without advancing its verified frontier.
- [x] A result without native plan/DAG identity cannot be published as comparable
  performance evidence. Evidence: correctness can pass with explicit
  `unavailable` reasons, but performance is accepted only when the same passed
  outcome carries validated, engine-namespaced `available` identities for both
  the native logical plan and native physical DAG. A diagnostic EXPLAIN digest
  is non-qualifying, and focused tests reject performance when either native
  identity is unavailable.

## Phase 1: General Group Keys

Goal: remove the current single-primary-key grouping restriction.

- [x] Represent zero, one, or multiple typed grouping expressions in the logical
  plan without changing existing plan hashes silently. Evidence: logical
  aggregate nodes and their state requirements use ordered
  `Vec<LogicalPlanColumnRef>` keys whose relation schemas provide the column
  types. `aggregate_group_key_arity_is_structured_and_hash_visible` proves
  zero-, one-, and two-key shapes are represented distinctly, every arity
  changes the canonical plan hash, and pins the existing single-key hash so a
  future wire/identity change cannot be silent. SQL admission and execution for
  zero and multiple keys remain tracked by the separate items below.
- [x] Define the output-key contract for global and multi-key aggregates.
  Evidence: the Aggregate Output Identity section of
  `incremental-key-semantics-v1.md` specifies the tagged `Singleton` versus
  ordered non-empty `GroupKey` contract, SQL NULL equality, empty-input global
  row, final-group retraction, domain-separated internal tokens, public-schema
  boundary, checkpoint fail-closed behavior, and versioning needed to preserve
  existing single-key hashes and key bytes. Implementation and exact-recovery
  evidence remain required by the separate state, admission, global aggregate,
  retraction, restore, and exit-gate items below.
- [x] Generalize aggregate state lookup and checkpoint encoding to composite
  keys. Evidence: the shared `KeyedAggregateKernel` indexes the canonical full
  `DeltaKey` rather than a scalar key type, and `EngineCheckpointPayload`
  persists the same `DeltaBatch` keys without narrowing. The
  `keyed_aggregate_kernel_restores_composite_null_group_state_from_wire_checkpoint`
  test uses three two-component keys (including NULL components), serializes
  the checkpoint through JSON, restores it, applies a post-restore retraction
  and insert, and proves exact equality with uninterrupted execution. All 8
  engine and 23 operator tests pass. Public multi-key admission and full runtime
  restore remain tracked separately below.
- [x] Admit grouping by multiple registered columns and supported deterministic
  scalar expressions. Evidence: `SupportedViewPlan` carries an ordered,
  explicitly typed direct-column-or-expression group-key contract while legacy
  primary-key-only plans omit the new field and retain their pinned wire hash.
  Computed Int64 keys reuse the admitted deterministic projection-expression
  family and lower through an explicit logical projection node before the
  aggregate; volatile or unknown functions still fail closed. The runtime
  rekeys schema-bound rows into ordered composite `DeltaKey` objects, preserves
  SQL NULL grouping for registered nullable columns, publishes every key
  component as the declared composite output primary key, and validates the
  plan again at construction. Core tests cover direct, computed, alias,
  ordinal, source-projection, mismatch, and volatile-function admission;
  runtime tests materialize both `(user_id, nullable category)` and
  `(user_id, score / 10)` `SUM`/`COUNT` outputs through the public runtime
  factory. All 442 `velorix-core` and 239 `velorix-runtime` crate tests pass;
  the existing single-key hash fixture remains unchanged. Full checkpoint
  recovery and public API admission-to-restart remain tracked below.
- [x] Support a global aggregate with no `GROUP BY`. Evidence: the first
  admitted global shape is `COUNT(*)` (including an equivalent count of a
  registered non-null column after normalization). Its plan uses the explicit
  `SupportedAggregateOutputIdentity::Singleton` variant, an empty logical
  aggregate key list, and a dedicated `Singleton` operator uniqueness
  guarantee rather than an invalid empty `CandidateKey`. The public relation
  has no synthetic key column. Versioned, domain-separated internal state and
  publication keys remain checkpoint-visible but are omitted from Arrow
  output. `global_count_lowers_with_explicit_singleton_output_identity` proves
  admission and fail-closed exclusions; the runtime test
  `runtime_materializes_global_count_empty_input_and_final_retract_across_restore`
  proves exactly one `count = 0` row on empty input, count updates, distinct
  state/publication key domains, checkpoint restore, and return to the empty
  row after the final retraction. All 442 core, 239 runtime, and 158 API tests
  pass. Global `SUM`/`MIN`/`MAX`/`AVG` remain unsupported until their empty-set
  NULL state is represented deliberately; public API admission-to-restart is
  still tracked by the Phase 1 exit gate.
- [x] Retract a group when its final input contribution is removed. Evidence:
  `runtime_materializes_registered_composite_group_keys_with_null` first
  materializes two `(user_id, category)` groups, then retracts the sole row in
  the nullable-category group. The committed materialized snapshot retains
  only the other group, and the published output delta contains the exact
  negative record for the removed composite NULL key. The global test above
  separately proves that final retraction follows Singleton semantics by
  restoring the mandatory empty-input row instead of deleting it.
- [x] Restore composite-key and global aggregate state from checkpoints.
  Evidence: `runtime_materializes_registered_composite_group_keys_with_null`
  checkpoints a two-component group map whose wire payload contains a NULL
  category, restores through the production runtime factory, then applies a
  post-restore final-group retraction and publishes the exact remaining
  snapshot/delta. The global-count restart test checkpoints distinct Singleton
  state/publication domains at count three, restores them, retracts every input,
  and publishes the mandatory `count = 0` row. The lower-level
  `keyed_aggregate_kernel_restores_composite_null_group_state_from_wire_checkpoint`
  independently proves checkpoint/replay equivalence for three composite keys.
- [x] Add cardinality and skew workloads to the benchmark corpus. Evidence: the
  shared corpus now declares separate, performance-only
  `aggregate_composite_high_cardinality` and
  `aggregate_composite_hot_key_skew` profiles without changing the nine
  correctness workloads or their archived baseline identity. Both profiles use
  the admitted two-column `SUM`/`COUNT` query and deterministic 512-row batches:
  4,096 distinct groups from 4,096 rows for cardinality, and 256 groups with
  90% of 4,096 rows assigned to one key for skew. The corpus contract test
  validates the distributions and performance-suite membership. The
  `local_incremental` release benchmark consumes the profiles through the
  production SQL admission/materialized-view runtime and fails unless final
  group count, total contribution count, and maximum group size match; an
  actual run emitted both named metrics and passed (high-cardinality p95
  27.99 ms, hot-key-skew p95 2.39 ms on this local run).

Initially excluded:

- `ROLLUP`, `CUBE`, and `GROUPING SETS` remain fail-closed until ordinary
  composite grouping is complete and measured.

Exit gate:

- [x] Multi-key `SUM`/`COUNT` and global `COUNT(*)` pass admission-to-restart tests
  through the public API. Evidence:
  `rest_composite_and_global_aggregates_survive_restart_and_final_retraction`
  uses the public 1.0 REST surface to register a schema-bound relation, admit a
  nullable two-column `SUM`/`COUNT` view and a keyless global `COUNT(*)` view,
  ingest one source batch, and query both published outputs. A fresh API state
  then restores both durable checkpoints and returns byte-equivalent logical
  rows; a post-restart batch retracts every source row and proves the composite
  view becomes empty while the Singleton view publishes exactly one
  `count = 0` row. The public output-schema factory now derives ordered direct
  or computed group keys, preserves nullable direct keys, and emits an empty
  public primary key for Singleton. Its focused schema test and all 160 API
  tests pass.

## Phase 2: General Inner Joins

Goal: replace the exactly-two-relations/one-key shape with a reusable incremental
inner-join plan.

- [x] Complete binary composite equi-inner join before widening any other join
  dimension.
- [x] Represent ordered composite equi-keys and per-side residual predicates.
  Evidence: `SupportedJoinViewPlan` and binary logical join nodes retain their
  legacy scalar first key pair and optionally carry a versioned, non-empty
  `additional_pairs` tail. Admission-contract validation rejects unknown
  versions, an empty tail, empty identities, non-lexicographic order, duplicate
  columns on either side, and logical pairs that reverse the admitted left/right
  relation direction. The scalar representation omits the new field, and
  `legacy_scalar_join_plan_round_trips_without_changing_bytes_or_identity`
  proves byte-identical JSON round trips with unchanged logical hash and physical
  execution identity. Existing `ON` and source residuals remain relation-scoped
  in `JoinPredicateExpr` and lower to the appropriate pre-join side when safe;
  the existing residual-predicate tests plus the new composite representation
  tests pass in all 447 `velorix-core` tests.
- [x] Generalize per-side indexes and multiplicity tracking to composite keys.
- [x] Propagate inserts and retractions from every input side.
- [x] Preserve duplicate SQL bag semantics under many-to-many matches.
- [x] Define deterministic checkpoint identity and restore for every join input.

  Evidence for the five completed binary-runtime items: admission accepts an
  `INNER JOIN` only when its canonical equality pairs cover every declared
  primary-key column on both inputs exactly once and every corresponding pair
  has the exact same Arrow physical type. Both Arrow inputs are encoded into the
  same position-based composite `DeltaKey`, so differently named columns such
  as `tenant_id = account_tenant_id` and `user_id = account_id` do not create
  different index identities. `KeyedEquiJoin` retains a value multiset
  under each composite key on each side and multiplies signed weights across
  matches. `runtime_materializes_composite_primary_key_join_across_retract_and_restart`
  proves that all key components participate, a left weight of 2 and right
  weight of 3 produce bag multiplicity 6, checkpoint restore preserves both
  side indexes, and later left retract plus right insert/retract yield exact
  output deltas. The durable
  `velorix-composite-pk-positional-json-array-join-key-v1` identity is derived
  into the execution implementation and persisted in the checkpoint. Restore
  rejects a missing, unknown, or binding-mismatched composite codec while a
  legacy scalar plan and checkpoint omit the additive field byte-for-byte.
  Right-side PK declaration order and SQL conjunct order differ in the fixture,
  proving canonical pairing is not dependent on either order. All 447 core,
  240 runtime, and 160 API tests pass. Composite
  outer joins, partial-primary-key equality, multi-column non-primary keys, and
  self-joins remain fail-closed.
  A strict follow-up ChatGPT Pro review returned `GO` for all five items and
  found no remaining P0/P1 correctness blocker. It classified an immutable
  checkpoint artifact produced by an older binary as useful long-term golden
  evidence, but not a completion requirement for this additive, byte-omitting
  scalar compatibility change.
- [x] Add non-primary duplicate handling and self-join only after binary
  composite-key correctness is proven.
  - [x] Admit one non-null, non-primary equality key across two distinct
    relations and verify duplicate many-to-many maintenance.
  - [x] Add durable relation-instance identity and verify one physical input
    feeding both roles of a self-join.

  Evidence for the completed first sub-item:
  `SupportedJoinKeyDomainV1::NonPrimaryNonNullScalarV1` admits exactly one
  equality across two distinct relations only when both columns are non-null,
  non-weight, outside both declared primary keys, use the exact same Arrow
  physical type, and belong to the already-supported scalar key-atom set.
  Its dedicated
  `velorix-non-primary-non-null-scalar-join-key-v1` identity is bound to the
  execution implementation and checkpoint; restore rejects cross-use with the
  composite-PK codec. `runtime_materializes_non_primary_duplicate_join_across_retract_and_restart`
  proves a left multiplicity of `2 + 1` and right multiplicity of `3 + 1`
  produce SQL bag count 12, then verifies checkpoint restore and exact
  simultaneous left/right retract results. Existing checked-arithmetic tests
  cover side-weight and match-product overflow without state mutation. All 448
  core, 241 runtime, and 160 API tests pass together with workspace all-target,
  release benchmark compilation, formatting, and diff checks. At that gate,
  self-join remained fail-closed pending canonical scan/input-instance identity.
  A strict ChatGPT Pro follow-up returned `GO` for this exact sub-item and
  confirmed there is no remaining P0/P1 correctness blocker within the stated
  single-key, distinct-relation boundary.

  Evidence for the completed self-join sub-item: the binder assigns the
  alias-independent `scan_left` and `scan_right` relation-instance identities
  to two scans of one physical catalog relation, persists those identities in
  `VelorixLogicalViewPlanV1`, and binds execution to
  `velorix-self-join-left-then-right-atomic-fanout-v1`. Admission is deliberately
  limited to an explicit-alias inner equality on one non-primary, non-null,
  same-physical-type scalar key feeding a global `COUNT(*)`; every wider shape
  fails closed. The runtime atomically fans each physical delta through staged
  left and right role indexes, checkpoints both indexes behind one physical
  input frontier, and preserves the singleton zero row. Core tests prove
  canonical identity across alias renaming and rejection of missing instances
  and wider SQL. `runtime_materializes_atomic_self_join_fanout_across_retract_and_restart`
  proves count `10`, restart, partial retract to `5`, final retract to `0`, and
  rollback after a staged-role overflow. The public REST test
  `rest_self_join_atomic_fanout_survives_restart_replay_and_final_retract`
  additionally proves SQL admission, durable plan serialization, runtime
  selection, published reads, service restart, a forced output-delta publication
  failure after durable source append, prior-pointer authority, tail replay,
  duplicate-offset idempotency, and the final `{count: 0}` row. All 450 core,
  242 runtime, and 162 API tests pass. A strict ChatGPT Pro re-review returned
  `GO` with no remaining P0/P1 correctness or durability blocker for this
  narrow slice.
- [x] Support more than two input relations only by lowering to the Foundation
  0B binary join DAG, not a new three-table state machine.

  Evidence: public 1.0 admission is deliberately bounded to exactly three
  distinct relations and exactly two left-deep `InnerEquiJoin` nodes. The
  accepted slice requires explicit aliases, complete non-null composite-PK
  equality from the root input to each newly added input, exact Arrow key
  types, canonical root-PK projection and grouping, and `COUNT(*)`; every wider
  three-input shape and every four-or-more-input shape fails closed.
  `SupportedThreeInputInnerJoinCountPlanV1` persists relation order, root key
  identity, output identity, the positional composite-key codec, and a
  bijective root-to-input PK permutation for each relation. Admission and
  restore re-lower SQL from the catalogs and output schema and require the
  complete stored logical plan to match. The runtime constructs only the
  Foundation 0B `NativeOperatorGraph`: `join_1 -> join_2 -> Project ->
  Aggregate`. It adds no N-way join operator or state machine.

  `runtime_materializes_three_input_composite_pk_join_through_binary_dag`
  proves bag multiplicity `2 * 3 * 4 = 24`, an unmatched key, changes on all
  three inputs (`24 -> 12 -> 8 -> 10`), checkpoint restore, and exact
  post-restore output. The companion atomicity test proves second-join overflow
  rollback, safe reuse of the same source offsets, non-contiguous frontier
  rejection, and fail-closed torn graph/output checkpoints.
  `rest_three_input_composite_pk_join_uses_binary_dag_and_survives_restart`
  proves the full public REST path through durable-plan serialization,
  publisher-manifest reads, restart, post-restart retract (`24 -> 18`), and
  duplicate-offset idempotency. All 451 core, 244 runtime, and 163 API tests
  pass with workspace all-target checking, release benchmark compilation, and
  formatting. A strict ChatGPT Pro re-review returned `GO` with no remaining
  P0/P1 blocker for this bounded item and its exit gate.
- [x] Verify join-order planning does not change SQL results or checkpoint
  compatibility.

  Evidence: new three-input admissions persist schema version 2 and
  `velorix-three-input-root-fixed-right-relation-id-order-v1`. The output-key
  root remains fixed, while both complete root-to-right PK bindings are
  validated and then sorted by stable right `relation_id` before the two binary
  joins are built. SQL encounter order therefore cannot change the canonical
  join subgraph, graph state, or materialized result. The policy is serialized
  into execution and physical-DAG identity; restore selects the policy stored
  in the plan rather than today's default planner and rejects unknown or
  tampered policy combinations.

  Compatibility with the field-absent format is explicit: schema version 1
  with no `join_order_policy_id` permanently means legacy SQL encounter order,
  continues to serialize without the additive field, and restores through the
  policy-aware lowerer. Different SQL text still has a different standing
  program identity and does not share checkpoints.
  `three_input_composite_pk_sql_lowers_to_validated_binary_dag` proves swapped
  right JOIN clauses have the same canonical execution, nodes, edge contract,
  state requirements, and physical DAG for one output identity, while legacy
  decode and unknown-policy rejection remain exact.
  `three_input_join_order_policy_preserves_results_state_and_legacy_restore`
  proves both SQL orders produce count `24`, identical graph/published state,
  and independent restart, plus successful field-absent legacy checkpoint
  restore. The public REST path runs both orderings concurrently, obtains the
  same rows, and restores both views. All 451 core, 245 runtime, and 163 API
  tests pass with workspace all-target checking, release benchmark compilation,
  formatting, and diff checks. ChatGPT Pro returned `GO` with no P0/P1 blocker.
- [x] Benchmark one-to-one, one-to-many, many-to-many, skewed, and unmatched
  key distributions.

  Evidence: the shared performance corpus defines five deterministic binary
  non-primary-key inner-join profiles. They use unique row primary keys and
  distinct payloads so multiple rows under one join key remain separate values
  in both per-side multisets: one-to-one has 512 matches over 512 keys;
  one-to-many has 1,024 matches and maximum fan-out 4; many-to-many has 4,096
  matches and maximum fan-out 32; the 80% hot-key profile has 167,384 matches,
  167,281 on one key; and the disjoint profile populates 512 rows on both sides
  while producing no matches. A right-side residual predicate deliberately
  retains the distinct right payload in indexed state.

  `scale_join_key_workloads` runs every profile through production SQL
  admission, Arrow conversion, the native incremental join, grouping, and
  materialized snapshot reads. For every one of five fresh-runtime samples it
  independently reconstructs the exact per-key `SUM(left payload)` and
  `COUNT(*)` map and compares it with output. It also inspects the untimed
  checkpoint and requires the left/right state record counts to equal the
  respective input row counts, preventing an optimizer change from silently
  collapsing the intended multiset distribution. The corpus contract test
  independently recomputes declared group, match, and maximum-fan-out counts.

  An actual release run passed result validation and the archived local
  PR-smoke gate. Descriptive p95 apply times on that run were 2.24 ms, 1.82 ms,
  4.09 ms, 144.94 ms, and 0.56 ms in the order above. Five samples are enough
  for workload evidence but not a statistically strong percentile or an SLO;
  local PR smoke therefore gates named workload presence and deterministic
  costs rather than wall-clock regression. ChatGPT Pro returned `GO`, found no
  P0/P1 methodological blocker, and judged the baseline/gate integration
  sufficient; its optional state-cardinality defense is included above.

Initially excluded:

- Non-equality, temporal, interval, cross, and natural joins remain
  fail-closed.

Exit gate:

- [x] A composite-key three-relation inner join produces exact results after
  changes on each side and after restart.

## Phase 3A: Complete Outer-Join State

Goal: support ordinary left, right, and full equi-join maintenance.

- [x] Define outer-join output as dynamic-table general-retract semantics.

  `incremental-processing-semantics-v1.md` now defines the committed SQL bag,
  checked multiplicity products, NULL-key non-matching, consolidated snapshot
  differences, and zero-to-positive/positive-to-zero match transitions. The
  operator-edge contract fixes outer-join output as `general_retract`, makes
  the null-extended side nullable, and drops unproved keys. The admitted-plan
  test checks those capabilities and the native operator test checks the exact
  signed deltas plus checkpoint round-trip. ChatGPT Pro returned `GO` with no
  P0/P1 semantic blocker; broader SQL support and the remaining transition
  matrix stay in the later Phase 3A items.
- [x] Reject an unbounded outer join feeding an append-only/final-output edge
  unless a bounded input or progress policy makes unmatched rows final.

  The V1 edge validator rejects `general_retract -> append_only`, and outer
  joins derive unbounded state plus `general_retract` output. A focused admitted
  left-join test changes its downstream requirement to append-only and proves
  admission fails on the incompatible changelog edge. V1 exposes no implicit
  finalization exception; a future bounded/progress exception must be a real
  closing operator with its own append-only guarantee.
- [x] Track match counts needed to retract and restore null-extended rows.

  `NativeLeftJoinOperator` derives each touched key's checked match count from
  the retained right-side multiset before and after applying its delta. That
  multiset is checkpoint authority, avoiding a second cached count that could
  drift on restore. The exact-delta test now removes one of two matches first
  (no null row), then the final match (matched-row retract plus null-row insert),
  and round-trips the resulting state.
- [x] Allow right-side values as nullable grouped-aggregate and filter inputs
  for admitted left joins.

  The bounded grouped-left-join family now retains required right columns in
  its persisted binding and accepts right inputs for `SUM`, `COUNT`,
  `COUNT(DISTINCT)`, `MIN`, `MAX`, and `AVG`, per-output right/cross-side
  aggregate filters, and post-join right-side `WHERE` predicates. It does not
  claim raw joined-row projection. NULL-accepting `WHERE` predicates are never
  pushed into the right input; an `IS NULL` regression proves matched-row
  suppression and last-match restoration across restart. Extended state
  separates group presence from qualifying aggregate inputs so unmatched,
  all-NULL, and FALSE/UNKNOWN-filter inputs publish SQL NULL for
  `SUM`/`AVG`/`MIN`/`MAX`, zero for `COUNT(expr)`/`COUNT(DISTINCT expr)`, and
  retain the null-extended occurrence for `COUNT(*)`. A tampered persisted
  right-value binding fails restore. Right-side CTE/derived-source filters, ON
  residuals, shared aggregate filters, right-side grouping, and general
  `SELECT left_col, right_col` publication remain fail-closed. Full core (453)
  and runtime (249) suites pass, including specialization/common-DAG and
  checkpoint equivalence. ChatGPT Pro first returned `NO-GO` for unsafe WHERE
  pushdown and empty-aggregate zero publication; after both fixes it returned
  `GO` with no remaining P0/P1 blocker for this bounded item.
- [x] Support right join only through the Foundation 0B operand-swap rewrite.

  SQL admission canonicalizes `RIGHT JOIN` by swapping its catalogs, aliases,
  equality-key direction, and aggregate relation sides before constructing the
  plan. The persisted representation has no right-join kind and logical nodes
  have no right-join variant. Planner evidence checks
  that the preserved SQL-right relation becomes the plan's left input and that
  the DAG contains `LeftEquiJoin`; runtime evidence materializes unmatched
  SQL-right rows, checkpoints the canonical plan as `join_kind = left` with
  swapped relation IDs, and restores the same output. Thus there is no second
  right-join state machine or checkpoint codec to drift from left-join
  semantics.
- [x] Support full join with exact transitions between unmatched and matched
  states.

  Admission accepts the bounded two-relation, scalar primary-key equi-join
  shape only when the output key is explicitly
  `COALESCE(left_key, right_key)` and grouped by that expression, ordinal, or
  alias. It lowers to a dedicated `FullEquiJoin` logical node with a non-null
  coalesced output key, nullable input schemas, unbounded state, and
  `general_retract` output; unsafe residual/source shapes remain fail-closed.
  `NativeFullJoinOperator` checkpoints both bags and implements the symmetric
  zero/nonzero boundary on either input. Its exact-delta test covers left-only,
  right-only, both match directions, duplicate multiplicity, partial versus
  final deletion, and checkpoint round-trip. The public runtime test covers
  SQL admission, nullable `SUM` for right-only rows, both match directions,
  duplicate multiplication, partial/final retractions, persisted
  `join_kind = full`, two restores, and continued tail changes. The retained
  specialization also matches the independent common-DAG reference in delta,
  logical state, checkpoint, restore, and continued tail. Full core (456) and
  runtime (250) all-target suites pass. The scheduled ChatGPT Pro review could
  not run because the required in-app Pro browser could not navigate after the
  prior finalized session; no fallback reviewer was substituted.
- [x] Verify row-level match multiplicity, duplicate matches, nullable payloads,
  key-changing updates, and deletes from either side.

  `full_join_preserves_row_multiplicity_nullable_payloads_and_key_changes`
  starts with two occurrences on each side and proves the matched weight is
  four, carries a JSON-NULL payload without conflating it with the
  null-extended side, changes the right key and then the left key through
  same-epoch retract/insert pairs, and checks every resulting matched and
  unmatched signed delta. After restore it removes duplicate occurrences from
  both sides, distinguishing partial deletion from the final transition.
  `runtime_materializes_full_join_symmetric_transitions_and_restores_state`
  additionally runs the production SQL/runtime path with a nullable right
  payload and a source-key change, while the earlier left/right tests cover
  their canonical and operand-swapped paths. Core (456) and runtime (250)
  all-target tests pass.
- [x] Restore unmatched-row state exactly from checkpoints.

  The native full-join transition test checkpoints simultaneous left-only and
  right-only bags, restores them into a fresh graph, and then transitions each
  to matched output with the exact unmatched retraction. Its final checkpoint
  also round-trips byte-for-byte under `velorix-native-full-join-v1`. The public
  SQL test checkpoints left-only and right-only groups, restores before
  matching either side, checkpoints again after duplicate matching, restores a
  second time, and continues through final-match deletion and a key change.
  Selected-specialization/common-DAG checkpoint equivalence and cross-mode
  restore rejection remain green.
- [x] Add high-unmatched-ratio and match-transition benchmarks.

  The local native benchmark now publishes two independently named FULL JOIN
  workloads. `full_join_high_unmatched_ratio` uses 512 rows per side with only
  25 matching keys (974 of 999 output groups are unmatched) and validates the
  complete nullable snapshot on every sample. `full_join_match_transitions`
  begins with 1,024 unmatched rows, then times a right-side key replacement
  that retracts 512 null-extended right rows, retracts 512 null-extended left
  rows, and creates 512 matches; every sample validates the final snapshot.
  Five-sample local debug execution passed and reported p95 values of
  2,119.715 ms and 10,462.309 ms respectively. These are descriptive local
  measurements, not release SLOs or cross-engine evidence.

Exit gate:

- [x] Left, right, and full equi-joins pass insert, retract, match-transition, and
  restart tests with bag semantics.

## Phase 3B: Semi/Anti Joins

Goal: provide the reusable existence operators required by later decorrelation.

The 2026-08-10 Pro architecture review returned GO for distinct semi, ordinary
anti, and future null-aware-anti node/codec identities. It also confirmed that
the complete input bags remain checkpoint authority, same-epoch output is
consolidated before publication, and nullable `IN`/`NOT IN` must fail closed.

- [x] Add binary semi join with duplicate-aware right-side match counts.
  `NativeSemiJoinOperator` derives checked per-key totals from its retained
  right bag, emits the retained left bag only across zero/nonzero existence
  boundaries, and checkpoints both bags with
  `velorix-native-semi-join-v1`. Evidence: the duplicate/restore transition
  test plus `cargo test -p velorix-core --all-targets` (457/457).
- [x] Add binary anti join with exact zero-to-one and one-to-zero match
  transitions. `NativeAntiJoinOperator` uses the independent
  `velorix-native-anti-join-v1` codec, retracts the retained left bag on the
  first right match, and restores the current left bag only after the final
  match disappears. Evidence: duplicate/blocked-left/restore transition test
  plus `cargo test -p velorix-core --all-targets` (458/458).
- [x] Keep null-aware anti join separate from ordinary anti join. The ordinary
  `NativeAntiJoinOperator` has no nullable-semantics mode, owns only the
  `velorix-native-anti-join-v1` identity, and the semantics contract reserves a
  distinct future null-aware node/codec. The Pro review independently confirmed
  this boundary; nullable SQL admission remains a separate unchecked item.
- [x] Verify right-side insert/delete, left key update, duplicates, checkpoint,
  and restart. The shared semi/anti matrix moves a multiplicity-two left row to
  a new key between two restores, applies a duplicate right match, proves the
  partial deletion is silent, and checks inverse final-delete deltas. Evidence:
  `semi_and_anti_join_key_update_matrix_survives_two_restarts` plus
  `cargo test -p velorix-core --all-targets` (459/459).
- [x] Reject `IN`/`NOT IN` forms with nullable semantics until the null-aware
  operator is proven. Admission now rejects nullable probe expressions, literal
  lists containing `NULL`, and nullable-build subquery forms while retaining
  non-null literal-list support. Evidence:
  `nullable_in_and_not_in_forms_fail_closed`, core 460/460, runtime 250/250,
  fmt, and diff checks.

Exit gate:

- [x] Semi/anti joins pass public admission-to-restart tests and introduce no
  subquery-specific runtime node. The bounded public V1 admits direct
  `EXISTS`/`NOT EXISTS` predicates correlated by one exact equality between the
  two relations' complete non-null scalar primary keys. Admission lowers them
  to `SemiEquiJoin`/`AntiEquiJoin` plus ordinary project/output nodes and the
  `TwoInputSemiAntiJoinProject` execution contract; there is no `Exists`,
  subquery, or null-aware runtime node. The public runtime is a one-node
  `NativeOperatorGraph` using the independent semi/anti codecs and validates
  checkpointed output against both authoritative input bags on restore.
  Evidence: `correlated_exists_and_not_exists_lower_to_generic_semi_anti_join_nodes`,
  `correlated_exists_v1_fails_closed_outside_complete_non_null_pk_equality`, and
  `public_exists_and_not_exists_materialize_through_restart_with_duplicate_matches`
  cover both join kinds, right zero/nonzero transitions, duplicate matches,
  left updates, two checkpoint/restart boundaries, and final deletion.
  `rest_exists_and_not_exists_views_survive_restart_and_match_transitions`
  additionally proves public relation/view creation, materialized ingest/query,
  fresh API-state restore, and post-restart inverse match transitions for both
  views. Full verification: core 462/462 and runtime 251/251 all-target tests,
  including benches, plus API 164/164; formatting passes. The only diagnostic
  is the pre-existing macOS `__eh_frame` linker-size warning.

## Phase 4: View-on-View Incremental Dependencies

Goal: let a materialized output act as a typed input to another standing view.

- [ ] Assign a relation-compatible schema, key, and frontier contract to published
  view output.
  - [x] Persist an immutable `PublishedRelationBindingV1` in the active native
    runtime record. It binds the public `RelationSchema`, schema hash, explicit
    key-descriptor hash, producer view generation, logical-plan hash, stable
    output-stream identity, signed-delta codec identity, and
    `producer_commit_epoch` frontier kind. The API derives the producer
    generation from the authoritative view-bootstrap record, and the registry
    revalidates all hashes and identities on read. The published relation has
    no hidden weight/delta column; signed bag weights belong to the internal
    delta codec. Evidence: core contract mutation tests, storage registry tests,
    and
    `rest_three_input_composite_pk_join_uses_binary_dag_and_survives_restart`.
  - [x] Back `producer_commit_epoch` with a durable output commit record,
    including empty-delta epochs. New published bindings emit a version-2
    `standing_runtime_output_commit_v1` envelope around the consolidated typed
    delta. The envelope seals producer generation and plan, output stream,
    schema/key hashes, delta codec, checkpoint key/state hash, direct-input
    coverage hash, and its own canonical commit digest. A distinct
    `standing-runtime-output-commit:` checkpoint reference makes the commit
    authoritative only with the checkpoint pointer; legacy delta references
    remain readable. Exactly one commit is required for every new producer
    epoch even when its consolidated delta is empty. Recovery inherits only the
    previous immutable coverage identity and advances its partition offsets
    monotonically when the restored runtime emits a checkpoint without the API
    coverage envelope.

    Evidence: `published_relation_output_commit_fences_empty_delta_and_checkpoint_cut`
    proves the empty commit, digest mutation rejection, and missing-empty-commit
    rejection. `standing_runtime_checkpoint_crash_matrix_keeps_previous_authoritative_pointer`
    now covers failures while writing the commit, state, and checkpoint, proves
    that the prior pointer remains authoritative, and proves identical retry
    convergence. The public three-input REST restart test verifies the persisted
    binding matches the authoritative commit envelope and that a post-restart
    epoch publishes another valid commit. API 165/165, Meta all-target, Control
    all-target, workspace all-target checking, formatting, and diff checks pass.
  - [x] Replace the temporary direct-source `producer_input_coverage_hash` seal
    with the authoritative `CausalCutV1` digest that can also represent direct
    view-input cursors before completing this parent item.
    - [x] Persist a versioned canonical cut in each new published checkpoint and
      bind its domain-separated digest into the producer commit. The cut keeps
      direct source catalog/frontier identities separate from direct view
      cursors (`input_edge`, producer generation, output stream/epoch, and
      commit digest); canonical ordering and duplicate identities fail closed.
      `producer_input_coverage_hash` has been removed. Existing bootstrap source
      coverage remains encoded alongside the cut only while its callers need
      it, and checkpoint validation requires the two source representations to
      be exactly equal.
    - [x] Prove canonical mixed Source/View encoding, cursor mutation rejection,
      generation/schema/key/codec/digest mismatch rejection, zero-row cursor
      advancement across authoritative restart, and orphan commit rejection.
      Evidence: `causal_cut_digest_is_canonical_for_mixed_source_and_view_inputs`,
      `causal_cut_rejects_duplicate_view_edges_and_source_coverage_mismatch`,
      `causal_cut_accepts_initial_catalog_epoch_with_source_frontiers`,
      `published_relation_output_commit_fences_empty_delta_and_checkpoint_cut`,
      `mixed_source_view_causal_cut_survives_authoritative_restart`, and
      `orphan_output_commit_is_not_authoritative_progress`. Core all-target,
      API 167/167, Meta all-target, Control all-target, workspace all-target
      checking, formatting, and diff checks pass.
    - [x] A strict follow-up architecture review returned bounded `GO` with no
      P0/P1. It accepted the transitional duplicate encoding only because
      `CausalCutV1` is explicitly the recovery truth, `input_coverage` is only a
      bootstrap-compatibility mirror, and exact equality is enforced. It also
      confirmed that checkpoint-key reuse is safe under create-only conflict and
      pointer/manifest authority. Production resolution of a non-empty view
      cursor through the producer's authoritative pointer/checkpoint/commit
      chain remains the next consumer boundary and is not claimed here.
- [ ] Build and validate an acyclic dependency graph during admission.
- [ ] Propagate signed output deltas to dependent views without reading full
  materialized snapshots.
- [ ] Reuse the Foundation 0A processing/output/dependency frontier and recovery
  contract across the chain; do not introduce a second progress model.
- [ ] Prevent queries from observing a dependent output beyond any input frontier.
- [ ] Fail closed on cycles, missing dependencies, incompatible schemas, and
  checkpoint generation mismatches.
- [ ] Restore a multi-level chain and replay only epochs after its consistent
  frontier.
- [ ] Benchmark fan-in, fan-out, and chain depth separately.

Initially excluded:

- Recursive dependencies remain fail-closed.

Exit gate:

- [ ] A three-level filter to aggregate to Top-K chain remains exact across
  insert, retract, restart, and replay.

Phase 4 architecture review (2026-08-10): a strict ChatGPT Pro review returned
`NO-GO` on the initial frontier proposal. The accepted corrections are: direct
typed `DeltaBatch` inputs rather than synthesized source relations; one
authoritative `CausalCutV1` made from direct source/view cursors; durable producer
commit records and consumer cursors; dependency edges bound to immutable view
generations under a tenant graph revision; and delta retention until every live
dependent has durably advanced. No Phase 4 parent checkbox may be completed by
schema metadata alone, and missing retained deltas fail closed instead of using
an implicit snapshot fallback.

The follow-up review of the durable producer-commit slice returned a bounded
`GO`: the metadata pointer remains the sole authority, and consumers must follow
`pointer -> authoritative checkpoint -> referenced commit -> validated commit
digest`. A commit object that exists without that chain is an orphan, not
published progress. Before the parent schema/key/frontier item can close, the
temporary direct-source coverage hash must be replaced completely by a
canonical `CausalCutV1` containing separately ordered source frontiers and view
cursors. Required focused evidence includes orphan rejection, mixed Source/View
restart, generation/schema/key/codec mismatch rejection, causal-cut mutation,
and zero-row cursor advancement.

The subsequent `CausalCutV1` implementation review returned bounded `GO` with no
P0/P1 for the nested replacement item only. `CausalCutV1` is the recovery truth;
the co-encoded `input_coverage` is a bootstrap compatibility mirror and cannot
diverge because checkpoint validation reconstructs and exactly matches the
source portion. Non-empty production view cursors remain forbidden until the
next slice resolves each cursor through the producer's authoritative
pointer/checkpoint/commit chain and fails closed on any identity or digest
mismatch.

## Phase 5: Public Event-Time Semantics

Goal: promote window support only after its observable time behavior is a stable
product contract.

- [ ] Keep experimental windows and ranking out of cross-engine comparable
  performance results until their observable time/order contracts are complete.
- [ ] Document event-time extraction, per-partition watermarks, window closure,
  allowed lateness, and late-row handling.
- [ ] Define multi-input watermark combination and idle-partition behavior.
- [ ] Decide whether late rows are rejected, dropped with evidence, or admitted
  within a configured allowance; do not make the choice implicit.
- [ ] Persist all watermark, session-merge, and closed-window state required for
  deterministic recovery.
- [ ] Verify TUMBLE and HOP retractions before and after window closure.
- [ ] Verify SESSION merge, bridge retraction, and recovery behavior.
- [ ] Bound state with an explicit retention contract that does not silently
  change all-history SQL semantics.
- [ ] Add out-of-order, late, idle-partition, and high-window-cardinality workloads.
- [ ] Remove the experimental gate only after the public API and operational
  documentation expose the policy.

Exit gate:

- [ ] TUMBLE, HOP, and SESSION have documented deterministic outcomes for
  in-order, out-of-order, late, restart, and replay cases.

## Phase 6: Types and Deterministic Expressions

Goal: broaden common SQL without weakening replay correctness.

- [ ] Inventory scalar and aggregate state requirements by type.
- [ ] Add checked arithmetic and comparison for the next selected type family.
- [ ] Add string and temporal expressions based on measured corpus demand.
- [ ] Define decimal precision, scale, overflow, and aggregate output rules.
- [ ] Keep nondeterministic time, randomness, external I/O, and process-local
  functions unmaterializable.
- [ ] Version expression encoding and checkpoint state when adding a type.
- [ ] Before a type becomes key/order capable, add canonical encoding, equality,
  hashing, ordering, NULL, exceptional-value, and semantic-version evidence
  required by Foundation 0A.
- [ ] Add null, overflow, boundary, and restart tests per type family.

Exit gate:

- [ ] Each newly admitted type has exact SQL, Arrow, delta, checkpoint, and query
  representations with no lossy implicit conversion.

## Phase 7: Relational Rewrites and Subqueries

Goal: admit common SQL syntax by lowering it to already proven operators.

- [ ] Normalize non-recursive CTEs and derived tables into the general logical
  plan.
- [ ] Lower uncorrelated scalar and relation subqueries where cardinality is
  statically valid.
- [ ] Decorrelate selected `EXISTS`, `NOT EXISTS`, `IN`, and `NOT IN` forms into
  the Phase 3B semi/anti operators with correct null semantics.
- [ ] Reject subqueries that cannot be represented by admitted incremental
  operators; do not evaluate them by source scan.
- [ ] Verify equivalent rewritten queries produce identical plan semantics and
  output deltas.

Exit gate:

- [ ] The selected subquery corpus lowers to existing operators and remains exact
  after retractions and restart.

## Phase 8: Deferred Advanced Capabilities

These items require a separate design decision and are not implied by completing
the earlier phases:

- [ ] General analytic window frames and navigation functions
- [ ] Exact percentile, median, ordered-set, and collection aggregates
- [ ] Non-equality, interval, temporal, and as-of joins
- [ ] Deterministic user-defined functions with portable serialized identity
- [ ] Recursive and mutually recursive standing queries

Before starting any item:

- [ ] Specify its worst-case state growth and retraction algorithm.
- [ ] Specify replay determinism and checkpoint compatibility.
- [ ] Establish a real workload that cannot be expressed by completed operators.
- [ ] Approve a dedicated design document and benchmark budget.

## Progress Record

Add one row when a phase or independently shipped capability completes. Link to
stable evidence; do not use this table for intentions.

| Date | Capability | Evidence | SQL contract updated | Benchmark result |
| --- | --- | --- | --- | --- |
| 2026-08-09 | Verified baseline | `view_plan` 304/304; `materialized_view_runtime` 163/163 | Yes | Existing local incremental gate |
| 2026-08-09 | Shared comparison schemas, changes, and nine-workload corpus | `incremental_sql_corpus` 1/1 | No SQL capability change | Measurement pending |
| 2026-08-09 | Versioned cross-engine correctness/performance result contract | `incremental_sql_comparison` 5/5 | No SQL capability change | Contract only |
| 2026-08-09 | Processing weight and key semantics bound into plan/recovery identity | `operators` 23/23; key adapter 19/19; `view_plan` 305/305; `materialized_view_runtime` 163/163 | No SQL capability change | Semantics gate only |
| 2026-08-10 | Foundation 0A processing, key, bag, publication, crash, and recovery semantics | `velorix-core` operator/key properties; runtime crash/replay matrix; API durable ACK retry and checkpoint publication tests | No SQL capability change | Semantics gate complete |
| 2026-08-10 | Foundation 0B common DAG plus retained-specialization differential gate | `materialized_view_runtime` 174/174; `velorix-api --lib` 158/158; keyed inner, narrow left, and aggregate join specialization/reference restart comparison | No SQL capability change | Differential correctness gate complete |
| 2026-08-10 | GreptimeDB Flow 1.1.4 shared-corpus baseline | Archived artifact contract test; two identical runs, SHA-256 `5932bc53fd3b7b4e009e2c3e576a9e28abad5705efdddc400cce21a21519b069` | No SQL capability change | 5 pass, 3 correctness failures, 1 unsupported |
| 2026-08-10 | Edition-scoped semantic-difference evidence contract | `incremental_sql_comparison` 14/14; structured reason, scope, expected digest, phase coverage, and fail-closed non-parity/non-performance claims | No SQL capability change | Evidence contract only |
| 2026-08-10 | Narrow atomic two-instance self-join global count | Canonical `scan_left`/`scan_right`; staged fanout rollback; public REST publication-failure/restart/replay path; core 450/450, runtime 242/242, API 162/162; Pro `GO` | Yes | Correctness and durability gate complete |
| 2026-08-10 | Bounded three-input composite-PK inner join over binary DAG | Exactly two Foundation 0B joins; SQL/catalog re-derivation on restore; overflow/frontier/torn-checkpoint rollback; public REST restart/replay; core 451/451, runtime 244/244, API 163/163; Pro `GO` | Yes | Correctness and durability gate complete; join-order benchmark pending |
| 2026-08-10 | Versioned canonical three-input join-order policy | Root-fixed/right-relation-id v2 policy; field-absent encounter-order v1 restore; swapped-SQL plan/state/result/restart equivalence; core 451/451, runtime 245/245, API 163/163; Pro `GO` | Yes | Compatibility gate complete |
| 2026-08-10 | Phase 2 general inner joins | Five exact non-primary join distributions; per-key output oracle; left/right checkpoint-state cardinality; corpus and benchmark-gate contracts; Pro `GO` | No SQL capability change | Local release benchmark and PR-smoke gate pass; p95 2.24/1.82/4.09/144.94/0.56 ms (descriptive, not SLO) |
| 2026-08-10 | Phase 3B bounded semi/anti join admission and recovery | Correlated complete non-null scalar-PK `EXISTS`/`NOT EXISTS` lower to generic semi/anti nodes; duplicate transitions, left update, two runtime restores, public REST restart; core 462/462, runtime 251/251, API 164/164; Pro `GO` | Yes | Phase 3B exit gate complete; nullable or broader subquery rewrites remain fail closed |

## Routine Verification

Run the focused correctness checks while completing checklist items:

```bash
cargo test -p velorix-core --test view_plan
cargo test -p velorix-runtime --test materialized_view_runtime
cargo test -p velorix-api
cargo bench -p velorix-runtime --bench local_incremental
```

Before changing a checkbox, also run the smallest new regression test that proves
the capability's failure before the implementation and success afterward.

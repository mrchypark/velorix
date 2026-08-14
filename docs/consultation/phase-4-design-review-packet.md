# Phase 4 Design Review Packet

## Title
[CODEX→PRO] Velorix — Phase 4: View-on-View Incremental Dependencies Design Review

## Goal
Review and validate the architectural design for Phase 4 (View-on-View Incremental Dependencies) of Velorix's incremental SQL pipeline. Identify any P0/P1 correctness, durability, or performance issues before implementation.

## Context

Velorix is a jarless materialized-view database/runtime that:
- Users register relations with explicit schemas
- Users ingest schema-bound rows into those relations
- Users define views over registered relations
- Supported views are admitted into the internal materialized view runtime
- Ingest updates the materialized output table automatically
- Queries read materialized output, not a full source recomputation
- Restart recovers from metadata and object/local storage checkpoints

Phase 4's goal is: "Let a materialized output act as a typed input to another standing view."

### Current Architecture (Foundation 0A/0B + Phases 1-3 Complete)

**Admission Pipeline** (`view_admission.rs`):
1. Feature gate → Read relation catalogs → Lower SQL to logical plan → Validate spec/types → Validate public plan → Build runtime binding → Begin bootstrap → Create published relation bindings → Build standing runtime → Register

**Key Structures**:
- `PublishedRelationBindingV1`: Immutable identity for consuming a materialized output. Has `producer_view_id`, `producer_view_generation`, `output_stream_id`, `delta_codec_identity`, `frontier_kind`.
- `CausalCutV1`: Versioned canonical cut with `direct_source_frontiers` and `direct_view_cursors: Vec<CausalViewCursorV1>`.
- `CausalViewCursorV1`: Contains `producer_view_id`, `producer_generation`, `output_stream`, `output_epoch`, `commit_digest`.
- `EpochCommit`: Contains `output_deltas: Vec<ViewOutputDelta>` (incremental changes per output).
- `RuntimeCheckpoint`: Contains `causal_cut: CausalCutV1` for frontier tracking.

**Runtime Processing** (`ingest_epoch.rs`):
- `apply_standing_runtime_prepared_ingests`: Flat loop over all active views, matching inputs from physical ingest streams only.
- Each view materialized independently, no dependency ordering.
- Producer view output deltas are never forwarded to consumer views.

**Bootstrap** (`view_bootstrap.rs`):
- `BeginViewBootstrapRequest.relations`: Only stores `IngestSourceRelationIdentityV1` (physical-source oriented).
- No tracking of view-produced input lineage.

### Identified Gaps for Phase 4

1. **No dependency graph**: No topological sort or cycle detection. Each view materialized independently.
2. **No inter-view input resolution**: Admission resolves inputs exclusively from physical relation catalogs.
3. **No delta forwarding**: Producer view output deltas never forwarded to consumer views.
4. **No coordinated epoch advancement**: Each view advances logical epoch independently.
5. **Bootstrap does not track view lineage**: Only physical source identities stored.
6. **Backfill replay is physical-source-only**: No multi-view backfill support.

## Constraints

1. **Fail-closed**: Unsupported shapes must fail during admission with clear error.
2. **No external dependencies**: Runtime must remain jarless and native.
3. **Deterministic replay**: Same input epochs must produce identical output.
4. **Checkpoint compatibility**: New state must be versioned and restorable.
5. **Foundation 0A contract**: Preserve signed input/output deltas, deterministic replay, checkpoint compatibility, and materialized-output query isolation.
6. **Foundation 0B contract**: Use existing native operator DAG and edge capabilities.

## Current State

### What Exists
- `PublishedRelationBindingV1` with complete schema/version/generation metadata
- `CausalCutV1` and `CausalViewCursorV1` for causal consistency tracking
- `EpochCommit.output_deltas` carrying incremental output per view
- `RuntimeCheckpoint.causal_cut` for frontier tracking
- `resolve_authoritative_direct_view_inputs` walking producer checkpoint lineage
- Single-view checkpoint/recovery complete

### What's Missing
- Dependency graph validation during admission
- Delta propagation from producer to consumer
- Frontier chaining across dependency chains
- Multi-level chain checkpoint/restore
- Bootstrap tracking for view-produced inputs

## Design Questions for Review

### 1. Dependency Graph Validation

**Proposed approach**:
- Add `validate_view_dependency_acyclicity()` in `view_admission.rs`
- Build graph from all active views' `input_relations` plus the new view
- Detect cycles using DFS with visited/stack tracking
- Validate topological ordering for execution scheduling
- Reject missing or unavailable dependencies

**Questions**:
- Should cycle detection happen at admission time or runtime?
- How should we handle views that reference non-existent producer views?
- Should we validate that producer views are in "Active" lifecycle state?

### 2. Delta Propagation

**Proposed approach**:
- After materializing each view, collect `EpochCommit.output_deltas`
- For downstream consumer views, inject producer's output delta as `RelationInputBatch`
- Topological sort ensures producers materialize before consumers

**Questions**:
- Should consumer views receive deltas as `DeltaBatch` (native format) or `RecordBatch` (Arrow format)?
- How should we handle the case where a consumer view has multiple producer inputs?
- Should we support partial materialization (only some producers advanced)?

### 3. Frontier Chaining

**Proposed approach**:
- For view-produced inputs, frontier is `(logical_epoch, content_hash)` not offset
- Extend `RelationFrontier` or add `ViewRelationFrontier` variant
- Consumer's frontier advances to match producer's committed epoch

**Questions**:
- Should we use the existing `CausalViewCursorV1` for frontier chaining?
- How should we handle frontier advancement when only some producers advance?
- Should we expose view-produced frontiers in the public query path?

### 4. Checkpoint/Restore for Multi-View Chains

**Proposed approach**:
- Extend `BeginViewBootstrapRequest` with `view_relations: Vec<ViewSourceRelationIdentityV1>`
- Extend `ViewBootstrapControlV1` with `view_bootstrap_cut`
- For multi-hop chains, recursive resolution of producer checkpoint lineage

**Questions**:
- How deep should we support dependency chains (max hops)?
- Should we cache producer checkpoint lineage for performance?
- How should we handle chain recovery when a producer view is deleted?

### 5. Exit Gate Test

**Proposed approach**:
- Three-level filter → aggregate → Top-K chain
- Test across insert, retract, restart, replay
- Verify exact output deltas at each level

**Questions**:
- What's the minimum viable chain depth for Phase 4?
- Should we test with signed deltas (positive/negative weights)?
- Should we test concurrent ingest during chain materialization?

## Recommended Implementation Order

1. **Admission path changes** (dependency graph validation, input resolution)
2. **Runtime changes** (delta propagation, epoch coordination)
3. **Checkpoint/restore changes** (bootstrap lineage, multi-view recovery)
4. **Exit gate test** (three-level chain verification)

## Evidence Requirements

Per the gap-closure plan, Phase 4 requires:
- [ ] Dependency graph validation tests
- [ ] Delta propagation tests
- [ ] Frontier chaining tests
- [ ] Multi-level chain checkpoint/restore tests
- [ ] Three-level exit gate test

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Deep dependency chains cause performance degradation | Medium | Set max hop limit, cache producer lineage |
| Concurrent ingest during chain materialization | High | Serialize chain materialization per epoch |
| Producer view deletion while consumer active | High | Fail closed, require explicit consumer deletion first |
| Checkpoint size grows with chain depth | Low | Use causal cut compression, lazy lineage resolution |

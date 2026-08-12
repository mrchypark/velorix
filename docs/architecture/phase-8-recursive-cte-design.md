# Phase 8 — Recursive CTEs (Design)

Status: DESIGN — implementation gated on this document.

## 1. Semantic design

Admitted grammar (MVP):
```sql
WITH RECURSIVE r AS (
    anchor_query
    UNION DISTINCT
    recursive_term
)
SELECT ... FROM r ...
```
Hard admission constraints:
- Exactly one self-reference; `UNION ALL` rejected (only DISTINCT fixpoint).
- Recursive term restricted to positive filter/project/equi-join over the
  recursion relation and registered base relations; aggregation, window
  functions, negation, anti-join, outer join, and recursive scalar
  arithmetic that creates new domain values are rejected. Recursion may
  only select/combine values from the finite active domain.
- Bounds: `RecursiveFixpointContractV1 { max_iterations, max_derived_rows,
  max_work_units_per_epoch }`; exceeding any bound rejects the epoch.

Lowering: `VelorixLogicalViewExecutionV1::RecursiveFixpointV1` (append-only
variant).

## 2. Worst-case state

`RecursiveFixpointStateV1 { base_multiset: CanonicalRelationStateV1, derived_set: CanonicalRelationSetV1 }`.
The derived set is bounded by the finite active domain (positive
selection-only recursion) and by `max_derived_rows`. Worst case O(domain
size). Work per epoch is bounded by `max_work_units_per_epoch`.

## 3. Retraction algorithm

Each epoch: (1) apply signed base deltas, (2) recompute the anchor, (3)
run the seminaive positive fixpoint, (4) diff the old and new derived
sets, (5) emit signed materialized deltas. Retractions are exact because
the closure is recomputed from the updated base state — full deterministic
recomputation, no incremental provenance (DRed is a later optimization).

This is ingest-time materialization (not query-time recomputation), so the
"queries read materialized output" invariant holds.

## 4. Replay determinism

The fixpoint is a pure function of the base multiset; iteration order is
canonical (sorted sets), and bounds are deterministic. The same signed
input sequence produces identical derived sets, deltas, and state.
Termination is guaranteed by the finite active domain plus
`max_iterations`.

## 5. Checkpoint schema

`RecursiveFixpointCheckpointPayloadV2` with `state_encoding_version`: plan
(anchor/recursive term lowered to relational sub-plans), catalogs, schemas,
view SQL, canonical base multiset and derived set, frontiers, applied
epochs, logical epoch, iteration counters.

## 6. Benchmark threshold

`cargo bench -p velorix-runtime -- recursive_fixpoint`: transitive closure
over 100k edges must materialize within 1s and incrementally update a 1k-
edge batch within 100ms; work-unit accounting must reject epochs that
would exceed the budget rather than degrade.

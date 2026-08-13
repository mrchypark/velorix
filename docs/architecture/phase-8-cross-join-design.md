# Phase 8 — CROSS JOIN (Design)

Status: IMPLEMENTED — `SupportedCrossJoinPlanV1` + `CrossJoinRuntime`.

## 1. Semantic design

Admitted grammar (MVP):
```sql
SELECT <projection> FROM left CROSS JOIN right
```
Hard admission constraints:
- Bare `CROSS JOIN` only; `ON`/`USING` constraints, `WHERE`, `GROUP BY`,
  `DISTINCT`, and aggregates are rejected at admission.
- The projection is a plain list of direct columns referencing one of the
  two joined table aliases and must include both the left and the right
  primary key columns, so output rows are unique per (left row, right row)
  pair and bag semantics are preserved.
- `CrossJoinResourceContractV1 { max_rows_per_side, max_pairs_per_epoch }`;
  exceeding either bound rejects the epoch (atomic).

Lowering: `VelorixLogicalViewExecutionV1::CrossJoin` with
`SupportedCrossJoinPlanV1`.

## 2. Worst-case state

`CrossJoinStateV1 { left: CanonicalRelationStateV1, right: CanonicalRelationStateV1 }`.
State is the union of both sides' multisets: O(|L| + |R|). Per-epoch output
is the full pair set, bounded by `max_pairs_per_epoch`.

## 3. Retraction algorithm

Each epoch: (1) apply signed side deltas, (2) recompute the full pair set
from the post-epoch states, (3) diff against the previous published output,
(4) emit signed deltas. Retractions are exact because pairs are recomputed
from state; no incremental provenance is kept.

## 4. Replay determinism

The pair set is a pure function of the two multisets; iteration order is
canonical (sorted keys) and bounds are deterministic. The same signed input
sequence produces identical outputs, deltas, and state.

## 5. Checkpoint schema

`CrossJoinCheckpointPayloadV2` with plan, catalogs, schemas, view SQL,
sorted side multisets, frontiers, applied epochs, logical epoch.

## 6. Benchmark threshold

`cargo bench -p velorix-runtime --bench local_incremental`:
`cross_join_epoch_apply` over 160k pairs per epoch must stay within the PR
smoke baseline plus the 25% regression budget.

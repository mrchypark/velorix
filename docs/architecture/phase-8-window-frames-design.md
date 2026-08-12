# Phase 8 — Window Frames and Navigation Functions (Design)

Status: DESIGN — implementation gated on this document.

## 1. Semantic design

Admitted scope (MVP, exact-only):
- `ROWS BETWEEN <bounded> PRECEDING AND <bounded> FOLLOWING` frames only.
  `UNBOUNDED PRECEDING/FOLLOWING` and `RANGE`/`GROUPS` frames are rejected at
  admission (a single retraction could invert the whole partition output).
- Navigation: `LAG(expr, constant_offset)`, `LEAD(expr, constant_offset)`,
  `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE(constant_n)`.
- `IGNORE NULLS` is rejected; offsets and N must be compile-time constants.
- Required: non-null primary key (deterministic tie-breaker), exactly one
  partition column, exactly one non-null sortable order column, explicit
  `NULLS FIRST/LAST` (order column is non-null, so the clause is inert but
  must be present for explicitness).
- The sort key is `(order value, primary key)` — a total order.

Lowering: `VelorixLogicalViewExecutionV1::AnalyticWindowFramesV1` (append-only
enum variant), plan carries the partition/order/frame/navigation spec. The
runtime reuses the analytic row-number partition machinery for state
partitioning.

## 2. Worst-case state

`OrderedPartitionStateV1`: per partition a BTreeMap keyed by canonical sort
key, holding the admitted rows. Worst case O(active rows per partition); a
bounded frame means one insert/retract changes output for at most
`frame_width` rows of its partition. Enforced budget:
`max_affected_rows_per_epoch` (exceed → epoch reject), `max_rows_per_partition`.

## 3. Retraction algorithm

- Insert/retract: apply the signed row to the partition's ordered multiset,
  recompute the frame-scoped outputs for the bounded neighborhood of the
  changed sort position, emit signed deltas for the affected output rows.
- The neighborhood is bounded because the frame is bounded; output for rows
  outside the neighborhood is provably unchanged (their frame membership is
  determined by positions that did not change).

## 4. Replay determinism

All decisions derive from the admitted rows and their canonical sort order
only: no wall clock, no iteration order, no environment. The same signed
input sequence produces identical frame outputs, deltas, and checkpoint
state. Program identity includes the frame spec and the expression program
hash.

## 5. Checkpoint schema

`AnalyticWindowFramesCheckpointPayloadV2` with `state_encoding_version: u16`:
catalog, schemas, view SQL, plan, per-partition sorted logical rows
(canonically sorted — never BTree iteration order), frontiers, applied
epochs, logical epoch. Restore validates identity and recompiles the plan;
unknown versions fail closed.

## 6. Benchmark threshold

`cargo bench -p velorix-runtime -- analytic_frames` must sustain
100k-row partitions with frame width 100 under 500ms/epoch and
`max_affected_rows_per_epoch` guard active; above threshold the guard
rejects the epoch instead of degrading.

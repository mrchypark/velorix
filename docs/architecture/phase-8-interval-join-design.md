# Phase 8 — Interval Joins (Non-Equi) (Design)

Status: DESIGN — implementation gated on this document.

## 1. Semantic design

Admitted scope (MVP):
- INNER JOIN only with an interval-overlap predicate:
  `left.start < right.end AND right.start < left.end`.
- Endpoints: non-null `TimestampNanosecond` (or Int64 nanoseconds) columns;
  `start < end` required per row; maximum interval duration required at
  admission; event-time watermark required on every input batch.
- All other non-equi predicates (`<`, `>`, arbitrary residual) and outer
  interval joins are rejected at admission.

Lowering: `VelorixLogicalViewExecutionV1::IntervalJoinV1` (append-only
variant) with `SupportedIntervalJoinPlanV1`.

## 2. Worst-case state

`IntervalJoinStateV1 { left: CanonicalIntervalIndexV1, right: CanonicalIntervalIndexV1, watermarks }`.
State holds both interval sets; worst case O(active intervals per side).
Eviction: an interval may be evicted only when the opposite side's
watermark plus the max allowed lateness and max interval span prove no
future overlap is possible. Early eviction is forbidden — a retraction of
an evicted match would be irrecoverable.

## 3. Retraction algorithm

- Insert: query the opposite index for overlaps, emit signed positive
  outputs for each match.
- Retract: re-query the opposite index with the retracted interval and emit
  signed negative outputs for every match (same query, negated weights).
- Overlap queries must be symmetric; the join predicate is commutative, so
  the left/right emission order is canonical (left interval, then right
  interval) to keep deltas deterministic.

## 4. Replay determinism

All matching is a function of the interval sets and their canonical
endpoints; watermarks only gate admission and eviction, and are monotonic
and persisted. The same signed input sequence produces identical match
sets, deltas, and state. Late rows (event time behind the watermark) fail
closed under the strict policy, which guarantees no eviction can be
retracted later.

## 5. Checkpoint schema

`IntervalJoinCheckpointPayloadV2` with `state_encoding_version`: plan,
catalogs, schemas, view SQL, sorted canonical interval indices, watermarks,
frontiers, applied epochs, logical epoch, eviction frontier.

## 6. Benchmark threshold

`cargo bench -p velorix-runtime -- interval_join`: 1M intervals per side,
average overlap 10, must sustain 500ms/epoch; eviction correctness is
enforced by the eviction-frontier proof and tested with retraction-after-
watermark-advance workloads.

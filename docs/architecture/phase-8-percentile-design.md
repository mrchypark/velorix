# Phase 8 — Exact Percentile and Median Aggregates (Design)

Status: DESIGN — implementation gated on this document.

## 1. Semantic design

Admitted scope (exact, not approximate):
- `PERCENTILE_DISC(p)`: Int64 and Decimal128 inputs; p is a compile-time
  literal in [0, 1].
- `PERCENTILE_CONT(p)`: Decimal128 inputs only (interpolation must stay
  exact; Float64 interpolation would be approximate and is rejected).
- `MEDIAN` is an explicit alias for `PERCENTILE_CONT(0.5)`.
- Aggregates appear in the supported single-key aggregate family with the
  same GROUP BY / filter / HAVING / Top-K shape as SUM/COUNT.
- Ordered-set syntax (`WITHIN GROUP`) is not supported; the ordering is the
  value's canonical order (no user ORDER BY inside the aggregate).

State: `ExactOrderStatisticStateV1 { counts: BTreeMap<CanonicalNumericValueV1, u64>, total_count: u64 }`
— a multiplicity multiset, checkpointed as sorted `(value, multiplicity)`
pairs.

## 2. Worst-case state

State grows with the number of distinct values per group (O(distinct)).
Budget: `max_distinct_values_per_group`, `max_rows_per_group`; exceeding the
budget rejects the epoch. Rank selection is O(distinct) via BTreeMap
iteration, acceptable under the caps; a benchmark threshold guards the
transition to an order-statistic tree.

## 3. Retraction algorithm

- Insert/retract: adjust `counts[value]` by the signed weight (removing the
  entry at zero) and `total_count` by the signed weight. DISK: pick the
  rank k = ceil(p * total) (DISC) or interpolate between the two bracketing
  values (CONT) after each update. Exactness follows from maintaining the
  full multiplicity multiset; no sampling, no merging error.

## 4. Replay determinism

The multiset is a pure function of the signed input sequence. Value
comparisons use the canonical decimal encoding (unscaled + scale), so
equality, ordering, and hashing are deterministic across restarts and
toolchain versions.

## 5. Checkpoint schema

`OrderStatisticCheckpointPayloadV2` with `state_encoding_version`: plan,
catalog, schemas, view SQL, sorted `(value, multiplicity)` state,
frontiers, applied epochs, logical epoch. PERCENTILE_CONT interpolation
uses wider intermediate arithmetic and checked conversion back to
Decimal128; overflow fails closed.

## 6. Benchmark threshold

`cargo bench -p velorix-runtime -- percentile`: 1M rows / 10k distinct per
group must compute within 200ms/epoch; exceeding the O(distinct) rank
selection threshold triggers the order-statistic tree requirement.

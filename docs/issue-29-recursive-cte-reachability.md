# Issue #29: recursive CTE reachability

The admitted recursive grammar validates one or two CTE definitions, but the
current SQL surface exposes only the first CTE: its recursive term is
self-referential and the outer query selects the first CTE. The second CTE is
admission-validated but is not executed; arbitrary historical two-CTE plans are
not guaranteed compatible.

The runtime computes the first CTE's anchor and recursive closure only. Restore
recomputes that closure from the persisted base multiset and rejects a
checkpoint whose derived set or published output differs. This preserves valid
single-CTE checkpoints and fails closed on checkpoints whose persisted rows were
contaminated by the former merged-CTE behavior. A future grammar that exposes
genuine second-CTE output or backward dependencies must introduce independent
per-CTE state and frontiers.

## Current API evidence

The API regression uses this concrete shape (the first CTE is the only outer
source):

```sql
WITH RECURSIVE fwd AS (
  SELECT src, dst FROM edges
  UNION DISTINCT
  SELECT r.src, e.dst FROM fwd r JOIN edges e ON r.dst = e.src
), bwd AS (
  SELECT dst AS src, src AS dst FROM edges WHERE src = 'isolated'
  UNION DISTINCT
  SELECT r.src, e.dst FROM bwd r JOIN edges e ON r.dst = e.src
)
SELECT src, dst FROM fwd
```

For `a → b`, `b → c`, and `isolated → leaf`, the independent oracle is exactly
`a → b`, `a → c`, `b → c`, and `isolated → leaf`; the disjoint second-CTE row
`leaf → isolated` must not appear. The API evidence is
`rest_recursive_cte_outer_first_ignores_disjoint_second_cte_across_restore`,
which passed locally, including fresh API-state restore. Runtime coverage also
includes `recursive_cte_materializes_closure_exactly_across_retract_restart_and_fail_closed`
and `recursive_cte_ignores_unreachable_second_cte`.

The focused runtime fixture also supplies the boundary counts: adding `c → a`
to the `a → b → c` chain yields the nine ordered pairs of the three-node cycle;
retracting that edge returns the three-row closure, followed by restart and
reinsertion checks. This is covered by
`recursive_cte_ignores_unreachable_second_cte` and the adjacent
`recursive_cte_restore_rejects_unreachable_state_contamination` test.

The compatibility boundary is narrow: valid single-CTE checkpoints remain
supported, while checkpoints contaminated by the former merged-CTE behavior
are rejected. This does not reject every valid old two-CTE checkpoint and does
not promise future admission compatibility for arbitrary old shapes.

# Interval join admission semantics

The interval-join planner admits only a strict two-bound overlap predicate:

```sql
left.start < right.end AND left.end > right.start
```

Operands may be written in the opposite relation order; normalization swaps
the references and inverts `<`/`>` together. Endpoint identifiers must use the
existing `_start`/`_end` suffixes, including `_start_time`/`_end_time` forms.
The operator direction is part of the proof and is never inferred from names
alone. The two joined inputs must have distinct aliases, and each endpoint is
checked against the weight column of its own relation.

`WHERE`, `ORDER BY`, extra `ON` conditions, duplicate aliases, reversed
non-overlap predicates, and other unsupported shapes fail closed during
admission. This may cause a previously (incorrectly) admitted view to be
rejected on re-admission; no automatic migration claim is made.

Focused coverage lives in `crates/velorix-core/tests/view_plan.rs` and includes
equivalent operand/conjunct permutations, reversed predicates, unsupported
outer conditions, alias validation, endpoint naming compatibility, and
asymmetric weight-column checks.

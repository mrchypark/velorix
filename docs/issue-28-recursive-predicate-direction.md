# Recursive predicate direction

Issue-level planner note. This describes a narrow admitted normalization and
does not imply general recursive SQL support or production qualification.
Canonical current status belongs in [specification.md](specification.md).

Recursive CTE base predicates normalize literal-left comparisons before they
become part of the logical fixpoint plan. The validator preserves `=` and
`<>`, and inverts only directional operators: `<`/`>` and `<=`/`>=`. Thus
`base.score < 5` and `5 > base.score` compile to the same predicate contract
for both anchor and recursive terms.

Predicates that are not supported by the existing recursive admission contract
still fail closed. Existing incorrectly compiled plans are not automatically
migrated; re-admission must produce the corrected plan. Runtime and checkpoint
shapes remain unchanged.

Implementation is in [`view_plan/mod.rs`](../crates/velorix-core/src/view_plan/mod.rs).
Focused normalization coverage is in
[`view_plan.rs`](../crates/velorix-core/tests/view_plan.rs), test
`recursive_base_predicates_normalize_literal_left_comparisons`.

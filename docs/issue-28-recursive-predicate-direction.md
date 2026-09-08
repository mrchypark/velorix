# Recursive predicate direction

Recursive CTE base predicates normalize literal-left comparisons before they
become part of the logical fixpoint plan. The validator preserves `=` and
`<>`, and inverts only directional operators: `<`/`>` and `<=`/`>=`. Thus
`base.score < 5` and `5 > base.score` compile to the same predicate contract
for both anchor and recursive terms.

Predicates that are not supported by the existing recursive admission contract
still fail closed. Existing incorrectly compiled plans are not automatically
migrated; re-admission must produce the corrected plan. Runtime and checkpoint
shapes remain unchanged.

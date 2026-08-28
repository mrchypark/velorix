## Generic Query and View Support

Velorix is a jarless materialized view database/runtime. Do not add external
compiler, runtime build/deploy, package-loading, or image-based execution paths
for view creation.

The target product flow is:

- users register relations with explicit schemas
- users ingest schema-bound rows into those relations
- users define views over registered relations
- supported views are admitted into the internal materialized view runtime
- ingest updates the materialized output table automatically
- queries read materialized output, not a full source recomputation
- restart recovers from metadata and object/local storage checkpoints

Completion for generic query/view support requires evidence that the internal
runtime handles multiple relation schemas and more than one SQL family. At
minimum, verify filters, projections, group by, sum/count/min/max/avg, and a
two-table join through the same admission and runtime path. Unsupported SQL or
view shapes must fail closed during admission with a clear error.

Do not expand Velorix-owned SQL support by silently adding fake fallbacks. If a
SQL family is unsupported, return an admission error and implement the internal
runtime capability deliberately.

## CI and Pre-Push Workflow

CI runs on GitHub Actions (`CI` workflow) and enforces `-D warnings` on clippy
and `cargo fmt --check`. Pushing a broken commit blocks the branch until fixed.

**Before pushing, always run locally:**

1. `cargo fmt` — fix formatting
2. `cargo clippy --workspace --all-targets -- -D warnings` — must pass clean
3. `cargo test -p velorix-runtime --lib` — quick sanity (3 tests, ~1s)
4. `cargo test -p velorix-runtime --test materialized_view_runtime` — full
   runtime integration (223 tests, ~30s on local)

If any of these fail, fix before pushing. Do not rely on CI to catch issues
after the fact.

**CI timing:** The CI workflow takes ~5-20 minutes depending on cache state.
Full workspace build from cold cache is ~3 minutes; tests add ~1-2 minutes.
Rust incremental compilation helps locally but CI runs with `CARGO_INCREMENTAL=0`.

**Nightly Benchmark Gate** runs on schedule (not per-push) and validates
performance invariants (rows/s, bytes/row, S3 operation counts, RSS, spill).
Failures here indicate performance regressions, not correctness issues.

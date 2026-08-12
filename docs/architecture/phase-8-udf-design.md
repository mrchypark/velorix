# Phase 8 — Deterministic Built-in UDF Registry (Design)

Status: DESIGN — implementation gated on this document.

## 1. Semantic design

"UDF" in Velorix means a **deterministic built-in extension registry**, not
user-code execution:
- `BuiltinUdfIdentityV1 { namespace, name, semantic_version, implementation_digest }`.
  The implementation digest is computed over the compiled function
  definition at build time and checked at admission and restore.
- The registry is compiled into the binary; there is no WASM/JAR/shared
  object loading, no network, no IO, no time/random/environment access.
- First scope: stateless scalar functions invoked from the typed expression
  IR: `TypedExprKindV1::Call { function: BuiltinUdfFunctionV1, .. }` where
  `BuiltinUdfFunctionV1` carries a resolved `BuiltinUdfIdentityV1` and the
  typed arguments/result.
- Unknown namespace/name, wrong semantic version, or wrong implementation
  digest fail closed at admission and restore.

## 2. Worst-case state

Stateless scalar UDFs hold no operator state. State impact is limited to
the expression evaluation stack (bounded by expression depth, capped at
admission). No checkpoint state.

## 3. Retraction algorithm

N/A for the function itself: UDF outputs are recomputed deterministically
from their typed arguments, so signed input retractions flow through the
host operator's existing retraction path unchanged.

## 4. Replay determinism

Determinism is enforced structurally: pure functions, fixed input types,
and a pinned implementation digest included in the program identity hash.
A toolchain upgrade that changes a function's observable behavior changes
the digest, invalidating stored checkpoints (fail closed) rather than
silently changing replay results.

## 5. Checkpoint schema

The plan and the checkpoint payload carry only the resolved UDF identity
(`BuiltinUdfIdentityV1`) plus the typed expression program; there is no
per-UDF runtime state. Restore verifies the identity against the registry
and fails closed on any mismatch.

## 6. Benchmark threshold

`cargo bench -p velorix-runtime -- builtin_udf`: 1M rows through a
registry function must sustain 100M calls/sec admission-bench and 1M
rows/epoch under 100ms in the runtime; a UDF slower than the same
expression inlined in the typed IR is rejected at the benchmark gate.

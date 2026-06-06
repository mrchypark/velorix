# Feldera Package-First Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Velorix's implementation toward the package-first Feldera runtime design by introducing a standing-program runtime contract and guardrails that prevent new product code from becoming a local SQL planner or DBSP operator implementation.

**Architecture:** Add a `velorix-core::standing_program` boundary beside the existing bootstrap `IncrementalEngine` trait. Keep the existing sum/count DBSP implementation as a quarantined spike, and add source-contract tests that make the production standing-view path depend on Feldera-owned compilation/runtime boundaries rather than DataFusion SQL shape parsing or Velorix-owned IR lowering.

**Tech Stack:** Rust workspace, `velorix-core`, Arrow `RecordBatch`, serde, existing `StandingViewSpec`/relation schema contracts, existing source-contract test style.

---

### Task 1: Core Standing Program Runtime Contract

**Files:**
- Create: `crates/velorix-core/src/standing_program.rs`
- Modify: `crates/velorix-core/src/lib.rs`
- Test: `crates/velorix-core/tests/standing_program_runtime.rs`

- [ ] **Step 1: Write failing tests**

Add tests that require:

- `StandingProgramIdentity::validate()` rejects missing program/catalog/schema/runtime/checkpoint identity.
- native code and external dependencies are disabled in the first product path.
- `RuntimeCheckpoint::validate_identity()` fails closed when the checkpoint identity does not match the runtime identity.
- a fake `StandingProgramRuntime` can apply a relation-scoped epoch and produce a view-scoped commit without using `DeltaBatch` as the public standing-program boundary.

Run:

```bash
cargo test -p velorix-core --test standing_program_runtime -- --nocapture
```

Expected: compile failure because `velorix_core::standing_program` does not exist.

- [ ] **Step 2: Implement the minimal boundary**

Create the `standing_program` module with:

- `StandingProgramIdentity`
- `FelderaRuntimePackageIdentity`
- `NativeCodePolicy`
- `RelationInputBatch`
- `ViewOutputBatch`
- `EpochCommit`
- `RuntimeCheckpoint`
- `RelationFrontier`
- `ViewFrontier`
- `DurableStateRoot`
- `StandingProgramRuntimeError`
- `StandingProgramRuntime`

The contract uses Arrow `RecordBatch` for typed input/output batches and keeps logical epoch, program identity, frontiers, checkpoint codec identity, and durable state root explicit.

- [ ] **Step 3: Verify green**

Run:

```bash
cargo test -p velorix-core --test standing_program_runtime -- --nocapture
```

Expected: all tests pass.

### Task 2: Architecture Guardrails

**Files:**
- Create: `crates/velorix-core/tests/standing_program_source_contract.rs`

- [ ] **Step 1: Write failing source-contract tests**

Add tests that scan production source and assert:

- only `crates/velorix-core/src/dbsp_view_plan.rs` and its tests may define/use `validate_supported_dbsp_view_sql`;
- production standing-program code must not import DataFusion SQL parser/planner modules;
- production standing-program code must not import `feldera_ir::{Op, MirNode, LirNode}` for execution;
- `DbspSingleKeySumCountEngine` remains gated behind `dbsp-spike`.

Run:

```bash
cargo test -p velorix-core --test standing_program_source_contract -- --nocapture
```

Expected: initial failure if any production path still treats the legacy SQL shape validator as the standing-program path.

- [ ] **Step 2: Implement guard allowlists**

Keep allowlists narrow:

- allow `dbsp_view_plan.rs` as legacy bootstrap guard;
- allow `dbsp_engine.rs` only because it is behind `dbsp-spike`;
- forbid future `standing_program` modules from importing DataFusion SQL parser/planner or matching Feldera IR nodes.

- [ ] **Step 3: Verify green**

Run:

```bash
cargo test -p velorix-core --test standing_program_source_contract -- --nocapture
```

Expected: all tests pass.

### Task 3: API Static Artifact Demotion

**Files:**
- Modify: `crates/velorix-api/src/lib.rs`
- Test: `crates/velorix-api/tests/rest_product.rs`

- [ ] **Step 1: Write failing API test**

Add a test that `POST /v1/views` returns an explicit field or status showing artifact-backed creation is the static release-bound path, not the package-first standing-program path.

Run:

```bash
cargo test -p velorix-api --test rest_product rest_product_creates_artifact_backed_view_without_dbsp_sql_shape_gate -- --nocapture
```

Expected: failure until the response exposes the static path classification.

- [ ] **Step 2: Implement response classification**

Extend the artifact response with an execution path value such as `static_release_artifact`.

- [ ] **Step 3: Verify green**

Run the focused API test and then the full `rest_product` test.

### Task 4: Package Compatibility Gate Scaffolding

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/velorix-core/Cargo.toml`
- Test: `crates/velorix-core/tests/standing_program_source_contract.rs`

- [ ] **Step 1: Add a non-default feature plan gate**

Add a feature name that communicates intent, for example `feldera-package-compat`, but do not enable it in default workspace builds until the Rust `1.93.1` MSRV decision is explicit.

- [ ] **Step 2: Add source-contract coverage**

Assert the feature is not part of default features and that `dbsp-spike` remains explicitly named as a spike.

- [ ] **Step 3: Verify**

Run:

```bash
cargo test -p velorix-core --test standing_program_source_contract -- --nocapture
cargo check --workspace --all-targets
```

Expected: default workspace remains green without promoting Feldera `0.299.x` packages into the release path.

### Task 5: Follow-On Runtime Adapter

This task is intentionally not started until Tasks 1-4 are green.

The next implementation plan must prove executable creation through a Feldera-owned runtime mechanism. Descriptor parsing alone is not enough. The first behavior test must use SQL with a filter or join, compare output against a Feldera server/reference adapter, and prove no Velorix code lowers SQL/Feldera IR into DBSP operators.

# Velorix Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal Velorix prototype that proves an object-storage-first, stateless, incremental streaming database can ingest deltas, maintain a materialized view, checkpoint progress, and recover without local durable state.

**Architecture:** Start with a single-process prototype whose only durable layer is an object-store abstraction backed by the local filesystem. Represent input, state, and checkpoints as immutable objects plus versioned manifests, then add incremental operators and recovery semantics before scaling workers.

**Tech Stack:** Rust is the assumed implementation language for the core engine because Velorix needs low overhead, predictable performance, async I/O, and deployable static binaries. Use `tokio` for async execution, `serde` for schemas, `object_store` for filesystem/S3-compatible storage, and `proptest` for invariant-heavy delta and manifest tests.

---

## File Structure

- Create: `Cargo.toml` for the Rust workspace and shared dependency versions.
- Create: `crates/velorix-core/src/lib.rs` for the public core module boundary.
- Create: `crates/velorix-core/src/delta.rs` for signed delta records and batches.
- Create: `crates/velorix-core/src/operator.rs` for map, filter, join, and aggregate traits.
- Create: `crates/velorix-storage/src/lib.rs` for the object-store-facing storage boundary.
- Create: `crates/velorix-storage/src/object_key.rs` for deterministic object key construction.
- Create: `crates/velorix-storage/src/manifest.rs` for checkpoint manifest schemas and validation.
- Create: `crates/velorix-runtime/src/lib.rs` for stateless worker orchestration.
- Create: `crates/velorix-runtime/src/recovery.rs` for manifest-based recovery.
- Create: `crates/velorix-cli/src/main.rs` for local prototype commands.
- Create: `tests/e2e/local_recovery.rs` for crash and recovery behavior.
- Modify: `README.md` as implementation status changes.

## Task 1: Workspace Scaffold

**Files:**
- Create: `Cargo.toml`
- Create: `crates/velorix-core/src/lib.rs`
- Create: `crates/velorix-storage/src/lib.rs`
- Create: `crates/velorix-runtime/src/lib.rs`
- Create: `crates/velorix-cli/src/main.rs`

- [ ] **Step 1: Create the workspace manifest**

```toml
[workspace]
members = [
  "crates/velorix-core",
  "crates/velorix-storage",
  "crates/velorix-runtime",
  "crates/velorix-cli",
]
resolver = "2"

[workspace.dependencies]
anyhow = "1"
async-trait = "0.1"
bytes = "1"
clap = { version = "4", features = ["derive"] }
object_store = "0.12"
proptest = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tempfile = "3"
thiserror = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
uuid = { version = "1", features = ["v7", "serde"] }
```

- [ ] **Step 2: Add minimal crate manifests and empty module boundaries**

Each crate should compile with a minimal `lib.rs` or `main.rs` that exposes the intended boundary without implementing runtime behavior yet.

- [ ] **Step 3: Verify the scaffold**

Run: `cargo test --workspace`

Expected: the workspace compiles and reports zero or minimal passing tests.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates
git commit -m "chore: scaffold velorix workspace"
```

## Task 2: Delta Data Model

**Files:**
- Create: `crates/velorix-core/src/delta.rs`
- Modify: `crates/velorix-core/src/lib.rs`

- [ ] **Step 1: Write tests for signed delta semantics**

Add tests that prove positive weights insert logical rows, negative weights retract them, and combining batches preserves net row weight.

- [ ] **Step 2: Implement the minimal delta model**

Represent records as typed keys and values with a signed integer weight. Keep the first version intentionally simple: JSON-compatible values are acceptable until the execution format is finalized.

- [ ] **Step 3: Add property-based tests**

Use `proptest` to check that batch combination is associative and that combining a batch with its inverse produces an empty net result.

- [ ] **Step 4: Verify**

Run: `cargo test -p velorix-core delta`

Expected: delta example tests and property tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/velorix-core
git commit -m "feat: add signed delta model"
```

## Task 3: Object Storage Contract

**Files:**
- Create: `crates/velorix-storage/src/object_key.rs`
- Create: `crates/velorix-storage/src/manifest.rs`
- Modify: `crates/velorix-storage/src/lib.rs`

- [ ] **Step 1: Write tests for deterministic object keys**

Cover ingest batch keys, state object keys, temporary publish keys, and checkpoint manifest keys. Object keys must be stable across process restarts.

- [ ] **Step 2: Write manifest validation tests**

Cover valid manifests, missing input progress, missing state object references, duplicate object identifiers, and non-monotonic checkpoint versions.

- [ ] **Step 3: Implement object key constructors**

Use structured constructors instead of string formatting at call sites. Keep all object key layout decisions in `object_key.rs`.

- [ ] **Step 4: Implement checkpoint manifest schemas**

The first manifest schema must include checkpoint version, input ranges, state object references, output object references, parent checkpoint, and creation timestamp.

- [ ] **Step 5: Verify**

Run: `cargo test -p velorix-storage`

Expected: key and manifest tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/velorix-storage
git commit -m "feat: define object storage contract"
```

## Task 4: Ingest Log and Replay

**Files:**
- Create: `crates/velorix-storage/src/log.rs`
- Modify: `crates/velorix-storage/src/lib.rs`
- Create: `crates/velorix-storage/tests/log_replay.rs`

- [ ] **Step 1: Write integration tests with a temporary filesystem object store**

Test appending ordered delta batches, listing committed batches, and replaying from a checkpoint boundary.

- [ ] **Step 2: Implement append-only log writes**

Persist batches as immutable objects. Treat overwrite attempts as errors.

- [ ] **Step 3: Implement replay reads**

Replay must return batches in deterministic order and skip batches already covered by a checkpoint.

- [ ] **Step 4: Verify**

Run: `cargo test -p velorix-storage log_replay`

Expected: temporary filesystem-backed replay tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/velorix-storage
git commit -m "feat: add object-backed ingest log"
```

## Task 5: First Incremental Operators

**Files:**
- Create: `crates/velorix-core/src/operator.rs`
- Modify: `crates/velorix-core/src/lib.rs`
- Create: `crates/velorix-core/tests/operators.rs`

- [ ] **Step 1: Write behavior tests for map, filter, join, and aggregate**

Assert on output deltas and materialized state, not internal calls.

- [ ] **Step 2: Implement map and filter**

Map and filter should transform input deltas without requiring persisted state.

- [ ] **Step 3: Implement join and aggregate**

Join and aggregate may maintain in-memory state during a worker run, but durable recovery must come from checkpointed state objects in later tasks.

- [ ] **Step 4: Verify**

Run: `cargo test -p velorix-core operators`

Expected: operator behavior tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/velorix-core
git commit -m "feat: add initial incremental operators"
```

## Task 6: State Objects and Checkpoints

**Files:**
- Create: `crates/velorix-storage/src/state.rs`
- Modify: `crates/velorix-storage/src/manifest.rs`
- Create: `crates/velorix-storage/tests/checkpoint_publish.rs`

- [ ] **Step 1: Write checkpoint publication tests**

Cover successful publication, crash before manifest publication, crash after state write but before manifest publication, and duplicate checkpoint publication.

- [ ] **Step 2: Implement immutable state object writes**

Write state objects before publishing the manifest. State objects without a manifest reference are recoverable garbage.

- [ ] **Step 3: Implement manifest publication**

Publish manifests as the only authoritative checkpoint marker. Use object-store conditional writes where available and emulate the condition in local tests.

- [ ] **Step 4: Verify**

Run: `cargo test -p velorix-storage checkpoint_publish`

Expected: crash-window tests prove that only published manifests advance durable progress.

- [ ] **Step 5: Commit**

```bash
git add crates/velorix-storage
git commit -m "feat: publish checkpointed state manifests"
```

## Task 7: Stateless Runtime Recovery

**Files:**
- Create: `crates/velorix-runtime/src/recovery.rs`
- Modify: `crates/velorix-runtime/src/lib.rs`
- Create: `tests/e2e/local_recovery.rs`

- [ ] **Step 1: Write end-to-end recovery test**

Use a temporary object-store directory. Ingest deltas, process a view, publish a checkpoint, drop the runtime instance, create a fresh runtime instance, and verify the materialized view recovers from object storage only.

- [ ] **Step 2: Implement runtime startup from manifest**

Startup should load the latest manifest, fetch referenced state objects, and resume replay from the manifest input boundary.

- [ ] **Step 3: Implement one local CLI recovery command**

Add a command that runs the local recovery flow against a user-provided directory so manual testing is possible without cloud credentials.

- [ ] **Step 4: Verify**

Run: `cargo test --workspace local_recovery`

Expected: the e2e test passes after dropping all runtime-local state.

- [ ] **Step 5: Commit**

```bash
git add crates/velorix-runtime crates/velorix-cli tests/e2e
git commit -m "feat: recover stateless runtime from manifests"
```

## Task 8: Hybrid Local Cache

**Files:**
- Create: `crates/velorix-runtime/src/cache.rs`
- Create: `crates/velorix-runtime/tests/cache.rs`

- [ ] **Step 1: Write cache behavior tests**

Cover memory hits, disk spill, eviction, restart with empty cache, and correctness when cached objects are missing.

- [ ] **Step 2: Implement memory cache**

Use bounded memory capacity and object-key-based lookup.

- [ ] **Step 3: Implement disk spill cache**

Store cached objects under a runtime-local cache directory. Never use cache contents as proof of durable progress.

- [ ] **Step 4: Verify**

Run: `cargo test -p velorix-runtime cache`

Expected: cache tests pass and restart behavior proves object storage remains authoritative.

- [ ] **Step 5: Commit**

```bash
git add crates/velorix-runtime
git commit -m "feat: add non-durable hybrid local cache"
```

## Task 9: Prototype Benchmark and Readiness Gate

**Files:**
- Create: `benches/local_incremental.rs`
- Create: `docs/architecture/storage-contract.md`
- Modify: `README.md`

- [ ] **Step 1: Add a local incremental benchmark**

Measure ingest throughput, checkpoint latency, recovery latency, and materialized view freshness against the local filesystem object-store adapter.

- [ ] **Step 2: Document the storage contract**

Describe object key layout, manifest atomicity requirements, crash windows, and garbage collection rules.

- [ ] **Step 3: Update README status**

Replace the initial plan-only language with the exact prototype capabilities that are implemented.

- [ ] **Step 4: Verify**

Run: `cargo test --workspace` and the local benchmark command selected for the final benchmark harness.

Expected: all tests pass and benchmark output includes throughput, checkpoint latency, recovery latency, and freshness metrics.

- [ ] **Step 5: Commit**

```bash
git add benches docs README.md
git commit -m "docs: define prototype readiness gate"
```

## Self-Review

- Spec coverage: The plan covers object storage as the database, stateless compute, delta-based execution, DBSP-inspired operators, LSM-style state objects, local cache, exact-once manifests, and horizontal scaling prerequisites.
- Placeholder scan: No task depends on undefined implementation details. The plan deliberately defers distributed scheduling until the single-process object-storage recovery invariant is proven.
- Type consistency: The same object key, manifest, delta, storage, runtime, and cache boundaries are used across tasks.

# Velorix Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal Velorix prototype that proves an object-storage-first, stateless, incremental streaming database can ingest deltas, maintain a materialized view, checkpoint progress, and recover without local durable state. Velorix is third-party-first: do not expand direct hand-written implementations where SlateDB, Foyer, Apache DataFusion, or Feldera DBSP fit the problem.

**Architecture:** Start with a single-process prototype whose only durable authority is object storage. Represent input and checkpoints as immutable objects plus versioned manifests, add package boundaries before scaling workers, and migrate prototype internals behind mature substrates. Foyer is current for runtime object-store fetch-through caching. SlateDB for durable LSM/SST/state, DataFusion for SQL/DataFrame/query planning and Arrow execution, and Feldera DBSP/dbsp for incremental algebra/operators/circuit semantics remain planned target directions unless matching code exists.

**Tech Stack:** Rust is the assumed implementation language for the core engine because Velorix needs low overhead, predictable performance, async I/O, and deployable static binaries. Use `tokio` for async execution, `serde` for schemas, `object_store` for filesystem/S3-compatible storage, `proptest` for invariant-heavy delta and manifest tests, and third-party data packages where they fit. Foyer is current for runtime hybrid object-cache internals. The planned package direction is SlateDB for object-storage-first durable state, Apache DataFusion for query planning/execution, and Feldera DBSP semantics or the Rust `dbsp` crate as the reference model or backing engine for incremental computation after embedded API, toolchain, checkpoint/state integration, and cost/resource gates are satisfied.

**Third-party-first directive:** Velorix-specific code should remain glue and policy: object-storage authority, deterministic `ObjectKey` rules, checkpoint manifests, stateless recovery orchestration, resource/cost policy, and integration boundaries. Current hand-written delta/operator/state/cache code is prototype scaffolding unless the plan explicitly says a Velorix-owned boundary is required.

---

## File Structure

- Create: `Cargo.toml` for the Rust workspace and shared dependency versions.
- Create: `crates/velorix-core/src/lib.rs` for the public core module boundary.
- Create: `crates/velorix-core/src/delta.rs` for signed delta records and batches.
- Create: `crates/velorix-core/src/operator.rs` for prototype map, filter, join, and aggregate traits behind a future `IncrementalEngine` boundary.
- Create: `crates/velorix-storage/src/lib.rs` for the object-store-facing storage boundary.
- Create: `crates/velorix-storage/src/object_key.rs` for deterministic object key construction.
- Create: `crates/velorix-storage/src/manifest.rs` for checkpoint manifest schemas and validation.
- Create: `crates/velorix-runtime/src/lib.rs` for stateless worker orchestration.
- Create: `crates/velorix-runtime/src/recovery.rs` for manifest-based recovery.
- Create: `crates/velorix-cli/src/main.rs` for local prototype commands.
- Create: `tests/e2e/local_recovery.rs` for crash and recovery behavior.
- Modify: `README.md` as implementation status changes.
- Modify/keep current: `docs/architecture/third-party-first.md` for package ownership and migration order.

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

- [ ] **Step 2: Implement the minimal prototype delta model**

Represent records as typed keys and values with a signed integer weight. Keep the first version intentionally simple and adapter-friendly: JSON-compatible values are acceptable until the execution format is finalized. Do not treat this as a long-term replacement for Feldera DBSP-shaped algebra or Arrow/DataFusion-compatible execution formats.

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

## Task 5: First Incremental Engine Boundary and Prototype Operators

**Files:**
- Create: `crates/velorix-core/src/operator.rs`
- Modify: `crates/velorix-core/src/lib.rs`
- Create: `crates/velorix-core/tests/operators.rs`

- [ ] **Step 1: Write behavior tests for an `IncrementalEngine` boundary and prototype map, filter, join, and aggregate behavior**

Assert on output deltas and materialized state, not internal calls. The tests should allow replacing the prototype implementation with a DBSP-backed engine without rewriting storage or runtime callers.

- [ ] **Step 2: Implement the adapter boundary**

Introduce the smallest useful `IncrementalEngine` abstraction before adding more hand-written operators. Keep ownership of incremental algebra behind the boundary so Feldera DBSP semantics or a DBSP-backed engine can replace the prototype over time.

- [ ] **Step 3: Implement prototype map and filter**

Map and filter should transform input deltas without requiring persisted state. Do not create a custom expression engine; route future expression/query needs through DataFusion.

- [ ] **Step 4: Implement prototype join and aggregate**

Join and aggregate may maintain in-memory state during a worker run, but durable recovery must come from checkpointed state objects in later tasks. Keep the implementation deliberately narrow and document it as scaffolding for a future DBSP/Feldera-shaped engine.

- [ ] **Step 5: Verify**

Run: `cargo test -p velorix-core operators`

Expected: `IncrementalEngine` boundary and prototype operator behavior tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/velorix-core
git commit -m "feat: add initial incremental engine boundary"
```

## Task 6: State Substrate and Checkpoints

**Files:**
- Create: `crates/velorix-storage/src/state.rs`
- Modify: `crates/velorix-storage/src/manifest.rs`
- Create: `crates/velorix-storage/tests/checkpoint_publish.rs`

- [ ] **Step 1: Write checkpoint publication tests**

Cover successful publication, crash before manifest publication, crash after state write but before manifest publication, and duplicate checkpoint publication.

- [ ] **Step 2: Implement the Velorix state boundary**

Define the interface Velorix needs from durable state: object-store authority, referenced state objects, checkpoint binding, and recovery reads. Do not build a durable LSM/SST/compaction engine in Velorix when SlateDB can own that substrate.

- [ ] **Step 3: Implement immutable state object writes through the current substrate**

Write state objects before publishing the manifest. State objects without a manifest reference are recoverable garbage. If SlateDB is not yet integrated, keep this implementation narrow and migration-friendly rather than expanding a custom LSM layout.

- [ ] **Step 4: Implement manifest publication**

Publish manifests as the only authoritative checkpoint marker. Use object-store conditional writes where available and emulate the condition in local tests.

- [ ] **Step 5: Verify**

Run: `cargo test -p velorix-storage checkpoint_publish`

Expected: crash-window tests prove that only published manifests advance durable progress.

- [ ] **Step 6: Commit**

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

Startup should load the latest manifest, fetch referenced state objects through the current state substrate, and resume replay from the manifest input boundary. Velorix owns recovery orchestration; SlateDB owns durable state internals once integrated.

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

## Task 8: Foyer-Backed Hybrid Local Cache

**Files:**
- Create: `crates/velorix-runtime/src/cache.rs`
- Create: `crates/velorix-runtime/tests/cache.rs`

- [ ] **Step 1: Write cache behavior tests**

Cover memory hits, disk spill, eviction, restart with empty cache, and correctness when cached objects are missing. Tests should prove the cache is never durable authority and should not depend on Foyer internals.

- [ ] **Step 2: Integrate or wrap Foyer for cache internals**

Use Foyer as the current owner of bounded runtime memory and disk object-cache behavior. Keep this cache scoped to object-store fetch-through and never treat it as durable authority. If SlateDB later uses Foyer internally for its own block/object cache, keep that policy separate from Velorix's runtime object cache instead of adding duplicate cache ownership.

- [ ] **Step 3: Enforce Velorix cache policy**

Wrap cache access with deterministic `ObjectKey` rules and object-store authority checks. Store cached objects under a runtime-local cache directory. Never use cache contents as proof of durable progress.

- [ ] **Step 4: Verify**

Run: `cargo test -p velorix-runtime cache`

Expected: cache tests pass and restart behavior proves object storage remains authoritative.

- [ ] **Step 5: Commit**

```bash
git add crates/velorix-runtime
git commit -m "feat: add foyer-backed local cache boundary"
```

## Task 9: DataFusion Query Boundary

**Files:**
- Create: `crates/velorix-core/src/query.rs`
- Modify: `crates/velorix-core/src/lib.rs`
- Create: `crates/velorix-core/tests/query.rs`

- [ ] **Step 1: Write query boundary tests**

Cover SQL/DataFrame-style query input, Arrow-compatible output expectations, and the absence of a custom query planner in Velorix-owned code.

- [ ] **Step 2: Integrate DataFusion at the boundary**

Use Apache DataFusion for SQL/DataFrame planning, expression handling, optimization, and Arrow execution where batch/query semantics apply. Velorix should provide object-backed inputs, checkpoint-aware outputs, and resource/cost policy.

- [ ] **Step 3: Verify**

Run: `cargo test -p velorix-core query`

Expected: query boundary tests pass without introducing a custom planner or expression engine.

- [ ] **Step 4: Commit**

```bash
git add crates/velorix-core
git commit -m "feat: add datafusion query boundary"
```

## Task 10: Prototype Benchmark and Readiness Gate

**Files:**
- Create: `benches/local_incremental.rs`
- Create: `docs/architecture/storage-contract.md`
- Modify/keep current: `docs/architecture/third-party-first.md`
- Modify: `README.md`

- [ ] **Step 1: Add a local incremental benchmark**

Measure ingest throughput, checkpoint latency, recovery latency, and materialized view freshness against the local filesystem object-store adapter.

- [ ] **Step 2: Document the storage contract**

Describe object key layout, manifest atomicity requirements, crash windows, and garbage collection rules. Clarify that SlateDB owns durable LSM/SST/state internals once integrated, while Velorix owns checkpoint manifest authority and recovery semantics.

- [ ] **Step 3: Document package ownership**

Keep `docs/architecture/third-party-first.md` current with ownership for SlateDB, Foyer, DataFusion, Feldera DBSP, and Velorix-specific glue.

- [ ] **Step 4: Update README status**

Replace the initial plan-only language with the exact prototype capabilities that are implemented.

- [ ] **Step 5: Verify**

Run: `cargo test --workspace` and the local benchmark command selected for the final benchmark harness.

Expected: all tests pass and benchmark output includes throughput, checkpoint latency, recovery latency, and freshness metrics.

- [ ] **Step 6: Commit**

```bash
git add benches docs README.md
git commit -m "docs: define prototype readiness gate"
```

## Self-Review

- Spec coverage: The plan covers object storage as the database, stateless compute, package-first execution, DBSP/Feldera-shaped incremental semantics, SlateDB-owned durable state, Foyer-owned local cache, DataFusion-owned query planning/execution, exact-once manifests, and horizontal scaling prerequisites.
- Placeholder scan: No task depends on undefined implementation details. The plan deliberately defers distributed scheduling until the single-process object-storage recovery invariant is proven.
- Type consistency: The same object key, manifest, delta, storage, runtime, `IncrementalEngine`, query, and cache boundaries are used across tasks.

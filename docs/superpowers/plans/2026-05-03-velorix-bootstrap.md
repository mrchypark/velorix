# Velorix Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for completed bootstrap work.

**Status as of 2026-05-04:** Bootstrap implementation is complete on
`feature/velorix-bootstrap` for the current single-process prototype readiness
goal. Completed work includes the object-storage contract, ingest replay,
checkpoint manifests, stateless recovery, Foyer runtime cache boundary,
DataFusion query boundary, SlateDB state-store boundary, DBSP-shaped
`IncrementalEngine` boundary, storage contract documentation, and a lightweight
non-default local incremental benchmark harness. Future work is separated after
Task 10 and should not be read as incomplete bootstrap scope.

**Goal:** Build a minimal Velorix prototype that proves an object-storage-first, stateless, incremental streaming database can ingest deltas, maintain a materialized view, checkpoint progress, and recover without local durable state. Velorix is third-party-first: do not expand direct hand-written implementations where SlateDB, Foyer, Apache DataFusion, or Feldera DBSP fit the problem.

**Architecture:** Start with a single-process prototype whose only durable authority is object storage. Represent input and checkpoints as immutable objects plus versioned manifests, add package boundaries before scaling workers, and migrate prototype internals behind mature substrates. Current minimal integrations are Foyer for runtime object-store fetch-through caching, SlateDB for experimental checkpoint-versioned state-store payloads, and DataFusion for the minimal SQL/query boundary over in-memory Arrow batches. Incremental execution currently remains behind a DBSP-shaped `IncrementalEngine` adapter backed by prototype operators. Future direct Feldera DBSP/dbsp integration and broader SlateDB durable layout, LSM/SST, compaction, and lifecycle work are gated follow-on directions.

**Tech Stack:** Rust is the assumed implementation language for the core engine because Velorix needs low overhead, predictable performance, async I/O, and deployable static binaries. Use `tokio` for async execution, `serde` for schemas, `object_store` for filesystem/S3-compatible storage, `proptest` for invariant-heavy delta and manifest tests, and third-party data packages where they fit. Foyer is current for runtime hybrid object-cache internals, SlateDB is current for the minimal experimental state-store path, and Apache DataFusion is current for query planning/execution at the minimal boundary. Feldera DBSP semantics or the Rust `dbsp` crate remain the reference model or future backing engine for incremental computation after embedded API, toolchain, checkpoint/state integration, and cost/resource gates are satisfied.

**Third-party-first directive:** Velorix-specific code should remain glue and policy: object-storage authority, deterministic `ObjectKey` rules, checkpoint manifests, stateless recovery orchestration, resource/cost policy, and integration boundaries. Current hand-written delta/operator/state/cache code is prototype scaffolding unless the plan explicitly says a Velorix-owned boundary is required.

---

## Implemented File Structure

- `Cargo.toml` defines the Rust workspace and shared dependency versions.
- `crates/velorix-core/src/lib.rs` exposes the public core module boundary.
- `crates/velorix-core/src/delta.rs` defines signed delta records and batches.
- `crates/velorix-core/src/operator.rs` contains prototype map, filter, join,
  and aggregate behavior behind the `IncrementalEngine` boundary.
- `crates/velorix-core/src/engine.rs` defines the DBSP-shaped incremental
  engine adapter and versioned engine checkpoint payload.
- `crates/velorix-core/src/query.rs` defines the DataFusion query boundary.
- `crates/velorix-storage/src/lib.rs` exposes the object-store-facing storage
  boundary.
- `crates/velorix-storage/src/object_key.rs` owns deterministic object key
  construction and parsing.
- `crates/velorix-storage/src/manifest.rs` owns checkpoint manifest schemas and
  validation.
- `crates/velorix-storage/src/log.rs` owns append-only ingest log replay.
- `crates/velorix-storage/src/state.rs` owns checkpoint publication and manifest
  authority.
- `crates/velorix-storage/src/state_store.rs` owns the raw object and SlateDB
  state-store boundary implementations.
- `crates/velorix-runtime/src/lib.rs` exposes runtime modules.
- `crates/velorix-runtime/src/recovery.rs` owns manifest-based recovery.
- `crates/velorix-runtime/src/cache.rs` owns the Foyer-backed runtime object
  cache wrapper.
- `crates/velorix-cli/src/main.rs` provides the local recovery command.
- `tests/e2e/local_recovery.rs` covers crash and recovery behavior.
- `benches/local_incremental.rs` provides a non-default local readiness
  benchmark harness.
- `README.md` describes current implemented prototype capabilities.
- `docs/architecture/third-party-first.md` documents package ownership and
  migration order.
- `docs/architecture/storage-contract.md` documents the current storage
  contract, crash windows, package boundaries, and follow-up limits.

## Task 1: Workspace Scaffold

**Files:**
- Create: `Cargo.toml`
- Create: `crates/velorix-core/src/lib.rs`
- Create: `crates/velorix-storage/src/lib.rs`
- Create: `crates/velorix-runtime/src/lib.rs`
- Create: `crates/velorix-cli/src/main.rs`

- [x] **Step 1: Create the workspace manifest**

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

- [x] **Step 2: Add minimal crate manifests and empty module boundaries**

Each crate should compile with a minimal `lib.rs` or `main.rs` that exposes the intended boundary without implementing runtime behavior yet.

- [x] **Step 3: Verify the scaffold**

Run: `cargo test --workspace`

Expected: the workspace compiles and reports zero or minimal passing tests.

- [x] **Step 4: Commit**

```bash
git add Cargo.toml crates
git commit -m "chore: scaffold velorix workspace"
```

## Task 2: Delta Data Model

**Files:**
- Create: `crates/velorix-core/src/delta.rs`
- Modify: `crates/velorix-core/src/lib.rs`

- [x] **Step 1: Write tests for signed delta semantics**

Add tests that prove positive weights insert logical rows, negative weights retract them, and combining batches preserves net row weight.

- [x] **Step 2: Implement the minimal prototype delta model**

Represent records as typed keys and values with a signed integer weight. Keep the first version intentionally simple and adapter-friendly: JSON-compatible values are acceptable until the execution format is finalized. Do not treat this as a long-term replacement for Feldera DBSP-shaped algebra or Arrow/DataFusion-compatible execution formats.

- [x] **Step 3: Add property-based tests**

Use `proptest` to check that batch combination is associative and that combining a batch with its inverse produces an empty net result.

- [x] **Step 4: Verify**

Run: `cargo test -p velorix-core delta`

Expected: delta example tests and property tests pass.

- [x] **Step 5: Commit**

```bash
git add crates/velorix-core
git commit -m "feat: add signed delta model"
```

## Task 3: Object Storage Contract

**Files:**
- Create: `crates/velorix-storage/src/object_key.rs`
- Create: `crates/velorix-storage/src/manifest.rs`
- Modify: `crates/velorix-storage/src/lib.rs`

- [x] **Step 1: Write tests for deterministic object keys**

Cover ingest batch keys, state object keys, temporary publish keys, and checkpoint manifest keys. Object keys must be stable across process restarts.

- [x] **Step 2: Write manifest validation tests**

Cover valid manifests, missing input progress, missing state object references, duplicate object identifiers, and non-monotonic checkpoint versions.

- [x] **Step 3: Implement object key constructors**

Use structured constructors instead of string formatting at call sites. Keep all object key layout decisions in `object_key.rs`.

- [x] **Step 4: Implement checkpoint manifest schemas**

The first manifest schema must include checkpoint version, input ranges, state object references, output object references, parent checkpoint, and creation timestamp.

- [x] **Step 5: Verify**

Run: `cargo test -p velorix-storage`

Expected: key and manifest tests pass.

- [x] **Step 6: Commit**

```bash
git add crates/velorix-storage
git commit -m "feat: define object storage contract"
```

## Task 4: Ingest Log and Replay

**Files:**
- Create: `crates/velorix-storage/src/log.rs`
- Modify: `crates/velorix-storage/src/lib.rs`
- Create: `crates/velorix-storage/tests/log_replay.rs`

- [x] **Step 1: Write integration tests with a temporary filesystem object store**

Test appending ordered delta batches, listing committed batches, and replaying from a checkpoint boundary.

- [x] **Step 2: Implement append-only log writes**

Persist batches as immutable objects. Treat overwrite attempts as errors.

- [x] **Step 3: Implement replay reads**

Replay must return batches in deterministic order and skip batches already covered by a checkpoint.

- [x] **Step 4: Verify**

Run: `cargo test -p velorix-storage log_replay`

Expected: temporary filesystem-backed replay tests pass.

- [x] **Step 5: Commit**

```bash
git add crates/velorix-storage
git commit -m "feat: add object-backed ingest log"
```

## Task 5: First Incremental Engine Boundary and Prototype Operators

**Files:**
- Create: `crates/velorix-core/src/operator.rs`
- Modify: `crates/velorix-core/src/lib.rs`
- Create: `crates/velorix-core/tests/operators.rs`

- [x] **Step 1: Write behavior tests for an `IncrementalEngine` boundary and prototype map, filter, join, and aggregate behavior**

Assert on output deltas and materialized state, not internal calls. The tests should allow replacing the prototype implementation with a DBSP-backed engine without rewriting storage or runtime callers.

- [x] **Step 2: Implement the adapter boundary**

Introduce the smallest useful `IncrementalEngine` abstraction before adding more hand-written operators. Keep ownership of incremental algebra behind the boundary so Feldera DBSP semantics or a DBSP-backed engine can replace the prototype over time.

- [x] **Step 3: Implement prototype map and filter**

Map and filter should transform input deltas without requiring persisted state. Do not create a custom expression engine; route future expression/query needs through DataFusion.

- [x] **Step 4: Implement prototype join and aggregate**

Join and aggregate may maintain in-memory state during a worker run, but durable recovery must come from checkpointed state objects in later tasks. Keep the implementation deliberately narrow and document it as scaffolding for a future DBSP/Feldera-shaped engine.

- [x] **Step 5: Verify**

Run: `cargo test -p velorix-core operators`

Expected: `IncrementalEngine` boundary and prototype operator behavior tests pass.

- [x] **Step 6: Commit**

```bash
git add crates/velorix-core
git commit -m "feat: add initial incremental engine boundary"
```

## Task 6: State Substrate and Checkpoints

**Files:**
- Create: `crates/velorix-storage/src/state.rs`
- Modify: `crates/velorix-storage/src/manifest.rs`
- Create: `crates/velorix-storage/tests/checkpoint_publish.rs`

- [x] **Step 1: Write checkpoint publication tests**

Cover successful publication, crash before manifest publication, crash after state write but before manifest publication, and duplicate checkpoint publication.

- [x] **Step 2: Implement the Velorix state boundary**

Define the interface Velorix needs from durable state: object-store authority, referenced state objects, checkpoint binding, and recovery reads. Do not build a durable LSM/SST/compaction engine in Velorix when SlateDB can own that substrate.

- [x] **Step 3: Implement immutable state object writes through the current substrate**

Write state objects before publishing the manifest. State objects without a manifest reference are recoverable garbage. Keep the current SlateDB-backed path narrow and migration-friendly rather than expanding Velorix-owned LSM layout, compaction, or lifecycle policy.

- [x] **Step 4: Implement manifest publication**

Publish manifests as the only authoritative checkpoint marker. Use object-store conditional writes where available and emulate the condition in local tests.

- [x] **Step 5: Verify**

Run: `cargo test -p velorix-storage checkpoint_publish`

Expected: crash-window tests prove that only published manifests advance durable progress.

- [x] **Step 6: Commit**

```bash
git add crates/velorix-storage
git commit -m "feat: publish checkpointed state manifests"
```

## Task 7: Stateless Runtime Recovery

**Files:**
- Create: `crates/velorix-runtime/src/recovery.rs`
- Modify: `crates/velorix-runtime/src/lib.rs`
- Create: `tests/e2e/local_recovery.rs`

- [x] **Step 1: Write end-to-end recovery test**

Use a temporary object-store directory. Ingest deltas, process a view, publish a checkpoint, drop the runtime instance, create a fresh runtime instance, and verify the materialized view recovers from object storage only.

- [x] **Step 2: Implement runtime startup from manifest**

Startup should load the latest manifest, fetch referenced state objects through the current state substrate, and resume replay from the manifest input boundary. Velorix owns recovery orchestration; SlateDB owns the current minimal state-store internals for checkpoint-versioned payloads, with broader durable layout and compaction policy still gated.

- [x] **Step 3: Implement one local CLI recovery command**

Add a command that runs the local recovery flow against a user-provided directory so manual testing is possible without cloud credentials.

- [x] **Step 4: Verify**

Run: `cargo test --workspace local_recovery`

Expected: the e2e test passes after dropping all runtime-local state.

- [x] **Step 5: Commit**

```bash
git add crates/velorix-runtime crates/velorix-cli tests/e2e
git commit -m "feat: recover stateless runtime from manifests"
```

## Task 8: Foyer-Backed Hybrid Local Cache

**Files:**
- Create: `crates/velorix-runtime/src/cache.rs`
- Create: `crates/velorix-runtime/tests/cache.rs`

- [x] **Step 1: Write cache behavior tests**

Cover memory hits, disk spill, eviction, restart with empty cache, and correctness when cached objects are missing. Tests should prove the cache is never durable authority and should not depend on Foyer internals.

- [x] **Step 2: Integrate or wrap Foyer for cache internals**

Use Foyer as the current owner of bounded runtime memory and disk object-cache behavior. Keep this cache scoped to object-store fetch-through and never treat it as durable authority. If SlateDB later uses Foyer internally for its own block/object cache, keep that policy separate from Velorix's runtime object cache instead of adding duplicate cache ownership.

- [x] **Step 3: Enforce Velorix cache policy**

Wrap cache access with deterministic `ObjectKey` rules and object-store authority checks. Store cached objects under a runtime-local cache directory. Never use cache contents as proof of durable progress.

- [x] **Step 4: Verify**

Run: `cargo test -p velorix-runtime cache`

Expected: cache tests pass and restart behavior proves object storage remains authoritative.

- [x] **Step 5: Commit**

```bash
git add crates/velorix-runtime
git commit -m "feat: add foyer-backed local cache boundary"
```

## Task 9: DataFusion Query Boundary

**Files:**
- Create: `crates/velorix-core/src/query.rs`
- Modify: `crates/velorix-core/src/lib.rs`
- Create: `crates/velorix-core/tests/query.rs`

- [x] **Step 1: Write query boundary tests**

Cover SQL/DataFrame-style query input, Arrow-compatible output expectations, and the absence of a custom query planner in Velorix-owned code.

- [x] **Step 2: Integrate DataFusion at the boundary**

Use Apache DataFusion for SQL/DataFrame planning, expression handling, optimization, and Arrow execution where batch/query semantics apply. Velorix should provide object-backed inputs, checkpoint-aware outputs, and resource/cost policy.

- [x] **Step 3: Verify**

Run: `cargo test -p velorix-core query`

Expected: query boundary tests pass without introducing a custom planner or expression engine.

- [x] **Step 4: Commit**

```bash
git add crates/velorix-core
git commit -m "feat: add datafusion query boundary"
```

## Task 10: Prototype Benchmark and Readiness Gate

**Files:**
- Create: `benches/local_incremental.rs`
- Create: `docs/architecture/storage-contract.md`
- Modify: `crates/velorix-runtime/Cargo.toml`
- Keep current: `docs/architecture/third-party-first.md`
- Keep current: `README.md`

- [x] **Step 1: Add a local incremental benchmark**

Measure ingest throughput, checkpoint latency, recovery latency, and materialized view freshness against the local filesystem object-store adapter.

Implemented as a lightweight non-default Cargo bench target:

```bash
cargo bench -p velorix-runtime --bench local_incremental
```

The harness intentionally avoids Criterion and does not run during
`cargo test --workspace`.

- [x] **Step 2: Document the storage contract**

Describe object key layout, manifest atomicity requirements, crash windows, and garbage collection rules. Clarify that SlateDB owns the current minimal experimental state-store path, broader SlateDB durable LSM/SST/layout and compaction work remains gated, and Velorix owns checkpoint manifest authority and recovery semantics.

Implemented in `docs/architecture/storage-contract.md`.

- [x] **Step 3: Document package ownership**

Keep `docs/architecture/third-party-first.md` current with ownership for SlateDB, Foyer, DataFusion, Feldera DBSP, and Velorix-specific glue.

- [x] **Step 4: Update README status**

Replace the initial design language with the exact prototype capabilities that are implemented.

Current `README.md` already describes implemented Foyer, SlateDB, DataFusion,
manifest, recovery, and DBSP-shaped `IncrementalEngine` boundaries without
claiming full production readiness.

- [x] **Step 5: Verify**

Run: `cargo test --workspace` and the local benchmark command selected for the final benchmark harness.

Expected: all tests pass and benchmark output includes throughput, checkpoint latency, recovery latency, and freshness metrics.

- [x] **Step 6: Commit**

```bash
git add benches docs crates/velorix-runtime/Cargo.toml README.md
git commit -m "docs: define prototype readiness gate"
```

## Future Follow-Up

The following work is intentionally outside the completed bootstrap readiness
scope:

- Direct Feldera DBSP or Rust `dbsp` crate integration after embedded API,
  toolchain, checkpoint/state, and cost/resource gates are cleared.
- Object-backed and checkpoint-aware query service integration beyond the
  current in-memory DataFusion `DeltaBatch` query boundary.
- Broader SlateDB durable layout, LSM/SST policy, compaction tuning, garbage
  collection integration, and lifecycle management.
- S3-compatible storage validation and distributed worker partitioning.
- Production benchmark suite on representative object-storage-backed workloads.
  The current `local_incremental` harness is a lightweight local readiness
  artifact, not a capacity-planning result.

## Self-Review

- Spec coverage: The plan covers object storage as the database, stateless compute, package-first execution, DBSP/Feldera-shaped incremental semantics, SlateDB-owned durable state, Foyer-owned local cache, DataFusion-owned query planning/execution, exact-once manifests, and horizontal scaling prerequisites.
- Placeholder scan: No task depends on undefined implementation details. The plan deliberately defers distributed scheduling until the single-process object-storage recovery invariant is proven.
- Type consistency: The same object key, manifest, delta, storage, runtime, `IncrementalEngine`, query, and cache boundaries are used across tasks.

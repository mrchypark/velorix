## Verdict

**Current status.** The original contract blockers for ingest acknowledgement, output-only query serving, metadata startup safety, and join frontier shape are resolved in the current docs/code contracts: public 1.0 ingest uses synchronous `materialized` acknowledgement, queries read published materialized output, production metadata startup fails closed, and joins use sequential per-relation frontier vectors. This critique still blocks release readiness where live evidence is missing, especially cross-store checkpoint fault injection, trusted disaster recovery, repair/GC/upgrade evidence, and adversarial scale/retention proof.

This is an architecture/contract review of the supplied bundle, not a full implementation audit. Line references are to `attachments-bundle.txt`.

## Top 10 risks

### [SEVERITY] (P0) — Ingest acknowledgements have two incompatible meanings

Location:

* File: `docs/architecture/materialized-view-runtime-roadmap.md`
* Lines: 733–765
* File: `docs/architecture/ingest-admission-contract.md`
* Lines: 1195–1203

Issue:

* One accepted document says an ingest acknowledgement covers durable input admission only and explicitly excludes view processing and checkpoint publication.
* Another says the default `materialized` acknowledgement is returned only after all active views have advanced and the checkpoint pointer is published.

Current status:

* Resolved by current code evidence: internal admission-layer acknowledgements are documented as durable input admission only, while the public 1.0 relation ingest route exposes only `materialized` acknowledgement after durable materialized-view output is committed.

Why It Matters:

* A client cannot determine what a `200` guarantees.
* Crash retries, visibility guarantees, and idempotency all depend on this distinction.
* Tests for either interpretation can pass while the product violates the other.

Recommendation:

* For 1.0, expose one acknowledgement contract: a successful response means the input is durable and every active dependent view has a durably published checkpoint at or beyond the returned epoch.
* Remove `append_committed` from the public 1.0 API.
* Return explicit `ingest_epoch` and `materialized_through` fields.
* A failure after durable append must return a resumable, idempotent failure—not claim success.

Verification:

* Inject process termination after range reservation, source write, each view-state write, manifest write, and pointer CAS.
* Every successful response must remain query-visible after a cold restart.
* Retrying every unsuccessful request must converge without duplicate effects.

---

### [SEVERITY] (P0) — The query path violates the materialized-output-only contract

Location:

* File: `docs/architecture/materialized-view-runtime-roadmap.md`
* Lines: 335–346, 397–414
* Lines: 741–748, 767–773

Issue:

* The architecture requires queries to read a published materialized output table/page index and explicitly forbids reconstruction from source batches or live accumulators.
* The optimization section instead allows a query to replay missing source batches and says queries read in-process runtime state.

Current status:

* Mitigated by current code evidence: the public query path reads durably published materialized output, and query-triggered source replay/runtime repair is not part of the public 1.0 relation query contract.

Why It Matters:

* This converts a read into an unbounded write/recovery operation.
* Query latency becomes proportional to materialization lag.
* Query replicas require source-log access and checkpoint-publishing authority.
* Concurrent queries can race to repair the same view.
* Cold query replicas may disagree with warm replicas.
* This directly violates the stated product constraint.

Recommendation:

* Delete query-triggered convergence.
* A query must read one immutable, durably published output version.
* If output is behind committed input, return a bounded `MATERIALIZATION_LAG` response with the committed and materialized frontiers.
* Catch-up belongs to the single fenced materializer, never to query serving.

Verification:

* Deny query workers all read access to source-ingest object prefixes and all metadata-write access.
* Kill the materializer and query from a fresh replica.
* The query must either read published output or return lag; it must perform no source reads or durable writes.

---

### [SEVERITY] (P0) — The metadata server is fail-open by default

Location:

* File: `crates/velorix-meta/src/main.rs`
* Lines: 1480–1493, 1961–1972, 2024–2052

Issue:

* The shown server:

  * defaults to `0.0.0.0:9090`;
  * defaults to the non-durable in-memory backend;
  * enables unauthenticated service operation when the token variable is absent;
  * accepts any non-empty Hiqlite node list, despite the documented three-voter requirement;
  * does not configure transport TLS in the binary itself.

Current status:

* Resolved by current code evidence: production startup now requires explicit `VELORIX_META_MODE` and `VELORIX_META_BACKEND`, rejects the memory backend, requires authentication, transport-security attestation and a durable backend, and requires exactly three unique Hiqlite voter nodes when Hiqlite is selected. Explicit development mode defaults to loopback-only binding for the in-memory backend.

Why It Matters:

* A production configuration omission can silently produce an unauthenticated, non-durable metadata authority.
* Restart loses catalogs, ownership and checkpoint pointers.
* An exposed unauthenticated metadata service can permit catalog manipulation or checkpoint fencing attacks.
* This is the opposite of fail-closed startup.

Recommendation:

* Separate production and development startup profiles, preferably separate binaries or compile-time features.
* Production startup must reject:

  * missing explicit backend;
  * in-memory backend;
  * missing authentication;
  * missing transport-security attestation;
  * anything other than exactly three unique Hiqlite voter endpoints when Hiqlite mode is used.
* Bind to loopback only in explicit development mode.

Verification:

* Start the release binary with every required setting individually omitted.
* Each case must terminate before binding a socket.
* Verify that no production deployment manifest can select memory or unauthenticated mode.

---

### [SEVERITY] (P0) — The cross-store checkpoint commit protocol is underspecified

Location:

* File: `docs/architecture/materialized-view-runtime.md`
* Lines: 196–232
* File: `docs/architecture/materialized-view-runtime-roadmap.md`
* Lines: 397–445

Issue:

* Operator state, output pages/deltas and source evidence are written to object storage, while progress is advanced through metadata pointers.
* The documents say pointers are advanced atomically, but do not define the invariant connecting:

  * admitted plan hash;
  * owner fencing epoch;
  * input frontier vector;
  * operator state objects;
  * materialized output objects;
  * output schema;
  * previous checkpoint;
  * query-visible version.

Current status:

* Partially mitigated, still open: the runtime now has immutable checkpoint manifest and pointer validation evidence, stale-owner fencing checks, and fail-closed recovery tests for orphaned or corrupt output/state objects. The remaining release blocker is the actual S3-compatible delayed-visibility/retry/fault-injection matrix proving the metadata CAS and object-store object set cannot publish a mixed checkpoint.

Why It Matters:

* A crash can produce state at epoch N with output at N−1, or a metadata pointer referring to incomplete objects.
* One view can advance while another dependent view fails.
* Query serving and restart can select different authorities.
* Object-store writes and a Hiqlite transaction do not constitute a distributed transaction.

Recommendation:

* Publish one immutable `CheckpointManifestV1` per view containing all of the above fields and content hashes.
* Write every referenced object create-only, read-verify it, then CAS exactly one authoritative checkpoint pointer using the current owner token and previous pointer.
* Query and recovery must begin from that same pointer.
* Define whether a multi-view ingest acknowledgement is atomic across all affected views or only reports a vector of independently committed view epochs.

Verification:

* Fault-inject before and after every object write, verification read, manifest write and metadata CAS.
* Recovery must select either the complete old checkpoint or complete new checkpoint—never a mixed state.
* Run the matrix against an actual S3-compatible deployment with delayed visibility, retries and injected request failures.

---

### [SEVERITY] (P0) — The release gate can report “complete” while accepted documents declare blockers

Location:

* File: `docs/architecture/production-readiness-status.md`
* Lines: 873–893
* File: `docs/architecture/ingest-admission-contract.md`
* Lines: 1298–1328
* File: `docs/architecture/hiqlite-meta-service.md`
* Lines: 1031–1107

Issue:

* The readiness matrix marks ingest, ownership, checkpointing, Kubernetes, S3 compatibility and GC complete with no blockers.
* The ingest contract says deployed writer/coordinator, crash/retry, leader-handoff and multi-pod evidence are still required.
* The metadata document says product-complete remains false and local attestations are not trusted release evidence.

Current status:

* Mitigated by current generated evidence gates: product completion is computed from evidence status, out-of-scope gates remain blockers for release completion, deployed image digests and sibling evidence are validated, and local diagnostic attestations are not allowed to make `product_complete=true`.

Why It Matters:

* The release validator can provide a false-positive release decision.
* Evidence labels such as “runtime checks” and “benchmark evidence” are not immutable evidence references.
* A static Markdown status becomes a self-asserted certification system.

Recommendation:

* Delete the hand-maintained all-green matrix.
* Generate release status from a signed, commit- and image-digest-bound evidence manifest.
* Each contract must point to concrete artifacts, scenario seeds, test logs and deployed image digests.
* Any missing artifact, stale revision or diagnostic-only attestation must make the gate fail.

Verification:

* Remove or mutate each required artifact and confirm the release gate fails.
* Run the gate against a dirty working tree, mismatched image, local-only smoke output and stale benchmark; all must be rejected.

---

### [SEVERITY] (P1) — Two-relation join consistency still needs live frontier evidence

Location:

* File: `docs/architecture/materialized-view-runtime.md`
* Lines: 177–194
* File: `docs/architecture/materialized-view-runtime-roadmap.md`
* Lines: 750–765

Issue:

* Joins are required to observe multi-relation changes as one epoch-consistent boundary.
* The public ingest API updates one relation at a time.
* The multi-relation epoch mechanism is described as internal/test-only, with no public way to atomically submit changes to both sides.

Current status:

* Contract resolved, release evidence still open: current docs define public 1.0 join consistency as sequential per-relation frontier vectors, not one grouped multi-relation commit. The REST join smoke exercises relation creation, relation ingest, two-relation view creation and materialized query through the public API, but product completion still needs live crash/retry/recovery evidence that published output and checkpoints preserve the exact frontier vector they claim.

Why It Matters:

* Sequential left/right ingests can expose an intermediate join result.
* The vector-frontier contract is meaningful only if every published output and recovered checkpoint preserves the exact per-relation frontier it represents.
* Recovery may replay the two sides in a different grouping than live execution.

Recommendation:

* Enforce the current per-relation frontier-vector contract, allowing sequential intermediate results.
* Persist the complete input frontier vector in every checkpoint and output manifest.
* Remove any remaining stronger atomic-epoch claim unless a grouped multi-relation commit is introduced.

Verification:

* Concurrently ingest both inputs with crashes, retries and reordered completion.
* Compare live output and cold-recovered output for every intermediate frontier.
* Assert that no published output claims a frontier it has not fully applied.

---

### [SEVERITY] (P1) — Direct crate boundaries must enforce the claimed ownership model

Location:

* File: `crates/velorix-core/src/lib.rs`
* Lines: 1366–1380
* File: `crates/velorix-runtime/src/lib.rs`
* Lines: 1383–1402
* File: `crates/velorix-storage/src/lib.rs`
* Lines: 1405–1422
* Files: crate `Cargo.toml` entries
* Lines: 2195–2492

Issue:

* `velorix-core` is described as runtime-independent but still exports
  `engine`, `operator` and `query`.
* `velorix-runtime` is described as stateless while owning materialized-view state, leased checkpoints, persisted views and recovery.
* `velorix-storage` owns `ownership`, catalogs and view registries rather than only object-storage mechanics.
* `velorix-api` previously depended directly on K8s, metadata, runtime and storage.
* `velorix-cli` previously depended directly on runtime and storage for local recovery and admin/GC paths, creating an alternate composition and mutation path.

Current status:

* Mitigated for CLI direct engine/storage links: `velorix-cli` no longer links
  `velorix-runtime` or `velorix-storage`. Benchmark/readiness evidence and the
  narrow storage-admin facade now live behind `velorix-control`; runtime keeps
  compatibility re-exports.
* Mitigated for Kubernetes direct storage links: `velorix-k8s` no longer links
  `velorix-storage` in production code. Existing tests may still use storage
  fixtures through a dev-dependency.
* Mitigated for API direct storage links: `velorix-api` no longer links
  `velorix-storage`; storage-backed registry/log/key types used by API are
  routed through the `velorix-control` storage-admin facade.
* Mitigated for API direct metadata links: `velorix-api` no longer links
  `velorix-meta`; metadata traits, client constructors, owner tokens and
  checkpoint pointer contracts used by API are routed through the
  `velorix-control` metadata facade.
* Mitigated for API direct Kubernetes links: `velorix-api` no longer links
  `velorix-k8s`; object-store authority, startup validation and deployed
  ingest-writer runtime contracts are routed through `velorix-control`, while
  Kubernetes CRD and pod/executor logic stays in `velorix-k8s`.
* Mitigated for model-layer direct runtime dependencies: `velorix-core` no
  longer has production Tokio or DataFusion dependencies. SQL admission uses a
  direct `sqlparser` dependency; DataFusion execution stays outside the model
  crate.
* Mitigated for API direct runtime links: `velorix-api` no longer links
  `velorix-runtime`; query execution helpers, runtime naming, and materialized
  runtime construction are routed through `velorix-control`/`velorix-core`, and
  runtime keeps compatibility re-exports.
* Open design debt: `velorix-control` now owns concrete authority and runtime
  boundary code, including the native materialized runtime facade. This removes
  route-level direct dependencies but makes `velorix-control` a real product
  boundary rather than a thin protocol crate; keep reviewing whether that
  responsibility should later split into a smaller execution crate.

Why It Matters:

* Trust boundaries are conventions rather than compiler-enforced constraints.
* API and CLI code can bypass the intended application service and manipulate storage directly.
* Legacy query or persisted-table paths can silently reintroduce source scans.
* Recovery, fencing and storage policy become difficult to review independently.

Recommendation:

* Move immutable schemas, IDs, logical plan types and errors into a dependency-light model crate.
* Move DataFusion admission/lowering into a plan crate.
* Keep operator execution, state and recovery in one engine crate.
* Keep object serialization, keys and immutable manifest I/O in storage.
* Move leases, owners and progress CAS into metadata/control.
* Make CLI and Kubernetes components protocol clients; they must not link the engine or storage implementation.

Verification:

* Add a `cargo metadata` dependency-policy test.
* Fail CI if API routes, CLI or K8s crates directly depend on engine internals or Velorix storage adapters.
* Fail CI if the model crate acquires Tokio, DataFusion, object-store or Kubernetes dependencies.

---

### [SEVERITY] (P1) — The no-PVC Hiqlite/fencing story is contradictory and not production-proven

Location:

* File: `docs/architecture/hiqlite-meta-service.md`
* Lines: 900–967, 1031–1107, 1136–1182
* File: `crates/velorix-meta/Cargo.toml`
* Lines: 2371–2394

Issue:

* The document simultaneously advertises production-safe fencing and says product-complete remains false until trusted bounded-failover evidence exists.
* The backend depends on a pinned fork commit rather than an upstream release.
* The product topology alternates between a Velorix-managed ephemeral-disk authority and an externally operated authority.
* Periodic object-store backup of an `emptyDir` Raft cluster does not establish zero-loss recovery for acknowledged metadata writes.

Current status:

* Partially mitigated, still open: release evidence now distinguishes managed Hiqlite authority, backend-time support and local failover smoke from product-complete evidence. The remaining blocker is a trusted no-PVC Hiqlite authority disaster-recovery test proving acknowledged metadata writes survive total voter loss through the permitted durable stores.

Why It Matters:

* Loss of all voters can lose owner epochs, catalog changes or checkpoint pointers newer than the last backup.
* Reconstructing a coherent Raft cluster from periodic backups has a different correctness model from restoring immutable application checkpoints.
* Operator ownership is unclear: Velorix-managed versus external service.

Recommendation:

* For 1.0, choose one:

  * single-writer OSS metadata with HA and multi-writer explicitly out of scope; or
  * externally operated Hiqlite with a precise external dependency and disaster-recovery contract.
* Do not ship the managed ephemeral-voter topology as the production authority.
* Do not claim production fencing until behavioral release evidence exists against the exact pinned binary.

Verification:

* Destroy every voter and all node-local disks immediately after an acknowledged materialized ingest.
* Recover using only permitted durable stores.
* Prove no acknowledged catalog, owner epoch or checkpoint pointer is lost.
* Exercise partitions, paused owners, leader failover and stale checkpoint publication.

---

### [SEVERITY] (P1) — Recovery lacks a complete upgrade, repair and GC contract

Location:

* File: `docs/architecture/materialized-view-runtime.md`
* Lines: 224–232
* File: `docs/architecture/materialized-view-runtime-roadmap.md`
* Lines: 349–368, 430–445
* File: `docs/architecture/production-readiness-status.md`
* Lines: 884–892

Issue:

* Plans and codecs are versioned in the proposed format, but there is no stated compatibility policy.
* “Fail closed” on a missing or corrupt latest checkpoint does not define how the service returns to operation without losing acknowledged data.
* GC is marked complete without a documented reachability model covering old checkpoints, replay logs, active readers, compaction and rolling upgrades.

Current status:

* Partially mitigated, still open: repair and fail-closed checkpoint-read routes are now evidence-bound, and background compaction remains experimental rather than public 1.0. The remaining blocker is a complete upgrade/rollback, repair and GC reachability contract with fault-injection evidence.

Why It Matters:

* A 1.0.1 binary may be unable to restore a 1.0 checkpoint.
* GC can remove replay data needed to repair the latest checkpoint.
* Falling back silently to an older checkpoint can violate acknowledged-write durability.
* Permanent fail-closed outage is not a recovery strategy.

Recommendation:

* Guarantee N reads N−1 checkpoint and output formats for the supported upgrade window.
* Persist a checkpoint predecessor chain and replay lower bounds.
* Add an explicit repair operation: restore the last valid checkpoint and replay committed epochs to the authoritative frontier.
* GC must be mark-and-sweep from authoritative pointers plus protected upgrade/repair roots, with generation fencing for concurrent readers.

Verification:

* Rolling upgrade and rollback using real persisted checkpoints.
* Corrupt every class of latest-checkpoint object and repair without source-query recomputation or acknowledged data loss.
* Run GC concurrently with query, compaction, recovery and checkpoint publication under fault injection.

---

### [SEVERITY] (P1) — Scope expansion introduces unbounded state before the core runtime is proven

Location:

* File: `docs/architecture/materialized-view-runtime-roadmap.md`
* Lines: 447–472, 591–688, 733–789

Issue:

* The first milestone says window SQL should be excluded, but the same roadmap claims tumbling, hopping and session windows, extrema multisets, predicate backfill, asynchronous materialization and background compaction.
* `min` and `max` retain per-window value multisets.
* Session-window merge/split behavior, global watermark aggregation, retention and state reclamation are not specified.
* Predicate backfill can intentionally replay entire batches beyond the requested predicate.

Current status:

* Mitigated for public 1.0 admission: advanced window SQL, request-scope/range backfill and background compaction are experimental-only and disabled for the public API. The remaining work is not to expose them until quotas, retention and adversarial replay evidence exist.

Why It Matters:

* State is unbounded under high-cardinality keys, large windows, join fan-out or repeated values.
* Session windows and retractions multiply recovery cases.
* Over-materializing predicate backfill can change results outside the requested scope.
* Operational load becomes impossible to bound.

Recommendation:

* Remove all user-facing window SQL, predicate backfill and asynchronous acknowledgement from 1.0.
* Keep only full resumable backfill through the ordinary ingest/replay path.
* Add per-view state, key-cardinality, join-fan-out and object-write quotas before restoring advanced operators.
* Require a defined retention and reclamation model before exposing any window operator.

Verification:

* High-cardinality, skewed-key and join-fan-out load tests must hit deterministic admission limits rather than OOM.
* State size must stabilize after retention.
* Repeated replay and restart must produce byte-equivalent output/checkpoint hashes.

## Top 10 simplifications

1. **One ingest acknowledgement.** Retain only synchronous `materialized` acknowledgement for 1.0; remove `append_committed`.

2. **One query authority.** Query immutable published output manifests/pages only. Never query live accumulator state and never replay source data on read.

3. **One checkpoint pointer per view.** State, output, schema, plan, frontiers and owner epoch belong in one immutable manifest selected by one CAS pointer.

4. **Single-writer 1.0.** Defer production multi-writer range coordination and lease-expiry failover. A fenced single active writer materially reduces the recovery state space.

5. **No window SQL in 1.0.** Retain event-time fields as reserved metadata only. Delete tumbling, hopping and session admission from the public capability table.

6. **One backfill mode.** Full, resumable replay from a committed frontier. Delete predicate and partial-batch backfill.

7. **One production metadata backend.** Prefer the OSS backend for the single-writer 1.0 profile; make memory development-only and Hiqlite an explicit later HA profile.

8. **Reduce the crate graph.** Model/plan, engine, object storage, metadata client/server and API server are sufficient. K8s and CLI should be remote clients.

9. **Move diagnostics out of API contracts.** Replace the large per-ingest stage schema with stable identifiers and counters; publish detailed timings through metrics/traces.

10. **Replace bespoke readiness prose with generated evidence.** One signed release manifest is sufficient. Remove parallel local attestations, status summaries and compatibility signatures that do not establish runtime correctness.

## What to split or delete first

1. **Delete the query-time convergence branch and public `append_committed` mode.** This is the only documented path that directly violates the required materialized-output query contract.

2. **Resolve and delete one ingest-ack contract.** OpenAPI must become the sole public authority; the rejected contract should not remain as an “accepted” architecture document.

3. **Split `velorix-core`.** Move `engine`, `operator` and `query` out. The surviving model crate must not depend on DataFusion or Tokio.

4. **Move `ownership` out of `velorix-storage`.** Storage owns immutable bytes and manifests; metadata/control owns leases, fencing and authoritative progress.

5. **Completed: delete `persisted_query`, `persisted_table` and
   `persisted_view`.** The runtime now exposes only native materialized-view
   execution and materialized-output query contracts.

6. **Keep direct storage/engine dependencies out of `velorix-cli`, `velorix-k8s`
   and route code.** Administrative clients should call authenticated APIs or
   explicit control-plane contracts, not manipulate storage implementation
   types.

7. **Feature-gate out windows, predicate backfill and background query repair.** Do not merely document them as non-goals while compiling and testing them as product features.

8. **Keep pressure on `velorix-control`'s scope.** It now owns concrete
   authority/runtime boundary code; if that scope keeps growing, split the
   execution surface instead of letting `velorix-control` become a catch-all
   crate.

9. **Remove the second `object_store` version.** Two versions in the runtime test path create divergent error, conditional-write and S3 behavior.

10. **Delete the static all-green readiness matrix.** Generate it from release evidence or mark it blocked.

## Evidence that would prove 1.0 readiness

1. **Canonical contract evidence**

   * One versioned OpenAPI specification defining acknowledgement, visibility, retry and lag semantics.
   * No conflicting accepted architecture documents.
   * Contract tests run against the release image.

2. **Fail-closed SQL admission**

   * Generated SQL corpus covering every unsupported DataFusion plan and expression node.
   * Unsupported constructs leave no persisted view metadata or runtime binding.
   * Mutation testing demonstrates that removing a capability check causes CI failure.

3. **Output-only query proof**

   * Query pods have IAM access to output and metadata prefixes but no source-ingest prefix.
   * Cold queries still succeed.
   * Object-store audit logs prove no source reads or writes occur during query execution.

4. **Crash-consistency matrix**

   * Deterministic process termination at every persistent-write boundary.
   * At least old-complete or new-complete state after restart; no mixed state.
   * Tests cover one view, multiple affected views, joins and compaction.

5. **Replay determinism**

   * Duplicate, reordered, gapped and retried batches.
   * Live execution, crash recovery and clean replay produce identical logical output and checkpoint hashes.
   * Non-contiguous input never advances a frontier.

6. **Join frontier proof**

   * A formal global-epoch or frontier-vector specification.
   * Concurrent two-input ingest, crash and leader handoff tests.
   * Every output manifest records exactly which input frontiers it represents.

7. **Fencing and disaster recovery**

   * Two competing materializers, network partitions, process pauses, expired owners and metadata-leader failover.
   * No stale owner can publish.
   * Complete destruction of compute and metadata-local disks recovers all acknowledged writes from allowed durable storage.

8. **Upgrade, rollback and repair**

   * N reads N−1 plans, state and output formats.
   * Rolling upgrade and rollback with active ingestion.
   * Corrupt-latest-checkpoint repair from an earlier checkpoint plus replay, with no source-query recomputation.

9. **GC, compaction and scale**

   * Concurrent GC/compaction/query/ingest/recovery fault tests.
   * No live or repair-required object is deleted.
   * Published limits for keys, state size, join fan-out, batch size and object requests.
   * Multi-day soak against the supported object store.

10. **Security and release provenance**

    * Mandatory API and metadata authentication, tenant authorization, TLS, secret rotation, body limits, rate limits and object-prefix isolation.
    * Negative cross-tenant tests.
    * Release artifacts bound to a clean source revision and exact deployed image digests, with SBOM, dependency policy and immutable test evidence.

## Claims that look overclaimed

* **“Production readiness: complete, no blockers.”** Directly contradicted by the ingest and Hiqlite documents, which enumerate unresolved production evidence.

* **“Ingest complete.”** The accepted ingest contract still calls deployed writer routing, orphan repair, crash/retry, restart, overlap races and leader handoff blockers.

* **“Queries read materialized output.”** The roadmap also says queries can replay missing source batches and read in-process runtime state.

* **“Ownership/fencing complete.”** The metadata document alternates between declaring production-safe capability bits and saying product-complete remains false.

* **“Compute nodes are replaceable.”** That has not been demonstrated while query serving depends on in-process runtime state and externally coordinated metadata pointers.

* **“Object-storage-first.”** Object storage is the artifact store, but Hiqlite is the hot admission/progress authority and process memory is described as a query surface. The authority model is hybrid, not object-store-first in the strong sense.

* **“Ultra-lightweight.”** Nine crates plus DataFusion, Arrow, Parquet, Foyer, SlateDB, Hiqlite, Kubernetes, gRPC and Sigstore is not an ultra-lightweight operational footprint.

* **“Two-relation joins are epoch-consistent.”** Current docs define a per-relation frontier-vector contract, but release still needs live crash/retry/recovery evidence that output manifests and checkpoints preserve it.

* **“Window foundation implemented.”** Named unit and REST tests do not establish cross-partition watermark aggregation, session merge/split recovery, retention, bounded state or adversarial replay correctness.

* **“Output changes are proportional to changed output keys.”** Join fan-out, session-window merges and extrema maintenance can perform work or retain state far beyond the number of changed output keys.

* **“S3-compatible tests complete.”** A compatibility harness does not prove production behavior under throttling, delayed operations, conditional-write races, credential rotation or whole-region failure.

* **“The source guard proves no Feldera/DBSP/JAR/PVC path.”** Current guards cover product Dockerfiles, runtime Rust source references, product release package dependency closure, Cargo manifests/lockfile, deployment scripts/workflows, the release-gate workflow and product-contract assertions. This is stronger than a source-reference lint, but still does not prove final image layers, every deployed binary’s runtime configuration or alternate API path without release-image evidence.

* **“Full workspace tests passed locally.”** Local command success on June 15, 2026 is development evidence, not release-scoped, image-bound or independently reproducible readiness evidence.

[Attached review source](sandbox:/mnt/data/attachments-bundle.txt)

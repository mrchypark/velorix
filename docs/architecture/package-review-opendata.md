# OpenData Package Review

Reviewed source:
[opendata-oss/opendata](https://github.com/opendata-oss/opendata) at
`0961492a72bc5911f44f782f774ede233af8778e` (2026-04-30).

## Fit Summary

OpenData is highly relevant to Velorix because it is explicitly organized as a
set of object-native databases over a shared storage and infrastructure
foundation. Its README names SlateDB as the common object-store-native storage
layer and OpenData as the common infrastructure layer for service
infrastructure, catalog, admin tooling, distributed state infrastructure,
configuration, and testing.

Velorix should treat OpenData as a product and architecture reference before
treating it as a dependency. The repository contains multiple database products,
shared crates, and RFCs. That makes it useful for design pressure, but too broad
to import wholesale into Velorix's core runtime without creating ownership
confusion.

## Strong Advantages for Velorix

### Object-Native Fleet Model

OpenData's strongest lesson is not one crate; it is the operational model:
standardize storage, service infrastructure, tooling, and testing across
different database surfaces. Velorix can use this to avoid becoming a one-off
streaming engine with bespoke admin and benchmark behavior.

Concrete Velorix pattern adoption:

- Keep one object-key and manifest policy across logs, state, query specs,
  table specs, and future connector offsets.
- Build one admin/inspection path for manifests, catalog objects, state refs,
  garbage-collection candidates, and benchmark output.
- Treat every specialized runtime feature as a layer above the same object
  storage authority.

### OpenData Buffer

OpenData Buffer is the most directly useful component. It describes stateless
producers that accumulate opaque entries, flush batch files to object storage,
and coordinate consumers through a manifest-backed queue.

Velorix should adopt this pattern before claiming product-grade ingest:

- Producers remain disposable.
- Object storage absorbs downstream slowness instead of coupling producer
  liveness to view execution speed.
- Consumers can resume from manifest/sequence state.
- Exactly-once is achievable only when the downstream database atomically stores
  both data and consumed sequence.

This maps cleanly to Velorix's existing immutable ingest batches and checkpoint
manifests. It should influence a future `velorix-runtime` write-buffer boundary,
but Velorix should not immediately replace its storage protocol with
`opendata-buffer` without checking API stability, object key compatibility, and
sequence-to-checkpoint semantics.

OpenData Buffer also makes a hard requirement visible: exactly-once delivery is
not provided by buffering alone. Velorix must atomically bind the consumed
buffer sequence to the downstream checkpoint manifest and output/state refs.
Until that mapping exists, a buffer is at-least-once durable ingest, not a
complete exactly-once product path.

### Write Coordination RFC

OpenData's write-coordination RFC is a direct answer to a future Velorix gap:
how to serialize writes, batch them, expose backpressure, and report durability
watermarks without corrupting in-memory state.

Reusable ideas:

- A single async coordinator assigns monotonically increasing epochs.
- Writes move through explicit durability levels such as applied, flushed, and
  durable.
- A frozen delta plus returned context allows non-blocking flushes.
- Readers can apply flush results incrementally if their local epoch matches,
  otherwise they rebootstrap from the durable snapshot.

Velorix should map these concepts onto its own vocabulary:

| OpenData concept | Velorix equivalent |
| --- | --- |
| Epoch | Engine logical epoch or ingest sequence, not necessarily manifest checkpoint version |
| Durable watermark | Manifest-published checkpoint version plus referenced state/output objects |
| Snapshot | Latest valid checkpoint manifest and SlateDB-backed state boundary |
| Frozen delta | Engine checkpoint payload or object-backed output batch |
| Backpressure | Query/write resource policy, pending flush size, object-store write lag |

### Common Encodings

OpenData's common-encodings RFC is valuable because object-native systems need
stable byte encodings for ordered storage. Velorix currently uses deterministic
object keys and JSON-heavy bootstrap payloads. As SlateDB usage deepens,
Velorix will need shared binary encodings for state keys and values.

Adopt the principle, not the exact format yet:

- Key encodings must preserve lexicographic order.
- Value encodings should be explicit about endianness and length limits.
- Encoding specs should be documented before they become migration debt.
- Property-based tests should protect ordering and round trips.

### Benchmarks

OpenData's benchmark RFC separates environment handling from workload design
and produces machine-readable output for CI/regression analysis. Velorix should
copy that shape for production readiness:

- Object-store initialization and cleanup are benchmark framework concerns.
- Workload definitions stay near the runtime or storage component under test.
- Output includes commit metadata and stable metric names.
- Regression analysis can be external; Velorix only needs consistent data.

## Risks and Non-Fit

- OpenData is early and broad. Its README still lists core shared foundation
  items such as service infrastructure, registry, admin tooling, benchmark
  frameworks, distributed mode, and shared ingest as roadmap or bigger ideas.
- Importing large OpenData crates could create a second storage authority if
  Velorix does not align object keys, manifests, and checkpoints first.
- OpenData currently uses SlateDB and Foyer versions that may not match
  Velorix's workspace versions. Direct dependency reuse should wait for a
  concrete integration target.
- OpenData's Buffer delivery semantics require the downstream database to
  atomically commit batch sequence with data. Velorix's exactly-once manifests
  can satisfy this eventually, but the mapping must be designed explicitly.
- The write-coordination RFC is intentionally non-distributed. Velorix still
  needs Kubernetes-native stream-partition leases and fencing before
  multi-worker writes are safe.

## Recommendation

Use OpenData as a reference for the next production-readiness documents and
implementation milestones:

1. `velorix-runtime` write buffer design based on object-backed batch files and
   manifest-backed queue semantics.
2. Write coordinator design with explicit epoch assignment, backpressure, and
   durability watermarks.
3. State key/value binary encoding specification before broader SlateDB layout
   work.
4. Object-store-backed benchmark framework with machine-readable output.
5. Admin inspection commands for queue manifests, checkpoint manifests,
   unreferenced objects, and state lifecycle.
6. A Kubernetes-native partition-owner fencing design before any distributed
   write coordinator is called product-ready.

Do not add an OpenData dependency until one of those milestones has a narrow API
fit, an adoption RFC, and a test proving it preserves Velorix's
object-storage authority.

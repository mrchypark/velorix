# Storage Contract

Velorix treats object storage as the durable database. Local compute, Foyer
cache directories, process memory, and worker-local files are disposable. A
worker has made durable progress only after it publishes a valid checkpoint
manifest in object storage.

This document describes the current prototype contract. It is readiness
documentation for the bootstrap implementation, not a claim that production
layout, compaction, lifecycle management, or multi-worker coordination are
finished.

## Object Key Layout

All externally accepted keys must parse through `ObjectKey` and use the `v1/`
namespace with no leading slash, empty path segment, or path-unsafe caller
segment.

| Object | Key form | Authority |
| --- | --- | --- |
| Ingest batch | `v1/ingest/{stream_id}/p={partition_id:010}/{start:020}-{end:020}.batch` | Immutable input delta object |
| State object | `v1/state/{owner}/p={partition_id:010}/chk={checkpoint_version:020}/{object_id}.state` | Checkpoint-referenced state payload |
| Temporary publish object | `v1/tmp/{checkpoint_version:020}/{attempt_or_object_id}/{kind}` | Non-authoritative staging location |
| Checkpoint manifest | `v1/checkpoints/{checkpoint_version:020}.manifest` | Authoritative progress marker |

Structured constructors in `crates/velorix-storage/src/object_key.rs` own these
formats. Call sites should not assemble storage paths with ad hoc string
formatting.

## Manifest Semantics

A `CheckpointManifest` is the only durable authority for stream progress. The
current schema includes:

- `schema_version`
- `checkpoint_version`
- sorted non-overlapping `input_ranges`
- referenced `state_objects`
- referenced `output_objects`
- `parent_checkpoint`
- caller-provided `created_at`

Manifest validation currently requires schema version 1, at least one input
range, at least one state object reference, monotonic parent linkage, nonempty
input and output ranges, checkpoint-matching state object metadata, and unique
object ids and object keys across state and output references.

The manifest checkpoint version is a publication/progress version. It is
separate from the incremental engine logical epoch. Current engine checkpoint
state is serialized as a versioned payload with `schema_version`,
`logical_epoch`, and `state`. Recovery still accepts legacy raw `DeltaBatch`
state objects by using the manifest checkpoint version as the best available
epoch fallback for those old payloads.

## Publication and Crash Windows

The publication order is:

1. Write immutable ingest batches or output objects.
2. Write checkpoint state objects.
3. Validate that all manifest-referenced state objects exist.
4. Publish the checkpoint manifest with create-only semantics.

Crash behavior follows from that order:

- Crash before state object write: no new manifest exists, so durable progress
  does not advance.
- Crash after state object write but before manifest publication: the state
  object is recoverable garbage because no manifest references it.
- Crash during manifest publication: recovery observes either the previous
  manifest or the fully written new manifest.
- Duplicate checkpoint publication is rejected by create-only manifest writes.

Object-store conditional create is used where the adapter supports it. Local
filesystem tests exercise the same create-only behavior.

## Replay and Recovery

Recovery loads the latest valid manifest, reads its state objects through the
current state-store boundary, reconstructs the `IncrementalEngine`, and replays
committed ingest batches whose offsets are not covered by the manifest input
ranges.

Replay boundaries are per stream and partition. If a manifest boundary falls
inside a committed batch range, replay fails instead of partially applying an
immutable batch.

## Package Boundaries

Velorix owns object key policy, manifest validation, exactly-once publication,
stream progress, and stateless recovery orchestration.

SlateDB backs the current minimal experimental state-store path for
checkpoint-versioned payloads. Broader SlateDB durable layout, LSM/SST policy,
compaction tuning, garbage collection integration, and state lifecycle design
remain future work.

DataFusion owns the current SQL/query planning and Arrow execution boundary over
in-memory `DeltaBatch` input. Runtime query calls can now recover materialized
state from object-backed checkpoint manifests and replay, then query that
recovered state through the same DataFusion `input` table. Minimal query policy
now covers SQL text size, output row caps, DataFusion batch size, and target
partitions. Persisted query services, direct object-backed scans, and broader
runtime resource policy remain future work.

Foyer owns the runtime local memory/disk object-cache internals behind the
Velorix cache wrapper. Cache reads verify object-store authority first, and
cache contents never prove durable progress.

Feldera DBSP semantics remain the target direction for incremental algebra. The
current `IncrementalEngine` boundary is DBSP-shaped but backed by prototype
operators until direct integration gates are satisfied.

## Garbage Collection

The current bootstrap implementation classifies unreferenced state and temporary
objects as recoverable garbage, but it does not implement production garbage
collection. A future collector must retain objects referenced by live manifests,
respect object-store listing consistency, and avoid deleting staging objects
that may still belong to an active publication attempt.

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
| Ingest batch | `v1/ingest/{stream_id}/p={partition_id:010}/{start:020}-{end:020}.batch` | Immutable input delta object; input-only namespace |
| State object | `v1/state/{owner}/p={partition_id:010}/chk={checkpoint_version:020}/{object_id}.state` | Checkpoint-referenced state payload |
| Output object | `v1/outputs/{stream_id}/p={partition_id:010}/chk={checkpoint_version:020}/{start:020}-{end:020}/{object_id}.output` | Manifest-referenced internal materialized output |
| Temporary publish object | `v1/tmp/{checkpoint_version:020}/{attempt_or_object_id}/{kind}` | Non-authoritative staging location |
| Checkpoint manifest | `v1/checkpoints/{checkpoint_version:020}.manifest` | Authoritative progress marker |
| Persisted query spec | `v1/queries/{query_id}.query.json` | Create-only query SQL/policy catalog object |
| Persisted table spec | `v1/tables/{table_id}.table.json` | Create-only Parquet scan URL catalog object |

Structured constructors in `crates/velorix-storage/src/object_key.rs` own these
formats. Call sites should not assemble storage paths with ad hoc string
formatting.

`v1/ingest` is reserved for committed input batches only. Internal
materialized outputs must use `v1/outputs`, carry the manifest checkpoint
version in both the key and `OutputObjectRef`, and are not replay candidates.
Fenced output refs may also carry the same optional `owner_claim` metadata used
for state refs. The field is omitted when absent so existing v1 refs remain
readable.
For v1 read compatibility, manifests whose output refs omit
`checkpoint_version` can still be deserialized, but validation does not treat
the missing metadata as authority. Legacy output refs that point at `v1/ingest`
are rejected as output key mismatches under the new contract.

The ingest object key layout remains stable. Current bootstrap code still
replays JSON `DeltaBatch` ingest payloads, but that durable payload shape is
deprecated by the accepted breaking direction: versioned Arrow IPC ingest
envelopes; see
[Ingest Storage Format Decision](ingest-storage-format-decision.md).

## Ingest API Acknowledgement Semantics

Ingest acknowledgement is scoped to durable input admission only. It must not
claim that SQL processing, materialized view updates, output publication, or
checkpoint publication has completed. View freshness and checkpoint progress
belong to separate status surfaces over checkpoint manifests.

A synchronous ingest API should use these meanings:

| Response | Meaning | Durability claim |
| --- | --- | --- |
| `201 Created` | A new canonical ingest batch object was created with create-only semantics. | The input batch is durable in object storage. |
| `200 OK` | A retry found the same canonical ingest batch identity with the same payload digest. | The input batch was already durable in object storage. |
| `409 Conflict` | The requested identity conflicts with an existing key, digest, idempotency mapping, or committed range. | No new durability claim. |
| `202 Accepted` | Future async admission only, before final batch object creation. | Not durable unless a separate durable admission record says so. |

`201 Created` is allowed only after the canonical `v1/ingest/...batch` object
write succeeds through the object-store create-only path. A crash before that
write commits must yield no successful durable acknowledgement. A crash after
the object write succeeds but before the client receives the response must be
recoverable through retry: if the retry presents the same batch identity and
payload digest, the API should return `200 OK`.

Idempotency must bind to the canonical ingest identity, not just to an opaque
request token. For the current deterministic batch-key path, the identity is
`stream_id`, `partition_id`, half-open offset range `[start, end)`, and payload
digest. Reusing the same idempotency key for a different stream, partition,
range, or digest is a conflict. Reusing the same object key with a different
payload digest is also a conflict.

Committed ingest ranges are half-open intervals. Adjacent ranges such as
`[0, 10)` and `[10, 20)` are allowed. Overlapping ranges for the same
stream/partition are rejected. Ingest admission must evaluate overlap against
committed `v1/ingest` batch objects, not against checkpoint manifests, because
manifests are processing progress authority rather than ingest admission
authority.

Every successful ingest response should include an explicit status body rather
than relying on the HTTP code alone. The durable synchronous shape is:

```json
{
  "ingest_status": "created",
  "durability": "object_created",
  "batch_key": "v1/ingest/orders/p=0000000000/00000000000000000000-00000000000000000100.batch",
  "stream_id": "orders",
  "partition_id": 0,
  "start_offset_inclusive": 0,
  "end_offset_exclusive": 100,
  "digest": "sha256:<hex>",
  "materialization_status": "pending",
  "checkpoint_version": null
}
```

`materialization_status` is advisory in the ingest response. It must not be the
source of truth for view freshness. A separate view or checkpoint status API
should report whether a view has processed a given stream/partition offset.

`202 Accepted` should remain future work until Velorix has a write buffer or
coordinator with a clear status endpoint and final states such as `created`,
`duplicate`, `conflict`, `failed`, and `expired`. If the accepted record itself
is not durable, the response body must say `durability: not_durable`. Foyer,
process memory, and worker-local files must never be used as the basis for a
persistent ingest acknowledgement.

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
input and output ranges, checkpoint-matching state and output object metadata,
and unique object ids and object keys across state and output references.
Genesis checkpoint 0 has no parent. Every non-genesis manifest must declare the
immediately preceding checkpoint as its parent.

Publication also validates the immediate parent lineage against durable object
storage authority. Before a child checkpoint manifest is created, the declared
parent manifest object must already be visible at its canonical checkpoint key,
the parent body must validate under the same manifest rules, and the parent
body key must match the object key that made it visible. The child must retain
input progress for every stream/partition present in the parent and must not
move either boundary behind the parent's covered range.

Listing manifests and selecting the latest manifest use the same fail-closed
lineage validation. An out-of-band manifest with a valid individual body but a
missing parent, invalid parent body/key consistency, dropped parent input
progress, or regressed input boundary is not skipped or treated as latest
authority; listing/latest return an error so recovery does not advance from an
invalid lineage.

State object references may carry an `owner_claim` with `owner_id` and
`owner_epoch`. This is distinct from the existing state ref `owner`, which
continues to mean the logical state/view owner used in the state object key.
The claim metadata is the storage-side contract for stale-worker detection and
structural progress authorization; it does not change `ObjectKey::state_object`
layout.

Output object references may also carry `owner_claim`. For unfenced and legacy
bootstrap publication the field remains optional, but fenced publication treats
missing or mismatched output claims as typed validation errors.

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
3. For non-genesis checkpoints, validate that the declared parent manifest is
   durably visible and that the child preserves the parent's input progress.
4. Validate that all manifest-referenced state and output objects exist.
5. Publish the checkpoint manifest with create-only semantics.

Manifest publication fails closed until every referenced state object is
readable through the state-store boundary and every referenced output object is
present in object storage. Non-genesis publication also fails closed until the
declared parent manifest is visible and valid as object-storage authority. Only
after those checks pass can the create-only manifest write make the checkpoint
durable authority.

State and output objects are written with create-only semantics. Duplicate
state or output object writes fail instead of overwriting a previously durable
payload.

Fenced state write, fenced output write, and fenced manifest publication APIs
accept the caller's current `owner_claim`. Fenced writes reject objects whose
embedded claim is missing or different from the requested claim before creating
the object. `publish_manifest_fenced` requires every state ref and every output
ref to carry the requested claim. It rejects input or output progress for any
partition absent from the set of state refs carrying that claim. It also
rejects a stale claim before creating a state object, output object, or
checkpoint manifest when an already published manifest for the same partition
carries a higher `owner_epoch` or the same epoch with a different `owner_id`.
Stale-owner detection considers both published state refs and published output
refs, so a newer output owner claim can fence older writers. Existing unfenced
local/bootstrap paths remain available for compatibility.

These checks are non-atomic storage-side stale-owner detection and structurally
unauthorized progress rejection. They are not production linearizable fencing.
A production fencing or marker-index commit protocol remains future design.

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

Kubernetes `Lease` acquisition, renewal, and `owner_epoch` assignment remain
future control-plane work. Kubernetes and etcd are not treated as the durable
database authority; object storage manifests remain the durable record that the
storage layer verifies.

## Replay and Recovery

Recovery loads the latest valid manifest, reads its state objects through the
current state-store boundary, reconstructs the `IncrementalEngine`, and replays
committed ingest batches whose offsets are not covered by the manifest input
ranges. Replay lists only the `v1/ingest` namespace; manifest-referenced
`v1/outputs` objects are durable output payloads, not committed input.

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

DataFusion owns the current SQL/query planning, validation, and Arrow execution
boundary over in-memory `DeltaBatch` input. Runtime query calls can now recover
materialized state from object-backed checkpoint manifests and replay, then
query that recovered state through the same DataFusion `input` table. Persisted
query service v0 writes validated JSON specs to object storage under
`v1/queries/{query_id}.query.json` using create-only semantics. Minimal query
policy now covers SQL text size, output row caps, DataFusion batch size, and
target partitions. Runtime also has a minimal direct Parquet object-backed scan
boundary that registers caller-provided object storage and Parquet object URLs
as DataFusion's `input` table. Persisted table catalog v0 writes JSON Parquet
scan URL specs to `v1/tables/{table_id}.table.json` using create-only
semantics; create validates the catalog id and URL shape but does not scan table
contents. Persisted view access v0 loads a stored query spec and a stored
object-backed Parquet table spec, then delegates SQL and Parquet execution to
DataFusion. Broader table layout, query scheduling/versioning, permissions, and
broader runtime resource policy remain future work.

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

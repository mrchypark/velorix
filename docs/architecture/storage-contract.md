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
| Persisted table spec | `v1/tables/{table_id}.table.json` | Create-only registry-backed table spec; raw Parquet URL specs are phase-0/dev-only |
| Relation spec | `v1/relations/{relation_id}/versions/{relation_version}.relation.json` | Create-only relation catalog object |
| Ownership claim | `v1/ownership/{stream_id}/p={partition_id:010}/epoch={owner_epoch:020}.claim` | Production distributed-writer epoch record |

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

The ingest object key layout remains stable. Current bootstrap code paths that
replay JSON `DeltaBatch` ingest payloads are known violations of the accepted
ingest contract and must be removed by the Arrow IPC envelope breaking slice.
No new durable ingest, recovery, query, or standing-view code may depend on JSON
`DeltaBatch` as an accepted storage format. See
[Ingest Storage Format Decision](ingest-storage-format-decision.md),
[Ingest Envelope V1](ingest-envelope-v1.md), and
[Legacy JSON DeltaBatch Removal](legacy-json-deltabatch-removal.md).

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
stream/partition are rejected only in modes where admission is serialized by a
single writer, durable admission index, or write coordinator. Deterministic
create-only object keys reject identical-key conflicts, but they do not
atomically reject different-key overlapping ranges. Production multi-writer
ingest must not advertise range-overlap `409 Conflict` until
`RangeAdmissionIndexV1` or an equivalent write coordinator is implemented. See
[Ingest Admission Contract](ingest-admission-contract.md).

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

Successful manifest publication also writes a best-effort checkpoint lifecycle
status record:

```text
v1/checkpoint-lifecycle/{checkpoint_version:020}.status.json
```

The current record is intentionally small: schema version, checkpoint version,
manifest key, manifest digest, `published` status, and status update time. The
checkpoint manifest remains the durable authority; the lifecycle record is an
admin/status surface and must match the manifest digest before readers attach it
to a checkpoint inspection result.

Admin inspection can list checkpoint manifests, validate each manifest body,
key, lineage, referenced state, and referenced outputs, report invalid future
manifests with reasons, and return the latest valid checkpoint for repair or
read-only diagnosis. For lineage diagnostics, a structurally valid parent
manifest remains usable as parent evidence even when that older manifest's own
raw state/output payloads have been GC-deleted; each manifest's own payload
availability still determines whether that manifest is reported valid. After a
GC run evidence object is written and read back successfully, GC also writes
append-only retention evidence for non-retained checkpoints whose raw
state/output payloads were deleted or whose SlateDB logical state refs were
released:

```text
v1/checkpoint-retention/{checkpoint_version:020}.retention.json
```

The retention record includes schema version, checkpoint version, manifest key,
manifest digest, GC run id, policy, retained manifest versions, deleted
candidate keys, and timestamp. Admin inspection attaches retention evidence
only when the record validates and its manifest digest matches the inspected
manifest. Successful checked recovery can also write append-only transition
evidence under
`v1/checkpoint-recovery/{checkpoint_version:020}/transitions/{transition_id}.transition.json`;
that record is digest-bound to the recovered manifest and records the recovery
mode and replay counts. It does not yet implement lifecycle transitions beyond
`published`, manifest deletion, compaction state, repair, or authoritative
recovery-mode state changes.

State object references may carry an `owner_claim` with `owner_id` and
`owner_epoch`. This is distinct from the existing state ref `owner`, which
continues to mean the logical state/view owner used in the state object key.
The claim metadata is the storage-side contract for stale-worker detection and
structural progress authorization; it does not change `ObjectKey::state_object`
layout.

Output object references may also carry `owner_claim`. For unfenced and legacy
bootstrap publication the field remains optional, but fenced publication treats
missing or mismatched output claims as typed validation errors. Production
distributed writes require
[Partition Ownership Protocol V1](partition-ownership-protocol-v1.md);
Kubernetes Lease acquisition alone is not sufficient to make `owner_epoch`
durable or monotonic.

The manifest checkpoint version is a publication/progress version. It is
separate from the incremental engine logical epoch. Current engine checkpoint
state is serialized as a versioned payload with `schema_version`,
`logical_epoch`, and `state`. Any remaining recovery path for legacy raw
`DeltaBatch` state objects is bootstrap-only disposable scaffolding, not a
production compatibility promise. The SlateDB/raw-state breaking slice must
delete that fallback or hide it behind an explicit migration flag before
production publication.

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
Production distributed writes require a durable epoch record reference in every
fenced state write, output write, and manifest publication.

Crash behavior follows from that order:

- Crash before state object write: no new manifest exists, so durable progress
  does not advance.
- Crash after state object write but before manifest publication: the state
  object is recoverable garbage because no manifest references it.
- Crash during manifest publication: recovery observes either the previous
  manifest or the fully written new manifest.
- Duplicate checkpoint publication is rejected by create-only manifest writes.

Production Velorix requires create-only or equivalent conditional write
semantics for every authoritative namespace. "Where the adapter supports it" is
not sufficient for production. If the configured backend cannot prove the
required capability set, startup must fail closed. Local filesystem emulation is
dev/test-only and must not be used as evidence that an S3-compatible backend
satisfies the contract. See
[Object Store Capabilities V1](object-store-capabilities-v1.md).

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
`CheckpointRecoveryIndexV1` may accelerate latest lookup, but it is advisory and
must be validated against immutable checkpoint manifests before use.

Replay boundaries are per stream and partition. If a manifest boundary falls
inside a committed batch range, replay fails instead of partially applying an
immutable batch.

## Package Boundaries

Velorix owns object key policy, manifest validation, exactly-once publication,
stream progress, and stateless recovery orchestration.

SlateDB backs the current minimal experimental state-store path for
checkpoint-versioned payloads. Production state references must distinguish
bootstrap raw state objects from SlateDB checkpoint/root handles. Velorix GC
must not delete SlateDB internal objects by prefix walking. See
[State Substrate Contract](state-substrate-contract.md).

DataFusion owns SQL/query planning, validation, and Arrow execution. The
existing DataFusion-over-`DeltaBatch` path is bootstrap-only and scheduled for
removal from durable ingest/replay. The accepted production boundary is typed
Arrow relation input driven by [Relation Contract V1](relation-contract-v1.md).
Persisted query/table/view execution requires
[DataFusion Resource Policy V1](datafusion-resource-policy-v1.md). Raw
caller-provided Parquet URLs are phase-0/dev-only; production external table
surfaces use registry-backed table specs described in
[External Table Surface Contract](external-table-surface-contract.md).

Foyer owns the runtime local memory/disk object-cache internals behind the
Velorix cache wrapper. Cache reads verify object-store authority first, and
cache contents never prove durable progress.

Feldera DBSP semantics remain the target direction for incremental algebra. The
current `IncrementalEngine` boundary is DBSP-shaped but backed by prototype
operators until direct integration gates are satisfied.

## Garbage Collection

The current implementation can build a deterministic manifest-aware GC plan and
execute that plan for Velorix-owned raw state objects under `v1/state/...`,
output objects under `v1/outputs/...`, and manifest-retired
`SlateDbCheckpointRefV1` state refs. Raw state/output candidates are deleted
through the object-store API. SlateDB candidates release only the logical state
key and Velorix marker key through the SlateDB state store; GC still does not
walk or delete SlateDB internal object-store prefixes. The plan retains objects
referenced by the latest N published manifests, where N must be at least one,
and classifies only unreferenced raw state/output objects and retired SlateDB
state refs as candidates. Executed runs can
write a stable `GcRunV1` evidence object under `v1/gc-runs/...` with the policy,
plan, deleted candidates, and skipped candidates. The execute path reports
success only after reading the persisted run evidence back and validating its
schema version and `run_id`. Before deleting candidates for an evidenced run,
the execute path recomputes the plan from the supplied policy and rejects
mismatched or stale caller plans so the persisted policy and executed plan
describe the same retention decision.
Storage also exposes a release-evidence prerequisite verifier that re-reads a
persisted `GcRunV1`, requires the run object to be visible in the `v1/gc-runs`
listing, and checks that any expected checkpoint retention records still match
the run's deleted checkpoint payloads. This verifier is a storage consistency
boundary only; it is not a production GC command or live backend attestation by
itself.

Manifest objects outside the latest-N retention set can remain listed and
readable after GC, but their Velorix-owned raw state and output payloads are not
part of the retained recovery set. Operators and recovery code must treat those
older manifests as historical metadata only unless their referenced payloads are
still available for some other reason. Admin inspection may still use those
older manifest bodies as structural parent-lineage evidence for newer retained
checkpoints. Retention evidence records the GC run that removed payloads for
non-retained checkpoints; broad manifest lifecycle retirement, manifest
deletion, and compaction remain future work.

This is not a broad production GC service. It does not collect staging
`v1/tmp/...` objects, does not add object-store listing-consistency controls,
and does not delete SlateDB internal objects by prefix walking. Broader SlateDB
retention handles, manifest deletion, and compaction policy remain future work.

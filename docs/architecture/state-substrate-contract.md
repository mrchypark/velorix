# State Substrate Contract

Status: Accepted
Applies to: runtime checkpoint state, object references, recovery, and GC.

Object storage holds Velorix runtime checkpoint state as immutable Arrow IPC
objects. Rhiza stores metadata and its snapshot uses the shared Arrow snapshot
format; Rhiza is not the runtime-state object store. Checkpoint manifests bind
the state object references needed to restore the materialized runtime.

## State Objects

State payloads use the versioned Arrow formats described in
[Checkpoint State Arrow V1](checkpoint-state-arrow-v1.md). Writers publish
create-only objects under the Velorix `v1/state/` namespace. A manifest
reference binds the object key, checkpoint identity, and state commitment so
recovery can verify the bytes it reads.

The object-store API is the authority for state bytes and visibility. Local
files, process memory, and caches are disposable. Rhiza provides the metadata
service and its own replicated KV snapshots; it does not own or interpret the
runtime checkpoint state objects.

## Recovery and Retention

- Recovery starts from the authoritative checkpoint pointer and validates its
  manifest before reading referenced Arrow state objects.
- State decoding validates the supported Arrow format, size bounds, identity,
  and commitment before restoring runtime state.
- A missing, corrupt, or mismatched state object fails recovery closed.
- GC retains every state object reachable from retained checkpoints and deletes
  only manifest-unreferenced Velorix objects selected by the retention policy.
- GC operates through the object-store API and does not walk unrelated key
  prefixes.
- Existing stores with earlier state encodings require a separately planned
  rebuild or migration before the current readers can consume them.

## Verification

Arrow encoding, bounds, identity checks, fresh-process recovery, and Rhiza
snapshot compatibility are covered by the storage and metadata test suites.
Checkpoint lineage, object visibility, and retention evidence are described in
[Checkpoint Recovery Index V1](checkpoint-recovery-index-v1.md) and
[Storage Contract](storage-contract.md).

Current implementation entry points are
[state.rs](../../crates/velorix-storage/src/state.rs) for manifest publication,
inspection and recovery validation, and
[checkpoint_publication.rs](../../crates/velorix-api/src/checkpoint_publication.rs)
for standing-runtime checkpoint publication. Runtime checkpoint pointer fields
are separately defined by the Meta RPC in
[meta.proto](../../crates/velorix-meta/proto/velorix/meta/v1/meta.proto);
metadata references object keys, hashes and manifest refs rather than owning
the large runtime state body. Source and local test coverage do not qualify
provider-wide durability or whole-DAG recovery.

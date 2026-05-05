# State Substrate Contract

Status: Accepted
Applies to: checkpoint manifest state references, SlateDB integration, and GC.

Velorix manifests own stream progress and exactly-once publication. SlateDB owns
the durable state substrate. Velorix must not take ownership of SlateDB internal
object layout.

## State Reference Types

`RawStateObjectRefV1` is bootstrap-only raw state payload:

```text
v1/state/{owner}/p={partition_id:010}/chk={checkpoint_version:020}/{object_id}.state
```

It is Velorix-owned raw state and should be disabled in production except under
an explicit bootstrap/migration flag.

`SlateDbCheckpointRefV1` is the production direction. Current manifests store
it as optional `StateObjectRef.slatedb` metadata when `ref_type` is
`slate_db_checkpoint`, keeping legacy/raw JSON compatible. It references a
SlateDB-owned state key under a SlateDB database path, not a list of SlateDB
internal object keys. The V1 metadata contains exactly:

- `db_path`
- `state_key`
- `state_digest`
- `state_bytes`
- `created_by_checkpoint_version`

The current implementation writes `state_digest` as a `sha256:`-prefixed digest
of the state bytes and verifies digest and byte length on read.

For publication-time validation, the SlateDB state store also writes a small
Velorix-owned marker key under the SlateDB keyspace:

```text
__velorix_state_ref_v1/{sha256(state_key)}
```

The marker is written in the same SlateDB transaction as the state payload and
contains the same V1 metadata as the manifest ref. Manifest publication checks
the marker metadata and verifies that the payload key is still readable, but it
does not re-hash the full state payload. Payload byte length and digest
validation remain on the recovery/read boundary. The marker is a publication
guard, not a retention handle.

## GC and Recovery

- Manifest `state_objects` must become a tagged reference type.
- Recovery dispatches by state reference type.
- Reading a SlateDB checkpoint ref through the raw store fails closed.
- Reading a raw ref through the SlateDB store fails closed.
- SlateDB reads require metadata presence and matching `db_path`, `state_key`,
  checkpoint version, byte length, and digest.
- SlateDB publication validation requires a matching marker and readable payload
  key, and fails closed for missing or mismatched marker metadata.
- Closing and reopening the SlateDB store can recover state written through the
  returned checkpoint ref.
- Current Velorix GC planning/execution is limited to Velorix-owned raw
  `v1/state/...` objects and `v1/outputs/...` objects not referenced by retained
  manifests.
- Velorix GC must never delete SlateDB internal objects by prefix walking.
- SlateDB state retention and release must use SlateDB-owned APIs or handles.
- Mixed raw-to-SlateDB lineage must be explicit and tested.

## Verification

- A manifest containing a `SlateDbCheckpointRefV1` can recover state through the
  SlateDB store, including after local close/reopen.
- Publication validation uses the small marker path plus payload-key existence,
  and leaves full payload digest validation to read/recovery.
- GC does not issue direct deletes for SlateDB internal prefixes; current tests
  cover an internal-looking non-Velorix prefix remaining untouched by plan
  execution.
- Production publication rejects raw state refs without a bootstrap flag.
- `ref_type` is required outside migration mode.

## Remaining Gaps

- SlateDB retention and release are still not wired through SlateDB-owned APIs.
- Mixed raw-to-SlateDB lineage remains a migration concern and is not a general
  production recovery path.
- The current V1 handle is intentionally narrow: it proves recoverable state
  bytes and manifest metadata integrity, not a full multi-handle SlateDB
  checkpoint retention protocol.

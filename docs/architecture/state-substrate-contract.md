# State Substrate Contract

Status: Accepted
Applies to: checkpoint manifest state references, SlateDB integration, and GC.

Velorix manifests own stream progress and exactly-once publication. SlateDB owns
the durable state substrate. Velorix must not take ownership of SlateDB internal
object layout.

## State Reference Types

`RawStateObjectRefV1` is bootstrap-only raw state payload:

```text
v1/state/raw/{owner}/p={partition_id:010}/chk={checkpoint_version:020}/{object_id}.state
```

It is Velorix-owned raw state and should be disabled in production except under
an explicit bootstrap/migration flag.

`SlateDbCheckpointRefV1` is the production direction. It references a
SlateDB-owned checkpoint or root handle, not a list of SlateDB internal object
keys. The manifest stores the handle identity, state codec, state schema
version, owner, partition, and checkpoint version.

## GC and Recovery

- Manifest `state_objects` must become a tagged reference type.
- Recovery dispatches by state reference type.
- Current Velorix GC planning/execution is limited to Velorix-owned raw
  `v1/state/...` objects and `v1/outputs/...` objects not referenced by retained
  manifests.
- Velorix GC must never delete SlateDB internal objects by prefix walking.
- SlateDB state retention and release must use SlateDB-owned APIs or handles.
- Mixed raw-to-SlateDB lineage must be explicit and tested.

## Verification

- A manifest containing only `SlateDbCheckpointRefV1` can recover state.
- GC does not issue direct deletes for SlateDB internal prefixes; current tests
  cover an internal-looking non-Velorix prefix remaining untouched by plan
  execution.
- Production publication rejects raw state refs without a bootstrap flag.
- `ref_type` is required outside migration mode.

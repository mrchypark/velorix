# Legacy JSON DeltaBatch Removal

Status: Accepted
Applies to: durable ingest, replay, query input, and standing-view integration.

JSON `DeltaBatch` was a bootstrap implementation detail. It is not an accepted
durable ingest format and must not be preserved as a compatibility path.

## Document States

| State | Meaning |
| --- | --- |
| Current bootstrap implementation | Code that may still exist temporarily. Not a production contract. |
| Accepted breaking contract | Direction that new implementation must follow. |
| Removed legacy contract | Contract that must be deleted or rejected. |

Durable JSON `DeltaBatch` ingest is a removed legacy contract. Versioned Arrow
IPC ingest envelopes are the accepted breaking contract.

## Rules

- `v1/ingest` durable input, replay, and recovery must not decode JSON
  `DeltaBatch`. JSON remains in request/response and internal runtime/output
  representations where their own contracts use it; those paths do not make it
  an ingest-batch compatibility format.
- `serde_json::from_slice::<DeltaBatch>` on `v1/ingest` payloads must be removed
  by the ingest envelope breaking slice.
- Existing JSON fixtures and local object-store data are disposable and should
  be regenerated, not migrated.
- `DeltaBatch` remains an internal runtime representation and may be serialized
  by output/checkpoint paths; it is not the durable source-ingest payload.
- `key_json`/`value_json` are not production durable relation columns.

## Verification

- Raw JSON bytes under a `v1/ingest` key fail validated replay with a typed
  `MalformedEnvelope` error; this is rejection, not a dedicated legacy-format
  or unsupported-version error.
- Arrow envelope fixture under the same key shape recovers successfully.
- A grep or lint check rejects durable ingest code paths that decode JSON
  `DeltaBatch`.
- Documentation lint rejects authoritative wording that describes JSON
  `DeltaBatch` as a durable ingest format.

Current codec and replay evidence: [ingest_envelope.rs](../../crates/velorix-storage/src/ingest_envelope.rs)
and [ingest_envelope.rs tests](../../crates/velorix-storage/tests/ingest_envelope.rs).

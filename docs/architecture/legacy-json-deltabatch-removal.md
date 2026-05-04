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

- No new durable ingest, recovery, query, or standing-view code may depend on
  JSON `DeltaBatch`.
- `serde_json::from_slice::<DeltaBatch>` on `v1/ingest` payloads must be removed
  by the ingest envelope breaking slice.
- Existing JSON fixtures and local object-store data are disposable and should
  be regenerated, not migrated.
- `DeltaBatch` may remain only as internal prototype scaffolding or tests that
  are not wired to durable ingest.
- `key_json`/`value_json` are not production durable relation columns.

## Verification

- JSON bytes under a `v1/ingest` key fail recovery with an unsupported envelope
  error.
- Arrow envelope fixture under the same key shape recovers successfully.
- A grep or lint check rejects durable ingest code paths that decode JSON
  `DeltaBatch`.
- Documentation lint rejects authoritative wording that describes JSON
  `DeltaBatch` as a durable ingest format.

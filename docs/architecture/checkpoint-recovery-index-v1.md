# Checkpoint Recovery Index V1

Status: Accepted
Applies to: latest checkpoint lookup, recovery, admin inspection, and selected
read-only recovery.

Immutable checkpoint manifests remain the durable authority:

```text
v1/checkpoints/{checkpoint_version:020}.manifest
```

Latest indexes are advisory acceleration only.

## Latest Candidate

An optional marker may point at a candidate latest checkpoint:

```text
v1/checkpoint-index/latest-candidate.json
```

It includes candidate checkpoint version, manifest key, manifest digest,
validated parent checkpoint, and diagnostic update time. Recovery must read and
validate the referenced manifest body, key, digest, parent, input, state, and
output invariants before using it.

Missing, stale, or corrupt marker bodies fall back to manifest listing. Object
store I/O errors while reading the marker or validating future checkpoint
visibility fail closed; they are not treated as advisory marker corruption. A
corrupt future manifest must be visible to admin inspection so it does not
permanently hide the last known good checkpoint.

## Modes

- `strict`: fail closed on invalid lineage.
- `admin_inspect`: report invalid manifests and last known good checkpoint.
- `last_known_good_read_only`: allow read-only recovery from an admin-selected
  valid checkpoint.

`velorix-cli recover-local --object-store-dir <path> --checkpoint-version <n>`
starts recovery from an admin-selected published checkpoint after validating
the manifest body/key/version, parent lineage, referenced payloads, and the
digest-bound `published` lifecycle record. Recovery then replays durable ingest
after that checkpoint boundary. SlateDB-backed checkpoint state is explicit:
`--slatedb-state-path <object-store-db-path>` opens the SlateDB state substrate
for selected-checkpoint or latest-checkpoint recovery; the path is the
object-store database path stored in the checkpoint ref, not another local
object-store root.

## Lifecycle Status

Published checkpoints may have a companion lifecycle record:

```text
v1/checkpoint-lifecycle/{checkpoint_version:020}.status.json
```

The current implementation writes `published` lifecycle records after manifest
publication and treats them as status metadata, not authority. Admin inspection
attaches the lifecycle status only when the record validates and its manifest
digest matches the inspected manifest. Missing, corrupt, or mismatched lifecycle
records do not make a valid manifest invalid.

`velorix-cli checkpoint-inspect-local --object-store-dir <path>` exposes the
read-only admin inspection path for local object-store directories. It reports
the latest valid checkpoint and each visible manifest with lifecycle/status
diagnostics. When GC leaves an older parent manifest listed but deletes that
parent's raw state/output payloads, inspection may still use the parent
manifest body as structural lineage evidence for newer retained checkpoints;
the older manifest itself is reported invalid if its own payloads are missing.
When a GC run has successfully persisted and read back its `GcRunV1` evidence,
inspection also reports digest-matched retention evidence from:

```text
v1/checkpoint-retention/{checkpoint_version:020}.retention.json
```

Retention evidence is an admin surface only. It records which GC run and policy
removed payloads for a non-retained checkpoint; it does not repair, rewrite,
delete, or recover from a checkpoint.

## Verification

- Valid marker fast path avoids full listing after manifest validation.
- Missing, stale, or corrupt marker bodies fall back to listing.
- Marker read I/O errors and future-checkpoint listing errors fail closed.
- Strict mode fails on invalid lineage.
- Admin inspect identifies last known good checkpoint when a future manifest is
  corrupt.
- Admin inspect treats GC-retired parent payloads as invalid for that older
  manifest without invalidating a newer retained child checkpoint whose own
  payloads remain available.
- Local CLI admin inspect prints deterministic read-only checkpoint diagnostics.
- Published lifecycle records are readable and digest-bound to their manifest.
- Retention evidence is readable after successful GC evidence read-back and is
  attached only when digest-bound to the inspected manifest.
- Selected-checkpoint recovery requires a matching published lifecycle digest
  record and validates referenced state/output payload visibility.
- SlateDB selected-checkpoint local recovery opens state through
  `--slatedb-state-path` and rejects raw state paths on the SlateDB recovery
  path.
- Large-manifest-count benchmark records lookup memory and latency.

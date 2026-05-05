# Checkpoint Recovery Index V1

Status: Accepted
Applies to: latest checkpoint lookup, recovery, and admin repair.

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

Missing, stale, or corrupt markers fall back to manifest listing. A corrupt
future manifest must be visible to admin inspection so it does not permanently
hide the last known good checkpoint.

## Modes

- `strict`: fail closed on invalid lineage.
- `admin_inspect`: report invalid manifests and last known good checkpoint.
- `last_known_good_read_only`: allow read-only recovery from an admin-selected
  valid checkpoint.

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
diagnostics. It does not repair, rewrite, or recover from a checkpoint.

## Verification

- Valid marker fast path avoids full listing after manifest validation.
- Missing, stale, or corrupt marker falls back to listing.
- Strict mode fails on invalid lineage.
- Admin inspect identifies last known good checkpoint when a future manifest is
  corrupt.
- Local CLI admin inspect prints deterministic read-only checkpoint diagnostics.
- Published lifecycle records are readable and digest-bound to their manifest.
- Large-manifest-count benchmark records lookup memory and latency.

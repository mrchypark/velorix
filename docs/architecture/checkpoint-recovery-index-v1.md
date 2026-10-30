# Checkpoint Recovery Index V1

Status: Accepted
Applies to: latest checkpoint lookup, recovery, admin inspection, and selected
read-only recovery.

Immutable checkpoint manifests remain the durable authority:

```text
v1/checkpoints/{checkpoint_version:020}.manifest
```

Latest indexes are advisory acceleration only.

## Storage checkpoint manifest invariants

The storage type is `velorix_storage::manifest::CheckpointManifest`; it binds
checkpoint version, input ranges, state/output object references, optional
parent checkpoint, and optional relation identity/schema fingerprint. Its
owner claims are attached to state/output references where applicable. It does
not itself carry an admitted plan hash, output schema, query-visible version,
or a content-addressed parent hash. See the separate API `RuntimeCheckpoint`
contract below for runtime identity, input frontiers, state root, output
manifest references, owner epoch, input coverage, and causal cut.

The storage latest-candidate marker accelerates lookup of a storage
`CheckpointManifest`; it does not replace that manifest or its object refs as
authority. The API's standing-runtime checkpoint pointer selects an immutable
Arrow checkpoint record and validates that record separately. Do not treat
these two checkpoint layouts or pointer paths as one interchangeable wire
format.

Implementation references: [`manifest.rs`](../../crates/velorix-storage/src/manifest.rs),
[`standing_program.rs`](../../crates/velorix-core/src/standing_program.rs),
[`checkpoint_index.rs`](../../crates/velorix-storage/src/checkpoint_index.rs),
and [`checkpoint_publication.rs`](../../crates/velorix-api/src/checkpoint_publication.rs).

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

## Lookup and recovery behavior

`CheckpointRecoveryMode` currently distinguishes `latest_candidate` and
`selected_checkpoint` recovery-transition evidence. `strict`, `admin_inspect`,
and `last_known_good_read_only` are descriptive behaviors in this document, not
variants of that Rust enum. Latest lookup fails closed on storage errors and
invalid lineage; admin inspection reports visible manifests and the latest
valid checkpoint. Do not infer that admin inspection itself restores a runtime.

The public CLI no longer starts engine recovery directly. It exposes local
inspection and admin metadata repair commands only; runtime recovery must happen
inside the runtime/API process that owns the materializer and checkpoint
authority. Runtime recovery reads the Arrow state objects named by the
validated checkpoint manifest through the shared object-store configuration.

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
diagnostics. JSON inspection output uses `schema_version=3` when reporting
diagnostics, retention evidence, GC transition records, and recovery transition
records. When GC leaves an older parent manifest listed but
deletes that parent's raw state/output payloads, inspection may still use the parent
manifest body as structural lineage evidence for newer retained checkpoints;
the older manifest itself is reported invalid if its own payloads are missing.
`velorix-cli checkpoint-repair-local --object-store-dir <path>` is the local
admin repair path for digest-bound status and advisory lookup metadata. It
rewrites missing, corrupt, or digest-mismatched `published` lifecycle records
only for manifests that pass admin inspection plus state/output payload
validation, then rebuilds `v1/checkpoint-index/latest-candidate.json` from the
latest validated manifest. It does not rewrite manifests, retention records, GC
transitions, or recovery transitions.
When a GC run has successfully persisted and read back its `GcRunV1` evidence,
inspection also reports digest-matched retention evidence from:

```text
v1/checkpoint-retention/{checkpoint_version:020}.retention.json
```

Retention evidence is an admin surface only. It records which GC run and policy
removed payloads for a non-retained checkpoint; it does not repair, rewrite,
delete, or recover from a checkpoint.

After retention evidence is written, GC may also emit deterministic append-only
GC transition evidence:

```text
v1/checkpoint-gc-transitions/{checkpoint_version:020}/transitions/{transition_id}.transition.json
```

The transition id is deterministic for the GC run so retries cannot inflate
admin evidence. The transition record is digest-bound to the checkpoint
manifest, the verified `GcRunV1` body, and the retention record body. Admin
inspection attaches it only when the matching GC run and retention record are
still readable and digest-matched. It is inspection evidence only:
latest-checkpoint lookup continues to depend on manifest lineage and payload
visibility, while selected-checkpoint recovery validity depends on the immutable
checkpoint manifest plus the `published` lifecycle status record, not on GC
transition presence.

Successful checked recovery from a published checkpoint writes append-only
recovery transition evidence:

```text
v1/checkpoint-recovery/{checkpoint_version:020}/transitions/{transition_id}.transition.json
```

The transition record is digest-bound to the recovered checkpoint manifest and
records the recovery mode, replay checkpoint count, replayed batch count, and
timestamp. It is readiness/admin evidence that recovery crossed a validated
checkpoint boundary; it is not checkpoint authority, does not mutate lifecycle
status, and does not claim broader compaction, repair, or manifest deletion
policy.

## Upgrade, Repair, And GC Reachability Contract

Supported release N must read release N-1 checkpoint manifests, output
manifests/pages/deltas, and state payload refs before an upgrade or rollback can
be called release-ready. Repair may restore only from the last valid published
checkpoint plus durable admitted replay up to the authoritative frontier; it
must not rewrite immutable manifests or reconstruct query output through source
queries.

GC reachability roots are the authoritative latest checkpoint pointers, the
predecessor chain required by the supported upgrade/rollback window, durable
admitted replay lower bounds, explicit repair holds, active reader generations,
and materialized output compaction source/output manifests. A candidate outside
those roots may be deleted only after the GC plan, persisted run evidence,
retention evidence, and transition evidence agree on the same manifest digests
and policy.

Current local evidence is contract/admin evidence only. It does not satisfy the
live upgrade, rollback, repair, and GC fault-injection matrix required to remove
the `docs/architecture-critique.md` recovery blocker.

Release readiness also requires S3-compatible delayed-visibility, retry, and
fault-injection checkpoint matrix evidence proving metadata CAS and the
object-store object set cannot publish a mixed checkpoint. Local object-store or
Versity Gateway-only checkpoint evidence does not satisfy this release gate.

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
- Local CLI checkpoint repair restores digest-bound `published` lifecycle
  records for validated manifests and rewrites only the advisory latest marker
  from the latest validated manifest.
- Published lifecycle records are readable and digest-bound to their manifest.
- Retention evidence is readable after successful GC evidence read-back and is
  attached only when digest-bound to the inspected manifest.
- GC transition evidence is deterministic, digest-bound to the manifest, GC run,
  and retention record, and ignored by inspection when causal evidence is absent
  or mismatched.
- Selected-checkpoint recovery requires a matching published lifecycle digest
  record and validates referenced state/output payload visibility.
- Checked recovery writes digest-bound append-only recovery transition evidence
  after successful recovery from a published checkpoint.
- Selected-checkpoint recovery reads manifest-bound Arrow state objects from
  object storage and validates their identity and commitment before restore.
- Large-manifest-count benchmark records lookup memory and latency.

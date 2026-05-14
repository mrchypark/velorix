# Ingest Admission Contract

Status: Accepted
Applies to: ingest API acknowledgement, idempotency, and range conflict
semantics.

Ingest acknowledgements cover durable input admission only. They do not claim
that SQL processing, standing views, output publication, or checkpoint
publication has completed.

## Admission Modes

`SingleWriter` mode assumes one writer or external coordinator serializes
admission for each stream/partition. In this mode Velorix may reject overlaps by
checking committed ranges, but the guarantee depends on serialized admission.
`IngestLog::append_validated_envelope_single_writer` implements this narrow
single-writer admission path for validated Arrow envelopes: adjacent ranges are
allowed, same-key retries remain idempotent, and visible committed range
overlaps return `range_overlap_committed`.

Production callers should use the catalog-aware variants:
`IngestLog::append_catalog_validated_envelope` and
`IngestLog::append_catalog_validated_envelope_single_writer`. These read the
persisted relation catalog before the create-only ingest write, require the
envelope relation id/version and schema fingerprint to match the catalog, and
validate Arrow batch schemas against the catalog. The older
`append_validated_envelope` variants remain bootstrap/dev compatibility
surfaces and do not prove relation-catalog admission.

`IngestAdmissionCoordinator` is a process-local write coordinator for
catalog-aware envelopes. It serializes admission per `(stream_id, partition_id)`
inside one coordinator instance, records immutable Velorix-owned serialized
admission evidence under `v1/ingest-admission/...`, rechecks committed and
reserved ranges while guarded, rejects visible overlaps, permits adjacent
ranges, and preserves same-digest retry idempotency. This is storage plumbing
for deployed admission and local/runtime coordinator evidence only; object-store
range records alone do not provide a distributed admission index across
processes or pods.

Production `Coordinated` mode uses a deployed write coordinator as the
serialization authority and `v1/ingest-admission` records as durable database
evidence and restart-reconstruction input. This is required before Velorix
advertises production multi-writer range-overlap rejection.

`AsyncBuffered` mode is future work. It may return `202 Accepted` only with a
separate admission status lifecycle and must not claim persistent ingest before
the canonical batch object is created.

## Admission-Before-Batch Orphans

An admission-before-batch orphan is a `v1/ingest-admission/...` record whose
canonical `v1/ingest/...` batch object is not visible. Checked recovery must
not replay that reservation as data. The deployed coordinator must treat the
record as a reservation until an operator explicitly repairs or expires it.

Before live writer cutover, operators must run an admission-repair inspection
against the same authority store and namespace prefix used by the deployed
coordinator:

1. List `v1/ingest-admission/{stream_id}/p=.../ranges/...` and decode each
   admission record.
2. For each admission record, use the recorded batch key, or derive the
   canonical ingest batch key from the recorded stream, partition, and range.
3. If the batch exists and its digest matches the admission record, classify
   the reservation as `committed`.
4. If the batch is missing, classify it as `orphan_reserved` and keep rejecting
   overlapping writes until an operator either replays the exact same payload
   through the coordinator or writes a durable expiry/repair decision for that
   reservation.
5. If the batch exists, validate the digest, relation identity, and schema
   fingerprint against the admission record. Any mismatch is
   `corrupt_conflict`; do not repair automatically, and block writer cutover
   until the operator resolves the authority-store conflict.

Expiry is not a time-only delete. An expiry decision must be a durable
Velorix-owned record bound to the admission record digest, reason, operator
identity, and observed missing batch key. It must be read during deployed
coordinator restart reconstruction before the coordinator can stop reserving
the orphaned range. Until that expiry record type and deployed coordinator
reconstruction path exist, `orphan_reserved` remains a production cutover
blocker.

## Conflict Semantics

Create-only object writes only reject identical-key conflicts. They do not
atomically reject different-key overlapping ranges such as `[0, 100)` and
`[50, 150)`. Production multi-writer ingest must not advertise range-overlap
`409 Conflict` until `RangeAdmissionIndexV1` or an equivalent write coordinator
exists.

Conflict reasons must be explicit:

- `same_key_different_digest`.
- `idempotency_key_reused`.
- `range_overlap_committed`.
- `range_overlap_reserved`.
- `unsupported_overlap_guarantee`.

## Verification

- Concurrent `[0,100)` and `[50,150)` admission rejects one request in the
  process-local coordinator.
- A separate coordinator rejects a range already reserved by durable serialized
  admission evidence before any batch object is committed.
- Admission-before-batch orphans are inspectable, remain reserved by default,
  and can be released only by a durable expiry/repair decision read during
  deployed coordinator restart reconstruction.
- The same race is not claimed safe in create-only-only mode.
- Adjacent ranges are allowed.
- Crash-after-create-before-response retry returns `200 OK` for same digest.
- Same key with different digest returns `409 Conflict`.
- Production-like harness source contracts reject catalog-aware append receivers
  other than `IngestAdmissionCoordinator`, including the runtime and
  persisted-query recovered-runtime fixtures plus catalog-backed runtime
  recovery fixtures.

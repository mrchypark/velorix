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
inside one coordinator instance, rechecks committed ranges while guarded,
rejects visible overlaps, permits adjacent ranges, and preserves same-digest
retry idempotency. This is local/runtime coordinator evidence only; it does not
provide a distributed admission index across processes or pods.

Production `Coordinated` mode uses a durable admission index or a deployed write
coordinator with the same semantics. This is required before Velorix advertises
production multi-writer range-overlap rejection.

`AsyncBuffered` mode is future work. It may return `202 Accepted` only with a
separate admission status lifecycle and must not claim persistent ingest before
the canonical batch object is created.

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
- The same race is not claimed safe in create-only-only mode.
- Adjacent ranges are allowed.
- Crash-after-create-before-response retry returns `200 OK` for same digest.
- Same key with different digest returns `409 Conflict`.
- Production-like harness source contracts reject catalog-aware append receivers
  other than `IngestAdmissionCoordinator`.

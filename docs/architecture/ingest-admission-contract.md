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
   overlapping writes until an operator writes a durable expiry/repair decision
   for that reservation. A stale retry of the original payload through normal
   ingest after expiry must be rejected as `admission_expired`; only an explicit
   operator repair path may restore the original batch.
5. If the batch exists, validate the digest, relation identity, and schema
   fingerprint against the admission record. Any mismatch is
   `corrupt_conflict`; do not repair automatically, and block writer cutover
   until the operator resolves the authority-store conflict.

Expiry is not a time-only delete. An expiry decision is a durable Velorix-owned
record under
`v1/ingest-admission/{stream_id}/p=.../ranges/{start-end}/expiry-decisions/{decision_id}.expiry.json`
bound to the full admission record digest, reason, operator identity, and
observed missing batch key. Coordinator restart reconstruction may omit the
orphan reservation only when the expiry decision matches the exact admission
record digest and the canonical batch is still missing. Expiry is terminal for
ordinary ingest retries and is not admission evidence for committed replay.
Until a deployed operator-authorized expiry/repair path and live restart
evidence exist, `orphan_reserved` remains a production cutover blocker.

Current local implementation exposes the restart reconstruction as a checked
startup preflight: `IngestAdmissionCoordinator::reconstruct_active_admissions`
reports active reservations and digest-bound expired orphan decisions, validates
visible committed batches against the admission record's digest and relation
metadata, and `IngestAdmissionCoordinatorProvider::startup` runs that
reconstruction before a Kubernetes operator path exposes a coordinator. The
provider's raw coordinator constructor is not a public production API.
`scripts/run-vind-k8s-gate.sh` now includes a live Kubernetes check that seeds a
run-local orphan admission record, persists a run-local expiry
decision, and verifies a restarted production provider reconstructs the expired
orphan as non-active. This is local vind evidence over a run-local authority
store, not live writer cutover evidence. The remaining row-closing evidence
requires a deployed writer/coordinator path that calls the preflight before
serving writers, plus floci/vind multi-process or multi-pod overlap races,
adjacent range races, crash/retry windows, restart reconstruction, and leader
handoff before this contract can close.

## Conflict Semantics

Create-only object writes only reject identical-key conflicts. They do not
atomically reject different-key overlapping ranges such as `[0, 100)` and
`[50, 150)`. Production multi-writer ingest must not advertise range-overlap
`409 Conflict` until `RangeAdmissionIndexV1` or an equivalent write coordinator
exists.

Conflict reasons must be explicit:

- `same_key_different_digest`.
- `same_range_different_digest_reserved`.
- `idempotency_key_reused`.
- `range_overlap_committed`.
- `range_overlap_reserved`.
- `admission_expired`.
- `unsupported_overlap_guarantee`.

## Verification

- Concurrent `[0,100)` and `[50,150)` admission rejects one request in the
  process-local coordinator.
- A separate coordinator rejects a range already reserved by durable serialized
  admission evidence before any batch object is committed.
- Admission-before-batch orphans are inspectable, remain reserved by default,
  and can be released only by a digest-bound durable expiry/repair decision read
  during coordinator restart reconstruction.
- A stale ordinary ingest retry of an expired orphan returns `admission_expired`
  and does not write the canonical batch.
- The same race is not claimed safe in create-only-only mode.
- Adjacent ranges are allowed.
- Crash-after-create-before-response retry returns `200 OK` for same digest.
- Same key with different digest returns `409 Conflict`.
- Production-like harness source contracts reject catalog-aware append receivers
  other than `IngestAdmissionCoordinator`, including the runtime and
  persisted-query recovered-runtime fixtures plus catalog-backed runtime
  recovery fixtures.

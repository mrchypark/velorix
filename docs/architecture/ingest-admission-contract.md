# Ingest Admission Contract

Status: Accepted
Applies to: ingest API acknowledgement, idempotency, and range conflict
semantics.

Internal admission-layer acknowledgements cover durable input admission only.
They do not claim that SQL processing, standing views, output publication, or
checkpoint publication has completed. This durable-admission-only guarantee is
an internal storage/runtime contract, not the public 1.0 relation ingest API
contract. The public `/v1/relations/.../ingest` API exposes only the
`materialized` acknowledgement: a successful response means the relation update
and admitted materialized-view effects have both reached the durable
materialized output contract, not merely append admission.

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

`IngestAdmissionCoordinator` is the catalog-aware admission facade. When
constructed through `new_checked`, it validates startup object-store capability
evidence, reserves a `RangeAdmissionIndexV1` transition, records immutable
Velorix-owned materialized admission evidence under `v1/ingest-admission/...`,
rechecks committed and reserved ranges while guarded, rejects visible overlaps,
permits adjacent ranges, and preserves same-digest retry idempotency. The
unchecked constructor remains bootstrap/dev compatibility and must not be used
as production evidence.

## Relation Ingest and Join Frontiers

The public relation ingest contract is sequential. A relation ingest advances
the frontier for the relation stream/partition named by that request, and active
dependent views publish materialized output for the resulting frontier vector
before returning the `materialized` acknowledgement. For joins, that vector may
contain one advanced relation and one relation still at its previous frontier.
Those intermediate vectors are valid published states.

Velorix does not expose a public atomic multi-relation transaction API for 1.0.
`/v1/relations/ingest`, when used with multiple relation batches, is a
deterministic convenience wrapper over relation ingest sequencing. It must not
claim all-or-nothing rollback, one client-visible global epoch, or hidden
multi-relation join atomicity.

Each runtime checkpoint must record the complete input frontier vector for the
published materialized output. Output manifests must either record that vector
directly or remain selected only through the checkpoint/pointer that records it.
Product completion is not proven by an output manifest that can be served
without the frontier vector that produced it.

`RangeAdmissionIndexV1` is a create-only, single-successor partition index under
`v1/ingest-admission-index/{stream_id}/p={partition_id}/advances/{previous_state_digest}.transition.json`.
For one `(stream_id, partition_id)` head, concurrent writers compete on the same
transition key. The winning transition records the admitted range and the next
state digest; losers reload the winning transition, recompute the head, and
retry or return `range_overlap_reserved`. Reconstruction fails closed if
transitions are unreachable, if more than one successor exists for one previous
state digest, if a transition admits an overlapping active range, if a
transition lacks its materialized `v1/ingest-admission` record at checked
startup/reconstruction, or if that materialized record diverges from the indexed
transition. Live reservation reloads may still treat a just-written transition
as in-flight during the short transition-before-materialization window, but that
state is not restart-safe until the materialized admission exists. Legacy
materialized admissions seed the index digest so existing authority history
remains reachable, while digest-bound expiry decisions remove only the active
reservation from conflict checks.

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
An indexed transition without its materialized `v1/ingest-admission` record is
not an expirable ordinary orphan; checked startup/reconstruction fails closed
until an operator repairs the materialized record or otherwise resolves the
authority-store state.

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
orphan as non-active. The vind run counts as live Kubernetes evidence for the
operator path it exercises, while the run-local authority store portion remains
object-store-local. Row-closing writer evidence still requires a deployed
writer/coordinator path running on vind that calls the preflight before serving
writers, plus RustFS/S3-compatible crash/retry and restart reconstruction
evidence and vind multi-pod overlap races, adjacent range races, crash/retry
windows, restart reconstruction, and leader handoff before this contract can
close.

## Conflict Semantics

Create-only object writes only reject identical-key conflicts. They do not
atomically reject different-key overlapping ranges such as `[0, 100)` and
`[50, 150)`. Checked catalog-aware coordinator admission now uses
`RangeAdmissionIndexV1` for that storage-level partition fence, but production
multi-writer ingest must not advertise the guarantee until the deployed
writer/operator path routes through the checked coordinator and has RustFS plus
vind live evidence for the exercised deployment topology.

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
  checked coordinator through `RangeAdmissionIndexV1`; the local and RustFS
  S3-compatible multi-process harnesses mark child processes ready only after
  store/coordinator/payload setup, then release them together into append with
  zero artificial post-release delay.
- Adjacent ranges produce chained `RangeAdmissionIndexV1` transitions and both
  append.
- A separate coordinator rejects a range already reserved by durable serialized
  admission evidence before any batch object is committed.
- Admission-before-batch orphans are inspectable, remain reserved by default,
  and can be released only by a digest-bound durable expiry/repair decision read
  during coordinator restart reconstruction.
- A stale ordinary ingest retry of an expired orphan returns `admission_expired`
  before creating a new index transition and does not write the canonical batch.
- The same race is not claimed safe in create-only-only mode.
- Adjacent ranges are allowed.
- Crash-after-create-before-response retry returns `200 OK` for same digest.
- Same key with different digest returns `409 Conflict`.
- Production-like harness source contracts reject catalog-aware append receivers
  other than `IngestAdmissionCoordinator`, including the runtime and
  persisted-query recovered-runtime fixtures plus catalog-backed runtime
  recovery fixtures.

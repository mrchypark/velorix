# Ingest Storage Format Decision

Status: Accepted
Date: 2026-05-04

## Decision

Velorix will make a breaking change to the ingest payload contract before the
project accumulates production data. Durable ingest remains object-storage
first: committed input is still stored as immutable, create-only objects under
`v1/ingest`. The payload format inside those objects will change from the
current JSON `DeltaBatch` bootstrap shape to a versioned Arrow IPC ingest
envelope.

Velorix will not keep a legacy JSON ingest read path as part of this decision.
Existing bootstrap fixtures, recovery tests, and local sample data should be
rewritten to the new format instead of supported indefinitely. Unsupported
ingest payload versions must fail closed with typed errors.

## Rationale

The project is still early enough to take breaking changes cheaply. Preserving
JSON `DeltaBatch` compatibility now would create a long-lived migration surface
around a format already known to be a bootstrap convenience. That would weaken
the main product goal: cost-efficient streaming ingest, replay, SQL processing,
and incremental view maintenance over object storage.

Arrow IPC is the preferred hot ingest payload because it aligns with the
existing Arrow/DataFusion boundary, avoids JSON parse and canonicalization
costs, preserves typed columnar data, and supports efficient batch-by-batch
replay into runtime processing. Parquet remains important, but it should serve
cold scan, compaction, export, and persisted table surfaces rather than every
hot ingest write. Writing many small Parquet files directly on ingest would
increase PUT/list cost, footer overhead, row-group inefficiency, and compaction
debt.

SlateDB remains the durable state substrate, not the primary ingest log.
Putting ingest entries directly into SlateDB would mix ingest-log authority,
state-store authority, and checkpoint-manifest progress semantics before the
write-buffer and state-layout contracts are ready.

## Format Contract

Each committed ingest object body must be a `VelorixIngestEnvelopeV1`:

- `magic`: fixed bytes identifying a Velorix ingest envelope
- `schema_version`: `1`
- `format`: `ArrowIpcDeltaBatchV1`
- `stream_id`
- `partition_id`
- `start_offset_inclusive`
- `end_offset_exclusive`
- `schema_fingerprint`
- `payload_digest`
- `compression`
- `body`: Arrow IPC payload

The envelope header is authoritative. Object metadata may duplicate digest,
format, or schema information for faster inspection, but replay and
idempotency must be correct from the object body alone. The concrete digest,
framing, and fail-closed validation rules are defined in
[Ingest Envelope V1](ingest-envelope-v1.md).

The object key remains deterministic:

```text
v1/ingest/{stream_id}/p={partition_id:010}/{start:020}-{end:020}.batch
```

The key range and the envelope range must match exactly. Mismatches are
corruption and must fail recovery rather than being repaired implicitly.

## Data Shape

`ArrowIpcDeltaBatchV1` represents signed input changes with typed Arrow
columns. Every relation must include a signed `weight` column using `Int64`.
Business fields should be relation-specific typed columns, not opaque JSON
values. Key identity should be derived from the declared relation schema and
view/input contract rather than serialized JSON strings.

For the first implementation, a narrow relation shape is acceptable if it is
explicitly versioned and tested. It must not preserve the old JSON
`DeltaKey`/`DeltaValue` model as the durable ingest contract.

## Storage-Layer Append Semantics

The storage-layer append acknowledgement contract in `storage-contract.md`
remains in force for durable input admission:

- `201 Created` means the canonical ingest object was created successfully.
- `200 OK` means an exact idempotent retry found the same durable object and
  digest.
- `409 Conflict` means the key, range, digest, or idempotency mapping conflicts.
- `202 Accepted` is future async admission only and cannot claim persistence
  without a separate durable admission record.

This is not the public 1.0 relation ingest contract. Public
`/v1/relations/{relation_id}/ingest` and `/v1/relations/ingest` expose only the
synchronous `materialized` acknowledgement: the relation update and active
dependent materialized-view effects must be durably published before success is
returned.

`schema_fingerprint` is not a hash of raw Arrow IPC schema bytes. It is a hash
of `VelorixRelationSchemaV1` as defined in
[Schema Fingerprint V1](schema-fingerprint-v1.md). The digest used for
idempotency is defined by [Ingest Envelope V1](ingest-envelope-v1.md) and
excludes the `payload_digest` field from its own digest input.

## Breaking Impact

This decision intentionally breaks current bootstrap assumptions:

- Recovery can no longer call `serde_json::from_slice::<DeltaBatch>` on ingest
  payloads.
- `IngestBatch` construction must move from arbitrary `Bytes` toward typed
  envelope creation or validation before append.
- Tests that write `serde_json::to_vec(&DeltaBatch)` into `v1/ingest` objects
  must be rewritten to produce Arrow IPC envelopes.
- Query and recovery code must convert Arrow IPC ingest batches into the
  incremental-engine input representation without routing through durable JSON.
- Documentation that describes JSON `DeltaBatch` ingest must be revised or
  scoped to pre-decision bootstrap history.
- Local object-store fixtures created before this decision are disposable and
  should be regenerated, not migrated.

## Implementation Requirements

The implementation should land as a single coherent breaking slice:

1. Add an ingest envelope module with typed encode/decode, digest calculation,
   schema fingerprinting, and fail-closed version/format validation.
2. Change `IngestLog::append` or its callers so committed ingest payloads are
   validated envelopes, not arbitrary bytes.
3. Change runtime replay to stream and decode ingest objects batch-by-batch
   instead of reading all committed payloads into memory first.
4. Replace JSON ingest fixtures and tests with Arrow IPC envelope fixtures.
5. Add tests for envelope/key range mismatch, digest mismatch, unsupported
   version, unsupported format, duplicate same digest, duplicate different
   digest, adjacent ranges, overlapping ranges, and recovery from multiple
   batches.
6. Keep Parquet out of hot ingest writes. Add Parquet only through a later
   compaction/export path over sealed or checkpointed ranges.
7. Keep SlateDB out of the ingest log unless a future write-buffer/index
   decision assigns it a narrow metadata role with checkpoint semantics.

## Benchmark Gates

Before merging or releasing the new ingest format as a production path, the
relevant [Benchmark Gate V1](benchmark-gate-v1.md) workload must pass. The
benchmark must emit machine-readable JSON and compare against an approved
baseline. Local filesystem benchmark results and S3-compatible benchmark
results are recorded separately and are not interchangeable. Benchmark at
least:

- ingest rows per second
- encode CPU per row
- bytes written per row
- object PUT count per GiB
- average committed object size
- recovery rows per second
- recovery peak RSS
- object GET/list/range-read count
- duplicate retry latency
- corrupt payload detection latency
- Parquet compaction amplification after sealed-range export exists

The comparison set should include the current JSON bootstrap path only as a
temporary baseline during development. It is not a compatibility requirement.

## Non-Goals

- No durable JSON `DeltaBatch` ingest compatibility path.
- No automatic migration of local bootstrap object-store data.
- No per-ingest Parquet object as the default hot write path.
- No SlateDB-backed primary ingest log in this decision.
- No Foyer-based durability or admission semantics.

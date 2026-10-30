# Ingest Envelope V1

Status: Accepted
Applies to: durable ingest payloads stored under `v1/ingest`.

`VelorixIngestEnvelopeV1` is the durable hot-ingest payload format. JSON
`DeltaBatch` is not a durable ingest compatibility format.

## Envelope Fields

Each committed ingest object body contains:

- `magic`: fixed bytes identifying a Velorix ingest envelope.
- `schema_version`: `1`.
- `format`: `ArrowIpcDeltaBatchV1`.
- `stream_id`.
- `partition_id`.
- `start_offset_inclusive`.
- `end_offset_exclusive`.
- `relation_id`.
- `relation_version`.
- `schema_fingerprint`.
- optional `event_time_watermark`, bound to the same stream and partition.
- `payload_digest`.
- `compression`.
- `body`: Arrow IPC payload bytes.

The envelope body is authoritative. Object metadata may duplicate fields for
inspection, but replay, idempotency, and corruption checks must be correct from
the object body alone.

## Digest Contract

`payload_digest` excludes itself. It is calculated over this domain-separated
input:

```text
sha256(
  "velorix-ingest-envelope-v1\0" ||
  canonical_header_without_payload_digest ||
  "\0" ||
  stored_body_bytes
)
```

`canonical_header_without_payload_digest` is the canonical envelope header after
removing `payload_digest`. `stored_body_bytes` are the bytes stored in the
object after compression. V1 does not define a separate decompressed body
digest. If future recompression-independent identity is required, it must be a
new versioned field.

The digest string format is `sha256:<lowercase-hex>`. Header mutation, body
mutation, compression mutation, relation mutation, schema fingerprint mutation,
range mutation, or digest-field mutation must fail decode with a typed error.

## Framing Requirements

The current implementation frames bytes as magic, little-endian `u32` JSON
header length, JSON header, and Arrow IPC stream body. The digest uses a
deterministically constructed JSON header without `payload_digest`, followed by
the stored body. Compression currently accepts only `none`. The decoder
rejects invalid magic, malformed/truncated headers, unsupported versions,
formats and compression, digest mismatches, invalid header identity/range, and
malformed or empty Arrow IPC payloads. No explicit maximum header/body byte
limits or unknown-header-field rejection are implemented; do not claim those
bounds from the current V1 codec.

The durable relation weight column is selected by the catalog. The envelope
codec itself validates the IPC stream and descriptor but does not require a
literal column named `weight` or require that column to be Int64.

## Verification

- Golden fixture digest is stable across producer and replay code.
- Changing only `payload_digest` fails decode.
- Changing range, relation, schema fingerprint, compression, or body bytes fails
  digest verification.
- Truncated header, unsupported format/compression, and malformed Arrow IPC
  return typed errors. Oversized header and body rejection remains
  unimplemented and unverified.

Current implementation: [ingest_envelope.rs](../../crates/velorix-storage/src/ingest_envelope.rs);
codec and catalog-append tests: [ingest_envelope.rs tests](../../crates/velorix-storage/tests/ingest_envelope.rs).

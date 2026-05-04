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

The implementation must define a deterministic binary framing before production
use. The framing must specify magic bytes, header length, header canonical
serialization, maximum header size, maximum body size, compression enum,
unknown-field behavior, and typed errors for truncation or unsupported versions.

## Verification

- Golden fixture digest is stable across producer and replay code.
- Changing only `payload_digest` fails decode.
- Changing range, relation, schema fingerprint, compression, or body bytes fails
  digest verification.
- Truncated header/body, oversized header/body, unsupported format, unsupported
  compression, and malformed Arrow IPC return typed errors.

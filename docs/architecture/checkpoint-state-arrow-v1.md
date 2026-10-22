# Runtime and durable checkpoint state: Arrow IPC with ZSTD 3

Persisted checkpoint state, output pages and deltas use typed Arrow bodies with
ZSTD level 3. Runtime state payloads use `velorix-runtime-arrow-v2`; the
checkpoint record containing `DemandCheckpoint` is stored as
`.checkpoint.arrow`. Native versioned pages use the current Arrow codecs, while
schema-less generic pages use `velorix-delta-batch-arrow-v1`. Output deltas are
typed Arrow records stored as `.output-delta.arrow`.

Small reference manifests and pointers remain JSON, including the output
manifest and latest pointer. JSON control metadata and semantic JSON values
retain their existing representations. This is a greenfield format: readers do
not support legacy JSON state bodies, and no live data is migrated or removed.

The full Rhiza metadata snapshot is also a persisted state body: its canonical
normalized `InMemoryMetaState` is now encoded as this Arrow IPC tree with ZSTD
level 3. Its Arrow body has a 16 MiB encoded-byte ceiling and a separate 64 MiB
expanded typed-tree budget; the Rhiza KV page protocol imposes a lower 8 MiB
aggregate encoded-body ceiling. Only the small KV root/page manifest remains
JSON. Relation catalogs, view specs, SQL JSON values, RPC requests/responses,
and other small control descriptors keep their existing JSON representations.
The 64 MiB limit is logical typed-tree accounting, not a peak-memory or RSS
cap. Arrow IPC preflight permits declared buffer expansion up to twice that
limit plus 1 KiB before final typed-tree validation; callers should not treat
the logical budget as a strict decompression-memory bound.

## General representation

A preorder tree occupies one Arrow record batch:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `kind` | UInt8 | null, boolean, signed/unsigned integer, float, string, sequence, map, bytes |
| `children` | UInt32 | Number of immediate children; map children alternate key/value |
| `boolean` | nullable Boolean | Boolean value |
| `signed` | nullable Int64 | Signed integer |
| `unsigned` | nullable UInt64 | Unsigned integer |
| `float` | nullable Float64 | Finite floating-point value |
| `text` | nullable Utf8 | String value, including map keys |
| `binary` | nullable Binary | Explicit bytes, nested Arrow snapshots, or 16-byte i128/u128 scalars |

The body is typed state, not an opaque JSON blob. Maps have deterministic key
order; sequences retain order. Signed and unsigned 64-bit and 128-bit limits, Unicode,
nulls and floating-point bits (including negative zero) survive round trips.
A bounded Serde serializer builds typed values before column construction;
it charges nodes and strings before allocating their contents. Restore reads
Arrow arrays through a direct Serde deserializer without constructing a second
complete value tree. Enums, numeric map keys and nested snapshots use this same
path. Existing row scalar types and JSON-valued SQL columns retain their own
semantics; this does not change public row formats.

`ArrowState` shares its decoded arrays, subtree boundaries and encoded IPC bytes
through `Arc`. Cloning a checkpoint does not clone its entire body. Top-level
field lookup skips whole subtrees; runtime selection, schema extraction and
output-layout inspection do not deserialize retained accumulators.

IPC compression is explicitly ZSTD level 3. The state commitment hashes the
canonical runtime IPC bytes. Storage binds tenant, program, view, epoch, codec,
expanded state size and state hash in schema metadata. Retained-state envelopes
bind object key, window key, epoch and hash. Adding/removing this envelope only
rewrites the schema message: compressed record buffers are copied verbatim,
not recompressed. Recovery strips the envelope before checking the runtime
commitment; the runtime additionally validates state identity and semantics.

## Bounds, fragments and cache

State byte budgets charge 48 bytes per typed node plus string/binary bytes,
independently of compression ratio. This is a conservative logical accounting
unit, not an exact process-RSS measurement. Readers bound encoded object size,
IPC frames, row counts, declared decompression sizes, tree depth, column types
and null layouts before restoring runtime objects. Extra batches, dictionaries,
truncated streams and trailing bytes fail closed. Query recovery reserves
additional space for compressed copies, Arrow buffers and restored state.

Fragmentation splits the already-compressed IPC stream. Each fragment binds its
byte count and hash. The v2 manifest declares encoded and expanded state sizes
separately, and its header contains an empty body. Recovery validates fragments,
assembles a bounded stream, decodes it and checks the complete state commitment.
Compressible states may remain a single object even when expanded state exceeds
the fragment threshold. Write accounting uses actual encoded bytes.

Foyer stores disposable encoded state and verified fragment bytes. Cache keys
bind codec and identity. Cache hits still validate the state commitment and
bounds; a cache is never an authority for accepting a different checkpoint.
Decoded arrays are shared within a hydrated checkpoint. This is not a new
unbounded global decoded-state cache.

## Greenfield format and verification

New stores write and read the current Arrow formats only; old JSON and previous
Arrow bodies have no compatibility readers. Existing objects and Rhiza KV
snapshots remain untouched, and no migration or rebuild is performed by this
change. Reusing an existing store requires a separately planned migration or
rebuild before the new readers can use its old bodies. A storage-format change
does not re-ingest source data.

Tests cover typed data, enums and numeric keys, nested snapshots, a fresh
process reading persisted IPC bytes, identity/commitment failures, compressed
size bounds, fragments and cache reuse. Existing synthetic admission/ingestion
integration tests exercise multiple SQL families, restart recovery and late
corrections followed by another restart. The ignored `snapshot_benchmark` test
measures raw JSON and Arrow/ZSTD creation, typed restoration and field access
on the same 25,000-row fixture; compression and I/O costs must be reported
separately when interpreting those results.

See [path audit and measured results](../development/runtime-arrow-snapshot-verification.md)
for verification scope, format tradeoffs and reproducible commands.

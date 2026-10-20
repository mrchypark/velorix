# Checkpoint state: Arrow IPC with ZSTD 3

Runtime state bodies have one durable format: `velorix-checkpoint-arrow-zstd-v1`.
Their object keys end in `.state-payload.arrow`. There is no JSON body reader,
JSON body writer, or fallback to a legacy inline state body. Checkpoint
descriptors (`.checkpoint.json`) and fragment manifests remain small JSON
metadata objects; they hold references and commitments, not the retained state.

## General state representation

The codec uses the same ordered tree for every runtime family and relation
schema. One Arrow record batch contains these columns:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `kind` | UInt8 | null, boolean, signed integer, unsigned integer, float, string, array, object |
| `name` | nullable Utf8 | Object member name; null for array elements and the root |
| `children` | UInt32 | Number of immediate children in preorder |
| `boolean` | nullable Boolean | Boolean value |
| `signed` | nullable Int64 | Signed integer value |
| `unsigned` | nullable UInt64 | Unsigned integer value |
| `float` | nullable Float64 | Finite floating-point value |
| `text` | nullable Utf8 | String value |

The state is not stored in an opaque JSON or binary payload column. Object
member order and array order are preserved. Integer range, nulls, Unicode and
floating-point round trips retain the runtime's existing state commitment.
The writer checks exact reconstruction before publication and rejects a state
whose representation would change. The IPC schema metadata binds the tenant,
program, view, epoch, runtime codec, logical state byte count and state hash.
IPC buffer compression is explicitly ZSTD level 3.

Retained correction-state archives use the same typed tree and compression,
with metadata bound to their object key, logical epoch, window key and state
hash. Their keys also end in `.arrow`; the late-data hydration path no longer
reads raw JSON archives. The runtime additionally verifies its identity binding
inside the restored archive. Small states carry IPC schema overhead, so this
format does not guarantee fewer bytes for every individual object.

The current runtime snapshot interface still produces a JSON string in memory.
The storage boundary parses it once into typed Arrow columns, and recovery
reconstructs that string for the runtime restore interface. This changes the
durable body format; it does not yet eliminate JSON serialization inside every
runtime implementation.

## Bounds, fragments and cache

The logical state byte limit applies before encoding and after recovery.
Readers validate IPC framing, row counts, compressed-buffer expansion lengths,
tree depth, column types, null layouts and output length. These checks run
before unbounded Arrow decompression or reconstructed-state allocation.
The runtime state hash is checked before restoration, together with the object
key and checkpoint descriptor identity. Extra batches and trailing bytes fail
closed. Publication remains deterministic and create-only.

Fragmentation splits the compressed IPC stream when its encoded size exceeds
the fragment limit. Each fragment has a byte count and hash. The version 2
manifest separately declares compressed bytes and logical state bytes, and its
header contains no state body. Recovery validates the manifest and fragments,
assembles the bounded stream, decodes it and checks the complete state hash.
Compressible states can therefore remain one object even when their logical
size exceeds the fragment threshold. Object-write accounting uses the actual
encoded size.

Foyer stores disposable Arrow bytes and verified fragment bytes. Cache keys
include the Arrow state codec; reads revalidate the logical state commitment
and identity. Cached compressed bytes do not bypass decoded-state bounds or
query memory policy.

## Format transition

Existing JSON state objects are neither deleted nor silently read as the new
format. Their keys are rejected by the new state-root parser. Existing input
objects are unchanged. An installation that has JSON checkpoints must explicitly
rebuild or migrate its state before using this reader; this change does not
perform that operation against existing user storage.

Verification includes typed scalar and ordering round trips, deterministic
encoding, corrupt/truncated data, JSON rejection, decompression bounds,
fragment corruption, cache reuse, multiple SQL families, restart recovery and
late event-time corrections followed by another restart. The integration
fixtures are synthetic and use the normal admission, ingestion and recovery
paths.

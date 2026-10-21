# Runtime Arrow snapshot verification

The runtime snapshot boundary is `velorix_core::arrow_state::ArrowState`.
Its typed tree and IPC contract are described in
[checkpoint state](../architecture/checkpoint-state-arrow-v1.md).
This review covers checkpoint creation, candidate validation, durable
publication, fragments, retained correction state, recovery, query metadata,
output normalization, caches and the existing local incremental benchmark.
It is not a claim that every query plan in the engine is optimally tuned.

## Path review

| Path | Result |
| --- | --- |
| Runtime checkpoint creation | All admitted runtime families serialize through the shared bounded Serde/Arrow codec. No whole-state JSON document is produced. |
| Candidate size checks | Single-key aggregates and typed joins validate the candidate without constructing/compressing discarded IPC. Configured state-byte limits still reject before commit. |
| Copying | Checkpoint clones share Arrow arrays, subtree boundaries and IPC bytes. Retained-state puts and benchmark retries share `Bytes` buffers. Debug output reports sizes instead of dumping entire snapshots. |
| State publication | The storage envelope replaces only the schema message; compressed record buffers are reused. Commitments bind canonical runtime IPC. |
| Fragments | Encoded IPC is fragmented, not decoded state. Existing byte/hash checks and bounded sequential reads remain. More fragments still incur more object requests; no remote throughput result is claimed here. |
| Retained late-data state | The same typed codec is used for export, object storage, Foyer validation and hydration. A duplicate immutable put uses a bounded read before comparison. |
| Restore | Arrow arrays feed the Rust deserializer directly; there is no reconstructed JSON string or second complete dynamic value tree. Typed numeric map keys, 128-bit integers, enums and nested states are covered. |
| Query metadata | Runtime-kind selection, schema extraction and output-layout inspection skip retained state subtrees. Existence checks do not decode the published output just to classify its family. |
| Query response | Indexed reads continue to use bounded materialized pages. A runtime restore microbenchmark is not a measurement of HTTP response latency or view computation. |
| Output publication | Compact keys, weights and nulls are preserved while normalization uses up to 512 rows per Arrow batch. A direct typed published-output read removes an intermediate dynamic-value conversion. |
| Output commitments | Object member order is canonicalized before output-delta hashing so restore cannot change logical commit identities solely by reordering map members. |
| Foyer | Encoded entries remain disposable and identity-bound. Hits still verify commitments and decode bounds. There is no unbounded decoded-state cache. |
| Local incremental benchmark | Its complete checkpoint envelope now uses Arrow rather than JSON containing serialized IPC bytes. The benchmark continues to count storage calls and recover through the normal runtime factory. |

SQL JSON values, key encodings, configuration/identity metadata and explicit
JSON HTTP responses retain their existing semantics. They are distinct from
serializing an entire runtime snapshot as JSON. Earlier JSON bodies and the
storage-only Arrow v1 body are rejected; existing user objects are not migrated
or deleted automatically.

## Recovery and correctness evidence

The fresh-process integration test persists a descriptor with no embedded state
body, a primary `state.arrow`, and retained accumulator IPC files. A child
process restores them, hydrates an expired window and applies a late row. It
checks the correction's retraction/replacement and exact sum, count, min, max
and average. A second child restores the corrected checkpoint and checks those
values again. The owning test removes and verifies removal of its temporary
directory after both children exit.

Other synthetic tests cover multiple SQL families, restart, differential output
commit identity, fragment corruption, foreign retained references, Foyer reuse,
resource limits, input permutation stability and failed-epoch rollback. Native
operator and engine wire-round-trip tests also use the shared Arrow codec.
No real dataset was ingested or re-ingested for these tests, and no deployed
server, VersityGW instance or Rhiza database was modified.

## Measurement method

The snapshot fixture contains 25,000 rows, including signed values, maximum
unsigned values, nullable strings, floating-point values and a numeric-key map.
Each reported snapshot time averages 10 iterations within a process. Paired
process runs were interleaved and medians reported to reduce shared-host noise.
JSON+ZSTD and Arrow IPC both use ZSTD level 3. Warm restoration starts with
already-decoded Arrow arrays; cold restoration includes IPC decode and
validation but excludes storage/network I/O. Neither means calculating a view.

The normalization fixture contains 25,000 compact rows with repeated keys,
weights greater than one, null values and page crossings. It compares one-row
normalization against batches of 512 and asserts identical complete manifests
and output pages. This stage measurement uses the debug test binary, separately
from the optimized snapshot microbenchmark, and performs no S3 writes.

Reproduce the component measurements with:

```sh
cargo test --release -p velorix-core --lib arrow_state::tests::snapshot_benchmark -- --ignored --nocapture
cargo test -p velorix-api --lib materialized_output_pages::tests::normalization_benchmark -- --ignored --nocapture
cargo test -p velorix-runtime --test materialized_view_runtime arrow_checkpoint_and_cold_correction_survive_fresh_processes -- --nocapture
```

State-byte accounting is now expanded typed state (48 bytes per node plus
string/binary bytes), rather than JSON text length. The default total limit is
still 8 MiB. The large-output correctness fixtures explicitly request 16 MiB;
production limits were not relaxed to make a test pass. This accounting unit
and the encoded object length must not be reported as process RSS.

## Measured results — 2026-10-21

Environment: macOS 26.6.2, arm64, Rust 1.98.1, Arrow 59.3.0. Snapshot figures
below are medians of 10 interleaved process pairs, with 10 iterations per process.
The host was shared; these are component measurements, not service guarantees.

| Component | Earlier typed Arrow implementation in this task | Final implementation |
| --- | ---: | ---: |
| Snapshot creation, including IPC/ZSTD | 25.584 ms | 19.435 ms |
| IPC decode, validation and typed restore, excluding I/O | 9.104 ms | 5.663 ms |
| Typed restore from resident arrays | 2.438 ms | 2.340 ms |
| Runtime-kind field lookup | 0.047 µs | 0.049 µs |

Creation improved about 24% and cold restoration about 38% in this paired
experiment. The reference is an earlier **typed Arrow implementation from this
task**, before removal of duplicate validation and repeated array downcasts;
it is not the previous released JSON runtime path. Warm restoration and tiny
field-lookup differences are too small to treat as a separate optimization win.

The final implementation's fair format comparison on the same fixture is:

| Format | Encoded bytes | Create | Restore |
| --- | ---: | ---: | ---: |
| Raw JSON | 1,477,831 | 1.280 ms | 3.461 ms |
| JSON + ZSTD 3 | 31,424 | 2.958 ms | 3.931 ms |
| Arrow IPC + ZSTD 3 | 102,728 | 19.435 ms | 5.663 ms from IPC; 2.340 ms from resident arrays |

Arrow creation and first restoration remain slower than compressed JSON on
this fixture, and IPC is about 3.27 times its compressed size. Arrow IPC uses
separate typed buffers, whereas JSON+ZSTD compresses one repetitive text stream;
there is no claim that switching formats universally improves speed or size.
The gain here is eliminating the JSON intermediate in the required Arrow path,
sharing resident state, cheap field access, and removing unnecessary work.
Candidate size validation takes 2.635 ms without constructing discarded IPC.

Three earlier paired process measurements using `time -l` showed approximately
175 MiB median maximum RSS for the **entire mixed-format benchmark process**,
including fixtures and both formats. That is not an isolated Arrow memory
measurement or a server RSS result; the before/after difference was negligible.

Output normalization measured 1,546 ms with one-row batches and 1,049 ms with
512-row batches (about 32% lower), with identical manifests/pages. These are
25,000-row **debug-build stage times**, including page construction and no
network/storage writes. They must not be presented as release HTTP latencies.

Local verification passed workspace tests, workspace/all-target clippy with
`-D warnings`, formatting, 401 API tests (plus one manually executed ignored
benchmark), 85 runtime unit tests and 262 runtime integration tests. The normal
core test suite includes the generic codec and fresh-process tests; its ignored
performance test was also executed explicitly in release mode. Deployed HTTP
latency, remote S3 throughput and new real-data comparisons were not measured.

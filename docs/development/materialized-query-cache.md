# Materialized query reads and cache

Queries read committed materialized output using its registered schema. They do
not recompute the source relations. View admission, incremental maintenance,
publication and query execution remain separate steps; unsupported view SQL
must fail during admission rather than use an external execution fallback.

Materialized read pages are an optional index over the authoritative materialized
checkpoint. If expanded output exceeds the index capacity, checkpoint publication
retains the compact weighted output through the existing checkpoint format;
bounded queries and restart validation remain available. This path can require
more reads and does not provide page pruning. Invalid typed rows or
checkpoint commitments still fail validation. Retrying a compatible legacy
checkpoint preserves its immutable descriptor bytes and publication references.

## Cache configuration

The API uses Foyer 0.22.6 for a disposable memory and disk cache of verified raw
bytes at explicitly integrated immutable materialized query read sites. S3 and
metadata remain authoritative. The cache does not change checkpoint
publication, durability, or recovery semantics. Production startup enables the
cache by default, and only these three environment variables configure it.
Configure it before starting the API with the environment and storage setup
described in [the how-to guide](../how-to.md).

| Environment variable | Default | Constraint |
| --- | --- | --- |
| `VELORIX_MATERIALIZED_READ_CACHE_DIR` | System temporary directory, then `velorix-materialized-read-cache/<operator-id digest>` | An explicitly empty value disables the cache. |
| `VELORIX_MATERIALIZED_READ_CACHE_MEMORY_BYTES` | `67108864` (64 MiB) | Positive and addressable by the process. |
| `VELORIX_MATERIALIZED_READ_CACHE_DISK_BYTES` | `536870912` (512 MiB) | At least 8 MiB, addressable, and a multiple of 4096 bytes. |

For production reuse across process restarts, set an explicit absolute path on a
persistent local filesystem, for example:

```sh
export VELORIX_MATERIALIZED_READ_CACHE_DIR=/var/lib/velorix/materialized-read-cache
```

Use a dedicated directory writable by the API service account. Two API
processes must use separate directories; a conflicting open safely disables
that process's cache. The default temporary directory may be cleared by the
host.

For example, choose a separate temporary directory for each API process:

```sh
CACHE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/velorix-query-cache.XXXXXX")
export VELORIX_MATERIALIZED_READ_CACHE_DIR="$CACHE_DIR"
export VELORIX_MATERIALIZED_READ_CACHE_MEMORY_BYTES=67108864
export VELORIX_MATERIALIZED_READ_CACHE_DISK_BYTES=536870912
```

Invalid capacity settings fail configuration validation. A cache directory or
device that cannot be opened causes startup to use authoritative reads with a
diagnostic. On Unix the directory has mode `0700`. An exclusive directory lock
prevents two processes from writing the same cache directory.

The memory capacity weights entry keys and their allocated byte buffers. It is
not a process RSS limit. At the default disk capacity, Foyer also configures a
16 MiB buffer pool and a 16 MiB submission queue threshold. Query batches,
HTTP bodies, cache indexes, concurrent work and allocator overhead add memory
outside the entry capacity. Smaller disk capacities reduce the block and
buffer settings.

## Identity, validation and lifecycle

Cache keys bind the storage scope and namespace, tenant, program, output,
object path, codec, content hash, checkpoint epoch and root hash. Startup
derives the storage scope from the configured endpoint, bucket, prefix and
authority identifier. Credentials are not part of this serialized identity.

The key type is `MaterializedReadCacheKey`; the configuration type is
`MaterializedReadCacheConfig`; the cache type is `MaterializedReadCache`.

The cache can reuse immutable checkpoint descriptors, output manifests and
data pages. Mutable metadata that selects the active view and checkpoint
remains authoritative. Hits still pass the production codec, identity,
schema, hash and root-binding checks. Cached bytes that fail validation, or a
cache I/O error, become misses followed by an authoritative fetch. Invalid
authoritative bytes fail the query. Cache hits do not waive query budgets or
checkpoint fencing.

Only immutable objects selected through current authoritative state are
eligible. Mutable heads and active metadata are not cached, and every hit
retains the normal authority freshness and fencing checks. A cache hit cannot
authorize an old checkpoint.

Cache-open failures are logged at startup and the API continues with
authoritative reads; these diagnostics do not include credentials or payload
contents. Invalid capacity configuration fails startup rather than silently
selecting another budget.

Foyer uses `WriteOnInsertion`, so a verified insertion is eligible for disk
persistence even when it fits in memory. Graceful API shutdown drains listeners
and in-flight requests before awaiting cache close. An embedded API can attach
an `Arc<MaterializedReadCache>` with `ApiState::with_materialized_read_cache`;
its owner must drain requests, await `close()` and drop all cache handles before
reopening the same directory. The directory lock remains held until the
handles are dropped. An abrupt stop can lose pending cache writes; a later
miss fetches from authoritative storage.

Production startup handles Ctrl-C and, on Unix, SIGTERM: it stops accepting
requests, drains both enabled HTTP and TLS listeners, then awaits the cache
close while retaining the cache handle and directory lock. If either listener
fails, the other listener still drains before cache closure, TLS connection
watchers also finish before cache closure, and the original listener error is
preserved. Cache-close failures are logged.

Reopening persisted storage starts with an empty memory tier. Use a distinct
process when measuring disk reuse, and record the process identity and cache
configuration. Repeated requests in one process demonstrate memory reuse;
they do not establish disk recovery. Removal is best effort for an insertion
already queued to disk, so validation on every hit remains necessary.

Removal is best effort in a second sense too: Foyer can still expose a queued
insertion immediately after `remove`. Every retrieval validates the bytes, so
delayed eviction cannot make an invalid payload serveable.

There are no additional cache metrics knobs.

## Read-cache integration points

The cache is consulted only where an immutable published byte range is read:

- bounded checkpoint fragment and descriptor reads,
- published materialized output page reads, including their codec, hash and
  root-binding validation,
- demand-view output reads that resolve an immutable page or manifest.

Mutable view heads, checkpoint selection metadata, and LIST-based discovery
stay authoritative and are never served from cache. Reads that lack a fresh
pointer commitment are not cached.

Focused helper tests cover memory hits, disk hits after close and reopen,
memory eviction, key isolation, invalid payload rejection, directory locking,
and capacity validation.

## Unsupported external family specs

`VELORIX_NATIVE_FAMILY_SPEC_PATH` is rejected when it is nonempty. External
family specifications do not prove SQL semantics, so API startup fails
instead of running an unproven family. Unset the variable and use supported
generic SQL views. The variable name is retained for compatibility with
existing deployments; only its value is unsupported.

See [the cache implementation](../../crates/velorix-api/src/materialized_read_cache.rs)
for its embedding interfaces and synthetic lifecycle tests.

## Conservative page pruning

Published page metadata can contain schema-bound null counts and column
minimum/maximum values. The reader skips a page only when these statistics
prove that its rows cannot satisfy the extracted predicate. Supported scalar
comparisons use compatible registered column types and SQL literals or typed
bindings. Null checks and `AND`/`OR` combine these proofs conservatively.

An unknown leaf retains its candidate pages. A known `AND` branch can exclude
a page even if the other branch is unknown; an `OR` requires both branches to
exclude it. Missing statistics, unsupported expressions, incompatible types
or an SQL shape that cannot be safely analyzed retain the complete candidate
input. Timestamp pruning accepts supported timezone-free `TIMESTAMP`
literals or typed timestamp bindings; an ordinary string or an offset-bearing
timestamp does not establish a pruning proof.

Candidate pages are read under scan, object-request, memory and execution
budgets before DataFusion evaluates the remaining SQL. The final output row
limit applies to the SQL result, not a truncated input prefix. For example,
`COUNT(*)` must count the complete bounded input even when that input has more
rows than the output row limit. A source budget failure must return an error
rather than a partial aggregate. A valid SQL query without a pruning proof
still needs complete bounded input.

For raw and templated HTTP SQL queries, `execution_timeout_ms` sets one total
deadline starting before materialized page reads. Snapshot reads and validation,
DataFusion planning, and SQL collection all consume that same budget; SQL receives
only the time remaining after the reads. `planning_timeout_ms` remains an
independent, potentially tighter cap on planning and cannot extend the total
deadline. Deadline expiry fails the request and releases its concurrency permit;
successful requests retain the permit through response encoding, which is outside
this execution deadline.

For legacy discovery without a metadata store, the request budget
conservatively charges LIST initialization and each discovery poll, including
termination, because `ObjectStore` hides provider pagination; this logical
query guard can reject earlier than the provider request count and is not an
exact S3 operation counter.

Pruning saves whole page reads. Arrow IPC page storage provides typed batches;
it does not provide Parquet-style selective column I/O. Cache hits and decoded
batch reuse can reduce work, but payload validation and response encoding
still cost CPU. Physical row ordering affects how selective page ranges are,
so a selective SQL filter alone does not guarantee that fewer pages are read.

The [predicate extractor](../../crates/velorix-core/src/query.rs) distinguishes
safe pruning proofs from expressions that must remain in residual execution.
Its pruning subset does not define the full SQL supported by view admission
or DataFusion.

## Response formats

DataFusion produces Arrow record batches internally. HTTP queries default to
an Arrow IPC stream with content type `application/vnd.apache.arrow.stream`.
Send `Accept: application/json` to request JSON rows. Unsupported or malformed
format preferences return HTTP `406`.

Both formats carry `x-velorix-logical-epoch` and, when applicable,
`x-velorix-next-page-token`. Arrow also carries `velorix.logical_epoch` and
`velorix.next_page_token` in schema metadata. A declared view response schema
can require type or shape adaptation before encoding; raw SQL uses its result
schema. Compare the actual executed branches when measuring formatter costs.

## Synthetic validation

Run the following from the repository root. These are reproduction commands,
not a claim that this document's author executed them:

```sh
cargo test --locked -p velorix-api --lib materialized_read_cache::tests
cargo test --locked -p velorix-api --lib generic_query_http_format_tests
cargo test --locked -p velorix-api --lib materialized_output_pages_tests
cargo test --locked -p velorix-core --lib
```

The cache tests use temporary directories and generated byte payloads to check
key isolation, memory eviction, persisted disk hits after close/reopen,
invalid-payload refetch, capacity validation and directory exclusion. The HTTP
tests register synthetic `left_table` and `right_table` schemas, ingest generated
rows, and exercise materialized filters, grouped sum/count, a two-table join,
raw SQL, request bindings, both formats and pagination. They require no private
source fixtures or external storage credentials.

The [materialized page tests](../../crates/velorix-api/src/materialized_output_pages_tests.rs)
use generated left/right rows and a small `grouped_output` output
schema. Their assertions cover IPC float/null/weight preservation and tamper
rejection, statistics binding and absent legacy statistics, normal public view
publication, complete `COUNT(*)` and CTE counts over 10,017 generated rows,
conservative equality/null/unknown-expression pruning, cache revalidation
after warm reads and close/reopen, persisted IPC reads in a fresh process, and
weighted cursors with epoch fencing. The fresh-process IPC test checks file
decoding; it is separate from the cache close/reopen test.

For a small manual correctness check, register a `left_table` schema with UTF-8
`key_col` and integer `amount` columns. Register a `right_table` schema with
UTF-8 `key_col` and `category` columns and an integer `value_col` column.
Use the API's configured weight column for generated ingest updates. Admit
these view definitions through the normal view API before ingesting generated
rows; give each view a distinct output relation identifier:

```sql
SELECT key_col, amount FROM left_table WHERE amount > 0;
SELECT key_col, SUM(amount) AS sum, COUNT(*) AS count
FROM left_table WHERE amount > 0 GROUP BY key_col;
SELECT r.key_col, SUM(l.amount) AS sum, COUNT(*) AS count
FROM left_table l JOIN right_table r ON l.key_col = r.key_col
GROUP BY r.key_col;
```

Query the resulting output relation with a literal equality filter, a numeric
range, their `AND`, and an `OR` that includes an expression with no pruning
proof. Compare each result with the same SQL over complete synthetic output.
Check every column's type and value, duplicate multiplicities and explicit
`ORDER BY` ordering, as well as epoch and cursor metadata. Repeat with legacy
statistics absent, cache disabled, memory warm and disk reopened. Include a
no-filter `COUNT(*)` and filtered counts on input larger than the default
10,000-row output cap to detect input truncation.

Use counted test-store reads to distinguish metadata reads from data-page
reads; keep those counts separate from engine timings. If adding a synthetic
benchmark, exclude publication, warmup and result comparison from query timing,
alternate response formats in the same process, retain outliers, and report
cache state and combined process RSS. This document makes no latency, memory
or transport-count performance claim.

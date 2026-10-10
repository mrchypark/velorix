# Materialized query reads and cache

Queries read committed materialized output using its registered schema. They do
not recompute the source relations. View admission, incremental maintenance,
publication and query execution remain separate steps; unsupported view SQL
must fail during admission rather than use an external execution fallback.

## Cache configuration

The API uses a disposable Foyer hybrid cache for validated immutable object
bytes. Configure it before starting the API with the environment and storage
setup described in [the how-to guide](../how-to.md).

| Environment variable | Default | Constraint |
| --- | --- | --- |
| `VELORIX_MATERIALIZED_READ_CACHE_DIR` | System temporary directory, then `velorix-materialized-read-cache/<operator-id digest>` | An explicitly empty value disables the cache. |
| `VELORIX_MATERIALIZED_READ_CACHE_MEMORY_BYTES` | `67108864` (64 MiB) | Positive and addressable by the process. |
| `VELORIX_MATERIALIZED_READ_CACHE_DISK_BYTES` | `536870912` (512 MiB) | At least 8 MiB, addressable, and a multiple of 4096 bytes. |

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

The cache can reuse immutable checkpoint descriptors, output manifests and
data pages. Mutable metadata that selects the active view and checkpoint
remains authoritative. Hits still pass the production codec, identity,
schema, hash and root-binding checks. Cached bytes that fail validation, or a
cache I/O error, become misses followed by an authoritative fetch. Invalid
authoritative bytes fail the query. Cache hits do not waive query budgets or
checkpoint fencing.

Foyer uses `WriteOnInsertion`, so a verified insertion is eligible for disk
persistence even when it fits in memory. Graceful API shutdown drains listeners
and in-flight requests before awaiting cache close. An embedded API can attach
an `Arc<MaterializedReadCache>` with `ApiState::with_materialized_read_cache`;
its owner must drain requests, await `close()` and drop all cache handles before
reopening the same directory. The directory lock remains held until the
handles are dropped. An abrupt stop can lose pending cache writes; a later
miss fetches from authoritative storage.

Reopening persisted storage starts with an empty memory tier. Use a distinct
process when measuring disk reuse, and record the process identity and cache
configuration. Repeated requests in one process demonstrate memory reuse;
they do not establish disk recovery. Removal is best effort for an insertion
already queued to disk, so validation on every hit remains necessary.

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
tests register synthetic `orders` and `inventory` schemas, ingest generated
rows, and exercise materialized filters, grouped sum/count, a two-table join,
raw SQL, request bindings, both formats and pagination. They require no private
source fixtures or external storage credentials.

The [materialized page tests](../../crates/velorix-api/src/materialized_output_pages_tests.rs)
use generated orders/inventory rows and a small `purchases_by_user` output
schema. Their assertions cover IPC float/null/weight preservation and tamper
rejection, statistics binding and absent legacy statistics, normal public view
publication, complete `COUNT(*)` and CTE counts over 10,017 generated rows,
conservative equality/null/unknown-expression pruning, cache revalidation
after warm reads and close/reopen, persisted IPC reads in a fresh process, and
weighted cursors with epoch fencing. The fresh-process IPC test checks file
decoding; it is separate from the cache close/reopen test.

For a small manual correctness check, register an `orders` schema with UTF-8
`customer_id` and integer `amount` columns. Register an `inventory` schema with
UTF-8 `customer_id` and `category` columns and an integer `quantity` column.
Use the API's configured weight column for generated ingest updates. Admit
these view definitions through the normal view API before ingesting generated
rows; give each view a distinct output relation identifier:

```sql
SELECT customer_id, amount FROM orders WHERE amount > 0;
SELECT customer_id, SUM(amount) AS sum, COUNT(*) AS count
FROM orders WHERE amount > 0 GROUP BY customer_id;
SELECT i.customer_id, SUM(o.amount) AS sum, COUNT(*) AS count
FROM orders o JOIN inventory i ON o.customer_id = i.customer_id
GROUP BY i.customer_id;
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

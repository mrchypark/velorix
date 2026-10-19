# Native query read cache

Velorix uses Foyer 0.22.6 for a disposable memory and disk cache of verified raw
bytes at explicitly integrated immutable native query read sites. S3 and metadata
remain authoritative. The cache does not change checkpoint publication, durability,
or recovery semantics.

## Configuration

Production startup enables the cache by default. Only these three environment
variables configure it:

| Variable | Default | Behavior |
| --- | --- | --- |
| `VELORIX_NATIVE_READ_CACHE_DIR` | `std::env::temp_dir()/velorix-native-read-cache/<operator-id digest>` | Dedicated cache directory. An explicitly empty value disables the cache. |
| `VELORIX_NATIVE_READ_CACHE_MEMORY_BYTES` | `67108864` (64 MiB) | Positive, addressable capacity for byte-weighted memory entries. |
| `VELORIX_NATIVE_READ_CACHE_DISK_BYTES` | `536870912` (512 MiB) | Explicit disk capacity; must be at least 8 MiB, addressable, and a multiple of 4096 bytes. |

For production reuse across process restarts, set an explicit absolute path on a
persistent local filesystem, for example:

```sh
export VELORIX_NATIVE_READ_CACHE_DIR=/var/lib/velorix/native-read-cache
```

Use a dedicated directory writable by the API service account. On Unix, the cache
directory is set to mode `0700`. Each open holds an exclusive file lock; two API
processes must use separate directories. A conflicting open safely disables that
process's cache. The default temporary directory may be cleared by the host.

The 64 MiB memory capacity weights allocated key strings, value buffers, and their
struct sizes. Foyer also has separately bounded flush-buffer and submission-queue
budgets, each 16 MiB at the default disk capacity. Indexes, allocator overhead,
in-flight reads, and query execution consume additional memory. **64 MiB is not a
total process RSS limit.** Smaller disk configurations reduce those two budgets.

## Identity and validation

Keys bind store scope, authority namespace, tenant, program, output, object path,
content hash, codec, checkpoint epoch, and root hash. Production store scope is a
stable digest of the configured endpoint, bucket, prefix, and authority store ID;
credentials are not serialized into cache keys. Separate codecs and checkpoint
roots cannot share an entry.

Only immutable objects selected through current authoritative state are eligible.
Mutable heads and active metadata are not cached. Every hit retains the normal
authority freshness and fencing checks, then validates payload identity, codec,
hash, and root binding. Source bytes are validated before insertion. A cache hit
cannot authorize an old checkpoint or bypass query budgets.

Invalid cached bytes or cache read errors cause a miss and an authoritative fetch.
Invalid source bytes still fail validation. Cache-open failures are logged at
startup and the API continues with authoritative reads; these diagnostics do not
include credentials or payload contents. Invalid capacity configuration fails
startup rather than silently selecting another budget.

## Persistence and verification

`WriteOnInsertion` sends entries to disk even when the entire working set fits in
memory. Production startup handles Ctrl-C and, on Unix, SIGTERM: it stops accepting
requests, drains both enabled HTTP and TLS listeners, then awaits
`NativeReadCache::close()` while retaining the cache handle and directory lock.
If either listener fails, the other listener still drains before cache closure;
TLS connection watchers also finish before cache closure. The original listener
error is preserved. Cache-close failures are logged. Graceful shutdown waits for
active connections to finish.

When embedding the API or reopening a cache in a benchmark, explicitly await
`NativeReadCache::close()` to drain pending writes, then release all cache handles
before reopening the directory.
Foyer recovers persisted entries into a new cache with initially empty memory.
An abrupt stop can lose pending cache writes; subsequent misses fetch authoritative
data normally.

Removal is best effort: Foyer can still expose a queued insertion immediately
after `remove`. Every retrieval validates the bytes, so delayed eviction cannot
make an invalid payload serveable.

Focused helper tests cover memory hits, disk hits after close and reopen, memory
eviction, key isolation, invalid payload rejection, directory locking, and capacity
validation.

There are no additional cache metrics knobs.

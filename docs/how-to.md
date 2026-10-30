# How to Use Velorix Locally

This guide runs Velorix as a single-node development service backed by a local
S3-compatible Versity Gateway instance. It exercises the public product flow:

1. register a schema-bound relation
2. create a materialized view
3. ingest rows
4. query the materialized output
5. restart the API while keeping Meta running and recover the same output

This setup is for local development only. It deliberately disables API
authentication and multi-writer fencing. Do not use these settings for a
shared or production deployment. Metadata lives in the development Meta process;
this guide does not demonstrate recovery after that process restarts.
The configured API defaults to authoritative relation ingest and demand
materialization. New views defer historical work; ingest publishes source data
without eagerly applying those views. A query advances bounded work and can
return HTTP 202 while materialization is incomplete, or HTTP 200 when its
requested work completes. Explicit backfill is also available. Legacy/eager
behavior is a separate mode and is not what this guide configures.

## Prerequisites

- Rust and Cargo
- Docker with a running daemon
- `curl`
- free local ports `8080`, `9090`, and `9000`

Run all commands from the repository root.

External family specifications are unsupported. A nonempty
`VELORIX_NATIVE_FAMILY_SPEC_PATH` causes API startup to fail; unset it and use
supported generic SQL views. External specs do not establish that SQL has the
same semantics as the fixed runtime.

## 1. Start Versity Gateway

Create an isolated Docker network and a persistent volume:

```bash
docker network create velorix-local
docker volume create velorix-local-versitygw-data

docker run -d \
  --name velorix-local-versitygw \
  --network velorix-local \
  -p 9000:9000 \
  -e ROOT_ACCESS_KEY=velorix-local \
  -e ROOT_SECRET_KEY=velorix-local-secret \
  -v velorix-local-versitygw-data:/data \
  versity/versitygw:v1.8.0 \
  --port :9000 posix /data
```

The fresh volume uses the Versity Gateway POSIX layout. Keep any existing
RustFS `velorix-local-data` volume intact: migrate its objects through an S3-level
export/copy into Versity Gateway; do not mount the raw RustFS volume at `/data`.

Wait for the S3 API and create a bucket. The AWS CLI runs in Docker, so a
host-side AWS CLI installation is not required:

```bash
until docker run --rm \
  --network velorix-local \
  -e AWS_ACCESS_KEY_ID=velorix-local \
  -e AWS_SECRET_ACCESS_KEY=velorix-local-secret \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli:2.17.36 \
  --endpoint-url http://velorix-local-versitygw:9000 \
  s3api list-buckets >/dev/null 2>&1; do
  sleep 1
done

docker run --rm \
  --network velorix-local \
  -e AWS_ACCESS_KEY_ID=velorix-local \
  -e AWS_SECRET_ACCESS_KEY=velorix-local-secret \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli:2.17.36 \
  --endpoint-url http://velorix-local-versitygw:9000 \
  s3api create-bucket \
  --bucket velorix-local \
  --region us-east-1
```

## 2. Start Meta and the API

In terminal 1, start development memory Meta on loopback:

```bash
export VELORIX_META_MODE=development
export VELORIX_META_BACKEND=memory
export VELORIX_META_BIND=127.0.0.1:9090
export VELORIX_META_BEARER_TOKEN=velorix-local-meta-token
cargo run -p velorix-meta
```

Keep Meta running throughout the guide, including the API restart. Memory Meta
supports the authoritative view bootstrap required here, but loses its metadata
when its process stops. The OSS Meta backend does not support that bootstrap;
substituting it would cause view creation to fail closed.

In terminal 2, configure the API with the Meta endpoint and the same Meta bearer
token:

```bash
export VELORIX_S3_COMPAT=1
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_ACCESS_KEY_ID=velorix-local
export AWS_SECRET_ACCESS_KEY=velorix-local-secret
export AWS_REGION=us-east-1
export VELORIX_S3_BUCKET=velorix-local
export VELORIX_S3_PREFIX=quickstart
export VELORIX_META_GRPC_ENDPOINT=http://127.0.0.1:9090
export VELORIX_META_BEARER_TOKEN=velorix-local-meta-token
export VELORIX_RELATION_INGEST_OWNER_ID=velorix-local-ingest-owner
export VELORIX_API_BIND=127.0.0.1:8080
export VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1
export VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only

# Optional materialized read cache directory. Unset this variable to use the
# default directory under the system temporary directory, or set it to an empty
# value to disable the cache.
export VELORIX_MATERIALIZED_READ_CACHE_DIR="${TMPDIR:-/tmp}/velorix-materialized-read-cache"

cargo run -p velorix-api
```

The first build can take several minutes. Keep this process running.

In terminal 3, check the service and run all remaining request commands:

```bash
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

`/readyz` should report `"status":"ready"`, an object store configured with
conditional updates, and `"standing_runtime_fencing_mode":"unsafe-dev-only"`.

For an isolated local experiment with multiple API processes, set
`VELORIX_API_REPLICA_COUNT` to the actual process count and
`VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-multi-api`. Set
`VELORIX_API_OWNER_ENDPOINT` on every process to its reachable HTTP(S) origin
(for example, `http://127.0.0.1:8080`); this origin is stored with each runtime
owner claim so requests can reach the current leaseholder. Multi-replica startup
fails if the endpoint is missing or is not a plain HTTP(S) origin. This explicit
mode requires loopback API/TLS listeners and an `http://` loopback Meta endpoint;
readiness reports `unsafe-dev-multi-api`. It skips the Meta fencing-capability
validation, while runtime-owner acquisition and owner-checked checkpoint
publication remain active. It is not a production or multi-writer-safety
guarantee. The existing `unsafe-dev-only` mode remains single-replica only.

### Split API and worker services

For v0.5.0, the `velorix-api` image defaults to
`VELORIX_SERVICE_ROLE=api`, and the `velorix-worker` image defaults to
`VELORIX_SERVICE_ROLE=worker`. This changes the API image's default from the
binary's `all` role: deployments that need the previous combined API/compute
behavior must set `VELORIX_SERVICE_ROLE=all` explicitly. The all-in-one image
is named `ghcr.io/mrchypark/velorix-all`. In its default `all` mode it requires
`VELORIX_META_BACKEND=rhiza-kv`, `VELORIX_META_MODE`, `VELORIX_META_BIND`, and
`VELORIX_META_GRPC_ENDPOINT`. Set the local client endpoint to
`http://127.0.0.1:<bind-port>`; with `VELORIX_META_TRANSPORT_SECURITY=native-mtls`,
use `https://127.0.0.1:<bind-port>` and the existing Meta TLS client settings.
Choose the Meta bind host for the Rhiza peer topology and restrict non-loopback
listeners through deployment network policy; the entrypoint validates the
client endpoint host and port, not the server bind host. It starts Meta, runs
the authenticated Rhiza capability smoke (`auth_enforced=true`,
`production_multi_writer_safe=false`) using the configured TLS when enabled,
then starts the API with role `all`.

Configure an external readiness probe against the API `/readyz` endpoint; the
entrypoint does not wait for API readiness. Allow at least 20 seconds for
container shutdown: the supervisor gives each tracked child up to 5 seconds
after TERM before KILL and wait. The CI entrypoint contract uses stub processes
and does not validate a real Rhiza-backed container launch.

`VELORIX_SERVICE_ROLE` defaults to `all`; set it to `api` or `worker` to run
separate services. Split mode requires `VELORIX_API_BEARER_TOKEN` and
`VELORIX_META_GRPC_ENDPOINT`. Configure the API and every worker with the same
complete comma-separated list of up to 32 reachable worker HTTP(S) origins in
`VELORIX_WORKER_ENDPOINTS`. Scale API and worker services independently; update
the list on every API and worker when workers are added or removed. Each
process needs its own bind address,
`VELORIX_API_OWNER_ENDPOINT`, `VELORIX_OPERATOR_ID`, and
`VELORIX_MATERIALIZED_READ_CACHE_DIR`. Worker endpoints and owner addresses must
be stable addresses reaching each worker directly; do not use a shared load
balancer that randomly distributes requests across workers, because owner-bound
RPCs reaching another process can cause hop/pending loops. All processes must use the same Meta
authority, S3-compatible endpoint, bucket, prefix, AWS credentials, Meta token,
and API bearer token. Keep secrets in your secret manager and pass them through
environment references. For secure Meta connections, reuse the native mTLS
settings described in [the Meta setup](#use-existing-rhiza-meta-for-the-default-server).

For upgrades, drain and stop API/worker/ingest-writer processes and any other
metadata writers; upgrade and start Meta first, then start the matching API,
worker, and ingest-writer release.
After the new release records owner endpoints in Meta snapshots, rolling Meta
back to an older binary is not safe: older Meta rejects the new snapshot field.
Rollback requires restoring a consistent Meta backup taken before cutover before
starting the older API/worker release. Do not infer old-binary compatibility from
unchanged snapshot or checkpoint schema-version numbers.

The commands below assume the shared Meta TLS, storage, credentials, and
fencing environment from the preceding setup is already exported in both
processes. Also export the same stable `$INGEST_OWNER_ID` as
`VELORIX_RELATION_INGEST_OWNER_ID` in both; the API uses it as the source
publisher identity. Common environment includes secret references, never
literal secret values. The example invokes the binary directly; launcher
forwarding of these settings has not been verified.

```sh
# API process
VELORIX_SERVICE_ROLE=api \
VELORIX_WORKER_ENDPOINTS=http://127.0.0.1:8081 \
VELORIX_API_BIND=127.0.0.1:8080 \
VELORIX_API_OWNER_ENDPOINT=http://127.0.0.1:8080 \
VELORIX_OPERATOR_ID=api-1 \
VELORIX_MATERIALIZED_READ_CACHE_DIR=/var/tmp/velorix-api-1-cache \
./target/release/velorix-api

# Worker process, inheriting the shared environment; one worker replica
VELORIX_SERVICE_ROLE=worker \
VELORIX_WORKER_ENDPOINTS=http://127.0.0.1:8081 \
VELORIX_API_BIND=127.0.0.1:8081 \
VELORIX_API_OWNER_ENDPOINT=http://127.0.0.1:8081 \
VELORIX_OPERATOR_ID=worker-1 \
VELORIX_MATERIALIZED_READ_CACHE_DIR=/var/tmp/velorix-worker-1-cache \
./target/release/velorix-api
```

Compute replica count is zero for `api`, `VELORIX_API_REPLICA_COUNT` for `all`,
and the worker endpoint list length (at least one) for `worker`.

`VELORIX_WORKER_MAX_CONCURRENT_JOBS` (default `4`) limits concurrent worker RPC
admissions per process; it does not limit `all` role local execution. It is an
admission limit alongside existing runtime and Foyer cache limits, not an OS
CPU or RSS cap. Split roles block public mutating routes on workers. See the
[split-role behavior and view constraints](development/generic-view-pipeline.md#split-api-and-worker-roles).

### Materialized Read Cache

The API caches verified immutable materialized read bytes in a Foyer hybrid
memory and disk cache. Three environment variables configure it:

| Environment variable | Default | Constraint |
| --- | --- | --- |
| `VELORIX_MATERIALIZED_READ_CACHE_DIR` | System temporary directory, then `velorix-materialized-read-cache/<operator-id digest>` | An explicitly empty value disables the cache. |
| `VELORIX_MATERIALIZED_READ_CACHE_MEMORY_BYTES` | `67108864` (64 MiB) | Positive and addressable by the process. |
| `VELORIX_MATERIALIZED_READ_CACHE_DISK_BYTES` | `536870912` (512 MiB) | At least 8 MiB, addressable, and a multiple of 4096 bytes. |

Use a dedicated directory per API process on a persistent local filesystem if
you want the cache to survive process restarts. Two API processes must not
share one directory; a conflicting open disables that process's cache and
falls back to authoritative reads. Invalid capacity values fail startup. See
[materialized query reads and cache](development/materialized-query-cache.md)
for the cache lifecycle and validation rules.

## 3. Create a Relation and a View

This quickstart registers a `scores` relation. It has these columns:

| Column | Type | Role |
| --- | --- | --- |
| `user_id` | UTF-8 string | primary key |
| `score` | signed 64-bit integer | business value |

Create the relation:

```bash
curl -fsS -X POST \
  http://127.0.0.1:8080/v1/relations \
  -H 'content-type: application/json' \
  -d '{
    "catalog": {
      "adapter_id": "incremental-adapter-single-key-sum-count-v1",
      "relation_schema": {
        "relation_id": "scores",
        "relation_name": "scores",
        "relation_version": "2026-05-24.v1",
        "columns": [
          {"column_id":"user_id","name":"user_id","logical_type":{"kind":"utf8"},"physical_arrow_type":{"kind":"utf8"},"nullable":false,"ordinal":0},
          {"column_id":"score","name":"score","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":1}
        ],
        "primary_key_column_ids": ["user_id"],
        "allowed_operations": ["insert", "update", "upsert"],
        "event_time_column_id": null
      }
    }
  }'
```

Create a standing materialized view that keeps positive-score totals by user:

```bash
curl -fsS -X POST \
  http://127.0.0.1:8080/v1/views \
  -H 'content-type: application/json' \
  -d '{
    "view_id": "positive_scores_by_user",
    "urlPath": "/scores/positive",
    "input_relation_id": "scores",
    "input_relation_version": "2026-05-24.v1",
    "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    "source_kind": "standing_view",
    "description": "Positive score totals by user",
    "response_formats": ["json"]
  }'
```

Creating the view defers materialization. After ingest, the first query can
perform a bounded demand step; if work remains, it returns HTTP 202 with
materialization progress. Continue the request or use explicit backfill until
the full view is materialized. The sequence below uses explicit backfill so its
final query reads a complete result.

The view response should contain:

```json
{
  "view_id": "positive_scores_by_user",
  "execution_mode": "standing_runtime",
  "query_enabled": false,
  "outcome": "created"
}
```

## 4. Ingest Rows

Ingest four rows into the `scores` relation:

```bash
curl -fsS -X POST \
  http://127.0.0.1:8080/v1/relations/scores/ingest \
  -H 'content-type: application/json' \
  -d '{
    "relation_version": "2026-05-24.v1",
    "stream_id": "quickstart",
    "partition_id": 0,
    "start_offset_inclusive": 0,
    "row_format": "business_rows",
    "rows": [
      {"user_id": "ada", "score": 10},
      {"user_id": "ada", "score": 15},
      {"user_id": "ada", "score": -7},
      {"user_id": "grace", "score": 4}
    ]
  }'
```

Inspect the ingest response: only `"ack_mode":"materialized"` together with
`"materialization":{"status":"completed",...}` confirms a materialized
checkpoint update. Authoritative activation is separate, so even a completed
acknowledgement does not establish query availability. After this first ingest,
explicitly backfill the view before querying:

```bash
curl -fsS -X POST \
  http://127.0.0.1:8080/v1/views/positive_scores_by_user/backfill \
  -H 'content-type: application/json' \
  -d '{}'
```

Querying before backfill can return HTTP `503` with `MATERIALIZATION_LAG` because
authoritative activation is incomplete. Running backfill before the first
ingest can return HTTP `409` because no checkpoint covers the bootstrap cut.
Keep the order above: create view, ingest, backfill, query.

Offsets identify an ordered stream partition. For the next request on the same
stream and partition, start at offset `4`; do not reuse or skip offsets.

## 5. Query Materialized Output

Query routes default to Arrow IPC streams (`application/vnd.apache.arrow.stream`)
when `Accept` is absent or `*/*`. Send `Accept: application/json` for the JSON
envelope below. Both formats include `x-velorix-logical-epoch` and, when another
page exists, `x-velorix-next-page-token` headers. Arrow schema metadata carries
`velorix.logical_epoch` and `velorix.next_page_token`. Continue with the token as
`page_token` and the same `epoch`. Unsupported or malformed Accept values return
406; output row and wire-byte limits apply to both formats.

Query the view directly:

```bash
curl -fsS -H 'Accept: application/json' \
  'http://127.0.0.1:8080/v1/views/positive_scores_by_user/query?max_rows=100'
```

Expected rows:

```json
{
  "rows": [
    {"count": 2, "sum": 25, "user_id": "ada"},
    {"count": 1, "sum": 4, "user_id": "grace"}
  ]
}
```

The negative score is stored in the relation but excluded by the view's
`WHERE score > 0` predicate. The query reads published materialized output; it
does not recompute the view from the source relation.

The same view is also exposed through its promoted API path:

```bash
curl -fsS -H 'Accept: application/json' \
  'http://127.0.0.1:8080/v1/api/scores/positive?max_rows=100'
```

## 6. Verify Reads After an API Restart

Stop only the API in terminal 2 with `Ctrl-C`. Keep Meta in terminal 1 and
Versity Gateway running. Rerun the API exports and command from step 2 with the
same `quickstart` prefix and Meta endpoint/token. Repeat the query from step 5
in terminal 3. The same rows should be returned without recreating the relation,
view, or ingest request.

This restart check verifies reads only. Memory Meta can retain the previous API
process's runtime-owner lease, so an immediate ingest or backfill after restart
can fail with HTTP `409` due to an owner conflict. Do not treat restarting or
retrying as proof that writes can resume. Inspect
`GET /v1/standing-runtime/owners` and follow the
[writer-owner attachment guidance](development/vind-product.md) before routing
writes; [ownership and fencing](architecture/partition-ownership-protocol-v1.md)
describes the fail-closed boundary.

Velorix restores active view metadata, runtime checkpoints, and committed
ingest state from the API prefix while bootstrap metadata remains in the live
memory Meta process. Restarting Meta loses that metadata, so this API-only
restart does not establish full durability. Full restart recovery requires a
bootstrap-capable durable metadata backend, such as Rhiza; see
[Meta service](architecture/meta-service.md) and
[Rhiza Kubernetes recovery](development/rhiza-kv-k8s.md). An absent Meta endpoint
can cause authoritative view admission to return HTTP `503`.

## Unsupported SQL Fails During Admission

Velorix accepts only SQL shapes supported by its internal materialized runtime.
For example, global median is unsupported and returns HTTP `400` without
registering a fallback view. Grouped median over an admitted Int64 column is
supported; standalone global aggregate views currently support only `COUNT(*)`.

```bash
curl -sS -i -X POST \
  http://127.0.0.1:8080/v1/views \
  -H 'content-type: application/json' \
  -d '{
    "view_id": "unsupported_median",
    "input_relation_id": "scores",
    "input_relation_version": "2026-05-24.v1",
    "sql": "select median(score) from scores",
    "response_formats": ["json"]
  }'
```

## Custom Relations

`POST /v1/relations` accepts the public catalog shape. List business columns
and their key/operation metadata; Velorix derives internal catalog fields.
`adapter_id` is optional and defaults to `incremental-adapter-generic-v1`.

```json
```bash
curl -fsS -X POST http://127.0.0.1:8080/v1/relations \
  -H 'content-type: application/json' \
  -d '{"catalog":{"adapter_id":"incremental-adapter-generic-v1","relation_schema":{"relation_id":"src_table","relation_name":"src_table","relation_version":"v1","columns":[{"column_id":"key_col","name":"key_col","logical_type":{"kind":"utf8"},"physical_arrow_type":{"kind":"utf8"},"nullable":false,"ordinal":0},{"column_id":"value_col","name":"value_col","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":1}],"primary_key_column_ids":["key_col"],"allowed_operations":["insert","update","upsert"],"event_time_column_id":null}}}'
```

Then submit business rows. A plain row is an insert; do not include any
engine-maintained multiplicity field:

```bash
curl -fsS -X POST http://127.0.0.1:8080/v1/relations/src_table/ingest \
  -H 'content-type: application/json' \
  -d '{"relation_version":"v1","stream_id":"src-table-stream","partition_id":0,"start_offset_inclusive":0,"row_format":"business_rows","rows":[{"key_col":"a","value_col":7}]}'
```

To replace that row, submit an update envelope at the next input offset. The
before and after payloads contain business fields only; Velorix generates the
retraction and replacement weights internally:

```bash
curl -fsS -X POST http://127.0.0.1:8080/v1/relations/src_table/ingest \
  -H 'content-type: application/json' \
  -d '{"relation_version":"v1","stream_id":"src-table-stream","partition_id":0,"start_offset_inclusive":1,"row_format":"operation_envelopes","rows":[{"operation":"update","before":{"key_col":"a","value_col":7},"after":{"key_col":"a","value_col":9}}]}'
```

Column names such as `weight`, `delta`, and `operation` remain available as
ordinary business columns when declared in the schema. Because `operation`
also discriminates update envelopes, a complete business row that matches a
recognized operation-envelope shape is rejected as ambiguous. `delete` may
appear in relation operation metadata, but public DELETE ingest is unsupported.
Set `row_format` to `business_rows` when the row is business data that also
matches an envelope shape, or to `operation_envelopes` when it is an operation.
For example, when a relation declares `operation` (UTF-8) and `before`/`after`
(JSON) as business columns, send the raw values with an explicit format:

```json
{
  "relation_version": "v1",
  "stream_id": "src-table-stream",
  "partition_id": 0,
  "start_offset_inclusive": 2,
  "row_format": "business_rows",
  "rows": [{"operation":"update","before":{"key_col":"a","value_col":7},"after":{"key_col":"a","value_col":9}}]
}
```

Omitting `row_format` auto-detects unambiguous rows and fails only when the
same row is valid both as a complete business record and as an operation
envelope. Choosing a format has observable effect only for those ambiguous
rows. Neither format accepts an engine multiplicity value from the caller.

The multi-relation endpoint puts `row_format` on each batch, so batches in one
request may choose their interpretation independently:

```bash
curl -fsS -X POST http://127.0.0.1:8080/v1/relations/ingest \
  -H 'content-type: application/json' \
  -d '{"batches":[{"relation_id":"src_table","relation_version":"v1","stream_id":"src-table-stream","partition_id":0,"start_offset_inclusive":1,"row_format":"operation_envelopes","rows":[{"operation":"update","before":{"key_col":"a","value_col":7},"after":{"key_col":"a","value_col":9}}]}]}'
```

Authoritative mode accepts one batch per request; this endpoint does not make
multiple relation batches an atomic transaction.

Request-shape errors return HTTP `422`: for example, an unknown/private field
in relation registration or an invalid `row_format` enum value. A request that
parses but fails relation-schema or row/operation validation returns HTTP `400`.
For example, an ambiguous row without an explicit format or a malformed
operation envelope is a semantic `400`, not a request-shape `422`.

The complete materialized-view SQL contract, including every supported feature
class and the fail-closed unsupported classes, is in
[`architecture/supported-sql.md`](architecture/supported-sql.md).

The live API contract is available at:

```bash
curl -fsS http://127.0.0.1:8080/v1/openapi.json
```

<a id="event-time-windows-with-bounded-corrections"></a>

## Event-Time Windows with Unlimited Lateness

This walkthrough uses the optional `correction_horizon_ns` API field as
hot-state retention. Core/runtime/API verification is complete, including
durable affected-window cold recovery and idempotent retries. It
requires a build containing the revised window correction extension; it is not a claim
that an older release or a deployed cluster supports it. Keep the Meta/API
processes from this guide running. Register a new relation before ingesting any
rows, then create its view. As in the quickstart, authoritative activation
requires first ingest, then backfill, then query.

Save this canonical schema as `/tmp/purchases-event-time-schema.json`:

```json
{
  "catalog": {
    "adapter_id": "incremental-adapter-generic-v1",
    "relation_schema": {
      "relation_id": "purchases",
      "relation_name": "purchases",
      "relation_version": "v1",
      "columns": [
        {"column_id":"user_id","name":"user_id","logical_type":{"kind":"utf8"},"physical_arrow_type":{"kind":"utf8"},"nullable":false,"ordinal":0},
        {"column_id":"amount","name":"amount","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":1},
        {"column_id":"event_time","name":"event_time","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":2}
      ],
      "primary_key_column_ids": ["user_id"],
      "allowed_operations": ["insert", "update", "upsert"],
      "event_time_column_id": "event_time"
    }
  }
}
```

Register the public schema directly; the event-time column is explicit and is
not inferred from the SQL:

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/relations \
  -H 'content-type: application/json' \
  --data-binary @/tmp/purchases-event-time-schema.json

curl -fsS -X POST http://127.0.0.1:8080/v1/views \
  -H 'content-type: application/json' --data-binary '{
    "view_id":"purchases_by_user_minute",
    "input_relation_id":"purchases",
    "input_relation_version":"v1",
    "source_kind":"standing_view",
    "correction_horizon_ns":120000000000,
    "sql":"SELECT TUMBLE(INTERVAL '\''60 seconds'\'') AS window, user_id, SUM(amount) AS total_amount, COUNT(*) AS event_count FROM purchases GROUP BY window, user_id"
  }'
```

The SQL expands the `window` alias into flat `window_start` and `window_end`
nanosecond output columns; it does not return a nested `window` object.
`HOP(INTERVAL '30 seconds', INTERVAL '60 seconds')` uses the same path for a
30-second slide and 60-second size. Normal mode also supports
`SESSION(INTERVAL '30 seconds') AS window` with the same flat-boundary output;
omit the correction field for that form. The business group must be the relation's
single primary key (`user_id` here), not an unrelated category column. Supported
window aggregates include `SUM`, `COUNT(*)`, `COUNT(amount)`, `MIN`, `MAX`, and
`AVG` over the admitted shared value column. Correction mode rejects `SESSION`
and window Top-K when explicitly configured. Omit `correction_horizon_ns` for
new compatible TUMBLE/HOP views to use one window width of hot retention
(`60000000000` here; HOP uses its size, not its slide). SESSION/TopK use plan
policy `None`, retain all state, and accept valid late rows. Strict rejection
requires the internal explicit `Some(Reject)` policy. Zero or negative hot
retention durations remain admission errors.

Ingest two rows and explicitly advance this partition's watermark to 60 seconds:

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/relations/purchases/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":0,
    "row_format":"business_rows",
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":70000000000,"watermark_ns":60000000000},
    "rows":[
      {"user_id":"alice","amount":10,"event_time":10000000000},
      {"user_id":"alice","amount":7,"event_time":70000000000}
    ]
  }'

curl -fsS -X POST http://127.0.0.1:8080/v1/views/purchases_by_user_minute/backfill \
  -H 'content-type: application/json' --data-binary '{}'

curl -fsS -H 'Accept: application/json' -X POST http://127.0.0.1:8080/v1/views/purchases_by_user_minute/query \
  -H 'content-type: application/json' --data-binary '{}'
```

The published `[0, 60 seconds)` row has `total_amount=10` and `event_count=1`;
the `[60, 120 seconds)` window is still open. Send a late row at 30 seconds while
keeping the watermark at 60 seconds:

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/relations/purchases/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":2,
    "row_format":"business_rows",
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":70000000000,"watermark_ns":60000000000},
    "rows":[{"user_id":"alice","amount":5,"event_time":30000000000}]
  }'
```

Query the same endpoint again: the already published row now has
`total_amount=15` and `event_count=2`. Public DELETE input is unsupported, and
the request has no caller-supplied multiplicity field. Correct the row with an
UPDATE envelope; Velorix creates the signed retraction internally:

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/relations/purchases/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":3,
    "row_format":"operation_envelopes",
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":70000000000,"watermark_ns":60000000000},
    "rows":[{"operation":"update","before":{"user_id":"alice","amount":5,"event_time":30000000000},"after":{"user_id":"alice","amount":1,"event_time":30000000000}}]
  }'

curl -fsS -X POST http://127.0.0.1:8080/v1/relations/purchases/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":5,
    "row_format":"business_rows",
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":180000000000,"watermark_ns":180000000000},
    "rows":[{"user_id":"alice","amount":1,"event_time":180000000000}]
  }'
```

The update replaces the old row with the corrected value. Advancing `W` to 180
seconds reaches its hot-retention boundary `end <= W - H`
(`60 <= 180 - 120`); the runtime recovers durable cold state for later corrections while the
published row remains queryable. The second window is published with total
7/count 1. Send a far-late UPDATE replacing the corrected event at 30 seconds
with its revised amount, at offset 6 and watermark 180 seconds. The required
ingest result is HTTP 201 Created,
followed by a query returning HTTP 200 with the old `[0, 60 seconds)` row
changing to total 15/count 2. API regression tests verify this behavior;
the commands below are not recorded live-service evidence:

```sh
curl -sS -i -X POST http://127.0.0.1:8080/v1/relations/purchases/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":6,
    "row_format":"operation_envelopes",
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":180000000000,"watermark_ns":180000000000},
    "rows":[{"operation":"update","before":{"user_id":"alice","amount":1,"event_time":30000000000},"after":{"user_id":"alice","amount":5,"event_time":30000000000}}]
  }'
```

Query the view endpoint again to check the old window's updated aggregate.
For `HOP`, all affected targets must update atomically even when some have moved
to cold state and others remain hot. An idempotent retry of this accepted range
must return HTTP 200 without applying the correction twice. Repeat after checkpoint
restart to verify durable affected-window recovery; no full-source DataFusion
recomputation may substitute for that recovery.
Missing cold state returns HTTP 503 and corrupt cold state returns HTTP 400:
these fail-closed availability/integrity errors must never be age cutoffs.

This walkthrough uses one API instance. The relation admission fence shared by
view creation, ingest, and backfill is local to one shared `ApiState` in that
process. It does not prove
safe correction admission across multiple API processes; that depends on the
existing metadata protocol and remains outside this walkthrough's evidence.

Every batch above supplies ingress watermark metadata. Watermarks must be
monotonic per partition, and the declared maximum must cover the actual batch
event times. Multiple tracked partitions advance at their minimum watermark.
In correction mode this effective global watermark must also remain monotonic:
a newly introduced partition cannot supply a watermark below the already
committed global value. Such regression is rejected before source writes.
Missing partition progress can pin closure/expiry, but every window input batch
still requires watermark metadata. Waiting or omitting metadata does not
automatically advance an idle partition's watermark.

Queries read materialized snapshots, and valid late rows may change historical
output beyond hot retention. Moving state to cold storage does not delete output or bound
all storage. Incremental state/delta work does not guarantee an entire epoch is
O(affected windows): `EpochCommit` may still require full published-snapshot
construction/serialization. No source full-recomputation fallback is used.
The existing 8 MiB snapshot cap and published-output memory cost remain
output-cardinality constraints, not lateness limits.
Immutable cold-object keys are intentionally outside legacy GC. Archive
versions remain without GC until retained standing-checkpoint references can
be traversed safely, so disk/object storage grows with history and corrections.
This does not guarantee source retention against external TTL/lifecycle
deletion. The full-snapshot/output-memory baseline remains unchanged.
See [the supported SQL contract](architecture/supported-sql.md#event-time-windows-and-unlimited-lateness)
for scope and evidence limits. Verification passed 363 core planner, 261 runtime
integration, and 55 runtime library tests; runtime clippy `-D warnings` and
formatting checks passed. API library verification passed 221/221 tests;
workspace formatting and clippy `--workspace --all-targets -- -D warnings`
also passed, as did storage library and storage test targets. API tests cover
legacy and authoritative ingress, affected-window reconstruction from legacy
checkpoints, missing/corrupt cold state, and identical retry after archive
failure without restart. This revised request sequence has not been
executed against a live Meta/API service; no benchmark result is claimed.

## Reproduce the Incremental SQL Baseline

Run the shared correctness corpus and replace the archived Velorix artifact:

```bash
./scripts/run-incremental-sql-baseline.sh
```

The runner uses Velorix admission and materialized runtime execution, compares
every committed frontier of admitted workloads with an independent DataFusion
batch SQL recomputation, checkpoints and restores at the recovery phase, and
writes `baselines/incremental-sql/velorix-v0.1.0.json`. Unsupported workloads
remain explicit `unsupported` outcomes with their actual admission error; they
are never converted to passing rows or zero performance values. Pass a path as
the first argument to write a temporary artifact instead of replacing the
archive.

### Reproduce the GreptimeDB Flow Baseline

The GreptimeDB comparison requires `psql`, `duckdb`, `curl`, and Python 3:

```bash
./scripts/run-greptimedb-flow-baseline.sh
```

The wrapper downloads the pinned GreptimeDB 1.1.4 package for the host platform,
verifies the official release checksum, starts a standalone instance on ports
14000-14003, and writes
`baselines/incremental-sql/greptimedb-flow-v1.1.4.json`. Set `GREPTIME_BIN` to
an already verified executable to skip the download, or pass an output path as
the first argument to avoid replacing the archive.

The runner evaluates every admitted sink after initial load, insert, update,
source removal, process restart, and tail replay. Each phase is compared with an
independent DuckDB batch recomputation. Because GreptimeDB does not accept SQL
`UPDATE`, the external comparison corpus models an update as a primary-key delete
followed by insert; that is benchmark input semantics, not a Velorix public-ingest
request. The artifact records the comparator's input semantic explicitly.
Admission errors and observed stale sink rows remain `unsupported` or `failed`;
the runner does not hide them with source recomputation.

## Clean Up

Stop the API in terminal 2, then Meta in terminal 1. Remove the local Versity
Gateway container and network:

```bash
docker rm -f velorix-local-versitygw
docker network rm velorix-local
```

Keep `velorix-local-versitygw-data` if you want to reuse the ingested data. To delete all
quickstart data permanently, remove the volume explicitly:

```bash
docker volume rm velorix-local-versitygw-data
```

## Production Boundary

Production operation requires authentication, a compatible metadata service,
and safe standing-runtime fencing. Do not set
`VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1` or
`VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only` in production. See
[`development/vind-product.md`](development/vind-product.md) for the deployed
product and evidence workflow.

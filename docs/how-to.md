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

## Prerequisites

- Rust and Cargo
- Docker with a running daemon
- `curl`
- free local ports `8080`, `9090`, and `9000`

Run all commands from the repository root.

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
export VELORIX_API_BIND=127.0.0.1:8080
export VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1
export VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only

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

## 3. Create a Relation and a View

The quickstart uses the built-in `scores` relation. It has these columns:

| Column | Type | Role |
| --- | --- | --- |
| `user_id` | UTF-8 string | primary key |
| `score` | signed 64-bit integer | value |
| `delta` | signed 64-bit integer | row weight |

Create the relation:

```bash
curl -fsS -X POST \
  http://127.0.0.1:8080/v1/relations/scores-default
```

Create a standing materialized view that keeps positive-score totals by user:

```bash
curl -fsS -X POST \
  http://127.0.0.1:8080/v1/views/scores-positive-default
```

This shortcut uses the built-in default query policy; no separate query-policy
registration is needed. Creating the view does not complete authoritative
activation: ingest the first batch, then backfill before querying.

The view response should contain:

```json
{
  "view_id": "positive_scores_by_user",
  "execution_mode": "standing_runtime",
  "query_enabled": true,
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
    "rows": [
      {"user_id": "ada", "score": 10, "delta": 1},
      {"user_id": "ada", "score": 15, "delta": 1},
      {"user_id": "ada", "score": -7, "delta": 1},
      {"user_id": "grace", "score": 4, "delta": 1}
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

`POST /v1/relations` accepts an explicit relation catalog. Create only the
canonical `VelorixRelationSchemaV1` input; the CLI computes the fingerprint,
copies it into every required binding, constructs the table registration, and
validates the selected ingest adapter.

Save this example as `/tmp/measurements-schema.json`:

```json
{
  "relation_id": "measurements",
  "relation_name": "measurements",
  "relation_version": "v1",
  "columns": [
    {
      "column_id": "sensor_id",
      "name": "sensor_id",
      "logical_type": {"kind": "utf8"},
      "physical_arrow_type": {"kind": "utf8"},
      "nullable": false,
      "ordinal": 0,
      "semantic_role": "primary_key"
    },
    {
      "column_id": "reading",
      "name": "reading",
      "logical_type": {"kind": "int64"},
      "physical_arrow_type": {"kind": "int64"},
      "nullable": false,
      "ordinal": 1,
      "semantic_role": "value"
    },
    {
      "column_id": "weight",
      "name": "weight",
      "logical_type": {"kind": "int64"},
      "physical_arrow_type": {"kind": "int64"},
      "nullable": false,
      "ordinal": 2,
      "semantic_role": "weight"
    }
  ],
  "primary_key_column_ids": ["sensor_id"],
  "weight_column_id": "weight",
  "allowed_operations": ["insert", "delete"],
  "event_time_column_id": null
}
```

Generate the exact API request body and register it:

```bash
cargo run -q -p velorix-cli -- relation-catalog \
  --schema /tmp/measurements-schema.json \
  --adapter-id incremental-adapter-generic-v1 \
  > /tmp/measurements-relation.json

curl -fsS -X POST \
  http://127.0.0.1:8080/v1/relations \
  -H 'content-type: application/json' \
  --data-binary @/tmp/measurements-relation.json
```

Use `--schema -` to read the schema explicitly from standard input. Both
`--schema` and `--adapter-id` are required; Velorix does not guess an adapter.
The general custom-relation adapter is
`incremental-adapter-generic-v1`. The narrower built-in adapter IDs remain
available only when their schema compatibility checks pass. Invalid JSON,
unknown fields, unsupported adapters, and adapter/schema mismatches exit
non-zero without producing a request body.

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
  "relation_id": "purchases",
  "relation_name": "purchases",
  "relation_version": "v1",
  "columns": [
    {"column_id":"user_id","name":"user_id","logical_type":{"kind":"utf8"},"physical_arrow_type":{"kind":"utf8"},"nullable":false,"ordinal":0,"semantic_role":"primary_key"},
    {"column_id":"amount","name":"amount","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":1,"semantic_role":"value"},
    {"column_id":"event_time","name":"event_time","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":2,"semantic_role":"event_time"},
    {"column_id":"delta","name":"delta","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":3,"semantic_role":"weight"}
  ],
  "primary_key_column_ids": ["user_id"],
  "weight_column_id": "delta",
  "allowed_operations": ["insert", "delete"],
  "event_time_column_id": "event_time"
}
```

The CLI computes the catalog bindings and fingerprint; the API does not infer
the event-time schema from the SQL:

```sh
cargo run -q -p velorix-cli -- relation-catalog \
  --schema /tmp/purchases-event-time-schema.json \
  --adapter-id incremental-adapter-generic-v1 \
  > /tmp/purchases-event-time-relation.json

curl -fsS -X POST http://127.0.0.1:8080/v1/relations \
  -H 'content-type: application/json' \
  --data-binary @/tmp/purchases-event-time-relation.json

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
curl -fsS -X POST http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_id":"purchases","relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":0,
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":70000000000,"watermark_ns":60000000000},
    "rows":[
      {"user_id":"alice","amount":10,"event_time":10000000000,"delta":1},
      {"user_id":"alice","amount":7,"event_time":70000000000,"delta":1}
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
curl -fsS -X POST http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_id":"purchases","relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":2,
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":70000000000,"watermark_ns":60000000000},
    "rows":[{"user_id":"alice","amount":5,"event_time":30000000000,"delta":1}]
  }'
```

Query the same endpoint again: the already published row now has
`total_amount=15` and `event_count=2`. Retract that exact row with signed weight
`-1` at the next offset:

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_id":"purchases","relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":3,
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":70000000000,"watermark_ns":60000000000},
    "rows":[{"user_id":"alice","amount":5,"event_time":30000000000,"delta":-1}]
  }'

curl -fsS -X POST http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_id":"purchases","relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":4,
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":180000000000,"watermark_ns":180000000000},
    "rows":[{"user_id":"alice","amount":1,"event_time":180000000000,"delta":1}]
  }'
```

The retraction restores the first row to total 10/count 1. Advancing `W` to 180
seconds reaches its hot-retention boundary `end <= W - H`
(`60 <= 180 - 120`); the runtime recovers durable cold state for later corrections while the
published row remains queryable. The second window is published with total
7/count 1. Send a far-late correction at event time 30 seconds with next offset
5 and watermark 180 seconds. The required ingest result is HTTP 201 Created,
followed by a query returning HTTP 200 with the old `[0, 60 seconds)` row
changing to total 15/count 2. API regression tests verify this behavior;
the commands below are not recorded live-service evidence:

```sh
curl -sS -i -X POST http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' --data-binary '{
    "relation_id":"purchases","relation_version":"v1",
    "stream_id":"purchases-stream","partition_id":0,"start_offset_inclusive":5,
    "event_time_watermark":{"event_time_column_id":"event_time","max_observed_event_time_ns":180000000000,"watermark_ns":180000000000},
    "rows":[{"user_id":"alice","amount":5,"event_time":30000000000,"delta":1}]
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
delete, process restart, and tail replay. Each phase is compared with an
independent DuckDB batch recomputation. Because GreptimeDB does not accept SQL
`UPDATE`, the corpus update is represented as the equivalent primary-key delete
followed by insert, and the artifact records that input semantic explicitly.
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

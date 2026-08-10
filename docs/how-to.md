# How to Use Velorix Locally

This guide runs Velorix as a single-node development service backed by a local
S3-compatible RustFS instance. It exercises the public product flow:

1. register a schema-bound relation
2. create a materialized view
3. ingest rows
4. query the materialized output
5. restart the API and recover the same output from durable storage

This setup is for local development only. It deliberately disables API
authentication and multi-writer fencing. Do not use these settings for a
shared or production deployment.

## Prerequisites

- Rust and Cargo
- Docker with a running daemon
- `curl`
- free local ports `8080` and `9000`

Run all commands from the repository root.

## 1. Start RustFS

Create an isolated Docker network and a persistent volume:

```bash
docker network create velorix-local
docker volume create velorix-local-data

docker run -d \
  --name velorix-local-rustfs \
  --network velorix-local \
  -p 9000:9000 \
  -e RUSTFS_ADDRESS=:9000 \
  -e RUSTFS_ACCESS_KEY=velorix-local \
  -e RUSTFS_SECRET_KEY=velorix-local-secret \
  -v velorix-local-data:/data \
  rustfs/rustfs:1.0.0-beta.4 \
  /data
```

Wait for the S3 API and create a bucket. The AWS CLI runs in Docker, so a
host-side AWS CLI installation is not required:

```bash
until docker run --rm \
  --network velorix-local \
  -e AWS_ACCESS_KEY_ID=velorix-local \
  -e AWS_SECRET_ACCESS_KEY=velorix-local-secret \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli:2.17.36 \
  --endpoint-url http://velorix-local-rustfs:9000 \
  s3api list-buckets >/dev/null 2>&1; do
  sleep 1
done

docker run --rm \
  --network velorix-local \
  -e AWS_ACCESS_KEY_ID=velorix-local \
  -e AWS_SECRET_ACCESS_KEY=velorix-local-secret \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli:2.17.36 \
  --endpoint-url http://velorix-local-rustfs:9000 \
  s3api create-bucket \
  --bucket velorix-local \
  --region us-east-1
```

## 2. Start the API

In the first terminal, configure the local object store and start Velorix:

```bash
export VELORIX_S3_COMPAT=1
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_ACCESS_KEY_ID=velorix-local
export AWS_SECRET_ACCESS_KEY=velorix-local-secret
export AWS_REGION=us-east-1
export VELORIX_S3_BUCKET=velorix-local
export VELORIX_S3_PREFIX=quickstart
export VELORIX_API_BIND=127.0.0.1:8080
export VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1
export VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only

cargo run -p velorix-api
```

The first build can take several minutes. Keep this process running.

In a second terminal, check the service:

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

A successful response has `"ack_mode":"materialized"` and
`"materialization":{"status":"completed",...}`. The acknowledged response
means the affected materialized view and its durable checkpoint were updated.

Offsets identify an ordered stream partition. For the next request on the same
stream and partition, start at offset `4`; do not reuse or skip offsets.

## 5. Query Materialized Output

Query the view directly:

```bash
curl -fsS \
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
curl -fsS \
  'http://127.0.0.1:8080/v1/api/scores/positive?max_rows=100'
```

## 6. Verify Recovery

Stop the API with `Ctrl-C`, leave RustFS running, and rerun the exports and
`cargo run -p velorix-api` command from step 2. Then repeat the query from step
5. The same rows should be returned without recreating the relation, view, or
ingest request.

Velorix restores active view metadata, runtime checkpoints, and committed
ingest state from the same bucket and `VELORIX_S3_PREFIX`.

## Unsupported SQL Fails During Admission

Velorix accepts only SQL shapes supported by its internal materialized runtime.
For example, the following unsupported aggregate returns HTTP `400` and does
not register a fake fallback view:

```bash
curl -sS -i -X POST \
  http://127.0.0.1:8080/v1/views \
  -H 'content-type: application/json' \
  -d '{
    "view_id": "unsupported_median",
    "input_relation_id": "scores",
    "input_relation_version": "2026-05-24.v1",
    "sql": "select user_id, median(score) from scores group by user_id",
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

Stop the API first. Then remove the local RustFS container and network:

```bash
docker rm -f velorix-local-rustfs
docker network rm velorix-local
```

Keep `velorix-local-data` if you want to reuse the ingested data. To delete all
quickstart data permanently, remove the volume explicitly:

```bash
docker volume rm velorix-local-data
```

## Production Boundary

Production operation requires authentication, a compatible metadata service,
and safe standing-runtime fencing. Do not set
`VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1` or
`VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only` in production. See
[`development/vind-product.md`](development/vind-product.md) for the deployed
product and evidence workflow.

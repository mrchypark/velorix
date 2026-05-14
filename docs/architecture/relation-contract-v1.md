# Relation Contract V1

Status: Accepted
Applies to: ingest, replay, DataFusion ad hoc SQL, Feldera standing views, and
incremental-engine input adapters.

Velorix uses one relation catalog to connect typed ingest payloads, SQL query
registration, standing-view artifacts, and incremental execution.

## Catalog Object

`InputRelationCatalogV1` is stored as a create-only object:

```text
v1/relations/{relation_id}/versions/{relation_version}.relation.json
```

The storage registry is create-only by `(relation_id, relation_version)`.
Duplicate creates with the same stored body are idempotent; duplicate creates
with a different body fail as conflicts. Reads must re-validate the catalog
body and reject records whose stored relation identity does not match the key.

Required fields:

- `schema_version`.
- `relation_id`.
- `relation_name`.
- `relation_version`.
- `stream_id`.
- partitioning policy and offset domain.
- `schema_fingerprint`.
- `primary_key_column_ids`.
- `weight_column_id`.
- `operation_model`, currently `signed_weight_delta`.
- Arrow IPC ingest format.
- DataFusion table registration name and mode.
- Feldera relation id and required schema fingerprint.
- incremental-engine input adapter id.

## Contract

- Ingest envelopes must include `relation_id`, `relation_version`, and
  `schema_fingerprint`.
- DataFusion tables are registered from relation catalog schemas.
- Feldera artifacts validate against relation catalog identity.
- For the closed Velorix 1.0 sum/count adapter scope, incremental execution
  converts catalog-validated Arrow batches into typed adapter inputs before
  runtime replay. Generic row-shaped or multi-value adapter inputs are not part
  of the 1.0 contract.
- Catalog-backed sum/count adapters must declare whether they use legacy scalar
  single-key encoding or row-key encoding. Row-key encoding keeps a single value
  column and encodes multi-column primary keys as deterministic JSON objects
  keyed by stable catalog column id.
- Catalog-backed sum/count execution selects exact Decimal128 value aggregation
  only when the relation catalog has exactly one Decimal128 value column for a
  known sum/count adapter. It consumes the adapter's canonical fixed-scale
  Decimal128 strings, aggregates scaled integers, and emits aggregate state with
  a string `sum` and numeric `count`; integer value columns keep the legacy
  numeric `sum` shape.
- Relation mismatch fails closed before view activation, query execution, or
  checkpoint publication.

## Velorix 1.0 Incremental Adapter Scope

Velorix 1.0 supports only catalog-backed sum/count incremental-input adapters.
The closed 1.0 adapter ID set is:

- `incremental-adapter-single-key-sum-count-v1`.
- `incremental-adapter-row-key-sum-count-v1`.
- `incremental-adapter-orders-sum-count-v1`, as a compatibility alias for the
  scalar single-key/single-value sum/count shape.

The scalar adapter requires exactly one primary-key column and exactly one value
column. The row-key adapter allows one or more primary-key columns but still
requires exactly one value column; row-key support does not imply generic
row-shaped incremental execution.

Velorix 1.0 does not support generic row-shaped incremental adapters,
multi-value adapters, or forward-compatible future adapter IDs. Checked
production catalog admission, activation, replay, and recovery reject
unsupported adapter IDs and unsupported adapter shapes fail-closed. Checked
recovery rejects them before checkpoint hydration and before incremental-engine
construction.

## Verification

- One Arrow ingest fixture produces equivalent net rows through DataFusion and
  the incremental input adapter.
- Feldera artifact schema fingerprint mismatch fails activation.
- DataFusion production input registration exposes typed columns, not
  `key_json`/`value_json`.
- The row-key sum/count adapter preserves existing scalar single-key output and
  proves multi-column primary-key replay through the relation catalog.
- Decimal128 value sum/count replay proves exact aggregation and checkpoint
  hydration in the catalog-backed single-value sum/count path.
- Unsupported/future adapter IDs and multi-value adapter shapes fail closed
  before catalog admission or recovery activation.
- Relation version pinning rejects payloads written for a different relation
  version.

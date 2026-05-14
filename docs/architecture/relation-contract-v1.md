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
- Incremental execution consumes typed relation batches, not durable JSON
  `DeltaKey`/`DeltaValue` payloads.
- Catalog-backed sum/count adapters must declare whether they use legacy scalar
  single-key encoding or row-key encoding. Row-key encoding keeps a single value
  column and encodes multi-column primary keys as deterministic JSON objects
  keyed by stable catalog column id.
- Relation mismatch fails closed before view activation, query execution, or
  checkpoint publication.

## Verification

- One Arrow ingest fixture produces equivalent net rows through DataFusion and
  the incremental input adapter.
- Feldera artifact schema fingerprint mismatch fails activation.
- DataFusion production input registration exposes typed columns, not
  `key_json`/`value_json`.
- The row-key sum/count adapter preserves existing scalar single-key output and
  proves multi-column primary-key replay through the relation catalog.
- Relation version pinning rejects payloads written for a different relation
  version.

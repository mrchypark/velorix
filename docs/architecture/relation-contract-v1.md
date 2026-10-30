# Relation Contract V1

Status: Accepted
Applies to: ingest, replay, DataFusion ad hoc SQL, internal materialized views, and
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

The public `POST /v1/relations` request uses `PublicRelationCatalogV1`, which
contains `relation_schema` and an optional `adapter_id` (defaulting to
`incremental-adapter-generic-v1`). Its `PublicRelationSchemaV1` contains
relation identity/version, business columns, primary-key column IDs, allowed
operations, and optional event-time column ID. Public columns do not contain
`semantic_role`; key and event-time roles are derived from the schema metadata.
The public request does not accept a schema fingerprint, engine registration,
incremental binding, or multiplicity/weight column.

The internal `VelorixRelationCatalogV1` retains these fields:

- `schema_version`.
- `relation_id`.
- `relation_name`.
- `relation_version`.
- `stream_id`.
- `schema_fingerprint`.
- nested `relation_schema`, including columns, primary-key column IDs,
  internally generated weight-column ID, allowed operations, and optional
  event-time column ID.
- `datafusion_registration` name and mode.
- `incremental_relation` relation ID and schema fingerprint.
- `incremental_adapter` ID.
- `relation_source`, distinguishing source relations from published-view
  output descriptors.

The `schema_fingerprint` returned at registration is a server-assigned opaque
catalog identity. It is carried consistently in response metadata, logical
metadata, storage/recovery, and internal ingest envelopes. Clients store and
return it as supplied; its encoding is not a public hash contract and does not
reveal the internal multiplicity column.

The Rust relation catalog does not currently store partitioning policy, offset
domain, or an ingest-format field. The ingest envelope separately carries
stream, partition, and offset range.

## Contract

- Ingest envelopes must include `relation_id`, `relation_version`, and
  `schema_fingerprint`.
- Public source rows use only declared business columns. The engine-maintained
  multiplicity column is generated internally; inserts have weight `+1`, and
  catalog-allowed updates/upserts can generate signed retractions internally.
  A `delete` value may be present in `allowed_operations` as relation metadata,
  but public source DELETE ingest remains unsupported.
  Business fields named `weight`, `delta`, or `operation` are not reserved by
  the internal multiplicity field. Operation-envelope detection is shape-based;
  if a complete business row also matches a recognized envelope shape, public
  ingest rejects it as ambiguous. Native runtime and derived-view signed
  weights remain internal runtime data.
- Ingest requests may set `row_format` to `business_rows` or
  `operation_envelopes`; the choice applies to every row in that request or
  batch. If omitted, unambiguous rows are detected automatically and only rows
  valid as both forms fail closed. The setting changes interpretation only for
  those ambiguous rows.
- DataFusion tables are registered from relation catalog schemas.
- View specs validate against relation catalog identity.
- For the checked sum/count adapter paths, incremental execution
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

## Incremental Adapter Scope

The source contains catalog-backed scalar and row-key sum/count adapters and a
generic adapter ID accepted by core relation validation. Local public API
tests exercise generic relation registration, ingest, and recursive-view
recovery, but that does not establish production deployment or whole-DAG
qualification for every generic shape.

Known adapter identifiers include:

- `incremental-adapter-single-key-sum-count-v1`.
- `incremental-adapter-row-key-sum-count-v1`.
- `incremental-adapter-orders-sum-count-v1`, as a compatibility alias for the
  scalar single-key/single-value sum/count shape.
- `incremental-adapter-generic-v1`, validated by core for generic relation
  shapes.

The scalar adapter requires exactly one primary-key column and exactly one value
column. The row-key adapter allows one or more primary-key columns but still
requires exactly one value column; row-key support does not imply generic
row-shaped incremental execution.

Multi-value and unknown adapter IDs remain rejected by the checked catalog
validators. The generic route evidence is scoped to the tested local public
API path; keep it separate from production/experimental admission and
deployment claims.

## Verification

- One Arrow ingest fixture produces equivalent net rows through DataFusion and
  the incremental input adapter.
- View schema fingerprint mismatch fails activation.
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

Implementation and model tests: [relation.rs](../../crates/velorix-core/src/relation.rs)
and [relation tests](../../crates/velorix-core/tests/relation.rs). The public
route fixture is [tests.rs](../../crates/velorix-api/src/tests.rs), including
`rest_recursive_cte_outer_first_ignores_disjoint_second_cte_across_restore`.

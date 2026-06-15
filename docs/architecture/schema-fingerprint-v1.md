# Schema Fingerprint V1

Status: Accepted
Applies to: ingest envelopes, relation catalog entries, DataFusion registration,
and materialized view validation.

Velorix does not hash raw Arrow `Schema` bytes as the durable schema identity.
It hashes a canonical relation schema model.

```text
schema_fingerprint =
  sha256("velorix-relation-schema-v1\0" || canonical_relation_schema_json)
```

## Canonical Model

`VelorixRelationSchemaV1` includes:

- `relation_id`.
- `relation_name`.
- `relation_version`.
- `columns[]` with `column_id`, `name`, `logical_type`,
  `physical_arrow_type`, `nullable`, `ordinal`, and `semantic_role`.
- `primary_key_column_ids[]`.
- `weight_column_id`.
- optional `event_time_column_id`.
- `allowed_operations`.
- timestamp timezone by column.
- decimal precision and scale by column.
- dictionary encoding policy when applicable.

Column order is part of the contract. Arrow schema metadata is excluded by
default; only allowlisted Velorix metadata may affect the canonical relation
schema.

The fingerprint must change for column add/drop/rename, column order changes,
logical or physical type changes, nullable changes, primary key changes, weight
column changes, timestamp timezone changes, decimal precision/scale changes, or
allowed operation changes.

## Cross-System Use

Ingest envelopes, DataFusion table registration, Velorix `StandingViewSpec`, and
the incremental-engine input adapter must use the same `relation_id`,
`relation_version`, and `schema_fingerprint`. Mismatch fails closed before view
activation, query execution, or checkpoint publication.

## Verification

- Arrow metadata ordering does not change the fingerprint.
- Column order, nullable, timezone, decimal precision/scale, primary key, and
  weight column changes do change the fingerprint.
- View validation rejects input relation fingerprint mismatch.
- DataFusion registration uses the cataloged relation schema, not ad hoc payload
  inference.

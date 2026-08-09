# External Table Surface Contract

Status: Accepted
Applies to: persisted table specs and DataFusion object-backed scans.

Raw caller-provided Parquet URLs are phase-0/dev-only. They are not production
authority and must not be exposed as a multi-tenant public surface.

## Production Table Spec

Production table specs use registry-backed storage identity:

- `table_id`.
- `tenant_id`.
- `store_id`.
- `object_key_prefix`.
- `snapshot_ref`.
- `format`, currently `parquet`.
- `relation_id`.
- `relation_version`.
- `schema_fingerprint`.
- `query_policy_id`.

The `store_id` refers to an allowlisted object-store registry entry. Raw
`s3://`, `http://`, or `file://` URLs are not stored as production catalog
authority. Prefixes must pass tenant namespace policy.

## Cost and Security Boundaries

Output row caps are not scan cost controls. Production scans require limits for
scan bytes, object requests, file count, row groups, timeout, memory, and spill.
Production table-scan policy admission is explicit: bootstrap catalog
`create`/`get` can still read default policies, but production catalog methods
reject policies missing required SQL-size, timeout, output, scan, object
request, memory, or spill bounds. Tenant/global concurrency remains a separate
shared-runtime boundary.
DataFusion must register object stores through the shared registry only.
Production Parquet scans register the table using the relation catalog
DataFusion registration name and catalog-derived Arrow schema. The bootstrap
`input` table alias is not registered for production persisted table scans.

## Verification

- Raw URL table specs fail in production mode.
- Unregistered store id and cross-tenant prefixes are rejected.
- Production SQL sees the relation catalog table name and not the bootstrap
  `input` alias.
- Default/bootstrap query policies are rejected by production table-scan
  catalog admission.
- Large scans are stopped by scan-byte limits before output collection.
- Many-small-file scans are stopped by file or object request limits.

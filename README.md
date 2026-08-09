# Velorix

Velorix is an ultra-lightweight, object-storage-first streaming database for
schema-bound relations and materialized views.

The first-completion product target is a jarless runtime:

- start with no predefined tables
- register relations with explicit schemas
- ingest JSON rows through REST
- define supported views over registered relations
- update materialized output tables as ingest epochs commit
- query relation data and materialized view output through REST
- recover after restart from metadata plus object/local storage checkpoints

The product execution path does not use external compiler services, runtime
build/deploy jobs, package-loading, official third-party manager images, or PVC
state.

## Architecture

Velorix treats object storage as the durable system of record. Compute nodes are
replaceable: input, metadata-derived identities, view state checkpoints, and
published progress live outside the process.

Main components:

- `velorix-api`: REST API for relation creation, ingest, view definition,
  promoted APIs, query, and readiness.
- `velorix-core`: schemas, relation contracts, view contracts, view admission,
  and runtime-independent domain types.
- `velorix-runtime`: internal materialized view runtime and query execution.
- `velorix-storage`: object-key policy and durable registries.
- `velorix-meta`: metadata service backends, including in-memory and Hiqlite
  modes.
- `velorix-k8s`: Kubernetes startup validation and operator-facing contracts.

## Query and View Model

Relations are the ingest boundary. A relation has a durable schema and accepts
rows matching that schema.

Views are derived output tables. Users do not ingest into views directly. When a
supported view is admitted, the internal runtime maintains its output as
relation ingest changes are committed.

Unsupported SQL or unsupported view shapes fail during admission. Velorix must
not fake support by silently falling back to full-source recomputation.

The exact production, experimental, query-time, and rejected SQL scopes are in
[Supported materialized-view SQL](docs/architecture/supported-sql.md) and the
[native runtime Rust API migration](docs/release/0.1-native-runtime-migration.md).

## Local Development

Common checks:

```bash
cargo check -p velorix-api --lib
cargo check -p velorix-runtime --lib
cargo check -p velorix-storage --lib
cargo check --workspace --all-targets
```

Useful smoke scripts live under `scripts/`. Product scenarios should exercise
the REST API directly: create relation, ingest rows, create view, query the view,
restart, and query again.

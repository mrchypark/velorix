# Public relation schemas and automatic multiplicity

Date: 2026-10-30. Validation of the worktree changes based on `c6222abd054858109c90a63a061db698fa9fc9e8`. This implementation phase followed the frozen EGM specification review and the earlier DELETE-policy validation.

The validated public-input implementation is recorded in commit `8f674a11fc03a0ff95ef169b80f0bba0c507d39f`.

## Public contract

Public registration accepts `PublicRelationCatalogV1` and `PublicRelationSchemaV1` from [the shared core module](../../crates/velorix-core/src/public_relation.rs). API and CLI use the same types. Callers declare business columns, primary keys, allowed operations, and optional event time. They do not submit the physical multiplicity column, `weight_column_id`, semantic roles, computed fingerprints, or internal catalog bindings.

The server derives a collision-safe private weight column and its binding. Stored catalogs retain the existing internal representation. The returned fingerprint remains a server-assigned opaque catalog identity; clients do not compute it from public JSON.

Plain records become internal `+1` changes. Catalog-authorized UPDATE/UPSERT produce their internal retractions and replacements. DELETE remains unsupported. Public input cannot assign the generated field, including negative, zero, or positive values. Ordinary declared business columns named `weight`, `delta`, or `operation` remain valid, including negative measurement values.

Logical source/view schemas and SQL column resolution hide the private field. `SELECT *` includes business columns only. Explicit references to the engine field are rejected. Persisted source batches, signed runtime changes, checkpoint state, and derived-view publication retain the internal multiplicity needed for incremental processing.

## Example

Register through `POST /v1/relations`:

```json
{
  "catalog": {
    "relation_schema": {
      "relation_id": "events",
      "relation_name": "events",
      "relation_version": "v1",
      "columns": [
        {"column_id":"id","name":"id","logical_type":{"kind":"utf8"},"physical_arrow_type":{"kind":"utf8"},"nullable":false,"ordinal":0},
        {"column_id":"value","name":"value","logical_type":{"kind":"int64"},"physical_arrow_type":{"kind":"int64"},"nullable":false,"ordinal":1}
      ],
      "primary_key_column_ids": ["id"],
      "allowed_operations": ["insert", "update", "upsert"]
    }
  }
}
```

Ingest through `POST /v1/relations/events/ingest`:

```json
{
  "relation_version": "v1",
  "stream_id": "input",
  "partition_id": 0,
  "start_offset_inclusive": 0,
  "rows": [{"id":"a","value":10}]
}
```

Operation envelopes contain business records as well:

```json
{
  "operation": "update",
  "before": {"id":"a","value":10},
  "after": {"id":"a","value":15}
}
```

Place that envelope in the next request's `rows` array; continue from the server-returned `end_offset_exclusive`. Existing offset/range semantics are unchanged.

Optional request-level `row_format` selects `business_rows` or `operation_envelopes`. Ordinary unambiguous requests need no selection. A JSON record that validates as both data and an operation must select a format; the server otherwise rejects it before publication. This avoids reserving business-column names or guessing the caller's intent. See the [complete how-to](../how-to.md) for requests and CLI use.

Malformed public request shapes, including private registration fields and invalid format enum values, return `422`. Parsed requests violating schema or operation policy return `400`. Public wire compatibility intentionally changes: old weighted registration and input payloads must be updated.

## Logical and physical schemas stay separate

The API regression run exposed an internal completion bug: demand replay constructed its synthetic empty frontier batch from the public logical schema, so the runtime correctly rejected the missing physical weight field. [Demand completion](../../crates/velorix-api/src/demand_materialization.rs) now builds that batch from the full catalog. It verifies the captured fingerprint and matching active logical input before doing so. Schema validation was retained; no permissive decoder or fallback execution path was introduced.

## Final validation

| Check | Result |
| --- | --- |
| cargo fmt --check; git diff --check | Passed |
| cargo clippy --workspace --all-targets -- -D warnings | Passed |
| cargo test -p velorix-core --quiet | 575 passed; 1 pre-existing ignored benchmark |
| cargo test -p velorix-runtime --lib --quiet | 86 passed |
| cargo test -p velorix-runtime --test materialized_view_runtime --quiet | 263 passed, including fresh-process Arrow recovery |
| cargo test -p velorix-api --lib --quiet -- --test-threads=2 | 466 passed, 0 failed, 1 pre-existing ignored benchmark; 56.21 s |
| cargo test -p velorix-cli --quiet | 27 passed |
| Focused OpenAPI demand/backfill/budget verification after response-status metadata update | 1 passed |
| Shell syntax checks for eight migrated public-client scripts | Passed |
| Offline Rhiza Chaos mock contract | Passed |
| bash scripts/check-vind-product-contract.sh | Passed, including Rhiza configuration/readiness and Versity Gateway helper subcontracts |

The tests cover weightless declarations/inserts, business-name collisions, explicit private-field injection at all signs, JSON row-format ambiguity without mutation, UPDATE/UPSERT deltas, public star projection, page cursor invalidation, sparse scopes, dependency cuts, backfill, pause/resume, correction, duplicate retry, and restart. Existing native signed-weight tests remain. Trusted fixtures use the actual registered internal fingerprint instead of an obsolete client-computed catalog.

API validation progressed from 420 passed / 46 failed to 461 passed / 5 failed and finally 466 passed / 0 failed. Failures identified the empty physical-batch bug and remaining fixture inputs, offsets, and old fingerprint expectations. Core/runtime SQL tests were updated to keep hidden-column rejection and business-only identity CTE coverage. No failing tests were disabled to make the suite pass.

## Scope and limits

This is local functional and contract validation. No live Kubernetes experiment, cloud-provider qualification, real-data ingestion, HTTP performance benchmark, image publication, or deployment ran. The JSON ambiguity case validates persisted Arrow input directly; it does not claim a new JSON SQL family. Existing retained legacy-checkpoint fixtures passed; a cross-release rollout is not qualified by this run.

Public HTTP schemas, CLI requests, current documentation, and development/recovery client payloads were updated. Internal typed ingest-writer payloads and stored catalog/checkpoint formats remain internal. No storage migration or real-data reload was performed. Temporary diagnosis code was removed. No dependency or deployment configuration change was required.

Existing user data and checkpoint locations were preserved. The local diagnostics and detailed resource-cleanup record remain in the ignored `target/public-weight-validation-20261030/` task log and are not release evidence.

No standalone or persistent service or container was started for this validation phase. In-process HTTP fixtures finished with their test processes; offline contract fixtures use cleanup traps. No commit or push occurred during this validation phase; this statement records that phase only and does not describe the repository's current PR or publication status. No release was performed as part of the validation. These correctness-test timings do not establish performance.

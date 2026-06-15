# API Serving Scope V1

Velorix 1.0 includes a minimal governed data API surface on top of relations,
queries, persisted tables, and internal materialized views.

Included scope:

- stable endpoint definitions for persisted queries, table scans, and admitted
  materialized views
- request parameter validation before execution
- response-shape metadata for stable JSON output
- OpenAPI-compatible catalog metadata
- query policy, row/byte caps, object-request limits, and concurrency admission
- immediate-response paths from materialized or checkpoint-recovered state

Excluded scope:

- arbitrary user-defined HTTP middleware
- unbounded SQL templating that bypasses query policy
- raw object URL serving outside the production storage registry
- treating cache entries as authoritative responses
- external runtime build/deploy or package-loading paths

Contract mapping:

- `DataFusion policy` covers bounded ad hoc queries.
- `table registry` covers endpoint-backed table scans.
- `materialized view runtime` covers standing-view serving from admitted
  internal runtime specs.
- `benchmark gate` covers served data paths under local and S3-compatible
  evidence profiles.
- `Kubernetes operator` keeps serving replicas stateless and coordinated by
  external authority records.

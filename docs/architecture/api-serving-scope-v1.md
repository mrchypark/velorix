# API Serving Scope V1

Velorix public 1.0 includes a minimal governed data API surface on top of
relations and admitted materialized views. Public query responses read published
materialized output only.

Included scope:

- stable endpoint definitions for relation ingest, admitted materialized views,
  promoted view APIs, and materialized-output queries
- request parameter validation before execution
- response-shape metadata for stable JSON output
- OpenAPI-compatible catalog metadata
- query policy, row/byte caps, and concurrency admission over materialized output
- immediate-response paths from published materialized output selected by
  checkpoint/frontier metadata

Excluded scope:

- public generic `/v1/query` source scans
- public persisted table scan APIs
- public ad hoc persisted query execution
- arbitrary user-defined HTTP middleware
- unbounded SQL templating that bypasses query policy
- raw object URL serving outside the production storage registry
- treating cache entries as authoritative responses
- external runtime build/deploy or package-loading paths

Contract mapping:

- `DataFusion policy` covers internal/dev object-backed scans and bounded
  post-filtering over materialized output; it is not a public source-query
  authority.
- `table registry` covers internal/dev endpoint-backed table scans.
- `materialized view runtime` covers standing-view serving from admitted
  internal runtime specs.
- `benchmark gate` covers served data paths under local and S3-compatible
  evidence profiles.
- `Kubernetes operator` keeps serving replicas stateless and coordinated by
  external authority records.

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

## Current route boundary at baseline `c6222ab`

The registered public routes are relation creation/ingest, view declaration,
view and output queries, promoted `/v1/api/{path}` queries, query-policy
catalog operations, backfill, and OpenAPI metadata
([route table](../../crates/velorix-api/src/lib.rs)). There is no public
persisted-table registration or arbitrary source-scan route in that table.
Query-policy catalog endpoints therefore do not by themselves expose a public
DataFusion table-scan API. Query-time SQL is validated and run over the selected
published view output under policy; it is distinct from view SQL admission and
from internal/dev object-backed DataFusion scans.

Admission remains bounded by the internal typed-plan validators and public
feature policy: event-time windows are enabled by default, analytic `OVER`
families are experimental by default, and
`VELORIX_EXPERIMENTAL_ADVANCED_VIEW_FEATURES=true` enables both. An enabled
runtime family is still only its validated SQL subset. See
[`validate_public_view_feature_admission`](../../crates/velorix-api/src/view_admission.rs)
and [`validate_public_runtime_plan_admission`](../../crates/velorix-api/src/view_admission.rs).

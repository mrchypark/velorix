# API Serving Scope V1

Velorix 1.0 includes a minimal data API serving product surface, not only a
storage/query substrate.

The target is similar in product shape to SQL-backed data API frameworks: a
user should be able to define a governed query or standing view, bind request
parameters through a policy-controlled contract, and serve a predictable JSON
response without writing bespoke application code for every endpoint. This
scope does not make Velorix a general web framework. Object storage remains the
database authority, DataFusion remains the ad hoc SQL execution engine, and
Feldera/DBSP remains the standing-view direction after artifact trust and
runtime gates are satisfied.

This document defines release scope, not current completion evidence. The
production-readiness status matrix remains the source of truth for which
serving-related contracts are still partial.

## 1.0 Included Scope

Velorix 1.0 must treat the following as product scope:

- Endpoint definitions that bind a stable route or tool name to a persisted
  query, production table scan, or trusted standing-view artifact after the
  relevant artifact trust gates pass.
- Request parameter validation before SQL execution, including type, required
  field, default, and bounds checks.
- Response-shape definitions that map Arrow/DataFusion result columns into
  stable JSON objects, including field renames, omitted fields, pagination
  metadata, and deterministic error envelopes.
- OpenAPI-compatible API catalog metadata so applications, internal tools, and
  AI agents can discover callable data endpoints without reading Rust code.
- Cost and latency policy for served endpoints, including query policy
  linkage, output row/byte caps, object-request limits, memory/spill limits,
  concurrency admission, and cache eligibility.
- Immediate-response paths where the endpoint can read an already materialized
  or checkpoint-recovered state instead of recomputing from raw ingest.

## 1.0 Excluded Scope

The 1.0 serving scope deliberately excludes:

- Arbitrary user-defined HTTP middleware.
- Unbounded SQL templating that bypasses the query policy catalog.
- Raw object URL serving outside the production storage registry.
- Treating Foyer cache entries as authoritative responses.
- Direct Feldera/DBSP runtime execution before artifact, state, resource, and
  recovery contracts are proven.

## Contract Mapping

This scope is cross-cutting and does not add a new row to the 1.0 status
matrix. Instead:

- `DataFusion policy` must prove served ad hoc queries are bounded and fail
  closed under policy.
- `table registry` must prove endpoint-backed table scans use registry,
  relation-catalog, and query-policy authority instead of raw URLs.
- `Feldera artifact registry` must prove standing-view serving only selects
  trusted artifacts and remains disabled until artifact hash, state, resource,
  recovery, and runtime gates are available.
- `benchmark gate` must include response-serving evidence for local and
  S3-compatible paths where an endpoint reads materialized or table-backed
  data.
- `Kubernetes operator` must keep serving replicas stateless and coordinated by
  object-store-backed authority records.

Velorix can expose higher-level SDKs, dashboards, or no-code API builders after
1.0, but the 1.0 release should already have the authoritative endpoint,
parameter, response, catalog, and policy contracts needed to support that
product direction.

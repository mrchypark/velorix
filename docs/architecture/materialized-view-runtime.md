# Materialized View Runtime

Status: Accepted
Applies to: relation ingest, user-defined views, incremental view execution,
checkpoint/recovery, and query serving.

Velorix views are executed by the Velorix native materialized view runtime. The
runtime maintains operator state, checkpoints, and materialized output tables so
queries read precomputed view results instead of recomputing from source
relations.

## Context

The first complete Velorix product path must start from an empty deployment,
create relations, ingest rows with different schemas, define views, keep view
outputs current after ingest, and recover after restart from metadata and
durable checkpoints.

The runtime path must not depend on internal compiler services, runtime package
artifact deployment, or runtime Rust compile/deploy for user-defined views. Those
paths may remain as historical references or future research, but they are not
the accepted product execution contract for the current completion target.

## Decision

Views are admitted into a Velorix-owned execution plan and run by the native
materialized view runtime.

The implementation roadmap is tracked in
[Materialized View Runtime Roadmap](materialized-view-runtime-roadmap.md).

The public product concepts are:

- relation
- ingest
- view
- materialized output
- checkpoint
- recovery

The implementation may use an internal operator graph to model dependencies
between scans, filters, projections, joins, aggregations, latest-by-key
operators, and output publication. That graph is an implementation detail, not a
public product name or API concept.

Unsupported SQL or view shapes must fail during admission with a clear error.
Velorix must not silently fall back to a fake generic implementation or a source
full-scan query path for materialized view serving.

## Execution Model

Relation ingest is committed in epochs. Each epoch records the relation, schema
fingerprint, batch identity, and durable source batch location.

For each admitted view, the runtime keeps operator state and applies only the
input changes that can affect the view. A filter evaluates changed rows. A join
maintains key indexes for participating relations. An aggregate maintains
group-level accumulators. A latest-by-key operator keeps the current selected
row by key and ordering rule.

When an ingest epoch is committed, affected views are advanced to that epoch and
publish changed materialized output. Query serving reads the materialized output
state for the view rather than scanning all source relation data.

Two-relation joins publish checkpoint-bound per-relation frontier vectors.
Sequential relation ingests may expose sequential intermediate results; each
published join output must state the exact input frontiers it has applied.

## Metadata, Cache, and Checkpoints

Hiqlite stores small metadata:

- relation definitions
- view definitions
- schema fingerprints
- view runtime status
- committed epoch pointers
- checkpoint manifests
- durable object references

Object or local storage stores durable data:

- source ingest batches
- materialized output checkpoints
- operator state checkpoints
- replayable epoch data after the latest checkpoint

The runtime should use cache layers to reduce repeated durable storage reads:

- in-memory state for hot operator indexes and accumulators
- Foyer for local block, page, and checkpoint cache
- durable object or local storage as the source of truth

Cache state is never the only correctness boundary. Recovery must be possible
from hiqlite metadata plus durable storage checkpoints and replay data.

## Recovery Model

On restart, Velorix loads relation and view metadata from hiqlite, restores view
runtime state from the latest durable checkpoint, and replays only committed
epochs after that checkpoint.

Recovery must not require source full recomputation when a valid checkpoint and
replay range exist. If a checkpoint is missing or invalid, recovery must fail
closed or use an explicitly accepted repair path.

## Non-Goals

- No product execution dependency on internal compiler services.
- No product execution dependency on runtime package deployment.
- No runtime Rust compile/deploy path for user-defined views in the current
  completion target.
- No PVC requirement.
- No fake SQL fallback.
- No source full-scan query path as proof of materialized view execution.

## Acceptance Criteria

- Empty state can create relations.
- Relations with different schemas can ingest data.
- Users can define views over registered relations.
- Supported views are admitted into the native materialized view runtime.
- Unsupported views fail during admission with a clear error.
- Ingest automatically updates materialized output tables.
- Query reads materialized output rather than recomputing from source relations.
- Restart recovers from hiqlite metadata and durable checkpoints.
- Replay after restart applies only epochs after the restored checkpoint.
- Two-relation join views publish checkpoint-bound per-relation frontier
  vectors.
- Product execution does not require internal compiler services, generated
  artifact deployment, or PVC.

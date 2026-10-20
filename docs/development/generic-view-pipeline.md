# Generic materialized view pipelines

## Status and execution model

Use separately declared views to compose supported SQL operators through the
existing view dependency graph. Each view consumes registered, schema-bound
relations or published upstream views. In authoritative mode, ingest commits
signed source changes; new demand-managed views apply the required changes on a
query or explicit backfill. Legacy mode retains eager ingest updates.
Reads consume the resulting materialized output.

### Runtime modes and actual defaults

The relation-ingest mode determines which materialization contract applies:

| Mode | Selection | New-view and ingest behavior | Query behavior |
| --- | --- | --- | --- |
| Legacy | Explicit `VELORIX_API_AUTHORITATIVE_RELATION_INGEST=false` or `0` | Retains the existing lifecycle/bootstrap paths and eager updates for views eligible for incremental ingest; historical readiness is still required. | Reads materialized output through the legacy readiness path; the new request-scoped demand contract does not apply. |
| Authoritative persisted demand | Default (`VELORIX_API_AUTHORITATIVE_RELATION_INGEST=1`), a stable nonempty `VELORIX_RELATION_INGEST_OWNER_ID`, and a compatible Meta authority | New declarations are deferred, including on empty input. Ingest durably publishes source changes without eagerly applying demand-managed views. | Unpinned queries advance bounded dependency work, persist state/coverage, and reuse completed ranges. Explicit epoch reads retain pinned-read semantics. |

The API environment configuration and product deployment launcher
[`run-vind-product.sh`](../../scripts/run-vind-product.sh) default the flag to
true/`1`. Missing Meta/owner configuration fails closed rather than falling back
to Legacy. Low-level `ApiState::from_validated_authority` construction still
starts in Legacy until the caller attaches Meta and authoritative ingest
configuration; it is not the configured server startup path.
Inspect `/readyz`'s `relation_ingest.mode` and
`relation_ingest.authoritative` to verify the configured mode. Production
authoritative deployments need a compatible durable metadata authority; the
memory Meta used by the local guide establishes only development behavior.

**Upgrade warning:** previous releases defaulted to Legacy. Existing deployments
must explicitly set the flag to `0` to retain that behavior, or provision and
validate authoritative configuration before upgrading. Do not change the flag
against existing data and assume that source history or old checkpoints were
migrated. Use a separate namespace for migration validation and preserve the old
data/checkpoints. The new default applies to new authoritative declarations;
existing non-demand checkpoints retain the eager behavior described below.

The product launcher connects to existing Rhiza Meta by default. It requires an
explicit HTTPS `VELORIX_META_GRPC_ENDPOINT`, existing `VELORIX_META_BEARER_TOKEN`,
and the four `VELORIX_META_TLS_*` client settings shown in the
[server startup instructions](../how-to.md#use-existing-rhiza-meta-for-the-default-server).
It does not build, deploy, restart, or configure the external Meta or its S3
storage. Its default fencing is `logical-fencing`; use `required` only when the
Meta capability supports the stronger contract. This is not a claim of bounded
wall-clock failover. Managed-Meta/data-mutating smoke suites default to disabled
in this external path and are rejected if enabled; run workload/recovery
validation separately in an isolated namespace. Readiness remains mandatory,
and product evidence leaves external recovery/workload validation unverified.

This mode distinction is separate from whether view bootstrap metadata is
authoritative: Legacy view admission also requires bootstrap metadata. It is
also separate from `VELORIX_EXPERIMENTAL_ADVANCED_VIEW_FEATURES`, which gates SQL
families rather than selecting ingest/materialization mode.

Legacy backfill keeps its existing `batch_limit` step path and returns HTTP 200
with progress; that status alone does not establish completion. Demand `scope`,
`budget`, pause/resume, and reverse order require authoritative mode and are
rejected in Legacy mode. The authoritative 202/200 progress contract below must
not be applied to Legacy backfill responses.

Existing durable checkpoints without demand metadata retain eager ingest
behavior even with authoritative ingest enabled. Enabling the flag is therefore
not a blanket migration of existing views or Legacy source history. The demand
contract below describes new authoritative views and persisted demand progress.
See [configuration and mode selection](../../crates/velorix-api/src/lib.rs),
[declaration](../../crates/velorix-api/src/view_admission.rs), and
[ingest classification](../../crates/velorix-api/src/ingest_epoch.rs).

The examples below use neutral names and syntax covered by core/runtime tests.
Coverage includes keyless aggregation, multiple published inputs, deferred
backfill, durable progress, restart, and runtime residency. The
[authoritative staged pipeline tests](../../crates/velorix-api/src/generic_authoritative_stage_tests.rs)
exercise six generic operator families together. Before the latest demand-mode
changes, the release API suite passed 297 tests; core/runtime, metadata progress,
checkpoint CAS, and gRPC checks also passed, as did workspace clippy across all
targets with warnings denied. These are historical results, not validation of
the current demand-mode implementation. The latest 15 API tests remain unrun:
the capacity-guard stop is not a successful test result.

**External real-data validation is incomplete.** The initial ten primary targets
passed; two approximation-related diagnostics failed and remain failures. The
remaining 24 original comparisons and the separate bounded probe are unverified.
The prepared 18-step demand-functional workload reuses the existing 94,661 source
rows without ingest, with independent DuckDB range oracles; it has not run.
Earlier real-data resume attempts stopped during startup, including a capacity
guard stop, and do not validate demand-mode behavior. These results do not establish
full equivalence or workload-scale resource and recovery behavior. Driver timings
are not production benchmarks.

There is no whole-source SQL fallback. Unsupported SQL fails admission. A view
does not invoke DataFusion to recompute its full input query after ingest. Ordered
operators may recompute affected partitions from their retained operator state
and emit the signed difference; this cost must be considered when sizing stages.
State restoration and checkpoints taken before or after an update may scale with
the stage's retained state even when the incoming delta is small. This is state
management work, not a full-source SQL fallback; incremental output does not imply
that every operation costs only the size of the delta.

## Current paginated query validation cost

On the legacy query path, a view with published-view inputs runs
`ensure_view_query_ready` and `ensure_published_input_cursors_current` before
reading output pages. These readiness checks read the consumer checkpoint and
its producers' checkpoints through full checkpoint readers, which hydrate state,
validate checkpoint identity and hashes, and validate output manifests. This
work repeats across page requests even when their checkpoint heads are unchanged.
Demand queries instead use `query_physical_demand` and authenticated, pinned demand
pagination. The legacy readiness cost must not be attributed to every demand
page without measuring that path. The native paged-output read path does not
itself require full-state hydration; its selected-page behavior does not
eliminate the legacy path's preceding checks.

The validation driver currently leaves `MaterializedReadCache` disabled. Production's
Foyer-backed cache stores materialized output metadata and page bytes and validates
cache hits; it does not cache the full-state readiness reads above. Enabling that
existing cache therefore would not remove this readiness cost. Validated cursor
proofs are request-local, not a cross-request validation cache.

A future change to the legacy path's readiness using checkpoint headers and cursor
commitments requires a separate trust-boundary review, including changed heads,
corrupt state, and freshness guarantees. That path retains full
validation. No query-speed improvement or workload-scale performance result is
claimed here.

## Declare schemas and stages

Register inputs with explicit types, nullability, and truthful key metadata.
Event-time metadata does not convert an `Int64` column into a timestamp. Use an
actual timestamp schema for timestamp series. Do not invent a unique key for a
relation containing duplicates.

The SQL blocks below are **SELECT bodies for separately declared views**, not a
script containing supported `CREATE VIEW` statements. Choose stage names when
declaring views through the API. The stages illustrate composable shapes; they
are alternatives and branches, not one required linear recipe.

### Declaration, query demand, and optional backfill

For new authoritative views, declaring a view and processing input are separate
operations. A declaration, including one over empty input, is admitted in
`Deploying` status with a `backfill_required` reason. The current
demand-mode source allows a query to request materialization or an explicit
backfill API (`POST /v1/views/{view_id}/backfill`) call to advance it. Declaration
must not silently replay the entire source. This behavior remains unverified as
described below.

Default startup leaves runtime hydration and historical processing deferred; it
does not restore or catch up the entire active DAG. Durable registry and progress
remain available for targeted work. Explicitly targeted legacy migration is a
separate opt-in path, not the default startup behavior.

Legacy declarations can enter `Running` when no committed input needs backfill.
New authoritative declarations remain deferred until demand or explicit backfill
establishes coverage. Their initial `query_enabled=false` is a lifecycle readiness
indicator, not a prohibition on querying an admitted view to advance demand.
Successful empty-input declarations verify admission and wiring, not completion
of real-data processing.

In authoritative mode, historical derived-view declaration is also deferred: it records dependency
bindings and the captured producer cut without applying the historical baseline.
Use a query or the explicit backfill API to advance it. This separates declaration from
snapshot processing even when the producer already has materialized output.

For native paged snapshots, `batch_limit` bounds the baseline pages applied in a
backfill request. Page validation may additionally read the preceding page to
check ordering at the resume boundary. Captured checkpoint headers, manifests,
schema bindings, and selected pages remain validated; the paged path avoids
producer runtime restoration and full producer-state hydration. References stay
bound to the captured checkpoint, never the producer's current live state.

Durable progress records each input's frozen cursor, manifest identity, total
pages/rows, and applied page/row prefix alongside the consumer checkpoint. Partial
progress must not claim the completed input frontier or expose the view as ready.
Stopping between explicit requests preserves that progress; restart resumes the
committed prefix instead of applying it again. Once the baseline completes,
later producer commits must be caught up before activation. Incremental source
replay skips payloads already covered by the restored frontier.

Late-commit catch-up uses the request budget remaining after baseline pages,
with unfinished work reported for the next request. `batch_limit` bounds applied
baseline pages and catch-up commits;
it does not bound all metadata/validation I/O, wall-clock time, or total memory.
Mixed source/published bindings fail closed on the derived replay path rather
than silently dropping source replay.

Published-only retries retain the captured bootstrap catalog binding and advance
through direct producer cursors. Unrelated ingest must not change an otherwise
identical retry's causal commitment and conflict with immutable output already
written by a failed publication. Canonical key encoding also sorts borrowed
object-field names instead of cloning keys on that restore path; ordering and
persisted codec semantics are unchanged.

Nonpaged snapshots include current aggregate and ordered operator families, not
just older persisted artifacts. These operators may process one whole guarded
snapshot as a work unit, requiring full state hydration and snapshot
reconstruction. Generic runtime resource caps still apply, but `batch_limit=1`
does not guarantee a row count, byte size, or small amount of work. Do not assume
all new operator outputs use native 512-row pages. An exhausted work budget skips
fallback hydration.

### Demand-driven materialization — validation pending

The demand routes, explicit controls, ordinary-query scope inference, physical
tumbling-window scope fix (including range B before A), sparse private frontiers,
and authoritative readiness checks are present in source. This document describes
the intended current contract; final integration validation remains pending.
Earlier test results above do not validate these changes.

The current `POST /v1/views/{view_id}/query` JSON envelope accepts optional
`materialization.scope.where` (an output-column predicate) and
`materialization.budget`. The budget fields are positive integers:

| Field | Default | Meaning |
| --- | --- | --- |
| `batch_limit` | 1 | Work units per request |
| `max_rows` | 4,000 | Input row inspection budget |
| `max_bytes` | 67,108,864 | Source read/decode acceptance budget |
| `max_duration_ms` | 1,000 | Cooperative duration limit |

The query's top-level `max_rows` still limits returned output rows, separately
from `materialization.budget.max_rows`. An ordinary bound query's simple output
`WHERE` predicate can infer scope without an explicit `materialization.scope`.
Supported literal and bound predicates are made self-contained; uncertain query
shapes use full dependency closure. Explicit epoch and continuation reads retain
their pinned-read semantics. GET requests use default demand controls; the JSON
materialization envelope configures POST requests.

An incomplete demand returns HTTP 202 with a JSON `materialization` object
containing `status`, `complete`, `processed_rows`, `read_bytes`, `remaining_rows`,
and `dependency_mode`. Completed queries retain Arrow IPC and JSON response
negotiation. HTTP 413 source-budget errors include `details.code` equal to
`DEMAND_SOURCE_BUDGET_REQUIRED`, `required_min_bytes`, `supplied_max_bytes`, and
`max_supported_bytes`, `budget_phase`, `required_min_bytes_is_lower_bound`, and
`retry_with_larger_budget_possible`. In `serialized_read`, HEAD establishes only
a lower bound without downloading the object. A retry may reveal a larger
`decoded_acceptance` requirement; that value is the maximum of serialized and
decoded sizes, not their sum. Decoded acceptance is per artifact; `read_bytes`
and the request's remaining read budget accumulate serialized bytes across
artifacts. Neither limit is a peak-RSS guarantee. If the required value exceeds
`max_supported_bytes`, increasing the request budget cannot make it acceptable
and `retry_with_larger_budget_possible` is false.
Published-input whole-object budget errors use
`DEMAND_PUBLISHED_BUDGET_REQUIRED` and report `required_min_bytes` and
`supplied_max_bytes`. An indivisible signed update can also exceed the row budget:
the expected contract is HTTP 413 with `details.required_min_rows`, rather than
splitting insertions and retractions into invalid intermediate operator state.
Signed atomic-unit handling remains pending integration validation.

Demand query continuation tokens are process-local. Restarting the process or
sending a token to another instance rejects that token; continuation across
instances or restart is not promised. Start a new query to reuse durably committed
materialization and validated coverage. Durable reuse does not make the old
continuation token portable.

The backfill request accepts `scope` (with `where`), `budget`, `action`, and
`order`. Actions are `step` (default), `pause`, and `resume`; order is `forward`
(default) or `reverse`, subject to signed causal/atomic closure. Optional top-level
`batch_limit` remains a compatibility alias for `budget.batch_limit`; conflicting
values are rejected. Pause/resume acknowledge control changes with HTTP 200 but
do not assert completion or start a background worker. A step returns 202 while
incomplete, paused, or yielded to foreground demand, and 200 when complete.

Declaration records the view without automatically backfilling historical input.
Startup restores durable progress without starting background historical
materialization. A query requests the output range it needs; the engine computes
the necessary input dependencies, persists the resulting materialization and
coverage, and reuses that work on subsequent requests. This is durable operator
state and coverage, not merely an output-page cache.

New authoritative views are demand-managed even before their first checkpoint. Ingest must not
hydrate or apply them merely because they have never been queried; this includes
declarations made before the first source input and correction preflight.
Demand-managed views refresh lazily: ingest durably appends source changes but
does not eagerly bring those views to the new cut. A subsequent query or explicit
backfill reconciles against the current cut before reporting completion. This
differs from the retained eager behavior of existing durable legacy checkpoints
without demand metadata.

Coverage must be bound to the admitted plan, schema and input identities, and a
validated source or producer frontier. A completed range is complete only at that
cut. Later inserts, updates, and retractions must update or invalidate affected
coverage before a newer cut is reported complete. Work captured at an older cut
cannot silently certify that later changes have been incorporated.

Not every admitted SQL family supports narrow dependency computation. Row-local
maps and supported grouping/window keys can provide conservative dependency
evidence. Unknown values remain possible matches. Unsupported dependency
optimizations require broader computation, potentially the full dependency
closure, without rejecting otherwise admitted SQL or claiming narrow execution.
For example, `LAG` needs predecessor history and cumulative `SUM` needs the
relevant preceding partition state. Restricting input to the requested output
time interval alone cannot establish those results. Broader closure uses the
internal operators; it is not a whole-source SQL fallback.
The implemented DAG path conservatively materializes ancestor stages through
full dependency closure. A narrow predicate on the requested view does not imply
that the same range is pushed through its ancestors or that ancestor work is small.

The intended response distinction is `202 Accepted` while required work remains
and `200 OK` only when the requested result is complete at its declared cut. A
completed subrange may be served while unrelated ranges remain unfinished only
when its dependency closure and coverage are proven. Advancing the entire
baseline is not evidence that unfinished requested ranges receive priority;
request ordering and prioritization need their own integration tests.

Explicit backfill remains available to choose work order and resource budgets
and advance bounded steps. Pausing between steps preserves committed progress;
resume and restart continue that progress without applying it twice. The final
API must distinguish stopping new work from interrupting an in-flight operator
operation; no instantaneous cancellation guarantee is implied here.

Work-unit limits and cooperative stopping checks are not strict resource bounds.
A source adapter's `max_bytes` is a strict serialized-read cap; decoded-size
acceptance is checked after Arrow decoding and therefore does not bound peak
allocation or RSS. Authentication may require one whole object as the minimum
work unit. An undersized budget must return the required-budget error rather
than silently perform a larger read. A `max_duration_ms` limit is checked at
cooperative stopping points, not enforced by preempting an in-flight operation.
A page or commit budget limits scheduled progress, while a nonpaged snapshot or
stateful dependency closure can still be large. Operator expansion, snapshot,
and state-payload guards reject work exceeding their respective limits; they do
not bound total process RSS, CPU time, all validation I/O, or elapsed request time. Retained
state restoration and checkpoint copies remain relevant even for a narrow
request. The resident-runtime count limit below is also a soft control.

### Resident runtime limits

`VELORIX_STANDING_RUNTIME_RESIDENT_LIMIT` defaults to 32. It is a soft count limit
for idle, durably checkpointed runtimes, not a byte or process-RSS bound. Busy,
borrowed, or uncheckpointed runtimes are retained, so the count may exceed the
limit. A later operation restores an evicted runtime from durable state. Large
individual states, temporary copies, and other allocations still require
separate resource planning.

### 1. Normalize with typed maps

For `events(epoch_ns Int64, metric Float64, ...)`, a map can select columns and
apply a typed predicate without retaining a source key in its output:

```sql
SELECT metric
FROM events
WHERE metric IS NOT NULL
```

To preserve the other source columns for downstream stages:

```sql
SELECT *
FROM events
WHERE epoch_ns IS NOT NULL AND metric IS NOT NULL
```

These shapes come from the [typed-map tests](../../crates/velorix-core/src/view_plan/typed_map.rs).
Projection may collapse different input rows into equal output rows; their
multiplicity is retained. A predicate that removes null values does not itself
guarantee that the inferred output schema becomes nonnullable. Check that schema
before choosing a downstream operator requiring a nonnullable column.

### 2. Expand a series or concatenate branches

For `measurements(id, lo Int64, hi Int64)`, declare an expansion stage:

```sql
SELECT m.id, g.n AS value
FROM measurements AS m
CROSS JOIN LATERAL generate_series(m.lo, m.hi, 1) AS g(n)
```

A comma lateral form and descending integer series are also admitted:

```sql
SELECT n
FROM measurements, LATERAL generate_series(3, 1, -1) AS g(n)
```

For `lo` and `hi` declared as timestamps, a fixed-duration interval supplies the
step:

```sql
SELECT g.t
FROM measurements AS m
CROSS JOIN LATERAL generate_series(m.lo, m.hi, INTERVAL '2 seconds') AS g(t)
```

Series bounds are inclusive. Bounds must have the same supported type: `Int64`
or nanosecond timestamp. Integer steps use integer units; timestamp steps use
nanoseconds internally. A step pointing away from the end produces no rows.
Any NULL bound or step produces zero rows. A non-NULL zero step is an error.
Overflow and expansion beyond the configured limit must fail rather than
truncate results. Literal-invalid plans can fail at admission; row-dependent
failures require runtime checks.

The core plan stores `max_emitted_rows_per_input`, with a default of 100,000.
Core callers can use the explicit-budget admission/lowering helpers. This is a
resource limit, not a domain-specific time horizon. It is separate from the API's
runtime byte budgets below; do not assume an HTTP field exists for this per-input
row setting.

The API reads these positive-integer byte limits from the environment:

| Environment variable | Default | Budget |
| --- | --- | --- |
| `VELORIX_STANDING_RUNTIME_MAX_EXPANSION_BYTES` | 16 MiB | Expansion work |
| `VELORIX_STANDING_RUNTIME_MAX_SNAPSHOT_BYTES` | 16 MiB | Snapshot work |
| `VELORIX_STANDING_RUNTIME_MAX_STATE_FRAGMENT_BYTES` | 4 MiB | Individual state fragment |
| `VELORIX_STANDING_RUNTIME_MAX_STATE_PAYLOAD_BYTES` | 8 MiB | Total state payload |

Expansion and snapshot limits are independent of the existing total state-payload
limit. Increasing one does not increase the others. These settings are wired in
[API configuration](../../crates/velorix-api/src/lib.rs). None guarantees a bound on total process
RSS: candidate-state clones, checkpoint copies, and other allocations can coexist
with the budgeted data.

In-process callers configure the independent work limits with
`ApiState::with_standing_runtime_work_byte_limits(expansion_bytes, snapshot_bytes)`
and fragment size with `with_standing_runtime_state_fragment_bytes(bytes)`.
Both builders return an error for zero values. The existing
`with_standing_runtime_budget_limits(max_output_delta_records, max_state_payload_bytes)`
builder sets the expansion row guard and total checkpoint limit; it does not
change either work-byte limit or fragment size. This older builder stores values
without validating them; runtime limit validation rejects zero limits when a
runtime is created or restored. Environment configuration rejects zero at startup.
Restoring a fragmented checkpoint also enforces the current total and fragment
limits, so lowering either can reject checkpoints written under larger limits.

For two inputs with matching positional types:

```sql
SELECT lo AS value FROM measurements
UNION ALL
SELECT lo FROM measurements_extra
```

`UNION ALL` preserves every branch's multiplicity, including repeated references
to the same relation. Output names come from the first branch; matching is by
position, not by name. Output nullability combines the branches. Types must
match; there is no implicit widening promise. Apply filters or other operations
in upstream map stages instead of assuming arbitrary branch clauses are admitted.

These forms are covered by the [set-operation tests](../../crates/velorix-core/src/view_plan/set_ops.rs).
Series and union outputs are bags with no declared primary key.

### 3. Aggregate or join materialized stages

Grouped aggregates accept keyless bag inputs. Source-row uniqueness is not
required: output group identity comes from the declared `GROUP BY` expressions.
Nullable grouping columns, including compound date/entity groups, are supported;
do not add a fictitious input primary key to admit an aggregate stage.

A downstream relation with `key` and a numeric `metric` can use the tested
grouped aggregate shape:

```sql
SELECT key, sum(metric) AS sum, count(*) AS count
FROM measurements
GROUP BY key
```

The aggregate family also has tested `min`, `max`, `avg`, and `count(column)`
projections. For example, the following shape uses `metric Float64` and
`adjustment Int64`:

```sql
SELECT key, sum(metric) AS sum_metric,
       min(adjustment) AS min_adj, max(adjustment) AS max_adj,
       avg(adjustment) AS avg_adj, count(adjustment) AS count_adj
FROM measurements
GROUP BY key
```

A tested keyless shape combines nullable `observed_day Date32` and `entity Utf8`
groups with nullable `metric Float64` and `quantity Int64` inputs:

```sql
SELECT observed_day, entity,
       avg(metric) AS mean, count(*) AS samples, count(quantity) AS measured,
       min(quantity) AS smallest, sum(quantity) AS total,
       approx_quantile(metric, 0.5) AS median
FROM measurements
GROUP BY observed_day, entity
```

`approx_quantile` takes a direct supported numeric column and a numeric percentile
literal in `[0, 1]`. This example exercises Float64 values; it does not establish
support for Decimal inputs or arbitrary computed quantile arguments. Core
admission preserves the source's empty primary key and records explicit group
identity; the runtime consumes signed input multiplicities.

`approx_quantile` is an algorithm-specific estimate, not an exact cross-engine
alias. Identical input values and multiplicities can produce different estimates
in engines using different estimators or merge strategies. When an estimate feeds
a threshold or classification, validate the resulting decisions as well as the
input bags; matching input bags alone does not establish equivalent decisions.

A tested two-input equijoin-plus-aggregate shape is:

```sql
SELECT s.key, sum(m.metric) AS sum, count(*) AS count,
       min(m.metric) AS min_metric, max(m.metric) AS max_metric,
       avg(m.metric) AS avg_metric
FROM measurements m
JOIN items s ON m.key = s.key
GROUP BY s.key
```

The join keys must satisfy the admitted schema/type contracts. These examples
do not promise arbitrary join predicates, all join kinds, or aggregate
expressions of every type. See the positive aggregate and join cases in
[core plan tests](../../crates/velorix-core/tests/view_plan.rs). Mixing a direct
source and a published view in the same declaration is rejected.

### 4. Compute ordered outputs

SQL `OVER` admission remains policy-gated. To admit these staged window examples,
start the API with:

```sh
export VELORIX_EXPERIMENTAL_ADVANCED_VIEW_FEATURES=true
```

The default is `false`. The full validation driver explicitly enables this flag;
validation under that setting does not change the product default. Generic
operator backfill is configured independently: this flag enables advanced SQL
admission, not backfill scheduling or resource budgets.

For `events` with the appropriate key metadata, a nonnullable `ts` order column,
partition columns `group_id, day`, and `metric Float64`:

```sql
SELECT id,
       lag(metric) OVER (PARTITION BY group_id, day ORDER BY ts) AS previous
FROM events
```

```sql
SELECT id,
       sum(metric) OVER (
         PARTITION BY group_id, day ORDER BY ts
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS cumulative
FROM events
```

These forms come from [ordered runtime tests](../../crates/velorix-runtime/tests/ordered_window_runtime.rs).
Keep the required identity projections for keyed ordered plans. The bag variant
also has explicit duplicate and retraction tests in
[ordered identity tests](../../crates/velorix-runtime/tests/ordered_window_identity.rs),
including key-free `LAG` and cumulative `SUM`. Do not manufacture a key merely to
connect a bag-producing stage to an ordered stage.

`ROWS` counts retained row occurrences, including duplicates. `RANGE ... CURRENT
ROW` includes peers at the same order value. Do not assume that tied rows have
an application-defined ordering. The shown `LAG` syntax does not imply support
for a default argument, `IGNORE NULLS`, or arbitrary frames. Ordered admission
requires supported partition/order/frame shapes; split incompatible windows into
separate declared views.

## Signed bags and operator boundaries

An insertion contributes positive multiplicity; a retraction contributes negative
multiplicity. Two equal input rows remain two occurrences after a map or union.
Retracting one removes one occurrence. Series propagates each input change to its
generated rows. Aggregates and joins must consume those multiplicities rather
than treating equal rows as a single event. Ordered stages retain state needed
to recompute touched partitions and retract obsolete output values.

Transport weights are internal change metadata, not ordinary user-projected
columns. API schema inference must preserve actual output types and nullability
and must not claim uniqueness for bag output. Schema binding validates typed
expression leaves and source projections against the declared inputs. Persisted
logical nodes, execution contracts, and hashes must agree; recomputing a hash
does not make a malformed plan valid.

No relation name, customer identifier, or special SQL identifier activates an
algorithm. Names in these examples can be replaced with registered schema-bound
names. Plan hashes establish integrity and compatibility, not an allowlist of
application queries.

## Rebuild after removal of specialized plans

The old `native_fact`, `native_family`, and `native_rollup` production paths have
been removed. Persisted specialized execution tags such as `native_projection`,
`native_windowed_fact`, and `native_rollup` are unsupported and must fail closed.
They are not recovery escape hatches.

Preserve the external raw input. Recreate derived views through generic admission
in a new namespace, then backfill using the existing dependency graph. Before
switching consumers, validate schemas, bag-equivalent output, live signed
updates, dependency frontiers, and checkpoint/restart behavior. Keep the existing
production namespace intact until the replacement passes those checks. An old
specialized checkpoint is not a substitute for rebuilding generic operator
state.

# Scalar aggregate subquery admission

Scalar aggregate subqueries admit only plain `COUNT`, `SUM`, `MIN`, `MAX`,
and `AVG` calls with the existing simple argument forms. Function-level
metadata (`FILTER`, `OVER`, `WITHIN GROUP`, null treatment, parameters, and
ODBC syntax) and unsupported argument-list metadata (`DISTINCT` and argument
clauses such as `ORDER BY`) fail closed before aggregate mapping. Explicit
`ALL` is accepted as the default multiset semantics; `DISTINCT` and argument
clauses are rejected.

Plain `COUNT(*)` and `COUNT(column)` remain supported. A previously invalidly
admitted aggregate plan must fail re-admission; it is not automatically
migrated. Runtime and checkpoint shapes remain
unchanged for supported plain projections. The scalar runtime captures the
comparison column even when it is not part of the output projection, while
logical plans derive their projected and computed columns from the admitted
projection rather than a fixed source column. Decimal128 `SUM`, `AVG`, `MIN`,
and `MAX` remain rejected until decimal runtime aggregation exists; Decimal128
`COUNT` remains valid.
Typed/string output projections are also rejected until the scalar runtime can
carry their typed projection programs; they are never silently dropped from a
persisted plan. Checkpoints from an older implementation that lack the
captured comparison column are rejected and require a full replay; no
automatic checkpoint migration is claimed.
Nullable column aggregates skip SQL `NULL` values, while `COUNT(*)` counts
rows regardless of nullable payload columns; both behaviors are covered across
ingest and restore.

The public API wiring carries the scalar schema and projection contract through
admission, ingest, query, and checkpoint restore. The bounded API exercise
covered twelve views (six normal and six nullable), ingest and query, then a
fresh API restore followed by the same exact requery. Both
nullable and non-nullable Decimal `SUM`/`AVG`/`MIN`/`MAX` variants are rejected
at admission, and typed/string projections are rejected rather than silently
discarded. The API fixture's outer score values do not distinguish the
`COUNT(column)` values 2 and 3 directly; the focused runtime regression does,
and no stronger API oracle claim is made. The nullable API matrix is now
verified for all five aggregate NULL-skipping cases plus `COUNT(*)` row
counting.
Typed checkpoint dispatch covers the scalar and temporal paths as part of the
partial issue-36 wiring. Subsequent API evidence now includes
`rest_temporal_asof_join_materializes_retracts_and_restores` for the narrow
[issue #27 ASOF contract](https://github.com/mrchypark/velorix/issues/27),
including restore/requery and right-side retraction. This completed evidence
supersedes the earlier pending-fixture note, but issue #36 remains pending PR
merge and is not closed; it does not claim full ASOF support or a cluster
rollout.

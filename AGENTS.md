## Generic Query and View Support

Velorix is a jarless materialized view database/runtime. Do not add external
compiler, runtime build/deploy, package-loading, or image-based execution paths
for view creation.

The target product flow is:

- users register relations with explicit schemas
- users ingest schema-bound rows into those relations
- users define views over registered relations
- supported views are admitted into the internal materialized view runtime
- ingest updates the materialized output table automatically
- queries read materialized output, not a full source recomputation
- restart recovers from metadata and object/local storage checkpoints

Completion for generic query/view support requires evidence that the internal
runtime handles multiple relation schemas and more than one SQL family. At
minimum, verify filters, projections, group by, sum/count/min/max/avg, and a
two-table join through the same admission and runtime path. Unsupported SQL or
view shapes must fail closed during admission with a clear error.

Do not expand Velorix-owned SQL support by silently adding fake fallbacks. If a
SQL family is unsupported, return an admission error and implement the internal
runtime capability deliberately.

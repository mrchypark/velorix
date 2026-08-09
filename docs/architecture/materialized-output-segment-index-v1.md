# Materialized Output Segment Index V1

Status: Proposed
Applies to: materialized output serving and object-store cost control.

Velorix queries must read published materialized output, not source relation
batches or live operator accumulators. A segment index is only a small planning
index that helps skip irrelevant materialized output pages.

## Contract

The checkpoint manifest remains the durable authority. Segment metadata never
advances progress, never proves an output page is valid, and never replaces
manifest-bound content-hash verification.

Valid behavior when metadata is missing or stale:

- scan the manifest-referenced materialized output pages when the admitted query
  allows a full materialized-output scan
- fail closed when the query needs metadata to prove bounded coverage, such as
  recent-K over ordered output

Invalid behavior:

- scanning source relation batches to satisfy a view query
- returning partial recent-K results without proving coverage
- depending on Foyer cache contents for correctness
- adding JARs, external compilers, runtime build/deploy, or package-loaded view
  execution

## Minimal Metadata

Do not add a broad storage schema until the first reader needs it. The first
implementation only needs enough metadata to skip pages and prove the result:

- `view_id`
- `checkpoint_version`
- `segment_id`
- `page_ref`
- `content_hash`
- `row_count`
- output primary-key bounds when sorted
- event-time bounds when the view has event time
- optional sort key used for recent-K coverage

Detailed column statistics belong in later slices when readers need them.

## Benchmark Gates

The benchmark gate covers these materialized-output read behaviors:

- `materialized_output_segment_pruning`
- `materialized_output_recent_k`
- `materialized_output_compaction_equivalence`
- `materialized_output_compaction_debt`
- `materialized_output_delete_vector`
- `materialized_output_ttl_vector`
- `materialized_output_late_materialization`

These are benchmark evidence slices for materialized-output read behavior.
Output compaction is internal and experimental for public 1.0; no public compact
endpoint is exposed. Internal maintenance may perform safe one-shot compaction
of fragmented output manifests and no-op when the latest checkpoint is already
published as one page.
They must use manifest-like output page refs and content hashes over
standing-runtime output page objects.

Pass criteria:

- full materialized-output oracle and optimized read return identical rows and
  ordering
- source relation batches are not read
- object request counts and bytes read are recorded
- the optimized path re-reads selected objects through the object store path

Recent-K is not general Top-K. It is valid only when page sort bounds prove
coverage for the requested `ORDER BY event_time DESC LIMIT K` shape.

# Specification owner review

Reviewed source: `4751ef51718ac7a041072ad70404d0907648cb12`.

## What exists

`docs/specification.md` is the canonical current-implementation draft. The EGM
plan, six reported fresh-context reviews, frozen snapshots, and consolidated
findings are preserved under `docs/reviews/egm-2026-10-30/`. Snapshot hashes were
checked in this review. The historical EGM record explicitly excludes later
public-input changes and does not claim implementation-readiness approval,
production certification, or two-round review-loop convergence.

This review used two new fresh-context Luna agents: an EGM design critic reading
the bundled persona/feature standard and the target/history/input documents,
and a source reviewer covering SQL, demand, query/resources, and checkpoints.
Both returned findings and scope. This is a focused follow-up, not a new full
EGM staged review or an exhaustive 68-document drift audit. No engine changes,
builds, or runtime tests were performed. See `2026-11-01_DRIFT_REPORT.md`.

## Already settled; do not reopen by default

- No public input DELETE; retain internal view retractions/replacements.
- Hide engine multiplicity from public schemas and inputs.
- Register views without automatic historical backfill. Materialize requested
  ranges and reuse completed output. A user's explicit backfill remains user-
  controlled; its execution mechanism is not settled here.
- Arrow IPC/ZSTD for runtime state/output, with JSON public compatibility where
  documented; caches are disposable and never publication authorities.
- SQL capabilities remain generic and jarless; no business-case-specific engine
  paths or silent full-source fallback for unsupported SQL.
- S3 release qualification cannot be waived; failed measurements cannot be
  excluded and the acceptance threshold cannot be weakened to publish.

## First decision round — pending owner answers

No answers to these questions are recorded in this review. The recommendations
below are proposals for the owner to consider, not selected product policy.

1. **Read freshness:** fix a request-start source cut and return its coverage,
   or chase inputs arriving during the request? Recommended: a stable cut with
   explicit freshness metadata. This prevents continuous ingest from preventing
   a request from ever reaching completion. Separate a range-complete result
   from whole-view completion and from ingest acknowledgement.
2. **Explicit backfill execution:** after a user declares range/order/resources,
   should a worker autonomously continue until completion, or should each API
   request advance a bounded step? Recommended: autonomous user-started jobs with
   query priority and persisted pause/resume. This does not enable background
   backfill by default. Current demand controls are request-driven.
3. **Very late data and retention:** how should data arriving after the normal
   processing window be handled, and what source history must remain available
   to correct affected ranges? The policy for data outside any proposed horizon
   is unanswered; this review does not select dropping, ignoring, or discarding
   it. Recommendation: correct affected ranges while source history is retained,
   with bounded recomputation, subject to a separately specified source-retention
   contract and resource controls. Do not imply current cold state is unlimited.

No option is selected until the owner answers. In particular, autonomous
user-started jobs remain a recommendation rather than an agreed execution
mechanism, and no retention cutoff authorizes silently losing late data.

## Next decision round

- **Public mutation semantics:** explicitly confirm catalog-gated UPDATE/UPSERT
  versus inserts only. Define business-key uniqueness, replacement scope, and
  retries; UPDATE/UPSERT currently exist and public DELETE remains excluded.
- **SQL and view composition acceptance:** define required families, data types,
  timezone/calendar semantics for day→week/month/year view chains, cross-view
  cut consistency, and minimum end-to-end cases. Existing bounded admission is
  not arbitrary SQL or arbitrary-DAG qualification.
- **Resource promises:** define query wait targets, CPU/memory admission, spill,
  foreground/background fairness, and whether budgets are hard limits or
  cooperative targets. Current numeric defaults are not hard process RSS caps.
- **Greenfield compatibility:** prior conversation selected greenfield work;
  confirm whether Legacy/eager support is required by any actual user before
  designing a migration framework. Its presence in code does not make backward
  compatibility a mandatory new product requirement.
- **Operational support:** distinguish single-writer Local recovery from
  clustered failover; define acceptable recovery time/data loss and provider
  support. Existing recovery tests and Rhiza features do not establish every
  production deployment guarantee.

Record answers with alternatives and rationale in a separate decision artifact;
then derive an implementation plan with explicit acceptance tests. Do not turn
unverified implementation limits into user-approved product promises.

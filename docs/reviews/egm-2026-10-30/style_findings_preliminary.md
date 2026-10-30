# Stage 7 style findings — preliminary, verification pending

Historical intermediate record, superseded by [the complete style report and verified read manifest](style_report.md). The statements below describe the preliminary state only; verification subsequently completed and approved repairs were applied.

## Findings

- **T7-1 — Major — Lines 103–120:** Status/gap repetitions; shorten to the linked index without deleting known gap requirements.
- **T7-2 — Major — Lines 105–106:** Separate default event-time from experimental analytic `OVER` labels.
- **T7-3 — Nit:** Add a status key distinguishing implementation, policy, and qualification near the introduction.
- **T7-4 — Nit:** Remove the “ready for requirements review” readiness judgment.
- **T7-5 — Nit:** Replace the abstract snapshot warning with a concrete table introduction.

## Disposition supplied by parent

- Parent approves these factual clarity repairs after read-manifest verification.
- Preserve the numeric defaults table for practical current reference; compress repeated summaries instead. This is the disposition for Z5-1.
- Preserve the four-authorities table and the append-only public-input requirement.
- No target edits have been applied.

## Provenance status

Reviewer identity/model and the final read manifest have not yet been supplied or verified. Do not add reviewer provenance or mark Stage 7 complete from these preliminary notes.

## Superseding parent interpretation correction

The phrase “append-only public-input requirement” above was too broad. The explicit product requirement excludes public input DELETE specifically. Catalog-gated UPDATE/UPSERT remain as-built capabilities whose public product policy is not selected here. Preserve signed-weight semantics and derived output retractions. This parent correction is not a Goldfish finding; see P-1 in `critic_report.md`.

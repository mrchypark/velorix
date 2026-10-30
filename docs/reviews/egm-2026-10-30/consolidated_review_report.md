# Velorix specification — consolidated EGM review

## Outcome and scope

The applicable specification-review stages are complete. The revised document is an as-built contract draft, **not** a decision-ready new feature design, a production qualification, or an implementation-readiness pass. Document clarity repairs were applied; engine behavior was not changed.

The user selected the uninterrupted consolidated preset. Six completed, fresh-context Luna reviews covered initial zoom-out, comprehension, design, structure, style, and intermediate zoom-out. Stage 9 is explicitly not applicable because no detailed implementation plan exists. This was a single staged review with an intermediate architecture check, not an iterative review-loop run; **two-clean-round convergence is not claimed**.

- Implementation baseline: `c6222abd054858109c90a63a061db698fa9fc9e8`.
- Before review: [immutable snapshot](specification-before-review.md), lines 1–144, SHA-256 `1b5ed4d6d2a1302fe4dbb75cd7235094c499d053459958904b463ea038e58065`.
- Reviewed revision after repairs: [frozen reviewed snapshot](specification-reviewed.md), lines 1–150, SHA-256 `8bfe6d2e9124de1ed2643926b411742e0c59739d0807aabf69ef856731821bf2`. The [current specification](../../specification.md) subsequently changed its review-status link, public-input DELETE policy, and business-only schema/automatic multiplicity contract during separately requested implementation work. Those later changes are outside this frozen EGM review; the findings and remaining-work list below describe the reviewed baseline.
- Earlier source reconciliation: [68-document drift report](../2026-10-30_DRIFT_REPORT.md). That report is a separate evidence phase, not a Goldfish review.

## Stage results

| Stage | Actual result | Evidence |
| --- | --- | --- |
| 5 — Initial zoom-out | Completed; one Nit about repeated detail | [Original report and read completion](zoom_out_report.md) |
| 6 — Comprehension | Completed; paraphrase supplied, no findings | [Complete paraphrase and findings](comprehension_report.md) |
| 7 — Design | Completed; one Major, one Nit | [Design report](design_report.md) |
| 7 — Structure | Completed; no structural findings | [Structural report](structural_report.md) |
| 7 — Style | Completed; two Majors, three Nits; required read manifest verified on follow-up | [Style report and addendum](style_report.md) |
| 8 — Intermediate zoom-out | Completed on one fresh-context retry; no findings | [Retry result and failed-attempt record](intermediate_zoom_out_report.md) |
| 9 — Implementation readiness | Not applicable; no reviewers run, not a pass | [Applicability record](implementation_readiness_report.md) |

Individual reports retain reviewer identities, read manifests, reviewed scopes, and evidence limits. A report with no findings is not a code correctness or operational certification.

## Prioritized traceability and dispositions

Original severity is retained below; the dispositions describe document changes, not engine fixes. Three Major and five Nit observations were returned across stages 5–7, with overlapping issues consolidated into six actions.

| Action | Raw finding IDs | Priority | Disposition |
| --- | --- | --- | --- |
| A1 — State the document's decision boundary | D7-1 | Major | Added an explicit boundary: future design choices require a separate alternatives/decision artifact. Historical rationale was not invented. This resolves the scope ambiguity; a future design still must satisfy the feature standard's alternatives requirement. |
| A2 — Reduce repeated qualification summaries | T7-1, Z5-1 | Major / Nit | Replaced the repeated gap ledger with a short linked index. Retained numeric defaults as a useful reference; this narrower disposition avoids losing current contract information. |
| A3 — Separate policy status from qualification | T7-2, T7-3 | Major / Nit | Separated default event-time windows from gated analytic OVER and added one status key. No admission capability changed. |
| A4 — Explain purpose and limits | D7-2 | Nit | Added product purpose, goals, and non-goals from the existing product flow; business motivation beyond that remains unspecified. |
| A5 — Remove an unqualified readiness judgment | T7-4 | Nit | Replaced “ready for requirements review” with a statement that this document records current behavior and leaves future choices open. |
| A6 — Explain persistence authorities concretely | T7-5 | Nit | Clarified the table introduction; retained all four separate protocols and publication authorities. |

Detailed per-finding records are in [critic_report.md](critic_report.md). Stage 8 found no new architecture-level issue in the revised target, but it did not repeat the full design/style review or independently audit linked source code.

### Parent integration correction — P-1

The recorded user requirement excludes **public input DELETE** and retains derived-view retractions/replacements. The earlier draft broadened this to a blanket append-only requirement. That inference was removed. UPDATE/UPSERT remain as-built catalog-gated capabilities whose public product policy is not selected by this document. This is a parent integration correction, not a Goldfish finding or a new approval of those operations.

## Product work that remains open

These are unresolved implementation or qualification matters from the specification, not defects repaired by editing prose:

1. **Input DELETE policy:** the catalog-gated API still supports DELETE although the recorded requirement excludes it. A separate engine change must preserve derived retractions/replacements.
2. **Public SQL and dependency scope:** support remains limited to admitted shapes; arbitrary SQL, general DAG interleavings, and production-scale closure are not qualified. Future expansion needs a bounded contract and a separate alternatives/design review.
3. **Freshness and demand controls:** the fixed materialized ACK label and local completed status do not certify demand-view freshness. Per-view coverage/cuts matter; autonomous scheduling and stronger fairness guarantees are not established.
4. **Operational safety and recovery:** production fencing, provider parity, GC, rollback, bounded failover, and multiwriter claims require their own execution evidence.
5. **Resource guarantees:** policy limits, cooperative budgets, cache capacities, and historical benchmarks do not establish hard process RSS limits or universal S3 costs.
6. **Migration and mutation policy:** Legacy/eager-to-demand migration and the public UPDATE/UPSERT policy need explicit future decisions.

## Execution limitations and validation

- Required persona and assigned-standard reads were reported complete. Initially partial Stage 5 linked reads and Stage 7 style manifest omissions were completed on follow-up; their history is preserved.
- The first Stage 8 execution returned no result and was stopped. Its fresh retry completed. The document executor also stalled during final consolidation and was stopped; the parent completed integration from received reports. Neither stopped execution is counted as a completed reviewer.
- Source comparison and local link checks support documentation consistency only. No builds, runtime tests, benchmarks, fault injection, live services, or deployment operations ran in this review. Existing baseline CI evidence is not new execution evidence.
- Final checks passed: the scope inventory, document index, and drift headings contain the exact same 68 paths; local Markdown targets resolve for the specification, index, plan, and stage artifacts; snapshot references are checked from their original `docs/` base. `git diff --check` passed. Current and new files are documentation only. No engine, dependency, configuration, or deployment change is part of this review.
- All stage artifacts are retained as requested deliverables. No test servers or containers were started and none require cleanup. No commit or push occurred during this specification-review phase; this records the review phase only and does not describe the repository's current PR or publication status.

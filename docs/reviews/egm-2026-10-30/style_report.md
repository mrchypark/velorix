## Style Goldfish review

**Target:** docs/specification.md, lines 1–144
**Feature range:** full specification
**Detailed range:** N/A
**Manifest:** Read target/egm-spec-reconciliation-20261030/scope.json in full.
**External style standard:** None applied.

- **T7-1 — Major — Lines 103–120.** The status categories and numbered gaps repeat several qualifications already stated in the body: demand views may lag ingest (36, 46), DAG and deployment limits (42, 48), resource limits (78, 82), and recovery gates (101). This repetition makes the document harder to scan and leaves readers to reconcile two summaries. **Suggested wording:** Keep one concise status summary here, such as “Unverified release areas include production deployment and failover, provider parity, release benchmarks and GC, whole-DAG behavior, and DataFusion memory/spill enforcement.” Remove gap items already explained in the relevant sections, or make this section a short index to them.

- **T7-2 — Major — Lines 105–106.** “Public default” and “Public experimental” do not form mutually clear status categories: line 106 calls analytic OVER experimental while saying event-time windows are enabled by default. Readers may interpret “experimental” as applying to both. **Suggested wording:** Split the entries by feature: “Default: … Event-time windows are policy-enabled by default.” and “Experimental: Analytic OVER admission requires …”

- **T7-3 — Nit — Lines 4, 6, 105, 108–109.** Status terms use different tests: “Supported” means an implementation path and admission boundary exist (6), “Public default” depends on a matrix label (105), and “Unverified” covers qualification gaps (109). The distinctions are useful, but the document does not define these labels together, so readers may mistake implementation status for qualification status. **Suggested wording:** Add a compact status key near line 6 that defines “Supported,” “Default,” “Experimental,” and “Unverified,” or use the SQL matrix’s exact labels consistently and link to their definitions.

- **T7-4 — Nit — Lines 124, 132.** “Ready for requirements review” is an unqualified readiness claim, while line 132 says no plan is selected. The intended status is clearer if stated as a scope boundary rather than a readiness judgment. **Suggested wording:** “This draft records current implementation behavior and leaves future architecture choices open.”

- **T7-5 — Nit — Lines 88–89.** “They have different authorities and must not be summarized as one snapshot” is abstract; the table that follows gives the concrete distinction. **Suggested wording:** “The protocols use different records and publication authorities; the table summarizes each separately.”

The review found no other clear style issue in the remaining scope.

## Corrected read manifest (follow-up)
- EGM agents/style.md — read in full, 66 lines.
- docs/specification.md — read in full, 144 lines.
- Additional target artifacts: none.
Findings unchanged.

## Provenance

- Reviewer: Bacon
- Reviewer ID: `01a18beb-7817-7600-8b60-40f33dd3727f`
- Model: Luna, medium
- Context: fresh fork, `fork_context: false`
- Stage: 7 — Style

## Read-manifest history

The initial manifest supplied with the review listed the scope file but omitted the required full-read entries for the style persona and target. The parent flagged this omission. The corrected follow-up manifest above records full reads of `EGM agents/style.md` (66 lines) and `docs/specification.md` (144 lines); the parent verified those required reads were completed. No inaccessible reads were reported. Findings remained unchanged. This history is retained rather than presenting the initial manifest as complete.

## Parent interpretation correction (not a reviewer finding)

The preliminary shorthand “append-only public-input requirement” was too broad. The explicit requirement excludes public input DELETE specifically. Catalog-gated UPDATE/UPSERT remain as-built capabilities; their public product policy is not selected here and is neither newly approved nor prohibited. Signed weights and derived output retractions remain distinct.

# SlateDB upstream comparison — 2026-09-27

**Decision: use exact official crates.io SlateDB 0.16.0.** The user explicitly
accepted its measured storage-cost increase after the original gate failure.
The original NO-GO and failed measurements below remain historical evidence.
Only the affected SlateDB workload cost baseline is recalibrated from recorded
full-workload measurements; the 25% budget, default GC and durable commit
acknowledgments remain unchanged. The former fork was archived on 2026-09-27;
its URL and refs remain available for historical builds.

## Sources and method

- Velorix base: `70776eafac9fc0a9149809abee11a4daab4a7d20`, branch
  `feature/slatedb-upstream`, created from freshly fetched `origin/main`.
  Local `main` was not reset.
- Fork: `git+https://github.com/mrchypark/slatedb`, revision
  `4b0a95a86c68b94d2fd368db76a0b75791951d5f`, package version 0.16.0.
- Official: exact crates.io `slatedb = "=0.16.0"`, checksum
  `fb40332f41e231926df3a0ef742c05316b43368ba4b520433d2a1eba1ab00f0f`.
  Both use `default-features = false`. The candidate lock changed only the
  three SlateDB packages' source/checksum entries.
- Fork history contains four commits beyond the 0.16.0 base: shared WAL GC
  listing, omitted empty manifest vectors, omitted default sequence trackers,
  and deferred initial GC for fresh databases. It is not identical to official.
- Both sets of release binaries were built and preserved before measurement.
  Measurements ran serially, fork then official, with no compilation overlap
  and no sample retries. A clean environment retained only PATH, HOME, TMPDIR
  and explicitly selected diagnostic variables. The production open path uses
  `Settings::default()`; GC and compaction were not disabled for gate/default runs.
- Existing `local_incremental`, `slatedb_reopen_diagnostic`, storage tests and
  `state_store_upgrade_fixture` were reused. The upgrade shell runner was not
  used because it hardcodes 0.15→0.16; direct fixture invocation distinguishes
  fork and registry binaries using separate locks and hashes.

## Original mandatory cost gate (before acceptance)

Original baseline: `baselines/benchmark/local/pr-smoke.json`, commit
`d8f8673add87d28f678b4ad6ab7f261b4008d5c2`; budget: **25%**.
One complete workload run per version. Both benchmark JSON validations passed.
Fork gate exit **0**, official gate exit **1**:

```text
benchmark workload slatedb_state_reopen metric bytes_written regressed by 0.268, over budget 0.250
```

| `slatedb_state_reopen` metric | Baseline | Fork | Official |
| --- | ---: | ---: | ---: |
| PUT attempts | 33 | 35 | 35 |
| GET | 7 | 4 | 4 |
| LIST | 9 | 5 | 10 |
| Range reads | 2 | 2 | 2 |
| Bytes written | 8,660 | 10,648 | **10,984** |
| Bytes read | 3,348 | 2,462 | 2,022 |
| p50/p95 ms (single sample, informational) | 6.311815 | 22.445415999999998 | 15.873875 |
| Scan bytes | 0 | 0 | 0 |

The original byte ceiling was **10,825**. Official exceeded it by **159 bytes**;
its baseline regression is 26.8360%, versus 22.9561% for the fork.
Official writes 336 more bytes (3.16%) than the fork in this workload. This is a
baseline-budget failure, not a claim of a 26.8% regression relative to the fork.
The comparator reports its first failure; this does not prove every other
candidate check passed. Full per-workload metrics are retained in both
`cost.json` artifacts.

Whole-workload metrics, exactly as recorded:

| Metric | Fork | Official |
| --- | ---: | ---: |
| Rows/s | 67952.53004804971 | 71989.0880290161 |
| Bytes/row | 223.36865234375 | 223.45068359375 |
| PUT/GiB | 1062102.1236001477 | 1061712.2140083518 |
| PUT attempts | 905 | 905 |
| GET | 230639 | 230639 |
| LIST | 2089 | 2094 |
| Range reads | 2 | 2 |
| Bytes written | 914918 | 915254 |
| Bytes read | 233363635 | 233363195 |
| Checkpoint p50 ms | 2.508583 | 2.415417 |
| Checkpoint p95 ms | 2.9521249999999997 | 3.426208 |
| Recovery p95 ms | 4.8643339999999995 | 3.5835 |
| Peak RSS bytes | 923795456 | 936296448 |
| Spill bytes | 0 | 0 |
| Scan bytes | 0 | 0 |

Timing and RSS are informational for this local PR gate. Faster observed
official timings do not override a cost failure. Metered calls include attempted
PUTs and lifecycle maintenance; they are not committed-byte or cloud-billing totals.

## Short diagnostics and correctness

Five default startup/write/close/reopen/readback samples per version all passed.
The existing five maintenance-limited controls per version also passed but are
diagnostic only and were not used to justify production acceptance.

| Default short-run range | Fork | Official |
| --- | ---: | ---: |
| PUT attempts | 33–35 | 35 |
| GET | 3–4 | 3–5 |
| LIST | 6–8 | 9–12 |
| Range reads | 2 | 2 |
| Bytes written | 9269–10457 | 10793 |
| Bytes read | 1815–2409 | 1375–2579 |
| Elapsed ms | 19.583875–33.544458 | 13.387542–19.691167 |

Neither default series had stable counts across all metrics. These component
results are not interchangeable with the authoritative-preflight cost workload.

- Both dependencies: **4/4 durability tests**, **10/10 state tests** passed.
  Durability tests cover blocked/failed WAL writes and deletes, and independent
  visibility before closing the writer.
- Fork write → official verify/write/delete/reopen → fork verify → official
  verify: **passed**.
- Official write → fork verify/write/delete/reopen → official verify → fork
  verify: **passed**.
- Fixture payloads include binary, empty and 256-KiB values; duplicate writes
  fail closed. Each fixture command verifies close/reopen. This is bounded
  compatibility evidence, not exhaustive crash or on-disk-format certification.

## Limits and artifacts

The planned 660-second observations were **not started** after the mandatory
gate failed. Both sources use a default 600-second GC
interval, but no recurring-GC or reclamation parity claim is supported here.
Deletes are unmetered in the component diagnostic. No accelerated GC was run.

Local evidence root: `target/slatedb-upstream-comparison/` (ignored, not durable
repository storage). It contains `PLAN.md`, `STATUS.md`, `ENVIRONMENT.md`,
both versions' build/test logs, complete `cost.json`/`short.json`, gate exit/error
files, preserved executables, `Cargo.lock`, lock source entries and verified
`SHA256SUMS`; plus both `compat-*-to-*` fixture trees and command results.
Official `gate.json` is empty because the CLI fails before emitting success JSON;
`official/gate.stderr` and `official/gate.exit` are the failure evidence.

`executed-source/` preserves the identical diagnostic source used for both
binaries, its temporary patch, and the unchanged cost/compatibility sources.
The unused long-observation extension was removed from the working tree after
preserving this provenance. Before explicit upstream acceptance, Cargo.toml,
Cargo.lock and the baseline were restored unchanged. The accepted follow-up
changes the dependency, its governance and the scoped cost baseline described
below. The report records the migration scope and validation evidence. The former
`mrchypark/slatedb` repository is archived; its Git history remains readable.

## Accepted upstream recalibration

After reviewing the original failure, the user explicitly chose the official
release and accepted its cost tradeoff. The dependency is exact crates.io
0.16.0 with the tested lockfile. Only SlateDB's Git allowlist entry is removed;
Hiqlite's entry is unchanged. The production storage wrapper and both
`await_durable()` calls are unchanged.

Following the baseline policy's conservative measured-envelope approach, seven
additional full `local_incremental` runs used the preserved official release
binary, sequentially before compilation, without retries. All seven validated.
The original official run is also included; no run was discarded. These are
full gate workloads, not the short component diagnostic. Exact source, binary,
lockfile and raw-result hashes, commands and per-run metrics are recorded in
[the calibration evidence](slatedb-upstream-calibration-2026-09-27.json).

| Full-workload sample | Bytes written | LIST |
| --- | ---: | ---: |
| Original official | 10984 | 10 |
| Additional 1 | 10984 | 11 |
| Additional 2 | 10984 | 11 |
| Additional 3 | 10984 | 12 |
| Additional 4 | 10984 | 12 |
| Additional 5 | 10984 | 10 |
| Additional 6 | 10984 | 10 |
| Additional 7 | 10984 | 10 |

A bytes-only recalibration still failed the real comparator on additional run
3: LIST 12 versus baseline 9 is a 33.3% increase, above 25%. Run 4 also recorded
12. After reviewing that evidence, the user approved both measured maxima:

- `slatedb_state_reopen.object_requests.bytes_written`: **8660 → 10984**.
- `slatedb_state_reopen.object_requests.list_count`: **9 → 12**.

All eight official results pass the resulting gate. The 25% budget, CLI and CI
are unchanged. Every other baseline value is unchanged, including all top-level
metrics, all other workloads, and the other SlateDB metrics. No arbitrary
provenance keys were added to the strict benchmark schema. The baseline retains
its historical commit identifier for the untouched values; the separate evidence
file identifies the later two-field contribution and makes this mixed provenance
explicit. This is an accepted cost change, not a performance improvement claim.

Original official storage/durability and bidirectional compatibility evidence
remains applicable: dependency source/lock and production wrapper are identical.
The long-GC limitation above still applies; no 22-minute observation was added.

Accepted-change checks: `cargo fmt`, workspace/all-target clippy with
`-D warnings`, **54 runtime library tests**, **251 runtime integration tests**,
cargo-deny using locked all-feature metadata, and dependency-governance
validation all passed. All eight recalibrated gate comparisons passed. Final
scope verification confirmed that undoing only the two approved baseline
values reproduces the complete original baseline, and that the production
wrapper, diagnostic/cost harnesses and CI workflow are unchanged. Logs are in
`target/slatedb-upstream-comparison/accepted-upstream/`.

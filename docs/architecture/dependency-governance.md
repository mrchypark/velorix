# Dependency Governance

Velorix runs `cargo-deny 0.20.2` with `cargo deny check -W unmaintained` in CI.
The declared MSRV remains Rust `1.98.0`; CI installs that version and runs
`cargo check --workspace --all-targets --locked` to enforce the declared MSRV
against the locked dependency graph.

The current query dependency train is DataFusion `55.1.0` with Arrow/Parquet
`59.3.0`; the declared MSRV remains unchanged. Velorix's admission parser uses
sqlparser `0.63.0`, while DataFusion retains `0.62.0`. These AST types are not
exchanged: admission ASTs stay inside `velorix-core`, and the DataFusion query
boundary receives SQL text through `SessionContext::sql`. Parser acceptance
does not imply materialized-view support; unsupported ordering and cast shapes
must continue to fail closed. The duplicate parser versions require explicit
review against the final cargo-deny diagnostics, not an advisory suppression.

Development and normal CI builds are pinned to Rust `1.98.1` in
`rust-toolchain.toml` and every non-MSRV `dtolnay/rust-toolchain` action. The
official `rust:1.98.1-bookworm` image tag was not available when this policy
was updated, so product builders retain the verified digest-pinned
`rust:1.98.0-bookworm` base and install/select Rust `1.98.1` through its
bundled official `rustup`; each builder verifies `rustc 1.98.1` before compiling.
The install disables rustup self-updates, so the builder does not replace its
bootstrapping client with an unpinned version. This preserves a reproducible
base image while keeping build output on the current stable toolchain. Revisit the base image pin when the exact official
`1.98.1-bookworm` tag is published.

Machine-readable local policy lives in `dependency-governance.json`. Validate it
with:

```bash
cargo run -p velorix-cli -- dependency-governance-validate --manifest dependency-governance.json
```

Use `--json` to emit stable local governance evidence:

```bash
cargo run -p velorix-cli -- dependency-governance-validate \
  --manifest dependency-governance.json \
  --cargo-deny-json target/dependency-governance/cargo-deny.jsonl \
  --json > target/dependency-governance/local-dependency-governance-evidence.json
```

The local evidence has `schema_version=1`,
`evidence_kind=dependency_governance_validated`, the manifest path/name, checked
cargo-deny diagnostics path, required and reviewed package subjects, exception
counts, and warning counts. `--json` requires `--cargo-deny-json` so release
evidence cannot claim a dependency-governance pass from manifest-only
validation. This cargo-deny-backed artifact is sufficient only for the
artifact-gated `readiness-report` dependency-governance subcheck when it has
`status=pass`, `evidence_kind=dependency_governance_validated`, checked
cargo-deny diagnostics, and no missing required package-review subjects.
`external_audit_attestation=false` is expected for this local governance
artifact; it does not satisfy the separate live release-readiness evidence
gates.

The manifest records the declared MSRV policy and requires package review
records for the high-risk production dependency subjects that shape Velorix's
database boundary: DataFusion, object storage, Kubernetes, SlateDB, Foyer, the
metadata authority, and the internal materialized view runtime. The product
metadata authority is the embedded Rhiza KV backend, reviewed on `2026-10-08`.
Hiqlite is no longer a dependency: it left `Cargo.toml` and the lockfile, its
git source was removed from the `deny.toml` `allow-git` list (which is now
empty, so every git source is denied), and its manifest package-review record
was removed with it. `deny.toml` also carries no advisory suppressions; the
quick-xml 0.39 Hiqlite -> cryptr -> s3-simple line that required
`RUSTSEC-2026-0194` and `RUSTSEC-2026-0195` exemptions is gone, so the authority
path's only quick-xml line is 0.41.
SlateDB uses exact crates.io 0.16.0 with default features disabled; its former
Git-source allowance has been removed. The user accepted
the official release's measured storage-cost increase. Default GC and durable
commit acknowledgments remain enabled. The
[comparison report](../development/slatedb-upstream-comparison-2026-09-27.md)
preserves the original gate failure, scoped measured baseline recalibration,
durability tests and bidirectional persisted-state compatibility evidence.
Each package review names an owner, review date, local
audit status, feature policy, and replacement plan. This is the required local
audit workflow for the release gate.

Every declared duplicate, unmaintained, advisory, or yanked exception must also name an
owner, expiry date, reason, replacement plan, and promotion rule. Expired
exceptions fail closed: either the warning is removed, the package is upgraded
or replaced, or the exception is renewed with a current owner and plan.

Licenses are fail-closed through the explicit allowlist in `deny.toml`.
`ISC` is allowed because current Rustls/ring transitive dependencies
(`ring`, `rustls-webpki`, and `untrusted`) use it; it is OSI-approved and
compatible with the rest of the current allowlist. Unknown licenses are not
broadly allowed.

## 2026-10-08 quarterly exception review

The Rhiza 0.19.0 upgrade and the Hiqlite removal were followed by a quarterly
review of every expired exception. Evidence came from the exact CI commands,
run without any build:

```bash
cargo metadata --format-version 1 --locked --all-features \
  > target/dependency-governance/cargo-metadata.json
cargo deny --color never --locked \
  --metadata-path target/dependency-governance/cargo-metadata.json \
  -f json check -W unmaintained \
  2> target/dependency-governance/cargo-deny.jsonl
```

That graph resolves 552 packages and produced `advisories 0 errors / 0
warnings`, `bans 0 errors / 17 warnings`, `licenses 0 errors / 1 warning`, and
`sources 0 errors / 0 warnings`.

Yanked dependencies are clean: `yanked = "deny"` in `[advisories]` is
unchanged and the advisory section reported zero errors, so no yanked package
is in the graph and no `yanked` exception is required. No advisory is
suppressed; `ignore = []` stays empty.

The 18 expired `duplicate` exceptions all carried `expires_on: 2026-09-30`,
which is before the review date, so the validator would have failed closed on
every one of them. They were resolved as follows:

- `pem` was removed. The graph has only one non-dev `pem` line:
  `kube-client 4.2.0` selects `pem 3.0.6`. The `pem 4.0.0` line comes only from
  the `rcgen 0.14.10` dev-dependency of `velorix-meta`, and `cargo-deny 0.20.2`
  excludes dev-dependencies from `check bans` by default, so no `pem` duplicate
  diagnostic exists and the exception was stale.
- The remaining 17 duplicates were renewed to `expires_on: 2027-01-08`, the
  next quarterly boundary after this review. Each record now names the
  concrete versions in the current graph, example introducers for each version,
  the retained owner, and a plan that names the upstream release whose version
  bump retires the record. Example introducers come from version-qualified,
  target-aware inverse trees over normal and build edges, for example:

  ```bash
  cargo tree --locked --all-features --target x86_64-unknown-linux-gnu \
    -e normal,build -i getrandom@0.2.17 --depth 1
  cargo tree --locked --all-features --target x86_64-unknown-linux-gnu \
    -e normal,build -i getrandom@0.4.3 --depth 2
  ```

  The version qualification is required: bare `-i getrandom` fails with
  `specification 'getrandom' is ambiguous`. Build edges must be included,
  because `const-random-macro 0.1.16`, `jobserver 0.1.35`, and
  `prost-build 0.14.4` reach getrandom, heck, and itertools without appearing
  under `-e normal`. Unreachable lock entries are excluded: `quinn-proto 0.11.18`
  and `rand_pcg 0.10.2` resolve in the metadata but print nothing under
  `cargo tree -i` because reqwest's `http3` feature is off, so they are not
  recorded as introducers. `cargo deny` reports exactly the 17 renewed
  records, and the governance validator's exact CI command passed with
  `status=pass` and matching exception/warning counts of 17/17, so no warning
  is uncovered and no record is stale.

Only one license allowance was removed: `bzip2-1.0.6`. `bzip2` left the graph
with the Hiqlite -> cryptr -> s3-simple line, so `unused-allowed-license`
stopped matching it. `CDLA-Permissive-2.0` is retained even though
cargo-deny 0.20.2 still reports it as not encountered:
`webpki-root-certs 1.0.9`, reached through `rustls-platform-verifier 0.7.0`
from `kube-client 4.2.0` and `reqwest 0.13.5`, declares exactly that license,
and removing the allowance makes `cargo deny check licenses` fail with
`error[rejected]`. That residual warning is accepted as a cargo-deny reporting
artifact; it does not weaken the allowlist.

The renewed exception set was captured against the lockfile after the
concurrent `yoke-derive 0.8.3 -> 0.8.4` bump. `yoke-derive 0.8.4` sits on the
`syn 3.0.6` side of the `syn` duplicate, so the `syn` record was re-verified
against `yoke-derive 0.8.4` rather than 0.8.3 and no exception set changed.

Duplicate dependency versions are warnings today. They require package review
coverage and exception promotion rules, but they do not fail CI until the
dependency tree is stable enough to promote selected warnings without blocking
routine upstream movement.

Unmaintained advisories are also warnings today. Each allowed exception is
tracked in the governance manifest with an owner, expiry, reason, replacement
plan, and promotion rule.

Yanked packages are denied, not warned: `[advisories] yanked = "deny"` makes a
yanked package in the graph a hard `cargo-deny` error, and the 2026-10-08 run
reported zero advisory errors. A yanked record in the manifest exists only to
describe a warning that is being tolerated, and the same owner, expiry,
reason, replacement-plan, and promotion-rule fields apply. Advisories
suppressed in `deny.toml` must still have one manifest record per advisory ID;
suppression does not constitute security approval.

The current generated 1.0 readiness report does not require a separate
`cargo-vet` attestation. Decisions about whether duplicate-version,
unmaintained, or advisory warnings later graduate from local-review exception
governance into hard `deny.toml` gates are ongoing maintenance policy, not a
substitute for the release evidence gates.

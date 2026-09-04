# Dependency Governance

Velorix runs `cargo-deny 0.20.2` with `cargo deny check -W unmaintained` in CI.
The declared MSRV remains Rust `1.98.0`; CI installs that version and runs
`cargo check --workspace --all-targets --locked` to enforce the declared MSRV
against the locked dependency graph.

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
database boundary: DataFusion, object storage, Kubernetes, SlateDB, Foyer,
Hiqlite, and the internal materialized view runtime. The `mrchypark/hiqlite` git source is explicitly
allowed in `deny.toml` while Velorix uses the pinned fork-main commit that carries
the required metadata-backend authority-time API before an upstream release
carries that support. Each package review names an owner, review date, local
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

Duplicate dependency versions are warnings today. They require package review
coverage and exception promotion rules, but they do not fail CI until the
dependency tree is stable enough to promote selected warnings without blocking
routine upstream movement.

Unmaintained advisories are also warnings today. Each allowed exception is
tracked in the governance manifest with an owner, expiry, reason, replacement
plan, and promotion rule.

Yanked packages remain visible as cargo-deny warnings and require the same
owner, expiry, and replacement-plan fields. Advisories suppressed in
`deny.toml` must still have one manifest record per advisory ID; suppression
does not constitute security approval.

The current generated 1.0 readiness report does not require a separate
`cargo-vet` attestation. Decisions about whether duplicate-version,
unmaintained, or advisory warnings later graduate from local-review exception
governance into hard `deny.toml` gates are ongoing maintenance policy, not a
substitute for the release evidence gates.

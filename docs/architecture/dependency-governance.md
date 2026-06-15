# Dependency Governance

Velorix runs `cargo deny check -W unmaintained` in CI.
CI also installs Rust `1.95.0` and runs
`cargo check --workspace --all-targets --locked` to enforce the declared MSRV
against the locked dependency graph.

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
validation. This cargo-deny-backed artifact is sufficient for the
artifact-gated `readiness-report` dependency-governance check when it has
`status=pass`, `evidence_kind=dependency_governance_validated`, checked
cargo-deny diagnostics, and no missing required package-review subjects.
`external_audit_attestation=false` is expected for this local governance
artifact and is not a release blocker.

The manifest records the declared MSRV policy and requires package review
records for the high-risk production dependency subjects that shape Velorix's
database boundary: DataFusion, object storage, Kubernetes, SlateDB, Foyer,
Hiqlite, and the internal materialized view runtime. The `mrchypark/hiqlite` git source is explicitly
allowed in `deny.toml` while Velorix uses the pinned fork-main commit that carries
the required metadata-backend authority-time API before an upstream release
carries that support. Each package review names an owner, review date, local
audit status, feature policy, and replacement plan. This is the required local
audit workflow for the release gate.

Every declared duplicate, unmaintained, or advisory exception must also name an
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

There is no separate `cargo-vet` release-attestation blocker in the 1.0
readiness contract. Decisions about whether duplicate-version, unmaintained, or
advisory warnings later graduate from local-review exception governance into
hard `deny.toml` gates are ongoing maintenance policy, not a 1.0 release
blocker.

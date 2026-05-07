# Dependency Governance

Velorix runs `cargo deny check -W unmaintained` in CI.
CI also installs Rust `1.88.0` and runs
`cargo check --workspace --all-targets --locked` to enforce the declared MSRV
against the locked dependency graph.

Machine-readable local policy lives in `dependency-governance.json`. Validate it
with:

```bash
cargo run -p velorix-cli -- dependency-governance-validate --manifest dependency-governance.json
```

The manifest records the declared MSRV policy and requires package review
records for the high-risk production dependency subjects that shape Velorix's
database boundary: DataFusion, object storage, Kubernetes, SlateDB, Foyer, and
Feldera artifacts. Each package review names an owner, review date, local audit
status, feature policy, and replacement plan. This is a lightweight local audit
workflow until external audit evidence such as `cargo-vet` exists.

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

Remaining gaps for 1.0 are external audit attestations such as `cargo-vet` and
decisions about which warning classes graduate from local-review exception
governance into hard `deny.toml` gates.

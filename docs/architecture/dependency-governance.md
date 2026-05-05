# Dependency Governance

Velorix runs `cargo deny check -W unmaintained` in CI.

Machine-readable local policy lives in `dependency-governance.json`. Validate it
with:

```bash
cargo run -p velorix-cli -- dependency-governance-validate --manifest dependency-governance.json
```

The manifest records the declared MSRV policy and requires every declared
duplicate, unmaintained, or advisory exception to name an owner, expiry date,
and reason. It is a lightweight local policy gate, not a replacement for
`cargo-vet` or a full machine parser for `cargo deny` output.

Licenses are fail-closed through the explicit allowlist in `deny.toml`.
`ISC` is allowed because current Rustls/ring transitive dependencies
(`ring`, `rustls-webpki`, and `untrusted`) use it; it is OSI-approved and
compatible with the rest of the current allowlist. Unknown licenses are not
broadly allowed.

Duplicate dependency versions are warnings today. They require review during
package changes, but they do not fail CI until the dependency tree is stable
enough to promote them without blocking routine upstream movement.

Unmaintained advisories are also warnings today. Each allowed exception is
tracked in the governance manifest with an owner, expiry, and reason.

Remaining gaps for 1.0 are a `cargo-vet` or equivalent audit process and an
MSRV enforcement beyond the declared local policy.

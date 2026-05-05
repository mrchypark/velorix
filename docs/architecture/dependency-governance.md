# Dependency Governance

Velorix runs `cargo deny check -W unmaintained` in CI.

Licenses are fail-closed through the explicit allowlist in `deny.toml`.
`ISC` is allowed because current Rustls/ring transitive dependencies
(`ring`, `rustls-webpki`, and `untrusted`) use it; it is OSI-approved and
compatible with the rest of the current allowlist. Unknown licenses are not
broadly allowed.

Duplicate dependency versions are warnings today. They require review during
package changes, but they do not fail CI until the dependency tree is stable
enough to promote them without blocking routine upstream movement.

Unmaintained advisories are also warnings today. Promotion to fail-closed
should name an owner and expiry for each allowed exception.

Remaining gaps for 1.0 are a `cargo-vet` or equivalent audit process and an
explicit MSRV policy.

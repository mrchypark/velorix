# Vendored Zstandard dependency backport

These are unmodified source payloads from the exact locked crates.io releases
below, except for the `zstd` dependency version in each normalized
`Cargo.toml`. The upstream Rust source, `Cargo.toml.orig`, and all other files
remain as published. The Arrow IPC and Parquet `LICENSE.txt` and `NOTICE.txt`
files are retained. The published `foyer-storage` archive contains no separate
license or notice file; its normalized and original manifests identify its
license as `Apache-2.0`. That SPDX license is supplied by the byte-identical
`third_party/LICENSE-APACHE-2.0.txt` copy of Arrow IPC's published license
text. Upstream copyright headers are unchanged, and no Foyer NOTICE file is
added because none was present in its published payload.

| Crate | Published source | Original `.crate` SHA-256 |
| --- | --- | --- |
| `arrow-ipc 59.3.0` | <https://crates.io/crates/arrow-ipc/59.3.0> | `dcc41681ea80f521df14c36725b74d4c60702c47f0793af2be469c04527e2599` |
| `parquet 59.3.0` | <https://crates.io/crates/parquet/59.3.0> | `ff322f54b1a0f9288e614ed1f2d329b380af5476420db19f46ffb865e1163d73` |
| `foyer-storage 0.22.6` | <https://crates.io/crates/foyer-storage/0.22.6> | `118192f532ba013f0cdc607efc22324b9ed855baed185608af0daa986e835c6b` |

The checksums were matched against the registry checksums in the pre-backport
`Cargo.lock` and the local crates.io sparse index. The current path-patched
entries in the workspace lockfile have no registry source checksum. The only
vendor manifest changes are:

| File | Published requirement | Local requirement |
| --- | --- | --- |
| `arrow-ipc-59.3.0/Cargo.toml` | `zstd 0.13.0` | `zstd 0.14.3` |
| `parquet-59.3.0/Cargo.toml` | `zstd 0.13` | `zstd 0.14.3` |
| `foyer-storage-0.22.6/Cargo.toml` | `zstd 0.13` | `zstd 0.14.3` |

The separate `pulldown-cmark 0.13.5` security fix is resolved in the
workspace-root `Cargo.lock`; it is not part of this Zstandard backport.

Velorix's `velorix-core` dev dependency is also updated to `zstd 0.14.3`.
Workspace path patches select the vendored packages globally; their original
package versions are unchanged. The vendored directories are explicitly
excluded from workspace membership so workspace formatting and lint commands
do not rewrite or treat upstream source as Velorix code. The nested
`Cargo.lock` files are retained as part of the published payloads; Cargo uses
the workspace-root `Cargo.lock` for this project, so these upstream lockfiles
do not select dependencies in Velorix and are not standalone Velorix build
locks.

## Reproduction and verification contract

For source provenance, download each exact `.crate` archive from
`https://static.crates.io/crates/<crate>/<crate>-<version>.crate`, verify its
SHA-256 against the table, extract it, and compare the result with its
`third_party/<crate>-<version>` directory. The only allowed difference is the
single `zstd` version line in the normalized `Cargo.toml`; do not change
`Cargo.toml.orig`, Rust source, licenses, notices, or other published files.

For dependency resolution, require `cargo metadata --format-version 1
--locked --all-features` and the repository's exact cargo-deny command to pass.
The separate inverse dependency checks must resolve to the fixed crate versions:

```sh
cargo tree --locked --all-features -i zstd
# root package: zstd v0.14.3 (or a compatible later fixed version)

cargo tree --locked --all-features -i zstd-safe
# root package: zstd-safe v8.2.0 (or a compatible later fixed version)
```

Workspace `cargo fmt --check` and clippy operate on the declared workspace
members; the vendored packages stay excluded.

For an offline source-integrity check from the repository root, run
`sh scripts/check-vendor-integrity.sh`. `third_party/SHA256SUMS` covers every
file under the three vendored package directories, including their approved
modified manifests. The script checks both file-list completeness and every
hash with `sha256sum` or `shasum`; it does not fetch files. Wire this command
into the security CI job.

Remove these vendor patches when released compatible Arrow IPC, Parquet, and
Foyer packages no longer require the vulnerable `zstd 0.13` line and the
updated locked graph passes cargo-deny. Until then, keep the packages on their
published versions and retain this provenance record.

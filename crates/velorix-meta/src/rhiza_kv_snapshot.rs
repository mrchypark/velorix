//! Bounded immutable-page snapshot protocol over Rhiza KV.
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const ROOT_KEY: &str = "velorix/snapshot/root";
// Each page is written as exactly one Rhiza KV value, and the backend
// base64-encodes that value (~+33%) before transport. rhizadb 0.19.0 rejects
// any replicated mutation whose encoded command exceeds
// `MaxReplicatedValueBytes = 128 << 10`, defined at
// native/pkg/quepaxa/types.go:19 and enforced in native/pkg/network/server.go
// and native/pkg/quepaxa/core.go. A 64 KiB page encodes to ~86 KiB, which
// leaves room for the key, the CAS `expected` value, and request metadata
// inside that 128 KiB budget.
const PAGE_BYTES: usize = 64 << 10;
// 128 pages at 64 KiB each. Every page digest serializes as 67 bytes of JSON
// (`"<64 hex chars>"` plus a comma), so the root manifest stays far below the
// 16 KiB root cap enforced by `parse_manifest`.
const MAX_SNAPSHOT_BYTES: usize = 8 << 20;

// Compile-time guard: the manifest of a maximum-size snapshot must fit the
// 16 KiB root cap checked in `parse_manifest`.
const _: () = {
    const PAGES: usize = MAX_SNAPSHOT_BYTES / PAGE_BYTES;
    const DIGEST_JSON_BYTES: usize = 64 + 3;
    const FIXED_MANIFEST_BYTES: usize = 512;
    assert!(PAGES * DIGEST_JSON_BYTES + FIXED_MANIFEST_BYTES <= 16 * 1024);
};

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("KV: {0}")]
    Kv(#[from] super::rhiza_kv::RhizaKvError),
    #[error("invalid snapshot: {0}")]
    Invalid(String),
    #[error("snapshot exceeds limit")]
    TooLarge,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootToken(Vec<u8>);
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompareExchange {
    Applied(RootToken),
    Conflict,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    generation: u64,
    pages: Vec<String>,
    len: usize,
    content_digest: String,
}
fn parse_manifest(root: &[u8]) -> Result<Manifest, SnapshotError> {
    if root.len() > 16 * 1024 {
        return Err(SnapshotError::TooLarge);
    }
    let m: Manifest =
        serde_json::from_slice(root).map_err(|e| SnapshotError::Invalid(e.to_string()))?;
    if m.schema_version != 1
        || m.generation == 0
        || m.len > MAX_SNAPSHOT_BYTES
        || m.pages.len() != m.len.div_ceil(PAGE_BYTES)
    {
        return Err(SnapshotError::Invalid(
            "manifest bounds/version/pages".into(),
        ));
    }
    if m.pages.iter().any(|p| {
        p.len() != 64
            || p.bytes()
                .any(|b| !b.is_ascii_hexdigit() || b.is_ascii_uppercase())
    }) {
        return Err(SnapshotError::Invalid("invalid page digest".into()));
    }
    Ok(m)
}

#[derive(Clone)]
pub struct RhizaKvSnapshot {
    kv: super::rhiza_kv::RhizaKvStore,
}
impl RhizaKvSnapshot {
    pub fn new(kv: super::rhiza_kv::RhizaKvStore) -> Self {
        Self { kv }
    }
    /// The current root token, read with exactly one KV get.
    pub async fn root(&self) -> Result<Option<RootToken>, SnapshotError> {
        Ok(self.kv.get(ROOT_KEY).await?.map(RootToken))
    }
    pub async fn load(&self) -> Result<Option<(RootToken, Vec<u8>)>, SnapshotError> {
        let root = self.kv.get(ROOT_KEY).await?;
        let Some(root) = root else { return Ok(None) };
        let manifest = parse_manifest(&root)?;
        if manifest.schema_version != 1 || manifest.len > MAX_SNAPSHOT_BYTES {
            return Err(SnapshotError::Invalid("manifest bounds/version".into()));
        }
        if manifest.pages.len() > MAX_SNAPSHOT_BYTES / PAGE_BYTES + 1 {
            return Err(SnapshotError::TooLarge);
        }
        let mut bytes = Vec::with_capacity(manifest.len);
        for hash in &manifest.pages {
            let page = self
                .kv
                .get(format!("velorix/snapshot/page/{hash}"))
                .await?
                .ok_or_else(|| SnapshotError::Invalid("page missing".into()))?;
            if digest(&page) != *hash {
                return Err(SnapshotError::Invalid("page digest mismatch".into()));
            }
            if page.len() > PAGE_BYTES
                || bytes
                    .len()
                    .checked_add(page.len())
                    .ok_or(SnapshotError::TooLarge)?
                    > MAX_SNAPSHOT_BYTES
            {
                return Err(SnapshotError::TooLarge);
            }
            bytes.extend(page);
        }
        if bytes.len() != manifest.len || digest(&bytes) != manifest.content_digest {
            return Err(SnapshotError::Invalid(
                "snapshot length or digest mismatch".into(),
            ));
        }
        Ok(Some((RootToken(root), bytes)))
    }
    pub async fn compare_exchange(
        &self,
        expected: Option<RootToken>,
        bytes: Vec<u8>,
        request_id: impl Into<String> + Send + 'static,
    ) -> Result<CompareExchange, SnapshotError> {
        let request_id = request_id.into();
        if request_id.is_empty() || request_id.len() > 64 {
            return Err(SnapshotError::Invalid(
                "request ID must be 1..64 bytes".into(),
            ));
        }
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(SnapshotError::TooLarge);
        }
        let current = expected.as_ref().map(|t| t.0.clone());
        let expected_manifest = current.as_deref().map(parse_manifest).transpose()?;
        let mut known_pages = expected_manifest
            .as_ref()
            .map(|manifest| manifest.pages.iter().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let mut pages = Vec::new();
        for chunk in bytes.chunks(PAGE_BYTES) {
            let hash = digest(chunk);
            if !known_pages.contains(&hash) {
                let page_request_id = Sha256::digest(format!("page:{request_id}:{hash}"))
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                self.kv
                    .put_if_absent(
                        page_request_id,
                        format!("velorix/snapshot/page/{hash}"),
                        chunk.to_vec(),
                    )
                    .await?;
                if self
                    .kv
                    .get(format!("velorix/snapshot/page/{hash}"))
                    .await?
                    .as_deref()
                    != Some(chunk)
                {
                    return Err(SnapshotError::Invalid("immutable page mismatch".into()));
                }
                known_pages.insert(hash.clone());
            }
            pages.push(hash);
        }
        let manifest = serde_json::to_vec(&Manifest {
            schema_version: 1,
            generation: expected_manifest
                .as_ref()
                .map(|manifest| {
                    manifest
                        .generation
                        .checked_add(1)
                        .ok_or(SnapshotError::Invalid("generation overflow".into()))
                })
                .transpose()?
                .unwrap_or(1),
            pages,
            len: bytes.len(),
            content_digest: digest(&bytes),
        })
        .map_err(|e| SnapshotError::Invalid(e.to_string()))?;
        let applied = self
            .kv
            .compare_and_set(request_id, ROOT_KEY, current, manifest.clone())
            .await?;
        let new_token = RootToken(manifest.clone());
        Ok(if applied {
            CompareExchange::Applied(new_token)
        } else {
            CompareExchange::Conflict
        })
    }

    /// The bound recovery address, if the underlying store started one.
    pub fn recovery_address(&self) -> Option<&str> {
        self.kv.recovery_address()
    }

    /// Native local readiness. See [`super::rhiza_kv::RhizaKvStore::ready`].
    pub async fn ready(&self) -> Result<bool, SnapshotError> {
        self.kv.ready().await.map_err(SnapshotError::Kv)
    }

    /// Close the native node owned by this snapshot facade. Closing also closes
    /// the recovery listener, which native ties to the same handle.
    pub async fn close(self) -> Result<(), SnapshotError> {
        self.kv.close().await.map_err(SnapshotError::Kv)
    }
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rhiza_kv::MAX_REPLICATED_MUTATION_BYTES;

    /// Mirrors `rhiza_kv::base64_len`: base64-encoded length of `bytes` input
    /// bytes, rounded up to a 4-byte boundary.
    fn encoded_len(bytes: usize) -> usize {
        bytes.div_ceil(3) * 4
    }

    #[test]
    fn snapshot_larger_than_one_page_splits_into_bounded_pages() {
        let bytes = vec![0x5au8; PAGE_BYTES * 2 + 17];
        let pages: Vec<&[u8]> = bytes.chunks(PAGE_BYTES).collect();
        assert_eq!(pages.len(), 3);
        assert_eq!(pages.len(), bytes.len().div_ceil(PAGE_BYTES));
        assert!(pages.iter().all(|page| page.len() <= PAGE_BYTES));
        assert!(pages.iter().any(|page| page.len() == PAGE_BYTES));
        assert_eq!(
            pages.iter().map(|page| page.len()).sum::<usize>(),
            bytes.len()
        );
    }

    #[test]
    fn maximal_page_stays_within_the_replicated_mutation_bound() {
        // Worst case key width: `velorix/snapshot/page/` plus 64 hex digests.
        let key_bytes = "velorix/snapshot/page/".len() + 64;
        let estimate = encoded_len(PAGE_BYTES) + encoded_len(0) + key_bytes + 1024;
        assert!(
            estimate <= MAX_REPLICATED_MUTATION_BYTES,
            "{estimate} > {MAX_REPLICATED_MUTATION_BYTES}"
        );
        assert_eq!(encoded_len(PAGE_BYTES), 87384);
    }

    #[test]
    fn maximum_snapshot_manifest_fits_the_root_cap() {
        let pages = MAX_SNAPSHOT_BYTES / PAGE_BYTES;
        assert_eq!(pages, 128);
        assert!(pages * (64 + 3) + 512 <= 16 * 1024);
    }

    #[tokio::test]
    async fn root_reads_only_the_root_key() {
        let directory = tempfile::tempdir().unwrap();
        let kv = crate::rhiza_kv::RhizaKvStore::open_config(
            rhizadb::Config::new(directory.path())
                .node_id("snapshot-root")
                .local(),
        )
        .await
        .unwrap();
        let snapshot = RhizaKvSnapshot::new(kv);
        assert_eq!(snapshot.root().await.unwrap(), None);
        let bytes = b"snapshot".to_vec();
        let CompareExchange::Applied(root) = snapshot
            .compare_exchange(None, bytes.clone(), "root-test")
            .await
            .unwrap()
        else {
            panic!("first publish must apply");
        };
        assert_eq!(snapshot.root().await.unwrap(), Some(root.clone()));
        assert_eq!(snapshot.load().await.unwrap(), Some((root, bytes)));
    }
}

//! Bounded immutable-page snapshot protocol over Rhiza KV.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const ROOT_KEY: &str = "velorix/snapshot/root";
const PAGE_BYTES: usize = 1 << 20;
const MAX_SNAPSHOT_BYTES: usize = 16 << 20;

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
        if let Some(root) = current.as_deref() {
            let _ = parse_manifest(root)?;
        }
        let mut pages = Vec::new();
        for chunk in bytes.chunks(PAGE_BYTES) {
            let hash = digest(chunk);
            let page_request_id =
                format!("{:x}", Sha256::digest(format!("page:{request_id}:{hash}")));
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
            pages.push(hash);
        }
        let manifest = serde_json::to_vec(&Manifest {
            schema_version: 1,
            generation: current
                .as_ref()
                .map(|r| {
                    serde_json::from_slice::<Manifest>(r)
                        .map_err(|e| SnapshotError::Invalid(e.to_string()))
                })
                .map(|m| {
                    m?.generation
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
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

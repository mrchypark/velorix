use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, memory::InMemory, path::Path, CopyOptions, GetOptions, GetResult,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as ObjectStoreResult,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use velorix_storage::{
    log::{IngestBatch, IngestLog, IngestStagingCleanupPolicy, IngestStagingWriteOutcome},
    object_key::ObjectKey,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
    (temp_dir, Arc::new(store))
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[derive(Debug)]
struct ConditionalCreateUnsupportedStore {
    inner: Arc<dyn ObjectStore>,
    not_implemented: bool,
}

impl std::fmt::Display for ConditionalCreateUnsupportedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("conditional-create-unsupported")
    }
}

#[async_trait]
impl ObjectStore for ConditionalCreateUnsupportedStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if matches!(opts.mode, PutMode::Create) {
            if self.not_implemented {
                return Err(object_store::Error::NotImplemented {
                    operation: "conditional put".to_string(),
                    implementer: "test-store".to_string(),
                });
            }
            return Err(object_store::Error::NotSupported {
                source: "conditional create is disabled".into(),
            });
        }
        unreachable!("test store only supports the staging create path")
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[tokio::test]
async fn staging_write_is_create_only_idempotent_and_digest_bound() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));
    let key = ObjectKey::ingest_staging("orders", 0, 0, 10, "attempt-a").unwrap();
    let payload = Bytes::from_static(b"staged-payload");
    let payload_digest = digest(&payload);

    assert!(matches!(
        log.stage_write(&key, payload.clone(), &payload_digest)
            .await
            .unwrap(),
        IngestStagingWriteOutcome::Created(_)
    ));
    assert!(matches!(
        log.stage_write(&key, payload.clone(), &payload_digest)
            .await
            .unwrap(),
        IngestStagingWriteOutcome::Duplicate(_)
    ));

    let different = Bytes::from_static(b"different-payload");
    let conflict = log
        .stage_write(&key, different.clone(), digest(&different))
        .await
        .unwrap();
    assert!(matches!(
        conflict,
        IngestStagingWriteOutcome::Conflict {
            existing_digest,
            requested_digest,
            ..
        } if existing_digest == payload_digest && requested_digest == digest(&different)
    ));

    assert_eq!(
        log.read_staging(&key, &payload_digest).await.unwrap(),
        payload
    );
    assert!(log.read_staging(&key, digest(&different)).await.is_err());
    let missing = ObjectKey::ingest_staging("orders", 0, 10, 20, "missing").unwrap();
    assert!(log.read_staging(&missing, &payload_digest).await.is_err());
}

#[tokio::test]
async fn staging_is_invisible_to_committed_listing_and_replay() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));
    let staged_key = ObjectKey::ingest_staging("orders", 0, 0, 10, "attempt-a").unwrap();
    let staged = Bytes::from_static(b"staged-only");
    log.stage_write(&staged_key, staged.clone(), digest(&staged))
        .await
        .unwrap();

    let committed =
        IngestBatch::new_bootstrap_unchecked("orders", 0, 10, 20, Bytes::from_static(b"committed"))
            .unwrap();
    log.append(&committed).await.unwrap();

    let listed = log.list_committed().await.unwrap();
    assert_eq!(listed, vec![committed.descriptor()]);
    assert_eq!(log.replay_from(&[]).await.unwrap(), vec![committed]);
}

#[tokio::test]
async fn staging_candidates_are_age_and_limit_bounded_and_metadata_only() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));
    let mut keys = Vec::new();
    for id in ["a", "b", "c"] {
        let key = ObjectKey::ingest_staging("orders", 0, 0, 10, id).unwrap();
        let payload = Bytes::from(format!("payload-{id}"));
        log.stage_write(&key, payload.clone(), digest(&payload))
            .await
            .unwrap();
        keys.push(key);
    }

    let policy = IngestStagingCleanupPolicy::new(std::time::Duration::ZERO, 2);
    let candidates = log.list_ingest_staging_candidates(policy).await.unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.object_key.clone())
            .collect::<Vec<_>>(),
        keys[..2].to_vec()
    );

    assert!(candidates.iter().all(|candidate| candidate.size > 0));
    assert!(candidates
        .iter()
        .all(|candidate| candidate.last_modified_unix_nanos > 0));
}

async fn assert_concurrent_create_is_idempotent(store: Arc<dyn ObjectStore>) {
    let log = IngestLog::new(store);
    let key = ObjectKey::ingest_staging("orders", 0, 0, 10, "concurrent").unwrap();
    let payload = Bytes::from_static(b"concurrent-payload");
    let payload_digest = digest(&payload);
    let first = log.stage_write(&key, payload.clone(), &payload_digest);
    let second = log.stage_write(&key, payload, &payload_digest);
    let (first, second) = tokio::join!(first, second);
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, IngestStagingWriteOutcome::Created(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, IngestStagingWriteOutcome::Duplicate(_)))
            .count(),
        1
    );
}

#[tokio::test]
async fn staging_concurrent_create_has_memory_and_local_parity() {
    assert_concurrent_create_is_idempotent(Arc::new(InMemory::new())).await;
    let (_temp_dir, local) = temp_store();
    assert_concurrent_create_is_idempotent(local).await;
}

#[tokio::test]
async fn staging_fails_closed_when_conditional_create_is_unsupported() {
    for not_implemented in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(ConditionalCreateUnsupportedStore {
            inner: Arc::new(InMemory::new()),
            not_implemented,
        });
        let log = IngestLog::new(store);
        let key = ObjectKey::ingest_staging("orders", 0, 0, 1, "unsupported").unwrap();
        let payload = Bytes::from_static(b"payload");
        let err = log
            .stage_write(&key, payload.clone(), digest(&payload))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            velorix_storage::log::IngestLogError::IngestStagingConditionalCreateUnsupported {
                object_key,
                ..
            } if object_key == key
        ));
    }
}

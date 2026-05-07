use std::sync::Arc;

use bytes::Bytes;
use futures::{stream, StreamExt};
use object_store::{
    local::LocalFileSystem, path::Path, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::Barrier;
use velorix_storage::{
    checkpoint_index::{
        CheckpointLifecycleRecord, CheckpointLifecycleStatus, CheckpointManifestInspectionStatus,
        CheckpointRetentionRecordV1, LatestCandidateMarker,
    },
    gc::{
        GarbageCollectionCandidate, GarbageCollectionCandidateKind, GarbageCollectionPlan,
        GarbageCollectionPolicy, GarbageCollectionReport, GarbageCollectionRunV1,
    },
    manifest::{
        CheckpointManifest, InputRange, ManifestError, OutputObjectRef, PartitionOwnerClaim,
        SlateDbCheckpointRefV1, StateObjectRef, StateRefType,
    },
    object_key::ObjectKey,
    ownership::OwnershipEpochRecord,
    state::{
        CheckpointPublishError, CheckpointPublisher, FencedOutputObjectWriteRequest,
        OutputObjectWrite, StateObjectWrite,
    },
    state_store::{RawObjectStateStore, SlateDbStateStore, StateObjectStore},
};

#[derive(Debug)]
struct FullListingFailsStore {
    inner: Arc<dyn ObjectStore>,
    failing_get_key: Option<&'static str>,
}

#[derive(Debug)]
struct PrefixListingFailsStore {
    inner: Arc<dyn ObjectStore>,
    failing_prefix: &'static str,
    fail_offset_listing: bool,
}

#[derive(Debug)]
struct ReadFailsStore {
    inner: Arc<dyn ObjectStore>,
    failing_key: &'static str,
}

impl FullListingFailsStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            failing_get_key: None,
        }
    }

    fn new_with_read_failure(inner: Arc<dyn ObjectStore>, failing_get_key: &'static str) -> Self {
        Self {
            inner,
            failing_get_key: Some(failing_get_key),
        }
    }
}

impl PrefixListingFailsStore {
    fn new(inner: Arc<dyn ObjectStore>, failing_prefix: &'static str) -> Self {
        Self {
            inner,
            failing_prefix,
            fail_offset_listing: false,
        }
    }

    fn new_with_offset_failure(inner: Arc<dyn ObjectStore>, failing_prefix: &'static str) -> Self {
        Self {
            inner,
            failing_prefix,
            fail_offset_listing: true,
        }
    }
}

impl ReadFailsStore {
    fn new(inner: Arc<dyn ObjectStore>, failing_key: &'static str) -> Self {
        Self { inner, failing_key }
    }
}

fn prefix_matches(prefix: Option<&Path>, expected: &str) -> bool {
    prefix
        .map(|prefix| prefix.as_ref() == expected)
        .unwrap_or(false)
}

fn generic_store_error(store: &'static str, message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn failing_stream(
    store: &'static str,
    message: impl Into<String>,
) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
    let message = message.into();
    stream::once(async move { Err(generic_store_error(store, message)) }).boxed()
}

impl std::fmt::Display for FullListingFailsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "full-listing-fails({})", self.inner)
    }
}

impl std::fmt::Display for PrefixListingFailsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "prefix-listing-fails({}, {})",
            self.failing_prefix, self.inner
        )
    }
}

impl std::fmt::Display for ReadFailsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "read-fails({}, {})", self.failing_key, self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FullListingFailsStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self
            .failing_get_key
            .is_some_and(|failing_key| location.as_ref() == failing_key)
        {
            return Err(generic_store_error(
                "full-listing-fails",
                format!("read failed for {location}"),
            ));
        }

        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        failing_stream(
            "full-listing-fails",
            "latest_manifest should use the marker fast path",
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait::async_trait]
impl ObjectStore for PrefixListingFailsStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        if prefix_matches(prefix, self.failing_prefix) {
            let failing_prefix = self.failing_prefix;
            return failing_stream(
                "prefix-listing-fails",
                format!("listing failed for {failing_prefix}/"),
            );
        }

        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.fail_offset_listing && prefix_matches(prefix, self.failing_prefix) {
            let failing_prefix = self.failing_prefix;
            return failing_stream(
                "prefix-listing-fails",
                format!("offset listing failed for {failing_prefix}/ after {offset}"),
            );
        }

        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReadFailsStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if location.as_ref() == self.failing_key {
            return Err(generic_store_error(
                "read-fails",
                format!("read failed for {location}"),
            ));
        }

        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

async fn object_exists(store: &dyn ObjectStore, object_key: &ObjectKey) -> bool {
    store.head(&Path::from(object_key.as_str())).await.is_ok()
}

fn garbage_collection_run(run_id: &str, schema_version: u16) -> GarbageCollectionRunV1 {
    GarbageCollectionRunV1 {
        schema_version,
        run_id: run_id.to_string(),
        policy: GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        },
        plan: GarbageCollectionPlan {
            retained_manifest_versions: vec![0],
            candidates: Vec::new(),
        },
        report: GarbageCollectionReport {
            deleted: Vec::new(),
            skipped: Vec::new(),
        },
    }
}

fn retention_record_for_test(
    manifest: &CheckpointManifest,
    deleted_candidate_keys: Vec<ObjectKey>,
) -> CheckpointRetentionRecordV1 {
    CheckpointRetentionRecordV1::for_manifest(
        manifest,
        &serde_json::to_vec(manifest).unwrap(),
        "run-0001".to_string(),
        GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        },
        vec![manifest.checkpoint_version + 1],
        deleted_candidate_keys,
        "unix:0.000000001".to_string(),
    )
}

fn manifest_digest_for_test(manifest: &CheckpointManifest) -> String {
    retention_record_for_test(manifest, Vec::new()).manifest_digest
}

fn input_range() -> InputRange {
    input_range_for("orders", 0, 0, 10)
}

fn input_range_for(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> InputRange {
    InputRange {
        stream_id: stream_id.to_string(),
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
    }
}

fn state_write(checkpoint_version: u64, object_id: &str, bytes: &'static [u8]) -> StateObjectWrite {
    state_write_for_partition(0, checkpoint_version, object_id, bytes)
}

fn state_write_for_partition(
    partition_id: u32,
    checkpoint_version: u64,
    object_id: &str,
    bytes: &'static [u8],
) -> StateObjectWrite {
    StateObjectWrite::new(
        "balances_by_account",
        partition_id,
        checkpoint_version,
        object_id,
        Bytes::from_static(bytes),
    )
    .unwrap()
}

fn state_write_bytes(checkpoint_version: u64, object_id: &str, bytes: Bytes) -> StateObjectWrite {
    StateObjectWrite::new(
        "balances_by_account",
        0,
        checkpoint_version,
        object_id,
        bytes,
    )
    .unwrap()
}

fn fenced_state_write(
    checkpoint_version: u64,
    object_id: &str,
    owner_claim: PartitionOwnerClaim,
    bytes: &'static [u8],
) -> StateObjectWrite {
    StateObjectWrite::new_fenced(
        "balances_by_account",
        0,
        checkpoint_version,
        object_id,
        owner_claim,
        Bytes::from_static(bytes),
    )
    .unwrap()
}

fn fenced_state_write_for_partition(
    partition_id: u32,
    checkpoint_version: u64,
    object_id: &str,
    owner_claim: PartitionOwnerClaim,
    bytes: &'static [u8],
) -> StateObjectWrite {
    StateObjectWrite::new_fenced(
        "balances_by_account",
        partition_id,
        checkpoint_version,
        object_id,
        owner_claim,
        Bytes::from_static(bytes),
    )
    .unwrap()
}

fn owner_claim(owner_id: &str, owner_epoch: u64) -> PartitionOwnerClaim {
    PartitionOwnerClaim {
        owner_id: owner_id.to_string(),
        owner_epoch,
    }
}

fn ownership_record(
    stream_id: &str,
    partition_id: u32,
    owner_id: &str,
    owner_epoch: u64,
) -> OwnershipEpochRecord {
    OwnershipEpochRecord {
        stream_id: stream_id.to_string(),
        partition_id,
        owner_id: owner_id.to_string(),
        owner_epoch,
        lease_identity: format!("{owner_id}-lease"),
        created_at: "2026-05-03T00:00:00Z".to_string(),
        previous_epoch: owner_epoch.checked_sub(1),
        previous_checkpoint_version: owner_epoch.checked_sub(1),
    }
}

fn output_ref_for_partition(partition_id: u32) -> OutputObjectRef {
    OutputObjectRef {
        object_id: format!("out-p{partition_id}"),
        object_key: ObjectKey::output_object(
            "settlements",
            partition_id,
            0,
            20,
            25,
            &format!("out-p{partition_id}"),
        )
        .unwrap(),
        stream_id: "settlements".to_string(),
        partition_id,
        checkpoint_version: 0,
        start_offset_inclusive: 20,
        end_offset_exclusive: 25,
        owner_claim: None,
    }
}

fn output_write(
    partition_id: u32,
    checkpoint_version: u64,
    object_id: &str,
    bytes: &'static [u8],
) -> OutputObjectWrite {
    OutputObjectWrite::new(
        "settlements",
        partition_id,
        checkpoint_version,
        20,
        25,
        object_id,
        Bytes::from_static(bytes),
    )
    .unwrap()
}

fn fenced_output_write(
    partition_id: u32,
    checkpoint_version: u64,
    object_id: &str,
    owner_claim: PartitionOwnerClaim,
    bytes: &'static [u8],
) -> OutputObjectWrite {
    OutputObjectWrite::new_fenced(FencedOutputObjectWriteRequest {
        stream_id: "settlements".to_string(),
        partition_id,
        checkpoint_version,
        start_offset_inclusive: 20,
        end_offset_exclusive: 25,
        object_id: object_id.to_string(),
        owner_claim,
        bytes: Bytes::from_static(bytes),
    })
    .unwrap()
}

fn state_ref(state: &StateObjectWrite) -> StateObjectRef {
    StateObjectRef {
        object_id: state.object_id().to_string(),
        object_key: state.object_key().clone(),
        owner: state.owner().to_string(),
        partition_id: state.partition_id(),
        checkpoint_version: state.checkpoint_version(),
        ref_type: StateRefType::RawObject,
        slatedb: None,
        owner_claim: state.owner_claim().cloned(),
    }
}

fn slatedb_state_ref(state: &StateObjectWrite) -> StateObjectRef {
    let mut state_ref = state_ref(state);
    state_ref.ref_type = StateRefType::SlateDbCheckpoint;
    state_ref.slatedb = Some(SlateDbCheckpointRefV1 {
        db_path: "v1/slatedb/state".to_string(),
        state_key: state.object_key().as_str().to_string(),
        state_digest: state_digest(state.bytes()),
        state_bytes: state.bytes().len() as u64,
        created_by_checkpoint_version: state.checkpoint_version(),
    });
    state_ref
}

fn state_digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn manifest(checkpoint_version: u64, state_ref: StateObjectRef) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version,
        input_ranges: vec![input_range()],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: checkpoint_version.checked_sub(1),
        created_at: "2026-05-03T00:00:00Z".to_string(),
    }
}

#[tokio::test]
async fn checkpoint_publish_makes_valid_manifest_visible_after_state_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"state-bytes");

    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = manifest(0, state_ref.clone());

    publisher.publish_manifest(&manifest).await.unwrap();

    assert_eq!(
        publisher.read_state_object(&state_ref).await.unwrap(),
        state.bytes().clone()
    );
    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn checkpoint_publish_crash_before_manifest_publication_leaves_no_visible_checkpoint() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);

    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert_eq!(
        publisher.list_published_manifests().await.unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn checkpoint_publish_orphan_state_object_does_not_advance_checkpoint_visibility() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"orphan-state");

    let state_ref = publisher.write_state_object(&state).await.unwrap();

    assert_eq!(
        publisher.read_state_object(&state_ref).await.unwrap(),
        Bytes::from_static(b"orphan-state")
    );
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert_eq!(
        publisher.list_published_manifests().await.unwrap(),
        Vec::new()
    );

    let state_path = Path::from(state_ref.object_key.as_str());
    assert!(store.head(&state_path).await.is_ok());
}

#[tokio::test]
async fn gc_plan_marks_orphan_raw_state_and_retains_manifest_referenced_objects() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");
    let retained_output = output_write(0, 0, "out-retained", b"retained-output");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    let retained_output_ref = publisher
        .write_output_object(&retained_output)
        .await
        .unwrap();
    let mut manifest = manifest(0, retained_state_ref);
    manifest.output_objects = vec![retained_output_ref];
    publisher.publish_manifest(&manifest).await.unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();

    assert_eq!(plan.retained_manifest_versions, vec![0]);
    assert_eq!(
        plan.candidates,
        vec![GarbageCollectionCandidate {
            object_key: orphan_state.object_key().clone(),
            kind: GarbageCollectionCandidateKind::RawStateObject,
        }]
    );
    assert!(object_exists(store.as_ref(), retained_state.object_key()).await);
    assert!(object_exists(store.as_ref(), retained_output.object_key()).await);
}

#[tokio::test]
async fn gc_plan_fails_closed_when_state_listing_fails() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let listing_fails_store =
        Arc::new(PrefixListingFailsStore::new(Arc::clone(&store), "v1/state"));
    let listing_fails_publisher = CheckpointPublisher::new(listing_fails_store);
    let err = listing_fails_publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap_err();

    let err = err.to_string();
    assert!(err.contains("prefix-listing-fails"));
    assert!(err.contains("listing failed for v1/state/"));
    assert!(object_exists(store.as_ref(), retained_state.object_key()).await);
    assert!(object_exists(store.as_ref(), orphan_state.object_key()).await);
}

#[tokio::test]
async fn gc_plan_fails_closed_when_output_listing_fails() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let retained_output = output_write(0, 0, "out-retained", b"retained-output");
    let orphan_output = output_write(0, 0, "out-orphan", b"orphan-output");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    let retained_output_ref = publisher
        .write_output_object(&retained_output)
        .await
        .unwrap();
    publisher.write_output_object(&orphan_output).await.unwrap();
    let mut manifest = manifest(0, retained_state_ref);
    manifest.output_objects = vec![retained_output_ref];
    publisher.publish_manifest(&manifest).await.unwrap();

    let listing_fails_store = Arc::new(PrefixListingFailsStore::new(
        Arc::clone(&store),
        "v1/outputs",
    ));
    let listing_fails_publisher = CheckpointPublisher::new(listing_fails_store);
    let err = listing_fails_publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap_err();

    let err = err.to_string();
    assert!(err.contains("prefix-listing-fails"));
    assert!(err.contains("listing failed for v1/outputs/"));
    assert!(object_exists(store.as_ref(), retained_output.object_key()).await);
    assert!(object_exists(store.as_ref(), orphan_output.object_key()).await);
}

#[tokio::test]
async fn gc_plan_marks_unreferenced_output_object() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_output = output_write(0, 0, "out-orphan", b"orphan-output");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_output_object(&orphan_output).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();

    assert_eq!(
        plan.candidates,
        vec![GarbageCollectionCandidate {
            object_key: orphan_output.object_key().clone(),
            kind: GarbageCollectionCandidateKind::OutputObject,
        }]
    );
}

#[tokio::test]
async fn gc_plan_rejects_zero_retained_manifests() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);

    let err = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 0,
        })
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidGarbageCollectionPolicy
    ));
}

#[tokio::test]
async fn gc_execution_deletes_only_velorix_owned_candidates() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");
    let orphan_output = output_write(0, 0, "out-orphan", b"orphan-output");
    let slatedb_internal_path = Path::from("v1/slatedb/000000.sst");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    publisher.write_output_object(&orphan_output).await.unwrap();
    store
        .put(
            &slatedb_internal_path,
            Bytes::from_static(b"slatedb").into(),
        )
        .await
        .unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();
    let report = publisher
        .execute_garbage_collection_plan(&plan)
        .await
        .unwrap();

    assert_eq!(report.deleted, plan.candidates);
    assert!(!object_exists(store.as_ref(), orphan_state.object_key()).await);
    assert!(!object_exists(store.as_ref(), orphan_output.object_key()).await);
    assert!(object_exists(store.as_ref(), retained_state.object_key()).await);
    assert!(store.head(&slatedb_internal_path).await.is_ok());
}

#[tokio::test]
async fn gc_execution_writes_stable_run_evidence() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    let run = publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap();

    assert_eq!(run.schema_version, 1);
    assert_eq!(run.run_id, "run-0001");
    assert_eq!(run.policy, policy);
    assert_eq!(run.plan, plan);
    assert_eq!(run.report.deleted.len(), 1);
    assert_eq!(run.report.skipped, Vec::new());

    let evidence_key = ObjectKey::garbage_collection_run("run-0001").unwrap();
    let evidence_bytes = store
        .get(&Path::from(evidence_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let restored: GarbageCollectionRunV1 = serde_json::from_slice(&evidence_bytes).unwrap();
    assert_eq!(restored, run);

    let read_back = publisher
        .read_garbage_collection_run_evidence("run-0001")
        .await
        .unwrap();
    assert_eq!(read_back, run);
}

#[tokio::test]
async fn gc_execution_writes_retention_evidence_after_run_evidence_readback() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    let manifest_0 = manifest(0, state_ref_0);
    publisher.publish_manifest(&manifest_0).await.unwrap();
    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    let manifest_1 = manifest(1, state_ref_1);
    publisher.publish_manifest(&manifest_1).await.unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    let run = publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap();

    let record = publisher.read_checkpoint_retention_record(0).await.unwrap();
    assert_eq!(record.schema_version, 1);
    assert_eq!(record.checkpoint_version, 0);
    assert_eq!(record.manifest_key, manifest_0.object_key());
    assert_eq!(record.gc_run_id, "run-0001");
    assert_eq!(record.policy, policy);
    assert_eq!(record.retained_manifest_versions, vec![1]);
    assert_eq!(
        record.deleted_candidate_keys,
        vec![state_0.object_key().clone()]
    );
    assert_eq!(
        record.manifest_digest,
        manifest_digest_for_test(&manifest_0)
    );
    assert_eq!(
        run.report.deleted[0].object_key,
        state_0.object_key().clone()
    );
    assert!(matches!(
        publisher.read_checkpoint_retention_record(1).await,
        Err(CheckpointPublishError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

#[tokio::test]
async fn gc_execution_releases_manifest_retired_slatedb_state_refs() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, state_ref_0.clone()))
        .await
        .unwrap();
    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    publisher
        .publish_manifest(&manifest(1, state_ref_1.clone()))
        .await
        .unwrap();
    store
        .put(
            &Path::from(state_0.object_key().as_str()),
            Bytes::from_static(b"raw-shadow-with-same-key").into(),
        )
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    assert_eq!(
        plan.candidates,
        vec![GarbageCollectionCandidate {
            object_key: state_0.object_key().clone(),
            kind: GarbageCollectionCandidateKind::SlateDbStateRef,
        }]
    );

    let run = publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap();

    assert_eq!(run.report.deleted, plan.candidates);
    assert!(matches!(
        publisher.read_state_object(&state_ref_0).await,
        Err(CheckpointPublishError::MissingStateObject(object_key))
            if object_key == *state_0.object_key()
    ));
    assert_eq!(
        publisher.read_state_object(&state_ref_1).await.unwrap(),
        Bytes::from_static(b"state-1")
    );

    let record = publisher.read_checkpoint_retention_record(0).await.unwrap();
    assert_eq!(
        record.deleted_candidate_keys,
        vec![state_0.object_key().clone()]
    );
    assert_eq!(
        record.manifest_digest,
        manifest_digest_for_test(&manifest(0, state_ref_0))
    );
}

#[tokio::test]
async fn gc_plan_keeps_slatedb_state_ref_referenced_by_retained_manifest() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let state = state_write(0, "state-0001", b"state");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, state_ref.clone()))
        .await
        .unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();

    assert_eq!(plan.retained_manifest_versions, vec![0]);
    assert!(plan.candidates.is_empty());
    assert_eq!(
        publisher.read_state_object(&state_ref).await.unwrap(),
        Bytes::from_static(b"state")
    );
}

#[tokio::test]
async fn gc_execution_does_not_write_retention_evidence_when_run_evidence_fails() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::new(ReadFailsStore::new(
        Arc::clone(&store),
        "v1/gc-runs/run-0001.run.json",
    )));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, state_ref_0))
        .await
        .unwrap();
    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    publisher
        .publish_manifest(&manifest(1, state_ref_1))
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap_err();

    assert!(matches!(
        store
            .head(&Path::from(
                ObjectKey::checkpoint_retention_record(0).as_str()
            ))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn gc_execution_rejects_conflicting_retention_evidence_before_deleting_candidates() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    let manifest_0 = manifest(0, state_ref_0);
    publisher.publish_manifest(&manifest_0).await.unwrap();
    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    publisher
        .publish_manifest(&manifest(1, state_ref_1))
        .await
        .unwrap();
    let mut conflicting =
        retention_record_for_test(&manifest_0, vec![state_0.object_key().clone()]);
    conflicting.gc_run_id = "other-run".to_string();
    store
        .put(
            &Path::from(ObjectKey::checkpoint_retention_record(0).as_str()),
            Bytes::from(serde_json::to_vec(&conflicting).unwrap()).into(),
        )
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    let err = publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("different causal fields"));
    assert!(object_exists(store.as_ref(), state_0.object_key()).await);
    assert!(matches!(
        store
            .head(&Path::from(
                ObjectKey::garbage_collection_run("run-0001")
                    .unwrap()
                    .as_str()
            ))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn checkpoint_admin_inspect_reports_retention_evidence_for_gc_retired_manifest() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, state_ref_0))
        .await
        .unwrap();
    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    publisher
        .publish_manifest(&manifest(1, state_ref_1))
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap();

    let report = publisher.inspect_checkpoints().await.unwrap();
    let retired = &report.manifests[0];
    assert_eq!(retired.checkpoint_version, 0);
    assert_eq!(
        retired.retention_record.as_ref().unwrap().gc_run_id,
        "run-0001"
    );
    assert!(matches!(
        retired.status,
        CheckpointManifestInspectionStatus::Invalid { .. }
    ));
    assert!(report.manifests[1].retention_record.is_none());
    assert!(matches!(
        report.manifests[1].status,
        CheckpointManifestInspectionStatus::Valid
    ));
}

#[tokio::test]
async fn checkpoint_admin_inspect_ignores_retention_record_without_gc_run_evidence() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());
    publisher.publish_manifest(&manifest).await.unwrap();
    let record = retention_record_for_test(&manifest, vec![state.object_key().clone()]);
    store
        .put(
            &Path::from(ObjectKey::checkpoint_retention_record(0).as_str()),
            Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
        )
        .await
        .unwrap();

    let report = publisher.inspect_checkpoints().await.unwrap();

    assert!(report.manifests[0].retention_record.is_none());
}

#[tokio::test]
async fn checkpoint_admin_inspect_ignores_retention_record_with_incomplete_deleted_keys() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    let manifest_0 = manifest(0, state_ref_0);
    publisher.publish_manifest(&manifest_0).await.unwrap();
    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    publisher
        .publish_manifest(&manifest(1, state_ref_1))
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap();
    let truncated_record = CheckpointRetentionRecordV1::for_manifest(
        &manifest_0,
        &serde_json::to_vec(&manifest_0).unwrap(),
        "run-0001".to_string(),
        policy,
        vec![1],
        Vec::new(),
        "unix:0.000000001".to_string(),
    );
    store
        .put(
            &Path::from(ObjectKey::checkpoint_retention_record(0).as_str()),
            Bytes::from(serde_json::to_vec(&truncated_record).unwrap()).into(),
        )
        .await
        .unwrap();

    let report = publisher.inspect_checkpoints().await.unwrap();

    assert!(report.manifests[0].retention_record.is_none());
}

#[tokio::test]
async fn checkpoint_admin_inspect_ignores_retention_record_with_manifest_digest_mismatch() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());
    publisher.publish_manifest(&manifest).await.unwrap();
    let mut record = retention_record_for_test(&manifest, vec![state.object_key().clone()]);
    record.manifest_digest = "sha256:mismatched".to_string();
    store
        .put(
            &Path::from(ObjectKey::checkpoint_retention_record(0).as_str()),
            Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
        )
        .await
        .unwrap();

    let report = publisher.inspect_checkpoints().await.unwrap();

    assert!(report.manifests[0].retention_record.is_none());
}

#[tokio::test]
async fn checkpoint_retention_record_key_and_body_must_match() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());
    publisher.publish_manifest(&manifest).await.unwrap();
    let mut record = retention_record_for_test(&manifest, vec![state.object_key().clone()]);
    record.checkpoint_version = 1;
    store
        .put(
            &Path::from(ObjectKey::checkpoint_retention_record(0).as_str()),
            Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher
        .read_checkpoint_retention_record(0)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("does not match body checkpoint 1"));
}

#[tokio::test]
async fn gc_execution_rejects_duplicate_run_evidence_before_deleting_candidates() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let evidence_key = ObjectKey::garbage_collection_run("run-0001").unwrap();
    store
        .put(
            &Path::from(evidence_key.as_str()),
            Bytes::from_static(b"existing-run-evidence").into(),
        )
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    let err = publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::GarbageCollectionRunAlreadyExists(_)
    ));
    assert!(object_exists(store.as_ref(), orphan_state.object_key()).await);
}

#[tokio::test]
async fn gc_execution_rejects_policy_mismatched_plan_before_deleting_candidates() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let stale_policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let requested_policy = GarbageCollectionPolicy {
        retain_latest_manifests: 2,
    };
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, state_ref_0))
        .await
        .unwrap();
    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    publisher
        .publish_manifest(&manifest(1, state_ref_1))
        .await
        .unwrap();

    let stale_plan = publisher
        .plan_garbage_collection(stale_policy)
        .await
        .unwrap();
    let err = publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", requested_policy, &stale_plan)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::GarbageCollectionPlanPolicyMismatch {
            retain_latest_manifests: 2,
            ..
        }
    ));
    assert!(object_exists(store.as_ref(), state_0.object_key()).await);
    assert!(object_exists(store.as_ref(), state_1.object_key()).await);

    let evidence_key = ObjectKey::garbage_collection_run("run-0001").unwrap();
    assert!(matches!(
        store.head(&Path::from(evidence_key.as_str())).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn gc_execution_requires_readable_run_evidence_after_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::new(ReadFailsStore::new(
        Arc::clone(&store),
        "v1/gc-runs/run-0001.run.json",
    )));
    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let plan = publisher.plan_garbage_collection(policy).await.unwrap();
    let err = publisher
        .execute_garbage_collection_plan_with_evidence("run-0001", policy, &plan)
        .await
        .unwrap_err();

    assert!(matches!(&err, CheckpointPublishError::ObjectStore(_)));
    assert!(err
        .to_string()
        .contains("read failed for v1/gc-runs/run-0001.run.json"));
}

#[tokio::test]
async fn gc_read_run_evidence_rejects_mismatched_run_id() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let evidence_key = ObjectKey::garbage_collection_run("run-0001").unwrap();
    let run = garbage_collection_run("other-run", 1);
    store
        .put(
            &Path::from(evidence_key.as_str()),
            Bytes::from(serde_json::to_vec(&run).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher
        .read_garbage_collection_run_evidence("run-0001")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
            object_key,
            reason,
        } if object_key == evidence_key && reason.contains("run_id other-run")
    ));
}

#[tokio::test]
async fn gc_read_run_evidence_rejects_unsupported_schema_version() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let evidence_key = ObjectKey::garbage_collection_run("run-0001").unwrap();
    let run = garbage_collection_run("run-0001", 2);
    store
        .put(
            &Path::from(evidence_key.as_str()),
            Bytes::from(serde_json::to_vec(&run).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher
        .read_garbage_collection_run_evidence("run-0001")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
            object_key,
            reason,
        } if object_key == evidence_key && reason.contains("unsupported schema_version 2")
    ));
}

#[tokio::test]
async fn gc_read_run_evidence_rejects_zero_retention_policy() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let evidence_key = ObjectKey::garbage_collection_run("run-0001").unwrap();
    let mut run = garbage_collection_run("run-0001", 1);
    run.policy.retain_latest_manifests = 0;
    store
        .put(
            &Path::from(evidence_key.as_str()),
            Bytes::from(serde_json::to_vec(&run).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher
        .read_garbage_collection_run_evidence("run-0001")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
            object_key,
            reason,
        } if object_key == evidence_key && reason.contains("retain_latest_manifests")
    ));
}

#[tokio::test]
async fn gc_read_run_evidence_rejects_report_entries_not_in_plan() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let evidence_key = ObjectKey::garbage_collection_run("run-0001").unwrap();
    let mut run = garbage_collection_run("run-0001", 1);
    let candidate = GarbageCollectionCandidate {
        object_key: ObjectKey::state_object("orders", 0, 7, "unplanned").unwrap(),
        kind: GarbageCollectionCandidateKind::RawStateObject,
    };
    run.report.deleted.push(candidate);
    store
        .put(
            &Path::from(evidence_key.as_str()),
            Bytes::from(serde_json::to_vec(&run).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher
        .read_garbage_collection_run_evidence("run-0001")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
            object_key,
            reason,
        } if object_key == evidence_key && reason.contains("not in the recorded plan")
    ));
}

#[tokio::test]
async fn gc_ignores_publish_temp_attempt_objects() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");
    let temp_attempt = ObjectKey::temp_publish(0, "attempt-0001", "manifest").unwrap();

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    store
        .put(
            &Path::from(temp_attempt.as_str()),
            Bytes::from_static(b"staged-manifest").into(),
        )
        .await
        .unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();

    assert_eq!(
        plan.candidates,
        vec![GarbageCollectionCandidate {
            object_key: orphan_state.object_key().clone(),
            kind: GarbageCollectionCandidateKind::RawStateObject,
        }]
    );

    let unsafe_plan = GarbageCollectionPlan {
        retained_manifest_versions: plan.retained_manifest_versions,
        candidates: vec![GarbageCollectionCandidate {
            object_key: temp_attempt.clone(),
            kind: GarbageCollectionCandidateKind::RawStateObject,
        }],
    };
    let report = publisher
        .execute_garbage_collection_plan(&unsafe_plan)
        .await
        .unwrap();

    assert_eq!(report.deleted, Vec::new());
    assert!(object_exists(store.as_ref(), &temp_attempt).await);
}

#[tokio::test]
async fn gc_execution_rejects_caller_plan_that_targets_manifest_referenced_objects() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_state = state_write(0, "state-orphan", b"orphan-state");
    let retained_output = output_write(0, 0, "out-retained", b"retained-output");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_state_object(&orphan_state).await.unwrap();
    let retained_output_ref = publisher
        .write_output_object(&retained_output)
        .await
        .unwrap();
    let mut manifest = manifest(0, retained_state_ref);
    manifest.output_objects = vec![retained_output_ref];
    publisher.publish_manifest(&manifest).await.unwrap();

    let plan = GarbageCollectionPlan {
        retained_manifest_versions: vec![0],
        candidates: vec![
            GarbageCollectionCandidate {
                object_key: orphan_state.object_key().clone(),
                kind: GarbageCollectionCandidateKind::RawStateObject,
            },
            GarbageCollectionCandidate {
                object_key: retained_state.object_key().clone(),
                kind: GarbageCollectionCandidateKind::RawStateObject,
            },
            GarbageCollectionCandidate {
                object_key: retained_output.object_key().clone(),
                kind: GarbageCollectionCandidateKind::OutputObject,
            },
        ],
    };

    let err = publisher
        .execute_garbage_collection_plan(&plan)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::GarbageCollectionCandidateStillReferenced { .. }
    ));
    assert!(object_exists(store.as_ref(), orphan_state.object_key()).await);
    assert!(object_exists(store.as_ref(), retained_state.object_key()).await);
    assert!(object_exists(store.as_ref(), retained_output.object_key()).await);
}

#[tokio::test]
async fn gc_execution_rejects_stale_plan_when_new_manifest_references_candidate() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let stale_candidate_state = state_write(1, "state-later-retained", b"later-retained-state");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    let stale_candidate_state_ref = publisher
        .write_state_object(&stale_candidate_state)
        .await
        .unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();

    publisher
        .publish_manifest(&manifest(1, stale_candidate_state_ref))
        .await
        .unwrap();
    let err = publisher
        .execute_garbage_collection_plan(&plan)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::GarbageCollectionCandidateStillReferenced { .. }
    ));
    assert!(object_exists(store.as_ref(), stale_candidate_state.object_key()).await);
}

#[tokio::test]
async fn gc_execution_fails_closed_when_manifest_revalidation_listing_fails() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let retained_state = state_write(0, "state-retained", b"retained-state");
    let orphan_output = output_write(0, 0, "out-orphan", b"orphan-output");

    let retained_state_ref = publisher.write_state_object(&retained_state).await.unwrap();
    publisher.write_output_object(&orphan_output).await.unwrap();
    publisher
        .publish_manifest(&manifest(0, retained_state_ref))
        .await
        .unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();
    let listing_fails_store = Arc::new(PrefixListingFailsStore::new(
        Arc::clone(&store),
        "v1/checkpoints",
    ));
    let listing_fails_publisher = CheckpointPublisher::new(listing_fails_store);
    let err = listing_fails_publisher
        .execute_garbage_collection_plan(&plan)
        .await
        .unwrap_err();

    let err = err.to_string();
    assert!(err.contains("prefix-listing-fails"));
    assert!(err.contains("listing failed for v1/checkpoints/"));
    assert!(object_exists(store.as_ref(), orphan_output.object_key()).await);
}

#[tokio::test]
async fn checkpoint_publish_rejects_duplicate_manifest_publication() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = manifest(0, state_ref);

    publisher.publish_manifest(&manifest).await.unwrap();
    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(err.to_string().contains("already exists"));
}

#[tokio::test]
async fn checkpoint_publish_rejects_duplicate_state_object_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"first");

    publisher.write_state_object(&state).await.unwrap();
    let duplicate = state_write(0, "state-0001", b"second");
    let err = publisher.write_state_object(&duplicate).await.unwrap_err();

    assert!(err.to_string().contains("already exists"));
}

#[tokio::test]
async fn checkpoint_publish_rejects_invalid_manifest_before_writing() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let invalid_manifest = CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: Vec::new(),
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    };

    let err = publisher
        .publish_manifest(&invalid_manifest)
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("manifest must include at least one input range"));
    let manifest_path = Path::from(invalid_manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_manifest_body_that_does_not_match_object_key() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = manifest(0, state_ref);
    let wrong_key = ObjectKey::checkpoint_manifest(99);

    store
        .put(
            &Path::from(wrong_key.as_str()),
            Bytes::from(serde_json::to_vec(&manifest).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher.list_published_manifests().await.unwrap_err();

    assert!(err.to_string().contains("does not match manifest body"));
}

#[tokio::test]
async fn checkpoint_publish_listing_rejects_old_output_ref_without_checkpoint_version_as_manifest_validation_error(
) {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let manifest_key = ObjectKey::checkpoint_manifest(1);
    let old_manifest = serde_json::json!({
        "schema_version": 1,
        "checkpoint_version": 1,
        "input_ranges": [{
            "stream_id": "orders",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "end_offset_exclusive": 10
        }],
        "state_objects": [{
            "object_id": "state-0001",
            "object_key": "v1/state/balances_by_account/p=0000000000/chk=00000000000000000001/state-0001.state",
            "owner": "balances_by_account",
            "partition_id": 0,
            "checkpoint_version": 1
        }],
        "output_objects": [{
            "object_id": "out-0001",
            "object_key": "v1/ingest/settlements/p=0000000000/00000000000000000020-00000000000000000025.batch",
            "stream_id": "settlements",
            "partition_id": 0,
            "start_offset_inclusive": 20,
            "end_offset_exclusive": 25
        }],
        "parent_checkpoint": 0,
        "created_at": "2026-05-03T00:00:00Z"
    });

    store
        .put(
            &Path::from(manifest_key.as_str()),
            Bytes::from(serde_json::to_vec(&old_manifest).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher.list_published_manifests().await.unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::Manifest(ManifestError::OutputObjectKeyMismatch { .. })
    ));
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_uses_numerically_latest_valid_checkpoint() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");
    let manifest_0 = manifest(0, publisher.write_state_object(&state_0).await.unwrap());
    let manifest_1 = manifest(1, publisher.write_state_object(&state_1).await.unwrap());

    publisher.publish_manifest(&manifest_0).await.unwrap();
    publisher.publish_manifest(&manifest_1).await.unwrap();

    assert_eq!(
        publisher.list_published_manifests().await.unwrap(),
        vec![manifest_0, manifest_1.clone()]
    );
    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest_1));
}

#[tokio::test]
async fn checkpoint_publish_writes_lifecycle_status_after_manifest_publication() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();

    publisher.publish_manifest(&manifest).await.unwrap();

    let record = publisher.read_checkpoint_lifecycle_record(0).await.unwrap();

    assert_eq!(
        record,
        CheckpointLifecycleRecord::published(
            &manifest,
            &manifest_bytes,
            record.status_updated_at.clone()
        )
    );
    assert_eq!(record.status, CheckpointLifecycleStatus::Published);
}

#[tokio::test]
async fn checkpoint_read_published_manifest_rejects_absent_lifecycle_record() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());

    publisher.publish_manifest(&manifest).await.unwrap();
    store
        .delete(&Path::from(
            ObjectKey::checkpoint_lifecycle_record(0).as_str(),
        ))
        .await
        .unwrap();

    let err = publisher
        .read_published_checkpoint_manifest(0)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn checkpoint_read_published_manifest_rejects_lifecycle_digest_mismatch() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();

    publisher.publish_manifest(&manifest).await.unwrap();
    let mut mismatched_record = CheckpointLifecycleRecord::published(
        &manifest,
        &manifest_bytes,
        "2026-05-03T00:00:01Z".to_string(),
    );
    mismatched_record.manifest_digest = "sha256:mismatched".to_string();
    store
        .put(
            &Path::from(ObjectKey::checkpoint_lifecycle_record(0).as_str()),
            Bytes::from(serde_json::to_vec(&mismatched_record).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher
        .read_published_checkpoint_manifest(0)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("manifest digest"));
}

#[tokio::test]
async fn checkpoint_admin_inspect_reports_last_known_good_when_future_manifest_is_corrupt() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());

    publisher.publish_manifest(&manifest).await.unwrap();
    store
        .put(
            &Path::from(ObjectKey::checkpoint_manifest(1).as_str()),
            Bytes::from_static(b"{not valid json").into(),
        )
        .await
        .unwrap();

    let report = publisher.inspect_checkpoints().await.unwrap();

    assert_eq!(report.latest_valid_checkpoint, Some(0));
    assert_eq!(report.manifests.len(), 2);
    assert_eq!(report.manifests[0].checkpoint_version, 0);
    assert_eq!(
        report.manifests[0].lifecycle_status,
        Some(CheckpointLifecycleStatus::Published)
    );
    assert!(matches!(
        report.manifests[0].status,
        CheckpointManifestInspectionStatus::Valid
    ));
    assert_eq!(report.manifests[1].checkpoint_version, 1);
    assert!(matches!(
        report.manifests[1].status,
        CheckpointManifestInspectionStatus::Invalid { .. }
    ));
    assert!(!report.manifests[1].status.reason().unwrap().is_empty());
}

#[tokio::test]
async fn checkpoint_admin_inspect_treats_gc_retired_parent_payload_as_parent_lineage() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");
    let output_0 = output_write(0, 0, "out-0001", b"output-0");
    let output_1 = output_write(0, 1, "out-0002", b"output-1");

    let state_ref_0 = publisher.write_state_object(&state_0).await.unwrap();
    let output_ref_0 = publisher.write_output_object(&output_0).await.unwrap();
    let mut manifest_0 = manifest(0, state_ref_0);
    manifest_0.output_objects = vec![output_ref_0];
    publisher.publish_manifest(&manifest_0).await.unwrap();

    let state_ref_1 = publisher.write_state_object(&state_1).await.unwrap();
    let output_ref_1 = publisher.write_output_object(&output_1).await.unwrap();
    let mut manifest_1 = manifest(1, state_ref_1);
    manifest_1.output_objects = vec![output_ref_1];
    publisher.publish_manifest(&manifest_1).await.unwrap();

    let plan = publisher
        .plan_garbage_collection(GarbageCollectionPolicy {
            retain_latest_manifests: 1,
        })
        .await
        .unwrap();
    publisher
        .execute_garbage_collection_plan(&plan)
        .await
        .unwrap();

    assert!(!object_exists(store.as_ref(), state_0.object_key()).await);
    assert!(!object_exists(store.as_ref(), output_0.object_key()).await);
    assert!(object_exists(store.as_ref(), state_1.object_key()).await);
    assert!(object_exists(store.as_ref(), output_1.object_key()).await);
    assert_eq!(
        publisher.latest_manifest().await.unwrap(),
        Some(manifest_1.clone())
    );

    let report = publisher.inspect_checkpoints().await.unwrap();

    assert_eq!(report.latest_valid_checkpoint, Some(1));
    assert_eq!(report.manifests.len(), 2);
    assert_eq!(report.manifests[0].checkpoint_version, 0);
    assert!(matches!(
        report.manifests[0].status,
        CheckpointManifestInspectionStatus::Invalid { .. }
    ));
    assert!(report.manifests[0]
        .status
        .reason()
        .unwrap()
        .contains(state_0.object_key().as_str()));
    assert_eq!(report.manifests[1].checkpoint_version, 1);
    assert!(matches!(
        report.manifests[1].status,
        CheckpointManifestInspectionStatus::Valid
    ));
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_uses_valid_marker_without_full_listing() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());

    publisher.publish_manifest(&manifest).await.unwrap();

    let listing_fails_store = Arc::new(FullListingFailsStore::new(store));
    let marker_reader = CheckpointPublisher::new(listing_fails_store);

    assert_eq!(
        marker_reader.latest_manifest().await.unwrap(),
        Some(manifest)
    );
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_fails_closed_when_marker_read_fails() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());

    publisher.publish_manifest(&manifest).await.unwrap();

    let marker_read_fails_store = Arc::new(FullListingFailsStore::new_with_read_failure(
        store,
        "v1/checkpoint-index/latest-candidate.json",
    ));
    let marker_reader = CheckpointPublisher::new(marker_read_fails_store);
    let err = marker_reader.latest_manifest().await.unwrap_err();

    assert!(err.to_string().contains("read failed"));
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_falls_back_when_marker_is_corrupt() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());

    publisher.publish_manifest(&manifest).await.unwrap();
    store
        .put(
            &Path::from(ObjectKey::checkpoint_latest_candidate_marker().as_str()),
            Bytes::from_static(b"{not valid json").into(),
        )
        .await
        .unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_fails_closed_when_future_check_listing_fails() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-0");
    let manifest = manifest(0, publisher.write_state_object(&state).await.unwrap());

    publisher.publish_manifest(&manifest).await.unwrap();

    let future_listing_fails_store = Arc::new(PrefixListingFailsStore::new_with_offset_failure(
        store,
        "v1/checkpoints",
    ));
    let marker_reader = CheckpointPublisher::new(future_listing_fails_store);
    let err = marker_reader.latest_manifest().await.unwrap_err();

    assert!(err.to_string().contains("offset listing failed"));
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_falls_back_when_marker_is_stale() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");
    let manifest_0 = manifest(0, publisher.write_state_object(&state_0).await.unwrap());
    let manifest_1 = manifest(1, publisher.write_state_object(&state_1).await.unwrap());

    publisher.publish_manifest(&manifest_0).await.unwrap();
    let stale_marker = LatestCandidateMarker::for_manifest(
        &manifest_0,
        &serde_json::to_vec(&manifest_0).unwrap(),
        "2026-05-03T00:00:01Z".to_string(),
    );
    publisher.publish_manifest(&manifest_1).await.unwrap();
    store
        .put(
            &Path::from(ObjectKey::checkpoint_latest_candidate_marker().as_str()),
            Bytes::from(serde_json::to_vec(&stale_marker).unwrap()).into(),
        )
        .await
        .unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest_1));
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_does_not_return_marker_manifest_with_missing_state() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let parent_state = state_write(0, "state-0001", b"state-0");
    let missing_child_state = state_write(1, "state-0002", b"missing-state");
    let parent = manifest(
        0,
        publisher.write_state_object(&parent_state).await.unwrap(),
    );
    let invalid_child = manifest(1, state_ref(&missing_child_state));

    publisher.publish_manifest(&parent).await.unwrap();
    let invalid_child_bytes = serde_json::to_vec(&invalid_child).unwrap();
    store
        .put(
            &Path::from(invalid_child.object_key().as_str()),
            Bytes::from(invalid_child_bytes.clone()).into(),
        )
        .await
        .unwrap();
    let marker = LatestCandidateMarker::for_manifest(
        &invalid_child,
        &invalid_child_bytes,
        "2026-05-03T00:00:01Z".to_string(),
    );
    store
        .put(
            &Path::from(ObjectKey::checkpoint_latest_candidate_marker().as_str()),
            Bytes::from(serde_json::to_vec(&marker).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher.latest_manifest().await.unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::MissingStateObject(object_key)
            if object_key == *missing_child_state.object_key()
    ));
}

#[tokio::test]
async fn checkpoint_publish_rejects_child_manifest_when_parent_manifest_is_missing() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(1, "state-0002", b"state-1");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let child = manifest(1, state_ref);

    let err = publisher.publish_manifest(&child).await.unwrap_err();

    assert!(err.to_string().contains("parent checkpoint manifest"));
    assert!(store
        .head(&Path::from(child.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_preflight_rejects_missing_parent_without_child_objects() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let child_state = state_write(1, "state-0002", b"state-1");
    let child = manifest(1, state_ref(&child_state));

    let err = publisher
        .preflight_manifest_publication(&child)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::MissingParentManifest {
            checkpoint_version: 1,
            parent_checkpoint: 0
        }
    ));
    assert!(store
        .head(&Path::from(child_state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(child.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_accepts_child_manifest_after_parent_manifest_is_visible() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let parent_state = state_write(0, "state-0001", b"state-0");
    let child_state = state_write(1, "state-0002", b"state-1");
    let parent = manifest(
        0,
        publisher.write_state_object(&parent_state).await.unwrap(),
    );
    let child = manifest(1, publisher.write_state_object(&child_state).await.unwrap());

    publisher.publish_manifest(&parent).await.unwrap();
    publisher.publish_manifest(&child).await.unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(child));
}

#[tokio::test]
async fn checkpoint_publish_rejects_child_manifest_that_drops_parent_input_progress() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let parent_state = state_write(0, "state-0001", b"state-0");
    let child_state = state_write(1, "state-0002", b"state-1");
    let mut parent = manifest(
        0,
        publisher.write_state_object(&parent_state).await.unwrap(),
    );
    parent.input_ranges = vec![
        input_range_for("orders", 0, 0, 10),
        input_range_for("orders", 1, 0, 10),
    ];
    let mut child = manifest(1, publisher.write_state_object(&child_state).await.unwrap());
    child.input_ranges = vec![input_range_for("orders", 0, 0, 12)];

    publisher.publish_manifest(&parent).await.unwrap();
    let err = publisher.publish_manifest(&child).await.unwrap_err();

    assert!(err.to_string().contains("drops parent input progress"));
    assert!(store
        .head(&Path::from(child.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_child_manifest_that_regresses_parent_input_boundary() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let parent_state = state_write(0, "state-0001", b"state-0");
    let child_state = state_write(1, "state-0002", b"state-1");
    let parent = manifest(
        0,
        publisher.write_state_object(&parent_state).await.unwrap(),
    );
    let mut child = manifest(1, publisher.write_state_object(&child_state).await.unwrap());
    child.input_ranges = vec![input_range_for("orders", 0, 0, 9)];

    publisher.publish_manifest(&parent).await.unwrap();
    let err = publisher.publish_manifest(&child).await.unwrap_err();

    assert!(err.to_string().contains("regresses parent input boundary"));
    assert!(store
        .head(&Path::from(child.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_preflight_rejects_regressed_parent_input_without_child_objects() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let parent_state = state_write(0, "state-0001", b"state-0");
    let child_state = state_write(1, "state-0002", b"state-1");
    let parent = manifest(
        0,
        publisher.write_state_object(&parent_state).await.unwrap(),
    );
    let mut child = manifest(1, state_ref(&child_state));
    child.input_ranges = vec![input_range_for("orders", 0, 0, 9)];

    publisher.publish_manifest(&parent).await.unwrap();
    let err = publisher
        .preflight_manifest_publication(&child)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::RegressedParentInputBoundary {
            checkpoint_version: 1,
            parent_checkpoint: 0,
            stream_id,
            partition_id: 0,
            parent_start_offset_inclusive: 0,
            parent_end_offset_exclusive: 10,
            child_start_offset_inclusive: 0,
            child_end_offset_exclusive: 9,
        } if stream_id == "orders"
    ));
    assert!(store
        .head(&Path::from(child_state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(child.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_latest_and_listing_reject_out_of_band_orphan_checkpoint_manifest() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let parent_state = state_write(0, "state-0001", b"state-0");
    let orphan_state = state_write_for_partition(0, 2, "state-0003", b"state-2");
    let parent = manifest(
        0,
        publisher.write_state_object(&parent_state).await.unwrap(),
    );
    let orphan = manifest(2, state_ref(&orphan_state));

    publisher.publish_manifest(&parent).await.unwrap();
    store
        .put(
            &Path::from(orphan.object_key().as_str()),
            Bytes::from(serde_json::to_vec(&orphan).unwrap()).into(),
        )
        .await
        .unwrap();

    let list_err = publisher.list_published_manifests().await.unwrap_err();
    let latest_err = publisher.latest_manifest().await.unwrap_err();

    assert!(list_err.to_string().contains("parent checkpoint manifest"));
    assert!(latest_err
        .to_string()
        .contains("parent checkpoint manifest"));
}

#[tokio::test]
async fn checkpoint_publish_rejects_manifest_that_references_missing_state_object() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let missing_state = state_write(0, "missing-state", b"not-written");
    let manifest = manifest(0, state_ref(&missing_state));

    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(err.to_string().contains("referenced state object"));
    let manifest_path = Path::from(manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_manifest_that_references_missing_output_object() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let missing_output = output_ref_for_partition(0);
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![missing_output.clone()];

    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::MissingOutputObject(object_key)
            if object_key == missing_output.object_key
    ));
    let manifest_path = Path::from(manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}

#[tokio::test]
async fn checkpoint_publish_accepts_manifest_after_referenced_output_object_exists() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let output = output_ref_for_partition(0);
    store
        .put(
            &Path::from(output.object_key.as_str()),
            Bytes::from_static(b"output-bytes").into(),
        )
        .await
        .unwrap();
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![output];

    publisher.publish_manifest(&manifest).await.unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn checkpoint_publish_rejects_duplicate_output_object_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let output = output_write(0, 0, "out-0001", b"first");

    publisher.write_output_object(&output).await.unwrap();
    let duplicate = output_write(0, 0, "out-0001", b"second");
    let err = publisher.write_output_object(&duplicate).await.unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::OutputObjectAlreadyExists(object_key)
            if object_key == *output.object_key()
    ));
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_output_write_without_requested_owner_claim() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let output = output_write(0, 0, "out-0001", b"output");

    let err = publisher
        .write_output_object_fenced(&output, &claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::OutputOwnerClaimMismatch {
            object_key,
            expected,
            actual
        } if object_key == *output.object_key() && expected == claim && actual.is_none()
    ));
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_output_write_with_mismatched_owner_claim() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let requested_claim = owner_claim("worker-a", 1);
    let other_claim = owner_claim("worker-b", 1);
    let output = fenced_output_write(0, 0, "out-0001", other_claim.clone(), b"output");

    let err = publisher
        .write_output_object_fenced(&output, &requested_claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::OutputOwnerClaimMismatch {
            object_key,
            expected,
            actual
        } if object_key == *output.object_key()
            && expected == requested_claim
            && actual == Some(other_claim)
    ));
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_output_ref_without_owner_claim() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let output = output_write(0, 0, "out-0001", b"output");
    let output_ref = publisher.write_output_object(&output).await.unwrap();
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![output_ref.clone()];

    let err = publisher
        .publish_manifest_fenced(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::Manifest(ManifestError::MissingOutputOwnerClaim { object_id })
            if object_id == output_ref.object_id
    ));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_output_ref_from_different_owner_claim() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let requested_claim = owner_claim("worker-a", 1);
    let other_claim = owner_claim("worker-b", 1);
    let state = fenced_state_write(0, "state-0001", requested_claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced(&state, &requested_claim)
        .await
        .unwrap();
    let output = fenced_output_write(0, 0, "out-0001", other_claim.clone(), b"output");
    let output_ref = publisher
        .write_output_object_fenced(&output, &other_claim)
        .await
        .unwrap();
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![output_ref];

    let err = publisher
        .publish_manifest_fenced(&manifest, &requested_claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::Manifest(ManifestError::OutputOwnerClaimMismatch {
            object_id,
            expected,
            actual
        }) if object_id == "out-0001" && expected == requested_claim && actual == other_claim
    ));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_accepts_fenced_manifest_with_matching_state_and_output_owner_claims() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let output = fenced_output_write(0, 0, "out-0001", claim.clone(), b"output");
    let state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let output_ref = publisher
        .write_output_object_fenced(&output, &claim)
        .await
        .unwrap();
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![output_ref];

    publisher
        .publish_manifest_fenced(&manifest, &claim)
        .await
        .unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn checkpoint_publish_creates_ownership_epoch_records_create_only_and_idempotently() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let record = ownership_record("orders", 0, "worker-a", 7);

    let created_key = publisher
        .create_ownership_epoch_record(&record)
        .await
        .unwrap();
    let duplicate_key = publisher
        .create_ownership_epoch_record(&record)
        .await
        .unwrap();
    let read_back = publisher
        .read_ownership_epoch_record("orders", 0, 7)
        .await
        .unwrap();

    assert_eq!(created_key, record.object_key().unwrap());
    assert_eq!(duplicate_key, created_key);
    assert_eq!(read_back, record);

    let conflicting_record = ownership_record("orders", 0, "worker-b", 7);
    let err = publisher
        .create_ownership_epoch_record(&conflicting_record)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("ownership epoch record conflict"));
}

#[tokio::test]
async fn checkpoint_publish_production_fenced_state_write_rejects_missing_ownership_record_before_write(
) {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");

    let err = publisher
        .write_state_object_fenced_production(&state, "orders", &claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("ownership epoch record"));
    assert!(store
        .head(&Path::from(state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_production_fenced_output_write_rejects_missing_ownership_record_before_write(
) {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let output = fenced_output_write(0, 0, "out-0001", claim.clone(), b"output");

    let err = publisher
        .write_output_object_fenced_production(&output, &claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("ownership epoch record"));
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_production_fenced_manifest_rejects_missing_ownership_record_before_write(
) {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let manifest = manifest(0, state_ref);

    let err = publisher
        .publish_manifest_fenced_production(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("ownership epoch record"));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_production_fenced_manifest_rejects_legacy_state_ref_before_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 1))
        .await
        .unwrap();
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let mut state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let legacy_ref = serde_json::json!({
        "object_id": state_ref.object_id,
        "object_key": state_ref.object_key,
        "owner": state_ref.owner,
        "partition_id": state_ref.partition_id,
        "checkpoint_version": state_ref.checkpoint_version,
        "owner_claim": state_ref.owner_claim
    });
    state_ref = serde_json::from_value(legacy_ref).unwrap();
    let manifest = manifest(0, state_ref);

    let err = publisher
        .publish_manifest_fenced_production(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::ProductionStateRefNotSlateDbCheckpoint { .. }
    ));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_production_fenced_manifest_rejects_explicit_raw_state_ref_before_write()
{
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 1))
        .await
        .unwrap();
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let mut state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    state_ref.ref_type = StateRefType::RawObject;
    let manifest = manifest(0, state_ref);

    let err = publisher
        .publish_manifest_fenced_production(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::ProductionStateRefNotSlateDbCheckpoint { .. }
    ));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_bootstrap_publish_accepts_legacy_raw_state_ref() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let legacy_ref = serde_json::json!({
        "object_id": state_ref.object_id,
        "object_key": state_ref.object_key,
        "owner": state_ref.owner,
        "partition_id": state_ref.partition_id,
        "checkpoint_version": state_ref.checkpoint_version
    });
    let manifest = manifest(0, serde_json::from_value(legacy_ref).unwrap());

    publisher.publish_manifest(&manifest).await.unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn checkpoint_publish_production_state_write_rejects_lower_epoch_after_higher_epoch_record_without_manifest(
) {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let stale_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 1))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-b", 2))
        .await
        .unwrap();

    let stale_state = fenced_state_write(0, "state-0001", stale_claim.clone(), b"stale");
    let err = publisher
        .write_state_object_fenced_production(&stale_state, "orders", &stale_claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::StaleOwnerClaim {
            partition_id: 0,
            current,
            attempted
        } if current == current_claim && attempted == stale_claim
    ));
    assert!(store
        .head(&Path::from(stale_state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_production_fenced_publish_rejects_raw_state_ref_with_matching_ownership_records(
) {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 1))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("settlements", 0, "worker-a", 1))
        .await
        .unwrap();

    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let output = fenced_output_write(0, 0, "out-0001", claim.clone(), b"output");
    let state_ref = publisher
        .write_state_object_fenced_production(&state, "orders", &claim)
        .await
        .unwrap();
    let output_ref = publisher
        .write_output_object_fenced_production(&output, &claim)
        .await
        .unwrap();
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![output_ref];

    publisher
        .publish_manifest_fenced_production(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_production_fenced_publish_succeeds_with_slatedb_state_ref() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let claim = owner_claim("worker-a", 1);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 1))
        .await
        .unwrap();

    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced_production(&state, "orders", &claim)
        .await
        .unwrap();
    assert_eq!(state_ref.ref_type, StateRefType::SlateDbCheckpoint);
    let manifest = manifest(0, state_ref);

    publisher
        .publish_manifest_fenced_production(&manifest, &claim)
        .await
        .unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn checkpoint_publish_rejects_stale_owner_when_newer_output_owner_claim_is_published() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let stale_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);

    let current_state = state_write(0, "state-0001", b"state");
    let current_output = fenced_output_write(0, 0, "out-0001", current_claim.clone(), b"output");
    let current_ref = publisher.write_state_object(&current_state).await.unwrap();
    let current_output_ref = publisher
        .write_output_object_fenced(&current_output, &current_claim)
        .await
        .unwrap();
    let mut current_manifest = manifest(0, current_ref);
    current_manifest.output_objects = vec![current_output_ref];
    publisher.publish_manifest(&current_manifest).await.unwrap();

    let stale_output = fenced_output_write(0, 1, "out-0002", stale_claim.clone(), b"stale");
    let err = publisher
        .write_output_object_fenced(&stale_output, &stale_claim)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::StaleOwnerClaim {
            partition_id: 0,
            current,
            attempted
        } if current == current_claim && attempted == stale_claim
    ));
    assert!(store
        .head(&Path::from(stale_output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_stale_owner_epoch_before_state_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let stale_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);

    let old_state = fenced_state_write(0, "state-0001", stale_claim.clone(), b"old-state");
    let old_ref = publisher
        .write_state_object_fenced(&old_state, &stale_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(0, old_ref), &stale_claim)
        .await
        .unwrap();

    let current_state = fenced_state_write(1, "state-0002", current_claim.clone(), b"current");
    let current_ref = publisher
        .write_state_object_fenced(&current_state, &current_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(1, current_ref), &current_claim)
        .await
        .unwrap();

    let stale_state = fenced_state_write(2, "state-0003", stale_claim.clone(), b"stale");
    let err = publisher
        .write_state_object_fenced(&stale_state, &stale_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("stale owner claim"));
    assert!(store
        .head(&Path::from(stale_state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_same_owner_epoch_with_different_owner_id_before_state_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let published_claim = owner_claim("worker-a", 7);
    let conflicting_claim = owner_claim("worker-b", 7);

    let published_state =
        fenced_state_write(0, "state-0001", published_claim.clone(), b"published");
    let published_ref = publisher
        .write_state_object_fenced(&published_state, &published_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(0, published_ref), &published_claim)
        .await
        .unwrap();

    let conflicting_state =
        fenced_state_write(1, "state-0002", conflicting_claim.clone(), b"conflict");
    let err = publisher
        .write_state_object_fenced(&conflicting_state, &conflicting_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("stale owner claim"));
    assert!(store
        .head(&Path::from(conflicting_state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_allows_newer_owner_after_old_orphan_state_object_exists() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let old_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);

    let orphan_state = fenced_state_write(0, "state-0001", old_claim.clone(), b"orphan");
    let orphan_ref = publisher
        .write_state_object_fenced(&orphan_state, &old_claim)
        .await
        .unwrap();

    let current_state = fenced_state_write(0, "state-0002", current_claim.clone(), b"current");
    let current_ref = publisher
        .write_state_object_fenced(&current_state, &current_claim)
        .await
        .unwrap();
    let current_manifest = manifest(0, current_ref.clone());
    publisher
        .publish_manifest_fenced(&current_manifest, &current_claim)
        .await
        .unwrap();

    assert_eq!(
        publisher.read_state_object(&orphan_ref).await.unwrap(),
        Bytes::from_static(b"orphan")
    );
    assert_eq!(
        publisher.read_state_object(&current_ref).await.unwrap(),
        Bytes::from_static(b"current")
    );
    assert_eq!(
        publisher.latest_manifest().await.unwrap(),
        Some(current_manifest)
    );
}

#[tokio::test]
async fn checkpoint_publish_rejects_stale_owner_epoch_before_manifest_publication() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let stale_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);

    let old_state = fenced_state_write(0, "state-0001", stale_claim.clone(), b"old-state");
    let old_ref = publisher
        .write_state_object_fenced(&old_state, &stale_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(0, old_ref), &stale_claim)
        .await
        .unwrap();

    let stale_state = fenced_state_write(2, "state-0003", stale_claim.clone(), b"stale");
    let stale_ref = publisher.write_state_object(&stale_state).await.unwrap();
    let stale_manifest = manifest(2, stale_ref);

    let current_state = fenced_state_write(1, "state-0002", current_claim.clone(), b"current");
    let current_ref = publisher
        .write_state_object_fenced(&current_state, &current_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(1, current_ref), &current_claim)
        .await
        .unwrap();

    let err = publisher
        .publish_manifest_fenced(&stale_manifest, &stale_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("stale owner claim"));
    let manifest_path = Path::from(stale_manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_unclaimed_input_partition_before_writing()
{
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let manifest = CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![
            input_range(),
            InputRange {
                stream_id: "orders".to_string(),
                partition_id: 1,
                start_offset_inclusive: 0,
                end_offset_exclusive: 10,
            },
        ],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    };

    let err = publisher
        .publish_manifest_fenced(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("owner claim"));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_unclaimed_output_partition_before_writing()
{
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![output_ref_for_partition(1)];

    let err = publisher
        .publish_manifest_fenced(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("owner claim"));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_state_refs_from_different_owner_claims() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let requested_claim = owner_claim("worker-a", 1);
    let other_claim = owner_claim("worker-b", 1);
    let requested_state =
        fenced_state_write_for_partition(0, 0, "state-0001", requested_claim.clone(), b"state-a");
    let other_state =
        fenced_state_write_for_partition(1, 0, "state-0002", other_claim.clone(), b"state-b");
    let requested_ref = publisher
        .write_state_object_fenced(&requested_state, &requested_claim)
        .await
        .unwrap();
    let other_ref = publisher
        .write_state_object_fenced(&other_state, &other_claim)
        .await
        .unwrap();
    let mut manifest = manifest(0, requested_ref);
    manifest.state_objects.push(other_ref);

    let err = publisher
        .publish_manifest_fenced(&manifest, &requested_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("owner claim mismatch"));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn raw_state_store_fails_closed_for_slatedb_state_store_refs() {
    let (_temp_dir, store) = temp_store();
    let state_store = RawObjectStateStore::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"raw-state");
    let mut written_ref = state_store.write_state_object(&state).await.unwrap();
    written_ref.ref_type = StateRefType::SlateDbCheckpoint;

    assert!(!state_store.state_object_exists(&written_ref).await.unwrap());
    let err = state_store
        .read_state_object(&written_ref)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CheckpointPublishError::MissingStateObject(object_key)
            if object_key == *state.object_key()
    ));
}

#[tokio::test]
async fn slatedb_state_store_reads_checkpoint_versioned_state_payloads() {
    let (_temp_dir, store) = temp_store();
    let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let state = state_write(7, "state-0007", b"slatedb-state");
    let written_ref = state_store.write_state_object(&state).await.unwrap();

    assert_eq!(written_ref, slatedb_state_ref(&state));
    assert!(state_store.state_object_exists(&written_ref).await.unwrap());
    assert_eq!(
        state_store.read_state_object(&written_ref).await.unwrap(),
        Bytes::from_static(b"slatedb-state")
    );
}

#[tokio::test]
async fn checkpoint_publish_slatedb_state_store_keeps_manifests_authoritative() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let published_state = state_write(0, "state-0001", b"published-state");
    let published_ref = publisher
        .write_state_object(&published_state)
        .await
        .unwrap();
    let published_manifest = manifest(0, published_ref.clone());
    publisher
        .publish_manifest(&published_manifest)
        .await
        .unwrap();

    let orphan_state = state_write(1, "state-0002", b"orphan-state");
    let orphan_ref = publisher.write_state_object(&orphan_state).await.unwrap();

    assert_eq!(
        publisher.read_state_object(&orphan_ref).await.unwrap(),
        Bytes::from_static(b"orphan-state")
    );
    assert_eq!(
        publisher.latest_manifest().await.unwrap(),
        Some(published_manifest.clone())
    );
    assert_eq!(
        publisher
            .read_state_object(&published_manifest.state_objects[0])
            .await
            .unwrap(),
        Bytes::from_static(b"published-state")
    );
}

#[tokio::test]
async fn checkpoint_publish_slatedb_state_store_rejects_duplicate_state_object_write() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let state = state_write(0, "state-0001", b"first");

    let written_ref = publisher.write_state_object(&state).await.unwrap();
    let duplicate = state_write(0, "state-0001", b"second");
    let err = publisher.write_state_object(&duplicate).await.unwrap_err();

    assert!(err.to_string().contains("already exists"));
    assert_eq!(
        publisher.read_state_object(&written_ref).await.unwrap(),
        Bytes::from_static(b"first")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_publish_slatedb_state_store_rejects_concurrent_duplicate_state_object_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = Arc::new(
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(16));

    let handles = (0..16)
        .map(|attempt| {
            let publisher = Arc::clone(&publisher);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                let state =
                    state_write_bytes(0, "state-0001", Bytes::from(format!("payload-{attempt}")));
                barrier.wait().await;
                let result = publisher.write_state_object(&state).await;
                (state, result)
            })
        })
        .collect::<Vec<_>>();

    let results = futures::future::try_join_all(handles).await.unwrap();
    let successes = results
        .iter()
        .filter(|(_, result)| result.is_ok())
        .collect::<Vec<_>>();
    let duplicates = results
        .iter()
        .filter(|(_, result)| {
            result
                .as_ref()
                .is_err_and(|err| err.to_string().contains("already exists"))
        })
        .count();

    assert_eq!(successes.len(), 1);
    assert_eq!(duplicates, 15);

    let (winning_state, winning_ref) = successes[0];
    assert_eq!(
        winning_ref.as_ref().unwrap(),
        &slatedb_state_ref(winning_state)
    );
    assert_eq!(
        publisher
            .read_state_object(winning_ref.as_ref().unwrap())
            .await
            .unwrap(),
        winning_state.bytes().clone()
    );
}

#[tokio::test]
async fn checkpoint_publish_slatedb_state_store_rejects_manifest_that_references_missing_state_object(
) {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let missing_state = state_write(0, "missing-state", b"not-written");
    let manifest = manifest(0, state_ref(&missing_state));

    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(err.to_string().contains("referenced state object"));
    let manifest_path = Path::from(manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}

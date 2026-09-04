use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::memory::InMemory;
use velorix_control::operator_authority::{
    validate_operator_authority, ObjectStoreAuthorityRef, OperatorAuthorityStartupComponents,
};
use velorix_control::relation_ingest_publisher::{
    deterministic_request_digest, deterministic_request_id, RelationIngestPublisher,
    RelationIngestPublisherConfig, RelationIngestPublisherError, RelationIngestScope,
};
use velorix_core::relation::VelorixRelationCatalogV1;
use velorix_meta::{
    AcquireRelationPartitionAuthorityOutcome, AcquireRelationPartitionAuthorityRequest,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest, InMemoryMetaStore,
    IngestRangeReservation, MetaStore, MetaStoreCapabilities, MetaStoreError,
    PublishIngestReservationOutcome, PublishRelationIngestReservationRequest,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    RelationAuthoritativeIngestPublication, RelationPartitionAuthorityKey,
    RelationPartitionAuthorityToken, ReserveIngestRangeOutcome,
    ReserveRelationAuthoritativeIngestRangeRequest, StandingRuntimeCheckpointPointer,
    StandingRuntimeOwnerClaim, StoreRelationCatalogOutcome,
};
use velorix_storage::object_key::ObjectKey;

fn config(owner_id: &str) -> RelationIngestPublisherConfig {
    RelationIngestPublisherConfig {
        namespace: "default".into(),
        relation_id: "orders".into(),
        relation_version: "v1".into(),
        schema_fingerprint: "sha256:schema".into(),
        stream_id: "orders-stream".into(),
        partition_id: 0,
        owner_id: owner_id.into(),
        authority_ttl_ms: 100,
    }
}

fn scope() -> RelationIngestScope {
    RelationIngestScope {
        namespace: "default".into(),
        relation_id: "orders".into(),
        relation_version: "v1".into(),
        schema_fingerprint: "sha256:schema".into(),
        stream_id: "orders-stream".into(),
        partition_id: 0,
    }
}

async fn startup_components() -> OperatorAuthorityStartupComponents {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: "test-store".into(),
            namespace: "default".into(),
        },
        store,
        "in-memory-test",
        "v1/test-capability-probes",
    )
    .await
    .unwrap();
    OperatorAuthorityStartupComponents::from_validated_authority(validated)
}

async fn publisher(meta: Arc<FaultMeta>, owner_id: &str) -> RelationIngestPublisher {
    let startup = startup_components().await;
    RelationIngestPublisher::new(meta, &startup, config(owner_id)).unwrap()
}

#[derive(Clone, Copy)]
enum FaultMode {
    PassThrough,
    CommitThenErrorOnce,
    AbsentThenRetry,
    MismatchOnRead,
    ResponseLossTwice,
}

struct FaultMeta {
    inner: Arc<InMemoryMetaStore>,
    mode: FaultMode,
    supports_relation: bool,
    publish_calls: AtomicUsize,
}

impl FaultMeta {
    fn new(mode: FaultMode) -> Self {
        Self {
            inner: Arc::new(InMemoryMetaStore::default()),
            mode,
            supports_relation: true,
            publish_calls: AtomicUsize::new(0),
        }
    }

    fn unsupported() -> Self {
        Self {
            inner: Arc::new(InMemoryMetaStore::default()),
            mode: FaultMode::PassThrough,
            supports_relation: false,
            publish_calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.publish_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MetaStore for FaultMeta {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        let mut capabilities = self.inner.read_meta_store_capabilities().await?;
        if self.supports_relation {
            capabilities.relation_ingest.durable_across_restart = true;
        }
        Ok(capabilities)
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        self.inner.store_relation_catalog(catalog).await
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        self.inner
            .read_relation_catalog(relation_id, relation_version)
            .await
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.inner.reserve_ingest_range(reservation).await
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        self.inner.acquire_standing_runtime_owner(request).await
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        self.inner
            .read_standing_runtime_owner(tenant_id, program_id, view_id)
            .await
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        self.inner
            .publish_standing_runtime_checkpoint(request)
            .await
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        self.inner
            .read_standing_runtime_checkpoint(tenant_id, program_id, view_id)
            .await
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        self.inner
            .read_view_dependency_graph_revision(tenant_id)
            .await
    }

    async fn acquire_relation_partition_authority(
        &self,
        request: AcquireRelationPartitionAuthorityRequest,
    ) -> Result<AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        self.inner
            .acquire_relation_partition_authority(request)
            .await
    }

    async fn read_relation_partition_authority(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Option<RelationPartitionAuthorityToken>, MetaStoreError> {
        self.inner.read_relation_partition_authority(key).await
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.inner
            .reserve_relation_authoritative_ingest_range(request)
            .await
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: PublishRelationIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        let call = self.publish_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let outcome = match self.mode {
            FaultMode::CommitThenErrorOnce if call == 1 => {
                self.inner
                    .publish_relation_ingest_reservation(request)
                    .await?;
                return Err(MetaStoreError::Remote("response lost".into()));
            }
            FaultMode::AbsentThenRetry if call == 1 => {
                return Err(MetaStoreError::Remote(
                    "request did not reach metadata".into(),
                ));
            }
            FaultMode::ResponseLossTwice if call <= 2 => {
                if call == 2 {
                    self.inner
                        .publish_relation_ingest_reservation(request)
                        .await?;
                }
                return Err(MetaStoreError::Remote("response lost".into()));
            }
            _ => {
                self.inner
                    .publish_relation_ingest_reservation(request)
                    .await?
            }
        };
        Ok(outcome)
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        let publication = self
            .inner
            .read_relation_authoritative_ingest_publication(request_id)
            .await?;
        if matches!(self.mode, FaultMode::MismatchOnRead) {
            Ok(publication.map(|mut publication| {
                publication.object_digest = "sha256:mismatch".into();
                publication
            }))
        } else {
            Ok(publication)
        }
    }
}

async fn fault_publisher(meta: Arc<FaultMeta>) -> RelationIngestPublisher {
    let startup = startup_components().await;
    RelationIngestPublisher::new(meta, &startup, config("writer-a")).unwrap()
}

#[test]
fn request_identity_is_deterministic_and_payload_bound() {
    let first = deterministic_request_id(&scope(), 0, 10, "sha256:a").unwrap();
    assert_eq!(
        first,
        deterministic_request_id(&scope(), 0, 10, "sha256:a").unwrap()
    );
    assert_ne!(
        first,
        deterministic_request_id(&scope(), 0, 10, "sha256:b").unwrap()
    );
    assert_ne!(
        deterministic_request_digest(&scope(), 0, 10, "sha256:a").unwrap(),
        deterministic_request_digest(&scope(), 1, 11, "sha256:a").unwrap()
    );
}

#[tokio::test]
async fn response_loss_after_commit_reads_back_exact_publication() {
    let meta = Arc::new(FaultMeta::new(FaultMode::CommitThenErrorOnce));
    let publisher = fault_publisher(Arc::clone(&meta)).await;
    publisher.start().await.unwrap();
    publisher
        .publish(0, 10, Bytes::from_static(b"loss-after-commit"))
        .await
        .unwrap();
    assert_eq!(meta.calls(), 1);
}

#[tokio::test]
async fn absent_response_retries_same_request_once() {
    let meta = Arc::new(FaultMeta::new(FaultMode::AbsentThenRetry));
    let publisher = fault_publisher(Arc::clone(&meta)).await;
    publisher.start().await.unwrap();
    publisher
        .publish(0, 10, Bytes::from_static(b"retry"))
        .await
        .unwrap();
    assert_eq!(meta.calls(), 2);
}

#[tokio::test]
async fn mismatched_readback_is_a_conflict() {
    let meta = Arc::new(FaultMeta::new(FaultMode::MismatchOnRead));
    let publisher = fault_publisher(Arc::clone(&meta)).await;
    publisher.start().await.unwrap();
    assert!(matches!(
        publisher
            .publish(0, 10, Bytes::from_static(b"mismatch"))
            .await,
        Err(RelationIngestPublisherError::PublicationMismatch { .. })
    ));
    assert_eq!(meta.calls(), 1);
}

#[tokio::test]
async fn second_response_loss_still_succeeds_from_full_readback() {
    let meta = Arc::new(FaultMeta::new(FaultMode::ResponseLossTwice));
    let publisher = fault_publisher(Arc::clone(&meta)).await;
    publisher.start().await.unwrap();
    publisher
        .publish(0, 10, Bytes::from_static(b"twice-loss"))
        .await
        .unwrap();
    assert_eq!(meta.calls(), 2);
}

#[tokio::test]
async fn success_exact_retry_and_pre_publish_staging_invisibility() {
    let meta = Arc::new(FaultMeta::new(FaultMode::PassThrough));
    let publisher = publisher(Arc::clone(&meta), "writer-a").await;
    publisher.start().await.unwrap();
    let first = publisher
        .publish(0, 10, Bytes::from_static(b"payload"))
        .await
        .unwrap();
    assert!(first.object_key.as_str().starts_with("v1/ingest-staging/"));
    assert!(publisher.session().token().await.is_some());
    assert_eq!(
        publisher
            .publish(0, 10, Bytes::from_static(b"payload"))
            .await
            .unwrap(),
        first
    );

    assert!(publisher.list_committed().await.unwrap().is_empty());
    let staging = ObjectKey::parse_ingest_staging(first.object_key.as_str()).unwrap();
    assert_eq!(staging.1.stream_id, "orders-stream");
    assert!(publisher.session().token().await.is_some());
}

#[tokio::test]
async fn different_digest_is_a_conflict_and_owner_is_required() {
    let meta = Arc::new(FaultMeta::new(FaultMode::PassThrough));
    let publisher = publisher(Arc::clone(&meta), "writer-a").await;
    publisher.start().await.unwrap();
    publisher
        .publish(0, 10, Bytes::from_static(b"payload-a"))
        .await
        .unwrap();
    assert!(matches!(
        publisher
            .publish(0, 10, Bytes::from_static(b"payload-b"))
            .await,
        Err(RelationIngestPublisherError::RangeConflict { .. })
    ));
    assert!(matches!(
        RelationIngestPublisher::new(
            meta.clone() as Arc<dyn MetaStore>,
            &startup_components().await,
            config(""),
        ),
        Err(RelationIngestPublisherError::InvalidConfig { field: "owner_id" })
    ));
}

#[tokio::test]
async fn capability_absence_fails_closed_at_startup() {
    let meta = Arc::new(FaultMeta::unsupported());
    let startup = startup_components().await;
    let publisher = RelationIngestPublisher::new(meta, &startup, config("writer-a")).unwrap();
    assert!(matches!(
        publisher.start().await,
        Err(RelationIngestPublisherError::UnsupportedRelationIngestCapability { .. })
    ));
}

#[tokio::test]
async fn stale_takeover_clears_old_session() {
    let meta = Arc::new(FaultMeta::new(FaultMode::PassThrough));
    let first = publisher(Arc::clone(&meta), "writer-a").await;
    first.start().await.unwrap();
    meta.inner.set_partition_authority_clock_for_test(101).await;
    let second = publisher(Arc::clone(&meta), "writer-b").await;
    second.start().await.unwrap();
    assert!(matches!(
        first.renew().await,
        Err(RelationIngestPublisherError::AuthorityConflict { .. })
    ));
    assert!(first.session().token().await.is_none());
    assert!(matches!(
        first.publish(0, 10, Bytes::from_static(b"stale")).await,
        Err(RelationIngestPublisherError::NotStarted)
    ));
}

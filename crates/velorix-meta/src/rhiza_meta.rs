//! Rhiza KV backed metadata store.
//!
//! Rhiza's KV API is deliberately kept small here: the Velorix metadata state
//! is one bounded, canonical snapshot and every mutation evaluates a fresh
//! [`InMemoryMetaStore`] before publishing that snapshot with a root CAS.

use std::future::Future;

use async_trait::async_trait;
use uuid::Uuid;

use crate::rhiza_kv::{RhizaKvError, RhizaKvStore};
use crate::rhiza_kv_snapshot::{CompareExchange, RhizaKvSnapshot, RootToken, SnapshotError};
use crate::rhiza_snapshot;
use crate::{
    unix_time_ms, AcquirePartitionAuthorityOutcome, AcquirePartitionAuthorityRequest,
    AcquireRelationPartitionAuthorityOutcome, AcquireRelationPartitionAuthorityRequest,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    AuthoritativeIngestPublication, BeginViewBootstrapOutcome, BeginViewBootstrapRequest,
    CaptureIngestSourceCutRequest, CaptureRelationIngestSourceCutRequest, CommitIngestRangeOutcome,
    FixViewBootstrapActivationCutOutcome, FixViewBootstrapActivationCutRequest, InMemoryMetaStore,
    IngestRangeReservation, IngestSourceCutV1, MetaStore, MetaStoreCapabilities, MetaStoreError,
    PartitionAuthorityCapability, PartitionAuthorityKey, PartitionAuthorityToken,
    PartitionCheckpointPointer, PromoteViewBootstrapOutcome, PromoteViewBootstrapRequest,
    PublishIngestReservationOutcome, PublishIngestReservationRequest,
    PublishPartitionCheckpointPointerOutcome, PublishPartitionCheckpointPointerRequest,
    PublishRelationIngestReservationRequest, PublishStandingRuntimeCheckpointOutcome,
    PublishStandingRuntimeCheckpointRequest, RelationAuthoritativeIngestPublication,
    RelationIngestCapability, RelationIngestSourceCutV1, RelationPartitionAuthorityKey,
    RelationPartitionAuthorityToken, ReserveAuthoritativeIngestRangeRequest,
    ReserveIngestRangeOutcome, ReserveRelationAuthoritativeIngestRangeRequest,
    StandingRuntimeCheckpointPointer, StandingRuntimeOwnerClaim, StoreRelationCatalogOutcome,
    VelorixRelationCatalogV1, ViewBootstrapControlV1,
};

const MAX_CAS_ATTEMPTS: usize = 8;

/// A metadata store using only Rhiza 0.12's linearizable KV operations.
#[derive(Clone)]
pub struct RhizaKvMetaStore {
    snapshot: RhizaKvSnapshot,
}

impl RhizaKvMetaStore {
    pub fn new(kv: RhizaKvStore) -> Self {
        Self {
            snapshot: RhizaKvSnapshot::new(kv),
        }
    }

    pub async fn open(
        data_dir: impl Into<String> + Send + 'static,
        node_id: impl Into<String> + Send + 'static,
    ) -> Result<Self, MetaStoreError> {
        Ok(Self::new(
            RhizaKvStore::open(data_dir, node_id)
                .await
                .map_err(rhiza_error)?,
        ))
    }

    pub async fn open_config(config: rhizadb::Config) -> Result<Self, MetaStoreError> {
        Ok(Self::new(
            RhizaKvStore::open_config(config)
                .await
                .map_err(rhiza_error)?,
        ))
    }

    pub fn from_snapshot(snapshot: RhizaKvSnapshot) -> Self {
        Self { snapshot }
    }

    /// Close the native node after all metadata callers have stopped.
    pub async fn close(self) -> Result<(), MetaStoreError> {
        self.snapshot.close().await.map_err(snapshot_error)
    }

    async fn load(&self) -> Result<(Option<RootToken>, crate::InMemoryMetaState), MetaStoreError> {
        match self.snapshot.load().await.map_err(snapshot_error)? {
            Some((root, bytes)) => rhiza_snapshot::decode(&bytes)
                .map(|state| (Some(root), state))
                .map_err(|error| MetaStoreError::Serialization(error.to_string())),
            None => Ok((None, crate::InMemoryMetaState::default())),
        }
    }

    async fn read_eval<R, F, Fut>(&self, evaluate: F) -> Result<R, MetaStoreError>
    where
        F: FnOnce(InMemoryMetaStore) -> Fut,
        Fut: Future<Output = Result<R, MetaStoreError>>,
    {
        let (_, state) = self.load().await?;
        let now = unix_time_ms()?;
        evaluate(InMemoryMetaStore::from_state_for_evaluation(state, now)).await
    }

    async fn mutate<R, F, Fut>(&self, evaluate: F) -> Result<R, MetaStoreError>
    where
        F: Fn(InMemoryMetaStore) -> Fut + Send + Sync,
        Fut: Future<Output = Result<R, MetaStoreError>> + Send,
        R: Send,
    {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (expected, state) = self.load().await?;
            let now = unix_time_ms()?;
            let evaluator = InMemoryMetaStore::from_state_for_evaluation(state, now);
            let outcome = evaluate(evaluator.clone()).await?;
            let encoded = rhiza_snapshot::encode(&evaluator.snapshot_state().await)
                .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
            let request_id = Uuid::now_v7().to_string();
            match self
                .snapshot
                .compare_exchange(expected, encoded, request_id)
                .await
                .map_err(snapshot_error)?
            {
                CompareExchange::Applied(_) => return Ok(outcome),
                CompareExchange::Conflict => continue,
            }
        }
        Err(MetaStoreError::RhizaContention {
            attempts: MAX_CAS_ATTEMPTS,
        })
    }

    fn capabilities(mut capabilities: MetaStoreCapabilities) -> MetaStoreCapabilities {
        let standing = &mut capabilities.standing_runtime_fencing;
        standing.backend_name = "rhiza-kv".into();
        standing.durable_monotonic_owner_epoch = true;
        standing.authoritative_backend_time = false;
        standing.backend_time_source_kind =
            crate::STANDING_RUNTIME_BACKEND_TIME_SOURCE_PROCESS_CLOCK.into();
        standing.backend_time_blocked_reason =
            "proposer_wall_clock_has_no_bounded_skew_guarantee".into();
        standing.lease_authority_kind = "rhiza_quepaxa_root_cas".into();
        standing.lease_expiry_semantics =
            crate::STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_PROCESS_CLOCK_TTL.into();
        standing.bounded_wall_clock_failover = false;
        standing.failover_time_bound_ms = 0;
        standing.multi_writer_fencing_safe = false;
        standing.production_bounded_failover_safe = false;
        standing.production_multi_writer_safe = false;

        capabilities.partition_authority = PartitionAuthorityCapability {
            backend_name: "rhiza-kv".into(),
            partition_scoped_authority: true,
            backend_owned_time: false,
            fenced_checkpoint_pointer_publish: true,
            durable_across_restart: true,
            production_safe: false,
        };
        capabilities.relation_ingest = RelationIngestCapability {
            backend_name: "rhiza-kv".into(),
            relation_scoped_authority: true,
            committed_publication_source_cut: true,
            durable_across_restart: true,
        };
        capabilities
    }
}

fn rhiza_error(error: RhizaKvError) -> MetaStoreError {
    match error {
        RhizaKvError::Indeterminate { request_id, detail } => {
            MetaStoreError::RhizaIndeterminate { request_id, detail }
        }
        other => MetaStoreError::Rhiza(other.to_string()),
    }
}

fn snapshot_error(error: SnapshotError) -> MetaStoreError {
    match error {
        SnapshotError::Kv(error) => rhiza_error(error),
        other => MetaStoreError::Serialization(other.to_string()),
    }
}

#[async_trait]
impl MetaStore for RhizaKvMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        self.read_eval(|store| async move {
            store
                .read_meta_store_capabilities()
                .await
                .map(Self::capabilities)
        })
        .await
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let catalog = catalog.clone();
            async move { store.store_relation_catalog(catalog).await }
        })
        .await
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        let relation_id = relation_id.to_owned();
        let relation_version = relation_version.to_owned();
        self.read_eval(move |store| async move {
            store
                .read_relation_catalog(&relation_id, &relation_version)
                .await
        })
        .await
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let reservation = reservation.clone();
            async move { store.reserve_ingest_range(reservation).await }
        })
        .await
    }

    async fn reserve_authoritative_ingest_range(
        &self,
        request: ReserveAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.reserve_authoritative_ingest_range(request).await }
        })
        .await
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let reservation = reservation.clone();
            async move { store.commit_ingest_range(reservation).await }
        })
        .await
    }

    async fn publish_ingest_reservation(
        &self,
        request: PublishIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.publish_ingest_reservation(request).await }
        })
        .await
    }

    async fn read_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<AuthoritativeIngestPublication>, MetaStoreError> {
        let request_id = request_id.to_owned();
        self.read_eval(move |store| async move {
            store
                .read_authoritative_ingest_publication(&request_id)
                .await
        })
        .await
    }

    async fn list_authoritative_ingest_publications(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Vec<AuthoritativeIngestPublication>, MetaStoreError> {
        let key = key.clone();
        self.read_eval(move |store| async move {
            store.list_authoritative_ingest_publications(&key).await
        })
        .await
    }

    async fn commit_ingest_ranges(
        &self,
        reservations: Vec<IngestRangeReservation>,
    ) -> Result<Vec<CommitIngestRangeOutcome>, MetaStoreError> {
        self.mutate(move |store| {
            let reservations = reservations.clone();
            async move { store.commit_ingest_ranges(reservations).await }
        })
        .await
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        self.read_eval(move |store| {
            let request = request.clone();
            async move { store.capture_ingest_source_cut(request).await }
        })
        .await
    }

    async fn read_relation_ingest_capability(
        &self,
    ) -> Result<RelationIngestCapability, MetaStoreError> {
        self.read_eval(|store| async move { store.read_relation_ingest_capability().await })
            .await
            .map(|mut capability| {
                capability.backend_name = "rhiza-kv".into();
                capability.durable_across_restart = true;
                capability
            })
    }

    async fn capture_relation_ingest_source_cut(
        &self,
        request: CaptureRelationIngestSourceCutRequest,
    ) -> Result<RelationIngestSourceCutV1, MetaStoreError> {
        self.read_eval(move |store| {
            let request = request.clone();
            async move { store.capture_relation_ingest_source_cut(request).await }
        })
        .await
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.begin_view_bootstrap(request).await }
        })
        .await
    }

    async fn read_view_bootstrap(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        let tenant_id = tenant_id.to_owned();
        let program_id = program_id.to_owned();
        let view_id = view_id.to_owned();
        self.read_eval(move |store| async move {
            store
                .read_view_bootstrap(&tenant_id, &program_id, &view_id)
                .await
        })
        .await
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: FixViewBootstrapActivationCutRequest,
    ) -> Result<FixViewBootstrapActivationCutOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.fix_view_bootstrap_activation_cut(request).await }
        })
        .await
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.promote_view_bootstrap(request).await }
        })
        .await
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.acquire_standing_runtime_owner(request).await }
        })
        .await
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        let tenant_id = tenant_id.to_owned();
        let program_id = program_id.to_owned();
        let view_id = view_id.to_owned();
        self.read_eval(move |store| async move {
            store
                .read_standing_runtime_owner(&tenant_id, &program_id, &view_id)
                .await
        })
        .await
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.publish_standing_runtime_checkpoint(request).await }
        })
        .await
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        let tenant_id = tenant_id.to_owned();
        let program_id = program_id.to_owned();
        let view_id = view_id.to_owned();
        self.read_eval(move |store| async move {
            store
                .read_standing_runtime_checkpoint(&tenant_id, &program_id, &view_id)
                .await
        })
        .await
    }

    async fn read_partition_authority_capability(
        &self,
    ) -> Result<PartitionAuthorityCapability, MetaStoreError> {
        self.read_eval(|store| async move { store.read_partition_authority_capability().await })
            .await
            .map(|mut capability| {
                capability.backend_name = "rhiza-kv".into();
                capability.backend_owned_time = false;
                capability.durable_across_restart = true;
                capability.production_safe = false;
                capability
            })
    }

    async fn acquire_partition_authority(
        &self,
        request: AcquirePartitionAuthorityRequest,
    ) -> Result<AcquirePartitionAuthorityOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.acquire_partition_authority(request).await }
        })
        .await
    }

    async fn read_partition_authority(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionAuthorityToken>, MetaStoreError> {
        let key = key.clone();
        self.read_eval(move |store| async move { store.read_partition_authority(&key).await })
            .await
    }

    async fn acquire_relation_partition_authority(
        &self,
        request: AcquireRelationPartitionAuthorityRequest,
    ) -> Result<AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.acquire_relation_partition_authority(request).await }
        })
        .await
    }

    async fn read_relation_partition_authority(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Option<RelationPartitionAuthorityToken>, MetaStoreError> {
        let key = key.clone();
        self.read_eval(
            move |store| async move { store.read_relation_partition_authority(&key).await },
        )
        .await
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move {
                store
                    .reserve_relation_authoritative_ingest_range(request)
                    .await
            }
        })
        .await
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: PublishRelationIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.publish_relation_ingest_reservation(request).await }
        })
        .await
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        let request_id = request_id.to_owned();
        self.read_eval(move |store| async move {
            store
                .read_relation_authoritative_ingest_publication(&request_id)
                .await
        })
        .await
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Vec<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        let key = key.clone();
        self.read_eval(move |store| async move {
            store
                .list_relation_authoritative_ingest_publications(&key)
                .await
        })
        .await
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: PublishPartitionCheckpointPointerRequest,
    ) -> Result<PublishPartitionCheckpointPointerOutcome, MetaStoreError> {
        self.mutate(move |store| {
            let request = request.clone();
            async move { store.publish_partition_checkpoint_pointer(request).await }
        })
        .await
    }

    async fn read_partition_checkpoint_pointer(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionCheckpointPointer>, MetaStoreError> {
        let key = key.clone();
        self.read_eval(
            move |store| async move { store.read_partition_checkpoint_pointer(&key).await },
        )
        .await
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        let tenant_id = tenant_id.to_owned();
        self.read_eval(move |store| async move {
            store.read_view_dependency_graph_revision(&tenant_id).await
        })
        .await
    }
}

//! Rhiza KV backed metadata store.
//!
//! Rhiza's KV API is deliberately kept small here: the Velorix metadata state
//! is one bounded, canonical snapshot and every mutation evaluates a fresh
//! [`InMemoryMetaStore`] before publishing that snapshot with a root CAS.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::rhiza_kv::{RhizaKvError, RhizaKvStore};
use crate::rhiza_kv_snapshot::{CompareExchange, RhizaKvSnapshot, RootToken, SnapshotError};
use crate::rhiza_snapshot;
use crate::{
    unix_time_ms, AcquirePartitionAuthorityOutcome, AcquirePartitionAuthorityRequest,
    AcquireRelationPartitionAuthorityOutcome, AcquireRelationPartitionAuthorityRequest,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    AuthoritativeIngestPublication, BeginViewBootstrapOutcome, BeginViewBootstrapRequest,
    CaptureIngestSourceCutRequest, CaptureRelationIngestSourceCutRequest,
    CaptureRelationIngestSourceCutsRequest, CommitIngestRangeOutcome,
    FixViewBootstrapActivationCutOutcome, FixViewBootstrapActivationCutRequest, InMemoryMetaStore,
    IngestRangeReservation, IngestSourceCutV1, MetaStore, MetaStoreCapabilities, MetaStoreError,
    PartitionAuthorityCapability, PartitionAuthorityKey, PartitionAuthorityToken,
    PartitionCheckpointPointer, PromoteViewBootstrapOutcome, PromoteViewBootstrapRequest,
    PublishIngestReservationOutcome, PublishIngestReservationRequest,
    PublishPartitionCheckpointPointerOutcome, PublishPartitionCheckpointPointerRequest,
    PublishRelationIngestReservationRequest, PublishStandingRuntimeCheckpointOutcome,
    PublishStandingRuntimeCheckpointRequest, RelationAuthoritativeIngestPublication,
    RelationIngestCapability, RelationIngestSourceCutV1, RelationIngestSourceIdentityCutV1,
    RelationPartitionAuthorityKey, RelationPartitionAuthorityToken,
    ReserveAuthoritativeIngestRangeRequest, ReserveIngestRangeOutcome,
    ReserveRelationAuthoritativeIngestRangeRequest, StandingRuntimeCheckpointPointer,
    StandingRuntimeOwnerClaim, StandingRuntimeOwnerToken, StoreRelationCatalogOutcome,
    VelorixRelationCatalogV1, ViewBootstrapControlV1,
};

const MAX_CAS_ATTEMPTS: usize = 8;

type CachedRootState = (RootToken, Arc<RwLock<crate::InMemoryMetaState>>);

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A metadata store using only Rhiza 0.19.0's linearizable KV operations.
#[derive(Clone)]
pub struct RhizaKvMetaStore {
    snapshot: RhizaKvSnapshot,
    /// Process-local cache of the decoded snapshot, valid only while the KV
    /// root token still matches the cached token.
    cache: Arc<RwLock<Option<CachedRootState>>>,
    /// Serializes full snapshot loads so one root change decodes once.
    full_load_lock: Arc<Mutex<()>>,
    /// Serializes same-process mutations before they read the root and clock.
    write_lock: Arc<Mutex<()>>,
    #[cfg(test)]
    full_loads: Arc<AtomicUsize>,
    /// Forces one simulated CAS conflict for the retry regression test.
    #[cfg(test)]
    conflict_once: Arc<AtomicBool>,
}

struct LoadedState {
    root: Option<RootToken>,
    state: Arc<RwLock<crate::InMemoryMetaState>>,
    cache_hit: bool,
}

impl RhizaKvMetaStore {
    pub fn new(kv: RhizaKvStore) -> Self {
        Self::from_snapshot(RhizaKvSnapshot::new(kv))
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
        Self {
            snapshot,
            cache: Arc::new(RwLock::new(None)),
            full_load_lock: Arc::new(Mutex::new(())),
            write_lock: Arc::new(Mutex::new(())),
            #[cfg(test)]
            full_loads: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            conflict_once: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Close the native node after all metadata callers have stopped.
    pub async fn close(self) -> Result<(), MetaStoreError> {
        self.snapshot.close().await.map_err(snapshot_error)
    }

    async fn load_state(&self) -> Result<LoadedState, MetaStoreError> {
        let Some(root) = self.snapshot.root().await.map_err(snapshot_error)? else {
            *self.cache.write().await = None;
            return Ok(LoadedState {
                root: None,
                state: Arc::new(RwLock::new(crate::InMemoryMetaState::default())),
                cache_hit: false,
            });
        };
        if let Some((cached_root, state)) = self.cache.read().await.as_ref() {
            if *cached_root == root {
                return Ok(LoadedState {
                    root: Some(root),
                    state: Arc::clone(state),
                    cache_hit: true,
                });
            }
        }
        let _guard = self.full_load_lock.lock().await;
        if let Some((cached_root, state)) = self.cache.read().await.as_ref() {
            if *cached_root == root {
                return Ok(LoadedState {
                    root: Some(root),
                    state: Arc::clone(state),
                    cache_hit: true,
                });
            }
        }
        #[cfg(test)]
        self.full_loads.fetch_add(1, Ordering::SeqCst);
        let (loaded_root, bytes) = self.snapshot.load_at(root).await.map_err(snapshot_error)?;
        let state = Arc::new(RwLock::new(
            rhiza_snapshot::decode(&bytes)
                .map_err(|error| MetaStoreError::Serialization(error.to_string()))?,
        ));
        *self.cache.write().await = Some((loaded_root.clone(), Arc::clone(&state)));
        Ok(LoadedState {
            root: Some(loaded_root),
            state,
            cache_hit: false,
        })
    }

    async fn read_eval<R, F, Fut>(
        &self,
        operation: &'static str,
        evaluate: F,
    ) -> Result<R, MetaStoreError>
    where
        F: FnOnce(InMemoryMetaStore) -> Fut,
        Fut: Future<Output = Result<R, MetaStoreError>>,
    {
        let started = std::time::Instant::now();
        let loaded = self.load_state().await;
        let cache_hit = loaded.as_ref().is_ok_and(|loaded| loaded.cache_hit);
        let result = match loaded {
            Ok(loaded) => match unix_time_ms() {
                Ok(now) => {
                    evaluate(InMemoryMetaStore::from_shared_state_for_evaluation(
                        loaded.state,
                        now,
                    ))
                    .await
                }
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        report_diag(operation, cache_hit, started);
        result
    }

    async fn mutate<R, F, Fut>(
        &self,
        operation: &'static str,
        evaluate: F,
    ) -> Result<R, MetaStoreError>
    where
        F: Fn(InMemoryMetaStore) -> Fut + Send + Sync,
        Fut: Future<Output = Result<R, MetaStoreError>> + Send,
        R: Send,
    {
        let started = std::time::Instant::now();
        let mut cache_hit = None;
        let _write_guard = self.write_lock.lock().await;
        let mut result = Err(MetaStoreError::RhizaContention {
            attempts: MAX_CAS_ATTEMPTS,
        });
        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = match self.load_state().await {
                Ok(loaded) => loaded,
                Err(error) => {
                    result = Err(error);
                    break;
                }
            };
            cache_hit = Some(loaded.cache_hit);
            let now = match unix_time_ms() {
                Ok(now) => now,
                Err(error) => {
                    result = Err(error);
                    break;
                }
            };
            // Mutations always own a deep candidate copy. A failed root CAS
            // must leave any same-root read evaluation pinned to its old state.
            let candidate = loaded.state.read().await.clone();
            let evaluator = InMemoryMetaStore::from_state_for_evaluation(candidate, now);
            let outcome = match evaluate(evaluator.clone()).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    result = Err(error);
                    break;
                }
            };
            let state = evaluator.snapshot_state().await;
            let encoded = match rhiza_snapshot::encode(&state) {
                Ok(encoded) => encoded,
                Err(error) => {
                    result = Err(MetaStoreError::Serialization(error.to_string()));
                    break;
                }
            };
            #[cfg(test)]
            if self.conflict_once.swap(false, Ordering::SeqCst) {
                *self.cache.write().await = None;
                continue;
            }
            let request_id = Uuid::now_v7().to_string();
            let exchanged = match self
                .snapshot
                .compare_exchange(loaded.root, encoded, request_id)
                .await
            {
                Ok(exchanged) => exchanged,
                Err(error) => {
                    result = Err(snapshot_error(error));
                    break;
                }
            };
            match exchanged {
                CompareExchange::Applied(new_root) => {
                    *self.cache.write().await = Some((new_root, Arc::new(RwLock::new(state))));
                    result = Ok(outcome);
                    break;
                }
                CompareExchange::Conflict => {
                    *self.cache.write().await = None;
                }
            }
        }
        report_diag(operation, cache_hit.unwrap_or(false), started);
        result
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
        SnapshotError::TooLarge => MetaStoreError::SnapshotTooLarge,
        other => MetaStoreError::Serialization(other.to_string()),
    }
}

/// Opt-in one-line-per-operation timing. Unset means no environment lookup cost
/// beyond the `var_os` call and no output.
fn report_diag(operation: &str, cache_hit: bool, started: std::time::Instant) {
    if std::env::var_os("VELORIX_META_DIAGNOSTICS").is_some() {
        eprintln!(
            "meta_diag op={operation} cache={} ms={}",
            if cache_hit { "hit" } else { "miss" },
            started.elapsed().as_millis()
        );
    }
}

impl RhizaKvMetaStore {
    /// Open the store from the canonical `RHIZA_*` process environment with the
    /// private recovery listener already bound. Metadata operations and the
    /// Operator's recovery endpoints then share one native handle, so the
    /// listener is closed by the same `close` that shuts the store down.
    pub async fn open_operator_managed(
        recovery_bind: impl Into<String> + Send + 'static,
    ) -> Result<Self, MetaStoreError> {
        Ok(Self::new(
            RhizaKvStore::open_operator_managed(recovery_bind)
                .await
                .map_err(rhiza_error)?,
        ))
    }

    /// The bound recovery address, if this store started the listener.
    pub fn recovery_address(&self) -> Option<&str> {
        self.snapshot.recovery_address()
    }

    /// Native local readiness, which a learner reports before it has a quorum.
    pub async fn ready(&self) -> Result<bool, MetaStoreError> {
        self.snapshot.ready().await.map_err(snapshot_error)
    }

    #[cfg(test)]
    fn full_loads(&self) -> usize {
        self.full_loads.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn force_conflict_once(&self) {
        self.conflict_once.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl MetaStore for RhizaKvMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        self.read_eval("read_meta_store_capabilities", |store| async move {
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
        self.mutate("store_relation_catalog", move |store| {
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
        self.read_eval("read_relation_catalog", move |store| async move {
            store
                .read_relation_catalog(&relation_id, &relation_version)
                .await
        })
        .await
    }

    async fn read_active_view_registry_snapshot(
        &self,
        request: crate::ActiveViewRegistryReadRequestV1,
    ) -> Result<crate::ActiveViewRegistrySnapshotV1, MetaStoreError> {
        self.read_eval(
            "read_active_view_registry_snapshot",
            move |store| async move { store.read_active_view_registry_snapshot(request).await },
        )
        .await
    }

    async fn create_active_view_record(
        &self,
        request: crate::CreateActiveViewRecordRequestV1,
    ) -> Result<crate::CreateActiveViewRecordOutcomeV1, MetaStoreError> {
        self.mutate("create_active_view_record", move |store| {
            let request = request.clone();
            async move { store.create_active_view_record(request).await }
        })
        .await
    }

    async fn compare_exchange_active_view_record(
        &self,
        request: crate::CompareExchangeActiveViewRecordRequestV1,
    ) -> Result<crate::CompareExchangeActiveViewRecordOutcomeV1, MetaStoreError> {
        self.mutate("compare_exchange_active_view_record", move |store| {
            let request = request.clone();
            async move { store.compare_exchange_active_view_record(request).await }
        })
        .await
    }

    async fn import_active_view_registry(
        &self,
        request: crate::ImportActiveViewRegistryRequestV1,
    ) -> Result<crate::ImportActiveViewRegistryOutcomeV1, MetaStoreError> {
        self.mutate("import_active_view_registry", move |store| {
            let request = request.clone();
            async move { store.import_active_view_registry(request).await }
        })
        .await
    }

    async fn promote_view_bootstrap_with_active_view_record(
        &self,
        request: crate::PromoteViewBootstrapWithActiveViewRecordRequestV1,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        self.mutate(
            "promote_view_bootstrap_with_active_view_record",
            move |store| {
                let request = request.clone();
                async move {
                    store
                        .promote_view_bootstrap_with_active_view_record(request)
                        .await
                }
            },
        )
        .await
    }

    async fn read_demand_metadata_batch(
        &self,
        request: crate::ReadDemandMetadataBatchRequest,
    ) -> Result<crate::ReadDemandMetadataBatchResponse, MetaStoreError> {
        request.validate()?;
        self.read_eval("read_demand_metadata_batch", move |store| async move {
            super::read_demand_metadata_batch_from_state(store, request).await
        })
        .await
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.mutate("reserve_ingest_range", move |store| {
            let reservation = reservation.clone();
            async move { store.reserve_ingest_range(reservation).await }
        })
        .await
    }

    async fn reserve_authoritative_ingest_range(
        &self,
        request: ReserveAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.mutate("reserve_authoritative_ingest_range", move |store| {
            let request = request.clone();
            async move { store.reserve_authoritative_ingest_range(request).await }
        })
        .await
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        self.mutate("commit_ingest_range", move |store| {
            let reservation = reservation.clone();
            async move { store.commit_ingest_range(reservation).await }
        })
        .await
    }

    async fn publish_ingest_reservation(
        &self,
        request: PublishIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        self.mutate("publish_ingest_reservation", move |store| {
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
        self.read_eval(
            "read_authoritative_ingest_publication",
            move |store| async move {
                store
                    .read_authoritative_ingest_publication(&request_id)
                    .await
            },
        )
        .await
    }

    async fn list_authoritative_ingest_publications(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Vec<AuthoritativeIngestPublication>, MetaStoreError> {
        let key = key.clone();
        self.read_eval(
            "list_authoritative_ingest_publications",
            move |store| async move { store.list_authoritative_ingest_publications(&key).await },
        )
        .await
    }

    async fn commit_ingest_ranges(
        &self,
        reservations: Vec<IngestRangeReservation>,
    ) -> Result<Vec<CommitIngestRangeOutcome>, MetaStoreError> {
        self.mutate("commit_ingest_ranges", move |store| {
            let reservations = reservations.clone();
            async move { store.commit_ingest_ranges(reservations).await }
        })
        .await
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        self.read_eval("capture_ingest_source_cut", move |store| {
            let request = request.clone();
            async move { store.capture_ingest_source_cut(request).await }
        })
        .await
    }

    async fn read_relation_ingest_capability(
        &self,
    ) -> Result<RelationIngestCapability, MetaStoreError> {
        self.read_eval("read_relation_ingest_capability", |store| async move {
            store.read_relation_ingest_capability().await
        })
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
        self.read_eval("capture_relation_ingest_source_cut", move |store| {
            let request = request.clone();
            async move { store.capture_relation_ingest_source_cut(request).await }
        })
        .await
    }

    async fn capture_relation_ingest_source_cuts(
        &self,
        request: CaptureRelationIngestSourceCutsRequest,
    ) -> Result<Vec<RelationIngestSourceIdentityCutV1>, MetaStoreError> {
        self.read_eval("capture_relation_ingest_source_cuts", move |store| {
            let request = request.clone();
            async move { store.capture_relation_ingest_source_cuts(request).await }
        })
        .await
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        self.mutate("begin_view_bootstrap", move |store| {
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
        self.read_eval("read_view_bootstrap", move |store| async move {
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
        self.mutate("fix_view_bootstrap_activation_cut", move |store| {
            let request = request.clone();
            async move { store.fix_view_bootstrap_activation_cut(request).await }
        })
        .await
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        self.mutate("promote_view_bootstrap", move |store| {
            let request = request.clone();
            async move { store.promote_view_bootstrap(request).await }
        })
        .await
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        self.mutate("acquire_standing_runtime_owner", move |store| {
            let request = request.clone();
            async move { store.acquire_standing_runtime_owner(request).await }
        })
        .await
    }

    async fn acquire_standing_runtime_owner_with_endpoint(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
        owner_endpoint: String,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        self.mutate(
            "acquire_standing_runtime_owner_with_endpoint",
            move |store| {
                let request = request.clone();
                let owner_endpoint = owner_endpoint.clone();
                async move {
                    store
                        .acquire_standing_runtime_owner_with_endpoint(request, owner_endpoint)
                        .await
                }
            },
        )
        .await
    }

    async fn renew_standing_runtime_owner(
        &self,
        owner: StandingRuntimeOwnerToken,
        ttl_ms: u64,
    ) -> Result<StandingRuntimeOwnerClaim, MetaStoreError> {
        self.mutate("renew_standing_runtime_owner", move |store| {
            let owner = owner.clone();
            async move { store.renew_standing_runtime_owner(owner, ttl_ms).await }
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
        self.read_eval("read_standing_runtime_owner", move |store| async move {
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
        self.mutate("publish_standing_runtime_checkpoint", move |store| {
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
        self.read_eval(
            "read_standing_runtime_checkpoint",
            move |store| async move {
                store
                    .read_standing_runtime_checkpoint(&tenant_id, &program_id, &view_id)
                    .await
            },
        )
        .await
    }

    async fn read_partition_authority_capability(
        &self,
    ) -> Result<PartitionAuthorityCapability, MetaStoreError> {
        self.read_eval("read_partition_authority_capability", |store| async move {
            store.read_partition_authority_capability().await
        })
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
        self.mutate("acquire_partition_authority", move |store| {
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
        self.read_eval("read_partition_authority", move |store| async move {
            store.read_partition_authority(&key).await
        })
        .await
    }

    async fn acquire_relation_partition_authority(
        &self,
        request: AcquireRelationPartitionAuthorityRequest,
    ) -> Result<AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        self.mutate("acquire_relation_partition_authority", move |store| {
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
            "read_relation_partition_authority",
            move |store| async move { store.read_relation_partition_authority(&key).await },
        )
        .await
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.mutate(
            "reserve_relation_authoritative_ingest_range",
            move |store| {
                let request = request.clone();
                async move {
                    store
                        .reserve_relation_authoritative_ingest_range(request)
                        .await
                }
            },
        )
        .await
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: PublishRelationIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        self.mutate("publish_relation_ingest_reservation", move |store| {
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
        self.read_eval(
            "read_relation_authoritative_ingest_publication",
            move |store| async move {
                store
                    .read_relation_authoritative_ingest_publication(&request_id)
                    .await
            },
        )
        .await
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Vec<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        let key = key.clone();
        self.read_eval(
            "list_relation_authoritative_ingest_publications",
            move |store| async move {
                store
                    .list_relation_authoritative_ingest_publications(&key)
                    .await
            },
        )
        .await
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: PublishPartitionCheckpointPointerRequest,
    ) -> Result<PublishPartitionCheckpointPointerOutcome, MetaStoreError> {
        self.mutate("publish_partition_checkpoint_pointer", move |store| {
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
            "read_partition_checkpoint_pointer",
            move |store| async move { store.read_partition_checkpoint_pointer(&key).await },
        )
        .await
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        let tenant_id = tenant_id.to_owned();
        self.read_eval(
            "read_view_dependency_graph_revision",
            move |store| async move { store.read_view_dependency_graph_revision(&tenant_id).await },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StoreRelationCatalogOutcome, VelorixRelationCatalogV1};
    use velorix_core::relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationSchemaV1, VelorixRelationSourceV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    };

    fn catalog(version: &str) -> VelorixRelationCatalogV1 {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "orders".into(),
            relation_name: "orders".into(),
            relation_version: version.into(),
            columns: vec![
                RelationColumnV1 {
                    column_id: "account_id".into(),
                    name: "account_id".into(),
                    logical_type: VelorixLogicalTypeV1::Utf8,
                    physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                    nullable: false,
                    ordinal: 0,
                    semantic_role: RelationSemanticRoleV1::PrimaryKey,
                },
                RelationColumnV1 {
                    column_id: "amount".into(),
                    name: "amount".into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 1,
                    semantic_role: RelationSemanticRoleV1::Value,
                },
                RelationColumnV1 {
                    column_id: "weight".into(),
                    name: "weight".into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 2,
                    semantic_role: RelationSemanticRoleV1::Weight,
                },
            ],
            primary_key_column_ids: vec!["account_id".into()],
            weight_column_id: "weight".into(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        };
        let schema_fingerprint =
            SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
        VelorixRelationCatalogV1 {
            relation_source: VelorixRelationSourceV1::SourceRelation,
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                mode: DataFusionRegistrationModeV1::View,
                name: "orders".into(),
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: "orders".into(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.into(),
            },
        }
    }

    async fn store() -> (tempfile::TempDir, RhizaKvMetaStore) {
        let directory = tempfile::tempdir().unwrap();
        // Local mode skips peer/operator listeners, which sandboxed test runs
        // cannot bind.
        let store = RhizaKvMetaStore::open_config(
            rhizadb::Config::new(directory.path())
                .node_id("meta-cache")
                .local(),
        )
        .await
        .unwrap();
        (directory, store)
    }

    #[tokio::test]
    async fn consecutive_reads_share_one_full_load() {
        let (_directory, store) = store().await;
        assert_eq!(
            store.store_relation_catalog(catalog("v1")).await.unwrap(),
            StoreRelationCatalogOutcome::Created
        );
        let after_write = store.full_loads();
        let pinned = store.cache.read().await.as_ref().unwrap().1.clone();
        for _ in 0..3 {
            store.read_relation_catalog("orders", "v1").await.unwrap();
        }
        let same_root = store.load_state().await.unwrap().state;
        assert!(Arc::ptr_eq(&pinned, &same_root));
        assert_eq!(store.full_loads(), after_write);
    }

    #[tokio::test]
    async fn shared_read_clock_override_does_not_change_standalone_authority_clock() {
        let memory = InMemoryMetaStore::default();
        memory.set_partition_authority_clock_for_test(100).await;
        let key = crate::PartitionAuthorityKey {
            namespace: "tenant".into(),
            view_id: "view".into(),
            stream_id: "stream".into(),
            partition_id: 0,
        };
        assert!(matches!(
            memory
                .acquire_partition_authority(crate::AcquirePartitionAuthorityRequest {
                    key: key.clone(),
                    owner_id: "owner".into(),
                    current_token: None,
                    ttl_ms: 10,
                })
                .await
                .unwrap(),
            crate::AcquirePartitionAuthorityOutcome::Acquired(_)
        ));

        let shared =
            InMemoryMetaStore::from_shared_state_for_evaluation(Arc::clone(&memory.inner), 110);
        assert!(shared
            .read_partition_authority(&key)
            .await
            .unwrap()
            .is_none());
        assert!(memory
            .read_partition_authority(&key)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn demand_metadata_batch_pins_one_fresh_root_for_all_typed_lookups() {
        let (_directory, store) = store().await;
        let relation_catalog = catalog("v1");
        store
            .store_relation_catalog(relation_catalog.clone())
            .await
            .unwrap();
        let source_cut_request = crate::CaptureRelationIngestSourceCutsRequest {
            namespace: "default".into(),
            relations: vec![crate::IngestSourceRelationIdentityV1 {
                relation_id: "orders".into(),
                relation_version: "v1".into(),
                relation_generation: crate::INGEST_SOURCE_IDENTITY_GENERATION_V1,
                schema_fingerprint: relation_catalog.schema_fingerprint.to_string(),
            }],
        };
        let request = crate::ReadDemandMetadataBatchRequest {
            source_cuts: Some(source_cut_request.clone()),
            relation_catalogs: vec![crate::DemandRelationCatalogLookup {
                relation_id: "orders".into(),
                relation_version: "v1".into(),
            }],
            standing_runtime_checkpoint: Some(crate::DemandStandingRuntimeCheckpointLookup {
                tenant_id: "tenant".into(),
                program_id: "program".into(),
                view_id: "view".into(),
            }),
        };

        store.snapshot.reset_get_calls();
        store
            .capture_relation_ingest_source_cuts(source_cut_request)
            .await
            .unwrap();
        store.read_relation_catalog("orders", "v1").await.unwrap();
        assert!(store
            .read_standing_runtime_checkpoint("tenant", "program", "view")
            .await
            .unwrap()
            .is_none());
        assert_eq!(store.snapshot.get_calls(), 3);

        store.snapshot.reset_get_calls();
        let result = store.read_demand_metadata_batch(request).await.unwrap();
        assert_eq!(store.snapshot.get_calls(), 1);
        assert_eq!(result.source_cuts.unwrap().len(), 1);
        assert_eq!(result.relation_catalogs, vec![relation_catalog]);
        assert!(result.standing_runtime_checkpoint.is_none());
    }

    #[tokio::test]
    async fn root_change_from_another_store_forces_reload() {
        // Two stores over one native node keep independent caches, which is
        // how a writer in another process changes the root.
        let directory = tempfile::tempdir().unwrap();
        let kv = RhizaKvStore::open_config(
            rhizadb::Config::new(directory.path())
                .node_id("meta-cache")
                .local(),
        )
        .await
        .unwrap();
        let first = RhizaKvMetaStore::new(kv.clone());
        let second = RhizaKvMetaStore::new(kv);
        first.store_relation_catalog(catalog("v1")).await.unwrap();
        assert_eq!(
            first.read_relation_catalog("orders", "v1").await.unwrap(),
            catalog("v1")
        );
        let before = first.full_loads();

        second.store_relation_catalog(catalog("v2")).await.unwrap();
        // A changed root causes one root read plus its immutable snapshot page.
        first.snapshot.reset_get_calls();
        let request = crate::ReadDemandMetadataBatchRequest {
            source_cuts: None,
            relation_catalogs: vec![crate::DemandRelationCatalogLookup {
                relation_id: "orders".into(),
                relation_version: "v2".into(),
            }],
            standing_runtime_checkpoint: None,
        };
        let result = first
            .read_demand_metadata_batch(request.clone())
            .await
            .unwrap();
        assert_eq!(result.relation_catalogs, vec![catalog("v2")]);
        assert_eq!(first.snapshot.get_calls(), 2);
        assert!(first.full_loads() > before);
        first.snapshot.reset_get_calls();
        first.read_demand_metadata_batch(request).await.unwrap();
        assert_eq!(first.snapshot.get_calls(), 1);
    }

    #[tokio::test]
    async fn applied_mutation_caches_without_reload() {
        let (_directory, store) = store().await;
        store.store_relation_catalog(catalog("v1")).await.unwrap();
        let after_apply = store.full_loads();
        store.read_relation_catalog("orders", "v1").await.unwrap();
        assert_eq!(store.full_loads(), after_apply);
    }

    #[tokio::test]
    async fn concurrent_cloned_owner_claims_and_renewals_preserve_owner() {
        let (_directory, store) = store().await;
        let request = AcquireStandingRuntimeOwnerRequest {
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: "owner".into(),
            ttl_ms: 60_000,
        };
        let endpoint = "http://owner.example:8080";
        let initial = store
            .acquire_standing_runtime_owner_with_endpoint(request.clone(), endpoint.into())
            .await
            .unwrap();
        let AcquireStandingRuntimeOwnerOutcome::Acquired(initial) = initial else {
            panic!("initial owner claim was not acquired");
        };
        const CALLERS: usize = 24;
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let mut tasks = Vec::new();
        for index in 0..CALLERS {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            let mut request = request.clone();
            let token = initial.token();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                if index % 3 == 0 {
                    return store.renew_standing_runtime_owner(token, 60_000).await;
                }
                let advertised_endpoint = if index % 3 == 1 {
                    endpoint
                } else {
                    request.owner_id = format!("foreign-{index}");
                    "http://foreign.example:8080"
                };
                let outcome = store
                    .acquire_standing_runtime_owner_with_endpoint(
                        request,
                        advertised_endpoint.into(),
                    )
                    .await?;
                match outcome {
                    AcquireStandingRuntimeOwnerOutcome::Renewed(claim) if index % 3 == 1 => {
                        Ok(claim)
                    }
                    AcquireStandingRuntimeOwnerOutcome::Conflict(claim) if index % 3 == 2 => {
                        Ok(claim)
                    }
                    other => panic!("unexpected concurrent owner outcome: {other:?}"),
                }
            }));
        }
        barrier.wait().await;
        for task in tasks {
            let claim = task.await.unwrap().unwrap();
            assert_eq!(claim.owner_id, initial.owner_id);
            assert_eq!(claim.owner_epoch, initial.owner_epoch);
            assert_eq!(claim.owner_endpoint.as_deref(), Some(endpoint));
        }
        let current = store
            .read_standing_runtime_owner("tenant", "program", "view")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.owner_id, initial.owner_id);
        assert_eq!(current.owner_epoch, initial.owner_epoch);
        assert_eq!(current.owner_endpoint.as_deref(), Some(endpoint));
    }

    #[tokio::test]
    async fn conflict_invalidates_cache_and_retries() {
        let (_directory, store) = store().await;
        store.store_relation_catalog(catalog("v1")).await.unwrap();
        store.read_relation_catalog("orders", "v1").await.unwrap();
        let pinned = store.cache.read().await.as_ref().unwrap().1.clone();
        let before = store.full_loads();
        store.force_conflict_once();
        assert_eq!(
            store.store_relation_catalog(catalog("v2")).await.unwrap(),
            StoreRelationCatalogOutcome::Created
        );
        assert!(store.full_loads() > before, "conflict must reload");
        let pinned_state = pinned.read().await;
        assert_eq!(
            pinned_state
                .relation_catalogs
                .get(&("orders".into(), "v1".into())),
            Some(&catalog("v1"))
        );
        assert!(!pinned_state
            .relation_catalogs
            .contains_key(&("orders".into(), "v2".into())));
        drop(pinned_state);
        assert_eq!(
            store.read_relation_catalog("orders", "v2").await.unwrap(),
            catalog("v2")
        );
    }
}

use std::{fmt, sync::Arc};

use object_store::{path::Path, ObjectStore};
use thiserror::Error;
use velorix_core::{
    delta::DeltaBatch,
    engine::{
        AggregateValueMode, EngineCheckpoint, EngineCheckpointPayload, EngineError,
        IncrementalEngine, LogicalEpoch, PrototypeIncrementalEngine,
        ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
    },
    relation::{
        arrow_record_batches_to_single_key_sum_count_delta_batch,
        supported_incremental_adapter_spec, ArrowPhysicalTypeV1, IncrementalInputAdapterError,
        RelationSchemaError, RelationSemanticRoleV1, VelorixRelationCatalogV1,
    },
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityError,
        ObjectStoreCapabilityProfile,
    },
    checkpoint_index::CheckpointRecoveryMode,
    ingest_envelope::IngestEnvelope,
    log::{IngestLog, IngestLogError, ReplayCheckpoint},
    manifest::CheckpointManifest,
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
    state::{CheckpointPublishError, CheckpointPublisher},
};

pub use velorix_core::relation::{
    orders_sum_count_relation_catalog, ORDERS_SUM_COUNT_ADAPTER_ID, ORDERS_SUM_COUNT_RELATION_ID,
    ORDERS_SUM_COUNT_RELATION_VERSION,
};

pub const ORDERS_SUM_COUNT_OWNER: &str = "orders_sum_count";

pub struct RecoveredRuntime {
    materialized: RuntimeIncrementalEngine,
    replay_checkpoints: Vec<ReplayCheckpoint>,
    replayed_batch_count: usize,
    latest_checkpoint_version: Option<u64>,
}

impl fmt::Debug for RecoveredRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveredRuntime")
            .field("engine_backend", &self.engine_backend())
            .field("logical_epoch", &self.logical_epoch())
            .field("replay_checkpoints", &self.replay_checkpoints)
            .field("replayed_batch_count", &self.replayed_batch_count)
            .field("latest_checkpoint_version", &self.latest_checkpoint_version)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalEngineBackend {
    Prototype,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncrementalEngineBackendSelection {
    Default,
    Explicit(IncrementalEngineBackend),
}

enum RuntimeIncrementalEngine {
    Prototype(PrototypeIncrementalEngine),
}

impl RuntimeIncrementalEngine {
    fn new_for_catalog(
        backend: IncrementalEngineBackend,
        _relation_catalog: &VelorixRelationCatalogV1,
        aggregate_value_mode: AggregateValueMode,
    ) -> Result<Self, RecoveryError> {
        match backend {
            IncrementalEngineBackend::Prototype => Ok(Self::Prototype(
                PrototypeIncrementalEngine::with_aggregate_value_mode(aggregate_value_mode),
            )),
        }
    }

    fn from_checkpoint_for_catalog(
        backend: IncrementalEngineBackend,
        _relation_catalog: &VelorixRelationCatalogV1,
        aggregate_value_mode: AggregateValueMode,
        checkpoint: EngineCheckpoint,
    ) -> Result<Self, RecoveryError> {
        match backend {
            IncrementalEngineBackend::Prototype => Ok(Self::Prototype(
                PrototypeIncrementalEngine::from_checkpoint_with_aggregate_value_mode(
                    checkpoint,
                    aggregate_value_mode,
                )?,
            )),
        }
    }

    fn backend(&self) -> IncrementalEngineBackend {
        match self {
            Self::Prototype(_) => IncrementalEngineBackend::Prototype,
        }
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        match self {
            Self::Prototype(engine) => engine.logical_epoch(),
        }
    }

    fn push_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        signed_input_changes: &DeltaBatch,
    ) -> Result<DeltaBatch, EngineError> {
        match self {
            Self::Prototype(engine) => engine.push_changes(logical_epoch, signed_input_changes),
        }
    }

    fn materialized_state(&self) -> DeltaBatch {
        match self {
            Self::Prototype(engine) => engine.materialized_state(),
        }
    }
}

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Checkpoint(#[from] CheckpointPublishError),
    #[error(transparent)]
    Ingest(#[from] IngestLogError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("unexpected state object owner `{actual}`, expected `{expected}`")]
    UnexpectedStateOwner { actual: String, expected: String },
    #[error("unsupported engine checkpoint payload schema version {0}")]
    UnsupportedEngineCheckpointPayloadSchema(u32),
    #[error(
        "state objects disagree on checkpoint logical epoch: expected={expected}, actual={actual}"
    )]
    InconsistentCheckpointLogicalEpoch {
        expected: LogicalEpoch,
        actual: LogicalEpoch,
    },
    #[error("logical epoch overflowed during recovery replay")]
    LogicalEpochOverflow,
    #[error(transparent)]
    RelationCatalog(#[from] RelationSchemaError),
    #[error(transparent)]
    RelationCatalogRegistry(#[from] RelationCatalogRegistryError),
    #[error(transparent)]
    AuthoritativeObjectStoreCapabilities(#[from] AuthoritativeObjectStoreCapabilityError),
    #[error(transparent)]
    ObjectStoreCapability(#[from] ObjectStoreCapabilityError),
    #[error("ingest relation mismatch for {field}: expected `{expected}`, actual `{actual}`")]
    IngestRelationMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("unsupported incremental adapter `{adapter_id}`")]
    UnsupportedIncrementalAdapter { adapter_id: String },
    #[error("unsupported incremental engine backend `{backend:?}`: {reason}")]
    UnsupportedIncrementalEngineBackend {
        backend: IncrementalEngineBackend,
        reason: String,
    },
    #[error("malformed prototype Arrow ingest envelope: {reason}")]
    MalformedPrototypeArrowIngest { reason: String },
}

struct ProductionRecoveryAuthority<'a> {
    capabilities: &'a AuthoritativeObjectStoreCapabilitiesV1,
}

impl<'a> ProductionRecoveryAuthority<'a> {
    fn new(
        capabilities: &'a AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        capabilities.validate_for_startup()?;

        Ok(Self { capabilities })
    }

    fn profile(&self, namespace: AuthoritativeNamespace) -> &ObjectStoreCapabilityProfile {
        self.capabilities
            .profiles
            .get(&namespace)
            .expect("startup capability validation guarantees every authoritative namespace")
    }
}

impl RecoveredRuntime {
    pub async fn recover_bootstrap(store: Arc<dyn ObjectStore>) -> Result<Self, RecoveryError> {
        Self::recover_bootstrap_with_owner(store, ORDERS_SUM_COUNT_OWNER).await
    }

    pub async fn recover_bootstrap_with_owner(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
    ) -> Result<Self, RecoveryError> {
        Self::recover_bootstrap_with_owner_and_relation_catalog(
            store,
            expected_owner,
            orders_sum_count_relation_catalog()?,
        )
        .await
    }

    pub async fn recover_bootstrap_with_owner_and_relation_catalog_record(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<Self, RecoveryError> {
        Self::recover_bootstrap_with_owner_and_relation_catalog_record_using_engine_backend_selection(
            store,
            expected_owner,
            relation_id,
            relation_version,
            default_incremental_engine_backend_selection(),
        )
        .await
    }

    pub async fn recover_bootstrap_with_owner_and_relation_catalog_record_using_engine_backend(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
        engine_backend: IncrementalEngineBackend,
    ) -> Result<Self, RecoveryError> {
        Self::recover_bootstrap_with_owner_and_relation_catalog_record_using_engine_backend_selection(
            store,
            expected_owner,
            relation_id,
            relation_version,
            IncrementalEngineBackendSelection::Explicit(engine_backend),
        )
        .await
    }

    async fn recover_bootstrap_with_owner_and_relation_catalog_record_using_engine_backend_selection(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
        engine_backend: IncrementalEngineBackendSelection,
    ) -> Result<Self, RecoveryError> {
        let relation_catalog = RelationCatalogRegistry::new(Arc::clone(&store))
            .read(relation_id, relation_version)
            .await?;

        Self::recover_bootstrap_with_owner_and_relation_catalog_using_engine_backend_selection(
            store,
            expected_owner,
            relation_catalog,
            engine_backend,
        )
        .await
    }

    pub async fn recover_with_owner_and_relation_catalog_record_checked(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let relation_catalog = RelationCatalogRegistry::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::RelationCatalog),
        )?
        .read(relation_id, relation_version)
        .await?;

        Self::recover_with_owner_and_relation_catalog_checked(
            store,
            expected_owner,
            relation_catalog,
            capabilities,
        )
        .await
    }

    pub async fn recover_bootstrap_with_owner_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        Self::recover_bootstrap_with_owner_and_relation_catalog_using_engine_backend_selection(
            store,
            expected_owner,
            relation_catalog,
            default_incremental_engine_backend_selection(),
        )
        .await
    }

    pub async fn recover_bootstrap_with_owner_and_relation_catalog_using_engine_backend(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        engine_backend: IncrementalEngineBackend,
    ) -> Result<Self, RecoveryError> {
        Self::recover_bootstrap_with_owner_and_relation_catalog_using_engine_backend_selection(
            store,
            expected_owner,
            relation_catalog,
            IncrementalEngineBackendSelection::Explicit(engine_backend),
        )
        .await
    }

    async fn recover_bootstrap_with_owner_and_relation_catalog_using_engine_backend_selection(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        engine_backend: IncrementalEngineBackendSelection,
    ) -> Result<Self, RecoveryError> {
        let publisher = CheckpointPublisher::new(Arc::clone(&store));

        Self::recover_with_publisher_and_relation_catalog_using_engine_backend(
            store,
            publisher,
            expected_owner,
            relation_catalog,
            engine_backend,
        )
        .await
    }

    pub async fn recover_with_owner_and_relation_catalog_checked(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let publisher = CheckpointPublisher::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::Checkpoint),
        )?;
        let ingest_log = IngestLog::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::Ingest),
        )?;

        let recovered = Self::recover_with_publisher_log_and_relation_catalog(
            publisher.clone(),
            ingest_log,
            expected_owner,
            relation_catalog,
            ReplayAdmissionEvidence::DurableAdmissionRequired,
            default_incremental_engine_backend_selection(),
        )
        .await?;
        Self::write_checked_recovery_transition(
            &publisher,
            &recovered,
            CheckpointRecoveryMode::LatestCandidate,
        )
        .await?;

        Ok(recovered)
    }

    pub async fn recover_bootstrap_from_published_checkpoint_version(
        store: Arc<dyn ObjectStore>,
        checkpoint_version: u64,
    ) -> Result<Self, RecoveryError> {
        Self::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(
            store,
            checkpoint_version,
            ORDERS_SUM_COUNT_OWNER,
            orders_sum_count_relation_catalog()?,
        )
        .await
    }

    pub async fn recover_from_published_checkpoint_version_checked(
        store: Arc<dyn ObjectStore>,
        checkpoint_version: u64,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        Self::recover_from_published_checkpoint_version_with_owner_and_relation_catalog_record_checked(
            store,
            checkpoint_version,
            ORDERS_SUM_COUNT_OWNER,
            ORDERS_SUM_COUNT_RELATION_ID,
            ORDERS_SUM_COUNT_RELATION_VERSION,
            capabilities,
        )
        .await
    }

    pub async fn recover_from_published_checkpoint_version_with_owner_and_relation_catalog_record_checked(
        store: Arc<dyn ObjectStore>,
        checkpoint_version: u64,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let relation_catalog = RelationCatalogRegistry::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::RelationCatalog),
        )?
        .read(relation_id, relation_version)
        .await?;

        Self::recover_from_published_checkpoint_version_with_owner_and_relation_catalog_checked(
            store,
            checkpoint_version,
            expected_owner,
            relation_catalog,
            capabilities,
        )
        .await
    }

    pub async fn recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        checkpoint_version: u64,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        let publisher = CheckpointPublisher::new(Arc::clone(&store));
        let manifest = publisher
            .read_published_checkpoint_manifest(checkpoint_version)
            .await?;

        Self::recover_with_selected_manifest_and_relation_catalog(
            store,
            publisher,
            manifest,
            expected_owner,
            relation_catalog,
        )
        .await
    }

    pub async fn recover_from_published_checkpoint_version_with_owner_and_relation_catalog_checked(
        store: Arc<dyn ObjectStore>,
        checkpoint_version: u64,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let publisher = CheckpointPublisher::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::Checkpoint),
        )?;
        let manifest = publisher
            .read_published_checkpoint_manifest(checkpoint_version)
            .await?;
        let ingest_log = IngestLog::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::Ingest),
        )?;

        let recovered = Self::recover_with_selected_manifest_log_and_relation_catalog(
            publisher.clone(),
            ingest_log,
            manifest,
            expected_owner,
            relation_catalog,
            ReplayAdmissionEvidence::DurableAdmissionRequired,
            default_incremental_engine_backend_selection(),
        )
        .await?;
        Self::write_checked_recovery_transition(
            &publisher,
            &recovered,
            CheckpointRecoveryMode::SelectedCheckpoint,
        )
        .await?;

        Ok(recovered)
    }

    pub async fn recover_from_published_checkpoint_version_with_slatedb_state_store_checked(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        checkpoint_version: u64,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        Self::recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog_record_checked(
            store,
            db_path,
            checkpoint_version,
            ORDERS_SUM_COUNT_OWNER,
            ORDERS_SUM_COUNT_RELATION_ID,
            ORDERS_SUM_COUNT_RELATION_VERSION,
            capabilities,
        )
        .await
    }

    pub async fn recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog_record_checked(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        checkpoint_version: u64,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let relation_catalog = RelationCatalogRegistry::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::RelationCatalog),
        )?
        .read(relation_id, relation_version)
        .await?;

        Self::recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog_checked(
            store,
            db_path,
            checkpoint_version,
            expected_owner,
            relation_catalog,
            capabilities,
        )
        .await
    }

    pub async fn recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog_checked(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        checkpoint_version: u64,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let publisher = CheckpointPublisher::with_slatedb_state_store_authoritative(
            Arc::clone(&store),
            db_path,
            capabilities,
        )
        .await?;
        let manifest = publisher
            .read_published_checkpoint_manifest(checkpoint_version)
            .await?;
        let ingest_log = IngestLog::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::Ingest),
        )?;

        let recovered = Self::recover_with_selected_manifest_log_and_relation_catalog(
            publisher.clone(),
            ingest_log,
            manifest,
            expected_owner,
            relation_catalog,
            ReplayAdmissionEvidence::DurableAdmissionRequired,
            default_incremental_engine_backend_selection(),
        )
        .await?;
        Self::write_checked_recovery_transition(
            &publisher,
            &recovered,
            CheckpointRecoveryMode::SelectedCheckpoint,
        )
        .await?;

        Ok(recovered)
    }

    pub async fn recover_bootstrap_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        checkpoint_version: u64,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        let publisher =
            CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), db_path).await?;
        let manifest = publisher
            .read_published_checkpoint_manifest(checkpoint_version)
            .await?;

        Self::recover_with_selected_manifest_and_relation_catalog(
            store,
            publisher,
            manifest,
            expected_owner,
            relation_catalog,
        )
        .await
    }

    pub async fn recover_bootstrap_with_slatedb_state_store_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        let publisher =
            CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), db_path).await?;

        Self::recover_with_publisher_and_relation_catalog(
            store,
            publisher,
            expected_owner,
            relation_catalog,
        )
        .await
    }

    pub async fn recover_with_slatedb_state_store_and_relation_catalog_checked(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let publisher = CheckpointPublisher::with_slatedb_state_store_authoritative(
            Arc::clone(&store),
            db_path,
            capabilities,
        )
        .await?;
        let ingest_log = IngestLog::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::Ingest),
        )?;

        let recovered = Self::recover_with_publisher_log_and_relation_catalog(
            publisher.clone(),
            ingest_log,
            expected_owner,
            relation_catalog,
            ReplayAdmissionEvidence::DurableAdmissionRequired,
            default_incremental_engine_backend_selection(),
        )
        .await?;
        Self::write_checked_recovery_transition(
            &publisher,
            &recovered,
            CheckpointRecoveryMode::SlateDbLatest,
        )
        .await?;

        Ok(recovered)
    }

    pub async fn recover_with_slatedb_state_store_and_catalog_record_checked(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, RecoveryError> {
        let authority = ProductionRecoveryAuthority::new(capabilities)?;
        let relation_catalog = RelationCatalogRegistry::new_checked(
            Arc::clone(&store),
            authority.profile(AuthoritativeNamespace::RelationCatalog),
        )?
        .read(relation_id, relation_version)
        .await?;

        Self::recover_with_slatedb_state_store_and_relation_catalog_checked(
            store,
            db_path,
            expected_owner,
            relation_catalog,
            capabilities,
        )
        .await
    }

    async fn write_checked_recovery_transition(
        publisher: &CheckpointPublisher,
        recovered: &Self,
        recovery_mode: CheckpointRecoveryMode,
    ) -> Result<(), RecoveryError> {
        if let Some(checkpoint_version) = recovered.latest_checkpoint_version {
            publisher
                .write_checkpoint_recovery_transition_record(
                    checkpoint_version,
                    recovery_mode,
                    recovered.replay_checkpoints.len(),
                    recovered.replayed_batch_count,
                )
                .await?;
        }

        Ok(())
    }

    async fn recover_with_publisher_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        publisher: CheckpointPublisher,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        Self::recover_with_publisher_and_relation_catalog_using_engine_backend(
            store,
            publisher,
            expected_owner,
            relation_catalog,
            default_incremental_engine_backend_selection(),
        )
        .await
    }

    async fn recover_with_publisher_and_relation_catalog_using_engine_backend(
        store: Arc<dyn ObjectStore>,
        publisher: CheckpointPublisher,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        engine_backend: IncrementalEngineBackendSelection,
    ) -> Result<Self, RecoveryError> {
        let ingest_log = IngestLog::new(store);
        Self::recover_with_publisher_log_and_relation_catalog(
            publisher,
            ingest_log,
            expected_owner,
            relation_catalog,
            ReplayAdmissionEvidence::EnvelopeOnly,
            engine_backend,
        )
        .await
    }

    async fn recover_with_publisher_log_and_relation_catalog(
        publisher: CheckpointPublisher,
        ingest_log: IngestLog,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        replay_admission_evidence: ReplayAdmissionEvidence,
        engine_backend: IncrementalEngineBackendSelection,
    ) -> Result<Self, RecoveryError> {
        validate_recovery_incremental_adapter_scope(&relation_catalog)?;
        let latest_manifest = publisher.latest_manifest().await?;
        Self::recover_from_manifest_and_relation_catalog(
            publisher,
            ingest_log,
            latest_manifest,
            expected_owner,
            relation_catalog,
            replay_admission_evidence,
            engine_backend,
        )
        .await
    }

    async fn recover_with_selected_manifest_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        publisher: CheckpointPublisher,
        manifest: CheckpointManifest,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        let ingest_log = IngestLog::new(store);
        Self::recover_with_selected_manifest_log_and_relation_catalog(
            publisher,
            ingest_log,
            manifest,
            expected_owner,
            relation_catalog,
            ReplayAdmissionEvidence::EnvelopeOnly,
            default_incremental_engine_backend_selection(),
        )
        .await
    }

    async fn recover_with_selected_manifest_log_and_relation_catalog(
        publisher: CheckpointPublisher,
        ingest_log: IngestLog,
        manifest: CheckpointManifest,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        replay_admission_evidence: ReplayAdmissionEvidence,
        engine_backend: IncrementalEngineBackendSelection,
    ) -> Result<Self, RecoveryError> {
        relation_catalog.validate()?;
        Self::recover_from_manifest_and_relation_catalog(
            publisher,
            ingest_log,
            Some(manifest),
            expected_owner,
            relation_catalog,
            replay_admission_evidence,
            engine_backend,
        )
        .await
    }

    async fn recover_from_manifest_and_relation_catalog(
        publisher: CheckpointPublisher,
        ingest_log: IngestLog,
        manifest: Option<CheckpointManifest>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        replay_admission_evidence: ReplayAdmissionEvidence,
        engine_backend: IncrementalEngineBackendSelection,
    ) -> Result<Self, RecoveryError> {
        validate_recovery_incremental_adapter_scope(&relation_catalog)?;
        let aggregate_value_mode = aggregate_value_mode_for_sum_count_catalog(&relation_catalog)?;
        let engine_backend = resolve_incremental_engine_backend(
            engine_backend,
            &relation_catalog,
            aggregate_value_mode,
        );
        let mut materialized = RuntimeIncrementalEngine::new_for_catalog(
            engine_backend,
            &relation_catalog,
            aggregate_value_mode,
        )?;

        if let Some(manifest) = manifest.as_ref() {
            let mut checkpointed_state = DeltaBatch::default();
            let mut checkpoint_logical_epoch = None;
            for state_ref in &manifest.state_objects {
                if state_ref.owner != expected_owner {
                    return Err(RecoveryError::UnexpectedStateOwner {
                        actual: state_ref.owner.clone(),
                        expected: expected_owner.to_string(),
                    });
                }

                let bytes = publisher.read_state_object(state_ref).await?;
                match decode_checkpoint_state(&bytes)? {
                    DecodedCheckpointState::Versioned(checkpoint) => {
                        let logical_epoch = checkpoint.logical_epoch();
                        if let Some(expected) = checkpoint_logical_epoch {
                            if expected != logical_epoch {
                                return Err(RecoveryError::InconsistentCheckpointLogicalEpoch {
                                    expected,
                                    actual: logical_epoch,
                                });
                            }
                        } else {
                            checkpoint_logical_epoch = Some(logical_epoch);
                        }

                        checkpointed_state = checkpointed_state.combine(checkpoint.state());
                    }
                    DecodedCheckpointState::Legacy(state) => {
                        checkpointed_state = checkpointed_state.combine(&state);
                    }
                }
            }
            let logical_epoch = checkpoint_logical_epoch.unwrap_or(manifest.checkpoint_version);
            materialized = RuntimeIncrementalEngine::from_checkpoint_for_catalog(
                engine_backend,
                &relation_catalog,
                aggregate_value_mode,
                EngineCheckpoint::new(logical_epoch, checkpointed_state),
            )?;
        }

        let replay_checkpoints = replay_checkpoints(manifest.as_ref());
        let replayed = match replay_admission_evidence {
            ReplayAdmissionEvidence::EnvelopeOnly => {
                ingest_log
                    .replay_validated_envelopes_from(&replay_checkpoints)
                    .await?
            }
            ReplayAdmissionEvidence::DurableAdmissionRequired => {
                ingest_log
                    .replay_admitted_validated_envelopes_from(&replay_checkpoints)
                    .await?
            }
        };
        let replayed_batch_count = replayed.len();
        let mut logical_epoch = materialized.logical_epoch();

        for batch in replayed {
            let envelope =
                IngestEnvelope::decode(batch.payload().clone()).map_err(IngestLogError::from)?;
            let input = prototype_delta_batch_from_arrow_envelope(&envelope, &relation_catalog)?;
            logical_epoch = logical_epoch
                .checked_add(1)
                .ok_or(RecoveryError::LogicalEpochOverflow)?;
            materialized.push_changes(logical_epoch, &input)?;
        }

        Ok(Self {
            materialized,
            replay_checkpoints,
            replayed_batch_count,
            latest_checkpoint_version: manifest.map(|manifest| manifest.checkpoint_version),
        })
    }

    pub fn materialized_state(&self) -> DeltaBatch {
        self.materialized.materialized_state()
    }

    pub fn engine_backend(&self) -> IncrementalEngineBackend {
        self.materialized.backend()
    }

    pub fn logical_epoch(&self) -> LogicalEpoch {
        self.materialized.logical_epoch()
    }

    pub fn replay_checkpoints(&self) -> &[ReplayCheckpoint] {
        &self.replay_checkpoints
    }

    pub fn replayed_batch_count(&self) -> usize {
        self.replayed_batch_count
    }

    pub fn latest_checkpoint_version(&self) -> Option<u64> {
        self.latest_checkpoint_version
    }
}

enum DecodedCheckpointState {
    Versioned(EngineCheckpoint),
    Legacy(DeltaBatch),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayAdmissionEvidence {
    EnvelopeOnly,
    DurableAdmissionRequired,
}

fn decode_checkpoint_state(bytes: &[u8]) -> Result<DecodedCheckpointState, RecoveryError> {
    // Checkpoint state has a separate compatibility lifecycle from durable
    // ingest; the legacy DeltaBatch fallback remains intentionally scoped here.
    match serde_json::from_slice::<EngineCheckpointPayload>(bytes) {
        Ok(payload) => {
            if payload.schema_version() != ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION {
                return Err(RecoveryError::UnsupportedEngineCheckpointPayloadSchema(
                    payload.schema_version(),
                ));
            }

            Ok(DecodedCheckpointState::Versioned(payload.into_checkpoint()))
        }
        Err(versioned_error) => match serde_json::from_slice::<DeltaBatch>(bytes) {
            Ok(state) => Ok(DecodedCheckpointState::Legacy(state)),
            Err(_) => Err(RecoveryError::Json(versioned_error)),
        },
    }
}

fn replay_checkpoints(manifest: Option<&CheckpointManifest>) -> Vec<ReplayCheckpoint> {
    manifest
        .into_iter()
        .flat_map(|manifest| &manifest.input_ranges)
        .map(|range| {
            ReplayCheckpoint::new(
                range.stream_id.clone(),
                range.partition_id,
                range.end_offset_exclusive,
            )
        })
        .collect()
}

fn prototype_delta_batch_from_arrow_envelope(
    envelope: &IngestEnvelope,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, RecoveryError> {
    let header = envelope.header();
    let batches = envelope.record_batches().map_err(IngestLogError::from)?;

    arrow_record_batches_to_single_key_sum_count_delta_batch(
        catalog,
        header.relation_id.as_str(),
        header.relation_version.as_str(),
        header.schema_fingerprint.as_str(),
        &batches,
    )
    .map_err(recovery_error_from_incremental_input)
}

fn aggregate_value_mode_for_sum_count_catalog(
    catalog: &VelorixRelationCatalogV1,
) -> Result<AggregateValueMode, RecoveryError> {
    if supported_incremental_adapter_spec(&catalog.incremental_adapter.adapter_id).is_none() {
        return Err(RecoveryError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        });
    }

    let mut value_columns = catalog
        .relation_schema
        .columns
        .iter()
        .filter(|column| column.semantic_role == RelationSemanticRoleV1::Value);
    let Some(column) = value_columns.next() else {
        return Err(RecoveryError::MalformedPrototypeArrowIngest {
            reason: "prototype adapter supports exactly one value column".to_string(),
        });
    };
    if value_columns.next().is_some() {
        return Err(RecoveryError::MalformedPrototypeArrowIngest {
            reason: "prototype adapter supports exactly one value column".to_string(),
        });
    }

    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(AggregateValueMode::Integer),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            Ok(AggregateValueMode::Decimal128 {
                precision: *precision,
                scale: *scale,
            })
        }
        _ => Err(RecoveryError::MalformedPrototypeArrowIngest {
            reason: format!(
                "prototype sum/count runtime value column `{}` must be Int64 or Decimal128",
                column.name
            ),
        }),
    }
}

fn default_incremental_engine_backend_selection() -> IncrementalEngineBackendSelection {
    match std::env::var("VELORIX_INCREMENTAL_ENGINE").ok().as_deref() {
        Some("prototype") => {
            IncrementalEngineBackendSelection::Explicit(IncrementalEngineBackend::Prototype)
        }
        _ => IncrementalEngineBackendSelection::Default,
    }
}

fn resolve_incremental_engine_backend(
    selection: IncrementalEngineBackendSelection,
    _catalog: &VelorixRelationCatalogV1,
    _aggregate_value_mode: AggregateValueMode,
) -> IncrementalEngineBackend {
    match selection {
        IncrementalEngineBackendSelection::Explicit(backend) => backend,
        IncrementalEngineBackendSelection::Default => IncrementalEngineBackend::Prototype,
    }
}

fn validate_recovery_incremental_adapter_scope(
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), RecoveryError> {
    catalog
        .validate_supported_incremental_adapter_scope()
        .map(|_| ())
        .map_err(|error| recovery_error_from_adapter_scope(catalog, error))
}

fn recovery_error_from_adapter_scope(
    catalog: &VelorixRelationCatalogV1,
    error: RelationSchemaError,
) -> RecoveryError {
    match error {
        RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.adapter_id",
        } => RecoveryError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        },
        RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.value_column",
        } => RecoveryError::MalformedPrototypeArrowIngest {
            reason: "relation catalog must define one value column".to_string(),
        },
        RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.value_columns",
        } => RecoveryError::MalformedPrototypeArrowIngest {
            reason: "prototype adapter supports exactly one value column".to_string(),
        },
        RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.primary_key_column_ids",
        } => RecoveryError::MalformedPrototypeArrowIngest {
            reason: "prototype adapter supports exactly one primary key column".to_string(),
        },
        error => RecoveryError::RelationCatalog(error),
    }
}

fn recovery_error_from_incremental_input(error: IncrementalInputAdapterError) -> RecoveryError {
    match error {
        IncrementalInputAdapterError::RelationCatalog(error) => {
            RecoveryError::RelationCatalog(error)
        }
        IncrementalInputAdapterError::IngestRelationMismatch {
            field,
            expected,
            actual,
        } => RecoveryError::IngestRelationMismatch {
            field,
            expected,
            actual,
        },
        IncrementalInputAdapterError::UnsupportedIncrementalAdapter { adapter_id } => {
            RecoveryError::UnsupportedIncrementalAdapter { adapter_id }
        }
        IncrementalInputAdapterError::MalformedArrowInput { reason } => {
            RecoveryError::MalformedPrototypeArrowIngest { reason }
        }
    }
}

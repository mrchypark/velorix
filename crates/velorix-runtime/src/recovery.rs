use std::sync::Arc;

use object_store::{path::Path, ObjectStore};
use thiserror::Error;
use velorix_core::{
    delta::DeltaBatch,
    engine::{
        EngineCheckpoint, EngineCheckpointPayload, EngineError, IncrementalEngine, LogicalEpoch,
        PrototypeIncrementalEngine, ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
    },
    relation::{
        arrow_record_batches_to_single_key_sum_count_delta_batch, ArrowPhysicalTypeV1,
        DataFusionRegistrationModeV1, DataFusionRegistrationV1, FelderaRelationBindingV1,
        IncrementalAdapterBindingV1, IncrementalInputAdapterError, RelationColumnV1,
        RelationOperationV1, RelationSchemaError, RelationSemanticRoleV1, SchemaFingerprintV1,
        VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
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

pub const ORDERS_SUM_COUNT_OWNER: &str = "orders_sum_count";
pub const ORDERS_SUM_COUNT_RELATION_ID: &str = "orders";
pub const ORDERS_SUM_COUNT_RELATION_VERSION: &str = "2026-05-05.v1";
pub const ORDERS_SUM_COUNT_ADAPTER_ID: &str = ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID;

#[derive(Clone, Debug)]
pub struct RecoveredRuntime {
    materialized: PrototypeIncrementalEngine,
    replay_checkpoints: Vec<ReplayCheckpoint>,
    replayed_batch_count: usize,
    latest_checkpoint_version: Option<u64>,
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
    pub async fn recover(store: Arc<dyn ObjectStore>) -> Result<Self, RecoveryError> {
        Self::recover_with_owner(store, ORDERS_SUM_COUNT_OWNER).await
    }

    pub async fn recover_with_owner(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
    ) -> Result<Self, RecoveryError> {
        Self::recover_with_owner_and_relation_catalog(
            store,
            expected_owner,
            orders_sum_count_relation_catalog()?,
        )
        .await
    }

    pub async fn recover_with_owner_and_relation_catalog_record(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<Self, RecoveryError> {
        let relation_catalog = RelationCatalogRegistry::new(Arc::clone(&store))
            .read(relation_id, relation_version)
            .await?;

        Self::recover_with_owner_and_relation_catalog(store, expected_owner, relation_catalog).await
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

    pub async fn recover_with_owner_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        let publisher = CheckpointPublisher::new(Arc::clone(&store));

        Self::recover_with_publisher_and_relation_catalog(
            store,
            publisher,
            expected_owner,
            relation_catalog,
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

    pub async fn recover_from_published_checkpoint_version(
        store: Arc<dyn ObjectStore>,
        checkpoint_version: u64,
    ) -> Result<Self, RecoveryError> {
        Self::recover_from_published_checkpoint_version_with_owner_and_relation_catalog(
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

    pub async fn recover_from_published_checkpoint_version_with_owner_and_relation_catalog(
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
        let publisher = CheckpointPublisher::with_slatedb_state_store_checked(
            Arc::clone(&store),
            db_path,
            authority.profile(AuthoritativeNamespace::Checkpoint),
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

    pub async fn recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(
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

    pub async fn recover_with_slatedb_state_store_and_relation_catalog(
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
        let publisher = CheckpointPublisher::with_slatedb_state_store_checked(
            Arc::clone(&store),
            db_path,
            authority.profile(AuthoritativeNamespace::Checkpoint),
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
        let ingest_log = IngestLog::new(store);
        Self::recover_with_publisher_log_and_relation_catalog(
            publisher,
            ingest_log,
            expected_owner,
            relation_catalog,
            ReplayAdmissionEvidence::EnvelopeOnly,
        )
        .await
    }

    async fn recover_with_publisher_log_and_relation_catalog(
        publisher: CheckpointPublisher,
        ingest_log: IngestLog,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
        replay_admission_evidence: ReplayAdmissionEvidence,
    ) -> Result<Self, RecoveryError> {
        relation_catalog.validate()?;
        let latest_manifest = publisher.latest_manifest().await?;
        Self::recover_from_manifest_and_relation_catalog(
            publisher,
            ingest_log,
            latest_manifest,
            expected_owner,
            relation_catalog,
            replay_admission_evidence,
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
    ) -> Result<Self, RecoveryError> {
        relation_catalog.validate()?;
        Self::recover_from_manifest_and_relation_catalog(
            publisher,
            ingest_log,
            Some(manifest),
            expected_owner,
            relation_catalog,
            replay_admission_evidence,
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
    ) -> Result<Self, RecoveryError> {
        let mut materialized = PrototypeIncrementalEngine::new();

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
            materialized = PrototypeIncrementalEngine::from_checkpoint(EngineCheckpoint::new(
                logical_epoch,
                checkpointed_state,
            ))?;
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

pub fn orders_sum_count_relation_catalog() -> Result<VelorixRelationCatalogV1, RelationSchemaError>
{
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
        relation_name: "orders".to_string(),
        relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)?;

    Ok(VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: ORDERS_SUM_COUNT_ADAPTER_ID.to_string(),
        },
    })
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

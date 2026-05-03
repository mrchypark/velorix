use std::sync::Arc;

use object_store::ObjectStore;
use thiserror::Error;
use velorix_core::{
    delta::DeltaBatch,
    engine::{
        EngineCheckpoint, EngineCheckpointPayload, EngineError, IncrementalEngine, LogicalEpoch,
        PrototypeIncrementalEngine, ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
    },
};
use velorix_storage::{
    log::{IngestLog, IngestLogError, ReplayCheckpoint},
    manifest::CheckpointManifest,
    state::{CheckpointPublishError, CheckpointPublisher},
};

pub const ORDERS_SUM_COUNT_OWNER: &str = "orders_sum_count";

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
}

impl RecoveredRuntime {
    pub async fn recover(store: Arc<dyn ObjectStore>) -> Result<Self, RecoveryError> {
        Self::recover_with_owner(store, ORDERS_SUM_COUNT_OWNER).await
    }

    pub async fn recover_with_owner(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
    ) -> Result<Self, RecoveryError> {
        let publisher = CheckpointPublisher::new(Arc::clone(&store));
        let latest_manifest = publisher.latest_manifest().await?;
        let mut materialized = PrototypeIncrementalEngine::new();

        if let Some(manifest) = latest_manifest.as_ref() {
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

        let replay_checkpoints = replay_checkpoints(latest_manifest.as_ref());
        let ingest_log = IngestLog::new(store);
        let replayed = ingest_log.replay_from(&replay_checkpoints).await?;
        let replayed_batch_count = replayed.len();
        let mut logical_epoch = materialized.logical_epoch();

        for batch in replayed {
            let input = serde_json::from_slice::<DeltaBatch>(batch.payload())?;
            logical_epoch = logical_epoch
                .checked_add(1)
                .ok_or(RecoveryError::LogicalEpochOverflow)?;
            materialized.push_changes(logical_epoch, &input)?;
        }

        Ok(Self {
            materialized,
            replay_checkpoints,
            replayed_batch_count,
            latest_checkpoint_version: latest_manifest.map(|manifest| manifest.checkpoint_version),
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

enum DecodedCheckpointState {
    Versioned(EngineCheckpoint),
    Legacy(DeltaBatch),
}

fn decode_checkpoint_state(bytes: &[u8]) -> Result<DecodedCheckpointState, RecoveryError> {
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

use std::sync::Arc;

use object_store::ObjectStore;
use thiserror::Error;
use velorix_core::{
    delta::DeltaBatch,
    operator::{KeyedSumCountAggregate, OperatorError},
};
use velorix_storage::{
    log::{IngestLog, IngestLogError, ReplayCheckpoint},
    manifest::CheckpointManifest,
    state::{CheckpointPublishError, CheckpointPublisher},
};

pub const ORDERS_SUM_COUNT_OWNER: &str = "orders_sum_count";

#[derive(Clone, Debug)]
pub struct RecoveredRuntime {
    materialized: KeyedSumCountAggregate,
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
    Operator(#[from] OperatorError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("unexpected state object owner `{actual}`, expected `{expected}`")]
    UnexpectedStateOwner { actual: String, expected: String },
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
        let mut materialized = KeyedSumCountAggregate::new();

        if let Some(manifest) = latest_manifest.as_ref() {
            let mut checkpointed_state = DeltaBatch::default();
            for state_ref in &manifest.state_objects {
                if state_ref.owner != expected_owner {
                    return Err(RecoveryError::UnexpectedStateOwner {
                        actual: state_ref.owner.clone(),
                        expected: expected_owner.to_string(),
                    });
                }

                let bytes = publisher.read_state_object(state_ref).await?;
                let state = serde_json::from_slice::<DeltaBatch>(&bytes)?;
                checkpointed_state = checkpointed_state.combine(&state);
            }
            materialized = KeyedSumCountAggregate::from_state(&checkpointed_state)?;
        }

        let replay_checkpoints = replay_checkpoints(latest_manifest.as_ref());
        let ingest_log = IngestLog::new(store);
        let replayed = ingest_log.replay_from(&replay_checkpoints).await?;
        let replayed_batch_count = replayed.len();

        for batch in replayed {
            let input = serde_json::from_slice::<DeltaBatch>(batch.payload())?;
            materialized.apply(&input)?;
        }

        Ok(Self {
            materialized,
            replay_checkpoints,
            replayed_batch_count,
            latest_checkpoint_version: latest_manifest.map(|manifest| manifest.checkpoint_version),
        })
    }

    pub fn materialized_state(&self) -> DeltaBatch {
        self.materialized.state()
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

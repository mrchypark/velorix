use std::{collections::BTreeMap, sync::Arc};

use object_store::ObjectStore;
use serde_json::Value;
use thiserror::Error;
use velorix_core::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue, DeltaWeight};
use velorix_storage::{
    log::{IngestLog, IngestLogError, ReplayCheckpoint},
    manifest::CheckpointManifest,
    state::{CheckpointPublishError, CheckpointPublisher},
};

#[derive(Clone, Debug)]
pub struct RecoveredRuntime {
    materialized: MaterializedSumCount,
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
    Json(#[from] serde_json::Error),
    #[error("materialized aggregate value must contain integer `sum` and `count` fields")]
    InvalidMaterializedAggregateValue,
    #[error("ingest delta value must be a signed integer")]
    NonIntegerIngestValue,
    #[error("aggregate arithmetic overflowed")]
    ArithmeticOverflow,
}

impl RecoveredRuntime {
    pub async fn recover(store: Arc<dyn ObjectStore>) -> Result<Self, RecoveryError> {
        let publisher = CheckpointPublisher::new(Arc::clone(&store));
        let latest_manifest = publisher.latest_manifest().await?;
        let mut materialized = MaterializedSumCount::default();

        if let Some(manifest) = latest_manifest.as_ref() {
            for state_ref in &manifest.state_objects {
                let bytes = publisher.read_state_object(state_ref).await?;
                let state = serde_json::from_slice::<DeltaBatch>(&bytes)?;
                materialized.apply_materialized_state(&state)?;
            }
        }

        let replay_checkpoints = replay_checkpoints(latest_manifest.as_ref());
        let ingest_log = IngestLog::new(store);
        let replayed = ingest_log.replay_from(&replay_checkpoints).await?;
        let replayed_batch_count = replayed.len();

        for batch in replayed {
            let input = serde_json::from_slice::<DeltaBatch>(batch.payload())?;
            materialized.apply_ingest_delta(&input)?;
        }

        Ok(Self {
            materialized,
            replay_checkpoints,
            replayed_batch_count,
            latest_checkpoint_version: latest_manifest.map(|manifest| manifest.checkpoint_version),
        })
    }

    pub fn materialized_state(&self) -> DeltaBatch {
        self.materialized.as_delta_batch()
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

#[derive(Clone, Debug, Default)]
struct MaterializedSumCount {
    entries: BTreeMap<String, MaterializedEntry>,
}

impl MaterializedSumCount {
    fn apply_materialized_state(&mut self, state: &DeltaBatch) -> Result<(), RecoveryError> {
        for record in state.records() {
            let (sum, count) = materialized_sum_count(record.value.as_json())?;
            let key = canonical_json(record.key.as_json());
            let entry = self
                .entries
                .entry(key.clone())
                .or_insert_with(|| MaterializedEntry::new(record.key.clone()));
            entry.add_weighted(sum, count, record.weight)?;
            if entry.is_zero() {
                self.entries.remove(&key);
            }
        }

        Ok(())
    }

    fn apply_ingest_delta(&mut self, input: &DeltaBatch) -> Result<(), RecoveryError> {
        for record in input.records() {
            let amount = record
                .value
                .as_json()
                .as_i64()
                .ok_or(RecoveryError::NonIntegerIngestValue)?;
            let key = canonical_json(record.key.as_json());
            let entry = self
                .entries
                .entry(key.clone())
                .or_insert_with(|| MaterializedEntry::new(record.key.clone()));
            entry.add_ingest(amount, record.weight)?;
            if entry.is_zero() {
                self.entries.remove(&key);
            }
        }

        Ok(())
    }

    fn as_delta_batch(&self) -> DeltaBatch {
        DeltaBatch::from_records(
            self.entries
                .values()
                .map(MaterializedEntry::to_record)
                .collect::<Result<Vec<_>, _>>()
                .expect("stored materialized state must fit delta records"),
        )
    }
}

#[derive(Clone, Debug)]
struct MaterializedEntry {
    key: DeltaKey,
    sum: i128,
    count: i128,
}

impl MaterializedEntry {
    fn new(key: DeltaKey) -> Self {
        Self {
            key,
            sum: 0,
            count: 0,
        }
    }

    fn add_ingest(&mut self, amount: i64, weight: DeltaWeight) -> Result<(), RecoveryError> {
        let sum_delta = i128::from(amount)
            .checked_mul(i128::from(weight))
            .ok_or(RecoveryError::ArithmeticOverflow)?;
        self.sum = self
            .sum
            .checked_add(sum_delta)
            .ok_or(RecoveryError::ArithmeticOverflow)?;
        self.count = self
            .count
            .checked_add(i128::from(weight))
            .ok_or(RecoveryError::ArithmeticOverflow)?;
        Ok(())
    }

    fn add_weighted(
        &mut self,
        sum: i64,
        count: i64,
        weight: DeltaWeight,
    ) -> Result<(), RecoveryError> {
        let sum_delta = i128::from(sum)
            .checked_mul(i128::from(weight))
            .ok_or(RecoveryError::ArithmeticOverflow)?;
        let count_delta = i128::from(count)
            .checked_mul(i128::from(weight))
            .ok_or(RecoveryError::ArithmeticOverflow)?;
        self.sum = self
            .sum
            .checked_add(sum_delta)
            .ok_or(RecoveryError::ArithmeticOverflow)?;
        self.count = self
            .count
            .checked_add(count_delta)
            .ok_or(RecoveryError::ArithmeticOverflow)?;
        Ok(())
    }

    fn is_zero(&self) -> bool {
        self.sum == 0 && self.count == 0
    }

    fn to_record(&self) -> Result<DeltaRecord, RecoveryError> {
        let sum: i64 = self
            .sum
            .try_into()
            .map_err(|_| RecoveryError::ArithmeticOverflow)?;
        let count: i64 = self
            .count
            .try_into()
            .map_err(|_| RecoveryError::ArithmeticOverflow)?;

        Ok(DeltaRecord::new(
            self.key.clone(),
            DeltaValue::from_json(serde_json::json!({
                "sum": sum,
                "count": count,
            })),
            1,
        ))
    }
}

fn materialized_sum_count(value: &Value) -> Result<(i64, i64), RecoveryError> {
    let sum = value
        .get("sum")
        .and_then(Value::as_i64)
        .ok_or(RecoveryError::InvalidMaterializedAggregateValue)?;
    let count = value
        .get("count")
        .and_then(Value::as_i64)
        .ok_or(RecoveryError::InvalidMaterializedAggregateValue)?;

    Ok((sum, count))
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("serializing JSON scalar cannot fail")
        }
        Value::Array(values) => {
            let items = values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{items}]")
        }
        Value::Object(values) => {
            let mut fields = values
                .iter()
                .map(|(key, value)| {
                    let key = serde_json::to_string(key).expect("serializing JSON key cannot fail");
                    format!("{key}:{}", canonical_json(value))
                })
                .collect::<Vec<_>>();
            fields.sort();
            let fields = fields.join(",");
            format!("{{{fields}}}")
        }
    }
}

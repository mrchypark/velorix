use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

pub use crate::operator::AggregateValueMode;
use crate::{
    delta::DeltaBatch,
    operator::{KeyedSumCountAggregate, OperatorError, PreparedAggregateChanges},
};
use serde::{Deserialize, Serialize};

pub type LogicalEpoch = u64;
pub const ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineCheckpoint {
    logical_epoch: LogicalEpoch,
    state: DeltaBatch,
}

impl EngineCheckpoint {
    pub fn new(logical_epoch: LogicalEpoch, state: DeltaBatch) -> Self {
        Self {
            logical_epoch,
            state,
        }
    }

    pub fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    pub fn state(&self) -> &DeltaBatch {
        &self.state
    }

    pub fn to_payload(&self) -> EngineCheckpointPayload {
        EngineCheckpointPayload::from_checkpoint(self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EngineCheckpointPayload {
    schema_version: u32,
    logical_epoch: LogicalEpoch,
    state: DeltaBatch,
}

impl EngineCheckpointPayload {
    pub fn from_checkpoint(checkpoint: &EngineCheckpoint) -> Self {
        Self {
            schema_version: ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            logical_epoch: checkpoint.logical_epoch(),
            state: checkpoint.state().clone(),
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn into_checkpoint(self) -> EngineCheckpoint {
        EngineCheckpoint::new(self.logical_epoch, self.state)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EngineError {
    #[error(transparent)]
    Operator(#[from] OperatorError),
    #[error("logical epoch must increase monotonically: current={current}, attempted={attempted}")]
    NonMonotonicLogicalEpoch {
        current: LogicalEpoch,
        attempted: LogicalEpoch,
    },
    #[error("prepared aggregate epoch belongs to a different kernel")]
    PreparedEpochWrongKernel,
    #[error("prepared aggregate epoch is stale: current={current}, prepared_base={prepared_base}")]
    PreparedEpochStale {
        current: LogicalEpoch,
        prepared_base: LogicalEpoch,
    },
}

pub trait IncrementalEngine {
    fn logical_epoch(&self) -> LogicalEpoch;

    fn push_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        signed_input_changes: &DeltaBatch,
    ) -> Result<DeltaBatch, EngineError>;

    fn materialized_state(&self) -> DeltaBatch;

    fn checkpoint_state(&self) -> EngineCheckpoint;

    fn from_checkpoint(checkpoint: EngineCheckpoint) -> Result<Self, EngineError>
    where
        Self: Sized;
}

#[derive(Debug)]
pub struct KeyedAggregateKernel {
    logical_epoch: LogicalEpoch,
    aggregate: KeyedSumCountAggregate,
    instance_id: u64,
}

static NEXT_KERNEL_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn next_kernel_instance_id() -> u64 {
    NEXT_KERNEL_INSTANCE_ID.fetch_add(1, Ordering::Relaxed)
}

impl Default for KeyedAggregateKernel {
    fn default() -> Self {
        Self {
            logical_epoch: 0,
            aggregate: KeyedSumCountAggregate::default(),
            instance_id: next_kernel_instance_id(),
        }
    }
}

impl Clone for KeyedAggregateKernel {
    fn clone(&self) -> Self {
        Self {
            logical_epoch: self.logical_epoch,
            aggregate: self.aggregate.clone(),
            instance_id: next_kernel_instance_id(),
        }
    }
}

/// A monotonically numbered aggregate epoch whose mutations are deferred
/// until commit.  Keeping this opaque prevents a caller from observing or
/// partially applying its internal touched-key overlay.
#[derive(Debug)]
pub struct PreparedKeyedAggregateEpoch {
    logical_epoch: LogicalEpoch,
    base_epoch: LogicalEpoch,
    kernel_instance_id: u64,
    changes: PreparedAggregateChanges,
}

impl PreparedKeyedAggregateEpoch {
    pub fn output_changes(&self) -> &DeltaBatch {
        self.changes.output_changes()
    }
}

impl KeyedAggregateKernel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_aggregate_value_mode(value_mode: AggregateValueMode) -> Self {
        Self::with_aggregate_value_mode_and_extrema(value_mode, false)
    }

    pub fn with_aggregate_value_mode_and_extrema(
        value_mode: AggregateValueMode,
        track_extrema: bool,
    ) -> Self {
        Self {
            logical_epoch: 0,
            aggregate: KeyedSumCountAggregate::with_value_mode_and_extrema(
                value_mode,
                track_extrema,
            ),
            instance_id: next_kernel_instance_id(),
        }
    }

    pub fn from_checkpoint_with_aggregate_value_mode(
        checkpoint: EngineCheckpoint,
        value_mode: AggregateValueMode,
    ) -> Result<Self, EngineError> {
        Self::from_checkpoint_with_aggregate_value_mode_and_extrema(checkpoint, value_mode, false)
    }

    pub fn from_checkpoint_with_aggregate_value_mode_and_extrema(
        checkpoint: EngineCheckpoint,
        value_mode: AggregateValueMode,
        track_extrema: bool,
    ) -> Result<Self, EngineError> {
        Ok(Self {
            logical_epoch: checkpoint.logical_epoch,
            aggregate: KeyedSumCountAggregate::from_state_with_value_mode_and_extrema(
                &checkpoint.state,
                value_mode,
                track_extrema,
            )?,
            instance_id: next_kernel_instance_id(),
        })
    }

    pub fn prepare_epoch(
        &self,
        logical_epoch: LogicalEpoch,
        signed_input_changes: &DeltaBatch,
    ) -> Result<PreparedKeyedAggregateEpoch, EngineError> {
        if logical_epoch <= self.logical_epoch {
            return Err(EngineError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }
        Ok(PreparedKeyedAggregateEpoch {
            logical_epoch,
            base_epoch: self.logical_epoch,
            kernel_instance_id: self.instance_id,
            changes: self.aggregate.prepare(signed_input_changes)?,
        })
    }

    /// Applies a token produced by this exact kernel generation.  All checks
    /// occur before mutation, so stale, out-of-order, and cross-kernel tokens
    /// fail closed in release builds.
    pub fn commit_prepared_epoch(
        &mut self,
        prepared: PreparedKeyedAggregateEpoch,
    ) -> Result<DeltaBatch, EngineError> {
        self.validate_prepared_epoch(&prepared)?;
        let output = self.aggregate.commit(prepared.changes);
        self.logical_epoch = prepared.logical_epoch;
        Ok(output)
    }

    /// Validates an opaque token without changing aggregate state, enabling a
    /// caller to fence every participant before an atomic publication step.
    pub fn validate_prepared_epoch(
        &self,
        prepared: &PreparedKeyedAggregateEpoch,
    ) -> Result<(), EngineError> {
        if prepared.kernel_instance_id != self.instance_id {
            return Err(EngineError::PreparedEpochWrongKernel);
        }
        if prepared.base_epoch != self.logical_epoch {
            return Err(EngineError::PreparedEpochStale {
                current: self.logical_epoch,
                prepared_base: prepared.base_epoch,
            });
        }
        Ok(())
    }
}

impl IncrementalEngine for KeyedAggregateKernel {
    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn push_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        signed_input_changes: &DeltaBatch,
    ) -> Result<DeltaBatch, EngineError> {
        let prepared = self.prepare_epoch(logical_epoch, signed_input_changes)?;
        self.commit_prepared_epoch(prepared)
    }

    fn materialized_state(&self) -> DeltaBatch {
        self.aggregate.state()
    }

    fn checkpoint_state(&self) -> EngineCheckpoint {
        EngineCheckpoint::new(self.logical_epoch, self.materialized_state())
    }

    fn from_checkpoint(checkpoint: EngineCheckpoint) -> Result<Self, EngineError> {
        Self::from_checkpoint_with_aggregate_value_mode(checkpoint, AggregateValueMode::Integer)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::delta::{DeltaKey, DeltaRecord, DeltaValue};

    fn input() -> DeltaBatch {
        DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!("key")),
            DeltaValue::from_json(json!(7)),
            1,
        )])
    }

    #[test]
    fn stale_prepared_epoch_rejects_without_mutation() {
        let mut kernel = KeyedAggregateKernel::new();
        let first = kernel.prepare_epoch(1, &input()).unwrap();
        let second = kernel.prepare_epoch(2, &DeltaBatch::default()).unwrap();

        kernel.commit_prepared_epoch(second).unwrap();
        let before = kernel.checkpoint_state();
        assert_eq!(
            kernel.commit_prepared_epoch(first),
            Err(EngineError::PreparedEpochStale {
                current: 2,
                prepared_base: 0,
            })
        );
        assert_eq!(kernel.checkpoint_state(), before);
    }

    #[test]
    fn cross_kernel_prepared_epoch_rejects_without_mutation() {
        let source = KeyedAggregateKernel::new();
        let prepared = source.prepare_epoch(1, &input()).unwrap();
        let mut other = KeyedAggregateKernel::new();
        let before = other.checkpoint_state();

        assert_eq!(
            other.commit_prepared_epoch(prepared),
            Err(EngineError::PreparedEpochWrongKernel)
        );
        assert_eq!(other.checkpoint_state(), before);
    }

    #[test]
    fn prepared_epoch_is_consumed_by_its_single_commit() {
        let mut kernel = KeyedAggregateKernel::new();
        let prepared = kernel.prepare_epoch(1, &input()).unwrap();
        kernel.commit_prepared_epoch(prepared).unwrap();
        // `PreparedKeyedAggregateEpoch` is opaque and non-Clone, so a second
        // commit of the same token is prevented by Rust ownership. A new
        // token still follows the normal monotonic commit path.
        kernel
            .commit_prepared_epoch(kernel.prepare_epoch(2, &DeltaBatch::default()).unwrap())
            .unwrap();
        assert_eq!(kernel.logical_epoch(), 2);
    }
}

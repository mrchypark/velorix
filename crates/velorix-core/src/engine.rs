use thiserror::Error;

pub use crate::operator::AggregateValueMode;
use crate::{
    delta::DeltaBatch,
    operator::{KeyedSumCountAggregate, OperatorError},
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

#[derive(Clone, Debug, Default)]
pub struct PrototypeIncrementalEngine {
    logical_epoch: LogicalEpoch,
    aggregate: KeyedSumCountAggregate,
}

impl PrototypeIncrementalEngine {
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
        })
    }
}

impl IncrementalEngine for PrototypeIncrementalEngine {
    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn push_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        signed_input_changes: &DeltaBatch,
    ) -> Result<DeltaBatch, EngineError> {
        if logical_epoch <= self.logical_epoch {
            return Err(EngineError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }

        let output_changes = self.aggregate.apply(signed_input_changes)?;
        self.logical_epoch = logical_epoch;
        Ok(output_changes)
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

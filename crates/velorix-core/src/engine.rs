use thiserror::Error;

use crate::{
    delta::DeltaBatch,
    operator::{KeyedSumCountAggregate, OperatorError},
};

pub type LogicalEpoch = u64;

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
        Ok(Self {
            logical_epoch: checkpoint.logical_epoch,
            aggregate: KeyedSumCountAggregate::from_state(&checkpoint.state)?,
        })
    }
}

use crate::engine::LogicalEpoch;
use crate::feldera_artifact::RelationSchema;
use crate::standing_program::{
    EpochCommit, EpochIdempotencyKey, MaterializedViewPage, RelationInputBatch, RuntimeCheckpoint,
    ScopedViewId, SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
    StandingProgramRuntimeError,
};

pub trait FelderaExecutableProgram: Sized {
    fn program_identity(&self) -> &StandingProgramIdentity;

    fn input_schemas(&self) -> Vec<RelationSchema>;

    fn output_schemas(&self) -> Vec<RelationSchema>;

    fn logical_epoch(&self) -> LogicalEpoch;

    fn apply_epoch(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError>;

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError>;

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError>;

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError>;
}

#[derive(Clone, Debug)]
pub struct FelderaPackageRuntime<E> {
    expected_identity: StandingProgramIdentity,
    executable: E,
}

impl<E> FelderaPackageRuntime<E>
where
    E: FelderaExecutableProgram,
{
    pub fn new(
        expected_identity: StandingProgramIdentity,
        executable: E,
    ) -> Result<Self, StandingProgramRuntimeError> {
        expected_identity.validate()?;
        if executable.program_identity() != &expected_identity {
            return Err(StandingProgramRuntimeError::ProgramIdentityMismatch {
                expected_program_id: expected_identity.program_id.clone(),
                actual_program_id: executable.program_identity().program_id.clone(),
            });
        }

        Ok(Self {
            expected_identity,
            executable,
        })
    }

    pub fn executable(&self) -> &E {
        &self.executable
    }

    pub fn executable_mut(&mut self) -> &mut E {
        &mut self.executable
    }
}

impl<E> StandingProgramRuntime for FelderaPackageRuntime<E>
where
    E: Clone + FelderaExecutableProgram,
{
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.expected_identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.executable.input_schemas()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        self.executable.output_schemas()
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.executable.logical_epoch()
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        if logical_epoch < self.logical_epoch() {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch(),
                attempted: logical_epoch,
            });
        }

        let mut candidate = self.executable.clone();
        let commit = candidate.apply_epoch(logical_epoch, idempotency_key, input_changes)?;
        for output in &commit.output_batches {
            if !self
                .expected_identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &output.view_id)
            {
                return Err(StandingProgramRuntimeError::UnknownView {
                    view_id: output.view_id.clone(),
                });
            }
        }

        self.executable = candidate;
        Ok(commit)
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.expected_identity.tenant_id
            || view.program_id != self.expected_identity.program_id
            || !self
                .expected_identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }

        self.executable.materialized_view_page(view, page)
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let checkpoint = self.executable.checkpoint()?;
        checkpoint.validate_identity(&self.expected_identity)?;
        Ok(checkpoint)
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError>
    where
        Self: Sized,
    {
        let expected_identity = checkpoint.identity.clone();
        checkpoint.validate_identity(&expected_identity)?;
        let executable = E::restore(checkpoint)?;
        Self::new(expected_identity, executable)
    }
}

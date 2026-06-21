use std::{fmt, marker::PhantomData};

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

use crate::storage_admin::{
    AppendValidatedEnvelopeOutcome, IngestAdmissionCoordinator,
    IngestAdmissionReconstructionReport, IngestCommitGuard,
};

#[async_trait]
pub trait IngestWriterRuntimeStartup<Authority>: Send + Sync {
    type Error;

    fn authority(&self) -> Authority;

    fn coordinator_without_startup_reconstruction(
        &self,
    ) -> Result<IngestAdmissionCoordinator, Self::Error>;

    async fn coordinator_after_startup_reconstruction(
        &self,
    ) -> Result<
        (
            IngestAdmissionCoordinator,
            IngestAdmissionReconstructionReport,
        ),
        Self::Error,
    >;
}

pub struct DeployedIngestWriterRuntime<Authority = (), RuntimeError = IngestWriterRuntimeError> {
    authority: Authority,
    coordinator: IngestAdmissionCoordinator,
    startup_report: IngestAdmissionReconstructionReport,
    runtime_error: PhantomData<fn() -> RuntimeError>,
}

impl<Authority: fmt::Debug, RuntimeError> fmt::Debug
    for DeployedIngestWriterRuntime<Authority, RuntimeError>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeployedIngestWriterRuntime")
            .field("authority", &self.authority)
            .field("startup_report", &self.startup_report)
            .finish_non_exhaustive()
    }
}

impl<Authority, RuntimeError> DeployedIngestWriterRuntime<Authority, RuntimeError> {
    pub async fn from_startup_components<Startup>(
        startup_components: &Startup,
    ) -> Result<Self, Startup::Error>
    where
        Startup: IngestWriterRuntimeStartup<Authority>,
    {
        let (coordinator, startup_report) = startup_components
            .coordinator_after_startup_reconstruction()
            .await?;

        Ok(Self {
            authority: startup_components.authority(),
            coordinator,
            startup_report,
            runtime_error: PhantomData,
        })
    }

    pub fn from_startup_components_without_reconstruction<Startup>(
        startup_components: &Startup,
    ) -> Result<Self, Startup::Error>
    where
        Startup: IngestWriterRuntimeStartup<Authority>,
    {
        let coordinator = startup_components.coordinator_without_startup_reconstruction()?;
        let startup_report = IngestAdmissionReconstructionReport {
            active_admission_records: 0,
            expired_orphan_admission_records: 0,
        };

        Ok(Self {
            authority: startup_components.authority(),
            coordinator,
            startup_report,
            runtime_error: PhantomData,
        })
    }

    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    pub fn startup_report(&self) -> &IngestAdmissionReconstructionReport {
        &self.startup_report
    }

    pub async fn append_catalog_validated_envelope(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, RuntimeError>
    where
        RuntimeError: From<IngestWriterRuntimeError>,
    {
        self.coordinator
            .append_catalog_validated_envelope(payload)
            .await
            .map_err(|error| RuntimeError::from(IngestWriterRuntimeError::append(error)))
    }

    pub async fn append_catalog_validated_envelope_with_commit_guard(
        &self,
        payload: Bytes,
        commit_guard: &dyn IngestCommitGuard,
    ) -> Result<AppendValidatedEnvelopeOutcome, RuntimeError>
    where
        RuntimeError: From<IngestWriterRuntimeError>,
    {
        self.coordinator
            .append_catalog_validated_envelope_with_commit_guard(payload, commit_guard)
            .await
            .map_err(|error| RuntimeError::from(IngestWriterRuntimeError::append(error)))
    }

    pub async fn append_catalog_validated_envelope_after_external_admission(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, RuntimeError>
    where
        RuntimeError: From<IngestWriterRuntimeError>,
    {
        self.coordinator
            .append_catalog_validated_envelope_after_external_admission(payload)
            .await
            .map_err(|error| RuntimeError::from(IngestWriterRuntimeError::append(error)))
    }

    pub async fn append_validated_envelope_after_external_admission(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, RuntimeError>
    where
        RuntimeError: From<IngestWriterRuntimeError>,
    {
        self.coordinator
            .append_validated_envelope_after_external_admission(payload)
            .await
            .map_err(|error| RuntimeError::from(IngestWriterRuntimeError::append(error)))
    }
}

#[derive(Debug, Error)]
pub enum IngestWriterRuntimeError {
    #[error("{message}")]
    Append { message: String },
}

impl IngestWriterRuntimeError {
    fn append(error: impl fmt::Display) -> Self {
        Self::Append {
            message: error.to_string(),
        }
    }
}

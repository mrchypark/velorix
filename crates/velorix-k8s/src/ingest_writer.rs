use std::fmt;

use bytes::Bytes;
use velorix_storage::log::{
    AppendValidatedEnvelopeOutcome, IngestAdmissionCoordinator, IngestAdmissionReconstructionReport,
};

use crate::{
    crd::ObjectStoreAuthorityRef, startup::OperatorAuthorityStartupComponents,
    stream_watch::StreamWatchError,
};

pub struct DeployedIngestWriterRuntime {
    authority: ObjectStoreAuthorityRef,
    coordinator: IngestAdmissionCoordinator,
    startup_report: IngestAdmissionReconstructionReport,
}

impl fmt::Debug for DeployedIngestWriterRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeployedIngestWriterRuntime")
            .field("authority", &self.authority)
            .field("startup_report", &self.startup_report)
            .finish_non_exhaustive()
    }
}

impl DeployedIngestWriterRuntime {
    pub async fn from_startup_components(
        startup_components: &OperatorAuthorityStartupComponents,
    ) -> Result<Self, StreamWatchError> {
        let provider = startup_components.ingest_admission_coordinator_provider();
        let (coordinator, startup_report) =
            provider.coordinator_after_startup_reconstruction().await?;

        Ok(Self {
            authority: startup_components.authority().clone(),
            coordinator,
            startup_report,
        })
    }

    pub fn authority(&self) -> &ObjectStoreAuthorityRef {
        &self.authority
    }

    pub fn startup_report(&self) -> &IngestAdmissionReconstructionReport {
        &self.startup_report
    }

    pub async fn append_catalog_validated_envelope(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, StreamWatchError> {
        self.coordinator
            .append_catalog_validated_envelope(payload)
            .await
            .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }
}

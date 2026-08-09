use std::sync::Arc;
pub use velorix_control::operator_authority::{
    validate_operator_authority, ObjectStoreAuthorityRef, OperatorStartupError,
    ValidatedOperatorAuthority,
};
use velorix_control::{
    operator_authority as control_authority,
    storage_admin::{AuthoritativeObjectStoreCapabilitiesV1, IngestAdmissionReconstructionReport},
};

use crate::{
    stream_watch::{
        IngestAdmissionCoordinatorProvider, RelationCatalogSnapshotProvider, StreamWatchError,
    },
    worker_shard::{CheckpointPublisherEpochStore, WorkerShardError},
};

#[derive(Clone, Debug)]
pub struct OperatorAuthorityStartupComponents {
    inner: control_authority::OperatorAuthorityStartupComponents,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ValidatedStartupAuthorityToken {
    _private: (),
}

impl ValidatedStartupAuthorityToken {
    fn new() -> Self {
        Self { _private: () }
    }
}

#[derive(Clone, Debug)]
pub struct OperatorAuthorityStartupReport {
    pub ingest_admission: IngestAdmissionReconstructionReport,
}

impl OperatorAuthorityStartupComponents {
    pub fn from_validated_authority(validated_authority: ValidatedOperatorAuthority) -> Self {
        Self {
            inner: control_authority::OperatorAuthorityStartupComponents::from_validated_authority(
                validated_authority,
            ),
        }
    }

    pub fn authority(&self) -> &ObjectStoreAuthorityRef {
        self.inner.authority()
    }

    pub fn capabilities(&self) -> &AuthoritativeObjectStoreCapabilitiesV1 {
        self.inner.capabilities()
    }

    pub fn store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.inner.store()
    }

    pub fn relation_snapshot_provider(&self) -> RelationCatalogSnapshotProvider {
        RelationCatalogSnapshotProvider::from_authority_parts(
            ValidatedStartupAuthorityToken::new(),
            self.authority().clone(),
            self.store(),
            self.inner.capabilities_handle(),
        )
    }

    pub fn ingest_admission_coordinator_provider(&self) -> IngestAdmissionCoordinatorProvider {
        IngestAdmissionCoordinatorProvider::from_authority_parts(
            ValidatedStartupAuthorityToken::new(),
            self.authority().clone(),
            self.store(),
            self.inner.capabilities_handle(),
        )
    }

    pub fn worker_shard_epoch_store(
        &self,
    ) -> Result<CheckpointPublisherEpochStore, WorkerShardError> {
        CheckpointPublisherEpochStore::from_authority_parts(
            ValidatedStartupAuthorityToken::new(),
            self.store(),
            self.inner.capabilities_handle(),
        )
    }

    pub async fn ingest_admission_startup_preflight(
        &self,
    ) -> Result<OperatorAuthorityStartupReport, StreamWatchError> {
        let ingest_admission = self
            .ingest_admission_coordinator_provider()
            .startup()
            .await?;

        Ok(OperatorAuthorityStartupReport { ingest_admission })
    }
}

impl From<OperatorStartupError> for StreamWatchError {
    fn from(error: OperatorStartupError) -> Self {
        StreamWatchError::snapshot(error.to_string())
    }
}

use std::sync::Arc;
use thiserror::Error;
use velorix_storage::capability::{
    probe_authoritative_object_store_capabilities, AuthoritativeObjectStoreCapabilitiesV1,
    AuthoritativeObjectStoreCapabilityError, AuthoritativeObjectStoreCapabilityProbeError,
};

use crate::{
    crd::ObjectStoreAuthorityRef,
    stream_watch::{
        IngestAdmissionCoordinatorProvider, RelationCatalogSnapshotProvider, StreamWatchError,
    },
    worker_shard::{CheckpointPublisherEpochStore, WorkerShardError},
};

#[derive(Clone, Debug)]
pub struct ValidatedOperatorAuthority {
    authority: ObjectStoreAuthorityRef,
    store: Arc<dyn object_store::ObjectStore>,
    capabilities: AuthoritativeObjectStoreCapabilitiesV1,
}

impl ValidatedOperatorAuthority {
    pub(crate) fn new(
        authority: ObjectStoreAuthorityRef,
        store: Arc<dyn object_store::ObjectStore>,
        capabilities: AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, OperatorStartupError> {
        capabilities.validate_for_startup()?;

        Ok(Self {
            authority,
            store,
            capabilities,
        })
    }

    pub fn authority(&self) -> &ObjectStoreAuthorityRef {
        &self.authority
    }

    pub fn capabilities(&self) -> &AuthoritativeObjectStoreCapabilitiesV1 {
        &self.capabilities
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ObjectStoreAuthorityRef,
        Arc<dyn object_store::ObjectStore>,
        AuthoritativeObjectStoreCapabilitiesV1,
    ) {
        (self.authority, self.store, self.capabilities)
    }
}

pub async fn validate_operator_authority(
    authority: ObjectStoreAuthorityRef,
    store: Arc<dyn object_store::ObjectStore>,
    backend_name: impl AsRef<str>,
    probe_prefix: impl AsRef<str>,
) -> Result<ValidatedOperatorAuthority, OperatorStartupError> {
    let capabilities =
        probe_authoritative_object_store_capabilities(store.as_ref(), backend_name, probe_prefix)
            .await?;
    ValidatedOperatorAuthority::new(authority, store, capabilities)
}

#[derive(Clone, Debug)]
pub struct OperatorAuthorityStartupComponents {
    authority: ObjectStoreAuthorityRef,
    store: Arc<dyn object_store::ObjectStore>,
    capabilities: Arc<AuthoritativeObjectStoreCapabilitiesV1>,
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
    pub ingest_admission: velorix_storage::log::IngestAdmissionReconstructionReport,
}

impl OperatorAuthorityStartupComponents {
    pub fn from_validated_authority(validated_authority: ValidatedOperatorAuthority) -> Self {
        let (authority, store, capabilities) = validated_authority.into_parts();
        Self {
            authority,
            store,
            capabilities: Arc::new(capabilities),
        }
    }

    pub fn authority(&self) -> &ObjectStoreAuthorityRef {
        &self.authority
    }

    pub fn capabilities(&self) -> &AuthoritativeObjectStoreCapabilitiesV1 {
        &self.capabilities
    }

    pub fn relation_snapshot_provider(&self) -> RelationCatalogSnapshotProvider {
        RelationCatalogSnapshotProvider::from_authority_parts(
            ValidatedStartupAuthorityToken::new(),
            self.authority.clone(),
            Arc::clone(&self.store),
            Arc::clone(&self.capabilities),
        )
    }

    pub fn ingest_admission_coordinator_provider(&self) -> IngestAdmissionCoordinatorProvider {
        IngestAdmissionCoordinatorProvider::from_authority_parts(
            ValidatedStartupAuthorityToken::new(),
            self.authority.clone(),
            Arc::clone(&self.store),
            Arc::clone(&self.capabilities),
        )
    }

    pub fn worker_shard_epoch_store(
        &self,
    ) -> Result<CheckpointPublisherEpochStore, WorkerShardError> {
        CheckpointPublisherEpochStore::from_authority_parts(
            ValidatedStartupAuthorityToken::new(),
            Arc::clone(&self.store),
            Arc::clone(&self.capabilities),
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

#[derive(Debug, Error)]
pub enum OperatorStartupError {
    #[error(transparent)]
    Probe(#[from] AuthoritativeObjectStoreCapabilityProbeError),
    #[error(transparent)]
    Capabilities(#[from] AuthoritativeObjectStoreCapabilityError),
}

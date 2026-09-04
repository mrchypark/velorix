use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ingest_writer_runtime::IngestWriterRuntimeStartup,
    storage_admin::{
        probe_authoritative_object_store_capabilities, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, AuthoritativeObjectStoreCapabilityProbeError,
        IngestAdmissionCoordinator, IngestAdmissionReconstructionReport,
    },
};

#[derive(
    Clone, Debug, Default, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreAuthorityRef {
    pub store_id: String,
    pub namespace: String,
}

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

    pub fn capabilities_handle(&self) -> Arc<AuthoritativeObjectStoreCapabilitiesV1> {
        Arc::clone(&self.capabilities)
    }

    pub fn store(&self) -> Arc<dyn object_store::ObjectStore> {
        Arc::clone(&self.store)
    }

    pub(crate) fn ingest_log(
        &self,
    ) -> Result<velorix_storage::log::IngestLog, OperatorStartupError> {
        velorix_storage::log::IngestLog::new_catalog_checked(
            Arc::clone(&self.store),
            &self.capabilities,
        )
        .map_err(OperatorStartupError::from)
    }

    fn ingest_admission_coordinator(
        &self,
    ) -> Result<IngestAdmissionCoordinator, OperatorStartupError> {
        IngestAdmissionCoordinator::new_checked(Arc::clone(&self.store), &self.capabilities)
            .map_err(OperatorStartupError::from)
    }
}

#[async_trait]
impl IngestWriterRuntimeStartup<ObjectStoreAuthorityRef> for OperatorAuthorityStartupComponents {
    type Error = OperatorStartupError;

    fn authority(&self) -> ObjectStoreAuthorityRef {
        self.authority.clone()
    }

    fn coordinator_without_startup_reconstruction(
        &self,
    ) -> Result<IngestAdmissionCoordinator, Self::Error> {
        self.ingest_admission_coordinator()
    }

    async fn coordinator_after_startup_reconstruction(
        &self,
    ) -> Result<
        (
            IngestAdmissionCoordinator,
            IngestAdmissionReconstructionReport,
        ),
        Self::Error,
    > {
        let coordinator = self.ingest_admission_coordinator()?;
        let report = coordinator
            .reconstruct_active_admissions()
            .await
            .map_err(|error| OperatorStartupError::IngestAdmission {
                message: error.to_string(),
            })?;

        Ok((coordinator, report))
    }
}

#[derive(Debug, Error)]
pub enum OperatorStartupError {
    #[error(transparent)]
    Probe(#[from] AuthoritativeObjectStoreCapabilityProbeError),
    #[error(transparent)]
    Capabilities(#[from] AuthoritativeObjectStoreCapabilityError),
    #[error("ingest admission startup failed: {message}")]
    IngestAdmission { message: String },
}

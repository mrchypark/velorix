use std::sync::Arc;
use thiserror::Error;
use velorix_storage::capability::{
    probe_authoritative_object_store_capabilities, AuthoritativeObjectStoreCapabilitiesV1,
    AuthoritativeObjectStoreCapabilityError, AuthoritativeObjectStoreCapabilityProbeError,
};

use crate::crd::ObjectStoreAuthorityRef;

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
    ) -> (ObjectStoreAuthorityRef, Arc<dyn object_store::ObjectStore>) {
        (self.authority, self.store)
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

#[derive(Debug, Error)]
pub enum OperatorStartupError {
    #[error(transparent)]
    Probe(#[from] AuthoritativeObjectStoreCapabilityProbeError),
    #[error(transparent)]
    Capabilities(#[from] AuthoritativeObjectStoreCapabilityError),
}

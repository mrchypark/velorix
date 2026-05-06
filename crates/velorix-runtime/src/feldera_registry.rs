use std::sync::Arc;

use object_store::ObjectStore;
use thiserror::Error;
use velorix_core::{
    feldera_artifact::{
        validate_feldera_compile_artifact_for_catalog,
        validate_feldera_compile_artifact_hash_for_catalog, FelderaArtifactError,
        FelderaCompileArtifactMetadata, StandingViewSpec,
    },
    relation::VelorixRelationCatalogV1,
};
use velorix_storage::{
    capability::{ObjectStoreCapabilityError, ObjectStoreCapabilityProfile},
    feldera_artifact_registry::{
        FelderaArtifactRegistry, FelderaArtifactRegistryError, RegisterFelderaArtifactOutcome,
    },
};

#[derive(Clone, Debug)]
pub struct RuntimeFelderaArtifactRegistry {
    storage: FelderaArtifactRegistry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredRuntimeFelderaArtifact {
    pub metadata: FelderaCompileArtifactMetadata,
    pub status: RuntimeFelderaArtifactSelectionStatus,
    pub register_outcome: RegisterFelderaArtifactOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFelderaArtifactSelection {
    pub metadata: FelderaCompileArtifactMetadata,
    pub status: RuntimeFelderaArtifactSelectionStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFelderaArtifactSelectionStatus {
    DirectExecutionDisabled,
}

#[derive(Debug, Error)]
pub enum RuntimeFelderaArtifactError {
    #[error(transparent)]
    Validation(#[from] FelderaArtifactError),
    #[error(transparent)]
    Storage(#[from] FelderaArtifactRegistryError),
}

impl RuntimeFelderaArtifactRegistry {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::from_storage_registry(FelderaArtifactRegistry::new(store))
    }

    pub fn new_checked(
        store: Arc<dyn ObjectStore>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        Ok(Self::from_storage_registry(
            FelderaArtifactRegistry::new_checked(store, profile)?,
        ))
    }

    pub fn from_storage_registry(storage: FelderaArtifactRegistry) -> Self {
        Self { storage }
    }

    pub async fn register_trusted_artifact(
        &self,
        catalog: &VelorixRelationCatalogV1,
        spec: &StandingViewSpec,
        artifact: &FelderaCompileArtifactMetadata,
    ) -> Result<RegisteredRuntimeFelderaArtifact, RuntimeFelderaArtifactError> {
        validate_feldera_compile_artifact_for_catalog(catalog, spec, artifact)?;

        let register_outcome = self.storage.register(spec, artifact).await?;

        Ok(RegisteredRuntimeFelderaArtifact {
            metadata: artifact.clone(),
            status: RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled,
            register_outcome,
        })
    }

    pub async fn register_hash_verified_artifact(
        &self,
        catalog: &VelorixRelationCatalogV1,
        spec: &StandingViewSpec,
        artifact: &FelderaCompileArtifactMetadata,
        artifact_bytes: &[u8],
    ) -> Result<RegisteredRuntimeFelderaArtifact, RuntimeFelderaArtifactError> {
        validate_feldera_compile_artifact_hash_for_catalog(
            catalog,
            spec,
            artifact,
            artifact_bytes,
        )?;

        let register_outcome = self.storage.register(spec, artifact).await?;

        Ok(RegisteredRuntimeFelderaArtifact {
            metadata: artifact.clone(),
            status: RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled,
            register_outcome,
        })
    }

    pub async fn select_trusted_artifact(
        &self,
        catalog: &VelorixRelationCatalogV1,
        spec: &StandingViewSpec,
        artifact_id: &str,
        artifact_hash: &str,
    ) -> Result<RuntimeFelderaArtifactSelection, RuntimeFelderaArtifactError> {
        let metadata = self.storage.read(artifact_id, artifact_hash).await?;
        validate_feldera_compile_artifact_for_catalog(catalog, spec, &metadata)?;

        Ok(RuntimeFelderaArtifactSelection {
            metadata,
            status: RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled,
        })
    }
}

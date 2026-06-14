use std::{collections::BTreeSet, sync::Arc};

use object_store::ObjectStore;
use thiserror::Error;
use velorix_core::{
    feldera_artifact::{
        validate_feldera_compile_artifact_for_catalog,
        validate_feldera_compile_artifact_for_catalogs,
        validate_feldera_compile_artifact_hash_for_catalog,
        validate_feldera_release_artifact_provenance, FelderaArtifactError,
        FelderaCompileArtifactMetadata, FelderaReleaseArtifactProvenanceV1, StandingViewSpec,
    },
    relation::VelorixRelationCatalogV1,
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError,
    },
    feldera_artifact_registry::{
        FelderaArtifactRegistry, FelderaArtifactRegistryError, RegisterFelderaArtifactOutcome,
    },
};

#[derive(Clone, Debug)]
pub struct RuntimeFelderaArtifactRegistry {
    storage: FelderaArtifactRegistry,
    generated_packages: BTreeSet<GeneratedRustArtifactPackage>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFelderaArtifactSelectionStatus {
    DirectExecutionDisabled,
    DirectExecutionEnabled {
        package: GeneratedRustArtifactPackage,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GeneratedRustArtifactPackage {
    pub abi_version: String,
    pub crate_name: String,
}

#[derive(Debug, Error)]
pub enum RuntimeFelderaArtifactError {
    #[error(transparent)]
    Validation(#[from] FelderaArtifactError),
    #[error(transparent)]
    Storage(#[from] FelderaArtifactRegistryError),
}

impl RuntimeFelderaArtifactRegistry {
    /// Builds a runtime artifact registry without startup object-store capability evidence.
    ///
    /// This is only for local bootstrap and development paths. Production runtime construction
    /// should use `new_with_startup_capabilities` so object-store requirements are validated from
    /// authoritative startup evidence.
    pub fn for_local_bootstrap_unchecked(store: Arc<dyn ObjectStore>) -> Self {
        Self::from_storage_registry(FelderaArtifactRegistry::new(store))
    }

    pub fn for_local_bootstrap_with_generated_packages(
        store: Arc<dyn ObjectStore>,
        packages: impl IntoIterator<Item = GeneratedRustArtifactPackage>,
    ) -> Self {
        Self::from_storage_registry_with_generated_packages(
            FelderaArtifactRegistry::new(store),
            packages,
        )
    }

    pub fn new_with_startup_capabilities(
        store: Arc<dyn ObjectStore>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, AuthoritativeObjectStoreCapabilityError> {
        Self::new_with_startup_capabilities_and_generated_packages(store, capabilities, [])
    }

    pub fn new_with_startup_capabilities_and_generated_packages(
        store: Arc<dyn ObjectStore>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
        packages: impl IntoIterator<Item = GeneratedRustArtifactPackage>,
    ) -> Result<Self, AuthoritativeObjectStoreCapabilityError> {
        capabilities.validate_for_startup()?;
        let profile = capabilities
            .profiles
            .get(&AuthoritativeNamespace::ArtifactCatalog)
            .expect("startup capability validation guarantees artifact catalog evidence");

        Ok(Self::from_storage_registry_with_generated_packages(
            FelderaArtifactRegistry::new_checked(store, profile).map_err(|source| {
                AuthoritativeObjectStoreCapabilityError::NamespaceProfile {
                    namespace: AuthoritativeNamespace::ArtifactCatalog,
                    source,
                }
            })?,
            packages,
        ))
    }

    fn from_storage_registry(storage: FelderaArtifactRegistry) -> Self {
        Self::from_storage_registry_with_generated_packages(storage, [])
    }

    fn from_storage_registry_with_generated_packages(
        storage: FelderaArtifactRegistry,
        packages: impl IntoIterator<Item = GeneratedRustArtifactPackage>,
    ) -> Self {
        Self {
            storage,
            generated_packages: packages.into_iter().collect(),
        }
    }

    fn selection_status(
        &self,
        artifact: &FelderaCompileArtifactMetadata,
    ) -> RuntimeFelderaArtifactSelectionStatus {
        let package = GeneratedRustArtifactPackage {
            abi_version: artifact.generated_rust.abi_version.clone(),
            crate_name: artifact.generated_rust.crate_name.clone(),
        };
        if self.generated_packages.contains(&package) {
            RuntimeFelderaArtifactSelectionStatus::DirectExecutionEnabled { package }
        } else {
            RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled
        }
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
            status: self.selection_status(artifact),
            register_outcome,
        })
    }

    pub async fn register_trusted_artifact_for_catalogs(
        &self,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
        artifact: &FelderaCompileArtifactMetadata,
    ) -> Result<RegisteredRuntimeFelderaArtifact, RuntimeFelderaArtifactError> {
        validate_feldera_compile_artifact_for_catalogs(catalogs, spec, artifact)?;

        let register_outcome = self.storage.register(spec, artifact).await?;

        Ok(RegisteredRuntimeFelderaArtifact {
            metadata: artifact.clone(),
            status: self.selection_status(artifact),
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
            status: self.selection_status(artifact),
            register_outcome,
        })
    }

    pub async fn register_release_provenance_verified_artifact(
        &self,
        catalog: &VelorixRelationCatalogV1,
        spec: &StandingViewSpec,
        artifact: &FelderaCompileArtifactMetadata,
        artifact_bytes: &[u8],
        provenance: &FelderaReleaseArtifactProvenanceV1,
    ) -> Result<RegisteredRuntimeFelderaArtifact, RuntimeFelderaArtifactError> {
        validate_feldera_compile_artifact_hash_for_catalog(
            catalog,
            spec,
            artifact,
            artifact_bytes,
        )?;
        validate_feldera_release_artifact_provenance(artifact, provenance)?;

        let register_outcome = self.storage.register(spec, artifact).await?;

        Ok(RegisteredRuntimeFelderaArtifact {
            metadata: artifact.clone(),
            status: self.selection_status(artifact),
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
            status: self.selection_status(&metadata),
            metadata,
        })
    }

    pub async fn select_release_provenance_verified_artifact(
        &self,
        catalog: &VelorixRelationCatalogV1,
        spec: &StandingViewSpec,
        artifact_id: &str,
        artifact_hash: &str,
        provenance: &FelderaReleaseArtifactProvenanceV1,
    ) -> Result<RuntimeFelderaArtifactSelection, RuntimeFelderaArtifactError> {
        let metadata = self.storage.read(artifact_id, artifact_hash).await?;
        validate_feldera_compile_artifact_for_catalog(catalog, spec, &metadata)?;
        validate_feldera_release_artifact_provenance(&metadata, provenance)?;

        Ok(RuntimeFelderaArtifactSelection {
            status: self.selection_status(&metadata),
            metadata,
        })
    }
}

use std::{collections::BTreeMap, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::object_store::{
    memory::InMemory as DataFusionInMemory, ObjectStore as DataFusionObjectStore,
};
use futures::stream::BoxStream;
use object_store::{
    memory::InMemory as AuthorityInMemory, path::Path, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore as AuthorityObjectStore, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use velorix_runtime::storage_registry::{StorageRegistry, StorageRegistryError};
use velorix_storage::capability::{
    AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
    AuthoritativeObjectStoreCapabilityError, AuthoritativeObjectStoreCapabilityProbeError,
    ObjectStoreCapabilityProbeError, ObjectStoreCapabilityProfile, RequiredObjectStoreCapability,
};

#[test]
fn storage_registry_accepts_simple_store_id_characters() {
    for store_id in ["primary", "primary-prod", "primary_prod", "primary.prod"] {
        let mut registry = StorageRegistry::new();

        registry
            .register(store_id, "memory://velorix/", scan_store())
            .unwrap();
    }
}

#[test]
fn storage_registry_rejects_invalid_unchecked_store_ids() {
    for store_id in [
        "",
        " ",
        "\t",
        "\n",
        " primary",
        "primary ",
        "primary\n",
        "primary/store",
        "primary\\store",
        ".",
        "..",
        "s3://prod",
        "file://tmp",
    ] {
        let mut registry = StorageRegistry::new();
        let err = registry
            .register(store_id, "memory://velorix/", scan_store())
            .unwrap_err();

        assert_invalid_store_id(err);
    }
}

#[tokio::test]
async fn storage_registry_rejects_invalid_probe_backed_store_ids_before_probe() {
    let authority_store: Arc<dyn AuthorityObjectStore> = Arc::new(OverwriteCreateStore {
        inner: Arc::new(AuthorityInMemory::new()),
    });
    let mut registry = StorageRegistry::new();

    let err = registry
        .register_production_with_probe(
            "s3://prod",
            "memory://velorix/",
            scan_store(),
            authority_store,
            "memory-test",
            "v1/probes",
        )
        .await
        .unwrap_err();

    assert_invalid_store_id(err);
}

#[tokio::test]
async fn storage_registry_registers_production_store_from_runtime_probe() {
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let authority_store: Arc<dyn AuthorityObjectStore> = Arc::new(AuthorityInMemory::new());
    let mut registry = StorageRegistry::new();

    registry
        .register_production_with_probe(
            "primary",
            "memory://velorix/",
            scan_store,
            authority_store,
            "memory-test",
            "v1/probes",
        )
        .await
        .unwrap();

    let location = registry
        .resolve_production_table_location(
            "primary",
            "tenant-a",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
        )
        .unwrap();

    assert_eq!(
        location.table_url,
        "memory://velorix/tenants/tenant-a/tables/orders/snapshots/0001/"
    );
}

#[test]
fn storage_registry_registers_production_store_from_validated_capabilities() {
    let mut registry = StorageRegistry::new();

    registry
        .register_production_with_capabilities(
            "primary",
            "memory://velorix/",
            scan_store(),
            all_namespace_capabilities(),
        )
        .unwrap();

    let location = registry
        .resolve_production_table_location(
            "primary",
            "tenant-a",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
        )
        .unwrap();

    assert_eq!(
        location.table_url,
        "memory://velorix/tenants/tenant-a/tables/orders/snapshots/0001/"
    );
}

#[test]
fn storage_registry_rejects_prevalidated_capabilities_missing_namespace() {
    let mut registry = StorageRegistry::new();
    let mut capabilities = all_namespace_capabilities();
    capabilities
        .profiles
        .remove(&AuthoritativeNamespace::RelationCatalog);

    let err = registry
        .register_production_with_capabilities(
            "primary",
            "memory://velorix/",
            scan_store(),
            capabilities,
        )
        .unwrap_err();

    assert_missing_namespace(err, AuthoritativeNamespace::RelationCatalog);
    assert_unregistered_store_id(&registry, "primary");
}

#[test]
fn storage_registry_rejects_prevalidated_capabilities_with_weak_profile() {
    let mut registry = StorageRegistry::new();
    let mut capabilities = all_namespace_capabilities();
    capabilities.profiles.insert(
        AuthoritativeNamespace::Output,
        ObjectStoreCapabilityProfile {
            backend_name: "weak-profile".to_string(),
            conditional_create: false,
            conditional_update: true,
            atomic_visibility: true,
            list_after_write: true,
            read_after_write: true,
        },
    );

    let err = registry
        .register_production_with_capabilities(
            "primary",
            "memory://velorix/",
            scan_store(),
            capabilities,
        )
        .unwrap_err();

    match err {
        StorageRegistryError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source },
        ) => {
            assert_eq!(namespace, AuthoritativeNamespace::Output);
            assert_eq!(source.backend_name(), "weak-profile");
            assert_eq!(
                source.required_capability(),
                RequiredObjectStoreCapability::ConditionalCreate
            );
        }
        other => panic!("expected namespace profile error, got {other:?}"),
    }
    assert_unregistered_store_id(&registry, "primary");
}

#[test]
fn storage_registry_rejects_duplicate_unchecked_store_id() {
    let mut registry = StorageRegistry::new();

    registry
        .register("primary", "memory://velorix/", scan_store())
        .unwrap();
    let err = registry
        .register("primary", "memory://velorix-shadow/", scan_store())
        .unwrap_err();

    assert_duplicate_store_id(err, "primary");
}

#[test]
fn storage_registry_rejects_capability_backed_reregistration_of_existing_store_id() {
    let mut registry = StorageRegistry::new();

    registry
        .register("primary", "memory://velorix/", scan_store())
        .unwrap();
    let err = registry
        .register_production_with_capabilities(
            "primary",
            "memory://velorix-shadow/",
            scan_store(),
            all_namespace_capabilities(),
        )
        .unwrap_err();

    assert_duplicate_store_id(err, "primary");
    let err = registry
        .resolve_production_table_location(
            "primary",
            "tenant-a",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
        )
        .unwrap_err();
    match err {
        StorageRegistryError::MissingProductionCapabilities { store_id } => {
            assert_eq!(store_id, "primary");
        }
        other => panic!("expected missing production capabilities, got {other:?}"),
    }
}

#[tokio::test]
async fn storage_registry_rejects_unchecked_reregistration_of_probe_backed_store() {
    let mut registry = StorageRegistry::new();

    registry
        .register_production_with_probe(
            "primary",
            "memory://velorix/",
            scan_store(),
            authority_store(),
            "memory-test",
            "v1/probes",
        )
        .await
        .unwrap();
    let err = registry
        .register("primary", "memory://velorix-shadow/", scan_store())
        .unwrap_err();

    assert_duplicate_store_id(err, "primary");
    registry
        .resolve_production_table_location(
            "primary",
            "tenant-a",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
        )
        .unwrap();
}

#[tokio::test]
async fn storage_registry_rejects_probe_backed_reregistration_of_existing_store_id() {
    let mut registry = StorageRegistry::new();

    registry
        .register("primary", "memory://velorix/", scan_store())
        .unwrap();
    let err = registry
        .register_production_with_probe(
            "primary",
            "memory://velorix-shadow/",
            scan_store(),
            authority_store(),
            "memory-test",
            "v1/probes",
        )
        .await
        .unwrap_err();

    assert_duplicate_store_id(err, "primary");
    let err = registry
        .resolve_production_table_location(
            "primary",
            "tenant-a",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
        )
        .unwrap_err();
    match err {
        StorageRegistryError::MissingProductionCapabilities { store_id } => {
            assert_eq!(store_id, "primary");
        }
        other => panic!("expected missing production capabilities, got {other:?}"),
    }
}

#[tokio::test]
async fn storage_registry_probe_rejects_store_without_create_only_behavior() {
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let authority_store: Arc<dyn AuthorityObjectStore> = Arc::new(OverwriteCreateStore {
        inner: Arc::new(AuthorityInMemory::new()),
    });
    let mut registry = StorageRegistry::new();

    let err = registry
        .register_production_with_probe(
            "primary",
            "memory://velorix/",
            scan_store,
            authority_store,
            "overwrite-create",
            "v1/probes",
        )
        .await
        .unwrap_err();

    match err {
        StorageRegistryError::ObjectStoreCapabilityProbe(
            AuthoritativeObjectStoreCapabilityProbeError::Namespace { namespace, source },
        ) => {
            assert_eq!(namespace, AuthoritativeNamespace::Ingest);
            match source {
                ObjectStoreCapabilityProbeError::Capability(err) => {
                    assert_eq!(err.backend_name(), "overwrite-create");
                    assert_eq!(
                        err.required_capability(),
                        RequiredObjectStoreCapability::ConditionalCreate
                    );
                }
                other => panic!("expected capability error, got {other:?}"),
            }
        }
        other => panic!("expected object-store capability probe error, got {other:?}"),
    }
}

fn scan_store() -> Arc<dyn DataFusionObjectStore> {
    Arc::new(DataFusionInMemory::new())
}

fn authority_store() -> Arc<dyn AuthorityObjectStore> {
    Arc::new(AuthorityInMemory::new())
}

fn all_namespace_capabilities() -> AuthoritativeObjectStoreCapabilitiesV1 {
    let profile = ObjectStoreCapabilityProfile::local_development();
    AuthoritativeObjectStoreCapabilitiesV1::new(
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| (namespace, profile.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn assert_duplicate_store_id(err: StorageRegistryError, expected_store_id: &str) {
    match err {
        StorageRegistryError::DuplicateStoreId { store_id } => {
            assert_eq!(store_id, expected_store_id);
        }
        other => panic!("expected duplicate store id error, got {other:?}"),
    }
}

fn assert_missing_namespace(err: StorageRegistryError, expected_namespace: AuthoritativeNamespace) {
    match err {
        StorageRegistryError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace { namespace },
        ) => {
            assert_eq!(namespace, expected_namespace);
        }
        other => panic!("expected missing namespace error, got {other:?}"),
    }
}

fn assert_invalid_store_id(err: StorageRegistryError) {
    match err {
        StorageRegistryError::InvalidStoreId => {}
        other => panic!("expected invalid store id error, got {other:?}"),
    }
}

fn assert_unregistered_store_id(registry: &StorageRegistry, store_id: &str) {
    let err = registry
        .resolve_production_table_location(
            store_id,
            "tenant-a",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
        )
        .unwrap_err();

    match err {
        StorageRegistryError::UnregisteredStoreId { store_id: actual } => {
            assert_eq!(actual, store_id);
        }
        other => panic!("expected unregistered store id, got {other:?}"),
    }
}

#[derive(Debug)]
struct OverwriteCreateStore {
    inner: Arc<dyn AuthorityObjectStore>,
}

impl fmt::Display for OverwriteCreateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OverwriteCreateStore")
    }
}

#[async_trait]
impl AuthorityObjectStore for OverwriteCreateStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if matches!(opts.mode, PutMode::Create) {
            opts.mode = PutMode::Overwrite;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

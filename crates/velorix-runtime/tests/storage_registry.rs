use std::{fmt, sync::Arc};

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
    AuthoritativeNamespace, AuthoritativeObjectStoreCapabilityProbeError,
    ObjectStoreCapabilityProbeError, RequiredObjectStoreCapability,
};

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

fn assert_duplicate_store_id(err: StorageRegistryError, expected_store_id: &str) {
    match err {
        StorageRegistryError::DuplicateStoreId { store_id } => {
            assert_eq!(store_id, expected_store_id);
        }
        other => panic!("expected duplicate store id error, got {other:?}"),
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

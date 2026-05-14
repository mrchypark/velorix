use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::object_store::{
    memory::InMemory as DataFusionInMemory, path::Path as DataFusionPath,
    ObjectStore as DataFusionObjectStore, ObjectStoreExt as DataFusionObjectStoreExt,
};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    query::{QueryError, QueryExecutionPolicyV1, QueryPolicy, QueryPolicyError},
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_runtime::{
    persisted_table::{
        query_persisted_object_backed_input_with_policy,
        query_production_persisted_object_backed_input,
        query_production_persisted_object_backed_input_with_limiter,
        query_production_persisted_object_backed_input_with_limiter_and_metrics,
        query_production_persisted_object_backed_input_with_metrics,
        CreateProductionPersistedTableSpecRequest, PersistedTableError, PersistedTableFormat,
        PersistedTableStore, ProductionPersistedTableFormat, ProductionPersistedTableQueryRequest,
    },
    query::{QueryExecutionLimiter, RuntimeQueryError},
    query_policy_catalog::{
        QueryPolicyCatalogError, QueryPolicyCatalogRecord, QueryPolicyCatalogStore,
        QUERY_POLICY_CATALOG_SCHEMA_VERSION,
    },
    storage_registry::StorageRegistry,
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityProfile,
        RequiredObjectStoreCapability,
    },
    object_key::ObjectKey,
    relation_catalog_registry::RelationCatalogRegistry,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_count: AtomicUsize,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            get_count: AtomicUsize::new(0),
        }
    }

    fn get_count(&self) -> usize {
        self.get_count.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if !options.head {
            self.get_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn production_request(
    store_id: &str,
    object_key_prefix: &str,
) -> CreateProductionPersistedTableSpecRequest {
    CreateProductionPersistedTableSpecRequest {
        table_id: "orders-current".to_string(),
        tenant_id: "tenant-a".to_string(),
        store_id: store_id.to_string(),
        object_key_prefix: object_key_prefix.to_string(),
        snapshot_ref: "snapshots/0001".to_string(),
        format: ProductionPersistedTableFormat::Parquet,
        relation_id: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        schema_fingerprint: orders_relation_catalog()
            .schema_fingerprint
            .as_str()
            .to_string(),
        query_policy_id: "standard".to_string(),
    }
}

async fn register_production_scan_store(
    registry: &mut StorageRegistry,
    scan_store: Arc<dyn DataFusionObjectStore>,
    authority_store: Arc<dyn ObjectStore>,
) {
    registry
        .register_production_with_probe(
            "primary",
            "memory://velorix/",
            scan_store,
            authority_store,
            "local-test",
            "v1/probes",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn persisted_table_store_creates_and_reads_spec_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    let created = catalog
        .create(
            "orders-current",
            "memory://velorix/input/",
            PersistedTableFormat::Parquet,
        )
        .await
        .unwrap();
    let read = catalog.get("orders-current").await.unwrap();

    assert_eq!(created, read);
    assert_eq!(read.schema_version, 1);
    assert_eq!(read.table_id, "orders-current");
    assert_eq!(read.table_url, "memory://velorix/input/");
    assert_eq!(read.format, PersistedTableFormat::Parquet);
}

#[tokio::test]
async fn persisted_table_store_rejects_duplicate_ids_using_create_semantics() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    catalog
        .create(
            "orders-current",
            "memory://velorix/input/",
            PersistedTableFormat::Parquet,
        )
        .await
        .unwrap();

    let error = catalog
        .create(
            "orders-current",
            "memory://velorix/other/",
            PersistedTableFormat::Parquet,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedTableError::ObjectStore(_)));
}

#[tokio::test]
async fn persisted_table_store_rejects_unsupported_schema_version_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 2,
            "table_id": "orders-current",
            "table_url": "memory://velorix/input/",
            "format": "Parquet",
        }),
    )
    .await;

    let error = catalog.get("orders-current").await.unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::UnsupportedSchemaVersion { schema_version: 2 }
    ));
}

#[tokio::test]
async fn persisted_table_store_rejects_mismatched_table_id_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 1,
            "table_id": "other-table",
            "table_url": "memory://velorix/input/",
            "format": "Parquet",
        }),
    )
    .await;

    let error = catalog.get("orders-current").await.unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::TableIdMismatch {
            expected,
            actual,
        } if expected == "orders-current" && actual == "other-table"
    ));
}

#[tokio::test]
async fn persisted_table_store_rejects_malformed_table_url_without_writing_catalog_object() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    let error = catalog
        .create("broken-table", "not a url", PersistedTableFormat::Parquet)
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedTableError::MalformedTableUrl(_)));
    let key = ObjectKey::query_table("broken-table").unwrap();
    let path = Path::from(key.as_str());
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn persisted_table_store_rejects_malformed_json_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    let key = ObjectKey::query_table("orders-current").unwrap();

    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(br#"{"schema_version":"#).into(),
        )
        .await
        .unwrap();

    let error = catalog.get("orders-current").await.unwrap_err();

    assert!(matches!(error, PersistedTableError::Json(_)));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_old_spec_without_relation_catalog_reference_as_json(
) {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 1,
            "table_id": "orders-current",
            "tenant_id": "tenant-a",
            "store_id": "primary",
            "object_key_prefix": "tenants/tenant-a/tables/orders",
            "snapshot_ref": "snapshots/0001",
            "format": "parquet",
            "schema_fingerprint": orders_relation_catalog().schema_fingerprint.as_str(),
            "query_policy_id": "standard",
        }),
    )
    .await;

    let error = catalog.get_production("orders-current").await.unwrap_err();

    assert!(matches!(error, PersistedTableError::Json(_)));
}

#[tokio::test]
async fn persisted_table_store_rejects_unknown_spec_field_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 1,
            "table_id": "orders-current",
            "table_url": "memory://velorix/input/",
            "format": "Parquet",
            "unexpected": true,
        }),
    )
    .await;

    let error = catalog.get("orders-current").await.unwrap_err();

    assert!(matches!(error, PersistedTableError::Json(_)));
}

#[tokio::test]
async fn persisted_table_store_rejects_unknown_format_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 1,
            "table_id": "orders-current",
            "table_url": "memory://velorix/input/",
            "format": "Csv",
        }),
    )
    .await;

    let error = catalog.get("orders-current").await.unwrap_err();

    assert!(matches!(error, PersistedTableError::Json(_)));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_raw_url_spec_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 1,
            "table_id": "orders-current",
            "table_url": "memory://velorix/input/",
            "format": "Parquet",
        }),
    )
    .await;

    let error = catalog.get_production("orders-current").await.unwrap_err();

    assert!(matches!(error, PersistedTableError::RawUrlProductionSpec));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_unknown_spec_field_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 1,
            "table_id": "orders-current",
            "tenant_id": "tenant-a",
            "store_id": "primary",
            "object_key_prefix": "tenants/tenant-a/tables/orders",
            "snapshot_ref": "snapshots/0001",
            "format": "parquet",
            "schema_fingerprint": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "query_policy_id": "standard",
            "unexpected": true,
        }),
    )
    .await;

    let error = catalog.get_production("orders-current").await.unwrap_err();

    assert!(matches!(error, PersistedTableError::Json(_)));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_missing_query_policy_id_as_json() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    write_table_catalog_object(
        Arc::clone(&store),
        "orders-current",
        json!({
            "schema_version": 1,
            "table_id": "orders-current",
            "tenant_id": "tenant-a",
            "store_id": "primary",
            "object_key_prefix": "tenants/tenant-a/tables/orders",
            "snapshot_ref": "snapshots/0001",
            "format": "parquet",
            "schema_fingerprint": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }),
    )
    .await;

    let error = catalog.get_production("orders-current").await.unwrap_err();

    assert!(matches!(error, PersistedTableError::Json(_)));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_unsafe_query_policy_id() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    create_orders_relation_catalog(&store).await;
    let registry = StorageRegistry::new();
    let mut request = production_request("primary", "tenants/tenant-a/tables/orders");
    request.query_policy_id = "standard/base".to_string();

    let error = catalog
        .create_production(Arc::clone(&store), Arc::clone(&store), &registry, request)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::InvalidProductionField {
            field: "query_policy_id"
        }
    ));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_cross_tenant_prefix() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    create_orders_relation_catalog(&store).await;
    let registry = StorageRegistry::new();

    let error = catalog
        .create_production(
            Arc::clone(&store),
            Arc::clone(&store),
            &registry,
            production_request("primary", "tenants/tenant-b/tables/orders"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::CrossTenantPrefix {
            tenant_id,
            object_key_prefix,
        } if tenant_id == "tenant-a"
            && object_key_prefix == "tenants/tenant-b/tables/orders"
    ));
}

#[tokio::test]
async fn production_persisted_table_store_creates_spec_when_relation_catalog_fingerprint_matches() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    let relation_catalog = create_orders_relation_catalog(&store).await;
    create_standard_policy(&store, QueryPolicy::default()).await;
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(&mut registry, scan_store, Arc::clone(&store)).await;

    let spec = catalog
        .create_production(
            Arc::clone(&store),
            Arc::clone(&store),
            &registry,
            production_request("primary", "tenants/tenant-a/tables/orders"),
        )
        .await
        .unwrap();

    assert_eq!(spec.relation_id, "orders");
    assert_eq!(spec.relation_version, "2026-05-05.v1");
    assert_eq!(
        spec.schema_fingerprint,
        relation_catalog.schema_fingerprint.as_str()
    );
}

#[tokio::test]
async fn production_persisted_table_store_rejects_unregistered_store_before_writing() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    create_orders_relation_catalog(&store).await;
    create_standard_policy(&store, QueryPolicy::default()).await;
    let registry = StorageRegistry::new();

    let error = catalog
        .create_production(
            Arc::clone(&store),
            Arc::clone(&store),
            &registry,
            production_request("missing-store", "tenants/tenant-a/tables/orders"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::StorageRegistry(
            velorix_runtime::storage_registry::StorageRegistryError::UnregisteredStoreId {
                store_id,
            }
        ) if store_id == "missing-store"
    ));
    let key = ObjectKey::query_table("orders-current").unwrap();
    assert!(matches!(
        store.head(&Path::from(key.as_str())).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_store_without_capabilities_before_writing() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    create_orders_relation_catalog(&store).await;
    create_standard_policy(&store, QueryPolicy::default()).await;
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    registry
        .register("primary", "memory://velorix/", scan_store)
        .unwrap();

    let error = catalog
        .create_production(
            Arc::clone(&store),
            Arc::clone(&store),
            &registry,
            production_request("primary", "tenants/tenant-a/tables/orders"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::StorageRegistry(
            velorix_runtime::storage_registry::StorageRegistryError::MissingProductionCapabilities {
                store_id,
            }
        ) if store_id == "primary"
    ));
    let key = ObjectKey::query_table("orders-current").unwrap();
    assert!(matches!(
        store.head(&Path::from(key.as_str())).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_missing_policy_before_writing() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    create_orders_relation_catalog(&store).await;
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(&mut registry, scan_store, Arc::clone(&store)).await;

    let error = catalog
        .create_production(
            Arc::clone(&store),
            Arc::clone(&store),
            &registry,
            production_request("primary", "tenants/tenant-a/tables/orders"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::QueryPolicyCatalog(QueryPolicyCatalogError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
    let key = ObjectKey::query_table("orders-current").unwrap();
    assert!(matches!(
        store.head(&Path::from(key.as_str())).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_unbounded_policy_before_writing() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    create_orders_relation_catalog(&store).await;
    QueryPolicyCatalogStore::new(Arc::clone(&store))
        .create("tenant-a", "standard", QueryPolicy::default())
        .await
        .unwrap();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(&mut registry, scan_store, Arc::clone(&store)).await;

    let error = catalog
        .create_production(
            Arc::clone(&store),
            Arc::clone(&store),
            &registry,
            production_request("primary", "tenants/tenant-a/tables/orders"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::QueryPolicyCatalog(QueryPolicyCatalogError::Policy(
            QueryPolicyError::MissingProductionTableScanLimit {
                field: "max_sql_bytes"
            }
        ))
    ));
    let key = ObjectKey::query_table("orders-current").unwrap();
    assert!(matches!(
        store.head(&Path::from(key.as_str())).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_relation_catalog_fingerprint_mismatch_before_writing(
) {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));
    create_orders_relation_catalog(&store).await;
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(&mut registry, scan_store, Arc::clone(&store)).await;
    let mut request = production_request("primary", "tenants/tenant-a/tables/orders");
    request.schema_fingerprint = format!("sha256:{}", "1".repeat(64));

    let error = catalog
        .create_production(Arc::clone(&store), Arc::clone(&store), &registry, request)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RelationCatalogFingerprintMismatch { .. }
    ));
    let key = ObjectKey::query_table("orders-current").unwrap();
    assert!(matches!(
        store.head(&Path::from(key.as_str())).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn production_persisted_table_store_rejects_non_table_relation_registration_before_writing() {
    let (_temp_dir, store) = temp_store();
    let mut catalog = orders_relation_catalog();
    catalog.datafusion_registration.mode = DataFusionRegistrationModeV1::View;
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(&mut registry, scan_store, Arc::clone(&store)).await;

    let error = PersistedTableStore::new(Arc::clone(&store))
        .create_production(
            Arc::clone(&store),
            Arc::clone(&store),
            &registry,
            production_request("primary", "tenants/tenant-a/tables/orders"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedTableError::RelationSchema(_)));
    let key = ObjectKey::query_table("orders-current").unwrap();
    let missing = store.get(&Path::from(key.as_str())).await.unwrap_err();
    assert!(matches!(missing, object_store::Error::NotFound { .. }));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_unregistered_store_id() {
    let (_temp_dir, catalog_store) = temp_store();
    let registry = StorageRegistry::new();

    create_orders_relation_catalog(&catalog_store).await;
    create_standard_policy(&catalog_store, QueryPolicy::default()).await;
    write_production_table_catalog_object(
        &catalog_store,
        production_request("missing-store", "tenants/tenant-a/tables/orders"),
    )
    .await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select key_json, value_json, weight from input",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::StorageRegistry(
            velorix_runtime::storage_registry::StorageRegistryError::UnregisteredStoreId {
                store_id,
            }
        ) if store_id == "missing-store"
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_store_registered_without_capabilities() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    registry
        .register("primary", "memory://velorix/", Arc::clone(&scan_store))
        .unwrap();

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select key_json, value_json, weight from input",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::StorageRegistry(
            velorix_runtime::storage_registry::StorageRegistryError::MissingProductionCapabilities {
                store_id,
            }
        ) if store_id == "primary"
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_missing_capabilities_before_malformed_relation_catalog(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    registry
        .register("primary", "memory://velorix/", Arc::clone(&scan_store))
        .unwrap();

    create_orders_relation_catalog(&catalog_store).await;
    create_standard_policy(&catalog_store, QueryPolicy::default()).await;
    write_production_table_catalog_object(
        &catalog_store,
        production_request("primary", "tenants/tenant-a/tables/orders"),
    )
    .await;
    overwrite_orders_relation_catalog_with_malformed_json(&catalog_store).await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::StorageRegistry(
            velorix_runtime::storage_registry::StorageRegistryError::MissingProductionCapabilities {
                store_id,
            }
        ) if store_id == "primary"
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_missing_table_catalog_capabilities_before_malformed_table_json(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let counting_catalog_store = Arc::new(CountingStore::new(Arc::clone(&catalog_store)));
    let counted_catalog_store: Arc<dyn ObjectStore> = counting_catalog_store.clone();
    let registry = StorageRegistry::new();
    write_malformed_table_catalog_json(&catalog_store, "orders-current").await;

    let error = query_production_persisted_object_backed_input(
        Arc::clone(&counted_catalog_store),
        Arc::clone(&counted_catalog_store),
        Arc::clone(&counted_catalog_store),
        &registry,
        &capabilities_missing(AuthoritativeNamespace::TableCatalog),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert_missing_persisted_table_capability(error, AuthoritativeNamespace::TableCatalog);
    assert_eq!(counting_catalog_store.get_count(), 0);
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_weak_table_catalog_capabilities_before_malformed_table_json(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let counting_catalog_store = Arc::new(CountingStore::new(Arc::clone(&catalog_store)));
    let counted_catalog_store: Arc<dyn ObjectStore> = counting_catalog_store.clone();
    let registry = StorageRegistry::new();
    write_malformed_table_catalog_json(&catalog_store, "orders-current").await;

    let error = query_production_persisted_object_backed_input(
        Arc::clone(&counted_catalog_store),
        Arc::clone(&counted_catalog_store),
        Arc::clone(&counted_catalog_store),
        &registry,
        &capabilities_with_weak_namespace(AuthoritativeNamespace::TableCatalog),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert_weak_persisted_table_capability(error, AuthoritativeNamespace::TableCatalog);
    assert_eq!(counting_catalog_store.get_count(), 0);
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_missing_query_policy_capabilities_before_malformed_policy_json(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let (_policy_temp_dir, policy_store) = temp_store();
    let counting_policy_store = Arc::new(CountingStore::new(Arc::clone(&policy_store)));
    let counted_policy_store: Arc<dyn ObjectStore> = counting_policy_store.clone();
    create_orders_relation_catalog(&catalog_store).await;
    write_production_table_catalog_object(
        &catalog_store,
        production_request("primary", "tenants/tenant-a/tables/orders"),
    )
    .await;
    write_malformed_query_policy_json(&policy_store, "tenant-a", "standard").await;
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    let error = query_production_persisted_object_backed_input(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&counted_policy_store),
        &registry,
        &capabilities_missing(AuthoritativeNamespace::QueryPolicy),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert_missing_policy_catalog_capability(error, AuthoritativeNamespace::QueryPolicy);
    assert_eq!(counting_policy_store.get_count(), 0);
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_weak_query_policy_capabilities_before_malformed_policy_json(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let (_policy_temp_dir, policy_store) = temp_store();
    let counting_policy_store = Arc::new(CountingStore::new(Arc::clone(&policy_store)));
    let counted_policy_store: Arc<dyn ObjectStore> = counting_policy_store.clone();
    create_orders_relation_catalog(&catalog_store).await;
    write_production_table_catalog_object(
        &catalog_store,
        production_request("primary", "tenants/tenant-a/tables/orders"),
    )
    .await;
    write_malformed_query_policy_json(&policy_store, "tenant-a", "standard").await;
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    let error = query_production_persisted_object_backed_input(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&counted_policy_store),
        &registry,
        &capabilities_with_weak_namespace(AuthoritativeNamespace::QueryPolicy),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert_weak_policy_catalog_capability(error, AuthoritativeNamespace::QueryPolicy);
    assert_eq!(counting_policy_store.get_count(), 0);
}

#[tokio::test]
async fn production_object_backed_table_query_accepts_capability_registered_store_and_scans_parquet_snapshot(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(
            &["account-a", "account-a", "account-b"],
            &[10, 5, 7],
            &[1, 1, -1],
        ),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;

    let output = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, sum(value) as total_value, sum(weight) as total_weight \
         from orders where weight > 0 group by account_id order by account_id",
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "account-a");
    assert_eq!(int64_value(&output[0], 1, 0), 15);
    assert_eq!(int64_value(&output[0], 2, 0), 2);
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_parquet_schema_not_matching_relation_catalog()
{
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_value_as_text_batch(&["account-a"], &["10"], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(_))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_does_not_register_legacy_input_table_name() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a"], &[10], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select key_json, value_json, weight from input",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(_))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_stale_relation_catalog_fingerprint() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;
    overwrite_orders_relation_catalog(&catalog_store, mutated_orders_relation_catalog()).await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RelationCatalogFingerprintMismatch { .. }
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_non_table_relation_registration_mode() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a"], &[10], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;
    let mut catalog = orders_relation_catalog();
    catalog.datafusion_registration.mode = DataFusionRegistrationModeV1::View;
    overwrite_orders_relation_catalog(&catalog_store, catalog).await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(error, PersistedTableError::RelationSchema(_)));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_missing_catalog_policy() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;
    let policy_key = ObjectKey::query_policy("tenant-a", "standard").unwrap();
    catalog_store
        .delete(&Path::from(policy_key.as_str()))
        .await
        .unwrap();

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::QueryPolicyCatalog(QueryPolicyCatalogError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_unbounded_catalog_policy() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;
    overwrite_standard_policy(&catalog_store, QueryPolicy::default()).await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::QueryPolicyCatalog(QueryPolicyCatalogError::Policy(
            QueryPolicyError::MissingProductionTableScanLimit {
                field: "max_sql_bytes"
            }
        ))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_applies_catalog_policy_id() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a", "account-b"], &[10, 7], &[1, 1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        QueryExecutionPolicyV1 {
            max_output_rows: Some(1),
            ..QueryExecutionPolicyV1::default()
        },
    )
    .await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders order by account_id",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::OutputRowsExceeded {
                observed_rows: 2,
                max_rows: 1,
            }
        )))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_requires_shared_limiter_when_catalog_concurrency_limit_is_set(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterRequired {
                max_concurrent_queries: 1,
            }
        )))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_accepts_shared_limiter_when_catalog_concurrency_limit_is_set(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a"], &[10], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;
    let policy = production_policy_with(QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    });

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        policy,
    )
    .await;

    let output = query_production_table_with_limiter(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
        QueryExecutionLimiter::from_policy(policy),
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
}

#[tokio::test]
async fn production_object_backed_table_query_with_metrics_uses_registry_and_relation_table() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a", "account-b"], &[10, 7], &[1, 1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;
    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;

    let output = query_production_persisted_object_backed_input_with_metrics(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders order by account_id",
    )
    .await
    .unwrap();

    assert_eq!(output.batches.len(), 1);
    assert_eq!(output.batches[0].num_rows(), 2);
    assert_eq!(string_value(&output.batches[0], 0, 0), "account-a");
    assert!(output.object_requests.list_count >= 1);
    assert!(
        output.object_requests.get_count + output.object_requests.range_read_count >= 1,
        "expected production table scan metrics to include a parquet read"
    );
}

#[tokio::test]
async fn production_object_backed_table_query_with_metrics_requires_shared_limiter_when_catalog_concurrency_limit_is_set(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await;

    let error = query_production_persisted_object_backed_input_with_metrics(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterRequired {
                max_concurrent_queries: 1,
            }
        )))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_with_metrics_accepts_shared_limiter_when_catalog_concurrency_limit_is_set(
) {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a"], &[10], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;
    let policy = production_policy_with(QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    });
    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        policy,
    )
    .await;

    let output = query_production_persisted_object_backed_input_with_limiter_and_metrics(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        ProductionPersistedTableQueryRequest {
            registry: &registry,
            startup_capabilities: &all_namespace_capabilities(),
            tenant_id: "tenant-a",
            table_id: "orders-current",
            sql: "select account_id, value, weight from orders",
            limiter: QueryExecutionLimiter::from_policy(policy),
        },
    )
    .await
    .unwrap();

    assert_eq!(output.batches.len(), 1);
    assert_eq!(output.batches[0].num_rows(), 1);
    assert!(output.object_requests.list_count >= 1);
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_limiter_that_does_not_match_catalog_policy() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;
    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        production_policy_with(QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        }),
    )
    .await;

    let oversized_limiter = QueryExecutionLimiter::from_policy(QueryPolicy {
        max_concurrent_queries: Some(2),
        ..QueryPolicy::default()
    });
    let error = query_production_table_with_limiter(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
        oversized_limiter,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterPolicyMismatch {
                required_max_concurrent_queries: 1,
                actual_max_concurrent_queries: 2,
            }
        )))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_cross_tenant_catalog_policy_use() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table(&catalog_store, "primary", "tenants/tenant-a/tables/orders").await;
    QueryPolicyCatalogStore::new(Arc::clone(&catalog_store))
        .create_for_production_table_scan(
            "tenant-b",
            "standard",
            production_policy_with(QueryPolicy::default()),
        )
        .await
        .unwrap();
    delete_orders_relation_catalog(&catalog_store).await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-b",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::CrossTenantPrefix {
            tenant_id,
            object_key_prefix,
        } if tenant_id == "tenant-b"
            && object_key_prefix == "tenants/tenant-a/tables/orders"
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_scan_above_file_count_limit() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a"], &[10], &[1]),
    )
    .await;
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-001.parquet",
        &parquet_orders_batch(&["account-b"], &[7], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        QueryPolicy {
            max_scan_files: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            velorix_core::query::QueryPolicyError::ScanFilesExceeded {
                observed_files: 2,
                max_files: 1,
            }
        )))
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_scan_above_byte_limit() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a"], &[10], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        QueryPolicy {
            max_scan_bytes: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ScanBytesExceeded {
                observed_bytes,
                max_bytes: 1,
            }
        ))) if observed_bytes > 1
    ));
}

#[tokio::test]
async fn production_object_backed_table_query_rejects_object_requests_above_limit() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a"], &[10], &[1]),
    )
    .await;
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-001.parquet",
        &parquet_orders_batch(&["account-b"], &[7], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        QueryPolicy {
            max_object_requests: Some(2),
            ..QueryPolicy::default()
        },
    )
    .await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders limit 1",
    )
    .await
    .unwrap_err();

    let PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
        QueryPolicyError::ObjectRequestsExceeded {
            observed_requests,
            max_requests,
        },
    ))) = error
    else {
        panic!("expected object request policy error, got {error:?}");
    };
    assert!(observed_requests > max_requests);
    assert_eq!(max_requests, 2);
}

#[tokio::test]
async fn production_object_backed_table_query_still_applies_output_row_limit() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_orders_batch(&["account-a", "account-b"], &[10, 7], &[1, 1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    register_production_scan_store(
        &mut registry,
        Arc::clone(&scan_store),
        Arc::clone(&catalog_store),
    )
    .await;

    create_production_table_with_policy(
        &catalog_store,
        "primary",
        "tenants/tenant-a/tables/orders",
        QueryPolicy {
            max_output_rows: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await;

    let error = query_production_table(
        &catalog_store,
        &registry,
        &all_namespace_capabilities(),
        "tenant-a",
        "orders-current",
        "select account_id, value, weight from orders order by account_id",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            velorix_core::query::QueryPolicyError::OutputRowsExceeded {
                observed_rows: 2,
                max_rows: 1,
            }
        )))
    ));
}

#[tokio::test]
async fn persisted_object_backed_table_query_loads_stored_parquet_url_and_scans_with_datafusion() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "input/part-000.parquet",
        &parquet_input_batch(
            &["\"account-a\"", "\"account-a\"", "\"account-b\""],
            &["10", "5", "7"],
            &[1, 1, -1],
        ),
    )
    .await;

    PersistedTableStore::new(Arc::clone(&catalog_store))
        .create(
            "orders-current",
            "memory://velorix/input/",
            PersistedTableFormat::Parquet,
        )
        .await
        .unwrap();

    let output = query_persisted_object_backed_input_with_policy(
        Arc::clone(&catalog_store),
        Arc::clone(&scan_store),
        "orders-current",
        "select key_json, sum(cast(value_json as int)) as total_value, sum(weight) as total_weight \
         from input where weight > 0 group by key_json order by key_json",
        QueryPolicy::default(),
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(int64_value(&output[0], 1, 0), 15);
    assert_eq!(int64_value(&output[0], 2, 0), 2);
}

#[tokio::test]
async fn persisted_object_backed_table_query_propagates_datafusion_errors() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());

    PersistedTableStore::new(Arc::clone(&catalog_store))
        .create(
            "orders-current",
            "memory://velorix/input/",
            PersistedTableFormat::Parquet,
        )
        .await
        .unwrap();

    let error = query_persisted_object_backed_input_with_policy(
        Arc::clone(&catalog_store),
        Arc::clone(&scan_store),
        "orders-current",
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedTableError::RuntimeQuery(RuntimeQueryError::Query(QueryError::DataFusion(_)))
    ));
}

async fn create_production_table(
    catalog_store: &Arc<dyn ObjectStore>,
    store_id: &str,
    object_key_prefix: &str,
) {
    create_production_table_with_policy(
        catalog_store,
        store_id,
        object_key_prefix,
        QueryPolicy::default(),
    )
    .await;
}

async fn create_production_table_with_policy(
    catalog_store: &Arc<dyn ObjectStore>,
    store_id: &str,
    object_key_prefix: &str,
    policy: QueryPolicy,
) {
    create_orders_relation_catalog(catalog_store).await;
    create_standard_policy(catalog_store, policy).await;
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(&mut registry, scan_store, Arc::clone(catalog_store)).await;
    PersistedTableStore::new_checked(Arc::clone(catalog_store), &all_namespace_capabilities())
        .unwrap()
        .create_production(
            Arc::clone(catalog_store),
            Arc::clone(catalog_store),
            &registry,
            production_request(store_id, object_key_prefix),
        )
        .await
        .unwrap();
}

async fn query_production_table(
    catalog_store: &Arc<dyn ObjectStore>,
    registry: &StorageRegistry,
    startup_capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    tenant_id: &str,
    table_id: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, PersistedTableError> {
    query_production_persisted_object_backed_input(
        Arc::clone(catalog_store),
        Arc::clone(catalog_store),
        Arc::clone(catalog_store),
        registry,
        startup_capabilities,
        tenant_id,
        table_id,
        sql,
    )
    .await
}

async fn query_production_table_with_limiter(
    catalog_store: &Arc<dyn ObjectStore>,
    registry: &StorageRegistry,
    startup_capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    tenant_id: &str,
    table_id: &str,
    sql: &str,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, PersistedTableError> {
    query_production_persisted_object_backed_input_with_limiter(
        Arc::clone(catalog_store),
        Arc::clone(catalog_store),
        Arc::clone(catalog_store),
        ProductionPersistedTableQueryRequest {
            registry,
            startup_capabilities,
            tenant_id,
            table_id,
            sql,
            limiter,
        },
    )
    .await
}

async fn write_malformed_table_catalog_json(store: &Arc<dyn ObjectStore>, table_id: &str) {
    let key = ObjectKey::query_table(table_id).unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(br#"{"schema_version":"#).into(),
        )
        .await
        .unwrap();
}

async fn write_malformed_query_policy_json(
    store: &Arc<dyn ObjectStore>,
    tenant_id: &str,
    query_policy_id: &str,
) {
    let key = ObjectKey::query_policy(tenant_id, query_policy_id).unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(br#"{"schema_version":"#).into(),
        )
        .await
        .unwrap();
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

fn capabilities_missing(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.remove(&namespace);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

fn capabilities_with_weak_namespace(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profile = ObjectStoreCapabilityProfile::local_development();
    profile.conditional_create = false;
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.insert(namespace, profile);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

fn assert_missing_persisted_table_capability(
    error: PersistedTableError,
    expected_namespace: AuthoritativeNamespace,
) {
    assert!(matches!(
        error,
        PersistedTableError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace { namespace }
        ) if namespace == expected_namespace
    ));
}

fn assert_weak_persisted_table_capability(
    error: PersistedTableError,
    expected_namespace: AuthoritativeNamespace,
) {
    assert!(matches!(
        error,
        PersistedTableError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source }
        ) if namespace == expected_namespace
            && source.required_capability() == RequiredObjectStoreCapability::ConditionalCreate
    ));
}

fn assert_missing_policy_catalog_capability(
    error: PersistedTableError,
    expected_namespace: AuthoritativeNamespace,
) {
    assert!(matches!(
        error,
        PersistedTableError::QueryPolicyCatalog(QueryPolicyCatalogError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace { namespace }
        )) if namespace == expected_namespace
    ));
}

fn assert_weak_policy_catalog_capability(
    error: PersistedTableError,
    expected_namespace: AuthoritativeNamespace,
) {
    assert!(matches!(
        error,
        PersistedTableError::QueryPolicyCatalog(QueryPolicyCatalogError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source }
        )) if namespace == expected_namespace
            && source.required_capability() == RequiredObjectStoreCapability::ConditionalCreate
    ));
}

async fn create_orders_relation_catalog(store: &Arc<dyn ObjectStore>) -> VelorixRelationCatalogV1 {
    let catalog = orders_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&catalog)
        .await
        .unwrap();

    catalog
}

async fn overwrite_orders_relation_catalog(
    store: &Arc<dyn ObjectStore>,
    catalog: VelorixRelationCatalogV1,
) {
    let key = RelationCatalogRegistry::new(Arc::clone(store))
        .object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from(serde_json::to_vec(&catalog).unwrap()).into(),
        )
        .await
        .unwrap();
}

async fn overwrite_orders_relation_catalog_with_malformed_json(store: &Arc<dyn ObjectStore>) {
    let catalog = orders_relation_catalog();
    let key = RelationCatalogRegistry::new(Arc::clone(store))
        .object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(br#"{"schema_version":"#).into(),
        )
        .await
        .unwrap();
}

async fn delete_orders_relation_catalog(store: &Arc<dyn ObjectStore>) {
    let catalog = orders_relation_catalog();
    let key = RelationCatalogRegistry::new(Arc::clone(store))
        .object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .unwrap();
    store.delete(&Path::from(key.as_str())).await.unwrap();
}

fn orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "value".to_string(),
                name: "value".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-v1".to_string(),
        },
    }
}

fn mutated_orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = orders_relation_catalog();
    catalog.relation_schema.relation_name = "orders_mutated".to_string();
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.feldera_relation.schema_fingerprint = schema_fingerprint;

    catalog
}

async fn write_table_catalog_object(
    store: Arc<dyn ObjectStore>,
    table_id: &str,
    catalog: serde_json::Value,
) {
    let key = ObjectKey::query_table(table_id).unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from(serde_json::to_vec(&catalog).unwrap()).into(),
        )
        .await
        .unwrap();
}

async fn write_production_table_catalog_object(
    store: &Arc<dyn ObjectStore>,
    request: CreateProductionPersistedTableSpecRequest,
) {
    write_table_catalog_object(
        Arc::clone(store),
        &request.table_id,
        json!({
            "schema_version": 1,
            "table_id": request.table_id,
            "tenant_id": request.tenant_id,
            "store_id": request.store_id,
            "object_key_prefix": request.object_key_prefix,
            "snapshot_ref": request.snapshot_ref,
            "format": "parquet",
            "relation_id": request.relation_id,
            "relation_version": request.relation_version,
            "schema_fingerprint": request.schema_fingerprint,
            "query_policy_id": request.query_policy_id,
        }),
    )
    .await;
}

async fn create_standard_policy(store: &Arc<dyn ObjectStore>, policy: QueryPolicy) {
    QueryPolicyCatalogStore::new_checked(Arc::clone(store), &all_namespace_capabilities())
        .unwrap()
        .create_for_production_table_scan("tenant-a", "standard", production_policy_with(policy))
        .await
        .unwrap();
}

async fn overwrite_standard_policy(store: &Arc<dyn ObjectStore>, policy: QueryPolicy) {
    let record = QueryPolicyCatalogRecord {
        schema_version: QUERY_POLICY_CATALOG_SCHEMA_VERSION,
        tenant_id: "tenant-a".to_string(),
        query_policy_id: "standard".to_string(),
        policy,
    };
    let key = ObjectKey::query_policy("tenant-a", "standard").unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
        )
        .await
        .unwrap();
}

fn production_policy_with(policy: QueryPolicy) -> QueryPolicy {
    QueryPolicy {
        max_sql_bytes: policy.max_sql_bytes.or(Some(16 * 1024)),
        planning_timeout_ms: policy.planning_timeout_ms.or(Some(1_000)),
        execution_timeout_ms: policy.execution_timeout_ms.or(Some(10_000)),
        max_output_rows: policy.max_output_rows.or(Some(1_000)),
        max_output_bytes: policy.max_output_bytes.or(Some(1_000_000)),
        max_scan_files: policy.max_scan_files.or(Some(100)),
        max_scan_bytes: policy.max_scan_bytes.or(Some(128 * 1024 * 1024)),
        max_object_requests: policy.max_object_requests.or(Some(1_000)),
        max_concurrent_queries: policy.max_concurrent_queries,
        memory_limit_bytes: policy.memory_limit_bytes.or(Some(512 * 1024 * 1024)),
        spill_limit_bytes: policy.spill_limit_bytes.or(Some(1024 * 1024 * 1024)),
        batch_size: policy.batch_size,
        target_partitions: policy.target_partitions,
    }
}

fn parquet_input_batch(keys: &[&str], values: &[&str], weights: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key_json", DataType::Utf8, false),
            Field::new("value_json", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn parquet_orders_batch(account_ids: &[&str], values: &[i64], weights: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn parquet_orders_value_as_text_batch(
    account_ids: &[&str],
    values: &[&str],
    weights: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn parquet_bytes(batch: &RecordBatch) -> Bytes {
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    Bytes::from(bytes)
}

async fn put_parquet_input(
    store: &Arc<dyn DataFusionObjectStore>,
    path: &str,
    batch: &RecordBatch,
) {
    store
        .put(&DataFusionPath::from(path), parquet_bytes(batch).into())
        .await
        .unwrap();
}

fn string_value(batch: &RecordBatch, column: usize, row: usize) -> String {
    let column = batch.column(column);
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return array.value(row).to_string();
    }
    if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        return array.value(row).to_string();
    }

    panic!(
        "expected string-compatible column, got {:?}",
        column.data_type()
    );
}

fn int64_value(batch: &RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}

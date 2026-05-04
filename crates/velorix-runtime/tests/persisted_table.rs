use std::sync::Arc;

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
use velorix_core::query::{QueryError, QueryPolicy};
use velorix_runtime::{
    persisted_table::{
        query_persisted_object_backed_input_with_policy,
        query_production_persisted_object_backed_input_with_policy, PersistedTableError,
        PersistedTableFormat, PersistedTableStore, ProductionPersistedTableFormat,
    },
    query::RuntimeQueryError,
    storage_registry::StorageRegistry,
};
use velorix_storage::object_key::ObjectKey;

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
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
async fn production_persisted_table_store_rejects_cross_tenant_prefix() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedTableStore::new(Arc::clone(&store));

    let error = catalog
        .create_production(
            "orders-current",
            "tenant-a",
            "primary",
            "tenants/tenant-b/tables/orders",
            "snapshots/0001",
            ProductionPersistedTableFormat::Parquet,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "standard",
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
async fn production_object_backed_table_query_rejects_unregistered_store_id() {
    let (_temp_dir, catalog_store) = temp_store();
    let registry = StorageRegistry::new();

    PersistedTableStore::new(Arc::clone(&catalog_store))
        .create_production(
            "orders-current",
            "tenant-a",
            "missing-store",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
            ProductionPersistedTableFormat::Parquet,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "standard",
        )
        .await
        .unwrap();

    let error = query_production_persisted_object_backed_input_with_policy(
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
        "orders-current",
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
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
async fn production_object_backed_table_query_resolves_registered_store_and_scans_parquet_snapshot()
{
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_input_batch(
            &["\"account-a\"", "\"account-a\"", "\"account-b\""],
            &["10", "5", "7"],
            &[1, 1, -1],
        ),
    )
    .await;
    let mut registry = StorageRegistry::new();
    registry
        .register("primary", "memory://velorix/", Arc::clone(&scan_store))
        .unwrap();

    PersistedTableStore::new(Arc::clone(&catalog_store))
        .create_production(
            "orders-current",
            "tenant-a",
            "primary",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
            ProductionPersistedTableFormat::Parquet,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "standard",
        )
        .await
        .unwrap();

    let output = query_production_persisted_object_backed_input_with_policy(
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
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
async fn production_object_backed_table_query_rejects_scan_above_file_count_limit() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-001.parquet",
        &parquet_input_batch(&["\"account-b\""], &["7"], &[1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    registry
        .register("primary", "memory://velorix/", Arc::clone(&scan_store))
        .unwrap();

    PersistedTableStore::new(Arc::clone(&catalog_store))
        .create_production(
            "orders-current",
            "tenant-a",
            "primary",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
            ProductionPersistedTableFormat::Parquet,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "standard",
        )
        .await
        .unwrap();

    let error = query_production_persisted_object_backed_input_with_policy(
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
        "orders-current",
        "select key_json, value_json, weight from input",
        QueryPolicy {
            max_scan_files: Some(1),
            ..QueryPolicy::default()
        },
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
async fn production_object_backed_table_query_still_applies_output_row_limit() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "tenants/tenant-a/tables/orders/snapshots/0001/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
    )
    .await;
    let mut registry = StorageRegistry::new();
    registry
        .register("primary", "memory://velorix/", Arc::clone(&scan_store))
        .unwrap();

    PersistedTableStore::new(Arc::clone(&catalog_store))
        .create_production(
            "orders-current",
            "tenant-a",
            "primary",
            "tenants/tenant-a/tables/orders",
            "snapshots/0001",
            ProductionPersistedTableFormat::Parquet,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "standard",
        )
        .await
        .unwrap();

    let error = query_production_persisted_object_backed_input_with_policy(
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
        "orders-current",
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            max_output_rows: Some(1),
            ..QueryPolicy::default()
        },
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

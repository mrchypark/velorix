use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::object_store::{
    memory::InMemory as DataFusionInMemory, path::Path as DataFusionPath,
    ObjectStore as DataFusionObjectStore, ObjectStoreExt as DataFusionObjectStoreExt,
};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;
use velorix_core::query::{QueryError, QueryPolicy, QueryPolicyError};
use velorix_runtime::{
    persisted_query::{PersistedQueryError, PersistedQueryStore},
    persisted_table::{PersistedTableError, PersistedTableFormat, PersistedTableStore},
    persisted_view::{query_persisted_object_backed_view, PersistedViewError},
    query::RuntimeQueryError,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[tokio::test]
async fn persisted_object_backed_view_loads_stored_table_url_sql_and_policy_when_querying_parquet_input(
) {
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
    PersistedQueryStore::new(Arc::clone(&catalog_store))
        .create(
            "positive-account-totals",
            "select key_json, sum(cast(value_json as int)) as total_value, sum(weight) as total_weight \
             from input where weight > 0 group by key_json order by key_json",
            QueryPolicy {
                max_output_rows: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let output = query_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&scan_store),
        "orders-current",
        "positive-account-totals",
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
async fn persisted_object_backed_view_applies_stored_policy_when_output_exceeds_limit() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &scan_store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
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
    PersistedQueryStore::new(Arc::clone(&catalog_store))
        .create(
            "too-many-rows",
            "select key_json, value_json, weight from input order by key_json",
            QueryPolicy {
                max_output_rows: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let error = query_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&scan_store),
        "orders-current",
        "too-many-rows",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::OutputRowsExceeded {
                observed_rows: 2,
                max_rows: 1
            }
        )))
    ));
}

#[tokio::test]
async fn persisted_object_backed_view_propagates_missing_query_catalog_error() {
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

    let error = query_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&scan_store),
        "orders-current",
        "missing-query",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::QueryCatalog(PersistedQueryError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

#[tokio::test]
async fn persisted_object_backed_view_propagates_missing_table_catalog_error() {
    let (_temp_dir, catalog_store) = temp_store();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());

    PersistedQueryStore::new(Arc::clone(&catalog_store))
        .create(
            "all-rows",
            "select key_json, value_json, weight from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap();

    let error = query_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&scan_store),
        "missing-table",
        "all-rows",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::TableCatalog(PersistedTableError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

#[tokio::test]
async fn persisted_object_backed_view_propagates_datafusion_scan_errors() {
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
    PersistedQueryStore::new(Arc::clone(&catalog_store))
        .create(
            "all-rows",
            "select key_json, value_json, weight from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap();

    let error = query_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&scan_store),
        "orders-current",
        "all-rows",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::RuntimeQuery(RuntimeQueryError::Query(QueryError::DataFusion(_)))
    ));
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

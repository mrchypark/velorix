use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::object_store::{
    memory::InMemory as DataFusionInMemory, path::Path as DataFusionPath,
    ObjectStore as DataFusionObjectStore, ObjectStoreExt as DataFusionObjectStoreExt,
};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, PutMode};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;
use velorix_core::{
    query::{QueryError, QueryPolicy, QueryPolicyError},
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_runtime::{
    persisted_query::{
        PersistedQueryError, PersistedQuerySpec, PersistedQueryStore,
        PERSISTED_QUERY_SCHEMA_VERSION,
    },
    persisted_table::{
        CreateProductionPersistedTableSpecRequest, PersistedTableError, PersistedTableFormat,
        PersistedTableStore, ProductionPersistedTableFormat,
    },
    persisted_view::{
        query_persisted_object_backed_view, query_production_persisted_object_backed_view,
        query_production_persisted_object_backed_view_with_limiter, PersistedViewError,
        ProductionPersistedViewQueryRequest,
    },
    query::{QueryExecutionLimiter, RuntimeQueryError},
    query_policy_catalog::QueryPolicyCatalogStore,
    storage_registry::StorageRegistry,
};
use velorix_storage::{object_key::ObjectKey, relation_catalog_registry::RelationCatalogRegistry};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
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

#[tokio::test]
async fn production_persisted_object_backed_view_rejects_concurrency_policy_without_shared_limiter()
{
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
        QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await;
    create_persisted_query(&catalog_store).await;

    let error = query_production_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
        "orders-current",
        "orders-view",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterRequired {
                max_concurrent_queries: 1,
            }
        )))
    ));
}

#[tokio::test]
async fn production_persisted_object_backed_view_accepts_matching_shared_limiter() {
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
    create_production_table_with_policy(&catalog_store, policy).await;
    create_persisted_query(&catalog_store).await;

    let output = query_production_persisted_object_backed_view_with_limiter(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        ProductionPersistedViewQueryRequest {
            registry: &registry,
            tenant_id: "tenant-a",
            table_id: "orders-current",
            query_id: "orders-view",
            limiter: QueryExecutionLimiter::from_policy(policy),
        },
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "account-a");
    assert_eq!(int64_value(&output[0], 1, 0), 10);
    assert_eq!(int64_value(&output[0], 2, 0), 1);
}

#[tokio::test]
async fn production_persisted_object_backed_view_rejects_bootstrap_input_query_record() {
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
        production_policy_with(QueryPolicy::default()),
    )
    .await;
    PersistedQueryStore::new(Arc::clone(&catalog_store))
        .create(
            "bootstrap-input-query",
            "select key_json from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap();

    let error = query_production_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
        "orders-current",
        "bootstrap-input-query",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::QueryCatalog(PersistedQueryError::Query(_))
    ));
}

#[tokio::test]
async fn production_persisted_object_backed_view_validates_sql_with_table_policy_before_execution()
{
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
        production_policy_with(QueryPolicy {
            max_sql_bytes: Some(40),
            ..QueryPolicy::default()
        }),
    )
    .await;
    create_persisted_query(&catalog_store).await;

    let error = query_production_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
        "orders-current",
        "orders-view",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::QueryCatalog(PersistedQueryError::Query(QueryError::Policy(
            QueryPolicyError::SqlTextTooLarge {
                actual_bytes: _,
                max_bytes: 40,
            }
        )))
    ));
}

#[tokio::test]
async fn production_persisted_object_backed_view_accepts_production_relation_query_record() {
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
        production_policy_with(QueryPolicy::default()),
    )
    .await;
    create_production_persisted_query(&catalog_store).await;

    let output = query_production_persisted_object_backed_view(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        &registry,
        "tenant-a",
        "orders-current",
        "orders-view",
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "account-a");
    assert_eq!(int64_value(&output[0], 1, 0), 10);
    assert_eq!(int64_value(&output[0], 2, 0), 1);
}

#[tokio::test]
async fn production_persisted_object_backed_view_rejects_mismatched_shared_limiter() {
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
        production_policy_with(QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        }),
    )
    .await;
    create_persisted_query(&catalog_store).await;

    let error = query_production_persisted_object_backed_view_with_limiter(
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        Arc::clone(&catalog_store),
        ProductionPersistedViewQueryRequest {
            registry: &registry,
            tenant_id: "tenant-a",
            table_id: "orders-current",
            query_id: "orders-view",
            limiter: QueryExecutionLimiter::from_policy(QueryPolicy {
                max_concurrent_queries: Some(2),
                ..QueryPolicy::default()
            }),
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedViewError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterPolicyMismatch {
                required_max_concurrent_queries: 1,
                actual_max_concurrent_queries: 2,
            }
        )))
    ));
}

async fn create_production_table_with_policy(
    catalog_store: &Arc<dyn ObjectStore>,
    policy: QueryPolicy,
) {
    let catalog = orders_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(catalog_store))
        .create(&catalog)
        .await
        .unwrap();
    QueryPolicyCatalogStore::new(Arc::clone(catalog_store))
        .create_for_production_table_scan("tenant-a", "standard", production_policy_with(policy))
        .await
        .unwrap();
    let scan_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let mut registry = StorageRegistry::new();
    register_production_scan_store(&mut registry, scan_store, Arc::clone(catalog_store)).await;
    PersistedTableStore::new(Arc::clone(catalog_store))
        .create_production(
            Arc::clone(catalog_store),
            Arc::clone(catalog_store),
            &registry,
            CreateProductionPersistedTableSpecRequest {
                table_id: "orders-current".to_string(),
                tenant_id: "tenant-a".to_string(),
                store_id: "primary".to_string(),
                object_key_prefix: "tenants/tenant-a/tables/orders".to_string(),
                snapshot_ref: "snapshots/0001".to_string(),
                format: ProductionPersistedTableFormat::Parquet,
                relation_id: "orders".to_string(),
                relation_version: "2026-05-05.v1".to_string(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                query_policy_id: "standard".to_string(),
            },
        )
        .await
        .unwrap();
}

async fn create_persisted_query(catalog_store: &Arc<dyn ObjectStore>) {
    let spec = PersistedQuerySpec {
        schema_version: PERSISTED_QUERY_SCHEMA_VERSION,
        query_id: "orders-view".to_string(),
        sql: "select account_id, value, weight from orders order by account_id".to_string(),
        policy: QueryPolicy::default(),
    };
    let key = ObjectKey::persisted_query(&spec.query_id).unwrap();

    catalog_store
        .put_opts(
            &Path::from(key.as_str()),
            Bytes::from(serde_json::to_vec(&spec).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
}

async fn create_production_persisted_query(catalog_store: &Arc<dyn ObjectStore>) {
    PersistedQueryStore::new(Arc::clone(catalog_store))
        .create_for_production_relation(
            "orders-view",
            "select account_id, value, weight from orders order by account_id",
            QueryPolicy::default(),
            &orders_relation_catalog(),
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

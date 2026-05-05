use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::object_store::{
    path::Path as DataFusionPath, ObjectStore as DataFusionObjectStore,
};
use object_store::{
    aws::AmazonS3Builder as AuthorityS3Builder, ObjectStore as AuthorityObjectStore,
};
use object_store_13::{aws::AmazonS3Builder as DataFusionS3Builder, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use velorix_core::{
    query::QueryPolicy,
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_runtime::{
    persisted_table::{
        query_production_persisted_object_backed_input, CreateProductionPersistedTableSpecRequest,
        PersistedTableStore, ProductionPersistedTableFormat,
    },
    query_policy_catalog::QueryPolicyCatalogStore,
    storage_registry::StorageRegistry,
};
use velorix_storage::relation_catalog_registry::RelationCatalogRegistry;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult = Result<(), TestError>;

#[tokio::test]
async fn s3_compatible_production_table_query_scans_parquet_through_registry() -> TestResult {
    let Some(config) = live_config() else {
        println!("skipping S3 runtime query harness; set VELORIX_S3_COMPAT=1 to enable");
        return Ok(());
    };

    let authority_store = authority_store(&config)?;
    let scan_store = scan_store(&config)?;
    let object_key_prefix = format!("{}/tenants/tenant-a/tables/orders", config.run_prefix);
    let snapshot_ref = "snapshots/0001";
    let parquet_path = format!("{object_key_prefix}/{snapshot_ref}/part-000.parquet");

    scan_store
        .put(
            &DataFusionPath::from(parquet_path.as_str()),
            parquet_bytes(&parquet_input_batch(
                &["\"account-a\"", "\"account-a\"", "\"account-b\""],
                &["10", "5", "7"],
                &[1, 1, -1],
            ))
            .into(),
        )
        .await?;

    let validation = async {
        create_orders_relation_catalog(&authority_store).await?;
        PersistedTableStore::new(Arc::clone(&authority_store))
            .create_production(
                Arc::clone(&authority_store),
                production_request("primary", &object_key_prefix),
            )
            .await?;
        QueryPolicyCatalogStore::new(Arc::clone(&authority_store))
            .create("tenant-a", "standard", QueryPolicy::default())
            .await?;

        let mut registry = StorageRegistry::new();
        registry
            .register_production_with_probe(
                "primary",
                &format!("s3://{}/", config.bucket),
                Arc::clone(&scan_store),
                Arc::clone(&authority_store),
                "s3-compatible",
                format!("{}/capability-probes", config.run_prefix),
            )
            .await?;

        let batches = query_production_persisted_object_backed_input(
            Arc::clone(&authority_store),
            Arc::clone(&authority_store),
            Arc::clone(&authority_store),
            &registry,
            "tenant-a",
            "orders-current",
            "select key_json, sum(cast(value_json as int)) as total_value, sum(weight) as total_weight \
             from input where weight > 0 group by key_json order by key_json",
        )
        .await?;

        if batches.len() != 1 || batches[0].num_rows() != 1 {
            return Err(test_error("expected one aggregated output row"));
        }
        if string_value(&batches[0], 0, 0) != "\"account-a\"" {
            return Err(test_error("unexpected grouped key from S3-backed query"));
        }

        Ok(())
    }
    .await;

    let _ = cleanup_prefix(scan_store.as_ref(), &config.run_prefix).await;
    validation
}

fn authority_store(config: &LiveConfig) -> Result<Arc<dyn AuthorityObjectStore>, TestError> {
    Ok(Arc::new(
        AuthorityS3Builder::new()
            .with_endpoint(config.endpoint.clone())
            .with_access_key_id(config.access_key_id.clone())
            .with_secret_access_key(config.secret_access_key.clone())
            .with_region(config.region.clone())
            .with_bucket_name(config.bucket.clone())
            .with_allow_http(config.allow_http)
            .build()?,
    ))
}

fn scan_store(config: &LiveConfig) -> Result<Arc<dyn DataFusionObjectStore>, TestError> {
    Ok(Arc::new(
        DataFusionS3Builder::new()
            .with_endpoint(config.endpoint.clone())
            .with_access_key_id(config.access_key_id.clone())
            .with_secret_access_key(config.secret_access_key.clone())
            .with_region(config.region.clone())
            .with_bucket_name(config.bucket.clone())
            .with_allow_http(config.allow_http)
            .build()?,
    ))
}

async fn cleanup_prefix(store: &dyn DataFusionObjectStore, prefix: &str) -> TestResult {
    use futures::TryStreamExt;

    let objects = store
        .list(Some(&DataFusionPath::from(prefix)))
        .try_collect::<Vec<_>>()
        .await?;
    for object in objects {
        let _ = store.delete(&object.location).await;
    }

    Ok(())
}

async fn create_orders_relation_catalog(
    store: &Arc<dyn AuthorityObjectStore>,
) -> Result<(), TestError> {
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&orders_relation_catalog())
        .await?;
    Ok(())
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

fn orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        columns: vec![
            relation_column(
                "key_json",
                VelorixLogicalTypeV1::Json,
                ArrowPhysicalTypeV1::JsonUtf8,
                RelationSemanticRoleV1::PrimaryKey,
                0,
            ),
            relation_column(
                "value_json",
                VelorixLogicalTypeV1::Json,
                ArrowPhysicalTypeV1::JsonUtf8,
                RelationSemanticRoleV1::Value,
                1,
            ),
            relation_column(
                "weight",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Weight,
                2,
            ),
        ],
        primary_key_column_ids: vec!["key_json".to_string()],
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

fn relation_column(
    id: &str,
    logical_type: VelorixLogicalTypeV1,
    physical_arrow_type: ArrowPhysicalTypeV1,
    semantic_role: RelationSemanticRoleV1,
    ordinal: u32,
) -> RelationColumnV1 {
    RelationColumnV1 {
        column_id: id.to_string(),
        name: id.to_string(),
        logical_type,
        physical_arrow_type,
        nullable: false,
        ordinal,
        semantic_role,
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

fn parquet_bytes(batch: &RecordBatch) -> Bytes {
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    Bytes::from(bytes)
}

fn string_value(batch: &RecordBatch, column: usize, row: usize) -> String {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected string column")
        .value(row)
        .to_string()
}

fn test_error(message: impl Into<String>) -> TestError {
    Box::new(std::io::Error::other(message.into()))
}

struct LiveConfig {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    bucket: String,
    allow_http: bool,
    run_prefix: String,
}

fn live_config() -> Option<LiveConfig> {
    if std::env::var("VELORIX_S3_COMPAT").ok().as_deref() != Some("1") {
        return None;
    }

    let endpoint = required_env("AWS_ENDPOINT_URL");
    let prefix = std::env::var("VELORIX_S3_PREFIX").unwrap_or_default();
    let run_prefix = join_prefixes(&prefix, &unique_run_prefix());
    let allow_http = endpoint.starts_with("http://");

    Some(LiveConfig {
        endpoint,
        access_key_id: required_env("AWS_ACCESS_KEY_ID"),
        secret_access_key: required_env("AWS_SECRET_ACCESS_KEY"),
        region: required_env("AWS_REGION"),
        bucket: required_env("VELORIX_S3_BUCKET"),
        allow_http,
        run_prefix,
    })
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required when VELORIX_S3_COMPAT=1"))
}

fn unique_run_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after Unix epoch")
        .as_nanos();

    format!("velorix-s3-runtime/{}-{nanos}", std::process::id())
}

fn join_prefixes(base: &str, run: &str) -> String {
    match base.trim_matches('/') {
        "" => run.to_string(),
        base => format!("{base}/{run}"),
    }
}

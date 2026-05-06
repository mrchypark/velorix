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
    aws::{AmazonS3, AmazonS3Builder as AuthorityS3Builder},
    prefix::PrefixStore,
    ObjectStore as AuthorityObjectStore,
};
use object_store_13::{aws::AmazonS3Builder as DataFusionS3Builder, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
    operator::KeyedSumCountAggregate,
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
    recovery::{
        RecoveredRuntime, ORDERS_SUM_COUNT_OWNER, ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
    },
    storage_registry::StorageRegistry,
};
use velorix_storage::{
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{IngestLog, ReplayCheckpoint},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    relation_catalog_registry::RelationCatalogRegistry,
    state::{CheckpointPublisher, StateObjectWrite},
};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult = Result<(), TestError>;

#[tokio::test]
async fn s3_compatible_production_table_query_scans_parquet_through_registry() -> TestResult {
    let Some(config) = live_config() else {
        println!("skipping S3 runtime query harness; set VELORIX_S3_COMPAT=1 to enable");
        return Ok(());
    };

    let authority_store = prefixed_authority_store(&config)?;
    let scan_store = scan_store(&config)?;
    let object_key_prefix = "tenants/tenant-a/tables/orders".to_string();
    let snapshot_ref = "snapshots/0001";
    let parquet_path = format!(
        "{}/{object_key_prefix}/{snapshot_ref}/part-000.parquet",
        config.run_prefix
    );

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
                &format!("s3://{}/{}/", config.bucket, config.run_prefix),
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
        if int64_value(&batches[0], 1, 0) != 15 || int64_value(&batches[0], 2, 0) != 2 {
            return Err(test_error("unexpected aggregate values from S3-backed query"));
        }

        Ok(())
    }
    .await;

    let _ = cleanup_prefix(scan_store.as_ref(), &config.run_prefix).await;
    validation
}

#[tokio::test]
async fn s3_compatible_runtime_recovery_reads_checkpoint_and_replays_validated_ingest() -> TestResult
{
    let Some(config) = live_config() else {
        println!("skipping S3 runtime recovery harness; set VELORIX_S3_COMPAT=1 to enable");
        return Ok(());
    };

    let raw_store = scan_store(&config)?;
    let store = prefixed_authority_store(&config)?;
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = delta_batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
    ]);
    let replay_input = delta_batch([
        input_delta("account-a", 3, 1),
        input_delta("account-b", 7, 1),
    ]);

    let validation = async {
        create_recovery_relation_catalog(&store).await?;
        append_ingest_envelope(&ingest_log, 0, 2, &checkpoint_input).await?;
        append_ingest_envelope(&ingest_log, 2, 4, &replay_input).await?;

        let mut checkpointed_view = KeyedSumCountAggregate::new();
        checkpointed_view.apply(&checkpoint_input)?;
        let state_ref = write_checkpoint_state(&publisher, &checkpointed_view.state()).await?;
        publisher
            .publish_manifest(&recovery_manifest(2, state_ref))
            .await?;

        let recovered = RecoveredRuntime::recover_with_owner_and_relation_catalog_record(
            Arc::clone(&store),
            ORDERS_SUM_COUNT_OWNER,
            ORDERS_SUM_COUNT_RELATION_ID,
            ORDERS_SUM_COUNT_RELATION_VERSION,
        )
        .await?;

        if recovered.latest_checkpoint_version() != Some(0) {
            return Err(test_error(
                "expected recovery to load checkpoint manifest 0",
            ));
        }
        if recovered.replay_checkpoints() != [ReplayCheckpoint::new("orders", 0, 2)] {
            return Err(test_error(
                "expected replay to start after checkpoint offset 2",
            ));
        }
        if recovered.replayed_batch_count() != 1 || recovered.logical_epoch() != 3 {
            return Err(test_error(
                "expected exactly one post-checkpoint replay batch",
            ));
        }
        if recovered.materialized_state().net_rows()? != expected_recovered_rows() {
            return Err(test_error("unexpected recovered aggregate state"));
        }

        Ok(())
    }
    .await;

    let _ = cleanup_prefix(raw_store.as_ref(), &config.run_prefix).await;
    validation
}

fn prefixed_authority_store(
    config: &LiveConfig,
) -> Result<Arc<dyn AuthorityObjectStore>, TestError> {
    Ok(Arc::new(PrefixStore::new(
        authority_s3(config)?,
        config.run_prefix.as_str(),
    )))
}

fn authority_s3(config: &LiveConfig) -> Result<AmazonS3, TestError> {
    Ok(AuthorityS3Builder::new()
        .with_endpoint(config.endpoint.clone())
        .with_access_key_id(config.access_key_id.clone())
        .with_secret_access_key(config.secret_access_key.clone())
        .with_region(config.region.clone())
        .with_bucket_name(config.bucket.clone())
        .with_allow_http(config.allow_http)
        .build()?)
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

async fn create_recovery_relation_catalog(
    store: &Arc<dyn AuthorityObjectStore>,
) -> Result<(), TestError> {
    let catalog = velorix_runtime::recovery::orders_sum_count_relation_catalog()?;
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&catalog)
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

fn ingest_record_batch(input: &DeltaBatch) -> RecordBatch {
    let accounts = input
        .records()
        .iter()
        .map(|record| record.key.as_json().as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let amounts = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(accounts)) as ArrayRef,
            Arc::new(Int64Array::from(amounts)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
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

fn int64_value(batch: &RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected int64 column")
        .value(row)
}

async fn append_ingest_envelope(
    ingest_log: &IngestLog,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
) -> TestResult {
    let catalog = velorix_runtime::recovery::orders_sum_count_relation_catalog()?;
    let bytes = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive,
            end_offset_exclusive,
        },
        &[ingest_record_batch(input)],
    )?;
    ingest_log.append_validated_envelope(bytes).await?;
    Ok(())
}

async fn write_checkpoint_state(
    publisher: &CheckpointPublisher,
    state: &DeltaBatch,
) -> Result<StateObjectRef, TestError> {
    let checkpoint = EngineCheckpoint::new(2, state.clone());
    let state = StateObjectWrite::new(
        "orders_sum_count",
        0,
        0,
        "s3-runtime-recovery-state",
        Bytes::from(serde_json::to_vec(&checkpoint.to_payload())?),
    )?;

    Ok(publisher.write_state_object(&state).await?)
}

fn recovery_manifest(input_end: u64, state_ref: StateObjectRef) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![InputRange {
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: input_end,
        }],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-06T00:00:00Z".to_string(),
    }
}

fn input_delta(account: &str, amount: i64, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!(amount)),
        weight,
    )
}

fn delta_batch(records: impl IntoIterator<Item = DeltaRecord>) -> DeltaBatch {
    DeltaBatch::from_records(records)
}

fn expected_recovered_rows() -> Vec<DeltaRecord> {
    vec![
        DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({ "count": 3, "sum": 18 })),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!("account-b")),
            DeltaValue::from_json(json!({ "count": 1, "sum": 7 })),
            1,
        ),
    ]
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

use std::{
    error::Error,
    fmt,
    ops::Range,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};

mod materialized_output_workloads;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::object_store::{
    memory::InMemory as DataFusionInMemory, path::Path as DataFusionPath,
    ObjectStore as DataFusionObjectStore, ObjectStoreExt as DataFusionObjectStoreExt,
};
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, CopyOptions, GetOptions, GetRange, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{IncrementalEngine, PrototypeIncrementalEngine},
    query::QueryPolicy,
};
use velorix_runtime::benchmark_gate::{
    BenchmarkBackend, BenchmarkEvidenceScope, BenchmarkGateLevel, BenchmarkGateResultV1,
    BenchmarkMetricsV1, BenchmarkWorkloadMetricsV1, ObjectRequestMetricsV1,
};
use velorix_runtime::persisted_table::{
    query_production_persisted_object_backed_input_with_metrics,
    CreateProductionPersistedTableSpecRequest, PersistedTableStore, ProductionPersistedTableFormat,
};
use velorix_runtime::query_policy_catalog::QueryPolicyCatalogStore;
use velorix_runtime::recovery::{
    orders_sum_count_relation_catalog, RecoveredRuntime, ORDERS_SUM_COUNT_OWNER,
    ORDERS_SUM_COUNT_RELATION_ID, ORDERS_SUM_COUNT_RELATION_VERSION,
};
use velorix_runtime::storage_registry::StorageRegistry;
use velorix_storage::{
    capability::{
        probe_authoritative_object_store_capabilities, AuthoritativeNamespace,
        AuthoritativeObjectStoreCapabilitiesV1, ObjectStoreCapabilityProfile,
    },
    gc::{GarbageCollectionPlan, GarbageCollectionPolicy},
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::IngestAdmissionCoordinator,
    manifest::{CheckpointManifest, InputRange},
    relation_catalog_registry::RelationCatalogRegistry,
    state::{CheckpointPublisher, OutputObjectWrite, StateObjectWrite},
    state_store::{SlateDbStateStore, StateObjectStore},
};

const STREAM_ID: &str = "orders";
const PARTITION_ID: u32 = 0;
const BATCH_COUNT: u64 = 64;
const RECORDS_PER_BATCH: u64 = 16;
const CHECKPOINT_VERSION: u64 = 0;
const GC_EXECUTION_RUN_ID: &str = "local-incremental-gc-execution";

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

struct MeasuredWorkload {
    samples: Vec<Duration>,
    object_requests: ObjectRequestMetricsV1,
    scan_bytes: u64,
}

struct GcDryRunPlanningWorkload {
    measurement: MeasuredWorkload,
    policy: GarbageCollectionPolicy,
    plan: GarbageCollectionPlan,
    released_checkpoint_version: u64,
    released_object_key: String,
}

fn main() -> BenchResult<()> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

async fn run() -> BenchResult<()> {
    let (_temp_dir, metered_store, store) = temp_store()?;
    let capability_probe_requests_before = metered_store.snapshot();
    let capability_probe_started = Instant::now();
    let capabilities = probe_authoritative_object_store_capabilities(
        store.as_ref(),
        "local-benchmark",
        "local-incremental-startup-capability-probes",
    )
    .await?;
    let capability_probe_elapsed = capability_probe_started.elapsed();
    let capability_probe_requests =
        request_delta(&metered_store.snapshot(), &capability_probe_requests_before);
    let ingest_coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities)?;
    let publisher = CheckpointPublisher::new_checked(
        Arc::clone(&store),
        capability_profile(&capabilities, AuthoritativeNamespace::Checkpoint)?,
    )?;
    let mut engine = PrototypeIncrementalEngine::new();

    RelationCatalogRegistry::new_checked(
        Arc::clone(&store),
        capability_profile(&capabilities, AuthoritativeNamespace::RelationCatalog)?,
    )?
    .create(&orders_sum_count_relation_catalog()?)
    .await?;

    let mut total_records = 0;
    let mut ingest_samples = Vec::new();
    let mut ingest_requests = empty_object_requests();
    let ingest_started = Instant::now();

    for batch_index in 0..BATCH_COUNT {
        let input = workload_batch(batch_index, RECORDS_PER_BATCH);
        let start_offset = total_records;
        let end_offset = start_offset + RECORDS_PER_BATCH;

        let requests_before = metered_store.snapshot();
        let validation_started = Instant::now();
        append_ingest_envelope(&ingest_coordinator, start_offset, end_offset, &input).await?;
        ingest_samples.push(validation_started.elapsed());
        add_request_delta(
            &mut ingest_requests,
            &metered_store.snapshot(),
            &requests_before,
        );
        engine.push_changes(batch_index + 1, &input)?;

        total_records = end_offset;
    }

    let ingest_elapsed = ingest_started.elapsed();
    let checkpoint_requests_before = metered_store.snapshot();
    let checkpoint_started = Instant::now();
    let checkpoint = engine.checkpoint_state();
    let state_ref = publisher
        .write_state_object(&StateObjectWrite::new(
            ORDERS_SUM_COUNT_OWNER,
            PARTITION_ID,
            CHECKPOINT_VERSION,
            "local-incremental-state",
            Bytes::from(serde_json::to_vec(&checkpoint.to_payload())?),
        )?)
        .await?;
    let checkpoint_state_key = state_ref.object_key.as_str().to_string();
    publisher
        .publish_manifest(&CheckpointManifest {
            schema_version: 1,
            checkpoint_version: CHECKPOINT_VERSION,
            input_ranges: vec![InputRange {
                stream_id: STREAM_ID.to_string(),
                partition_id: PARTITION_ID,
                start_offset_inclusive: 0,
                end_offset_exclusive: total_records,
            }],
            state_objects: vec![state_ref],
            output_objects: vec![],
            parent_checkpoint: None,
            created_at: "2026-05-03T00:00:00Z".to_string(),
        })
        .await?;
    let checkpoint_elapsed = checkpoint_started.elapsed();
    let checkpoint_requests = request_delta(&metered_store.snapshot(), &checkpoint_requests_before);

    let tail_input = workload_batch(BATCH_COUNT, RECORDS_PER_BATCH);
    let requests_before = metered_store.snapshot();
    let validation_started = Instant::now();
    append_ingest_envelope(
        &ingest_coordinator,
        total_records,
        total_records + RECORDS_PER_BATCH,
        &tail_input,
    )
    .await?;
    ingest_samples.push(validation_started.elapsed());
    add_request_delta(
        &mut ingest_requests,
        &metered_store.snapshot(),
        &requests_before,
    );

    let recovery_requests_before = metered_store.snapshot();
    let recovery_started = Instant::now();
    let recovered = RecoveredRuntime::recover_with_owner_and_relation_catalog_record_checked(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
    )
    .await?;
    let recovery_elapsed = recovery_started.elapsed();
    let recovery_requests = request_delta(&metered_store.snapshot(), &recovery_requests_before);
    let recovered_rows = recovered.materialized_state().net_rows()?;

    assert_eq!(
        recovered.latest_checkpoint_version(),
        Some(CHECKPOINT_VERSION)
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert!(!recovered_rows.is_empty());

    let gc_dry_run_planning = gc_dry_run_planning(
        &publisher,
        &metered_store,
        &checkpoint_state_key,
        total_records,
    )
    .await?;
    let gc_execution_evidence =
        gc_execution_evidence(&publisher, &metered_store, &gc_dry_run_planning).await?;
    let slatedb_state_reopen = slatedb_state_reopen(
        Arc::clone(&store),
        Arc::clone(&metered_store),
        &capabilities,
    )
    .await?;
    let datafusion_scan = datafusion_table_scan(
        Arc::clone(&store),
        Arc::clone(&metered_store),
        &capabilities,
    )
    .await?;
    let materialized_output_workloads =
        materialized_output_workloads::run_materialized_output_workloads(
            Arc::clone(&store),
            || metered_store.snapshot(),
            CHECKPOINT_VERSION,
        )
        .await?;

    let records_per_second = total_records as f64 / ingest_elapsed.as_secs_f64();
    let mut object_requests = metered_store.snapshot();
    add_request_delta(
        &mut object_requests,
        &datafusion_scan.object_requests,
        &empty_object_requests(),
    );
    let result = BenchmarkGateResultV1 {
        schema_version: 1,
        commit: git_commit(),
        gate_level: BenchmarkGateLevel::PrSmoke,
        backend: BenchmarkBackend::Local,
        backend_evidence_scope: BenchmarkEvidenceScope::LiveOrNative,
        workload: "local_incremental".to_string(),
        metrics: BenchmarkMetricsV1 {
            rows_per_second: records_per_second,
            bytes_per_row: bytes_per_row(object_requests.bytes_written, total_records),
            put_per_gib: put_per_gib(object_requests.put_count, object_requests.bytes_written),
            object_requests,
            checkpoint_p50_ms: millis(checkpoint_elapsed),
            checkpoint_p95_ms: millis(checkpoint_elapsed),
            recovery_p95_ms: millis(recovery_elapsed),
            peak_rss_bytes: current_rss_bytes().unwrap_or(0),
            spill_bytes: 0,
            scan_bytes: datafusion_scan.scan_bytes,
        },
        workload_metrics: {
            let mut workload_metrics = vec![
                workload_metric(
                    "object_store_capability_probe",
                    &[capability_probe_elapsed],
                    capability_probe_requests,
                    0,
                ),
                workload_metric(
                    "ingest_envelope_validation",
                    &ingest_samples,
                    ingest_requests,
                    0,
                ),
                workload_metric(
                    "checkpoint_publish",
                    &[checkpoint_elapsed],
                    checkpoint_requests,
                    0,
                ),
                workload_metric(
                    "checkpoint_recovery",
                    &[recovery_elapsed],
                    recovery_requests,
                    0,
                ),
                workload_metric(
                    "datafusion_table_scan",
                    &datafusion_scan.samples,
                    datafusion_scan.object_requests,
                    datafusion_scan.scan_bytes,
                ),
                workload_metric(
                    "slatedb_state_reopen",
                    &slatedb_state_reopen.samples,
                    slatedb_state_reopen.object_requests,
                    slatedb_state_reopen.scan_bytes,
                ),
                workload_metric(
                    "gc_dry_run_planning",
                    &gc_dry_run_planning.measurement.samples,
                    gc_dry_run_planning.measurement.object_requests,
                    gc_dry_run_planning.measurement.scan_bytes,
                ),
                workload_metric(
                    "gc_execution_evidence",
                    &gc_execution_evidence.samples,
                    gc_execution_evidence.object_requests,
                    gc_execution_evidence.scan_bytes,
                ),
            ];
            workload_metrics.extend(materialized_output_workloads);
            workload_metrics
        },
    };
    result.validate()?;

    println!("{}", serde_json::to_string(&result)?);

    Ok(())
}

async fn gc_dry_run_planning(
    publisher: &CheckpointPublisher,
    metered_store: &MeteredObjectStore,
    previous_state_key: &str,
    parent_end_offset_exclusive: u64,
) -> BenchResult<GcDryRunPlanningWorkload> {
    let retained_state = StateObjectWrite::new(
        ORDERS_SUM_COUNT_OWNER,
        PARTITION_ID,
        CHECKPOINT_VERSION + 1,
        "gc-retained-state",
        Bytes::from_static(b"gc-retained-state"),
    )?;
    let orphan_state = StateObjectWrite::new(
        ORDERS_SUM_COUNT_OWNER,
        PARTITION_ID,
        CHECKPOINT_VERSION + 1,
        "gc-orphan-state",
        Bytes::from_static(b"gc-orphan-state"),
    )?;
    let retained_output = OutputObjectWrite::new(
        "settlements",
        PARTITION_ID,
        CHECKPOINT_VERSION + 1,
        0,
        RECORDS_PER_BATCH,
        "gc-retained-output",
        Bytes::from_static(b"gc-retained-output"),
    )?;
    let orphan_output = OutputObjectWrite::new(
        "settlements",
        PARTITION_ID,
        CHECKPOINT_VERSION + 1,
        RECORDS_PER_BATCH,
        RECORDS_PER_BATCH * 2,
        "gc-orphan-output",
        Bytes::from_static(b"gc-orphan-output"),
    )?;

    let retained_state_ref = publisher.write_state_object(&retained_state).await?;
    publisher.write_state_object(&orphan_state).await?;
    let retained_output_ref = publisher.write_output_object(&retained_output).await?;
    publisher.write_output_object(&orphan_output).await?;
    publisher
        .publish_manifest(&CheckpointManifest {
            schema_version: 1,
            checkpoint_version: CHECKPOINT_VERSION + 1,
            input_ranges: vec![InputRange {
                stream_id: STREAM_ID.to_string(),
                partition_id: PARTITION_ID,
                start_offset_inclusive: 0,
                end_offset_exclusive: parent_end_offset_exclusive + RECORDS_PER_BATCH,
            }],
            state_objects: vec![retained_state_ref],
            output_objects: vec![retained_output_ref],
            parent_checkpoint: Some(CHECKPOINT_VERSION),
            created_at: "2026-05-03T00:01:00Z".to_string(),
        })
        .await?;

    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let requests_before = metered_store.snapshot();
    let started = Instant::now();
    let plan = publisher.plan_garbage_collection(policy).await?;
    let elapsed = started.elapsed();

    assert_eq!(
        plan.retained_manifest_versions,
        vec![CHECKPOINT_VERSION + 1]
    );
    let candidate_keys = plan
        .candidates
        .iter()
        .map(|candidate| candidate.object_key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(candidate_keys.len(), 3);
    assert!(candidate_keys.contains(&previous_state_key));
    assert!(candidate_keys.contains(&orphan_state.object_key().as_str()));
    assert!(candidate_keys.contains(&orphan_output.object_key().as_str()));

    Ok(GcDryRunPlanningWorkload {
        measurement: MeasuredWorkload {
            samples: vec![elapsed],
            object_requests: request_delta(&metered_store.snapshot(), &requests_before),
            scan_bytes: 0,
        },
        policy,
        plan,
        released_checkpoint_version: CHECKPOINT_VERSION,
        released_object_key: previous_state_key.to_string(),
    })
}

async fn gc_execution_evidence(
    publisher: &CheckpointPublisher,
    metered_store: &MeteredObjectStore,
    planning: &GcDryRunPlanningWorkload,
) -> BenchResult<MeasuredWorkload> {
    let requests_before = metered_store.snapshot();
    let started = Instant::now();
    let run = publisher
        .execute_garbage_collection_plan_with_evidence(
            GC_EXECUTION_RUN_ID,
            planning.policy,
            &planning.plan,
        )
        .await?;
    let read_back = publisher
        .read_garbage_collection_run_evidence(GC_EXECUTION_RUN_ID)
        .await?;
    let retention_record = publisher
        .read_checkpoint_retention_record(planning.released_checkpoint_version)
        .await?;
    let elapsed = started.elapsed();

    assert_eq!(read_back, run);
    assert_eq!(retention_record.gc_run_id, GC_EXECUTION_RUN_ID);
    assert_eq!(
        retention_record.retained_manifest_versions,
        run.plan.retained_manifest_versions
    );
    assert!(run
        .report
        .deleted
        .iter()
        .any(|candidate| candidate.object_key.as_str() == planning.released_object_key));
    assert!(retention_record
        .deleted_candidate_keys
        .iter()
        .any(|object_key| object_key.as_str() == planning.released_object_key));

    Ok(MeasuredWorkload {
        samples: vec![elapsed],
        object_requests: request_delta(&metered_store.snapshot(), &requests_before),
        scan_bytes: 0,
    })
}

async fn datafusion_table_scan(
    authority_store: Arc<dyn ObjectStore>,
    metered_store: Arc<MeteredObjectStore>,
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
) -> BenchResult<MeasuredWorkload> {
    let inner: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let input = parquet_input_batch()?;
    let input_bytes = parquet_bytes(&input)?;
    let scan_bytes = input_bytes.len() as u64;
    let object_key_prefix = "tenants/tenant-a/tables/orders";
    let snapshot_ref = "snapshots/local-benchmark";
    let parquet_path = format!("{object_key_prefix}/{snapshot_ref}/part-000.parquet");

    inner
        .put(
            &DataFusionPath::from(parquet_path.as_str()),
            input_bytes.into(),
        )
        .await?;

    let requests_before = metered_store.snapshot();
    create_production_query_policy(&authority_store, capabilities).await?;
    let mut registry = StorageRegistry::new();
    registry.register_production_with_capabilities(
        "primary",
        "memory://velorix/",
        Arc::clone(&inner),
        capabilities.clone(),
    )?;
    PersistedTableStore::new_checked(Arc::clone(&authority_store), capabilities)?
        .create_production(
            Arc::clone(&authority_store),
            Arc::clone(&authority_store),
            &registry,
            capabilities,
            production_request("primary", object_key_prefix, snapshot_ref)?,
        )
        .await?;

    let started = Instant::now();
    let output = query_production_persisted_object_backed_input_with_metrics(
        Arc::clone(&authority_store),
        Arc::clone(&authority_store),
        Arc::clone(&authority_store),
        &registry,
        capabilities,
        "tenant-a",
        "orders-current",
        "select account_id, sum(amount) as total_value, sum(weight) as total_weight \
         from orders where weight > 0 group by account_id order by account_id",
    )
    .await?;
    let elapsed = started.elapsed();

    assert_eq!(output.batches.len(), 1);
    assert_eq!(output.batches[0].num_rows(), 2);

    let mut object_requests = request_delta(&metered_store.snapshot(), &requests_before);
    add_request_delta(
        &mut object_requests,
        &output.object_requests,
        &empty_object_requests(),
    );

    Ok(MeasuredWorkload {
        samples: vec![elapsed],
        object_requests,
        scan_bytes,
    })
}

async fn slatedb_state_reopen(
    object_store: Arc<dyn ObjectStore>,
    metered_store: Arc<MeteredObjectStore>,
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
) -> BenchResult<MeasuredWorkload> {
    let payload = Bytes::from_static(br#"{"state":"slatedb-reopen-smoke","version":1}"#);
    let state = StateObjectWrite::new(
        ORDERS_SUM_COUNT_OWNER,
        PARTITION_ID,
        CHECKPOINT_VERSION + 1,
        "slatedb-state-reopen",
        payload.clone(),
    )?;
    let requests_before = metered_store.snapshot();
    let started = Instant::now();

    let state_ref = {
        let state_store = SlateDbStateStore::open_authoritative(
            "v1/slatedb/benchmark-state",
            Arc::clone(&object_store),
            capabilities,
        )
        .await?;
        let state_ref = state_store.write_state_object(&state).await?;
        state_store.close().await?;
        state_ref
    };

    let reopened = SlateDbStateStore::open_authoritative(
        "v1/slatedb/benchmark-state",
        Arc::clone(&object_store),
        capabilities,
    )
    .await?;
    let recovered = reopened.read_state_object(&state_ref).await?;
    reopened.close().await?;
    let elapsed = started.elapsed();

    assert_eq!(recovered, payload);

    Ok(MeasuredWorkload {
        samples: vec![elapsed],
        object_requests: request_delta(&metered_store.snapshot(), &requests_before),
        scan_bytes: 0,
    })
}

fn temp_store() -> BenchResult<(TempDir, Arc<MeteredObjectStore>, Arc<dyn ObjectStore>)> {
    let temp_dir = tempfile::tempdir()?;
    let inner = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);
    let metered_store = Arc::new(MeteredObjectStore::new(inner));
    let store: Arc<dyn ObjectStore> = metered_store.clone();

    Ok((temp_dir, metered_store, store))
}

fn workload_batch(batch_index: u64, records: u64) -> DeltaBatch {
    DeltaBatch::from_records((0..records).map(|record_index| {
        let account = format!("account-{:03}", record_index % 32);
        let amount = ((batch_index + record_index) % 101) as i64;

        DeltaRecord::new(
            DeltaKey::from_json(json!(account)),
            DeltaValue::from_json(json!(amount)),
            1,
        )
    }))
}

fn parquet_input_batch() -> BenchResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![
                "account-a",
                "account-a",
                "account-b",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 5, 7])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
        ],
    )?)
}

fn parquet_bytes(batch: &RecordBatch) -> BenchResult<Bytes> {
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
    }
    Ok(Bytes::from(bytes))
}

fn ingest_record_batch(input: &DeltaBatch) -> BenchResult<RecordBatch> {
    let keys = input
        .records()
        .iter()
        .map(|record| {
            record
                .key
                .as_json()
                .as_str()
                .ok_or("prototype workload key must be a string")
                .map(str::to_string)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let values = input
        .records()
        .iter()
        .map(|record| {
            record
                .value
                .as_json()
                .as_i64()
                .ok_or("prototype workload value must be an int64")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )?)
}

async fn append_ingest_envelope(
    ingest_coordinator: &IngestAdmissionCoordinator,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
) -> BenchResult<()> {
    let batch = ingest_record_batch(input)?;
    let catalog = orders_sum_count_relation_catalog()?;
    let bytes = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: STREAM_ID.to_string(),
            partition_id: PARTITION_ID,
            start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: None,
        },
        &[batch],
    )?;

    ingest_coordinator
        .append_catalog_validated_envelope(bytes)
        .await?;
    Ok(())
}

async fn create_production_query_policy(
    store: &Arc<dyn ObjectStore>,
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
) -> BenchResult<()> {
    QueryPolicyCatalogStore::new_checked(Arc::clone(store), capabilities)?
        .create_for_production_table_scan("tenant-a", "standard", production_query_policy())
        .await?;
    Ok(())
}

fn capability_profile(
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    namespace: AuthoritativeNamespace,
) -> BenchResult<&ObjectStoreCapabilityProfile> {
    capabilities.profiles.get(&namespace).ok_or_else(|| {
        Box::new(std::io::Error::other(format!(
            "missing capability profile for authoritative namespace `{namespace}`"
        ))) as Box<dyn Error + Send + Sync>
    })
}

fn production_request(
    store_id: &str,
    object_key_prefix: &str,
    snapshot_ref: &str,
) -> BenchResult<CreateProductionPersistedTableSpecRequest> {
    let catalog = orders_sum_count_relation_catalog()?;
    Ok(CreateProductionPersistedTableSpecRequest {
        table_id: "orders-current".to_string(),
        tenant_id: "tenant-a".to_string(),
        store_id: store_id.to_string(),
        object_key_prefix: object_key_prefix.to_string(),
        snapshot_ref: snapshot_ref.to_string(),
        format: ProductionPersistedTableFormat::Parquet,
        relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
        relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
        schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
        query_policy_id: "standard".to_string(),
    })
}

fn production_query_policy() -> QueryPolicy {
    QueryPolicy {
        max_sql_bytes: Some(16 * 1024),
        planning_timeout_ms: Some(1_000),
        execution_timeout_ms: Some(10_000),
        max_output_rows: Some(1_000),
        max_output_bytes: Some(1_000_000),
        max_scan_files: Some(100),
        max_scan_bytes: Some(128 * 1024 * 1024),
        max_object_requests: Some(1_000),
        memory_limit_bytes: Some(512 * 1024 * 1024),
        spill_limit_bytes: Some(1024 * 1024 * 1024),
        ..QueryPolicy::default()
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn workload_metric(
    name: &str,
    samples: &[Duration],
    object_requests: ObjectRequestMetricsV1,
    scan_bytes: u64,
) -> BenchmarkWorkloadMetricsV1 {
    BenchmarkWorkloadMetricsV1 {
        name: name.to_string(),
        p50_ms: percentile_ms(samples, 0.50),
        p95_ms: percentile_ms(samples, 0.95),
        object_requests: Some(object_requests),
        scan_bytes,
    }
}

fn percentile_ms(samples: &[Duration], percentile: f64) -> f64 {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    let index = ((samples.len().saturating_sub(1)) as f64 * percentile).ceil() as usize;
    millis(samples[index])
}

fn empty_object_requests() -> ObjectRequestMetricsV1 {
    ObjectRequestMetricsV1 {
        put_count: 0,
        get_count: 0,
        list_count: 0,
        range_read_count: 0,
        bytes_written: 0,
        bytes_read: 0,
    }
}

fn add_request_delta(
    target: &mut ObjectRequestMetricsV1,
    after: &ObjectRequestMetricsV1,
    before: &ObjectRequestMetricsV1,
) {
    let delta = request_delta(after, before);
    target.put_count += delta.put_count;
    target.get_count += delta.get_count;
    target.list_count += delta.list_count;
    target.range_read_count += delta.range_read_count;
    target.bytes_written += delta.bytes_written;
    target.bytes_read += delta.bytes_read;
}

fn request_delta(
    after: &ObjectRequestMetricsV1,
    before: &ObjectRequestMetricsV1,
) -> ObjectRequestMetricsV1 {
    ObjectRequestMetricsV1 {
        put_count: after.put_count.saturating_sub(before.put_count),
        get_count: after.get_count.saturating_sub(before.get_count),
        list_count: after.list_count.saturating_sub(before.list_count),
        range_read_count: after
            .range_read_count
            .saturating_sub(before.range_read_count),
        bytes_written: after.bytes_written.saturating_sub(before.bytes_written),
        bytes_read: after.bytes_read.saturating_sub(before.bytes_read),
    }
}

#[derive(Debug)]
struct MeteredObjectStore {
    inner: Arc<dyn ObjectStore>,
    put_count: AtomicU64,
    get_count: AtomicU64,
    list_count: AtomicU64,
    range_read_count: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

impl MeteredObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            put_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            list_count: AtomicU64::new(0),
            range_read_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> ObjectRequestMetricsV1 {
        ObjectRequestMetricsV1 {
            put_count: self.put_count.load(Ordering::SeqCst),
            get_count: self.get_count.load(Ordering::SeqCst),
            list_count: self.list_count.load(Ordering::SeqCst),
            range_read_count: self.range_read_count.load(Ordering::SeqCst),
            bytes_written: self.bytes_written.load(Ordering::SeqCst),
            bytes_read: self.bytes_read.load(Ordering::SeqCst),
        }
    }

    fn record_put(&self, payload: &PutPayload) {
        self.put_count.fetch_add(1, Ordering::SeqCst);
        self.bytes_written
            .fetch_add(payload.content_length() as u64, Ordering::SeqCst);
    }

    fn record_get(&self, options: &GetOptions, result: &GetResult) {
        if options.head {
            return;
        }

        if let Some(range) = &options.range {
            self.range_read_count.fetch_add(1, Ordering::SeqCst);
            self.bytes_read
                .fetch_add(range_len(range, result.meta.size), Ordering::SeqCst);
        } else {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            self.bytes_read
                .fetch_add(result.meta.size, Ordering::SeqCst);
        }
    }
}

impl fmt::Display for MeteredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metered({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for MeteredObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.record_put(&payload);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.put_count.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options.clone()).await?;
        self.record_get(&options, &result);
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let bytes = self.inner.get_ranges(location, ranges).await?;
        self.range_read_count
            .fetch_add(ranges.len() as u64, Ordering::SeqCst);
        self.bytes_read.fetch_add(
            bytes.iter().map(|chunk| chunk.len() as u64).sum(),
            Ordering::SeqCst,
        );
        Ok(bytes)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_count.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_count.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.list_count.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn range_len(range: &GetRange, object_size: u64) -> u64 {
    match range {
        GetRange::Bounded(range) => range.end.min(object_size).saturating_sub(range.start),
        GetRange::Offset(offset) => object_size.saturating_sub(*offset),
        GetRange::Suffix(bytes) => object_size.min(*bytes),
    }
}

fn bytes_per_row(bytes_written: u64, rows: u64) -> f64 {
    if rows == 0 {
        0.0
    } else {
        bytes_written as f64 / rows as f64
    }
}

fn put_per_gib(put_count: u64, bytes_written: u64) -> f64 {
    if bytes_written == 0 {
        0.0
    } else {
        put_count as f64 / (bytes_written as f64 / 1_073_741_824.0)
    }
}

fn git_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|commit| !commit.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn current_rss_bytes() -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let rss_kib = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(rss_kib * 1024)
}

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
mod phase8_family_workloads;
mod scale_group_key_workloads;
mod scale_join_key_workloads;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, CopyOptions, GetOptions, GetRange, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    relation::{
        orders_sum_count_relation_catalog, VelorixRelationCatalogV1, ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochIdempotencyKey, NativeCodePolicy, RelationInputBatch,
        RelationInputEncodingV1, RuntimeCheckpoint, ScopedViewId, SnapshotPageRequest,
        StandingProgramIdentity, StandingProgramRuntime,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
};
use velorix_runtime::benchmark_gate::{
    BenchmarkBackend, BenchmarkEvidenceScope, BenchmarkGateLevel, BenchmarkGateResultV1,
    BenchmarkMetricsV1, BenchmarkWorkloadMetricsV1, ObjectRequestMetricsV1,
};
use velorix_runtime::materialized_view_runtime::{
    create_standing_runtime_with_sql_and_catalogs, restore_standing_runtime, CRATE_NAME,
};
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
const BATCH_COUNT: u64 = 256;
const RECORDS_PER_BATCH: u64 = 16;
const CHECKPOINT_VERSION: u64 = 0;
const CHECKPOINT_SAMPLE_COUNT: usize = 9;
const RECOVERY_SAMPLE_COUNT: usize = 9;
const STATE_OWNER: &str = "orders_sum_count";
const GC_EXECUTION_RUN_ID: &str = "local-incremental-gc-execution";
const MATERIALIZED_VIEW_SQL: &str =
    "select account_id, sum(amount) as sum, count(*) as count from orders group by account_id";
const MATERIALIZED_VIEW_ID: &str = "orders_by_account";

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
    let catalog = orders_sum_count_relation_catalog()?;
    let input_schema = catalog_input_relation_schema(&catalog)?;
    let output_schema = materialized_view_output_schema();
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &standing_identity(MATERIALIZED_VIEW_SQL),
        std::slice::from_ref(&catalog),
        MATERIALIZED_VIEW_SQL,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .map_err(std::io::Error::other)?;

    RelationCatalogRegistry::new_checked(
        Arc::clone(&store),
        capability_profile(&capabilities, AuthoritativeNamespace::RelationCatalog)?,
    )?
    .create(&catalog)
    .await?;

    let mut total_records = 0;
    let mut ingest_samples = Vec::new();
    let mut materialized_view_apply_samples = Vec::new();
    let mut materialized_view_apply_elapsed = Duration::ZERO;
    let mut ingest_requests = empty_object_requests();
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
        let materialization_started = Instant::now();
        runtime.apply_changes(
            batch_index + 1,
            EpochIdempotencyKey::new(format!("local-benchmark-epoch-{}", batch_index + 1))?,
            vec![relation_input_batch(
                &catalog,
                start_offset,
                end_offset,
                ingest_record_batch(&input)?,
            )],
        )?;
        let materialization_elapsed = materialization_started.elapsed();
        materialized_view_apply_samples.push(materialization_elapsed);
        materialized_view_apply_elapsed += materialization_elapsed;

        total_records = end_offset;
    }

    let checkpoint_requests_before = metered_store.snapshot();
    let checkpoint_started = Instant::now();
    let checkpoint = runtime.checkpoint()?;
    let checkpoint_bytes = serde_json::to_vec(&checkpoint)?;
    let state_ref = publisher
        .write_state_object(&StateObjectWrite::new(
            STATE_OWNER,
            PARTITION_ID,
            CHECKPOINT_VERSION,
            "local-incremental-state",
            Bytes::from(checkpoint_bytes.clone()),
        )?)
        .await?;
    let checkpoint_state_key = state_ref.object_key.as_str().to_string();
    let checkpoint_state_ref = state_ref.clone();
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
            relation_id: None,
            relation_version: None,
            schema_fingerprint: None,
        })
        .await?;
    let checkpoint_elapsed = checkpoint_started.elapsed();
    let mut checkpoint_samples = vec![checkpoint_elapsed];
    let mut checkpoint_requests =
        request_delta(&metered_store.snapshot(), &checkpoint_requests_before);
    for sample_index in 1..CHECKPOINT_SAMPLE_COUNT {
        let (_sample_temp_dir, sample_metered_store, sample_store) = temp_store()?;
        let sample_capabilities = probe_authoritative_object_store_capabilities(
            sample_store.as_ref(),
            "local-benchmark",
            &format!("checkpoint-publish-sample-{sample_index}"),
        )
        .await?;
        let sample_publisher = CheckpointPublisher::new_checked(
            Arc::clone(&sample_store),
            capability_profile(&sample_capabilities, AuthoritativeNamespace::Checkpoint)?,
        )?;
        let requests_before = sample_metered_store.snapshot();
        let started = Instant::now();
        let state_ref = sample_publisher
            .write_state_object(&StateObjectWrite::new(
                STATE_OWNER,
                PARTITION_ID,
                CHECKPOINT_VERSION,
                "local-incremental-state",
                Bytes::from(checkpoint_bytes.clone()),
            )?)
            .await?;
        sample_publisher
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
                relation_id: None,
                relation_version: None,
                schema_fingerprint: None,
            })
            .await?;
        checkpoint_samples.push(started.elapsed());
        add_request_delta(
            &mut checkpoint_requests,
            &sample_metered_store.snapshot(),
            &requests_before,
        );
    }

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

    let mut recovery_samples = Vec::with_capacity(RECOVERY_SAMPLE_COUNT);
    let mut recovery_requests = empty_object_requests();
    for _ in 0..RECOVERY_SAMPLE_COUNT {
        let requests_before = metered_store.snapshot();
        let recovery_started = Instant::now();
        let checkpoint_bytes = publisher.read_state_object(&checkpoint_state_ref).await?;
        let checkpoint: RuntimeCheckpoint = serde_json::from_slice(&checkpoint_bytes)?;
        let mut recovered = restore_standing_runtime(checkpoint).map_err(std::io::Error::other)?;
        recovered.apply_changes(
            BATCH_COUNT + 1,
            EpochIdempotencyKey::new("local-benchmark-tail")?,
            vec![relation_input_batch(
                &catalog,
                total_records,
                total_records + RECORDS_PER_BATCH,
                ingest_record_batch(&tail_input)?,
            )],
        )?;
        recovery_samples.push(recovery_started.elapsed());
        add_request_delta(
            &mut recovery_requests,
            &metered_store.snapshot(),
            &requests_before,
        );
        assert_materialized_view(&*recovered, BATCH_COUNT + 1)?;
    }

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
    let materialized_output_workloads =
        materialized_output_workloads::run_materialized_output_workloads(
            Arc::clone(&store),
            || metered_store.snapshot(),
            CHECKPOINT_VERSION,
        )
        .await?;
    let scale_group_key_workloads = scale_group_key_workloads::run()?;
    let scale_join_key_workloads = scale_join_key_workloads::run()?;
    let phase8_family_workloads = phase8_family_workloads::run()?;

    let records_per_second = total_records as f64 / materialized_view_apply_elapsed.as_secs_f64();
    let object_requests = metered_store.snapshot();
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
            checkpoint_p50_ms: percentile_ms(&checkpoint_samples, 0.50),
            checkpoint_p95_ms: percentile_ms(&checkpoint_samples, 0.95),
            recovery_p95_ms: percentile_ms(&recovery_samples, 0.95),
            peak_rss_bytes: current_rss_bytes().unwrap_or(0),
            spill_bytes: 0,
            scan_bytes: 0,
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
                    "native_sql_materialized_view_apply",
                    &materialized_view_apply_samples,
                    empty_object_requests(),
                    0,
                ),
                workload_metric(
                    "checkpoint_publish",
                    &checkpoint_samples,
                    checkpoint_requests,
                    0,
                ),
                workload_metric(
                    "checkpoint_recovery",
                    &recovery_samples,
                    recovery_requests,
                    0,
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
            workload_metrics.extend(scale_group_key_workloads.into_iter().map(|measurement| {
                workload_metric(
                    &measurement.name,
                    &measurement.samples,
                    empty_object_requests(),
                    0,
                )
            }));
            workload_metrics.extend(scale_join_key_workloads.into_iter().map(|measurement| {
                workload_metric(
                    &measurement.name,
                    &measurement.samples,
                    empty_object_requests(),
                    0,
                )
            }));
            workload_metrics.extend(phase8_family_workloads.into_iter().map(|measurement| {
                workload_metric(
                    &measurement.name,
                    &measurement.samples,
                    empty_object_requests(),
                    0,
                )
            }));
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
        STATE_OWNER,
        PARTITION_ID,
        CHECKPOINT_VERSION + 1,
        "gc-retained-state",
        Bytes::from_static(b"gc-retained-state"),
    )?;
    let orphan_state = StateObjectWrite::new(
        STATE_OWNER,
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
            relation_id: None,
            relation_version: None,
            schema_fingerprint: None,
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

async fn slatedb_state_reopen(
    object_store: Arc<dyn ObjectStore>,
    metered_store: Arc<MeteredObjectStore>,
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
) -> BenchResult<MeasuredWorkload> {
    let payload = Bytes::from_static(br#"{"state":"slatedb-reopen-smoke","version":1}"#);
    let state = StateObjectWrite::new(
        STATE_OWNER,
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

fn materialized_view_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: MATERIALIZED_VIEW_ID.to_string(),
        relation_name: MATERIALIZED_VIEW_ID.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: stable_bytes_hash(b"orders-by-account-output-v1"),
        columns: vec![
            ColumnSchema {
                name: "account_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn standing_identity(sql: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant-a".to_string(),
        program_id: "local-incremental-benchmark".to_string(),
        view_ids: vec![MATERIALIZED_VIEW_ID.to_string()],
        sql_hash: stable_bytes_hash(sql.as_bytes()),
        input_catalog_hash: stable_bytes_hash(b"orders-sum-count-catalog-v1"),
        output_schema_hash: stable_bytes_hash(b"orders-by-account-output-v1"),
        planner_identity: "velorix-logical-view-planner@1".to_string(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: CRATE_NAME.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }],
        runtime_capabilities: vec!["materialized-view-runtime-v1".to_string()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    }
}

fn relation_input_batch(
    catalog: &VelorixRelationCatalogV1,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    batch: RecordBatch,
) -> RelationInputBatch {
    RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: STREAM_ID.to_string(),
        partition_id: PARTITION_ID,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive,
        end_offset_exclusive,
        event_time_watermark: None,
        batches: vec![batch],
    }
}

fn assert_materialized_view(
    runtime: &(dyn StandingProgramRuntime + Send),
    logical_epoch: u64,
) -> BenchResult<()> {
    let page = runtime.materialized_view_page(
        ScopedViewId {
            tenant_id: "tenant-a".to_string(),
            program_id: "local-incremental-benchmark".to_string(),
            view_id: MATERIALIZED_VIEW_ID.to_string(),
        },
        SnapshotPageRequest {
            committed_epoch: Some(logical_epoch),
            page_token: None,
            max_rows: None,
        },
    )?;
    if page.logical_epoch != logical_epoch || page.batches.len() != 1 {
        return Err(std::io::Error::other("unexpected materialized view page").into());
    }
    let batch = &page.batches[0];
    let accounts = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| std::io::Error::other("materialized account column is not utf8"))?;
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| std::io::Error::other("materialized sum column is not int64"))?;
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| std::io::Error::other("materialized count column is not int64"))?;
    if batch.num_rows() != RECORDS_PER_BATCH as usize {
        return Err(std::io::Error::other("materialized view contents are incorrect").into());
    }
    for row in 0..batch.num_rows() {
        let account_index = accounts
            .value(row)
            .strip_prefix("account-")
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| std::io::Error::other("materialized account key is malformed"))?;
        let expected_sum = (0..=BATCH_COUNT)
            .map(|batch_index| ((batch_index + account_index) % 101) as i64)
            .sum::<i64>();
        if sums.value(row) != expected_sum || counts.value(row) != BATCH_COUNT as i64 + 1 {
            return Err(std::io::Error::other("materialized view contents are incorrect").into());
        }
    }
    Ok(())
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

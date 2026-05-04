use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{IncrementalEngine, PrototypeIncrementalEngine},
};
use velorix_runtime::recovery::{
    orders_sum_count_relation_catalog, RecoveredRuntime, ORDERS_SUM_COUNT_OWNER,
    ORDERS_SUM_COUNT_RELATION_ID, ORDERS_SUM_COUNT_RELATION_VERSION,
};
use velorix_storage::{
    ingest_envelope::IngestEnvelope,
    log::IngestLog,
    manifest::{CheckpointManifest, InputRange},
    state::{CheckpointPublisher, StateObjectWrite},
};

const STREAM_ID: &str = "orders";
const PARTITION_ID: u32 = 0;
const BATCH_COUNT: u64 = 64;
const RECORDS_PER_BATCH: u64 = 16;
const CHECKPOINT_VERSION: u64 = 0;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> BenchResult<()> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

async fn run() -> BenchResult<()> {
    let (_temp_dir, store) = temp_store()?;
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let mut engine = PrototypeIncrementalEngine::new();

    let mut total_records = 0;
    let mut max_view_freshness = Duration::ZERO;
    let ingest_started = Instant::now();

    for batch_index in 0..BATCH_COUNT {
        let batch_started = Instant::now();
        let input = workload_batch(batch_index, RECORDS_PER_BATCH);
        let start_offset = total_records;
        let end_offset = start_offset + RECORDS_PER_BATCH;

        append_ingest_envelope(&ingest_log, start_offset, end_offset, &input).await?;
        engine.push_changes(batch_index + 1, &input)?;

        max_view_freshness = max_view_freshness.max(batch_started.elapsed());
        total_records = end_offset;
    }

    let ingest_elapsed = ingest_started.elapsed();
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

    let tail_input = workload_batch(BATCH_COUNT, RECORDS_PER_BATCH);
    append_ingest_envelope(
        &ingest_log,
        total_records,
        total_records + RECORDS_PER_BATCH,
        &tail_input,
    )
    .await?;

    let recovery_started = Instant::now();
    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await?;
    let recovery_elapsed = recovery_started.elapsed();
    let recovered_rows = recovered.materialized_state().net_rows()?;

    assert_eq!(
        recovered.latest_checkpoint_version(),
        Some(CHECKPOINT_VERSION)
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert!(!recovered_rows.is_empty());

    let records_per_second = total_records as f64 / ingest_elapsed.as_secs_f64();
    println!("local_incremental_records={total_records}");
    println!("local_incremental_batches={BATCH_COUNT}");
    println!("ingest_throughput_records_per_sec={records_per_second:.2}");
    println!("checkpoint_latency_ms={:.3}", millis(checkpoint_elapsed));
    println!("recovery_latency_ms={:.3}", millis(recovery_elapsed));
    println!(
        "materialized_view_freshness_max_ms={:.3}",
        millis(max_view_freshness)
    );
    println!("recovered_materialized_rows={}", recovered_rows.len());

    Ok(())
}

fn temp_store() -> BenchResult<(TempDir, Arc<dyn ObjectStore>)> {
    let temp_dir = tempfile::tempdir()?;
    let store = LocalFileSystem::new_with_prefix(temp_dir.path())?;

    Ok((temp_dir, Arc::new(store)))
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
    ingest_log: &IngestLog,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
) -> BenchResult<()> {
    let batch = ingest_record_batch(input)?;
    let catalog = orders_sum_count_relation_catalog()?;
    let bytes = IngestEnvelope::encode_batches(
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        catalog.schema_fingerprint.as_str(),
        STREAM_ID,
        PARTITION_ID,
        start_offset_inclusive,
        end_offset_exclusive,
        &[batch],
    )?;

    ingest_log.append_validated_envelope(bytes).await?;
    Ok(())
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

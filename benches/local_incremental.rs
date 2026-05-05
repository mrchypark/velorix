use std::{
    error::Error,
    fmt,
    ops::Range,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, GetOptions, GetRange, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{IncrementalEngine, PrototypeIncrementalEngine},
};
use velorix_runtime::benchmark_gate::{
    BenchmarkBackend, BenchmarkGateLevel, BenchmarkGateResultV1, BenchmarkMetricsV1,
    BenchmarkWorkloadMetricsV1, ObjectRequestMetricsV1,
};
use velorix_runtime::recovery::{
    orders_sum_count_relation_catalog, RecoveredRuntime, ORDERS_SUM_COUNT_OWNER,
    ORDERS_SUM_COUNT_RELATION_ID, ORDERS_SUM_COUNT_RELATION_VERSION,
};
use velorix_storage::{
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
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
    let (_temp_dir, metered_store, store) = temp_store()?;
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let mut engine = PrototypeIncrementalEngine::new();

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
        append_ingest_envelope(&ingest_log, start_offset, end_offset, &input).await?;
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
        &ingest_log,
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
    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await?;
    let recovery_elapsed = recovery_started.elapsed();
    let recovery_requests = request_delta(&metered_store.snapshot(), &recovery_requests_before);
    let recovered_rows = recovered.materialized_state().net_rows()?;

    assert_eq!(
        recovered.latest_checkpoint_version(),
        Some(CHECKPOINT_VERSION)
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert!(!recovered_rows.is_empty());

    let records_per_second = total_records as f64 / ingest_elapsed.as_secs_f64();
    let object_requests = metered_store.snapshot();
    let result = BenchmarkGateResultV1 {
        schema_version: 1,
        commit: git_commit(),
        gate_level: BenchmarkGateLevel::PrSmoke,
        backend: BenchmarkBackend::Local,
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
            scan_bytes: 0,
        },
        workload_metrics: vec![
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
        ],
    };
    result.validate()?;

    println!("{}", serde_json::to_string(&result)?);

    Ok(())
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
        IngestEnvelopeEncodeRequest {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: STREAM_ID.to_string(),
            partition_id: PARTITION_ID,
            start_offset_inclusive,
            end_offset_exclusive,
        },
        &[batch],
    )?;

    ingest_log.append_validated_envelope(bytes).await?;
    Ok(())
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

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        let bytes = self.inner.get_range(location, range).await?;
        self.range_read_count.fetch_add(1, Ordering::SeqCst);
        self.bytes_read
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        Ok(bytes)
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

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
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

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
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

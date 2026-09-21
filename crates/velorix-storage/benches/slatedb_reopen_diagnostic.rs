//! SlateDbStateStore component diagnostic only, not a production performance gate.
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{local::LocalFileSystem, path::Path, *};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use slatedb::config::Settings;
use std::{
    fmt,
    ops::Range,
    process::Command,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use velorix_storage::{
    state::StateObjectWrite,
    state_store::{SlateDbStateStore, StateObjectStore},
};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct Counts {
    put: u64,
    get: u64,
    list: u64,
    range: u64,
    bytes_written: u64,
    bytes_read: u64,
}
impl Counts {
    fn delta(self, before: Self) -> Self {
        Self {
            put: self.put - before.put,
            get: self.get - before.get,
            list: self.list - before.list,
            range: self.range - before.range,
            bytes_written: self.bytes_written - before.bytes_written,
            bytes_read: self.bytes_read - before.bytes_read,
        }
    }
}
#[derive(Debug)]
struct Meter {
    inner: Arc<dyn ObjectStore>,
    counts: Arc<Mutex<Counts>>,
}
impl Meter {
    fn snapshot(&self) -> Counts {
        *self.counts.lock().unwrap()
    }
}
impl fmt::Display for Meter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "diagnostic({})", self.inner)
    }
}
#[derive(Debug)]
struct Upload {
    inner: Box<dyn MultipartUpload>,
    counts: Arc<Mutex<Counts>>,
}
#[async_trait::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.counts.lock().unwrap().bytes_written += data.content_length() as u64;
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}
#[async_trait::async_trait]
impl ObjectStore for Meter {
    async fn put_opts(
        &self,
        p: &Path,
        data: PutPayload,
        o: PutOptions,
    ) -> object_store::Result<PutResult> {
        {
            let mut c = self.counts.lock().unwrap();
            c.put += 1;
            c.bytes_written += data.content_length() as u64;
        }
        self.inner.put_opts(p, data, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.counts.lock().unwrap().put += 1;
        Ok(Box::new(Upload {
            inner: self.inner.put_multipart_opts(p, o).await?,
            counts: self.counts.clone(),
        }))
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        let r = self.inner.get_opts(p, o.clone()).await?;
        if !o.head {
            let mut c = self.counts.lock().unwrap();
            if o.range.is_some() {
                c.range += 1;
            } else {
                c.get += 1;
            }
            c.bytes_read += r.range.end - r.range.start;
        }
        Ok(r)
    }
    async fn get_ranges(
        &self,
        p: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let r = self.inner.get_ranges(p, ranges).await?;
        let mut c = self.counts.lock().unwrap();
        c.range += ranges.len() as u64;
        c.bytes_read += r.iter().map(|b| b.len() as u64).sum::<u64>();
        Ok(r)
    }
    fn delete_stream(
        &self,
        p: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(p)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counts.lock().unwrap().list += 1;
        self.inner.list(p)
    }
    fn list_with_offset(
        &self,
        p: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counts.lock().unwrap().list += 1;
        self.inner.list_with_offset(p, offset)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.counts.lock().unwrap().list += 1;
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, o: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, o).await
    }
}

fn settings(controlled: bool) -> Settings {
    if controlled {
        Settings {
            manifest_poll_interval: Duration::from_secs(3600),
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        }
    } else {
        Settings::default()
    }
}

fn phase(meter: &Meter, before: &mut Counts, phases: &mut Vec<Value>, name: &str) {
    let after = meter.snapshot();
    phases.push(json!({"phase": name, "object_requests": after.delta(*before)}));
    *before = after;
}

async fn sample(controlled: bool, payload: &[u8]) -> Result<Value, Error> {
    let dir = tempfile::tempdir()?;
    let meter = Arc::new(Meter {
        inner: Arc::new(LocalFileSystem::new_with_prefix(dir.path())?),
        counts: Arc::new(Mutex::new(Counts::default())),
    });
    let started = Instant::now();
    let mut before = Counts::default();
    let mut phases = Vec::new();
    let state = StateObjectWrite::new(
        "diagnostic",
        0,
        1,
        "slatedb-state-reopen",
        Bytes::copy_from_slice(payload),
    )?;
    let db = open(controlled, meter.clone()).await?;
    phase(&meter, &mut before, &mut phases, "open");
    let state_ref = db.write_state_object(&state).await?;
    phase(&meter, &mut before, &mut phases, "write_state_object");
    db.close().await?;
    phase(&meter, &mut before, &mut phases, "close");
    let reopened = open(controlled, meter.clone()).await?;
    phase(&meter, &mut before, &mut phases, "reopen");
    let readback = reopened.read_state_object(&state_ref).await?;
    let matches = readback.as_ref() == payload;
    phase(&meter, &mut before, &mut phases, "readback");
    reopened.close().await?;
    phase(&meter, &mut before, &mut phases, "reopened_close");
    if !matches {
        return Err("readback did not match fixture".into());
    }
    Ok(
        json!({"elapsed_ms": started.elapsed().as_secs_f64()*1000.0, "object_requests": meter.snapshot(), "readback_verified": true, "phases": phases}),
    )
}

async fn open(controlled: bool, store: Arc<dyn ObjectStore>) -> Result<SlateDbStateStore, Error> {
    if controlled {
        Ok(SlateDbStateStore::open_with_settings_for_diagnostics(
            "component",
            store,
            settings(true),
        )
        .await?)
    } else {
        Ok(SlateDbStateStore::open("component", store).await?)
    }
}

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn summary(samples: &[Value]) -> Value {
    let mut ranges = serde_json::Map::new();
    let mut stable = samples.len() > 1;
    for key in ["put", "get", "list", "range", "bytes_written", "bytes_read"] {
        let values: Vec<u64> = samples
            .iter()
            .filter_map(|s| s["object_requests"][key].as_u64())
            .collect();
        let min = values.iter().min().copied();
        let max = values.iter().max().copied();
        stable &= min == max && values.len() == samples.len();
        ranges.insert(key.into(), json!({"min": min, "max": max}));
    }
    let elapsed: Vec<f64> = samples
        .iter()
        .filter_map(|s| s["elapsed_ms"].as_f64())
        .collect();
    let elapsed_min = elapsed.iter().copied().reduce(f64::min);
    let elapsed_max = elapsed.iter().copied().reduce(f64::max);
    json!({"object_request_ranges": ranges, "elapsed_ms": {"min": elapsed_min, "max": elapsed_max}, "observed_request_counts_and_bytes_stable": stable})
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let count: usize = std::env::var("VELORIX_DIAGNOSTIC_SAMPLES")
        .unwrap_or_else(|_| "5".into())
        .parse()?;
    if !(1..=30).contains(&count) {
        return Err("VELORIX_DIAGNOSTIC_SAMPLES must be 1..30".into());
    }
    let payload = br#"{"state":"slatedb-reopen-smoke","version":1}"#.to_vec();
    let commit = git_output(&["rev-parse", "HEAD"]);
    let dirty = git_output(&["status", "--porcelain"]).map(|s| !s.is_empty());
    let mut default_samples = Vec::new();
    let mut controlled_samples = Vec::new();
    let mut failed = false;
    for index in 0..count {
        let order = if index % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for (position, controlled) in order.into_iter().enumerate() {
            let result =
                tokio::time::timeout(Duration::from_secs(30), sample(controlled, &payload)).await;
            let value = match result {
                Ok(Ok(mut value)) => {
                    value["sample"] = json!(index + 1);
                    value["position_in_pair"] = json!(position + 1);
                    value
                }
                other => {
                    failed = true;
                    json!({"sample": index+1, "position_in_pair": position + 1, "error": format!("{other:?}")})
                }
            };
            if controlled {
                controlled_samples.push(value);
            } else {
                default_samples.push(value);
            }
            // A timed-out database may retain background workers: never contaminate later samples.
            if failed {
                break;
            }
        }
        if failed {
            break;
        }
    }
    let modes: Vec<Value> = [(false, default_samples), (true, controlled_samples)].into_iter().map(|(controlled,samples)| {
        let s = settings(controlled);
        json!({"mode": if controlled {"maintenance_limited"} else {"default"}, "settings": {
            "flush_interval_ms": s.flush_interval.map(|d| d.as_millis()), "manifest_poll_interval_ms": s.manifest_poll_interval.as_millis(),
            "compactor_enabled": s.compactor_options.is_some(), "garbage_collector_enabled": s.garbage_collector_options.is_some()},
            "summary": summary(&samples), "samples": samples})
    }).collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"schema_version": 1, "diagnostic_only": true,
        "gate_evidence": false, "comparable_to_pr_smoke_baseline": false, "component": "Velorix SlateDbStateStore transaction and marker path", "samples_per_mode_requested": count,
        "git_commit": commit, "git_dirty": dirty, "pair_order": "odd: default first; even: maintenance_limited first",
        "sample_timeout_seconds": 30, "failed": failed, "fixture_payload_bytes": payload.len(),
        "fixture_payload_sha256": format!("{:x}", Sha256::digest(&payload)),
        "measurement_notes": "Local object-store API calls, not cloud requests. PUT attempts and submitted bytes; successful GET/range returned bytes; LIST invocations. HEAD/delete/copy/RSS unmeasured. Phase boundaries may include asynchronous work. Both modes use write_state_object transaction and marker creation with default durable commit and periodic flush. Maintenance-limited mode disables compaction and GC and delays manifest polling; it does not remove all background work. No authoritative capability preflight or full gate workload. Stability is observational, not a performance gate. No retries.", "modes": modes}))?
    );
    if failed {
        return Err("diagnostic sample failed; see JSON".into());
    }
    Ok(())
}

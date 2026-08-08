use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};
use velorix_runtime::benchmark_gate::{BenchmarkWorkloadMetricsV1, ObjectRequestMetricsV1};
use velorix_storage::object_key::ObjectKey;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub async fn run_materialized_output_workloads(
    store: Arc<dyn ObjectStore>,
    snapshot: impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<Vec<BenchmarkWorkloadMetricsV1>> {
    let mut metrics = Vec::new();
    metrics.push(segment_pruning(Arc::clone(&store), &snapshot, checkpoint_version).await?);
    metrics.push(recent_k(Arc::clone(&store), &snapshot, checkpoint_version).await?);
    metrics.push(compaction_equivalence(Arc::clone(&store), &snapshot, checkpoint_version).await?);
    metrics.push(compaction_debt(Arc::clone(&store), &snapshot, checkpoint_version).await?);
    metrics.push(delete_vector(Arc::clone(&store), &snapshot, checkpoint_version).await?);
    metrics.push(ttl_vector(Arc::clone(&store), &snapshot, checkpoint_version).await?);
    metrics.push(late_materialization(store, &snapshot, checkpoint_version).await?);
    Ok(metrics)
}

struct Page {
    key: ObjectKey,
    content_hash: String,
    min_key: &'static str,
    max_key: &'static str,
    max_sort: i64,
    bytes_len: u64,
}

struct BenchPageInput<'a> {
    checkpoint_version: u64,
    view_id: &'a str,
    page_index: u32,
    min_key: &'static str,
    max_key: &'static str,
    max_sort: i64,
    bytes: Bytes,
}

async fn segment_pruning(
    store: Arc<dyn ObjectStore>,
    snapshot: &impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<BenchmarkWorkloadMetricsV1> {
    let pages = write_pages(
        &store,
        checkpoint_version,
        "segment-pruning",
        &[
            (
                "account-001",
                "account-002",
                0,
                b"account-001=10\naccount-002=20\n".as_slice(),
            ),
            (
                "account-100",
                "account-100",
                0,
                b"account-100=7\n".as_slice(),
            ),
        ],
    )
    .await?;

    let full_scan = read_rows(&store, pages.iter()).await?;
    let expected = full_scan
        .iter()
        .filter(|row| row.starts_with("account-100="))
        .cloned()
        .collect::<Vec<_>>();
    let selected_pages = pages
        .iter()
        .filter(|page| page.min_key <= "account-100" && "account-100" <= page.max_key)
        .collect::<Vec<_>>();
    let selected_scan_bytes = selected_pages.iter().map(|page| page.bytes_len).sum();

    let before = snapshot();
    let started = Instant::now();
    let indexed = read_rows(&store, selected_pages.iter().copied()).await?;
    let cache_cold_indexed = read_rows(&store, selected_pages.iter().copied()).await?;
    let elapsed = started.elapsed();

    assert_eq!(indexed, expected);
    assert_eq!(cache_cold_indexed, expected);

    Ok(workload_metric(
        "materialized_output_segment_pruning",
        elapsed,
        request_delta(&snapshot(), &before),
        selected_scan_bytes,
    ))
}

async fn recent_k(
    store: Arc<dyn ObjectStore>,
    snapshot: &impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<BenchmarkWorkloadMetricsV1> {
    let pages = write_pages(
        &store,
        checkpoint_version,
        "recent-k",
        &[
            (
                "events-001",
                "events-002",
                10,
                b"event_id=001,ts=010,user=ada\n\
                  event_id=002,ts=009,user=grace\n"
                    .as_slice(),
            ),
            (
                "events-003",
                "events-004",
                30,
                b"event_id=003,ts=030,user=linus\n\
                  event_id=004,ts=022,user=ken\n"
                    .as_slice(),
            ),
            (
                "events-005",
                "events-006",
                50,
                b"event_id=005,ts=050,user=margaret\n\
                  event_id=006,ts=045,user=barbara\n"
                    .as_slice(),
            ),
        ],
    )
    .await?;

    let full_recent = recent_rows(read_rows(&store, pages.iter()).await?, 3);

    let before = snapshot();
    let started = Instant::now();
    let (indexed_recent, selected_scan_bytes) = indexed_recent_k(&store, &pages, 3).await?;
    let (cache_cold_recent, _) = indexed_recent_k(&store, &pages, 3).await?;
    let elapsed = started.elapsed();

    assert_eq!(indexed_recent, full_recent);
    assert_eq!(cache_cold_recent, full_recent);

    Ok(workload_metric(
        "materialized_output_recent_k",
        elapsed,
        request_delta(&snapshot(), &before),
        selected_scan_bytes,
    ))
}

async fn indexed_recent_k(
    store: &Arc<dyn ObjectStore>,
    pages: &[Page],
    k: usize,
) -> BenchResult<(Vec<String>, u64)> {
    let mut ordered = pages.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|page| std::cmp::Reverse(page.max_sort));

    let mut rows = Vec::new();
    let mut selected_scan_bytes = 0;
    for (index, page) in ordered.iter().enumerate() {
        rows.extend(read_rows(store, std::iter::once(*page)).await?);
        selected_scan_bytes += page.bytes_len;
        let current = recent_rows(rows.clone(), k);
        if current.len() == k {
            let kth = row_event_time(&current[k - 1])?;
            let next_max = ordered.get(index + 1).map(|next| next.max_sort);
            if next_max.is_none_or(|max_sort| max_sort <= kth) {
                return Ok((current, selected_scan_bytes));
            }
        }
    }
    Ok((recent_rows(rows, k), selected_scan_bytes))
}

async fn compaction_equivalence(
    store: Arc<dyn ObjectStore>,
    snapshot: &impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<BenchmarkWorkloadMetricsV1> {
    let pages = write_pages(
        &store,
        checkpoint_version,
        "compaction-equivalence",
        &[
            (
                "account-001",
                "account-002",
                0,
                b"account-001=10\naccount-002=5\n".as_slice(),
            ),
            (
                "account-002",
                "account-003",
                0,
                b"account-002=-\naccount-003=9\n".as_slice(),
            ),
            (
                "account-001",
                "account-004",
                0,
                b"account-001=11\naccount-004=1\n".as_slice(),
            ),
        ],
    )
    .await?;

    let before = snapshot();
    let started = Instant::now();
    let compacted_rows = compact_rows(read_rows(&store, pages.iter()).await?);
    let compacted_bytes = rows_to_bytes(&compacted_rows);
    let compacted_page = write_page(
        &store,
        BenchPageInput {
            checkpoint_version,
            view_id: "compaction-equivalence-compact",
            page_index: 0,
            min_key: "account-001",
            max_key: "account-004",
            max_sort: 0,
            bytes: compacted_bytes,
        },
    )
    .await?;
    let after_compaction = read_rows(&store, std::iter::once(&compacted_page)).await?;
    let elapsed = started.elapsed();

    assert_eq!(after_compaction, compacted_rows);

    Ok(workload_metric(
        "materialized_output_compaction_equivalence",
        elapsed,
        request_delta(&snapshot(), &before),
        pages.iter().map(|page| page.bytes_len).sum(),
    ))
}

async fn compaction_debt(
    store: Arc<dyn ObjectStore>,
    snapshot: &impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<BenchmarkWorkloadMetricsV1> {
    let specs = (0..8)
        .map(|index| {
            let row = format!("account-{index:03}=1\n");
            let key = Box::leak(format!("account-{index:03}").into_boxed_str());
            (key as &str, key as &str, 0, Bytes::from(row))
        })
        .collect::<Vec<_>>();
    let page_specs = specs
        .iter()
        .map(|(min, max, sort, bytes)| (*min, *max, *sort, bytes.as_ref()))
        .collect::<Vec<_>>();
    let pages = write_pages(&store, checkpoint_version, "compaction-debt", &page_specs).await?;

    let before = snapshot();
    let started = Instant::now();
    let fragmented = read_rows(&store, pages.iter()).await?;
    let compacted_bytes = rows_to_bytes(&fragmented);
    let compacted = write_page(
        &store,
        BenchPageInput {
            checkpoint_version,
            view_id: "compaction-debt-compact",
            page_index: 0,
            min_key: "account-000",
            max_key: "account-007",
            max_sort: 0,
            bytes: compacted_bytes,
        },
    )
    .await?;
    let compacted_rows = read_rows(&store, std::iter::once(&compacted)).await?;
    let elapsed = started.elapsed();

    assert_eq!(compacted_rows, fragmented);
    let fragmented_get_fanout = pages.len();
    let compacted_get_fanout = 1;
    assert!(fragmented_get_fanout > compacted_get_fanout);

    Ok(workload_metric(
        "materialized_output_compaction_debt",
        elapsed,
        request_delta(&snapshot(), &before),
        pages.iter().map(|page| page.bytes_len).sum(),
    ))
}

async fn delete_vector(
    store: Arc<dyn ObjectStore>,
    snapshot: &impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<BenchmarkWorkloadMetricsV1> {
    let pages = write_pages(
        &store,
        checkpoint_version,
        "delete-vector",
        &[
            (
                "account-001",
                "account-002",
                0,
                b"account-001=10\naccount-002=20\n".as_slice(),
            ),
            (
                "account-050",
                "account-050",
                0,
                b"account-050=deleted-padding-padding\n".as_slice(),
            ),
            (
                "account-100",
                "account-100",
                0,
                b"account-100=7\n".as_slice(),
            ),
        ],
    )
    .await?;
    let delete_vector = write_page(
        &store,
        BenchPageInput {
            checkpoint_version,
            view_id: "delete-vector",
            page_index: 3,
            min_key: "account-002",
            max_key: "account-050",
            max_sort: 0,
            bytes: Bytes::from_static(b"account-002\naccount-050\n"),
        },
    )
    .await?;
    let deleted = read_rows(&store, std::iter::once(&delete_vector)).await?;
    let expected = apply_delete_vector(read_rows(&store, pages.iter()).await?, &deleted);
    let selected_pages = pages
        .iter()
        .filter(|page| {
            !deleted
                .iter()
                .any(|key| key == page.min_key && key == page.max_key)
        })
        .collect::<Vec<_>>();
    let selected_scan_bytes = delete_vector.bytes_len
        + selected_pages
            .iter()
            .map(|page| page.bytes_len)
            .sum::<u64>();

    let before = snapshot();
    let started = Instant::now();
    let deleted = read_rows(&store, std::iter::once(&delete_vector)).await?;
    let optimized = apply_delete_vector(
        read_rows(&store, selected_pages.iter().copied()).await?,
        &deleted,
    );
    let cache_cold_optimized = apply_delete_vector(
        read_rows(&store, selected_pages.iter().copied()).await?,
        &deleted,
    );
    let elapsed = started.elapsed();

    assert_eq!(optimized, expected);
    assert_eq!(cache_cold_optimized, expected);
    assert!(!optimized.iter().any(|row| row.starts_with("account-002=")));
    assert!(!optimized.iter().any(|row| row.starts_with("account-050=")));

    Ok(workload_metric(
        "materialized_output_delete_vector",
        elapsed,
        request_delta(&snapshot(), &before),
        selected_scan_bytes,
    ))
}

async fn ttl_vector(
    store: Arc<dyn ObjectStore>,
    snapshot: &impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<BenchmarkWorkloadMetricsV1> {
    let pages = write_pages(
        &store,
        checkpoint_version,
        "ttl-vector",
        &[
            (
                "account-001",
                "account-001",
                0,
                b"account-001=10@100\n".as_slice(),
            ),
            (
                "account-050",
                "account-050",
                0,
                b"account-050=expired-padding-padding@10\n".as_slice(),
            ),
            (
                "account-100",
                "account-101",
                0,
                b"account-100=7@100\naccount-101=8@10\n".as_slice(),
            ),
        ],
    )
    .await?;
    let ttl_vector = write_page(
        &store,
        BenchPageInput {
            checkpoint_version,
            view_id: "ttl-vector",
            page_index: 3,
            min_key: "account-050",
            max_key: "account-101",
            max_sort: 0,
            bytes: Bytes::from_static(b"account-050@50\naccount-101@50\n"),
        },
    )
    .await?;
    let expired = read_rows(&store, std::iter::once(&ttl_vector)).await?;
    let expected = apply_ttl_vector(read_rows(&store, pages.iter()).await?, &expired, 50);
    let selected_pages = pages
        .iter()
        .filter(|page| {
            !expired
                .iter()
                .any(|entry| entry.starts_with(page.min_key) && page.min_key == page.max_key)
        })
        .collect::<Vec<_>>();
    let selected_scan_bytes = ttl_vector.bytes_len
        + selected_pages
            .iter()
            .map(|page| page.bytes_len)
            .sum::<u64>();

    let before = snapshot();
    let started = Instant::now();
    let expired = read_rows(&store, std::iter::once(&ttl_vector)).await?;
    let optimized = apply_ttl_vector(
        read_rows(&store, selected_pages.iter().copied()).await?,
        &expired,
        50,
    );
    let cache_cold_optimized = apply_ttl_vector(
        read_rows(&store, selected_pages.iter().copied()).await?,
        &expired,
        50,
    );
    let elapsed = started.elapsed();

    assert_eq!(optimized, expected);
    assert_eq!(cache_cold_optimized, expected);
    assert!(optimized.iter().any(|row| row.starts_with("account-001=")));
    assert!(!optimized.iter().any(|row| row.starts_with("account-050=")));
    assert!(!optimized.iter().any(|row| row.starts_with("account-101=")));

    Ok(workload_metric(
        "materialized_output_ttl_vector",
        elapsed,
        request_delta(&snapshot(), &before),
        selected_scan_bytes,
    ))
}

async fn late_materialization(
    store: Arc<dyn ObjectStore>,
    snapshot: &impl Fn() -> ObjectRequestMetricsV1,
    checkpoint_version: u64,
) -> BenchResult<BenchmarkWorkloadMetricsV1> {
    let index = write_page(
        &store,
        BenchPageInput {
            checkpoint_version,
            view_id: "late-materialization-index",
            page_index: 0,
            min_key: "account-001",
            max_key: "account-003",
            max_sort: 0,
            bytes: Bytes::from_static(b"account-001|cold\naccount-002|hot\naccount-003|cold\n"),
        },
    )
    .await?;
    let payloads = write_pages(
        &store,
        checkpoint_version,
        "late-materialization-payloads",
        &[
            (
                "account-001",
                "account-001",
                0,
                b"account-001=rejected-payload-padding-padding-padding-padding-padding-padding\n"
                    .as_slice(),
            ),
            (
                "account-002",
                "account-002",
                0,
                b"account-002=ok\n".as_slice(),
            ),
            (
                "account-003",
                "account-003",
                0,
                b"account-003=rejected-payload-padding-padding-padding-padding-padding-padding\n"
                    .as_slice(),
            ),
        ],
    )
    .await?;

    let full = read_rows(&store, payloads.iter()).await?;
    let expected = full
        .into_iter()
        .filter(|row| row.starts_with("account-002="))
        .collect::<Vec<_>>();
    let eager_payload_bytes = payloads.iter().map(|page| page.bytes_len).sum::<u64>();

    let before = snapshot();
    let started = Instant::now();
    let index_rows = read_rows(&store, std::iter::once(&index)).await?;
    let selected_key = index_rows
        .iter()
        .find_map(|row| {
            row.ends_with("|hot")
                .then(|| row.split('|').next().unwrap())
        })
        .ok_or("late materialization index did not select a hot row")?;
    let selected_payloads = payloads
        .iter()
        .filter(|page| page.min_key <= selected_key && selected_key <= page.max_key)
        .collect::<Vec<_>>();
    let selected_scan_bytes = index.bytes_len
        + selected_payloads
            .iter()
            .map(|page| page.bytes_len)
            .sum::<u64>();
    let late = read_rows(&store, selected_payloads.iter().copied()).await?;
    let elapsed = started.elapsed();

    assert_eq!(late, expected);
    let measured = request_delta(&snapshot(), &before);
    assert!(measured.bytes_read < eager_payload_bytes);

    Ok(workload_metric(
        "materialized_output_late_materialization",
        elapsed,
        measured,
        selected_scan_bytes,
    ))
}

async fn write_pages(
    store: &Arc<dyn ObjectStore>,
    checkpoint_version: u64,
    view_id: &str,
    specs: &[(&'static str, &'static str, i64, &[u8])],
) -> BenchResult<Vec<Page>> {
    let mut pages = Vec::new();
    for (index, (min_key, max_key, max_sort, bytes)) in specs.iter().enumerate() {
        pages.push(
            write_page(
                store,
                BenchPageInput {
                    checkpoint_version,
                    view_id,
                    page_index: index as u32,
                    min_key,
                    max_key,
                    max_sort: *max_sort,
                    bytes: Bytes::copy_from_slice(bytes),
                },
            )
            .await?,
        );
    }
    Ok(pages)
}

async fn write_page(store: &Arc<dyn ObjectStore>, input: BenchPageInput<'_>) -> BenchResult<Page> {
    let BenchPageInput {
        checkpoint_version,
        view_id,
        page_index,
        min_key,
        max_key,
        max_sort,
        bytes,
    } = input;

    let content_hash = sha256_digest(&bytes);
    // Benchmarks run against temp local stores or S3 run-prefix stores. Reusing
    // the runtime output key shape exercises the product path without adding a
    // benchmark-only namespace that would hide key-layout regressions.
    let key = ObjectKey::standing_runtime_output_page(
        "tenant-a",
        "benchmark",
        view_id,
        checkpoint_version,
        page_index,
        &content_hash,
    )?;
    let bytes_len = bytes.len() as u64;
    store.put(&Path::from(key.as_str()), bytes.into()).await?;
    Ok(Page {
        key,
        content_hash,
        min_key,
        max_key,
        max_sort,
        bytes_len,
    })
}

async fn read_rows<'a>(
    store: &Arc<dyn ObjectStore>,
    pages: impl Iterator<Item = &'a Page>,
) -> BenchResult<Vec<String>> {
    let mut rows = Vec::new();
    for page in pages {
        let bytes = store
            .get(&Path::from(page.key.as_str()))
            .await?
            .bytes()
            .await?;
        assert_eq!(sha256_digest(&bytes), page.content_hash);
        rows.extend(std::str::from_utf8(&bytes)?.lines().map(str::to_string));
    }
    rows.sort();
    Ok(rows)
}

fn recent_rows(mut rows: Vec<String>, k: usize) -> Vec<String> {
    rows.sort_by(|left, right| {
        row_event_time(right)
            .unwrap_or(i64::MIN)
            .cmp(&row_event_time(left).unwrap_or(i64::MIN))
            .then_with(|| left.cmp(right))
    });
    rows.truncate(k);
    rows
}

fn row_event_time(row: &str) -> BenchResult<i64> {
    let timestamp = row
        .split(',')
        .find_map(|field| field.trim().strip_prefix("ts="))
        .ok_or("row missing event timestamp")?;
    Ok(timestamp.parse::<i64>()?)
}

fn compact_rows(rows: Vec<String>) -> Vec<String> {
    let mut by_key = BTreeMap::new();
    for row in rows {
        let Some((key, value)) = row.split_once('=') else {
            continue;
        };
        if value == "-" {
            by_key.remove(key);
        } else {
            by_key.insert(key.to_string(), value.to_string());
        }
    }
    by_key
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}

fn apply_delete_vector(rows: Vec<String>, deleted: &[String]) -> Vec<String> {
    let mut visible = rows
        .into_iter()
        .filter(|row| {
            row.split_once('=')
                .is_some_and(|(key, _)| !deleted.iter().any(|deleted| deleted == key))
        })
        .collect::<Vec<_>>();
    visible.sort();
    visible
}

fn apply_ttl_vector(rows: Vec<String>, expired: &[String], cutoff: i64) -> Vec<String> {
    let mut visible = rows
        .into_iter()
        .filter_map(|row| {
            let (key, rest) = row.split_once('=')?;
            let (value, timestamp) = rest.split_once('@')?;
            let timestamp = timestamp.parse::<i64>().ok()?;
            let expired_key = expired.iter().any(|expired| {
                expired
                    .split_once('@')
                    .is_some_and(|(expired_key, expiry)| {
                        expired_key == key && expiry.parse::<i64>().ok() <= Some(cutoff)
                    })
            });
            (!expired_key && timestamp >= cutoff).then(|| format!("{key}={value}"))
        })
        .collect::<Vec<_>>();
    visible.sort();
    visible
}

fn rows_to_bytes(rows: &[String]) -> Bytes {
    Bytes::from(format!("{}\n", rows.join("\n")))
}

fn workload_metric(
    name: &str,
    sample: Duration,
    object_requests: ObjectRequestMetricsV1,
    scan_bytes: u64,
) -> BenchmarkWorkloadMetricsV1 {
    BenchmarkWorkloadMetricsV1 {
        name: name.to_string(),
        p50_ms: millis(sample),
        p95_ms: millis(sample),
        object_requests: Some(object_requests),
        scan_bytes,
    }
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

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

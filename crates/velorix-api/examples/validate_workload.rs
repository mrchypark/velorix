//! Run: cargo run -p velorix-api --example validate_workload -- workload.json output-dir
//!
//! The output directory must not exist. Input file paths are relative to the manifest.
//! Manifest: {"steps": [...]}, optionally "experimental": true and
//! "continue_on_error": true. Each step has an "action":
//! Optional request_limits: {"max_body_bytes": N, "max_ingest_rows": N};
//! runtime_limits: {"max_output_delta_records": N, "max_state_payload_bytes": N}.
//! Limits must be positive and are applied unchanged on restart.
//! runtime_limits may also supply max_expansion_bytes and max_snapshot_bytes
//! together; omitting both retains API defaults independently of checkpoint size.
//! max_expansion_rows/max_checkpoint_bytes alias the two original limit names.
//! standing_runtime_resident_limit defaults to 2; this is a soft count, not an RSS bound.
//! object_store_dir is rejected: LocalFileSystem does not support update CAS.
//! Optional object_store_s3: {"prefix":"isolated/run-prefix"} uses path-style S3
//! with ETagMatch conditional puts. Required env: VELORIX_VALIDATION_S3_ENDPOINT,
//! VELORIX_VALIDATION_S3_REGION, VELORIX_VALIDATION_S3_BUCKET, AWS_ACCESS_KEY_ID,
//! AWS_SECRET_ACCESS_KEY; optional AWS_SESSION_TOKEN. No credentials in JSON.
//! Omit both store options for memory. A bounded create/update/stale-CAS/readback
//! preflight runs before any workload step. Optional VELORIX_META_GRPC_ENDPOINT
//! and VELORIX_META_BEARER_TOKEN select GrpcMetaStore; otherwise metadata is in memory.
//! resume=true permits a nonempty S3 prefix only with external metadata. It does
//! not skip steps: supply a remaining-steps manifest to avoid duplicate ingestion.
//! Use the same isolated metadata service and S3 prefix for recovery. The restart
//! action remains in-process; cross-process recovery requires separate evidence.
//! Preflight failures suppress backend details.
//! VELORIX_VALIDATION_CACHE_MEMORY_MIB and VELORIX_VALIDATION_CACHE_DISK_MIB
//! override cache capacities (defaults: 256 MiB memory, 1024 MiB disk).
//! step_timeout_seconds defaults to 60; request/restart timeout_seconds overrides
//! it. Every timeout must be in 1..=3600 seconds, including the full page loop.
//! - "request": name, path, optional method (POST), body or body_file, rows_file,
//!   accept (application/json), expected_file, expected_pointer (JSON Pointer into
//!   the response, e.g. /rows), unordered (false), expected_status (200).
//!   collect_pages (false) follows JSON continuations for POST view queries with
//!   no sql or starting page_token. max_pages defaults to 4096 and
//!   max_collected_rows to 1000000; both must be positive. Each response is
//!   bounded to 64 MiB. Output NNNN.pages/ retains pages, headers and schema
//!   snapshots; NNNN.json contains all rows (duplicates retained), and
//!   NNNN.collection.json is written only after complete verified collection.
//! - "restart": drops the router, reconstructs admission and restores runtimes
//!   against the same object and metadata stores. This is an in-process restart,
//!   not a disk durability or process-crash test.
//!   Restart waits 31 seconds for the previous owner's 30-second lease to expire.
//! - "replay": step (zero-based index of an earlier request).
//!
//! Schema registration, view admission, backfill, ingestion and queries use their
//! normal HTTP paths and unmodified API JSON bodies. rows_file supplies a JSON
//! array as body.rows. Multiple request steps supply multiple ingest batches.
//! For Arrow use accept=application/vnd.apache.arrow.stream; expected_file then
//! contains an Arrow stream. JSON expectations compare the selected value;
//! unordered sorts arrays without deduplicating. Arrow expectations compare
//! decoded schemas and batches (including batch boundaries).
//! Without collect_pages, query pages remain explicit requests. JSON query
//! responses omit types, so collection checks the published output schema before
//! and after paging, row field names, and stable body/header epochs and tokens.
//! Value/type/tolerance checks belong to expected_file or an external comparator.

#[path = "validate_workload/evidence.rs"]
mod evidence;

use std::{collections::BTreeSet, fs, io::Cursor, path::Path, sync::Arc, time::Duration};

use anyhow::{bail, ensure, Context, Result};
use arrow::ipc::reader::StreamReader;
use axum::{
    body::Body,
    http::{HeaderMap, Request},
    Router,
};
use futures::StreamExt;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    memory::InMemory,
    path::Path as ObjectPath,
    prefix::PrefixStore,
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tower::ServiceExt;
use velorix_api::{app, ApiState};
use velorix_control::{
    meta_admin::{GrpcMetaStore, InMemoryMetaStore, MetaStore},
    operator_authority::{validate_operator_authority, ObjectStoreAuthorityRef},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    /// Explicit target and transitive predecessor identities; no implicit DAG discovery.
    #[serde(default)]
    evidence_targets: Vec<EvidenceTarget>,
    steps: Vec<Step>,
    #[serde(default)]
    experimental: bool,
    #[serde(default)]
    continue_on_error: bool,
    request_limits: Option<RequestLimits>,
    runtime_limits: Option<RuntimeLimits>,
    /// Soft resident runtime count; validation defaults to two to exercise reload.
    standing_runtime_resident_limit: Option<usize>,
    object_store_dir: Option<String>,
    object_store_s3: Option<S3Config>,
    #[serde(default)]
    resume: bool,
    #[serde(default = "default_timeout_seconds")]
    step_timeout_seconds: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceTarget {
    tenant_id: String,
    program_id: String,
    view_id: String,
}

async fn checkpoint_evidence(meta: &dyn MetaStore, targets: &[EvidenceTarget]) -> Result<Value> {
    let mut result = Vec::new();
    for target in targets {
        let pointer = meta
            .read_standing_runtime_checkpoint(
                &target.tenant_id,
                &target.program_id,
                &target.view_id,
            )
            .await
            .map_err(|_| anyhow::anyhow!("checkpoint evidence read failed; details suppressed"))?;
        result.push(
            json!({"tenant_id":target.tenant_id,"program_id":target.program_id,
            "view_id":target.view_id,"pointer":pointer}),
        );
    }
    Ok(Value::Array(result))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct S3Config {
    prefix: String,
}

async fn workload_store(workload: &Workload) -> Result<Arc<dyn ObjectStore>> {
    ensure!(workload.object_store_dir.is_none(), "object_store_dir is unsupported: LocalFileSystem lacks conditional PutMode::Update; use object_store_s3 or memory");
    let Some(config) = &workload.object_store_s3 else {
        ensure!(!workload.resume, "resume requires S3 and external metadata");
        return Ok(Arc::new(InMemory::new()));
    };
    ensure!(
        !config.prefix.is_empty()
            && config
                .prefix
                .split('/')
                .all(|p| !p.is_empty() && p != "." && p != ".."),
        "object_store_s3.prefix must be an explicit isolated nonempty relative prefix"
    );
    let env = |name: &str| -> Result<String> {
        let value = std::env::var(name).with_context(|| format!("{name} is required"))?;
        ensure!(!value.trim().is_empty(), "{name} must be nonempty");
        Ok(value)
    };
    let endpoint = env("VELORIX_VALIDATION_S3_ENDPOINT")?;
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_region(env("VELORIX_VALIDATION_S3_REGION")?)
        .with_bucket_name(env("VELORIX_VALIDATION_S3_BUCKET")?)
        .with_access_key_id(env("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(env("AWS_SECRET_ACCESS_KEY")?)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_allow_http(endpoint.starts_with("http://"));
    if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
        builder = builder.with_token(token);
    }
    let store = builder
        .build()
        .map_err(|_| anyhow::anyhow!("S3 configuration failed (details suppressed)"))?;
    let store = Arc::new(PrefixStore::new(
        store,
        ObjectPath::from(config.prefix.as_str()),
    ));
    if !workload.resume {
        let first = store.list(None).next().await;
        match first {
            None => {},
            Some(Ok(_)) => bail!("S3 prefix is nonempty: explicit resume=true and matching external metadata required"),
            Some(Err(_)) => bail!("S3 prefix inspection failed (details suppressed)"),
        }
    }
    Ok(store)
}

fn validate_bearer_endpoint(endpoint: &str) -> Result<()> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| anyhow::anyhow!("invalid metadata endpoint URL"))?;
    let loopback = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "metadata endpoint must not contain credentials"
    );
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && loopback),
        "metadata bearer token requires HTTPS or loopback HTTP"
    );
    Ok(())
}

async fn workload_meta(workload: &Workload) -> Result<Arc<dyn MetaStore>> {
    let endpoint = std::env::var("VELORIX_META_GRPC_ENDPOINT").ok();
    let token = std::env::var("VELORIX_META_BEARER_TOKEN").ok();
    let Some(endpoint) = endpoint else {
        ensure!(
            token.is_none(),
            "VELORIX_META_BEARER_TOKEN requires VELORIX_META_GRPC_ENDPOINT"
        );
        ensure!(
            !workload.resume,
            "resume requires matching external metadata; InMemory cannot recover a prior process"
        );
        return Ok(Arc::new(InMemoryMetaStore::default()));
    };
    ensure!(
        !endpoint.trim().is_empty(),
        "VELORIX_META_GRPC_ENDPOINT must be nonempty"
    );
    let store = if let Some(token) = token {
        validate_bearer_endpoint(&endpoint)?;
        ensure!(
            !token.trim().is_empty(),
            "VELORIX_META_BEARER_TOKEN must be nonempty when set"
        );
        GrpcMetaStore::connect_with_bearer_token(endpoint, token).await
    } else {
        GrpcMetaStore::connect(endpoint).await
    }
    .map_err(|_| anyhow::anyhow!("metadata connection failed (details suppressed)"))?;
    Ok(Arc::new(store))
}

async fn preflight_conditional_updates(store: &dyn ObjectStore) -> Result<()> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = ObjectPath::from(format!(
        "_validation_preflight/{}-{nonce}",
        std::process::id()
    ));
    let first = store
        .put_opts(
            &path,
            b"first".to_vec().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await?;
    let version = UpdateVersion {
        e_tag: first.e_tag,
        version: first.version,
    };
    let preflight = async {
        store
            .put_opts(
                &path,
                b"second".to_vec().into(),
                PutOptions {
                    mode: PutMode::Update(version.clone()),
                    ..Default::default()
                },
            )
            .await?;
        let stale = store
            .put_opts(
                &path,
                b"stale".to_vec().into(),
                PutOptions {
                    mode: PutMode::Update(version),
                    ..Default::default()
                },
            )
            .await;
        ensure!(
            matches!(stale, Err(object_store::Error::Precondition { .. })),
            "store did not reject stale conditional update"
        );
        ensure!(
            store.get(&path).await?.bytes().await?.as_ref() == b"second",
            "conditional update readback mismatch"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let cleanup = store.delete(&path).await;
    preflight?;
    cleanup?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestLimits {
    max_body_bytes: usize,
    max_ingest_rows: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeLimits {
    #[serde(alias = "max_expansion_rows")]
    max_output_delta_records: usize,
    #[serde(alias = "max_checkpoint_bytes")]
    max_state_payload_bytes: usize,
    max_expansion_bytes: Option<usize>,
    max_snapshot_bytes: Option<usize>,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Step {
    Request(Box<HttpStep>),
    Restart { timeout_seconds: Option<u64> },
    Replay { step: usize },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpStep {
    name: String,
    path: String,
    method: Option<String>,
    body: Option<Value>,
    body_file: Option<String>,
    rows_file: Option<String>,
    accept: Option<String>,
    expected_file: Option<String>,
    #[serde(default)]
    expected_pointer: String,
    #[serde(default)]
    unordered: bool,
    expected_status: Option<u16>,
    timeout_seconds: Option<u64>,
    #[serde(default)]
    collect_pages: bool,
    #[serde(default = "default_max_pages")]
    max_pages: usize,
    #[serde(default = "default_max_rows")]
    max_collected_rows: usize,
}

fn default_timeout_seconds() -> u64 {
    60
}
fn default_max_pages() -> usize {
    4096
}
fn default_max_rows() -> usize {
    1_000_000
}

fn step_timeout(workload: &Workload, index: usize, step: &Step) -> Result<Duration> {
    let timeout = match step {
        Step::Request(request) => request.timeout_seconds,
        Step::Restart { timeout_seconds } => *timeout_seconds,
        Step::Replay { step } => {
            ensure!(*step < index, "replay must reference an earlier request");
            match &workload.steps[*step] {
                Step::Request(request) => request.timeout_seconds,
                _ => bail!("replay target must be a request"),
            }
        }
    }
    .unwrap_or(workload.step_timeout_seconds);
    ensure!(
        (1..=3600).contains(&timeout),
        "step timeout must be 1..=3600 seconds"
    );
    Ok(Duration::from_secs(timeout))
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path).with_context(|| path.display().to_string())?)
        .with_context(|| format!("invalid JSON: {}", path.display()))
}

fn compare_json(mut actual: Value, mut expected: Value, unordered: bool) -> Result<()> {
    if unordered {
        for value in [&mut actual, &mut expected] {
            value
                .as_array_mut()
                .context("unordered comparison requires arrays")?
                .sort_by_key(Value::to_string);
        }
    }
    ensure!(
        actual == expected,
        "JSON expectation mismatch; inspect saved response"
    );
    Ok(())
}

fn cache_capacity_bytes(name: &str, value: Option<&str>, default_mib: usize) -> Result<usize> {
    let mib = value
        .map(str::parse::<usize>)
        .transpose()
        .with_context(|| format!("{name} must be a positive integer in MiB"))?
        .unwrap_or(default_mib);
    let bytes = mib
        .checked_mul(1024 * 1024)
        .with_context(|| format!("{name} exceeds addressable memory"))?;
    ensure!(
        bytes > 0 && bytes <= isize::MAX as usize,
        "{name} must be positive and fit addressable memory"
    );
    Ok(bytes)
}

async fn router(
    store: Arc<dyn ObjectStore>,
    meta: Arc<dyn MetaStore>,
    workload: &Workload,
    cache_dir: &Path,
) -> Result<Router> {
    let external_endpoint = std::env::var("VELORIX_META_GRPC_ENDPOINT").ok();
    let authoritative = workload.object_store_s3.is_some() && external_endpoint.is_some();
    let ingest_owner = if authoritative {
        let owner = std::env::var("VELORIX_RELATION_INGEST_OWNER_ID")
            .context("authoritative S3 ingest requires VELORIX_RELATION_INGEST_OWNER_ID")?;
        ensure!(
            !owner.trim().is_empty(),
            "VELORIX_RELATION_INGEST_OWNER_ID must be nonempty"
        );
        let capability = meta.read_meta_store_capabilities().await?.relation_ingest;
        ensure!(capability.relation_scoped_authority && capability.committed_publication_source_cut && capability.durable_across_restart,
            "authoritative ingest requires durable metadata relation authority and committed publication source cuts");
        Some(owner)
    } else {
        None
    };
    let authority = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: "workload-validation".into(),
            namespace: "workload".into(),
        },
        store,
        if workload.object_store_s3.is_some() {
            "s3"
        } else {
            "memory"
        },
        "workload-probe",
    )
    .await?;
    let cache = velorix_api::materialized_read_cache::MaterializedReadCache::open(
        velorix_api::materialized_read_cache::MaterializedReadCacheConfig {
            directory: cache_dir.to_owned(),
            memory_bytes: cache_capacity_bytes(
                "VELORIX_VALIDATION_CACHE_MEMORY_MIB",
                std::env::var("VELORIX_VALIDATION_CACHE_MEMORY_MIB")
                    .ok()
                    .as_deref(),
                256,
            )?,
            disk_bytes: cache_capacity_bytes(
                "VELORIX_VALIDATION_CACHE_DISK_MIB",
                std::env::var("VELORIX_VALIDATION_CACHE_DISK_MIB")
                    .ok()
                    .as_deref(),
                1024,
            )?,
        },
    )
    .await?;
    let mut state = ApiState::from_validated_authority_with_ingest_admission_startup(
        authority,
        "unused",
        "workload-driver",
        !authoritative,
    )
    .await?
    .with_meta_store(meta)
    .with_materialized_read_cache(Arc::new(cache))
    .with_experimental_advanced_view_features(workload.experimental);
    if let Some(endpoint) = external_endpoint {
        state = state.with_meta_store_endpoint(endpoint);
    }
    if let Some(owner) = ingest_owner {
        state = state.with_authoritative_relation_ingest(
            owner,
            "workload",
            "workload-validation",
            "s3",
            "v1/api-probes/workload-driver",
            30_000,
        )?;
        eprintln!("relation ingest mode: authoritative");
    } else {
        eprintln!("relation ingest mode: legacy (no source-fencing claim)");
    }
    if let Some(limits) = &workload.request_limits {
        ensure!(
            limits.max_body_bytes > 0 && limits.max_ingest_rows > 0,
            "request limits must be positive"
        );
        state = state.with_request_limits(limits.max_body_bytes, limits.max_ingest_rows);
    }
    let resident_limit = workload.standing_runtime_resident_limit.unwrap_or(2);
    ensure!(
        resident_limit > 0,
        "standing_runtime_resident_limit must be positive"
    );
    state = state.with_standing_runtime_resident_limit(resident_limit);
    if let Some(limits) = &workload.runtime_limits {
        ensure!(
            limits.max_output_delta_records > 0 && limits.max_state_payload_bytes > 0,
            "runtime limits must be positive"
        );
        state = state.with_standing_runtime_budget_limits(
            limits.max_output_delta_records,
            limits.max_state_payload_bytes,
        );
        match (limits.max_expansion_bytes, limits.max_snapshot_bytes) {
            (Some(expansion), Some(snapshot)) => {
                state = state.with_standing_runtime_work_byte_limits(expansion, snapshot)?;
            }
            (None, None) => {}
            _ => bail!("supply both max_expansion_bytes and max_snapshot_bytes, or neither"),
        }
    }
    let restored = state
        .restore_standing_program_runtimes_from_active_views()
        .await?;
    eprintln!("restored {restored} runtimes");
    Ok(app(state))
}

async fn dispatch(router: &Router, step: &HttpStep, base: &Path, output: &Path) -> Result<()> {
    ensure!(
        step.body.is_none() || step.body_file.is_none(),
        "body and body_file are exclusive"
    );
    let mut body = match &step.body_file {
        Some(path) => read_json(&base.join(path))?,
        None => step.body.clone().unwrap_or_else(|| json!({})),
    };
    if let Some(path) = &step.rows_file {
        let rows = read_json(&base.join(path))?;
        ensure!(rows.is_array(), "rows_file must contain a JSON array");
        let object = body
            .as_object_mut()
            .context("rows_file requires an object body")?;
        ensure!(
            !object.contains_key("rows"),
            "rows_file must not replace body.rows"
        );
        object.insert("rows".into(), rows);
    }
    if step.collect_pages {
        return collect_pages(router, step, body, base, output).await;
    }
    let request = Request::builder()
        .method(step.method.as_deref().unwrap_or("POST"))
        .uri(&step.path)
        .header("content-type", "application/json")
        .header(
            "accept",
            step.accept.as_deref().unwrap_or("application/json"),
        )
        .body(Body::from(serde_json::to_vec(&body)?))?;
    eprintln!("{}: {} {}", step.name, request.method(), request.uri());
    let response = router.clone().oneshot(request).await?;
    let status = response.status();
    let headers = response.headers().clone();
    let arrow = headers.get("content-type").is_some_and(|value| {
        value
            .as_bytes()
            .starts_with(b"application/vnd.apache.arrow.stream")
    });
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    fs::write(
        output.with_extension(if arrow { "arrow" } else { "json" }),
        &bytes,
    )?;
    let headers = headers
        .iter()
        .map(|(key, value)| {
            (
                key.to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect::<Vec<_>>();
    fs::write(
        output.with_extension("response.json"),
        serde_json::to_vec_pretty(&json!({
            "status": status.as_u16(), "headers": headers,
        }))?,
    )?;
    if status.as_u16() != step.expected_status.unwrap_or(200) {
        bail!(
            "{}: HTTP {status}: {}",
            step.name,
            String::from_utf8_lossy(&bytes)
        );
    }
    if let Some(path) = &step.expected_file {
        if arrow {
            ensure!(
                step.expected_pointer.is_empty() && !step.unordered,
                "Arrow comparisons do not support expected_pointer or unordered"
            );
            let actual = StreamReader::try_new(Cursor::new(bytes), None)?;
            let expected = StreamReader::try_new(fs::File::open(base.join(path))?, None)?;
            ensure!(
                actual.schema().fields() == expected.schema().fields(),
                "Arrow schema mismatch"
            );
            ensure!(
                actual.collect::<std::result::Result<Vec<_>, _>>()?
                    == expected.collect::<std::result::Result<Vec<_>, _>>()?,
                "Arrow batch mismatch"
            );
        } else {
            let actual: Value = serde_json::from_slice(&bytes)?;
            let actual = actual
                .pointer(&step.expected_pointer)
                .context("expected_pointer missing from response")?
                .clone();
            compare_json(actual, read_json(&base.join(path))?, step.unordered)?;
        }
    }
    Ok(())
}

async fn saved_json_request(
    router: &Router,
    method: &str,
    path: &str,
    body: &Value,
    output: &Path,
    expected_status: u16,
) -> Result<(Value, HeaderMap)> {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .body(Body::from(serde_json::to_vec(body)?))?;
    let response = router.clone().oneshot(request).await?;
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024).await?;
    fs::write(output.with_extension("json"), &bytes)?;
    let saved_headers = headers
        .iter()
        .map(|(key, value)| {
            (
                key.to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect::<Vec<_>>();
    fs::write(
        output.with_extension("response.json"),
        serde_json::to_vec_pretty(&json!({
            "status": status.as_u16(), "headers": saved_headers,
        }))?,
    )?;
    ensure!(
        status.as_u16() == expected_status,
        "{path}: HTTP {status}; inspect saved response"
    );
    Ok((
        serde_json::from_slice(&bytes).context("expected JSON response")?,
        headers,
    ))
}

fn page_token(page: &Value, headers: &HeaderMap) -> Result<Option<String>> {
    let token = match page.get("next_page_token") {
        None | Some(Value::Null) => None,
        Some(Value::String(token)) if !token.is_empty() => Some(token.clone()),
        _ => bail!("invalid next_page_token"),
    };
    let header = headers
        .get("x-velorix-next-page-token")
        .map(|v| v.to_str())
        .transpose()?;
    ensure!(
        token.as_deref() == header,
        "body/header continuation token mismatch"
    );
    Ok(token)
}

fn append_page(
    page: &Value,
    headers: &HeaderMap,
    epoch: &mut Option<u64>,
    fields: &BTreeSet<String>,
    rows: &mut Vec<Value>,
    seen: &mut BTreeSet<String>,
    max_rows: usize,
) -> Result<Option<String>> {
    let current = page
        .get("logical_epoch")
        .and_then(Value::as_u64)
        .context("page requires integer logical_epoch")?;
    let header_epoch = headers
        .get("x-velorix-logical-epoch")
        .context("missing epoch header")?
        .to_str()?
        .parse::<u64>()?;
    ensure!(current == header_epoch, "body/header epoch mismatch");
    ensure!(
        epoch.is_none_or(|previous| previous == current),
        "epoch changed across pages"
    );
    *epoch = Some(current);
    let batch = page
        .get("rows")
        .and_then(Value::as_array)
        .context("page rows must be an array")?;
    ensure!(
        batch.len() <= max_rows.saturating_sub(rows.len()),
        "collection row limit exceeded"
    );
    for row in batch {
        let object = row.as_object().context("query row must be an object")?;
        ensure!(
            object.keys().cloned().collect::<BTreeSet<_>>() == *fields,
            "query row schema changed"
        );
    }
    let token = page_token(page, headers)?;
    if let Some(token) = &token {
        ensure!(seen.insert(token.clone()), "repeated continuation token");
    }
    rows.extend(batch.iter().cloned());
    Ok(token)
}

async fn collect_pages(
    router: &Router,
    step: &HttpStep,
    mut body: Value,
    base: &Path,
    output: &Path,
) -> Result<()> {
    ensure!(
        step.method.as_deref().unwrap_or("POST") == "POST",
        "collect_pages requires POST"
    );
    ensure!(
        step.accept.as_deref().unwrap_or("application/json") == "application/json",
        "collect_pages requires JSON"
    );
    ensure!(
        step.expected_status.unwrap_or(200) == 200,
        "collect_pages requires successful queries"
    );
    ensure!(
        step.max_pages > 0 && step.max_collected_rows > 0,
        "collection limits must be positive"
    );
    let object = body
        .as_object_mut()
        .context("query body must be an object")?;
    ensure!(
        !object.contains_key("sql"),
        "collect_pages collects an entire output, without query SQL"
    );
    ensure!(
        object.get("page_token").is_none_or(Value::is_null),
        "collection must start at the first page"
    );
    let query = step
        .path
        .strip_suffix("/query")
        .context("expected view query path")?;
    let (view_path, output_id) = query
        .split_once("/outputs/")
        .map_or((query, None), |(view, output)| (view, Some(output)));
    ensure!(
        view_path.starts_with("/v1/views/"),
        "expected view query path"
    );
    let pages = output.with_extension("pages");
    fs::create_dir(&pages)?;
    let (before, _) = saved_json_request(
        router,
        "GET",
        view_path,
        &json!({}),
        &pages.join("schema-before"),
        200,
    )
    .await?;
    let schemas = before
        .get("output_relations")
        .and_then(Value::as_array)
        .context("view output schemas missing")?;
    ensure!(
        before["spec_hash"]
            .as_str()
            .is_some_and(|hash| !hash.is_empty()),
        "view spec hash missing"
    );
    let schema = if let Some(id) = output_id {
        schemas
            .iter()
            .find(|schema| schema["relation_id"].as_str() == Some(id))
            .context("output schema missing")?
    } else {
        ensure!(
            schemas.len() == 1,
            "collection requires an explicit output for multi-output views"
        );
        &schemas[0]
    };
    let fields = schema["columns"]
        .as_array()
        .context("schema columns missing")?
        .iter()
        .map(|column| {
            column["name"]
                .as_str()
                .map(str::to_owned)
                .context("column name missing")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let mut rows = Vec::new();
    let mut epoch = None;
    let mut seen = BTreeSet::new();
    let mut records = Vec::new();
    let mut complete = false;
    for index in 0..step.max_pages {
        let request_token = body.get("page_token").cloned().unwrap_or(Value::Null);
        let stem = format!("{index:04}");
        let (page, headers) =
            saved_json_request(router, "POST", &step.path, &body, &pages.join(&stem), 200).await?;
        let token = append_page(
            &page,
            &headers,
            &mut epoch,
            &fields,
            &mut rows,
            &mut seen,
            step.max_collected_rows,
        )?;
        records.push(json!({"file": format!("{stem}.json"), "request_page_token": request_token}));
        fs::write(
            pages.join("index.json"),
            serde_json::to_vec_pretty(&records)?,
        )?;
        match token {
            Some(token) => {
                body["page_token"] = Value::String(token);
                if let Some(object) = body.as_object_mut() {
                    object.remove("materialization");
                }
            }
            None => {
                complete = true;
                break;
            }
        }
    }
    ensure!(
        complete,
        "collection page limit exceeded before terminal page"
    );
    let (after, _) = saved_json_request(
        router,
        "GET",
        view_path,
        &json!({}),
        &pages.join("schema-after"),
        200,
    )
    .await?;
    ensure!(
        before["spec_hash"] == after["spec_hash"]
            && before["output_relations"] == after["output_relations"],
        "view schema changed during collection"
    );
    let aggregate = json!({"rows": rows, "logical_epoch": epoch, "next_page_token": null});
    fs::write(
        output.with_extension("json"),
        serde_json::to_vec(&aggregate)?,
    )?;
    if let Some(path) = &step.expected_file {
        compare_json(
            aggregate
                .pointer(&step.expected_pointer)
                .context("expected_pointer missing from collected response")?
                .clone(),
            read_json(&base.join(path))?,
            step.unordered,
        )?;
    }
    fs::write(
        output.with_extension("collection.json"),
        serde_json::to_vec_pretty(&json!({
            "complete": true, "page_count": records.len(), "row_count": aggregate["rows"].as_array().unwrap().len(),
            "logical_epoch": epoch, "schema": schema,
        }))?,
    )?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    ensure!(
        args.len() == 2,
        "usage: validate_workload workload.json output-dir"
    );
    let manifest = Path::new(&args[0]);
    let base = manifest.parent().unwrap_or_else(|| Path::new("."));
    let workload: Workload = serde_json::from_value(read_json(manifest)?)?;
    for (index, step) in workload.steps.iter().enumerate() {
        step_timeout(&workload, index, step)?;
    }
    let output = Path::new(&args[1]);
    fs::create_dir(output).context("output directory must be new")?;
    let meta = tokio::time::timeout(Duration::from_secs(60), workload_meta(&workload))
        .await
        .context("metadata connection timed out")??;
    let store = tokio::time::timeout(Duration::from_secs(60), workload_store(&workload))
        .await
        .context("object-store setup timed out")??;
    let observations = Arc::new(evidence::Evidence::default());
    let store: Arc<dyn ObjectStore> = Arc::new(evidence::ObservedStore {
        inner: store,
        evidence: observations.clone(),
    });
    tokio::time::timeout(Duration::from_secs(60), preflight_conditional_updates(store.as_ref()))
        .await.context("object-store conditional-update preflight timed out before any workload step")?
        .map_err(|_| anyhow::anyhow!("object-store conditional-update preflight failed before any workload step (details suppressed)"))?;
    fs::write(
        output.join("store-preflight.json"),
        "{\"conditional_update\":\"passed\"}\n",
    )?;
    let cache_dir = output.join("cache");
    let mut app = Some(router(store.clone(), meta.clone(), &workload, &cache_dir).await?);
    let mut report = Vec::new();
    let mut failed = false;
    for (index, step) in workload.steps.iter().enumerate() {
        let timeout = step_timeout(&workload, index, step)?;
        let started = std::time::Instant::now();
        let reads_before = observations.snapshot();
        let checkpoints_before =
            checkpoint_evidence(meta.as_ref(), &workload.evidence_targets).await?;
        let result = tokio::time::timeout(timeout, async {
            let request = match step {
                Step::Restart { .. } => {
                    drop(app.take());
                    eprintln!("waiting for the previous runtime owner's lease to expire");
                    tokio::time::sleep(Duration::from_secs(31)).await;
                    app = Some(router(store.clone(), meta.clone(), &workload, &cache_dir).await?);
                    return Ok(());
                }
                Step::Request(request) => request,
                Step::Replay { step } => {
                    ensure!(*step < index, "replay must reference an earlier request");
                    match &workload.steps[*step] {
                        Step::Request(request) => request,
                        _ => bail!("replay target must be a request"),
                    }
                }
            };
            dispatch(
                app.as_ref()
                    .context("router unavailable after failed restart")?,
                request,
                base,
                &output.join(format!("{index:04}")),
            )
            .await
        })
        .await
        .with_context(|| format!("step exceeded {} seconds", timeout.as_secs()))
        .and_then(|result| result);
        let error = result.err().map(|error| format!("{error:#}"));
        let reads_after = observations.snapshot();
        let checkpoints_after =
            checkpoint_evidence(meta.as_ref(), &workload.evidence_targets).await?;
        let response = fs::read(output.join(format!("{index:04}.json")))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        fs::write(
            output.join(format!("{index:04}.evidence.json")),
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "step": index,
                "completed_without_error": error.is_none(),
                "scope": "process-wide store counters during this step; concurrent background I/O may contribute",
                "reads_before": reads_before,
                "reads_after": reads_after,
                "response_logical_epoch": response.as_ref().and_then(|v| v.get("logical_epoch")),
                "response_materialization": response.as_ref().and_then(|v| v.get("materialization")),
                "checkpoints_before": checkpoints_before,
                "checkpoints_after": checkpoints_after,
                "checkpoint_scope": "explicit evidence_targets only; caller must include transitive predecessor DAG",
                "engine_recomputation": "UNVERIFIED: completed queries do not expose processing counters",
                "wire_http_retry_counts": "UNMEASURED"
            }))?,
        )?;
        eprintln!("step {index}: {}", error.as_deref().unwrap_or("ok"));
        failed |= error.is_some();
        report.push(json!({"step": index, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0, "error": error}));
        fs::write(
            output.join("report.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        if failed && !workload.continue_on_error {
            break;
        }
    }
    ensure!(
        !failed,
        "workload failed; inspect {}",
        output.join("report.json").display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn cache_capacities_default_override_and_reject_invalid_sizes() {
        for default in [256, 1024] {
            assert_eq!(
                super::cache_capacity_bytes("cache", None, default).unwrap(),
                default * 1024 * 1024
            );
            assert_eq!(
                super::cache_capacity_bytes("cache", Some("8"), default).unwrap(),
                8 * 1024 * 1024
            );
        }
        for invalid in [
            "",
            "0",
            "-1",
            "1.5",
            "invalid",
            &usize::MAX.to_string(),
            &(isize::MAX as usize / (1024 * 1024) + 1).to_string(),
        ] {
            assert!(
                super::cache_capacity_bytes("cache", Some(invalid), 256).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn bearer_endpoint_transport_policy() {
        for endpoint in [
            "https://meta.example:443",
            "http://127.0.0.1:49152",
            "http://[::1]:49152",
            "http://localhost:49152",
        ] {
            assert!(super::validate_bearer_endpoint(endpoint).is_ok());
        }
        for endpoint in [
            "http://meta.example",
            "http://192.168.1.2",
            "http://localhost.example",
            "ftp://127.0.0.1",
            "https://user:password@meta.example",
            "invalid",
        ] {
            assert!(super::validate_bearer_endpoint(endpoint).is_err());
        }
    }
    use super::*;

    #[test]
    fn unordered_comparison_retains_multiplicity() {
        assert!(compare_json(json!([2, 1, 1]), json!([1, 2, 1]), true).is_ok());
        assert!(compare_json(json!([1, 1, 2]), json!([1, 2]), true).is_err());
    }

    fn headers(epoch: u64, token: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velorix-logical-epoch",
            epoch.to_string().parse().unwrap(),
        );
        if let Some(token) = token {
            headers.insert("x-velorix-next-page-token", token.parse().unwrap());
        }
        headers
    }

    #[test]
    fn collection_preserves_duplicates_and_rejects_token_cycles() {
        let fields = BTreeSet::from(["value".into()]);
        let mut rows = Vec::new();
        let mut epoch = None;
        let mut seen = BTreeSet::new();
        let page = json!({"rows": [{"value": null}, {"value": null}],
            "logical_epoch": 7, "next_page_token": "next"});
        assert_eq!(
            append_page(
                &page,
                &headers(7, Some("next")),
                &mut epoch,
                &fields,
                &mut rows,
                &mut seen,
                10
            )
            .unwrap(),
            Some("next".into())
        );
        assert_eq!(rows.len(), 2);
        assert!(append_page(
            &page,
            &headers(7, Some("next")),
            &mut epoch,
            &fields,
            &mut rows,
            &mut seen,
            10
        )
        .is_err());
        let final_page = json!({"rows": [{"value": 1}], "logical_epoch": 7});
        assert_eq!(
            append_page(
                &final_page,
                &headers(7, None),
                &mut epoch,
                &fields,
                &mut rows,
                &mut seen,
                10
            )
            .unwrap(),
            None
        );
        assert_eq!(
            rows,
            vec![
                json!({"value": null}),
                json!({"value": null}),
                json!({"value": 1})
            ]
        );
    }

    #[test]
    fn collection_rejects_epoch_schema_header_and_budget_changes() {
        for (page, headers, max_rows) in [
            (
                json!({"rows": [], "logical_epoch": 8}),
                headers(8, None),
                10,
            ),
            (
                json!({"rows": [{"other": 1}], "logical_epoch": 7}),
                headers(7, None),
                10,
            ),
            (
                json!({"rows": [], "logical_epoch": 7}),
                headers(8, None),
                10,
            ),
            (
                json!({"rows": [], "logical_epoch": 7, "next_page_token": "a"}),
                headers(7, Some("b")),
                10,
            ),
            (
                json!({"rows": [{"value": 1}], "logical_epoch": 7}),
                headers(7, None),
                0,
            ),
        ] {
            assert!(append_page(
                &page,
                &headers,
                &mut Some(7),
                &BTreeSet::from(["value".into()]),
                &mut Vec::new(),
                &mut BTreeSet::new(),
                max_rows
            )
            .is_err());
        }
    }

    #[test]
    fn timeouts_are_bounded_and_replay_inherits_request_timeout() {
        let workload: Workload = serde_json::from_value(json!({"steps": [
            {"action": "request", "name": "slow", "path": "/example", "timeout_seconds": 600},
            {"action": "replay", "step": 0}, {"action": "restart"}
        ]}))
        .unwrap();
        assert_eq!(
            step_timeout(&workload, 0, &workload.steps[0])
                .unwrap()
                .as_secs(),
            600
        );
        assert_eq!(
            step_timeout(&workload, 1, &workload.steps[1])
                .unwrap()
                .as_secs(),
            600
        );
        assert_eq!(
            step_timeout(&workload, 2, &workload.steps[2])
                .unwrap()
                .as_secs(),
            60
        );
        for timeout in [0, 3601] {
            assert!(step_timeout(
                &workload,
                0,
                &Step::Restart {
                    timeout_seconds: Some(timeout)
                }
            )
            .is_err());
        }
        assert!(step_timeout(&workload, 0, &Step::Replay { step: 0 }).is_err());
    }

    #[tokio::test]
    async fn collection_saves_pages_and_only_marks_a_complete_scan() {
        use axum::{
            routing::{get, post},
            Json,
        };
        let router = Router::new()
            .route(
                "/v1/views/example",
                get(|| async {
                    Json(json!({"spec_hash": "stable", "output_relations": [{
                        "relation_id": "example", "columns": [{"name": "value"}]
                    }]}))
                }),
            )
            .route(
                "/v1/views/example/query",
                post(|Json(body): Json<Value>| async move {
                    let continuation = body.get("page_token").is_some();
                    assert_eq!(
                        body.get("materialization").is_some(),
                        !continuation,
                        "materialization is sent only on the initial page"
                    );
                    let next = if continuation { None } else { Some("next") };
                    (
                        headers(7, next),
                        Json(json!({"rows": [{"value": 1}],
                    "logical_epoch": 7, "next_page_token": next})),
                    )
                }),
            );
        let temp = tempfile::tempdir().unwrap();
        let mut step: HttpStep = serde_json::from_value(json!({
            "name": "pages", "path": "/v1/views/example/query", "collect_pages": true,
            "body": {"materialization": {"mode": "demand"}}
        }))
        .unwrap();
        let output = temp.path().join("complete");
        dispatch(&router, &step, temp.path(), &output)
            .await
            .unwrap();
        assert_eq!(
            read_json(&output.with_extension("json")).unwrap()["rows"],
            json!([{"value": 1}, {"value": 1}])
        );
        assert!(output
            .with_extension("pages")
            .join("0001.response.json")
            .exists());
        assert!(read_json(&output.with_extension("collection.json")).unwrap()["complete"] == true);
        step.max_pages = 1;
        let output = temp.path().join("truncated");
        assert!(dispatch(&router, &step, temp.path(), &output)
            .await
            .is_err());
        assert!(!output.with_extension("collection.json").exists());
    }
}

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
    prefix::PrefixStore,
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use serde_json::{json, Value};
use velorix_storage::capability::{
    probe_authoritative_object_store_capabilities, AuthoritativeNamespace,
};
use velorix_storage::{
    gc::GarbageCollectionPolicy,
    manifest::{CheckpointManifest, InputRange},
    state::{CheckpointPublisher, StateObjectWrite},
};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult = Result<(), TestError>;

#[tokio::test]
async fn s3_compatible_store_supports_velorix_required_object_semantics() -> TestResult {
    let Some(config) = live_config() else {
        println!("skipping S3 compatibility harness; set VELORIX_S3_COMPAT=1 to enable");
        return Ok(());
    };

    let store = live_store(&config)?;
    let key = Path::from(format!("{}/object.bin", config.run_prefix));
    let prefix = Path::from(config.run_prefix);
    let payload = Bytes::from_static(b"velorix-s3-compatible-harness");

    store
        .put_opts(&key, payload.clone().into(), PutMode::Create.into())
        .await?;

    let validation = validate_written_object(&store, &key, &prefix, payload).await;

    let _ = store.delete(&key).await;
    validation
}

#[tokio::test]
async fn s3_compatible_store_supports_authoritative_namespace_startup_capabilities() -> TestResult {
    let Some(config) = live_config() else {
        println!("skipping S3 compatibility harness; set VELORIX_S3_COMPAT=1 to enable");
        return Ok(());
    };

    let store = live_store(&config)?;
    let capabilities = probe_authoritative_object_store_capabilities(
        &store,
        "s3-compatible",
        format!("{}/authoritative-capabilities", config.run_prefix),
    )
    .await?;

    capabilities.validate_for_startup()?;
    for namespace in AuthoritativeNamespace::all() {
        let profile = capabilities.profiles.get(&namespace).ok_or_else(|| {
            test_error(format!(
                "missing capability profile for authoritative namespace `{namespace}`"
            ))
        })?;
        if profile.backend_name != "s3-compatible" {
            return Err(test_error(format!(
                "authoritative namespace `{namespace}` reported backend `{}`",
                profile.backend_name
            )));
        }
    }

    Ok(())
}

#[tokio::test]
async fn s3_compatible_gc_execution_persists_listed_run_and_retention_evidence() -> TestResult {
    let Some(config) = live_config() else {
        println!("skipping S3-compatible GC execution harness; set VELORIX_S3_COMPAT=1 to enable");
        return Ok(());
    };
    let gc_config = live_gc_config(&config);

    let store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
        live_store(&config)?,
        Path::from(gc_config.run_prefix),
    ));
    let capabilities = probe_authoritative_object_store_capabilities(
        store.as_ref(),
        "s3-compatible-gc",
        "authoritative-gc-capabilities",
    )
    .await?;
    capabilities.validate_for_startup()?;
    let publisher = CheckpointPublisher::new_authoritative(Arc::clone(&store), &capabilities)?;
    let state_0 = StateObjectWrite::new(
        "s3_compatible_gc",
        0,
        0,
        "state-0000",
        Bytes::from_static(b"s3-compatible-gc-state-0"),
    )?;
    let state_ref_0 = publisher.write_state_object(&state_0).await?;
    publisher
        .publish_manifest(&gc_manifest(0, 0, 1, None, vec![state_ref_0]))
        .await?;
    let state_1 = StateObjectWrite::new(
        "s3_compatible_gc",
        0,
        1,
        "state-0001",
        Bytes::from_static(b"s3-compatible-gc-state-1"),
    )?;
    let state_ref_1 = publisher.write_state_object(&state_1).await?;
    publisher
        .publish_manifest(&gc_manifest(1, 0, 2, Some(0), vec![state_ref_1]))
        .await?;

    let policy = GarbageCollectionPolicy {
        retain_latest_manifests: 1,
    };
    let plan = publisher.plan_garbage_collection(policy).await?;
    let run = publisher
        .execute_garbage_collection_plan_with_evidence(&gc_config.run_id, policy, &plan)
        .await?;
    let verified = publisher
        .verify_garbage_collection_run_retention_evidence(&gc_config.run_id)
        .await?;

    if run != verified {
        return Err(test_error("verified GC run differed from executed run"));
    }
    if verified.report.deleted.is_empty() {
        return Err(test_error(
            "S3-compatible GC run did not delete any candidates",
        ));
    }

    Ok(())
}

#[tokio::test]
async fn s3_compatible_checkpoint_fault_matrix_writes_live_scenario_evidence() -> TestResult {
    let Some(config) = live_config() else {
        println!(
            "skipping S3-compatible checkpoint fault matrix; set VELORIX_S3_COMPAT=1 to enable"
        );
        return Ok(());
    };

    let scenario_dir = scenario_dir();
    fs::create_dir_all(&scenario_dir)?;

    let scenarios = [
        object_write_failure_scenario(&config).await?,
        verification_read_failure_scenario(&config).await?,
        manifest_write_failure_scenario(&config).await?,
        delayed_visibility_scenario(&config).await?,
        retry_after_failure_scenario(&config).await?,
    ];

    for scenario in scenarios {
        write_scenario_evidence(&scenario_dir, scenario)?;
    }

    Ok(())
}

async fn validate_written_object(
    store: &dyn ObjectStore,
    key: &Path,
    prefix: &Path,
    payload: Bytes,
) -> TestResult {
    match store
        .put_opts(
            key,
            Bytes::from_static(b"duplicate").into(),
            PutMode::Create.into(),
        )
        .await
    {
        Err(ObjectStoreError::AlreadyExists { .. }) => {}
        Err(error) => {
            return Err(test_error(format!(
                "create-only put returned {error} instead of AlreadyExists"
            )));
        }
        Ok(_) => return Err(test_error("create-only put overwrote an existing key")),
    }

    let read = store.get(key).await?.bytes().await?;
    if read != payload {
        return Err(test_error("written object body did not match"));
    }

    let listed = store.list(Some(prefix)).try_collect::<Vec<_>>().await?;
    if !listed.iter().any(|object| object.location == *key) {
        return Err(test_error("written object was not listable by prefix"));
    }

    let range = store.get_range(key, 8..11).await?;
    if range != Bytes::from_static(b"s3-") {
        return Err(test_error("range read returned unexpected bytes"));
    }

    Ok(())
}

async fn object_write_failure_scenario(config: &LiveConfig) -> Result<Value, TestError> {
    let base = live_scenario_store(config, "object-write-failure")?;
    let publisher = CheckpointPublisher::new(Arc::new(
        FaultInjectingStore::new(Arc::clone(&base)).fail_put_prefix("v1/state/"),
    ));
    let state = fault_state(0, "object-write-failure");

    let error = publisher
        .write_state_object(&state)
        .await
        .expect_err("state write should fail under injected object-write failure");
    assert_no_visible_checkpoint(&base, "object_write_failure").await?;

    Ok(scenario_pass(
        "object_write_failure",
        "state object write failed before manifest publication",
        "put v1/state/ returned a deterministic ObjectStore error",
        error.to_string(),
    ))
}

async fn verification_read_failure_scenario(config: &LiveConfig) -> Result<Value, TestError> {
    let base = live_scenario_store(config, "verification-read-failure")?;
    let writer = CheckpointPublisher::new(Arc::clone(&base));
    let state = fault_state(0, "verification-read-failure");
    let state_ref = writer.write_state_object(&state).await?;
    let manifest = gc_manifest(0, 0, 1, None, vec![state_ref.clone()]);
    let publisher = CheckpointPublisher::new(Arc::new(
        FaultInjectingStore::new(Arc::clone(&base))
            .fail_get_prefix(state_ref.object_key.as_str().to_string()),
    ));

    let error = publisher.publish_manifest(&manifest).await.expect_err(
        "manifest publication should fail when state verification cannot read object metadata",
    );
    assert_no_visible_checkpoint(&base, "verification_read_failure").await?;

    Ok(scenario_pass(
        "verification_read_failure",
        "manifest publication failed closed during referenced state verification",
        "head/get for the referenced state object returned a deterministic ObjectStore error",
        error.to_string(),
    ))
}

async fn manifest_write_failure_scenario(config: &LiveConfig) -> Result<Value, TestError> {
    let base = live_scenario_store(config, "manifest-write-failure")?;
    let writer = CheckpointPublisher::new(Arc::clone(&base));
    let state_ref = writer
        .write_state_object(&fault_state(0, "manifest-write-failure"))
        .await?;
    let manifest = gc_manifest(0, 0, 1, None, vec![state_ref]);
    let publisher = CheckpointPublisher::new(Arc::new(
        FaultInjectingStore::new(Arc::clone(&base)).fail_put_prefix("v1/checkpoints/"),
    ));

    let error = publisher
        .publish_manifest(&manifest)
        .await
        .expect_err("manifest write should fail under injected manifest-write failure");
    assert_no_visible_checkpoint(&base, "manifest_write_failure").await?;

    Ok(scenario_pass(
        "manifest_write_failure",
        "manifest put failure left no visible checkpoint",
        "put v1/checkpoints/ returned a deterministic ObjectStore error",
        error.to_string(),
    ))
}

async fn delayed_visibility_scenario(config: &LiveConfig) -> Result<Value, TestError> {
    let base = live_scenario_store(config, "delayed-visibility")?;
    let publisher = CheckpointPublisher::new(Arc::clone(&base));
    let state_ref = publisher
        .write_state_object(&fault_state(0, "delayed-visibility"))
        .await?;
    publisher
        .publish_manifest(&gc_manifest(0, 0, 1, None, vec![state_ref]))
        .await?;

    let delayed_reader = CheckpointPublisher::new(Arc::new(
        FaultInjectingStore::new(Arc::clone(&base))
            .hide_get_prefix("v1/checkpoint-index/latest-candidate.json")
            .hide_list_prefix("v1/checkpoints"),
    ));
    if delayed_reader.latest_manifest().await?.is_some() {
        return Err(test_error(
            "delayed visibility injection did not hide the checkpoint on the first read",
        ));
    }
    assert_visible_checkpoint_with(&delayed_reader, 0, "delayed_visibility retry").await?;

    Ok(scenario_pass(
        "delayed_visibility",
        "temporary marker and listing invisibility produced no false checkpoint and later recovered",
        "first marker get returned NotFound and first checkpoint listing returned empty",
        "first read returned None; second read returned checkpoint 0",
    ))
}

async fn retry_after_failure_scenario(config: &LiveConfig) -> Result<Value, TestError> {
    let base = live_scenario_store(config, "retry-after-failure")?;
    let failing_publisher = CheckpointPublisher::new(Arc::new(
        FaultInjectingStore::new(Arc::clone(&base)).fail_put_prefix("v1/state/"),
    ));
    let state = fault_state(0, "retry-after-failure");
    let first_error = failing_publisher
        .write_state_object(&state)
        .await
        .expect_err("first state write should fail under transient object-write failure");
    assert_no_visible_checkpoint(&base, "retry_after_failure first attempt").await?;

    let retry_publisher = CheckpointPublisher::new(Arc::clone(&base));
    let state_ref = retry_publisher.write_state_object(&state).await?;
    retry_publisher
        .publish_manifest(&gc_manifest(0, 0, 1, None, vec![state_ref]))
        .await?;
    assert_visible_checkpoint(&base, 0, "retry_after_failure").await?;

    Ok(scenario_pass(
        "retry_after_failure",
        "explicit retry after transient write failure published one valid checkpoint",
        "first put v1/state/ returned a deterministic ObjectStore error",
        first_error.to_string(),
    ))
}

async fn assert_no_visible_checkpoint(store: &Arc<dyn ObjectStore>, scenario: &str) -> TestResult {
    let publisher = CheckpointPublisher::new(Arc::clone(store));
    match publisher.latest_manifest().await? {
        None => Ok(()),
        Some(manifest) => Err(test_error(format!(
            "{scenario}: unexpected visible checkpoint {}",
            manifest.checkpoint_version
        ))),
    }
}

async fn assert_visible_checkpoint(
    store: &Arc<dyn ObjectStore>,
    checkpoint_version: u64,
    scenario: &str,
) -> TestResult {
    let publisher = CheckpointPublisher::new(Arc::clone(store));
    assert_visible_checkpoint_with(&publisher, checkpoint_version, scenario).await
}

async fn assert_visible_checkpoint_with(
    publisher: &CheckpointPublisher,
    checkpoint_version: u64,
    scenario: &str,
) -> TestResult {
    let manifest = publisher.latest_manifest().await?.ok_or_else(|| {
        test_error(format!(
            "{scenario}: expected a visible checkpoint after live S3 write"
        ))
    })?;
    if manifest.checkpoint_version != checkpoint_version {
        return Err(test_error(format!(
            "{scenario}: expected checkpoint {checkpoint_version}, got {}",
            manifest.checkpoint_version
        )));
    }

    Ok(())
}

fn fault_state(checkpoint_version: u64, object_id: &str) -> StateObjectWrite {
    StateObjectWrite::new(
        "s3_checkpoint_fault_matrix",
        0,
        checkpoint_version,
        object_id,
        Bytes::from(format!("{object_id}-state")),
    )
    .expect("fault matrix state key should be valid")
}

fn scenario_pass(
    name: &'static str,
    verified: &'static str,
    fault_injection: &'static str,
    observed: impl Into<String>,
) -> Value {
    json!({
        "name": name,
        "status": "pass",
        "live_s3_compatible": true,
        "backend": "external-s3-compatible",
        "verified": verified,
        "fault_injection": fault_injection,
        "observed": observed.into(),
    })
}

fn write_scenario_evidence(scenario_dir: &std::path::Path, scenario: Value) -> TestResult {
    let name = scenario
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| test_error("scenario evidence missing name"))?;
    let path = scenario_dir.join(format!("{name}.json"));
    fs::write(path, serde_json::to_vec_pretty(&scenario)?)?;

    Ok(())
}

fn live_scenario_store(
    config: &LiveConfig,
    scenario_name: &str,
) -> Result<Arc<dyn ObjectStore>, TestError> {
    let store = live_store(config)?;
    let prefix = Path::from(format!(
        "{}/checkpoint-fault-matrix/{scenario_name}",
        config.run_prefix
    ));

    Ok(Arc::new(PrefixStore::new(store, prefix)))
}

#[derive(Debug)]
struct FaultInjectingStore {
    inner: Arc<dyn ObjectStore>,
    fail_put_prefix: Option<OneShotPrefix>,
    fail_get_prefix: Option<OneShotPrefix>,
    hide_get_prefix: Option<OneShotPrefix>,
    hide_list_prefix: Option<OneShotPrefix>,
}

#[derive(Debug)]
struct OneShotPrefix {
    prefix: String,
    remaining: AtomicUsize,
}

impl FaultInjectingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fail_put_prefix: None,
            fail_get_prefix: None,
            hide_get_prefix: None,
            hide_list_prefix: None,
        }
    }

    fn fail_put_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.fail_put_prefix = Some(OneShotPrefix::new(prefix));
        self
    }

    fn fail_get_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.fail_get_prefix = Some(OneShotPrefix::new(prefix));
        self
    }

    fn hide_get_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.hide_get_prefix = Some(OneShotPrefix::new(prefix));
        self
    }

    fn hide_list_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.hide_list_prefix = Some(OneShotPrefix::new(prefix));
        self
    }
}

impl OneShotPrefix {
    fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            remaining: AtomicUsize::new(1),
        }
    }

    fn consume_if_matches(&self, path: &str) -> bool {
        path.starts_with(&self.prefix)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

impl std::fmt::Display for FaultInjectingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fault-injecting-live-s3({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultInjectingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self
            .fail_put_prefix
            .as_ref()
            .is_some_and(|fault| fault.consume_if_matches(location.as_ref()))
        {
            return Err(generic_store_error(
                "fault-injecting-live-s3",
                format!("injected put failure for {location}"),
            ));
        }

        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self
            .fail_get_prefix
            .as_ref()
            .is_some_and(|fault| fault.consume_if_matches(location.as_ref()))
        {
            return Err(generic_store_error(
                "fault-injecting-live-s3",
                format!("injected get failure for {location}"),
            ));
        }
        if self
            .hide_get_prefix
            .as_ref()
            .is_some_and(|fault| fault.consume_if_matches(location.as_ref()))
        {
            return Err(not_found_error(location, "injected delayed get visibility"));
        }

        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Some(prefix) = prefix {
            if self
                .hide_list_prefix
                .as_ref()
                .is_some_and(|fault| fault.consume_if_matches(prefix.as_ref()))
            {
                return stream::empty().boxed();
            }
        }

        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
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

fn generic_store_error(store: &'static str, message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn not_found_error(location: &Path, message: impl Into<String>) -> object_store::Error {
    object_store::Error::NotFound {
        path: location.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            message.into(),
        )),
    }
}

fn test_error(message: impl Into<String>) -> TestError {
    Box::new(std::io::Error::other(message.into()))
}

fn gc_manifest(
    checkpoint_version: u64,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    parent_checkpoint: Option<u64>,
    state_objects: Vec<velorix_storage::manifest::StateObjectRef>,
) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version,
        input_ranges: vec![InputRange {
            stream_id: "s3-compatible-gc".to_string(),
            partition_id: 0,
            start_offset_inclusive,
            end_offset_exclusive,
        }],
        state_objects,
        output_objects: vec![],
        parent_checkpoint,
        created_at: "2026-05-18T00:00:00Z".to_string(),
        relation_id: None,
        relation_version: None,
        schema_fingerprint: None,
    }
}

fn live_store(config: &LiveConfig) -> object_store::Result<impl ObjectStore> {
    AmazonS3Builder::new()
        .with_endpoint(config.endpoint.clone())
        .with_access_key_id(config.access_key_id.clone())
        .with_secret_access_key(config.secret_access_key.clone())
        .with_region(config.region.clone())
        .with_bucket_name(config.bucket.clone())
        .with_allow_http(config.allow_http)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
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

struct LiveGcConfig {
    run_prefix: String,
    run_id: String,
}

fn scenario_dir() -> PathBuf {
    std::env::var("VELORIX_S3_CHECKPOINT_FAULT_MATRIX_SCENARIO_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from("target/velorix-product/s3-checkpoint-fault-matrix-scenarios")
        })
}

fn live_gc_config(config: &LiveConfig) -> LiveGcConfig {
    live_gc_config_from_lookup(config, |name| std::env::var(name).ok())
}

fn live_gc_config_from_lookup(
    config: &LiveConfig,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> LiveGcConfig {
    let run_prefix = lookup("VELORIX_S3_GC_PREFIX")
        .map(|prefix| prefix.trim().trim_matches('/').to_string())
        .filter(|prefix| !prefix.is_empty())
        .unwrap_or_else(|| config.run_prefix.clone());
    let run_id = lookup("VELORIX_S3_GC_RUN_ID")
        .map(|run_id| run_id.trim().to_string())
        .filter(|run_id| !run_id.is_empty())
        .unwrap_or_else(|| "s3-compatible-gc-run".to_string());

    LiveGcConfig { run_prefix, run_id }
}

#[test]
fn s3_compatible_gc_config_accepts_known_prefix_and_run_id_for_release_evidence() {
    let config = LiveConfig {
        endpoint: "http://127.0.0.1:9000".to_string(),
        access_key_id: "rustfsadmin".to_string(),
        secret_access_key: "rustfsadmin".to_string(),
        region: "us-east-1".to_string(),
        bucket: "velorix-rustfs".to_string(),
        allow_http: true,
        run_prefix: "generated/test-prefix".to_string(),
    };

    let gc_config = live_gc_config_from_lookup(&config, |name| match name {
        "VELORIX_S3_GC_PREFIX" => Some("/rustfs-s3-gate/run-1/production-gc/".to_string()),
        "VELORIX_S3_GC_RUN_ID" => Some("rustfs-production-gc-run-1".to_string()),
        _ => None,
    });

    assert_eq!(gc_config.run_prefix, "rustfs-s3-gate/run-1/production-gc");
    assert_eq!(gc_config.run_id, "rustfs-production-gc-run-1");
}

#[test]
fn s3_compatible_gc_config_defaults_to_isolated_test_prefix() {
    let config = LiveConfig {
        endpoint: "http://127.0.0.1:9000".to_string(),
        access_key_id: "rustfsadmin".to_string(),
        secret_access_key: "rustfsadmin".to_string(),
        region: "us-east-1".to_string(),
        bucket: "velorix-rustfs".to_string(),
        allow_http: true,
        run_prefix: "generated/test-prefix".to_string(),
    };

    let gc_config = live_gc_config_from_lookup(&config, |_| None);

    assert_eq!(gc_config.run_prefix, "generated/test-prefix");
    assert_eq!(gc_config.run_id, "s3-compatible-gc-run");
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

    format!("velorix-s3-compat/{}-{nanos}", std::process::id())
}

fn join_prefixes(base: &str, run: &str) -> String {
    match base.trim_matches('/') {
        "" => run.to_string(),
        base => format!("{base}/{run}"),
    }
}

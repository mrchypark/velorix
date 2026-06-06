use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
    prefix::PrefixStore,
    Error as ObjectStoreError, ObjectStore, PutMode,
};
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

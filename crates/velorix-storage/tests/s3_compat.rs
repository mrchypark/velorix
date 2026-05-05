use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{
    aws::AmazonS3Builder, path::Path, Error as ObjectStoreError, ObjectStore, PutMode,
};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult = Result<(), TestError>;

#[tokio::test]
async fn s3_compatible_store_supports_velorix_required_object_semantics() -> TestResult {
    let Some(config) = live_config() else {
        println!("skipping S3 compatibility harness; set VELORIX_S3_COMPAT=1 to enable");
        return Ok(());
    };

    let store = AmazonS3Builder::new()
        .with_endpoint(config.endpoint)
        .with_access_key_id(config.access_key_id)
        .with_secret_access_key(config.secret_access_key)
        .with_region(config.region)
        .with_bucket_name(config.bucket)
        .with_allow_http(config.allow_http)
        .build()?;
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

    format!("velorix-s3-compat/{}-{nanos}", std::process::id())
}

fn join_prefixes(base: &str, run: &str) -> String {
    match base.trim_matches('/') {
        "" => run.to_string(),
        base => format!("{base}/{run}"),
    }
}

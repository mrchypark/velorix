//! Opt-in contract checks against isolated, provider-compatible Floci endpoints.
//!
//! Run with `--ignored`; every endpoint is mandatory so a missing provider can
//! never turn this into a partial or fake pass.

use futures::TryStreamExt;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    azure::MicrosoftAzureBuilder,
    client::ClientConfigKey,
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
    path::Path,
    ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
};
use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| {
        panic!("{name} is required; contract tests never use live-cloud defaults")
    })
}

fn s3() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::from_env()
            .with_endpoint(required("VELORIX_CONTRACT_S3_ENDPOINT"))
            .with_bucket_name(required("VELORIX_CONTRACT_S3_BUCKET"))
            .with_access_key_id(required("VELORIX_CONTRACT_S3_ACCESS_KEY"))
            .with_secret_access_key(required("VELORIX_CONTRACT_S3_SECRET_KEY"))
            .with_region("us-east-1")
            .with_virtual_hosted_style_request(false)
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .expect("build isolated S3 client"),
    )
}

fn gcs() -> Arc<dyn ObjectStore> {
    Arc::new(
        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(required("VELORIX_CONTRACT_GCS_BUCKET"))
            .with_config(
                GoogleConfigKey::BaseUrl,
                required("VELORIX_CONTRACT_GCS_ENDPOINT"),
            )
            .with_config(GoogleConfigKey::SkipSignature, "true")
            .with_config(GoogleConfigKey::Client(ClientConfigKey::AllowHttp), "true")
            .build()
            .expect("build isolated GCS client"),
    )
}

fn azure() -> Arc<dyn ObjectStore> {
    Arc::new(
        MicrosoftAzureBuilder::from_env()
            .with_endpoint(required("VELORIX_CONTRACT_AZURE_ENDPOINT"))
            .with_container_name(required("VELORIX_CONTRACT_AZURE_CONTAINER"))
            .with_account(required("VELORIX_CONTRACT_AZURE_ACCOUNT"))
            .with_access_key(required("VELORIX_CONTRACT_AZURE_KEY"))
            .with_allow_http(true)
            .with_use_emulator(false)
            .build()
            .expect("build isolated Azure client"),
    )
}

fn rejected_condition(error: object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
    )
}

async fn contract(name: &str, store: Arc<dyn ObjectStore>, run: &str) {
    let path = Path::from(format!("velorix-contract/{run}/{name}/object"));
    let first = b"first-version".to_vec();
    let second = b"second-version".to_vec();

    let created = store
        .put_opts(&path, first.clone().into(), PutMode::Create.into())
        .await
        .unwrap_or_else(|error| panic!("{name} conditional create failed: {error}"));
    let conflict = store
        .put_opts(&path, first.clone().into(), PutMode::Create.into())
        .await
        .expect_err("conditional create conflict");
    assert!(
        rejected_condition(conflict),
        "{name} returned a non-conflict error for duplicate create"
    );

    let updated = store
        .put_opts(
            &path,
            second.clone().into(),
            PutMode::Update(UpdateVersion {
                e_tag: created.e_tag.clone(),
                version: created.version.clone(),
            })
            .into(),
        )
        .await
        .unwrap_or_else(|error| panic!("{name} conditional update failed: {error}"));
    let stale = store
        .put_opts(
            &path,
            first.into(),
            PutMode::Update(UpdateVersion {
                e_tag: created.e_tag,
                version: created.version,
            })
            .into(),
        )
        .await
        .expect_err("stale conditional update");
    assert!(
        rejected_condition(stale),
        "{name} returned a non-conflict error for stale update"
    );

    let head = store.head(&path).await.expect("head");
    assert_eq!(head.e_tag, updated.e_tag);
    assert_eq!(
        store
            .get(&path)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("get bytes")
            .as_ref(),
        second
    );
    assert_eq!(
        store.get_range(&path, 6..14).await.expect("range").as_ref(),
        b"-version"
    );
    let listed = store
        .list(Some(&Path::from(format!("velorix-contract/{run}/{name}"))))
        .try_collect::<Vec<_>>()
        .await
        .expect("list");
    assert!(
        listed.iter().any(|meta| meta.location == path),
        "{name} list omitted the object"
    );
    store.delete(&path).await.expect("delete");
    assert!(
        matches!(
            store.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "{name} object remained after delete"
    );
}

#[tokio::test]
#[ignore = "requires isolated S3 Floci endpoint and disposable pre-created bucket"]
async fn floci_s3_provider_contract() {
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
        .to_string();
    contract("s3", s3(), &run).await;
}

#[tokio::test]
#[ignore = "requires isolated GCS Floci endpoint and disposable pre-created bucket"]
async fn floci_gcs_provider_contract() {
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
        .to_string();
    contract("gcs", gcs(), &run).await;
}

#[tokio::test]
#[ignore = "requires isolated Azure Floci endpoint and disposable pre-created container"]
async fn floci_azure_provider_contract() {
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
        .to_string();
    contract("azure", azure(), &run).await;
}

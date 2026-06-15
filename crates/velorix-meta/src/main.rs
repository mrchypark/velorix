use std::{
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use object_store::{aws::AmazonS3Builder, path::Path, prefix::PrefixStore, ObjectStore};
use tonic::{transport::Server, Code, Request};
use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
    RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
    VelorixRelationCatalogV1, VelorixRelationSchemaV1,
    CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
};
use velorix_meta::{
    proto::{
        velorix_meta_client::VelorixMetaClient, velorix_meta_server::VelorixMetaServer,
        ReadMetaStoreCapabilitiesRequest,
    },
    validate_bearer_token, AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    GrpcMetaStore, InMemoryMetaStore, MetaGrpcService, MetaStore, OssMetaStore,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    StandingRuntimeCheckpointPointer, StandingRuntimeOwnerClaim, StandingRuntimeOwnerToken,
};
use velorix_storage::object_key::ObjectKey;

#[cfg(feature = "hiqlite-backend")]
use velorix_meta::HiqliteMetaStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = env::args();
    let _program = args.next();
    match args.next().as_deref() {
        None | Some("serve") => serve().await,
        Some("smoke") => run_meta_smoke(parse_meta_smoke_args(args)?).await,
        Some(other) => {
            anyhow::bail!("unknown velorix-meta command `{other}`; expected `serve` or `smoke`")
        }
    }
}

async fn serve() -> anyhow::Result<()> {
    let bind = env::var("VELORIX_META_BIND")
        .unwrap_or_else(|_| "0.0.0.0:9090".to_string())
        .parse::<SocketAddr>()?;
    let store = meta_store_from_env().await?;
    let service = match optional_bearer_token_env("VELORIX_META_BEARER_TOKEN")? {
        Some(token) => MetaGrpcService::with_bearer_token(store, token)?,
        None => MetaGrpcService::new(store),
    };

    Server::builder()
        .add_service(VelorixMetaServer::new(service))
        .serve(bind)
        .await?;

    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetaSmokeConfig {
    endpoint: String,
    bearer_token: String,
    expect_backend: String,
    expect_auth_enforced: bool,
    expect_production_multi_writer_safe: bool,
    require_unauthenticated_rejected: bool,
    run_standing_runtime_fencing_adversarial: bool,
    catalog_probe_id: String,
    connect_retry_timeout: Duration,
}

fn parse_meta_smoke_args(
    args: impl IntoIterator<Item = String>,
) -> anyhow::Result<MetaSmokeConfig> {
    let mut endpoint = env::var("VELORIX_META_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9090".to_string());
    let mut bearer_token = env::var("VELORIX_META_BEARER_TOKEN").unwrap_or_default();
    let mut expect_backend = String::new();
    let mut expect_auth_enforced = true;
    let mut expect_production_multi_writer_safe = false;
    let mut require_unauthenticated_rejected = true;
    let mut run_standing_runtime_fencing_adversarial = false;
    let mut catalog_probe_id = default_catalog_probe_id();
    let mut connect_retry_timeout = env::var("VELORIX_META_SMOKE_CONNECT_RETRY_TIMEOUT_SECONDS")
        .ok()
        .map(|value| parse_duration_seconds(&value))
        .transpose()?
        .unwrap_or_else(|| Duration::from_secs(30));

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--endpoint" => endpoint = next_arg(&mut args, "--endpoint")?,
            "--bearer-token" => bearer_token = next_arg(&mut args, "--bearer-token")?,
            "--expect-backend" => expect_backend = next_arg(&mut args, "--expect-backend")?,
            "--expect-auth-enforced" => {
                expect_auth_enforced = parse_bool(&next_arg(&mut args, "--expect-auth-enforced")?)?
            }
            "--expect-production-multi-writer-safe" => {
                expect_production_multi_writer_safe = parse_bool(&next_arg(
                    &mut args,
                    "--expect-production-multi-writer-safe",
                )?)?
            }
            "--catalog-probe-id" => catalog_probe_id = next_arg(&mut args, "--catalog-probe-id")?,
            "--connect-retry-timeout-seconds" => {
                connect_retry_timeout = parse_duration_seconds(&next_arg(
                    &mut args,
                    "--connect-retry-timeout-seconds",
                )?)?
            }
            "--run-standing-runtime-fencing-adversarial" => {
                run_standing_runtime_fencing_adversarial = true
            }
            "--allow-unauthenticated" => require_unauthenticated_rejected = false,
            "--help" | "-h" => {
                print_meta_smoke_usage();
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown velorix-meta smoke argument `{other}`"),
        }
    }

    if expect_backend.trim().is_empty() {
        anyhow::bail!("velorix-meta smoke requires --expect-backend");
    }
    if expect_auth_enforced {
        validate_bearer_token(&bearer_token)
            .map_err(|error| anyhow::anyhow!("invalid smoke bearer token: {error}"))?;
    }
    if catalog_probe_id.trim().is_empty() || catalog_probe_id.chars().any(char::is_whitespace) {
        anyhow::bail!("--catalog-probe-id must be nonempty and contain no whitespace");
    }

    Ok(MetaSmokeConfig {
        endpoint,
        bearer_token,
        expect_backend,
        expect_auth_enforced,
        expect_production_multi_writer_safe,
        require_unauthenticated_rejected,
        run_standing_runtime_fencing_adversarial,
        catalog_probe_id,
        connect_retry_timeout,
    })
}

fn default_catalog_probe_id() -> String {
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("pid{}-{unix_ms}", std::process::id())
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> anyhow::Result<String> {
    args.next()
        .ok_or_else(|| anyhow::anyhow!("{name} requires a value"))
}

fn parse_bool(value: &str) -> anyhow::Result<bool> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => anyhow::bail!("expected boolean true/false or 1/0, got `{other}`"),
    }
}

fn parse_duration_seconds(value: &str) -> anyhow::Result<Duration> {
    let seconds = value.parse::<u64>().map_err(|error| {
        anyhow::anyhow!("expected positive integer seconds, got `{value}`: {error}")
    })?;
    if seconds == 0 {
        anyhow::bail!("retry timeout seconds must be greater than zero");
    }
    Ok(Duration::from_secs(seconds))
}

fn print_meta_smoke_usage() {
    eprintln!(
        "Usage: velorix-meta smoke --endpoint http://velorix-meta:9090 --expect-backend in-memory [--bearer-token TOKEN]"
    );
}

async fn run_meta_smoke(config: MetaSmokeConfig) -> anyhow::Result<()> {
    let deadline = Instant::now() + config.connect_retry_timeout;
    loop {
        match run_meta_smoke_once(&config).await {
            Ok(()) => return Ok(()),
            Err(error) if smoke_error_retryable(&error) && Instant::now() < deadline => {
                eprintln!("velorix-meta smoke retrying transient connection error: {error:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn run_meta_smoke_once(config: &MetaSmokeConfig) -> anyhow::Result<()> {
    if config.require_unauthenticated_rejected {
        assert_unauthenticated_capability_read_rejected(&config.endpoint).await?;
    }

    let store = if config.expect_auth_enforced {
        GrpcMetaStore::connect_with_bearer_token(&config.endpoint, config.bearer_token.clone())
            .await?
    } else {
        GrpcMetaStore::connect(&config.endpoint).await?
    };
    let capability = store
        .read_meta_store_capabilities()
        .await?
        .standing_runtime_fencing;

    if capability.backend_name != config.expect_backend {
        anyhow::bail!(
            "metadata backend mismatch: expected `{}`, got `{}`",
            config.expect_backend,
            capability.backend_name
        );
    }
    if capability.control_plane_auth_enforced != config.expect_auth_enforced {
        anyhow::bail!(
            "metadata auth enforcement mismatch: expected {}, got {}",
            config.expect_auth_enforced,
            capability.control_plane_auth_enforced
        );
    }
    if capability.production_multi_writer_safe != config.expect_production_multi_writer_safe {
        anyhow::bail!(
            "metadata production safety mismatch: expected {}, got {}",
            config.expect_production_multi_writer_safe,
            capability.production_multi_writer_safe
        );
    }
    let catalog = smoke_relation_catalog(&config.catalog_probe_id)?;
    let store_outcome = store.store_relation_catalog(catalog.clone()).await?;
    let read_catalog = store
        .read_relation_catalog(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await?;
    if read_catalog != catalog {
        anyhow::bail!("metadata catalog write/read smoke returned a different catalog");
    }
    if config.run_standing_runtime_fencing_adversarial {
        run_standing_runtime_fencing_adversarial_smoke(&store, &config.catalog_probe_id).await?;
    }

    println!(
        "velorix-meta smoke ok: endpoint={} backend={} auth_enforced={} production_multi_writer_safe={} backend_time_source_kind={} backend_time_blocked_reason={} catalog_probe_id={} catalog_store_outcome={:?}",
        config.endpoint,
        capability.backend_name,
        capability.control_plane_auth_enforced,
        capability.production_multi_writer_safe,
        capability.backend_time_source_kind,
        capability.backend_time_blocked_reason,
        config.catalog_probe_id,
        store_outcome
    );
    Ok(())
}

async fn run_standing_runtime_fencing_adversarial_smoke<S>(
    store: &S,
    probe_id: &str,
) -> anyhow::Result<()>
where
    S: MetaStore + ?Sized,
{
    const OWNER_A_TTL_MS: u64 = 250;
    const OWNER_A_EXPIRY_WAIT_MS: u64 = 500;

    let tenant_id = format!("smoke-tenant-{probe_id}");
    let program_id = "smoke-program".to_string();
    let view_id = "smoke-view".to_string();

    let owner_a = match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: tenant_id.clone(),
            program_id: program_id.clone(),
            view_id: view_id.clone(),
            owner_id: "owner-a".to_string(),
            ttl_ms: OWNER_A_TTL_MS,
        })
        .await?
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => claim,
        outcome => {
            anyhow::bail!("owner-a initial acquire returned unexpected outcome: {outcome:?}")
        }
    };

    let checkpoint_1 = smoke_checkpoint_pointer(&tenant_id, &program_id, &view_id, 1, 'a')?;
    let publish_1 = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: checkpoint_1.clone(),
            owner: smoke_owner_token(&owner_a),
        })
        .await?;
    if publish_1 != PublishStandingRuntimeCheckpointOutcome::Published {
        anyhow::bail!("owner-a initial checkpoint publish returned {publish_1:?}");
    }

    tokio::time::sleep(Duration::from_millis(OWNER_A_EXPIRY_WAIT_MS)).await;

    let checkpoint_2 = smoke_checkpoint_pointer(&tenant_id, &program_id, &view_id, 2, 'b')?;
    let expired_owner_publish = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(checkpoint_1.clone()),
            candidate: checkpoint_2.clone(),
            owner: smoke_owner_token(&owner_a),
        })
        .await;
    match expired_owner_publish {
        Ok(PublishStandingRuntimeCheckpointOutcome::Published)
        | Ok(PublishStandingRuntimeCheckpointOutcome::Duplicate) => {
            anyhow::bail!(
                "expired owner-a checkpoint publish unexpectedly succeeded: publish={expired_owner_publish:?}"
            );
        }
        Ok(PublishStandingRuntimeCheckpointOutcome::Conflict) | Err(_) => {}
    }

    let owner_b = match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: tenant_id.clone(),
            program_id: program_id.clone(),
            view_id: view_id.clone(),
            owner_id: "owner-b".to_string(),
            ttl_ms: 30_000,
        })
        .await?
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => claim,
        outcome => anyhow::bail!("owner-b acquire after logical expiry returned {outcome:?}"),
    };
    if owner_b.owner_epoch <= owner_a.owner_epoch {
        anyhow::bail!(
            "owner-b epoch {} did not fence owner-a epoch {}",
            owner_b.owner_epoch,
            owner_a.owner_epoch
        );
    }

    let publish_2 = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(checkpoint_1.clone()),
            candidate: checkpoint_2.clone(),
            owner: smoke_owner_token(&owner_b),
        })
        .await?;
    if publish_2 != PublishStandingRuntimeCheckpointOutcome::Published {
        anyhow::bail!("owner-b checkpoint publish returned {publish_2:?}");
    }

    let checkpoint_3 = smoke_checkpoint_pointer(&tenant_id, &program_id, &view_id, 3, 'c')?;
    let stale_owner_publish = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(checkpoint_2.clone()),
            candidate: checkpoint_3,
            owner: smoke_owner_token(&owner_a),
        })
        .await;
    if stale_owner_publish.is_ok() {
        anyhow::bail!("stale owner-a publish after owner-b acquisition unexpectedly succeeded");
    }

    let latest = store
        .read_standing_runtime_checkpoint(&tenant_id, &program_id, &view_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("latest checkpoint disappeared after owner-b publish"))?;
    if latest != checkpoint_2 {
        anyhow::bail!("latest checkpoint mismatch after stale owner rejection: {latest:?}");
    }

    println!(
        "velorix-meta standing runtime adversarial smoke ok: tenant={} program={} view={} owner_a_epoch={} owner_b_epoch={} latest_epoch={}",
        tenant_id,
        program_id,
        view_id,
        owner_a.owner_epoch,
        owner_b.owner_epoch,
        latest.logical_epoch
    );
    Ok(())
}

fn smoke_checkpoint_pointer(
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    logical_epoch: u64,
    hash_char: char,
) -> anyhow::Result<StandingRuntimeCheckpointPointer> {
    let content_hash = format!("sha256:{}", hash_char.to_string().repeat(64));
    let checkpoint_key = ObjectKey::standing_runtime_checkpoint(
        tenant_id,
        program_id,
        view_id,
        logical_epoch,
        &content_hash,
    )?
    .to_string();
    Ok(StandingRuntimeCheckpointPointer {
        tenant_id: tenant_id.to_string(),
        program_id: program_id.to_string(),
        view_id: view_id.to_string(),
        checkpoint_key,
        logical_epoch,
        content_hash,
        output_manifest_refs: Vec::new(),
    })
}

fn smoke_owner_token(claim: &StandingRuntimeOwnerClaim) -> StandingRuntimeOwnerToken {
    StandingRuntimeOwnerToken {
        tenant_id: claim.tenant_id.clone(),
        program_id: claim.program_id.clone(),
        view_id: claim.view_id.clone(),
        owner_id: claim.owner_id.clone(),
        owner_epoch: claim.owner_epoch,
    }
}

fn smoke_error_retryable(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("transport error")
            || message.contains("tcp connect error")
            || message.contains("Connection refused")
            || message.contains("connection refused")
            || message.contains("service was not ready")
    })
}

fn smoke_relation_catalog(probe_id: &str) -> anyhow::Result<VelorixRelationCatalogV1> {
    let relation_version = format!("smoke-{probe_id}");
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "velorix_meta_smoke".to_string(),
        relation_name: "velorix_meta_smoke".to_string(),
        relation_version,
        columns: vec![
            RelationColumnV1 {
                column_id: "probe_id".to_string(),
                name: "probe_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "probe_value".to_string(),
                name: "probe_value".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["probe_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)?;

    Ok(VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            mode: DataFusionRegistrationModeV1::View,
            name: "velorix_meta_smoke".to_string(),
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "velorix_meta_smoke".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    })
}

async fn assert_unauthenticated_capability_read_rejected(endpoint: &str) -> anyhow::Result<()> {
    let mut client = VelorixMetaClient::connect(endpoint.to_string()).await?;
    match client
        .read_meta_store_capabilities(Request::new(ReadMetaStoreCapabilitiesRequest {}))
        .await
    {
        Err(status) if status.code() == Code::Unauthenticated => Ok(()),
        Err(status) => anyhow::bail!(
            "unauthenticated metadata capability read failed with {}, expected unauthenticated",
            status.code()
        ),
        Ok(_) => anyhow::bail!("unauthenticated metadata capability read unexpectedly succeeded"),
    }
}

async fn meta_store_from_env() -> anyhow::Result<Arc<dyn MetaStore>> {
    match env::var("VELORIX_META_BACKEND")
        .unwrap_or_else(|_| "memory".to_string())
        .as_str()
    {
        "memory" | "in-memory" => Ok(Arc::new(InMemoryMetaStore::default())),
        "hiqlite" => hiqlite_meta_store_from_env().await,
        "oss" | "object-store" => Ok(Arc::new(OssMetaStore::new(oss_object_store_from_env()?))),
        other => Err(anyhow::anyhow!(
            "unsupported VELORIX_META_BACKEND `{other}`; expected `memory`, `hiqlite`, or `oss`"
        )),
    }
}

fn oss_object_store_from_env() -> anyhow::Result<Arc<dyn ObjectStore>> {
    if env::var("VELORIX_S3_COMPAT").ok().as_deref() != Some("1") {
        anyhow::bail!("VELORIX_META_BACKEND=oss requires VELORIX_S3_COMPAT=1");
    }
    let endpoint = required_env("AWS_ENDPOINT_URL")?;
    let region = required_env("AWS_REGION")?;
    let bucket = required_env("VELORIX_S3_BUCKET")?;
    let access_key_id = required_env("AWS_ACCESS_KEY_ID")?;
    let secret_access_key = required_env("AWS_SECRET_ACCESS_KEY")?;
    let session_token = optional_nonempty_env("AWS_SESSION_TOKEN");
    let prefix = env::var("VELORIX_S3_PREFIX").unwrap_or_else(|_| "meta".to_string());
    let force_path_style = parse_bool(
        &env::var("VELORIX_S3_FORCE_PATH_STYLE").unwrap_or_else(|_| "true".to_string()),
    )?;
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_region(&region)
        .with_bucket_name(&bucket)
        .with_access_key_id(&access_key_id)
        .with_secret_access_key(&secret_access_key)
        .with_virtual_hosted_style_request(!force_path_style);
    if let Some(session_token) = session_token {
        builder = builder.with_token(session_token);
    }
    if endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    let store = builder.build()?;
    if prefix.trim().is_empty() {
        Ok(Arc::new(store))
    } else {
        Ok(Arc::new(PrefixStore::new(
            store,
            Path::from(prefix.trim_matches('/')),
        )))
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    env::var(name).map_err(|_| anyhow::anyhow!("{name} is required"))
}

fn optional_nonempty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_bearer_token_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => {
            validate_bearer_token(&value)
                .map_err(|error| anyhow::anyhow!("invalid {name}: {error}"))?;
            Ok(Some(value))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(anyhow::anyhow!("invalid {name}: {error}")),
    }
}

#[cfg(feature = "hiqlite-backend")]
async fn hiqlite_meta_store_from_env() -> anyhow::Result<Arc<dyn MetaStore>> {
    let nodes = env::var("VELORIX_HIQLITE_NODES")?
        .split(',')
        .map(str::trim)
        .filter(|node| !node.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if nodes.is_empty() {
        anyhow::bail!("VELORIX_HIQLITE_NODES must contain at least one node");
    }
    let api_secret = env::var("VELORIX_HIQLITE_API_SECRET")?;
    let with_proxy = env::var("VELORIX_HIQLITE_WITH_PROXY").ok().as_deref() == Some("1");

    Ok(Arc::new(
        HiqliteMetaStore::connect_remote(nodes, api_secret, with_proxy).await?,
    ))
}

#[cfg(not(feature = "hiqlite-backend"))]
async fn hiqlite_meta_store_from_env() -> anyhow::Result<Arc<dyn MetaStore>> {
    anyhow::bail!(
        "VELORIX_META_BACKEND=hiqlite requires building velorix-meta with `--features hiqlite-backend`"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_meta_smoke_args_accepts_expected_flags() {
        let config = parse_meta_smoke_args([
            "--endpoint".to_string(),
            "http://velorix-meta:9090".to_string(),
            "--bearer-token".to_string(),
            "secret".to_string(),
            "--expect-backend".to_string(),
            "in-memory".to_string(),
            "--expect-auth-enforced".to_string(),
            "true".to_string(),
            "--expect-production-multi-writer-safe".to_string(),
            "false".to_string(),
            "--catalog-probe-id".to_string(),
            "test-probe".to_string(),
        ])
        .unwrap();

        assert_eq!(
            config,
            MetaSmokeConfig {
                endpoint: "http://velorix-meta:9090".to_string(),
                bearer_token: "secret".to_string(),
                expect_backend: "in-memory".to_string(),
                expect_auth_enforced: true,
                expect_production_multi_writer_safe: false,
                require_unauthenticated_rejected: true,
                run_standing_runtime_fencing_adversarial: false,
                catalog_probe_id: "test-probe".to_string(),
                connect_retry_timeout: Duration::from_secs(30),
            }
        );
    }

    #[test]
    fn parse_meta_smoke_args_accepts_standing_runtime_adversarial_flag() {
        let config = parse_meta_smoke_args([
            "--endpoint".to_string(),
            "http://velorix-meta:9090".to_string(),
            "--bearer-token".to_string(),
            "secret".to_string(),
            "--expect-backend".to_string(),
            "hiqlite".to_string(),
            "--run-standing-runtime-fencing-adversarial".to_string(),
        ])
        .unwrap();

        assert!(config.run_standing_runtime_fencing_adversarial);
    }

    #[test]
    fn parse_meta_smoke_args_accepts_retry_timeout() {
        let config = parse_meta_smoke_args([
            "--endpoint".to_string(),
            "http://velorix-meta:9090".to_string(),
            "--bearer-token".to_string(),
            "secret".to_string(),
            "--expect-backend".to_string(),
            "in-memory".to_string(),
            "--connect-retry-timeout-seconds".to_string(),
            "7".to_string(),
        ])
        .unwrap();

        assert_eq!(config.connect_retry_timeout, Duration::from_secs(7));
    }

    #[test]
    fn smoke_error_retryable_detects_transient_transport_errors() {
        let error = anyhow::anyhow!("transport error: tcp connect error: Connection refused");

        assert!(smoke_error_retryable(&error));
    }

    #[test]
    fn smoke_error_retryable_rejects_semantic_failures() {
        let error =
            anyhow::anyhow!("metadata backend mismatch: expected `hiqlite`, got `in-memory`");

        assert!(!smoke_error_retryable(&error));
    }

    #[test]
    fn standing_runtime_adversarial_smoke_waits_for_backend_time_expiry() {
        let source = include_str!("main.rs");
        let smoke_impl = source
            .split("async fn run_standing_runtime_fencing_adversarial_smoke")
            .nth(1)
            .expect("standing runtime adversarial smoke should be present");
        let first_publish = smoke_impl
            .find("owner-a initial checkpoint publish returned")
            .expect("smoke should publish an initial checkpoint before expiry testing");
        let expiry_wait = smoke_impl
            .find("tokio::time::sleep(Duration::from_millis(OWNER_A_EXPIRY_WAIT_MS)).await")
            .expect("smoke should wait for backend wall-clock lease expiry");
        let expired_publish = smoke_impl
            .find("expired_owner_publish")
            .expect("smoke should verify expired owner publish rejection");

        assert!(
            first_publish < expiry_wait && expiry_wait < expired_publish,
            "standing runtime adversarial smoke must wait for backend authority-time expiry after the initial publish and before expired-owner assertions"
        );
    }

    #[test]
    fn parse_meta_smoke_args_requires_backend() {
        let error = parse_meta_smoke_args([
            "--endpoint".to_string(),
            "http://velorix-meta:9090".to_string(),
            "--bearer-token".to_string(),
            "secret".to_string(),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("--expect-backend"));
    }

    #[test]
    fn smoke_relation_catalog_uses_probe_id_as_version() {
        let catalog = smoke_relation_catalog("abc").unwrap();

        assert_eq!(catalog.relation_schema.relation_id, "velorix_meta_smoke");
        assert_eq!(catalog.relation_schema.relation_version, "smoke-abc");
        catalog.validate().unwrap();
    }
}

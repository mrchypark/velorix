use std::{
    collections::{BTreeSet, HashMap},
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
    VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixRelationSourceV1,
    CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
};
use velorix_meta::{
    proto::{
        velorix_meta_client::VelorixMetaClient, velorix_meta_server::VelorixMetaServer,
        ReadMetaStoreCapabilitiesRequest,
    },
    validate_bearer_token, AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    GrpcMetaStore, InMemoryMetaStore, MetaGrpcService, MetaStore, MetaStoreError, OssMetaStore,
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
    let config = parse_meta_serve_config_from_env()?;
    let store = meta_store_from_config(&config).await?;
    let service = match config.bearer_token.clone() {
        Some(token) => MetaGrpcService::with_bearer_token(store, token)?,
        None => MetaGrpcService::new(store),
    };

    Server::builder()
        .add_service(VelorixMetaServer::new(service))
        .serve(config.bind)
        .await?;

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetaServeMode {
    Production,
    Development,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetaBackendKind {
    Memory,
    Hiqlite,
    Oss,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetaServeConfig {
    mode: MetaServeMode,
    bind: SocketAddr,
    backend: MetaBackendKind,
    bearer_token: Option<String>,
    transport_security: Option<String>,
    transport_security_attestation: Option<String>,
    hiqlite_nodes: Vec<String>,
    hiqlite_api_secret: Option<String>,
}

fn parse_meta_serve_config_from_env() -> anyhow::Result<MetaServeConfig> {
    parse_meta_serve_config(&env::vars().collect())
}

#[cfg(test)]
fn parse_meta_serve_config_from_pairs<const N: usize>(
    pairs: [(&str, &str); N],
) -> anyhow::Result<MetaServeConfig> {
    parse_meta_serve_config(
        &pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    )
}

fn parse_meta_serve_config(vars: &HashMap<String, String>) -> anyhow::Result<MetaServeConfig> {
    let mode = match required_nonempty_config(vars, "VELORIX_META_MODE")?.as_str() {
        "production" | "prod" => MetaServeMode::Production,
        "development" | "dev" => MetaServeMode::Development,
        other => anyhow::bail!(
            "unsupported VELORIX_META_MODE `{other}`; expected `production` or `development`"
        ),
    };
    let allow_development_non_loopback =
        match optional_config(vars, "VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK").as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            Some(_) => {
                anyhow::bail!("VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK must be exactly 0 or 1")
            }
        };
    let backend = parse_meta_backend(&required_nonempty_config(vars, "VELORIX_META_BACKEND")?)?;
    let bind = parse_meta_bind(vars, &mode)?;
    let bearer_token = optional_raw_config(vars, "VELORIX_META_BEARER_TOKEN");
    if let Some(token) = &bearer_token {
        validate_bearer_token(token)
            .map_err(|error| anyhow::anyhow!("invalid VELORIX_META_BEARER_TOKEN: {error}"))?;
    }
    let transport_security = optional_config(vars, "VELORIX_META_TRANSPORT_SECURITY");
    let transport_security_attestation =
        optional_config(vars, "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION");
    let hiqlite_nodes = if backend == MetaBackendKind::Hiqlite {
        parse_hiqlite_nodes(&required_nonempty_config(vars, "VELORIX_HIQLITE_NODES")?)?
    } else {
        Vec::new()
    };
    let hiqlite_api_secret = if backend == MetaBackendKind::Hiqlite {
        Some(required_nonempty_config(
            vars,
            "VELORIX_HIQLITE_API_SECRET",
        )?)
    } else {
        None
    };

    match mode {
        MetaServeMode::Production => validate_production_meta_serve_config(
            &backend,
            &bearer_token,
            &transport_security,
            &transport_security_attestation,
            &hiqlite_nodes,
        )?,
        MetaServeMode::Development => {
            if !bind.ip().is_loopback() && !allow_development_non_loopback {
                anyhow::bail!(
                    "development VELORIX_META_BIND must use a loopback address unless VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK=1"
                );
            }
            if !bind.ip().is_loopback()
                && (backend == MetaBackendKind::Memory || bearer_token.is_none())
            {
                anyhow::bail!(
                    "development VELORIX_META_BIND must use a loopback address unless a durable backend has bearer authentication"
                );
            }
            if !bind.ip().is_loopback() {
                eprintln!(
                    "warning: development non-loopback Meta transport is enabled for ephemeral local validation only; this is not production TLS or durability evidence"
                );
            }
        }
    }

    Ok(MetaServeConfig {
        mode,
        bind,
        backend,
        bearer_token,
        transport_security,
        transport_security_attestation,
        hiqlite_nodes,
        hiqlite_api_secret,
    })
}

fn parse_meta_backend(value: &str) -> anyhow::Result<MetaBackendKind> {
    match value {
        "memory" | "in-memory" => Ok(MetaBackendKind::Memory),
        "hiqlite" => Ok(MetaBackendKind::Hiqlite),
        "oss" | "object-store" => Ok(MetaBackendKind::Oss),
        other => anyhow::bail!(
            "unsupported VELORIX_META_BACKEND `{other}`; expected `memory`, `hiqlite`, or `oss`"
        ),
    }
}

fn parse_meta_bind(
    vars: &HashMap<String, String>,
    mode: &MetaServeMode,
) -> anyhow::Result<SocketAddr> {
    let value = match optional_config(vars, "VELORIX_META_BIND") {
        Some(value) => value,
        None if *mode == MetaServeMode::Development => "127.0.0.1:9090".to_string(),
        None => anyhow::bail!("VELORIX_META_BIND is required in production mode"),
    };
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid VELORIX_META_BIND `{value}`: {error}"))
}

fn validate_production_meta_serve_config(
    backend: &MetaBackendKind,
    bearer_token: &Option<String>,
    transport_security: &Option<String>,
    transport_security_attestation: &Option<String>,
    hiqlite_nodes: &[String],
) -> anyhow::Result<()> {
    if *backend == MetaBackendKind::Memory {
        anyhow::bail!(
            "production VELORIX_META_BACKEND must be durable; memory is development-only"
        );
    }
    if bearer_token.is_none() {
        anyhow::bail!("VELORIX_META_BEARER_TOKEN is required in production mode");
    }
    match transport_security.as_deref() {
        Some("service-mesh-mtls") => {}
        Some(other) => anyhow::bail!(
            "unsupported VELORIX_META_TRANSPORT_SECURITY `{other}`; velorix-meta has no native TLS listener, use `service-mesh-mtls` attestation"
        ),
        None => anyhow::bail!("VELORIX_META_TRANSPORT_SECURITY is required in production mode"),
    }
    if transport_security_attestation.is_none() {
        anyhow::bail!("VELORIX_META_TRANSPORT_SECURITY_ATTESTATION is required in production mode");
    }
    if *backend == MetaBackendKind::Hiqlite && hiqlite_nodes.len() != 3 {
        anyhow::bail!(
            "production VELORIX_HIQLITE_NODES must contain exactly three unique voter nodes"
        );
    }
    Ok(())
}

fn parse_hiqlite_nodes(value: &str) -> anyhow::Result<Vec<String>> {
    let nodes = value
        .split(',')
        .map(str::trim)
        .filter(|node| !node.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let unique_nodes = nodes.iter().collect::<BTreeSet<_>>();
    if nodes.is_empty() {
        anyhow::bail!("VELORIX_HIQLITE_NODES must contain at least one node");
    }
    if unique_nodes.len() != nodes.len() {
        anyhow::bail!("VELORIX_HIQLITE_NODES must contain exactly three unique voter nodes");
    }
    Ok(nodes)
}

fn required_nonempty_config(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> anyhow::Result<String> {
    optional_config(vars, name).ok_or_else(|| anyhow::anyhow!("{name} is required"))
}

fn optional_config(vars: &HashMap<String, String>, name: &str) -> Option<String> {
    vars.get(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_raw_config(vars: &HashMap<String, String>, name: &str) -> Option<String> {
    vars.get(name)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
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
    const OWNER_A_TTL_MS: u64 = 5_000;
    const OWNER_A_EXPIRY_WAIT_MS: u64 = 5_500;

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

    let checkpoint_1 = smoke_checkpoint_pointer(&tenant_id, &program_id, &view_id, 1, 'a', None)?;
    let publish_1 = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: checkpoint_1.clone(),
            owner: smoke_owner_token(&owner_a),
        })
        .await?;
    if publish_1 != PublishStandingRuntimeCheckpointOutcome::Published {
        let current_owner = store
            .read_standing_runtime_owner(&tenant_id, &program_id, &view_id)
            .await?;
        let current_checkpoint = store
            .read_standing_runtime_checkpoint(&tenant_id, &program_id, &view_id)
            .await?;
        anyhow::bail!(
            "owner-a initial checkpoint publish returned {publish_1:?}; owner_a={owner_a:?}; current_owner={current_owner:?}; current_checkpoint={current_checkpoint:?}"
        );
    }

    tokio::time::sleep(Duration::from_millis(OWNER_A_EXPIRY_WAIT_MS)).await;

    let checkpoint_2 = smoke_checkpoint_pointer(
        &tenant_id,
        &program_id,
        &view_id,
        2,
        'b',
        Some(&checkpoint_1),
    )?;
    let expired_owner_publish = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(checkpoint_1.clone()),
            candidate: checkpoint_2.clone(),
            owner: smoke_owner_token(&owner_a),
        })
        .await;
    match expired_owner_publish {
        Err(error) if expired_owner_publish_error_is_expected(&error) => {}
        other => {
            anyhow::bail!(
                "expired owner-a checkpoint publish expected lease fencing rejection: publish={other:?}"
            );
        }
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

    let checkpoint_3 = smoke_checkpoint_pointer(
        &tenant_id,
        &program_id,
        &view_id,
        3,
        'c',
        Some(&checkpoint_2),
    )?;
    let stale_checkpoint_3 = smoke_checkpoint_pointer(
        &tenant_id,
        &program_id,
        &view_id,
        3,
        'c',
        Some(&checkpoint_1),
    )?;
    let stale_expected_previous_publish = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(checkpoint_1),
            candidate: stale_checkpoint_3,
            owner: smoke_owner_token(&owner_b),
        })
        .await?;
    if stale_expected_previous_publish != PublishStandingRuntimeCheckpointOutcome::Conflict {
        anyhow::bail!(
            "stale expected_previous checkpoint publish returned {stale_expected_previous_publish:?}"
        );
    }

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
        "velorix-meta standing runtime adversarial smoke ok: tenant={} program={} view={} owner_a_epoch={} owner_b_epoch={} latest_epoch={} stale_checkpoint_pointer_publish_conflicted=true",
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
    previous: Option<&StandingRuntimeCheckpointPointer>,
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
        manifest_hash: format!("sha256:{}", hash_char.to_string().repeat(64)),
        output_manifest_refs: Vec::new(),
        bootstrap_generation: 0,
        plan_hash: String::new(),
        coverage_hash: String::new(),
        input_coverage: None,
        previous_checkpoint_key: previous
            .map(|pointer| pointer.checkpoint_key.clone())
            .unwrap_or_default(),
        previous_manifest_hash: previous
            .map(|pointer| pointer.manifest_hash.clone())
            .unwrap_or_default(),
    })
}

fn expired_owner_publish_error_is_expected(error: &MetaStoreError) -> bool {
    const EXPECTED_MESSAGE: &str =
        "standing runtime owner token does not match the current unexpired owner";
    error.to_string().contains(EXPECTED_MESSAGE)
        && matches!(
            error,
            MetaStoreError::StandingRuntimeOwnerMismatch | MetaStoreError::Remote(_)
        )
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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

async fn meta_store_from_config(config: &MetaServeConfig) -> anyhow::Result<Arc<dyn MetaStore>> {
    match config.backend {
        MetaBackendKind::Memory => Ok(Arc::new(InMemoryMetaStore::default())),
        MetaBackendKind::Hiqlite => {
            hiqlite_meta_store_from_config(
                &config.hiqlite_nodes,
                config
                    .hiqlite_api_secret
                    .as_ref()
                    .expect("hiqlite api secret is validated before store construction"),
            )
            .await
        }
        MetaBackendKind::Oss => Ok(Arc::new(OssMetaStore::new(oss_object_store_from_env()?))),
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

#[cfg(feature = "hiqlite-backend")]
async fn hiqlite_meta_store_from_config(
    nodes: &[String],
    api_secret: &str,
) -> anyhow::Result<Arc<dyn MetaStore>> {
    let with_proxy = env::var("VELORIX_HIQLITE_WITH_PROXY").ok().as_deref() == Some("1");

    Ok(Arc::new(
        HiqliteMetaStore::connect_remote(nodes.to_vec(), api_secret.to_string(), with_proxy)
            .await?,
    ))
}

#[cfg(not(feature = "hiqlite-backend"))]
async fn hiqlite_meta_store_from_config(
    _nodes: &[String],
    _api_secret: &str,
) -> anyhow::Result<Arc<dyn MetaStore>> {
    anyhow::bail!(
        "VELORIX_META_BACKEND=hiqlite requires building velorix-meta with `--features hiqlite-backend`"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_checkpoint_successors_bind_predecessor_commitments() {
        let first = smoke_checkpoint_pointer("tenant", "program", "view", 1, 'a', None)
            .expect("initial smoke checkpoint should be valid");
        assert!(first.previous_checkpoint_key.is_empty());
        assert!(first.previous_manifest_hash.is_empty());

        let second = smoke_checkpoint_pointer("tenant", "program", "view", 2, 'b', Some(&first))
            .expect("successor smoke checkpoint should be valid");
        assert_eq!(second.previous_checkpoint_key, first.checkpoint_key);
        assert_eq!(second.previous_manifest_hash, first.manifest_hash);

        let third = smoke_checkpoint_pointer("tenant", "program", "view", 3, 'c', Some(&second))
            .expect("second successor smoke checkpoint should be valid");
        assert_eq!(third.previous_checkpoint_key, second.checkpoint_key);
        assert_eq!(third.previous_manifest_hash, second.manifest_hash);
    }

    #[test]
    fn expired_owner_publish_requires_exact_lease_fencing_error() {
        assert!(expired_owner_publish_error_is_expected(
            &MetaStoreError::StandingRuntimeOwnerMismatch
        ));
        assert!(expired_owner_publish_error_is_expected(&MetaStoreError::Remote(
            "remote metadata service error: standing runtime owner token does not match the current unexpired owner"
                .to_string(),
        )));
        assert!(!expired_owner_publish_error_is_expected(
            &MetaStoreError::Serialization(
                "standing runtime checkpoint predecessor commitment mismatch".to_string(),
            )
        ));
    }

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
    fn serve_config_requires_explicit_mode_and_backend() {
        let mode_error = parse_meta_serve_config_from_pairs([]).unwrap_err();
        assert!(mode_error.to_string().contains("VELORIX_META_MODE"));

        let backend_error =
            parse_meta_serve_config_from_pairs([("VELORIX_META_MODE", "development")]).unwrap_err();
        assert!(backend_error.to_string().contains("VELORIX_META_BACKEND"));
    }

    #[test]
    fn development_memory_config_defaults_to_loopback_only() {
        let config = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "memory"),
        ])
        .unwrap();

        assert_eq!(config.mode, MetaServeMode::Development);
        assert_eq!(config.bind, "127.0.0.1:9090".parse::<SocketAddr>().unwrap());
        assert_eq!(config.backend, MetaBackendKind::Memory);
        assert_eq!(config.bearer_token, None);

        let public_bind_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "memory"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
        ])
        .unwrap_err();
        assert!(public_bind_error.to_string().contains("loopback"));
    }

    #[test]
    fn development_hiqlite_config_allows_authenticated_cluster_bind() {
        let config = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "hiqlite"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK", "1"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_HIQLITE_API_SECRET", "api-secret"),
            (
                "VELORIX_HIQLITE_NODES",
                "node-a:8200,node-b:8200,node-c:8200",
            ),
        ])
        .unwrap();

        assert_eq!(config.mode, MetaServeMode::Development);
        assert_eq!(config.backend, MetaBackendKind::Hiqlite);
        assert_eq!(config.bind, "0.0.0.0:9090".parse::<SocketAddr>().unwrap());
        assert_eq!(config.hiqlite_nodes.len(), 3);
    }

    #[test]
    fn development_durable_cluster_bind_requires_bearer_authentication() {
        let error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "hiqlite"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK", "1"),
            ("VELORIX_HIQLITE_API_SECRET", "api-secret"),
            (
                "VELORIX_HIQLITE_NODES",
                "node-a:8200,node-b:8200,node-c:8200",
            ),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("loopback"));
    }

    #[test]
    fn development_non_loopback_opt_in_requires_exact_boolean_value() {
        let error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "hiqlite"),
            ("VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK", "true"),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("must be exactly 0 or 1"));
    }

    #[test]
    fn production_config_rejects_missing_durable_backend_and_memory_backend() {
        let missing_backend_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "service-mesh-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
        ])
        .unwrap_err();
        assert!(missing_backend_error
            .to_string()
            .contains("VELORIX_META_BACKEND"));

        let memory_backend_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "memory"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "service-mesh-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
        ])
        .unwrap_err();
        assert!(memory_backend_error.to_string().contains("memory"));
    }

    #[test]
    fn production_config_requires_auth_and_transport_security_attestation() {
        let missing_auth_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_TRANSPORT_SECURITY", "service-mesh-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
        ])
        .unwrap_err();
        assert!(missing_auth_error
            .to_string()
            .contains("VELORIX_META_BEARER_TOKEN"));

        let missing_transport_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
        ])
        .unwrap_err();
        assert!(missing_transport_error
            .to_string()
            .contains("VELORIX_META_TRANSPORT_SECURITY"));

        let unsupported_transport_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "native-tls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
        ])
        .unwrap_err();
        assert!(unsupported_transport_error
            .to_string()
            .contains("service-mesh-mtls"));

        let missing_attestation_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "service-mesh-mtls"),
        ])
        .unwrap_err();
        assert!(missing_attestation_error
            .to_string()
            .contains("VELORIX_META_TRANSPORT_SECURITY_ATTESTATION"));
    }

    #[test]
    fn production_hiqlite_requires_exactly_three_unique_voter_nodes() {
        let missing_api_secret_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "hiqlite"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "service-mesh-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
            (
                "VELORIX_HIQLITE_NODES",
                "node-a:8200,node-b:8200,node-c:8200",
            ),
        ])
        .unwrap_err();
        assert!(missing_api_secret_error
            .to_string()
            .contains("VELORIX_HIQLITE_API_SECRET"));

        let duplicate_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "hiqlite"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "service-mesh-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
            ("VELORIX_HIQLITE_API_SECRET", "api-secret"),
            (
                "VELORIX_HIQLITE_NODES",
                "node-a:8200,node-a:8200,node-b:8200",
            ),
        ])
        .unwrap_err();
        assert!(duplicate_error.to_string().contains("three unique"));

        let valid = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "hiqlite"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "service-mesh-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
            ("VELORIX_HIQLITE_API_SECRET", "api-secret"),
            (
                "VELORIX_HIQLITE_NODES",
                "node-a:8200,node-b:8200,node-c:8200",
            ),
        ])
        .unwrap();

        assert_eq!(
            valid.hiqlite_nodes,
            vec![
                "node-a:8200".to_string(),
                "node-b:8200".to_string(),
                "node-c:8200".to_string()
            ]
        );
    }

    #[test]
    fn smoke_relation_catalog_uses_probe_id_as_version() {
        let catalog = smoke_relation_catalog("abc").unwrap();

        assert_eq!(catalog.relation_schema.relation_id, "velorix_meta_smoke");
        assert_eq!(catalog.relation_schema.relation_version, "smoke-abc");
        catalog.validate().unwrap();
    }
}

use std::{
    collections::{BTreeSet, HashMap},
    env, fmt,
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use object_store::{aws::AmazonS3Builder, path::Path, prefix::PrefixStore, ObjectStore};
use serde::{Deserialize, Serialize};
#[cfg(feature = "rhiza-backend")]
use serde_json::json;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
    RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
    VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixRelationSourceV1,
    CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
};
use velorix_meta::GrpcClientTlsConfig;
use velorix_meta::{
    proto::velorix_meta_server::VelorixMetaServer, validate_bearer_token,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest, GrpcMetaStore,
    InMemoryMetaStore, MetaGrpcService, MetaStore, MetaStoreError, OssMetaStore,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    StandingRuntimeCheckpointPointer, StandingRuntimeOwnerClaim, StandingRuntimeOwnerToken,
};
use velorix_storage::object_key::ObjectKey;

#[cfg(feature = "rhiza-backend")]
use velorix_meta::{rhiza_kv::RhizaKvStore, rhiza_meta::RhizaKvMetaStore};

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
    // Startup is only successful after a backend operation has completed. The
    // Operator-managed Rhiza path already waited on native local readiness
    // inside its constructor, because the Operator clones this binary for a
    // learner that must start, and serve recovery endpoints, before it has a
    // quorum. Velorix business traffic is a separate gate: it stays closed
    // until a quorum-backed operation succeeds, so a learner that has not been
    // promoted yet never answers metadata requests. That readiness distinction
    // is the same one native reports on `/recovery/status`.
    if let Err(error) = wait_for_meta_store_readiness(&config, &store).await {
        if !rhiza_operator_managed_learner(&config) {
            return Err(error);
        }
        // Keep the recovery endpoints serving and keep re-probing for quorum.
        // Promotion happens in place: the Operator promotes this same process
        // with this same environment, so the only thing that changes is that
        // native now has a quorum. Exiting removes exactly the endpoints the
        // promotion needs, and parking forever leaves a promoted node serving
        // recovery endpoints with Velorix traffic closed for the rest of its
        // life. The store handle stays alive here, so the native recovery
        // listener stays bound, and no Velorix gRPC listener exists until the
        // quorum-backed probe below actually succeeds.
        eprintln!(
            "operator-managed learner has no quorum yet, so Velorix metadata traffic stays closed while it keeps re-probing: {error}"
        );
        let probe_store = Arc::clone(&store);
        await_meta_store_quorum(
            move || {
                let store = Arc::clone(&probe_store);
                async move { store.read_meta_store_capabilities().await.map(|_| ()) }
            },
            QUORUM_REPROBE_INITIAL_DELAY,
            QUORUM_REPROBE_MAX_DELAY,
        )
        .await?;
        eprintln!("operator-managed learner now has a quorum, so Velorix metadata traffic opens");
    }
    let service = match config.bearer_token.clone() {
        Some(token) => MetaGrpcService::with_bearer_token(store, token)?,
        None => MetaGrpcService::new(store),
    };

    if config.transport_security.as_deref() == Some("native-mtls") {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = std::fs::read(
            config
                .tls_cert_file
                .as_ref()
                .expect("native-mtls cert path validated before serve"),
        )?;
        let key = std::fs::read(
            config
                .tls_key_file
                .as_ref()
                .expect("native-mtls key path validated before serve"),
        )?;
        let client_ca = std::fs::read(
            config
                .tls_client_ca_file
                .as_ref()
                .expect("native-mtls client CA path validated before serve"),
        )?;
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(cert, key))
                    .client_ca_root(Certificate::from_pem(client_ca)),
            )?
            .add_service(VelorixMetaServer::new(service))
            .serve(config.bind)
            .await?;
    } else {
        Server::builder()
            .add_service(VelorixMetaServer::new(service))
            .serve(config.bind)
            .await?;
    }

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
    RhizaKv,
    Oss,
}

#[derive(Clone, Copy)]
struct MetaTlsFiles<'a> {
    cert: Option<&'a String>,
    key: Option<&'a String>,
    client_ca: Option<&'a String>,
}

/// The service's Rhiza configuration is deliberately explicit. In particular,
/// membership and peer addresses are not inferred from local process state.
/// This prevents a restarted no-PVC node from silently joining a different
/// cluster or advertising an unreachable address.
///
/// Membership carries public identity only. Rhiza 0.19.0 rejects the legacy
/// per-member `token` field, and a node's private peer token is derived from a
/// process-only secret, so it never enters this document.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RhizaMemberConfig {
    node_id: String,
    url: String,
    peer_url: String,
    /// Standard padded base64 Ed25519 public key derived from that node's
    /// private peer token. Empty only for a single-node cluster that does not
    /// authenticate peers.
    #[serde(default)]
    public_key: String,
    /// Native `quepaxa.Member` publishes this for members whose decision log is
    /// served separately. Native decodes members with unknown fields disallowed,
    /// so accepting exactly the native schema keeps the Operator's documents
    /// readable without allowing a second, Velorix-only spelling of a member.
    #[serde(default)]
    log_url: String,
    /// Native WAL identity nonce, published when a member's WAL is not the
    /// default. Private peer tokens never appear here.
    #[serde(default)]
    wal_identity: Option<String>,
}

impl fmt::Debug for RhizaMemberConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RhizaMemberConfig")
            .field("node_id", &self.node_id)
            .field("url", &self.url)
            .field("peer_url", &self.peer_url)
            .field("public_key", &redacted_value(&self.public_key))
            .field("log_url", &self.log_url)
            .field(
                "wal_identity",
                &self.wal_identity.as_ref().map(|_| "[present]"),
            )
            .finish()
    }
}

/// Rhiza backend configuration. The two variants differ in who owns the
/// canonical `RHIZA_*` contract.
#[derive(Clone, Debug, Eq, PartialEq)]
enum RhizaServeConfig {
    /// Velorix-owned explicit configuration, translated into a
    /// `rhizadb::Config`. Retained for the single-node development path and for
    /// deployments that do not run the Rhiza Operator. Boxed so the operator
    /// variant stays small next to this one.
    Explicit(Box<RhizaExplicitConfig>),
    /// The Rhiza Operator owns generation identity and rewrites the canonical
    /// `RHIZA_*` environment between generations. Native reads that environment
    /// itself on every process start, so this mode holds only the private
    /// recovery listener address and whether the Operator cloned this binary as
    /// a learner, and never a parsed copy of the canonical variables that could
    /// shadow them.
    OperatorManaged {
        /// Address passed to `Db::start_operator`. Only the recovery endpoints
        /// are served there; Velorix gRPC stays on `VELORIX_META_BIND`.
        recovery_bind: String,
        /// `RHIZA_LEARNER` was set, so this process is a new identity outside the
        /// voter set awaiting promotion into the target generation.
        learner: bool,
    },
}

impl RhizaServeConfig {
    #[cfg(test)]
    fn recovery_bind(&self) -> Option<&str> {
        match self {
            RhizaServeConfig::OperatorManaged { recovery_bind, .. } => Some(recovery_bind),
            RhizaServeConfig::Explicit(_) => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
struct RhizaExplicitConfig {
    data_dir: String,
    node_id: String,
    cluster_id: String,
    bind_addr: String,
    peer_addr: String,
    admin_token: Option<String>,
    /// This process's private peer identity token. It is never published and
    /// never serialized into membership, logs, or diagnostics.
    peer_token: Option<String>,
    members: Vec<RhizaMemberConfig>,
    object_store_provider: Option<String>,
    object_store_endpoint: Option<String>,
    object_store_bucket: Option<String>,
    object_store_region: Option<String>,
    object_store_prefix: Option<String>,
    object_store_access_key: Option<String>,
    object_store_secret_key: Option<String>,
    object_store_session_token: Option<String>,
    object_store_insecure: bool,
    object_store_durability: String,
}

/// Renders a secret as a presence marker only. Rhiza tokens, derived peer keys,
/// and object-store credentials never reach a log line or panic message.
fn redacted_optional(value: &Option<String>) -> Option<&'static str> {
    value.as_ref().map(|_| "[redacted]")
}

fn redacted_value(value: &str) -> &'static str {
    if value.is_empty() {
        ""
    } else {
        "[redacted]"
    }
}

impl fmt::Debug for RhizaExplicitConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RhizaExplicitConfig")
            .field("data_dir", &self.data_dir)
            .field("node_id", &self.node_id)
            .field("cluster_id", &self.cluster_id)
            .field("bind_addr", &self.bind_addr)
            .field("peer_addr", &self.peer_addr)
            .field("admin_token", &redacted_optional(&self.admin_token))
            .field("peer_token", &redacted_optional(&self.peer_token))
            .field("members", &self.members)
            .field("object_store_provider", &self.object_store_provider)
            .field("object_store_endpoint", &self.object_store_endpoint)
            .field("object_store_bucket", &self.object_store_bucket)
            .field("object_store_region", &self.object_store_region)
            .field("object_store_prefix", &self.object_store_prefix)
            .field(
                "object_store_access_key",
                &redacted_optional(&self.object_store_access_key),
            )
            .field(
                "object_store_secret_key",
                &redacted_optional(&self.object_store_secret_key),
            )
            .field(
                "object_store_session_token",
                &redacted_optional(&self.object_store_session_token),
            )
            .field("object_store_insecure", &self.object_store_insecure)
            .field("object_store_durability", &self.object_store_durability)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetaServeConfig {
    mode: MetaServeMode,
    bind: SocketAddr,
    backend: MetaBackendKind,
    bearer_token: Option<String>,
    transport_security: Option<String>,
    transport_security_attestation: Option<String>,
    rhiza: Option<RhizaServeConfig>,
    tls_cert_file: Option<String>,
    tls_key_file: Option<String>,
    tls_client_ca_file: Option<String>,
    readiness_timeout: Duration,
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
    let tls_cert_file = if transport_security.as_deref() == Some("native-mtls") {
        Some(required_nonempty_config(
            vars,
            "VELORIX_META_TLS_CERT_FILE",
        )?)
    } else {
        None
    };
    let tls_key_file = if transport_security.as_deref() == Some("native-mtls") {
        Some(required_nonempty_config(vars, "VELORIX_META_TLS_KEY_FILE")?)
    } else {
        None
    };
    let tls_client_ca_file = if transport_security.as_deref() == Some("native-mtls") {
        Some(required_nonempty_config(
            vars,
            "VELORIX_META_TLS_CLIENT_CA_FILE",
        )?)
    } else {
        None
    };
    let readiness_timeout = optional_config(vars, "VELORIX_META_READINESS_TIMEOUT_SECONDS")
        .map(|value| parse_duration_seconds(&value))
        .transpose()?
        .unwrap_or_else(|| Duration::from_secs(60));
    let rhiza = if backend == MetaBackendKind::RhizaKv {
        Some(match parse_rhiza_operator_managed_flag(vars)? {
            // The Rhiza Operator owns the canonical `RHIZA_*` contract and
            // rewrites it between generations. In this mode the native engine
            // reads that environment itself, so this process must not also hold
            // a parsed Velorix copy that could shadow it.
            true => rhiza_operator_managed_config(vars)?,
            false => RhizaServeConfig::Explicit(Box::new(parse_rhiza_config(vars, &mode)?)),
        })
    } else {
        None
    };

    match mode {
        MetaServeMode::Production => validate_production_meta_serve_config(
            &backend,
            &bearer_token,
            &transport_security,
            rhiza.as_ref(),
            MetaTlsFiles {
                cert: tls_cert_file.as_ref(),
                key: tls_key_file.as_ref(),
                client_ca: tls_client_ca_file.as_ref(),
            },
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
        rhiza,
        tls_cert_file,
        tls_key_file,
        tls_client_ca_file,
        readiness_timeout,
    })
}

fn parse_meta_backend(value: &str) -> anyhow::Result<MetaBackendKind> {
    match value {
        "memory" | "in-memory" => Ok(MetaBackendKind::Memory),
        "rhiza-kv" | "rhiza_kv" => Ok(MetaBackendKind::RhizaKv),
        "oss" | "object-store" => Ok(MetaBackendKind::Oss),
        other => anyhow::bail!(
            "unsupported VELORIX_META_BACKEND `{other}`; expected `memory`, `rhiza-kv`, or `oss`"
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
    rhiza: Option<&RhizaServeConfig>,
    tls: MetaTlsFiles<'_>,
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
        Some("native-mtls") => {}
        Some("service-mesh-mtls") => anyhow::bail!(
            "production metadata transport must use native-mtls; service-mesh-mtls is operator-attested and not verified by this binary"
        ),
        Some(other) => anyhow::bail!(
            "unsupported VELORIX_META_TRANSPORT_SECURITY `{other}`; expected `native-mtls`"
        ),
        None => anyhow::bail!("VELORIX_META_TRANSPORT_SECURITY is required in production mode"),
    }
    if transport_security.as_deref() == Some("native-mtls")
        && (tls.cert.is_none() || tls.key.is_none() || tls.client_ca.is_none())
    {
        anyhow::bail!(
            "native-mtls requires VELORIX_META_TLS_CERT_FILE, VELORIX_META_TLS_KEY_FILE, and VELORIX_META_TLS_CLIENT_CA_FILE"
        );
    }
    if *backend == MetaBackendKind::RhizaKv {
        let config = rhiza.expect("Rhiza config is parsed for the Rhiza backend");
        // In Operator-managed mode the canonical `RHIZA_*` contract is validated
        // by `rhiza_operator_managed_config`, which reads the same variables
        // native will read.
        let RhizaServeConfig::Explicit(config) = config else {
            return Ok(());
        };
        if config.members.len() != 3 {
            anyhow::bail!(
                "production VELORIX_RHIZA_MEMBERS_JSON or VELORIX_RHIZA_MEMBERS_FILE must contain exactly three unique voter nodes"
            );
        }
        if config.object_store_provider.is_none() {
            anyhow::bail!(
                "production Rhiza KV requires explicit durable object storage; filesystem/local storage is not permitted"
            );
        }
        if config.object_store_durability != "before-ack" {
            anyhow::bail!(
                "production Rhiza KV requires VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack"
            );
        }
    }
    Ok(())
}

/// Read the Operator-managed opt-in. This is the only Velorix-owned Rhiza
/// setting that participates in this mode; every other canonical `RHIZA_*`
/// value stays owned by the Operator and by native.
fn parse_rhiza_operator_managed_flag(vars: &HashMap<String, String>) -> anyhow::Result<bool> {
    match optional_config(vars, "VELORIX_RHIZA_OPERATOR_MANAGED").as_deref() {
        None | Some("0") | Some("false") => Ok(false),
        Some("1") | Some("true") => Ok(true),
        Some(_) => {
            anyhow::bail!("VELORIX_RHIZA_OPERATOR_MANAGED must be exactly 0, 1, false, or true")
        }
    }
}

/// Velorix-owned `VELORIX_RHIZA_*` names. In Operator-managed mode every one of
/// them except the opt-in itself is a conflict: native reads the canonical
/// `RHIZA_*` environment, so a lingering shadow would either be silently ignored
/// or, worse, invite a future edit that constructs a Config that contradicts
/// the generation the Operator just published.
const VELORIX_RHIZA_SHADOW_VARS: &[&str] = &[
    "VELORIX_RHIZA_DATA_DIR",
    "VELORIX_RHIZA_NODE_ID",
    "VELORIX_RHIZA_CLUSTER_ID",
    "VELORIX_RHIZA_BIND_ADDR",
    "VELORIX_RHIZA_PEER_ADDR",
    "VELORIX_RHIZA_MEMBERS_JSON",
    "VELORIX_RHIZA_MEMBERS_FILE",
    "VELORIX_RHIZA_ADMIN_TOKEN",
    "VELORIX_RHIZA_PEER_TOKEN",
    "VELORIX_RHIZA_PEER_TOKEN_FILE",
    "VELORIX_RHIZA_PEER_TOKENS",
    "VELORIX_RHIZA_OBJECT_STORE_PROVIDER",
    "VELORIX_RHIZA_OBJECT_STORE_ENDPOINT",
    "VELORIX_RHIZA_OBJECT_STORE_BUCKET",
    "VELORIX_RHIZA_OBJECT_STORE_REGION",
    "VELORIX_RHIZA_OBJECT_STORE_PREFIX",
    "VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY",
    "VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY",
    "VELORIX_RHIZA_OBJECT_STORE_SESSION_TOKEN",
    "VELORIX_RHIZA_OBJECT_STORE_INSECURE",
    "VELORIX_RHIZA_OBJECT_STORE_DURABILITY",
];

/// Validate the canonical contract the Operator maintains, then read the private
/// recovery listener address the Operator declares.
///
/// This validates only. It never constructs a `rhizadb::Config` and never writes
/// an environment variable, because the Operator rewrites `RHIZA_CLUSTER_ID`,
/// `RHIZA_CLUSTER_MEMBERS`, `RHIZA_PEER_TOKENS`, `RHIZA_ADMIN_TOKEN`, and
/// `RHIZA_OBJSTORE_DURABILITY` between generations and expects the next process
/// to read them afresh.
fn rhiza_operator_managed_config(
    vars: &HashMap<String, String>,
) -> anyhow::Result<RhizaServeConfig> {
    let conflicts = VELORIX_RHIZA_SHADOW_VARS
        .iter()
        .filter(|name| optional_config(vars, name).is_some())
        .copied()
        .collect::<Vec<_>>();
    if !conflicts.is_empty() {
        anyhow::bail!(
            "VELORIX_RHIZA_OPERATOR_MANAGED=true reads the canonical RHIZA_* environment natively, but {} also set; remove the Velorix-owned shadow or unset VELORIX_RHIZA_OPERATOR_MANAGED",
            conflicts.join(", ")
        );
    }

    let node_id = required_nonempty_config(vars, "RHIZA_NODE_ID")?;
    // Checked for shape and size only. The membership values themselves stay
    // in the environment for native to read.
    required_nonempty_config(vars, "RHIZA_CLUSTER_ID")?;
    required_nonempty_config(vars, "RHIZA_DATA_DIR")?;
    required_nonempty_config(vars, "RHIZA_PEER_ADDR")?;
    let members =
        parse_rhiza_members_json(&required_nonempty_config(vars, "RHIZA_CLUSTER_MEMBERS")?)?;
    // The Operator may clone this binary as a learner before the target
    // generation exists. It sets `RHIZA_NODE_ID` to the learner name and
    // `RHIZA_LEARNER` to that learner's own member record, so the learner is
    // deliberately absent from `RHIZA_CLUSTER_MEMBERS`. Requiring membership to
    // contain this node unconditionally would reject the learner before it could
    // serve the recovery endpoints the Operator needs to promote it.
    let learner = match optional_raw_config(vars, "RHIZA_LEARNER") {
        None => None,
        Some(raw) => Some(
            serde_json::from_str::<RhizaMemberConfig>(&raw)
                .map_err(|error| anyhow::anyhow!("invalid RHIZA_LEARNER JSON: {error}"))?,
        ),
    };
    match &learner {
        Some(learner) if learner.node_id != node_id => anyhow::bail!(
            "RHIZA_LEARNER must describe this node's RHIZA_NODE_ID `{node_id}`, but it names `{}`",
            learner.node_id
        ),
        Some(_) => {
            if members.iter().any(|member| member.node_id == node_id) {
                anyhow::bail!(
                    "RHIZA_LEARNER and RHIZA_CLUSTER_MEMBERS both name `{node_id}`; a learner is a new identity outside the voter set"
                );
            }
        }
        None if !members.iter().any(|member| member.node_id == node_id) => {
            anyhow::bail!(
                "RHIZA_CLUSTER_MEMBERS must include this node's RHIZA_NODE_ID `{node_id}`, or RHIZA_LEARNER must supply this node's identity"
            )
        }
        None => {}
    }
    if members.len() != 3 {
        anyhow::bail!("RHIZA_CLUSTER_MEMBERS must contain exactly three unique voter nodes");
    }
    if members.iter().any(|member| member.public_key.is_empty()) {
        anyhow::bail!(
            "every RHIZA_CLUSTER_MEMBERS entry must publish a public_key derived from that node's private peer token"
        );
    }

    // A private peer token must be present for this node, from exactly one of
    // the two forms native accepts. The map form is what a StatefulSet uses,
    // since one Pod template cannot mount a per-replica Secret; native selects
    // this node's own entry from it.
    let peer_tokens = optional_raw_config(vars, "RHIZA_PEER_TOKENS");
    let peer_token = optional_raw_config(vars, "RHIZA_PEER_TOKEN");
    if peer_tokens.is_some() == peer_token.is_some() {
        anyhow::bail!(
            "RHIZA_PEER_TOKENS and RHIZA_PEER_TOKEN are mutually exclusive; set exactly one so native can select this node's private peer token"
        );
    }
    if let Some(map) = &peer_tokens {
        let entries = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(map)
            .map_err(|error| anyhow::anyhow!("invalid RHIZA_PEER_TOKENS JSON: {error}"))?;
        let own = entries
            .get(&node_id)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|token| !token.is_empty());
        if own.is_none() {
            anyhow::bail!(
                "RHIZA_PEER_TOKENS must map this node's RHIZA_NODE_ID `{node_id}` to a nonempty private peer token"
            );
        }
    }
    // An empty admin token disables archive publication, which the Operator
    // needs for a before-ack source generation. Refuse it here rather than
    // letting recovery fail later with no archive to certify.
    let admin_token = optional_raw_config(vars, "RHIZA_ADMIN_TOKEN");
    if admin_token.is_none() {
        anyhow::bail!(
            "VELORIX_RHIZA_OPERATOR_MANAGED=true requires a nonempty RHIZA_ADMIN_TOKEN; an empty token disables the archive publication the Operator recovers from"
        );
    }
    if admin_token == peer_token {
        anyhow::bail!("RHIZA_ADMIN_TOKEN must differ from this node's peer identity token");
    }

    let durability = required_nonempty_config(vars, "RHIZA_OBJSTORE_DURABILITY")?;
    if durability != "before-ack" {
        anyhow::bail!(
            "VELORIX_RHIZA_OPERATOR_MANAGED=true requires RHIZA_OBJSTORE_DURABILITY=before-ack; `async` cannot produce a certified archive for recovery"
        );
    }
    let provider = required_nonempty_config(vars, "RHIZA_OBJSTORE_PROVIDER")?;
    if !matches!(provider.as_str(), "s3" | "gcs" | "azure") {
        anyhow::bail!(
            "unsupported RHIZA_OBJSTORE_PROVIDER `{provider}`; expected `s3`, `gcs`, or `azure`"
        );
    }
    required_nonempty_config(vars, "RHIZA_OBJSTORE_BUCKET")?;

    // The recovery listener is private and separate from Velorix gRPC. A Pod
    // that bound both on one address would silently drop one of them.
    //
    // There is no upstream default to inherit. Native's own `RHIZA_BIND_ADDR`
    // (`127.0.0.1:8080`) is the database HTTP API bind and `start_operator` never
    // reads it; the Rhiza Operator instead probes the Pod's container port named
    // `recovery` or `http` and falls back to 8080. A Velorix-chosen default would
    // bind a port the Operator never probes, so the Operator must declare the
    // bind address, and it must equal the `recovery`/`http` container port.
    let recovery_bind = required_nonempty_config(vars, "RHIZA_BIND_ADDR")?;
    recovery_bind
        .parse::<SocketAddr>()
        .map_err(|error| anyhow::anyhow!("invalid RHIZA_BIND_ADDR `{recovery_bind}`: {error}"))?;
    if Some(recovery_bind.as_str()) == optional_config(vars, "VELORIX_META_BIND").as_deref() {
        anyhow::bail!(
            "RHIZA_BIND_ADDR and VELORIX_META_BIND both request `{recovery_bind}`; the recovery listener and the Velorix gRPC listener need separate addresses"
        );
    }

    Ok(RhizaServeConfig::OperatorManaged {
        recovery_bind,
        learner: learner.is_some(),
    })
}

/// Parse fixed Rhiza membership from JSON or a secret-mounted file. Membership
/// publishes only derived public keys; the private peer token and the admin
/// token are retained only in the in-memory native config and never included in
/// diagnostics. A single-node cluster authenticates no peers and may omit both;
/// every voter in a multi-node cluster must publish a public key and this node
/// must supply its own private peer token.
fn parse_rhiza_config(
    vars: &HashMap<String, String>,
    mode: &MetaServeMode,
) -> anyhow::Result<RhizaExplicitConfig> {
    let data_dir = required_nonempty_config(vars, "VELORIX_RHIZA_DATA_DIR")?;
    let node_id = required_nonempty_config(vars, "VELORIX_RHIZA_NODE_ID")?;
    let cluster_id = required_nonempty_config(vars, "VELORIX_RHIZA_CLUSTER_ID")?;
    let bind_addr = required_nonempty_config(vars, "VELORIX_RHIZA_BIND_ADDR")?;
    let peer_addr = required_nonempty_config(vars, "VELORIX_RHIZA_PEER_ADDR")?;
    let admin_token = optional_raw_config(vars, "VELORIX_RHIZA_ADMIN_TOKEN");
    let members_json = optional_config(vars, "VELORIX_RHIZA_MEMBERS_JSON");
    let members_file = optional_config(vars, "VELORIX_RHIZA_MEMBERS_FILE");
    if members_json.is_some() && members_file.is_some() {
        anyhow::bail!(
            "VELORIX_RHIZA_MEMBERS_JSON and VELORIX_RHIZA_MEMBERS_FILE are mutually exclusive"
        );
    }
    let members_text = match (members_json, members_file) {
        (Some(value), None) => value,
        (None, Some(path)) => std::fs::read_to_string(&path)
            .map_err(|error| anyhow::anyhow!("cannot read VELORIX_RHIZA_MEMBERS_FILE: {error}"))?,
        (None, None) => {
            anyhow::bail!("VELORIX_RHIZA_MEMBERS_JSON or VELORIX_RHIZA_MEMBERS_FILE is required")
        }
        (Some(_), Some(_)) => unreachable!("mutually exclusive member sources were checked"),
    };
    let members = parse_rhiza_members_json(&members_text)?;
    if !members.iter().any(|member| member.node_id == node_id) {
        anyhow::bail!("Rhiza membership must include VELORIX_RHIZA_NODE_ID `{node_id}`");
    }
    if *mode == MetaServeMode::Development && members.len() != 1 {
        anyhow::bail!("development Rhiza KV is single-node only; use exactly one membership entry");
    }
    let peer_token = rhiza_peer_token(vars)?;
    if members.len() > 1 {
        if members.iter().any(|member| member.public_key.is_empty()) {
            anyhow::bail!(
                "every multi-node Rhiza membership entry must publish a public_key; derive it from that node's private peer token with base64_std(Ed25519_pubkey(HMAC-SHA256(token, \"rhiza-peer-certificate\\0\" || cluster_id || 0x00 || node_id)))"
            );
        }
        if peer_token.is_none() {
            anyhow::bail!(
                "multi-node Rhiza clusters require this node's private peer identity token; set exactly one of VELORIX_RHIZA_PEER_TOKEN or VELORIX_RHIZA_PEER_TOKEN_FILE"
            );
        }
        if let (Some(admin_token), Some(peer_token)) = (&admin_token, &peer_token) {
            if admin_token == peer_token {
                anyhow::bail!(
                    "VELORIX_RHIZA_ADMIN_TOKEN must differ from this node's peer identity token"
                );
            }
        }
    }

    let provider = optional_config(vars, "VELORIX_RHIZA_OBJECT_STORE_PROVIDER");
    if let Some(provider) = &provider {
        if !matches!(provider.as_str(), "s3" | "gcs" | "azure") {
            anyhow::bail!(
                "unsupported VELORIX_RHIZA_OBJECT_STORE_PROVIDER `{provider}`; expected `s3`, `gcs`, or `azure`"
            );
        }
    }
    let object_store_endpoint = optional_config(vars, "VELORIX_RHIZA_OBJECT_STORE_ENDPOINT")
        .or_else(|| optional_config(vars, "AWS_ENDPOINT_URL"));
    let object_store_bucket = optional_config(vars, "VELORIX_RHIZA_OBJECT_STORE_BUCKET")
        .or_else(|| optional_config(vars, "VELORIX_S3_BUCKET"));
    if provider.is_some() && object_store_bucket.is_none() {
        anyhow::bail!(
            "VELORIX_RHIZA_OBJECT_STORE_BUCKET is required when Rhiza object storage is configured"
        );
    }
    let object_store_durability = optional_config(vars, "VELORIX_RHIZA_OBJECT_STORE_DURABILITY")
        .unwrap_or_else(|| {
            if *mode == MetaServeMode::Production {
                "before-ack".to_string()
            } else {
                "async".to_string()
            }
        });
    if !matches!(object_store_durability.as_str(), "async" | "before-ack") {
        anyhow::bail!("VELORIX_RHIZA_OBJECT_STORE_DURABILITY must be `async` or `before-ack`");
    }
    let object_store_insecure =
        match optional_config(vars, "VELORIX_RHIZA_OBJECT_STORE_INSECURE").as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            Some(_) => anyhow::bail!("VELORIX_RHIZA_OBJECT_STORE_INSECURE must be exactly 0 or 1"),
        };

    Ok(RhizaExplicitConfig {
        data_dir,
        node_id,
        cluster_id,
        bind_addr,
        peer_addr,
        admin_token,
        peer_token,
        members,
        object_store_provider: provider,
        object_store_endpoint,
        object_store_bucket,
        object_store_region: optional_config(vars, "VELORIX_RHIZA_OBJECT_STORE_REGION")
            .or_else(|| optional_config(vars, "AWS_REGION")),
        object_store_prefix: optional_config(vars, "VELORIX_RHIZA_OBJECT_STORE_PREFIX"),
        object_store_access_key: optional_raw_config(vars, "VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY")
            .or_else(|| optional_raw_config(vars, "AWS_ACCESS_KEY_ID")),
        object_store_secret_key: optional_raw_config(vars, "VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY")
            .or_else(|| optional_raw_config(vars, "AWS_SECRET_ACCESS_KEY")),
        object_store_session_token: optional_raw_config(
            vars,
            "VELORIX_RHIZA_OBJECT_STORE_SESSION_TOKEN",
        )
        .or_else(|| optional_raw_config(vars, "AWS_SESSION_TOKEN")),
        object_store_insecure,
        object_store_durability,
    })
}

fn parse_rhiza_members_json(value: &str) -> anyhow::Result<Vec<RhizaMemberConfig>> {
    let members = serde_json::from_str::<Vec<RhizaMemberConfig>>(value)
        .map_err(|error| anyhow::anyhow!("invalid Rhiza membership JSON: {error}"))?;
    let mut node_ids = BTreeSet::new();
    let mut public_keys = BTreeSet::new();
    for member in &members {
        if member.node_id.trim().is_empty()
            || member.url.trim().is_empty()
            || member.peer_url.trim().is_empty()
        {
            anyhow::bail!("every Rhiza membership entry requires node_id, url, and peer_url");
        }
        if !member.url.starts_with("http://") && !member.url.starts_with("https://") {
            anyhow::bail!("Rhiza member url must use http:// or https://");
        }
        if !member.peer_url.starts_with("quic://") {
            anyhow::bail!("Rhiza member peer_url must use quic://");
        }
        if !node_ids.insert(member.node_id.clone()) {
            anyhow::bail!("Rhiza membership must contain unique node IDs");
        }
        if !member.public_key.is_empty() {
            let public_key = validate_rhiza_public_key(&member.public_key)?;
            if !public_keys.insert(public_key) {
                anyhow::bail!("Rhiza membership must contain unique voter public keys");
            }
        }
    }
    if members.is_empty() {
        anyhow::bail!("Rhiza membership must contain at least one node");
    }
    Ok(members)
}

/// Resolve this process's private peer identity token. The inline form is for
/// single processes; the file form lets a per-node Secret be mounted without
/// putting the secret in an environment variable or in the shared membership
/// document. Only one source may be set, and the value is byte-exact because it
/// deterministically derives the local public key.
fn rhiza_peer_token(vars: &HashMap<String, String>) -> anyhow::Result<Option<String>> {
    let token = optional_raw_config(vars, "VELORIX_RHIZA_PEER_TOKEN");
    let token_file = optional_config(vars, "VELORIX_RHIZA_PEER_TOKEN_FILE");
    if token.is_some() && token_file.is_some() {
        anyhow::bail!(
            "VELORIX_RHIZA_PEER_TOKEN and VELORIX_RHIZA_PEER_TOKEN_FILE are mutually exclusive"
        );
    }
    let Some(path) = token_file else {
        return Ok(token);
    };
    // A mounted Secret conventionally ends with a newline that is not part of
    // the token itself.
    let contents = std::fs::read_to_string(&path)
        .map_err(|error| anyhow::anyhow!("cannot read VELORIX_RHIZA_PEER_TOKEN_FILE: {error}"))?;
    let token = contents.trim();
    if token.is_empty() {
        anyhow::bail!("VELORIX_RHIZA_PEER_TOKEN_FILE must contain a nonempty peer token");
    }
    if token.contains(char::is_whitespace) {
        anyhow::bail!(
            "VELORIX_RHIZA_PEER_TOKEN_FILE must contain exactly one peer token with no internal whitespace"
        );
    }
    Ok(Some(token.to_string()))
}

/// Validate one published `public_key`. Rhiza 0.19.0 accepts exactly one
/// encoding: a standard padded base64 string of 32 bytes. The all-zero key is
/// rejected because native treats it as "no key published".
fn validate_rhiza_public_key(value: &str) -> anyhow::Result<[u8; 32]> {
    let public_key = decode_rhiza_public_key(value).ok_or_else(|| {
        anyhow::anyhow!(
            "Rhiza member public_key must be a standard padded base64 Ed25519 public key of exactly 32 bytes"
        )
    })?;
    if public_key == [0_u8; 32] {
        anyhow::bail!("Rhiza member public_key must not be the all-zero key");
    }
    Ok(public_key)
}

/// Decode a 32-byte key from standard padded base64. Rhiza peer tokens are
/// generated with the URL-safe alphabet, so reusing a token as a public key
/// yields a string this decoder rejects. `STANDARD` additionally demands
/// canonical padding and zeroed trailing bits in the final symbol, so an
/// unpadded or non-canonically padded key of the right length is refused
/// instead of being accepted as a second spelling of the same key.
fn decode_rhiza_public_key(value: &str) -> Option<[u8; 32]> {
    STANDARD.decode(value).ok()?.try_into().ok()
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
    unauthenticated_probe: bool,
    expect_backend: String,
    expect_auth_enforced: bool,
    expect_production_multi_writer_safe: bool,
    require_unauthenticated_rejected: bool,
    run_standing_runtime_fencing_adversarial: bool,
    verify_only: bool,
    capabilities_only: bool,
    tls_ca_file: Option<String>,
    tls_client_cert_file: Option<String>,
    tls_client_key_file: Option<String>,
    tls_domain_name: Option<String>,
    catalog_probe_id: String,
    connect_retry_timeout: Duration,
}

fn parse_meta_smoke_args(
    args: impl IntoIterator<Item = String>,
) -> anyhow::Result<MetaSmokeConfig> {
    let mut endpoint = env::var("VELORIX_META_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9090".to_string());
    let mut bearer_token = env::var("VELORIX_META_BEARER_TOKEN").unwrap_or_default();
    let mut unauthenticated_probe = false;
    let mut expect_backend = String::new();
    let mut expect_auth_enforced = true;
    let mut expect_production_multi_writer_safe = false;
    let mut require_unauthenticated_rejected = true;
    let mut run_standing_runtime_fencing_adversarial = false;
    let mut verify_only = false;
    let mut capabilities_only = false;
    let mut tls_ca_file = optional_nonempty_env("VELORIX_META_TLS_CA_FILE");
    let mut tls_client_cert_file = optional_nonempty_env("VELORIX_META_TLS_CLIENT_CERT_FILE");
    let mut tls_client_key_file = optional_nonempty_env("VELORIX_META_TLS_CLIENT_KEY_FILE");
    let mut tls_domain_name = optional_nonempty_env("VELORIX_META_TLS_DOMAIN_NAME");
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
            "--probe-unauthenticated" => unauthenticated_probe = true,
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
            "--verify-only" => verify_only = true,
            "--capabilities-only" => capabilities_only = true,
            "--tls-ca-file" => tls_ca_file = Some(next_arg(&mut args, "--tls-ca-file")?),
            "--tls-client-cert-file" => {
                tls_client_cert_file = Some(next_arg(&mut args, "--tls-client-cert-file")?)
            }
            "--tls-client-key-file" => {
                tls_client_key_file = Some(next_arg(&mut args, "--tls-client-key-file")?)
            }
            "--tls-domain-name" => {
                tls_domain_name = Some(next_arg(&mut args, "--tls-domain-name")?)
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
    if expect_auth_enforced && !unauthenticated_probe {
        validate_bearer_token(&bearer_token)
            .map_err(|error| anyhow::anyhow!("invalid smoke bearer token: {error}"))?;
    }
    if unauthenticated_probe {
        if !expect_auth_enforced {
            anyhow::bail!("--probe-unauthenticated requires --expect-auth-enforced true");
        }
        if tls_ca_file.is_none()
            || tls_client_cert_file.is_none()
            || tls_client_key_file.is_none()
            || tls_domain_name.is_none()
        {
            anyhow::bail!(
                "--probe-unauthenticated requires TLS CA, domain, client certificate, and client key"
            );
        }
        // The authenticated TLS identity is the transport identity. The probe
        // deliberately omits only the bearer token and must not open a second
        // insecure/plaintext channel.
        require_unauthenticated_rejected = false;
    }
    if catalog_probe_id.trim().is_empty() || catalog_probe_id.chars().any(char::is_whitespace) {
        anyhow::bail!("--catalog-probe-id must be nonempty and contain no whitespace");
    }

    Ok(MetaSmokeConfig {
        endpoint,
        bearer_token,
        unauthenticated_probe,
        expect_backend,
        expect_auth_enforced,
        expect_production_multi_writer_safe,
        require_unauthenticated_rejected,
        run_standing_runtime_fencing_adversarial,
        verify_only,
        capabilities_only,
        tls_ca_file,
        tls_client_cert_file,
        tls_client_key_file,
        tls_domain_name,
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
        "Usage: velorix-meta smoke --endpoint http://velorix-meta:9090 --expect-backend in-memory [--bearer-token TOKEN] [--catalog-probe-id ID] [--capabilities-only|--verify-only] [--probe-unauthenticated --tls-ca-file PATH --tls-domain-name NAME --tls-client-cert-file PATH --tls-client-key-file PATH]"
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
    let tls = smoke_tls_config(config)?;
    if config.require_unauthenticated_rejected {
        assert_unauthenticated_capability_read_rejected(&config.endpoint, tls.clone()).await?;
    }

    let store = if let Some(tls) = tls {
        if config.expect_auth_enforced && !config.unauthenticated_probe {
            GrpcMetaStore::connect_with_bearer_token_and_tls(
                &config.endpoint,
                config.bearer_token.clone(),
                tls,
            )
            .await?
        } else {
            GrpcMetaStore::connect_with_tls(&config.endpoint, tls).await?
        }
    } else if config.expect_auth_enforced && !config.unauthenticated_probe {
        GrpcMetaStore::connect_with_bearer_token(&config.endpoint, config.bearer_token.clone())
            .await?
    } else {
        GrpcMetaStore::connect(&config.endpoint).await?
    };
    if config.unauthenticated_probe {
        match store.read_meta_store_capabilities().await {
            Err(error) if unauthenticated_error_is_expected(&error) => {
                println!(
                    "velorix-meta smoke unauthenticated probe verified: endpoint={} client_identity=trusted bearer_token=omitted mutations=0",
                    config.endpoint
                );
                return Ok(());
            }
            Err(error) => anyhow::bail!(
                "unauthenticated metadata capability read failed with unexpected error: {error}"
            ),
            Ok(_) => {
                anyhow::bail!("unauthenticated metadata capability read unexpectedly succeeded")
            }
        }
    }
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
    if config.capabilities_only {
        println!(
            "velorix-meta smoke capabilities verified: endpoint={} backend={} auth_enforced={} mutations=0",
            config.endpoint, capability.backend_name, capability.control_plane_auth_enforced
        );
        return Ok(());
    }
    let catalog = smoke_relation_catalog(&config.catalog_probe_id)?;
    if config.verify_only {
        let read_catalog = store
            .read_relation_catalog(
                &catalog.relation_schema.relation_id,
                &catalog.relation_schema.relation_version,
            )
            .await?;
        if read_catalog != catalog {
            anyhow::bail!("metadata catalog verification returned a different catalog");
        }
        println!(
            "velorix-meta smoke verified: endpoint={} backend={} auth_enforced={} catalog_probe_id={} mutations=0",
            config.endpoint, capability.backend_name, capability.control_plane_auth_enforced, config.catalog_probe_id
        );
        return Ok(());
    }
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
            expected_relation_source_cuts: None,
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
            expected_relation_source_cuts: None,
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
            expected_relation_source_cuts: None,
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
            expected_relation_source_cuts: None,
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
            expected_relation_source_cuts: None,
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

async fn assert_unauthenticated_capability_read_rejected(
    endpoint: &str,
    tls: Option<GrpcClientTlsConfig>,
) -> anyhow::Result<()> {
    let client = if let Some(tls) = tls {
        GrpcMetaStore::connect_with_tls(endpoint, tls).await?
    } else {
        GrpcMetaStore::connect(endpoint).await?
    };
    match client.read_meta_store_capabilities().await {
        Err(error) if unauthenticated_error_is_expected(&error) => Ok(()),
        Err(error) => anyhow::bail!(
            "unauthenticated metadata capability read failed with unexpected error: {error}"
        ),
        Ok(_) => anyhow::bail!("unauthenticated metadata capability read unexpectedly succeeded"),
    }
}

fn unauthenticated_error_is_expected(error: &MetaStoreError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("unauthenticated")
        || message.contains("authentication credentials")
        || message.contains("authorization bearer token")
}

fn smoke_tls_config(config: &MetaSmokeConfig) -> anyhow::Result<Option<GrpcClientTlsConfig>> {
    let Some(ca_file) = &config.tls_ca_file else {
        if config.tls_client_cert_file.is_some()
            || config.tls_client_key_file.is_some()
            || config.tls_domain_name.is_some()
        {
            anyhow::bail!("TLS smoke requires a CA file, domain name, and paired client identity");
        }
        return Ok(None);
    };
    let client_cert = config
        .tls_client_cert_file
        .as_ref()
        .map(std::fs::read)
        .transpose()?;
    let client_key = config
        .tls_client_key_file
        .as_ref()
        .map(std::fs::read)
        .transpose()?;
    Ok(Some(GrpcClientTlsConfig {
        domain_name: config
            .tls_domain_name
            .clone()
            .ok_or_else(|| anyhow::anyhow!("TLS smoke requires VELORIX_META_TLS_DOMAIN_NAME"))?,
        ca_cert_pem: std::fs::read(ca_file)?,
        client_cert_pem: client_cert,
        client_key_pem: client_key,
    }))
}

/// An opened metadata store plus the concrete handles a backend may need for
/// its own startup probe. Holding the concrete Rhiza handle keeps `MetaStore`
/// free of a Rhiza-only readiness method that no other backend could honor.
async fn meta_store_from_config(config: &MetaServeConfig) -> anyhow::Result<Arc<dyn MetaStore>> {
    match config.backend {
        MetaBackendKind::Memory => Ok(Arc::new(InMemoryMetaStore::default())),
        MetaBackendKind::RhizaKv => {
            rhiza_meta_store_from_config(
                config
                    .rhiza
                    .as_ref()
                    .expect("Rhiza config is parsed for the Rhiza backend"),
                config.readiness_timeout,
            )
            .await
        }
        MetaBackendKind::Oss => Ok(Arc::new(OssMetaStore::new(oss_object_store_from_env()?))),
    }
}

/// True when the Rhiza Operator cloned this binary as a learner that has not
/// joined the target generation's voter set yet.
fn rhiza_operator_managed_learner(config: &MetaServeConfig) -> bool {
    matches!(
        config.rhiza,
        Some(RhizaServeConfig::OperatorManaged { learner: true, .. })
    )
}

async fn wait_for_meta_store_readiness(
    config: &MetaServeConfig,
    store: &Arc<dyn MetaStore>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + config.readiness_timeout;
    loop {
        match store.read_meta_store_capabilities().await {
            Ok(_) => return Ok(()),
            Err(error)
                if config.backend == MetaBackendKind::RhizaKv
                    && rhiza_readiness_error_is_retryable(&error)
                    && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => {
                anyhow::bail!("metadata backend readiness probe failed: {error}");
            }
        }
    }
}

/// The first delay between quorum re-probes, and its cap. Short enough that a
/// promoted learner starts serving promptly, long enough not to hammer a learner
/// that is still copying its certified suffix.
const QUORUM_REPROBE_INITIAL_DELAY: Duration = Duration::from_millis(250);
const QUORUM_REPROBE_MAX_DELAY: Duration = Duration::from_secs(5);

/// Keep re-probing for quorum until one linearizable read succeeds.
///
/// The bounded startup wait above is the wrong final answer for a learner:
/// promotion happens in place, so this process is the process the Operator
/// promotes and its environment does not change. The probe therefore continues
/// with capped backoff until quorum really exists, which is also the point at
/// which the caller may bind the Velorix gRPC listener. Native recovery keeps
/// serving because the store handle outlives this function. Only transient
/// native errors are retried: a rejected configuration or invalid request is
/// not going to heal by waiting, so it fails closed instead of looping.
async fn await_meta_store_quorum<F, Fut>(
    mut probe: F,
    initial_delay: Duration,
    max_delay: Duration,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), MetaStoreError>>,
{
    let mut delay = initial_delay.min(max_delay);
    loop {
        tokio::time::sleep(delay).await;
        match probe().await {
            Ok(()) => return Ok(()),
            Err(error) if rhiza_readiness_error_is_retryable(&error) => {
                eprintln!(
                    "quorum-backed metadata read is still unavailable, re-probing in {delay:?}: {error}"
                );
                delay = (delay * 2).min(max_delay);
            }
            Err(error) => {
                anyhow::bail!("metadata backend quorum probe failed: {error}");
            }
        }
    }
}

/// What a bounded native local-readiness wait concluded.
#[cfg(feature = "rhiza-backend")]
enum RhizaLocalReadiness {
    /// Native reports this node locally ready.
    Ready,
    /// Native has not reported local readiness by the deadline. Native flips
    /// `ready()` only after a learner finishes copying the certified suffix, so
    /// this is the known still-copying state and not a diagnosis of the node.
    StillCopying(anyhow::Error),
    /// The probe failed for a reason that waiting cannot resolve, such as a
    /// rejected configuration or an invalid request.
    Failed(anyhow::Error),
}

/// Poll native `ready()`. This is local readiness, not quorum: the Operator
/// distinguishes the two, and a learner must be able to start and serve
/// recovery endpoints before it joins a quorum.
#[cfg(feature = "rhiza-backend")]
async fn wait_for_rhiza_local_readiness(
    store: &RhizaKvMetaStore,
    timeout: Duration,
) -> RhizaLocalReadiness {
    let deadline = Instant::now() + timeout;
    loop {
        match store.ready().await {
            Ok(true) => return RhizaLocalReadiness::Ready,
            Ok(false) => {
                if Instant::now() >= deadline {
                    let still_copying = anyhow::anyhow!(
                        "native Rhiza local readiness is still false after {timeout:?}"
                    );
                    return RhizaLocalReadiness::StillCopying(still_copying);
                }
            }
            // A transient native failure is the same pre-quorum state as a
            // learner that has not finished copying, so it is retried until the
            // deadline and reported as still copying afterwards.
            Err(error) if rhiza_readiness_error_is_retryable(&error) => {
                if Instant::now() >= deadline {
                    let still_copying = anyhow::anyhow!(
                        "native Rhiza local readiness probe is still failing after {timeout:?}: {error}"
                    );
                    return RhizaLocalReadiness::StillCopying(still_copying);
                }
            }
            Err(error) => {
                let failed = anyhow::anyhow!("Rhiza local readiness probe failed: {error}");
                return RhizaLocalReadiness::Failed(failed);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Decide whether a native local-readiness wait that did not report ready may be
/// tolerated.
///
/// Native flips `ready()` only after a learner has copied the certified suffix
/// from its peers, so a large learner can outlast the readiness timeout while the
/// recovery endpoints the Operator needs in order to promote it are already
/// serving. Exiting there would remove exactly those endpoints, so the learner
/// keeps running and the caller's quorum gate still holds Velorix business traffic
/// closed. Only that still-copying state is tolerated: a probe failure is not
/// evidence that the node is still copying, so a learner fails closed on it too,
/// and a voter has no promotion path to protect and fails closed either way.
#[cfg(feature = "rhiza-backend")]
fn rhiza_local_readiness_gate(learner: bool, readiness: RhizaLocalReadiness) -> anyhow::Result<()> {
    match (learner, readiness) {
        (true, RhizaLocalReadiness::StillCopying(error)) => {
            eprintln!(
                "native Rhiza local readiness is still pending, so the Operator-managed learner keeps its recovery endpoints serving: {error}"
            );
            Ok(())
        }
        (false, RhizaLocalReadiness::StillCopying(error)) => {
            anyhow::bail!("a voter has no promotion path to wait for: {error}")
        }
        (_, RhizaLocalReadiness::Failed(error)) => Err(error),
        (_, RhizaLocalReadiness::Ready) => Ok(()),
    }
}

fn rhiza_readiness_error_is_retryable(error: &MetaStoreError) -> bool {
    let detail = error.to_string().to_ascii_lowercase();
    matches!(
        error,
        MetaStoreError::Rhiza(_) | MetaStoreError::RhizaContention { .. }
    ) && [
        "quorum",
        "not ready",
        "unavailable",
        "timeout",
        "deadline",
        "connection",
        "leader",
    ]
    .iter()
    .any(|marker| detail.contains(marker))
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

/// Build the Rhiza metadata store.
///
/// Operator-managed startup is only successful after the backend answers a real
/// operation, but a quorum read is the wrong gate: the Operator clones this
/// binary for a learner, and a learner reports native local readiness before it
/// has a quorum. Blocking a learner on a quorum read would leave the Operator's
/// own recovery path unable to start, so this path waits on native `ready()` and
/// tolerates a learner that is still copying, while a probe failure still fails
/// closed. Quorum stays the caller's gate: `serve` re-probes for it on a learner
/// and every other path gates on a linearizable read, so a node that cannot
/// reach a quorum never advertises a ready gRPC listener.
#[cfg(feature = "rhiza-backend")]
async fn rhiza_meta_store_from_config(
    config: &RhizaServeConfig,
    readiness_timeout: Duration,
) -> anyhow::Result<Arc<dyn MetaStore>> {
    if let RhizaServeConfig::OperatorManaged {
        recovery_bind,
        learner,
    } = config
    {
        // Native reads the canonical `RHIZA_*` environment itself and starts
        // the private recovery listener before the handle is shared, so
        // metadata operations and the Operator's recovery endpoints are one DB
        // and the listener closes with it.
        let store = RhizaKvMetaStore::open_operator_managed(recovery_bind.clone())
            .await
            .map_err(|error| {
                anyhow::anyhow!("cannot open Rhiza from the RHIZA_* environment: {error}")
            })?;
        let address = store
            .recovery_address()
            .expect("operator-managed open always binds a recovery listener");
        eprintln!(
            "rhiza operator recovery listener bound at {address}; Velorix gRPC remains on VELORIX_META_BIND"
        );
        rhiza_local_readiness_gate(
            *learner,
            wait_for_rhiza_local_readiness(&store, readiness_timeout).await,
        )?;
        if *learner {
            // A learner exists only to be promoted into the target generation.
            // The recovery endpoints are already serving, so the Operator can
            // probe and promote it; Velorix business traffic stays closed until
            // this node actually holds quorum, which the caller still gates on
            // with the quorum-backed readiness read.
            eprintln!(
                "rhiza is running as an Operator-managed learner; recovery endpoints are serving but Velorix metadata traffic stays closed until this node has quorum"
            );
        }
        return Ok(Arc::new(store));
    }
    Ok(Arc::new(
        rhiza_explicit_meta_store_from_config(config).await?,
    ))
}

#[cfg(feature = "rhiza-backend")]
async fn rhiza_explicit_meta_store_from_config(
    config: &RhizaServeConfig,
) -> anyhow::Result<RhizaKvMetaStore> {
    let RhizaExplicitConfig {
        data_dir,
        node_id,
        cluster_id,
        bind_addr,
        peer_addr,
        admin_token,
        peer_token,
        members,
        object_store_provider,
        object_store_endpoint,
        object_store_bucket,
        object_store_region,
        object_store_prefix,
        object_store_access_key,
        object_store_secret_key,
        object_store_session_token,
        object_store_insecure,
        object_store_durability,
    } = &**match config {
        RhizaServeConfig::Explicit(config) => config,
        RhizaServeConfig::OperatorManaged { .. } => {
            unreachable!("explicit construction handles only the explicit variant")
        }
    };
    let members = members
        .iter()
        .map(|member| {
            let mut entry = serde_json::Map::new();
            entry.insert("node_id".to_string(), json!(member.node_id));
            entry.insert("url".to_string(), json!(member.url));
            entry.insert("peer_url".to_string(), json!(member.peer_url));
            // Native decodes the members array rejecting unknown fields, so the
            // private peer token is never emitted. A single-node cluster
            // publishes no key at all; native derives the zero key from its
            // absent peer token and accepts it.
            if !member.public_key.is_empty() {
                entry.insert("public_key".to_string(), json!(member.public_key));
            }
            serde_json::Value::Object(entry)
        })
        .collect::<Vec<_>>();
    let mut native = rhizadb::Config::new(data_dir)
        .node_id(node_id.clone())
        .cluster_id(cluster_id.clone())
        .bind_addr(bind_addr.clone())
        .peer_addr(peer_addr.clone())
        .set_option("Members", json!(members))
        .set_option("ObjStoreDurability", json!(object_store_durability))
        .set_option("ObjStoreInsecure", json!(object_store_insecure));
    if let Some(value) = &peer_token {
        native = native.set_option("PeerToken", json!(value));
    }
    if let Some(value) = &admin_token {
        native = native.set_option("AdminToken", json!(value));
    }
    if let Some(value) = &object_store_provider {
        native = native.set_option("ObjStoreProvider", json!(value));
    }
    if let Some(value) = &object_store_endpoint {
        native = native.set_option("ObjStoreEndpoint", json!(value));
    }
    if let Some(value) = &object_store_bucket {
        native = native.set_option("ObjStoreBucket", json!(value));
    }
    if let Some(value) = &object_store_region {
        native = native.set_option("ObjStoreRegion", json!(value));
    }
    if let Some(value) = &object_store_prefix {
        native = native.set_option("ObjStorePrefix", json!(value));
    }
    if let Some(value) = &object_store_access_key {
        native = native.set_option("ObjStoreAccessKey", json!(value));
    }
    if let Some(value) = &object_store_secret_key {
        native = native.set_option("ObjStoreSecretKey", json!(value));
    }
    if let Some(value) = &object_store_session_token {
        native = native.set_option("ObjStoreSessionToken", json!(value));
    }
    let kv = RhizaKvStore::open_config(native).await?;
    Ok(RhizaKvMetaStore::new(kv))
}

#[cfg(not(feature = "rhiza-backend"))]
async fn rhiza_meta_store_from_config(
    _config: &RhizaServeConfig,
    _readiness_timeout: Duration,
) -> anyhow::Result<Arc<dyn MetaStore>> {
    anyhow::bail!(
        "VELORIX_META_BACKEND=rhiza-kv requires building velorix-meta with `--features rhiza-backend`"
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
                unauthenticated_probe: false,
                expect_backend: "in-memory".to_string(),
                expect_auth_enforced: true,
                expect_production_multi_writer_safe: false,
                require_unauthenticated_rejected: true,
                run_standing_runtime_fencing_adversarial: false,
                verify_only: false,
                capabilities_only: false,
                tls_ca_file: None,
                tls_client_cert_file: None,
                tls_client_key_file: None,
                tls_domain_name: None,
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
            "rhiza-kv".to_string(),
            "--run-standing-runtime-fencing-adversarial".to_string(),
        ])
        .unwrap();

        assert!(config.run_standing_runtime_fencing_adversarial);
    }

    #[test]
    fn parse_meta_smoke_args_accepts_verify_only_flag() {
        let config = parse_meta_smoke_args([
            "--endpoint".to_string(),
            "http://velorix-meta:9090".to_string(),
            "--expect-backend".to_string(),
            "rhiza-kv".to_string(),
            "--expect-auth-enforced".to_string(),
            "false".to_string(),
            "--allow-unauthenticated".to_string(),
            "--catalog-probe-id".to_string(),
            "recovery-probe".to_string(),
            "--verify-only".to_string(),
        ])
        .unwrap();

        assert!(config.verify_only);
        assert!(!config.expect_auth_enforced);
    }

    #[test]
    fn production_native_mtls_requires_explicit_certificate_files() {
        let error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "native-mtls"),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("VELORIX_META_TLS_CERT_FILE"));
    }

    #[test]
    fn parse_meta_smoke_args_accepts_capabilities_only_flag() {
        let config = parse_meta_smoke_args([
            "--endpoint".to_string(),
            "https://velorix-meta:9090".to_string(),
            "--bearer-token".to_string(),
            "secret".to_string(),
            "--expect-backend".to_string(),
            "rhiza-kv".to_string(),
            "--capabilities-only".to_string(),
        ])
        .unwrap();
        assert!(config.capabilities_only);
    }

    #[test]
    fn parse_meta_smoke_args_requires_tls_identity_for_unauthenticated_probe() {
        let error = parse_meta_smoke_args([
            "--endpoint".to_string(),
            "https://velorix-meta:9090".to_string(),
            "--expect-backend".to_string(),
            "rhiza-kv".to_string(),
            "--probe-unauthenticated".to_string(),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("requires TLS CA"));
    }

    #[test]
    fn rhiza_readiness_retries_only_transient_native_errors() {
        assert!(rhiza_readiness_error_is_retryable(&MetaStoreError::Rhiza(
            "Rhiza KV operation failed (quorum_unavailable): no leader".into(),
        )));
        assert!(!rhiza_readiness_error_is_retryable(
            &MetaStoreError::Serialization("invalid snapshot digest".into())
        ));
        assert!(!rhiza_readiness_error_is_retryable(&MetaStoreError::Rhiza(
            "Rhiza KV operation failed (invalid_request): malformed key".into(),
        )));
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
            anyhow::anyhow!("metadata backend mismatch: expected `rhiza-kv`, got `in-memory`");

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
    fn development_object_store_config_allows_authenticated_cluster_bind() {
        let config = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK", "1"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
        ])
        .unwrap();

        assert_eq!(config.mode, MetaServeMode::Development);
        assert_eq!(config.backend, MetaBackendKind::Oss);
        assert_eq!(config.bind, "0.0.0.0:9090".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn development_durable_cluster_bind_requires_bearer_authentication() {
        let error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK", "1"),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("loopback"));
    }

    #[test]
    fn development_non_loopback_opt_in_requires_exact_boolean_value() {
        let error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "oss"),
            ("VELORIX_META_DEVELOPMENT_ALLOW_NON_LOOPBACK", "true"),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("must be exactly 0 or 1"));
    }

    /// Fixed upstream-derived voter public keys for cluster `velorix-meta`. Each
    /// value is `base64_std(Ed25519_pubkey(HMAC-SHA256(token,
    /// "rhiza-peer-certificate\0" || cluster_id || 0x00 || node_id)))` for the
    /// peer token named beside it. They are checked-in fixtures, not secrets, and
    /// are deliberately not recomputed in-process so the config layer needs no
    /// crypto dependency. Native still validates the derivation at open.
    const NODE_A_PUBLIC_KEY: &str = "p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st72k=";
    const NODE_B_PUBLIC_KEY: &str = "Z1Lth3pFo3C8Qf2+wUHSw/84Pk9/iBb/77ad9vXpOwo=";
    const NODE_C_PUBLIC_KEY: &str = "GXLhvPu3GS35ofGR+FaL7Scove612heC7i+InZJYMNA=";
    const NODE_A_PEER_TOKEN: &str = "velorix-test-peer-token-node-a-0001";

    /// Rhiza 0.19.0 membership: public identity only, never a private token.
    const THREE_NODE_MEMBERS: &str = r#"[{"node_id":"node-a","url":"http://meta-a:9091","peer_url":"quic://meta-a:9191","public_key":"p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st72k="},{"node_id":"node-b","url":"http://meta-b:9091","peer_url":"quic://meta-b:9191","public_key":"Z1Lth3pFo3C8Qf2+wUHSw/84Pk9/iBb/77ad9vXpOwo="},{"node_id":"node-c","url":"http://meta-c:9091","peer_url":"quic://meta-c:9191","public_key":"GXLhvPu3GS35ofGR+FaL7Scove612heC7i+InZJYMNA="}]"#;

    fn rhiza_member_json(node_id: &str, public_key: &str) -> String {
        serde_json::json!({
            "node_id": node_id,
            "url": format!("http://{node_id}:9091"),
            "peer_url": format!("quic://{node_id}:9191"),
            "public_key": public_key,
        })
        .to_string()
    }

    fn rhiza_member_pair_json(public_key: &str) -> String {
        format!(
            "[{},{}]",
            rhiza_member_json("node-a", public_key),
            rhiza_member_json("node-b", NODE_B_PUBLIC_KEY)
        )
    }

    /// A complete Operator-managed environment. These are the canonical `RHIZA_*`
    /// names the Rhiza Operator rewrites between generations, plus the one
    /// Velorix-owned opt-in. No `VELORIX_RHIZA_*` shadow is present, which is what
    /// the mode requires.
    fn operator_managed_rhiza_vars(overrides: &[(&str, &str)]) -> HashMap<String, String> {
        let members = serde_json::json!([
        {"node_id":"node-a","url":"http://meta-a:9091","peer_url":"quic://meta-a:8200","public_key":NODE_A_PUBLIC_KEY},
        {"node_id":"node-b","url":"http://meta-b:9091","peer_url":"quic://meta-b:8200","public_key":NODE_B_PUBLIC_KEY},
        {"node_id":"node-c","url":"http://meta-c:9091","peer_url":"quic://meta-c:8200","public_key":NODE_C_PUBLIC_KEY},
    ])
    .to_string();
        let mut vars: HashMap<String, String> = [
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "rhiza-kv"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "native-mtls"),
            ("VELORIX_META_TLS_CERT_FILE", "/tmp/meta.crt"),
            ("VELORIX_META_TLS_KEY_FILE", "/tmp/meta.key"),
            ("VELORIX_META_TLS_CLIENT_CA_FILE", "/tmp/meta-ca.crt"),
            ("VELORIX_RHIZA_OPERATOR_MANAGED", "true"),
            ("RHIZA_NODE_ID", "node-a"),
            ("RHIZA_DATA_DIR", "/var/lib/velorix-meta"),
            ("RHIZA_CLUSTER_ID", "generation-7"),
            ("RHIZA_BIND_ADDR", "0.0.0.0:9091"),
            ("RHIZA_PEER_ADDR", "0.0.0.0:8200"),
            ("RHIZA_CLUSTER_MEMBERS", members.as_str()),
            ("RHIZA_PEER_TOKEN", NODE_A_PEER_TOKEN),
            ("RHIZA_ADMIN_TOKEN", "operator-admin-token"),
            ("RHIZA_OBJSTORE_PROVIDER", "s3"),
            ("RHIZA_OBJSTORE_ENDPOINT", "minio.minio:9000"),
            ("RHIZA_OBJSTORE_BUCKET", "velorix-meta"),
            ("RHIZA_OBJSTORE_DURABILITY", "before-ack"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        for (name, value) in overrides {
            vars.insert(name.to_string(), value.to_string());
        }
        vars
    }

    fn expect_explicit(rhiza: Option<RhizaServeConfig>) -> RhizaExplicitConfig {
        match rhiza.expect("Rhiza configuration should be present") {
            RhizaServeConfig::Explicit(config) => *config,
            RhizaServeConfig::OperatorManaged { .. } => {
                panic!("expected Velorix-owned explicit Rhiza configuration")
            }
        }
    }

    #[test]
    fn operator_managed_rhiza_reads_the_canonical_environment() {
        let config = parse_meta_serve_config(&operator_managed_rhiza_vars(&[])).unwrap();
        let rhiza = config.rhiza.expect("Rhiza configuration should be present");
        // The recovery listener address is the canonical `RHIZA_BIND_ADDR` the
        // Operator probes, not a Velorix-owned shadow. The only Velorix-owned value
        // is the opt-in itself; everything else stays in the environment for
        // native to read.
        assert_eq!(rhiza.recovery_bind(), Some("0.0.0.0:9091"));
        assert!(matches!(rhiza, RhizaServeConfig::OperatorManaged { .. }));

        // Debug output must not carry canonical credentials either, since the mode
        // holds no copy of them and diagnostics must not imply one exists.
        let rendered = format!("{:?}", rhiza);
        assert!(rendered.contains("0.0.0.0:9091"));
        for secret in [NODE_A_PEER_TOKEN, NODE_A_PUBLIC_KEY, "operator-admin-token"] {
            assert!(
                !rendered.contains(secret),
                "operator-managed debug output must not contain {secret}: {rendered}"
            );
        }
    }

    #[test]
    fn operator_managed_rhiza_requires_the_declared_recovery_bind() {
        // Native's own `RHIZA_BIND_ADDR` default belongs to the database HTTP API
        // and `start_operator` never reads it, while the Operator probes the
        // `recovery`/`http` container port. Guessing a port here would bind a
        // listener nobody probes, so an unset value must fail instead.
        for absent in [None, Some("")] {
            let mut vars = operator_managed_rhiza_vars(&[]);
            match absent {
                Some(value) => {
                    vars.insert("RHIZA_BIND_ADDR".to_string(), value.to_string());
                }
                None => {
                    vars.remove("RHIZA_BIND_ADDR");
                }
            }
            let error = parse_meta_serve_config(&vars)
                .expect_err("operator-managed mode must not invent a recovery bind address");
            assert!(
                error.to_string().contains("RHIZA_BIND_ADDR is required"),
                "error must name the missing recovery bind: {error}"
            );
        }
    }

    #[test]
    fn operator_managed_rhiza_rejects_a_conflicting_velorix_shadow() {
        for shadow in [
            "VELORIX_RHIZA_CLUSTER_ID",
            "VELORIX_RHIZA_MEMBERS_JSON",
            "VELORIX_RHIZA_PEER_TOKEN",
            "VELORIX_RHIZA_OBJECT_STORE_DURABILITY",
        ] {
            let error =
                parse_meta_serve_config(&operator_managed_rhiza_vars(&[(shadow, "leftover")]))
                    .expect_err("a Velorix-owned shadow must be rejected");
            let message = error.to_string();
            assert!(
                message.contains(shadow),
                "error must name {shadow}: {message}"
            );
            assert!(
                message.contains("canonical RHIZA_*"),
                "error must explain the conflict: {message}"
            );
        }

        // The opt-in itself is not a conflict.
        let config = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "VELORIX_RHIZA_NODE_ID",
            "",
        )]))
        .unwrap();
        assert!(config.rhiza.is_some());
    }

    #[test]
    fn operator_managed_rhiza_accepts_the_upstream_learner_environment() {
        // Upstream `automatic_workload.go` clones the application Pod and sets
        // `RHIZA_NODE_ID` to the learner name, `RHIZA_LEARNER` to that learner's
        // member record, and `RHIZA_PEER_TOKEN` to the learner's own private token
        // while clearing `RHIZA_PEER_TOKENS`. The learner is deliberately outside the
        // three voters, so a check that membership must contain this node would fail
        // the learner before it could serve the recovery endpoints it exists for.
        let mut vars = operator_managed_rhiza_vars(&[
            ("RHIZA_NODE_ID", "myapp-learner"),
            ("RHIZA_PEER_TOKENS", ""),
            ("RHIZA_PEER_TOKEN", "velorix-learner-peer-token"),
            (
                "RHIZA_LEARNER",
                r#"{"node_id":"myapp-learner","url":"http://myapp-learner:9091","peer_url":"quic://myapp-learner:8200","log_url":"","public_key":"p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st72k="}"#,
            ),
        ]);
        vars.remove("RHIZA_PEER_TOKENS");

        let config =
            parse_meta_serve_config(&vars).expect("the learner environment must be admitted");
        let rhiza = config.rhiza.expect("Rhiza configuration should be present");
        assert_eq!(rhiza.recovery_bind(), Some("0.0.0.0:9091"));
        assert!(
            matches!(
                rhiza,
                RhizaServeConfig::OperatorManaged { learner: true, .. }
            ),
            "RHIZA_LEARNER must mark this process as a learner"
        );

        // A voter of the same generation keeps the non-learner path.
        let voters = parse_meta_serve_config(&operator_managed_rhiza_vars(&[])).unwrap();
        assert!(matches!(
            voters.rhiza.expect("Rhiza configuration should be present"),
            RhizaServeConfig::OperatorManaged { learner: false, .. }
        ));

        // A learner identity that does not name this process would make native read
        // one node ID and validate another.
        let error = parse_meta_serve_config(&{
            let mut mismatched = vars.clone();
            mismatched.insert(
            "RHIZA_LEARNER".to_string(),
            r#"{"node_id":"someone-else","url":"http://other:9091","peer_url":"quic://other:8200"}"#
                .to_string(),
        );
            mismatched
        })
        .unwrap_err();
        assert!(error.to_string().contains("must describe this node"));

        // A learner is a new identity, not an extra voter: naming a voter as the
        // learner would leave native with two identities for one node.
        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_LEARNER",
            r#"{"node_id":"node-a","url":"http://meta-a:9091","peer_url":"quic://meta-a:8200"}"#,
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("outside the voter set"));

        // An unknown field in the learner record is still refused.
        let error = parse_meta_serve_config(&{
        let mut malformed = vars.clone();
        malformed.insert(
            "RHIZA_LEARNER".to_string(),
            r#"{"node_id":"myapp-learner","url":"http://l:9091","peer_url":"quic://l:8200","token":"legacy"}"#
                .to_string(),
        );
        malformed
    })
    .unwrap_err();
        assert!(error.to_string().contains("invalid RHIZA_LEARNER JSON"));
    }

    /// Native flips `ready()` only after a learner finishes copying the certified
    /// suffix, so the default 60s timeout can expire while the recovery endpoints
    /// the Operator needs in order to promote that learner are already serving.
    #[cfg(feature = "rhiza-backend")]
    #[test]
    fn operator_managed_learner_keeps_its_recovery_endpoints_after_readiness_timeout() {
        let still_copying = || {
            RhizaLocalReadiness::StillCopying(anyhow::anyhow!(
                "native Rhiza local readiness is still false after 60s"
            ))
        };

        // A learner exists only to be promoted. Losing the process here would
        // remove exactly the endpoints the promotion needs.
        assert!(rhiza_local_readiness_gate(true, still_copying()).is_ok());
        assert!(rhiza_local_readiness_gate(true, RhizaLocalReadiness::Ready).is_ok());

        // A voter has no promotion path to protect and still fails closed.
        assert!(
            rhiza_local_readiness_gate(false, still_copying()).is_err(),
            "a voter must not serve on a local-readiness timeout"
        );

        // A failed probe is not evidence that a learner is still copying, so it
        // fails closed for a learner too instead of being swallowed.
        for learner in [true, false] {
            let rejected = RhizaLocalReadiness::Failed(anyhow::anyhow!(
                "Rhiza local readiness probe failed: Rhiza KV operation failed (invalid_request): malformed key"
            ));
            let error = rhiza_local_readiness_gate(learner, rejected)
                .expect_err("a failed local-readiness probe must not be tolerated");
            assert!(
                error.to_string().contains("invalid_request"),
                "the underlying native failure must survive the gate: {error}"
            );
        }
    }

    /// Promotion happens in place, so a learner that has no quorum yet has to
    /// keep re-probing through the same gate instead of parking, and that gate
    /// must not report ready until a quorum-backed read actually succeeds.
    #[tokio::test]
    async fn operator_managed_learner_reprobes_for_quorum_until_it_has_one() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Native's own shape for a learner that has no quorum yet.
        const PRE_QUORUM: &str = "Rhiza KV operation failed (quorum_unavailable): no leader";
        let pre_quorum_error = || MetaStoreError::Rhiza(PRE_QUORUM.to_string());
        let probe_delay = Duration::from_millis(1);

        // Promoted without a restart: the same process keeps the same
        // environment, and the quorum probe eventually succeeds.
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        await_meta_store_quorum(
            move || {
                let attempts = Arc::clone(&counter);
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(pre_quorum_error())
                    } else {
                        Ok(())
                    }
                }
            },
            probe_delay,
            probe_delay,
        )
        .await
        .expect("a promoted learner must reach quorum on this same process");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        // Still pre-quorum: the same gate keeps re-probing and never reports
        // ready, so `serve` cannot open a Velorix metadata listener here.
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let pre_quorum = await_meta_store_quorum(
            move || {
                let attempts = Arc::clone(&counter);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(pre_quorum_error())
                }
            },
            probe_delay,
            probe_delay,
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), pre_quorum)
                .await
                .is_err(),
            "a learner without a quorum must stay closed instead of reporting ready"
        );
        assert!(
            attempts.load(Ordering::SeqCst) > 2,
            "the pre-quorum learner must keep re-probing instead of parking: {}",
            attempts.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn operator_managed_rhiza_requires_membership_or_a_matching_learner() {
        let without_self = serde_json::json!([
        {"node_id":"node-b","url":"http://meta-b:9091","peer_url":"quic://meta-b:8200","public_key":NODE_B_PUBLIC_KEY},
        {"node_id":"node-c","url":"http://meta-c:9091","peer_url":"quic://meta-c:8200","public_key":NODE_C_PUBLIC_KEY},
        {"node_id":"node-d","url":"http://meta-d:9091","peer_url":"quic://meta-d:8200","public_key":NODE_A_PUBLIC_KEY},
    ])
    .to_string();
        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_CLUSTER_MEMBERS",
            without_self.as_str(),
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("RHIZA_CLUSTER_MEMBERS"));
    }

    #[test]
    fn operator_managed_rhiza_requires_the_production_three_voter_contract() {
        let two_members = serde_json::json!([
        {"node_id":"node-a","url":"http://meta-a:9091","peer_url":"quic://meta-a:8200","public_key":NODE_A_PUBLIC_KEY},
        {"node_id":"node-b","url":"http://meta-b:9091","peer_url":"quic://meta-b:8200","public_key":NODE_B_PUBLIC_KEY},
    ])
    .to_string();
        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_CLUSTER_MEMBERS",
            two_members.as_str(),
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("three unique voter nodes"));

        // `async` cannot produce a certified archive, so it cannot be recovered from.
        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_OBJSTORE_DURABILITY",
            "async",
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("before-ack"));

        // An empty admin token disables archive publication entirely.
        let error =
            parse_meta_serve_config(&operator_managed_rhiza_vars(&[("RHIZA_ADMIN_TOKEN", "")]))
                .unwrap_err();
        assert!(error.to_string().contains("nonempty RHIZA_ADMIN_TOKEN"));

        // Both or neither peer token form leaves native unable to select this node's
        // private identity.
        // Both forms at once, or neither, leaves native unable to select this
        // node's private identity.
        let error =
            parse_meta_serve_config(&operator_managed_rhiza_vars(&[("RHIZA_PEER_TOKEN", "")]))
                .unwrap_err();
        assert!(
            error.to_string().contains("mutually exclusive"),
            "unexpected error: {error}"
        );
        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[
            ("RHIZA_PEER_TOKEN", NODE_A_PEER_TOKEN),
            ("RHIZA_PEER_TOKENS", "{}"),
        ]))
        .unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));

        // The map must actually carry this node's entry, and nothing else.
        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[
            ("RHIZA_PEER_TOKEN", ""),
            ("RHIZA_PEER_TOKENS", "{}"),
        ]))
        .unwrap_err();
        assert!(error.to_string().contains("RHIZA_NODE_ID"));

        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[
            ("RHIZA_PEER_TOKEN", ""),
            ("RHIZA_PEER_TOKENS", r#"{"node-b":"velorix-peer-token-b"}"#),
        ]))
        .unwrap_err();
        assert!(error.to_string().contains("RHIZA_NODE_ID"));

        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[
            ("RHIZA_PEER_TOKEN", ""),
            ("RHIZA_PEER_TOKENS", "not-json"),
        ]))
        .unwrap_err();
        assert!(error.to_string().contains("invalid RHIZA_PEER_TOKENS JSON"));

        let peers = parse_meta_serve_config(&operator_managed_rhiza_vars(&[
            ("RHIZA_PEER_TOKEN", ""),
            ("RHIZA_PEER_TOKENS", r#"{"node-a":"velorix-peer-token-a"}"#),
        ]))
        .unwrap();
        assert!(peers.rhiza.is_some());

        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_ADMIN_TOKEN",
            NODE_A_PEER_TOKEN,
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("must differ"));

        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_OBJSTORE_PROVIDER",
            "",
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("RHIZA_OBJSTORE_PROVIDER"));

        let error =
            parse_meta_serve_config(&operator_managed_rhiza_vars(&[("RHIZA_CLUSTER_ID", "")]))
                .unwrap_err();
        assert!(error.to_string().contains("RHIZA_CLUSTER_ID"));

        let error =
            parse_meta_serve_config(&operator_managed_rhiza_vars(&[("RHIZA_DATA_DIR", "")]))
                .unwrap_err();
        assert!(error.to_string().contains("RHIZA_DATA_DIR"));

        let error =
            parse_meta_serve_config(&operator_managed_rhiza_vars(&[("RHIZA_PEER_ADDR", "")]))
                .unwrap_err();
        assert!(error.to_string().contains("RHIZA_PEER_ADDR"));
    }

    #[test]
    fn operator_managed_rhiza_rejects_a_shared_recovery_and_grpc_address() {
        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_BIND_ADDR",
            "0.0.0.0:9090",
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("separate addresses"));

        let error = parse_meta_serve_config(&operator_managed_rhiza_vars(&[(
            "RHIZA_BIND_ADDR",
            "not-an-address",
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("invalid RHIZA_BIND_ADDR"));
    }

    #[test]
    fn operator_managed_flag_rejects_anything_but_zero_or_one() {
        let error = parse_meta_serve_config(&production_rhiza_vars(&[(
            "VELORIX_RHIZA_OPERATOR_MANAGED",
            "yes",
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("exactly 0, 1, false, or true"));

        // `0` keeps the existing single-node development path untouched.
        let config = parse_meta_serve_config(&production_rhiza_vars(&[(
            "VELORIX_RHIZA_OPERATOR_MANAGED",
            "0",
        )]))
        .unwrap();
        assert!(expect_explicit(config.rhiza.clone()).members.len() == 3);
    }

    fn production_rhiza_vars(overrides: &[(&str, &str)]) -> HashMap<String, String> {
        let mut vars: HashMap<String, String> = [
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BACKEND", "rhiza-kv"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "native-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
            ("VELORIX_META_TLS_CERT_FILE", "/tmp/meta.crt"),
            ("VELORIX_META_TLS_KEY_FILE", "/tmp/meta.key"),
            ("VELORIX_META_TLS_CLIENT_CA_FILE", "/tmp/meta-ca.crt"),
            ("VELORIX_RHIZA_DATA_DIR", "/var/lib/velorix-meta"),
            ("VELORIX_RHIZA_NODE_ID", "node-a"),
            ("VELORIX_RHIZA_CLUSTER_ID", "velorix-meta"),
            ("VELORIX_RHIZA_BIND_ADDR", "0.0.0.0:9091"),
            ("VELORIX_RHIZA_PEER_ADDR", "0.0.0.0:9191"),
            ("VELORIX_RHIZA_MEMBERS_JSON", THREE_NODE_MEMBERS),
            ("VELORIX_RHIZA_PEER_TOKEN", NODE_A_PEER_TOKEN),
            ("VELORIX_RHIZA_OBJECT_STORE_PROVIDER", "s3"),
            ("VELORIX_RHIZA_OBJECT_STORE_BUCKET", "velorix-meta"),
            ("VELORIX_RHIZA_OBJECT_STORE_DURABILITY", "before-ack"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        for (name, value) in overrides {
            vars.insert(name.to_string(), value.to_string());
        }
        vars
    }

    #[test]
    fn development_rhiza_is_explicit_single_node_and_defaults_async() {
        let config = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "rhiza-kv"),
            ("VELORIX_RHIZA_DATA_DIR", "/tmp/velorix-rhiza-dev"),
            ("VELORIX_RHIZA_NODE_ID", "dev-1"),
            ("VELORIX_RHIZA_CLUSTER_ID", "velorix-dev"),
            ("VELORIX_RHIZA_BIND_ADDR", "127.0.0.1:9091"),
            ("VELORIX_RHIZA_PEER_ADDR", "127.0.0.1:9191"),
            (
                "VELORIX_RHIZA_MEMBERS_JSON",
                r#"[{"node_id":"dev-1","url":"http://127.0.0.1:9091","peer_url":"quic://127.0.0.1:9191"}]"#,
            ),
        ])
        .unwrap();

        assert_eq!(config.backend, MetaBackendKind::RhizaKv);
        let rhiza = expect_explicit(config.rhiza.clone());
        assert_eq!(rhiza.members.len(), 1);
        // A single-node cluster authenticates no peer, so it publishes no key
        // and this process holds no private peer token.
        assert!(rhiza.members[0].public_key.is_empty());
        assert!(rhiza.peer_token.is_none());
        assert_eq!(rhiza.object_store_durability, "async");
        assert!(rhiza.object_store_provider.is_none());
    }

    #[test]
    fn development_rhiza_rejects_multi_node_membership() {
        let members = format!(
            "[{},{}]",
            rhiza_member_json("dev-1", ""),
            rhiza_member_json("dev-2", NODE_B_PUBLIC_KEY)
        );
        let error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "development"),
            ("VELORIX_META_BACKEND", "rhiza-kv"),
            ("VELORIX_RHIZA_DATA_DIR", "/tmp/velorix-rhiza-dev"),
            ("VELORIX_RHIZA_NODE_ID", "dev-1"),
            ("VELORIX_RHIZA_CLUSTER_ID", "velorix-dev"),
            ("VELORIX_RHIZA_BIND_ADDR", "127.0.0.1:9091"),
            ("VELORIX_RHIZA_PEER_ADDR", "127.0.0.1:9191"),
            ("VELORIX_RHIZA_MEMBERS_JSON", members.as_str()),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("single-node"));
    }

    #[test]
    fn production_rhiza_requires_three_voters_before_ack_and_object_storage() {
        let error = parse_meta_serve_config(&production_rhiza_vars(&[
            ("VELORIX_RHIZA_OBJECT_STORE_PROVIDER", ""),
            ("VELORIX_RHIZA_OBJECT_STORE_BUCKET", ""),
        ]))
        .unwrap_err();
        assert!(error.to_string().contains("durable object storage"));

        let error = parse_meta_serve_config(&production_rhiza_vars(&[(
            "VELORIX_RHIZA_OBJECT_STORE_DURABILITY",
            "async",
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("before-ack"));

        // The published membership is accepted as-is: public keys only, no
        // per-member token, and a local peer token selected by this process.
        let config = parse_meta_serve_config(&production_rhiza_vars(&[])).unwrap();
        let rhiza = expect_explicit(config.rhiza.clone());
        assert_eq!(rhiza.members.len(), 3);
        assert_eq!(rhiza.members[0].public_key, NODE_A_PUBLIC_KEY);
        assert_eq!(rhiza.members[1].public_key, NODE_B_PUBLIC_KEY);
        assert_eq!(rhiza.members[2].public_key, NODE_C_PUBLIC_KEY);
        assert_eq!(rhiza.peer_token.as_deref(), Some(NODE_A_PEER_TOKEN));
    }

    #[test]
    fn multi_node_rhiza_requires_published_keys_and_one_local_peer_token() {
        let missing_key = rhiza_member_pair_json("");
        let error = parse_meta_serve_config(&production_rhiza_vars(&[(
            "VELORIX_RHIZA_MEMBERS_JSON",
            missing_key.as_str(),
        )]))
        .unwrap_err();
        assert!(
            error.to_string().contains("must publish a public_key"),
            "unexpected error: {error}"
        );

        let error =
            parse_meta_serve_config(&production_rhiza_vars(&[("VELORIX_RHIZA_PEER_TOKEN", "")]))
                .unwrap_err();
        assert!(
            error.to_string().contains("private peer identity token"),
            "unexpected error: {error}"
        );

        let secrets = tempfile::tempdir().expect("peer token directory");
        let mut from_file = production_rhiza_vars(&[("VELORIX_RHIZA_PEER_TOKEN", "")]);
        from_file.insert(
            "VELORIX_RHIZA_PEER_TOKEN_FILE".to_string(),
            write_peer_token_file(secrets.path(), NODE_A_PEER_TOKEN),
        );
        let config = parse_meta_serve_config(&from_file).unwrap();
        let rhiza = expect_explicit(config.rhiza.clone());
        // A mounted Secret's trailing newline is not part of the token.
        assert_eq!(rhiza.peer_token.as_deref(), Some(NODE_A_PEER_TOKEN));

        // Both sources at once must fail closed rather than silently picking one.
        let mut both_sources = from_file.clone();
        both_sources.insert(
            "VELORIX_RHIZA_PEER_TOKEN".to_string(),
            NODE_A_PEER_TOKEN.to_string(),
        );
        let error = parse_meta_serve_config(&both_sources).unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));
        assert!(error.to_string().contains("VELORIX_RHIZA_PEER_TOKEN_FILE"));

        let empty_secret = tempfile::tempdir().expect("empty peer token directory");
        let mut empty = production_rhiza_vars(&[("VELORIX_RHIZA_PEER_TOKEN", "")]);
        empty.insert(
            "VELORIX_RHIZA_PEER_TOKEN_FILE".to_string(),
            write_peer_token_file(empty_secret.path(), ""),
        );
        let error = parse_meta_serve_config(&empty).unwrap_err();
        assert!(
            error.to_string().contains("nonempty peer token"),
            "unexpected error: {error}"
        );

        let shared_admin = parse_meta_serve_config(&production_rhiza_vars(&[(
            "VELORIX_RHIZA_ADMIN_TOKEN",
            NODE_A_PEER_TOKEN,
        )]))
        .unwrap_err();
        assert!(
            shared_admin
                .to_string()
                .contains("must differ from this node's peer identity token"),
            "unexpected error: {shared_admin}"
        );
    }

    fn write_peer_token_file(directory: &std::path::Path, token: &str) -> String {
        let path = directory.join("peer-token");
        // A mounted Secret conventionally carries a trailing newline.
        std::fs::write(&path, format!("{token}\n")).expect("peer token file");
        path.display().to_string()
    }

    #[test]
    fn rhiza_members_reject_duplicate_or_malformed_entries() {
        let duplicate = parse_rhiza_members_json(&format!(
            "[{},{}]",
            rhiza_member_json("node-a", NODE_A_PUBLIC_KEY),
            rhiza_member_json("node-a", NODE_B_PUBLIC_KEY)
        ))
        .unwrap_err();
        assert!(duplicate.to_string().contains("unique node IDs"));

        let malformed = parse_rhiza_members_json(
            r#"[{"node_id":"node-a","url":"http://a","peer_url":"tcp://a"}]"#,
        )
        .unwrap_err();
        assert!(malformed.to_string().contains("peer_url must use quic://"));
    }

    #[test]
    fn rhiza_members_reject_legacy_token_and_invalid_public_keys() {
        let legacy = parse_rhiza_members_json(
            r#"[{"node_id":"node-a","url":"http://a","peer_url":"quic://a","token":"legacy"}]"#,
        )
        .unwrap_err();
        let legacy = legacy.to_string();
        assert!(
            legacy.contains("unknown field") && legacy.contains("token"),
            "legacy member token must be rejected by serde: {legacy}"
        );

        // `log_url` and `wal_identity` are legal native `quepaxa.Member`
        // fields, so the Operator's own documents must decode. Anything outside
        // the native schema is still refused, so this does not become a second,
        // Velorix-only spelling of a member.
        let native_shape = parse_rhiza_members_json(
            r#"[{"node_id":"node-a","url":"http://a","peer_url":"quic://a","log_url":"http://a:8080/log","public_key":"p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st72k=","wal_identity":"deadbeef"}]"#,
        )
        .expect("the native member schema must decode");
        assert_eq!(native_shape[0].log_url, "http://a:8080/log");
        assert_eq!(native_shape[0].wal_identity.as_deref(), Some("deadbeef"));

        let wal_identity = parse_rhiza_members_json(
            r#"[{"node_id":"node-a","url":"http://a","peer_url":"quic://a","snapshot_url":"x"}]"#,
        )
        .unwrap_err();
        assert!(wal_identity.to_string().contains("unknown field"));

        // Short, unpadded, wrong-length, and URL-safe (the alphabet Rhiza uses
        // for peer tokens) keys are all refused before the native DB is opened.
        for rejected in [
            "p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st72k",
            "p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st72k=A",
            "p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st7=",
            "p1Pr7TD2f8ccrYFAHg9m-fGpIwTJLXq9TeL1Q2st72k=",
        ] {
            let error = parse_rhiza_members_json(&rhiza_member_pair_json(rejected))
                .expect_err("invalid public_key must be rejected");
            assert!(
                error.to_string().contains("standard padded base64"),
                "unexpected error for {rejected}: {error}"
            );
        }

        // Native reads the all-zero key as "no key published", so accepting it
        // would turn a real identity into a silent no-op.
        let all_zero = parse_rhiza_members_json(&rhiza_member_pair_json(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        ))
        .expect_err("the all-zero public key must be rejected");
        assert!(
            all_zero.to_string().contains("all-zero"),
            "unexpected error: {all_zero}"
        );

        let non_string = parse_rhiza_members_json(
            r#"[{"node_id":"node-a","url":"http://a","peer_url":"quic://a","public_key":["A"]}]"#,
        )
        .unwrap_err();
        assert!(non_string
            .to_string()
            .contains("invalid Rhiza membership JSON"));

        let duplicate_keys = parse_rhiza_members_json(&format!(
            "[{},{}]",
            rhiza_member_json("node-a", NODE_A_PUBLIC_KEY),
            rhiza_member_json("node-b", NODE_A_PUBLIC_KEY)
        ))
        .unwrap_err();
        assert!(duplicate_keys
            .to_string()
            .contains("unique voter public keys"));

        let decoded = decode_rhiza_public_key(NODE_A_PUBLIC_KEY)
            .expect("upstream fixture must decode to 32 bytes");
        assert_eq!(decoded.len(), 32);
        assert_ne!(decoded, [0_u8; 32]);
    }

    /// The decoder must not accept a second spelling of an otherwise valid key.
    /// A lenient group reader accepts a 43-character unpadded string and a
    /// final symbol with non-zero trailing bits, both of which yield the same
    /// 32 bytes as the canonical form. Two members could then publish distinct
    /// strings for one identity, or native could disagree with us about which
    /// bytes a key names. `STANDARD` rejects both, and this pins that.
    #[test]
    fn rhiza_public_key_decoder_rejects_noncanonical_base64() {
        // 32 bytes fit in 43 unpadded characters, so length alone cannot reject
        // this one; only canonical padding can.
        let unpadded = &NODE_A_PUBLIC_KEY[..43];
        assert_eq!(unpadded.len(), 43);
        assert!(decode_rhiza_public_key(unpadded).is_none());
        assert!(decode_rhiza_public_key(&format!("{unpadded}=")).is_some());

        // 'l' is one above 'k', so it carries non-zero bits in the two
        // positions the final group discards. A lenient decoder maps it to the
        // same key as NODE_A_PUBLIC_KEY.
        let noncanonical = "p1Pr7TD2f8ccrYFAHg9m+fGpIwTJLXq9TeL1Q2st72l=";
        assert_ne!(noncanonical, NODE_A_PUBLIC_KEY);
        assert!(decode_rhiza_public_key(noncanonical).is_none());
    }

    #[test]
    fn rhiza_config_debug_never_reveals_tokens_or_keys() {
        let mut vars =
            production_rhiza_vars(&[("VELORIX_RHIZA_ADMIN_TOKEN", "admin-secret-value")]);
        vars.insert(
            "VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY".to_string(),
            "object-store-secret-value".to_string(),
        );
        let config = parse_meta_serve_config(&vars).unwrap();
        let rendered = format!("{:?}", config);

        for secret in [
            NODE_A_PEER_TOKEN,
            NODE_A_PUBLIC_KEY,
            "admin-secret-value",
            "object-store-secret-value",
        ] {
            assert!(
                !rendered.contains(secret),
                "debug output must not contain {secret}: {rendered}"
            );
        }
        assert!(rendered.contains("[redacted]"));
        assert!(rendered.contains("node-a"));
    }

    #[test]
    fn production_config_rejects_missing_durable_backend_and_memory_backend() {
        let missing_backend_error = parse_meta_serve_config_from_pairs([
            ("VELORIX_META_MODE", "production"),
            ("VELORIX_META_BIND", "0.0.0.0:9090"),
            ("VELORIX_META_BEARER_TOKEN", "secret"),
            ("VELORIX_META_TRANSPORT_SECURITY", "native-mtls"),
            (
                "VELORIX_META_TRANSPORT_SECURITY_ATTESTATION",
                "mesh-policy/velorix-meta",
            ),
            ("VELORIX_META_TLS_CERT_FILE", "/tmp/meta.crt"),
            ("VELORIX_META_TLS_KEY_FILE", "/tmp/meta.key"),
            ("VELORIX_META_TLS_CLIENT_CA_FILE", "/tmp/meta-ca.crt"),
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
    fn production_config_requires_auth_and_native_transport_security() {
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
            .contains("native-mtls"));

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
            .contains("native-mtls"));
    }

    #[test]
    fn smoke_relation_catalog_uses_probe_id_as_version() {
        let catalog = smoke_relation_catalog("abc").unwrap();

        assert_eq!(catalog.relation_schema.relation_id, "velorix_meta_smoke");
        assert_eq!(catalog.relation_schema.relation_version, "smoke-abc");
        catalog.validate().unwrap();
    }
}

#![forbid(unsafe_code)]

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::Url;
use velorix_feldera_compiler_worker::{
    run_once, JarlessProductRuntimeConfig, WorkerBackendKind, WorkerConfig,
};

#[derive(Debug, Parser)]
#[command(name = "velorix-feldera-compiler-worker")]
#[command(about = "Velorix Feldera compile/deploy control-plane worker")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Once {
        #[arg(long, env = "VELORIX_API_URL")]
        api_url: Url,
        #[arg(long, env = "VELORIX_ADMIN_AUTH_HEADER")]
        admin_auth_header: Option<String>,
        #[arg(long, env = "VELORIX_ADMIN_BEARER_TOKEN")]
        admin_bearer_token: Option<String>,
        #[arg(long, env = "VELORIX_FELDERA_COMPILER_WORKER_ID")]
        worker_id: String,
        #[arg(
            long,
            env = "VELORIX_FELDERA_COMPILER_WORKER_LEASE_MS",
            default_value_t = 300_000
        )]
        lease_duration_ms: u64,
        #[arg(
            long,
            env = "VELORIX_FELDERA_COMPILER_WORKER_MAX_CLAIMS",
            default_value_t = 1
        )]
        max_claims: usize,
        #[arg(
            long,
            env = "VELORIX_FELDERA_COMPILER_WORKER_REQUEST_TIMEOUT_MS",
            default_value_t = 30_000
        )]
        request_timeout_ms: u64,
        #[arg(long, env = "VELORIX_FELDERA_PIPELINE_MANAGER_URL")]
        feldera_pipeline_manager_url: Option<Url>,
        #[arg(long, env = "VELORIX_FELDERA_BEARER_TOKEN")]
        feldera_bearer_token: Option<String>,
        #[arg(long, env = "VELORIX_FELDERA_COMPILER_PROFILE", default_value = "dev")]
        feldera_program_profile: String,
        #[arg(long, env = "VELORIX_FELDERA_COMPILER_WORKERS", default_value_t = 1)]
        feldera_pipeline_workers: u32,
        #[arg(
            long,
            env = "VELORIX_FELDERA_COMPILER_POLL_INTERVAL_MS",
            default_value_t = 1_000
        )]
        feldera_poll_interval_ms: u64,
        #[arg(
            long,
            env = "VELORIX_FELDERA_COMPILER_TIMEOUT_MS",
            default_value_t = 3_600_000
        )]
        feldera_poll_timeout_ms: u64,
        #[arg(long, env = "VELORIX_FELDERA_COMPILER_CLAIM_WITHOUT_BACKEND")]
        claim_without_backend: bool,
        #[arg(
            long,
            env = "VELORIX_FELDERA_COMPILER_BACKEND",
            default_value = "feldera-package-jarless"
        )]
        compiler_backend: CliCompilerBackend,
        #[arg(long, env = "VELORIX_FELDERA_PACKAGE_RUNTIME_CRATE_NAME")]
        jarless_runtime_crate_name: Option<String>,
        #[arg(long, env = "VELORIX_FELDERA_PACKAGE_RUNTIME_CRATE_VERSION")]
        jarless_runtime_crate_version: Option<String>,
        #[arg(
            long,
            env = "VELORIX_FELDERA_PACKAGE_RUNTIME_FACTORY_SYMBOL",
            default_value = "create_standing_runtime"
        )]
        jarless_runtime_factory_symbol: String,
        #[arg(
            long,
            env = "VELORIX_FELDERA_PACKAGE_RUNTIME_STATE_CODEC",
            default_value = "feldera-package-runtime-state-v1"
        )]
        jarless_runtime_state_codec: String,
        #[arg(
            long,
            env = "VELORIX_FELDERA_PACKAGE_RUNTIME_STATE_SCHEMA_VERSION",
            default_value_t = 1
        )]
        jarless_runtime_state_schema_version: u32,
        #[arg(
            long,
            env = "VELORIX_FELDERA_PACKAGE_BACKEND_VERSION",
            default_value = env!("CARGO_PKG_VERSION")
        )]
        jarless_backend_version: String,
        #[arg(
            long,
            env = "VELORIX_FELDERA_PACKAGE_BACKEND_SOURCE",
            default_value = "feldera public Rust packages"
        )]
        jarless_backend_source: String,
    },
}

#[derive(Clone, Debug, ValueEnum)]
enum CliCompilerBackend {
    FelderaPackageJarless,
    CompatibilityPipelineManager,
}

impl From<CliCompilerBackend> for WorkerBackendKind {
    fn from(value: CliCompilerBackend) -> Self {
        match value {
            CliCompilerBackend::FelderaPackageJarless => Self::FelderaPackageJarless,
            CliCompilerBackend::CompatibilityPipelineManager => Self::CompatibilityPipelineManager,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Once {
            api_url,
            admin_auth_header,
            admin_bearer_token,
            worker_id,
            lease_duration_ms,
            max_claims,
            request_timeout_ms,
            feldera_pipeline_manager_url,
            feldera_bearer_token,
            feldera_program_profile,
            feldera_pipeline_workers,
            feldera_poll_interval_ms,
            feldera_poll_timeout_ms,
            claim_without_backend,
            compiler_backend,
            jarless_runtime_crate_name,
            jarless_runtime_crate_version,
            jarless_runtime_factory_symbol,
            jarless_runtime_state_codec,
            jarless_runtime_state_schema_version,
            jarless_backend_version,
            jarless_backend_source,
        } => {
            let admin_auth_header = admin_auth_header
                .or_else(|| {
                    admin_bearer_token.map(|token| format!("authorization: Bearer {token}"))
                })
                .context("set VELORIX_ADMIN_AUTH_HEADER or VELORIX_ADMIN_BEARER_TOKEN")?;
            let jarless_product_runtime = match (
                jarless_runtime_crate_name,
                jarless_runtime_crate_version,
            ) {
                (Some(runtime_crate_name), Some(runtime_crate_version)) => {
                    Some(JarlessProductRuntimeConfig {
                        backend_version: jarless_backend_version,
                        backend_source: jarless_backend_source,
                        runtime_crate_name,
                        runtime_crate_version,
                        runtime_factory_symbol: jarless_runtime_factory_symbol,
                        state_codec: jarless_runtime_state_codec,
                        state_schema_version: jarless_runtime_state_schema_version,
                    })
                }
                (None, None) => None,
                _ => anyhow::bail!(
                    "set both VELORIX_FELDERA_PACKAGE_RUNTIME_CRATE_NAME and VELORIX_FELDERA_PACKAGE_RUNTIME_CRATE_VERSION to enable jarless product runtime completion"
                ),
            };
            let report = run_once(WorkerConfig {
                api_url,
                admin_auth_header,
                worker_id,
                lease_duration_ms,
                max_claims,
                request_timeout_ms,
                feldera_pipeline_manager_url,
                feldera_bearer_token,
                feldera_program_profile,
                feldera_pipeline_workers,
                feldera_poll_interval_ms,
                feldera_poll_timeout_ms,
                claim_without_backend,
                backend_kind: compiler_backend.into(),
                jarless_product_runtime,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

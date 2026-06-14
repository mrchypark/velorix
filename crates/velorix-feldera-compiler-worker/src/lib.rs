#![forbid(unsafe_code)]

use std::time::Duration;

use anyhow::{bail, Context};
use reqwest::{header, Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use velorix_core::{
    feldera_artifact::{
        feldera_output_schemas_from_program_info, feldera_pipeline_manager_sql_compile_request,
        feldera_pipeline_name_for_parts, feldera_sql_program_for_compile_request,
        standing_view_spec_for_compile_request, FelderaCompileRequestV1,
        FelderaPipelineManagerRuntimeDeployment, FelderaPipelineManagerRuntimeDeploymentMode,
        FelderaRustExtensionV1, OutputSchemaContract, RelationSchema, SqlDialect, SqlSourceKind,
        StandingViewShape, StandingViewSpec,
    },
    feldera_product_runtime::{
        build_feldera_package_runtime_descriptor, BuildFelderaPackageRuntimeDescriptorRequest,
        FelderaPackageBackendIdentity, FelderaPackageRuntimeDescriptorV1,
        FelderaPackageRuntimeFactoryBinding,
    },
    feldera_program_descriptor::{
        feldera_program_schema_for_standing_view_spec, FelderaProgramDescriptor,
    },
    relation::VelorixRelationCatalogV1,
};

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub api_url: Url,
    pub admin_auth_header: String,
    pub worker_id: String,
    pub lease_duration_ms: u64,
    pub max_claims: usize,
    pub request_timeout_ms: u64,
    pub feldera_pipeline_manager_url: Option<Url>,
    pub feldera_bearer_token: Option<String>,
    pub feldera_program_profile: String,
    pub feldera_pipeline_workers: u32,
    pub feldera_poll_interval_ms: u64,
    pub feldera_poll_timeout_ms: u64,
    pub claim_without_backend: bool,
    pub backend_kind: WorkerBackendKind,
    pub jarless_product_runtime: Option<JarlessProductRuntimeConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JarlessProductRuntimeConfig {
    pub backend_version: String,
    pub backend_source: String,
    pub runtime_crate_name: String,
    pub runtime_crate_version: String,
    pub runtime_factory_symbol: String,
    pub state_codec: String,
    pub state_schema_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerBackendKind {
    FelderaPackageJarless,
    CompatibilityPipelineManager,
}

impl WorkerBackendKind {
    fn label(&self) -> &'static str {
        match self {
            Self::FelderaPackageJarless => "feldera-package-jarless",
            Self::CompatibilityPipelineManager => "compatibility-pipeline-manager",
        }
    }

    fn unavailable_reason(&self) -> &'static str {
        match self {
            Self::FelderaPackageJarless => {
                "jarless Feldera Rust-package backend is not implemented for SQL compilation yet"
            }
            Self::CompatibilityPipelineManager => {
                "Feldera pipeline-manager backend is not configured"
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ViewCompileDeployJobCatalogResponse {
    pub pending_jobs: usize,
    #[serde(default)]
    pub jobs: Vec<ViewCompileDeployJobSummary>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ViewCompileDeployJobSummary {
    pub job_id: String,
    pub view_id: String,
    #[serde(default)]
    pub spec_hash: Option<String>,
    #[serde(default)]
    pub compiler_backend: Option<String>,
    #[serde(default)]
    pub compile_status: Option<String>,
    #[serde(default)]
    pub deployment_status: Option<String>,
    #[serde(default)]
    pub compiler_request: Option<Value>,
    #[serde(default)]
    pub input_relation_catalogs: Vec<VelorixRelationCatalogV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewCompileDeployCompilerRequestV1 {
    pub request_kind: String,
    pub view_id: String,
    pub compile_request_hash: String,
    pub spec_hash: String,
    pub sql: String,
    pub dialect: SqlDialect,
    pub source_kind: SqlSourceKind,
    #[serde(default, skip_serializing_if = "FelderaRustExtensionV1::is_empty")]
    pub rust_extension: FelderaRustExtensionV1,
    pub input_relations: Vec<RelationSchema>,
    pub output_contract: OutputSchemaContract,
    pub output_relations: Vec<RelationSchema>,
    pub shape: StandingViewShape,
}

impl ViewCompileDeployCompilerRequestV1 {
    fn feldera_compile_request(&self) -> FelderaCompileRequestV1 {
        FelderaCompileRequestV1 {
            view_id: self.view_id.clone(),
            sql: self.sql.clone(),
            dialect: self.dialect.clone(),
            source_kind: self.source_kind.clone(),
            rust_extension: self.rust_extension.clone(),
            input_relations: self.input_relations.clone(),
            output_contract: self.output_contract.clone(),
            shape: self.shape.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClaimViewCompileDeployJobResponse {
    pub claim_status: ClaimStatus,
    pub tenant_id: String,
    pub view_id: String,
    pub job_generation: u64,
    pub compile_request_hash: String,
    pub worker_id: String,
    pub lease_id: String,
    pub fencing_token: u64,
    pub claimed_at_ms: u64,
    pub lease_expires_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Claimed,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ClaimSummary {
    pub claim_status: ClaimStatus,
    pub tenant_id: String,
    pub view_id: String,
    pub job_generation: u64,
    pub compile_request_hash: String,
    pub worker_id: String,
    pub lease_expires_at_ms: u64,
}

impl From<&ClaimViewCompileDeployJobResponse> for ClaimSummary {
    fn from(claim: &ClaimViewCompileDeployJobResponse) -> Self {
        Self {
            claim_status: claim.claim_status.clone(),
            tenant_id: claim.tenant_id.clone(),
            view_id: claim.view_id.clone(),
            job_generation: claim.job_generation,
            compile_request_hash: claim.compile_request_hash.clone(),
            worker_id: claim.worker_id.clone(),
            lease_expires_at_ms: claim.lease_expires_at_ms,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct WorkerRunReport {
    pub pending_jobs: usize,
    pub claimed: usize,
    pub duplicate_claims: usize,
    pub skipped: usize,
    pub failed: usize,
    pub outcomes: Vec<WorkerJobOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkerJobOutcome {
    pub job_id: String,
    pub view_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim: Option<ClaimSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
enum CompileOutcome {
    Unsupported {
        reason: String,
        requires_java_sql_compiler: bool,
    },
    SchemaOnly {
        resolved_spec: StandingViewSpec,
        descriptor_identity: String,
    },
    ProductRuntime {
        resolved_spec: StandingViewSpec,
        descriptor: FelderaPackageRuntimeDescriptorV1,
    },
    CompatibilityRuntime {
        resolved_spec: StandingViewSpec,
        deployment: FelderaPipelineManagerRuntimeDeployment,
    },
}

pub async fn run_once(config: WorkerConfig) -> anyhow::Result<WorkerRunReport> {
    if config.worker_id.trim().is_empty() {
        bail!("worker_id must not be blank");
    }
    if config.lease_duration_ms == 0 {
        bail!("lease_duration_ms must be greater than zero");
    }
    if config.feldera_pipeline_manager_url.is_some()
        && config.backend_kind != WorkerBackendKind::CompatibilityPipelineManager
    {
        bail!(
            "Feldera pipeline-manager URL was supplied, but compiler backend is {}; set compiler backend to compatibility-pipeline-manager to use the JAR-backed compatibility fixture",
            config.backend_kind.label()
        );
    }
    if config.backend_kind == WorkerBackendKind::CompatibilityPipelineManager {
        if config.feldera_pipeline_manager_url.is_none() && !config.claim_without_backend {
            bail!("compatibility-pipeline-manager backend requires feldera_pipeline_manager_url");
        }
        if config.feldera_pipeline_manager_url.is_some() {
            if config.feldera_program_profile.trim().is_empty() {
                bail!("feldera_program_profile must not be blank when Feldera pipeline-manager backend is configured");
            }
            if config.feldera_pipeline_workers == 0 {
                bail!("feldera_pipeline_workers must be greater than zero");
            }
            if config.feldera_poll_interval_ms == 0 {
                bail!("feldera_poll_interval_ms must be greater than zero");
            }
            if config.feldera_poll_timeout_ms < config.feldera_poll_interval_ms {
                bail!(
                    "feldera_poll_timeout_ms must be greater than or equal to feldera_poll_interval_ms"
                );
            }
        }
    }

    let client = Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms.max(1)))
        .build()
        .context("build worker HTTP client")?;
    let auth_header = parse_auth_header(&config.admin_auth_header)?;
    let catalog: ViewCompileDeployJobCatalogResponse = client
        .get(api_url(&config.api_url, "/v1/view-compile-deploy/jobs")?)
        .header(auth_header.0.clone(), auth_header.1.clone())
        .send()
        .await
        .context("list compile/deploy jobs")?
        .error_for_status()
        .context("list compile/deploy jobs returned non-success status")?
        .json()
        .await
        .context("decode compile/deploy job catalog")?;

    let mut report = WorkerRunReport {
        pending_jobs: catalog.pending_jobs,
        ..WorkerRunReport::default()
    };
    let claim_limit = if config.max_claims == 0 {
        catalog.jobs.len()
    } else {
        config.max_claims.min(catalog.jobs.len())
    };

    for job in catalog.jobs.into_iter().take(claim_limit) {
        if !config.backend_ready() && !config.claim_without_backend {
            report.skipped += 1;
            report.outcomes.push(WorkerJobOutcome {
                job_id: job.job_id,
                view_id: job.view_id,
                status: "backend_not_configured".to_string(),
                reason: Some(format!(
                    "{}; refusing to claim without --claim-without-backend",
                    config.backend_kind.unavailable_reason()
                )),
                claim: None,
            });
            continue;
        }
        let claim_url = api_url(
            &config.api_url,
            &format!("/v1/view-compile-deploy/jobs/{}/claim", job.view_id),
        )?;
        let response = client
            .post(claim_url)
            .header(auth_header.0.clone(), auth_header.1.clone())
            .json(&serde_json::json!({
                "worker_id": config.worker_id,
                "lease_duration_ms": config.lease_duration_ms,
            }))
            .send()
            .await;
        match response {
            Ok(response) => {
                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    report.failed += 1;
                    report.outcomes.push(WorkerJobOutcome {
                        job_id: job.job_id,
                        view_id: job.view_id,
                        status: "claim_failed".to_string(),
                        reason: Some(if body.trim().is_empty() {
                            format!("claim returned HTTP {status}")
                        } else {
                            format!("claim returned HTTP {status}: {body}")
                        }),
                        claim: None,
                    });
                    continue;
                }
                let claim = response
                    .json::<ClaimViewCompileDeployJobResponse>()
                    .await
                    .context("decode claim response")?;
                validate_claim_for_job(&config, &job, &claim)?;
                match &claim.claim_status {
                    ClaimStatus::Claimed => report.claimed += 1,
                    ClaimStatus::Duplicate => report.duplicate_claims += 1,
                }
                if !config.backend_ready() {
                    report.outcomes.push(WorkerJobOutcome {
                        job_id: job.job_id,
                        view_id: job.view_id,
                        status: "claimed_not_compiled".to_string(),
                        reason: Some(config.backend_kind.unavailable_reason().to_string()),
                        claim: Some(ClaimSummary::from(&claim)),
                    });
                    continue;
                }
                if claim.claim_status != ClaimStatus::Claimed {
                    report.outcomes.push(WorkerJobOutcome {
                        job_id: job.job_id,
                        view_id: job.view_id,
                        status: "duplicate_claim_not_compiled".to_string(),
                        reason: Some(
                            "duplicate claim is not compile-eligible in this worker".to_string(),
                        ),
                        claim: Some(ClaimSummary::from(&claim)),
                    });
                    continue;
                }
                match compile_with_selected_backend(&client, &config, &job, &claim).await {
                    Ok(CompileOutcome::Unsupported {
                        reason,
                        requires_java_sql_compiler,
                    }) => report.outcomes.push(WorkerJobOutcome {
                        job_id: job.job_id,
                        view_id: job.view_id,
                        status: "unsupported_by_selected_backend".to_string(),
                        reason: Some(if requires_java_sql_compiler {
                            format!("{reason}; requires_java_sql_compiler=true")
                        } else {
                            reason
                        }),
                        claim: Some(ClaimSummary::from(&claim)),
                    }),
                    Ok(CompileOutcome::SchemaOnly {
                        resolved_spec,
                        descriptor_identity,
                    }) => report.outcomes.push(WorkerJobOutcome {
                        job_id: job.job_id,
                        view_id: job.view_id,
                        status: "compiled_schema_only_not_deployed".to_string(),
                        reason: Some(format!(
                            "resolved_spec_view_id={}; descriptor_identity={}",
                            resolved_spec.view_id, descriptor_identity
                        )),
                        claim: Some(ClaimSummary::from(&claim)),
                    }),
                    Ok(CompileOutcome::ProductRuntime {
                        resolved_spec,
                        descriptor,
                    }) => {
                        match complete_product_runtime_job(
                            &client,
                            &auth_header,
                            &config,
                            &claim,
                            resolved_spec,
                            descriptor,
                        )
                        .await
                        {
                            Ok(()) => report.outcomes.push(WorkerJobOutcome {
                                job_id: job.job_id,
                                view_id: job.view_id,
                                status: "completed_product_runtime_deployment".to_string(),
                                reason: None,
                                claim: Some(ClaimSummary::from(&claim)),
                            }),
                            Err(source) => {
                                report.failed += 1;
                                report.outcomes.push(WorkerJobOutcome {
                                    job_id: job.job_id,
                                    view_id: job.view_id,
                                    status: "compile_complete_failed".to_string(),
                                    reason: Some(source.to_string()),
                                    claim: Some(ClaimSummary::from(&claim)),
                                });
                            }
                        }
                    }
                    Ok(CompileOutcome::CompatibilityRuntime {
                        resolved_spec,
                        deployment,
                    }) => {
                        match complete_compatibility_runtime_job(
                            &client,
                            &auth_header,
                            &config,
                            &claim,
                            resolved_spec,
                            deployment,
                        )
                        .await
                        {
                            Ok(()) => report.outcomes.push(WorkerJobOutcome {
                                job_id: job.job_id,
                                view_id: job.view_id,
                                status: "completed_compatibility_runtime_deployment".to_string(),
                                reason: None,
                                claim: Some(ClaimSummary::from(&claim)),
                            }),
                            Err(source) => {
                                report.failed += 1;
                                report.outcomes.push(WorkerJobOutcome {
                                    job_id: job.job_id,
                                    view_id: job.view_id,
                                    status: "compile_complete_failed".to_string(),
                                    reason: Some(source.to_string()),
                                    claim: Some(ClaimSummary::from(&claim)),
                                });
                            }
                        }
                    }
                    Err(source) => {
                        report.failed += 1;
                        report.outcomes.push(WorkerJobOutcome {
                            job_id: job.job_id,
                            view_id: job.view_id,
                            status: "compile_complete_failed".to_string(),
                            reason: Some(source.to_string()),
                            claim: Some(ClaimSummary::from(&claim)),
                        });
                    }
                }
            }
            Err(source) => {
                report.failed += 1;
                report.outcomes.push(WorkerJobOutcome {
                    job_id: job.job_id,
                    view_id: job.view_id,
                    status: "claim_failed".to_string(),
                    reason: Some(source.to_string()),
                    claim: None,
                });
            }
        }
    }
    if claim_limit < report.pending_jobs {
        report.skipped += report.pending_jobs - claim_limit;
    }
    Ok(report)
}

impl WorkerConfig {
    fn backend_ready(&self) -> bool {
        match self.backend_kind {
            WorkerBackendKind::FelderaPackageJarless => true,
            WorkerBackendKind::CompatibilityPipelineManager => {
                self.feldera_pipeline_manager_url.is_some()
            }
        }
    }
}

fn validate_claim_for_job(
    config: &WorkerConfig,
    job: &ViewCompileDeployJobSummary,
    claim: &ClaimViewCompileDeployJobResponse,
) -> anyhow::Result<()> {
    if claim.tenant_id.trim().is_empty() {
        bail!("claim tenant_id must not be blank");
    }
    if claim.view_id != job.view_id {
        bail!(
            "claim view_id does not match job: claim={}, job={}",
            claim.view_id,
            job.view_id
        );
    }
    if claim.worker_id != config.worker_id {
        bail!(
            "claim worker_id does not match configured worker: claim={}, configured={}",
            claim.worker_id,
            config.worker_id
        );
    }
    if claim.compile_request_hash.trim().is_empty() {
        bail!("claim compile_request_hash must not be blank");
    }
    if claim.lease_id.trim().is_empty() {
        bail!("claim lease_id must not be blank");
    }
    if claim.fencing_token == 0 {
        bail!("claim fencing_token must be greater than zero");
    }
    Ok(())
}

async fn compile_with_selected_backend(
    client: &Client,
    config: &WorkerConfig,
    job: &ViewCompileDeployJobSummary,
    claim: &ClaimViewCompileDeployJobResponse,
) -> anyhow::Result<CompileOutcome> {
    match config.backend_kind {
        WorkerBackendKind::FelderaPackageJarless => {
            compile_with_jarless_package_backend(job, claim, config.jarless_product_runtime.as_ref())
        }
        WorkerBackendKind::CompatibilityPipelineManager => {
            compile_with_pipeline_manager_compatibility_backend(client, config, job, claim).await
        }
    }
}

fn compile_with_jarless_package_backend(
    job: &ViewCompileDeployJobSummary,
    claim: &ClaimViewCompileDeployJobResponse,
    product_runtime_config: Option<&JarlessProductRuntimeConfig>,
) -> anyhow::Result<CompileOutcome> {
    let compiler_request: ViewCompileDeployCompilerRequestV1 = serde_json::from_value(
        job.compiler_request
            .clone()
            .context("compile/deploy job is missing compiler_request")?,
    )
    .context("decode compile/deploy job compiler_request")?;
    if compiler_request.compile_request_hash != claim.compile_request_hash {
        bail!(
            "claim compile_request_hash does not match compiler_request: claim={}, request={}",
            claim.compile_request_hash,
            compiler_request.compile_request_hash
        );
    }
    let request = compiler_request.feldera_compile_request();
    if matches!(request.output_contract, OutputSchemaContract::Infer) {
        return Ok(CompileOutcome::Unsupported {
            reason: "jarless Feldera package backend cannot infer output schemas without the SQL compiler; provide an output_contract=must_match descriptor or use an explicit compatibility fixture"
                .to_string(),
            requires_java_sql_compiler: true,
        });
    }

    let resolved_spec = standing_view_spec_for_compile_request(&request);
    let program_schema = feldera_program_schema_for_standing_view_spec(&resolved_spec)
        .context("build Feldera package ProgramSchema from resolved spec")?;
    let descriptor = FelderaProgramDescriptor::new(program_schema);
    descriptor
        .validate_standing_view_spec(&resolved_spec)
        .context("validate resolved spec through Feldera package descriptor")?;

    if let Some(product_runtime_config) = product_runtime_config {
        let product_runtime =
            build_feldera_package_runtime_descriptor(BuildFelderaPackageRuntimeDescriptorRequest {
                spec: resolved_spec.clone(),
                compile_request: request,
                backend: FelderaPackageBackendIdentity {
                    name: WorkerBackendKind::FelderaPackageJarless.label().to_string(),
                    version: product_runtime_config.backend_version.clone(),
                    source: product_runtime_config.backend_source.clone(),
                },
                runtime_factory: FelderaPackageRuntimeFactoryBinding {
                    crate_name: product_runtime_config.runtime_crate_name.clone(),
                    crate_version: product_runtime_config.runtime_crate_version.clone(),
                    factory_symbol: product_runtime_config.runtime_factory_symbol.clone(),
                },
                state_codec: product_runtime_config.state_codec.clone(),
                state_schema_version: product_runtime_config.state_schema_version,
            })
            .context("build jarless Feldera package product runtime descriptor")?;
        return Ok(CompileOutcome::ProductRuntime {
            resolved_spec,
            descriptor: product_runtime,
        });
    }

    Ok(CompileOutcome::SchemaOnly {
        resolved_spec,
        descriptor_identity: format!(
            "feldera-package-schema-only-v1:{}",
            claim.compile_request_hash
        ),
    })
}

async fn compile_with_pipeline_manager_compatibility_backend(
    client: &Client,
    config: &WorkerConfig,
    job: &ViewCompileDeployJobSummary,
    claim: &ClaimViewCompileDeployJobResponse,
) -> anyhow::Result<CompileOutcome> {
    let compiler_request: ViewCompileDeployCompilerRequestV1 = serde_json::from_value(
        job.compiler_request
            .clone()
            .context("compile/deploy job is missing compiler_request")?,
    )
    .context("decode compile/deploy job compiler_request")?;
    if compiler_request.compile_request_hash != claim.compile_request_hash {
        bail!(
            "claim compile_request_hash does not match compiler_request: claim={}, request={}",
            claim.compile_request_hash,
            compiler_request.compile_request_hash
        );
    }
    if job.input_relation_catalogs.is_empty() {
        bail!("compile/deploy job is missing input_relation_catalogs");
    }
    let request = compiler_request.feldera_compile_request();
    let pipeline_request =
        feldera_pipeline_manager_sql_compile_request(&request, &job.input_relation_catalogs)
            .context("build Feldera pipeline-manager compile request")?;
    let program_code = feldera_sql_program_for_compile_request(&pipeline_request)
        .context("render Feldera SQL program")?;
    let pipeline_name =
        feldera_pipeline_name_for_parts(&request.view_id, &claim.compile_request_hash);
    let pipeline = compile_feldera_pipeline(
        client,
        config,
        &pipeline_name,
        &request.view_id,
        program_code,
        &pipeline_request,
    )
    .await?;
    let outputs = feldera_output_schemas_from_program_info(
        request.view_id.as_str(),
        pipeline.program_version,
        pipeline.program_info.as_ref(),
        request.shape.multi_output,
    )
    .context("resolve Feldera output schemas")?;
    let mut resolved_spec = standing_view_spec_for_compile_request(&request);
    resolved_spec.output_relations = outputs;
    resolved_spec.shape.multi_output = resolved_spec.output_relations.len() > 1;

    Ok(CompileOutcome::CompatibilityRuntime {
        resolved_spec,
        deployment: FelderaPipelineManagerRuntimeDeployment {
            pipeline_name,
            mode: FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged,
        },
    })
}

async fn complete_product_runtime_job(
    client: &Client,
    auth_header: &(header::HeaderName, header::HeaderValue),
    config: &WorkerConfig,
    claim: &ClaimViewCompileDeployJobResponse,
    resolved_spec: StandingViewSpec,
    descriptor: FelderaPackageRuntimeDescriptorV1,
) -> anyhow::Result<()> {
    let complete_body = serde_json::json!({
        "compile_request_hash": claim.compile_request_hash,
        "tenant_id": claim.tenant_id,
        "job_generation": claim.job_generation,
        "worker_id": claim.worker_id,
        "lease_id": claim.lease_id,
        "fencing_token": claim.fencing_token,
        "resolved_spec": resolved_spec,
        "product_runtime": descriptor
    });
    let response = client
        .post(api_url(
            &config.api_url,
            &format!("/v1/view-compile-deploy/jobs/{}/complete", claim.view_id),
        )?)
        .header(auth_header.0.clone(), auth_header.1.clone())
        .json(&complete_body)
        .send()
        .await
        .context("complete product runtime compile/deploy job")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("complete product runtime compile/deploy job returned HTTP {status}: {body}");
    }
    Ok(())
}

async fn complete_compatibility_runtime_job(
    client: &Client,
    auth_header: &(header::HeaderName, header::HeaderValue),
    config: &WorkerConfig,
    claim: &ClaimViewCompileDeployJobResponse,
    resolved_spec: StandingViewSpec,
    runtime_deployment: FelderaPipelineManagerRuntimeDeployment,
) -> anyhow::Result<()> {
    let complete_body = serde_json::json!({
        "compile_request_hash": claim.compile_request_hash,
        "tenant_id": claim.tenant_id,
        "job_generation": claim.job_generation,
        "worker_id": claim.worker_id,
        "lease_id": claim.lease_id,
        "fencing_token": claim.fencing_token,
        "resolved_spec": resolved_spec,
        "runtime_deployment": runtime_deployment
    });
    let response = client
        .post(api_url(
            &config.api_url,
            &format!("/v1/view-compile-deploy/jobs/{}/complete", claim.view_id),
        )?)
        .header(auth_header.0.clone(), auth_header.1.clone())
        .json(&complete_body)
        .send()
        .await
        .context("complete compile/deploy job")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("complete compile/deploy job returned HTTP {status}: {body}");
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
struct FelderaPipelineStatusResponse {
    program_status: String,
    #[serde(default)]
    program_version: u64,
    #[serde(default)]
    program_info: Option<Value>,
    #[serde(default)]
    program_error: Option<Value>,
}

async fn compile_feldera_pipeline(
    client: &Client,
    config: &WorkerConfig,
    pipeline_name: &str,
    view_id: &str,
    program_code: String,
    compiler_request: &FelderaCompileRequestV1,
) -> anyhow::Result<FelderaPipelineStatusResponse> {
    let base = config
        .feldera_pipeline_manager_url
        .as_ref()
        .context("Feldera pipeline-manager backend is not configured")?;
    let pipeline_url = base
        .join(&format!("v0/pipelines/{pipeline_name}"))
        .context("build Feldera pipeline URL")?;
    let mut body = serde_json::json!({
        "name": pipeline_name,
        "description": format!("Velorix compile/deploy worker for {view_id}"),
        "runtime_config": {
            "workers": config.feldera_pipeline_workers
        },
        "program_config": {
            "profile": config.feldera_program_profile,
            "cache": true
        },
        "program_code": program_code
    });
    if let Some(udf_rust) = compiler_request.rust_extension.udf_rust.as_ref() {
        body["udf_rust"] = serde_json::json!(udf_rust);
    }
    if let Some(udf_toml) = compiler_request.rust_extension.udf_toml.as_ref() {
        body["udf_toml"] = serde_json::json!(udf_toml);
    }

    let response = feldera_request(
        client,
        config.feldera_bearer_token.as_deref(),
        reqwest::Method::PUT,
        pipeline_url.clone(),
    )
    .json(&body)
    .send()
    .await
    .context("Feldera pipeline create/update failed")?;
    ensure_success("Feldera pipeline create/update", response).await?;

    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(config.feldera_poll_timeout_ms);
    loop {
        let response = feldera_request(
            client,
            config.feldera_bearer_token.as_deref(),
            reqwest::Method::GET,
            pipeline_url.clone(),
        )
        .send()
        .await
        .context("Feldera pipeline status poll failed")?;
        let pipeline = ensure_success("Feldera pipeline status poll", response)
            .await?
            .json::<FelderaPipelineStatusResponse>()
            .await
            .context("decode Feldera pipeline status response")?;
        match pipeline.program_status.as_str() {
            "Success" => return Ok(pipeline),
            "SqlError" | "RustError" | "SystemError" => {
                bail!(
                    "Feldera compiler returned {} for view `{}`: {}",
                    pipeline.program_status,
                    view_id,
                    pipeline
                        .program_error
                        .as_ref()
                        .map(Value::to_string)
                        .unwrap_or_else(|| "<missing program_error>".to_string())
                );
            }
            "Pending" | "CompilingSql" | "SqlCompiled" | "CompilingRust" => {
                if tokio::time::Instant::now() >= deadline {
                    bail!(
                        "Feldera compiler timed out waiting for `{view_id}` to compile; last program_status={}",
                        pipeline.program_status
                    );
                }
                tokio::time::sleep(Duration::from_millis(config.feldera_poll_interval_ms)).await;
            }
            other => {
                bail!("Feldera pipeline status response contains unknown program_status `{other}`")
            }
        }
    }
}

fn feldera_request(
    client: &Client,
    bearer_token: Option<&str>,
    method: reqwest::Method,
    url: Url,
) -> reqwest::RequestBuilder {
    let request = client.request(method, url);
    match bearer_token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

async fn ensure_success(
    operation: &'static str,
    response: reqwest::Response,
) -> anyhow::Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    bail!("{operation} returned HTTP {status}: {body}");
}

fn parse_auth_header(value: &str) -> anyhow::Result<(header::HeaderName, header::HeaderValue)> {
    let (name, value) = value
        .split_once(':')
        .context("admin_auth_header must be formatted as `name: value`")?;
    let name = header::HeaderName::from_bytes(name.trim().as_bytes())
        .context("admin_auth_header name is invalid")?;
    let value = header::HeaderValue::from_str(value.trim())
        .context("admin_auth_header value is invalid")?;
    Ok((name, value))
}

fn api_url(base: &Url, path: &str) -> anyhow::Result<Url> {
    let path = path.strip_prefix('/').unwrap_or(path);
    base.join(path).context("build Velorix API URL")
}

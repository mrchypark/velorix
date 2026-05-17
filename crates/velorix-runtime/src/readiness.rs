use serde::{Deserialize, Serialize};
use velorix_core::feldera_artifact::{
    validate_feldera_compile_artifact_hash, validate_feldera_release_artifact_provenance,
    FelderaCompileArtifactMetadata, FelderaReleaseArtifactProvenanceV1, StandingViewSpec,
};

const PRODUCTION_READINESS_SCHEMA_VERSION: u16 = 4;
const FELDERA_ARTIFACT_EVIDENCE_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionReadinessEvidenceV1 {
    pub schema_version: u16,
    pub deployment_id: String,
    pub authority_store_id: String,
    pub capability_status: ReadinessCheck,
    pub s3_compatible_test_status: ReadinessCheck,
    pub ownership_status: ReadinessCheck,
    pub checkpoint_status: ReadinessCheck,
    pub ingest_status: ReadinessCheck,
    pub relation_catalog_status: ReadinessCheck,
    pub state_status: ReadinessCheck,
    pub query_policy_status: ReadinessCheck,
    pub table_catalog_status: ReadinessCheck,
    pub feldera_artifact_status: ReadinessCheck,
    pub dependency_governance_status: ReadinessCheck,
    pub benchmark_gate_status: ReadinessCheck,
    pub gc_status: ReadinessCheck,
    pub kubernetes_status: ReadinessCheck,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessCheck {
    pub status: ReadinessStatus,
    pub evidence: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_kind: Vec<ReadinessEvidenceKind>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessStatus {
    Pass,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessEvidenceKind {
    S3Compatible,
    S3CompatibleIntegrationHarness,
    KubernetesLeaseClient,
    BootstrapRawStatePath,
    DurableOwnershipEpochRecord,
    PublishedCheckpointLifecycleRecord,
    CheckpointRecoveryTransitionRecord,
    CatalogBackedIngestAdmission,
    DeployedIngestAdmission,
    RelationCatalogRecord,
    RelationCatalogRegistry,
    RelationCatalogClosedAdapterScope,
    RelationCatalogUnsupportedAdapterFailClosed,
    SlateDbCheckpointRef,
    QueryPolicyCatalog,
    RegistryBackedTableCatalog,
    FelderaArtifactRegistry,
    FelderaArtifactHashVerified,
    FelderaArtifactReleaseProvenance,
    DependencyGovernanceValidated,
    S3CompatibleBenchmarkGate,
    GcRunEvidence,
    ProductionGcRunEvidence,
    CheckpointRetentionRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaArtifactHashVerifiedEvidenceV1 {
    pub schema_version: u16,
    pub status: ReadinessStatus,
    pub evidence_kind: ReadinessEvidenceKind,
    pub view_id: String,
    pub artifact_id: String,
    pub artifact_hash: String,
    pub spec_hash: String,
    pub generated_rust_abi_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaArtifactReleaseProvenanceEvidenceV1 {
    pub schema_version: u16,
    pub status: ReadinessStatus,
    pub evidence_kind: ReadinessEvidenceKind,
    pub release_id: String,
    pub release_version: String,
    pub build_id: String,
    pub builder_id: String,
    pub artifact_id: String,
    pub artifact_hash: String,
    pub spec_hash: String,
    pub generated_rust_abi_version: String,
    pub generated_rust_crate_name: String,
    pub source_repository: String,
    pub source_revision: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionReadinessReportV1 {
    pub schema_version: u16,
    pub deployment_id: String,
    pub authority_store_id: String,
    pub capability_status: ReadinessCheck,
    pub s3_compatible_test_status: ReadinessCheck,
    pub ownership_status: ReadinessCheck,
    pub checkpoint_status: ReadinessCheck,
    pub ingest_status: ReadinessCheck,
    pub relation_catalog_status: ReadinessCheck,
    pub state_status: ReadinessCheck,
    pub query_policy_status: ReadinessCheck,
    pub table_catalog_status: ReadinessCheck,
    pub feldera_artifact_status: ReadinessCheck,
    pub dependency_governance_status: ReadinessCheck,
    pub benchmark_gate_status: ReadinessCheck,
    pub gc_status: ReadinessCheck,
    pub kubernetes_status: ReadinessCheck,
    pub production_ready: bool,
    pub blocking_reasons: Vec<String>,
}

impl ProductionReadinessEvidenceV1 {
    pub fn from_json_str(value: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(value)
    }

    pub fn try_into_report(self) -> Result<ProductionReadinessReportV1, String> {
        validate_readiness_schema_version(&self)?;
        Ok(self.into_report())
    }

    pub fn into_report(self) -> ProductionReadinessReportV1 {
        let mut blocking_reasons = Vec::new();

        if self.deployment_id.trim().is_empty() {
            blocking_reasons.push("deployment_id is empty".to_string());
        }
        if self.authority_store_id.trim().is_empty() {
            blocking_reasons.push("authority_store_id is empty".to_string());
        } else if is_local_dev_authority_store(&self.authority_store_id) {
            blocking_reasons.push("authority_store_id uses local/dev authority".to_string());
        }

        push_check_blockers(
            &mut blocking_reasons,
            "capability_status",
            &self.capability_status,
        );
        push_check_blockers(
            &mut blocking_reasons,
            "s3_compatible_test_status",
            &self.s3_compatible_test_status,
        );
        push_check_blockers(
            &mut blocking_reasons,
            "ownership_status",
            &self.ownership_status,
        );
        push_check_blockers(
            &mut blocking_reasons,
            "checkpoint_status",
            &self.checkpoint_status,
        );
        push_check_blockers(&mut blocking_reasons, "ingest_status", &self.ingest_status);
        push_check_blockers(
            &mut blocking_reasons,
            "relation_catalog_status",
            &self.relation_catalog_status,
        );
        push_check_blockers(&mut blocking_reasons, "state_status", &self.state_status);
        push_check_blockers(
            &mut blocking_reasons,
            "query_policy_status",
            &self.query_policy_status,
        );
        push_check_blockers(
            &mut blocking_reasons,
            "table_catalog_status",
            &self.table_catalog_status,
        );
        push_check_blockers(
            &mut blocking_reasons,
            "feldera_artifact_status",
            &self.feldera_artifact_status,
        );
        push_check_blockers(
            &mut blocking_reasons,
            "dependency_governance_status",
            &self.dependency_governance_status,
        );
        push_check_blockers(
            &mut blocking_reasons,
            "benchmark_gate_status",
            &self.benchmark_gate_status,
        );
        push_check_blockers(&mut blocking_reasons, "gc_status", &self.gc_status);
        push_check_blockers(
            &mut blocking_reasons,
            "kubernetes_status",
            &self.kubernetes_status,
        );

        if !self
            .capability_status
            .has_evidence(ReadinessEvidenceKind::S3Compatible)
        {
            blocking_reasons.push("capability_status missing s3_compatible evidence".to_string());
        }
        if !self
            .s3_compatible_test_status
            .has_evidence(ReadinessEvidenceKind::S3CompatibleIntegrationHarness)
        {
            blocking_reasons.push(
                "s3_compatible_test_status missing s3_compatible_integration_harness evidence"
                    .to_string(),
            );
        }
        if !self
            .kubernetes_status
            .has_evidence(ReadinessEvidenceKind::KubernetesLeaseClient)
        {
            blocking_reasons
                .push("kubernetes_status missing kubernetes_lease_client evidence".to_string());
        }
        if !self
            .ownership_status
            .has_evidence(ReadinessEvidenceKind::DurableOwnershipEpochRecord)
        {
            blocking_reasons.push(
                "ownership_status missing durable_ownership_epoch_record evidence".to_string(),
            );
        }
        if !self
            .checkpoint_status
            .has_evidence(ReadinessEvidenceKind::PublishedCheckpointLifecycleRecord)
        {
            blocking_reasons.push(
                "checkpoint_status missing published_checkpoint_lifecycle_record evidence"
                    .to_string(),
            );
        }
        if !self
            .checkpoint_status
            .has_evidence(ReadinessEvidenceKind::CheckpointRecoveryTransitionRecord)
        {
            blocking_reasons.push(
                "checkpoint_status missing checkpoint_recovery_transition_record evidence"
                    .to_string(),
            );
        }
        if !self
            .ingest_status
            .has_evidence(ReadinessEvidenceKind::CatalogBackedIngestAdmission)
        {
            blocking_reasons
                .push("ingest_status missing catalog_backed_ingest_admission evidence".to_string());
        }
        if !self
            .ingest_status
            .has_evidence(ReadinessEvidenceKind::DeployedIngestAdmission)
        {
            blocking_reasons
                .push("ingest_status missing deployed_ingest_admission evidence".to_string());
        }
        if !self
            .relation_catalog_status
            .has_evidence(ReadinessEvidenceKind::RelationCatalogRecord)
        {
            blocking_reasons.push(
                "relation_catalog_status missing relation_catalog_record evidence".to_string(),
            );
        }
        if !self
            .relation_catalog_status
            .has_evidence(ReadinessEvidenceKind::RelationCatalogRegistry)
        {
            blocking_reasons.push(
                "relation_catalog_status missing relation_catalog_registry evidence".to_string(),
            );
        }
        if !self
            .relation_catalog_status
            .has_evidence(ReadinessEvidenceKind::RelationCatalogClosedAdapterScope)
        {
            blocking_reasons.push(
                "relation_catalog_status missing relation_catalog_closed_adapter_scope evidence"
                    .to_string(),
            );
        }
        if !self
            .relation_catalog_status
            .has_evidence(ReadinessEvidenceKind::RelationCatalogUnsupportedAdapterFailClosed)
        {
            blocking_reasons.push(
                "relation_catalog_status missing relation_catalog_unsupported_adapter_fail_closed evidence"
                    .to_string(),
            );
        }
        if self
            .state_status
            .has_evidence(ReadinessEvidenceKind::BootstrapRawStatePath)
        {
            blocking_reasons.push("state_status uses bootstrap raw state path".to_string());
        }
        if !self
            .state_status
            .has_evidence(ReadinessEvidenceKind::SlateDbCheckpointRef)
        {
            blocking_reasons
                .push("state_status missing slate_db_checkpoint_ref evidence".to_string());
        }
        if !self
            .query_policy_status
            .has_evidence(ReadinessEvidenceKind::QueryPolicyCatalog)
        {
            blocking_reasons
                .push("query_policy_status missing query_policy_catalog evidence".to_string());
        }
        if !self
            .table_catalog_status
            .has_evidence(ReadinessEvidenceKind::RegistryBackedTableCatalog)
        {
            blocking_reasons.push(
                "table_catalog_status missing registry_backed_table_catalog evidence".to_string(),
            );
        }
        if !self
            .feldera_artifact_status
            .has_evidence(ReadinessEvidenceKind::FelderaArtifactRegistry)
        {
            blocking_reasons.push(
                "feldera_artifact_status missing feldera_artifact_registry evidence".to_string(),
            );
        }
        if !self
            .feldera_artifact_status
            .has_evidence(ReadinessEvidenceKind::FelderaArtifactHashVerified)
        {
            blocking_reasons.push(
                "feldera_artifact_status missing feldera_artifact_hash_verified evidence"
                    .to_string(),
            );
        }
        if !self
            .feldera_artifact_status
            .has_evidence(ReadinessEvidenceKind::FelderaArtifactReleaseProvenance)
        {
            blocking_reasons.push(
                "feldera_artifact_status missing feldera_artifact_release_provenance evidence"
                    .to_string(),
            );
        }
        if !self
            .benchmark_gate_status
            .has_evidence(ReadinessEvidenceKind::S3CompatibleBenchmarkGate)
        {
            blocking_reasons.push(
                "benchmark_gate_status missing s3_compatible_benchmark_gate evidence".to_string(),
            );
        }
        if !self
            .gc_status
            .has_evidence(ReadinessEvidenceKind::GcRunEvidence)
        {
            blocking_reasons.push("gc_status missing gc_run_evidence evidence".to_string());
        }
        if !self
            .gc_status
            .has_evidence(ReadinessEvidenceKind::ProductionGcRunEvidence)
        {
            blocking_reasons
                .push("gc_status missing production_gc_run_evidence evidence".to_string());
        }
        if !self
            .gc_status
            .has_evidence(ReadinessEvidenceKind::CheckpointRetentionRecord)
        {
            blocking_reasons
                .push("gc_status missing checkpoint_retention_record evidence".to_string());
        }
        if !self
            .dependency_governance_status
            .has_evidence(ReadinessEvidenceKind::DependencyGovernanceValidated)
        {
            blocking_reasons.push(
                "dependency_governance_status missing dependency_governance_validated evidence"
                    .to_string(),
            );
        }

        ProductionReadinessReportV1 {
            schema_version: self.schema_version,
            deployment_id: self.deployment_id,
            authority_store_id: self.authority_store_id,
            capability_status: self.capability_status,
            s3_compatible_test_status: self.s3_compatible_test_status,
            ownership_status: self.ownership_status,
            checkpoint_status: self.checkpoint_status,
            ingest_status: self.ingest_status,
            relation_catalog_status: self.relation_catalog_status,
            state_status: self.state_status,
            query_policy_status: self.query_policy_status,
            table_catalog_status: self.table_catalog_status,
            feldera_artifact_status: self.feldera_artifact_status,
            dependency_governance_status: self.dependency_governance_status,
            benchmark_gate_status: self.benchmark_gate_status,
            gc_status: self.gc_status,
            kubernetes_status: self.kubernetes_status,
            production_ready: blocking_reasons.is_empty(),
            blocking_reasons,
        }
    }
}

impl ProductionReadinessReportV1 {
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl FelderaArtifactHashVerifiedEvidenceV1 {
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl FelderaArtifactReleaseProvenanceEvidenceV1 {
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl ReadinessCheck {
    fn has_evidence(&self, kind: ReadinessEvidenceKind) -> bool {
        self.evidence_kind.contains(&kind)
    }
}

pub fn verify_feldera_artifact_hash_evidence(
    spec_json: &str,
    metadata_json: &str,
    artifact_bytes: &[u8],
) -> Result<FelderaArtifactHashVerifiedEvidenceV1, String> {
    let spec: StandingViewSpec = serde_json::from_str(spec_json)
        .map_err(|error| format!("failed to parse Feldera standing view spec JSON: {error}"))?;
    let artifact: FelderaCompileArtifactMetadata = serde_json::from_str(metadata_json)
        .map_err(|error| format!("failed to parse Feldera artifact metadata JSON: {error}"))?;

    validate_feldera_compile_artifact_hash(&spec, &artifact, artifact_bytes)
        .map_err(|error| error.to_string())?;

    Ok(FelderaArtifactHashVerifiedEvidenceV1 {
        schema_version: FELDERA_ARTIFACT_EVIDENCE_SCHEMA_VERSION,
        status: ReadinessStatus::Pass,
        evidence_kind: ReadinessEvidenceKind::FelderaArtifactHashVerified,
        view_id: artifact.view_id,
        artifact_id: artifact.artifact_id,
        artifact_hash: artifact.artifact_hash,
        spec_hash: artifact.spec_hash,
        generated_rust_abi_version: artifact.generated_rust.abi_version,
    })
}

pub fn verify_feldera_artifact_release_provenance_evidence(
    metadata_json: &str,
    provenance_json: &str,
) -> Result<FelderaArtifactReleaseProvenanceEvidenceV1, String> {
    let artifact: FelderaCompileArtifactMetadata = serde_json::from_str(metadata_json)
        .map_err(|error| format!("failed to parse Feldera artifact metadata JSON: {error}"))?;
    let provenance: FelderaReleaseArtifactProvenanceV1 = serde_json::from_str(provenance_json)
        .map_err(|error| {
            format!("failed to parse Feldera release artifact provenance JSON: {error}")
        })?;

    validate_feldera_release_artifact_provenance(&artifact, &provenance)
        .map_err(|error| error.to_string())?;

    Ok(FelderaArtifactReleaseProvenanceEvidenceV1 {
        schema_version: FELDERA_ARTIFACT_EVIDENCE_SCHEMA_VERSION,
        status: ReadinessStatus::Pass,
        evidence_kind: ReadinessEvidenceKind::FelderaArtifactReleaseProvenance,
        release_id: provenance.release.release_id,
        release_version: provenance.release.release_version,
        build_id: provenance.build.build_id,
        builder_id: provenance.build.builder_id,
        artifact_id: provenance.build.artifact_id,
        artifact_hash: provenance.build.artifact_hash,
        spec_hash: provenance.build.spec_hash,
        generated_rust_abi_version: provenance.build.generated_rust.abi_version,
        generated_rust_crate_name: provenance.build.generated_rust.crate_name,
        source_repository: provenance.provenance.source_repository,
        source_revision: provenance.provenance.source_revision,
    })
}

fn push_check_blockers(blocking_reasons: &mut Vec<String>, field: &str, check: &ReadinessCheck) {
    push_failed_check(blocking_reasons, field, check);
    push_blank_evidence_check(blocking_reasons, field, check);
    push_placeholder_evidence_check(blocking_reasons, field, check);
}

fn push_failed_check(blocking_reasons: &mut Vec<String>, field: &str, check: &ReadinessCheck) {
    if check.status == ReadinessStatus::Fail {
        blocking_reasons.push(format!("{field} failed: {}", check.evidence));
    }
}

fn push_blank_evidence_check(
    blocking_reasons: &mut Vec<String>,
    field: &str,
    check: &ReadinessCheck,
) {
    if check.status == ReadinessStatus::Pass && check.evidence.trim().is_empty() {
        blocking_reasons.push(format!("{field} missing evidence text"));
    }
}

fn push_placeholder_evidence_check(
    blocking_reasons: &mut Vec<String>,
    field: &str,
    check: &ReadinessCheck,
) {
    if check.status != ReadinessStatus::Pass {
        return;
    }

    let evidence = check.evidence.to_lowercase();
    for (marker, reason) in [
        ("placeholder", "placeholder"),
        ("bootstrap", "bootstrap"),
        ("local-only", "local-only"),
        ("local only", "local-only"),
        ("local filesystem", "local filesystem"),
        ("emulator", "emulator"),
    ] {
        if evidence.contains(marker) {
            blocking_reasons.push(format!("{field} uses {reason} evidence"));
            return;
        }
    }
}

fn is_local_dev_authority_store(authority_store_id: &str) -> bool {
    let authority_store_id = authority_store_id.to_lowercase();
    [
        "memory://",
        "file://",
        "localhost",
        "127.0.0.1",
        "emulator",
        "local-only",
        "local only",
        "local filesystem",
    ]
    .iter()
    .any(|marker| authority_store_id.contains(marker))
}

pub fn validate_readiness_schema_version(
    evidence: &ProductionReadinessEvidenceV1,
) -> Result<(), String> {
    if evidence.schema_version == PRODUCTION_READINESS_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(format!(
            "unsupported readiness schema_version {}, expected {PRODUCTION_READINESS_SCHEMA_VERSION}",
            evidence.schema_version
        ))
    }
}

use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionReadinessEvidenceV1 {
    pub schema_version: u16,
    pub deployment_id: String,
    pub authority_store_id: String,
    pub capability_status: ReadinessCheck,
    pub ownership_status: ReadinessCheck,
    pub checkpoint_status: ReadinessCheck,
    pub state_status: ReadinessCheck,
    pub query_policy_status: ReadinessCheck,
    pub table_catalog_status: ReadinessCheck,
    pub feldera_artifact_status: ReadinessCheck,
    pub benchmark_gate_status: ReadinessCheck,
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
    KubernetesLeaseClient,
    BootstrapRawStatePath,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionReadinessReportV1 {
    pub schema_version: u16,
    pub deployment_id: String,
    pub authority_store_id: String,
    pub capability_status: ReadinessCheck,
    pub ownership_status: ReadinessCheck,
    pub checkpoint_status: ReadinessCheck,
    pub state_status: ReadinessCheck,
    pub query_policy_status: ReadinessCheck,
    pub table_catalog_status: ReadinessCheck,
    pub feldera_artifact_status: ReadinessCheck,
    pub benchmark_gate_status: ReadinessCheck,
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

        push_failed_check(
            &mut blocking_reasons,
            "capability_status",
            &self.capability_status,
        );
        push_failed_check(
            &mut blocking_reasons,
            "ownership_status",
            &self.ownership_status,
        );
        push_failed_check(
            &mut blocking_reasons,
            "checkpoint_status",
            &self.checkpoint_status,
        );
        push_failed_check(&mut blocking_reasons, "state_status", &self.state_status);
        push_failed_check(
            &mut blocking_reasons,
            "query_policy_status",
            &self.query_policy_status,
        );
        push_failed_check(
            &mut blocking_reasons,
            "table_catalog_status",
            &self.table_catalog_status,
        );
        push_failed_check(
            &mut blocking_reasons,
            "feldera_artifact_status",
            &self.feldera_artifact_status,
        );
        push_failed_check(
            &mut blocking_reasons,
            "benchmark_gate_status",
            &self.benchmark_gate_status,
        );
        push_failed_check(
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
            .kubernetes_status
            .has_evidence(ReadinessEvidenceKind::KubernetesLeaseClient)
        {
            blocking_reasons
                .push("kubernetes_status missing kubernetes_lease_client evidence".to_string());
        }
        if self
            .state_status
            .has_evidence(ReadinessEvidenceKind::BootstrapRawStatePath)
        {
            blocking_reasons.push("state_status uses bootstrap raw state path".to_string());
        }

        ProductionReadinessReportV1 {
            schema_version: self.schema_version,
            deployment_id: self.deployment_id,
            authority_store_id: self.authority_store_id,
            capability_status: self.capability_status,
            ownership_status: self.ownership_status,
            checkpoint_status: self.checkpoint_status,
            state_status: self.state_status,
            query_policy_status: self.query_policy_status,
            table_catalog_status: self.table_catalog_status,
            feldera_artifact_status: self.feldera_artifact_status,
            benchmark_gate_status: self.benchmark_gate_status,
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

impl ReadinessCheck {
    fn has_evidence(&self, kind: ReadinessEvidenceKind) -> bool {
        self.evidence_kind.contains(&kind)
    }
}

fn push_failed_check(blocking_reasons: &mut Vec<String>, field: &str, check: &ReadinessCheck) {
    if check.status == ReadinessStatus::Fail {
        blocking_reasons.push(format!("{field} failed: {}", check.evidence));
    }
}

pub fn validate_readiness_schema_version(
    evidence: &ProductionReadinessEvidenceV1,
) -> Result<(), String> {
    if evidence.schema_version == SCHEMA_VERSION {
        Ok(())
    } else {
        Err(format!(
            "unsupported readiness schema_version {}, expected {SCHEMA_VERSION}",
            evidence.schema_version
        ))
    }
}

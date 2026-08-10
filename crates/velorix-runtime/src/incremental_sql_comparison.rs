//! Cross-engine incremental SQL comparison evidence.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u8 = 2;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementalSqlComparisonResultV2 {
    pub schema_version: u8,
    pub corpus_version: String,
    pub engine: ComparisonEngineV1,
    pub protocol: ComparisonProtocolV1,
    pub correctness: Vec<CorrectnessOutcomeV2>,
    pub performance: Vec<PerformanceMeasurementV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonEngineV1 {
    pub name: String,
    pub version: String,
    pub source_revision: String,
    pub configuration: BTreeMap<String, String>,
    pub durability_mode: String,
    pub input_semantics: String,
    pub state_retention_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonProtocolV1 {
    pub warm_up_iterations: u32,
    pub measured_iterations: u32,
    pub initial_rows: u64,
    pub change_events: u64,
    pub change_mix: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrectnessOutcomeV2 {
    pub workload_id: String,
    pub outcome: CorrectnessStatusV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CorrectnessStatusV2 {
    Passed {
        expected_digest: String,
        observed_digest: String,
        verified_phases: Vec<String>,
        plan_evidence: ComparisonPlanEvidenceV1,
    },
    Unsupported {
        reason: String,
    },
    SemanticDifference {
        reason_code: String,
        reason: String,
        scope: SemanticDifferenceScopeV1,
        expected_digest: String,
        verified_phases: Vec<String>,
        blocked_phases: Vec<String>,
        recovery_parity_claimed: bool,
        performance_comparable: bool,
    },
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticDifferenceScopeV1 {
    EditionWide,
    WorkloadSpecific,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonPlanEvidenceV1 {
    pub native_logical_plan: NativeIdentityEvidenceV1,
    pub native_physical_dag: NativeIdentityEvidenceV1,
    pub diagnostic_explain_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "availability", rename_all = "snake_case")]
pub enum NativeIdentityEvidenceV1 {
    Available { identity: String },
    Unavailable { reason_code: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceMeasurementV1 {
    pub workload_id: String,
    pub feature_family: String,
    pub required_semantics: PerformanceCellSemanticsV1,
    pub observed_semantics: PerformanceCellSemanticsV1,
    pub repetitions: u32,
    pub input_rows: u64,
    pub change_events: u64,
    pub output_change_records: u64,
    pub input_rows_per_second: f64,
    pub output_rows_per_second: f64,
    pub output_amplification: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub state_bytes: u64,
    pub checkpoint_bytes: u64,
    pub checkpoint_ms: f64,
    pub restore_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceCellSemanticsV1 {
    pub sql_identity: String,
    pub durability_mode: String,
    pub output_acknowledgement: String,
    pub watermark_lateness: String,
    pub state_retention: String,
    pub restart_success: String,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum IncrementalSqlComparisonError {
    #[error("comparison result is not valid JSON V2: {0}")]
    Json(String),
    #[error("comparison result schema_version must be 1, got {actual}")]
    UnsupportedSchemaVersion { actual: u8 },
    #[error("comparison result field {field} must be present")]
    MissingRequiredField { field: &'static str },
    #[error("comparison protocol must have at least one measured iteration")]
    MissingMeasuredIterations,
    #[error(
        "comparison protocol change_mix total {actual} does not equal change_events {expected}"
    )]
    ChangeMixMismatch { expected: u64, actual: u64 },
    #[error("comparison correctness contains duplicate workload {workload_id}")]
    DuplicateCorrectnessWorkload { workload_id: String },
    #[error("comparison performance contains duplicate workload {workload_id}")]
    DuplicatePerformanceWorkload { workload_id: String },
    #[error("comparison result is missing correctness outcome for workload {workload_id}")]
    MissingCorrectnessWorkload { workload_id: String },
    #[error("comparison result contains unexpected correctness workload {workload_id}")]
    UnexpectedCorrectnessWorkload { workload_id: String },
    #[error("passed workload {workload_id} has invalid or mismatched result digests")]
    InvalidPassedDigest { workload_id: String },
    #[error("passed workload {workload_id} has no verified phases")]
    MissingVerifiedPhases { workload_id: String },
    #[error("passed workload {workload_id} has invalid plan identity field {field}")]
    InvalidPlanEvidence {
        workload_id: String,
        field: &'static str,
    },
    #[error("workload {workload_id} status reason must be present")]
    MissingStatusReason { workload_id: String },
    #[error("workload {workload_id} has invalid semantic-difference evidence field {field}")]
    InvalidSemanticDifferenceEvidence {
        workload_id: String,
        field: &'static str,
    },
    #[error("performance workload {workload_id} did not pass correctness")]
    PerformanceWithoutPassedCorrectness { workload_id: String },
    #[error("performance workload {workload_id} has no native plan/DAG identity")]
    PerformanceWithoutNativePlanEvidence { workload_id: String },
    #[error("performance workload {workload_id} must have at least one repetition")]
    MissingPerformanceRepetitions { workload_id: String },
    #[error("performance workload {workload_id} metric {metric} must be finite and non-negative")]
    InvalidPerformanceMetric {
        workload_id: String,
        metric: &'static str,
    },
    #[error("performance workload {workload_id} p95_ms is below p50_ms")]
    InvalidLatencyOrder { workload_id: String },
    #[error("performance workload {workload_id} output amplification does not match output records/change events")]
    InvalidOutputAmplification { workload_id: String },
    #[error(
        "performance workload {workload_id} is not semantically comparable: {mismatched_fields:?}"
    )]
    IncomparablePerformanceCell {
        workload_id: String,
        mismatched_fields: Vec<&'static str>,
    },
}

impl IncrementalSqlComparisonResultV2 {
    pub fn from_json_str(json: &str) -> Result<Self, IncrementalSqlComparisonError> {
        let result = serde_json::from_str::<Self>(json)
            .map_err(|error| IncrementalSqlComparisonError::Json(error.to_string()))?;
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), IncrementalSqlComparisonError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(IncrementalSqlComparisonError::UnsupportedSchemaVersion {
                actual: self.schema_version,
            });
        }
        require_text("corpus_version", &self.corpus_version)?;
        self.engine.validate()?;
        self.protocol.validate()?;

        let mut correctness = BTreeMap::new();
        for outcome in &self.correctness {
            require_text("correctness.workload_id", &outcome.workload_id)?;
            outcome.validate(&self.engine.name)?;
            if correctness
                .insert(outcome.workload_id.as_str(), &outcome.outcome)
                .is_some()
            {
                return Err(
                    IncrementalSqlComparisonError::DuplicateCorrectnessWorkload {
                        workload_id: outcome.workload_id.clone(),
                    },
                );
            }
        }

        let mut performance = BTreeSet::new();
        for measurement in &self.performance {
            require_text("performance.workload_id", &measurement.workload_id)?;
            if !performance.insert(measurement.workload_id.as_str()) {
                return Err(
                    IncrementalSqlComparisonError::DuplicatePerformanceWorkload {
                        workload_id: measurement.workload_id.clone(),
                    },
                );
            }
            match correctness.get(measurement.workload_id.as_str()) {
                Some(CorrectnessStatusV2::Passed { plan_evidence, .. })
                    if plan_evidence.performance_qualifying() => {}
                Some(CorrectnessStatusV2::Passed { .. }) => {
                    return Err(
                        IncrementalSqlComparisonError::PerformanceWithoutNativePlanEvidence {
                            workload_id: measurement.workload_id.clone(),
                        },
                    );
                }
                _ => {
                    return Err(
                        IncrementalSqlComparisonError::PerformanceWithoutPassedCorrectness {
                            workload_id: measurement.workload_id.clone(),
                        },
                    );
                }
            }
            measurement.validate()?;
        }
        Ok(())
    }

    pub fn validate_for_workloads(
        &self,
        expected_workloads: &[&str],
    ) -> Result<(), IncrementalSqlComparisonError> {
        self.validate()?;
        let expected = expected_workloads.iter().copied().collect::<BTreeSet<_>>();
        let actual = self
            .correctness
            .iter()
            .map(|outcome| outcome.workload_id.as_str())
            .collect::<BTreeSet<_>>();
        if let Some(workload_id) = expected.difference(&actual).next() {
            return Err(IncrementalSqlComparisonError::MissingCorrectnessWorkload {
                workload_id: (*workload_id).to_string(),
            });
        }
        if let Some(workload_id) = actual.difference(&expected).next() {
            return Err(
                IncrementalSqlComparisonError::UnexpectedCorrectnessWorkload {
                    workload_id: (*workload_id).to_string(),
                },
            );
        }
        Ok(())
    }
}

impl ComparisonEngineV1 {
    fn validate(&self) -> Result<(), IncrementalSqlComparisonError> {
        require_text("engine.name", &self.name)?;
        require_text("engine.version", &self.version)?;
        require_text("engine.source_revision", &self.source_revision)?;
        require_text("engine.durability_mode", &self.durability_mode)?;
        require_text("engine.input_semantics", &self.input_semantics)?;
        require_text(
            "engine.state_retention_policy",
            &self.state_retention_policy,
        )?;
        if self.configuration.is_empty()
            || self
                .configuration
                .iter()
                .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
        {
            return Err(IncrementalSqlComparisonError::MissingRequiredField {
                field: "engine.configuration",
            });
        }
        Ok(())
    }
}

impl ComparisonProtocolV1 {
    fn validate(&self) -> Result<(), IncrementalSqlComparisonError> {
        if self.measured_iterations == 0 {
            return Err(IncrementalSqlComparisonError::MissingMeasuredIterations);
        }
        let actual = self.change_mix.values().sum();
        if actual != self.change_events {
            return Err(IncrementalSqlComparisonError::ChangeMixMismatch {
                expected: self.change_events,
                actual,
            });
        }
        Ok(())
    }
}

impl CorrectnessOutcomeV2 {
    fn validate(&self, engine_name: &str) -> Result<(), IncrementalSqlComparisonError> {
        match &self.outcome {
            CorrectnessStatusV2::Passed {
                expected_digest,
                observed_digest,
                verified_phases,
                plan_evidence,
            } => {
                if !valid_digest(expected_digest) || expected_digest != observed_digest {
                    return Err(IncrementalSqlComparisonError::InvalidPassedDigest {
                        workload_id: self.workload_id.clone(),
                    });
                }
                if verified_phases.is_empty()
                    || verified_phases.iter().any(|phase| phase.trim().is_empty())
                {
                    return Err(IncrementalSqlComparisonError::MissingVerifiedPhases {
                        workload_id: self.workload_id.clone(),
                    });
                }
                plan_evidence.validate(&self.workload_id, engine_name)?;
            }
            CorrectnessStatusV2::Unsupported { reason }
            | CorrectnessStatusV2::Failed { reason } => {
                if reason.trim().is_empty() {
                    return Err(IncrementalSqlComparisonError::MissingStatusReason {
                        workload_id: self.workload_id.clone(),
                    });
                }
            }
            CorrectnessStatusV2::SemanticDifference {
                reason_code,
                reason,
                expected_digest,
                verified_phases,
                blocked_phases,
                recovery_parity_claimed,
                performance_comparable,
                ..
            } => {
                if reason.trim().is_empty() {
                    return Err(IncrementalSqlComparisonError::MissingStatusReason {
                        workload_id: self.workload_id.clone(),
                    });
                }
                if !valid_reason_code(reason_code) {
                    return Err(self.invalid_semantic_difference("reason_code"));
                }
                if !valid_digest(expected_digest) {
                    return Err(self.invalid_semantic_difference("expected_digest"));
                }
                if verified_phases.is_empty() || !valid_distinct_texts(verified_phases) {
                    return Err(self.invalid_semantic_difference("verified_phases"));
                }
                if !valid_distinct_texts(blocked_phases)
                    || verified_phases
                        .iter()
                        .any(|phase| blocked_phases.contains(phase))
                {
                    return Err(self.invalid_semantic_difference("blocked_phases"));
                }
                if *recovery_parity_claimed {
                    return Err(self.invalid_semantic_difference("recovery_parity_claimed"));
                }
                if *performance_comparable {
                    return Err(self.invalid_semantic_difference("performance_comparable"));
                }
            }
        }
        Ok(())
    }

    fn invalid_semantic_difference(&self, field: &'static str) -> IncrementalSqlComparisonError {
        IncrementalSqlComparisonError::InvalidSemanticDifferenceEvidence {
            workload_id: self.workload_id.clone(),
            field,
        }
    }
}

fn valid_distinct_texts(values: &[String]) -> bool {
    let mut distinct = BTreeSet::new();
    values
        .iter()
        .all(|value| !value.trim().is_empty() && distinct.insert(value.as_str()))
}

impl ComparisonPlanEvidenceV1 {
    fn validate(
        &self,
        workload_id: &str,
        engine_name: &str,
    ) -> Result<(), IncrementalSqlComparisonError> {
        self.native_logical_plan
            .validate(workload_id, "native_logical_plan", engine_name)?;
        self.native_physical_dag
            .validate(workload_id, "native_physical_dag", engine_name)?;
        if self
            .diagnostic_explain_digest
            .as_deref()
            .is_some_and(|digest| !valid_namespaced_digest(digest, engine_name))
        {
            return Err(IncrementalSqlComparisonError::InvalidPlanEvidence {
                workload_id: workload_id.to_string(),
                field: "diagnostic_explain_digest",
            });
        }
        Ok(())
    }

    fn performance_qualifying(&self) -> bool {
        matches!(
            (&self.native_logical_plan, &self.native_physical_dag),
            (
                NativeIdentityEvidenceV1::Available { .. },
                NativeIdentityEvidenceV1::Available { .. }
            )
        )
    }
}

impl NativeIdentityEvidenceV1 {
    fn validate(
        &self,
        workload_id: &str,
        field: &'static str,
        engine_name: &str,
    ) -> Result<(), IncrementalSqlComparisonError> {
        let valid = match self {
            Self::Available { identity } => valid_namespaced_digest(identity, engine_name),
            Self::Unavailable { reason_code } => valid_reason_code(reason_code),
        };
        if !valid {
            return Err(IncrementalSqlComparisonError::InvalidPlanEvidence {
                workload_id: workload_id.to_string(),
                field,
            });
        }
        Ok(())
    }
}

impl PerformanceMeasurementV1 {
    fn validate(&self) -> Result<(), IncrementalSqlComparisonError> {
        require_text("performance.feature_family", &self.feature_family)?;
        self.required_semantics.validate()?;
        self.observed_semantics.validate()?;
        let mismatched_fields = self
            .required_semantics
            .mismatched_fields(&self.observed_semantics);
        if !mismatched_fields.is_empty() {
            return Err(IncrementalSqlComparisonError::IncomparablePerformanceCell {
                workload_id: self.workload_id.clone(),
                mismatched_fields,
            });
        }
        if self.repetitions == 0 {
            return Err(
                IncrementalSqlComparisonError::MissingPerformanceRepetitions {
                    workload_id: self.workload_id.clone(),
                },
            );
        }
        for (metric, value) in [
            ("input_rows_per_second", self.input_rows_per_second),
            ("output_rows_per_second", self.output_rows_per_second),
            ("output_amplification", self.output_amplification),
            ("p50_ms", self.p50_ms),
            ("p95_ms", self.p95_ms),
            ("checkpoint_ms", self.checkpoint_ms),
            ("restore_ms", self.restore_ms),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(IncrementalSqlComparisonError::InvalidPerformanceMetric {
                    workload_id: self.workload_id.clone(),
                    metric,
                });
            }
        }
        if self.p95_ms < self.p50_ms {
            return Err(IncrementalSqlComparisonError::InvalidLatencyOrder {
                workload_id: self.workload_id.clone(),
            });
        }
        let expected_amplification = if self.change_events == 0 {
            0.0
        } else {
            self.output_change_records as f64 / self.change_events as f64
        };
        if (self.output_amplification - expected_amplification).abs() > f64::EPSILON {
            return Err(IncrementalSqlComparisonError::InvalidOutputAmplification {
                workload_id: self.workload_id.clone(),
            });
        }
        Ok(())
    }
}

impl PerformanceCellSemanticsV1 {
    fn validate(&self) -> Result<(), IncrementalSqlComparisonError> {
        for (field, value) in [
            ("performance.sql_identity", self.sql_identity.as_str()),
            ("performance.durability_mode", self.durability_mode.as_str()),
            (
                "performance.output_acknowledgement",
                self.output_acknowledgement.as_str(),
            ),
            (
                "performance.watermark_lateness",
                self.watermark_lateness.as_str(),
            ),
            ("performance.state_retention", self.state_retention.as_str()),
            ("performance.restart_success", self.restart_success.as_str()),
        ] {
            require_text(field, value)?;
        }
        Ok(())
    }

    pub fn mismatched_fields(&self, observed: &Self) -> Vec<&'static str> {
        let mut mismatches = Vec::new();
        if self.sql_identity != observed.sql_identity {
            mismatches.push("sql_identity");
        }
        if self.durability_mode != observed.durability_mode {
            mismatches.push("durability_mode");
        }
        if self.output_acknowledgement != observed.output_acknowledgement {
            mismatches.push("output_acknowledgement");
        }
        if self.watermark_lateness != observed.watermark_lateness {
            mismatches.push("watermark_lateness");
        }
        if self.state_retention != observed.state_retention {
            mismatches.push("state_retention");
        }
        if self.restart_success != observed.restart_success {
            mismatches.push("restart_success");
        }
        mismatches
    }
}

fn require_text(field: &'static str, value: &str) -> Result<(), IncrementalSqlComparisonError> {
    if value.trim().is_empty() {
        Err(IncrementalSqlComparisonError::MissingRequiredField { field })
    } else {
        Ok(())
    }
}

fn valid_digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn valid_namespaced_digest(value: &str, engine_name: &str) -> bool {
    let Some((namespace, hex)) = value.rsplit_once(":sha256:") else {
        return false;
    };
    let Some(version) = namespace
        .rsplit_once("-sha256-v")
        .map(|(_, version)| version)
    else {
        return false;
    };
    namespace.starts_with(&format!("{}-", engine_name.trim().to_ascii_lowercase()))
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !version.is_empty()
        && version.bytes().all(|byte| byte.is_ascii_digit())
        && valid_digest(&format!("sha256:{hex}"))
}

fn valid_reason_code(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

//! Versioned capability contracts for native incremental operator DAGs.
//!
//! Output ports carry producer guarantees, input ports carry consumer
//! requirements, and edges only connect the two identities. The planner must
//! derive these contracts from operator semantics; they are not user claims.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const OPERATOR_DAG_CONTRACT_VERSION_V1: &str = "velorix-operator-dag-contract-v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDagContractV1 {
    pub contract_version: String,
    pub operators: Vec<OperatorContractV1>,
    pub edges: Vec<OperatorEdgeV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorContractV1 {
    pub node_id: String,
    pub operator: OperatorKindIdentityV1,
    pub inputs: Vec<InputPortContractV1>,
    pub outputs: Vec<OutputPortContractV1>,
    pub state: Option<StateContractV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorKindIdentityV1 {
    pub kind: String,
    pub version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputPortContractV1 {
    pub port_id: String,
    pub accepted_changelog: AcceptedChangelogV1,
    pub required_columns: Vec<RequiredColumnV1>,
    pub required_keys: Vec<CandidateKeyV1>,
    pub required_determinism: DeterminismRequirementV1,
    pub required_progress: ProgressRequirementV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutputPortContractV1 {
    pub port_id: String,
    pub schema: RowSchemaV1,
    pub changelog: ChangelogModeV1,
    pub candidate_keys: Vec<CandidateKeyV1>,
    pub uniqueness: UniquenessGuaranteeV1,
    pub determinism: DeterminismGuaranteeV1,
    pub progress: ProgressGuaranteeV1,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorEdgeV1 {
    pub from: OutputPortRefV1,
    pub to: InputPortRefV1,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutputPortRefV1 {
    pub node_id: String,
    pub port_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputPortRefV1 {
    pub node_id: String,
    pub port_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowSchemaV1 {
    pub columns: Vec<PortColumnV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortColumnV1 {
    pub column_id: String,
    pub logical_type: String,
    pub nullability: NullabilityV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredColumnV1 {
    pub column_id: String,
    pub nullability: NullabilityV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NullabilityV1 {
    NonNull,
    Nullable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateKeyV1 {
    pub columns: Vec<String>,
    pub equality: KeyEqualityV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyEqualityV1 {
    NonNullEquality,
    SqlNotDistinct,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChangelogModeV1 {
    AppendOnly,
    Upsert { identity_key: CandidateKeyV1 },
    GeneralRetract,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcceptedChangelogV1 {
    AppendOnly,
    Upsert { identity_key: CandidateKeyV1 },
    GeneralRetract,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UniquenessGuaranteeV1 {
    NotGuaranteed,
    CandidateKeys,
    Singleton,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeterminismGuaranteeV1 {
    ReplayDeterministic,
    NotGuaranteed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeterminismRequirementV1 {
    ReplayDeterministic,
    Any,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressGuaranteeV1 {
    pub processing: ProcessingFrontierGuaranteeV1,
    pub watermark: WatermarkGuaranteeV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressRequirementV1 {
    pub processing: ProcessingFrontierRequirementV1,
    pub watermark: WatermarkRequirementV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingFrontierGuaranteeV1 {
    None,
    PerInputCheckpointed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingFrontierRequirementV1 {
    None,
    PerInputCheckpointed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WatermarkGuaranteeV1 {
    None,
    Monotonic { event_time_column_id: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WatermarkRequirementV1 {
    None,
    Monotonic { event_time_column_id: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StateContractV1 {
    pub boundedness: StateBoundednessV1,
    pub checkpoint_codec: CheckpointCodecIdentityV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateBoundednessV1 {
    StaticallyBounded {
        max_rows: u64,
    },
    RetentionBounded {
        retention_ns: u64,
    },
    WatermarkBounded {
        event_time_column_id: String,
        allowed_lateness_ns: u64,
    },
    Unbounded,
}

/// Explicit state retention contract for watermark-bounded operators. The
/// contract bounds state growth without silently changing SQL semantics:
/// windows fully closed before `closed_window_retention_ns` may be evicted
/// from operator state, and `late_row_evidence_retention_ns` bounds how long
/// dropped-late-row evidence is kept for observability.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StateRetentionContractV1 {
    /// Retention window for closed window state, in nanoseconds.
    pub closed_window_retention_ns: u64,
    /// Retention window for late-row handling evidence, in nanoseconds.
    pub late_row_evidence_retention_ns: u64,
    /// Hard cap on retained open window groups; admission rejects plans that
    /// could exceed it only when the operator can prove the bound.
    pub max_open_windows: u64,
}

impl StateRetentionContractV1 {
    pub fn validate(&self) -> Result<(), OperatorContractError> {
        if self.max_open_windows == 0 {
            return Err(OperatorContractError::Invalid(
                "state retention contract max_open_windows must be non-zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointCodecIdentityV1 {
    pub codec_id: String,
    pub codec_version: u32,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OperatorContractError {
    #[error("unsupported operator DAG contract version")]
    UnsupportedVersion,
    #[error("invalid operator DAG contract: {0}")]
    Invalid(String),
    #[error("incompatible operator edge {from_node}.{from_port} -> {to_node}.{to_port}: {reason}")]
    IncompatibleEdge {
        from_node: String,
        from_port: String,
        to_node: String,
        to_port: String,
        reason: String,
    },
}

impl OperatorDagContractV1 {
    pub fn validate(&self) -> Result<(), OperatorContractError> {
        if self.contract_version != OPERATOR_DAG_CONTRACT_VERSION_V1 {
            return Err(OperatorContractError::UnsupportedVersion);
        }
        if self.operators.is_empty() {
            return invalid("at least one operator is required");
        }

        let mut operators = BTreeMap::new();
        for operator in &self.operators {
            require_identity("node_id", &operator.node_id)?;
            require_identity("operator.kind", &operator.operator.kind)?;
            if operator.operator.version == 0 {
                return invalid("operator version must be positive");
            }
            if operators
                .insert(operator.node_id.as_str(), operator)
                .is_some()
            {
                return invalid("operator node ids must be unique");
            }
            validate_operator(operator)?;
        }

        let mut seen_edges = BTreeSet::new();
        let mut connected_inputs = BTreeSet::new();
        for edge in &self.edges {
            if !seen_edges.insert(edge) {
                return invalid("operator edges must be unique");
            }
            let producer = operators
                .get(edge.from.node_id.as_str())
                .and_then(|node| {
                    node.outputs
                        .iter()
                        .find(|port| port.port_id == edge.from.port_id)
                })
                .ok_or_else(|| {
                    OperatorContractError::Invalid("edge producer port is missing".into())
                })?;
            let consumer = operators
                .get(edge.to.node_id.as_str())
                .and_then(|node| {
                    node.inputs
                        .iter()
                        .find(|port| port.port_id == edge.to.port_id)
                })
                .ok_or_else(|| {
                    OperatorContractError::Invalid("edge consumer port is missing".into())
                })?;
            if !connected_inputs.insert(&edge.to) {
                return invalid("an input port must have exactly one producer");
            }
            if let Err(reason) = producer_satisfies_consumer(producer, consumer) {
                return Err(OperatorContractError::IncompatibleEdge {
                    from_node: edge.from.node_id.clone(),
                    from_port: edge.from.port_id.clone(),
                    to_node: edge.to.node_id.clone(),
                    to_port: edge.to.port_id.clone(),
                    reason,
                });
            }
        }

        for operator in &self.operators {
            for input in &operator.inputs {
                let input_ref = InputPortRefV1 {
                    node_id: operator.node_id.clone(),
                    port_id: input.port_id.clone(),
                };
                if !connected_inputs.contains(&input_ref) {
                    return invalid("every input port must have exactly one producer");
                }
            }
        }
        Ok(())
    }
}

pub fn producer_satisfies_consumer(
    producer: &OutputPortContractV1,
    consumer: &InputPortContractV1,
) -> Result<(), String> {
    if !changelog_satisfies(&producer.changelog, &consumer.accepted_changelog) {
        return Err("changelog guarantee does not satisfy consumer".into());
    }
    for required in &consumer.required_columns {
        let Some(column) = producer
            .schema
            .columns
            .iter()
            .find(|column| column.column_id == required.column_id)
        else {
            return Err(format!("required column {} is missing", required.column_id));
        };
        if required.nullability == NullabilityV1::NonNull
            && column.nullability != NullabilityV1::NonNull
        {
            return Err(format!(
                "required column {} may be NULL",
                required.column_id
            ));
        }
    }
    for required in &consumer.required_keys {
        if !producer
            .candidate_keys
            .iter()
            .any(|provided| candidate_key_satisfies(provided, required))
        {
            return Err("candidate key guarantee does not satisfy consumer".into());
        }
    }
    if consumer.required_determinism == DeterminismRequirementV1::ReplayDeterministic
        && producer.determinism != DeterminismGuaranteeV1::ReplayDeterministic
    {
        return Err("replay determinism is required".into());
    }
    if consumer.required_progress.processing
        == ProcessingFrontierRequirementV1::PerInputCheckpointed
        && producer.progress.processing != ProcessingFrontierGuaranteeV1::PerInputCheckpointed
    {
        return Err("checkpointed processing frontier is required".into());
    }
    match (
        &producer.progress.watermark,
        &consumer.required_progress.watermark,
    ) {
        (_, WatermarkRequirementV1::None) => {}
        (
            WatermarkGuaranteeV1::Monotonic {
                event_time_column_id: provided,
            },
            WatermarkRequirementV1::Monotonic {
                event_time_column_id: required,
            },
        ) if provided == required => {}
        _ => return Err("matching monotonic watermark is required".into()),
    }
    Ok(())
}

fn validate_operator(operator: &OperatorContractV1) -> Result<(), OperatorContractError> {
    let mut ports = BTreeSet::new();
    for input in &operator.inputs {
        require_identity("input.port_id", &input.port_id)?;
        if !ports.insert(("input", input.port_id.as_str())) {
            return invalid("input port ids must be unique per operator");
        }
        for key in &input.required_keys {
            validate_candidate_key(key)?;
        }
    }
    for output in &operator.outputs {
        require_identity("output.port_id", &output.port_id)?;
        if !ports.insert(("output", output.port_id.as_str())) {
            return invalid("output port ids must be unique per operator");
        }
        validate_output(output)?;
    }
    if let Some(state) = &operator.state {
        require_identity(
            "state.checkpoint_codec.codec_id",
            &state.checkpoint_codec.codec_id,
        )?;
        if state.checkpoint_codec.codec_version == 0 {
            return invalid("checkpoint codec version must be positive");
        }
        match &state.boundedness {
            StateBoundednessV1::StaticallyBounded { max_rows } if *max_rows == 0 => {
                return invalid("static state bound must be positive");
            }
            StateBoundednessV1::RetentionBounded { retention_ns } if *retention_ns == 0 => {
                return invalid("state retention must be positive");
            }
            StateBoundednessV1::WatermarkBounded {
                event_time_column_id,
                ..
            } => require_identity("state.event_time_column_id", event_time_column_id)?,
            _ => {}
        }
    }
    Ok(())
}

fn validate_output(output: &OutputPortContractV1) -> Result<(), OperatorContractError> {
    if output.schema.columns.is_empty() {
        return invalid("output schema must contain columns");
    }
    let mut columns = BTreeSet::new();
    for column in &output.schema.columns {
        require_identity("output.column_id", &column.column_id)?;
        require_identity("output.logical_type", &column.logical_type)?;
        if !columns.insert(column.column_id.as_str()) {
            return invalid("output column ids must be unique");
        }
    }
    for key in &output.candidate_keys {
        validate_candidate_key(key)?;
        if key
            .columns
            .iter()
            .any(|column| !columns.contains(column.as_str()))
        {
            return invalid("candidate key references an unknown output column");
        }
    }
    if output.uniqueness == UniquenessGuaranteeV1::CandidateKeys && output.candidate_keys.is_empty()
    {
        return invalid("candidate-key uniqueness requires a candidate key");
    }
    if output.uniqueness == UniquenessGuaranteeV1::NotGuaranteed
        && !output.candidate_keys.is_empty()
    {
        return invalid("candidate keys require an explicit uniqueness guarantee");
    }
    if output.uniqueness == UniquenessGuaranteeV1::Singleton && !output.candidate_keys.is_empty() {
        return invalid("singleton uniqueness must not declare a candidate key");
    }
    if let ChangelogModeV1::Upsert { identity_key } = &output.changelog {
        validate_candidate_key(identity_key)?;
        if identity_key.equality != KeyEqualityV1::NonNullEquality
            || !output.candidate_keys.contains(identity_key)
        {
            return invalid("upsert identity must be a non-null candidate key");
        }
    }
    Ok(())
}

fn changelog_satisfies(provided: &ChangelogModeV1, accepted: &AcceptedChangelogV1) -> bool {
    match (provided, accepted) {
        (ChangelogModeV1::AppendOnly, _) => true,
        (ChangelogModeV1::Upsert { .. }, AcceptedChangelogV1::GeneralRetract) => true,
        (
            ChangelogModeV1::Upsert {
                identity_key: provided,
            },
            AcceptedChangelogV1::Upsert {
                identity_key: required,
            },
        ) => provided == required,
        (ChangelogModeV1::GeneralRetract, AcceptedChangelogV1::GeneralRetract) => true,
        _ => false,
    }
}

fn candidate_key_satisfies(provided: &CandidateKeyV1, required: &CandidateKeyV1) -> bool {
    provided.equality == required.equality
        && provided
            .columns
            .iter()
            .all(|column| required.columns.contains(column))
}

fn validate_candidate_key(key: &CandidateKeyV1) -> Result<(), OperatorContractError> {
    if key.columns.is_empty() || key.columns.iter().any(|column| column.trim().is_empty()) {
        return invalid("candidate keys must contain non-empty columns");
    }
    let unique = key.columns.iter().collect::<BTreeSet<_>>();
    if unique.len() != key.columns.len() {
        return invalid("candidate key columns must be unique");
    }
    Ok(())
}

fn require_identity(field: &str, value: &str) -> Result<(), OperatorContractError> {
    if value.trim().is_empty() {
        return invalid(format!("{field} must be non-empty"));
    }
    Ok(())
}

fn invalid<T>(reason: impl Into<String>) -> Result<T, OperatorContractError> {
    Err(OperatorContractError::Invalid(reason.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(columns: &[&str], equality: KeyEqualityV1) -> CandidateKeyV1 {
        CandidateKeyV1 {
            columns: columns.iter().map(|column| (*column).to_string()).collect(),
            equality,
        }
    }

    fn output(changelog: ChangelogModeV1) -> OutputPortContractV1 {
        OutputPortContractV1 {
            port_id: "out".into(),
            schema: RowSchemaV1 {
                columns: vec![
                    PortColumnV1 {
                        column_id: "id".into(),
                        logical_type: "utf8".into(),
                        nullability: NullabilityV1::NonNull,
                    },
                    PortColumnV1 {
                        column_id: "value".into(),
                        logical_type: "int64".into(),
                        nullability: NullabilityV1::Nullable,
                    },
                ],
            },
            changelog,
            candidate_keys: vec![],
            uniqueness: UniquenessGuaranteeV1::NotGuaranteed,
            determinism: DeterminismGuaranteeV1::ReplayDeterministic,
            progress: ProgressGuaranteeV1 {
                processing: ProcessingFrontierGuaranteeV1::PerInputCheckpointed,
                watermark: WatermarkGuaranteeV1::None,
            },
        }
    }

    fn input(accepted_changelog: AcceptedChangelogV1) -> InputPortContractV1 {
        InputPortContractV1 {
            port_id: "in".into(),
            accepted_changelog,
            required_columns: vec![],
            required_keys: vec![],
            required_determinism: DeterminismRequirementV1::Any,
            required_progress: ProgressRequirementV1 {
                processing: ProcessingFrontierRequirementV1::None,
                watermark: WatermarkRequirementV1::None,
            },
        }
    }

    #[test]
    fn changelog_compatibility_is_ordered_but_upsert_identity_requires_exact_match() {
        let id = key(&["id"], KeyEqualityV1::NonNullEquality);
        let other = key(&["value"], KeyEqualityV1::NonNullEquality);
        assert!(producer_satisfies_consumer(
            &output(ChangelogModeV1::AppendOnly),
            &input(AcceptedChangelogV1::GeneralRetract),
        )
        .is_ok());
        assert!(producer_satisfies_consumer(
            &output(ChangelogModeV1::GeneralRetract),
            &input(AcceptedChangelogV1::AppendOnly),
        )
        .is_err());
        assert!(producer_satisfies_consumer(
            &output(ChangelogModeV1::Upsert {
                identity_key: id.clone(),
            }),
            &input(AcceptedChangelogV1::Upsert { identity_key: id }),
        )
        .is_ok());
        assert!(producer_satisfies_consumer(
            &output(ChangelogModeV1::Upsert {
                identity_key: other,
            }),
            &input(AcceptedChangelogV1::Upsert {
                identity_key: key(&["id"], KeyEqualityV1::NonNullEquality),
            }),
        )
        .is_err());
    }

    #[test]
    fn candidate_key_subset_satisfies_wider_key_but_equality_semantics_must_match() {
        let mut producer = output(ChangelogModeV1::GeneralRetract);
        producer.candidate_keys = vec![key(&["id"], KeyEqualityV1::SqlNotDistinct)];
        producer.uniqueness = UniquenessGuaranteeV1::CandidateKeys;
        let mut consumer = input(AcceptedChangelogV1::GeneralRetract);
        consumer.required_keys = vec![key(&["id", "value"], KeyEqualityV1::SqlNotDistinct)];
        assert!(producer_satisfies_consumer(&producer, &consumer).is_ok());
        consumer.required_keys[0].equality = KeyEqualityV1::NonNullEquality;
        assert!(producer_satisfies_consumer(&producer, &consumer).is_err());
    }

    #[test]
    fn nullable_output_cannot_satisfy_non_null_requirement() {
        let producer = output(ChangelogModeV1::GeneralRetract);
        let mut consumer = input(AcceptedChangelogV1::GeneralRetract);
        consumer.required_columns.push(RequiredColumnV1 {
            column_id: "value".into(),
            nullability: NullabilityV1::NonNull,
        });
        assert!(producer_satisfies_consumer(&producer, &consumer).is_err());
        consumer.required_columns[0].nullability = NullabilityV1::Nullable;
        assert!(producer_satisfies_consumer(&producer, &consumer).is_ok());
    }

    #[test]
    fn processing_frontier_and_watermark_requirements_are_independent() {
        let mut producer = output(ChangelogModeV1::GeneralRetract);
        let mut consumer = input(AcceptedChangelogV1::GeneralRetract);
        consumer.required_progress.processing =
            ProcessingFrontierRequirementV1::PerInputCheckpointed;
        consumer.required_progress.watermark = WatermarkRequirementV1::Monotonic {
            event_time_column_id: "event_time".into(),
        };
        assert!(producer_satisfies_consumer(&producer, &consumer).is_err());
        producer.progress.watermark = WatermarkGuaranteeV1::Monotonic {
            event_time_column_id: "event_time".into(),
        };
        assert!(producer_satisfies_consumer(&producer, &consumer).is_ok());
        producer.progress.processing = ProcessingFrontierGuaranteeV1::None;
        assert!(producer_satisfies_consumer(&producer, &consumer).is_err());
    }

    #[test]
    fn state_contract_keeps_boundedness_separate_from_checkpoint_codec() {
        let contract = OperatorDagContractV1 {
            contract_version: OPERATOR_DAG_CONTRACT_VERSION_V1.into(),
            operators: vec![OperatorContractV1 {
                node_id: "top_k".into(),
                operator: OperatorKindIdentityV1 {
                    kind: "top_k".into(),
                    version: 1,
                },
                inputs: vec![],
                outputs: vec![output(ChangelogModeV1::GeneralRetract)],
                state: Some(StateContractV1 {
                    boundedness: StateBoundednessV1::Unbounded,
                    checkpoint_codec: CheckpointCodecIdentityV1 {
                        codec_id: "velorix-top-k-state".into(),
                        codec_version: 1,
                    },
                }),
            }],
            edges: vec![],
        };
        assert!(contract.validate().is_ok());
        let encoded = serde_json::to_vec(&contract).unwrap();
        assert_eq!(
            serde_json::from_slice::<OperatorDagContractV1>(&encoded).unwrap(),
            contract
        );
    }
}

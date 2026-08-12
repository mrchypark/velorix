//! Composable native delta operators with one checkpoint/replay envelope.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    delta::{DeltaBatch, DeltaError, DeltaRecord},
    operator::{
        filter_delta_batch, map_delta_batch, AggregateValueMode, KeyedEquiJoin,
        KeyedSumCountAggregate, OperatorError,
    },
};

pub const NATIVE_OPERATOR_CHECKPOINT_SCHEMA_VERSION_V1: u32 = 1;
pub type NativeSortKeyV1 = Vec<u8>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOperatorGraphCheckpointV1 {
    pub schema_version: u32,
    pub logical_epoch: u64,
    pub operators: Vec<NativeOperatorCheckpointV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOperatorCheckpointV1 {
    pub node_id: String,
    pub codec_id: String,
    pub codec_version: u32,
    pub state: NativeOperatorStateV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NativeOperatorStateV1 {
    Stateless,
    Unary {
        state: DeltaBatch,
    },
    Binary {
        left_state: DeltaBatch,
        right_state: DeltaBatch,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOperatorEdgeV1 {
    pub from_node_id: String,
    pub to_node_id: String,
    pub to_port_id: String,
}

#[derive(Debug, Error)]
pub enum NativeOperatorError {
    #[error(transparent)]
    Operator(#[from] OperatorError),
    #[error(transparent)]
    Delta(#[from] DeltaError),
    #[error("native operator graph is invalid: {0}")]
    InvalidGraph(String),
    #[error("native operator checkpoint is invalid: {0}")]
    InvalidCheckpoint(String),
    #[error("logical epoch must increase: current={current}, attempted={attempted}")]
    NonMonotonicEpoch { current: u64, attempted: u64 },
    #[error("top-k state contains a negative multiplicity")]
    NegativeTopKMultiplicity,
    #[error("left-join state contains a negative multiplicity")]
    NegativeLeftJoinMultiplicity,
    #[error("full-join state contains a negative multiplicity")]
    NegativeFullJoinMultiplicity,
    #[error("semi-join state contains a negative multiplicity")]
    NegativeSemiJoinMultiplicity,
    #[error("anti-join state contains a negative multiplicity")]
    NegativeAntiJoinMultiplicity,
}

pub trait NativeDeltaOperator: Send {
    fn node_id(&self) -> &str;
    fn input_ports(&self) -> &[&'static str];
    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError>;
    fn checkpoint(&self) -> NativeOperatorCheckpointV1;
    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError>;
}

pub struct NativeOperatorGraph {
    logical_epoch: u64,
    operators: BTreeMap<String, Box<dyn NativeDeltaOperator>>,
    edges: Vec<NativeOperatorEdgeV1>,
}

impl Default for NativeOperatorGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeOperatorGraph {
    pub fn new() -> Self {
        Self {
            logical_epoch: 0,
            operators: BTreeMap::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_operator(
        &mut self,
        operator: impl NativeDeltaOperator + 'static,
    ) -> Result<(), NativeOperatorError> {
        let node_id = operator.node_id().to_string();
        require_identity("node_id", &node_id).map_err(NativeOperatorError::InvalidGraph)?;
        if self.operators.contains_key(&node_id) {
            return Err(NativeOperatorError::InvalidGraph(
                "operator node ids must be unique".into(),
            ));
        }
        self.operators.insert(node_id, Box::new(operator));
        Ok(())
    }

    pub fn add_edge(&mut self, edge: NativeOperatorEdgeV1) {
        self.edges.push(edge);
        self.edges.sort();
    }

    pub fn validate(&self) -> Result<(), NativeOperatorError> {
        if self.operators.is_empty() {
            return Err(NativeOperatorError::InvalidGraph(
                "at least one operator is required".into(),
            ));
        }
        let mut unique_edges = BTreeSet::new();
        for edge in &self.edges {
            if !unique_edges.insert(edge) {
                return Err(NativeOperatorError::InvalidGraph(
                    "operator edges must be unique".into(),
                ));
            }
            if !self.operators.contains_key(&edge.from_node_id) {
                return Err(NativeOperatorError::InvalidGraph(
                    "edge producer is missing".into(),
                ));
            }
            let Some(consumer) = self.operators.get(&edge.to_node_id) else {
                return Err(NativeOperatorError::InvalidGraph(
                    "edge consumer is missing".into(),
                ));
            };
            if !consumer.input_ports().contains(&edge.to_port_id.as_str()) {
                return Err(NativeOperatorError::InvalidGraph(format!(
                    "operator {} does not accept input port {}",
                    edge.to_node_id, edge.to_port_id
                )));
            }
        }
        validate_acyclic(&self.operators, &self.edges)
    }

    pub fn apply_epoch(
        &mut self,
        logical_epoch: u64,
        inputs: Vec<NativeOperatorInputV1>,
    ) -> Result<BTreeMap<String, DeltaBatch>, NativeOperatorError> {
        self.validate()?;
        if logical_epoch <= self.logical_epoch {
            return Err(NativeOperatorError::NonMonotonicEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }
        let previous = self.checkpoint()?;
        match self.apply_epoch_inner(logical_epoch, inputs) {
            Ok(outputs) => Ok(outputs),
            Err(error) => {
                self.restore_inner(&previous).map_err(|rollback| {
                    NativeOperatorError::InvalidCheckpoint(format!(
                        "epoch failed with {error}; graph rollback failed with {rollback}"
                    ))
                })?;
                Err(error)
            }
        }
    }

    fn apply_epoch_inner(
        &mut self,
        logical_epoch: u64,
        mut inputs: Vec<NativeOperatorInputV1>,
    ) -> Result<BTreeMap<String, DeltaBatch>, NativeOperatorError> {
        inputs.sort_by(|left, right| {
            (&left.node_id, &left.port_id).cmp(&(&right.node_id, &right.port_id))
        });
        let mut queue = VecDeque::from(inputs);
        let mut sinks = BTreeMap::<String, DeltaBatch>::new();
        while let Some(input) = queue.pop_front() {
            let operator = self.operators.get_mut(&input.node_id).ok_or_else(|| {
                NativeOperatorError::InvalidGraph(format!(
                    "input target operator {} is missing",
                    input.node_id
                ))
            })?;
            if !operator.input_ports().contains(&input.port_id.as_str()) {
                return Err(NativeOperatorError::InvalidGraph(format!(
                    "operator {} does not accept input port {}",
                    input.node_id, input.port_id
                )));
            }
            let output = operator.apply(&input.port_id, &input.batch)?;
            let outgoing = self
                .edges
                .iter()
                .filter(|edge| edge.from_node_id == input.node_id)
                .cloned()
                .collect::<Vec<_>>();
            if outgoing.is_empty() {
                let current = sinks.entry(input.node_id).or_default();
                *current = current.combine(&output);
            } else {
                for edge in outgoing {
                    queue.push_back(NativeOperatorInputV1 {
                        node_id: edge.to_node_id,
                        port_id: edge.to_port_id,
                        batch: output.clone(),
                    });
                }
            }
        }
        self.logical_epoch = logical_epoch;
        for output in sinks.values_mut() {
            *output = DeltaBatch::from_records(output.net_rows()?);
        }
        Ok(sinks)
    }

    pub fn checkpoint(&self) -> Result<NativeOperatorGraphCheckpointV1, NativeOperatorError> {
        self.validate()?;
        Ok(NativeOperatorGraphCheckpointV1 {
            schema_version: NATIVE_OPERATOR_CHECKPOINT_SCHEMA_VERSION_V1,
            logical_epoch: self.logical_epoch,
            operators: self
                .operators
                .values()
                .map(|operator| operator.checkpoint())
                .collect(),
        })
    }

    pub fn restore(
        &mut self,
        checkpoint: &NativeOperatorGraphCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        self.validate()?;
        let previous = self.checkpoint()?;
        match self.restore_inner(checkpoint) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.restore_inner(&previous).map_err(|rollback| {
                    NativeOperatorError::InvalidCheckpoint(format!(
                        "restore failed with {error}; graph rollback failed with {rollback}"
                    ))
                })?;
                Err(error)
            }
        }
    }

    fn restore_inner(
        &mut self,
        checkpoint: &NativeOperatorGraphCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        if checkpoint.schema_version != NATIVE_OPERATOR_CHECKPOINT_SCHEMA_VERSION_V1 {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "unsupported checkpoint schema version".into(),
            ));
        }
        if checkpoint.operators.len() != self.operators.len() {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "checkpoint operator set does not match graph".into(),
            ));
        }
        let checkpoints = checkpoint
            .operators
            .iter()
            .map(|operator| (operator.node_id.as_str(), operator))
            .collect::<BTreeMap<_, _>>();
        for (node_id, operator) in &mut self.operators {
            let state = checkpoints.get(node_id.as_str()).ok_or_else(|| {
                NativeOperatorError::InvalidCheckpoint(format!(
                    "checkpoint for operator {node_id} is missing"
                ))
            })?;
            operator.restore(state)?;
        }
        self.logical_epoch = checkpoint.logical_epoch;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeOperatorInputV1 {
    pub node_id: String,
    pub port_id: String,
    pub batch: DeltaBatch,
}

pub struct NativeFilterOperator<F>
where
    F: Fn(&DeltaRecord) -> Result<bool, OperatorError> + Send,
{
    node_id: String,
    predicate: F,
}

impl<F> NativeFilterOperator<F>
where
    F: Fn(&DeltaRecord) -> Result<bool, OperatorError> + Send,
{
    pub fn new(node_id: impl Into<String>, predicate: F) -> Self {
        Self {
            node_id: node_id.into(),
            predicate,
        }
    }
}

impl<F> NativeDeltaOperator for NativeFilterOperator<F>
where
    F: Fn(&DeltaRecord) -> Result<bool, OperatorError> + Send,
{
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["input"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        require_port(port_id, "input")?;
        Ok(filter_delta_batch(input, &mut self.predicate)?)
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        stateless_checkpoint(&self.node_id, "velorix-native-filter-v1")
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_identity(
            checkpoint,
            &self.node_id,
            "velorix-native-filter-v1",
            &NativeOperatorStateV1::Stateless,
        )
    }
}

pub struct NativeProjectOperator<F>
where
    F: Fn(
            &DeltaRecord,
        ) -> Result<(crate::delta::DeltaKey, crate::delta::DeltaValue), OperatorError>
        + Send,
{
    node_id: String,
    project: F,
}

impl<F> NativeProjectOperator<F>
where
    F: Fn(
            &DeltaRecord,
        ) -> Result<(crate::delta::DeltaKey, crate::delta::DeltaValue), OperatorError>
        + Send,
{
    pub fn new(node_id: impl Into<String>, project: F) -> Self {
        Self {
            node_id: node_id.into(),
            project,
        }
    }
}

impl<F> NativeDeltaOperator for NativeProjectOperator<F>
where
    F: Fn(
            &DeltaRecord,
        ) -> Result<(crate::delta::DeltaKey, crate::delta::DeltaValue), OperatorError>
        + Send,
{
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["input"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        require_port(port_id, "input")?;
        Ok(map_delta_batch(input, &mut self.project)?)
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        stateless_checkpoint(&self.node_id, "velorix-native-project-v1")
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_identity(
            checkpoint,
            &self.node_id,
            "velorix-native-project-v1",
            &NativeOperatorStateV1::Stateless,
        )
    }
}

pub struct NativeAggregateOperator {
    node_id: String,
    aggregate: KeyedSumCountAggregate,
    value_mode: AggregateValueMode,
    track_extrema: bool,
}

impl NativeAggregateOperator {
    pub fn new(
        node_id: impl Into<String>,
        value_mode: AggregateValueMode,
        track_extrema: bool,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            aggregate: KeyedSumCountAggregate::with_value_mode_and_extrema(
                value_mode,
                track_extrema,
            ),
            value_mode,
            track_extrema,
        }
    }
}

impl NativeDeltaOperator for NativeAggregateOperator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["input"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        require_port(port_id, "input")?;
        Ok(self.aggregate.apply(input)?)
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-aggregate-v1".into(),
            codec_version: 1,
            state: NativeOperatorStateV1::Unary {
                state: self.aggregate.state(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_header(checkpoint, &self.node_id, "velorix-native-aggregate-v1")?;
        let NativeOperatorStateV1::Unary { state } = &checkpoint.state else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "aggregate checkpoint requires unary state".into(),
            ));
        };
        self.aggregate = KeyedSumCountAggregate::from_state_with_value_mode_and_extrema(
            state,
            self.value_mode,
            self.track_extrema,
        )?;
        Ok(())
    }
}

pub struct NativeBinaryJoinOperator<F>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
{
    node_id: String,
    join: KeyedEquiJoin<F>,
}

/// A duplicate-aware binary semi join.
///
/// Left rows are emitted with their original multiplicity whenever the total
/// right multiplicity for the key is positive. Additional right duplicates do
/// not duplicate the output; only zero-to-positive and positive-to-zero right
/// match-count transitions emit the retained left bag.
pub struct NativeSemiJoinOperator {
    node_id: String,
    left_state: DeltaBatch,
    right_state: DeltaBatch,
}

impl NativeSemiJoinOperator {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            left_state: DeltaBatch::default(),
            right_state: DeltaBatch::default(),
        }
    }

    fn validate_non_negative(state: &DeltaBatch) -> Result<(), NativeOperatorError> {
        if state.net_rows()?.iter().any(|row| row.weight < 0) {
            return Err(NativeOperatorError::NegativeSemiJoinMultiplicity);
        }
        Ok(())
    }

    fn right_count_for_key(
        state: &DeltaBatch,
        key: &crate::delta::DeltaKey,
    ) -> Result<i64, NativeOperatorError> {
        let mut count = 0_i64;
        for row in state.net_rows()?.into_iter().filter(|row| row.key == *key) {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeSemiJoinMultiplicity);
            }
            count = count
                .checked_add(row.weight)
                .ok_or(OperatorError::WeightOverflow)?;
        }
        Ok(count)
    }

    fn retained_left_rows(
        &self,
        key: &crate::delta::DeltaKey,
        sign: i64,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        let rows = self
            .left_state
            .net_rows()?
            .into_iter()
            .filter(|row| row.key == *key)
            .map(|row| {
                if row.weight < 0 {
                    return Err(NativeOperatorError::NegativeSemiJoinMultiplicity);
                }
                Ok(DeltaRecord::new(
                    row.key,
                    row.value,
                    row.weight
                        .checked_mul(sign)
                        .ok_or(OperatorError::WeightOverflow)?,
                ))
            })
            .collect::<Result<Vec<_>, NativeOperatorError>>()?;
        Ok(DeltaBatch::from_records(rows))
    }
}

impl NativeDeltaOperator for NativeSemiJoinOperator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["left", "right"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        match port_id {
            "left" => {
                let next_left = self.left_state.combine(input);
                Self::validate_non_negative(&next_left)?;
                let output = input
                    .net_rows()?
                    .into_iter()
                    .filter_map(|row| {
                        match Self::right_count_for_key(&self.right_state, &row.key) {
                            Ok(0) => None,
                            Ok(_) => Some(Ok(row)),
                            Err(error) => Some(Err(error)),
                        }
                    })
                    .collect::<Result<Vec<_>, NativeOperatorError>>()?;
                self.left_state = DeltaBatch::from_records(next_left.net_rows()?);
                Ok(DeltaBatch::from_records(output))
            }
            "right" => {
                let next_right = self.right_state.combine(input);
                Self::validate_non_negative(&next_right)?;
                let touched_keys = input
                    .records()
                    .iter()
                    .map(|row| (canonical_json(row.key.as_json()), row.key.clone()))
                    .collect::<BTreeMap<_, _>>();
                let mut output = DeltaBatch::default();
                for key in touched_keys.into_values() {
                    let before = Self::right_count_for_key(&self.right_state, &key)?;
                    let after = Self::right_count_for_key(&next_right, &key)?;
                    match (before == 0, after == 0) {
                        (true, false) => {
                            output = output.combine(&self.retained_left_rows(&key, 1)?);
                        }
                        (false, true) => {
                            output = output.combine(&self.retained_left_rows(&key, -1)?);
                        }
                        _ => {}
                    }
                }
                self.right_state = DeltaBatch::from_records(next_right.net_rows()?);
                Ok(DeltaBatch::from_records(output.net_rows()?))
            }
            _ => Err(NativeOperatorError::InvalidGraph(format!(
                "semi join operator {} does not accept input port {port_id}",
                self.node_id
            ))),
        }
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-semi-join-v1".into(),
            codec_version: 1,
            state: NativeOperatorStateV1::Binary {
                left_state: self.left_state.clone(),
                right_state: self.right_state.clone(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_header(checkpoint, &self.node_id, "velorix-native-semi-join-v1")?;
        let NativeOperatorStateV1::Binary {
            left_state,
            right_state,
        } = &checkpoint.state
        else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "semi join checkpoint requires left and right state".into(),
            ));
        };
        if left_state.net_rows()?.iter().any(|row| row.weight < 0)
            || right_state.net_rows()?.iter().any(|row| row.weight < 0)
        {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "semi join checkpoint contains negative multiplicity".into(),
            ));
        }
        self.left_state = DeltaBatch::from_records(left_state.net_rows()?);
        self.right_state = DeltaBatch::from_records(right_state.net_rows()?);
        Ok(())
    }
}

/// A duplicate-aware ordinary binary anti join.
///
/// Left rows are emitted with their original multiplicity exactly while the
/// total right multiplicity for the key is zero. The first right match retracts
/// the retained left bag, additional matches are silent, and deletion of the
/// final match restores the current retained left bag.
pub struct NativeAntiJoinOperator {
    node_id: String,
    left_state: DeltaBatch,
    right_state: DeltaBatch,
}

impl NativeAntiJoinOperator {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            left_state: DeltaBatch::default(),
            right_state: DeltaBatch::default(),
        }
    }

    fn validate_non_negative(state: &DeltaBatch) -> Result<(), NativeOperatorError> {
        if state.net_rows()?.iter().any(|row| row.weight < 0) {
            return Err(NativeOperatorError::NegativeAntiJoinMultiplicity);
        }
        Ok(())
    }

    fn right_count_for_key(
        state: &DeltaBatch,
        key: &crate::delta::DeltaKey,
    ) -> Result<i64, NativeOperatorError> {
        let mut count = 0_i64;
        for row in state.net_rows()?.into_iter().filter(|row| row.key == *key) {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeAntiJoinMultiplicity);
            }
            count = count
                .checked_add(row.weight)
                .ok_or(OperatorError::WeightOverflow)?;
        }
        Ok(count)
    }

    fn retained_left_rows(
        &self,
        key: &crate::delta::DeltaKey,
        sign: i64,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        let rows = self
            .left_state
            .net_rows()?
            .into_iter()
            .filter(|row| row.key == *key)
            .map(|row| {
                if row.weight < 0 {
                    return Err(NativeOperatorError::NegativeAntiJoinMultiplicity);
                }
                Ok(DeltaRecord::new(
                    row.key,
                    row.value,
                    row.weight
                        .checked_mul(sign)
                        .ok_or(OperatorError::WeightOverflow)?,
                ))
            })
            .collect::<Result<Vec<_>, NativeOperatorError>>()?;
        Ok(DeltaBatch::from_records(rows))
    }
}

impl NativeDeltaOperator for NativeAntiJoinOperator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["left", "right"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        match port_id {
            "left" => {
                let next_left = self.left_state.combine(input);
                Self::validate_non_negative(&next_left)?;
                let output = input
                    .net_rows()?
                    .into_iter()
                    .filter_map(|row| {
                        match Self::right_count_for_key(&self.right_state, &row.key) {
                            Ok(0) => Some(Ok(row)),
                            Ok(_) => None,
                            Err(error) => Some(Err(error)),
                        }
                    })
                    .collect::<Result<Vec<_>, NativeOperatorError>>()?;
                self.left_state = DeltaBatch::from_records(next_left.net_rows()?);
                Ok(DeltaBatch::from_records(output))
            }
            "right" => {
                let next_right = self.right_state.combine(input);
                Self::validate_non_negative(&next_right)?;
                let touched_keys = input
                    .records()
                    .iter()
                    .map(|row| (canonical_json(row.key.as_json()), row.key.clone()))
                    .collect::<BTreeMap<_, _>>();
                let mut output = DeltaBatch::default();
                for key in touched_keys.into_values() {
                    let before = Self::right_count_for_key(&self.right_state, &key)?;
                    let after = Self::right_count_for_key(&next_right, &key)?;
                    match (before == 0, after == 0) {
                        (true, false) => {
                            output = output.combine(&self.retained_left_rows(&key, -1)?);
                        }
                        (false, true) => {
                            output = output.combine(&self.retained_left_rows(&key, 1)?);
                        }
                        _ => {}
                    }
                }
                self.right_state = DeltaBatch::from_records(next_right.net_rows()?);
                Ok(DeltaBatch::from_records(output.net_rows()?))
            }
            _ => Err(NativeOperatorError::InvalidGraph(format!(
                "anti join operator {} does not accept input port {port_id}",
                self.node_id
            ))),
        }
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-anti-join-v1".into(),
            codec_version: 1,
            state: NativeOperatorStateV1::Binary {
                left_state: self.left_state.clone(),
                right_state: self.right_state.clone(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_header(checkpoint, &self.node_id, "velorix-native-anti-join-v1")?;
        let NativeOperatorStateV1::Binary {
            left_state,
            right_state,
        } = &checkpoint.state
        else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "anti join checkpoint requires left and right state".into(),
            ));
        };
        if left_state.net_rows()?.iter().any(|row| row.weight < 0)
            || right_state.net_rows()?.iter().any(|row| row.weight < 0)
        {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "anti join checkpoint contains negative multiplicity".into(),
            ));
        }
        self.left_state = DeltaBatch::from_records(left_state.net_rows()?);
        self.right_state = DeltaBatch::from_records(right_state.net_rows()?);
        Ok(())
    }
}

/// A general-retract left outer equi-join.
///
/// The operator retains both inputs. A right-side transition between zero and
/// non-zero matches retracts or inserts the null-extended left row, while the
/// ordinary binary join supplies matched-row deltas.
pub struct NativeLeftJoinOperator<F, U>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
    U: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
{
    node_id: String,
    join: KeyedEquiJoin<F>,
    unmatched_value: U,
}

impl<F, U> NativeLeftJoinOperator<F, U>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
    U: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
{
    pub fn new(node_id: impl Into<String>, join_values: F, unmatched_value: U) -> Self {
        Self {
            node_id: node_id.into(),
            join: KeyedEquiJoin::new(join_values),
            unmatched_value,
        }
    }

    fn right_count_for_key(
        state: &DeltaBatch,
        key: &crate::delta::DeltaKey,
    ) -> Result<i64, NativeOperatorError> {
        let mut count = 0_i64;
        for row in state.net_rows()?.into_iter().filter(|row| row.key == *key) {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeLeftJoinMultiplicity);
            }
            count = count
                .checked_add(row.weight)
                .ok_or(OperatorError::WeightOverflow)?;
        }
        Ok(count)
    }

    fn unmatched_rows(
        &self,
        left: &DeltaBatch,
        key: &crate::delta::DeltaKey,
        sign: i64,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        let mut rows = Vec::new();
        for row in left.net_rows()?.into_iter().filter(|row| row.key == *key) {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeLeftJoinMultiplicity);
            }
            rows.push(DeltaRecord::new(
                row.key,
                (self.unmatched_value)(&row.value)?,
                row.weight
                    .checked_mul(sign)
                    .ok_or(OperatorError::WeightOverflow)?,
            ));
        }
        Ok(DeltaBatch::from_records(rows))
    }
}

impl<F, U> NativeDeltaOperator for NativeLeftJoinOperator<F, U>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
    U: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
{
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["left", "right"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        match port_id {
            "left" => {
                let right_state = self.join.right_state();
                let joined = self.join.apply_left(input)?;
                let mut output = joined;
                for row in input.net_rows()? {
                    if Self::right_count_for_key(&right_state, &row.key)? == 0 {
                        output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                            row.key,
                            (self.unmatched_value)(&row.value)?,
                            row.weight,
                        )]));
                    }
                }
                Ok(DeltaBatch::from_records(output.net_rows()?))
            }
            "right" => {
                let before = self.join.right_state();
                let joined = self.join.apply_right(input)?;
                let after = self.join.right_state();
                let left = self.join.left_state();
                let touched_keys = input
                    .records()
                    .iter()
                    .map(|row| (canonical_json(row.key.as_json()), row.key.clone()))
                    .collect::<BTreeMap<_, _>>();
                let mut output = joined;
                for key in touched_keys.into_values() {
                    match (
                        Self::right_count_for_key(&before, &key)? == 0,
                        Self::right_count_for_key(&after, &key)? == 0,
                    ) {
                        (true, false) => {
                            output = output.combine(&self.unmatched_rows(&left, &key, -1)?);
                        }
                        (false, true) => {
                            output = output.combine(&self.unmatched_rows(&left, &key, 1)?);
                        }
                        _ => {}
                    }
                }
                Ok(DeltaBatch::from_records(output.net_rows()?))
            }
            _ => Err(NativeOperatorError::InvalidGraph(format!(
                "left join operator {} does not accept input port {port_id}",
                self.node_id
            ))),
        }
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-left-join-v1".into(),
            codec_version: 1,
            state: NativeOperatorStateV1::Binary {
                left_state: self.join.left_state(),
                right_state: self.join.right_state(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_header(checkpoint, &self.node_id, "velorix-native-left-join-v1")?;
        let NativeOperatorStateV1::Binary {
            left_state,
            right_state,
        } = &checkpoint.state
        else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "left join checkpoint requires left and right state".into(),
            ));
        };
        if left_state.net_rows()?.iter().any(|row| row.weight < 0)
            || right_state.net_rows()?.iter().any(|row| row.weight < 0)
        {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "left join checkpoint contains negative multiplicity".into(),
            ));
        }
        self.join.restore_state(left_state, right_state)?;
        Ok(())
    }
}

/// A general-retract full outer equi-join.
///
/// Both input bags are retained. Each side emits its own null-extended rows
/// while the opposite match count is zero and retracts or restores the other
/// side's null-extended rows when its count crosses the zero boundary.
pub struct NativeFullJoinOperator<F, UL, UR>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
    UL: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
    UR: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
{
    node_id: String,
    join: KeyedEquiJoin<F>,
    unmatched_left_value: UL,
    unmatched_right_value: UR,
}

impl<F, UL, UR> NativeFullJoinOperator<F, UL, UR>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
    UL: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
    UR: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
{
    pub fn new(
        node_id: impl Into<String>,
        join_values: F,
        unmatched_left_value: UL,
        unmatched_right_value: UR,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            join: KeyedEquiJoin::new(join_values),
            unmatched_left_value,
            unmatched_right_value,
        }
    }

    fn count_for_key(
        state: &DeltaBatch,
        key: &crate::delta::DeltaKey,
    ) -> Result<i64, NativeOperatorError> {
        let mut count = 0_i64;
        for row in state.net_rows()?.into_iter().filter(|row| row.key == *key) {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeFullJoinMultiplicity);
            }
            count = count
                .checked_add(row.weight)
                .ok_or(OperatorError::WeightOverflow)?;
        }
        Ok(count)
    }

    fn unmatched_left_rows(
        &self,
        left: &DeltaBatch,
        key: &crate::delta::DeltaKey,
        sign: i64,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        let mut rows = Vec::new();
        for row in left.net_rows()?.into_iter().filter(|row| row.key == *key) {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeFullJoinMultiplicity);
            }
            rows.push(DeltaRecord::new(
                row.key,
                (self.unmatched_left_value)(&row.value)?,
                row.weight
                    .checked_mul(sign)
                    .ok_or(OperatorError::WeightOverflow)?,
            ));
        }
        Ok(DeltaBatch::from_records(rows))
    }

    fn unmatched_right_rows(
        &self,
        right: &DeltaBatch,
        key: &crate::delta::DeltaKey,
        sign: i64,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        let mut rows = Vec::new();
        for row in right.net_rows()?.into_iter().filter(|row| row.key == *key) {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeFullJoinMultiplicity);
            }
            rows.push(DeltaRecord::new(
                row.key,
                (self.unmatched_right_value)(&row.value)?,
                row.weight
                    .checked_mul(sign)
                    .ok_or(OperatorError::WeightOverflow)?,
            ));
        }
        Ok(DeltaBatch::from_records(rows))
    }
}

impl<F, UL, UR> NativeDeltaOperator for NativeFullJoinOperator<F, UL, UR>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
    UL: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
    UR: Fn(&crate::delta::DeltaValue) -> Result<crate::delta::DeltaValue, OperatorError> + Send,
{
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["left", "right"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        let touched_keys = input
            .records()
            .iter()
            .map(|row| (canonical_json(row.key.as_json()), row.key.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut output = match port_id {
            "left" => {
                let before = self.join.left_state();
                let right = self.join.right_state();
                let mut output = self.join.apply_left(input)?;
                let after = self.join.left_state();
                for row in input.net_rows()? {
                    if Self::count_for_key(&right, &row.key)? == 0 {
                        output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                            row.key,
                            (self.unmatched_left_value)(&row.value)?,
                            row.weight,
                        )]));
                    }
                }
                for key in touched_keys.into_values() {
                    let sign = match (
                        Self::count_for_key(&before, &key)? == 0,
                        Self::count_for_key(&after, &key)? == 0,
                    ) {
                        (true, false) => -1,
                        (false, true) => 1,
                        _ => continue,
                    };
                    output = output.combine(&self.unmatched_right_rows(&right, &key, sign)?);
                }
                output
            }
            "right" => {
                let before = self.join.right_state();
                let left = self.join.left_state();
                let mut output = self.join.apply_right(input)?;
                let after = self.join.right_state();
                for row in input.net_rows()? {
                    if Self::count_for_key(&left, &row.key)? == 0 {
                        output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                            row.key,
                            (self.unmatched_right_value)(&row.value)?,
                            row.weight,
                        )]));
                    }
                }
                for key in touched_keys.into_values() {
                    let sign = match (
                        Self::count_for_key(&before, &key)? == 0,
                        Self::count_for_key(&after, &key)? == 0,
                    ) {
                        (true, false) => -1,
                        (false, true) => 1,
                        _ => continue,
                    };
                    output = output.combine(&self.unmatched_left_rows(&left, &key, sign)?);
                }
                output
            }
            _ => {
                return Err(NativeOperatorError::InvalidGraph(format!(
                    "full join operator {} does not accept input port {port_id}",
                    self.node_id
                )))
            }
        };
        output = DeltaBatch::from_records(output.net_rows()?);
        Ok(output)
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-full-join-v1".into(),
            codec_version: 1,
            state: NativeOperatorStateV1::Binary {
                left_state: self.join.left_state(),
                right_state: self.join.right_state(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_header(checkpoint, &self.node_id, "velorix-native-full-join-v1")?;
        let NativeOperatorStateV1::Binary {
            left_state,
            right_state,
        } = &checkpoint.state
        else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "full join checkpoint requires left and right state".into(),
            ));
        };
        if left_state.net_rows()?.iter().any(|row| row.weight < 0)
            || right_state.net_rows()?.iter().any(|row| row.weight < 0)
        {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "full join checkpoint contains negative multiplicity".into(),
            ));
        }
        self.join.restore_state(left_state, right_state)?;
        Ok(())
    }
}

impl<F> NativeBinaryJoinOperator<F>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
{
    pub fn new(node_id: impl Into<String>, join_values: F) -> Self {
        Self {
            node_id: node_id.into(),
            join: KeyedEquiJoin::new(join_values),
        }
    }
}

impl<F> NativeDeltaOperator for NativeBinaryJoinOperator<F>
where
    F: Fn(
            &crate::delta::DeltaValue,
            &crate::delta::DeltaValue,
        ) -> Result<crate::delta::DeltaValue, OperatorError>
        + Send,
{
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["left", "right"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        match port_id {
            "left" => Ok(self.join.apply_left(input)?),
            "right" => Ok(self.join.apply_right(input)?),
            _ => Err(NativeOperatorError::InvalidGraph(format!(
                "join operator {} does not accept input port {port_id}",
                self.node_id
            ))),
        }
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-binary-join-v1".into(),
            codec_version: 1,
            state: NativeOperatorStateV1::Binary {
                left_state: self.join.left_state(),
                right_state: self.join.right_state(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_header(checkpoint, &self.node_id, "velorix-native-binary-join-v1")?;
        let NativeOperatorStateV1::Binary {
            left_state,
            right_state,
        } = &checkpoint.state
        else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "binary join checkpoint requires left and right state".into(),
            ));
        };
        self.join.restore_state(left_state, right_state)?;
        Ok(())
    }
}

#[allow(clippy::type_complexity)]
pub struct NativeTopKOperator {
    node_id: String,
    offset: usize,
    limit: usize,
    descending: bool,
    order_key: Box<dyn Fn(&DeltaRecord) -> Result<NativeSortKeyV1, OperatorError> + Send>,
    input_state: DeltaBatch,
    output_state: DeltaBatch,
}

impl NativeTopKOperator {
    pub fn new(
        node_id: impl Into<String>,
        offset: usize,
        limit: usize,
        descending: bool,
        order_key: impl Fn(&DeltaRecord) -> Result<NativeSortKeyV1, OperatorError> + Send + 'static,
    ) -> Result<Self, NativeOperatorError> {
        if limit == 0 {
            return Err(NativeOperatorError::InvalidGraph(
                "top-k limit must be positive".into(),
            ));
        }
        Ok(Self {
            node_id: node_id.into(),
            offset,
            limit,
            descending,
            order_key: Box::new(order_key),
            input_state: DeltaBatch::default(),
            output_state: DeltaBatch::default(),
        })
    }

    fn selected_output_from(
        &self,
        input_state: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        let mut rows = Vec::new();
        for row in input_state.net_rows()? {
            if row.weight < 0 {
                return Err(NativeOperatorError::NegativeTopKMultiplicity);
            }
            rows.push(((self.order_key)(&row)?, canonical_record(&row), row));
        }
        rows.sort_by(|left, right| {
            if self.descending {
                (&right.0, &right.1).cmp(&(&left.0, &left.1))
            } else {
                (&left.0, &left.1).cmp(&(&right.0, &right.1))
            }
        });
        let mut skipped = self.offset as u64;
        let mut remaining = self.limit as u64;
        let mut selected = Vec::new();
        for (_, _, mut row) in rows {
            let multiplicity = row.weight as u64;
            let skip = skipped.min(multiplicity);
            skipped -= skip;
            let available = multiplicity - skip;
            let take = remaining.min(available);
            if take > 0 {
                row.weight = take.try_into().map_err(|_| DeltaError::WeightOverflow)?;
                selected.push(row);
                remaining -= take;
            }
            if remaining == 0 {
                break;
            }
        }
        Ok(DeltaBatch::from_records(selected))
    }
}

impl NativeDeltaOperator for NativeTopKOperator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["input"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        require_port(port_id, "input")?;
        let next_input = DeltaBatch::from_records(self.input_state.combine(input).net_rows()?);
        let next_output = self.selected_output_from(&next_input)?;
        let delta = self.output_state.inverse()?.combine(&next_output);
        self.input_state = next_input;
        self.output_state = next_output;
        Ok(DeltaBatch::from_records(delta.net_rows()?))
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-top-k-v1".into(),
            codec_version: 1,
            state: NativeOperatorStateV1::Binary {
                left_state: self.input_state.clone(),
                right_state: self.output_state.clone(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        validate_checkpoint_header(checkpoint, &self.node_id, "velorix-native-top-k-v1")?;
        let NativeOperatorStateV1::Binary {
            left_state,
            right_state,
        } = &checkpoint.state
        else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "top-k checkpoint requires input and output state".into(),
            ));
        };
        let restored_input = DeltaBatch::from_records(left_state.net_rows()?);
        let restored_output = DeltaBatch::from_records(right_state.net_rows()?);
        if self.selected_output_from(&restored_input)? != restored_output {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "top-k output state does not match input state".into(),
            ));
        }
        self.input_state = restored_input;
        self.output_state = restored_output;
        Ok(())
    }
}

fn stateless_checkpoint(node_id: &str, codec_id: &str) -> NativeOperatorCheckpointV1 {
    NativeOperatorCheckpointV1 {
        node_id: node_id.to_string(),
        codec_id: codec_id.to_string(),
        codec_version: 1,
        state: NativeOperatorStateV1::Stateless,
    }
}

fn validate_checkpoint_identity(
    checkpoint: &NativeOperatorCheckpointV1,
    node_id: &str,
    codec_id: &str,
    state: &NativeOperatorStateV1,
) -> Result<(), NativeOperatorError> {
    validate_checkpoint_header(checkpoint, node_id, codec_id)?;
    if &checkpoint.state != state {
        return Err(NativeOperatorError::InvalidCheckpoint(
            "stateless operator checkpoint contains state".into(),
        ));
    }
    Ok(())
}

fn validate_checkpoint_header(
    checkpoint: &NativeOperatorCheckpointV1,
    node_id: &str,
    codec_id: &str,
) -> Result<(), NativeOperatorError> {
    if checkpoint.node_id != node_id
        || checkpoint.codec_id != codec_id
        || checkpoint.codec_version != 1
    {
        return Err(NativeOperatorError::InvalidCheckpoint(
            "operator checkpoint identity or codec does not match".into(),
        ));
    }
    Ok(())
}

fn require_port(port_id: &str, expected: &str) -> Result<(), NativeOperatorError> {
    if port_id != expected {
        return Err(NativeOperatorError::InvalidGraph(format!(
            "expected input port {expected}, found {port_id}"
        )));
    }
    Ok(())
}

fn require_identity(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must be non-empty"));
    }
    Ok(())
}

fn canonical_record(record: &DeltaRecord) -> (String, String) {
    (
        canonical_json(record.key.as_json()),
        canonical_json(record.value.as_json()),
    )
}

fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => serde_json::to_string(value).unwrap(),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        serde_json::Value::Object(values) => {
            let mut fields = values
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        canonical_json(value)
                    )
                })
                .collect::<Vec<_>>();
            fields.sort();
            format!("{{{}}}", fields.join(","))
        }
    }
}

fn validate_acyclic(
    operators: &BTreeMap<String, Box<dyn NativeDeltaOperator>>,
    edges: &[NativeOperatorEdgeV1],
) -> Result<(), NativeOperatorError> {
    let mut indegree = operators
        .keys()
        .map(|node_id| (node_id.clone(), 0usize))
        .collect::<BTreeMap<_, _>>();
    for edge in edges {
        *indegree.get_mut(&edge.to_node_id).unwrap() += 1;
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(node_id, degree)| (*degree == 0).then_some(node_id.clone()))
        .collect::<VecDeque<_>>();
    let mut visited = 0usize;
    while let Some(node_id) = ready.pop_front() {
        visited += 1;
        for edge in edges.iter().filter(|edge| edge.from_node_id == node_id) {
            let degree = indegree.get_mut(&edge.to_node_id).unwrap();
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(edge.to_node_id.clone());
            }
        }
    }
    if visited != operators.len() {
        return Err(NativeOperatorError::InvalidGraph(
            "operator graph contains a cycle".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::delta::{DeltaKey, DeltaValue};

    use super::*;

    fn row(key: &str, value: i64, weight: i64) -> DeltaRecord {
        DeltaRecord::new(
            DeltaKey::from_json(json!(key)),
            DeltaValue::from_json(json!(value)),
            weight,
        )
    }

    fn three_operator_graph() -> NativeOperatorGraph {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(NativeFilterOperator::new("filter", |record| {
                Ok(record.value.as_json().as_i64().unwrap() > 0)
            }))
            .unwrap();
        graph
            .add_operator(NativeProjectOperator::new("project", |record| {
                Ok((record.key.clone(), record.value.clone()))
            }))
            .unwrap();
        graph
            .add_operator(NativeAggregateOperator::new(
                "aggregate",
                AggregateValueMode::Integer,
                false,
            ))
            .unwrap();
        graph.add_edge(NativeOperatorEdgeV1 {
            from_node_id: "filter".into(),
            to_node_id: "project".into(),
            to_port_id: "input".into(),
        });
        graph.add_edge(NativeOperatorEdgeV1 {
            from_node_id: "project".into(),
            to_node_id: "aggregate".into(),
            to_port_id: "input".into(),
        });
        graph
    }

    #[test]
    fn filter_project_aggregate_composes_and_restores_through_one_checkpoint() {
        let initial = DeltaBatch::from_records([row("a", 3, 1), row("b", -2, 1)]);
        let mut uninterrupted = three_operator_graph();
        let first = uninterrupted
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "filter".into(),
                    port_id: "input".into(),
                    batch: initial,
                }],
            )
            .unwrap();
        assert_eq!(first["aggregate"].net_rows().unwrap().len(), 1);
        let checkpoint = uninterrupted.checkpoint().unwrap();

        let mut restored = three_operator_graph();
        restored.restore(&checkpoint).unwrap();
        let change = DeltaBatch::from_records([row("a", 3, -1), row("a", 5, 1)]);
        let expected = uninterrupted
            .apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "filter".into(),
                    port_id: "input".into(),
                    batch: change.clone(),
                }],
            )
            .unwrap();
        let actual = restored
            .apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "filter".into(),
                    port_id: "input".into(),
                    batch: change,
                }],
            )
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            restored.checkpoint().unwrap(),
            uninterrupted.checkpoint().unwrap()
        );
    }

    fn join_top_k_graph() -> NativeOperatorGraph {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(NativeBinaryJoinOperator::new("join", |left, right| {
                Ok(DeltaValue::from_json(json!({
                    "left": left.as_json(),
                    "right": right.as_json()
                })))
            }))
            .unwrap();
        graph
            .add_operator(
                NativeTopKOperator::new("top_k", 0, 1, false, |record| {
                    Ok(canonical_json(record.key.as_json()).into_bytes())
                })
                .unwrap(),
            )
            .unwrap();
        graph.add_edge(NativeOperatorEdgeV1 {
            from_node_id: "join".into(),
            to_node_id: "top_k".into(),
            to_port_id: "input".into(),
        });
        graph
    }

    #[test]
    fn binary_join_and_top_k_share_checkpoint_and_replay_contract() {
        let mut graph = join_top_k_graph();
        let output = graph
            .apply_epoch(
                1,
                vec![
                    NativeOperatorInputV1 {
                        node_id: "join".into(),
                        port_id: "left".into(),
                        batch: DeltaBatch::from_records([row("a", 1, 1), row("b", 2, 1)]),
                    },
                    NativeOperatorInputV1 {
                        node_id: "join".into(),
                        port_id: "right".into(),
                        batch: DeltaBatch::from_records([row("a", 10, 1), row("b", 20, 1)]),
                    },
                ],
            )
            .unwrap();
        assert_eq!(output["top_k"].net_rows().unwrap().len(), 1);
        let checkpoint = graph.checkpoint().unwrap();
        let mut restored = join_top_k_graph();
        restored.restore(&checkpoint).unwrap();
        assert_eq!(restored.checkpoint().unwrap(), checkpoint);
    }

    fn semi_join_graph() -> NativeOperatorGraph {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(NativeSemiJoinOperator::new("semi_join"))
            .unwrap();
        graph
    }

    #[test]
    fn semi_join_uses_right_existence_without_multiplying_left_duplicates() {
        let mut graph = semi_join_graph();
        let retained = graph
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 1, 2), row("b", 2, 1)]),
                }],
            )
            .unwrap();
        assert!(retained["semi_join"].net_rows().unwrap().is_empty());

        let first_match = graph
            .apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 10, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            first_match["semi_join"].net_rows().unwrap(),
            vec![row("a", 1, 2)]
        );

        let duplicate_match = graph
            .apply_epoch(
                3,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 20, 3)]),
                }],
            )
            .unwrap();
        assert!(duplicate_match["semi_join"].net_rows().unwrap().is_empty());

        let matched_left_insert = graph
            .apply_epoch(
                4,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 3, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            matched_left_insert["semi_join"].net_rows().unwrap(),
            vec![row("a", 3, 1)]
        );

        let checkpoint = graph.checkpoint().unwrap();
        assert_eq!(
            checkpoint.operators[0].codec_id,
            "velorix-native-semi-join-v1"
        );
        let mut restored = semi_join_graph();
        restored.restore(&checkpoint).unwrap();

        let still_matched = restored
            .apply_epoch(
                5,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 10, -1)]),
                }],
            )
            .unwrap();
        assert!(still_matched["semi_join"].net_rows().unwrap().is_empty());

        let final_match_deleted = restored
            .apply_epoch(
                6,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 20, -3)]),
                }],
            )
            .unwrap();
        assert_eq!(
            final_match_deleted["semi_join"].net_rows().unwrap(),
            vec![row("a", 1, -2), row("a", 3, -1)]
        );
    }

    fn anti_join_graph() -> NativeOperatorGraph {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(NativeAntiJoinOperator::new("anti_join"))
            .unwrap();
        graph
    }

    #[test]
    fn anti_join_emits_exact_zero_to_positive_to_zero_match_transitions() {
        let mut graph = anti_join_graph();
        let initially_unmatched = graph
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 1, 2), row("a", 2, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            initially_unmatched["anti_join"].net_rows().unwrap(),
            vec![row("a", 1, 2), row("a", 2, 1)]
        );

        let first_match = graph
            .apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 10, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            first_match["anti_join"].net_rows().unwrap(),
            vec![row("a", 1, -2), row("a", 2, -1)]
        );

        let duplicate_match = graph
            .apply_epoch(
                3,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 20, 3)]),
                }],
            )
            .unwrap();
        assert!(duplicate_match["anti_join"].net_rows().unwrap().is_empty());

        let blocked_left_change = graph
            .apply_epoch(
                4,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 1, -1), row("a", 3, 2)]),
                }],
            )
            .unwrap();
        assert!(blocked_left_change["anti_join"]
            .net_rows()
            .unwrap()
            .is_empty());

        let checkpoint = graph.checkpoint().unwrap();
        assert_eq!(
            checkpoint.operators[0].codec_id,
            "velorix-native-anti-join-v1"
        );
        let mut restored = anti_join_graph();
        restored.restore(&checkpoint).unwrap();

        let still_matched = restored
            .apply_epoch(
                5,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 10, -1)]),
                }],
            )
            .unwrap();
        assert!(still_matched["anti_join"].net_rows().unwrap().is_empty());

        let final_match_deleted = restored
            .apply_epoch(
                6,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 20, -3)]),
                }],
            )
            .unwrap();
        assert_eq!(
            final_match_deleted["anti_join"].net_rows().unwrap(),
            vec![row("a", 1, 1), row("a", 2, 1), row("a", 3, 2)]
        );
    }

    #[test]
    fn semi_and_anti_join_key_update_matrix_survives_two_restarts() {
        let mut semi = semi_join_graph();
        let mut anti = anti_join_graph();

        let left = DeltaBatch::from_records([row("a", 1, 2)]);
        assert!(semi
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "left".into(),
                    batch: left.clone(),
                }],
            )
            .unwrap()["semi_join"]
            .net_rows()
            .unwrap()
            .is_empty());
        assert_eq!(
            anti.apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "left".into(),
                    batch: left,
                }],
            )
            .unwrap()["anti_join"]
                .net_rows()
                .unwrap(),
            vec![row("a", 1, 2)]
        );

        let right_a = DeltaBatch::from_records([row("a", 10, 1)]);
        assert_eq!(
            semi.apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "semi_join".into(),
                    port_id: "right".into(),
                    batch: right_a.clone(),
                }],
            )
            .unwrap()["semi_join"]
                .net_rows()
                .unwrap(),
            vec![row("a", 1, 2)]
        );
        assert_eq!(
            anti.apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "anti_join".into(),
                    port_id: "right".into(),
                    batch: right_a,
                }],
            )
            .unwrap()["anti_join"]
                .net_rows()
                .unwrap(),
            vec![row("a", 1, -2)]
        );

        let semi_checkpoint = semi.checkpoint().unwrap();
        let anti_checkpoint = anti.checkpoint().unwrap();
        let mut restored_semi = semi_join_graph();
        let mut restored_anti = anti_join_graph();
        restored_semi.restore(&semi_checkpoint).unwrap();
        restored_anti.restore(&anti_checkpoint).unwrap();

        let key_update = DeltaBatch::from_records([row("a", 1, -2), row("b", 1, 2)]);
        assert_eq!(
            restored_semi
                .apply_epoch(
                    3,
                    vec![NativeOperatorInputV1 {
                        node_id: "semi_join".into(),
                        port_id: "left".into(),
                        batch: key_update.clone(),
                    }],
                )
                .unwrap()["semi_join"]
                .net_rows()
                .unwrap(),
            vec![row("a", 1, -2)]
        );
        assert_eq!(
            restored_anti
                .apply_epoch(
                    3,
                    vec![NativeOperatorInputV1 {
                        node_id: "anti_join".into(),
                        port_id: "left".into(),
                        batch: key_update,
                    }],
                )
                .unwrap()["anti_join"]
                .net_rows()
                .unwrap(),
            vec![row("b", 1, 2)]
        );

        let right_b_duplicates = DeltaBatch::from_records([row("b", 20, 2)]);
        assert_eq!(
            restored_semi
                .apply_epoch(
                    4,
                    vec![NativeOperatorInputV1 {
                        node_id: "semi_join".into(),
                        port_id: "right".into(),
                        batch: right_b_duplicates.clone(),
                    }],
                )
                .unwrap()["semi_join"]
                .net_rows()
                .unwrap(),
            vec![row("b", 1, 2)]
        );
        assert_eq!(
            restored_anti
                .apply_epoch(
                    4,
                    vec![NativeOperatorInputV1 {
                        node_id: "anti_join".into(),
                        port_id: "right".into(),
                        batch: right_b_duplicates,
                    }],
                )
                .unwrap()["anti_join"]
                .net_rows()
                .unwrap(),
            vec![row("b", 1, -2)]
        );

        for (graph, node_id) in [
            (&mut restored_semi, "semi_join"),
            (&mut restored_anti, "anti_join"),
        ] {
            assert!(graph
                .apply_epoch(
                    5,
                    vec![NativeOperatorInputV1 {
                        node_id: node_id.into(),
                        port_id: "right".into(),
                        batch: DeltaBatch::from_records([row("b", 20, -1)]),
                    }],
                )
                .unwrap()[node_id]
                .net_rows()
                .unwrap()
                .is_empty());
        }

        let semi_checkpoint = restored_semi.checkpoint().unwrap();
        let anti_checkpoint = restored_anti.checkpoint().unwrap();
        let mut twice_restored_semi = semi_join_graph();
        let mut twice_restored_anti = anti_join_graph();
        twice_restored_semi.restore(&semi_checkpoint).unwrap();
        twice_restored_anti.restore(&anti_checkpoint).unwrap();

        assert_eq!(
            twice_restored_semi
                .apply_epoch(
                    6,
                    vec![NativeOperatorInputV1 {
                        node_id: "semi_join".into(),
                        port_id: "right".into(),
                        batch: DeltaBatch::from_records([row("b", 20, -1)]),
                    }],
                )
                .unwrap()["semi_join"]
                .net_rows()
                .unwrap(),
            vec![row("b", 1, -2)]
        );
        assert_eq!(
            twice_restored_anti
                .apply_epoch(
                    6,
                    vec![NativeOperatorInputV1 {
                        node_id: "anti_join".into(),
                        port_id: "right".into(),
                        batch: DeltaBatch::from_records([row("b", 20, -1)]),
                    }],
                )
                .unwrap()["anti_join"]
                .net_rows()
                .unwrap(),
            vec![row("b", 1, 2)]
        );
    }

    #[test]
    fn left_join_emits_zero_to_one_to_many_to_zero_match_transitions() {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(NativeLeftJoinOperator::new(
                "left_join",
                |left, right| {
                    Ok(DeltaValue::from_json(json!({
                        "left": left.as_json(),
                        "right": right.as_json()
                    })))
                },
                |left| {
                    Ok(DeltaValue::from_json(json!({
                        "left": left.as_json(),
                        "right": null
                    })))
                },
            ))
            .unwrap();

        let unmatched = graph
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "left_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 1, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            unmatched["left_join"].net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                DeltaValue::from_json(json!({ "left": 1, "right": null })),
                1,
            )]
        );

        let first_match = graph
            .apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "left_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 10, 1)]),
                }],
            )
            .unwrap();
        let first_match = first_match["left_join"].net_rows().unwrap();
        assert_eq!(first_match.len(), 2);
        assert!(first_match.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": 1, "right": null })),
            -1,
        )));
        assert!(first_match.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": 1, "right": 10 })),
            1,
        )));

        let second_match = graph
            .apply_epoch(
                3,
                vec![NativeOperatorInputV1 {
                    node_id: "left_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 20, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            second_match["left_join"].net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                DeltaValue::from_json(json!({ "left": 1, "right": 20 })),
                1,
            )]
        );

        let still_matched = graph
            .apply_epoch(
                4,
                vec![NativeOperatorInputV1 {
                    node_id: "left_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 10, -1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            still_matched["left_join"].net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                DeltaValue::from_json(json!({ "left": 1, "right": 10 })),
                -1,
            )]
        );

        let back_to_unmatched = graph
            .apply_epoch(
                5,
                vec![NativeOperatorInputV1 {
                    node_id: "left_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 20, -1)]),
                }],
            )
            .unwrap();
        let back_to_unmatched = back_to_unmatched["left_join"].net_rows().unwrap();
        assert_eq!(back_to_unmatched.len(), 2);
        assert!(back_to_unmatched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": 1, "right": 20 })),
            -1,
        )));
        assert!(back_to_unmatched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": 1, "right": null })),
            1,
        )));

        let checkpoint = graph.checkpoint().unwrap();
        let mut restored = NativeOperatorGraph::new();
        restored
            .add_operator(NativeLeftJoinOperator::new(
                "left_join",
                |left, right| {
                    Ok(DeltaValue::from_json(json!({
                        "left": left.as_json(),
                        "right": right.as_json()
                    })))
                },
                |left| {
                    Ok(DeltaValue::from_json(json!({
                        "left": left.as_json(),
                        "right": null
                    })))
                },
            ))
            .unwrap();
        restored.restore(&checkpoint).unwrap();
        assert_eq!(restored.checkpoint().unwrap(), checkpoint);
    }

    fn full_join_graph() -> NativeOperatorGraph {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(NativeFullJoinOperator::new(
                "full_join",
                |left, right| {
                    Ok(DeltaValue::from_json(json!({
                        "left": left.as_json(),
                        "right": right.as_json()
                    })))
                },
                |left| {
                    Ok(DeltaValue::from_json(json!({
                        "left": left.as_json(),
                        "right": null
                    })))
                },
                |right| {
                    Ok(DeltaValue::from_json(json!({
                        "left": null,
                        "right": right.as_json()
                    })))
                },
            ))
            .unwrap();
        graph
    }

    #[test]
    fn full_join_emits_symmetric_unmatched_and_match_transitions() {
        let mut graph = full_join_graph();
        let left_unmatched = graph
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 1, 2)]),
                }],
            )
            .unwrap();
        assert_eq!(
            left_unmatched["full_join"].net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                DeltaValue::from_json(json!({ "left": 1, "right": null })),
                2,
            )]
        );

        let right_unmatched = graph
            .apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("b", 10, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            right_unmatched["full_join"].net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("b")),
                DeltaValue::from_json(json!({ "left": null, "right": 10 })),
                1,
            )]
        );

        let checkpoint = graph.checkpoint().unwrap();
        let mut restored = full_join_graph();
        restored.restore(&checkpoint).unwrap();

        let right_to_matched = restored
            .apply_epoch(
                3,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([row("a", 20, 1)]),
                }],
            )
            .unwrap();
        let right_to_matched = right_to_matched["full_join"].net_rows().unwrap();
        assert!(right_to_matched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": 1, "right": null })),
            -2,
        )));
        assert!(right_to_matched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": 1, "right": 20 })),
            2,
        )));

        let left_to_matched = restored
            .apply_epoch(
                4,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("b", 5, 1)]),
                }],
            )
            .unwrap();
        let left_to_matched = left_to_matched["full_join"].net_rows().unwrap();
        assert!(left_to_matched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("b")),
            DeltaValue::from_json(json!({ "left": null, "right": 10 })),
            -1,
        )));
        assert!(left_to_matched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("b")),
            DeltaValue::from_json(json!({ "left": 5, "right": 10 })),
            1,
        )));

        let left_back_to_unmatched = restored
            .apply_epoch(
                5,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("b", 5, -1)]),
                }],
            )
            .unwrap();
        let left_back_to_unmatched = left_back_to_unmatched["full_join"].net_rows().unwrap();
        assert!(left_back_to_unmatched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("b")),
            DeltaValue::from_json(json!({ "left": 5, "right": 10 })),
            -1,
        )));
        assert!(left_back_to_unmatched.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("b")),
            DeltaValue::from_json(json!({ "left": null, "right": 10 })),
            1,
        )));

        let partial_left_delete = restored
            .apply_epoch(
                6,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 1, -1)]),
                }],
            )
            .unwrap();
        assert_eq!(
            partial_left_delete["full_join"].net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                DeltaValue::from_json(json!({ "left": 1, "right": 20 })),
                -1,
            )]
        );

        let final_left_delete = restored
            .apply_epoch(
                7,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([row("a", 1, -1)]),
                }],
            )
            .unwrap();
        let final_left_delete = final_left_delete["full_join"].net_rows().unwrap();
        assert!(final_left_delete.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": 1, "right": 20 })),
            -1,
        )));
        assert!(final_left_delete.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            DeltaValue::from_json(json!({ "left": null, "right": 20 })),
            1,
        )));

        let final_checkpoint = restored.checkpoint().unwrap();
        let full_join = final_checkpoint
            .operators
            .iter()
            .find(|operator| operator.node_id == "full_join")
            .unwrap();
        assert_eq!(full_join.codec_id, "velorix-native-full-join-v1");
        let mut round_trip = full_join_graph();
        round_trip.restore(&final_checkpoint).unwrap();
        assert_eq!(round_trip.checkpoint().unwrap(), final_checkpoint);
    }

    #[test]
    fn full_join_preserves_row_multiplicity_nullable_payloads_and_key_changes() {
        let record = |key: &str, value: serde_json::Value, weight| {
            DeltaRecord::new(
                DeltaKey::from_json(json!(key)),
                DeltaValue::from_json(value),
                weight,
            )
        };
        let left_value = json!({ "payload": null });
        let right_value = json!({ "payload": "right" });
        let joined_value = DeltaValue::from_json(json!({
            "left": left_value,
            "right": right_value,
        }));
        let unmatched_left = DeltaValue::from_json(json!({
            "left": left_value,
            "right": null,
        }));
        let unmatched_right = DeltaValue::from_json(json!({
            "left": null,
            "right": right_value,
        }));
        let mut graph = full_join_graph();

        let duplicated = graph
            .apply_epoch(
                1,
                vec![
                    NativeOperatorInputV1 {
                        node_id: "full_join".into(),
                        port_id: "left".into(),
                        batch: DeltaBatch::from_records([record("a", left_value.clone(), 2)]),
                    },
                    NativeOperatorInputV1 {
                        node_id: "full_join".into(),
                        port_id: "right".into(),
                        batch: DeltaBatch::from_records([record("a", right_value.clone(), 2)]),
                    },
                ],
            )
            .unwrap();
        assert_eq!(
            duplicated["full_join"].net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                joined_value.clone(),
                4,
            )]
        );

        let right_key_change = graph
            .apply_epoch(
                2,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "right".into(),
                    batch: DeltaBatch::from_records([
                        record("a", right_value.clone(), -2),
                        record("b", right_value.clone(), 2),
                    ]),
                }],
            )
            .unwrap();
        let right_key_change = right_key_change["full_join"].net_rows().unwrap();
        assert!(right_key_change.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            joined_value.clone(),
            -4,
        )));
        assert!(right_key_change.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            unmatched_left.clone(),
            2,
        )));
        assert!(right_key_change.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("b")),
            unmatched_right.clone(),
            2,
        )));

        let left_key_change = graph
            .apply_epoch(
                3,
                vec![NativeOperatorInputV1 {
                    node_id: "full_join".into(),
                    port_id: "left".into(),
                    batch: DeltaBatch::from_records([
                        record("a", left_value.clone(), -2),
                        record("b", left_value.clone(), 2),
                    ]),
                }],
            )
            .unwrap();
        let left_key_change = left_key_change["full_join"].net_rows().unwrap();
        assert!(left_key_change.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("a")),
            unmatched_left.clone(),
            -2,
        )));
        assert!(left_key_change.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("b")),
            unmatched_right.clone(),
            -2,
        )));
        assert!(left_key_change.contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("b")),
            joined_value.clone(),
            4,
        )));

        let checkpoint = graph.checkpoint().unwrap();
        let mut graph = full_join_graph();
        graph.restore(&checkpoint).unwrap();
        let cases = [
            (4, "right", -1, vec![(joined_value.clone(), -2)]),
            (5, "left", -1, vec![(joined_value.clone(), -1)]),
            (
                6,
                "left",
                -1,
                vec![(joined_value.clone(), -1), (unmatched_right.clone(), 1)],
            ),
            (7, "right", -1, vec![(unmatched_right, -1)]),
        ];
        for (epoch, port_id, weight, expected) in cases {
            let output = graph
                .apply_epoch(
                    epoch,
                    vec![NativeOperatorInputV1 {
                        node_id: "full_join".into(),
                        port_id: port_id.into(),
                        batch: DeltaBatch::from_records([record(
                            "b",
                            if port_id == "left" {
                                left_value.clone()
                            } else {
                                right_value.clone()
                            },
                            weight,
                        )]),
                    }],
                )
                .unwrap()["full_join"]
                .net_rows()
                .unwrap();
            assert_eq!(output.len(), expected.len());
            for (value, weight) in expected {
                assert!(output.contains(&DeltaRecord::new(
                    DeltaKey::from_json(json!("b")),
                    value,
                    weight,
                )));
            }
        }
    }

    #[test]
    fn top_k_uses_the_planner_supplied_typed_order_key() {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(
                NativeTopKOperator::new("top_k", 0, 1, true, |record| {
                    let value = record.value.as_json().as_i64().unwrap();
                    Ok(value.to_be_bytes().to_vec())
                })
                .unwrap(),
            )
            .unwrap();
        let output = graph
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "top_k".into(),
                    port_id: "input".into(),
                    batch: DeltaBatch::from_records([row("a", 100, 1), row("z", 1, 1)]),
                }],
            )
            .unwrap();
        assert_eq!(output["top_k"].net_rows().unwrap(), vec![row("a", 100, 1)]);
    }

    #[test]
    fn restore_rejects_checkpoint_codec_mismatch() {
        let graph = three_operator_graph();
        let mut checkpoint = graph.checkpoint().unwrap();
        checkpoint.operators[0].codec_id = "wrong-codec".into();
        let mut restored = three_operator_graph();
        assert!(matches!(
            restored.restore(&checkpoint),
            Err(NativeOperatorError::InvalidCheckpoint(_))
        ));
    }

    #[test]
    fn graph_checkpoint_round_trips_through_the_wire_format() {
        let checkpoint = three_operator_graph().checkpoint().unwrap();
        let encoded = serde_json::to_vec(&checkpoint).unwrap();
        let decoded: NativeOperatorGraphCheckpointV1 = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn failed_epoch_rolls_back_every_operator_and_epoch() {
        let mut graph = join_top_k_graph();
        let before = graph.checkpoint().unwrap();
        let error = graph
            .apply_epoch(
                1,
                vec![
                    NativeOperatorInputV1 {
                        node_id: "join".into(),
                        port_id: "left".into(),
                        batch: DeltaBatch::from_records([row("a", 1, 1)]),
                    },
                    NativeOperatorInputV1 {
                        node_id: "join".into(),
                        port_id: "right".into(),
                        batch: DeltaBatch::from_records([row("a", 10, -1)]),
                    },
                ],
            )
            .unwrap_err();
        assert!(matches!(
            error,
            NativeOperatorError::NegativeTopKMultiplicity
        ));
        assert_eq!(graph.checkpoint().unwrap(), before);
    }

    #[test]
    fn failed_restore_rolls_back_operators_already_restored() {
        let mut graph = three_operator_graph();
        graph
            .apply_epoch(
                1,
                vec![NativeOperatorInputV1 {
                    node_id: "filter".into(),
                    port_id: "input".into(),
                    batch: DeltaBatch::from_records([row("a", 3, 1)]),
                }],
            )
            .unwrap();
        let before = graph.checkpoint().unwrap();
        let mut invalid = NativeOperatorGraphCheckpointV1 {
            schema_version: NATIVE_OPERATOR_CHECKPOINT_SCHEMA_VERSION_V1,
            logical_epoch: 9,
            operators: three_operator_graph().checkpoint().unwrap().operators,
        };
        invalid
            .operators
            .iter_mut()
            .find(|operator| operator.node_id == "project")
            .unwrap()
            .codec_id = "wrong-codec".into();

        assert!(matches!(
            graph.restore(&invalid),
            Err(NativeOperatorError::InvalidCheckpoint(_))
        ));
        assert_eq!(graph.checkpoint().unwrap(), before);
    }

    #[test]
    fn duplicate_operator_does_not_replace_existing_node() {
        let mut graph = NativeOperatorGraph::new();
        graph
            .add_operator(NativeFilterOperator::new("same", |_| Ok(true)))
            .unwrap();
        assert!(graph
            .add_operator(NativeProjectOperator::new("same", |record| {
                Ok((record.key.clone(), record.value.clone()))
            }))
            .is_err());
        assert_eq!(
            graph.checkpoint().unwrap().operators[0].codec_id,
            "velorix-native-filter-v1"
        );
    }
}

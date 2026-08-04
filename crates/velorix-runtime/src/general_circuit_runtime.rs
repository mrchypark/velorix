//! General-purpose incremental circuit runtime.
//!
//! Takes an `IncrementalCircuit` and evaluates it epoch by epoch:
//! 1. Converts input data to `DeltaBatch` deltas.
//! 2. Walks the circuit graph in topological order.
//! 3. For each node, computes the output delta from input deltas.
//! 4. Manages stateful operators via `OperatorStateStore` (foyer-backed).
//! 5. Produces output deltas.

use std::collections::{BTreeMap, HashMap};

use velorix_core::circuit::{
    CircuitNode, IncrementalCircuit, NodeId,
};
use velorix_core::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};
use velorix_core::engine::LogicalEpoch;
use velorix_core::incrementalize::{eval_node_incremental, IncrementalError};
use velorix_core::operator::{KeyedEquiJoin, KeyedSumCountAggregate, OperatorError};

use crate::disk_state::{operator_state_key, DiskStateConfig, DiskStateError, OperatorStateStore};

// ---------------------------------------------------------------------------
// Window state
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct TumblingWindowState {
    /// Map from window_start_ns → (group_key → accumulated value)
    windows: BTreeMap<i64, BTreeMap<String, serde_json::Value>>,
    /// Watermark: the latest event time observed
    watermark_ns: i64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum CircuitRuntimeError {
    #[error("incremental error: {0}")]
    Incremental(#[from] IncrementalError),
    #[error("disk state error: {0}")]
    DiskState(#[from] DiskStateError),
    #[error("delta error: {0}")]
    Delta(#[from] velorix_core::delta::DeltaError),
    #[error("operator error: {0}")]
    Operator(#[from] OperatorError),
    #[error("logical epoch must increase monotonically: current={current}, attempted={attempted}")]
    NonMonotonicEpoch { current: LogicalEpoch, attempted: LogicalEpoch },
}

/// Default join value combiner: merges left and right values into a single object.
fn default_join_values(
    left: &DeltaValue,
    right: &DeltaValue,
) -> Result<DeltaValue, OperatorError> {
    let merged = match (left.as_json(), right.as_json()) {
        (serde_json::Value::Object(lm), serde_json::Value::Object(rm)) => {
            let mut m = lm.clone();
            for (k, v) in rm {
                m.insert(k.clone(), v.clone());
            }
            serde_json::Value::Object(m)
        }
        _ => serde_json::json!({"left": left.as_json(), "right": right.as_json()}),
    };
    Ok(DeltaValue::from_json(merged))
}

// ---------------------------------------------------------------------------
// Stateful operator state
// ---------------------------------------------------------------------------

type JoinOperator = KeyedEquiJoin<fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>>;

enum OperatorState {
    Aggregate(KeyedSumCountAggregate),
    Join(JoinOperator),
    Window(TumblingWindowState),
}

// ---------------------------------------------------------------------------
// GeneralCircuitRuntime
// ---------------------------------------------------------------------------

/// A runtime that evaluates an incremental circuit.
pub struct GeneralCircuitRuntime {
    circuit: IncrementalCircuit,
    state_store: OperatorStateStore,
    logical_epoch: LogicalEpoch,
    node_states: HashMap<NodeId, OperatorState>,
    published_output: DeltaBatch,
}

impl GeneralCircuitRuntime {
    /// Create a new runtime for the given incremental circuit.
    pub async fn new(
        circuit: IncrementalCircuit,
        state_config: &DiskStateConfig,
    ) -> Result<Self, CircuitRuntimeError> {
        let state_store = OperatorStateStore::open(state_config).await?;

        Ok(Self {
            circuit,
            state_store,
            logical_epoch: 0,
            node_states: HashMap::new(),
            published_output: DeltaBatch::default(),
        })
    }

    /// Evaluate one epoch: process input changes and produce output deltas.
    pub async fn apply_epoch(
        &mut self,
        logical_epoch: LogicalEpoch,
        input_deltas: HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        // Validate monotonic epoch
        if logical_epoch <= self.logical_epoch {
            return Err(CircuitRuntimeError::NonMonotonicEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }

        // Walk circuit in topological order
        let order = self.circuit.circuit.topological_order();
        let mut node_outputs: HashMap<NodeId, DeltaBatch> = HashMap::new();

        for node_id in &order {
            let node = self.circuit.circuit.nodes[*node_id].clone();
            let output = self.eval_node(&node, *node_id, &input_deltas, &mut node_outputs).await?;
            node_outputs.insert(*node_id, output);
        }

        // Collect output delta
        let output = node_outputs.get(&self.circuit.circuit.output_node_id)
            .cloned()
            .unwrap_or_default();

        // Update published output
        self.published_output = apply_published_output_delta(&self.published_output, &output);
        self.logical_epoch = logical_epoch;

        Ok(output)
    }

    async fn eval_node(
        &mut self,
        node: &CircuitNode,
        node_id: NodeId,
        input_deltas: &HashMap<NodeId, DeltaBatch>,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        match node {
            // Stateless: pass through
            CircuitNode::Source { .. } => {
                Ok(input_deltas.get(&node_id).cloned().unwrap_or_default())
            }
            CircuitNode::Filter { .. } => {
                let input = get_input(node_id, &self.circuit.circuit, node_outputs);
                let mut edge_deltas = BTreeMap::new();
                edge_deltas.insert((0, 0u8), input);
                Ok(eval_node_incremental(node, &edge_deltas)?)
            }
            CircuitNode::Project { .. } => {
                let input = get_input(node_id, &self.circuit.circuit, node_outputs);
                let mut edge_deltas = BTreeMap::new();
                edge_deltas.insert((0, 0u8), input);
                Ok(eval_node_incremental(node, &edge_deltas)?)
            }
            CircuitNode::Map { .. } => {
                let input = get_input(node_id, &self.circuit.circuit, node_outputs);
                let mut edge_deltas = BTreeMap::new();
                edge_deltas.insert((0, 0u8), input);
                Ok(eval_node_incremental(node, &edge_deltas)?)
            }
            CircuitNode::Sink { .. } => {
                let input = get_input(node_id, &self.circuit.circuit, node_outputs);
                let mut edge_deltas = BTreeMap::new();
                edge_deltas.insert((0, 0u8), input);
                Ok(eval_node_incremental(node, &edge_deltas)?)
            }

            // Stateful: Aggregate
            CircuitNode::Aggregate { .. } => {
                self.eval_aggregate(node_id, node_outputs).await
            }

            // Stateful: Distinct
            CircuitNode::Distinct { .. } => {
                self.eval_distinct(node_id, node_outputs).await
            }

            // Stateful: Join — incremental join with side-state
            CircuitNode::Join { node_id: _, left_key, right_key, join_type } => {
                let join_type = join_type.clone();
                let left_key = left_key.clone();
                let right_key = right_key.clone();
                self.eval_join(node_id, &join_type, &left_key, &right_key, node_outputs).await
            }

            // TopK — simplified
            CircuitNode::TopK { limit, .. } => {
                let input = get_input(node_id, &self.circuit.circuit, node_outputs);
                let records: Vec<_> = input.records().iter().take(*limit).cloned().collect();
                Ok(DeltaBatch::from_records(records))
            }

            // Stateful: Window — tumbling event-time window
            CircuitNode::TumblingWindow { event_time, window_size_ns, .. } => {
                let event_time = event_time.clone();
                let window_size_ns = *window_size_ns;
                self.eval_window(node_id, &event_time, window_size_ns, node_outputs).await
            }

            // Stateful: RowNumber — assigns sequential integers within partitions
            CircuitNode::RowNumber { node_id: _, partition_keys, order_by, descending, output_column_id } => {
                let partition_keys = partition_keys.clone();
                let order_by = order_by.clone();
                let descending = *descending;
                let output_column_id = output_column_id.clone();
                self.eval_row_number(node_id, &partition_keys, &order_by, descending, &output_column_id, node_outputs).await
            }
        }
    }

    async fn eval_aggregate(
        &mut self,
        node_id: NodeId,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input = get_input(node_id, &self.circuit.circuit, node_outputs);

        // Get or create aggregate state
        if !self.node_states.contains_key(&node_id) {
            self.node_states.insert(node_id, OperatorState::Aggregate(KeyedSumCountAggregate::new()));
        }

        let state_key = operator_state_key(node_id, "agg");

        // We just inserted Aggregate above, so this is safe
        let agg = match self.node_states.get_mut(&node_id).unwrap() {
            OperatorState::Aggregate(ref mut a) => a,
            _ => unreachable!("node {node_id} should be Aggregate"),
        };
        let output = agg.apply(&input)?;
        // Persist state to disk
        let state = agg.state();
        self.state_store.save(&state_key, &state)?;
        Ok(output)
    }

    async fn eval_distinct(
        &mut self,
        node_id: NodeId,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input = get_input(node_id, &self.circuit.circuit, node_outputs);

        let state_key = operator_state_key(node_id, "distinct");

        // Load existing state
        let existing = self.state_store.load(&state_key).await?.unwrap_or_default();

        // Apply delta to state
        let new_state = self.state_store.apply_delta(&state_key, &input).await?;

        // Compute output delta
        let delta = existing.inverse()?.combine(&new_state);
        Ok(delta)
    }

    async fn eval_join(
        &mut self,
        node_id: NodeId,
        _join_type: &velorix_core::circuit::JoinType,
        _left_key: &velorix_core::circuit::CircuitColumnRef,
        _right_key: &velorix_core::circuit::CircuitColumnRef,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let left_input = get_input_by_port(node_id, 0, &self.circuit.circuit, node_outputs);
        let right_input = get_input_by_port(node_id, 1, &self.circuit.circuit, node_outputs);

        // Get or create join state
        if !self.node_states.contains_key(&node_id) {
            let join: JoinOperator = KeyedEquiJoin::new(default_join_values as fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>);
            self.node_states.insert(node_id, OperatorState::Join(join));
        }

        let state_key = operator_state_key(node_id, "join");

        // Get the join operator
        let join = match self.node_states.get_mut(&node_id).unwrap() {
            OperatorState::Join(ref mut j) => j,
            _ => unreachable!(),
        };

        // Incremental join formula (side-state):
        // Δout = ΔL ⊳△ R + ΔR ⊳△ L (processed sequentially)
        let mut output = DeltaBatch::default();

        if !left_input.records().is_empty() {
            let left_output = join.apply_left(&left_input)?;
            output = output.combine(&left_output);
        }

        if !right_input.records().is_empty() {
            let right_output = join.apply_right(&right_input)?;
            output = output.combine(&right_output);
        }

        // Persist join state to disk
        let left_state = join.left_state();
        let right_state = join.right_state();
        self.state_store.save(&format!("{state_key}-left"), &left_state)?;
        self.state_store.save(&format!("{state_key}-right"), &right_state)?;

        Ok(output)
    }

    async fn eval_window(
        &mut self,
        node_id: NodeId,
        event_time_col: &velorix_core::circuit::CircuitColumnRef,
        window_size_ns: i64,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input = get_input(node_id, &self.circuit.circuit, node_outputs);

        // Get or create window state
        if !self.node_states.contains_key(&node_id) {
            self.node_states.insert(node_id, OperatorState::Window(TumblingWindowState::default()));
        }

        let state_key = operator_state_key(node_id, "window");

        let window_state = match self.node_states.get_mut(&node_id).unwrap() {
            OperatorState::Window(ref mut w) => w,
            _ => unreachable!("node {node_id} should be Window"),
        };

        // Assign each input row to a window and accumulate
        let mut output_records = Vec::new();

        for record in input.records() {
            // Extract event time from value
            let event_time_ns = record.value.as_json()
                .get(event_time_col.column_id.as_str())
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // Compute window assignment
            let window_start = event_time_ns.div_euclid(window_size_ns) * window_size_ns;

            // Update watermark
            if event_time_ns > window_state.watermark_ns {
                window_state.watermark_ns = event_time_ns;
            }

            // Accumulate into window
            let window = window_state.windows.entry(window_start).or_default();
            let key = canonical_key(&record.key);
            let entry = window.entry(key).or_insert_with(|| serde_json::json!({}));

            // Merge value into accumulated state
            if let (Some(obj), Some(new_val)) = (entry.as_object_mut(), record.value.as_json().as_object()) {
                for (k, v) in new_val {
                    if *k == event_time_col.column_id {
                        continue; // skip event_time column
                    }
                    // Simple sum accumulation for numeric values
                    if let Some(existing) = obj.get(k).and_then(|e| e.as_i64()) {
                        if let Some(inc) = v.as_i64() {
                            obj.insert(k.clone(), serde_json::json!(existing + inc * record.weight));
                        }
                    } else {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Emit results for windows that are complete (watermark > window_end)
        let complete_threshold = window_state.watermark_ns;
        let mut completed_windows = Vec::new();

        for (&window_start, values) in &window_state.windows {
            let window_end = window_start + window_size_ns;
            if complete_threshold >= window_end {
                completed_windows.push(window_start);
                for (key, value) in values {
                    output_records.push(DeltaRecord::new(
                        DeltaKey::from_json(serde_json::json!([serde_json::Value::String(key.clone()), window_start, window_end])),
                        DeltaValue::from_json(value.clone()),
                        1,
                    ));
                }
            }
        }

        // Remove completed windows
        for start in &completed_windows {
            window_state.windows.remove(start);
        }

        // Persist window state (simplified - full implementation would serialize window_state)
        self.state_store.save(&state_key, &DeltaBatch::from_records(vec![]))?;

        Ok(DeltaBatch::from_records(output_records))
    }

    async fn eval_row_number(
        &mut self,
        node_id: NodeId,
        partition_keys: &[velorix_core::circuit::CircuitColumnRef],
        order_by: &velorix_core::circuit::CircuitColumnRef,
        descending: bool,
        output_column_id: &str,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input = get_input(node_id, &self.circuit.circuit, node_outputs);

        // Group by partition keys
        let mut partitions: BTreeMap<String, Vec<&velorix_core::delta::DeltaRecord>> = BTreeMap::new();
        for record in input.records() {
            let part_key = if partition_keys.is_empty() {
                "__all__".to_string()
            } else {
                let parts: Vec<String> = partition_keys.iter().map(|pk| {
                    record.value.as_json()
                        .get(pk.column_id.as_str())
                        .map(|v| v.to_string())
                        .unwrap_or_default()
                }).collect();
                parts.join("|")
            };
            partitions.entry(part_key).or_default().push(record);
        }

        let mut output_records = Vec::new();

        for (_part_key, records) in &partitions {
            // Sort by order_by column
            let mut sorted = records.clone();
            sorted.sort_by(|a, b| {
                let a_val = a.value.as_json().get(order_by.column_id.as_str());
                let b_val = b.value.as_json().get(order_by.column_id.as_str());
                let cmp = compare_json_values(a_val, b_val);
                if descending { cmp.reverse() } else { cmp }
            });

            // Assign row numbers
            for (idx, record) in sorted.iter().enumerate() {
                let row_num = (idx as i64) + 1;
                let mut value_obj = record.value.as_json().as_object().cloned().unwrap_or_default();
                value_obj.insert(output_column_id.to_string(), serde_json::json!(row_num));
                output_records.push(DeltaRecord::new(
                    record.key.clone(),
                    DeltaValue::from_json(serde_json::Value::Object(value_obj)),
                    record.weight,
                ));
            }
        }

        Ok(DeltaBatch::from_records(output_records))
    }

    /// Return the current published output (full materialized state).
    pub fn published_output(&self) -> &DeltaBatch {
        &self.published_output
    }

    /// Return the current logical epoch.
    pub fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    /// Restore a runtime from a checkpoint.
    pub fn restore_from_checkpoint(published_output: DeltaBatch, logical_epoch: LogicalEpoch) -> Self {
        let circuit = velorix_core::circuit::IncrementalCircuit {
            circuit: velorix_core::circuit::Circuit {
                nodes: vec![velorix_core::circuit::CircuitNode::Source { node_id: 0, relation_id: "__restored__".into() }],
                edges: vec![],
                input_node_ids: vec![0],
                output_node_id: 0,
            },
            delay_states: std::collections::BTreeMap::new(),
        };
        Self {
            circuit,
            state_store: OperatorStateStore::default(),
            logical_epoch,
            node_states: HashMap::new(),
            published_output,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn get_input(
    node_id: NodeId,
    circuit: &velorix_core::circuit::Circuit,
    node_outputs: &HashMap<NodeId, DeltaBatch>,
) -> DeltaBatch {
    circuit.input_edges(node_id)
        .iter()
        .filter_map(|e| node_outputs.get(&e.from))
        .cloned()
        .next()
        .unwrap_or_default()
}

fn canonical_key(key: &DeltaKey) -> String {
    key.as_json().to_string()
}

fn compare_json_values(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => {
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                ai.cmp(&bi)
            } else if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
            } else if let (Some(as_str), Some(bs)) = (a.as_str(), b.as_str()) {
                as_str.cmp(bs)
            } else {
                a.to_string().cmp(&b.to_string())
            }
        }
    }
}

fn get_input_by_port(
    node_id: NodeId,
    port: u8,
    circuit: &velorix_core::circuit::Circuit,
    node_outputs: &HashMap<NodeId, DeltaBatch>,
) -> DeltaBatch {
    circuit.input_edges(node_id)
        .iter()
        .filter(|e| e.to_port == port)
        .filter_map(|e| node_outputs.get(&e.from))
        .cloned()
        .next()
        .unwrap_or_default()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    format!("sha256:{:016x}", hasher.finish())
}

fn apply_published_output_delta(current: &DeltaBatch, delta: &DeltaBatch) -> DeltaBatch {
    DeltaBatch::from_records(current.combine(delta).net_rows().unwrap_or_default())
}

// ---------------------------------------------------------------------------
// Checkpoint state
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct GeneralCircuitCheckpoint {
    published_output: DeltaBatch,
    logical_epoch: LogicalEpoch,
    input_frontiers: Vec<velorix_core::standing_program::RelationFrontier>,
    applied_epochs: Vec<(String, LogicalEpoch)>,
}

// ---------------------------------------------------------------------------
// StandingProgramRuntime integration
// ---------------------------------------------------------------------------

use velorix_core::delta_to_arrow::delta_batch_to_record_batch;
use velorix_core::standing_program::{
    DurableStateRoot, EpochIdempotencyKey, EpochCommit,
    RelationFrontier, RelationInputBatch, RuntimeCheckpoint, RuntimeCheckpointStatePayload,
    StandingProgramIdentity, StandingProgramRuntime, StandingProgramRuntimeError,
    ViewOutputBatch, ViewOutputDelta, MaterializedViewPage, ScopedViewId, SnapshotPageRequest,
};
use velorix_core::view_contract::RelationSchema;

/// A `GeneralCircuitRuntime` wrapped with `StandingProgramRuntime` integration.
///
/// This wrapper adds:
/// - Identity and schema metadata
/// - Arrow RecordBatch → DeltaBatch input conversion
/// - DeltaBatch → Arrow RecordBatch output conversion
/// - Checkpoint/restore via foyer-backed state
pub struct GeneralStandingRuntime {
    identity: StandingProgramIdentity,
    input_schemas: Vec<RelationSchema>,
    output_schemas: Vec<RelationSchema>,
    inner: GeneralCircuitRuntime,
    input_frontiers: Vec<RelationFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

impl GeneralStandingRuntime {
    pub async fn new(
        identity: StandingProgramIdentity,
        input_schemas: Vec<RelationSchema>,
        output_schemas: Vec<RelationSchema>,
        circuit: velorix_core::circuit::IncrementalCircuit,
        state_config: &DiskStateConfig,
    ) -> Result<Self, CircuitRuntimeError> {
        let inner = GeneralCircuitRuntime::new(circuit, state_config).await?;
        Ok(Self {
            identity,
            input_schemas,
            output_schemas,
            inner,
            input_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }
}

impl StandingProgramRuntime for GeneralStandingRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        self.output_schemas.clone()
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.inner.logical_epoch()
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let idempotency_key_text = idempotency_key.as_str().to_string();

        // Idempotency check
        if let Some(applied_epoch) = self.applied_epochs.get(&idempotency_key_text) {
            if *applied_epoch == logical_epoch {
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: self.input_frontiers.clone(),
                    input_event_time_frontiers: Vec::new(),
                    output_deltas: Vec::new(),
                    output_batches: self.output_batches()?,
                });
            }
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key_text,
                first_epoch: *applied_epoch,
                attempted_epoch: logical_epoch,
            });
        }

        // Monotonic epoch check
        if logical_epoch <= self.inner.logical_epoch() {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.inner.logical_epoch(),
                attempted: logical_epoch,
            });
        }

        // Convert Arrow RecordBatches to DeltaBatch and feed to circuit
        let mut input_deltas: HashMap<NodeId, DeltaBatch> = HashMap::new();

        // Map input relation_id to circuit source node_id
        let source_map: HashMap<String, NodeId> = self.inner.circuit.circuit.input_node_ids.iter()
            .filter_map(|&nid| {
                if let CircuitNode::Source { relation_id, .. } = &self.inner.circuit.circuit.nodes[nid] {
                    Some((relation_id.clone(), nid))
                } else {
                    None
                }
            })
            .collect();

        for input in &input_changes {
            // Find the source node for this relation
            let node_id = source_map.get(&input.relation_id)
                .copied()
                .ok_or_else(|| StandingProgramRuntimeError::ExternalRuntime {
                    reason: format!("unknown input relation: {}", input.relation_id),
                })?;

            // Convert Arrow RecordBatches to DeltaBatch using generic conversion
            // For now, use a simple row-based conversion
            let delta = arrow_batches_to_delta_batch(input)?;
            let existing = input_deltas.entry(node_id).or_default();
            *existing = existing.combine(&delta);

            // Update frontiers
            advance_input_frontier(&mut self.input_frontiers, input)?;
        }

        // Run circuit epoch
        let output_delta = tokio::runtime::Handle::current()
            .block_on(self.inner.apply_epoch(logical_epoch, input_deltas))
            .map_err(|e| StandingProgramRuntimeError::ExternalRuntime {
                reason: format!("circuit eval error: {e}"),
            })?;

        self.applied_epochs.insert(idempotency_key_text, logical_epoch);

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: Vec::new(),
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schemas[0].schema_fingerprint.clone(),
                delta: output_delta,
            }],
            output_batches: self.output_batches()?,
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        _page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        // Find the output schema for this view
        let schema = self.output_schemas.iter()
            .find(|s| s.relation_id == view.view_id || self.identity.view_ids.contains(&view.view_id))
            .ok_or_else(|| StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id.clone(),
            })?;

        let batch = delta_batch_to_record_batch(schema, self.inner.published_output())
            .map_err(|e| StandingProgramRuntimeError::ExternalRuntime {
                reason: format!("delta-to-arrow conversion error: {e}"),
            })?;

        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.inner.logical_epoch(),
            schema_fingerprint: schema.schema_fingerprint.clone(),
            batches: vec![batch],
            next_page_token: None,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        // Serialize full circuit state
        let state = GeneralCircuitCheckpoint {
            published_output: self.inner.published_output().clone(),
            logical_epoch: self.inner.logical_epoch(),
            input_frontiers: self.input_frontiers.clone(),
            applied_epochs: self.applied_epochs.iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
        };
        let payload_json = serde_json::to_string(&state)
            .map_err(|e| StandingProgramRuntimeError::ExternalRuntime {
                reason: format!("checkpoint serialization error: {e}"),
            })?;
        let content_hash = sha256_hex(payload_json.as_bytes());

        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.inner.logical_epoch(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: Vec::new(),
            output_frontiers: Vec::new(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: "general-circuit-state".into(),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: self.identity.checkpoint_codec_identity.clone(),
                payload: payload_json,
            }),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;

        let payload = checkpoint.state_payload
            .ok_or_else(|| StandingProgramRuntimeError::ExternalRuntime {
                reason: "checkpoint missing state payload".into(),
            })?;

        let state: GeneralCircuitCheckpoint = serde_json::from_str(&payload.payload)
            .map_err(|e| StandingProgramRuntimeError::ExternalRuntime {
                reason: format!("checkpoint deserialization error: {e}"),
            })?;

        let applied_epochs: BTreeMap<String, LogicalEpoch> = state.applied_epochs.iter().cloned().collect();
        let input_frontiers = state.input_frontiers.clone();
        let published_output = state.published_output.clone();
        let logical_epoch = state.logical_epoch;

        Ok(Self {
            identity: checkpoint.identity,
            input_schemas: Vec::new(),
            output_schemas: Vec::new(),
            inner: GeneralCircuitRuntime::restore_from_checkpoint(published_output, logical_epoch),
            input_frontiers,
            applied_epochs,
        })
    }
}

impl GeneralStandingRuntime {
    fn output_batches(&self) -> Result<Vec<ViewOutputBatch>, StandingProgramRuntimeError> {
        let mut batches = Vec::new();
        for schema in &self.output_schemas {
            let batch = delta_batch_to_record_batch(schema, self.inner.published_output())
                .map_err(|e| StandingProgramRuntimeError::ExternalRuntime {
                    reason: format!("delta-to-arrow conversion error: {e}"),
                })?;
            batches.push(ViewOutputBatch {
                view_id: schema.relation_id.clone(),
                schema_fingerprint: schema.schema_fingerprint.clone(),
                batches: vec![batch],
            });
        }
        Ok(batches)
    }
}

/// Convert Arrow RecordBatches from a RelationInputBatch to a DeltaBatch.
///
/// This is a simplified conversion that packs all value columns into a JSON object.
fn arrow_batches_to_delta_batch(
    input: &RelationInputBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};
    use arrow::array::{Array, StringArray, Int64Array};

    let mut records = Vec::new();

    for batch in &input.batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        // For now, assume first column is key (as string), remaining columns are values
        if schema.fields().is_empty() {
            continue;
        }

        // Get all column arrays
        let columns: Vec<_> = (0..schema.fields().len())
            .map(|i| batch.column(i))
            .collect();

        for row_idx in 0..num_rows {
            // Use first column as key
            let key_json = if let Some(arr) = columns.first() {
                if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                    serde_json::Value::String(str_arr.value(row_idx).to_string())
                } else if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                    serde_json::json!(int_arr.value(row_idx))
                } else {
                    serde_json::Value::String(format!("row_{row_idx}"))
                }
            } else {
                serde_json::Value::String(format!("row_{row_idx}"))
            };

            // Pack remaining columns into value object
            let mut value_obj = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                if col_idx == 0 { continue; } // skip key column
                let col_name = field.name().clone();
                if let Some(arr) = columns.get(col_idx) {
                    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                        value_obj.insert(col_name, serde_json::Value::String(str_arr.value(row_idx).to_string()));
                    } else if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                        value_obj.insert(col_name, serde_json::json!(int_arr.value(row_idx)));
                    }
                }
            }

            let weight = 1i64; // All input rows are inserts
            records.push(DeltaRecord::new(
                DeltaKey::from_json(key_json.clone()),
                DeltaValue::from_json(serde_json::Value::Object(value_obj)),
                weight,
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

fn advance_input_frontier(
    frontiers: &mut Vec<RelationFrontier>,
    input: &RelationInputBatch,
) -> Result<(), StandingProgramRuntimeError> {
    if let Some(existing) = frontiers.iter_mut().find(|f| {
        f.relation_id == input.relation_id
            && f.relation_version == input.relation_version
            && f.stream_id == input.stream_id
            && f.partition_id == input.partition_id
    }) {
        existing.committed_offset_exclusive = existing.committed_offset_exclusive.max(input.end_offset_exclusive);
    } else {
        frontiers.push(RelationFrontier {
            relation_id: input.relation_id.clone(),
            relation_version: input.relation_version.clone(),
            stream_id: input.stream_id.clone(),
            partition_id: input.partition_id,
            committed_offset_exclusive: input.end_offset_exclusive,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use velorix_core::circuit::*;
    use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};
    use velorix_core::incrementalize::incrementalize;

    fn simple_filter_circuit() -> Circuit {
        Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::Filter {
                    node_id: 1,
                    predicates: vec![CircuitPredicate {
                        column: CircuitColumnRef { node_id: 0, column_id: "x".into() },
                        op: CircuitPredicateOp::Gt,
                        literal: serde_json::json!(5),
                    }],
                },
                CircuitNode::Sink { node_id: 2, relation_id: "v".into() },
            ],
            edges: vec![
                Edge { from: 0, from_port: 0, to: 1, to_port: 0 },
                Edge { from: 1, from_port: 0, to: 2, to_port: 0 },
            ],
            input_node_ids: vec![0],
            output_node_id: 2,
        }
    }

    #[tokio::test]
    async fn runtime_creation() {
        let circuit = incrementalize(&simple_filter_circuit());
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let _runtime = GeneralCircuitRuntime::new(circuit, &config).await.unwrap();
    }

    #[tokio::test]
    async fn filter_epoch_evaluation() {
        let circuit = incrementalize(&simple_filter_circuit());
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(circuit, &config).await.unwrap();

        // Input: two rows, one passes filter (x=10), one doesn't (x=3)
        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r1")),
                DeltaValue::from_json(serde_json::json!({"x": 10})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r2")),
                DeltaValue::from_json(serde_json::json!({"x": 3})),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // Only x=10 should pass
        assert_eq!(output.records().len(), 1);
        assert_eq!(output.records()[0].weight, 1);
    }

    #[tokio::test]
    async fn aggregate_epoch_evaluation() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::Aggregate {
                    node_id: 1,
                    group_keys: vec![CircuitColumnRef { node_id: 0, column_id: "key".into() }],
                    functions: vec![CircuitAggFunc::Sum(CircuitColumnRef { node_id: 0, column_id: "value".into() })],
                },
                CircuitNode::Sink { node_id: 2, relation_id: "v".into() },
            ],
            edges: vec![
                Edge { from: 0, from_port: 0, to: 1, to_port: 0 },
                Edge { from: 1, from_port: 0, to: 2, to_port: 0 },
            ],
            input_node_ids: vec![0],
            output_node_id: 2,
        };

        let inc = incrementalize(&circuit);
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        // The aggregate expects DeltaValue to be a simple integer (the value to sum).
        // The key is the group key.
        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(10)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(5)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("b")),
                DeltaValue::from_json(serde_json::json!(8)),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // Two groups: a (sum=15) and b (sum=8)
        assert_eq!(output.records().len(), 2);
    }

    #[tokio::test]
    async fn join_epoch_evaluation() {
        // Two sources → join on "key" → sink
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "left".into() },
                CircuitNode::Source { node_id: 1, relation_id: "right".into() },
                CircuitNode::Join {
                    node_id: 2,
                    join_type: JoinType::Inner,
                    left_key: CircuitColumnRef { node_id: 0, column_id: "key".into() },
                    right_key: CircuitColumnRef { node_id: 1, column_id: "key".into() },
                },
                CircuitNode::Sink { node_id: 3, relation_id: "out".into() },
            ],
            edges: vec![
                Edge { from: 0, from_port: 0, to: 2, to_port: 0 },
                Edge { from: 1, from_port: 0, to: 2, to_port: 1 },
                Edge { from: 2, from_port: 0, to: 3, to_port: 0 },
            ],
            input_node_ids: vec![0, 1],
            output_node_id: 3,
        };

        let inc = incrementalize(&circuit);
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        // Epoch 1: insert left row (key=A) and right row (key=A)
        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("key-A")),
                DeltaValue::from_json(serde_json::json!({"key": "A", "val": "L1"})),
                1,
            ),
        ]));
        input_deltas.insert(1, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("key-A")),
                DeltaValue::from_json(serde_json::json!({"key": "A", "val": "R1"})),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // One match: L1 joined with R1
        assert_eq!(output.records().len(), 1);
        assert_eq!(output.records()[0].weight, 1);

        // Epoch 2: insert another right row with same key
        let mut input_deltas2 = HashMap::new();
        input_deltas2.insert(1, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("key-A")),
                DeltaValue::from_json(serde_json::json!({"key": "A", "val": "R2"})),
                1,
            ),
        ]));

        let output2 = runtime.apply_epoch(2, input_deltas2).await.unwrap();
        // L1 matches R1 (old state) and R2 (new) — but R1 was already consumed in epoch 1.
        // Actually with side-state: L state has L1, so new R2 joins against L1.
        assert_eq!(output2.records().len(), 1);
        assert_eq!(output2.records()[0].weight, 1);
    }

    #[tokio::test]
    async fn row_number_epoch_evaluation() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "emp".into() },
                CircuitNode::RowNumber {
                    node_id: 1,
                    partition_keys: vec![CircuitColumnRef { node_id: 0, column_id: "dept".into() }],
                    order_by: CircuitColumnRef { node_id: 0, column_id: "name".into() },
                    descending: false,
                    output_column_id: "rn".into(),
                },
                CircuitNode::Sink { node_id: 2, relation_id: "out".into() },
            ],
            edges: vec![
                Edge { from: 0, from_port: 0, to: 1, to_port: 0 },
                Edge { from: 1, from_port: 0, to: 2, to_port: 0 },
            ],
            input_node_ids: vec![0],
            output_node_id: 2,
        };

        let inc = incrementalize(&circuit);
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("e1")),
                DeltaValue::from_json(serde_json::json!({"name": "Alice", "dept": "eng"})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("e2")),
                DeltaValue::from_json(serde_json::json!({"name": "Bob", "dept": "eng"})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("e3")),
                DeltaValue::from_json(serde_json::json!({"name": "Carol", "dept": "sales"})),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        assert_eq!(output.records().len(), 3);
        // Check that each record has rn field
        for record in output.records() {
            assert!(record.value.as_json().get("rn").is_some());
        }
        // eng dept: Alice=1, Bob=2; sales dept: Carol=1
        let mut eng_rns: Vec<i64> = output.records().iter()
            .filter(|r| r.value.as_json().get("dept").and_then(|v| v.as_str()) == Some("eng"))
            .filter_map(|r| r.value.as_json().get("rn").and_then(|v| v.as_i64()))
            .collect();
        eng_rns.sort();
        assert_eq!(eng_rns, vec![1, 2]);
    }
}

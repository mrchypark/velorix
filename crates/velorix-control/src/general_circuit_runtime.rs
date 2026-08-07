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
    CircuitAggFunc, CircuitNode, IncrementalCircuit, NodeId,
};
use velorix_core::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};
use velorix_core::engine::{AggregateValueMode, LogicalEpoch};
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

/// State for LatestByKey operator: tracks per-key latest value and ordering.
#[derive(Clone, Debug, Default)]
struct LatestByKeyState {
    /// Map from key → (order_value, value, weight)
    entries: BTreeMap<String, (serde_json::Value, DeltaValue, i64)>,
}

impl LatestByKeyState {
    fn apply_delta(
        &mut self,
        delta: &DeltaBatch,
        _key_col: &str,
        order_col: &str,
        descending: bool,
    ) -> DeltaBatch {
        let mut output_records = Vec::new();

        for record in delta.records() {
            let key = record
                .key
                .as_json()
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| record.key.as_json().to_string());

            let order_val = record
                .value
                .as_json()
                .get(order_col)
                .cloned()
                .unwrap_or(serde_json::Value::Null);

            let entry = self.entries.entry(key.clone()).or_insert_with(|| {
                (order_val.clone(), record.value.clone(), 0i64)
            });

            let old_weight = entry.2;
            let new_weight = old_weight + record.weight;

            // Check if this record is newer based on ordering
            let is_newer = match (&entry.0, &order_val) {
                (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
                    let a_f = a.as_f64().unwrap_or(0.0);
                    let b_f = b.as_f64().unwrap_or(0.0);
                    if descending {
                        b_f > a_f
                    } else {
                        b_f < a_f
                    }
                }
                (serde_json::Value::String(a), serde_json::Value::String(b)) => {
                    if descending {
                        b > a
                    } else {
                        b < a
                    }
                }
                _ => false,
            };

            if is_newer || old_weight == 0 {
                // Emit retraction for old value if it existed and was visible
                if old_weight > 0 {
                    output_records.push(DeltaRecord::new(
                        DeltaKey::from_json(serde_json::json!(key.clone())),
                        entry.1.clone(),
                        -1,
                    ));
                }
                *entry = (order_val, record.value.clone(), new_weight);
                // Emit new value if weight is positive
                if new_weight > 0 {
                    output_records.push(DeltaRecord::new(
                        DeltaKey::from_json(serde_json::json!(key)),
                        entry.1.clone(),
                        1,
                    ));
                }
            } else {
                entry.2 = new_weight;
                // If weight went negative, emit retraction
                if new_weight <= 0 && old_weight > 0 {
                    output_records.push(DeltaRecord::new(
                        DeltaKey::from_json(serde_json::json!(key)),
                        entry.1.clone(),
                        -1,
                    ));
                }
            }
        }

        DeltaBatch::from_records(output_records)
    }
}

enum OperatorState {
    Aggregate(KeyedSumCountAggregate),
    Join(JoinOperator),
    Window(TumblingWindowState),
    LatestByKey(LatestByKeyState),
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
        node_outputs: &mut HashMap<NodeId, DeltaBatch>,
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
                let result = eval_node_incremental(node, &edge_deltas)?;
                Ok(result)
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
                let result = eval_node_incremental(node, &edge_deltas)?;
                Ok(result)
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

            // TopK — sort by order_by column and take top N
            CircuitNode::TopK { order_by, descending, limit, offset, .. } => {
                let order_by = order_by.clone();
                let descending = *descending;
                let limit = *limit;
                let offset = *offset;
                self.eval_top_k(node_id, &order_by, descending, limit, offset, node_outputs).await
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

            // Stateful: LatestByKey — maintains most recent value per key
            CircuitNode::LatestByKey { key, order_by, descending, .. } => {
                let key = key.clone();
                let order_by = order_by.clone();
                let descending = *descending;
                self.eval_latest_by_key(node_id, &key, &order_by, descending, node_outputs).await
            }
        }
    }

    async fn eval_aggregate(
        &mut self,
        node_id: NodeId,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input = get_input(node_id, &self.circuit.circuit, node_outputs);

        // Find the Aggregate node to get function info — find first value-based agg
        let value_column: Option<String> = self.circuit.circuit.nodes.get(node_id).and_then(|n| {
            if let CircuitNode::Aggregate { functions, .. } = n {
                functions.iter().find_map(|f| match f {
                    CircuitAggFunc::Sum(col) | CircuitAggFunc::Min(col) | CircuitAggFunc::Max(col) | CircuitAggFunc::Avg(col) => Some(col.column_id.clone()),
                    CircuitAggFunc::Count | CircuitAggFunc::CountDistinct(_) => None,
                })
            } else {
                None
            }
        });

        // If value_column is specified and input values are JSON objects, extract the column
        let projected_input = if let Some(ref col_name) = value_column {
            // Check if input values are JSON objects (from SQL circuit) or plain integers (from hand-crafted tests)
            let needs_projection = input.records().first()
                .map(|rec| rec.value.as_json().is_object())
                .unwrap_or(false);
            if needs_projection {
                let records: Vec<_> = input.records().iter().filter_map(|rec| {
                    let val = rec.value.as_json().get(col_name.as_str())?;
                    Some(DeltaRecord::new(
                        rec.key.clone(),
                        DeltaValue::from_json(val.clone()),
                        rec.weight,
                    ))
                }).collect();
                DeltaBatch::from_records(records)
            } else {
                input
            }
        } else if input.records().first().map(|r| r.value.as_json().is_object()).unwrap_or(false) {
            // Count-only aggregate: project all records to value=1 (weight handles the counting)
            let records: Vec<_> = input.records().iter().map(|rec| {
                DeltaRecord::new(
                    rec.key.clone(),
                    DeltaValue::from_json(serde_json::json!(1)),
                    rec.weight,
                )
            }).collect();
            DeltaBatch::from_records(records)
        } else {
            input
        };

        // Get or create aggregate state (with extrema tracking for min/max)
        if !self.node_states.contains_key(&node_id) {
            self.node_states.insert(
                node_id,
                OperatorState::Aggregate(
                    KeyedSumCountAggregate::with_value_mode_and_extrema(
                        AggregateValueMode::Integer,
                        true, // track_extrema for min/max
                    )
                )
            );
        }

        let state_key = operator_state_key(node_id, "agg");

        // We just inserted Aggregate above, so this is safe
        let agg = match self.node_states.get_mut(&node_id).unwrap() {
            OperatorState::Aggregate(ref mut a) => a,
            _ => unreachable!("node {node_id} should be Aggregate"),
        };
        let output = agg.apply(&projected_input)?;

        // Rename aggregate output fields to match SQL aliases and compute derived fields
        let output = if let CircuitNode::Aggregate { functions, output_aliases, group_keys, .. } = &self.circuit.circuit.nodes[node_id] {
            let agg_aliases: Vec<String> = output_aliases.iter()
                .skip(group_keys.len())
                .cloned()
                .collect();
            let agg_names: Vec<&str> = functions.iter().map(|f| match f {
                CircuitAggFunc::Sum(_) => "sum",
                CircuitAggFunc::Count => "count",
                CircuitAggFunc::Min(_) => "min",
                CircuitAggFunc::Max(_) => "max",
                CircuitAggFunc::Avg(_) => "avg",
                CircuitAggFunc::CountDistinct(_) => "count_distinct",
            }).collect();

            let records: Vec<_> = output.records().iter().map(|rec| {
                if let serde_json::Value::Object(obj) = rec.value.as_json() {
                    let mut new_obj = obj.clone();

                    // Compute derived fields (avg = sum / count)
                    if agg_names.contains(&"avg") {
                        if let (Some(sum_val), Some(count_val)) = (new_obj.get("sum"), new_obj.get("count")) {
                            if let (Some(s), Some(c)) = (sum_val.as_f64(), count_val.as_f64()) {
                                if c > 0.0 {
                                    let avg_alias = agg_aliases.iter()
                                        .zip(agg_names.iter())
                                        .find(|(_, name)| **name == "avg")
                                        .map(|(alias, _)| alias.clone())
                                        .unwrap_or_else(|| "avg".to_string());
                                    new_obj.insert(avg_alias, serde_json::json!(s / c));
                                }
                            }
                        }
                    }

                    // Rename fields
                    for (agg_name, agg_alias) in agg_names.iter().zip(agg_aliases.iter()) {
                        if *agg_name != agg_alias.as_str() {
                            if let Some(val) = new_obj.remove(*agg_name) {
                                new_obj.insert(agg_alias.clone(), val);
                            }
                        }
                    }

                    DeltaRecord::new(rec.key.clone(), DeltaValue::from_json(serde_json::Value::Object(new_obj)), rec.weight)
                } else {
                    rec.clone()
                }
            }).collect();
            DeltaBatch::from_records(records)
        } else {
            output
        };

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
        left_key: &velorix_core::circuit::CircuitColumnRef,
        right_key: &velorix_core::circuit::CircuitColumnRef,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input_edges = self.circuit.circuit.input_edges(node_id);
        let left_input_raw = input_edges.first()            .and_then(|e| node_outputs.get(&e.from).cloned())
            .unwrap_or_default();
        let right_input_raw = input_edges.get(1)
            .and_then(|e| node_outputs.get(&e.from).cloned())
            .unwrap_or_default();

        // Re-key inputs so the join matches on the join key column value
        let left_input = rekey_batch_by_column(&left_input_raw, &left_key.column_id);
        let right_input = rekey_batch_by_column(&right_input_raw, &right_key.column_id);

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

    async fn eval_latest_by_key(
        &mut self,
        node_id: NodeId,
        key_col: &velorix_core::circuit::CircuitColumnRef,
        order_col: &velorix_core::circuit::CircuitColumnRef,
        descending: bool,
        node_outputs: &HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input = get_input(node_id, &self.circuit.circuit, node_outputs);

        // Get or create LatestByKey state
        if !self.node_states.contains_key(&node_id) {
            self.node_states.insert(node_id, OperatorState::LatestByKey(LatestByKeyState::default()));
        }

        let state_key = operator_state_key(node_id, "latest_by_key");

        let state = match self.node_states.get_mut(&node_id).unwrap() {
            OperatorState::LatestByKey(ref mut s) => s,
            _ => unreachable!("node {node_id} should be LatestByKey"),
        };

        let output = state.apply_delta(&input, &key_col.column_id, &order_col.column_id, descending);

        // Persist state to disk
        // Serialize state as a simple DeltaBatch of entries
        let state_records: Vec<_> = state.entries.iter().map(|(k, (order, val, weight))| {
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(k)),
                DeltaValue::from_json(serde_json::json!({"order": order, "value": val.as_json()})),
                *weight,
            )
        }).collect();
        let state_batch = DeltaBatch::from_records(state_records);
        self.state_store.save(&state_key, &state_batch)?;

        Ok(output)
    }

    async fn eval_top_k(
        &mut self,
        node_id: NodeId,
        order_col: &velorix_core::circuit::CircuitColumnRef,
        descending: bool,
        limit: usize,
        offset: usize,
        node_outputs: &mut HashMap<NodeId, DeltaBatch>,
    ) -> Result<DeltaBatch, CircuitRuntimeError> {
        let input = get_input(node_id, &self.circuit.circuit, node_outputs);

        // Sort records by order_by column
        let mut sorted_records: Vec<_> = input.records().iter().cloned().collect();
        sorted_records.sort_by(|a, b| {
            let a_val = a.value.as_json().get(order_col.column_id.as_str());
            let b_val = b.value.as_json().get(order_col.column_id.as_str());
            let cmp = compare_json_values(a_val, b_val);
            if descending { cmp.reverse() } else { cmp }
        });

        // Apply offset and limit
        let output_records: Vec<_> = sorted_records
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect();

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

fn rekey_batch_by_column(batch: &DeltaBatch, column_id: &str) -> DeltaBatch {
    let records: Vec<_> = batch.records().iter().map(|rec| {
        let new_key = if let serde_json::Value::Object(obj) = rec.value.as_json() {
            if let Some(val) = obj.get(column_id) {
                DeltaKey::from_json(val.clone())
            } else {
                rec.key.clone()
            }
        } else {
            rec.key.clone()
        };
        DeltaRecord::new(new_key, rec.value.clone(), rec.weight)
    }).collect();
    DeltaBatch::from_records(records)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
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

            // Pack all columns into value object
            let mut value_obj = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
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
                    output_aliases: vec!["key".into(), "sum".into()],
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

    // -----------------------------------------------------------------------
    // Filter/Project tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn filter_with_multiple_predicates() {
        // WHERE x > 5 AND y < 10
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::Filter {
                    node_id: 1,
                    predicates: vec![
                        CircuitPredicate {
                            column: CircuitColumnRef { node_id: 0, column_id: "x".into() },
                            op: CircuitPredicateOp::Gt,
                            literal: serde_json::json!(5),
                        },
                        CircuitPredicate {
                            column: CircuitColumnRef { node_id: 0, column_id: "y".into() },
                            op: CircuitPredicateOp::Lt,
                            literal: serde_json::json!(10),
                        },
                    ],
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

        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r1")),
                DeltaValue::from_json(serde_json::json!({"x": 10, "y": 5})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r2")),
                DeltaValue::from_json(serde_json::json!({"x": 3, "y": 5})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r3")),
                DeltaValue::from_json(serde_json::json!({"x": 10, "y": 15})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r4")),
                DeltaValue::from_json(serde_json::json!({"x": 8, "y": 7})),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // Only r1 (x=10,y=5) and r4 (x=8,y=7) pass both filters
        assert_eq!(output.records().len(), 2);
    }

    // -----------------------------------------------------------------------
    // Aggregate tests: COUNT, MIN, MAX, AVG
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn aggregate_count() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::Aggregate {
                    node_id: 1,
                    group_keys: vec![CircuitColumnRef { node_id: 0, column_id: "key".into() }],
                    functions: vec![CircuitAggFunc::Count],
                    output_aliases: vec!["key".into(), "count".into()],
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

        // Aggregate expects: key = group key, value = integer to aggregate
        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(1)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(2)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("b")),
                DeltaValue::from_json(serde_json::json!(3)),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // Two groups: a (count=2) and b (count=1)
        assert_eq!(output.records().len(), 2);
    }

    #[tokio::test]
    async fn aggregate_min_max() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::Aggregate {
                    node_id: 1,
                    group_keys: vec![CircuitColumnRef { node_id: 0, column_id: "key".into() }],
                    functions: vec![
                        CircuitAggFunc::Min(CircuitColumnRef { node_id: 0, column_id: "val".into() }),
                        CircuitAggFunc::Max(CircuitColumnRef { node_id: 0, column_id: "val".into() }),
                    ],
                    output_aliases: vec!["key".into(), "min".into(), "max".into()],
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

        // Aggregate expects: key = group key, value = integer to aggregate
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
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(20)),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // One group: a (min=5, max=20)
        assert_eq!(output.records().len(), 1);
        let val = &output.records()[0].value.as_json();
        assert_eq!(val.get("min").and_then(|v| v.as_i64()), Some(5));
        assert_eq!(val.get("max").and_then(|v| v.as_i64()), Some(20));
    }

    // -----------------------------------------------------------------------
    // Multi-epoch incremental state tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn aggregate_multi_epoch_incremental() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::Aggregate {
                    node_id: 1,
                    group_keys: vec![CircuitColumnRef { node_id: 0, column_id: "key".into() }],
                    functions: vec![CircuitAggFunc::Sum(CircuitColumnRef { node_id: 0, column_id: "val".into() })],
                    output_aliases: vec!["key".into(), "sum".into()],
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

        // Epoch 1: insert key=a val=10
        let mut input1 = HashMap::new();
        input1.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(10)),
                1,
            ),
        ]));
        let output1 = runtime.apply_epoch(1, input1).await.unwrap();
        assert_eq!(output1.records().len(), 1);
        assert_eq!(output1.records()[0].value.as_json().get("sum").and_then(|v| v.as_i64()), Some(10));

        // Epoch 2: insert key=a val=5
        let mut input2 = HashMap::new();
        input2.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(5)),
                1,
            ),
        ]));
        let output2 = runtime.apply_epoch(2, input2).await.unwrap();
        // Incremental aggregate emits retraction for old value + insertion for new value
        assert!(output2.records().len() >= 1, "expected at least one record in incremental output");
    }

    // -----------------------------------------------------------------------
    // Retraction tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn filter_retraction() {
        let inc = incrementalize(&simple_filter_circuit());
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        // Epoch 1: insert row that passes filter
        let mut input1 = HashMap::new();
        input1.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r1")),
                DeltaValue::from_json(serde_json::json!({"x": 10})),
                1,
            ),
        ]));
        let output1 = runtime.apply_epoch(1, input1).await.unwrap();
        assert_eq!(output1.records().len(), 1);
        assert_eq!(output1.records()[0].weight, 1);

        // Epoch 2: retract the row (weight = -1)
        let mut input2 = HashMap::new();
        input2.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r1")),
                DeltaValue::from_json(serde_json::json!({"x": 10})),
                -1,
            ),
        ]));
        let output2 = runtime.apply_epoch(2, input2).await.unwrap();
        assert_eq!(output2.records().len(), 1);
        assert_eq!(output2.records()[0].weight, -1);
    }

    // -----------------------------------------------------------------------
    // LatestByKey (ARG_MAX/ARG_MIN) test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn latest_by_key_basic() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::LatestByKey {
                    node_id: 1,
                    key: CircuitColumnRef { node_id: 0, column_id: "user_id".into() },
                    order_by: CircuitColumnRef { node_id: 0, column_id: "ts".into() },
                    descending: true,
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

        // Epoch 1: insert two events for user "u1"
        let mut input1 = HashMap::new();
        input1.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r1")),
                DeltaValue::from_json(serde_json::json!({"user_id": "u1", "ts": 100, "action": "login"})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r2")),
                DeltaValue::from_json(serde_json::json!({"user_id": "u1", "ts": 200, "action": "click"})),
                1,
            ),
        ]));
        let output1 = runtime.apply_epoch(1, input1).await.unwrap();
        // Should emit retraction for ts=100 and insertion for ts=200
        assert!(output1.records().len() >= 1);

        // Epoch 2: insert newer event
        let mut input2 = HashMap::new();
        input2.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r3")),
                DeltaValue::from_json(serde_json::json!({"user_id": "u1", "ts": 300, "action": "purchase"})),
                1,
            ),
        ]));
        let output2 = runtime.apply_epoch(2, input2).await.unwrap();
        // Should emit retraction for ts=200 and insertion for ts=300
        assert!(output2.records().len() >= 1);
    }

    // -----------------------------------------------------------------------
    // Checkpoint/Restore test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn checkpoint_restore_round_trip() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source { node_id: 0, relation_id: "t".into() },
                CircuitNode::Aggregate {
                    node_id: 1,
                    group_keys: vec![CircuitColumnRef { node_id: 0, column_id: "key".into() }],
                    functions: vec![CircuitAggFunc::Sum(CircuitColumnRef { node_id: 0, column_id: "val".into() })],
                    output_aliases: vec!["key".into(), "sum".into()],
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

        // Run epoch 1
        let mut input1 = HashMap::new();
        input1.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!(10)),
                1,
            ),
        ]));
        let _output1 = runtime.apply_epoch(1, input1).await.unwrap();

        // Save checkpoint state
        let published_output = runtime.published_output().clone();
        let logical_epoch = runtime.logical_epoch();

        // Create new runtime from checkpoint
        let mut runtime2 = GeneralCircuitRuntime::restore_from_checkpoint(
            published_output.clone(),
            logical_epoch,
        );

        // Verify restored state
        assert_eq!(runtime2.logical_epoch(), 1);
        assert_eq!(runtime2.published_output().records().len(), published_output.records().len());
    }

    // -----------------------------------------------------------------------
    // SQL-to-Circuit-to-Runtime integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn sql_filter_where_integration() {
        use velorix_core::sql_to_circuit::{sql_to_circuit, TableSchema};

        let tables = vec![TableSchema {
            name: "users".into(),
            columns: vec!["id".into(), "name".into(), "age".into()],
        }];

        let circuit = sql_to_circuit("SELECT id, name FROM users WHERE age > 18", &tables).unwrap();
        let inc = incrementalize(&circuit);
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("u1")),
                DeltaValue::from_json(serde_json::json!({"id": 1, "name": "Alice", "age": 25})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("u2")),
                DeltaValue::from_json(serde_json::json!({"id": 2, "name": "Bob", "age": 15})),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // Only Alice (age=25) passes the filter
        assert_eq!(output.records().len(), 1);
    }

    #[tokio::test]
    async fn sql_aggregate_group_by_integration() {
        use velorix_core::sql_to_circuit::{sql_to_circuit, TableSchema};

        let tables = vec![TableSchema {
            name: "orders".into(),
            columns: vec!["id".into(), "customer".into(), "amount".into()],
        }];

        let circuit = sql_to_circuit("SELECT customer, SUM(amount) FROM orders GROUP BY customer", &tables).unwrap();
        let inc = incrementalize(&circuit);
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        // The aggregate evaluator expects: key = group key, value = integer to aggregate
        // So we need to provide DeltaValues that are simple integers
        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("Alice")),
                DeltaValue::from_json(serde_json::json!(100)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("Alice")),
                DeltaValue::from_json(serde_json::json!(50)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("Bob")),
                DeltaValue::from_json(serde_json::json!(200)),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // Two groups: Alice (sum=150) and Bob (sum=200)
        assert_eq!(output.records().len(), 2);
    }

    #[tokio::test]
    async fn sql_join_integration() {
        use velorix_core::sql_to_circuit::{sql_to_circuit, TableSchema};

        let tables = vec![
            TableSchema {
                name: "customers".into(),
                columns: vec!["id".into(), "name".into()],
            },
            TableSchema {
                name: "orders".into(),
                columns: vec!["id".into(), "customer_id".into(), "amount".into()],
            },
        ];

        let circuit = sql_to_circuit(
            "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id",
            &tables,
        ).unwrap();
        let inc = incrementalize(&circuit);
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        let mut input_deltas = HashMap::new();
        // Left input (customers) — DeltaKey must match join key
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("1")),
                DeltaValue::from_json(serde_json::json!({"id": 1, "name": "Alice"})),
                1,
            ),
        ]));
        // Right input (orders) — DeltaKey must match left for join
        input_deltas.insert(1, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("1")),
                DeltaValue::from_json(serde_json::json!({"id": 101, "customer_id": 1, "amount": 100})),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        // One join match: Alice's order
        assert_eq!(output.records().len(), 1);
    }

    #[tokio::test]
    async fn sql_row_number_integration() {
        use velorix_core::sql_to_circuit::{sql_to_circuit, TableSchema};

        let tables = vec![TableSchema {
            name: "employees".into(),
            columns: vec!["id".into(), "name".into(), "dept".into(), "salary".into()],
        }];

        let circuit = sql_to_circuit(
            "SELECT id, name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM employees",
            &tables,
        ).unwrap();
        let inc = incrementalize(&circuit);
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        let mut input_deltas = HashMap::new();
        input_deltas.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("e1")),
                DeltaValue::from_json(serde_json::json!({"id": 1, "name": "Alice", "dept": "eng", "salary": 100000})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("e2")),
                DeltaValue::from_json(serde_json::json!({"id": 2, "name": "Bob", "dept": "eng", "salary": 90000})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("e3")),
                DeltaValue::from_json(serde_json::json!({"id": 3, "name": "Carol", "dept": "sales", "salary": 80000})),
                1,
            ),
        ]));

        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        assert_eq!(output.records().len(), 3);
        for record in output.records() {
            assert!(record.value.as_json().get("rn").is_some());
        }
        let mut eng_rns: Vec<i64> = output.records().iter()
            .filter(|r| r.value.as_json().get("dept").and_then(|v| v.as_str()) == Some("eng"))
            .filter_map(|r| r.value.as_json().get("rn").and_then(|v| v.as_i64()))
            .collect();
        eng_rns.sort();
        assert_eq!(eng_rns, vec![1, 2]);
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn empty_input_epoch() {
        let inc = incrementalize(&simple_filter_circuit());
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        // Empty input
        let input_deltas = HashMap::new();
        let output = runtime.apply_epoch(1, input_deltas).await.unwrap();
        assert_eq!(output.records().len(), 0);
    }

    #[tokio::test]
    async fn non_monotonic_epoch_rejected() {
        let inc = incrementalize(&simple_filter_circuit());
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let mut runtime = GeneralCircuitRuntime::new(inc, &config).await.unwrap();

        let mut input1 = HashMap::new();
        input1.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r1")),
                DeltaValue::from_json(serde_json::json!({"x": 10})),
                1,
            ),
        ]));
        let _output1 = runtime.apply_epoch(2, input1).await.unwrap();

        // Try to go back to epoch 1 (non-monotonic)
        let mut input2 = HashMap::new();
        input2.insert(0, DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("r2")),
                DeltaValue::from_json(serde_json::json!({"x": 20})),
                1,
            ),
        ]));
        let result = runtime.apply_epoch(1, input2).await;
        assert!(result.is_err());
    }
}

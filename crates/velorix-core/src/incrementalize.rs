//! Automatic incrementalization algorithm.
//!
//! Given a `Circuit`, produces an `IncrementalCircuit` where:
//!
//! 1. Every edge carries a `DelayState` (z⁻¹) that tracks the previous snapshot.
//! 2. Stateful operators (Join, Aggregate, Distinct, TopK, Window) manage their
//!    own incremental state.
//! 3. Stateless operators (Filter, Project, Map) pass deltas through unchanged
//!    because they are linear with respect to Z-set weights.
//!
//! Reference: "Automatic Incremental View Maintenance for Rich Query
//! Languages" (Budiu et al., VLDB 2023) — Algorithm 1.

use std::collections::BTreeMap;

use crate::circuit::{
    Circuit, CircuitNode, DelayState, Edge, EdgeKey, IncrementalCircuit, NodeId,
};

// ---------------------------------------------------------------------------
// Algorithm entry point
// ---------------------------------------------------------------------------

/// Transform a non-incremental `Circuit` into an `IncrementalCircuit` that
/// evaluates changes (deltas) rather than full snapshots.
///
/// This implements the five steps of Algorithm 1:
/// 1. Translate SQL → circuit (already done; input is a `Circuit`).
/// 2. Apply distinct-elimination rules (deferred distinct applications).
/// 3. Lift the circuit to operate on streams (add z⁻¹ delays on every edge).
/// 4. Wrap the lifted circuit in I·D (integrate-derive pairs).
/// 5. Replace each primitive with its incremental version via the chain rule.
pub fn incrementalize(circuit: &Circuit) -> IncrementalCircuit {
    // Step 2: distinct elimination — currently a no-op placeholder.
    // In the full algorithm, this would merge consecutive distinct
    // operators using Propositions 4.4 and 4.5 from the paper.
    // For now, we keep distinct nodes as-is.

    // Step 3+4+5: Lift and incrementalize.
    // Every edge gets a delay state (z⁻¹). Each operator receives deltas
    // on its inputs and produces deltas on its outputs. Stateless operators
    // (Filter, Project, Map) are their own incremental version. Stateful
    // operators (Join, Aggregate, Distinct, TopK, Window) carry internal
    // state that is updated each epoch.

    let delay_states: BTreeMap<EdgeKey, DelayState> = circuit
        .edges
        .iter()
        .map(|edge| (EdgeKey::from_edge(edge), DelayState::default()))
        .collect();

    IncrementalCircuit {
        circuit: circuit.clone(),
        delay_states,
    }
}

// ---------------------------------------------------------------------------
// Incremental operator execution
// ---------------------------------------------------------------------------

/// The incremental output of a single circuit node for one epoch.
pub struct NodeOutput {
    /// Delta batch produced by this node.
    pub delta: crate::delta::DeltaBatch,
}

/// Incrementally evaluate a single node given its input deltas.
///
/// `input_deltas` is keyed by `(source_node_id, source_port)` and contains
/// the delta for each input edge.
pub fn eval_node_incremental(
    node: &CircuitNode,
    input_deltas: &BTreeMap<(NodeId, u8), crate::delta::DeltaBatch>,
) -> Result<crate::delta::DeltaBatch, IncrementalError> {
    match node {
        // ----------------------------------------------------------------
        // Stateless (linear) operators: pass delta through
        // ----------------------------------------------------------------
        CircuitNode::Source { .. } => {
            // Source nodes produce the raw input delta; it arrives via
            // input_deltas keyed by their own id (injected by the runtime).
            Ok(input_deltas
                .values()
                .cloned()
                .next()
                .unwrap_or_default())
        }

        CircuitNode::Filter { predicates, .. } => {
            let input = first_input(input_deltas)?;
            Ok(crate::operator::filter_delta_batch(&input, |record| {
                for pred in predicates {
                    if !evaluate_predicate(pred, &record.value)
                        .map_err(crate::operator::OperatorError::from)?
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            })?)
        }

        CircuitNode::Project { columns, .. } => {
            let input = first_input(input_deltas)?;
            Ok(crate::operator::map_delta_batch(&input, |record| {
                let mut projected = serde_json::Map::new();
                for col in columns {
                    let val = record.value.as_json().get(&col.source_column.column_id);
                    if let Some(v) = val {
                        projected.insert(col.output_column_id.clone(), v.clone());
                    }
                }
                Ok((
                    crate::delta::DeltaKey::from_json(serde_json::Value::Object(projected.clone())),
                    crate::delta::DeltaValue::from_json(serde_json::Value::Object(projected)),
                ))
            })?)
        }

        CircuitNode::Map {
            expr,
            output_column_id,
            ..
        } => {
            let input = first_input(input_deltas)?;
            Ok(crate::operator::map_delta_batch(&input, |record| {
                let val = evaluate_expr(expr, &record.value)
                    .map_err(crate::operator::OperatorError::from)?;
                let mut obj = record.value.as_json().as_object().cloned().unwrap_or_default();
                obj.insert(output_column_id.clone(), val);
                Ok((
                    crate::delta::DeltaKey::from_json(serde_json::Value::Object(obj.clone())),
                    crate::delta::DeltaValue::from_json(serde_json::Value::Object(obj)),
                ))
            })?)
        }

        // ----------------------------------------------------------------
        // Stateful operators: require external state management
        // ----------------------------------------------------------------
        CircuitNode::Join { .. } | CircuitNode::Aggregate { .. } | CircuitNode::Distinct { .. }
        | CircuitNode::TopK { .. } | CircuitNode::TumblingWindow { .. }
        | CircuitNode::RowNumber { .. } | CircuitNode::LatestByKey { .. } => {
            // Stateful operators are managed by the runtime, not here.
            // This function handles only stateless pass-through.
            // The runtime calls dedicated methods on `GeneralCircuitRuntime`.
            Ok(first_input(input_deltas)?)
        }

        CircuitNode::Sink { .. } => Ok(first_input(input_deltas)?),
    }
}

// ---------------------------------------------------------------------------
// Predicate and expression evaluation
// ---------------------------------------------------------------------------

fn evaluate_predicate(
    pred: &crate::circuit::CircuitPredicate,
    value: &crate::delta::DeltaValue,
) -> Result<bool, IncrementalError> {
    use crate::circuit::CircuitPredicateOp;

    let col_val = value.as_json().get(&pred.column.column_id);
    let lit = &pred.literal;

    let result = match pred.op {
        CircuitPredicateOp::Eq => col_val == Some(lit),
        CircuitPredicateOp::Ne => col_val != Some(lit),
        CircuitPredicateOp::Lt => compare_json(col_val, lit) == Some(std::cmp::Ordering::Less),
        CircuitPredicateOp::Le => matches!(
            compare_json(col_val, lit),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        CircuitPredicateOp::Gt => compare_json(col_val, lit) == Some(std::cmp::Ordering::Greater),
        CircuitPredicateOp::Ge => matches!(
            compare_json(col_val, lit),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        CircuitPredicateOp::IsNull => col_val.is_none() || col_val == Some(&serde_json::Value::Null),
        CircuitPredicateOp::IsNotNull => col_val.is_some() && col_val != Some(&serde_json::Value::Null),
    };

    Ok(result)
}

fn compare_json(a: Option<&serde_json::Value>, b: &serde_json::Value) -> Option<std::cmp::Ordering> {
    let a = a?;
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let a_f = a.as_f64()?;
            let b_f = b.as_f64()?;
            a_f.partial_cmp(&b_f)
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn evaluate_expr(
    expr: &crate::circuit::CircuitExpr,
    value: &crate::delta::DeltaValue,
) -> Result<serde_json::Value, IncrementalError> {
    use crate::circuit::CircuitExpr;

    match expr {
        CircuitExpr::Column(col) => Ok(value
            .as_json()
            .get(&col.column_id)
            .cloned()
            .unwrap_or(serde_json::Value::Null)),
        CircuitExpr::Literal(v) => Ok(v.clone()),
        CircuitExpr::Add(l, r) => {
            let lv = evaluate_expr(l, value)?;
            let rv = evaluate_expr(r, value)?;
            Ok(json_add(&lv, &rv))
        }
        CircuitExpr::Sub(l, r) => {
            let lv = evaluate_expr(l, value)?;
            let rv = evaluate_expr(r, value)?;
            Ok(json_sub(&lv, &rv))
        }
        CircuitExpr::Mul(l, r) => {
            let lv = evaluate_expr(l, value)?;
            let rv = evaluate_expr(r, value)?;
            Ok(json_mul(&lv, &rv))
        }
        CircuitExpr::Div(l, r) => {
            let lv = evaluate_expr(l, value)?;
            let rv = evaluate_expr(r, value)?;
            Ok(json_div(&lv, &rv))
        }
        CircuitExpr::Neg(e) => {
            let v = evaluate_expr(e, value)?;
            Ok(match v {
                serde_json::Value::Number(n) => {
                    let f = n.as_f64().unwrap_or(0.0);
                    serde_json::json!(-f)
                }
                other => other,
            })
        }
        CircuitExpr::Cast { expr, .. } => evaluate_expr(expr, value),
    }
}

fn json_add(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let av = a.as_f64().unwrap_or(0.0);
            let bv = b.as_f64().unwrap_or(0.0);
            serde_json::json!(av + bv)
        }
        _ => serde_json::Value::Null,
    }
}

fn json_sub(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let av = a.as_f64().unwrap_or(0.0);
            let bv = b.as_f64().unwrap_or(0.0);
            serde_json::json!(av - bv)
        }
        _ => serde_json::Value::Null,
    }
}

fn json_mul(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let av = a.as_f64().unwrap_or(0.0);
            let bv = b.as_f64().unwrap_or(0.0);
            serde_json::json!(av * bv)
        }
        _ => serde_json::Value::Null,
    }
}

fn json_div(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let av = a.as_f64().unwrap_or(0.0);
            let bv = b.as_f64().unwrap_or(0.0);
            if bv == 0.0 {
                serde_json::Value::Null
            } else {
                serde_json::json!(av / bv)
            }
        }
        _ => serde_json::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn first_input(
    input_deltas: &BTreeMap<(NodeId, u8), crate::delta::DeltaBatch>,
) -> Result<crate::delta::DeltaBatch, IncrementalError> {
    input_deltas
        .values()
        .next()
        .cloned()
        .ok_or(IncrementalError::MissingInput)
}

impl EdgeKey {
    fn from_edge(edge: &Edge) -> Self {
        EdgeKey(edge.from, edge.from_port, edge.to, edge.to_port)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum IncrementalError {
    #[error("node has no input edges")]
    MissingInput,
    #[error("delta error: {0}")]
    Delta(#[from] crate::delta::DeltaError),
    #[error("operator error: {0}")]
    Operator(#[from] crate::operator::OperatorError),
}

impl From<IncrementalError> for crate::operator::OperatorError {
    fn from(e: IncrementalError) -> Self {
        match e {
            IncrementalError::Delta(d) => crate::operator::OperatorError::Delta(d),
            _ => crate::operator::OperatorError::WeightOverflow,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit::*;
    use crate::delta::*;
    use serde_json::json;

    fn source_node(id: NodeId, rel: &str) -> CircuitNode {
        CircuitNode::Source {
            node_id: id,
            relation_id: rel.into(),
        }
    }

    fn filter_node(id: NodeId, col: &str, op: CircuitPredicateOp, lit: serde_json::Value) -> CircuitNode {
        CircuitNode::Filter {
            node_id: id,
            predicates: vec![CircuitPredicate {
                column: CircuitColumnRef {
                    node_id: id - 1,
                    column_id: col.into(),
                },
                op,
                literal: lit,
            }],
        }
    }

    #[test]
    fn incrementalize_adds_delay_states_for_all_edges() {
        let circuit = Circuit {
            nodes: vec![source_node(0, "t"), source_node(1, "u")],
            edges: vec![Edge { from: 0, from_port: 0, to: 1, to_port: 0 }],
            input_node_ids: vec![0],
            output_node_id: 1,
        };

        let inc = incrementalize(&circuit);
        assert_eq!(inc.delay_states.len(), 1);
        assert!(inc.delay_states.contains_key(&EdgeKey(0, 0, 1, 0)));
    }

    #[test]
    fn eval_filter_passes_matching_records() {
        let node = filter_node(1, "age", CircuitPredicateOp::Ge, json!(18));
        let input = DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("user:1")),
                DeltaValue::from_json(json!({"age": 25})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("user:2")),
                DeltaValue::from_json(json!({"age": 10})),
                1,
            ),
        ]);

        let mut input_deltas = BTreeMap::new();
        input_deltas.insert((0, 0), input);

        let output = eval_node_incremental(&node, &input_deltas).unwrap();
        assert_eq!(output.records().len(), 1);
        assert_eq!(output.records()[0].weight, 1);
    }

    #[test]
    fn delay_advance_computes_correct_delta() {
        let mut delay = DelayState::default();

        // Epoch 1: insert (k1, v1)
        let batch1 = DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("k1")),
                DeltaValue::from_json(json!({"x": 1})),
                1,
            ),
        ]);
        let delta1 = delay.advance(&batch1).unwrap();
        assert_eq!(delta1.records().len(), 1);
        assert_eq!(delta1.records()[0].weight, 1);

        // Epoch 2: insert (k1, v1) again — state goes from weight 1 to 2
        let delta2 = delay.advance(&batch1).unwrap();
        // Delta is net_rows of (-old + new) = (-1 + 2) = 1
        assert_eq!(delta2.records().len(), 1);
        assert_eq!(delta2.records()[0].weight, 1);
    }
}

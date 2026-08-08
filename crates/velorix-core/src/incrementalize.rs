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

use crate::circuit::{Circuit, CircuitNode, DelayState, Edge, EdgeKey, IncrementalCircuit, NodeId};

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
            Ok(input_deltas.values().cloned().next().unwrap_or_default())
        }

        CircuitNode::Filter { predicate, .. } => {
            let input = first_input(input_deltas)?;
            Ok(crate::operator::filter_delta_batch(&input, |record| {
                evaluate_filter_expr(predicate, &record.value)
                    .map_err(crate::operator::OperatorError::from)
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
                let key_val = columns
                    .first()
                    .and_then(|col| projected.get(&col.output_column_id))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                Ok((
                    crate::delta::DeltaKey::from_json(key_val),
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
                let mut obj = record
                    .value
                    .as_json()
                    .as_object()
                    .cloned()
                    .unwrap_or_default();
                obj.insert(output_column_id.clone(), val);
                Ok((
                    record.key.clone(),
                    crate::delta::DeltaValue::from_json(serde_json::Value::Object(obj)),
                ))
            })?)
        }

        // ----------------------------------------------------------------
        // Stateful operators: require external state management
        // ----------------------------------------------------------------
        CircuitNode::Join { .. }
        | CircuitNode::Aggregate { .. }
        | CircuitNode::Distinct { .. }
        | CircuitNode::TopK { .. }
        | CircuitNode::TumblingWindow { .. }
        | CircuitNode::RowNumber { .. }
        | CircuitNode::LatestByKey { .. } => {
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

/// Evaluate a compound filter expression against a record's value.
fn evaluate_filter_expr(
    expr: &crate::circuit::CircuitFilterExpr,
    value: &crate::delta::DeltaValue,
) -> Result<bool, IncrementalError> {
    use crate::circuit::CircuitFilterExpr;
    match expr {
        CircuitFilterExpr::AlwaysTrue => Ok(true),
        CircuitFilterExpr::Predicate(pred) => evaluate_predicate(pred, value),
        CircuitFilterExpr::Comparison { left, op, right } => {
            let left = evaluate_expr(left, value)?;
            let right = evaluate_expr(right, value)?;
            Ok(evaluate_comparison(&left, *op, &right))
        }
        CircuitFilterExpr::And(children) => {
            for child in children {
                if !evaluate_filter_expr(child, value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        CircuitFilterExpr::Or(children) => {
            for child in children {
                if evaluate_filter_expr(child, value)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        CircuitFilterExpr::Not(child) => Ok(!evaluate_filter_expr(child, value)?),
    }
}

fn evaluate_comparison(
    left: &serde_json::Value,
    op: crate::circuit::CircuitPredicateOp,
    right: &serde_json::Value,
) -> bool {
    use crate::circuit::CircuitPredicateOp;
    match op {
        CircuitPredicateOp::Eq => left == right,
        CircuitPredicateOp::Ne => left != right,
        CircuitPredicateOp::Lt => compare_json_value(Some(left), right).is_some_and(|o| o.is_lt()),
        CircuitPredicateOp::Le => compare_json_value(Some(left), right).is_some_and(|o| o.is_le()),
        CircuitPredicateOp::Gt => compare_json_value(Some(left), right).is_some_and(|o| o.is_gt()),
        CircuitPredicateOp::Ge => compare_json_value(Some(left), right).is_some_and(|o| o.is_ge()),
        CircuitPredicateOp::IsDistinctFrom => left != right,
        _ => false,
    }
}

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
        CircuitPredicateOp::IsNull => {
            col_val.is_none() || col_val == Some(&serde_json::Value::Null)
        }
        CircuitPredicateOp::IsNotNull => {
            col_val.is_some() && col_val != Some(&serde_json::Value::Null)
        }
        CircuitPredicateOp::IsDistinctFrom => {
            // IS DISTINCT FROM: true when (col IS NULL AND lit IS NOT NULL) or (col IS NOT NULL AND lit IS NULL) or col != lit
            match (col_val, lit) {
                (None, serde_json::Value::Null) => false,
                (None, _) | (Some(serde_json::Value::Null), _)
                    if lit != &serde_json::Value::Null =>
                {
                    true
                }
                (_, serde_json::Value::Null) if col_val != Some(&serde_json::Value::Null) => true,
                (Some(a), b) => a != b,
                _ => false,
            }
        }
        CircuitPredicateOp::In => {
            // literal is the first element; literals contains all values
            let all_vals = if pred.literals.is_empty() {
                vec![lit.clone()]
            } else {
                pred.literals.clone()
            };
            match col_val {
                None => false,
                Some(cv) => all_vals.iter().any(|v| cv == v),
            }
        }
        CircuitPredicateOp::NotIn => {
            let all_vals = if pred.literals.is_empty() {
                vec![lit.clone()]
            } else {
                pred.literals.clone()
            };
            match col_val {
                None => true,
                Some(cv) => all_vals.iter().all(|v| cv != v),
            }
        }
        CircuitPredicateOp::Between => {
            // literals[0] = low, literals[1] = high
            if pred.literals.len() >= 2 {
                let low = &pred.literals[0];
                let high = &pred.literals[1];
                matches!(
                    compare_json(col_val, low),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    compare_json(col_val, high),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            } else {
                false
            }
        }
        CircuitPredicateOp::Like => {
            if let Some(serde_json::Value::String(pattern)) = col_val {
                let pattern_str = lit.as_str().unwrap_or("");
                simple_like(pattern, pattern_str)
            } else {
                false
            }
        }
    };

    Ok(result)
}

/// Simple SQL LIKE pattern matching (% = any chars, _ = single char).
fn simple_like(value: &str, pattern: &str) -> bool {
    // Convert SQL LIKE pattern to a simple regex-like match
    let mut pi = 0;
    let mut vi = 0;
    let mut star_pi = usize::MAX;
    let mut star_vi = 0;

    while vi < value.len() {
        if pi < pattern.len()
            && (pattern.as_bytes()[pi] == b'%' || pattern.as_bytes()[pi] == value.as_bytes()[vi])
        {
            if pattern.as_bytes()[pi] == b'%' {
                star_pi = pi;
                star_vi = vi;
                pi += 1;
            } else {
                pi += 1;
                vi += 1;
            }
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern.as_bytes()[pi] == b'%' {
        pi += 1;
    }

    pi == pattern.len()
}

fn compare_json(
    a: Option<&serde_json::Value>,
    b: &serde_json::Value,
) -> Option<std::cmp::Ordering> {
    let a = a?;
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let a_f = a.as_f64()?;
            let b_f = b.as_f64()?;
            a_f.partial_cmp(&b_f)
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => Some(a.cmp(b)),
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
        CircuitExpr::Modulo(l, r) => {
            let lv = evaluate_expr(l, value)?;
            let rv = evaluate_expr(r, value)?;
            Ok(json_modulo(&lv, &rv))
        }
        CircuitExpr::Neg(e) => {
            let v = evaluate_expr(e, value)?;
            Ok(match v {
                serde_json::Value::Number(n) => {
                    if let Some(value) = n.as_i64().and_then(i64::checked_neg) {
                        serde_json::json!(value)
                    } else {
                        let value = n.as_f64().unwrap_or(0.0);
                        serde_json::json!(-value)
                    }
                }
                other => other,
            })
        }
        CircuitExpr::Abs(e) => {
            let v = evaluate_expr(e, value)?;
            Ok(match v {
                serde_json::Value::Number(n) => {
                    let f = n.as_f64().unwrap_or(0.0);
                    serde_json::json!(f.abs())
                }
                other => other,
            })
        }
        CircuitExpr::Cast { expr, .. } => evaluate_expr(expr, value),
        CircuitExpr::Greatest(args) => {
            let mut best: Option<serde_json::Value> = None;
            for arg in args {
                let v = evaluate_expr(arg, value)?;
                if let Some(ref b) = best {
                    if let Some(std::cmp::Ordering::Greater) = compare_json_value(Some(&v), b) {
                        best = Some(v);
                    }
                } else {
                    best = Some(v);
                }
            }
            Ok(best.unwrap_or(serde_json::Value::Null))
        }
        CircuitExpr::Least(args) => {
            let mut best: Option<serde_json::Value> = None;
            for arg in args {
                let v = evaluate_expr(arg, value)?;
                if let Some(ref b) = best {
                    if let Some(std::cmp::Ordering::Less) = compare_json_value(Some(&v), b) {
                        best = Some(v);
                    }
                } else {
                    best = Some(v);
                }
            }
            Ok(best.unwrap_or(serde_json::Value::Null))
        }
        CircuitExpr::If {
            condition,
            then_expr,
            else_expr,
        } => {
            if evaluate_filter_expr(condition, value)? {
                evaluate_expr(then_expr, value)
            } else {
                evaluate_expr(else_expr, value)
            }
        }
        CircuitExpr::CaseWhen {
            when_clauses,
            else_result,
        } => {
            for (cond, result) in when_clauses {
                if evaluate_filter_expr(cond, value)? {
                    return evaluate_expr(result, value);
                }
            }
            match else_result {
                Some(e) => evaluate_expr(e, value),
                None => Ok(serde_json::Value::Null),
            }
        }
        CircuitExpr::AggregateRef(col_name) => Ok(value
            .as_json()
            .get(col_name.as_str())
            .cloned()
            .unwrap_or(serde_json::Value::Null)),
    }
}

fn compare_json_value(
    a: Option<&serde_json::Value>,
    b: &serde_json::Value,
) -> Option<std::cmp::Ordering> {
    let a = a?;
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let a_f = a.as_f64()?;
            let b_f = b.as_f64()?;
            a_f.partial_cmp(&b_f)
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => Some(a.cmp(b)),
        _ => None,
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

fn json_modulo(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let ai = a.as_i64().unwrap_or(0);
            let bi = b.as_i64().unwrap_or(1);
            if bi == 0 {
                serde_json::Value::Null
            } else {
                serde_json::json!(ai % bi)
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

    fn filter_node(
        id: NodeId,
        col: &str,
        op: CircuitPredicateOp,
        lit: serde_json::Value,
    ) -> CircuitNode {
        CircuitNode::Filter {
            node_id: id,
            predicate: CircuitFilterExpr::Predicate(CircuitPredicate {
                column: CircuitColumnRef {
                    node_id: id - 1,
                    column_id: col.into(),
                },
                op,
                literal: lit,
                literals: vec![],
            }),
        }
    }

    #[test]
    fn incrementalize_adds_delay_states_for_all_edges() {
        let circuit = Circuit {
            nodes: vec![source_node(0, "t"), source_node(1, "u")],
            edges: vec![Edge {
                from: 0,
                from_port: 0,
                to: 1,
                to_port: 0,
            }],
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
        let batch1 = DeltaBatch::from_records(vec![DeltaRecord::new(
            DeltaKey::from_json(json!("k1")),
            DeltaValue::from_json(json!({"x": 1})),
            1,
        )]);
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

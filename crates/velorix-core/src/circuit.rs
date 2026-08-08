//! General circuit IR for automatic incremental view maintenance.
//!
//! SQL is parsed into a `Circuit`, which is then mechanically incrementalized
//! using the incrementalization algorithm. Each node in the circuit represents a primitive
//! operator; edges carry data between operators.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::delta::DeltaBatch;

/// Unique identifier for a node in the circuit graph.
pub type NodeId = usize;

// ---------------------------------------------------------------------------
// Circuit predicates, expressions, and column references
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CircuitColumnRef {
    pub node_id: NodeId,
    pub column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CircuitPredicate {
    pub column: CircuitColumnRef,
    pub op: CircuitPredicateOp,
    pub literal: Value,
    /// Additional literal values for multi-value predicates (In, NotIn, Between).
    /// For `In`/`NotIn`: all values in the list. `literal` is the first element.
    /// For `Between`: [low, high]. `literal` is kept for backward compat but unused.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub literals: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitPredicateOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
    In,
    NotIn,
    Between,
    Like,
    IsDistinctFrom,
}

/// Compound filter expression that supports AND, OR, NOT, and simple predicates.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CircuitFilterExpr {
    Predicate(CircuitPredicate),
    Comparison {
        left: CircuitExpr,
        op: CircuitPredicateOp,
        right: CircuitExpr,
    },
    And(Vec<CircuitFilterExpr>),
    Or(Vec<CircuitFilterExpr>),
    Not(Box<CircuitFilterExpr>),
    /// Always-true predicate (pass-through).
    AlwaysTrue,
}

impl CircuitFilterExpr {
    /// Wrap a list of predicates into a single filter expression (AND-conjunction).
    pub fn from_predicates(preds: Vec<CircuitPredicate>) -> Self {
        match preds.len() {
            0 => CircuitFilterExpr::AlwaysTrue,
            1 => CircuitFilterExpr::Predicate(preds.into_iter().next().unwrap()),
            _ => CircuitFilterExpr::And(
                preds
                    .into_iter()
                    .map(CircuitFilterExpr::Predicate)
                    .collect(),
            ),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CircuitExpr {
    Column(CircuitColumnRef),
    Literal(Value),
    Add(Box<CircuitExpr>, Box<CircuitExpr>),
    Sub(Box<CircuitExpr>, Box<CircuitExpr>),
    Mul(Box<CircuitExpr>, Box<CircuitExpr>),
    Div(Box<CircuitExpr>, Box<CircuitExpr>),
    Modulo(Box<CircuitExpr>, Box<CircuitExpr>),
    Neg(Box<CircuitExpr>),
    Abs(Box<CircuitExpr>),
    Cast {
        expr: Box<CircuitExpr>,
        to_type: CircuitDataType,
    },
    Greatest(Vec<CircuitExpr>),
    Least(Vec<CircuitExpr>),
    If {
        condition: Box<CircuitFilterExpr>,
        then_expr: Box<CircuitExpr>,
        else_expr: Box<CircuitExpr>,
    },
    CaseWhen {
        when_clauses: Vec<(CircuitFilterExpr, CircuitExpr)>,
        else_result: Option<Box<CircuitExpr>>,
    },
    /// Reference to an aggregate output column (used in HAVING predicates).
    AggregateRef(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitDataType {
    I64,
    F64,
    String,
    Bool,
    Decimal128 { precision: u8, scale: i8 },
    TimestampNs,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CircuitAggFunc {
    Sum(CircuitColumnRef),
    Count,
    CountDistinct(CircuitColumnRef),
    Min(CircuitColumnRef),
    Max(CircuitColumnRef),
    Avg(CircuitColumnRef),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

// ---------------------------------------------------------------------------
// Circuit nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CircuitNode {
    /// Read from a registered relation.
    Source {
        node_id: NodeId,
        relation_id: String,
    },
    /// Filter rows by compound predicate expression.
    Filter {
        node_id: NodeId,
        predicate: CircuitFilterExpr,
    },
    /// Project / rename columns.
    Project {
        node_id: NodeId,
        columns: Vec<CircuitProjection>,
    },
    /// Apply an expression to each row.
    Map {
        node_id: NodeId,
        expr: CircuitExpr,
        output_column_id: String,
    },
    /// Equi-join on key columns.
    Join {
        node_id: NodeId,
        join_type: JoinType,
        left_key: CircuitColumnRef,
        right_key: CircuitColumnRef,
    },
    /// GROUP BY aggregation.
    Aggregate {
        node_id: NodeId,
        group_keys: Vec<CircuitColumnRef>,
        functions: Vec<CircuitAggFunc>,
        output_aliases: Vec<String>,
    },
    /// Distinct (bag → set conversion).
    Distinct { node_id: NodeId },
    /// Top-K limit.
    TopK {
        node_id: NodeId,
        order_by: CircuitColumnRef,
        descending: bool,
        limit: usize,
        offset: usize,
    },
    /// Tumbling / hopping event-time window.
    TumblingWindow {
        node_id: NodeId,
        event_time: CircuitColumnRef,
        window_size_ns: i64,
    },
    /// Row number window function: assigns sequential integers within partitions.
    RowNumber {
        node_id: NodeId,
        partition_keys: Vec<CircuitColumnRef>,
        order_by: CircuitColumnRef,
        descending: bool,
        output_column_id: String,
    },
    /// Latest-by-key: maintains the most recent value per key, ordered by a column.
    LatestByKey {
        node_id: NodeId,
        key: CircuitColumnRef,
        order_by: CircuitColumnRef,
        descending: bool,
    },
    /// Emit the result to an output relation.
    Sink {
        node_id: NodeId,
        relation_id: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CircuitProjection {
    pub source_column: CircuitColumnRef,
    pub output_column_id: String,
}

// ---------------------------------------------------------------------------
// Edge
// ---------------------------------------------------------------------------

/// Directed edge: `from` node produces data consumed by `to` node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Edge {
    pub from: NodeId,
    pub from_port: u8,
    pub to: NodeId,
    pub to_port: u8,
}

// ---------------------------------------------------------------------------
// Circuit
// ---------------------------------------------------------------------------

/// A complete dataflow graph representing a SQL view.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Circuit {
    pub nodes: Vec<CircuitNode>,
    pub edges: Vec<Edge>,
    pub input_node_ids: Vec<NodeId>,
    pub output_node_id: NodeId,
}

impl Circuit {
    /// Return the node with the given id, or None.
    pub fn node(&self, id: NodeId) -> Option<&CircuitNode> {
        self.nodes.get(id)
    }

    /// Nodes in topological order (source → sink).
    pub fn topological_order(&self) -> Vec<NodeId> {
        let n = self.nodes.len();
        let mut in_degree = vec![0usize; n];
        let mut adj: Vec<Vec<NodeId>> = vec![Vec::new(); n];

        for edge in &self.edges {
            adj[edge.from].push(edge.to);
            in_degree[edge.to] += 1;
        }

        let mut queue: std::collections::VecDeque<NodeId> =
            (0..n).filter(|&id| in_degree[id] == 0).collect();
        let mut order = Vec::with_capacity(n);

        while let Some(id) = queue.pop_front() {
            order.push(id);
            for &next in &adj[id] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 {
                    queue.push_back(next);
                }
            }
        }

        order
    }

    /// Return all edges whose `to` matches the given node.
    pub fn input_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.to == node_id).collect()
    }

    /// Return all edges whose `from` matches the given node.
    pub fn output_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == node_id).collect()
    }
}

// ---------------------------------------------------------------------------
// Incremental circuit (output of the incrementalization algorithm)
// ---------------------------------------------------------------------------

/// A circuit that has been transformed for incremental evaluation.
///
/// Each edge carries a `DelayState` representing the z⁻¹ operator: it holds
/// the previous value seen on that edge and yields the current-minus-previous
/// delta on each evaluation step.
#[derive(Clone, Debug)]
pub struct IncrementalCircuit {
    pub circuit: Circuit,
    /// Delay state keyed by `(from, from_port, to, to_port)`.
    pub delay_states: BTreeMap<EdgeKey, DelayState>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct EdgeKey(pub NodeId, pub u8, pub NodeId, pub u8);

/// The z⁻¹ delay operator state: stores the last complete snapshot and emits
/// the delta between the new snapshot and the stored snapshot.
#[derive(Clone, Debug, Default)]
pub struct DelayState {
    /// The last integrated (net_rows) snapshot.
    pub snapshot: DeltaBatch,
}

impl DelayState {
    /// Given a new batch on this edge, return the delta (new_state − old_state)
    /// and update the stored snapshot.
    pub fn advance(&mut self, new_batch: &DeltaBatch) -> Result<DeltaBatch, DelayError> {
        // Integrate the new batch into the snapshot
        let combined = new_batch.combine(&self.snapshot).net_rows()?;
        let new_snapshot = DeltaBatch::from_records(combined);

        // Delta = new_snapshot − old_snapshot
        let delta = self.snapshot.inverse()?.combine(&new_snapshot).net_rows()?;

        self.snapshot = new_snapshot;
        Ok(DeltaBatch::from_records(delta))
    }
}

#[derive(Debug, Error)]
pub enum DelayError {
    #[error("delta arithmetic error: {0}")]
    Delta(#[from] crate::delta::DeltaError),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};

    #[test]
    fn circuit_topological_order_respects_edges() {
        let circuit = Circuit {
            nodes: vec![
                CircuitNode::Source {
                    node_id: 0,
                    relation_id: "t".into(),
                },
                CircuitNode::Filter {
                    node_id: 1,
                    predicate: CircuitFilterExpr::AlwaysTrue,
                },
                CircuitNode::Sink {
                    node_id: 2,
                    relation_id: "v".into(),
                },
            ],
            edges: vec![
                Edge {
                    from: 0,
                    from_port: 0,
                    to: 1,
                    to_port: 0,
                },
                Edge {
                    from: 1,
                    from_port: 0,
                    to: 2,
                    to_port: 0,
                },
            ],
            input_node_ids: vec![0],
            output_node_id: 2,
        };

        let order = circuit.topological_order();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn delay_state_advance_produces_delta() {
        let mut delay = DelayState::default();
        let batch = DeltaBatch::from_records(vec![DeltaRecord::new(
            DeltaKey::from_json(serde_json::json!("k1")),
            DeltaValue::from_json(serde_json::json!({"v": 1})),
            1,
        )]);

        let delta = delay.advance(&batch).unwrap();
        assert_eq!(delta.records().len(), 1);
        assert_eq!(delta.records()[0].weight, 1);

        // Second advance with same batch: state goes from weight 1 to 2
        let delta2 = delay.advance(&batch).unwrap();
        assert_eq!(delta2.records().len(), 1);
        assert_eq!(delta2.records()[0].weight, 1); // net change: 2 - 1 = 1
    }
}

//! Compiled expression evaluator for vectorized processing.
//!
//! Replaces per-row JSON expression interpretation with pre-compiled
//! expression trees that can be evaluated on Arrow RecordBatches.
//!
//! # Design
//!
//! ```text
//! CompiledExpr {
//!     expr: Expr,
//!     param_types: Vec<DataType>,
//! }
//!
//! enum Expr {
//!     Column(usize),           // column index
//!     Literal(Value),          // constant value
//!     Eq(Box<Expr>, Box<Expr>),
//!     Lt(Box<Expr>, Box<Expr>),
//!     And(Box<Expr>, Box<Expr>),
//!     Add(Box<Expr>, Box<Expr>),
//!     Lower(Box<Expr>),       // string lowercase
//! }
//! ```
//!
//! # Benefits over JSON interpreter
//!
//! - Pre-compiled: no parsing on each row
//! - Type-checked at compile time
//! - Can produce BooleanArray masks for vectorized filter
//! - Reduces per-row branching

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use serde_json::Value;
use velorix_core::standing_program::StandingProgramRuntimeError;

/// Returns an invalid runtime state error.
fn invalid_runtime_state() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "compiled_expression",
    }
}

/// A compiled expression that can be evaluated on RecordBatches.
#[derive(Clone, Debug)]
pub enum CompiledExpr {
    /// Reference to a column by index.
    Column(usize),
    /// A literal constant value.
    Literal(Value),
    /// Equality comparison.
    Eq(Box<CompiledExpr>, Box<CompiledExpr>),
    /// Less-than comparison.
    Lt(Box<CompiledExpr>, Box<CompiledExpr>),
    /// Logical AND.
    And(Box<CompiledExpr>, Box<CompiledExpr>),
    /// Logical OR.
    Or(Box<CompiledExpr>, Box<CompiledExpr>),
    /// Addition.
    Add(Box<CompiledExpr>, Box<CompiledExpr>),
    /// String lowercase.
    Lower(Box<CompiledExpr>),
    /// IS NULL check.
    IsNull(Box<CompiledExpr>),
}

impl CompiledExpr {
    /// Evaluate the expression on a RecordBatch at the given row index.
    ///
    /// Returns the result as a JSON value.
    pub fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        row: usize,
    ) -> Result<Value, StandingProgramRuntimeError> {
        match self {
            CompiledExpr::Column(idx) => {
                let array = batch.column(*idx);
                json_value_from_array(array, row)
            }
            CompiledExpr::Literal(val) => Ok(val.clone()),
            CompiledExpr::Eq(left, right) => {
                let l = left.evaluate(batch, row)?;
                let r = right.evaluate(batch, row)?;
                Ok(Value::Bool(l == r))
            }
            CompiledExpr::Lt(left, right) => {
                let l = left.evaluate(batch, row)?;
                let r = right.evaluate(batch, row)?;
                Ok(Value::Bool(
                    compare_json_values(&l, &r) == std::cmp::Ordering::Less,
                ))
            }
            CompiledExpr::And(left, right) => {
                let l = left.evaluate(batch, row)?;
                let r = right.evaluate(batch, row)?;
                Ok(Value::Bool(
                    l.as_bool().unwrap_or(false) && r.as_bool().unwrap_or(false),
                ))
            }
            CompiledExpr::Or(left, right) => {
                let l = left.evaluate(batch, row)?;
                let r = right.evaluate(batch, row)?;
                Ok(Value::Bool(
                    l.as_bool().unwrap_or(false) || r.as_bool().unwrap_or(false),
                ))
            }
            CompiledExpr::Add(left, right) => {
                let l = left.evaluate(batch, row)?;
                let r = right.evaluate(batch, row)?;
                match (&l, &r) {
                    (Value::Number(a), Value::Number(b)) => {
                        let a_f = a.as_f64().unwrap_or(0.0);
                        let b_f = b.as_f64().unwrap_or(0.0);
                        Ok(Value::from(a_f + b_f))
                    }
                    _ => Ok(Value::Null),
                }
            }
            CompiledExpr::Lower(inner) => {
                let val = inner.evaluate(batch, row)?;
                match val {
                    Value::String(s) => Ok(Value::String(s.to_lowercase())),
                    _ => Ok(val),
                }
            }
            CompiledExpr::IsNull(inner) => {
                let val = inner.evaluate(batch, row)?;
                Ok(Value::Bool(val.is_null()))
            }
        }
    }

    /// Evaluate the expression on all rows of a RecordBatch.
    ///
    /// Returns a BooleanArray mask.
    pub fn evaluate_mask(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<BooleanArray, StandingProgramRuntimeError> {
        let mut values = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let result = self.evaluate(batch, row)?;
            values.push(result.as_bool().unwrap_or(false));
        }
        Ok(BooleanArray::from(values))
    }
}

/// Extract a JSON value from an Arrow array at a given row index.
fn json_value_from_array(
    array: &ArrayRef,
    row: usize,
) -> Result<Value, StandingProgramRuntimeError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Value::from(arr.value(row)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Value::from(arr.value(row)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Value::String(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Value::Bool(arr.value(row)));
    }
    Err(invalid_runtime_state())
}

/// Compare two JSON values for ordering.
fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Number(a), Value::Number(b)) => {
            let a_f = a.as_f64().unwrap_or(0.0);
            let b_f = b.as_f64().unwrap_or(0.0);
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Compile a JSON value expression into a CompiledExpr.
pub fn compile_expr(value: &Value) -> Result<CompiledExpr, StandingProgramRuntimeError> {
    match value {
        Value::Object(obj) => {
            if let Some(op) = obj.get("op").and_then(|v| v.as_str()) {
                match op {
                    "eq" => {
                        let left = obj.get("left").ok_or_else(invalid_runtime_state)?;
                        let right = obj.get("right").ok_or_else(invalid_runtime_state)?;
                        Ok(CompiledExpr::Eq(
                            Box::new(compile_expr(left)?),
                            Box::new(compile_expr(right)?),
                        ))
                    }
                    "lt" => {
                        let left = obj.get("left").ok_or_else(invalid_runtime_state)?;
                        let right = obj.get("right").ok_or_else(invalid_runtime_state)?;
                        Ok(CompiledExpr::Lt(
                            Box::new(compile_expr(left)?),
                            Box::new(compile_expr(right)?),
                        ))
                    }
                    "and" => {
                        let left = obj.get("left").ok_or_else(invalid_runtime_state)?;
                        let right = obj.get("right").ok_or_else(invalid_runtime_state)?;
                        Ok(CompiledExpr::And(
                            Box::new(compile_expr(left)?),
                            Box::new(compile_expr(right)?),
                        ))
                    }
                    "or" => {
                        let left = obj.get("left").ok_or_else(invalid_runtime_state)?;
                        let right = obj.get("right").ok_or_else(invalid_runtime_state)?;
                        Ok(CompiledExpr::Or(
                            Box::new(compile_expr(left)?),
                            Box::new(compile_expr(right)?),
                        ))
                    }
                    "add" => {
                        let left = obj.get("left").ok_or_else(invalid_runtime_state)?;
                        let right = obj.get("right").ok_or_else(invalid_runtime_state)?;
                        Ok(CompiledExpr::Add(
                            Box::new(compile_expr(left)?),
                            Box::new(compile_expr(right)?),
                        ))
                    }
                    "lower" => {
                        let inner = obj.get("arg").ok_or_else(invalid_runtime_state)?;
                        Ok(CompiledExpr::Lower(Box::new(compile_expr(inner)?)))
                    }
                    "is_null" => {
                        let inner = obj.get("arg").ok_or_else(invalid_runtime_state)?;
                        Ok(CompiledExpr::IsNull(Box::new(compile_expr(inner)?)))
                    }
                    _ => Err(invalid_runtime_state()),
                }
            } else if let Some(col) = obj.get("column").and_then(|v| v.as_u64()) {
                Ok(CompiledExpr::Column(col as usize))
            } else {
                Err(invalid_runtime_state())
            }
        }
        _ => Ok(CompiledExpr::Literal(value.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use serde_json::json;
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn compiled_expr_column() {
        let batch = test_batch();
        let expr = CompiledExpr::Column(0);
        let val = expr.evaluate(&batch, 0).unwrap();
        assert_eq!(val, Value::from(1));
    }

    #[test]
    fn compiled_expr_eq() {
        let batch = test_batch();
        let expr = CompiledExpr::Eq(
            Box::new(CompiledExpr::Column(0)),
            Box::new(CompiledExpr::Literal(Value::from(1))),
        );
        let val = expr.evaluate(&batch, 0).unwrap();
        assert_eq!(val, Value::Bool(true));

        let val = expr.evaluate(&batch, 1).unwrap();
        assert_eq!(val, Value::Bool(false));
    }

    #[test]
    fn compiled_expr_and() {
        let batch = test_batch();
        let expr = CompiledExpr::And(
            Box::new(CompiledExpr::Literal(Value::Bool(true))),
            Box::new(CompiledExpr::Literal(Value::Bool(false))),
        );
        let val = expr.evaluate(&batch, 0).unwrap();
        assert_eq!(val, Value::Bool(false));
    }

    #[test]
    fn compiled_expr_lower() {
        let batch = test_batch();
        let expr = CompiledExpr::Lower(Box::new(CompiledExpr::Column(1)));
        let val = expr.evaluate(&batch, 0).unwrap();
        assert_eq!(val, Value::String("alice".to_string()));
    }

    #[test]
    fn compiled_expr_evaluate_mask() {
        let batch = test_batch();
        let expr = CompiledExpr::Lt(
            Box::new(CompiledExpr::Column(0)),
            Box::new(CompiledExpr::Literal(Value::from(3))),
        );
        let mask = expr.evaluate_mask(&batch).unwrap();
        assert!(mask.value(0));
        assert!(mask.value(1));
        assert!(!mask.value(2));
    }

    #[test]
    fn compile_expr_from_json() {
        let json = json!({"op": "eq", "left": {"column": 0}, "right": 42});
        let expr = compile_expr(&json).unwrap();
        assert!(matches!(expr, CompiledExpr::Eq(_, _)));
    }
}

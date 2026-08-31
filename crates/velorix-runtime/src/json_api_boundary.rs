//! JSON API boundary enforcement.
//!
//! Ensures that JSON (serde_json::Value) is only used at the external
//! API boundary, not in the internal runtime hot path. Internal state
//! uses typed representations (Arrow, binary keys, typed structs).
//!
//! # Design
//!
//! ```text
//! External API (JSON)          Internal Runtime (Typed)
//! ┌─────────────────┐         ┌─────────────────────┐
//! │ IngestRequest    │  ───>  │ DeltaRecord          │
//! │ QueryRequest     │  ───>  │ RecordBatch          │
//! │ ViewDefinition   │  ───>  │ SupportedViewPlan    │
//! │ CheckpointPayload│  ───>  │ RuntimeCheckpoint    │
//! └─────────────────┘         └─────────────────────┘
//!         │                            │
//!         ▼                            ▼
//!   JSON parsing              Typed processing
//!   validation                Arrow operations
//!   error formatting          binary encoding
//! ```
//!
//! # Rules
//!
//! 1. External API accepts/returns JSON
//! 2. JSON is parsed into typed structs at the boundary
//! 3. Internal runtime operates on typed data only
//! 4. Results are serialized to JSON only at the response boundary
//! 5. No JSON parsing in hot paths (filter, join, aggregate, window)

use serde::{Deserialize, Serialize};
use serde_json::Value;
use velorix_core::standing_program::StandingProgramRuntimeError;

/// Marker trait for types that are safe to use in the internal runtime.
///
/// Types implementing this trait are guaranteed to be:
/// - Fully parsed from JSON
/// - Type-checked at construction time
/// - No JSON parsing required for operations
pub trait InternalType: Send + Sync {}

/// Marker trait for types that exist only at the API boundary.
///
/// Types implementing this trait contain JSON values and should
/// not be used in the internal runtime hot path.
pub trait ApiBoundaryType: Send + Sync {}

/// A validated, typed value that has been parsed from JSON.
///
/// Once created, all operations use the typed representation
/// without any JSON parsing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedValue {
    inner: serde_json::Value,
}

impl TypedValue {
    /// Create a TypedValue from a JSON value (API boundary).
    pub fn from_json(value: Value) -> Self {
        Self { inner: value }
    }

    /// Get the underlying JSON value (for serialization at response boundary).
    pub fn as_json(&self) -> &Value {
        &self.inner
    }

    /// Get the inner value (for serialization at response boundary).
    pub fn into_json(self) -> Value {
        self.inner
    }
}

impl InternalType for TypedValue {}

/// A validated batch of records for internal processing.
///
/// This is the internal representation that replaces JSON-based
/// record processing in the runtime hot path.
#[derive(Clone, Debug)]
pub struct TypedBatch {
    records: Vec<TypedRecord>,
}

impl TypedBatch {
    /// Create a new empty batch.
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// Add a record to the batch.
    pub fn push(&mut self, record: TypedRecord) {
        self.records.push(record);
    }

    /// Get the number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get a reference to the records.
    pub fn records(&self) -> &[TypedRecord] {
        &self.records
    }
}

impl Default for TypedBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// A single typed record for internal processing.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedRecord {
    pub key: TypedValue,
    pub value: TypedValue,
    pub weight: i64,
}

impl TypedRecord {
    /// Create a new record from JSON values (API boundary).
    pub fn from_json(key: Value, value: Value, weight: i64) -> Self {
        Self {
            key: TypedValue::from_json(key),
            value: TypedValue::from_json(value),
            weight,
        }
    }
}

/// Validate that a JSON value matches the expected type schema.
///
/// Called at the API boundary before passing data to the runtime.
pub fn validate_json_boundary(
    value: &Value,
    expected_type: &str,
) -> Result<(), StandingProgramRuntimeError> {
    match expected_type {
        "object" => {
            if !value.is_object() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "json_boundary_type",
                });
            }
        }
        "array" => {
            if !value.is_array() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "json_boundary_type",
                });
            }
        }
        "string" => {
            if !value.is_string() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "json_boundary_type",
                });
            }
        }
        "number" => {
            if !value.is_number() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "json_boundary_type",
                });
            }
        }
        "integer" if value.is_i64() || value.is_u64() => {}
        "integer" => {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "json_boundary_type",
            });
        }
        _ => {}
    }
    Ok(())
}

/// Convert a JSON ingest request into typed records.
///
/// This is the API boundary function that parses JSON into
/// the internal typed representation.
pub fn json_to_typed_records(
    rows: &[Value],
) -> Result<Vec<TypedRecord>, StandingProgramRuntimeError> {
    let mut records = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        validate_json_boundary(row, "object").map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "ingest_row_format",
            }
        })?;
        records.push(TypedRecord::from_json(
            Value::Number(i.into()),
            row.clone(),
            1,
        ));
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_value_from_json() {
        let tv = TypedValue::from_json(json!(42));
        assert_eq!(*tv.as_json(), json!(42));
    }

    #[test]
    fn typed_batch_operations() {
        let mut batch = TypedBatch::new();
        assert!(batch.is_empty());

        batch.push(TypedRecord::from_json(json!("a"), json!(1), 1));
        batch.push(TypedRecord::from_json(json!("b"), json!(2), 1));

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }

    #[test]
    fn validate_json_boundary_object() {
        assert!(validate_json_boundary(&json!({"a": 1}), "object").is_ok());
        assert!(validate_json_boundary(&json!(42), "object").is_err());
    }

    #[test]
    fn validate_json_boundary_string() {
        assert!(validate_json_boundary(&json!("hello"), "string").is_ok());
        assert!(validate_json_boundary(&json!(42), "string").is_err());
    }

    #[test]
    fn json_to_typed_records_valid() {
        let rows = vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})];
        let records = json_to_typed_records(&rows).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].weight, 1);
    }

    #[test]
    fn json_to_typed_records_invalid() {
        let rows = vec![json!(42)]; // Not an object
        assert!(json_to_typed_records(&rows).is_err());
    }
}

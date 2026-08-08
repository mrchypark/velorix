//! Conversion from `DeltaBatch` to Arrow `RecordBatch`.
//!
//! Provides generic delta-to-arrow conversion using column schema metadata.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use thiserror::Error;

use crate::delta::{DeltaBatch, DeltaError};
use crate::view_contract::{ColumnSchema, RelationSchema, SqlDataType};

#[derive(Debug, Error)]
pub enum DeltaToArrowError {
    #[error("delta error: {0}")]
    Delta(#[from] DeltaError),
    #[error("schema must have at least one column")]
    EmptySchema,
    #[error("invalid row weight: expected 1, got {0}")]
    NonUnitWeight(i64),
    #[error("missing column '{name}' in delta value")]
    MissingColumn { name: String },
    #[error("invalid value type for column '{name}': expected {expected}")]
    InvalidValueType { name: String, expected: String },
    #[error("arrow error: {0}")]
    Arrow(String),
}

/// Convert a `DeltaBatch` to an Arrow `RecordBatch` using the given schema.
///
/// The schema defines the column names and types. The key column is the first
/// column, and value columns follow. Each delta row must have weight == 1
/// (net rows are expected).
pub fn delta_batch_to_record_batch(
    schema: &RelationSchema,
    batch: &DeltaBatch,
) -> Result<RecordBatch, DeltaToArrowError> {
    let columns = &schema.columns;
    if columns.is_empty() {
        return Err(DeltaToArrowError::EmptySchema);
    }

    let [key_col, value_cols @ ..] = columns.as_slice() else {
        return Err(DeltaToArrowError::EmptySchema);
    };

    let rows = batch.net_rows()?;

    let mut keys = Vec::with_capacity(rows.len());
    let mut value_arrays: Vec<Vec<serde_json::Value>> = vec![Vec::new(); value_cols.len()];

    for row in &rows {
        if row.weight != 1 {
            return Err(DeltaToArrowError::NonUnitWeight(row.weight));
        }
        let key_val = row.key.as_json().clone();
        // If key is an object, try to extract the primary key column value
        let key_val = if let Some(obj) = key_val.as_object() {
            obj.get(key_col.name.as_str()).cloned().unwrap_or(key_val)
        } else {
            key_val
        };
        keys.push(key_val);

        if !value_cols.is_empty() {
            let obj = row.value.as_json().as_object().ok_or_else(|| {
                DeltaToArrowError::InvalidValueType {
                    name: key_col.name.clone(),
                    expected: "object".into(),
                }
            })?;
            for (i, col) in value_cols.iter().enumerate() {
                let val = obj
                    .get(col.name.as_str())
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                value_arrays[i].push(val);
            }
        }
    }

    let mut fields = Vec::with_capacity(columns.len());
    fields.push(Field::new(
        key_col.name.as_str(),
        arrow_data_type(&key_col.data_type)?,
        false,
    ));
    for col in value_cols {
        fields.push(Field::new(
            col.name.as_str(),
            arrow_data_type(&col.data_type)?,
            col.nullable,
        ));
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    arrays.push(json_to_arrow_array(&key_col.data_type, &keys)?);
    for (col, values) in value_cols.iter().zip(value_arrays.iter()) {
        arrays.push(json_to_arrow_array_nullable(col, values)?);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| DeltaToArrowError::Arrow(e.to_string()))
}

/// Map a `SqlDataType` to Arrow `DataType`.
pub fn arrow_data_type(data_type: &SqlDataType) -> Result<DataType, DeltaToArrowError> {
    match data_type {
        SqlDataType::Utf8 => Ok(DataType::Utf8),
        SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Bool => Ok(DataType::Boolean),
        SqlDataType::Decimal { precision, scale } => {
            Ok(DataType::Decimal128(*precision as u8, *scale as i8))
        }
        SqlDataType::Json => Ok(DataType::Utf8),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Timestamp { timezone } => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            timezone.as_deref().map(Arc::from),
        )),
        _ => Err(DeltaToArrowError::InvalidValueType {
            name: "data_type".into(),
            expected: "supported SQL type".into(),
        }),
    }
}

fn json_to_arrow_array(
    data_type: &SqlDataType,
    values: &[serde_json::Value],
) -> Result<ArrayRef, DeltaToArrowError> {
    match data_type {
        SqlDataType::Utf8 | SqlDataType::Json => {
            let strs: Vec<Option<&str>> = values.iter().map(|v| v.as_str()).collect();
            Ok(Arc::new(StringArray::from(strs)))
        }
        SqlDataType::Int64 => {
            let nums: Vec<Option<i64>> = values.iter().map(|v| v.as_i64()).collect();
            Ok(Arc::new(arrow::array::Int64Array::from(nums)))
        }
        SqlDataType::Float64 => {
            let nums: Vec<Option<f64>> = values.iter().map(|v| v.as_f64()).collect();
            Ok(Arc::new(arrow::array::Float64Array::from(nums)))
        }
        SqlDataType::Bool => {
            let bools: Vec<Option<bool>> = values.iter().map(|v| v.as_bool()).collect();
            Ok(Arc::new(arrow::array::BooleanArray::from(bools)))
        }
        _ => Err(DeltaToArrowError::InvalidValueType {
            name: "key".into(),
            expected: "simple type (string, int, float, bool)".into(),
        }),
    }
}

fn json_to_arrow_array_nullable(
    column: &ColumnSchema,
    values: &[serde_json::Value],
) -> Result<ArrayRef, DeltaToArrowError> {
    if !column.nullable && values.iter().any(|v| v.is_null()) {
        return Err(DeltaToArrowError::InvalidValueType {
            name: column.name.clone(),
            expected: "non-nullable".into(),
        });
    }
    match &column.data_type {
        SqlDataType::Utf8 | SqlDataType::Json => {
            let strs: Vec<Option<&str>> = values.iter().map(|v| v.as_str()).collect();
            Ok(Arc::new(StringArray::from(strs)))
        }
        SqlDataType::Int64 => {
            let nums: Vec<Option<i64>> = values.iter().map(|v| v.as_i64()).collect();
            Ok(Arc::new(arrow::array::Int64Array::from(nums)))
        }
        SqlDataType::Float64 => {
            let nums: Vec<Option<f64>> = values.iter().map(|v| v.as_f64()).collect();
            Ok(Arc::new(arrow::array::Float64Array::from(nums)))
        }
        SqlDataType::Bool => {
            let bools: Vec<Option<bool>> = values.iter().map(|v| v.as_bool()).collect();
            Ok(Arc::new(arrow::array::BooleanArray::from(bools)))
        }
        _ => Err(DeltaToArrowError::InvalidValueType {
            name: column.name.clone(),
            expected: "supported SQL type".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::{DeltaKey, DeltaRecord, DeltaValue};

    #[test]
    fn delta_to_arrow_basic() {
        let schema = RelationSchema {
            relation_id: "test".into(),
            relation_name: "test".into(),
            relation_version: "1".into(),
            schema_fingerprint: "fp".into(),
            columns: vec![
                ColumnSchema {
                    name: "key".into(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "value".into(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["key".into()],
        };

        let batch = DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("a")),
                DeltaValue::from_json(serde_json::json!({"key": "a", "value": 10})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("b")),
                DeltaValue::from_json(serde_json::json!({"key": "b", "value": 20})),
                1,
            ),
        ]);

        let rb = delta_batch_to_record_batch(&schema, &batch).unwrap();
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.num_columns(), 2);
    }

    #[test]
    fn delta_to_arrow_empty() {
        let schema = RelationSchema {
            relation_id: "test".into(),
            relation_name: "test".into(),
            relation_version: "1".into(),
            schema_fingerprint: "fp".into(),
            columns: vec![ColumnSchema {
                name: "key".into(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            }],
            primary_key: vec!["key".into()],
        };

        let batch = DeltaBatch::default();
        let rb = delta_batch_to_record_batch(&schema, &batch).unwrap();
        assert_eq!(rb.num_rows(), 0);
    }
}

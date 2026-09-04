//! Arrow RecordBatch-based batch operator for vectorized processing.
//!
//! Provides a thin wrapper around Arrow RecordBatch that enables
//! vectorized filter, projection, and aggregation operations.
//! This replaces row-by-row JSON interpretation with columnar operations.
//!
//! # Design
//!
//! ```text
//! ArrowBatchOperator {
//!     schema: SchemaRef,
//!     batches: Vec<RecordBatch>,
//! }
//! ```
//!
//! Operations:
//! - filter: BooleanArray mask → filtered RecordBatch
//! - projection: column selection → new RecordBatch
//! - aggregate: column-wise sum/count/min/max/avg
//!
//! # Benefits over JSON interpreter
//!
//! - SIMD-friendly columnar operations
//! - No per-row JSON parsing/casting
//! - Reduced heap allocation
//! - Better CPU cache locality

use std::{borrow::Cow, sync::Arc};

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use velorix_core::standing_program::StandingProgramRuntimeError;

/// Returns an invalid runtime state error.
fn invalid_runtime_state() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "arrow_batch_operator",
    }
}

/// Arrow-based batch operator for vectorized processing.
///
/// Wraps Arrow RecordBatch to provide efficient filter, projection,
/// and aggregation without per-row JSON interpretation.
pub struct ArrowBatchOperator {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl ArrowBatchOperator {
    /// Create a new operator from existing RecordBatches.
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    /// Create an operator from DeltaBatch records (bridges JSON → Arrow).
    pub fn from_delta_records(
        records: &[velorix_core::delta::DeltaRecord],
        schema: SchemaRef,
    ) -> Result<Self, StandingProgramRuntimeError> {
        let batch = delta_records_to_record_batch(records, &schema)?;
        Ok(Self {
            schema,
            batches: vec![batch],
        })
    }

    /// Apply a boolean filter mask to all batches.
    ///
    /// Returns a new operator with only matching rows.
    pub fn filter(&self, mask: &BooleanArray) -> Result<Self, StandingProgramRuntimeError> {
        let mut filtered = Vec::new();
        for batch in &self.batches {
            let filtered_batch = arrow::compute::filter_record_batch(batch, mask)
                .map_err(|_| invalid_runtime_state())?;
            if filtered_batch.num_rows() > 0 {
                filtered.push(filtered_batch);
            }
        }
        Ok(Self {
            schema: self.schema.clone(),
            batches: filtered,
        })
    }

    /// Select columns by index.
    ///
    /// Returns a new operator with only the selected columns.
    pub fn select(&self, indices: &[usize]) -> Result<Self, StandingProgramRuntimeError> {
        let fields: Vec<_> = indices
            .iter()
            .map(|&i| self.schema.field(i).clone())
            .collect();
        let new_schema = Arc::new(Schema::new(arrow::datatypes::Fields::from(fields)));
        let mut selected = Vec::new();
        for batch in &self.batches {
            let columns: Vec<ArrayRef> = indices.iter().map(|&i| batch.column(i).clone()).collect();
            let selected_batch = RecordBatch::try_new(new_schema.clone(), columns)
                .map_err(|_| invalid_runtime_state())?;
            selected.push(selected_batch);
        }
        Ok(Self {
            schema: new_schema,
            batches: selected,
        })
    }

    /// Compute sum of a numeric column.
    pub fn sum(&self, column_index: usize) -> Result<i64, StandingProgramRuntimeError> {
        let mut total: i64 = 0;
        for batch in &self.batches {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(invalid_runtime_state)?;
            for i in 0..array.len() {
                if !array.is_null(i) {
                    total = total.saturating_add(array.value(i));
                }
            }
        }
        Ok(total)
    }

    /// Compute count of non-null values in a column.
    pub fn count(&self, column_index: usize) -> Result<u64, StandingProgramRuntimeError> {
        let mut count: u64 = 0;
        for batch in &self.batches {
            let array = batch.column(column_index);
            count += (array.len() - array.null_count()) as u64;
        }
        Ok(count)
    }

    /// Compute min of a numeric column.
    pub fn min(&self, column_index: usize) -> Result<Option<i64>, StandingProgramRuntimeError> {
        let mut result: Option<i64> = None;
        for batch in &self.batches {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(invalid_runtime_state)?;
            for i in 0..array.len() {
                if !array.is_null(i) {
                    let val = array.value(i);
                    result = Some(match result {
                        Some(current) => current.min(val),
                        None => val,
                    });
                }
            }
        }
        Ok(result)
    }

    /// Compute max of a numeric column.
    pub fn max(&self, column_index: usize) -> Result<Option<i64>, StandingProgramRuntimeError> {
        let mut result: Option<i64> = None;
        for batch in &self.batches {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(invalid_runtime_state)?;
            for i in 0..array.len() {
                if !array.is_null(i) {
                    let val = array.value(i);
                    result = Some(match result {
                        Some(current) => current.max(val),
                        None => val,
                    });
                }
            }
        }
        Ok(result)
    }

    /// Get the total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Get the number of batches.
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Get the schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

/// Convert DeltaBatch records to an Arrow RecordBatch.
fn delta_records_to_record_batch(
    records: &[velorix_core::delta::DeltaRecord],
    schema: &SchemaRef,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let mut columns: Vec<Vec<serde_json::Value>> = vec![Vec::new(); schema.fields().len()];

    for record in records {
        if record.weight != 1 {
            return Err(invalid_runtime_state());
        }
        let value = record.value.as_json();
        if let Some(obj) = value.as_object() {
            for (i, field) in schema.fields().iter().enumerate() {
                let val = obj
                    .get(field.name())
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                columns[i].push(val);
            }
        } else {
            return Err(invalid_runtime_state());
        }
    }

    let arrays: Vec<ArrayRef> = columns
        .iter()
        .zip(schema.fields().iter())
        .map(|(values, field)| json_values_to_array(values, field))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema.clone(), arrays).map_err(|_| invalid_runtime_state())
}

/// Convert JSON values to an Arrow array.
fn json_values_to_array(
    values: &[serde_json::Value],
    field: &Field,
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    match field.data_type() {
        DataType::Utf8 => {
            let arr = values
                .iter()
                .map(|value| json_string_value(value, field))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StringArray::from(arr)))
        }
        DataType::Int64 => {
            let arr = values
                .iter()
                .map(|value| json_int64_value(value, field))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(Int64Array::from(arr)))
        }
        DataType::Float64 => {
            let arr = values
                .iter()
                .map(|value| json_float64_value(value, field))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(Float64Array::from(arr)))
        }
        DataType::Boolean => {
            let arr = values
                .iter()
                .map(|value| json_boolean_value(value, field))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(BooleanArray::from(arr)))
        }
        _ => Err(invalid_runtime_state()),
    }
}

fn json_string_value<'a>(
    value: &'a serde_json::Value,
    field: &Field,
) -> Result<Option<&'a str>, StandingProgramRuntimeError> {
    match value {
        serde_json::Value::Null => nullable_json_null(field),
        serde_json::Value::String(value) => Ok(Some(value)),
        _ => Err(json_type_error(field, "Utf8", json_value_type(value))),
    }
}

fn json_int64_value(
    value: &serde_json::Value,
    field: &Field,
) -> Result<Option<i64>, StandingProgramRuntimeError> {
    match value {
        serde_json::Value::Null => nullable_json_null(field),
        serde_json::Value::Number(number) => number
            .as_i64()
            .map(Some)
            .ok_or_else(|| json_type_error(field, "Int64", json_number_type(number))),
        _ => Err(json_type_error(field, "Int64", json_value_type(value))),
    }
}

fn json_float64_value(
    value: &serde_json::Value,
    field: &Field,
) -> Result<Option<f64>, StandingProgramRuntimeError> {
    match value {
        serde_json::Value::Null => nullable_json_null(field),
        serde_json::Value::Number(number) => number
            .as_f64()
            .map(Some)
            .ok_or_else(|| json_type_error(field, "Float64", "number(out-of-range)")),
        _ => Err(json_type_error(field, "Float64", json_value_type(value))),
    }
}

fn json_boolean_value(
    value: &serde_json::Value,
    field: &Field,
) -> Result<Option<bool>, StandingProgramRuntimeError> {
    match value {
        serde_json::Value::Null => nullable_json_null(field),
        serde_json::Value::Bool(value) => Ok(Some(*value)),
        _ => Err(json_type_error(field, "Boolean", json_value_type(value))),
    }
}

fn nullable_json_null<T>(field: &Field) -> Result<Option<T>, StandingProgramRuntimeError> {
    if field.is_nullable() {
        Ok(None)
    } else {
        Err(json_type_error(
            field,
            field.data_type().to_string().as_str(),
            "null",
        ))
    }
}

fn json_type_error(field: &Field, expected: &str, actual: &str) -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentityDynamic {
        field: Cow::Owned(format!(
            "arrow_json_type:field={}:expected={expected}:actual={actual}",
            field.name()
        )),
    }
}

fn json_value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(number) => json_number_type(number),
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn json_number_type(number: &serde_json::Number) -> &'static str {
    if number.is_i64() {
        "integer"
    } else if number.is_u64() {
        "integer(out-of-range)"
    } else {
        "float"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use serde_json::json;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    #[test]
    fn arrow_operator_filter() {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let op = ArrowBatchOperator::new(schema, vec![batch]);
        let mask = BooleanArray::from(vec![true, false, true, false, true]);
        let filtered = op.filter(&mask).unwrap();

        assert_eq!(filtered.row_count(), 3);
    }

    #[test]
    fn arrow_operator_select() {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let op = ArrowBatchOperator::new(schema, vec![batch]);
        let selected = op.select(&[0, 2]).unwrap();

        assert_eq!(selected.schema().fields().len(), 2);
        assert_eq!(selected.row_count(), 3);
    }

    #[test]
    fn arrow_operator_aggregates() {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let op = ArrowBatchOperator::new(schema, vec![batch]);

        assert_eq!(op.sum(2).unwrap(), 60);
        assert_eq!(op.count(2).unwrap(), 3);
        assert_eq!(op.min(2).unwrap(), Some(10));
        assert_eq!(op.max(2).unwrap(), Some(30));
    }

    #[test]
    fn arrow_operator_from_delta_records() {
        let schema = test_schema();
        // DeltaRecord: key is the primary key, value contains all columns
        let records = vec![
            velorix_core::delta::DeltaRecord::new(
                velorix_core::delta::DeltaKey::from_json(json!({"id": 1})),
                velorix_core::delta::DeltaValue::from_json(
                    json!({"id": 1, "name": "a", "value": 10}),
                ),
                1,
            ),
            velorix_core::delta::DeltaRecord::new(
                velorix_core::delta::DeltaKey::from_json(json!({"id": 2})),
                velorix_core::delta::DeltaValue::from_json(
                    json!({"id": 2, "name": "b", "value": 20}),
                ),
                1,
            ),
        ];

        let op = ArrowBatchOperator::from_delta_records(&records, schema).unwrap();
        assert_eq!(op.row_count(), 2);
        // Column index 2 is "value"
        assert_eq!(op.sum(2).unwrap(), 30);
    }

    #[test]
    fn delta_records_reject_non_null_json_values_with_the_wrong_primitive_type() {
        let cases = [
            ("text", DataType::Utf8, json!(false), "Utf8", "boolean"),
            ("integer", DataType::Int64, json!(1.5), "Int64", "float"),
            (
                "float",
                DataType::Float64,
                json!("1.0"),
                "Float64",
                "string",
            ),
            ("flag", DataType::Boolean, json!({}), "Boolean", "object"),
        ];

        for (field_name, data_type, value, expected, actual) in cases {
            let schema = Arc::new(Schema::new(vec![Field::new(field_name, data_type, true)]));
            let records = vec![delta_record(json!({ field_name: value }))];

            let error = conversion_error(&records, schema);
            assert_eq!(
                error.to_string(),
                format!(
                    "invalid standing program identity field: arrow_json_type:field={field_name}:expected={expected}:actual={actual}"
                )
            );
        }
    }

    #[test]
    fn delta_records_reject_integer_values_outside_the_int64_range() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let records = vec![delta_record(json!({ "id": u64::MAX }))];

        let error = conversion_error(&records, schema);
        assert_eq!(
            error.to_string(),
            "invalid standing program identity field: arrow_json_type:field=id:expected=Int64:actual=integer(out-of-range)"
        );
    }

    #[test]
    fn delta_records_preserve_int64_numeric_boundaries() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let records = vec![
            delta_record(json!({ "id": i64::MIN })),
            delta_record(json!({ "id": i64::MAX })),
        ];

        let op = ArrowBatchOperator::from_delta_records(&records, schema).unwrap();
        let ids = op.batches()[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[i64::MIN, i64::MAX]);
    }

    #[test]
    fn delta_records_convert_json_integer_to_float64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let records = vec![delta_record(json!({ "value": 1 }))];

        let op = ArrowBatchOperator::from_delta_records(&records, schema).unwrap();
        let values = op.batches()[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.values(), &[1.0]);
    }

    #[test]
    fn delta_records_preserve_explicit_null_for_nullable_fields() {
        let schema = test_schema();
        let records = vec![delta_record(json!({
            "id": 1,
            "name": "a",
            "value": null,
        }))];

        let op = ArrowBatchOperator::from_delta_records(&records, schema).unwrap();
        let values = op.batches()[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(values.is_null(0));
    }

    #[test]
    fn delta_records_reject_explicit_null_for_non_nullable_fields() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let records = vec![delta_record(json!({ "id": null }))];

        let error = conversion_error(&records, schema);
        assert_eq!(
            error.to_string(),
            "invalid standing program identity field: arrow_json_type:field=id:expected=Int64:actual=null"
        );
    }

    fn delta_record(value: serde_json::Value) -> velorix_core::delta::DeltaRecord {
        velorix_core::delta::DeltaRecord::new(
            velorix_core::delta::DeltaKey::from_json(json!({"id": 1})),
            velorix_core::delta::DeltaValue::from_json(value),
            1,
        )
    }

    fn conversion_error(
        records: &[velorix_core::delta::DeltaRecord],
        schema: SchemaRef,
    ) -> StandingProgramRuntimeError {
        match ArrowBatchOperator::from_delta_records(records, schema) {
            Ok(_) => panic!("expected JSON-to-Arrow conversion to fail"),
            Err(error) => error,
        }
    }
}

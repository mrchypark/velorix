use std::borrow::Cow;
use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        StringArray, TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use serde_json::Value;
use velorix_core::{
    delta::DeltaBatch,
    standing_program::{SnapshotPageRequest, StandingProgramRuntimeError},
    view_contract::{ColumnSchema, RelationSchema, SqlDataType},
    view_plan::SupportedAggregateOutput,
};

use super::{
    canonical_json, fixed_sum_count_outputs, invalid_runtime_state, project_aggregate_value,
};

pub(super) fn materialized_delta_to_record_batch(
    output_schema: &RelationSchema,
    state: &DeltaBatch,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let key_count = output_schema.primary_key.len();
    if output_schema.columns.len() < key_count {
        return Err(invalid_runtime_state());
    }
    let (key_columns, aggregate_columns) = output_schema.columns.split_at(key_count);
    let default_outputs;
    let aggregate_outputs = if let Some(aggregate_outputs) = aggregate_outputs {
        aggregate_outputs
    } else {
        default_outputs = fixed_sum_count_outputs();
        default_outputs.as_slice()
    };
    if aggregate_columns.len() != aggregate_outputs.len() {
        return Err(invalid_runtime_state());
    }
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let mut key_values = vec![Vec::new(); key_count];
    let mut aggregate_values = vec![Vec::new(); aggregate_outputs.len()];
    for row in rows {
        if row.weight != 1 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentityDynamic {
                field: Cow::Owned(format!("generic_page_weight:{}", row.weight)),
            });
        }
        let value = row
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        if key_count == 1 {
            key_values[0].push(row.key.as_json().clone());
        } else {
            let key = row
                .key
                .as_json()
                .as_object()
                .ok_or_else(invalid_runtime_state)?;
            for (index, column) in key_columns.iter().enumerate() {
                key_values[index].push(
                    key.get(column.name.as_str())
                        .cloned()
                        .ok_or_else(invalid_runtime_state)?,
                );
            }
        }
        for (index, aggregate) in aggregate_outputs.iter().enumerate() {
            aggregate_values[index].push(project_aggregate_value(value, aggregate)?);
        }
    }

    let mut fields = Vec::with_capacity(output_schema.columns.len());
    for column in key_columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            column.nullable,
        ));
    }
    for column in aggregate_columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            column.nullable,
        ));
    }
    let mut arrays = Vec::with_capacity(output_schema.columns.len());
    for (column, values) in key_columns.iter().zip(key_values.iter()) {
        arrays.push(output_column_value_array(column, values)?);
    }
    for (column, values) in aggregate_columns.iter().zip(aggregate_values.iter()) {
        arrays.push(output_column_value_array(column, values)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| invalid_runtime_state())
}

pub(super) fn materialized_delta_page_batch(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    logical_epoch: u64,
    page: SnapshotPageRequest,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
    if let Some(requested) = page.committed_epoch {
        if requested != logical_epoch {
            return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                requested,
                current: logical_epoch,
            });
        }
    }
    let mut rows = published_output
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    if let Some(page_token) = &page.page_token {
        // Since rows from net_rows() are already sorted by BTreeMap key order,
        // we can use binary search to find the starting position instead of
        // scanning from the beginning.
        let start = rows.partition_point(|row| canonical_json(row.key.as_json()) <= *page_token);
        rows = rows[start..].to_vec();
    }

    let limit = page.max_rows.unwrap_or(rows.len());
    if page.max_rows == Some(0) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "snapshot_page.max_rows",
        });
    }
    let has_next = rows.len() > limit;
    if has_next {
        rows.truncate(limit);
    }
    let next_page_token = if has_next {
        rows.last().map(|row| canonical_json(row.key.as_json()))
    } else {
        None
    };
    materialized_delta_to_record_batch(
        output_schema,
        &DeltaBatch::from_records(rows),
        aggregate_outputs,
    )
    .map(|batch| (batch, next_page_token))
}

pub(super) fn materialized_tumbling_delta_to_record_batch(
    output_schema: &RelationSchema,
    state: &DeltaBatch,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let [key_column, window_start_column, window_end_column, aggregate_columns @ ..] =
        output_schema.columns.as_slice()
    else {
        return Err(invalid_runtime_state());
    };
    if aggregate_columns.len() != aggregate_outputs.len() {
        return Err(invalid_runtime_state());
    }
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let mut group_keys = Vec::new();
    let mut window_starts = Vec::new();
    let mut window_ends = Vec::new();
    let mut aggregate_values = vec![Vec::new(); aggregate_columns.len()];
    for row in rows {
        if row.weight != 1 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentityDynamic {
                field: Cow::Owned(format!("generic_page_weight:{}", row.weight)),
            });
        }
        let key_values = row
            .key
            .as_json()
            .as_array()
            .ok_or_else(invalid_runtime_state)?;
        let [group_key, window_start, window_end] = key_values.as_slice() else {
            return Err(invalid_runtime_state());
        };
        let value = row
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        group_keys.push(group_key.clone());
        window_starts.push(window_start.clone());
        window_ends.push(window_end.clone());
        for (index, aggregate) in aggregate_outputs.iter().enumerate() {
            aggregate_values[index].push(project_aggregate_value(value, aggregate)?);
        }
    }

    let mut fields = Vec::with_capacity(output_schema.columns.len());
    for column in [key_column, window_start_column, window_end_column] {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            false,
        ));
    }
    for column in aggregate_columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            column.nullable,
        ));
    }
    let mut arrays = Vec::with_capacity(output_schema.columns.len());
    arrays.push(key_array(&key_column.data_type, &group_keys)?);
    arrays.push(output_value_array(
        &window_start_column.data_type,
        &window_starts,
    )?);
    arrays.push(output_value_array(
        &window_end_column.data_type,
        &window_ends,
    )?);
    for (column, values) in aggregate_columns.iter().zip(aggregate_values.iter()) {
        arrays.push(output_column_value_array(column, values)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| invalid_runtime_state())
}

pub(super) fn materialized_tumbling_delta_page_batch(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    aggregate_outputs: &[SupportedAggregateOutput],
    logical_epoch: u64,
    page: SnapshotPageRequest,
) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
    if let Some(requested) = page.committed_epoch {
        if requested != logical_epoch {
            return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                requested,
                current: logical_epoch,
            });
        }
    }
    let mut rows = published_output
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    if let Some(page_token) = &page.page_token {
        let start = rows.partition_point(|row| canonical_json(row.key.as_json()) <= *page_token);
        rows = rows[start..].to_vec();
    }

    let limit = page.max_rows.unwrap_or(rows.len());
    if page.max_rows == Some(0) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "snapshot_page.max_rows",
        });
    }
    let has_next = rows.len() > limit;
    if has_next {
        rows.truncate(limit);
    }
    let next_page_token = if has_next {
        rows.last().map(|row| canonical_json(row.key.as_json()))
    } else {
        None
    };
    materialized_tumbling_delta_to_record_batch(
        output_schema,
        &DeltaBatch::from_records(rows),
        aggregate_outputs,
    )
    .map(|batch| (batch, next_page_token))
}

pub(super) fn materialized_generic_delta_to_record_batch(
    output_schema: &RelationSchema,
    state: &DeltaBatch,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let [key_column, value_columns @ ..] = output_schema.columns.as_slice() else {
        return Err(invalid_runtime_state());
    };
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let mut keys = Vec::new();
    let mut column_values = vec![Vec::new(); value_columns.len()];
    for row in rows {
        if row.weight != 1 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentityDynamic {
                field: Cow::Owned(format!("generic_page_weight:{}", row.weight)),
            });
        }
        keys.push(row.key.as_json().clone());
        if !value_columns.is_empty() {
            let value = row
                .value
                .as_json()
                .as_object()
                .ok_or_else(invalid_runtime_state)?;
            for (index, column) in value_columns.iter().enumerate() {
                column_values[index].push(
                    value
                        .get(column.name.as_str())
                        .cloned()
                        .ok_or_else(invalid_runtime_state)?,
                );
            }
        }
    }

    let mut fields = Vec::with_capacity(output_schema.columns.len());
    fields.push(Field::new(
        key_column.name.as_str(),
        arrow_data_type(&key_column.data_type)?,
        false,
    ));
    for column in value_columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            column.nullable,
        ));
    }
    let mut arrays = Vec::with_capacity(output_schema.columns.len());
    arrays.push(key_array(&key_column.data_type, &keys)?);
    for (column, values) in value_columns.iter().zip(column_values.iter()) {
        arrays.push(output_column_value_array(column, values)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| invalid_runtime_state())
}

pub(super) fn materialized_generic_delta_page_batch(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    logical_epoch: u64,
    page: SnapshotPageRequest,
) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
    if let Some(requested) = page.committed_epoch {
        if requested != logical_epoch {
            return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                requested,
                current: logical_epoch,
            });
        }
    }
    let mut rows = published_output
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    if let Some(page_token) = &page.page_token {
        let start = rows.partition_point(|row| canonical_json(row.key.as_json()) <= *page_token);
        rows = rows[start..].to_vec();
    }

    let limit = page.max_rows.unwrap_or(rows.len());
    if page.max_rows == Some(0) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "snapshot_page.max_rows",
        });
    }
    let has_next = rows.len() > limit;
    if has_next {
        rows.truncate(limit);
    }
    let next_page_token = if has_next {
        rows.last().map(|row| canonical_json(row.key.as_json()))
    } else {
        None
    };
    materialized_generic_delta_to_record_batch(output_schema, &DeltaBatch::from_records(rows))
        .map(|batch| (batch, next_page_token))
}

fn key_array(
    data_type: &SqlDataType,
    values: &[Value],
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.as_str().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| value.as_f64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Bool => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.as_bool().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Decimal { precision, scale } => Ok(Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .and_then(|value| parse_decimal128(value, *precision, *scale))
                            .ok_or_else(invalid_runtime_state)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_precision_and_scale(
                *precision,
                i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
            )
            .map_err(|_| invalid_runtime_state())?,
        )),
        SqlDataType::Json => Ok(Arc::new(StringArray::from(
            values.iter().map(canonical_json).collect::<Vec<_>>(),
        ))),
        SqlDataType::Date => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|value| {
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(invalid_runtime_state)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Timestamp { timezone } => Ok(Arc::new(
            TimestampNanosecondArray::from(
                values
                    .iter()
                    .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_timezone_opt(timezone.clone()),
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.key",
        }),
    }
}

fn output_value_array(
    data_type: &SqlDataType,
    values: &[Value],
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    output_column_value_array(
        &ColumnSchema {
            name: String::new(),
            data_type: data_type.clone(),
            nullable: false,
        },
        values,
    )
}

fn output_column_value_array(
    column: &ColumnSchema,
    values: &[Value],
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    if !column.nullable && values.iter().any(Value::is_null) {
        return Err(invalid_runtime_state());
    }
    match &column.data_type {
        SqlDataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value.as_str().map(Some).ok_or_else(invalid_runtime_state)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value.as_i64().map(Some).ok_or_else(invalid_runtime_state)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Bool => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value.as_bool().map(Some).ok_or_else(invalid_runtime_state)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value.as_f64().map(Some).ok_or_else(invalid_runtime_state)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Date => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        return Ok(None);
                    }
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .map(Some)
                        .ok_or_else(invalid_runtime_state)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Timestamp { timezone } => Ok(Arc::new(
            TimestampNanosecondArray::from(
                values
                    .iter()
                    .map(|value| {
                        if value.is_null() {
                            return Ok(None);
                        }
                        value.as_i64().map(Some).ok_or_else(invalid_runtime_state)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_timezone_opt(timezone.clone()),
        )),
        SqlDataType::Decimal { precision, scale } => Ok(Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|value| {
                        if value.is_null() {
                            return Ok(None);
                        }
                        value
                            .as_str()
                            .and_then(|value| parse_decimal128(value, *precision, *scale))
                            .map(Some)
                            .ok_or_else(invalid_runtime_state)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_precision_and_scale(
                *precision,
                i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
            )
            .map_err(|_| invalid_runtime_state())?,
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.sum",
        }),
    }
}

fn arrow_data_type(data_type: &SqlDataType) -> Result<DataType, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Utf8 => Ok(DataType::Utf8),
        SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Bool => Ok(DataType::Boolean),
        SqlDataType::Decimal { precision, scale } => Ok(DataType::Decimal128(
            *precision,
            i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
        )),
        SqlDataType::Json => Ok(DataType::Utf8),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Timestamp { timezone } => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            timezone.clone().map(Into::into),
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        }),
    }
}

pub(super) fn parse_decimal128(value: &str, precision: u8, scale: u8) -> Option<i128> {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let scale = usize::from(scale);
    let (whole, fractional) = match digits.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None if scale == 0 => (digits, ""),
        None => return None,
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fractional.len() != scale
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let mut magnitude = whole.parse::<i128>().ok()?;
    let factor = 10_i128.checked_pow(scale.try_into().ok()?)?;
    magnitude = magnitude.checked_mul(factor)?;
    if scale > 0 {
        magnitude = magnitude.checked_add(fractional.parse::<i128>().ok()?)?;
    }
    if magnitude.unsigned_abs().to_string().len() > usize::from(precision) {
        return None;
    }
    if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    }
}

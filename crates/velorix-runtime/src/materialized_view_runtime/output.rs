use std::borrow::Cow;
use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        Time64NanosecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
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

use velorix_core::delta::DeltaRecord;

use super::{
    canonical_json, fixed_sum_count_outputs, invalid_runtime_state, project_aggregate_value,
};
use velorix_core::delta::encode_kv_ordered;

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
        // Use the same encoding as net_rows() for consistent ordering.
        // page_token is canonical_json(key), which sorts identically to
        // encode_kv_ordered(key, Null) for key comparison purposes.
        let token_key = encode_kv_ordered(
            &serde_json::from_str::<serde_json::Value>(page_token)
                .map_err(|_| invalid_runtime_state())?,
            &Value::Null,
        );
        let start = rows
            .partition_point(|row| encode_kv_ordered(row.key.as_json(), &Value::Null) <= token_key);
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
        let token_key = encode_kv_ordered(
            &serde_json::from_str::<serde_json::Value>(page_token)
                .map_err(|_| invalid_runtime_state())?,
            &Value::Null,
        );
        let start = rows
            .partition_point(|row| encode_kv_ordered(row.key.as_json(), &Value::Null) <= token_key);
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
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    materialized_generic_rows_to_record_batch(output_schema, &rows)
}

// Call only with already consolidated rows; weights and column values are still
// validated here. This avoids sorting a freshly consolidated snapshot again.
pub(super) fn materialized_generic_rows_to_record_batch(
    output_schema: &RelationSchema,
    rows: &[DeltaRecord],
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let [key_column, value_columns @ ..] = output_schema.columns.as_slice() else {
        return Err(invalid_runtime_state());
    };
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
    if rows.iter().any(|row| row.weight != 1) {
        let total = rows.iter().try_fold(0usize, |total, row| {
            let copies = usize::try_from(row.weight)
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(invalid_runtime_state)?;
            total.checked_add(copies).ok_or_else(invalid_runtime_state)
        })?;
        let offset = if let Some(token) = &page.page_token {
            let token = token
                .strip_prefix("velorix-bag:")
                .ok_or_else(invalid_runtime_state)?;
            let (schema, epoch, offset): (String, u64, usize) =
                serde_json::from_str(token).map_err(|_| invalid_runtime_state())?;
            if schema != output_schema.schema_fingerprint
                || epoch != logical_epoch
                || offset >= total
            {
                return Err(invalid_runtime_state());
            }
            offset
        } else {
            0
        };
        let limit = page.max_rows.unwrap_or(total);
        if limit == 0 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "snapshot_page.max_rows",
            });
        }
        let count = limit.min(total - offset);
        let end = offset + count;
        let next = if end < total {
            Some(format!(
                "velorix-bag:{}",
                serde_json::to_string(&(&output_schema.schema_fingerprint, logical_epoch, end,))
                    .map_err(|_| invalid_runtime_state())?
            ))
        } else {
            None
        };
        return materialized_canonical_output_rows_batch(output_schema, &rows, offset, count)
            .map(|batch| (batch, next));
    }
    if let Some(page_token) = &page.page_token {
        let token_key = encode_kv_ordered(
            &serde_json::from_str::<serde_json::Value>(page_token)
                .map_err(|_| invalid_runtime_state())?,
            &Value::Null,
        );
        let start = rows
            .partition_point(|row| encode_kv_ordered(row.key.as_json(), &Value::Null) <= token_key);
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
    if !matches!(data_type, SqlDataType::Json) {
        return output_value_array(data_type, values);
    }
    Ok(Arc::new(StringArray::from(
        values.iter().map(canonical_json).collect::<Vec<_>>(),
    )))
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
    macro_rules! primitive_array {
        ($array:ty, $convert:expr) => {
            Ok(Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|value| {
                        if value.is_null() {
                            Ok(None)
                        } else {
                            ($convert)(value)
                                .map(Some)
                                .ok_or_else(invalid_runtime_state)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )))
        };
    }
    match &column.data_type {
        SqlDataType::Int8 => primitive_array!(Int8Array, |v: &Value| v
            .as_i64()
            .and_then(|n| i8::try_from(n).ok())),
        SqlDataType::Int16 => primitive_array!(Int16Array, |v: &Value| v
            .as_i64()
            .and_then(|n| i16::try_from(n).ok())),
        SqlDataType::Int32 => primitive_array!(Int32Array, |v: &Value| v
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())),
        SqlDataType::UInt8 => primitive_array!(UInt8Array, |v: &Value| v
            .as_u64()
            .and_then(|n| u8::try_from(n).ok())),
        SqlDataType::UInt16 => primitive_array!(UInt16Array, |v: &Value| v
            .as_u64()
            .and_then(|n| u16::try_from(n).ok())),
        SqlDataType::UInt32 => primitive_array!(UInt32Array, |v: &Value| v
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())),
        SqlDataType::UInt64 => primitive_array!(UInt64Array, Value::as_u64),
        SqlDataType::Float32 => primitive_array!(Float32Array, |v: &Value| v
            .as_f64()
            .map(|n| n as f32)
            .filter(|n| n.is_finite())),
        SqlDataType::Time => primitive_array!(Time64NanosecondArray, Value::as_i64),
        SqlDataType::Json => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| (!value.is_null()).then(|| canonical_json(value)))
                .collect::<Vec<_>>(),
        ))),
        SqlDataType::Binary { .. } | SqlDataType::Varbinary => {
            let bytes = values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        return Ok(None);
                    }
                    let raw = value.as_str().ok_or_else(invalid_runtime_state)?.trim();
                    let raw = raw
                        .strip_prefix("0x")
                        .or_else(|| raw.strip_prefix("0X"))
                        .unwrap_or(raw);
                    let bytes = hex::decode(raw).map_err(|_| invalid_runtime_state())?;
                    if let SqlDataType::Binary { length } = &column.data_type {
                        if bytes.len() != *length as usize {
                            return Err(invalid_runtime_state());
                        }
                    }
                    Ok(Some(bytes))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(BinaryArray::from_iter(
                bytes.iter().map(|bytes| bytes.as_deref()),
            )))
        }
        SqlDataType::Utf8 | SqlDataType::Char { .. } | SqlDataType::Uuid => {
            Ok(Arc::new(StringArray::from(
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
            )))
        }
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
        SqlDataType::Utf8 | SqlDataType::Char { .. } | SqlDataType::Uuid => Ok(DataType::Utf8),
        SqlDataType::Int8 => Ok(DataType::Int8),
        SqlDataType::Int16 => Ok(DataType::Int16),
        SqlDataType::Int32 => Ok(DataType::Int32),
        SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::UInt8 => Ok(DataType::UInt8),
        SqlDataType::UInt16 => Ok(DataType::UInt16),
        SqlDataType::UInt32 => Ok(DataType::UInt32),
        SqlDataType::UInt64 => Ok(DataType::UInt64),
        SqlDataType::Float32 => Ok(DataType::Float32),
        SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Binary { .. } | SqlDataType::Varbinary => Ok(DataType::Binary),
        SqlDataType::Time => Ok(DataType::Time64(TimeUnit::Nanosecond)),
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

pub(super) fn materialized_bag_delta_page_batch(
    schema: &RelationSchema,
    state: &DeltaBatch,
    epoch: u64,
    page: SnapshotPageRequest,
) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
    if page
        .committed_epoch
        .is_some_and(|requested| requested != epoch)
    {
        return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
            requested: page.committed_epoch.unwrap(),
            current: epoch,
        });
    }
    if page.max_rows == Some(0) {
        return Err(invalid_runtime_state());
    }
    let offset = match page.page_token {
        Some(token) => {
            let (token_epoch, offset): (u64, usize) =
                serde_json::from_str(&token).map_err(|_| invalid_runtime_state())?;
            if token_epoch != epoch {
                return Err(invalid_runtime_state());
            }
            offset
        }
        None => 0,
    };
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let total = rows.iter().try_fold(0usize, |total, row| {
        let copies = usize::try_from(row.weight)
            .ok()
            .filter(|copies| *copies > 0)
            .ok_or_else(invalid_runtime_state)?;
        total.checked_add(copies).ok_or_else(invalid_runtime_state)
    })?;
    if offset > total {
        return Err(invalid_runtime_state());
    }
    let end = offset
        .saturating_add(page.max_rows.unwrap_or(total))
        .min(total);
    let batch = materialized_bag_canonical_rows_batch(schema, &rows, offset, end - offset)?;
    let token = if end < total {
        Some(serde_json::to_string(&(epoch, end)).map_err(|_| invalid_runtime_state())?)
    } else {
        None
    };
    Ok((batch, token))
}

/// Converts only supplied canonical stored-page rows. Offsets are relative to
/// these rows, not the full snapshot. Never consolidates or reads runtime state.
fn materialized_bag_canonical_rows_batch(
    schema: &RelationSchema,
    rows: &[DeltaRecord],
    offset: usize,
    count: usize,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let end = offset
        .checked_add(count)
        .ok_or_else(invalid_runtime_state)?;
    let mut values = vec![Vec::new(); schema.columns.len()];
    let mut position = 0usize;
    for row in rows {
        let copies = usize::try_from(row.weight).map_err(|_| invalid_runtime_state())?;
        let next = position
            .checked_add(copies)
            .ok_or_else(invalid_runtime_state)?;
        let take = next.min(end).saturating_sub(position.max(offset));
        if take > 0 {
            for (index, column) in schema.columns.iter().enumerate() {
                let value = row
                    .key
                    .as_json()
                    .as_object()
                    .and_then(|key| key.get(&column.name))
                    .or_else(|| {
                        if schema.primary_key == [column.name.clone()] {
                            Some(row.key.as_json())
                        } else {
                            None
                        }
                    })
                    .or_else(|| {
                        row.value
                            .as_json()
                            .as_object()
                            .and_then(|value| value.get(&column.name))
                    })
                    .ok_or_else(invalid_runtime_state)?;
                values[index].extend(std::iter::repeat_n(value.clone(), take));
            }
        }
        position = next;
        if position >= end {
            break;
        }
    }
    let fields = schema
        .columns
        .iter()
        .map(|column| {
            Ok(Field::new(
                &column.name,
                arrow_data_type(&column.data_type)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
    let arrays = schema
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, values)| output_column_value_array(column, values))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|_| invalid_runtime_state())?;
    Ok(batch)
}

pub(super) fn materialized_bag_canonical_batch(
    schema: &RelationSchema,
    rows: &DeltaBatch,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let mut total = 0usize;
    let mut previous = None;
    for row in rows.records() {
        let copies = usize::try_from(row.weight)
            .ok()
            .filter(|copies| *copies > 0)
            .ok_or_else(invalid_runtime_state)?;
        total = total
            .checked_add(copies)
            .filter(|total| *total <= 512)
            .ok_or_else(invalid_runtime_state)?;
        let order = encode_kv_ordered(row.key.as_json(), row.value.as_json());
        if previous.as_ref().is_some_and(|previous| previous >= &order) {
            return Err(invalid_runtime_state());
        }
        previous = Some(order);
    }
    materialized_bag_canonical_rows_batch(schema, rows.records(), 0, total)
}
fn materialized_canonical_output_rows_batch(
    schema: &RelationSchema,
    rows: &[DeltaRecord],
    offset: usize,
    count: usize,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let end = offset
        .checked_add(count)
        .ok_or_else(invalid_runtime_state)?;
    let mut values = vec![Vec::new(); schema.columns.len()];
    let mut position = 0usize;
    for row in rows {
        let copies = usize::try_from(row.weight).map_err(|_| invalid_runtime_state())?;
        let next = position
            .checked_add(copies)
            .ok_or_else(invalid_runtime_state)?;
        let take = next.min(end).saturating_sub(position.max(offset));
        if take > 0 {
            for (index, column) in schema.columns.iter().enumerate() {
                let value = row
                    .value
                    .as_json()
                    .as_object()
                    .and_then(|value| value.get(&column.name))
                    .or_else(|| {
                        row.key
                            .as_json()
                            .as_object()
                            .and_then(|key| key.get(&column.name))
                    })
                    .or_else(|| {
                        if schema.primary_key == [column.name.clone()] {
                            Some(row.key.as_json())
                        } else {
                            None
                        }
                    })
                    .ok_or_else(invalid_runtime_state)?;
                values[index].extend(std::iter::repeat_n(value.clone(), take));
            }
        }
        position = next;
        if position >= end {
            break;
        }
    }
    let fields = schema
        .columns
        .iter()
        .map(|column| {
            Ok(Field::new(
                &column.name,
                arrow_data_type(&column.data_type)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
    let arrays = schema
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, values)| output_column_value_array(column, values))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|_| invalid_runtime_state())?;
    Ok(batch)
}

pub(super) fn materialized_canonical_output_batch(
    schema: &RelationSchema,
    rows: &DeltaBatch,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let mut total = 0usize;
    let mut previous = None;
    for row in rows.records() {
        let copies = usize::try_from(row.weight)
            .ok()
            .filter(|copies| *copies > 0)
            .ok_or_else(invalid_runtime_state)?;
        total = total
            .checked_add(copies)
            .filter(|total| *total <= 512)
            .ok_or_else(invalid_runtime_state)?;
        let order = encode_kv_ordered(row.key.as_json(), row.value.as_json());
        if previous.as_ref().is_some_and(|previous| previous >= &order) {
            return Err(invalid_runtime_state());
        }
        previous = Some(order);
    }
    materialized_canonical_output_rows_batch(schema, rows.records(), 0, total)
}
#[cfg(test)]
mod bag_page_tests {
    use super::*;
    use serde_json::json;
    use velorix_core::delta::{DeltaKey, DeltaValue};

    fn stored_page_schema() -> RelationSchema {
        RelationSchema {
            relation_id: "fact".into(),
            relation_name: "fact".into(),
            relation_version: "v1".into(),
            schema_fingerprint: "bound".into(),
            columns: vec![
                ColumnSchema {
                    name: "date".into(),
                    data_type: SqlDataType::Timestamp { timezone: None },
                    nullable: false,
                },
                ColumnSchema {
                    name: "power".into(),
                    data_type: SqlDataType::Float64,
                    nullable: true,
                },
            ],
            primary_key: vec!["date".into()],
        }
    }

    fn stored_row(power: Value, weight: i64) -> DeltaRecord {
        DeltaRecord {
            key: DeltaKey::from_json(json!({"date": 1_234_567_890_123_i64})),
            value: DeltaValue::from_json(json!({"power": power})),
            weight,
        }
    }

    #[test]
    fn canonical_stored_pages_preserve_split_weights_and_same_key_values() {
        let schema = stored_page_schema();
        let null = stored_row(Value::Null, 510);
        let number = stored_row(json!(2.5), 2);
        let mut first_rows = vec![null.clone(), number.clone()];
        first_rows
            .sort_by_cached_key(|row| encode_kv_ordered(row.key.as_json(), row.value.as_json()));
        let first = materialized_bag_canonical_batch(
            &schema,
            &DeltaBatch::from_records(first_rows.clone()),
        )
        .unwrap();
        assert_eq!(first.num_rows(), 512);
        assert_eq!(first.column(1).null_count(), 510);
        let timestamps = first
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(timestamps.value(511), 1_234_567_890_123);
        // The boundary pair continues in the next page without deduplication.
        let mut continuation = first_rows.last().unwrap().clone();
        continuation.weight = 3;
        let second = materialized_bag_canonical_batch(
            &schema,
            &DeltaBatch::from_records(vec![continuation]),
        )
        .unwrap();
        assert_eq!(second.num_rows(), 3);
        for column in 0..2 {
            assert_eq!(
                first.column(column).slice(511, 1).to_data(),
                second.column(column).slice(0, 1).to_data()
            );
        }
        let empty =
            materialized_bag_canonical_batch(&schema, &DeltaBatch::from_records(vec![])).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), first.schema());
    }

    #[test]
    fn canonical_stored_pages_fail_closed_without_consolidating() {
        let schema = stored_page_schema();
        for weight in [-1, 0, 513, i64::MAX] {
            assert!(materialized_bag_canonical_batch(
                &schema,
                &DeltaBatch::from_records(vec![stored_row(Value::Null, weight)]),
            )
            .is_err());
        }
        // Signed cancellation and repeated pairs are not repaired on reads.
        for rows in [
            vec![stored_row(Value::Null, 2), stored_row(Value::Null, -1)],
            vec![stored_row(Value::Null, 1), stored_row(Value::Null, 1)],
            vec![stored_row(Value::Null, 512), stored_row(json!(1), i64::MAX)],
        ] {
            assert!(
                materialized_bag_canonical_batch(&schema, &DeltaBatch::from_records(rows),)
                    .is_err()
            );
        }
        let mut rows = vec![stored_row(Value::Null, 1), stored_row(json!(1), 1)];
        rows.sort_by_cached_key(|row| encode_kv_ordered(row.key.as_json(), row.value.as_json()));
        rows.reverse();
        assert!(
            materialized_bag_canonical_batch(&schema, &DeltaBatch::from_records(rows),).is_err()
        );
        for value in [json!({}), json!({"power": "NaN"})] {
            let mut row = stored_row(Value::Null, 1);
            row.value = DeltaValue::from_json(value);
            assert!(materialized_bag_canonical_batch(
                &schema,
                &DeltaBatch::from_records(vec![row]),
            )
            .is_err());
        }
        let mut row = stored_row(Value::Null, 1);
        row.key = DeltaKey::from_json(json!({"date": null}));
        assert!(
            materialized_bag_canonical_batch(&schema, &DeltaBatch::from_records(vec![row]),)
                .is_err()
        );
    }

    #[test]
    fn composite_keys_nonadjacent_columns_and_bag_copy_pagination() {
        let schema = RelationSchema {
            relation_id: "raw".into(),
            relation_name: "raw".into(),
            relation_version: "r42".into(),
            schema_fingerprint: "bound".into(),
            columns: vec![
                ColumnSchema {
                    name: "timestamp".into(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "power".into(),
                    data_type: SqlDataType::Float64,
                    nullable: true,
                },
                ColumnSchema {
                    name: "series_key".into(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
            ],
            primary_key: vec!["timestamp".into(), "series_key".into()],
        };
        let output = DeltaBatch::from_records(vec![DeltaRecord {
            key: DeltaKey::from_json(json!({"timestamp": 17, "series_key": "p"})),
            value: DeltaValue::from_json(json!({"power": null})),
            weight: 2,
        }]);
        let request = SnapshotPageRequest {
            committed_epoch: Some(7),
            max_rows: Some(1),
            ..Default::default()
        };
        let (first, token) =
            materialized_bag_delta_page_batch(&schema, &output, 7, request).unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            17
        );
        assert_eq!(
            first
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "p"
        );
        assert_eq!(first.column(1).null_count(), 1);
        let (second, end) = materialized_bag_delta_page_batch(
            &schema,
            &output,
            7,
            SnapshotPageRequest {
                page_token: token.clone(),
                max_rows: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(second, first);
        assert!(end.is_none());
        assert!(materialized_bag_delta_page_batch(
            &schema,
            &output,
            8,
            SnapshotPageRequest {
                page_token: token,
                ..Default::default()
            }
        )
        .is_err());
    }
}

//! Shared deterministic scalar evaluator for `TypedExprProgramV1`.
//!
//! Every operator (filter/project, aggregate, join, window, analytic) that
//! admits typed expressions evaluates them here, so expression semantics
//! live in exactly one place. All functions are pure and deterministic;
//! float arithmetic is finite-only (NaN/Inf inputs or results fail closed).

use std::time::Duration;

use velorix_core::view_plan::{
    BuiltinScalarFunctionV1, RuntimeScalarTypeV1, ScalarLiteralV1, TypedExprKindV1,
    TypedExprNodeV1, TypedExprProgramV1,
};

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeScalarValue {
    Null(RuntimeScalarTypeV1),
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Decimal128 { unscaled: i128, scale: i8 },
    Utf8(String),
    Date32(i32),
    TimestampNanosecond(i64),
}

impl RuntimeScalarValue {
    pub fn data_type(&self) -> RuntimeScalarTypeV1 {
        match self {
            RuntimeScalarValue::Null(data_type) => *data_type,
            RuntimeScalarValue::Boolean(_) => RuntimeScalarTypeV1::Boolean,
            RuntimeScalarValue::Int64(_) => RuntimeScalarTypeV1::Int64,
            RuntimeScalarValue::Float64(_) => RuntimeScalarTypeV1::Float64,
            RuntimeScalarValue::Decimal128 { scale, .. } => RuntimeScalarTypeV1::Decimal128 {
                precision: 38,
                scale: *scale,
            },
            RuntimeScalarValue::Utf8(_) => RuntimeScalarTypeV1::Utf8,
            RuntimeScalarValue::Date32(_) => RuntimeScalarTypeV1::Date32,
            RuntimeScalarValue::TimestampNanosecond(_) => RuntimeScalarTypeV1::TimestampNanosecond,
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ExpressionEvaluationError {
    #[error("typed expression column `{0}` is missing from the input batch")]
    MissingColumn(String),
    #[error("typed expression evaluated to a non-finite float (NaN or infinity)")]
    NonFiniteFloat,
    #[error("typed expression failed: {0}")]
    Failed(String),
}

use thiserror::Error;

/// Evaluates a typed expression program against a row accessor.
pub fn evaluate_typed_expr(
    program: &TypedExprProgramV1,
    get_column: &dyn Fn(&str) -> Option<RuntimeScalarValue>,
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    program
        .validate()
        .map_err(|error| ExpressionEvaluationError::Failed(error.to_string()))?;
    evaluate_node(&program.root, get_column)
}

pub fn evaluate_node(
    node: &TypedExprNodeV1,
    get_column: &dyn Fn(&str) -> Option<RuntimeScalarValue>,
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    match &node.kind {
        TypedExprKindV1::Column { column_id } => get_column(column_id)
            .ok_or_else(|| ExpressionEvaluationError::MissingColumn(column_id.clone())),
        TypedExprKindV1::Literal { value } => Ok(literal_value(value)),
        TypedExprKindV1::Call { function, args } => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(evaluate_node(arg, get_column)?);
            }
            evaluate_call(*function, &values)
        }
        TypedExprKindV1::UdfCall { identity, args } => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(evaluate_node(arg, get_column)?);
            }
            evaluate_udf(identity, &values)
        }
    }
}

fn literal_value(value: &ScalarLiteralV1) -> RuntimeScalarValue {
    match value {
        ScalarLiteralV1::Null { data_type } => RuntimeScalarValue::Null(*data_type),
        ScalarLiteralV1::Boolean(value) => RuntimeScalarValue::Boolean(*value),
        ScalarLiteralV1::Int64(value) => RuntimeScalarValue::Int64(*value),
        ScalarLiteralV1::Float64 { canonical_bits } => {
            RuntimeScalarValue::Float64(f64::from_bits(*canonical_bits))
        }
        ScalarLiteralV1::Decimal128 {
            unscaled, scale, ..
        } => RuntimeScalarValue::Decimal128 {
            unscaled: unscaled.value(),
            scale: *scale,
        },
        ScalarLiteralV1::Utf8 { value } => RuntimeScalarValue::Utf8(value.clone()),
        ScalarLiteralV1::Date32(value) => RuntimeScalarValue::Date32(*value),
        ScalarLiteralV1::TimestampNanosecond(value) => {
            RuntimeScalarValue::TimestampNanosecond(*value)
        }
    }
}

fn evaluate_call(
    function: BuiltinScalarFunctionV1,
    args: &[RuntimeScalarValue],
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    match function {
        BuiltinScalarFunctionV1::Concat => {
            let mut out = String::new();
            for arg in args {
                match arg {
                    RuntimeScalarValue::Utf8(value) => out.push_str(value),
                    RuntimeScalarValue::Null(_) => {
                        return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Utf8))
                    }
                    other => {
                        return Err(ExpressionEvaluationError::Failed(format!(
                            "CONCAT requires utf8 arguments, got {:?}",
                            other.data_type()
                        )))
                    }
                }
            }
            Ok(RuntimeScalarValue::Utf8(out))
        }
        BuiltinScalarFunctionV1::Substring => {
            let (value, start, length) = match args {
                [RuntimeScalarValue::Utf8(value), RuntimeScalarValue::Int64(start), length] => {
                    let length = match length {
                        RuntimeScalarValue::Int64(length) => Some(*length),
                        RuntimeScalarValue::Null(_) => None,
                        other => {
                            return Err(ExpressionEvaluationError::Failed(format!(
                                "SUBSTRING length must be int64 or null, got {:?}",
                                other.data_type()
                            )))
                        }
                    };
                    (value, start, length)
                }
                [RuntimeScalarValue::Null(_), ..] => {
                    return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Utf8))
                }
                _ => {
                    return Err(ExpressionEvaluationError::Failed(
                        "SUBSTRING requires (utf8, int64, [int64])".to_string(),
                    ))
                }
            };
            if *start <= 0 {
                return Err(ExpressionEvaluationError::Failed(
                    "SUBSTRING start must be positive (1-based)".to_string(),
                ));
            }
            if let Some(length) = length {
                if length < 0 {
                    return Err(ExpressionEvaluationError::Failed(
                        "SUBSTRING length must be non-negative".to_string(),
                    ));
                }
            }
            let chars = value.chars().collect::<Vec<_>>();
            let from = (*start - 1) as usize;
            let selected = match length {
                Some(length) => {
                    if from >= chars.len() {
                        String::new()
                    } else {
                        let end = from.saturating_add(length as usize).min(chars.len());
                        chars[from..end].iter().collect()
                    }
                }
                None => chars.get(from..).unwrap_or_default().iter().collect(),
            };
            Ok(RuntimeScalarValue::Utf8(selected))
        }
        BuiltinScalarFunctionV1::Upper | BuiltinScalarFunctionV1::Lower => match &args[0] {
            RuntimeScalarValue::Utf8(value) => Ok(RuntimeScalarValue::Utf8(
                if function == BuiltinScalarFunctionV1::Upper {
                    value.to_uppercase()
                } else {
                    value.to_lowercase()
                },
            )),
            RuntimeScalarValue::Null(_) => Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Utf8)),
            other => Err(ExpressionEvaluationError::Failed(format!(
                "case conversion requires utf8, got {:?}",
                other.data_type()
            ))),
        },
        BuiltinScalarFunctionV1::Trim => {
            let value = match &args[0] {
                RuntimeScalarValue::Utf8(value) => value.clone(),
                RuntimeScalarValue::Null(_) => {
                    return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Utf8))
                }
                other => {
                    return Err(ExpressionEvaluationError::Failed(format!(
                        "TRIM requires utf8, got {:?}",
                        other.data_type()
                    )))
                }
            };
            let trimmed = if args.len() > 1 {
                match &args[1] {
                    RuntimeScalarValue::Utf8(chars) => {
                        value.trim_matches(|c| chars.contains(c)).to_string()
                    }
                    RuntimeScalarValue::Null(_) => {
                        return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Utf8))
                    }
                    other => {
                        return Err(ExpressionEvaluationError::Failed(format!(
                            "TRIM characters must be utf8, got {:?}",
                            other.data_type()
                        )))
                    }
                }
            } else {
                value.trim().to_string()
            };
            Ok(RuntimeScalarValue::Utf8(trimmed))
        }
        BuiltinScalarFunctionV1::Length => match &args[0] {
            RuntimeScalarValue::Utf8(value) => {
                Ok(RuntimeScalarValue::Int64(value.chars().count() as i64))
            }
            RuntimeScalarValue::Null(_) => Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64)),
            other => Err(ExpressionEvaluationError::Failed(format!(
                "LENGTH requires utf8, got {:?}",
                other.data_type()
            ))),
        },
        BuiltinScalarFunctionV1::ExtractYear
        | BuiltinScalarFunctionV1::ExtractMonth
        | BuiltinScalarFunctionV1::ExtractDay
        | BuiltinScalarFunctionV1::ExtractHour
        | BuiltinScalarFunctionV1::ExtractMinute
        | BuiltinScalarFunctionV1::ExtractSecond => {
            // The function form carries a leading field literal; the
            // timestamp is the trailing argument.
            extract_part(function, args.last().expect("arity checked"))
        }
        BuiltinScalarFunctionV1::DateTruncDay
        | BuiltinScalarFunctionV1::DateTruncHour
        | BuiltinScalarFunctionV1::DateTruncMinute
        | BuiltinScalarFunctionV1::DateTruncSecond => {
            date_trunc(function, args.last().expect("arity checked"))
        }
        BuiltinScalarFunctionV1::TimestampAddNanoseconds
        | BuiltinScalarFunctionV1::TimestampSubtractNanoseconds => {
            timestamp_add_interval(function, &args[0], &args[1])
        }
        BuiltinScalarFunctionV1::DateAddDays => date_add_days(&args[0], &args[1]),
        BuiltinScalarFunctionV1::AbsFloat64 => unary_float(function, &args[0]),
        BuiltinScalarFunctionV1::CeilFloat64 | BuiltinScalarFunctionV1::FloorFloat64 => {
            unary_float(function, &args[0])
        }
        BuiltinScalarFunctionV1::RoundFloat64 => {
            let value = match &args[0] {
                RuntimeScalarValue::Float64(value) => *value,
                RuntimeScalarValue::Null(_) => {
                    return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Float64))
                }
                other => {
                    return Err(ExpressionEvaluationError::Failed(format!(
                        "ROUND requires float64, got {:?}",
                        other.data_type()
                    )))
                }
            };
            if !value.is_finite() {
                return Err(ExpressionEvaluationError::NonFiniteFloat);
            }
            Ok(RuntimeScalarValue::Float64(round_half_away(value)))
        }
        BuiltinScalarFunctionV1::AddFloat64
        | BuiltinScalarFunctionV1::SubtractFloat64
        | BuiltinScalarFunctionV1::MultiplyFloat64
        | BuiltinScalarFunctionV1::DivideFloat64 => {
            if args
                .iter()
                .any(|arg| matches!(arg, RuntimeScalarValue::Null(_)))
            {
                return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Float64));
            }
            let left = float_operand(&args[0], function)?;
            let right = float_operand(&args[1], function)?;
            if left.is_nan() || right.is_nan() {
                return Err(ExpressionEvaluationError::NonFiniteFloat);
            }
            let result = match function {
                BuiltinScalarFunctionV1::AddFloat64 => left + right,
                BuiltinScalarFunctionV1::SubtractFloat64 => left - right,
                BuiltinScalarFunctionV1::MultiplyFloat64 => left * right,
                BuiltinScalarFunctionV1::DivideFloat64 => {
                    if right == 0.0 {
                        return Err(ExpressionEvaluationError::Failed(
                            "float division by zero".to_string(),
                        ));
                    }
                    left / right
                }
                _ => unreachable!(),
            };
            if !result.is_finite() {
                return Err(ExpressionEvaluationError::NonFiniteFloat);
            }
            Ok(RuntimeScalarValue::Float64(canonicalize_float(result)))
        }
        BuiltinScalarFunctionV1::GreatestFloat64 | BuiltinScalarFunctionV1::LeastFloat64 => {
            let mut best: Option<f64> = None;
            for arg in args {
                match arg {
                    RuntimeScalarValue::Float64(value) => {
                        if !value.is_finite() {
                            return Err(ExpressionEvaluationError::NonFiniteFloat);
                        }
                        best = Some(match best {
                            None => *value,
                            Some(current) => {
                                if function == BuiltinScalarFunctionV1::GreatestFloat64 {
                                    current.max(*value)
                                } else {
                                    current.min(*value)
                                }
                            }
                        });
                    }
                    RuntimeScalarValue::Null(_) => {}
                    other => {
                        return Err(ExpressionEvaluationError::Failed(format!(
                            "GREATEST/LEAST requires float64, got {:?}",
                            other.data_type()
                        )))
                    }
                }
            }
            match best {
                Some(value) => Ok(RuntimeScalarValue::Float64(value)),
                None => Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Float64)),
            }
        }
        BuiltinScalarFunctionV1::AgeDays => {
            let ts1 = match &args[0] {
                RuntimeScalarValue::Int64(value) => *value,
                RuntimeScalarValue::TimestampNanosecond(value) => *value,
                RuntimeScalarValue::Null(_) => {
                    return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64))
                }
                other => {
                    return Err(ExpressionEvaluationError::Failed(format!(
                        "AGE_DAYS requires int64 or timestamp_nanosecond, got {:?}",
                        other.data_type()
                    )))
                }
            };
            let ts2 = match &args[1] {
                RuntimeScalarValue::Int64(value) => *value,
                RuntimeScalarValue::TimestampNanosecond(value) => *value,
                RuntimeScalarValue::Null(_) => {
                    return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64))
                }
                other => {
                    return Err(ExpressionEvaluationError::Failed(format!(
                        "AGE_DAYS requires int64 or timestamp_nanosecond, got {:?}",
                        other.data_type()
                    )))
                }
            };
            let diff_ns = ts1.saturating_sub(ts2);
            let days = diff_ns / (86_400 * 1_000_000_000);
            Ok(RuntimeScalarValue::Int64(days))
        }
    }
}

fn float_operand(
    arg: &RuntimeScalarValue,
    function: BuiltinScalarFunctionV1,
) -> Result<f64, ExpressionEvaluationError> {
    match arg {
        RuntimeScalarValue::Float64(value) => Ok(*value),
        // Documented exact coercion for |value| < 2^53; the promotion matrix
        // admits Int64 in float arithmetic.
        RuntimeScalarValue::Int64(value) => Ok(*value as f64),
        RuntimeScalarValue::Null(_) => Err(ExpressionEvaluationError::Failed(format!(
            "{function:?} received NULL; null propagation must be handled by the caller"
        ))),
        other => Err(ExpressionEvaluationError::Failed(format!(
            "{function:?} requires float64, got {:?}",
            other.data_type()
        ))),
    }
}

fn canonicalize_float(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn unary_float(
    function: BuiltinScalarFunctionV1,
    arg: &RuntimeScalarValue,
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    let value = match arg {
        RuntimeScalarValue::Float64(value) => *value,
        RuntimeScalarValue::Null(_) => {
            return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Float64))
        }
        other => {
            return Err(ExpressionEvaluationError::Failed(format!(
                "{function:?} requires float64, got {:?}",
                other.data_type()
            )))
        }
    };
    if !value.is_finite() {
        return Err(ExpressionEvaluationError::NonFiniteFloat);
    }
    let result = match function {
        BuiltinScalarFunctionV1::AbsFloat64 => value.abs(),
        BuiltinScalarFunctionV1::CeilFloat64 => value.ceil(),
        BuiltinScalarFunctionV1::FloorFloat64 => value.floor(),
        _ => unreachable!("unary_float called for {function:?}"),
    };
    if !result.is_finite() {
        return Err(ExpressionEvaluationError::NonFiniteFloat);
    }
    Ok(RuntimeScalarValue::Float64(canonicalize_float(result)))
}

fn round_half_away(value: f64) -> f64 {
    if value >= 0.0 {
        (value + 0.5).floor()
    } else {
        (value - 0.5).ceil()
    }
}

/// UTC Gregorian extraction. Timestamps are nanoseconds since the Unix epoch.
fn extract_part(
    function: BuiltinScalarFunctionV1,
    arg: &RuntimeScalarValue,
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    let ts_ns = match arg {
        RuntimeScalarValue::TimestampNanosecond(ns) => *ns,
        RuntimeScalarValue::Null(_) => {
            return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64))
        }
        other => {
            return Err(ExpressionEvaluationError::Failed(format!(
                "{function:?} requires timestamp_nanosecond, got {:?}",
                other.data_type()
            )))
        }
    };
    let (year, month, day, hour, minute, second) = utc_gregorian(ts_ns)?;
    let value = match function {
        BuiltinScalarFunctionV1::ExtractYear => year,
        BuiltinScalarFunctionV1::ExtractMonth => month,
        BuiltinScalarFunctionV1::ExtractDay => day,
        BuiltinScalarFunctionV1::ExtractHour => hour,
        BuiltinScalarFunctionV1::ExtractMinute => minute,
        BuiltinScalarFunctionV1::ExtractSecond => second,
        _ => unreachable!(),
    };
    Ok(RuntimeScalarValue::Int64(value as i64))
}

/// UTC Gregorian date/time parts from nanoseconds since the Unix epoch using
/// the proleptic Gregorian calendar (civil-from-days algorithm).
fn utc_gregorian(ts_ns: i64) -> Result<(i64, i64, i64, i64, i64, i64), ExpressionEvaluationError> {
    let days = ts_ns.div_euclid(86_400_000_000_000);
    let day_remainder = ts_ns.rem_euclid(86_400_000_000_000);
    let hour = day_remainder / 3_600_000_000_000;
    let minute = (day_remainder % 3_600_000_000_000) / 60_000_000_000;
    let second = (day_remainder % 60_000_000_000) / 1_000_000_000;
    let (year, month, day) = civil_from_days(days)?;
    Ok((year, month, day, hour, minute, second))
}

/// Howard Hinnant's civil_from_days (proleptic Gregorian).
fn civil_from_days(days: i64) -> Result<(i64, i64, i64), ExpressionEvaluationError> {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    if !(1..=9999).contains(&year) {
        return Err(ExpressionEvaluationError::Failed(format!(
            "timestamp year {year} is outside the supported 1..=9999 range"
        )));
    }
    Ok((year, month, day))
}

fn date_trunc(
    function: BuiltinScalarFunctionV1,
    arg: &RuntimeScalarValue,
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    let ts_ns = match arg {
        RuntimeScalarValue::TimestampNanosecond(ns) => *ns,
        RuntimeScalarValue::Null(_) => {
            return Ok(RuntimeScalarValue::Null(
                RuntimeScalarTypeV1::TimestampNanosecond,
            ))
        }
        other => {
            return Err(ExpressionEvaluationError::Failed(format!(
                "{function:?} requires timestamp_nanosecond, got {:?}",
                other.data_type()
            )))
        }
    };
    let days = ts_ns.div_euclid(86_400_000_000_000);
    let day_remainder = ts_ns.rem_euclid(86_400_000_000_000);
    let truncated = match function {
        BuiltinScalarFunctionV1::DateTruncSecond => ts_ns - day_remainder.rem_euclid(1_000_000_000),
        BuiltinScalarFunctionV1::DateTruncMinute => {
            ts_ns - day_remainder.rem_euclid(60_000_000_000)
        }
        BuiltinScalarFunctionV1::DateTruncHour => {
            ts_ns - day_remainder.rem_euclid(3_600_000_000_000)
        }
        BuiltinScalarFunctionV1::DateTruncDay => days * 86_400_000_000_000,
        _ => unreachable!(),
    };
    Ok(RuntimeScalarValue::TimestampNanosecond(truncated))
}

fn timestamp_add_interval(
    function: BuiltinScalarFunctionV1,
    timestamp: &RuntimeScalarValue,
    interval_ns: &RuntimeScalarValue,
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    let ts = match timestamp {
        RuntimeScalarValue::TimestampNanosecond(ns) => *ns,
        RuntimeScalarValue::Null(_) => {
            return Ok(RuntimeScalarValue::Null(
                RuntimeScalarTypeV1::TimestampNanosecond,
            ))
        }
        other => {
            return Err(ExpressionEvaluationError::Failed(format!(
                "timestamp arithmetic requires timestamp_nanosecond, got {:?}",
                other.data_type()
            )))
        }
    };
    let interval = match interval_ns {
        RuntimeScalarValue::Int64(ns) => *ns,
        RuntimeScalarValue::Null(_) => {
            return Ok(RuntimeScalarValue::Null(
                RuntimeScalarTypeV1::TimestampNanosecond,
            ))
        }
        other => {
            return Err(ExpressionEvaluationError::Failed(format!(
                "timestamp arithmetic requires an int64 nanosecond interval, got {:?}",
                other.data_type()
            )))
        }
    };
    let result = match function {
        BuiltinScalarFunctionV1::TimestampAddNanoseconds => {
            ts.checked_add(interval).ok_or_else(|| {
                ExpressionEvaluationError::Failed("timestamp addition overflow".to_string())
            })?
        }
        BuiltinScalarFunctionV1::TimestampSubtractNanoseconds => {
            ts.checked_sub(interval).ok_or_else(|| {
                ExpressionEvaluationError::Failed("timestamp subtraction overflow".to_string())
            })?
        }
        _ => unreachable!(),
    };
    Ok(RuntimeScalarValue::TimestampNanosecond(result))
}

fn date_add_days(
    date: &RuntimeScalarValue,
    days: &RuntimeScalarValue,
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    let date = match date {
        RuntimeScalarValue::Date32(days_since_epoch) => *days_since_epoch,
        RuntimeScalarValue::Null(_) => {
            return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Date32))
        }
        other => {
            return Err(ExpressionEvaluationError::Failed(format!(
                "DATE + days requires date32, got {:?}",
                other.data_type()
            )))
        }
    };
    let days = match days {
        RuntimeScalarValue::Int64(days) => *days,
        RuntimeScalarValue::Null(_) => {
            return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Date32))
        }
        other => {
            return Err(ExpressionEvaluationError::Failed(format!(
                "DATE + days requires an int64 day count, got {:?}",
                other.data_type()
            )))
        }
    };
    let result =
        i32::try_from(i64::from(date).checked_add(days).ok_or_else(|| {
            ExpressionEvaluationError::Failed("date addition overflow".to_string())
        })?)
        .map_err(|_| ExpressionEvaluationError::Failed("date addition overflow".to_string()))?;
    Ok(RuntimeScalarValue::Date32(result))
}

/// Duration in nanoseconds for a fixed-duration interval literal.
pub fn interval_nanoseconds(millis: i64, seconds: i64, minutes: i64, hours: i64, days: i64) -> i64 {
    let _ = Duration::new(0, 0);
    (days * 86_400_000_000_000_i64)
        .checked_add(hours * 3_600_000_000_000)
        .and_then(|value| value.checked_add(minutes * 60_000_000_000))
        .and_then(|value| value.checked_add(seconds * 1_000_000_000))
        .and_then(|value| value.checked_add(millis * 1_000_000))
        .unwrap_or(i64::MAX)
}

/// Compiled-in UDF implementations (Phase 8.4). The identity must be
/// resolved against the registry before evaluation; an unknown identity or
/// a semantic version / digest mismatch fails closed.
pub fn evaluate_udf(
    identity: &velorix_core::view_plan::BuiltinUdfIdentityV1,
    args: &[RuntimeScalarValue],
) -> Result<RuntimeScalarValue, ExpressionEvaluationError> {
    let registered = velorix_core::view_plan::builtin_udf_identity_for_name(&identity.name)
        .ok_or_else(|| {
            ExpressionEvaluationError::Failed(format!(
                "builtin UDF `{}` is not in the compiled registry",
                identity.name
            ))
        })?;
    if registered != *identity {
        return Err(ExpressionEvaluationError::Failed(format!(
            "builtin UDF identity mismatch for `{}`: expected version {} digest {}",
            identity.name, registered.semantic_version, registered.implementation_digest
        )));
    }
    match identity.name.as_str() {
        "vx_strlen" => {
            let value = match &args[0] {
                RuntimeScalarValue::Utf8(value) => value,
                RuntimeScalarValue::Null(_) => {
                    return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64))
                }
                other => {
                    return Err(ExpressionEvaluationError::Failed(format!(
                        "vx_strlen requires utf8, got {:?}",
                        other.data_type()
                    )))
                }
            };
            Ok(RuntimeScalarValue::Int64(value.chars().count() as i64))
        }
        "vx_sign" => {
            let value = match &args[0] {
                RuntimeScalarValue::Int64(value) => *value,
                RuntimeScalarValue::Null(_) => {
                    return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64))
                }
                other => {
                    return Err(ExpressionEvaluationError::Failed(format!(
                        "vx_sign requires int64, got {:?}",
                        other.data_type()
                    )))
                }
            };
            Ok(RuntimeScalarValue::Int64(value.signum()))
        }
        "vx_clamp" => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                match arg {
                    RuntimeScalarValue::Int64(value) => values.push(*value),
                    RuntimeScalarValue::Null(_) => {
                        return Ok(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64))
                    }
                    other => {
                        return Err(ExpressionEvaluationError::Failed(format!(
                            "vx_clamp requires int64, got {:?}",
                            other.data_type()
                        )))
                    }
                }
            }
            if values[1] > values[2] {
                return Err(ExpressionEvaluationError::Failed(
                    "vx_clamp: lower bound exceeds upper bound".to_string(),
                ));
            }
            Ok(RuntimeScalarValue::Int64(
                values[0].clamp(values[1], values[2]),
            ))
        }
        other => Err(ExpressionEvaluationError::Failed(format!(
            "builtin UDF `{other}` has no compiled implementation"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velorix_core::view_plan::{
        BuiltinScalarFunctionV1, RuntimeScalarTypeV1, ScalarLiteralV1, TypedExprKindV1,
        TypedExprNodeV1, TypedExprProgramV1, TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
    };

    #[test]
    fn trim_with_a_nullable_character_control_propagates_null() {
        let program = TypedExprProgramV1 {
            encoding_version: TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
            root: TypedExprNodeV1 {
                result_type: RuntimeScalarTypeV1::Utf8,
                nullable: true,
                kind: TypedExprKindV1::Call {
                    function: BuiltinScalarFunctionV1::Trim,
                    args: vec![
                        TypedExprNodeV1 {
                            result_type: RuntimeScalarTypeV1::Utf8,
                            nullable: false,
                            kind: TypedExprKindV1::Literal {
                                value: ScalarLiteralV1::Utf8 {
                                    value: "xxvaluexx".to_string(),
                                },
                            },
                        },
                        TypedExprNodeV1 {
                            result_type: RuntimeScalarTypeV1::Utf8,
                            nullable: true,
                            kind: TypedExprKindV1::Literal {
                                value: ScalarLiteralV1::Null {
                                    data_type: RuntimeScalarTypeV1::Utf8,
                                },
                            },
                        },
                    ],
                },
            },
        };
        assert_eq!(
            evaluate_typed_expr(&program, &|_| None).unwrap(),
            RuntimeScalarValue::Null(RuntimeScalarTypeV1::Utf8)
        );
    }
}

//! Parallel typed expression IR (Phase 6).
//!
//! The legacy `SupportedProjectionExpr` stays frozen for persisted-plan
//! compatibility. New scalar expression families (string, temporal, float)
//! are admitted as a `TypedExprProgramV1`, which is type-checked at
//! admission, evaluated by the shared runtime evaluator, and versioned so
//! checkpoint payloads stay forward-compatible.

use crate::view_contract::stable_bytes_hash;
use serde::{Deserialize, Serialize};

pub const TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1: u16 = 1;

/// Canonical signed 128-bit integer with an explicit decimal-string JSON
/// encoding so persisted values are never at the mercy of JSON number
/// round-tripping.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CanonicalI128V1(i128);

impl CanonicalI128V1 {
    pub fn new(value: i128) -> Self {
        Self(value)
    }

    pub fn value(self) -> i128 {
        self.0
    }
}

impl From<i128> for CanonicalI128V1 {
    fn from(value: i128) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TypedExprProgramV1 {
    pub encoding_version: u16,
    pub root: TypedExprNodeV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypedExprNodeV1 {
    pub result_type: RuntimeScalarTypeV1,
    pub nullable: bool,
    pub kind: TypedExprKindV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TypedExprKindV1 {
    Column {
        column_id: String,
    },
    Literal {
        value: ScalarLiteralV1,
    },
    Call {
        function: BuiltinScalarFunctionV1,
        args: Vec<TypedExprNodeV1>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeScalarTypeV1 {
    Boolean,
    Int64,
    Float64,
    Decimal128 { precision: u8, scale: i8 },
    Utf8,
    Date32,
    TimestampNanosecond,
}

impl RuntimeScalarTypeV1 {
    pub fn canonical_tag(&self) -> &'static str {
        match self {
            RuntimeScalarTypeV1::Boolean => "boolean",
            RuntimeScalarTypeV1::Int64 => "int64",
            RuntimeScalarTypeV1::Float64 => "float64",
            RuntimeScalarTypeV1::Decimal128 { .. } => "decimal128",
            RuntimeScalarTypeV1::Utf8 => "utf8",
            RuntimeScalarTypeV1::Date32 => "date32",
            RuntimeScalarTypeV1::TimestampNanosecond => "timestamp_nanosecond",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ScalarLiteralV1 {
    Null {
        data_type: RuntimeScalarTypeV1,
    },
    Boolean(bool),
    Int64(i64),
    Float64 {
        canonical_bits: u64,
    },
    Decimal128 {
        unscaled: CanonicalI128V1,
        precision: u8,
        scale: i8,
    },
    Utf8 {
        value: String,
    },
    Date32(i32),
    TimestampNanosecond(i64),
}

/// Built-in deterministic scalar functions. New functions are appended only;
/// the variant order is part of the persisted program identity hash, so
/// reordering or renaming existing variants invalidates stored checkpoints.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinScalarFunctionV1 {
    /// String family: strict null propagation; LENGTH counts Unicode scalar
    /// values; no collation (binary comparison everywhere).
    Concat,
    Substring,
    Upper,
    Lower,
    Trim,
    Length,
    /// Temporal family (UTC Gregorian only).
    ExtractYear,
    ExtractMonth,
    ExtractDay,
    ExtractHour,
    ExtractMinute,
    ExtractSecond,
    DateTruncDay,
    DateTruncHour,
    DateTruncMinute,
    DateTruncSecond,
    /// Timestamp + fixed-duration interval (nanoseconds).
    TimestampAddNanoseconds,
    TimestampSubtractNanoseconds,
    /// Date + integer days.
    DateAddDays,
    /// Float family: finite-only arithmetic (NaN/Inf inputs and results
    /// fail closed; -0.0 results canonicalize to +0.0).
    AbsFloat64,
    CeilFloat64,
    FloorFloat64,
    RoundFloat64,
    GreatestFloat64,
    LeastFloat64,
    AddFloat64,
    SubtractFloat64,
    MultiplyFloat64,
    DivideFloat64,
}

impl TypedExprProgramV1 {
    pub fn validate(&self) -> Result<(), TypedExprError> {
        if self.encoding_version != TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1 {
            return Err(TypedExprError::UnsupportedVersion {
                version: self.encoding_version,
            });
        }
        validate_typed_expr_node(&self.root)
    }

    /// Canonical identity hash over the fully typed program. Included in
    /// plan hashes so expression semantics changes invalidate restore.
    pub fn program_hash(&self) -> Result<String, TypedExprError> {
        self.validate()?;
        let canonical = serde_json::to_vec(self).map_err(|_| {
            TypedExprError::Invalid("typed expression program is not serializable".to_string())
        })?;
        Ok(stable_bytes_hash(&canonical))
    }
}

pub fn validate_typed_expr_node(node: &TypedExprNodeV1) -> Result<(), TypedExprError> {
    match &node.kind {
        TypedExprKindV1::Column { column_id } => {
            if column_id.trim().is_empty() {
                return Err(TypedExprError::Invalid(
                    "typed expression column id must be non-empty".to_string(),
                ));
            }
        }
        TypedExprKindV1::Literal { value } => {
            validate_literal_type(value, node.result_type)?;
        }
        TypedExprKindV1::Call { function, args } => {
            validate_call_arity(*function, args)?;
            for arg in args {
                validate_typed_expr_node(arg)?;
            }
        }
    }
    Ok(())
}

fn validate_literal_type(
    value: &ScalarLiteralV1,
    declared: RuntimeScalarTypeV1,
) -> Result<(), TypedExprError> {
    let literal_type = match value {
        ScalarLiteralV1::Null { data_type } => *data_type,
        ScalarLiteralV1::Boolean(_) => RuntimeScalarTypeV1::Boolean,
        ScalarLiteralV1::Int64(_) => RuntimeScalarTypeV1::Int64,
        ScalarLiteralV1::Float64 { .. } => RuntimeScalarTypeV1::Float64,
        ScalarLiteralV1::Decimal128 {
            precision, scale, ..
        } => RuntimeScalarTypeV1::Decimal128 {
            precision: *precision,
            scale: *scale,
        },
        ScalarLiteralV1::Utf8 { .. } => RuntimeScalarTypeV1::Utf8,
        ScalarLiteralV1::Date32(_) => RuntimeScalarTypeV1::Date32,
        ScalarLiteralV1::TimestampNanosecond(_) => RuntimeScalarTypeV1::TimestampNanosecond,
    };
    if literal_type != declared {
        return Err(TypedExprError::Invalid(format!(
            "typed expression literal type {:?} does not match declared {:?}",
            literal_type, declared
        )));
    }
    if let ScalarLiteralV1::Decimal128 {
        precision, scale, ..
    } = value
    {
        if *precision == 0 || *precision > 38 || *scale > *precision as i8 {
            return Err(TypedExprError::Invalid(
                "decimal literal precision/scale out of range".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_call_arity(
    function: BuiltinScalarFunctionV1,
    args: &[TypedExprNodeV1],
) -> Result<(), TypedExprError> {
    let expected = match function {
        BuiltinScalarFunctionV1::Concat => 1..=8,
        BuiltinScalarFunctionV1::Substring | BuiltinScalarFunctionV1::Trim => 1..=3,
        BuiltinScalarFunctionV1::Upper | BuiltinScalarFunctionV1::Lower => 1..=1,
        BuiltinScalarFunctionV1::Length => 1..=1,
        BuiltinScalarFunctionV1::ExtractYear
        | BuiltinScalarFunctionV1::ExtractMonth
        | BuiltinScalarFunctionV1::ExtractDay
        | BuiltinScalarFunctionV1::ExtractHour
        | BuiltinScalarFunctionV1::ExtractMinute
        | BuiltinScalarFunctionV1::ExtractSecond
        | BuiltinScalarFunctionV1::DateTruncDay
        | BuiltinScalarFunctionV1::DateTruncHour
        | BuiltinScalarFunctionV1::DateTruncMinute
        | BuiltinScalarFunctionV1::DateTruncSecond => 1..=2,
        BuiltinScalarFunctionV1::TimestampAddNanoseconds
        | BuiltinScalarFunctionV1::TimestampSubtractNanoseconds => 2..=2,
        BuiltinScalarFunctionV1::DateAddDays => 2..=2,
        BuiltinScalarFunctionV1::AbsFloat64
        | BuiltinScalarFunctionV1::CeilFloat64
        | BuiltinScalarFunctionV1::FloorFloat64
        | BuiltinScalarFunctionV1::RoundFloat64 => 1..=1,
        BuiltinScalarFunctionV1::GreatestFloat64 | BuiltinScalarFunctionV1::LeastFloat64 => 2..=8,
        BuiltinScalarFunctionV1::AddFloat64
        | BuiltinScalarFunctionV1::SubtractFloat64
        | BuiltinScalarFunctionV1::MultiplyFloat64
        | BuiltinScalarFunctionV1::DivideFloat64 => 2..=2,
    };
    if !expected.contains(&args.len()) {
        return Err(TypedExprError::Invalid(format!(
            "function {function:?} expects {expected:?} arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TypedExprError {
    #[error("unsupported typed expression program version {version}")]
    UnsupportedVersion { version: u16 },
    #[error("invalid typed expression program: {0}")]
    Invalid(String),
}

use thiserror::Error;

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
    /// Phase 8.4: deterministic built-in UDF invocation. The identity
    /// (namespace/name/version/implementation digest) is resolved against
    /// the compiled registry at admission and restore; unknown or
    /// mismatched identities fail closed.
    UdfCall {
        identity: BuiltinUdfIdentityV1,
        args: Vec<TypedExprNodeV1>,
    },
}

/// Identity of a compiled-in deterministic scalar UDF. The
/// `implementation_digest` is computed over the function definition at
/// build time and is part of the program identity, so a behavior change
/// invalidates stored checkpoints instead of silently changing replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuiltinUdfIdentityV1 {
    pub namespace: String,
    pub name: String,
    pub semantic_version: u32,
    pub implementation_digest: String,
}

impl BuiltinUdfIdentityV1 {
    pub fn canonical_key(&self) -> String {
        format!(
            "{}/{}@v{}:{}",
            self.namespace, self.name, self.semantic_version, self.implementation_digest
        )
    }
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
    /// Temporal difference in days (Int64). Returns (ts1 - ts2) / 86_400_000_000_000
    /// where ts values are nanoseconds. Avoids month-length complexity.
    AgeDays,
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
            validate_call_argument_types(*function, args)?;
            let expected_result = builtin_call_result_type(*function);
            if node.result_type != expected_result {
                return Err(TypedExprError::Invalid(format!(
                    "function {function:?} result type mismatch: expected {expected_result:?}, got {:?}",
                    node.result_type
                )));
            }
            for arg in args {
                validate_typed_expr_node(arg)?;
            }
        }
        TypedExprKindV1::UdfCall { identity, args } => {
            let Some((arity, argument_types, result_type)) = builtin_udf_spec(identity) else {
                return Err(TypedExprError::Invalid(format!(
                    "unknown builtin UDF identity `{}`",
                    identity.canonical_key()
                )));
            };
            if args.len() != arity {
                return Err(TypedExprError::Invalid(format!(
                    "builtin UDF `{}` expects {arity} arguments, got {}",
                    identity.name,
                    args.len()
                )));
            }
            for (argument, expected) in args.iter().zip(argument_types.iter()) {
                if argument.result_type != *expected {
                    return Err(TypedExprError::Invalid(format!(
                        "builtin UDF `{}` argument type mismatch: expected {:?}, got {:?}",
                        identity.name, expected, argument.result_type
                    )));
                }
            }
            if node.result_type != result_type {
                return Err(TypedExprError::Invalid(format!(
                    "builtin UDF `{}` result type mismatch",
                    identity.name
                )));
            }
            for arg in args {
                validate_typed_expr_node(arg)?;
            }
        }
    }
    Ok(())
}

/// Compiled-in deterministic UDF registry (Phase 8.4). Each entry maps the
/// SQL name to a pinned identity whose implementation digest is computed
/// over the canonical definition below. New UDFs are appended only.
pub fn builtin_udf_identity_for_name(name: &str) -> Option<BuiltinUdfIdentityV1> {
    let name = name.to_ascii_lowercase();
    let (name, semantic_version, definition) = match name.as_str() {
        "vx_strlen" => (
            "vx_strlen",
            1,
            "fn vx_strlen(s: Utf8) -> Int64 { s.chars().count() as i64 }",
        ),
        "vx_sign" => ("vx_sign", 1, "fn vx_sign(v: Int64) -> Int64 { v.signum() }"),
        "vx_clamp" => (
            "vx_clamp",
            1,
            "fn vx_clamp(v: Int64, lo: Int64, hi: Int64) -> Int64 { v.clamp(lo, hi) }",
        ),
        _ => return None,
    };
    Some(BuiltinUdfIdentityV1 {
        namespace: "velorix".to_string(),
        name: name.to_string(),
        semantic_version,
        implementation_digest: stable_bytes_hash(definition.as_bytes()),
    })
}

/// Type specification for a compiled UDF identity: (arity, argument types,
/// result type). `None` means the identity is not in the compiled registry.
pub fn builtin_udf_spec(
    identity: &BuiltinUdfIdentityV1,
) -> Option<(usize, Vec<RuntimeScalarTypeV1>, RuntimeScalarTypeV1)> {
    let registered = builtin_udf_identity_for_name(&identity.name)?;
    if registered != *identity {
        return None;
    }
    match identity.name.as_str() {
        "vx_strlen" => Some((
            1,
            vec![RuntimeScalarTypeV1::Utf8],
            RuntimeScalarTypeV1::Int64,
        )),
        "vx_sign" => Some((
            1,
            vec![RuntimeScalarTypeV1::Int64],
            RuntimeScalarTypeV1::Int64,
        )),
        "vx_clamp" => Some((
            3,
            vec![
                RuntimeScalarTypeV1::Int64,
                RuntimeScalarTypeV1::Int64,
                RuntimeScalarTypeV1::Int64,
            ],
            RuntimeScalarTypeV1::Int64,
        )),
        _ => None,
    }
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

fn builtin_call_result_type(function: BuiltinScalarFunctionV1) -> RuntimeScalarTypeV1 {
    use RuntimeScalarTypeV1::*;
    match function {
        BuiltinScalarFunctionV1::Concat
        | BuiltinScalarFunctionV1::Substring
        | BuiltinScalarFunctionV1::Upper
        | BuiltinScalarFunctionV1::Lower
        | BuiltinScalarFunctionV1::Trim => Utf8,
        BuiltinScalarFunctionV1::Length => Int64,
        BuiltinScalarFunctionV1::ExtractYear
        | BuiltinScalarFunctionV1::ExtractMonth
        | BuiltinScalarFunctionV1::ExtractDay
        | BuiltinScalarFunctionV1::ExtractHour
        | BuiltinScalarFunctionV1::ExtractMinute
        | BuiltinScalarFunctionV1::ExtractSecond => Int64,
        BuiltinScalarFunctionV1::DateTruncDay
        | BuiltinScalarFunctionV1::DateTruncHour
        | BuiltinScalarFunctionV1::DateTruncMinute
        | BuiltinScalarFunctionV1::DateTruncSecond => TimestampNanosecond,
        BuiltinScalarFunctionV1::TimestampAddNanoseconds
        | BuiltinScalarFunctionV1::TimestampSubtractNanoseconds => TimestampNanosecond,
        BuiltinScalarFunctionV1::DateAddDays => Date32,
        BuiltinScalarFunctionV1::AbsFloat64
        | BuiltinScalarFunctionV1::CeilFloat64
        | BuiltinScalarFunctionV1::FloorFloat64
        | BuiltinScalarFunctionV1::RoundFloat64
        | BuiltinScalarFunctionV1::GreatestFloat64
        | BuiltinScalarFunctionV1::LeastFloat64
        | BuiltinScalarFunctionV1::AddFloat64
        | BuiltinScalarFunctionV1::SubtractFloat64
        | BuiltinScalarFunctionV1::MultiplyFloat64
        | BuiltinScalarFunctionV1::DivideFloat64 => Float64,
        BuiltinScalarFunctionV1::AgeDays => Int64,
    }
}

fn validate_call_argument_types(
    function: BuiltinScalarFunctionV1,
    args: &[TypedExprNodeV1],
) -> Result<(), TypedExprError> {
    use RuntimeScalarTypeV1::*;

    let allowed_at = |idx: usize| -> &[RuntimeScalarTypeV1] {
        match function {
            BuiltinScalarFunctionV1::Concat | BuiltinScalarFunctionV1::Trim => &[Utf8],
            BuiltinScalarFunctionV1::Substring => match idx {
                0 => &[Utf8],
                _ => &[Int64],
            },
            BuiltinScalarFunctionV1::Upper
            | BuiltinScalarFunctionV1::Lower
            | BuiltinScalarFunctionV1::Length => &[Utf8],
            BuiltinScalarFunctionV1::ExtractYear
            | BuiltinScalarFunctionV1::ExtractMonth
            | BuiltinScalarFunctionV1::ExtractDay
            | BuiltinScalarFunctionV1::ExtractHour
            | BuiltinScalarFunctionV1::ExtractMinute
            | BuiltinScalarFunctionV1::ExtractSecond
            | BuiltinScalarFunctionV1::DateTruncDay
            | BuiltinScalarFunctionV1::DateTruncHour
            | BuiltinScalarFunctionV1::DateTruncMinute
            | BuiltinScalarFunctionV1::DateTruncSecond => {
                if args.len() == 1 || idx == 1 {
                    &[TimestampNanosecond]
                } else {
                    &[Utf8]
                }
            }
            BuiltinScalarFunctionV1::TimestampAddNanoseconds
            | BuiltinScalarFunctionV1::TimestampSubtractNanoseconds => match idx {
                0 => &[TimestampNanosecond],
                _ => &[Int64],
            },
            BuiltinScalarFunctionV1::DateAddDays => match idx {
                0 => &[Date32],
                _ => &[Int64],
            },
            BuiltinScalarFunctionV1::AbsFloat64
            | BuiltinScalarFunctionV1::CeilFloat64
            | BuiltinScalarFunctionV1::FloorFloat64
            | BuiltinScalarFunctionV1::RoundFloat64
            | BuiltinScalarFunctionV1::GreatestFloat64
            | BuiltinScalarFunctionV1::LeastFloat64 => &[Float64],
            BuiltinScalarFunctionV1::AddFloat64
            | BuiltinScalarFunctionV1::SubtractFloat64
            | BuiltinScalarFunctionV1::MultiplyFloat64
            | BuiltinScalarFunctionV1::DivideFloat64 => &[Float64, Int64],
            BuiltinScalarFunctionV1::AgeDays => &[Int64, TimestampNanosecond],
        }
    };

    for (idx, arg) in args.iter().enumerate() {
        let allowed = allowed_at(idx);
        if !allowed.contains(&arg.result_type) {
            return Err(TypedExprError::Invalid(format!(
                "function {function:?} argument {idx} type mismatch: expected one of {allowed:?}, got {:?}",
                arg.result_type
            )));
        }
        // The runtime treats a NULL start/length differently from the
        // strict-null string-input contract, so do not admit an ambiguous
        // nullable control argument until that behavior is represented in
        // the persisted expression semantics.
        if function == BuiltinScalarFunctionV1::Substring && idx > 0 && arg.nullable {
            return Err(TypedExprError::Invalid(format!(
                "function {function:?} argument {idx} must be non-null"
            )));
        }
    }
    validate_temporal_unit_literal(function, args)?;
    Ok(())
}

fn validate_temporal_unit_literal(
    function: BuiltinScalarFunctionV1,
    args: &[TypedExprNodeV1],
) -> Result<(), TypedExprError> {
    let expected = match function {
        BuiltinScalarFunctionV1::ExtractYear => "year",
        BuiltinScalarFunctionV1::ExtractMonth => "month",
        BuiltinScalarFunctionV1::ExtractDay => "day",
        BuiltinScalarFunctionV1::ExtractHour => "hour",
        BuiltinScalarFunctionV1::ExtractMinute => "minute",
        BuiltinScalarFunctionV1::ExtractSecond => "second",
        BuiltinScalarFunctionV1::DateTruncDay => "day",
        BuiltinScalarFunctionV1::DateTruncHour => "hour",
        BuiltinScalarFunctionV1::DateTruncMinute => "minute",
        BuiltinScalarFunctionV1::DateTruncSecond => "second",
        _ => return Ok(()),
    };
    if args.len() == 1 {
        return Ok(());
    }
    let Some(TypedExprNodeV1 {
        kind:
            TypedExprKindV1::Literal {
                value: ScalarLiteralV1::Utf8 { value },
            },
        ..
    }) = args.first()
    else {
        return Err(TypedExprError::Invalid(format!(
            "function {function:?} requires its first two-argument field/unit to be the normalized `{expected}` literal"
        )));
    };
    if !value.eq_ignore_ascii_case(expected) {
        return Err(TypedExprError::Invalid(format!(
            "function {function:?} field/unit literal must name `{expected}`, got `{value}`"
        )));
    }
    Ok(())
}

fn validate_call_arity(
    function: BuiltinScalarFunctionV1,
    args: &[TypedExprNodeV1],
) -> Result<(), TypedExprError> {
    let expected = match function {
        BuiltinScalarFunctionV1::Concat => 1..=8,
        BuiltinScalarFunctionV1::Substring => 2..=3,
        BuiltinScalarFunctionV1::Trim => 1..=2,
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
        BuiltinScalarFunctionV1::AgeDays => 2..=2,
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

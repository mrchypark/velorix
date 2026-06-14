use feldera_ir::Dataflow;
use std::collections::BTreeMap;

use feldera_types::program_schema::{
    ColumnType, Field as FelderaField, IntervalUnit, ProgramSchema, Relation as FelderaRelation,
    SqlIdentifier, SqlType,
};
use thiserror::Error;

use crate::feldera_artifact::{
    RelationSchema, SqlDataType, SqlIntervalUnit, SqlStructField, StandingViewSpec,
};

#[derive(Clone, Debug)]
pub struct FelderaProgramDescriptor {
    pub program_schema: ProgramSchema,
    pub dataflow: Option<Dataflow>,
}

impl FelderaProgramDescriptor {
    pub fn new(program_schema: ProgramSchema) -> Self {
        Self {
            program_schema,
            dataflow: None,
        }
    }

    pub fn with_dataflow(mut self, dataflow: Dataflow) -> Self {
        self.dataflow = Some(dataflow);
        self
    }

    pub fn validate_standing_view_spec(
        &self,
        spec: &StandingViewSpec,
    ) -> Result<FelderaProgramDescriptorValidation, FelderaProgramDescriptorError> {
        let input_relations = self.validate_relations(
            "input",
            &spec.input_relations,
            &self.program_schema.inputs,
            false,
        )?;
        let output_relations = self.validate_relations(
            "output",
            &spec.output_relations,
            &self.program_schema.outputs,
            spec.shape.is_materialized,
        )?;

        Ok(FelderaProgramDescriptorValidation {
            input_relations,
            output_relations,
        })
    }

    fn validate_relations(
        &self,
        kind: &'static str,
        expected: &[RelationSchema],
        actual: &[FelderaRelation],
        require_materialized: bool,
    ) -> Result<Vec<String>, FelderaProgramDescriptorError> {
        let expected_names = expected
            .iter()
            .map(|relation| relation.relation_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut actual_names = std::collections::BTreeSet::new();
        for actual_relation in actual {
            let actual_name = actual_relation.name.name();
            if !actual_names.insert(actual_name.clone()) {
                return Err(FelderaProgramDescriptorError::DuplicateRelation {
                    kind,
                    relation: actual_name,
                });
            }
            if !actual_relation.properties.is_empty() {
                return Err(FelderaProgramDescriptorError::RelationHasProperties {
                    kind,
                    relation: actual_name,
                    properties: actual_relation.properties.keys().cloned().collect(),
                });
            }
            if !expected_names.contains(actual_name.as_str()) {
                return Err(FelderaProgramDescriptorError::UnexpectedRelation {
                    kind,
                    relation: actual_name,
                });
            }
        }

        expected
            .iter()
            .map(|expected_relation| {
                let actual_relation = actual
                    .iter()
                    .find(|relation| relation.name.name() == expected_relation.relation_name)
                    .ok_or_else(|| FelderaProgramDescriptorError::MissingRelation {
                        kind,
                        relation: expected_relation.relation_name.clone(),
                    })?;
                validate_relation_schema(expected_relation, actual_relation)?;
                if require_materialized && !actual_relation.materialized {
                    return Err(FelderaProgramDescriptorError::OutputNotMaterialized {
                        relation: expected_relation.relation_name.clone(),
                    });
                }
                Ok(expected_relation.relation_name.clone())
            })
            .collect()
    }
}

pub fn feldera_program_schema_for_standing_view_spec(
    spec: &StandingViewSpec,
) -> Result<ProgramSchema, FelderaProgramDescriptorError> {
    Ok(ProgramSchema {
        inputs: spec
            .input_relations
            .iter()
            .map(|relation| feldera_relation_from_relation_schema(relation, false))
            .collect::<Result<Vec<_>, _>>()?,
        outputs: spec
            .output_relations
            .iter()
            .map(|relation| {
                feldera_relation_from_relation_schema(relation, spec.shape.is_materialized)
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub fn feldera_relation_from_relation_schema(
    schema: &RelationSchema,
    materialized: bool,
) -> Result<FelderaRelation, FelderaProgramDescriptorError> {
    let fields = schema
        .columns
        .iter()
        .map(|column| {
            Ok(FelderaField::new(
                SqlIdentifier::from(column.name.as_str()),
                feldera_column_type(
                    &schema.relation_name,
                    &column.name,
                    &column.data_type,
                    column.nullable,
                )?,
            ))
        })
        .collect::<Result<Vec<_>, FelderaProgramDescriptorError>>()?;
    let primary_key = schema
        .primary_key
        .iter()
        .map(|name| SqlIdentifier::from(name.as_str()))
        .collect::<Vec<_>>();

    Ok(FelderaRelation::new(
        SqlIdentifier::from(schema.relation_name.as_str()),
        fields,
        materialized,
        BTreeMap::new(),
    )
    .with_primary_key(&primary_key))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FelderaProgramDescriptorValidation {
    pub input_relations: Vec<String>,
    pub output_relations: Vec<String>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum FelderaProgramDescriptorError {
    #[error("Feldera descriptor is missing {kind} relation `{relation}`")]
    MissingRelation {
        kind: &'static str,
        relation: String,
    },
    #[error("Feldera descriptor contains unexpected {kind} relation `{relation}`")]
    UnexpectedRelation {
        kind: &'static str,
        relation: String,
    },
    #[error("Feldera descriptor contains duplicate {kind} relation `{relation}`")]
    DuplicateRelation {
        kind: &'static str,
        relation: String,
    },
    #[error(
        "Feldera descriptor {kind} relation `{relation}` contains unmanaged properties: {properties:?}"
    )]
    RelationHasProperties {
        kind: &'static str,
        relation: String,
        properties: Vec<String>,
    },
    #[error("Feldera output relation `{relation}` is not materialized")]
    OutputNotMaterialized { relation: String },
    #[error(
        "Feldera relation `{relation}` column count mismatch: expected={expected}, actual={actual}"
    )]
    ColumnCountMismatch {
        relation: String,
        expected: usize,
        actual: usize,
    },
    #[error(
        "Feldera relation `{relation}` column name mismatch at ordinal {ordinal}: expected=`{expected}`, actual=`{actual}`"
    )]
    ColumnNameMismatch {
        relation: String,
        ordinal: usize,
        expected: String,
        actual: String,
    },
    #[error(
        "Feldera relation `{relation}` column `{column}` type mismatch: expected={expected}, actual={actual}"
    )]
    ColumnTypeMismatch {
        relation: String,
        column: String,
        expected: String,
        actual: String,
    },
    #[error(
        "Feldera relation `{relation}` column `{column}` uses unsupported SQL type: actual={actual}"
    )]
    UnsupportedColumnType {
        relation: String,
        column: String,
        actual: String,
    },
    #[error(
        "Velorix relation `{relation}` column `{column}` cannot be mapped to Feldera descriptor type: data_type={data_type}"
    )]
    UnsupportedVelorixColumnType {
        relation: String,
        column: String,
        data_type: String,
    },
    #[error(
        "Feldera relation `{relation}` column `{column}` nullability mismatch: expected={expected}, actual={actual}"
    )]
    ColumnNullabilityMismatch {
        relation: String,
        column: String,
        expected: bool,
        actual: bool,
    },
    #[error(
        "Feldera relation `{relation}` primary key mismatch: expected={expected:?}, actual={actual:?}"
    )]
    PrimaryKeyMismatch {
        relation: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },
}

fn validate_relation_schema(
    expected: &RelationSchema,
    actual: &FelderaRelation,
) -> Result<(), FelderaProgramDescriptorError> {
    if expected.columns.len() != actual.fields.len() {
        return Err(FelderaProgramDescriptorError::ColumnCountMismatch {
            relation: expected.relation_name.clone(),
            expected: expected.columns.len(),
            actual: actual.fields.len(),
        });
    }

    for (ordinal, (expected_column, actual_field)) in expected
        .columns
        .iter()
        .zip(actual.fields.iter())
        .enumerate()
    {
        let actual_name = actual_field.name.name();
        if expected_column.name != actual_name {
            return Err(FelderaProgramDescriptorError::ColumnNameMismatch {
                relation: expected.relation_name.clone(),
                ordinal,
                expected: expected_column.name.clone(),
                actual: actual_name,
            });
        }
        let expected_type = velorix_type_signature(&expected_column.data_type);
        let actual_type = feldera_type_signature(&actual_field.columntype).map_err(|actual| {
            FelderaProgramDescriptorError::UnsupportedColumnType {
                relation: expected.relation_name.clone(),
                column: expected_column.name.clone(),
                actual,
            }
        })?;
        if expected_type != actual_type {
            return Err(FelderaProgramDescriptorError::ColumnTypeMismatch {
                relation: expected.relation_name.clone(),
                column: expected_column.name.clone(),
                expected: expected_type,
                actual: actual_type,
            });
        }
        if expected_column.nullable != actual_field.columntype.nullable {
            return Err(FelderaProgramDescriptorError::ColumnNullabilityMismatch {
                relation: expected.relation_name.clone(),
                column: expected_column.name.clone(),
                expected: expected_column.nullable,
                actual: actual_field.columntype.nullable,
            });
        }
    }

    let actual_primary_key = actual.primary_key.clone().unwrap_or_default();
    if expected.primary_key != actual_primary_key {
        return Err(FelderaProgramDescriptorError::PrimaryKeyMismatch {
            relation: expected.relation_name.clone(),
            expected: expected.primary_key.clone(),
            actual: actual_primary_key,
        });
    }

    Ok(())
}

fn velorix_type_signature(data_type: &SqlDataType) -> String {
    match data_type {
        SqlDataType::Bool => "Bool".to_string(),
        SqlDataType::Int8 => "Int8".to_string(),
        SqlDataType::Int16 => "Int16".to_string(),
        SqlDataType::Int32 => "Int32".to_string(),
        SqlDataType::Int64 => "Int64".to_string(),
        SqlDataType::UInt8 => "UInt8".to_string(),
        SqlDataType::UInt16 => "UInt16".to_string(),
        SqlDataType::UInt32 => "UInt32".to_string(),
        SqlDataType::UInt64 => "UInt64".to_string(),
        SqlDataType::Float32 => "Float32".to_string(),
        SqlDataType::Float64 => "Float64".to_string(),
        SqlDataType::Decimal { precision, scale } => format!("Decimal({precision},{scale})"),
        SqlDataType::Char { length } => length
            .map(|length| format!("Char({length})"))
            .unwrap_or_else(|| "Char".to_string()),
        SqlDataType::Utf8 => "Varchar".to_string(),
        SqlDataType::Binary { length } => format!("Binary({length})"),
        SqlDataType::Varbinary => "Varbinary".to_string(),
        SqlDataType::Time => "Time".to_string(),
        SqlDataType::Date => "Date".to_string(),
        SqlDataType::Timestamp { timezone } => match timezone {
            Some(timezone) => format!("Timestamp(timezone={timezone})"),
            None => "Timestamp".to_string(),
        },
        SqlDataType::Interval { unit } => format!("Interval({unit:?})"),
        SqlDataType::Array { element_type } => {
            format!("Array<{}>", velorix_type_signature(element_type))
        }
        SqlDataType::Struct { fields } => format!(
            "Struct({})",
            fields
                .iter()
                .map(|field| format!(
                    "{}:{}{}",
                    field.name,
                    velorix_type_signature(&field.data_type),
                    if field.nullable { "?" } else { "" }
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        SqlDataType::Map {
            key_type,
            value_type,
        } => format!(
            "Map<{},{}>",
            velorix_type_signature(key_type),
            velorix_type_signature(value_type)
        ),
        SqlDataType::Null => "Null".to_string(),
        SqlDataType::Uuid => "Uuid".to_string(),
        SqlDataType::Json => "Variant".to_string(),
        SqlDataType::Geometry => "Geometry".to_string(),
    }
}

fn feldera_column_type(
    relation: &str,
    column: &str,
    data_type: &SqlDataType,
    nullable: bool,
) -> Result<ColumnType, FelderaProgramDescriptorError> {
    Ok(match data_type {
        SqlDataType::Bool => ColumnType::boolean(nullable),
        SqlDataType::Int8 => ColumnType::tinyint(nullable),
        SqlDataType::Int16 => ColumnType::smallint(nullable),
        SqlDataType::Int32 => ColumnType::int(nullable),
        SqlDataType::Int64 => ColumnType::bigint(nullable),
        SqlDataType::UInt8 => ColumnType::utinyint(nullable),
        SqlDataType::UInt16 => ColumnType::usmallint(nullable),
        SqlDataType::UInt32 => ColumnType::uint(nullable),
        SqlDataType::UInt64 => ColumnType::ubigint(nullable),
        SqlDataType::Float32 => ColumnType::real(nullable),
        SqlDataType::Float64 => ColumnType::double(nullable),
        SqlDataType::Decimal { precision, scale } => {
            ColumnType::decimal(i64::from(*precision), i64::from(*scale), nullable)
        }
        SqlDataType::Char { length } => ColumnType {
            typ: SqlType::Char,
            nullable,
            precision: length.map(i64::from),
            scale: None,
            component: None,
            fields: None,
            key: None,
            value: None,
        },
        SqlDataType::Utf8 => ColumnType::varchar(nullable),
        SqlDataType::Binary { length } => ColumnType::fixed(i64::from(*length), nullable),
        SqlDataType::Varbinary => ColumnType::varbinary(nullable),
        SqlDataType::Time => ColumnType::time(nullable),
        SqlDataType::Date => ColumnType::date(nullable),
        SqlDataType::Timestamp { timezone: None } => ColumnType::timestamp(nullable),
        SqlDataType::Timestamp {
            timezone: Some(timezone),
        } => {
            return Err(
                FelderaProgramDescriptorError::UnsupportedVelorixColumnType {
                    relation: relation.to_string(),
                    column: column.to_string(),
                    data_type: format!("Timestamp(timezone={timezone})"),
                },
            )
        }
        SqlDataType::Interval { unit } => ColumnType {
            typ: SqlType::Interval(feldera_interval_unit(*unit)),
            nullable,
            precision: None,
            scale: None,
            component: None,
            fields: None,
            key: None,
            value: None,
        },
        SqlDataType::Array { element_type } => ColumnType::array(
            nullable,
            feldera_column_type(relation, column, element_type, false)?,
        ),
        SqlDataType::Struct { fields } => ColumnType::structure(
            nullable,
            &fields
                .iter()
                .map(|field| feldera_field_from_struct_field(relation, column, field))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SqlDataType::Map {
            key_type,
            value_type,
        } => ColumnType::map(
            nullable,
            feldera_column_type(relation, column, key_type, false)?,
            feldera_column_type(relation, column, value_type, false)?,
        ),
        SqlDataType::Null => ColumnType {
            typ: SqlType::Null,
            nullable,
            precision: None,
            scale: None,
            component: None,
            fields: None,
            key: None,
            value: None,
        },
        SqlDataType::Uuid => ColumnType::uuid(nullable),
        SqlDataType::Json => ColumnType::variant(nullable),
        SqlDataType::Geometry => {
            return Err(
                FelderaProgramDescriptorError::UnsupportedVelorixColumnType {
                    relation: relation.to_string(),
                    column: column.to_string(),
                    data_type: "Geometry".to_string(),
                },
            )
        }
    })
}

fn feldera_type_signature(column_type: &ColumnType) -> Result<String, String> {
    match column_type.typ {
        SqlType::Boolean => Ok("Bool".to_string()),
        SqlType::TinyInt => Ok("Int8".to_string()),
        SqlType::SmallInt => Ok("Int16".to_string()),
        SqlType::Int => Ok("Int32".to_string()),
        SqlType::BigInt => Ok("Int64".to_string()),
        SqlType::UTinyInt => Ok("UInt8".to_string()),
        SqlType::USmallInt => Ok("UInt16".to_string()),
        SqlType::UInt => Ok("UInt32".to_string()),
        SqlType::UBigInt => Ok("UInt64".to_string()),
        SqlType::Real => Ok("Float32".to_string()),
        SqlType::Double => Ok("Float64".to_string()),
        SqlType::Decimal => Ok(format!(
            "Decimal({},{})",
            column_type
                .precision
                .map(|value| value.to_string())
                .unwrap_or_else(|| "?".to_string()),
            column_type
                .scale
                .map(|value| value.to_string())
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Char => Ok(column_type
            .precision
            .map(|length| format!("Char({length})"))
            .unwrap_or_else(|| "Char".to_string())),
        SqlType::Varchar => Ok("Varchar".to_string()),
        SqlType::Binary => Ok(format!(
            "Binary({})",
            column_type
                .precision
                .map(|value| value.to_string())
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Varbinary => Ok("Varbinary".to_string()),
        SqlType::Time => Ok("Time".to_string()),
        SqlType::Date => Ok("Date".to_string()),
        SqlType::Timestamp => Ok("Timestamp".to_string()),
        SqlType::Interval(unit) => Ok(format!("Interval({unit:?})")),
        SqlType::Array => Ok(format!(
            "Array<{}>",
            column_type
                .component
                .as_deref()
                .map(feldera_type_label)
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Struct => Ok(format!(
            "Struct({})",
            column_type
                .fields
                .as_ref()
                .map(|fields| {
                    fields
                        .iter()
                        .map(|field| {
                            format!(
                                "{}:{}{}",
                                field.name.name(),
                                feldera_type_label(&field.columntype),
                                if field.columntype.nullable { "?" } else { "" }
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Map => Ok(format!(
            "Map<{},{}>",
            column_type
                .key
                .as_deref()
                .map(feldera_type_label)
                .unwrap_or_else(|| "?".to_string()),
            column_type
                .value
                .as_deref()
                .map(feldera_type_label)
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Null => Ok("Null".to_string()),
        SqlType::Uuid => Ok("Uuid".to_string()),
        SqlType::Variant => Ok("Variant".to_string()),
    }
}

fn feldera_type_label(column_type: &ColumnType) -> String {
    feldera_type_signature(column_type).unwrap_or_else(|label| label)
}

fn feldera_field_from_struct_field(
    relation: &str,
    column: &str,
    field: &SqlStructField,
) -> Result<FelderaField, FelderaProgramDescriptorError> {
    Ok(FelderaField::new(
        SqlIdentifier::from(field.name.as_str()),
        feldera_column_type(relation, column, &field.data_type, field.nullable)?,
    ))
}

fn feldera_interval_unit(unit: SqlIntervalUnit) -> IntervalUnit {
    match unit {
        SqlIntervalUnit::Day => IntervalUnit::Day,
        SqlIntervalUnit::DayToHour => IntervalUnit::DayToHour,
        SqlIntervalUnit::DayToMinute => IntervalUnit::DayToMinute,
        SqlIntervalUnit::DayToSecond => IntervalUnit::DayToSecond,
        SqlIntervalUnit::Hour => IntervalUnit::Hour,
        SqlIntervalUnit::HourToMinute => IntervalUnit::HourToMinute,
        SqlIntervalUnit::HourToSecond => IntervalUnit::HourToSecond,
        SqlIntervalUnit::Minute => IntervalUnit::Minute,
        SqlIntervalUnit::MinuteToSecond => IntervalUnit::MinuteToSecond,
        SqlIntervalUnit::Month => IntervalUnit::Month,
        SqlIntervalUnit::Second => IntervalUnit::Second,
        SqlIntervalUnit::Year => IntervalUnit::Year,
        SqlIntervalUnit::YearToMonth => IntervalUnit::YearToMonth,
    }
}

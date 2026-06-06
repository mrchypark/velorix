use feldera_ir::Dataflow;
use std::collections::BTreeMap;

use feldera_types::program_schema::{
    ColumnType, Field as FelderaField, ProgramSchema, Relation as FelderaRelation, SqlIdentifier,
    SqlType,
};
use thiserror::Error;

use crate::feldera_artifact::{RelationSchema, SqlDataType, StandingViewSpec};

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
        SqlDataType::Int64 => "Int64".to_string(),
        SqlDataType::Float64 => "Float64".to_string(),
        SqlDataType::Decimal { precision, scale } => format!("Decimal({precision},{scale})"),
        SqlDataType::Utf8 => "Varchar".to_string(),
        SqlDataType::Date => "Date".to_string(),
        SqlDataType::Timestamp { timezone } => match timezone {
            Some(timezone) => format!("Timestamp(timezone={timezone})"),
            None => "Timestamp".to_string(),
        },
        SqlDataType::Json => "Variant".to_string(),
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
        SqlDataType::Int64 => ColumnType::bigint(nullable),
        SqlDataType::Float64 => ColumnType::double(nullable),
        SqlDataType::Decimal { precision, scale } => {
            ColumnType::decimal(i64::from(*precision), i64::from(*scale), nullable)
        }
        SqlDataType::Utf8 => ColumnType::varchar(nullable),
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
        SqlDataType::Json => ColumnType::variant(nullable),
    })
}

fn feldera_type_signature(column_type: &ColumnType) -> Result<String, String> {
    match column_type.typ {
        SqlType::Boolean => Ok("Bool".to_string()),
        SqlType::BigInt => Ok("Int64".to_string()),
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
        SqlType::Varchar | SqlType::Char => Ok("Varchar".to_string()),
        SqlType::Date => Ok("Date".to_string()),
        SqlType::Timestamp => Ok("Timestamp".to_string()),
        SqlType::Variant => Ok("Variant".to_string()),
        SqlType::TinyInt => Err("Int8".to_string()),
        SqlType::SmallInt => Err("Int16".to_string()),
        SqlType::Int => Err("Int32".to_string()),
        SqlType::UTinyInt => Err("UInt8".to_string()),
        SqlType::USmallInt => Err("UInt16".to_string()),
        SqlType::UInt => Err("UInt32".to_string()),
        SqlType::UBigInt => Err("UInt64".to_string()),
        SqlType::Real => Err("Float32".to_string()),
        SqlType::Binary => Err(format!(
            "Binary({})",
            column_type
                .precision
                .map(|value| value.to_string())
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Varbinary => Err("Varbinary".to_string()),
        SqlType::Time => Err("Time".to_string()),
        SqlType::Interval(unit) => Err(format!("Interval({unit:?})")),
        SqlType::Array => Err(format!(
            "Array<{}>",
            column_type
                .component
                .as_deref()
                .map(feldera_type_label)
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Struct => Err(format!(
            "Struct({})",
            column_type
                .fields
                .as_ref()
                .map(|fields| fields.len().to_string())
                .unwrap_or_else(|| "?".to_string())
        )),
        SqlType::Map => Err(format!(
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
        SqlType::Null => Err("Null".to_string()),
        SqlType::Uuid => Err("Uuid".to_string()),
    }
}

fn feldera_type_label(column_type: &ColumnType) -> String {
    feldera_type_signature(column_type).unwrap_or_else(|label| label)
}

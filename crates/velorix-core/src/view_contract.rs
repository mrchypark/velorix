use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::relation::{
    RelationColumnV1, RelationSchemaError, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
};

pub const SPEC_HASH_PREFIX: &str = "velorix-view-spec-sha256-v1";
pub const MAX_RELATION_COLUMNS: usize = 1024;
pub const MAX_SQL_TYPE_NESTING_DEPTH: usize = 16;
pub const MAX_SQL_TYPE_NODES: usize = 4096;
pub const MAX_SQL_STRUCT_FIELDS: usize = 256;
pub const MAX_SQL_STRUCT_FIELD_NAME_BYTES: usize = 128;
pub const MAX_SQL_TIMEZONE_BYTES: usize = 128;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingViewSpec {
    pub view_id: String,
    pub sql: String,
    pub dialect: SqlDialect,
    pub source_kind: SqlSourceKind,
    pub input_relations: Vec<RelationSchema>,
    pub output_relations: Vec<RelationSchema>,
    pub shape: StandingViewShape,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum SqlDialect {
    VelorixSql,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum SqlSourceKind {
    StandingView,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingViewShape {
    pub is_materialized: bool,
    pub multi_input: bool,
    pub multi_output: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationSchema {
    pub relation_id: String,
    pub relation_name: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SqlDataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Char {
        length: Option<u32>,
    },
    Utf8,
    Binary {
        length: u32,
    },
    Varbinary,
    Time,
    Date,
    Timestamp {
        timezone: Option<String>,
    },
    Interval {
        unit: SqlIntervalUnit,
    },
    Array {
        element_type: Box<SqlDataType>,
    },
    Struct {
        fields: Vec<SqlStructField>,
    },
    Map {
        key_type: Box<SqlDataType>,
        value_type: Box<SqlDataType>,
    },
    Null,
    Uuid,
    Json,
    Geometry,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum SqlIntervalUnit {
    Day,
    DayToHour,
    DayToMinute,
    DayToSecond,
    Hour,
    HourToMinute,
    HourToSecond,
    Minute,
    MinuteToSecond,
    Month,
    Second,
    Year,
    YearToMonth,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlStructField {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ViewContractError {
    #[error("missing view contract field: {field}")]
    MissingField { field: &'static str },
    #[error("invalid view contract field: {field}")]
    InvalidField { field: &'static str },
    #[error("relation schema mismatch: {field}")]
    RelationSchemaMismatch { field: &'static str },
    #[error("could not serialize canonical view contract: {reason}")]
    Serialization { reason: String },
}

pub fn validate_materialized_standing_view_spec(
    spec: &StandingViewSpec,
) -> Result<(), ViewContractError> {
    require_non_empty("view_id", &spec.view_id)?;
    require_non_empty("sql", &spec.sql)?;
    if !spec.shape.is_materialized {
        return Err(ViewContractError::InvalidField {
            field: "shape.is_materialized",
        });
    }
    if spec.input_relations.is_empty() {
        return Err(ViewContractError::InvalidField {
            field: "input_relations",
        });
    }
    if spec.output_relations.is_empty() {
        return Err(ViewContractError::InvalidField {
            field: "output_relations",
        });
    }
    validate_relation_schemas(&spec.input_relations)?;
    validate_relation_schemas(&spec.output_relations)?;
    for relation in spec
        .input_relations
        .iter()
        .chain(spec.output_relations.iter())
    {
        validate_relation_schema(relation)?;
    }
    Ok(())
}

pub fn catalog_input_relation_schema(
    catalog: &VelorixRelationCatalogV1,
) -> Result<RelationSchema, ViewContractError> {
    catalog.validate().map_err(catalog_relation_error)?;
    Ok(RelationSchema {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_name: catalog.relation_schema.relation_name.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        columns: catalog
            .relation_schema
            .columns
            .iter()
            .map(catalog_column_schema)
            .collect::<Result<Vec<_>, _>>()?,
        primary_key: catalog_primary_key_columns(catalog)?,
    })
}

pub fn view_spec_hash(spec: &StandingViewSpec) -> Result<String, ViewContractError> {
    validate_materialized_standing_view_spec(spec)?;
    let canonical_json =
        serde_json::to_vec(spec).map_err(|source| ViewContractError::Serialization {
            reason: source.to_string(),
        })?;
    let content_hash = stable_bytes_hash(&canonical_json);
    let hex =
        content_hash
            .strip_prefix("sha256:")
            .ok_or_else(|| ViewContractError::Serialization {
                reason: format!("unexpected view spec content hash format `{content_hash}`"),
            })?;
    Ok(format!("{SPEC_HASH_PREFIX}:{hex}"))
}

pub fn stable_bytes_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn validate_relation_schemas(schemas: &[RelationSchema]) -> Result<(), ViewContractError> {
    if schemas.len() > MAX_RELATION_COLUMNS {
        return Err(ViewContractError::InvalidField {
            field: "relation_count",
        });
    }
    let mut relation_ids = BTreeSet::new();
    for relation in schemas {
        if !relation_ids.insert(relation.relation_id.as_str()) {
            return Err(ViewContractError::InvalidField {
                field: "relation_id",
            });
        }
    }
    Ok(())
}

fn validate_relation_schema(schema: &RelationSchema) -> Result<(), ViewContractError> {
    require_non_empty("relation_id", &schema.relation_id)?;
    require_non_empty("relation_name", &schema.relation_name)?;
    require_non_empty("relation_version", &schema.relation_version)?;
    validate_schema_fingerprint(&schema.schema_fingerprint)?;
    if schema.columns.is_empty() || schema.columns.len() > MAX_RELATION_COLUMNS {
        return Err(ViewContractError::InvalidField { field: "columns" });
    }
    if schema.primary_key.is_empty() {
        return Err(ViewContractError::InvalidField {
            field: "primary_key",
        });
    }
    let mut column_names = BTreeSet::new();
    for column in &schema.columns {
        require_non_empty("column.name", &column.name)?;
        validate_sql_data_type(&column.data_type)?;
        if !column_names.insert(column.name.as_str()) {
            return Err(ViewContractError::InvalidField {
                field: "column.name",
            });
        }
    }
    for key in &schema.primary_key {
        require_non_empty("primary_key", key)?;
        if !column_names.contains(key.as_str()) {
            return Err(ViewContractError::InvalidField {
                field: "primary_key",
            });
        }
    }
    Ok(())
}

fn catalog_column_schema(column: &RelationColumnV1) -> Result<ColumnSchema, ViewContractError> {
    Ok(ColumnSchema {
        name: column.name.clone(),
        data_type: sql_data_type_for_logical_type(&column.logical_type)?,
        nullable: column.nullable,
    })
}

fn catalog_primary_key_columns(
    catalog: &VelorixRelationCatalogV1,
) -> Result<Vec<String>, ViewContractError> {
    let by_id = catalog
        .relation_schema
        .columns
        .iter()
        .map(|column| (column.column_id.as_str(), column))
        .collect::<std::collections::BTreeMap<_, _>>();
    catalog
        .relation_schema
        .primary_key_column_ids
        .iter()
        .map(|column_id| {
            by_id
                .get(column_id.as_str())
                .map(|column| column.name.clone())
                .ok_or(ViewContractError::InvalidField {
                    field: "primary_key_column_ids",
                })
        })
        .collect()
}

fn sql_data_type_for_logical_type(
    logical_type: &VelorixLogicalTypeV1,
) -> Result<SqlDataType, ViewContractError> {
    match logical_type {
        VelorixLogicalTypeV1::Bool => Ok(SqlDataType::Bool),
        VelorixLogicalTypeV1::Int8 => Ok(SqlDataType::Int8),
        VelorixLogicalTypeV1::Int16 => Ok(SqlDataType::Int16),
        VelorixLogicalTypeV1::Int32 => Ok(SqlDataType::Int32),
        VelorixLogicalTypeV1::Int64 => Ok(SqlDataType::Int64),
        VelorixLogicalTypeV1::UInt8 => Ok(SqlDataType::UInt8),
        VelorixLogicalTypeV1::UInt16 => Ok(SqlDataType::UInt16),
        VelorixLogicalTypeV1::UInt32 => Ok(SqlDataType::UInt32),
        VelorixLogicalTypeV1::UInt64 => Ok(SqlDataType::UInt64),
        VelorixLogicalTypeV1::Float32 => Ok(SqlDataType::Float32),
        VelorixLogicalTypeV1::Float64 => Ok(SqlDataType::Float64),
        VelorixLogicalTypeV1::Decimal { precision, scale } => Ok(SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        VelorixLogicalTypeV1::Char { length } => Ok(SqlDataType::Char { length: *length }),
        VelorixLogicalTypeV1::Utf8 => Ok(SqlDataType::Utf8),
        VelorixLogicalTypeV1::Binary { length } => Ok(SqlDataType::Binary { length: *length }),
        VelorixLogicalTypeV1::Varbinary => Ok(SqlDataType::Varbinary),
        VelorixLogicalTypeV1::Date => Ok(SqlDataType::Date),
        VelorixLogicalTypeV1::Time => Ok(SqlDataType::Time),
        VelorixLogicalTypeV1::Timestamp { timezone } => Ok(SqlDataType::Timestamp {
            timezone: timezone.clone(),
        }),
        VelorixLogicalTypeV1::Uuid => Ok(SqlDataType::Uuid),
        VelorixLogicalTypeV1::Json => Ok(SqlDataType::Json),
        VelorixLogicalTypeV1::Array { element_type } => Ok(SqlDataType::Array {
            element_type: Box::new(sql_data_type_for_logical_type(element_type)?),
        }),
        VelorixLogicalTypeV1::Struct { fields } => Ok(SqlDataType::Struct {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(SqlStructField {
                        name: field.name.clone(),
                        data_type: sql_data_type_for_logical_type(&field.logical_type)?,
                        nullable: field.nullable,
                    })
                })
                .collect::<Result<Vec<_>, ViewContractError>>()?,
        }),
        VelorixLogicalTypeV1::Map {
            key_type,
            value_type,
        } => Ok(SqlDataType::Map {
            key_type: Box::new(sql_data_type_for_logical_type(key_type)?),
            value_type: Box::new(sql_data_type_for_logical_type(value_type)?),
        }),
    }
}

fn validate_sql_data_type(data_type: &SqlDataType) -> Result<(), ViewContractError> {
    let mut node_count = 0;
    validate_sql_data_type_with_limits(data_type, 0, &mut node_count)
}

fn validate_sql_data_type_with_limits(
    data_type: &SqlDataType,
    depth: usize,
    node_count: &mut usize,
) -> Result<(), ViewContractError> {
    if depth > MAX_SQL_TYPE_NESTING_DEPTH {
        return Err(ViewContractError::InvalidField {
            field: "data_type.depth",
        });
    }
    *node_count += 1;
    if *node_count > MAX_SQL_TYPE_NODES {
        return Err(ViewContractError::InvalidField {
            field: "data_type.nodes",
        });
    }
    match data_type {
        SqlDataType::Decimal { precision, scale } => {
            if *precision == 0 || *precision > 38 || *scale > *precision {
                return Err(ViewContractError::InvalidField { field: "decimal" });
            }
        }
        SqlDataType::Char { length: Some(0) } => {
            return Err(ViewContractError::InvalidField {
                field: "char.length",
            });
        }
        SqlDataType::Binary { length: 0 } => {
            return Err(ViewContractError::InvalidField {
                field: "binary.length",
            });
        }
        SqlDataType::Timestamp {
            timezone: Some(timezone),
        } if timezone.trim().is_empty() || timezone.len() > MAX_SQL_TIMEZONE_BYTES => {
            return Err(ViewContractError::InvalidField {
                field: "timestamp.timezone",
            });
        }
        SqlDataType::Array { element_type } => {
            validate_sql_data_type_with_limits(element_type, depth + 1, node_count)?
        }
        SqlDataType::Struct { fields } => {
            if fields.len() > MAX_SQL_STRUCT_FIELDS {
                return Err(ViewContractError::InvalidField {
                    field: "struct.fields",
                });
            }
            let mut names = BTreeSet::new();
            for field in fields {
                if field.name.trim().is_empty()
                    || field.name.len() > MAX_SQL_STRUCT_FIELD_NAME_BYTES
                    || !names.insert(field.name.as_str())
                {
                    return Err(ViewContractError::InvalidField {
                        field: "struct.field.name",
                    });
                }
                validate_sql_data_type_with_limits(&field.data_type, depth + 1, node_count)?;
            }
        }
        SqlDataType::Map {
            key_type,
            value_type,
        } => {
            validate_sql_data_type_with_limits(key_type, depth + 1, node_count)?;
            validate_sql_data_type_with_limits(value_type, depth + 1, node_count)?;
        }
        SqlDataType::Bool
        | SqlDataType::Int8
        | SqlDataType::Int16
        | SqlDataType::Int32
        | SqlDataType::Int64
        | SqlDataType::UInt8
        | SqlDataType::UInt16
        | SqlDataType::UInt32
        | SqlDataType::UInt64
        | SqlDataType::Float32
        | SqlDataType::Float64
        | SqlDataType::Char { .. }
        | SqlDataType::Utf8
        | SqlDataType::Binary { .. }
        | SqlDataType::Varbinary
        | SqlDataType::Time
        | SqlDataType::Date
        | SqlDataType::Timestamp { .. }
        | SqlDataType::Interval { .. }
        | SqlDataType::Null
        | SqlDataType::Uuid
        | SqlDataType::Json
        | SqlDataType::Geometry => {}
    }
    Ok(())
}

fn validate_schema_fingerprint(value: &str) -> Result<(), ViewContractError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ViewContractError::InvalidField {
            field: "schema_fingerprint",
        });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ViewContractError::InvalidField {
            field: "schema_fingerprint",
        });
    }
    Ok(())
}

fn catalog_relation_error(error: RelationSchemaError) -> ViewContractError {
    match error {
        RelationSchemaError::UnsupportedSchemaVersion { .. } => ViewContractError::InvalidField {
            field: "catalog.schema_version",
        },
        RelationSchemaError::MissingIdentityField { field }
        | RelationSchemaError::InvalidRelationSchema { field }
        | RelationSchemaError::RelationIdentityMismatch { field }
        | RelationSchemaError::SchemaFingerprintMismatch { field } => {
            ViewContractError::RelationSchemaMismatch { field }
        }
        RelationSchemaError::Serialization { reason } => {
            ViewContractError::Serialization { reason }
        }
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), ViewContractError> {
    if value.trim().is_empty() {
        return Err(ViewContractError::MissingField { field });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_spec_hash_uses_path_safe_namespaced_hex() {
        let spec = StandingViewSpec {
            view_id: "device_status_latest".to_string(),
            sql: "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id".to_string(),
            dialect: SqlDialect::VelorixSql,
            source_kind: SqlSourceKind::StandingView,
            input_relations: vec![RelationSchema {
                relation_id: "device_status".to_string(),
                relation_name: "device_status".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "1".repeat(64)),
                columns: vec![
                    ColumnSchema {
                        name: "device_id".to_string(),
                        data_type: SqlDataType::Utf8,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "enabled".to_string(),
                        data_type: SqlDataType::Bool,
                        nullable: false,
                    },
                ],
                primary_key: vec!["device_id".to_string()],
            }],
            output_relations: vec![RelationSchema {
                relation_id: "device_status_latest".to_string(),
                relation_name: "device_status_latest".to_string(),
                relation_version: "v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
                columns: vec![
                    ColumnSchema {
                        name: "device_id".to_string(),
                        data_type: SqlDataType::Utf8,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "enabled".to_string(),
                        data_type: SqlDataType::Bool,
                        nullable: false,
                    },
                ],
                primary_key: vec!["device_id".to_string()],
            }],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };

        let hash = view_spec_hash(&spec).unwrap();
        let hex = hash.strip_prefix("velorix-view-spec-sha256-v1:").unwrap();
        assert_eq!(hex.len(), 64);
        assert!(hex.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}

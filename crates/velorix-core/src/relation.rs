use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const RELATION_SCHEMA_VERSION_V1: u32 = 1;
pub const SCHEMA_FINGERPRINT_V1_DOMAIN: &[u8] = b"velorix-relation-schema-v1\0";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixRelationCatalogV1 {
    pub schema_version: u32,
    pub relation_schema: VelorixRelationSchemaV1,
    pub schema_fingerprint: SchemaFingerprintV1,
    pub datafusion_registration: DataFusionRegistrationV1,
    pub feldera_relation: FelderaRelationBindingV1,
    pub incremental_adapter: IncrementalAdapterBindingV1,
}

impl VelorixRelationCatalogV1 {
    pub fn validate(&self) -> Result<(), RelationSchemaError> {
        if self.schema_version != RELATION_SCHEMA_VERSION_V1 {
            return Err(RelationSchemaError::UnsupportedSchemaVersion {
                found: self.schema_version,
            });
        }

        let computed = SchemaFingerprintV1::for_relation_schema(&self.relation_schema)?;
        self.schema_fingerprint.validate("schema_fingerprint")?;
        if self.schema_fingerprint != computed {
            return Err(RelationSchemaError::SchemaFingerprintMismatch { field: "catalog" });
        }

        require_non_empty(
            "datafusion_registration.name",
            &self.datafusion_registration.name,
        )?;
        require_non_empty(
            "feldera_relation.relation_id",
            &self.feldera_relation.relation_id,
        )?;
        if self.feldera_relation.relation_id != self.relation_schema.relation_id {
            return Err(RelationSchemaError::RelationIdentityMismatch {
                field: "feldera_relation.relation_id",
            });
        }
        self.feldera_relation
            .schema_fingerprint
            .validate("feldera_relation.schema_fingerprint")?;
        if self.feldera_relation.schema_fingerprint != self.schema_fingerprint {
            return Err(RelationSchemaError::SchemaFingerprintMismatch {
                field: "feldera_relation",
            });
        }
        require_non_empty(
            "incremental_adapter.adapter_id",
            &self.incremental_adapter.adapter_id,
        )?;

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixRelationSchemaV1 {
    pub relation_id: String,
    pub relation_name: String,
    pub relation_version: String,
    pub columns: Vec<RelationColumnV1>,
    pub primary_key_column_ids: Vec<String>,
    pub weight_column_id: String,
    pub allowed_operations: Vec<RelationOperationV1>,
    pub event_time_column_id: Option<String>,
}

impl VelorixRelationSchemaV1 {
    pub fn validate(&self) -> Result<(), RelationSchemaError> {
        require_non_empty("relation_id", &self.relation_id)?;
        require_non_empty("relation_name", &self.relation_name)?;
        require_non_empty("relation_version", &self.relation_version)?;
        if self.columns.is_empty() {
            return Err(RelationSchemaError::InvalidRelationSchema { field: "columns" });
        }
        if self.primary_key_column_ids.is_empty() {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "primary_key_column_ids",
            });
        }
        require_non_empty("weight_column_id", &self.weight_column_id)?;
        if self.allowed_operations.is_empty() {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "allowed_operations",
            });
        }

        let mut column_ids = BTreeSet::new();
        let mut column_names = BTreeSet::new();
        let mut ordinals = BTreeSet::new();
        for (index, column) in self.columns.iter().enumerate() {
            column.validate()?;
            if !column_ids.insert(column.column_id.as_str()) {
                return Err(RelationSchemaError::InvalidRelationSchema {
                    field: "duplicate_column_id",
                });
            }
            if !column_names.insert(column.name.as_str()) {
                return Err(RelationSchemaError::InvalidRelationSchema {
                    field: "duplicate_column_name",
                });
            }
            if !ordinals.insert(column.ordinal) {
                return Err(RelationSchemaError::InvalidRelationSchema {
                    field: "duplicate_ordinal",
                });
            }
            if column.ordinal as usize != index {
                return Err(RelationSchemaError::InvalidRelationSchema { field: "ordinal" });
            }
        }

        for column_id in &self.primary_key_column_ids {
            require_non_empty("primary_key_column_ids", column_id)?;
            if !column_ids.contains(column_id.as_str()) {
                return Err(RelationSchemaError::InvalidRelationSchema {
                    field: "primary_key_column_ids",
                });
            }
        }

        if !column_ids.contains(self.weight_column_id.as_str()) {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "weight_column_id",
            });
        }

        if let Some(column_id) = &self.event_time_column_id {
            require_non_empty("event_time_column_id", column_id)?;
            if !column_ids.contains(column_id.as_str()) {
                return Err(RelationSchemaError::InvalidRelationSchema {
                    field: "event_time_column_id",
                });
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationColumnV1 {
    pub column_id: String,
    pub name: String,
    pub logical_type: VelorixLogicalTypeV1,
    pub physical_arrow_type: ArrowPhysicalTypeV1,
    pub nullable: bool,
    pub ordinal: u32,
    pub semantic_role: RelationSemanticRoleV1,
}

impl RelationColumnV1 {
    fn validate(&self) -> Result<(), RelationSchemaError> {
        require_non_empty("column.column_id", &self.column_id)?;
        require_non_empty("column.name", &self.name)?;
        self.logical_type.validate()?;
        self.physical_arrow_type.validate()?;

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VelorixLogicalTypeV1 {
    Bool,
    Int64,
    Float64,
    Decimal { precision: u8, scale: u8 },
    Utf8,
    Date,
    Timestamp { timezone: Option<String> },
    Json,
}

impl VelorixLogicalTypeV1 {
    fn validate(&self) -> Result<(), RelationSchemaError> {
        match self {
            Self::Decimal { precision, scale } => validate_decimal(*precision, *scale),
            Self::Timestamp { timezone } => validate_timezone(timezone.as_deref()),
            Self::Bool | Self::Int64 | Self::Float64 | Self::Utf8 | Self::Date | Self::Json => {
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArrowPhysicalTypeV1 {
    Boolean,
    Int64,
    Float64,
    Decimal128 {
        precision: u8,
        scale: u8,
    },
    Utf8,
    Date32,
    TimestampNanosecond {
        timezone: Option<String>,
    },
    DictionaryUtf8 {
        key_type: DictionaryKeyTypeV1,
        ordered: bool,
    },
    JsonUtf8,
}

impl ArrowPhysicalTypeV1 {
    fn validate(&self) -> Result<(), RelationSchemaError> {
        match self {
            Self::Decimal128 { precision, scale } => validate_decimal(*precision, *scale),
            Self::TimestampNanosecond { timezone } => validate_timezone(timezone.as_deref()),
            Self::Boolean
            | Self::Int64
            | Self::Float64
            | Self::Utf8
            | Self::Date32
            | Self::DictionaryUtf8 { .. }
            | Self::JsonUtf8 => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DictionaryKeyTypeV1 {
    Int8,
    Int16,
    Int32,
    Int64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationSemanticRoleV1 {
    PrimaryKey,
    Value,
    Weight,
    EventTime,
    Metadata,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationOperationV1 {
    Insert,
    Delete,
    Update,
    Upsert,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataFusionRegistrationV1 {
    pub name: String,
    pub mode: DataFusionRegistrationModeV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataFusionRegistrationModeV1 {
    Table,
    View,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaRelationBindingV1 {
    pub relation_id: String,
    pub schema_fingerprint: SchemaFingerprintV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementalAdapterBindingV1 {
    pub adapter_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SchemaFingerprintV1(String);

impl SchemaFingerprintV1 {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn for_relation_schema(
        schema: &VelorixRelationSchemaV1,
    ) -> Result<Self, RelationSchemaError> {
        schema.validate()?;
        let canonical_json =
            serde_json::to_vec(schema).map_err(|source| RelationSchemaError::Serialization {
                reason: source.to_string(),
            })?;
        let mut hasher = Sha256::new();
        hasher.update(SCHEMA_FINGERPRINT_V1_DOMAIN);
        hasher.update(canonical_json);

        Ok(Self(format!("sha256:{:x}", hasher.finalize())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(&self, field: &'static str) -> Result<(), RelationSchemaError> {
        validate_schema_fingerprint(field, &self.0)
    }
}

impl fmt::Display for SchemaFingerprintV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum RelationSchemaError {
    #[error("unsupported relation catalog schema version {found}")]
    UnsupportedSchemaVersion { found: u32 },
    #[error("missing relation schema identity field: {field}")]
    MissingIdentityField { field: &'static str },
    #[error("invalid relation schema field: {field}")]
    InvalidRelationSchema { field: &'static str },
    #[error("relation identity mismatch: {field}")]
    RelationIdentityMismatch { field: &'static str },
    #[error("relation schema fingerprint mismatch: {field}")]
    SchemaFingerprintMismatch { field: &'static str },
    #[error("could not serialize canonical relation schema: {reason}")]
    Serialization { reason: String },
}

pub fn validate_schema_fingerprint(
    field: &'static str,
    fingerprint: &str,
) -> Result<(), RelationSchemaError> {
    if fingerprint.trim().is_empty() {
        return Err(RelationSchemaError::MissingIdentityField { field });
    }
    let Some(hex) = fingerprint.strip_prefix("sha256:") else {
        return Err(RelationSchemaError::InvalidRelationSchema { field });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RelationSchemaError::InvalidRelationSchema { field });
    }

    Ok(())
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), RelationSchemaError> {
    if value.trim().is_empty() {
        return Err(RelationSchemaError::MissingIdentityField { field });
    }

    Ok(())
}

fn validate_decimal(precision: u8, scale: u8) -> Result<(), RelationSchemaError> {
    if precision == 0 || scale > precision {
        return Err(RelationSchemaError::InvalidRelationSchema { field: "decimal" });
    }

    Ok(())
}

fn validate_timezone(timezone: Option<&str>) -> Result<(), RelationSchemaError> {
    if timezone.is_some_and(|timezone| timezone.trim().is_empty()) {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "timestamp.timezone",
        });
    }

    Ok(())
}

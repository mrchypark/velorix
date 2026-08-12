use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    Time64NanosecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{
    DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};

pub const RELATION_SCHEMA_VERSION_V1: u32 = 1;
pub const SCHEMA_FINGERPRINT_V1_DOMAIN: &[u8] = b"velorix-relation-schema-v1\0";
pub const DATAFUSION_RELATION_ID_METADATA_KEY: &str = "velorix.relation_id";
pub const DATAFUSION_RELATION_VERSION_METADATA_KEY: &str = "velorix.relation_version";
pub const DATAFUSION_SCHEMA_FINGERPRINT_METADATA_KEY: &str = "velorix.schema_fingerprint";
pub const CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID: &str =
    "incremental-adapter-single-key-sum-count-v1";
pub const CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID: &str =
    "incremental-adapter-row-key-sum-count-v1";
pub const ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID: &str = "incremental-adapter-orders-sum-count-v1";
pub const ORDERS_SUM_COUNT_RELATION_ID: &str = "orders";
pub const ORDERS_SUM_COUNT_RELATION_VERSION: &str = "2026-05-05.v1";
pub const ORDERS_SUM_COUNT_ADAPTER_ID: &str = ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID;
pub const CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID: &str = "incremental-adapter-generic-v1";
const MAX_RELATION_TYPE_NESTING_DEPTH: usize = 16;
const MAX_RELATION_STRUCT_FIELDS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupportedIncrementalAdapterSpec {
    ScalarSumCount,
    RowKeySumCount,
    Generic,
}

pub fn supported_incremental_adapter_spec(
    adapter_id: &str,
) -> Option<SupportedIncrementalAdapterSpec> {
    match adapter_id {
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID
        | ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID => {
            Some(SupportedIncrementalAdapterSpec::ScalarSumCount)
        }
        CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID => {
            Some(SupportedIncrementalAdapterSpec::RowKeySumCount)
        }
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID => Some(SupportedIncrementalAdapterSpec::Generic),
        _ => None,
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixRelationCatalogV1 {
    pub schema_version: u32,
    pub relation_schema: VelorixRelationSchemaV1,
    pub schema_fingerprint: SchemaFingerprintV1,
    pub datafusion_registration: DataFusionRegistrationV1,
    pub incremental_relation: IncrementalRelationBindingV1,
    pub incremental_adapter: IncrementalAdapterBindingV1,
    /// Provenance of this catalog. Source catalogs are registered relations
    /// backed by the ingest log. Published-view-output catalogs are runtime
    /// planning descriptors derived from a producer's immutable
    /// `PublishedRelationBindingV1`; they are never registered in the relation
    /// registry and must never be targets of external ingest.
    #[serde(default)]
    pub relation_source: VelorixRelationSourceV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VelorixRelationSourceV1 {
    SourceRelation,
    PublishedViewOutput {
        producer_view_id: String,
        producer_view_generation: u64,
        output_stream_id: String,
    },
}

impl Default for VelorixRelationSourceV1 {
    fn default() -> Self {
        VelorixRelationSourceV1::SourceRelation
    }
}

impl VelorixRelationCatalogV1 {
    pub fn from_relation_schema(
        relation_schema: VelorixRelationSchemaV1,
        adapter_id: String,
    ) -> Result<Self, RelationSchemaError> {
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)?;
        let catalog = Self {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            datafusion_registration: DataFusionRegistrationV1 {
                name: relation_schema.relation_name.clone(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: relation_schema.relation_id.clone(),
                schema_fingerprint: schema_fingerprint.clone(),
            },
            incremental_adapter: IncrementalAdapterBindingV1 { adapter_id },
            relation_schema,
            schema_fingerprint,
            relation_source: VelorixRelationSourceV1::SourceRelation,
        };
        catalog.validate_ingest_adapter_scope()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), RelationSchemaError> {
        if self.schema_version != RELATION_SCHEMA_VERSION_V1 {
            return Err(RelationSchemaError::UnsupportedSchemaVersion {
                found: self.schema_version,
            });
        }

        let computed = SchemaFingerprintV1::for_relation_schema(&self.relation_schema)?;
        self.schema_fingerprint.validate("schema_fingerprint")?;
        if matches!(
            self.relation_source,
            VelorixRelationSourceV1::SourceRelation
        ) && self.schema_fingerprint != computed
        {
            // Published-view-output descriptors carry the producer's signed
            // output schema fingerprint, which is computed over the public
            // output relation (not over the internal descriptor schema).
            return Err(RelationSchemaError::SchemaFingerprintMismatch { field: "catalog" });
        }

        require_non_empty(
            "datafusion_registration.name",
            &self.datafusion_registration.name,
        )?;
        validate_datafusion_registration_name(&self.datafusion_registration.name)?;
        require_non_empty(
            "incremental_relation.relation_id",
            &self.incremental_relation.relation_id,
        )?;
        if self.incremental_relation.relation_id != self.relation_schema.relation_id {
            return Err(RelationSchemaError::RelationIdentityMismatch {
                field: "incremental_relation.relation_id",
            });
        }
        self.incremental_relation
            .schema_fingerprint
            .validate("incremental_relation.schema_fingerprint")?;
        if self.incremental_relation.schema_fingerprint != self.schema_fingerprint {
            return Err(RelationSchemaError::SchemaFingerprintMismatch {
                field: "incremental_relation",
            });
        }
        require_non_empty(
            "incremental_adapter.adapter_id",
            &self.incremental_adapter.adapter_id,
        )?;

        Ok(())
    }

    pub fn validate_supported_incremental_adapter_scope(
        &self,
    ) -> Result<SupportedIncrementalAdapterSpec, RelationSchemaError> {
        self.validate()?;

        let spec = supported_incremental_adapter_spec(&self.incremental_adapter.adapter_id).ok_or(
            RelationSchemaError::InvalidRelationSchema {
                field: "incremental_adapter.adapter_id",
            },
        )?;

        match spec {
            SupportedIncrementalAdapterSpec::ScalarSumCount
                if self.relation_schema.primary_key_column_ids.len() != 1 =>
            {
                Err(RelationSchemaError::InvalidRelationSchema {
                    field: "incremental_adapter.primary_key_column_ids",
                })
            }
            SupportedIncrementalAdapterSpec::ScalarSumCount => Ok(spec),
            SupportedIncrementalAdapterSpec::RowKeySumCount => {
                validate_single_value_column_for_adapter(&self.relation_schema).map(|()| spec)
            }
            SupportedIncrementalAdapterSpec::Generic => {
                validate_generic_ingest_relation(&self.relation_schema).map(|()| spec)
            }
        }
    }

    pub fn validate_ingest_adapter_scope(
        &self,
    ) -> Result<SupportedIncrementalAdapterSpec, RelationSchemaError> {
        self.validate()?;

        let spec = supported_incremental_adapter_spec(&self.incremental_adapter.adapter_id).ok_or(
            RelationSchemaError::InvalidRelationSchema {
                field: "incremental_adapter.adapter_id",
            },
        )?;

        match spec {
            SupportedIncrementalAdapterSpec::ScalarSumCount
            | SupportedIncrementalAdapterSpec::RowKeySumCount => {
                self.validate_supported_incremental_adapter_scope()
            }
            SupportedIncrementalAdapterSpec::Generic => {
                validate_generic_ingest_relation(&self.relation_schema)?;
                Ok(spec)
            }
        }
    }
}

pub fn orders_sum_count_relation_catalog() -> Result<VelorixRelationCatalogV1, RelationSchemaError>
{
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
        relation_name: "orders".to_string(),
        relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    VelorixRelationCatalogV1::from_relation_schema(
        relation_schema,
        ORDERS_SUM_COUNT_ADAPTER_ID.to_string(),
    )
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
        validate_logical_physical_type_pair(&self.logical_type, &self.physical_arrow_type)?;

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VelorixLogicalTypeV1 {
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
    Date,
    Time,
    Timestamp {
        timezone: Option<String>,
    },
    Uuid,
    Json,
    Array {
        element_type: Box<VelorixLogicalTypeV1>,
    },
    Struct {
        fields: Vec<VelorixStructFieldV1>,
    },
    Map {
        key_type: Box<VelorixLogicalTypeV1>,
        value_type: Box<VelorixLogicalTypeV1>,
    },
}

impl VelorixLogicalTypeV1 {
    fn validate(&self) -> Result<(), RelationSchemaError> {
        self.validate_with_depth(0)
    }

    fn validate_with_depth(&self, depth: usize) -> Result<(), RelationSchemaError> {
        if depth > MAX_RELATION_TYPE_NESTING_DEPTH {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "logical_type.depth",
            });
        }
        match self {
            Self::Decimal { precision, scale } => validate_decimal(*precision, *scale),
            Self::Char { length: Some(0) } => Err(RelationSchemaError::InvalidRelationSchema {
                field: "char_length",
            }),
            Self::Timestamp { timezone } => validate_timezone(timezone.as_deref()),
            Self::Binary { length } if *length == 0 => {
                Err(RelationSchemaError::InvalidRelationSchema {
                    field: "binary_length",
                })
            }
            Self::Array { element_type } => element_type.validate_with_depth(depth + 1),
            Self::Struct { fields } => validate_logical_struct_fields(fields, depth),
            Self::Map {
                key_type,
                value_type,
            } => {
                key_type.validate_with_depth(depth + 1)?;
                value_type.validate_with_depth(depth + 1)
            }
            Self::Bool
            | Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::UInt8
            | Self::UInt16
            | Self::UInt32
            | Self::UInt64
            | Self::Float32
            | Self::Float64
            | Self::Char { .. }
            | Self::Utf8
            | Self::Binary { .. }
            | Self::Varbinary
            | Self::Date
            | Self::Time
            | Self::Uuid
            | Self::Json => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixStructFieldV1 {
    pub name: String,
    pub logical_type: VelorixLogicalTypeV1,
    pub nullable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArrowPhysicalTypeV1 {
    Boolean,
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
    Decimal128 {
        precision: u8,
        scale: u8,
    },
    Utf8,
    Binary,
    Date32,
    Time64Nanosecond,
    TimestampNanosecond {
        timezone: Option<String>,
    },
    DictionaryUtf8 {
        key_type: DictionaryKeyTypeV1,
        ordered: bool,
    },
    JsonUtf8,
    List {
        element_type: Box<ArrowPhysicalTypeV1>,
    },
    Struct {
        fields: Vec<ArrowStructFieldV1>,
    },
    Map {
        key_type: Box<ArrowPhysicalTypeV1>,
        value_type: Box<ArrowPhysicalTypeV1>,
    },
}

impl ArrowPhysicalTypeV1 {
    fn validate(&self) -> Result<(), RelationSchemaError> {
        self.validate_with_depth(0)
    }

    fn validate_with_depth(&self, depth: usize) -> Result<(), RelationSchemaError> {
        if depth > MAX_RELATION_TYPE_NESTING_DEPTH {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "physical_arrow_type.depth",
            });
        }
        match self {
            Self::Decimal128 { precision, scale } => validate_decimal(*precision, *scale),
            Self::TimestampNanosecond { timezone } => validate_timezone(timezone.as_deref()),
            Self::List { element_type } => element_type.validate_with_depth(depth + 1),
            Self::Struct { fields } => validate_arrow_struct_fields(fields, depth),
            Self::Map {
                key_type,
                value_type,
            } => {
                key_type.validate_with_depth(depth + 1)?;
                value_type.validate_with_depth(depth + 1)
            }
            Self::Boolean
            | Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::UInt8
            | Self::UInt16
            | Self::UInt32
            | Self::UInt64
            | Self::Float32
            | Self::Float64
            | Self::Utf8
            | Self::Binary
            | Self::Date32
            | Self::Time64Nanosecond
            | Self::DictionaryUtf8 { .. }
            | Self::JsonUtf8 => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowStructFieldV1 {
    pub name: String,
    pub physical_arrow_type: ArrowPhysicalTypeV1,
    pub nullable: bool,
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
pub struct IncrementalRelationBindingV1 {
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

#[derive(Debug, Error, Eq, PartialEq)]
pub enum IncrementalInputAdapterError {
    #[error(transparent)]
    RelationCatalog(#[from] RelationSchemaError),
    #[error("ingest relation mismatch for {field}: expected `{expected}`, actual `{actual}`")]
    IngestRelationMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("unsupported incremental adapter `{adapter_id}`")]
    UnsupportedIncrementalAdapter { adapter_id: String },
    #[error("malformed prototype Arrow ingest: {reason}")]
    MalformedArrowInput { reason: String },
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

fn validate_datafusion_registration_name(name: &str) -> Result<(), RelationSchemaError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(RelationSchemaError::MissingIdentityField {
            field: "datafusion_registration.name",
        });
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|char| char.is_ascii_alphanumeric() || char == '_')
    {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "datafusion_registration.name",
        });
    }

    if matches!(
        name.to_ascii_lowercase().as_str(),
        "input" | "information_schema" | "datafusion"
    ) {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "datafusion_registration.name",
        });
    }

    Ok(())
}

pub fn datafusion_schema_from_catalog(
    catalog: &VelorixRelationCatalogV1,
) -> Result<Arc<Schema>, RelationSchemaError> {
    catalog.validate()?;

    let fields = catalog
        .relation_schema
        .columns
        .iter()
        .map(|column| {
            Ok(Field::new(
                column.name.as_str(),
                data_type_for_arrow_physical_type(&column.physical_arrow_type)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>, RelationSchemaError>>()?;

    let mut metadata = HashMap::new();
    metadata.insert(
        DATAFUSION_RELATION_ID_METADATA_KEY.to_string(),
        catalog.relation_schema.relation_id.clone(),
    );
    metadata.insert(
        DATAFUSION_RELATION_VERSION_METADATA_KEY.to_string(),
        catalog.relation_schema.relation_version.clone(),
    );
    metadata.insert(
        DATAFUSION_SCHEMA_FINGERPRINT_METADATA_KEY.to_string(),
        catalog.schema_fingerprint.to_string(),
    );

    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

pub fn validate_record_batch_matches_catalog(
    catalog: &VelorixRelationCatalogV1,
    batch: &RecordBatch,
) -> Result<(), RelationSchemaError> {
    let expected = datafusion_schema_from_catalog(catalog)?;
    if batch.schema().fields() != expected.fields() {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "batch_schema",
        });
    }

    Ok(())
}

pub fn arrow_record_batches_to_single_key_sum_count_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    batches: &[RecordBatch],
) -> Result<DeltaBatch, IncrementalInputAdapterError> {
    catalog.validate()?;
    validate_incremental_input_identity(
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
    )?;
    let adapter_shape = supported_incremental_adapter_spec(&catalog.incremental_adapter.adapter_id)
        .ok_or_else(
            || IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
                adapter_id: catalog.incremental_adapter.adapter_id.clone(),
            },
        )?;
    if batches.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "at least one Arrow record batch is required".to_string(),
        });
    }

    let key_columns = match adapter_shape {
        SupportedIncrementalAdapterSpec::ScalarSumCount => {
            vec![single_primary_key_column(&catalog.relation_schema)?]
        }
        SupportedIncrementalAdapterSpec::RowKeySumCount => {
            primary_key_columns(&catalog.relation_schema)?
        }
        SupportedIncrementalAdapterSpec::Generic => {
            return Err(
                IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
                    adapter_id: catalog.incremental_adapter.adapter_id.clone(),
                },
            );
        }
    };
    let value_column = single_value_column(&catalog.relation_schema)?;
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(incremental_input_batch_schema_error)?;
        let keys = key_columns
            .iter()
            .map(|column| incremental_key_column(batch, column).map(|key| (*column, key)))
            .collect::<Result<Vec<_>, _>>()?;
        let value = incremental_value_column(batch, value_column)?;
        let weight = int64_column(batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if keys.iter().any(|(_, key)| key.is_null(row))
                || value.is_null(row)
                || weight.is_null(row)
            {
                return Err(IncrementalInputAdapterError::MalformedArrowInput {
                    reason: "prototype ingest columns must be non-null".to_string(),
                });
            }

            records.push(DeltaRecord::new(
                delta_key_from_columns(&keys, row)?,
                value.delta_value(row)?,
                weight.value(row),
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

pub fn arrow_record_batches_to_key_value_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    key_column_ids: &[String],
    value_column_id: &str,
    batches: &[RecordBatch],
) -> Result<DeltaBatch, IncrementalInputAdapterError> {
    catalog.validate()?;
    supported_incremental_adapter_spec(&catalog.incremental_adapter.adapter_id).ok_or_else(
        || IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        },
    )?;
    validate_incremental_input_identity(
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
    )?;
    if key_column_ids.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view input must define at least one key column".to_string(),
        });
    }
    if batches.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "at least one Arrow record batch is required".to_string(),
        });
    }

    let key_columns = key_column_ids
        .iter()
        .map(|column_id| relation_column(&catalog.relation_schema, column_id))
        .collect::<Result<Vec<_>, _>>()?;
    let value_column = relation_column(&catalog.relation_schema, value_column_id)?;
    if value_column.column_id == catalog.relation_schema.weight_column_id {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view value column must not be the weight column".to_string(),
        });
    }
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(incremental_input_batch_schema_error)?;
        let keys = key_columns
            .iter()
            .map(|column| incremental_key_column(batch, column).map(|key| (*column, key)))
            .collect::<Result<Vec<_>, _>>()?;
        let value = incremental_value_column(batch, value_column)?;
        let weight = int64_column(batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if keys.iter().any(|(_, key)| key.is_null(row))
                || value.is_null(row)
                || weight.is_null(row)
            {
                return Err(IncrementalInputAdapterError::MalformedArrowInput {
                    reason: "materialized view input columns must be non-null".to_string(),
                });
            }

            records.push(DeltaRecord::new(
                delta_key_from_columns(&keys, row)?,
                value.delta_value(row)?,
                weight.value(row),
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

pub fn arrow_record_batches_to_key_nullable_value_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    key_column_ids: &[String],
    value_column_id: &str,
    batches: &[RecordBatch],
) -> Result<DeltaBatch, IncrementalInputAdapterError> {
    catalog.validate()?;
    supported_incremental_adapter_spec(&catalog.incremental_adapter.adapter_id).ok_or_else(
        || IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        },
    )?;
    validate_incremental_input_identity(
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
    )?;
    if key_column_ids.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view input must define at least one key column".to_string(),
        });
    }
    if batches.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "at least one Arrow record batch is required".to_string(),
        });
    }

    let key_columns = key_column_ids
        .iter()
        .map(|column_id| relation_column(&catalog.relation_schema, column_id))
        .collect::<Result<Vec<_>, _>>()?;
    let value_column = relation_column(&catalog.relation_schema, value_column_id)?;
    if value_column.column_id == catalog.relation_schema.weight_column_id {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view value column must not be the weight column".to_string(),
        });
    }
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(incremental_input_batch_schema_error)?;
        let keys = key_columns
            .iter()
            .map(|column| incremental_key_column(batch, column).map(|key| (*column, key)))
            .collect::<Result<Vec<_>, _>>()?;
        let value = incremental_value_column(batch, value_column)?;
        let weight = int64_column(batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if keys.iter().any(|(_, key)| key.is_null(row)) || weight.is_null(row) {
                return Err(IncrementalInputAdapterError::MalformedArrowInput {
                    reason: "materialized view key and weight columns must be non-null".to_string(),
                });
            }

            let value = if value.is_null(row) {
                DeltaValue::from_json(Value::Null)
            } else {
                value.delta_value(row)?
            };
            records.push(DeltaRecord::new(
                delta_key_from_columns(&keys, row)?,
                value,
                weight.value(row),
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

pub fn arrow_record_batches_to_key_value_delta_batch_skipping_null_values(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    key_column_ids: &[String],
    value_column_id: &str,
    batches: &[RecordBatch],
) -> Result<DeltaBatch, IncrementalInputAdapterError> {
    catalog.validate()?;
    supported_incremental_adapter_spec(&catalog.incremental_adapter.adapter_id).ok_or_else(
        || IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        },
    )?;
    validate_incremental_input_identity(
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
    )?;
    if key_column_ids.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view input must define at least one key column".to_string(),
        });
    }
    if batches.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "at least one Arrow record batch is required".to_string(),
        });
    }

    let key_columns = key_column_ids
        .iter()
        .map(|column_id| relation_column(&catalog.relation_schema, column_id))
        .collect::<Result<Vec<_>, _>>()?;
    let value_column = relation_column(&catalog.relation_schema, value_column_id)?;
    if value_column.column_id == catalog.relation_schema.weight_column_id {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view value column must not be the weight column".to_string(),
        });
    }
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(incremental_input_batch_schema_error)?;
        let keys = key_columns
            .iter()
            .map(|column| incremental_key_column(batch, column).map(|key| (*column, key)))
            .collect::<Result<Vec<_>, _>>()?;
        let value = incremental_value_column(batch, value_column)?;
        let weight = int64_column(batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if value.is_null(row) {
                continue;
            }
            if keys.iter().any(|(_, key)| key.is_null(row)) || weight.is_null(row) {
                return Err(IncrementalInputAdapterError::MalformedArrowInput {
                    reason: "materialized view input columns must be non-null".to_string(),
                });
            }

            records.push(DeltaRecord::new(
                delta_key_from_columns(&keys, row)?,
                value.delta_value(row)?,
                weight.value(row),
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

pub fn arrow_record_batches_to_key_multi_value_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    key_column_ids: &[String],
    value_column_ids: &[String],
    batches: &[RecordBatch],
) -> Result<DeltaBatch, IncrementalInputAdapterError> {
    catalog.validate()?;
    supported_incremental_adapter_spec(&catalog.incremental_adapter.adapter_id).ok_or_else(
        || IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        },
    )?;
    validate_incremental_input_identity(
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
    )?;
    if key_column_ids.is_empty() || value_column_ids.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view input must define key and value columns".to_string(),
        });
    }
    if batches.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "at least one Arrow record batch is required".to_string(),
        });
    }

    let key_columns = key_column_ids
        .iter()
        .map(|column_id| relation_column(&catalog.relation_schema, column_id))
        .collect::<Result<Vec<_>, _>>()?;
    let value_columns = value_column_ids
        .iter()
        .map(|column_id| relation_column(&catalog.relation_schema, column_id))
        .collect::<Result<Vec<_>, _>>()?;
    if value_columns
        .iter()
        .any(|column| column.column_id == catalog.relation_schema.weight_column_id)
    {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "materialized view value column must not be the weight column".to_string(),
        });
    }
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(incremental_input_batch_schema_error)?;
        let keys = key_columns
            .iter()
            .map(|column| incremental_key_column(batch, column).map(|key| (*column, key)))
            .collect::<Result<Vec<_>, _>>()?;
        let values = value_columns
            .iter()
            .map(|column| incremental_value_column(batch, column).map(|value| (*column, value)))
            .collect::<Result<Vec<_>, _>>()?;
        let weight = int64_column(batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if keys.iter().any(|(_, key)| key.is_null(row)) || weight.is_null(row) {
                return Err(IncrementalInputAdapterError::MalformedArrowInput {
                    reason: "materialized view input columns must be non-null".to_string(),
                });
            }
            let value = values
                .iter()
                .map(|(column, value)| {
                    let json_value = if value.is_null(row) {
                        if !column.nullable {
                            return Err(IncrementalInputAdapterError::MalformedArrowInput {
                                reason: "materialized view input columns must be non-null"
                                    .to_string(),
                            });
                        }
                        Value::Null
                    } else {
                        value.json_value(row)?
                    };
                    Ok((column.column_id.clone(), json_value))
                })
                .collect::<Result<serde_json::Map<_, _>, IncrementalInputAdapterError>>()?;

            records.push(DeltaRecord::new(
                delta_key_from_columns(&keys, row)?,
                DeltaValue::from_json(Value::Object(value)),
                weight.value(row),
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

pub struct KeyLatestByDeltaBatchInput<'a> {
    pub catalog: &'a VelorixRelationCatalogV1,
    pub relation_id: &'a str,
    pub relation_version: &'a str,
    pub schema_fingerprint: &'a str,
    pub key_column_id: &'a str,
    pub value_column_id: &'a str,
    pub ordering_column_id: &'a str,
    pub batches: &'a [RecordBatch],
}

pub fn arrow_record_batches_to_key_latest_by_delta_batch(
    input: KeyLatestByDeltaBatchInput<'_>,
) -> Result<DeltaBatch, IncrementalInputAdapterError> {
    let KeyLatestByDeltaBatchInput {
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
        key_column_id,
        value_column_id,
        ordering_column_id,
        batches,
    } = input;

    catalog.validate()?;
    supported_incremental_adapter_spec(&catalog.incremental_adapter.adapter_id).ok_or_else(
        || IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        },
    )?;
    validate_incremental_input_identity(
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
    )?;
    if batches.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "at least one Arrow record batch is required".to_string(),
        });
    }

    let key_column = relation_column(&catalog.relation_schema, key_column_id)?;
    let value_column = relation_column(&catalog.relation_schema, value_column_id)?;
    let ordering_column = relation_column(&catalog.relation_schema, ordering_column_id)?;
    if value_column.column_id == catalog.relation_schema.weight_column_id
        || ordering_column.column_id == catalog.relation_schema.weight_column_id
    {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "latest-by-key value and ordering columns must not be the weight column"
                .to_string(),
        });
    }
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(incremental_input_batch_schema_error)?;
        let key = incremental_key_column(batch, key_column)?;
        let value = incremental_value_column(batch, value_column)?;
        let ordering = incremental_key_column(batch, ordering_column)?;
        let weight = int64_column(batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if key.is_null(row) || ordering.is_null(row) || weight.is_null(row) {
                return Err(IncrementalInputAdapterError::MalformedArrowInput {
                    reason: "materialized view input columns must be non-null".to_string(),
                });
            }
            let value = if value.is_null(row) {
                if !value_column.nullable {
                    return Err(IncrementalInputAdapterError::MalformedArrowInput {
                        reason: "materialized view input columns must be non-null".to_string(),
                    });
                }
                Value::Null
            } else {
                value.json_value(row)?
            };

            records.push(DeltaRecord::new(
                key.delta_key(row)?,
                DeltaValue::from_json(json!({
                    "value": value,
                    "ordering": ordering.json_value(row)?,
                })),
                weight.value(row),
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

pub fn arrow_record_batches_to_orders_sum_count_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    batches: &[RecordBatch],
) -> Result<DeltaBatch, IncrementalInputAdapterError> {
    arrow_record_batches_to_single_key_sum_count_delta_batch(
        catalog,
        relation_id,
        relation_version,
        schema_fingerprint,
        batches,
    )
}

fn incremental_input_batch_schema_error(
    error: RelationSchemaError,
) -> IncrementalInputAdapterError {
    match error {
        RelationSchemaError::InvalidRelationSchema {
            field: "batch_schema",
        } => IncrementalInputAdapterError::MalformedArrowInput {
            reason: "relation batch schema does not match catalog".to_string(),
        },
        error => IncrementalInputAdapterError::RelationCatalog(error),
    }
}

fn data_type_for_arrow_physical_type(
    physical_type: &ArrowPhysicalTypeV1,
) -> Result<DataType, RelationSchemaError> {
    Ok(match physical_type {
        ArrowPhysicalTypeV1::Boolean => DataType::Boolean,
        ArrowPhysicalTypeV1::Int8 => DataType::Int8,
        ArrowPhysicalTypeV1::Int16 => DataType::Int16,
        ArrowPhysicalTypeV1::Int32 => DataType::Int32,
        ArrowPhysicalTypeV1::Int64 => DataType::Int64,
        ArrowPhysicalTypeV1::UInt8 => DataType::UInt8,
        ArrowPhysicalTypeV1::UInt16 => DataType::UInt16,
        ArrowPhysicalTypeV1::UInt32 => DataType::UInt32,
        ArrowPhysicalTypeV1::UInt64 => DataType::UInt64,
        ArrowPhysicalTypeV1::Float32 => DataType::Float32,
        ArrowPhysicalTypeV1::Float64 => DataType::Float64,
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            let scale =
                i8::try_from(*scale).map_err(|_| RelationSchemaError::InvalidRelationSchema {
                    field: "unsupported_arrow_physical_type",
                })?;
            DataType::Decimal128(*precision, scale)
        }
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::JsonUtf8 => DataType::Utf8,
        ArrowPhysicalTypeV1::Binary => DataType::Binary,
        ArrowPhysicalTypeV1::Date32 => DataType::Date32,
        ArrowPhysicalTypeV1::Time64Nanosecond => DataType::Time64(TimeUnit::Nanosecond),
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => {
            DataType::Timestamp(TimeUnit::Nanosecond, timezone.clone().map(Into::into))
        }
        ArrowPhysicalTypeV1::DictionaryUtf8 { key_type, .. } => DataType::Dictionary(
            Box::new(dictionary_key_data_type(key_type)),
            Box::new(DataType::Utf8),
        ),
        ArrowPhysicalTypeV1::List { element_type } => DataType::List(Arc::new(Field::new(
            "item",
            data_type_for_arrow_physical_type(element_type)?,
            true,
        ))),
        ArrowPhysicalTypeV1::Struct { fields } => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok(Field::new(
                        field.name.as_str(),
                        data_type_for_arrow_physical_type(&field.physical_arrow_type)?,
                        field.nullable,
                    ))
                })
                .collect::<Result<Vec<_>, RelationSchemaError>>()?
                .into(),
        ),
        ArrowPhysicalTypeV1::Map {
            key_type,
            value_type,
        } => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", data_type_for_arrow_physical_type(key_type)?, false),
                        Field::new(
                            "values",
                            data_type_for_arrow_physical_type(value_type)?,
                            true,
                        ),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
    })
}

fn dictionary_key_data_type(key_type: &DictionaryKeyTypeV1) -> DataType {
    match key_type {
        DictionaryKeyTypeV1::Int8 => DataType::Int8,
        DictionaryKeyTypeV1::Int16 => DataType::Int16,
        DictionaryKeyTypeV1::Int32 => DataType::Int32,
        DictionaryKeyTypeV1::Int64 => DataType::Int64,
    }
}

fn validate_incremental_input_identity(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
) -> Result<(), IncrementalInputAdapterError> {
    if relation_id != catalog.relation_schema.relation_id {
        return Err(IncrementalInputAdapterError::IngestRelationMismatch {
            field: "relation_id",
            expected: catalog.relation_schema.relation_id.clone(),
            actual: relation_id.to_string(),
        });
    }
    if relation_version != catalog.relation_schema.relation_version {
        return Err(IncrementalInputAdapterError::IngestRelationMismatch {
            field: "relation_version",
            expected: catalog.relation_schema.relation_version.clone(),
            actual: relation_version.to_string(),
        });
    }
    if schema_fingerprint != catalog.schema_fingerprint.as_str() {
        return Err(IncrementalInputAdapterError::IngestRelationMismatch {
            field: "schema_fingerprint",
            expected: catalog.schema_fingerprint.to_string(),
            actual: schema_fingerprint.to_string(),
        });
    }

    Ok(())
}

fn relation_column<'a>(
    schema: &'a VelorixRelationSchemaV1,
    column_id: &str,
) -> Result<&'a RelationColumnV1, IncrementalInputAdapterError> {
    schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("relation catalog is missing column `{column_id}`"),
        })
}

fn primary_key_columns(
    schema: &VelorixRelationSchemaV1,
) -> Result<Vec<&RelationColumnV1>, IncrementalInputAdapterError> {
    schema
        .primary_key_column_ids
        .iter()
        .map(|column_id| relation_column(schema, column_id.as_str()))
        .collect()
}

fn single_primary_key_column(
    schema: &VelorixRelationSchemaV1,
) -> Result<&RelationColumnV1, IncrementalInputAdapterError> {
    if schema.primary_key_column_ids.len() != 1 {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "prototype adapter supports exactly one primary key column".to_string(),
        });
    }

    relation_column(schema, schema.primary_key_column_ids[0].as_str())
}

fn single_value_column(
    schema: &VelorixRelationSchemaV1,
) -> Result<&RelationColumnV1, IncrementalInputAdapterError> {
    validate_single_value_column_for_adapter(schema).map_err(|error| {
        IncrementalInputAdapterError::MalformedArrowInput {
            reason: match error {
                RelationSchemaError::InvalidRelationSchema {
                    field: "incremental_adapter.value_column",
                } => "relation catalog must define one value column".to_string(),
                RelationSchemaError::InvalidRelationSchema {
                    field: "incremental_adapter.value_columns",
                } => "prototype adapter supports exactly one value column".to_string(),
                _ => "prototype adapter supports exactly one value column".to_string(),
            },
        }
    })?;

    Ok(schema
        .columns
        .iter()
        .find(|column| column.semantic_role == RelationSemanticRoleV1::Value)
        .expect("validated single value column must exist"))
}

fn validate_single_value_column_for_adapter(
    schema: &VelorixRelationSchemaV1,
) -> Result<(), RelationSchemaError> {
    let mut values = schema
        .columns
        .iter()
        .filter(|column| column.semantic_role == RelationSemanticRoleV1::Value);
    if values.next().is_none() {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.value_column",
        });
    }
    if values.next().is_some() {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.value_columns",
        });
    }

    Ok(())
}

fn validate_generic_ingest_relation(
    schema: &VelorixRelationSchemaV1,
) -> Result<(), RelationSchemaError> {
    let weight_column = schema
        .columns
        .iter()
        .find(|column| column.column_id == schema.weight_column_id)
        .ok_or(RelationSchemaError::InvalidRelationSchema {
            field: "weight_column_id",
        })?;
    if !matches!(weight_column.logical_type, VelorixLogicalTypeV1::Int64)
        || !matches!(
            weight_column.physical_arrow_type,
            ArrowPhysicalTypeV1::Int64
        )
    {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "generic_adapter.weight_column.type",
        });
    }
    if weight_column.nullable {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "generic_adapter.weight_column.nullable",
        });
    }
    if schema
        .primary_key_column_ids
        .iter()
        .any(|column_id| column_id == &schema.weight_column_id)
    {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "generic_adapter.weight_column.primary_key",
        });
    }
    if !schema
        .allowed_operations
        .iter()
        .any(|operation| operation == &RelationOperationV1::Insert)
    {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "generic_adapter.allowed_operations.insert",
        });
    }

    Ok(())
}

fn delta_key_from_columns(
    columns: &[(&RelationColumnV1, IncrementalKeyColumn<'_>)],
    row: usize,
) -> Result<DeltaKey, IncrementalInputAdapterError> {
    if let [(_, column)] = columns {
        return column.delta_key(row);
    }

    let mut object = BTreeMap::new();
    for (catalog_column, column) in columns {
        object.insert(catalog_column.column_id.clone(), column.json_value(row)?);
    }

    Ok(DeltaKey::from_json(Value::Object(
        object.into_iter().collect(),
    )))
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, IncrementalInputAdapterError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("`{name}` column must be Utf8"),
        })
}

enum IncrementalKeyColumn<'a> {
    Boolean(&'a BooleanArray),
    Utf8(&'a StringArray),
    JsonUtf8(&'a StringArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Binary(&'a BinaryArray),
    Decimal128(&'a Decimal128Array, u8, u8),
    Date32(&'a Date32Array),
    Time64Nanosecond(&'a Time64NanosecondArray),
    TimestampNanosecond(&'a TimestampNanosecondArray),
    DictionaryUtf8Int8(&'a DictionaryArray<Int8Type>, &'a StringArray),
    DictionaryUtf8Int16(&'a DictionaryArray<Int16Type>, &'a StringArray),
    DictionaryUtf8Int32(&'a DictionaryArray<Int32Type>, &'a StringArray),
    DictionaryUtf8Int64(&'a DictionaryArray<Int64Type>, &'a StringArray),
}

enum IncrementalValueColumn<'a> {
    Boolean(&'a BooleanArray),
    Utf8(&'a StringArray),
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array, u8, u8),
}

impl IncrementalKeyColumn<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Boolean(column) => column.is_null(row),
            Self::Utf8(column) => column.is_null(row),
            Self::JsonUtf8(column) => column.is_null(row),
            Self::Int8(column) => column.is_null(row),
            Self::Int16(column) => column.is_null(row),
            Self::Int32(column) => column.is_null(row),
            Self::Int64(column) => column.is_null(row),
            Self::UInt8(column) => column.is_null(row),
            Self::UInt16(column) => column.is_null(row),
            Self::UInt32(column) => column.is_null(row),
            Self::UInt64(column) => column.is_null(row),
            Self::Float32(column) => column.is_null(row),
            Self::Float64(column) => column.is_null(row),
            Self::Binary(column) => column.is_null(row),
            Self::Decimal128(column, _, _) => column.is_null(row),
            Self::Date32(column) => column.is_null(row),
            Self::Time64Nanosecond(column) => column.is_null(row),
            Self::TimestampNanosecond(column) => column.is_null(row),
            Self::DictionaryUtf8Int8(column, values) => {
                dictionary_utf8_is_null(column, values, row)
            }
            Self::DictionaryUtf8Int16(column, values) => {
                dictionary_utf8_is_null(column, values, row)
            }
            Self::DictionaryUtf8Int32(column, values) => {
                dictionary_utf8_is_null(column, values, row)
            }
            Self::DictionaryUtf8Int64(column, values) => {
                dictionary_utf8_is_null(column, values, row)
            }
        }
    }

    fn delta_key(&self, row: usize) -> Result<DeltaKey, IncrementalInputAdapterError> {
        self.json_value(row).map(DeltaKey::from_json)
    }

    fn json_value(&self, row: usize) -> Result<Value, IncrementalInputAdapterError> {
        match self {
            Self::Boolean(column) => Ok(json!(column.value(row))),
            Self::Utf8(column) => Ok(json!(column.value(row))),
            Self::JsonUtf8(column) => serde_json::from_str(column.value(row)).map_err(|error| {
                IncrementalInputAdapterError::MalformedArrowInput {
                    reason: format!("JsonUtf8 key column contains invalid JSON: {error}"),
                }
            }),
            Self::Int8(column) => Ok(json!(column.value(row))),
            Self::Int16(column) => Ok(json!(column.value(row))),
            Self::Int32(column) => Ok(json!(column.value(row))),
            Self::Int64(column) => Ok(json!(column.value(row))),
            Self::UInt8(column) => Ok(json!(column.value(row))),
            Self::UInt16(column) => Ok(json!(column.value(row))),
            Self::UInt32(column) => Ok(json!(column.value(row))),
            Self::UInt64(column) => Ok(json!(column.value(row))),
            Self::Float32(column) => {
                let value = column.value(row);
                if !value.is_finite() {
                    return Err(IncrementalInputAdapterError::MalformedArrowInput {
                        reason: "Float32 key column must contain only finite values".to_string(),
                    });
                }

                Ok(json!(if value == 0.0 { 0.0 } else { value }))
            }
            Self::Float64(column) => {
                let value = column.value(row);
                if !value.is_finite() {
                    return Err(IncrementalInputAdapterError::MalformedArrowInput {
                        reason: "Float64 key column must contain only finite values".to_string(),
                    });
                }

                Ok(json!(if value == 0.0 { 0.0 } else { value }))
            }
            Self::Decimal128(column, precision, scale) => Ok(json!(decimal128_string(
                column.value(row),
                *precision,
                *scale
            )?)),
            Self::Binary(column) => Ok(json!(format_hex_binary(column.value(row)))),
            Self::Date32(column) => Ok(json!(column.value(row))),
            Self::Time64Nanosecond(column) => Ok(json!(column.value(row))),
            Self::TimestampNanosecond(column) => Ok(json!(column.value(row))),
            Self::DictionaryUtf8Int8(column, values) => {
                Ok(json!(dictionary_utf8_value(column, values, row)))
            }
            Self::DictionaryUtf8Int16(column, values) => {
                Ok(json!(dictionary_utf8_value(column, values, row)))
            }
            Self::DictionaryUtf8Int32(column, values) => {
                Ok(json!(dictionary_utf8_value(column, values, row)))
            }
            Self::DictionaryUtf8Int64(column, values) => {
                Ok(json!(dictionary_utf8_value(column, values, row)))
            }
        }
    }
}

impl IncrementalValueColumn<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Boolean(column) => column.is_null(row),
            Self::Utf8(column) => column.is_null(row),
            Self::Int64(column) => column.is_null(row),
            Self::Float64(column) => column.is_null(row),
            Self::Decimal128(column, _, _) => column.is_null(row),
        }
    }

    fn delta_value(&self, row: usize) -> Result<DeltaValue, IncrementalInputAdapterError> {
        self.json_value(row).map(DeltaValue::from_json)
    }

    fn json_value(&self, row: usize) -> Result<Value, IncrementalInputAdapterError> {
        match self {
            Self::Boolean(column) => Ok(json!(column.value(row))),
            Self::Utf8(column) => Ok(json!(column.value(row))),
            Self::Int64(column) => Ok(json!(column.value(row))),
            Self::Float64(column) => {
                let value = column.value(row);
                if !value.is_finite() {
                    return Err(IncrementalInputAdapterError::MalformedArrowInput {
                        reason: "Float64 value column must contain only finite values".to_string(),
                    });
                }

                Ok(json!(value))
            }
            Self::Decimal128(column, precision, scale) => Ok(json!(decimal128_string(
                column.value(row),
                *precision,
                *scale
            )?)),
        }
    }
}

fn incremental_key_column<'a>(
    batch: &'a RecordBatch,
    column: &RelationColumnV1,
) -> Result<IncrementalKeyColumn<'a>, IncrementalInputAdapterError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => {
            boolean_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Boolean)
        }
        ArrowPhysicalTypeV1::Int8 => {
            int8_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Int8)
        }
        ArrowPhysicalTypeV1::Int16 => {
            int16_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Int16)
        }
        ArrowPhysicalTypeV1::Int32 => {
            int32_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Int32)
        }
        ArrowPhysicalTypeV1::Utf8 => {
            string_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Utf8)
        }
        ArrowPhysicalTypeV1::JsonUtf8 => {
            string_column(batch, column.name.as_str()).map(IncrementalKeyColumn::JsonUtf8)
        }
        ArrowPhysicalTypeV1::Int64 => {
            int64_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Int64)
        }
        ArrowPhysicalTypeV1::UInt8 => {
            uint8_column(batch, column.name.as_str()).map(IncrementalKeyColumn::UInt8)
        }
        ArrowPhysicalTypeV1::UInt16 => {
            uint16_column(batch, column.name.as_str()).map(IncrementalKeyColumn::UInt16)
        }
        ArrowPhysicalTypeV1::UInt32 => {
            uint32_column(batch, column.name.as_str()).map(IncrementalKeyColumn::UInt32)
        }
        ArrowPhysicalTypeV1::UInt64 => {
            uint64_column(batch, column.name.as_str()).map(IncrementalKeyColumn::UInt64)
        }
        ArrowPhysicalTypeV1::Float32 => {
            float32_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Float32)
        }
        ArrowPhysicalTypeV1::Float64 => {
            float64_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Float64)
        }
        ArrowPhysicalTypeV1::Binary => {
            binary_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Binary)
        }
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            decimal128_column(batch, column.name.as_str())
                .map(|array| IncrementalKeyColumn::Decimal128(array, *precision, *scale))
        }
        ArrowPhysicalTypeV1::Date32 => {
            date32_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Date32)
        }
        ArrowPhysicalTypeV1::Time64Nanosecond => {
            time64_nanosecond_column(batch, column.name.as_str())
                .map(IncrementalKeyColumn::Time64Nanosecond)
        }
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            timestamp_nanosecond_column(batch, column.name.as_str())
                .map(IncrementalKeyColumn::TimestampNanosecond)
        }
        ArrowPhysicalTypeV1::DictionaryUtf8 { key_type, .. } => {
            dictionary_utf8_column(batch, column.name.as_str(), key_type)
        }
        ArrowPhysicalTypeV1::List { .. }
        | ArrowPhysicalTypeV1::Struct { .. }
        | ArrowPhysicalTypeV1::Map { .. } => {
            Err(IncrementalInputAdapterError::MalformedArrowInput {
                reason: format!(
                "`{}` column uses a nested Arrow type that is not supported as an incremental key",
                column.name
            ),
            })
        }
    }
}

fn incremental_value_column<'a>(
    batch: &'a RecordBatch,
    column: &RelationColumnV1,
) -> Result<IncrementalValueColumn<'a>, IncrementalInputAdapterError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => {
            boolean_column(batch, column.name.as_str()).map(IncrementalValueColumn::Boolean)
        }
        ArrowPhysicalTypeV1::Utf8 => {
            string_column(batch, column.name.as_str()).map(IncrementalValueColumn::Utf8)
        }
        ArrowPhysicalTypeV1::Int64 => {
            int64_column(batch, column.name.as_str()).map(IncrementalValueColumn::Int64)
        }
        ArrowPhysicalTypeV1::Float64 => {
            float64_column(batch, column.name.as_str()).map(IncrementalValueColumn::Float64)
        }
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            decimal128_column(batch, column.name.as_str())
                .map(|array| IncrementalValueColumn::Decimal128(array, *precision, *scale))
        }
        _ => Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!(
                "prototype adapter value column `{}` must be Utf8, Int64, Float64, or Decimal128",
                column.name
            ),
        }),
    }
}

fn boolean_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a BooleanArray, IncrementalInputAdapterError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("`{name}` column must be Boolean"),
        })
}

macro_rules! primitive_column_reader {
    ($fn_name:ident, $array_type:ty, $type_name:literal) => {
        fn $fn_name<'a>(
            batch: &'a RecordBatch,
            name: &str,
        ) -> Result<&'a $array_type, IncrementalInputAdapterError> {
            batch
                .column_by_name(name)
                .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
                    reason: format!("missing `{name}` column"),
                })?
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
                    reason: format!("`{name}` column must be {}", $type_name),
                })
        }
    };
}

primitive_column_reader!(int8_column, Int8Array, "Int8");
primitive_column_reader!(int16_column, Int16Array, "Int16");
primitive_column_reader!(int32_column, Int32Array, "Int32");
primitive_column_reader!(uint8_column, UInt8Array, "UInt8");
primitive_column_reader!(uint16_column, UInt16Array, "UInt16");
primitive_column_reader!(uint32_column, UInt32Array, "UInt32");
primitive_column_reader!(uint64_column, UInt64Array, "UInt64");
primitive_column_reader!(float32_column, Float32Array, "Float32");
primitive_column_reader!(binary_column, BinaryArray, "Binary");
primitive_column_reader!(
    time64_nanosecond_column,
    Time64NanosecondArray,
    "Time64(Nanosecond)"
);

fn dictionary_utf8_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    key_type: &DictionaryKeyTypeV1,
) -> Result<IncrementalKeyColumn<'a>, IncrementalInputAdapterError> {
    macro_rules! downcast_dictionary {
        ($arrow_key_type:ty, $variant:ident) => {{
            let column = batch
                .column_by_name(name)
                .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
                    reason: format!("missing `{name}` column"),
                })?
                .as_any()
                .downcast_ref::<DictionaryArray<$arrow_key_type>>()
                .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
                    reason: format!("`{name}` column must be DictionaryUtf8"),
                })?;
            let values = column
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
                    reason: format!("`{name}` dictionary values must be DictionaryUtf8"),
                })?;
            Ok(IncrementalKeyColumn::$variant(column, values))
        }};
    }

    match key_type {
        DictionaryKeyTypeV1::Int8 => downcast_dictionary!(Int8Type, DictionaryUtf8Int8),
        DictionaryKeyTypeV1::Int16 => downcast_dictionary!(Int16Type, DictionaryUtf8Int16),
        DictionaryKeyTypeV1::Int32 => downcast_dictionary!(Int32Type, DictionaryUtf8Int32),
        DictionaryKeyTypeV1::Int64 => downcast_dictionary!(Int64Type, DictionaryUtf8Int64),
    }
}

fn dictionary_utf8_value<'a, K>(
    column: &'a DictionaryArray<K>,
    values: &'a StringArray,
    row: usize,
) -> &'a str
where
    K: arrow::array::types::ArrowDictionaryKeyType,
{
    values.value(
        column
            .key(row)
            .expect("caller checked dictionary key is non-null"),
    )
}

fn dictionary_utf8_is_null<K>(column: &DictionaryArray<K>, values: &StringArray, row: usize) -> bool
where
    K: arrow::array::types::ArrowDictionaryKeyType,
{
    match column.key(row) {
        Some(key) => values.is_null(key),
        None => true,
    }
}

fn int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, IncrementalInputAdapterError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("`{name}` column must be Int64"),
        })
}

fn float64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float64Array, IncrementalInputAdapterError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("`{name}` column must be Float64"),
        })
}

fn decimal128_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Decimal128Array, IncrementalInputAdapterError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("`{name}` column must be Decimal128"),
        })
}

fn decimal128_string(
    value: i128,
    precision: u8,
    scale: u8,
) -> Result<String, IncrementalInputAdapterError> {
    let magnitude = value.unsigned_abs();
    if decimal128_digit_count(magnitude) > precision {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "Decimal128 column value exceeds declared precision".to_string(),
        });
    }

    let mut digits = magnitude.to_string();
    let scale = usize::from(scale);
    let mut decimal = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        let leading_zeroes = "0".repeat(scale - digits.len());
        format!("0.{leading_zeroes}{digits}")
    } else {
        let fractional = digits.split_off(digits.len() - scale);
        format!("{digits}.{fractional}")
    };

    if value.is_negative() {
        decimal.insert(0, '-');
    }

    Ok(decimal)
}

fn decimal128_digit_count(value: u128) -> u8 {
    if value == 0 {
        1
    } else {
        value.ilog10() as u8 + 1
    }
}

fn format_hex_binary(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(2 + bytes.len() * 2);
    output.push_str("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn date32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Date32Array, IncrementalInputAdapterError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("`{name}` column must be Date32"),
        })
}

fn timestamp_nanosecond_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampNanosecondArray, IncrementalInputAdapterError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!("`{name}` column must be TimestampNanosecond"),
        })
}

fn validate_logical_physical_type_pair(
    logical_type: &VelorixLogicalTypeV1,
    physical_type: &ArrowPhysicalTypeV1,
) -> Result<(), RelationSchemaError> {
    let matches = match (logical_type, physical_type) {
        (VelorixLogicalTypeV1::Bool, ArrowPhysicalTypeV1::Boolean) => true,
        (VelorixLogicalTypeV1::Int8, ArrowPhysicalTypeV1::Int8) => true,
        (VelorixLogicalTypeV1::Int16, ArrowPhysicalTypeV1::Int16) => true,
        (VelorixLogicalTypeV1::Int32, ArrowPhysicalTypeV1::Int32) => true,
        (VelorixLogicalTypeV1::Int64, ArrowPhysicalTypeV1::Int64) => true,
        (VelorixLogicalTypeV1::UInt8, ArrowPhysicalTypeV1::UInt8) => true,
        (VelorixLogicalTypeV1::UInt16, ArrowPhysicalTypeV1::UInt16) => true,
        (VelorixLogicalTypeV1::UInt32, ArrowPhysicalTypeV1::UInt32) => true,
        (VelorixLogicalTypeV1::UInt64, ArrowPhysicalTypeV1::UInt64) => true,
        (VelorixLogicalTypeV1::Float32, ArrowPhysicalTypeV1::Float32) => true,
        (VelorixLogicalTypeV1::Float64, ArrowPhysicalTypeV1::Float64) => true,
        (
            VelorixLogicalTypeV1::Decimal {
                precision: logical_precision,
                scale: logical_scale,
            },
            ArrowPhysicalTypeV1::Decimal128 {
                precision: physical_precision,
                scale: physical_scale,
            },
        ) => logical_precision == physical_precision && logical_scale == physical_scale,
        (VelorixLogicalTypeV1::Utf8, ArrowPhysicalTypeV1::Utf8) => true,
        (VelorixLogicalTypeV1::Utf8, ArrowPhysicalTypeV1::DictionaryUtf8 { .. }) => true,
        (VelorixLogicalTypeV1::Char { .. }, ArrowPhysicalTypeV1::Utf8) => true,
        (VelorixLogicalTypeV1::Char { .. }, ArrowPhysicalTypeV1::DictionaryUtf8 { .. }) => true,
        (VelorixLogicalTypeV1::Uuid, ArrowPhysicalTypeV1::Utf8) => true,
        (VelorixLogicalTypeV1::Binary { .. }, ArrowPhysicalTypeV1::Binary) => true,
        (VelorixLogicalTypeV1::Varbinary, ArrowPhysicalTypeV1::Binary) => true,
        (VelorixLogicalTypeV1::Date, ArrowPhysicalTypeV1::Date32) => true,
        (VelorixLogicalTypeV1::Time, ArrowPhysicalTypeV1::Time64Nanosecond) => true,
        (
            VelorixLogicalTypeV1::Timestamp {
                timezone: logical_timezone,
            },
            ArrowPhysicalTypeV1::TimestampNanosecond {
                timezone: physical_timezone,
            },
        ) => logical_timezone == physical_timezone,
        (VelorixLogicalTypeV1::Json, ArrowPhysicalTypeV1::JsonUtf8) => true,
        (
            VelorixLogicalTypeV1::Array {
                element_type: logical_element,
            },
            ArrowPhysicalTypeV1::List {
                element_type: physical_element,
            },
        ) => validate_logical_physical_type_pair(logical_element, physical_element).is_ok(),
        (
            VelorixLogicalTypeV1::Struct {
                fields: logical_fields,
            },
            ArrowPhysicalTypeV1::Struct {
                fields: physical_fields,
            },
        ) => logical_struct_fields_match_physical(logical_fields, physical_fields),
        (
            VelorixLogicalTypeV1::Map {
                key_type: logical_key,
                value_type: logical_value,
            },
            ArrowPhysicalTypeV1::Map {
                key_type: physical_key,
                value_type: physical_value,
            },
        ) => {
            validate_logical_physical_type_pair(logical_key, physical_key).is_ok()
                && validate_logical_physical_type_pair(logical_value, physical_value).is_ok()
        }
        _ => false,
    };

    if matches {
        Ok(())
    } else {
        Err(RelationSchemaError::InvalidRelationSchema {
            field: "logical_physical_type",
        })
    }
}

fn validate_logical_struct_fields(
    fields: &[VelorixStructFieldV1],
    depth: usize,
) -> Result<(), RelationSchemaError> {
    if fields.is_empty() || fields.len() > MAX_RELATION_STRUCT_FIELDS {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "logical_type.struct.fields",
        });
    }
    let mut names = BTreeSet::new();
    for field in fields {
        require_non_empty("logical_type.struct.field.name", &field.name)?;
        if !names.insert(field.name.as_str()) {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "logical_type.struct.field.name",
            });
        }
        field.logical_type.validate_with_depth(depth + 1)?;
    }
    Ok(())
}

fn validate_arrow_struct_fields(
    fields: &[ArrowStructFieldV1],
    depth: usize,
) -> Result<(), RelationSchemaError> {
    if fields.is_empty() || fields.len() > MAX_RELATION_STRUCT_FIELDS {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "physical_arrow_type.struct.fields",
        });
    }
    let mut names = BTreeSet::new();
    for field in fields {
        require_non_empty("physical_arrow_type.struct.field.name", &field.name)?;
        if !names.insert(field.name.as_str()) {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "physical_arrow_type.struct.field.name",
            });
        }
        field.physical_arrow_type.validate_with_depth(depth + 1)?;
    }
    Ok(())
}

fn logical_struct_fields_match_physical(
    logical_fields: &[VelorixStructFieldV1],
    physical_fields: &[ArrowStructFieldV1],
) -> bool {
    logical_fields.len() == physical_fields.len()
        && logical_fields
            .iter()
            .zip(physical_fields)
            .all(|(logical, physical)| {
                logical.name == physical.name
                    && logical.nullable == physical.nullable
                    && validate_logical_physical_type_pair(
                        &logical.logical_type,
                        &physical.physical_arrow_type,
                    )
                    .is_ok()
            })
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), RelationSchemaError> {
    if value.trim().is_empty() {
        return Err(RelationSchemaError::MissingIdentityField { field });
    }

    Ok(())
}

fn validate_decimal(precision: u8, scale: u8) -> Result<(), RelationSchemaError> {
    if precision == 0 || precision > 38 || scale > precision {
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

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, DictionaryArray, Int64Array, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{
    DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use serde_json::json;
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
pub const ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID: &str = "incremental-adapter-orders-sum-count-v1";

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
        validate_datafusion_registration_name(&self.datafusion_registration.name)?;
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
        validate_logical_physical_type_pair(&self.logical_type, &self.physical_arrow_type)?;

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
    if !is_single_key_sum_count_incremental_adapter_id(
        catalog.incremental_adapter.adapter_id.as_str(),
    ) {
        return Err(
            IncrementalInputAdapterError::UnsupportedIncrementalAdapter {
                adapter_id: catalog.incremental_adapter.adapter_id.clone(),
            },
        );
    }
    if batches.is_empty() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "at least one Arrow record batch is required".to_string(),
        });
    }

    let key_column = single_primary_key_column(&catalog.relation_schema)?;
    let value_column = single_value_column(&catalog.relation_schema)?;
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(incremental_input_batch_schema_error)?;
        let key = incremental_key_column(batch, key_column)?;
        let value = int64_column(batch, value_column.name.as_str())?;
        let weight = int64_column(batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if key.is_null(row) || value.is_null(row) || weight.is_null(row) {
                return Err(IncrementalInputAdapterError::MalformedArrowInput {
                    reason: "prototype ingest columns must be non-null".to_string(),
                });
            }

            records.push(DeltaRecord::new(
                key.delta_key(row),
                DeltaValue::from_json(json!(value.value(row))),
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

fn is_single_key_sum_count_incremental_adapter_id(adapter_id: &str) -> bool {
    matches!(
        adapter_id,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID
            | ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID
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

pub fn register_datafusion_catalog_batches(
    context: &SessionContext,
    catalog: &VelorixRelationCatalogV1,
    batches: Vec<RecordBatch>,
) -> Result<(), DataFusionError> {
    if catalog.datafusion_registration.mode != DataFusionRegistrationModeV1::Table {
        return Err(relation_datafusion_error(
            RelationSchemaError::InvalidRelationSchema {
                field: "datafusion_registration.mode",
            },
        ));
    }

    for batch in &batches {
        validate_record_batch_matches_catalog(catalog, batch).map_err(relation_datafusion_error)?;
    }

    let schema = datafusion_schema_from_catalog(catalog).map_err(relation_datafusion_error)?;
    let table = MemTable::try_new(schema, vec![batches])?;
    context.register_table(
        catalog.datafusion_registration.name.as_str(),
        Arc::new(table),
    )?;

    Ok(())
}

fn data_type_for_arrow_physical_type(
    physical_type: &ArrowPhysicalTypeV1,
) -> Result<DataType, RelationSchemaError> {
    Ok(match physical_type {
        ArrowPhysicalTypeV1::Boolean => DataType::Boolean,
        ArrowPhysicalTypeV1::Int64 => DataType::Int64,
        ArrowPhysicalTypeV1::Float64 => DataType::Float64,
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            let scale =
                i8::try_from(*scale).map_err(|_| RelationSchemaError::InvalidRelationSchema {
                    field: "unsupported_arrow_physical_type",
                })?;
            DataType::Decimal128(*precision, scale)
        }
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::JsonUtf8 => DataType::Utf8,
        ArrowPhysicalTypeV1::Date32 => DataType::Date32,
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => {
            DataType::Timestamp(TimeUnit::Nanosecond, timezone.clone().map(Into::into))
        }
        ArrowPhysicalTypeV1::DictionaryUtf8 { key_type, .. } => DataType::Dictionary(
            Box::new(dictionary_key_data_type(key_type)),
            Box::new(DataType::Utf8),
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
    let mut values = schema
        .columns
        .iter()
        .filter(|column| column.semantic_role == RelationSemanticRoleV1::Value);
    let Some(column) = values.next() else {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "relation catalog must define one value column".to_string(),
        });
    };
    if values.next().is_some() {
        return Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: "prototype adapter supports exactly one value column".to_string(),
        });
    }

    Ok(column)
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
    Int64(&'a Int64Array),
    Date32(&'a Date32Array),
    TimestampNanosecond(&'a TimestampNanosecondArray),
    DictionaryUtf8Int8(&'a DictionaryArray<Int8Type>, &'a StringArray),
    DictionaryUtf8Int16(&'a DictionaryArray<Int16Type>, &'a StringArray),
    DictionaryUtf8Int32(&'a DictionaryArray<Int32Type>, &'a StringArray),
    DictionaryUtf8Int64(&'a DictionaryArray<Int64Type>, &'a StringArray),
}

impl IncrementalKeyColumn<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Boolean(column) => column.is_null(row),
            Self::Utf8(column) => column.is_null(row),
            Self::Int64(column) => column.is_null(row),
            Self::Date32(column) => column.is_null(row),
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

    fn delta_key(&self, row: usize) -> DeltaKey {
        match self {
            Self::Boolean(column) => DeltaKey::from_json(json!(column.value(row))),
            Self::Utf8(column) => DeltaKey::from_json(json!(column.value(row))),
            Self::Int64(column) => DeltaKey::from_json(json!(column.value(row))),
            Self::Date32(column) => DeltaKey::from_json(json!(column.value(row))),
            Self::TimestampNanosecond(column) => DeltaKey::from_json(json!(column.value(row))),
            Self::DictionaryUtf8Int8(column, values) => {
                DeltaKey::from_json(json!(dictionary_utf8_value(column, values, row)))
            }
            Self::DictionaryUtf8Int16(column, values) => {
                DeltaKey::from_json(json!(dictionary_utf8_value(column, values, row)))
            }
            Self::DictionaryUtf8Int32(column, values) => {
                DeltaKey::from_json(json!(dictionary_utf8_value(column, values, row)))
            }
            Self::DictionaryUtf8Int64(column, values) => {
                DeltaKey::from_json(json!(dictionary_utf8_value(column, values, row)))
            }
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
        ArrowPhysicalTypeV1::Utf8 => {
            string_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Utf8)
        }
        ArrowPhysicalTypeV1::Int64 => {
            int64_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Int64)
        }
        ArrowPhysicalTypeV1::Date32 => {
            date32_column(batch, column.name.as_str()).map(IncrementalKeyColumn::Date32)
        }
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            timestamp_nanosecond_column(batch, column.name.as_str())
                .map(IncrementalKeyColumn::TimestampNanosecond)
        }
        ArrowPhysicalTypeV1::DictionaryUtf8 { key_type, .. } => {
            dictionary_utf8_column(batch, column.name.as_str(), key_type)
        }
        _ => Err(IncrementalInputAdapterError::MalformedArrowInput {
            reason: format!(
                "prototype adapter key column `{}` must be Boolean, Utf8, Int64, Date32, TimestampNanosecond, or DictionaryUtf8",
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

fn relation_datafusion_error(error: RelationSchemaError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn validate_logical_physical_type_pair(
    logical_type: &VelorixLogicalTypeV1,
    physical_type: &ArrowPhysicalTypeV1,
) -> Result<(), RelationSchemaError> {
    let matches = match (logical_type, physical_type) {
        (VelorixLogicalTypeV1::Bool, ArrowPhysicalTypeV1::Boolean) => true,
        (VelorixLogicalTypeV1::Int64, ArrowPhysicalTypeV1::Int64) => true,
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
        (VelorixLogicalTypeV1::Date, ArrowPhysicalTypeV1::Date32) => true,
        (
            VelorixLogicalTypeV1::Timestamp {
                timezone: logical_timezone,
            },
            ArrowPhysicalTypeV1::TimestampNanosecond {
                timezone: physical_timezone,
            },
        ) => logical_timezone == physical_timezone,
        (VelorixLogicalTypeV1::Json, ArrowPhysicalTypeV1::JsonUtf8) => true,
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

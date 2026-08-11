use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::relation::{
    RelationColumnV1, RelationSchemaError, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
};

pub const SPEC_HASH_PREFIX: &str = "velorix-view-spec-sha256-v1";
pub const PUBLISHED_RELATION_BINDING_SCHEMA_VERSION_V1: u32 = 1;
pub const PUBLISHED_RELATION_DELTA_CODEC_V1: &str = "velorix-published-relation-delta-v1";
pub const PUBLISHED_RELATION_FRONTIER_KIND_V1: &str = "producer_commit_epoch";
/// Tagged input binding for view admission.
///
/// Distinguishes between physical source relations and upstream materialized
/// view outputs. This is critical for:
/// - Trust boundary enforcement (source vs. view inputs)
/// - Graph mutation CAS (view edges require cycle detection)
/// - Checkpoint identity (dependency binding digest)
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BoundInputV1 {
    /// Input from a physical ingest source relation.
    Source(SourceInputBindingV1),
    /// Input from an upstream materialized view output.
    View(ViewDependencyEdgeBindingV1),
}

/// A view admission input resolved to a concrete authority domain.
///
/// Admission resolves each requested input to either a physical source
/// (with its registered catalog) or a published view output (with its
/// producer binding and admitted dependency edge). The input kind is
/// explicit: the resolver must NOT fall back between source and view based
/// on relation ID/version availability, because a physical source and a
/// published view output can share the same relation identity.
#[derive(Clone, Debug)]
pub enum ResolvedAdmissionInput {
    /// Input from a registered physical ingest source.
    Source {
        catalog: VelorixRelationCatalogV1,
        relation: RelationSchema,
        binding: SourceInputBindingV1,
    },
    /// Input from an upstream materialized view output.
    View {
        relation: RelationSchema,
        /// The producer's published binding, used only for admission-time
        /// verification. Persist `edge`, not this full object.
        published: PublishedRelationBindingV1,
        /// The immutable dependency edge this input is bound to.
        edge: ViewDependencyEdgeBindingV1,
    },
}

impl ResolvedAdmissionInput {
    /// Returns the relation schema the consumer planner resolves against.
    pub fn relation(&self) -> &RelationSchema {
        match self {
            ResolvedAdmissionInput::Source { relation, .. }
            | ResolvedAdmissionInput::View { relation, .. } => relation,
        }
    }
}

/// Binding for a physical source relation input.
///
/// Wraps the existing source relation identity fields used throughout
/// the ingest pipeline.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceInputBindingV1 {
    pub relation_id: String,
    pub relation_version: String,
    pub relation_generation: u64,
    pub schema_fingerprint: String,
}

/// Binding for a view-to-view dependency edge.
///
/// Contains all information needed to:
/// - Resolve the producer view's published output
/// - Validate schema/key/codec consistency
/// - Track the dependency in the graph revision
/// - Verify the authority chain during checkpoint/restore
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewDependencyEdgeBindingV1 {
    /// Unique identifier for this edge within the dependency graph.
    pub input_edge_id: String,
    /// Graph revision under which this edge was admitted.
    pub graph_revision: u64,
    /// Producer view's tenant ID.
    pub producer_tenant_id: String,
    /// Producer view's program ID.
    pub producer_program_id: String,
    /// Producer view's view ID.
    pub producer_view_id: String,
    /// Producer view's generation at admission time.
    pub producer_generation: u64,
    /// Producer view's logical plan hash.
    pub producer_plan_hash: String,
    /// Output schema hash from the producer's PublishedRelationBindingV1.
    pub output_schema_hash: String,
    /// Key descriptor hash from the producer's PublishedRelationBindingV1.
    pub key_descriptor_hash: String,
    /// Output stream ID from the producer's PublishedRelationBindingV1.
    pub output_stream_id: String,
    /// Delta codec identity from the producer's PublishedRelationBindingV1.
    pub delta_codec_identity: String,
    /// Frontier kind from the producer's PublishedRelationBindingV1.
    pub frontier_kind: String,
}

/// Canonical hash of a dependency edge binding set.
///
/// Computed over the sorted set of `ViewDependencyEdgeBindingV1` entries,
/// ensuring that any change to the dependency graph produces a different
/// program identity.
pub const DEPENDENCY_BINDING_DIGEST_PREFIX: &str = "velorix-dependency-binding-sha256-v1";

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

/// Immutable identity for consuming a materialized output as a typed relation.
///
/// The public relation schema never contains a physical delta-weight column. The
/// internal delta codec named here carries signed bag weights separately.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedRelationBindingV1 {
    pub schema_version: u32,
    pub producer_view_id: String,
    pub producer_view_generation: u64,
    pub producer_plan_hash: String,
    pub relation: RelationSchema,
    pub output_schema_hash: String,
    pub key_descriptor_hash: String,
    pub output_stream_id: String,
    pub delta_codec_identity: String,
    pub frontier_kind: String,
    /// Graph revision under which this binding was admitted.
    /// 0 for direct source inputs (no dependency graph).
    pub graph_revision: u64,
    /// Canonical digest of the dependency edge binding set.
    /// Empty for direct source inputs.
    pub dependency_binding_digest: String,
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
    #[error("dependency edge binding mismatch: {field}")]
    DependencyEdgeMismatch { field: &'static str },
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

pub fn published_relation_binding_v1(
    producer_view_id: &str,
    producer_view_generation: u64,
    producer_plan_hash: &str,
    relation: &RelationSchema,
) -> Result<PublishedRelationBindingV1, ViewContractError> {
    require_non_empty("published_relation.producer_view_id", producer_view_id)?;
    require_non_empty("published_relation.producer_plan_hash", producer_plan_hash)?;
    if producer_view_generation == 0 {
        return Err(ViewContractError::InvalidField {
            field: "published_relation.producer_view_generation",
        });
    }
    validate_relation_schema(relation)?;
    let output_schema_hash = stable_serialized_hash(relation, "published relation output schema")?;
    let key_descriptor_hash =
        stable_serialized_hash(&relation.primary_key, "published relation key descriptor")?;
    let binding = PublishedRelationBindingV1 {
        schema_version: PUBLISHED_RELATION_BINDING_SCHEMA_VERSION_V1,
        producer_view_id: producer_view_id.to_string(),
        producer_view_generation,
        producer_plan_hash: producer_plan_hash.to_string(),
        relation: relation.clone(),
        output_schema_hash,
        key_descriptor_hash,
        output_stream_id: format!(
            "view/{producer_view_id}/generation/{producer_view_generation}/output/{}",
            relation.relation_id
        ),
        delta_codec_identity: PUBLISHED_RELATION_DELTA_CODEC_V1.to_string(),
        frontier_kind: PUBLISHED_RELATION_FRONTIER_KIND_V1.to_string(),
        graph_revision: 0,
        dependency_binding_digest: String::new(),
    };
    validate_published_relation_binding_v1(&binding)?;
    Ok(binding)
}

pub fn validate_published_relation_binding_v1(
    binding: &PublishedRelationBindingV1,
) -> Result<(), ViewContractError> {
    if binding.schema_version != PUBLISHED_RELATION_BINDING_SCHEMA_VERSION_V1
        || binding.producer_view_generation == 0
    {
        return Err(ViewContractError::InvalidField {
            field: "published_relation",
        });
    }
    require_non_empty(
        "published_relation.producer_view_id",
        &binding.producer_view_id,
    )?;
    require_non_empty(
        "published_relation.producer_plan_hash",
        &binding.producer_plan_hash,
    )?;
    validate_relation_schema(&binding.relation)?;
    let expected_schema_hash =
        stable_serialized_hash(&binding.relation, "published relation output schema")?;
    let expected_key_hash = stable_serialized_hash(
        &binding.relation.primary_key,
        "published relation key descriptor",
    )?;
    let expected_stream_id = format!(
        "view/{}/generation/{}/output/{}",
        binding.producer_view_id, binding.producer_view_generation, binding.relation.relation_id
    );
    if binding.output_schema_hash != expected_schema_hash {
        return Err(ViewContractError::InvalidField {
            field: "published_relation.output_schema_hash",
        });
    }
    if binding.key_descriptor_hash != expected_key_hash {
        return Err(ViewContractError::InvalidField {
            field: "published_relation.key_descriptor_hash",
        });
    }
    if binding.output_stream_id != expected_stream_id {
        return Err(ViewContractError::InvalidField {
            field: "published_relation.output_stream_id",
        });
    }
    if binding.delta_codec_identity != PUBLISHED_RELATION_DELTA_CODEC_V1 {
        return Err(ViewContractError::InvalidField {
            field: "published_relation.delta_codec_identity",
        });
    }
    if binding.frontier_kind != PUBLISHED_RELATION_FRONTIER_KIND_V1 {
        return Err(ViewContractError::InvalidField {
            field: "published_relation.frontier_kind",
        });
    }
    Ok(())
}

/// Resolve a consumer view input relation from the producer's published binding.
///
/// This is the authoritative source for a consumer view input schema. It verifies
/// every identity field of the admitted dependency edge against the producer's
/// `PublishedRelationBindingV1` with exact matching, so a stale generation,
/// cross-tenant producer, changed key descriptor, or mismatched stream/codec is
/// rejected before the consumer planner/runtime can bind to it.
///
/// On success, returns `published.relation` which the consumer must use as its
/// input schema. The consumer must NOT re-resolve the schema from a live catalog
/// or producer runtime later.
pub fn resolve_view_input_relation_v1(
    edge: &ViewDependencyEdgeBindingV1,
    producer_tenant_id: &str,
    producer_program_id: &str,
    published: &PublishedRelationBindingV1,
) -> Result<RelationSchema, ViewContractError> {
    validate_published_relation_binding_v1(published)?;

    if edge.producer_tenant_id != producer_tenant_id {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "producer_tenant_id",
        });
    }
    if edge.producer_program_id != producer_program_id {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "producer_program_id",
        });
    }
    if edge.producer_view_id != published.producer_view_id {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "producer_view_id",
        });
    }
    if edge.producer_generation != published.producer_view_generation {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "producer_generation",
        });
    }
    if edge.producer_plan_hash != published.producer_plan_hash {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "producer_plan_hash",
        });
    }
    if edge.output_schema_hash != published.output_schema_hash {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "output_schema_hash",
        });
    }
    if edge.key_descriptor_hash != published.key_descriptor_hash {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "key_descriptor_hash",
        });
    }
    if edge.output_stream_id != published.output_stream_id {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "output_stream_id",
        });
    }
    if edge.delta_codec_identity != published.delta_codec_identity {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "delta_codec_identity",
        });
    }
    if edge.frontier_kind != published.frontier_kind {
        return Err(ViewContractError::DependencyEdgeMismatch {
            field: "frontier_kind",
        });
    }

    Ok(published.relation.clone())
}

fn stable_serialized_hash<T: Serialize>(
    value: &T,
    description: &str,
) -> Result<String, ViewContractError> {
    let bytes = serde_json::to_vec(value).map_err(|source| ViewContractError::Serialization {
        reason: format!("could not serialize {description}: {source}"),
    })?;
    Ok(stable_bytes_hash(&bytes))
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

    #[test]
    fn published_relation_binding_fences_schema_key_generation_and_frontier() {
        let relation = RelationSchema {
            relation_id: "orders_by_region".to_string(),
            relation_name: "orders_by_region".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
            columns: vec![ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            }],
            primary_key: vec!["region".to_string()],
        };

        let binding = published_relation_binding_v1(
            "orders_by_region",
            7,
            "velorix-logical-view-plan-sha256-v1:plan",
            &relation,
        )
        .unwrap();

        assert_eq!(binding.relation, relation);
        assert_eq!(binding.producer_view_generation, 7);
        assert_eq!(
            binding.output_stream_id,
            "view/orders_by_region/generation/7/output/orders_by_region"
        );
        assert_eq!(
            binding.delta_codec_identity,
            PUBLISHED_RELATION_DELTA_CODEC_V1
        );
        assert_eq!(binding.frontier_kind, PUBLISHED_RELATION_FRONTIER_KIND_V1);
        validate_published_relation_binding_v1(&binding).unwrap();

        let mut stale_key = binding.clone();
        stale_key.key_descriptor_hash = format!("sha256:{}", "0".repeat(64));
        assert_eq!(
            validate_published_relation_binding_v1(&stale_key),
            Err(ViewContractError::InvalidField {
                field: "published_relation.key_descriptor_hash"
            })
        );

        let mut wrong_generation_stream = binding;
        wrong_generation_stream.producer_view_generation += 1;
        assert_eq!(
            validate_published_relation_binding_v1(&wrong_generation_stream),
            Err(ViewContractError::InvalidField {
                field: "published_relation.output_stream_id"
            })
        );
    }

    #[test]
    fn resolve_view_input_relation_matches_edge_to_published_binding() {
        let relation = RelationSchema {
            relation_id: "orders_by_region".to_string(),
            relation_name: "orders_by_region".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
            columns: vec![ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            }],
            primary_key: vec!["region".to_string()],
        };
        let published = published_relation_binding_v1(
            "orders_by_region",
            7,
            "velorix-logical-view-plan-sha256-v1:plan",
            &relation,
        )
        .unwrap();

        let edge = ViewDependencyEdgeBindingV1 {
            input_edge_id: "edge-1".to_string(),
            graph_revision: 4,
            producer_tenant_id: "tenant-a".to_string(),
            producer_program_id: "program-a".to_string(),
            producer_view_id: published.producer_view_id.clone(),
            producer_generation: published.producer_view_generation,
            producer_plan_hash: published.producer_plan_hash.clone(),
            output_schema_hash: published.output_schema_hash.clone(),
            key_descriptor_hash: published.key_descriptor_hash.clone(),
            output_stream_id: published.output_stream_id.clone(),
            delta_codec_identity: published.delta_codec_identity.clone(),
            frontier_kind: published.frontier_kind.clone(),
        };

        // Exact match resolves to the published relation schema.
        let resolved =
            resolve_view_input_relation_v1(&edge, "tenant-a", "program-a", &published).unwrap();
        assert_eq!(resolved, relation);

        // Cross-tenant producer is rejected.
        assert_eq!(
            resolve_view_input_relation_v1(&edge, "tenant-b", "program-a", &published),
            Err(ViewContractError::DependencyEdgeMismatch {
                field: "producer_tenant_id"
            })
        );

        // Cross-program producer is rejected.
        assert_eq!(
            resolve_view_input_relation_v1(&edge, "tenant-a", "program-b", &published),
            Err(ViewContractError::DependencyEdgeMismatch {
                field: "producer_program_id"
            })
        );

        // Stale producer generation is rejected before schema binding.
        let mut stale_edge = edge.clone();
        stale_edge.producer_generation = published.producer_view_generation + 1;
        assert_eq!(
            resolve_view_input_relation_v1(&stale_edge, "tenant-a", "program-a", &published),
            Err(ViewContractError::DependencyEdgeMismatch {
                field: "producer_generation"
            })
        );

        // Changed key descriptor is rejected.
        let mut changed_key = edge.clone();
        changed_key.key_descriptor_hash = format!("sha256:{}", "0".repeat(64));
        assert_eq!(
            resolve_view_input_relation_v1(&changed_key, "tenant-a", "program-a", &published),
            Err(ViewContractError::DependencyEdgeMismatch {
                field: "key_descriptor_hash"
            })
        );

        // Mismatched output stream is rejected.
        let mut changed_stream = edge;
        changed_stream.output_stream_id = "view/other/generation/1/output/other".to_string();
        assert_eq!(
            resolve_view_input_relation_v1(&changed_stream, "tenant-a", "program-a", &published),
            Err(ViewContractError::DependencyEdgeMismatch {
                field: "output_stream_id"
            })
        );
    }
}

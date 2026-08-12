use crate::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
    RelationOperationV1, RelationSchemaError, RelationSemanticRoleV1, SchemaFingerprintV1,
    VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
    VelorixRelationSourceV1, CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
};
use crate::standing_program::CausalViewCursorV1;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use thiserror::Error;
pub const SPEC_HASH_PREFIX: &str = "velorix-view-spec-sha256-v1";
pub const PUBLISHED_RELATION_BINDING_SCHEMA_VERSION_V1: u32 = 1;
pub const PUBLISHED_RELATION_DELTA_CODEC_V1: &str = "velorix-published-relation-delta-v1";
pub const PUBLISHED_RELATION_FRONTIER_KIND_V1: &str = "producer_commit_epoch";
pub const PUBLISHED_DELTA_WEIGHT_FIELD_V1: &str = "__velorix_internal_weight_v1";
pub const STANDING_INPUT_BINDING_SCHEMA_VERSION_V1: u32 = 1;
pub const VIEW_DEPENDENCY_EDGE_SCHEMA_VERSION_V1: u32 = 1;
pub const VIEW_DEPENDENCY_EDGE_ID_DOMAIN_V1: &str = "velorix-view-dependency-edge-v1";
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
}
/// Durable description of one input edge of a materialized view: either a
/// direct source relation or the published output of a producer view.
///
/// A view input is bound to an immutable producer generation at admission
/// time. The public relation schema never contains a physical delta-weight
/// column; signed bag weights travel through the internal published-delta
/// Arrow encoding instead.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum StandingInputBindingV1 {
    Source {
        relation: RelationSchema,
        relation_generation: u64,
    },
    PublishedView {
        edge_id: String,
        producer_tenant_id: String,
        producer_program_id: String,
        published_relation: PublishedRelationBindingV1,
        graph_revision: u64,
        bootstrap_cursor: CausalViewCursorV1,
    },
}
/// Durable view-on-view dependency edge, persisted alongside the consumer's
/// active view record. Direction is consumer -> producer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewDependencyEdgeV1 {
    pub schema_version: u32,
    pub edge_id: String,
    pub tenant_id: String,
    pub consumer_program_id: String,
    pub consumer_view_id: String,
    pub consumer_generation: u64,
    pub input_relation_id: String,
    pub input_relation_version: String,
    pub producer_program_id: String,
    pub producer_view_id: String,
    pub producer_generation: u64,
    pub producer_plan_hash: String,
    pub output_stream_id: String,
    pub output_schema_hash: String,
    pub key_descriptor_hash: String,
    pub delta_codec_identity: String,
    pub frontier_kind: String,
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
            .filter(|column| {
                // The internal weight column of a published-view-output
                // descriptor is an encoding artifact, not part of the public
                // relation schema.
                !(matches!(
                    catalog.relation_source,
                    VelorixRelationSourceV1::PublishedViewOutput { .. }
                ) && column.column_id == catalog.relation_schema.weight_column_id)
            })
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
/// Synthesizes the runtime planning descriptor for a published-view-output
/// input relation. The descriptor mirrors the producer's signed binding: the
/// public columns in binding order plus exactly one private Int64 weight
/// column carrying signed bag weights. The declared schema fingerprint is the
/// producer's signed output fingerprint, not a recomputation over the
/// descriptor schema.
///
/// The descriptor is a runtime-only artifact: it is never registered in the
/// relation registry and must never be a target of external ingest.
pub fn catalog_from_published_relation_binding(
    binding: &PublishedRelationBindingV1,
) -> Result<VelorixRelationCatalogV1, ViewContractError> {
    validate_published_relation_binding_v1(binding)?;
    for column in &binding.relation.columns {
        if column.name == PUBLISHED_DELTA_WEIGHT_FIELD_V1 {
            return Err(ViewContractError::InvalidField {
                field: "published_relation.column.name",
            });
        }
    }
    let mut columns = Vec::with_capacity(binding.relation.columns.len() + 1);
    let primary_key = binding.relation.primary_key.iter().collect::<BTreeSet<_>>();
    for (ordinal, column) in binding.relation.columns.iter().enumerate() {
        let (logical_type, physical_arrow_type) =
            published_column_type(&column.data_type).ok_or(ViewContractError::InvalidField {
                field: "published_relation.column.data_type",
            })?;
        let semantic_role = if primary_key.contains(&column.name) {
            RelationSemanticRoleV1::PrimaryKey
        } else {
            RelationSemanticRoleV1::Value
        };
        columns.push(RelationColumnV1 {
            column_id: column.name.clone(),
            name: column.name.clone(),
            logical_type,
            physical_arrow_type,
            nullable: column.nullable,
            ordinal: u32::try_from(ordinal).map_err(|_| ViewContractError::InvalidField {
                field: "published_relation.column.ordinal",
            })?,
            semantic_role,
        });
    }
    columns.push(RelationColumnV1 {
        column_id: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
        name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: false,
        ordinal: u32::try_from(binding.relation.columns.len()).map_err(|_| {
            ViewContractError::InvalidField {
                field: "published_relation.weight_column.ordinal",
            }
        })?,
        semantic_role: RelationSemanticRoleV1::Weight,
    });
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: binding.relation.relation_id.clone(),
        relation_name: binding.relation.relation_name.clone(),
        relation_version: binding.relation.relation_version.clone(),
        columns,
        primary_key_column_ids: binding.relation.primary_key.clone(),
        weight_column_id: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    relation_schema
        .validate()
        .map_err(|_| ViewContractError::InvalidField {
            field: "published_relation.catalog_schema",
        })?;
    let schema_fingerprint = SchemaFingerprintV1::new(binding.relation.schema_fingerprint.clone());
    schema_fingerprint
        .validate("published_relation.schema_fingerprint")
        .map_err(|_| ViewContractError::InvalidField {
            field: "published_relation.schema_fingerprint",
        })?;
    let catalog = VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: binding.relation.relation_name.clone(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: binding.relation.relation_id.clone(),
            schema_fingerprint: schema_fingerprint.clone(),
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
        relation_source: VelorixRelationSourceV1::PublishedViewOutput {
            producer_view_id: binding.producer_view_id.clone(),
            producer_view_generation: binding.producer_view_generation,
            output_stream_id: binding.output_stream_id.clone(),
        },
    };
    catalog
        .validate_ingest_adapter_scope()
        .map_err(|_| ViewContractError::InvalidField {
            field: "published_relation.catalog_adapter",
        })?;
    Ok(catalog)
}
fn published_column_type(
    data_type: &SqlDataType,
) -> Option<(VelorixLogicalTypeV1, ArrowPhysicalTypeV1)> {
    match data_type {
        SqlDataType::Bool => Some((VelorixLogicalTypeV1::Bool, ArrowPhysicalTypeV1::Boolean)),
        SqlDataType::Int8 => Some((VelorixLogicalTypeV1::Int8, ArrowPhysicalTypeV1::Int8)),
        SqlDataType::Int16 => Some((VelorixLogicalTypeV1::Int16, ArrowPhysicalTypeV1::Int16)),
        SqlDataType::Int32 => Some((VelorixLogicalTypeV1::Int32, ArrowPhysicalTypeV1::Int32)),
        SqlDataType::Int64 => Some((VelorixLogicalTypeV1::Int64, ArrowPhysicalTypeV1::Int64)),
        SqlDataType::UInt8 => Some((VelorixLogicalTypeV1::UInt8, ArrowPhysicalTypeV1::UInt8)),
        SqlDataType::UInt16 => Some((VelorixLogicalTypeV1::UInt16, ArrowPhysicalTypeV1::UInt16)),
        SqlDataType::UInt32 => Some((VelorixLogicalTypeV1::UInt32, ArrowPhysicalTypeV1::UInt32)),
        SqlDataType::UInt64 => Some((VelorixLogicalTypeV1::UInt64, ArrowPhysicalTypeV1::UInt64)),
        SqlDataType::Float32 => Some((VelorixLogicalTypeV1::Float32, ArrowPhysicalTypeV1::Float32)),
        SqlDataType::Float64 => Some((VelorixLogicalTypeV1::Float64, ArrowPhysicalTypeV1::Float64)),
        SqlDataType::Decimal { precision, scale } => Some((
            VelorixLogicalTypeV1::Decimal {
                precision: *precision,
                scale: *scale,
            },
            ArrowPhysicalTypeV1::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
        )),
        SqlDataType::Char { length } => Some((
            VelorixLogicalTypeV1::Char { length: *length },
            ArrowPhysicalTypeV1::Utf8,
        )),
        SqlDataType::Utf8 | SqlDataType::Uuid | SqlDataType::Json | SqlDataType::Geometry => {
            Some((VelorixLogicalTypeV1::Utf8, ArrowPhysicalTypeV1::Utf8))
        }
        SqlDataType::Binary { length } => Some((
            VelorixLogicalTypeV1::Binary { length: *length },
            ArrowPhysicalTypeV1::Binary,
        )),
        SqlDataType::Varbinary => {
            Some((VelorixLogicalTypeV1::Varbinary, ArrowPhysicalTypeV1::Binary))
        }
        SqlDataType::Date => Some((VelorixLogicalTypeV1::Date, ArrowPhysicalTypeV1::Date32)),
        SqlDataType::Time => Some((
            VelorixLogicalTypeV1::Time,
            ArrowPhysicalTypeV1::Time64Nanosecond,
        )),
        SqlDataType::Timestamp { timezone } => Some((
            VelorixLogicalTypeV1::Timestamp {
                timezone: timezone.clone(),
            },
            ArrowPhysicalTypeV1::TimestampNanosecond {
                timezone: timezone.clone(),
            },
        )),
        SqlDataType::Interval { .. }
        | SqlDataType::Null
        | SqlDataType::Array { .. }
        | SqlDataType::Struct { .. }
        | SqlDataType::Map { .. } => None,
    }
}
impl StandingInputBindingV1 {
    pub fn validate(&self) -> Result<(), ViewContractError> {
        match self {
            StandingInputBindingV1::Source {
                relation,
                relation_generation,
            } => {
                validate_relation_schema(relation)?;
                if *relation_generation == 0 {
                    return Err(ViewContractError::InvalidField {
                        field: "input_binding.source.relation_generation",
                    });
                }
                Ok(())
            }
            StandingInputBindingV1::PublishedView {
                edge_id,
                producer_tenant_id,
                producer_program_id,
                published_relation,
                graph_revision,
                bootstrap_cursor,
            } => {
                require_non_empty("input_binding.published_view.edge_id", edge_id)?;
                require_non_empty(
                    "input_binding.published_view.producer_tenant_id",
                    producer_tenant_id,
                )?;
                require_non_empty(
                    "input_binding.published_view.producer_program_id",
                    producer_program_id,
                )?;
                validate_published_relation_binding_v1(published_relation)?;
                if *graph_revision == 0 {
                    return Err(ViewContractError::InvalidField {
                        field: "input_binding.published_view.graph_revision",
                    });
                }
                // The bootstrap cursor must be bound to the same producer
                // scope as the binding: edge, tenant, program, view,
                // generation, and output stream. A format-valid cursor that
                // points at a different producer would let a consumer read
                // the wrong commit lineage.
                if bootstrap_cursor.input_edge != *edge_id
                    || bootstrap_cursor.producer_tenant_id != *producer_tenant_id
                    || bootstrap_cursor.producer_program_id != *producer_program_id
                    || bootstrap_cursor.producer_view_id != published_relation.producer_view_id
                    || bootstrap_cursor.producer_generation
                        != published_relation.producer_view_generation
                    || bootstrap_cursor.output_stream != published_relation.output_stream_id
                {
                    return Err(ViewContractError::InvalidField {
                        field: "input_binding.published_view.bootstrap_cursor",
                    });
                }
                bootstrap_cursor
                    .validate()
                    .map_err(|_| ViewContractError::InvalidField {
                        field: "input_binding.published_view.bootstrap_cursor",
                    })?;
                let expected_edge_id = view_dependency_edge_id(
                    producer_tenant_id,
                    producer_program_id,
                    published_relation,
                )
                .map_err(|_| ViewContractError::InvalidField {
                    field: "input_binding.published_view.edge_id",
                })?;
                if expected_edge_id != *edge_id {
                    return Err(ViewContractError::InvalidField {
                        field: "input_binding.published_view.edge_id",
                    });
                }
                Ok(())
            }
        }
    }
    /// Canonical identity hash over the full binding: producer scope,
    /// generation, plan hash, output stream, schema hash, key hash, codec,
    /// and frontier kind. View inputs must never be identified by the public
    /// schema fingerprint alone.
    pub fn input_catalog_hash(&self) -> Result<String, ViewContractError> {
        match self {
            StandingInputBindingV1::Source { relation, .. } => {
                require_non_empty(
                    "input_binding.schema_fingerprint",
                    &relation.schema_fingerprint,
                )?;
                Ok(relation.schema_fingerprint.clone())
            }
            StandingInputBindingV1::PublishedView {
                producer_tenant_id,
                producer_program_id,
                published_relation: binding,
                ..
            } => {
                #[derive(Serialize)]
                struct BindingIdentity<'a> {
                    domain: &'static str,
                    kind: &'static str,
                    schema_version: u32,
                    producer_tenant_id: &'a str,
                    producer_program_id: &'a str,
                    producer_view_id: &'a str,
                    producer_view_generation: u64,
                    producer_plan_hash: &'a str,
                    output_stream_id: &'a str,
                    output_schema_hash: &'a str,
                    key_descriptor_hash: &'a str,
                    delta_codec_identity: &'a str,
                    frontier_kind: &'a str,
                    schema_fingerprint: &'a str,
                }
                let identity = BindingIdentity {
                    domain: "velorix-standing-input-binding-v1",
                    kind: "published_view",
                    schema_version: STANDING_INPUT_BINDING_SCHEMA_VERSION_V1,
                    producer_tenant_id,
                    producer_program_id,
                    producer_view_id: &binding.producer_view_id,
                    producer_view_generation: binding.producer_view_generation,
                    producer_plan_hash: &binding.producer_plan_hash,
                    output_stream_id: &binding.output_stream_id,
                    output_schema_hash: &binding.output_schema_hash,
                    key_descriptor_hash: &binding.key_descriptor_hash,
                    delta_codec_identity: &binding.delta_codec_identity,
                    frontier_kind: &binding.frontier_kind,
                    schema_fingerprint: &binding.relation.schema_fingerprint,
                };
                let bytes = serde_json::to_vec(&identity).map_err(|source| {
                    ViewContractError::Serialization {
                        reason: format!("could not serialize input binding identity: {source}"),
                    }
                })?;
                Ok(stable_bytes_hash(&bytes))
            }
        }
    }
}
/// Canonical domain-separated edge id for a view-on-view dependency edge.
pub fn view_dependency_edge_id(
    producer_tenant_id: &str,
    producer_program_id: &str,
    binding: &PublishedRelationBindingV1,
) -> Result<String, ViewContractError> {
    require_non_empty("edge.producer_tenant_id", producer_tenant_id)?;
    require_non_empty("edge.producer_program_id", producer_program_id)?;
    validate_published_relation_binding_v1(binding)?;
    #[derive(Serialize)]
    struct EdgeIdentity<'a> {
        domain: &'static str,
        schema_version: u32,
        producer_tenant_id: &'a str,
        producer_program_id: &'a str,
        producer_view_id: &'a str,
        producer_generation: u64,
        producer_plan_hash: &'a str,
        output_stream_id: &'a str,
        output_schema_hash: &'a str,
        key_descriptor_hash: &'a str,
        delta_codec_identity: &'a str,
        frontier_kind: &'a str,
        schema_fingerprint: &'a str,
    }
    let identity = EdgeIdentity {
        domain: VIEW_DEPENDENCY_EDGE_ID_DOMAIN_V1,
        schema_version: VIEW_DEPENDENCY_EDGE_SCHEMA_VERSION_V1,
        producer_tenant_id,
        producer_program_id,
        producer_view_id: &binding.producer_view_id,
        producer_generation: binding.producer_view_generation,
        producer_plan_hash: &binding.producer_plan_hash,
        output_stream_id: &binding.output_stream_id,
        output_schema_hash: &binding.output_schema_hash,
        key_descriptor_hash: &binding.key_descriptor_hash,
        delta_codec_identity: &binding.delta_codec_identity,
        frontier_kind: &binding.frontier_kind,
        schema_fingerprint: &binding.relation.schema_fingerprint,
    };
    let bytes =
        serde_json::to_vec(&identity).map_err(|source| ViewContractError::Serialization {
            reason: format!("could not serialize view dependency edge identity: {source}"),
        })?;
    Ok(stable_bytes_hash(&bytes))
}
#[allow(clippy::too_many_arguments)]
pub fn view_dependency_edge_from_binding(
    tenant_id: &str,
    consumer_program_id: &str,
    consumer_view_id: &str,
    consumer_generation: u64,
    input_relation_id: &str,
    input_relation_version: &str,
    producer_program_id: &str,
    binding: &PublishedRelationBindingV1,
) -> Result<ViewDependencyEdgeV1, ViewContractError> {
    let edge_id = view_dependency_edge_id(tenant_id, producer_program_id, binding)?;
    let edge = ViewDependencyEdgeV1 {
        schema_version: VIEW_DEPENDENCY_EDGE_SCHEMA_VERSION_V1,
        edge_id,
        tenant_id: tenant_id.to_string(),
        consumer_program_id: consumer_program_id.to_string(),
        consumer_view_id: consumer_view_id.to_string(),
        consumer_generation,
        input_relation_id: input_relation_id.to_string(),
        input_relation_version: input_relation_version.to_string(),
        producer_program_id: producer_program_id.to_string(),
        producer_view_id: binding.producer_view_id.clone(),
        producer_generation: binding.producer_view_generation,
        producer_plan_hash: binding.producer_plan_hash.clone(),
        output_stream_id: binding.output_stream_id.clone(),
        output_schema_hash: binding.output_schema_hash.clone(),
        key_descriptor_hash: binding.key_descriptor_hash.clone(),
        delta_codec_identity: binding.delta_codec_identity.clone(),
        frontier_kind: binding.frontier_kind.clone(),
    };
    validate_view_dependency_edge(&edge)?;
    Ok(edge)
}
pub fn validate_view_dependency_edge(edge: &ViewDependencyEdgeV1) -> Result<(), ViewContractError> {
    if edge.schema_version != VIEW_DEPENDENCY_EDGE_SCHEMA_VERSION_V1
        || edge.consumer_generation == 0
    {
        return Err(ViewContractError::InvalidField {
            field: "dependency_edge",
        });
    }
    require_non_empty("dependency_edge.edge_id", &edge.edge_id)?;
    require_non_empty("dependency_edge.tenant_id", &edge.tenant_id)?;
    require_non_empty(
        "dependency_edge.consumer_program_id",
        &edge.consumer_program_id,
    )?;
    require_non_empty("dependency_edge.consumer_view_id", &edge.consumer_view_id)?;
    require_non_empty("dependency_edge.input_relation_id", &edge.input_relation_id)?;
    require_non_empty(
        "dependency_edge.input_relation_version",
        &edge.input_relation_version,
    )?;
    require_non_empty(
        "dependency_edge.producer_program_id",
        &edge.producer_program_id,
    )?;
    require_non_empty("dependency_edge.producer_view_id", &edge.producer_view_id)?;
    require_non_empty(
        "dependency_edge.producer_plan_hash",
        &edge.producer_plan_hash,
    )?;
    require_non_empty("dependency_edge.output_stream_id", &edge.output_stream_id)?;
    require_non_empty(
        "dependency_edge.output_schema_hash",
        &edge.output_schema_hash,
    )?;
    require_non_empty(
        "dependency_edge.key_descriptor_hash",
        &edge.key_descriptor_hash,
    )?;
    require_non_empty(
        "dependency_edge.delta_codec_identity",
        &edge.delta_codec_identity,
    )?;
    require_non_empty("dependency_edge.frontier_kind", &edge.frontier_kind)?;
    if edge.producer_view_id == edge.consumer_view_id
        && edge.producer_program_id == edge.consumer_program_id
    {
        return Err(ViewContractError::InvalidField {
            field: "dependency_edge.self_cycle",
        });
    }
    Ok(())
}
/// Validates that the dependency edges form an acyclic graph and returns the
/// producer-first topological ordering of consumer view ids.
///
/// Each edge points consumer -> producer. A cycle exists when following
/// producer links from a consumer reaches the consumer again (or itself).
pub fn validate_view_dependency_graph(
    edges: &[ViewDependencyEdgeV1],
) -> Result<Vec<String>, ViewContractError> {
    for edge in edges {
        validate_view_dependency_edge(edge)?;
    }
    let mut edges_by_consumer: std::collections::BTreeMap<&str, Vec<&ViewDependencyEdgeV1>> =
        std::collections::BTreeMap::new();
    for edge in edges {
        edges_by_consumer
            .entry(edge.consumer_view_id.as_str())
            .or_default()
            .push(edge);
    }
    let mut visited: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut on_stack: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut order: Vec<String> = Vec::new();
    let mut stack: Vec<&str> = edges
        .iter()
        .map(|edge| edge.consumer_view_id.as_str())
        .chain(edges.iter().map(|edge| edge.producer_view_id.as_str()))
        .collect();
    stack.sort_unstable();
    stack.dedup();
    fn visit<'a>(
        view_id: &'a str,
        edges_by_consumer: &std::collections::BTreeMap<&'a str, Vec<&'a ViewDependencyEdgeV1>>,
        visited: &mut std::collections::BTreeSet<String>,
        on_stack: &mut std::collections::BTreeSet<String>,
        order: &mut Vec<String>,
    ) -> Result<(), ViewContractError> {
        if on_stack.contains(view_id) {
            return Err(ViewContractError::InvalidField {
                field: "dependency_cycle",
            });
        }
        if !visited.insert(view_id.to_string()) {
            return Ok(());
        }
        on_stack.insert(view_id.to_string());
        if let Some(producers) = edges_by_consumer.get(view_id) {
            for edge in producers {
                visit(
                    edge.producer_view_id.as_str(),
                    edges_by_consumer,
                    visited,
                    on_stack,
                    order,
                )?;
            }
        }
        on_stack.remove(view_id);
        order.push(view_id.to_string());
        Ok(())
    }
    for view_id in stack {
        visit(
            view_id,
            &edges_by_consumer,
            &mut visited,
            &mut on_stack,
            &mut order,
        )?;
    }
    Ok(order)
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
    fn sample_binding() -> PublishedRelationBindingV1 {
        published_relation_binding_v1(
            "filtered_orders",
            7,
            "velorix-logical-view-plan-sha256-v1:plan",
            &RelationSchema {
                relation_id: "filtered_orders".to_string(),
                relation_name: "filtered_orders".to_string(),
                relation_version: "v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
                columns: vec![
                    ColumnSchema {
                        name: "region".to_string(),
                        data_type: SqlDataType::Utf8,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "total_amount".to_string(),
                        data_type: SqlDataType::Int64,
                        nullable: false,
                    },
                ],
                primary_key: vec!["region".to_string()],
            },
        )
        .unwrap()
    }
    fn sample_cursor() -> CausalViewCursorV1 {
        CausalViewCursorV1 {
            input_edge: view_dependency_edge_id("default", "filtered_orders", &sample_binding())
                .unwrap(),
            producer_tenant_id: "default".to_string(),
            producer_program_id: "filtered_orders".to_string(),
            producer_view_id: "filtered_orders".to_string(),
            producer_generation: 7,
            output_stream: "view/filtered_orders/generation/7/output/filtered_orders".to_string(),
            output_epoch: 41,
            commit_digest: format!("sha256:{}", "a".repeat(64)),
        }
    }
    #[test]
    fn published_view_input_binding_validation_is_generation_fenced() {
        let binding = sample_binding();
        let edge_id = view_dependency_edge_id("default", "filtered_orders", &binding).unwrap();
        let input = StandingInputBindingV1::PublishedView {
            edge_id: edge_id.clone(),
            producer_tenant_id: "default".to_string(),
            producer_program_id: "filtered_orders".to_string(),
            published_relation: binding,
            graph_revision: 3,
            bootstrap_cursor: sample_cursor(),
        };
        input.validate().unwrap();
        let mut stale = input.clone();
        if let StandingInputBindingV1::PublishedView {
            published_relation, ..
        } = &mut stale
        {
            published_relation.producer_view_generation += 1;
        }
        assert!(stale.validate().is_err());
        let mut wrong_cursor = input.clone();
        if let StandingInputBindingV1::PublishedView {
            bootstrap_cursor, ..
        } = &mut wrong_cursor
        {
            bootstrap_cursor.commit_digest = "not-a-sha256".to_string();
        }
        assert_eq!(
            wrong_cursor.validate(),
            Err(ViewContractError::InvalidField {
                field: "input_binding.published_view.bootstrap_cursor"
            })
        );
        // The cursor must be bound to the same producer scope as the binding.
        #[allow(clippy::type_complexity)]
        let scope_mutations: [(&str, fn(&mut CausalViewCursorV1)); 6] = [
            ("edge", |cursor: &mut CausalViewCursorV1| {
                cursor.input_edge.push_str("-other")
            }),
            ("tenant", |cursor: &mut CausalViewCursorV1| {
                cursor.producer_tenant_id.push_str("-other")
            }),
            ("program", |cursor: &mut CausalViewCursorV1| {
                cursor.producer_program_id.push_str("-other")
            }),
            ("view", |cursor: &mut CausalViewCursorV1| {
                cursor.producer_view_id.push_str("-other")
            }),
            ("generation", |cursor: &mut CausalViewCursorV1| {
                cursor.producer_generation += 1
            }),
            ("stream", |cursor: &mut CausalViewCursorV1| {
                cursor.output_stream.push_str("-other")
            }),
        ];
        for (field, mutate) in scope_mutations {
            let mut scope_mismatch = input.clone();
            if let StandingInputBindingV1::PublishedView {
                bootstrap_cursor, ..
            } = &mut scope_mismatch
            {
                mutate(bootstrap_cursor);
            }
            assert_eq!(
                scope_mismatch.validate(),
                Err(ViewContractError::InvalidField {
                    field: "input_binding.published_view.bootstrap_cursor"
                }),
                "cursor {field} mismatch must fail validation"
            );
        }
        let source = StandingInputBindingV1::Source {
            relation: RelationSchema {
                relation_id: "orders".to_string(),
                relation_name: "orders".to_string(),
                relation_version: "v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
                columns: vec![ColumnSchema {
                    name: "region".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                }],
                primary_key: vec!["region".to_string()],
            },
            relation_generation: 2,
        };
        source.validate().unwrap();
    }
    #[test]
    fn published_view_input_binding_hash_fences_generation_plan_schema_and_codec() {
        let binding = sample_binding();
        let edge_id = view_dependency_edge_id("default", "filtered_orders", &binding).unwrap();
        let input = StandingInputBindingV1::PublishedView {
            edge_id,
            producer_tenant_id: "default".to_string(),
            producer_program_id: "filtered_orders".to_string(),
            published_relation: binding,
            graph_revision: 3,
            bootstrap_cursor: sample_cursor(),
        };
        let base = input.input_catalog_hash().unwrap();
        let mut generation = input.clone();
        if let StandingInputBindingV1::PublishedView {
            published_relation, ..
        } = &mut generation
        {
            published_relation.producer_view_generation += 1;
        }
        assert_ne!(generation.input_catalog_hash().unwrap(), base);
        let mut plan = input.clone();
        if let StandingInputBindingV1::PublishedView {
            published_relation, ..
        } = &mut plan
        {
            published_relation.producer_plan_hash =
                "velorix-logical-view-plan-sha256-v1:other".to_string();
        }
        assert_ne!(plan.input_catalog_hash().unwrap(), base);
        let mut schema = input.clone();
        if let StandingInputBindingV1::PublishedView {
            published_relation, ..
        } = &mut schema
        {
            published_relation.relation.schema_fingerprint = format!("sha256:{}", "9".repeat(64));
        }
        assert_ne!(schema.input_catalog_hash().unwrap(), base);
        let mut codec = input.clone();
        if let StandingInputBindingV1::PublishedView {
            published_relation, ..
        } = &mut codec
        {
            published_relation.delta_codec_identity = "other-codec".to_string();
        }
        assert_ne!(codec.input_catalog_hash().unwrap(), base);
        let source = StandingInputBindingV1::Source {
            relation: RelationSchema {
                relation_id: "orders".to_string(),
                relation_name: "orders".to_string(),
                relation_version: "v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
                columns: vec![ColumnSchema {
                    name: "region".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                }],
                primary_key: vec!["region".to_string()],
            },
            relation_generation: 2,
        };
        assert_eq!(
            source.input_catalog_hash().unwrap(),
            format!("sha256:{}", "3".repeat(64))
        );
    }
    fn edge(consumer: &str, producer: &str) -> ViewDependencyEdgeV1 {
        ViewDependencyEdgeV1 {
            schema_version: VIEW_DEPENDENCY_EDGE_SCHEMA_VERSION_V1,
            edge_id: format!("edge-{consumer}-{producer}"),
            tenant_id: "default".to_string(),
            consumer_program_id: consumer.to_string(),
            consumer_view_id: consumer.to_string(),
            consumer_generation: 1,
            input_relation_id: producer.to_string(),
            input_relation_version: "v1".to_string(),
            producer_program_id: producer.to_string(),
            producer_view_id: producer.to_string(),
            producer_generation: 1,
            producer_plan_hash: format!("sha256:{}", "1".repeat(64)),
            output_stream_id: format!("view/{producer}/generation/1/output/{producer}"),
            output_schema_hash: format!("sha256:{}", "2".repeat(64)),
            key_descriptor_hash: format!("sha256:{}", "3".repeat(64)),
            delta_codec_identity: PUBLISHED_RELATION_DELTA_CODEC_V1.to_string(),
            frontier_kind: PUBLISHED_RELATION_FRONTIER_KIND_V1.to_string(),
        }
    }
    #[test]
    fn view_dependency_edge_id_is_deterministic_and_domain_separated() {
        let binding = sample_binding();
        let first = view_dependency_edge_id("default", "filtered_orders", &binding).unwrap();
        let second = view_dependency_edge_id("default", "filtered_orders", &binding).unwrap();
        assert_eq!(first, second);
        assert!(first.starts_with("sha256:"));
        let other_plan = {
            let mut binding = binding.clone();
            binding.producer_plan_hash = "velorix-logical-view-plan-sha256-v1:other".to_string();
            binding
        };
        let other_plan_id =
            view_dependency_edge_id("default", "filtered_orders", &other_plan).unwrap();
        assert_ne!(first, other_plan_id);
        let other_tenant =
            view_dependency_edge_id("other-tenant", "filtered_orders", &binding).unwrap();
        assert_ne!(first, other_tenant);
    }
    #[test]
    fn view_dependency_graph_rejects_self_two_node_and_three_node_cycles() {
        let self_cycle = vec![edge("a", "a")];
        assert!(matches!(
            validate_view_dependency_graph(&self_cycle),
            Err(ViewContractError::InvalidField {
                field: "dependency_cycle" | "dependency_edge.self_cycle",
                ..
            })
        ));
        let two_cycle = vec![edge("a", "b"), edge("b", "a")];
        assert_eq!(
            validate_view_dependency_graph(&two_cycle),
            Err(ViewContractError::InvalidField {
                field: "dependency_cycle"
            })
        );
        let three_cycle = vec![edge("a", "b"), edge("b", "c"), edge("c", "a")];
        assert_eq!(
            validate_view_dependency_graph(&three_cycle),
            Err(ViewContractError::InvalidField {
                field: "dependency_cycle"
            })
        );
    }
    #[test]
    fn view_dependency_graph_produces_producer_first_topological_order() {
        let edges = vec![
            edge("topk", "aggregate"),
            edge("aggregate", "filtered"),
            edge("filtered", "orders"),
        ];
        let order = validate_view_dependency_graph(&edges).unwrap();
        let orders_pos = order.iter().position(|id| id == "orders").unwrap();
        let filtered_pos = order.iter().position(|id| id == "filtered").unwrap();
        let aggregate_pos = order.iter().position(|id| id == "aggregate").unwrap();
        let topk_pos = order.iter().position(|id| id == "topk").unwrap();
        assert!(
            orders_pos < filtered_pos && filtered_pos < aggregate_pos && aggregate_pos < topk_pos
        );
    }
    #[test]
    fn published_relation_descriptor_catalog_round_trips_public_schema_with_signed_weight_column() {
        let binding = sample_binding();
        let catalog = catalog_from_published_relation_binding(&binding).unwrap();
        catalog.validate().unwrap();
        assert!(matches!(
            catalog.relation_source,
            VelorixRelationSourceV1::PublishedViewOutput { .. }
        ));
        assert_eq!(
            catalog.schema_fingerprint.as_str(),
            binding.relation.schema_fingerprint
        );
        assert_eq!(
            catalog.relation_schema.weight_column_id,
            PUBLISHED_DELTA_WEIGHT_FIELD_V1
        );
        let public = catalog_input_relation_schema(&catalog).unwrap();
        assert_eq!(public, binding.relation);
        let weight_column = catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == PUBLISHED_DELTA_WEIGHT_FIELD_V1)
            .unwrap();
        assert_eq!(
            weight_column.ordinal as usize,
            binding.relation.columns.len()
        );
        assert_eq!(weight_column.logical_type, VelorixLogicalTypeV1::Int64);
        assert!(!weight_column.nullable);
    }
    #[test]
    fn published_relation_descriptor_catalog_rejects_reserved_weight_column_in_public_schema() {
        let mut binding = sample_binding();
        binding.relation.columns.push(ColumnSchema {
            name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
            data_type: SqlDataType::Int64,
            nullable: false,
        });
        assert!(catalog_from_published_relation_binding(&binding).is_err());
    }
    #[test]
    fn published_relation_descriptor_catalog_rejects_unsupported_column_types() {
        let mut binding = sample_binding();
        binding.relation.columns.push(ColumnSchema {
            name: "payload".to_string(),
            data_type: SqlDataType::Null,
            nullable: true,
        });
        assert!(catalog_from_published_relation_binding(&binding).is_err());
    }
}

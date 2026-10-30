use super::*;
pub(crate) use velorix_core::example_fixtures::orders_sum_count_relation_catalog;

/// Convert a trusted engine fixture into the caller-facing relation registration
/// payload. This deliberately drops the engine weight column and its bindings.
pub(crate) fn public_relation_registration(catalog: &VelorixRelationCatalogV1) -> Value {
    let schema = &catalog.relation_schema;
    let columns = schema
        .columns
        .iter()
        .filter(|column| column.column_id != schema.weight_column_id)
        .enumerate()
        .map(|(ordinal, column)| {
            json!({
                "column_id": column.column_id,
                "name": column.name,
                "logical_type": column.logical_type,
                "physical_arrow_type": column.physical_arrow_type,
                "nullable": column.nullable,
                "ordinal": ordinal as u32,
            })
        })
        .collect::<Vec<_>>();
    json!({"catalog": {
        "relation_schema": {
            "relation_id": schema.relation_id,
            "relation_name": schema.relation_name,
            "relation_version": schema.relation_version,
            "columns": columns,
            "primary_key_column_ids": schema.primary_key_column_ids,
            "allowed_operations": schema.allowed_operations,
            "event_time_column_id": schema.event_time_column_id,
        },
        "adapter_id": catalog.incremental_adapter.adapter_id,
    }})
}

const DEFAULT_SCORES_RELATION_ID: &str = "scores";
const DEFAULT_SCORES_RELATION_VERSION: &str = "2026-05-24.v1";
const DEFAULT_POSITIVE_SCORES_VIEW_ID: &str = "positive_scores_by_user";
const DEFAULT_POSITIVE_SCORES_SQL: &str = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";

pub(crate) fn default_scores_relation_catalog() -> Result<VelorixRelationCatalogV1, ApiError> {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
        relation_name: DEFAULT_SCORES_RELATION_ID.to_string(),
        relation_version: DEFAULT_SCORES_RELATION_VERSION.to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["user_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
        .map_err(ApiError::bad_request)?;
    Ok(VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: DEFAULT_SCORES_RELATION_ID.to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    })
}

pub(crate) fn default_positive_scores_view_request(
    _catalog: &VelorixRelationCatalogV1,
) -> Result<CreateViewRequest, ApiError> {
    Ok(CreateViewRequest {
        view_id: DEFAULT_POSITIVE_SCORES_VIEW_ID.to_string(),
        url_path: Some("/scores/positive".to_string()),
        output_relation_id: None,
        input_relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
        input_relation_version: DEFAULT_SCORES_RELATION_VERSION.to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: DEFAULT_POSITIVE_SCORES_SQL.to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("Positive score totals by user".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    })
}

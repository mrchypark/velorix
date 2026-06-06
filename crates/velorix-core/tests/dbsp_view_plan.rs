use velorix_core::{
    dbsp_view_plan::{validate_supported_dbsp_view_sql, DbspViewPlanError},
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
};

#[test]
fn supported_dbsp_view_sql_accepts_catalog_backed_sum_count_shape() {
    let catalog = scores_catalog();

    let plan = validate_supported_dbsp_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.input_relation_id, "scores");
    assert_eq!(plan.group_key_column_id, "user_id");
    assert_eq!(plan.sum_value_column_id, "score");
}

#[test]
fn supported_dbsp_view_sql_rejects_filter_until_predicate_execution_is_wired() {
    let catalog = scores_catalog();

    let error = validate_supported_dbsp_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(
        matches!(error, DbspViewPlanError::UnsupportedShape { .. }),
        "unexpected error: {error}"
    );
    assert!(
        error
            .to_string()
            .contains("only plain SELECT/FROM/GROUP BY sum/count views are supported"),
        "unexpected error: {error}"
    );
}

fn scores_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
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
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "scores".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "scores".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

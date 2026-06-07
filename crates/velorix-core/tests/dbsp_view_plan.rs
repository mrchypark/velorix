use serde_json::json;
use velorix_core::{
    dbsp_view_plan::{validate_supported_dbsp_view_sql, DbspPredicateOp, DbspViewPlanError},
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
    assert_eq!(plan.predicate, None);
}

#[test]
fn supported_dbsp_view_sql_accepts_single_literal_comparison_filter() {
    let catalog = scores_catalog();

    let plan = validate_supported_dbsp_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where score > -1 group by user_id",
        &catalog,
    )
    .unwrap();

    let predicate = plan.predicate.unwrap();
    assert_eq!(predicate.column_id, "score");
    assert_eq!(predicate.op, DbspPredicateOp::Gt);
    assert_eq!(predicate.literal, json!(-1));
}

#[test]
fn supported_dbsp_view_sql_rejects_join_until_multi_input_runtime_is_wired() {
    let catalog = scores_catalog();

    let error = validate_supported_dbsp_view_sql(
        "select s.user_id, sum(s.score) as sum, count(*) as count from scores s join users u on s.user_id = u.user_id group by s.user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, DbspViewPlanError::UnsupportedShape { .. }));
    assert!(error.to_string().contains("joins are not supported"));
}

#[test]
fn supported_dbsp_view_sql_rejects_filter_on_non_runtime_visible_column() {
    let mut catalog = scores_catalog();
    catalog.relation_schema.columns.push(RelationColumnV1 {
        column_id: "status".to_string(),
        name: "status".to_string(),
        logical_type: VelorixLogicalTypeV1::Utf8,
        physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
        nullable: false,
        ordinal: 3,
        semantic_role: RelationSemanticRoleV1::Metadata,
    });
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();

    let error = validate_supported_dbsp_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where status = 'paid' group by user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, DbspViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("WHERE column must be the primary key or value column"));
}

#[test]
fn supported_dbsp_view_sql_rejects_filter_on_weight_column() {
    let catalog = scores_catalog();

    let error = validate_supported_dbsp_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where delta = 1 group by user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, DbspViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("WHERE column must be the primary key or value column"));
}

#[test]
fn supported_dbsp_view_sql_rejects_qualified_column_references_for_now() {
    let catalog = scores_catalog();

    let error = validate_supported_dbsp_view_sql(
        "select s.user_id, sum(s.score) as sum, count(*) as count from scores as s group by s.user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, DbspViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("GROUP BY key must be the catalog primary key column"));
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

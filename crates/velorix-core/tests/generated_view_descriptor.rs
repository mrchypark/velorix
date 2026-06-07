use velorix_core::{
    feldera_artifact::{
        feldera_spec_hash, ColumnSchema, FelderaCompilerIdentity, GeneratedRustIdentity,
        RelationSchema, SqlDataType, SUPPORTED_GENERATED_RUST_ABI_VERSION,
    },
    generated_view_descriptor::{DynamicGeneratedViewBinding, TrustedGeneratedViewDescriptor},
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
};

#[test]
fn trusted_generated_view_descriptor_builds_artifact_metadata_for_catalog() {
    let catalog = scores_catalog();
    let descriptor = descriptor(&catalog);

    let artifact = descriptor.artifact_metadata(&catalog).unwrap();
    let spec = descriptor.standing_view_spec(&catalog).unwrap();

    assert_eq!(artifact.view_id, "positive_scores_by_user");
    assert_eq!(artifact.artifact_id, "builtin-positive-scores-by-user");
    assert_eq!(
        artifact.generated_rust.crate_name,
        "scores_by_user_generated"
    );
    assert_eq!(artifact.input_schemas[0].relation_id, "scores");
    assert_eq!(
        artifact.output_schemas[0].relation_id,
        "positive_scores_by_user"
    );
    assert_eq!(
        artifact.output_schemas[0].schema_fingerprint,
        catalog.schema_fingerprint.as_str()
    );
    assert_eq!(artifact.spec_hash, feldera_spec_hash(&spec).unwrap());
}

#[test]
fn trusted_generated_view_descriptor_matches_request_sql_whitespace_case_insensitively() {
    let catalog = scores_catalog();
    let descriptor = descriptor(&catalog);

    assert!(descriptor.matches_view_request(
        "positive_scores_by_user",
        "scores",
        "2026-05-24.v1",
        "SELECT user_id, sum(score) AS sum, count(*) AS count FROM scores WHERE score > 0 GROUP BY user_id",
    ));
    assert!(!descriptor.matches_view_request(
        "other_view",
        "scores",
        "2026-05-24.v1",
        &descriptor.sql,
    ));
}

#[test]
fn trusted_generated_view_descriptor_matches_shape_without_binding_view_identity() {
    let catalog = scores_catalog();
    let descriptor = descriptor(&catalog);

    assert!(descriptor.matches_view_shape(
        "scores",
        "2026-05-24.v1",
        "SELECT user_id, sum(score) AS sum, count(*) AS count FROM scores WHERE score > 0 GROUP BY user_id",
    ));
    assert!(!descriptor.matches_view_shape(
        "scores",
        "2026-05-24.v1",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
    ));
}

#[test]
fn trusted_generated_view_descriptor_rejects_wrong_input_catalog() {
    let catalog = scores_catalog();
    let mut wrong_catalog = catalog.clone();
    wrong_catalog.relation_schema.relation_version = "other".to_string();
    let descriptor = descriptor(&catalog);

    let error = descriptor.artifact_metadata(&wrong_catalog).unwrap_err();

    assert!(format!("{error}").contains("input relation version mismatch"));
}

fn descriptor(catalog: &VelorixRelationCatalogV1) -> TrustedGeneratedViewDescriptor {
    TrustedGeneratedViewDescriptor {
        view_id: "positive_scores_by_user".to_string(),
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
        dynamic_view_binding: Some(DynamicGeneratedViewBinding {
            shape_id: "scores.positive-scores-by-user.v1".to_string(),
        }),
        artifact_id: "builtin-positive-scores-by-user".to_string(),
        artifact_identity_bytes: b"velorix-builtin-scores-by-user-generated-package".to_vec(),
        compiler: FelderaCompilerIdentity {
            name: "feldera-sql-compiler".to_string(),
            version: "builtin-default".to_string(),
            source: "velorix-linked-generated-package".to_string(),
        },
        generated_rust: GeneratedRustIdentity {
            abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
            crate_name: "scores_by_user_generated".to_string(),
        },
        output_schemas: vec![RelationSchema {
            relation_id: "positive_scores_by_user".to_string(),
            relation_name: "positive_scores_by_user".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            columns: vec![
                ColumnSchema {
                    name: "user_id".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "sum".to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "count".to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["user_id".to_string()],
        }],
        state_schema_version: 1,
    }
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

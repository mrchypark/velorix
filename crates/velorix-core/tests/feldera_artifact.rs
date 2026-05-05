use std::path::PathBuf;

use velorix_core::feldera_artifact::{
    catalog_input_relation_schema, feldera_spec_hash, validate_feldera_compile_artifact,
    validate_feldera_compile_artifact_for_catalog, FelderaArtifactError,
    FelderaCompileArtifactMetadata, StandingViewSpec,
};
use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1, RelationOperationV1,
    RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
    VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
};

fn load_spec(name: &str) -> StandingViewSpec {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn load_artifact(name: &str) -> FelderaCompileArtifactMetadata {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn parse_artifact(name: &str) -> Result<FelderaCompileArtifactMetadata, serde_json::Error> {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap())
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("feldera")
        .join(format!("{name}.json"))
}

#[test]
fn feldera_artifact_accepts_valid_single_input_output_standing_view() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_valid");

    assert_eq!(
        feldera_spec_hash(&spec).unwrap(),
        "velorix-feldera-spec-sha256-v1:0e24cbe06543d735a6d62868f230c4610fb9139cb91e5e8f72042f17da0ecbea"
    );
    validate_feldera_compile_artifact(&spec, &artifact).unwrap();
}

#[test]
fn feldera_catalog_relation_schema_is_the_single_input_identity() {
    let catalog = orders_relation_catalog();
    let schema = catalog_input_relation_schema(&catalog).unwrap();

    assert_eq!(schema.relation_id, catalog.relation_schema.relation_id);
    assert_eq!(
        schema.relation_version,
        catalog.relation_schema.relation_version
    );
    assert_eq!(
        schema.schema_fingerprint,
        catalog.schema_fingerprint.as_str()
    );
    assert_eq!(schema.primary_key, vec!["order_id"]);
    assert_eq!(
        schema.columns[1].data_type,
        velorix_core::feldera_artifact::SqlDataType::Int64
    );
}

#[test]
fn feldera_catalog_validation_fails_closed_when_spec_or_artifact_input_differs() {
    let catalog = orders_relation_catalog();
    let mut spec = catalog_backed_standing_view_spec(&catalog);
    let mut artifact = catalog_backed_artifact(&spec);

    spec.input_relations[0].schema_fingerprint = format!("sha256:{}", "1".repeat(64));
    artifact.spec_hash = feldera_spec_hash(&spec).unwrap();
    artifact.input_schemas = spec.input_relations.clone();
    let error =
        validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap_err();
    assert!(matches!(
        error,
        FelderaArtifactError::SchemaFingerprintMismatch {
            field: "spec.input_relations"
        }
    ));

    let spec = catalog_backed_standing_view_spec(&catalog);
    let mut artifact = catalog_backed_artifact(&spec);
    artifact.input_schemas[0].relation_version = "2026-05-06.v1".to_string();
    let error =
        validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap_err();
    assert!(matches!(
        error,
        FelderaArtifactError::SchemaMismatch {
            field: "input_schemas"
        }
    ));
}

#[test]
fn feldera_catalog_validation_accepts_artifact_derived_from_catalog() {
    let catalog = orders_relation_catalog();
    let spec = catalog_backed_standing_view_spec(&catalog);
    let artifact = catalog_backed_artifact(&spec);

    validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap();
}

#[test]
fn feldera_artifact_rejects_unsupported_metadata_version() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_invalid_version");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedMetadataVersion { version: 2 }
    ));
}

fn orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "order_id".to_string(),
                name: "order_id".to_string(),
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
        primary_key_column_ids: vec!["order_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-v1".to_string(),
        },
    }
}

fn catalog_backed_standing_view_spec(catalog: &VelorixRelationCatalogV1) -> StandingViewSpec {
    StandingViewSpec {
        view_id: "orders_sum".to_string(),
        sql: "select order_id, sum(amount) as total_amount from orders group by order_id"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::StandingView,
        input_relations: vec![catalog_input_relation_schema(catalog).unwrap()],
        output_relations: vec![velorix_core::feldera_artifact::RelationSchema {
            relation_id: "orders_sum".to_string(),
            relation_name: "orders_sum".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
            columns: vec![
                velorix_core::feldera_artifact::ColumnSchema {
                    name: "order_id".to_string(),
                    data_type: velorix_core::feldera_artifact::SqlDataType::Utf8,
                    nullable: false,
                },
                velorix_core::feldera_artifact::ColumnSchema {
                    name: "total_amount".to_string(),
                    data_type: velorix_core::feldera_artifact::SqlDataType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["order_id".to_string()],
        }],
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn catalog_backed_artifact(spec: &StandingViewSpec) -> FelderaCompileArtifactMetadata {
    FelderaCompileArtifactMetadata {
        metadata_version: 1,
        view_id: spec.view_id.clone(),
        spec_hash: feldera_spec_hash(spec).unwrap(),
        artifact_id: "artifact-orders-sum-v1".to_string(),
        artifact_hash: format!("sha256:{}", "3".repeat(64)),
        compiler: velorix_core::feldera_artifact::FelderaCompilerIdentity {
            name: "feldera".to_string(),
            version: "0.1.0".to_string(),
            source: "fixture".to_string(),
        },
        generated_rust: velorix_core::feldera_artifact::GeneratedRustIdentity {
            abi_version: velorix_core::feldera_artifact::SUPPORTED_GENERATED_RUST_ABI_VERSION
                .to_string(),
            crate_name: "orders_sum".to_string(),
        },
        input_schemas: spec.input_relations.clone(),
        output_schemas: spec.output_relations.clone(),
        state_codec: velorix_core::feldera_artifact::SUPPORTED_STATE_CODEC.to_string(),
        state_schema_version: 1,
        epoch_policy: velorix_core::feldera_artifact::SUPPORTED_EPOCH_POLICY.to_string(),
    }
}

#[test]
fn feldera_artifact_rejects_missing_schema() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_missing_schema");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MissingSchema {
            field: "input_schemas"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_missing_artifact_id() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_missing_artifact_id");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MissingIdentityField {
            field: "artifact_id"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_mismatched_spec_hash() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_mismatched_spec_hash");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedSpecHash { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_mismatched_view_id() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_mismatched_view_id");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedViewId { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_unknown_state_codec() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_unknown_state_codec");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedStateCodec { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_unsupported_epoch_policy() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_unsupported_epoch_policy");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedEpochPolicy { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_unsupported_generated_rust_abi() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_unsupported_generated_rust_abi");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedGeneratedRustAbi { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_input_schema_mismatch() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_schema_mismatch");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::SchemaMismatch {
            field: "input_schemas"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_input_schema_fingerprint_mismatch() {
    let mut spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_valid");
    artifact.input_schemas[0].schema_fingerprint = format!("sha256:{}", "1".repeat(64));

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::SchemaFingerprintMismatch {
            field: "input_schemas"
        }
    ));

    spec.input_relations[0].schema_fingerprint = format!("sha256:{}", "2".repeat(64));
    let mut artifact = load_artifact("compile_artifact_valid");
    artifact.spec_hash = feldera_spec_hash(&spec).unwrap();
    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::SchemaFingerprintMismatch {
            field: "input_schemas"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_unknown_wire_fields() {
    let error = parse_artifact("compile_artifact_unknown_field").unwrap_err();

    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn feldera_artifact_rejects_missing_required_wire_fields() {
    let error = parse_artifact("compile_artifact_missing_required_field").unwrap_err();

    assert!(error.to_string().contains("missing field"));
}

#[test]
fn feldera_artifact_rejects_malformed_wire_json() {
    let error = parse_artifact("compile_artifact_malformed_json").unwrap_err();

    assert!(!error.is_data());
}

#[test]
fn feldera_artifact_rejects_multi_input_shape_for_now() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_multi_input");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedShape {
            shape: "multi_input"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_multi_output_shape_for_now() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_multi_output");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedShape {
            shape: "multi_output"
        }
    ));
}

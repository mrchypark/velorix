use velorix_core::{
    feldera_artifact::{
        feldera_artifact_bytes_hash, ColumnSchema, FelderaCompileRequestV1, OutputSchemaContract,
        RelationSchema, SqlDataType, SqlDialect, SqlSourceKind, StandingViewShape,
        StandingViewSpec,
    },
    feldera_product_runtime::{
        build_feldera_package_runtime_descriptor, validate_feldera_package_runtime_descriptor,
        BuildFelderaPackageRuntimeDescriptorRequest, FelderaPackageBackendIdentity,
        FelderaPackageRuntimeDescriptorV1, FelderaPackageRuntimeFactoryBinding,
        FelderaProductRuntimeDescriptorError,
    },
    standing_program::NativeCodePolicy,
};

fn input_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: feldera_artifact_bytes_hash(b"scores-schema"),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec![],
    }
}

fn output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores_by_user".to_string(),
        relation_name: "positive_scores_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: feldera_artifact_bytes_hash(b"positive-scores-by-user-schema"),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "total_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn spec() -> StandingViewSpec {
    StandingViewSpec {
        view_id: "positive_scores_by_user".to_string(),
        sql:
            "select user_id, sum(score) as total_score from scores where score > 0 group by user_id"
                .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema()],
        output_relations: vec![output_schema()],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn compile_request(spec: &StandingViewSpec) -> FelderaCompileRequestV1 {
    FelderaCompileRequestV1 {
        view_id: spec.view_id.clone(),
        sql: spec.sql.clone(),
        dialect: spec.dialect.clone(),
        source_kind: spec.source_kind.clone(),
        rust_extension: Default::default(),
        input_relations: spec.input_relations.clone(),
        output_contract: OutputSchemaContract::Infer,
        shape: spec.shape.clone(),
    }
}

fn descriptor(
    spec: &StandingViewSpec,
    compile_request: &FelderaCompileRequestV1,
) -> FelderaPackageRuntimeDescriptorV1 {
    build_feldera_package_runtime_descriptor(BuildFelderaPackageRuntimeDescriptorRequest {
        spec: spec.clone(),
        compile_request: compile_request.clone(),
        backend: FelderaPackageBackendIdentity {
            name: "feldera-package-jarless".to_string(),
            version: "0.299.0".to_string(),
            source: "feldera public Rust packages".to_string(),
        },
        runtime_factory: FelderaPackageRuntimeFactoryBinding {
            crate_name: "velorix_feldera_package_runtime".to_string(),
            crate_version: "0.299.0".to_string(),
            factory_symbol: "create_runtime".to_string(),
        },
        state_codec: "feldera-package-runtime-state-v1".to_string(),
        state_schema_version: 1,
    })
    .unwrap()
}

#[test]
fn feldera_package_runtime_descriptor_accepts_matching_jarless_product_identity() {
    let spec = spec();
    let compile_request = compile_request(&spec);
    let descriptor = descriptor(&spec, &compile_request);

    validate_feldera_package_runtime_descriptor(&spec, &compile_request, &descriptor).unwrap();
}

#[test]
fn feldera_package_runtime_descriptor_rejects_native_code_policy_when_product_runtime() {
    let spec = spec();
    let compile_request = compile_request(&spec);
    let mut descriptor = descriptor(&spec, &compile_request);
    descriptor.standing_program_identity.native_code_policy =
        NativeCodePolicy::NativeCodeOrExternalDependenciesPresent {
            reason: "udf_rust present".to_string(),
        };

    let error = validate_feldera_package_runtime_descriptor(&spec, &compile_request, &descriptor)
        .unwrap_err();

    assert!(matches!(
        error,
        FelderaProductRuntimeDescriptorError::NativeCodePolicyNotDisabled
    ));
}

#[test]
fn feldera_package_runtime_descriptor_rejects_mismatched_compile_request_hash() {
    let spec = spec();
    let compile_request = compile_request(&spec);
    let mut descriptor = descriptor(&spec, &compile_request);
    descriptor.compile_request_hash = feldera_artifact_bytes_hash(b"different-request");

    let error = validate_feldera_package_runtime_descriptor(&spec, &compile_request, &descriptor)
        .unwrap_err();

    assert!(matches!(
        error,
        FelderaProductRuntimeDescriptorError::MismatchedCompileRequestHash { .. }
    ));
}

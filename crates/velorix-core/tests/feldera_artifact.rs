use std::path::PathBuf;

use velorix_core::feldera_artifact::{
    catalog_input_relation_schema, feldera_artifact_bytes_hash, feldera_compile_request_hash,
    feldera_spec_hash, feldera_sql_program_for_compile_request, validate_feldera_compile_artifact,
    validate_feldera_compile_artifact_for_catalog, validate_feldera_compile_artifact_for_catalogs,
    validate_feldera_compile_artifact_for_compile_request, validate_feldera_compile_artifact_hash,
    validate_feldera_compile_request, validate_feldera_release_artifact_provenance, ColumnSchema,
    FelderaArtifactError, FelderaCompileArtifactMetadata, FelderaCompileRequestV1,
    FelderaReleaseArtifactProvenanceV1, FelderaRustExtensionV1, OutputSchemaContract,
    RelationSchema, SqlDataType, SqlIntervalUnit, SqlStructField, StandingViewSpec,
};
use velorix_core::relation::{
    ArrowPhysicalTypeV1, ArrowStructFieldV1, DataFusionRegistrationModeV1,
    DataFusionRegistrationV1, FelderaRelationBindingV1, IncrementalAdapterBindingV1,
    RelationColumnV1, RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1,
    VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixStructFieldV1,
    RELATION_SCHEMA_VERSION_V1,
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

fn load_provenance(name: &str) -> FelderaReleaseArtifactProvenanceV1 {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
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
fn feldera_compile_request_hash_excludes_inferred_output_schema() {
    let mut spec = load_spec("standing_view_spec_valid");
    let request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);
    let request_hash = feldera_compile_request_hash(&request).unwrap();

    spec.output_relations[0].columns[1].name = "total_score".to_string();
    let same_request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);

    validate_feldera_compile_request(&same_request).unwrap();
    assert_eq!(same_request.output_contract, OutputSchemaContract::Infer);
    assert_eq!(
        feldera_compile_request_hash(&same_request).unwrap(),
        request_hash
    );
}

#[test]
fn feldera_compile_request_hash_includes_must_match_output_contract() {
    let spec = load_spec("standing_view_spec_valid");
    let infer_request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);
    let must_match_request = FelderaCompileRequestV1 {
        output_contract: OutputSchemaContract::MustMatch {
            output_relations: spec.output_relations.clone(),
        },
        ..infer_request.clone()
    };

    assert_ne!(
        feldera_compile_request_hash(&infer_request).unwrap(),
        feldera_compile_request_hash(&must_match_request).unwrap()
    );
}

#[test]
fn feldera_compile_request_rejects_multi_input_shape_mismatch() {
    let spec = load_spec("standing_view_spec_valid");
    let mut request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);
    request
        .input_relations
        .push(request.input_relations[0].clone());
    request.shape.multi_input = false;

    assert!(matches!(
        validate_feldera_compile_request(&request),
        Err(FelderaArtifactError::UnsupportedShape {
            shape: "compile_request.shape.multi_input"
        })
    ));
}

#[test]
fn feldera_compile_request_program_uses_feldera_sql_for_multi_relation_join() {
    let orders = catalog_input_relation_schema(&orders_relation_catalog()).unwrap();
    let customers = RelationSchema {
        relation_id: "customers".to_string(),
        relation_name: "customers".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "4".repeat(64)),
        columns: vec![
            column("customer_id", SqlDataType::Utf8, false),
            column("region", SqlDataType::Utf8, true),
        ],
        primary_key: vec!["customer_id".to_string()],
    };
    let request = FelderaCompileRequestV1 {
        view_id: "regional_order_totals".to_string(),
        sql: "select c.region, sum(o.amount) as total_amount\nfrom orders o\njoin customers c on o.order_id = c.customer_id\nwhere o.amount > 0\ngroup by c.region;"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![orders, customers],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: true,
            multi_output: false,
        },
    };

    let program = feldera_sql_program_for_compile_request(&request).unwrap();

    assert_eq!(
        program,
        "CREATE TABLE \"orders\" (\n    \"order_id\" VARCHAR NOT NULL,\n    \"amount\" BIGINT NOT NULL,\n    \"weight\" BIGINT NOT NULL,\n    PRIMARY KEY (\"order_id\")\n);\n\nCREATE TABLE \"customers\" (\n    \"customer_id\" VARCHAR NOT NULL,\n    \"region\" VARCHAR NULL,\n    PRIMARY KEY (\"customer_id\")\n);\n\nCREATE MATERIALIZED VIEW \"regional_order_totals\" AS\nselect c.region, sum(o.amount) as total_amount\nfrom orders o\njoin customers c on o.order_id = c.customer_id\nwhere o.amount > 0\ngroup by c.region;"
    );
}

#[test]
fn feldera_compile_request_program_quotes_catalog_identifiers() {
    let request = FelderaCompileRequestV1 {
        view_id: "quoted\"view".to_string(),
        sql: "select \"select\" from \"odd\"\"relation\"".to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "odd-relation".to_string(),
            relation_name: "odd\"relation".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "5".repeat(64)),
            columns: vec![column("select", SqlDataType::Utf8, false)],
            primary_key: vec![],
        }],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };

    let program = feldera_sql_program_for_compile_request(&request).unwrap();

    assert!(program.contains("CREATE TABLE \"odd\"\"relation\""));
    assert!(program.contains("\"select\" VARCHAR NOT NULL"));
    assert!(program.contains("CREATE MATERIALIZED VIEW \"quoted\"\"view\" AS"));
}

#[test]
fn feldera_compile_request_program_preserves_feldera_program_source() {
    let request = FelderaCompileRequestV1 {
        view_id: "score_program".to_string(),
        sql: "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id;\nCREATE VIEW positive_scores AS SELECT * FROM scores WHERE score > 0;"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![
                column("user_id", SqlDataType::Utf8, false),
                column("score", SqlDataType::Int64, false),
            ],
            primary_key: vec![],
        }],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };

    let program = feldera_sql_program_for_compile_request(&request).unwrap();

    assert!(program.starts_with("CREATE TABLE \"scores\""));
    assert!(program.contains("CREATE MATERIALIZED VIEW by_user AS"));
    assert!(program.contains("CREATE VIEW positive_scores AS"));
    assert!(!program.contains("CREATE MATERIALIZED VIEW \"score_program\" AS"));
}

#[test]
fn feldera_compile_request_hash_includes_rust_extension_payload() {
    let mut request = FelderaCompileRequestV1 {
        view_id: "score_program".to_string(),
        sql: "CREATE LINEAR AGGREGATE signed_sum(value BIGINT) RETURNS BIGINT; CREATE MATERIALIZED VIEW by_user AS SELECT user_id, signed_sum(score) AS total_score FROM scores GROUP BY user_id"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::FelderaProgram,
        rust_extension: FelderaRustExtensionV1 {
            udf_rust: Some(
                "pub type signed_sum_accumulator_type = i64;\npub fn signed_sum_map(value: i64) -> i64 { value }\npub fn signed_sum_post(value: i64) -> i64 { value }\n"
                    .to_string(),
            ),
            udf_toml: None,
        },
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![
                column("user_id", SqlDataType::Utf8, false),
                column("score", SqlDataType::Int64, false),
            ],
            primary_key: vec![],
        }],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };
    let original_hash = feldera_compile_request_hash(&request).unwrap();
    request.rust_extension.udf_rust = Some(
        "pub type signed_sum_accumulator_type = i64;\npub fn signed_sum_map(value: i64) -> i64 { value + 1 }\npub fn signed_sum_post(value: i64) -> i64 { value }\n"
            .to_string(),
    );
    let changed_hash = feldera_compile_request_hash(&request).unwrap();

    assert_ne!(original_hash, changed_hash);
}

#[test]
fn feldera_compile_request_rejects_rust_extension_on_standing_view() {
    let request = FelderaCompileRequestV1 {
        view_id: "scores_by_user".to_string(),
        sql: "select user_id, sum(score) as total_score from scores group by user_id".to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::StandingView,
        rust_extension: FelderaRustExtensionV1 {
            udf_rust: Some("pub fn unused() {}".to_string()),
            udf_toml: None,
        },
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![column("user_id", SqlDataType::Utf8, false)],
            primary_key: vec![],
        }],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };

    assert_eq!(
        validate_feldera_compile_request(&request),
        Err(FelderaArtifactError::UnsupportedShape {
            shape: "rust_extension.source_kind"
        })
    );
}

#[test]
fn feldera_compile_request_allows_empty_udf_toml_dependencies_table() {
    let request = FelderaCompileRequestV1 {
        view_id: "scores_by_user".to_string(),
        sql: "CREATE LINEAR AGGREGATE signed_sum(value BIGINT) RETURNS BIGINT; CREATE MATERIALIZED VIEW by_user AS SELECT user_id, signed_sum(score) AS total_score FROM scores GROUP BY user_id"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::FelderaProgram,
        rust_extension: FelderaRustExtensionV1 {
            udf_rust: Some("pub fn unused() {}".to_string()),
            udf_toml: Some("# no external crates\n[dependencies]\n".to_string()),
        },
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![column("user_id", SqlDataType::Utf8, false)],
            primary_key: vec![],
        }],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };

    validate_feldera_compile_request(&request).unwrap();
}

#[test]
fn feldera_compile_request_rejects_udf_toml_external_dependencies() {
    let request = FelderaCompileRequestV1 {
        view_id: "scores_by_user".to_string(),
        sql: "CREATE LINEAR AGGREGATE signed_sum(value BIGINT) RETURNS BIGINT; CREATE MATERIALIZED VIEW by_user AS SELECT user_id, signed_sum(score) AS total_score FROM scores GROUP BY user_id"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::FelderaProgram,
        rust_extension: FelderaRustExtensionV1 {
            udf_rust: Some("pub fn unused() {}".to_string()),
            udf_toml: Some("[dependencies]\nserde = \"1\"\n".to_string()),
        },
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![column("user_id", SqlDataType::Utf8, false)],
            primary_key: vec![],
        }],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };

    assert_eq!(
        validate_feldera_compile_request(&request),
        Err(FelderaArtifactError::UnsupportedShape {
            shape: "rust_extension.udf_toml.external_dependencies"
        })
    );
}

#[test]
fn feldera_compile_request_discovers_feldera_program_outputs_without_hints() {
    let spec = StandingViewSpec {
        view_id: "score_program".to_string(),
        sql: "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id;"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![
                column("user_id", SqlDataType::Utf8, false),
                column("score", SqlDataType::Int64, false),
            ],
            primary_key: vec![],
        }],
        output_relations: vec![RelationSchema {
            relation_id: "score_program".to_string(),
            relation_name: "score_program".to_string(),
            relation_version: "pending".to_string(),
            schema_fingerprint: format!("sha256:{}", "7".repeat(64)),
            columns: vec![column("value", SqlDataType::Utf8, true)],
            primary_key: vec![],
        }],
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };

    let request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);

    assert_eq!(request.output_contract, OutputSchemaContract::Infer);
    assert!(request.shape.multi_output);
    assert_eq!(request.source_kind, spec.source_kind);
    assert_eq!(request.input_relations, spec.input_relations);
}

#[test]
fn feldera_compile_request_program_covers_supported_feldera_table_schema_types() {
    let request = FelderaCompileRequestV1 {
        view_id: "wide_events_view".to_string(),
        sql: "select id, tags, attributes, nested, uuid_value, payload from wide_events"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "wide_events".to_string(),
            relation_name: "wide_events".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![
                column("id", SqlDataType::Utf8, false),
                column("enabled", SqlDataType::Bool, false),
                column("i8_value", SqlDataType::Int8, false),
                column("i16_value", SqlDataType::Int16, false),
                column("i32_value", SqlDataType::Int32, false),
                column("i64_value", SqlDataType::Int64, false),
                column("u8_value", SqlDataType::UInt8, false),
                column("u16_value", SqlDataType::UInt16, false),
                column("u32_value", SqlDataType::UInt32, false),
                column("u64_value", SqlDataType::UInt64, false),
                column("f32_value", SqlDataType::Float32, false),
                column("f64_value", SqlDataType::Float64, false),
                column(
                    "amount",
                    SqlDataType::Decimal {
                        precision: 18,
                        scale: 2,
                    },
                    true,
                ),
                column("code", SqlDataType::Char { length: Some(8) }, true),
                column("raw", SqlDataType::Binary { length: 16 }, true),
                column("bytes", SqlDataType::Varbinary, true),
                column("event_time", SqlDataType::Time, true),
                column("event_date", SqlDataType::Date, true),
                column(
                    "created_at",
                    SqlDataType::Timestamp { timezone: None },
                    true,
                ),
                column(
                    "tags",
                    SqlDataType::Array {
                        element_type: Box::new(SqlDataType::Utf8),
                    },
                    true,
                ),
                column(
                    "attributes",
                    SqlDataType::Map {
                        key_type: Box::new(SqlDataType::Utf8),
                        value_type: Box::new(SqlDataType::Int64),
                    },
                    true,
                ),
                column(
                    "nested",
                    SqlDataType::Struct {
                        fields: vec![
                            SqlStructField {
                                name: "inner_name".to_string(),
                                data_type: SqlDataType::Utf8,
                                nullable: false,
                            },
                            SqlStructField {
                                name: "inner_count".to_string(),
                                data_type: SqlDataType::Int64,
                                nullable: true,
                            },
                        ],
                    },
                    true,
                ),
                column("uuid_value", SqlDataType::Uuid, true),
                column("payload", SqlDataType::Json, true),
                column("shape", SqlDataType::Geometry, true),
            ],
            primary_key: vec!["id".to_string()],
        }],
        output_contract: OutputSchemaContract::Infer,
        shape: velorix_core::feldera_artifact::StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };

    let program = feldera_sql_program_for_compile_request(&request).unwrap();

    for expected in [
        "\"enabled\" BOOLEAN NOT NULL",
        "\"i8_value\" TINYINT NOT NULL",
        "\"i16_value\" SMALLINT NOT NULL",
        "\"i32_value\" INTEGER NOT NULL",
        "\"i64_value\" BIGINT NOT NULL",
        "\"u8_value\" TINYINT UNSIGNED NOT NULL",
        "\"u16_value\" SMALLINT UNSIGNED NOT NULL",
        "\"u32_value\" INTEGER UNSIGNED NOT NULL",
        "\"u64_value\" BIGINT UNSIGNED NOT NULL",
        "\"f32_value\" REAL NOT NULL",
        "\"f64_value\" DOUBLE NOT NULL",
        "\"amount\" DECIMAL(18, 2) NULL",
        "\"code\" CHAR(8) NULL",
        "\"raw\" BINARY(16) NULL",
        "\"bytes\" VARBINARY NULL",
        "\"event_time\" TIME NULL",
        "\"event_date\" DATE NULL",
        "\"created_at\" TIMESTAMP NULL",
        "\"tags\" VARCHAR ARRAY NULL",
        "\"attributes\" MAP<VARCHAR, BIGINT> NULL",
        "\"nested\" ROW(\"inner_name\" VARCHAR NOT NULL, \"inner_count\" BIGINT NULL) NULL",
        "\"uuid_value\" UUID NULL",
        "\"payload\" VARIANT NULL",
        "\"shape\" GEOMETRY NULL",
    ] {
        assert!(
            program.contains(expected),
            "program did not contain expected DDL fragment: {expected}\n{program}"
        );
    }
}

#[test]
fn feldera_compile_request_program_rejects_types_not_supported_in_table_schemas() {
    for data_type in [
        SqlDataType::Timestamp {
            timezone: Some("UTC".to_string()),
        },
        SqlDataType::Interval {
            unit: SqlIntervalUnit::DayToSecond,
        },
        SqlDataType::Null,
    ] {
        let request = FelderaCompileRequestV1 {
            view_id: "unsupported_type_view".to_string(),
            sql: "select id, unsupported from unsupported_type_relation".to_string(),
            dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
            source_kind: velorix_core::feldera_artifact::SqlSourceKind::StandingView,
            rust_extension: Default::default(),
            input_relations: vec![RelationSchema {
                relation_id: "unsupported_type_relation".to_string(),
                relation_name: "unsupported_type_relation".to_string(),
                relation_version: "2026-05-05.v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "7".repeat(64)),
                columns: vec![
                    column("id", SqlDataType::Utf8, false),
                    column("unsupported", data_type, true),
                ],
                primary_key: vec!["id".to_string()],
            }],
            output_contract: OutputSchemaContract::Infer,
            shape: velorix_core::feldera_artifact::StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };

        let error = feldera_sql_program_for_compile_request(&request).unwrap_err();

        assert!(matches!(
            error,
            FelderaArtifactError::UnsupportedTableSchemaType {
                field: "column.data_type",
                ..
            }
        ));
    }
}

#[test]
fn catalog_input_relation_schema_accepts_expanded_feldera_scalar_input_types() {
    let catalog = expanded_scalar_relation_catalog();

    let schema = catalog_input_relation_schema(&catalog).unwrap();

    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("id", &SqlDataType::Utf8),
            ("i8_value", &SqlDataType::Int8),
            ("i16_value", &SqlDataType::Int16),
            ("i32_value", &SqlDataType::Int32),
            ("u8_value", &SqlDataType::UInt8),
            ("u16_value", &SqlDataType::UInt16),
            ("u32_value", &SqlDataType::UInt32),
            ("u64_value", &SqlDataType::UInt64),
            ("f32_value", &SqlDataType::Float32),
            ("code", &SqlDataType::Char { length: Some(8) },),
            ("raw", &SqlDataType::Binary { length: 3 }),
            ("bytes", &SqlDataType::Varbinary),
            ("event_time", &SqlDataType::Time),
            ("uuid_value", &SqlDataType::Uuid),
            ("amount", &SqlDataType::Int64),
            ("weight", &SqlDataType::Int64),
        ]
    );
}

#[test]
fn catalog_input_relation_schema_accepts_nested_feldera_input_types() {
    let catalog = nested_input_relation_catalog();

    let schema = catalog_input_relation_schema(&catalog).unwrap();

    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("id", &SqlDataType::Utf8),
            (
                "scores",
                &SqlDataType::Array {
                    element_type: Box::new(SqlDataType::Int64)
                }
            ),
            (
                "attributes",
                &SqlDataType::Map {
                    key_type: Box::new(SqlDataType::Utf8),
                    value_type: Box::new(SqlDataType::Int64)
                }
            ),
            (
                "profile",
                &SqlDataType::Struct {
                    fields: vec![
                        SqlStructField {
                            name: "name".to_string(),
                            data_type: SqlDataType::Utf8,
                            nullable: false,
                        },
                        SqlStructField {
                            name: "tier".to_string(),
                            data_type: SqlDataType::Int32,
                            nullable: true,
                        },
                    ]
                }
            ),
            ("amount", &SqlDataType::Int64),
            ("weight", &SqlDataType::Int64),
        ]
    );
}

#[test]
fn feldera_artifact_hash_verification_accepts_matching_artifact_bytes() {
    let spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_valid");
    let artifact_bytes = b"compiled Feldera artifact bytes";
    artifact.artifact_hash = feldera_artifact_bytes_hash(artifact_bytes);

    validate_feldera_compile_artifact_hash(&spec, &artifact, artifact_bytes).unwrap();
}

#[test]
fn feldera_artifact_hash_verification_rejects_mismatched_artifact_bytes() {
    let spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_valid");
    artifact.artifact_hash = feldera_artifact_bytes_hash(b"release artifact bytes");

    let error =
        validate_feldera_compile_artifact_hash(&spec, &artifact, b"different bytes").unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedArtifactHash { .. }
    ));
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
        schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["order_id", "amount", "weight"]
    );
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
fn feldera_catalog_validation_checks_every_input_catalog_for_join_views() {
    let orders = orders_relation_catalog();
    let customers = customers_relation_catalog();
    let spec = two_catalog_backed_standing_view_spec(&orders, &customers);
    let artifact = catalog_backed_artifact(&spec);

    validate_feldera_compile_artifact_for_catalogs(
        &[orders.clone(), customers.clone()],
        &spec,
        &artifact,
    )
    .unwrap();

    let mut drifted_customers = customers;
    drifted_customers
        .relation_schema
        .columns
        .push(RelationColumnV1 {
            column_id: "region".to_string(),
            name: "region".to_string(),
            logical_type: VelorixLogicalTypeV1::Utf8,
            physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
            nullable: true,
            ordinal: 3,
            semantic_role: RelationSemanticRoleV1::Value,
        });
    let drifted_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&drifted_customers.relation_schema).unwrap();
    drifted_customers.schema_fingerprint = drifted_fingerprint.clone();
    drifted_customers.feldera_relation.schema_fingerprint = drifted_fingerprint;

    let error = validate_feldera_compile_artifact_for_catalogs(
        &[orders, drifted_customers],
        &spec,
        &artifact,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::SchemaFingerprintMismatch {
            field: "spec.input_relations"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_excessively_deep_nested_sql_types() {
    let catalog = orders_relation_catalog();
    let mut spec = catalog_backed_standing_view_spec(&catalog);
    spec.output_relations[0].columns[1].data_type = nested_array_type(18);
    let artifact = catalog_backed_artifact(&spec);

    let error =
        validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::InvalidRelationSchema {
            field: "sql_type.depth"
        }
    ));
}

#[test]
fn feldera_artifact_accepts_nested_sql_types_within_limits() {
    let catalog = orders_relation_catalog();
    let mut spec = catalog_backed_standing_view_spec(&catalog);
    spec.output_relations[0].columns[1].data_type = SqlDataType::Struct {
        fields: vec![SqlStructField {
            name: "totals".to_string(),
            data_type: nested_array_type(3),
            nullable: false,
        }],
    };
    let artifact = catalog_backed_artifact(&spec);

    validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap();
}

#[test]
fn feldera_artifact_rejects_oversized_struct_types() {
    let catalog = orders_relation_catalog();
    let mut spec = catalog_backed_standing_view_spec(&catalog);
    spec.output_relations[0].columns[1].data_type = SqlDataType::Struct {
        fields: (0..257)
            .map(|index| SqlStructField {
                name: format!("field_{index}"),
                data_type: SqlDataType::Int64,
                nullable: false,
            })
            .collect(),
    };
    let artifact = catalog_backed_artifact(&spec);

    let error =
        validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::InvalidRelationSchema {
            field: "struct.fields"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_oversized_relation_column_sets() {
    let catalog = orders_relation_catalog();
    let mut spec = catalog_backed_standing_view_spec(&catalog);
    spec.output_relations[0].columns = (0..1025)
        .map(|index| velorix_core::feldera_artifact::ColumnSchema {
            name: format!("c_{index}"),
            data_type: SqlDataType::Int64,
            nullable: false,
        })
        .collect();
    let artifact = catalog_backed_artifact(&spec);

    let error =
        validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::InvalidRelationSchema { field: "columns" }
    ));
}

#[test]
fn feldera_artifact_rejects_oversized_sql_type_trees() {
    let catalog = orders_relation_catalog();
    let mut spec = catalog_backed_standing_view_spec(&catalog);
    spec.output_relations[0].columns[1].data_type = balanced_map_type(12);
    let artifact = catalog_backed_artifact(&spec);

    let error =
        validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::InvalidRelationSchema {
            field: "sql_type.nodes"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_oversized_timezone_strings() {
    let catalog = orders_relation_catalog();
    let mut spec = catalog_backed_standing_view_spec(&catalog);
    spec.output_relations[0].columns[1].data_type = SqlDataType::Timestamp {
        timezone: Some("A".repeat(129)),
    };
    let artifact = catalog_backed_artifact(&spec);

    let error =
        validate_feldera_compile_artifact_for_catalog(&catalog, &spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::InvalidRelationSchema {
            field: "timestamp.timezone"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_unsupported_metadata_version() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_invalid_version");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedMetadataVersion { version: 999 }
    ));
}

fn nested_array_type(depth: usize) -> SqlDataType {
    (0..depth).fold(SqlDataType::Int64, |element_type, _| SqlDataType::Array {
        element_type: Box::new(element_type),
    })
}

fn balanced_map_type(depth: usize) -> SqlDataType {
    if depth == 0 {
        return SqlDataType::Int64;
    }
    let child = balanced_map_type(depth - 1);
    SqlDataType::Map {
        key_type: Box::new(child.clone()),
        value_type: Box::new(child),
    }
}

fn column(name: &str, data_type: SqlDataType, nullable: bool) -> ColumnSchema {
    ColumnSchema {
        name: name.to_string(),
        data_type,
        nullable,
    }
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

fn customers_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "customers".to_string(),
        relation_name: "customers".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "customer_id".to_string(),
                name: "customer_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "name".to_string(),
                name: "name".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: true,
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
        primary_key_column_ids: vec!["customer_id".to_string()],
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
            name: "customers".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "customers".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-customers-v1".to_string(),
        },
    }
}

fn expanded_scalar_relation_catalog() -> VelorixRelationCatalogV1 {
    let columns = vec![
        relation_column(
            "id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::PrimaryKey,
            0,
        ),
        relation_column(
            "i8_value",
            VelorixLogicalTypeV1::Int8,
            ArrowPhysicalTypeV1::Int8,
            RelationSemanticRoleV1::Metadata,
            1,
        ),
        relation_column(
            "i16_value",
            VelorixLogicalTypeV1::Int16,
            ArrowPhysicalTypeV1::Int16,
            RelationSemanticRoleV1::Metadata,
            2,
        ),
        relation_column(
            "i32_value",
            VelorixLogicalTypeV1::Int32,
            ArrowPhysicalTypeV1::Int32,
            RelationSemanticRoleV1::Metadata,
            3,
        ),
        relation_column(
            "u8_value",
            VelorixLogicalTypeV1::UInt8,
            ArrowPhysicalTypeV1::UInt8,
            RelationSemanticRoleV1::Metadata,
            4,
        ),
        relation_column(
            "u16_value",
            VelorixLogicalTypeV1::UInt16,
            ArrowPhysicalTypeV1::UInt16,
            RelationSemanticRoleV1::Metadata,
            5,
        ),
        relation_column(
            "u32_value",
            VelorixLogicalTypeV1::UInt32,
            ArrowPhysicalTypeV1::UInt32,
            RelationSemanticRoleV1::Metadata,
            6,
        ),
        relation_column(
            "u64_value",
            VelorixLogicalTypeV1::UInt64,
            ArrowPhysicalTypeV1::UInt64,
            RelationSemanticRoleV1::Metadata,
            7,
        ),
        relation_column(
            "f32_value",
            VelorixLogicalTypeV1::Float32,
            ArrowPhysicalTypeV1::Float32,
            RelationSemanticRoleV1::Metadata,
            8,
        ),
        relation_column(
            "code",
            VelorixLogicalTypeV1::Char { length: Some(8) },
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::Metadata,
            9,
        ),
        relation_column(
            "raw",
            VelorixLogicalTypeV1::Binary { length: 3 },
            ArrowPhysicalTypeV1::Binary,
            RelationSemanticRoleV1::Metadata,
            10,
        ),
        relation_column(
            "bytes",
            VelorixLogicalTypeV1::Varbinary,
            ArrowPhysicalTypeV1::Binary,
            RelationSemanticRoleV1::Metadata,
            11,
        ),
        relation_column(
            "event_time",
            VelorixLogicalTypeV1::Time,
            ArrowPhysicalTypeV1::Time64Nanosecond,
            RelationSemanticRoleV1::Metadata,
            12,
        ),
        relation_column(
            "uuid_value",
            VelorixLogicalTypeV1::Uuid,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::Metadata,
            13,
        ),
        relation_column(
            "amount",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Value,
            14,
        ),
        relation_column(
            "weight",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Weight,
            15,
        ),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "expanded_scalars".to_string(),
        relation_name: "expanded_scalars".to_string(),
        relation_version: "2026-06-09.v1".to_string(),
        columns,
        primary_key_column_ids: vec!["id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "expanded_scalars".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "expanded_scalars".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-single-key-sum-count-v1".to_string(),
        },
    }
}

fn nested_input_relation_catalog() -> VelorixRelationCatalogV1 {
    let columns = vec![
        relation_column(
            "id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::PrimaryKey,
            0,
        ),
        relation_column(
            "scores",
            VelorixLogicalTypeV1::Array {
                element_type: Box::new(VelorixLogicalTypeV1::Int64),
            },
            ArrowPhysicalTypeV1::List {
                element_type: Box::new(ArrowPhysicalTypeV1::Int64),
            },
            RelationSemanticRoleV1::Metadata,
            1,
        ),
        relation_column(
            "attributes",
            VelorixLogicalTypeV1::Map {
                key_type: Box::new(VelorixLogicalTypeV1::Utf8),
                value_type: Box::new(VelorixLogicalTypeV1::Int64),
            },
            ArrowPhysicalTypeV1::Map {
                key_type: Box::new(ArrowPhysicalTypeV1::Utf8),
                value_type: Box::new(ArrowPhysicalTypeV1::Int64),
            },
            RelationSemanticRoleV1::Metadata,
            2,
        ),
        relation_column(
            "profile",
            VelorixLogicalTypeV1::Struct {
                fields: vec![
                    VelorixStructFieldV1 {
                        name: "name".to_string(),
                        logical_type: VelorixLogicalTypeV1::Utf8,
                        nullable: false,
                    },
                    VelorixStructFieldV1 {
                        name: "tier".to_string(),
                        logical_type: VelorixLogicalTypeV1::Int32,
                        nullable: true,
                    },
                ],
            },
            ArrowPhysicalTypeV1::Struct {
                fields: vec![
                    ArrowStructFieldV1 {
                        name: "name".to_string(),
                        physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                        nullable: false,
                    },
                    ArrowStructFieldV1 {
                        name: "tier".to_string(),
                        physical_arrow_type: ArrowPhysicalTypeV1::Int32,
                        nullable: true,
                    },
                ],
            },
            RelationSemanticRoleV1::Metadata,
            3,
        ),
        relation_column(
            "amount",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Value,
            4,
        ),
        relation_column(
            "weight",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Weight,
            5,
        ),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "nested_inputs".to_string(),
        relation_name: "nested_inputs".to_string(),
        relation_version: "2026-06-10.v1".to_string(),
        columns,
        primary_key_column_ids: vec!["id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "nested_inputs".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "nested_inputs".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-single-key-sum-count-v1".to_string(),
        },
    }
}

fn relation_column(
    name: &str,
    logical_type: VelorixLogicalTypeV1,
    physical_arrow_type: ArrowPhysicalTypeV1,
    semantic_role: RelationSemanticRoleV1,
    ordinal: u32,
) -> RelationColumnV1 {
    RelationColumnV1 {
        column_id: name.to_string(),
        name: name.to_string(),
        logical_type,
        physical_arrow_type,
        nullable: false,
        ordinal,
        semantic_role,
    }
}

fn catalog_backed_standing_view_spec(catalog: &VelorixRelationCatalogV1) -> StandingViewSpec {
    StandingViewSpec {
        view_id: "orders_sum".to_string(),
        sql: "select order_id, sum(amount) as total_amount from orders group by order_id"
            .to_string(),
        dialect: velorix_core::feldera_artifact::SqlDialect::FelderaSql,
        source_kind: velorix_core::feldera_artifact::SqlSourceKind::StandingView,
        rust_extension: Default::default(),
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

fn two_catalog_backed_standing_view_spec(
    orders: &VelorixRelationCatalogV1,
    customers: &VelorixRelationCatalogV1,
) -> StandingViewSpec {
    let mut spec = catalog_backed_standing_view_spec(orders);
    spec.sql = "select c.customer_id, sum(o.amount) as total_amount from orders o join customers c on o.order_id = c.customer_id group by c.customer_id".to_string();
    spec.input_relations
        .push(catalog_input_relation_schema(customers).unwrap());
    spec.shape.multi_input = true;
    spec
}

fn catalog_backed_artifact(spec: &StandingViewSpec) -> FelderaCompileArtifactMetadata {
    FelderaCompileArtifactMetadata {
        metadata_version: 1,
        view_id: spec.view_id.clone(),
        spec_hash: feldera_spec_hash(spec).unwrap(),
        compile_request_hash: None,
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
fn feldera_artifact_v2_accepts_matching_compile_request_hash() {
    let spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_valid");
    let compile_request_hash = feldera_compile_request_hash(
        &FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec),
    )
    .unwrap();
    artifact.metadata_version = 2;
    artifact.compile_request_hash = Some(compile_request_hash.clone());

    validate_feldera_compile_artifact_for_compile_request(&spec, &artifact, &compile_request_hash)
        .unwrap();
}

#[test]
fn feldera_artifact_v2_rejects_missing_compile_request_hash() {
    let spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_valid");
    artifact.metadata_version = 2;
    artifact.compile_request_hash = None;

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MissingIdentityField {
            field: "compile_request_hash"
        }
    ));
}

#[test]
fn feldera_artifact_v2_rejects_mismatched_compile_request_hash() {
    let spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_valid");
    let compile_request_hash = feldera_compile_request_hash(
        &FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec),
    )
    .unwrap();
    artifact.metadata_version = 2;
    artifact.compile_request_hash = Some(format!(
        "velorix-feldera-compile-request-sha256-v1:{}",
        "0".repeat(64)
    ));

    let error = validate_feldera_compile_artifact_for_compile_request(
        &spec,
        &artifact,
        &compile_request_hash,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedCompileRequestHash { .. }
    ));
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
fn feldera_artifact_rejects_invalid_artifact_hash() {
    let spec = load_spec("standing_view_spec_valid");

    for artifact_hash in [
        "not-a-sha",
        "sha256:not-hex",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        let mut artifact = load_artifact("compile_artifact_valid");
        artifact.artifact_hash = artifact_hash.to_string();

        let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

        assert!(matches!(
            error,
            FelderaArtifactError::InvalidArtifactHash {
                field: "artifact_hash"
            }
        ));
    }
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
fn feldera_release_provenance_accepts_matching_compile_metadata() {
    let artifact = load_artifact("compile_artifact_valid");
    let provenance = load_provenance("release_provenance_valid");

    validate_feldera_release_artifact_provenance(&artifact, &provenance).unwrap();
}

#[test]
fn feldera_release_provenance_rejects_artifact_identity_mismatch() {
    let artifact = load_artifact("compile_artifact_valid");
    let mut provenance = load_provenance("release_provenance_valid");
    provenance.build.artifact_hash = format!("sha256:{}", "9".repeat(64));

    let error = validate_feldera_release_artifact_provenance(&artifact, &provenance).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedReleaseProvenanceField {
            field: "build.artifact_hash"
        }
    ));
}

#[test]
fn feldera_release_provenance_rejects_unsupported_metadata_version() {
    let mut artifact = load_artifact("compile_artifact_valid");
    let provenance = load_provenance("release_provenance_valid");
    artifact.metadata_version = 999;

    let error = validate_feldera_release_artifact_provenance(&artifact, &provenance).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedMetadataVersion { version: 999 }
    ));
}

#[test]
fn feldera_release_provenance_rejects_generated_rust_identity_mismatch() {
    let artifact = load_artifact("compile_artifact_valid");
    let mut provenance = load_provenance("release_provenance_valid");
    provenance.build.generated_rust.crate_name = "different_pipeline".to_string();

    let error = validate_feldera_release_artifact_provenance(&artifact, &provenance).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedReleaseProvenanceField {
            field: "build.generated_rust.crate_name"
        }
    ));
}

#[test]
fn feldera_release_provenance_rejects_compiler_identity_mismatch() {
    let mut artifact = load_artifact("compile_artifact_valid");
    let provenance = load_provenance("release_provenance_valid");
    artifact.compiler.name = "different-compiler".to_string();

    let error = validate_feldera_release_artifact_provenance(&artifact, &provenance).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedReleaseProvenanceField {
            field: "provenance.compiler_name"
        }
    ));
}

#[test]
fn feldera_release_provenance_rejects_missing_release_identity() {
    let artifact = load_artifact("compile_artifact_valid");
    let mut provenance = load_provenance("release_provenance_valid");
    provenance.release.release_id.clear();

    let error = validate_feldera_release_artifact_provenance(&artifact, &provenance).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MissingReleaseProvenanceField {
            field: "release.release_id"
        }
    ));
}

#[test]
fn feldera_release_provenance_rejects_unknown_wire_fields() {
    let mut provenance: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_path("release_provenance_valid")).unwrap(),
    )
    .unwrap();
    provenance["surprise"] = serde_json::json!(true);
    let error: serde_json::Error =
        serde_json::from_value::<FelderaReleaseArtifactProvenanceV1>(provenance).unwrap_err();

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
fn feldera_artifact_accepts_multi_input_single_output_shape() {
    let mut spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_multi_input");
    spec.input_relations = artifact.input_schemas.clone();
    spec.shape.multi_input = true;
    artifact.spec_hash = feldera_spec_hash(&spec).unwrap();

    validate_feldera_compile_artifact(&spec, &artifact).unwrap();
}

#[test]
fn feldera_artifact_accepts_multi_output_shape_when_spec_matches() {
    let mut spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_multi_output");
    spec.input_relations = artifact.input_schemas.clone();
    spec.output_relations = artifact.output_schemas.clone();
    spec.shape.multi_output = true;
    artifact.spec_hash = feldera_spec_hash(&spec).unwrap();

    validate_feldera_compile_artifact(&spec, &artifact).unwrap();
}

#[test]
fn feldera_compile_request_accepts_multi_output_must_match_when_shape_matches() {
    let mut spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_multi_output");
    spec.input_relations = artifact.input_schemas.clone();
    spec.output_relations = artifact.output_schemas.clone();
    spec.shape.multi_output = true;
    let request = FelderaCompileRequestV1 {
        output_contract: OutputSchemaContract::MustMatch {
            output_relations: spec.output_relations.clone(),
        },
        ..FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec)
    };

    validate_feldera_compile_request(&request).unwrap();
}

#[test]
fn feldera_compile_request_rejects_multi_output_shape_mismatch() {
    let mut spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_multi_output");
    spec.input_relations = artifact.input_schemas.clone();
    spec.output_relations = artifact.output_schemas.clone();
    spec.shape.multi_output = false;
    let request = FelderaCompileRequestV1 {
        output_contract: OutputSchemaContract::MustMatch {
            output_relations: spec.output_relations.clone(),
        },
        ..FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec)
    };

    assert!(matches!(
        validate_feldera_compile_request(&request),
        Err(FelderaArtifactError::UnsupportedShape {
            shape: "compile_request.shape.multi_output"
        })
    ));
}

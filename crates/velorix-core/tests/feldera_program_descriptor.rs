#![cfg(feature = "feldera-package-compat")]

use std::collections::BTreeMap;

use feldera_types::program_schema::{
    ColumnType, Field, IntervalUnit, ProgramSchema, PropertyValue, Relation, SourcePosition,
    SqlIdentifier, SqlType,
};
use velorix_core::{
    feldera_artifact::{
        ColumnSchema, RelationSchema, SqlDataType, SqlDialect, SqlIntervalUnit, SqlSourceKind,
        SqlStructField, StandingViewShape, StandingViewSpec,
    },
    feldera_program_descriptor::{FelderaProgramDescriptor, FelderaProgramDescriptorError},
};

#[test]
fn feldera_program_schema_maps_standing_view_spec_into_feldera_descriptor_shape() {
    let spec = scalar_type_standing_view_spec(None);
    let schema =
        velorix_core::feldera_program_descriptor::feldera_program_schema_for_standing_view_spec(
            &spec,
        )
        .unwrap();

    assert_eq!(schema.inputs.len(), 1);
    assert_eq!(schema.outputs.len(), 1);
    assert_eq!(schema.inputs[0].name.name(), "events");
    assert_eq!(schema.outputs[0].name.name(), "events_by_id");
    assert!(!schema.inputs[0].materialized);
    assert!(schema.outputs[0].materialized);
    assert_eq!(
        schema.inputs[0].primary_key.clone().unwrap(),
        vec!["event_id".to_string()]
    );
    assert_eq!(
        schema.inputs[0].fields[2].columntype,
        ColumnType::decimal(18, 2, true)
    );
    assert_eq!(
        schema.inputs[0].fields[6].columntype,
        ColumnType::variant(true)
    );

    let validation = FelderaProgramDescriptor::new(schema)
        .validate_standing_view_spec(&spec)
        .unwrap();

    assert_eq!(validation.input_relations, vec!["events"]);
    assert_eq!(validation.output_relations, vec!["events_by_id"]);
}

#[test]
fn feldera_program_schema_rejects_timezone_timestamp_mapping_until_feldera_descriptor_supports_it()
{
    let error =
        velorix_core::feldera_program_descriptor::feldera_program_schema_for_standing_view_spec(
            &scalar_type_standing_view_spec(Some("UTC")),
        )
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::UnsupportedVelorixColumnType {
            relation: "events".to_string(),
            column: "event_time".to_string(),
            data_type: "Timestamp(timezone=UTC)".to_string(),
        }
    );
}

#[test]
fn feldera_program_schema_rejects_geometry_mapping_until_feldera_descriptor_supports_it() {
    let error =
        velorix_core::feldera_program_descriptor::feldera_program_schema_for_standing_view_spec(
            &geometry_type_standing_view_spec(),
        )
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::UnsupportedVelorixColumnType {
            relation: "shapes".to_string(),
            column: "shape".to_string(),
            data_type: "Geometry".to_string(),
        }
    );
}

#[test]
fn feldera_types_column_type_rejects_geometry_descriptor_json_until_sqltype_exists() {
    let error = serde_json::from_value::<ColumnType>(serde_json::json!({
        "type": "GEOMETRY",
        "nullable": true
    }))
    .unwrap_err();

    assert!(
        error.to_string().contains("Unknown SQL type: GEOMETRY"),
        "unexpected error: {error}"
    );
}

#[test]
fn feldera_program_descriptor_accepts_matching_standing_view_spec() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "scores",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("score", ColumnType::bigint(false)),
                field("delta", ColumnType::bigint(false)),
            ],
            false,
            &["user_id"],
        ),
        relation(
            "scores_by_user",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("sum", ColumnType::bigint(false)),
                field("count", ColumnType::bigint(false)),
            ],
            true,
            &["user_id"],
        ),
    ));

    let validation = descriptor
        .validate_standing_view_spec(&standing_view_spec())
        .unwrap();

    assert_eq!(validation.input_relations, vec!["scores"]);
    assert_eq!(validation.output_relations, vec!["scores_by_user"]);
}

#[test]
fn feldera_program_descriptor_rejects_unexpected_input_relation() {
    let descriptor = FelderaProgramDescriptor::new(ProgramSchema {
        inputs: vec![
            relation(
                "scores",
                vec![
                    field("user_id", ColumnType::varchar(false)),
                    field("score", ColumnType::bigint(false)),
                    field("delta", ColumnType::bigint(false)),
                ],
                false,
                &["user_id"],
            ),
            relation(
                "external_scores",
                vec![field("payload", ColumnType::varchar(false))],
                false,
                &[],
            ),
        ],
        outputs: vec![relation(
            "scores_by_user",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("sum", ColumnType::bigint(false)),
                field("count", ColumnType::bigint(false)),
            ],
            true,
            &["user_id"],
        )],
    });

    let error = descriptor
        .validate_standing_view_spec(&standing_view_spec())
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::UnexpectedRelation {
            kind: "input",
            relation: "external_scores".to_string(),
        }
    );
}

#[test]
fn feldera_program_descriptor_rejects_relation_properties_as_unmanaged_io() {
    let mut properties = BTreeMap::new();
    properties.insert("connectors".to_string(), property_value("kafka://scores"));
    let descriptor = FelderaProgramDescriptor::new(ProgramSchema {
        inputs: vec![relation_with_properties(
            "scores",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("score", ColumnType::bigint(false)),
                field("delta", ColumnType::bigint(false)),
            ],
            false,
            &["user_id"],
            properties,
        )],
        outputs: vec![relation(
            "scores_by_user",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("sum", ColumnType::bigint(false)),
                field("count", ColumnType::bigint(false)),
            ],
            true,
            &["user_id"],
        )],
    });

    let error = descriptor
        .validate_standing_view_spec(&standing_view_spec())
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::RelationHasProperties {
            kind: "input",
            relation: "scores".to_string(),
            properties: vec!["connectors".to_string()],
        }
    );
}

#[test]
fn feldera_program_descriptor_rejects_schema_drift() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "scores",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("score", ColumnType::varchar(false)),
                field("delta", ColumnType::bigint(false)),
            ],
            false,
            &["user_id"],
        ),
        relation(
            "scores_by_user",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("sum", ColumnType::bigint(false)),
                field("count", ColumnType::bigint(false)),
            ],
            true,
            &["user_id"],
        ),
    ));

    let error = descriptor
        .validate_standing_view_spec(&standing_view_spec())
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::ColumnTypeMismatch {
            relation: "scores".to_string(),
            column: "score".to_string(),
            expected: "Int64".to_string(),
            actual: "Varchar".to_string(),
        }
    );
}

#[test]
fn feldera_program_descriptor_accepts_supported_scalar_type_details() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "events",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("enabled", ColumnType::boolean(false)),
                field("amount", ColumnType::decimal(18, 2, true)),
                field("ratio", ColumnType::double(false)),
                field("event_date", ColumnType::date(true)),
                field("event_time", ColumnType::timestamp(false)),
                field("payload", ColumnType::variant(true)),
            ],
            false,
            &["event_id"],
        ),
        relation(
            "events_by_id",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("amount", ColumnType::decimal(18, 2, true)),
                field("payload", ColumnType::variant(true)),
            ],
            true,
            &["event_id"],
        ),
    ));

    let validation = descriptor
        .validate_standing_view_spec(&scalar_type_standing_view_spec(None))
        .unwrap();

    assert_eq!(validation.input_relations, vec!["events"]);
    assert_eq!(validation.output_relations, vec!["events_by_id"]);
}

#[test]
fn feldera_program_descriptor_rejects_decimal_precision_or_scale_drift() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "events",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("enabled", ColumnType::boolean(false)),
                field("amount", ColumnType::decimal(18, 3, true)),
                field("ratio", ColumnType::double(false)),
                field("event_date", ColumnType::date(true)),
                field("event_time", ColumnType::timestamp(false)),
                field("payload", ColumnType::variant(true)),
            ],
            false,
            &["event_id"],
        ),
        relation(
            "events_by_id",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("amount", ColumnType::decimal(18, 2, true)),
                field("payload", ColumnType::variant(true)),
            ],
            true,
            &["event_id"],
        ),
    ));

    let error = descriptor
        .validate_standing_view_spec(&scalar_type_standing_view_spec(None))
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::ColumnTypeMismatch {
            relation: "events".to_string(),
            column: "amount".to_string(),
            expected: "Decimal(18,2)".to_string(),
            actual: "Decimal(18,3)".to_string(),
        }
    );
}

#[test]
fn feldera_program_descriptor_rejects_timestamp_timezone_when_descriptor_has_no_timezone() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "events",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("enabled", ColumnType::boolean(false)),
                field("amount", ColumnType::decimal(18, 2, true)),
                field("ratio", ColumnType::double(false)),
                field("event_date", ColumnType::date(true)),
                field("event_time", ColumnType::timestamp(false)),
                field("payload", ColumnType::variant(true)),
            ],
            false,
            &["event_id"],
        ),
        relation(
            "events_by_id",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("amount", ColumnType::decimal(18, 2, true)),
                field("payload", ColumnType::variant(true)),
            ],
            true,
            &["event_id"],
        ),
    ));

    let error = descriptor
        .validate_standing_view_spec(&scalar_type_standing_view_spec(Some("UTC")))
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::ColumnTypeMismatch {
            relation: "events".to_string(),
            column: "event_time".to_string(),
            expected: "Timestamp(timezone=UTC)".to_string(),
            actual: "Timestamp".to_string(),
        }
    );
}

#[test]
fn feldera_program_descriptor_rejects_supported_array_descriptor_type_drift() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "events",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("enabled", ColumnType::boolean(false)),
                field("amount", ColumnType::decimal(18, 2, true)),
                field("ratio", ColumnType::double(false)),
                field("event_date", ColumnType::date(true)),
                field("event_time", ColumnType::timestamp(false)),
                field(
                    "payload",
                    ColumnType::array(true, ColumnType::varchar(false)),
                ),
            ],
            false,
            &["event_id"],
        ),
        relation(
            "events_by_id",
            vec![
                field("event_id", ColumnType::varchar(false)),
                field("amount", ColumnType::decimal(18, 2, true)),
                field("payload", ColumnType::variant(true)),
            ],
            true,
            &["event_id"],
        ),
    ));

    let error = descriptor
        .validate_standing_view_spec(&scalar_type_standing_view_spec(None))
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::ColumnTypeMismatch {
            relation: "events".to_string(),
            column: "payload".to_string(),
            expected: "Variant".to_string(),
            actual: "Array<Varchar>".to_string(),
        }
    );
}

#[test]
fn feldera_program_descriptor_rejects_supported_narrow_integer_descriptor_type_drift() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "scores",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("score", ColumnType::int(false)),
                field("delta", ColumnType::bigint(false)),
            ],
            false,
            &["user_id"],
        ),
        relation(
            "scores_by_user",
            vec![
                field("user_id", ColumnType::varchar(false)),
                field("sum", ColumnType::bigint(false)),
                field("count", ColumnType::bigint(false)),
            ],
            true,
            &["user_id"],
        ),
    ));

    let error = descriptor
        .validate_standing_view_spec(&standing_view_spec())
        .unwrap_err();

    assert_eq!(
        error,
        FelderaProgramDescriptorError::ColumnTypeMismatch {
            relation: "scores".to_string(),
            column: "score".to_string(),
            expected: "Int64".to_string(),
            actual: "Int32".to_string(),
        }
    );
}

#[test]
fn feldera_program_descriptor_accepts_expanded_feldera_sql_type_surface() {
    let descriptor = FelderaProgramDescriptor::new(program_schema(
        relation(
            "wide_events",
            vec![
                field("id", ColumnType::varchar(false)),
                field("i8_value", ColumnType::tinyint(false)),
                field("i16_value", ColumnType::smallint(false)),
                field("i32_value", ColumnType::int(false)),
                field("u8_value", ColumnType::utinyint(false)),
                field("u16_value", ColumnType::usmallint(false)),
                field("u32_value", ColumnType::uint(false)),
                field("u64_value", ColumnType::ubigint(false)),
                field("f32_value", ColumnType::real(false)),
                field(
                    "char_value",
                    ColumnType {
                        typ: SqlType::Char,
                        nullable: true,
                        precision: Some(8),
                        scale: None,
                        component: None,
                        fields: None,
                        key: None,
                        value: None,
                    },
                ),
                field("binary_value", ColumnType::fixed(16, true)),
                field("bytes_value", ColumnType::varbinary(true)),
                field("time_value", ColumnType::time(true)),
                field(
                    "interval_value",
                    ColumnType {
                        typ: SqlType::Interval(IntervalUnit::DayToSecond),
                        nullable: true,
                        precision: None,
                        scale: None,
                        component: None,
                        fields: None,
                        key: None,
                        value: None,
                    },
                ),
                field("tags", ColumnType::array(true, ColumnType::varchar(false))),
                field(
                    "attributes",
                    ColumnType::map(true, ColumnType::varchar(false), ColumnType::bigint(true)),
                ),
                field(
                    "nested",
                    ColumnType::structure(
                        true,
                        &[
                            field("inner_name", ColumnType::varchar(false)),
                            field("inner_count", ColumnType::bigint(true)),
                        ],
                    ),
                ),
                field("uuid_value", ColumnType::uuid(true)),
                field(
                    "null_value",
                    ColumnType {
                        typ: SqlType::Null,
                        nullable: true,
                        precision: None,
                        scale: None,
                        component: None,
                        fields: None,
                        key: None,
                        value: None,
                    },
                ),
            ],
            false,
            &["id"],
        ),
        relation(
            "wide_events_view",
            vec![
                field("id", ColumnType::varchar(false)),
                field("tags", ColumnType::array(true, ColumnType::varchar(false))),
                field("uuid_value", ColumnType::uuid(true)),
            ],
            true,
            &["id"],
        ),
    ));

    let validation = descriptor
        .validate_standing_view_spec(&expanded_feldera_type_standing_view_spec())
        .unwrap();

    assert_eq!(validation.input_relations, vec!["wide_events"]);
    assert_eq!(validation.output_relations, vec!["wide_events_view"]);
}

fn program_schema(input: Relation, output: Relation) -> ProgramSchema {
    ProgramSchema {
        inputs: vec![input],
        outputs: vec![output],
    }
}

fn relation(name: &str, fields: Vec<Field>, materialized: bool, primary_key: &[&str]) -> Relation {
    relation_with_properties(name, fields, materialized, primary_key, BTreeMap::new())
}

fn relation_with_properties(
    name: &str,
    fields: Vec<Field>,
    materialized: bool,
    primary_key: &[&str],
    properties: BTreeMap<String, PropertyValue>,
) -> Relation {
    let ids = primary_key
        .iter()
        .map(|name| SqlIdentifier::from(*name))
        .collect::<Vec<_>>();
    Relation::new(SqlIdentifier::from(name), fields, materialized, properties)
        .with_primary_key(&ids)
}

fn field(name: &str, columntype: ColumnType) -> Field {
    Field::new(SqlIdentifier::from(name), columntype)
}

fn property_value(value: &str) -> PropertyValue {
    let position = SourcePosition {
        start_line_number: 0,
        start_column: 0,
        end_line_number: 0,
        end_column: 0,
    };
    PropertyValue {
        value: value.to_string(),
        key_position: position,
        value_position: position,
    }
}

fn standing_view_spec() -> StandingViewSpec {
    StandingViewSpec {
        view_id: "scores_by_user".to_string(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "1".repeat(64)),
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
                ColumnSchema {
                    name: "delta".to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["user_id".to_string()],
        }],
        output_relations: vec![RelationSchema {
            relation_id: "scores_by_user".to_string(),
            relation_name: "scores_by_user".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
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
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn scalar_type_standing_view_spec(timezone: Option<&str>) -> StandingViewSpec {
    StandingViewSpec {
        view_id: "events_by_id".to_string(),
        sql: "select event_id, amount, payload from events".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "events".to_string(),
            relation_name: "events".to_string(),
            relation_version: "2026-05-30.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
            columns: vec![
                ColumnSchema {
                    name: "event_id".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "enabled".to_string(),
                    data_type: SqlDataType::Bool,
                    nullable: false,
                },
                ColumnSchema {
                    name: "amount".to_string(),
                    data_type: SqlDataType::Decimal {
                        precision: 18,
                        scale: 2,
                    },
                    nullable: true,
                },
                ColumnSchema {
                    name: "ratio".to_string(),
                    data_type: SqlDataType::Float64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "event_date".to_string(),
                    data_type: SqlDataType::Date,
                    nullable: true,
                },
                ColumnSchema {
                    name: "event_time".to_string(),
                    data_type: SqlDataType::Timestamp {
                        timezone: timezone.map(ToString::to_string),
                    },
                    nullable: false,
                },
                ColumnSchema {
                    name: "payload".to_string(),
                    data_type: SqlDataType::Json,
                    nullable: true,
                },
            ],
            primary_key: vec!["event_id".to_string()],
        }],
        output_relations: vec![RelationSchema {
            relation_id: "events_by_id".to_string(),
            relation_name: "events_by_id".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "4".repeat(64)),
            columns: vec![
                ColumnSchema {
                    name: "event_id".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "amount".to_string(),
                    data_type: SqlDataType::Decimal {
                        precision: 18,
                        scale: 2,
                    },
                    nullable: true,
                },
                ColumnSchema {
                    name: "payload".to_string(),
                    data_type: SqlDataType::Json,
                    nullable: true,
                },
            ],
            primary_key: vec!["event_id".to_string()],
        }],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn expanded_feldera_type_standing_view_spec() -> StandingViewSpec {
    StandingViewSpec {
        view_id: "wide_events_view".to_string(),
        sql: "select id, tags, uuid_value from wide_events".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "wide_events".to_string(),
            relation_name: "wide_events".to_string(),
            relation_version: "2026-06-07.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "5".repeat(64)),
            columns: vec![
                column("id", SqlDataType::Utf8, false),
                column("i8_value", SqlDataType::Int8, false),
                column("i16_value", SqlDataType::Int16, false),
                column("i32_value", SqlDataType::Int32, false),
                column("u8_value", SqlDataType::UInt8, false),
                column("u16_value", SqlDataType::UInt16, false),
                column("u32_value", SqlDataType::UInt32, false),
                column("u64_value", SqlDataType::UInt64, false),
                column("f32_value", SqlDataType::Float32, false),
                column("char_value", SqlDataType::Char { length: Some(8) }, true),
                column("binary_value", SqlDataType::Binary { length: 16 }, true),
                column("bytes_value", SqlDataType::Varbinary, true),
                column("time_value", SqlDataType::Time, true),
                column(
                    "interval_value",
                    SqlDataType::Interval {
                        unit: SqlIntervalUnit::DayToSecond,
                    },
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
                column("null_value", SqlDataType::Null, true),
            ],
            primary_key: vec!["id".to_string()],
        }],
        output_relations: vec![RelationSchema {
            relation_id: "wide_events_view".to_string(),
            relation_name: "wide_events_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "6".repeat(64)),
            columns: vec![
                column("id", SqlDataType::Utf8, false),
                column(
                    "tags",
                    SqlDataType::Array {
                        element_type: Box::new(SqlDataType::Utf8),
                    },
                    true,
                ),
                column("uuid_value", SqlDataType::Uuid, true),
            ],
            primary_key: vec!["id".to_string()],
        }],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn geometry_type_standing_view_spec() -> StandingViewSpec {
    StandingViewSpec {
        view_id: "shapes_by_id".to_string(),
        sql: "select shape_id, shape from shapes".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![RelationSchema {
            relation_id: "shapes".to_string(),
            relation_name: "shapes".to_string(),
            relation_version: "2026-06-10.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "7".repeat(64)),
            columns: vec![
                column("shape_id", SqlDataType::Utf8, false),
                column("shape", SqlDataType::Geometry, true),
            ],
            primary_key: vec!["shape_id".to_string()],
        }],
        output_relations: vec![RelationSchema {
            relation_id: "shapes_by_id".to_string(),
            relation_name: "shapes_by_id".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "8".repeat(64)),
            columns: vec![
                column("shape_id", SqlDataType::Utf8, false),
                column("shape", SqlDataType::Geometry, true),
            ],
            primary_key: vec!["shape_id".to_string()],
        }],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn column(name: &str, data_type: SqlDataType, nullable: bool) -> ColumnSchema {
    ColumnSchema {
        name: name.to_string(),
        data_type,
        nullable,
    }
}

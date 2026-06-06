#![cfg(feature = "feldera-package-compat")]

use std::collections::BTreeMap;

use feldera_types::program_schema::{ColumnType, Field, ProgramSchema, Relation, SqlIdentifier};
use velorix_core::{
    feldera_artifact::{
        ColumnSchema, RelationSchema, SqlDataType, SqlDialect, SqlSourceKind, StandingViewShape,
        StandingViewSpec,
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
fn feldera_program_descriptor_rejects_unsupported_descriptor_type_explicitly() {
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
        FelderaProgramDescriptorError::UnsupportedColumnType {
            relation: "events".to_string(),
            column: "payload".to_string(),
            actual: "Array<Varchar>".to_string(),
        }
    );
}

#[test]
fn feldera_program_descriptor_rejects_narrow_integer_descriptor_type_explicitly() {
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
        FelderaProgramDescriptorError::UnsupportedColumnType {
            relation: "scores".to_string(),
            column: "score".to_string(),
            actual: "Int32".to_string(),
        }
    );
}

fn program_schema(input: Relation, output: Relation) -> ProgramSchema {
    ProgramSchema {
        inputs: vec![input],
        outputs: vec![output],
    }
}

fn relation(name: &str, fields: Vec<Field>, materialized: bool, primary_key: &[&str]) -> Relation {
    let ids = primary_key
        .iter()
        .map(|name| SqlIdentifier::from(*name))
        .collect::<Vec<_>>();
    Relation::new(
        SqlIdentifier::from(name),
        fields,
        materialized,
        BTreeMap::new(),
    )
    .with_primary_key(&ids)
}

fn field(name: &str, columntype: ColumnType) -> Field {
    Field::new(SqlIdentifier::from(name), columntype)
}

fn standing_view_spec() -> StandingViewSpec {
    StandingViewSpec {
        view_id: "scores_by_user".to_string(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
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

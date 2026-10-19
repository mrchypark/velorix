use velorix_core::relation::*;
use velorix_core::view_plan::{validate_supported_filter_project_sql, RuntimeScalarTypeV1};
use velorix_runtime::materialized_view_runtime::expression_eval::{
    evaluate_typed_expr, RuntimeScalarValue as V,
};

fn catalog() -> VelorixRelationCatalogV1 {
    let columns = [
        (
            "id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            false,
        ),
        (
            "x",
            VelorixLogicalTypeV1::Float64,
            ArrowPhysicalTypeV1::Float64,
            true,
        ),
        (
            "y",
            VelorixLogicalTypeV1::Float64,
            ArrowPhysicalTypeV1::Float64,
            true,
        ),
        (
            "payload",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            true,
        ),
        (
            "ts",
            VelorixLogicalTypeV1::Timestamp { timezone: None },
            ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
            false,
        ),
        (
            "delta",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            false,
        ),
    ]
    .into_iter()
    .enumerate()
    .map(
        |(ordinal, (name, logical_type, physical_arrow_type, nullable))| RelationColumnV1 {
            column_id: name.into(),
            name: name.into(),
            logical_type,
            physical_arrow_type,
            nullable,
            ordinal: ordinal as u32,
            semantic_role: match name {
                "id" => RelationSemanticRoleV1::PrimaryKey,
                "delta" => RelationSemanticRoleV1::Weight,
                _ => RelationSemanticRoleV1::Value,
            },
        },
    )
    .collect();
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "events".into(),
        relation_name: "events".into(),
        relation_version: "v1".into(),
        columns,
        primary_key_column_ids: vec!["id".into()],
        weight_column_id: "delta".into(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: Some("ts".into()),
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        relation_source: VelorixRelationSourceV1::SourceRelation,
        datafusion_registration: DataFusionRegistrationV1 {
            name: "events".into(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "events".into(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        },
    }
}

fn eval(expression: &str, x: V, y: V, payload: &str, ts: i64) -> V {
    let plan = validate_supported_filter_project_sql(
        &format!("SELECT id, {expression} AS result FROM events"),
        &catalog(),
    )
    .unwrap();
    let program = &plan.typed_value_columns[0].program;
    let restored = serde_json::from_str(&serde_json::to_string(program).unwrap()).unwrap();
    evaluate_typed_expr(&restored, &|name| match name {
        "x" => Some(x.clone()),
        "y" => Some(y.clone()),
        "payload" => Some(V::Utf8(payload.into())),
        "ts" => Some(V::TimestampNanosecond(ts)),
        _ => None,
    })
    .unwrap()
}

#[test]
fn nullable_boolean_and_lazy_case_use_shared_evaluation() {
    let null = V::Null(RuntimeScalarTypeV1::Float64);
    assert_eq!(
        eval("x > y", null.clone(), V::Float64(2.0), "", 0),
        V::Null(RuntimeScalarTypeV1::Boolean)
    );
    assert_eq!(
        eval("x IS NULL OR x > y", null.clone(), V::Float64(2.0), "", 0),
        V::Boolean(true)
    );
    assert_eq!(
        eval(
            "NOT (x BETWEEN 1.0 AND 3.0)",
            V::Float64(2.0),
            null.clone(),
            "",
            0
        ),
        V::Boolean(false)
    );
    assert_eq!(
        eval(
            "CASE WHEN x IS NULL THEN 4.0 ELSE 1.0 / x END",
            null.clone(),
            null.clone(),
            "",
            0
        ),
        V::Float64(4.0)
    );
    assert_eq!(
        eval(
            "CASE WHEN x IS NULL THEN NULL ELSE x END",
            null.clone(),
            null.clone(),
            "",
            0
        ),
        null
    );
    assert_eq!(
        eval(
            "CASE WHEN x > 1.0 THEN TRUE ELSE FALSE END",
            V::Float64(2.0),
            V::Float64(0.0),
            "",
            0
        ),
        V::Boolean(true)
    );
}

#[test]
fn json_boolean_cast_matches_nullable_string_contract() {
    for (text, expected) in [
        ("yes", Some(true)),
        ("N", Some(false)),
        (" TRUE ", None),
        ("bad", None),
    ] {
        let value = eval(
            "TRY_CAST(json_extract_string(payload, '$.flag') AS BOOLEAN)",
            V::Float64(0.0),
            V::Float64(0.0),
            &format!("{{\"flag\":\"{text}\"}}"),
            0,
        );
        assert_eq!(
            value,
            expected
                .map(V::Boolean)
                .unwrap_or(V::Null(RuntimeScalarTypeV1::Boolean))
        );
    }
}

#[test]
fn temporal_aliases_round_at_microseconds_and_preserve_bucket_edges() {
    let scalar = |expr: &str, ts| eval(expr, V::Float64(0.0), V::Float64(0.0), "", ts);
    assert_eq!(
        scalar("to_timestamp(0.0000005)", 0),
        V::TimestampNanosecond(0)
    );
    assert_eq!(
        scalar("to_timestamp(0.0000015)", 0),
        V::TimestampNanosecond(2_000)
    );
    assert_eq!(
        scalar("CAST(ts AS TIMESTAMP)", 1_999),
        V::TimestampNanosecond(1_000)
    );
    assert_eq!(
        scalar("ts + 0.0000015 * INTERVAL '1 second'", 0),
        V::TimestampNanosecond(2_000)
    );
    assert_eq!(
        scalar("time_bucket('2 minutes', ts)", 121_000_000_000),
        V::TimestampNanosecond(120_000_000_000)
    );
    assert_eq!(
        scalar("time_bucket('7 minutes', ts)", 0),
        V::TimestampNanosecond(-60_000_000_000)
    );
    assert_eq!(scalar("epoch(ts)", 1_500_000_000), V::Float64(1.5));
    assert_eq!(
        scalar("strftime(ts, '%H:%M:%S')", -1_000_000_000),
        V::Utf8("23:59:59".into())
    );
    assert_eq!(
        scalar("greatest(ts, ts - INTERVAL '1 second')", 0),
        V::TimestampNanosecond(0)
    );
}

#[test]
fn integer_string_cast_and_concat_preserve_large_identifiers() {
    assert_eq!(
        eval("CAST(length(payload) AS VARCHAR) || ':' || payload || ':' || CAST(9007199254740993 AS VARCHAR)", V::Float64(0.0), V::Float64(0.0), "abc", 0),
        V::Utf8("3:abc:9007199254740993".into())
    );
}

#[test]
fn json_text_arrow_reuses_extraction_and_nullable_cast() {
    for (payload, expected) in [
        (r#"{"reading":2.5}"#, V::Float64(2.5)),
        (r#"{"reading":"3.25"}"#, V::Float64(3.25)),
        (
            r#"{"reading":"bad"}"#,
            V::Null(RuntimeScalarTypeV1::Float64),
        ),
        (r#"{"reading":null}"#, V::Null(RuntimeScalarTypeV1::Float64)),
        (r#"{}"#, V::Null(RuntimeScalarTypeV1::Float64)),
    ] {
        for expression in [
            "TRY_CAST(payload ->> 'reading' AS DOUBLE)",
            "TRY_CAST(json_extract_string(payload, '$.reading') AS DOUBLE)",
        ] {
            assert_eq!(
                eval(expression, V::Float64(0.0), V::Float64(0.0), payload, 0),
                expected
            );
        }
    }
    for expression in ["payload ->> id", "payload ->> 'a.b'", "payload ->> '$.*'"] {
        assert!(validate_supported_filter_project_sql(
            &format!("SELECT id, {expression} AS result FROM events"),
            &catalog()
        )
        .is_err());
    }
}

#[test]
fn typed_map_admits_temporal_cast_and_json_arrow_independently_and_together() {
    use velorix_core::view_plan::validate_supported_typed_map_sql;
    let temporal = "(TO_TIMESTAMP(x / 1000000000.0) + INTERVAL '2 hours')::TIMESTAMP";
    let json = "TRY_CAST(payload ->> 'reading' AS DOUBLE)";
    for projection in [
        format!("{temporal} AS instant"),
        format!("{json} AS reading"),
        format!("id, {temporal} AS instant, {json} AS reading, payload IS NOT NULL AS present, TRY_CAST(json_extract_string(payload, '$.flag') AS BOOLEAN) AS flag"),
    ] {
        let sql = format!("SELECT {projection} FROM events");
        validate_supported_typed_map_sql(&sql, &catalog())
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    assert_eq!(
        eval(temporal, V::Float64(1_500.0), V::Float64(0.0), "{}", 0),
        V::TimestampNanosecond(7_200_000_002_000)
    );
}

#[test]
fn existing_filter_atoms_and_independent_nullable_aggregates_remain_admitted() {
    validate_supported_filter_project_sql(
        "SELECT id, x FROM events WHERE x > 1.5 AND y IS NOT NULL",
        &catalog(),
    )
    .unwrap();
    velorix_core::view_plan::validate_supported_view_sql("SELECT id, AVG(x) AS a, COUNT(x) AS n, SUM(y) AS b, COUNT(y) AS m, COUNT(*) AS c FROM events GROUP BY id", &catalog()).unwrap();
    assert!(validate_supported_filter_project_sql(
        "SELECT id, strftime(ts, '%Y') AS result FROM events",
        &catalog()
    )
    .is_err());
    assert!(validate_supported_filter_project_sql(
        "SELECT id, ts > payload AS result FROM events",
        &catalog()
    )
    .is_err());
}

#[test]
fn typed_map_numeric_identity_casts_and_integer_nullif_are_exact() {
    use velorix_core::view_plan::validate_supported_typed_map_sql;
    for (expression, expected) in [
        (
            "CAST(9007199254740993 AS BIGINT)",
            V::Int64(9_007_199_254_740_993),
        ),
        ("CAST(1.25 AS DOUBLE)", V::Float64(1.25)),
        (
            "COALESCE(NULLIF(7,7),9007199254740993)",
            V::Int64(9_007_199_254_740_993),
        ),
        (
            "COALESCE(NULLIF(7,7),NULLIF(8,8))",
            V::Null(RuntimeScalarTypeV1::Int64),
        ),
        (
            "NULLIF(9007199254740993,9007199254740992)",
            V::Int64(9_007_199_254_740_993),
        ),
        ("NULLIF(7,7)", V::Null(RuntimeScalarTypeV1::Int64)),
        (
            "CAST(NULLIF(7,7) AS BIGINT)",
            V::Null(RuntimeScalarTypeV1::Int64),
        ),
        ("12.0 / NULLIF(3,0)", V::Float64(4.0)),
        ("12.0 / NULLIF(0,0)", V::Null(RuntimeScalarTypeV1::Float64)),
        (
            "LEAST(GREATEST(CASE WHEN 2 > 1 THEN 1 ELSE 0.5 END,0.0),1.0)",
            V::Float64(1.0),
        ),
        ("COALESCE(NULL,2.5)", V::Float64(2.5)),
        ("(9007199254740993-9007199254740992)/1e9", V::Float64(1e-9)),
    ] {
        let sql = format!("SELECT {expression} AS result FROM events");
        let plan = validate_supported_typed_map_sql(&sql, &catalog())
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let restored =
            serde_json::from_str(&serde_json::to_string(&plan.projection[0]).unwrap()).unwrap();
        assert_eq!(
            evaluate_typed_expr(&restored, &|_| None).unwrap(),
            expected,
            "{expression}"
        );
    }
    for expression in ["CAST(payload AS DOUBLE)", "CAST(x AS BIGINT)"] {
        assert!(validate_supported_typed_map_sql(
            &format!("SELECT {expression} AS result FROM events"),
            &catalog()
        )
        .is_err());
    }
}

#[test]
fn typed_map_coalesce_preserves_scalar_types_nulls_and_short_circuiting() {
    use velorix_core::view_plan::validate_supported_typed_map_sql;
    let mut dates = catalog();
    let date = dates
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.name == "ts")
        .unwrap();
    date.logical_type = VelorixLogicalTypeV1::Date;
    date.physical_arrow_type = ArrowPhysicalTypeV1::Date32;
    date.nullable = true;
    dates.relation_schema.event_time_column_id = None;
    dates.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&dates.relation_schema).unwrap();
    dates.incremental_relation.schema_fingerprint = dates.schema_fingerprint.clone();
    let date_plan = validate_supported_typed_map_sql(
        "SELECT COALESCE(NULL,ts,ts) AS value FROM events",
        &dates,
    )
    .unwrap();
    for value in [V::Date32(-123), V::Null(RuntimeScalarTypeV1::Date32)] {
        assert_eq!(
            evaluate_typed_expr(&date_plan.projection[0], &|_| Some(value.clone())).unwrap(),
            value
        );
    }
    for (expr, expected) in [
        ("COALESCE(NULL,payload,'fallback')", V::Utf8("value".into())),
        (
            "COALESCE(NULL,ts,ts)",
            V::TimestampNanosecond(9_007_199_254_740_993),
        ),
        (
            "COALESCE(NULL,NULLIF(1,1),9007199254740993)",
            V::Int64(9_007_199_254_740_993),
        ),
        ("COALESCE(NULL,FALSE,TRUE)", V::Boolean(false)),
        ("COALESCE(NULL,x,0.0)", V::Float64(0.0)),
        ("COALESCE('first',payload)", V::Utf8("first".into())),
    ] {
        let plan = validate_supported_typed_map_sql(
            &format!("SELECT {expr} AS value FROM events"),
            &catalog(),
        )
        .unwrap();
        let program =
            serde_json::from_str(&serde_json::to_string(&plan.projection[0]).unwrap()).unwrap();
        let result = evaluate_typed_expr(&program, &|name| match name {
            "payload" if expr == "COALESCE('first',payload)" => {
                panic!("evaluated unused COALESCE operand")
            }
            "payload" => Some(V::Utf8("value".into())),
            "ts" => Some(V::TimestampNanosecond(9_007_199_254_740_993)),
            "x" => Some(V::Null(RuntimeScalarTypeV1::Float64)),
            _ => None,
        })
        .unwrap();
        assert_eq!(result, expected, "{expr}");
    }
    for (expr, ty) in [
        ("COALESCE(payload,payload)", RuntimeScalarTypeV1::Utf8),
        ("COALESCE(x,x)", RuntimeScalarTypeV1::Float64),
    ] {
        let plan = validate_supported_typed_map_sql(
            &format!("SELECT {expr} AS value FROM events"),
            &catalog(),
        )
        .unwrap();
        assert_eq!(
            evaluate_typed_expr(&plan.projection[0], &|_| Some(V::Null(ty))).unwrap(),
            V::Null(ty)
        );
    }
    for expr in [
        "COALESCE(ts,payload)",
        "COALESCE(payload,1)",
        "COALESCE(TRUE,1)",
    ] {
        assert!(validate_supported_typed_map_sql(
            &format!("SELECT {expr} AS value FROM events"),
            &catalog()
        )
        .is_err());
    }
}

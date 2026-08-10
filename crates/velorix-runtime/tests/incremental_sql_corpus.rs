use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::{Map, Value};

const CORPUS: &str = include_str!("../benches/fixtures/incremental_sql_corpus_v1.json");

#[derive(Deserialize)]
struct Corpus {
    schema_version: u32,
    relations: Vec<Relation>,
    workloads: Vec<Workload>,
    scale_workloads: Vec<ScaleWorkload>,
    join_scale_workloads: Vec<JoinScaleWorkload>,
    suites: Suites,
    phases: Vec<Phase>,
}

#[derive(Deserialize)]
struct Suites {
    conformance: Suite,
    recovery: Suite,
    performance: Suite,
}

#[derive(Deserialize)]
struct Suite {
    workload_ids: Vec<String>,
    #[serde(default)]
    scale_workload_ids: Vec<String>,
    #[serde(default)]
    join_scale_workload_ids: Vec<String>,
    phase_names: Vec<String>,
    semantic_equivalence_required: bool,
}

#[derive(Deserialize)]
struct JoinScaleWorkload {
    id: String,
    sql: String,
    distribution: String,
    key_count: u64,
    left_rows: u64,
    right_rows: u64,
    hot_key_basis_points: u16,
    expected_groups: u64,
    expected_matches: u64,
    expected_max_group_matches: u64,
    samples: u32,
}

#[derive(Deserialize)]
struct ScaleWorkload {
    id: String,
    sql: String,
    distribution: String,
    total_rows: u64,
    batch_rows: u64,
    distinct_groups: u64,
    hot_group_basis_points: u16,
}

#[derive(Deserialize)]
struct Relation {
    name: String,
    columns: Vec<Column>,
    primary_key: Vec<String>,
    event_time: Option<String>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
}

#[derive(Deserialize)]
struct Workload {
    id: String,
    sql: Vec<String>,
    expected_final: Vec<Map<String, Value>>,
}

#[derive(Deserialize)]
struct Phase {
    name: String,
    events: Vec<Change>,
    expected: BTreeMap<String, Vec<Map<String, Value>>>,
}

#[derive(Deserialize)]
struct Change {
    change_id: String,
    relation: String,
    op: String,
    before: Option<Map<String, Value>>,
    after: Option<Map<String, Value>>,
}

#[test]
fn shared_incremental_sql_corpus_has_valid_schemas_and_snapshots() {
    let corpus: Corpus = serde_json::from_str(CORPUS).expect("corpus JSON must parse");
    assert_eq!(corpus.schema_version, 1);
    assert_eq!(
        corpus
            .relations
            .iter()
            .map(|relation| relation.name.as_str())
            .collect::<Vec<_>>(),
        ["orders", "customers", "products"]
    );
    assert_eq!(
        corpus
            .phases
            .iter()
            .map(|phase| phase.name.as_str())
            .collect::<Vec<_>>(),
        [
            "initial_load",
            "insert",
            "update",
            "delete",
            "checkpoint_restart",
            "replay_tail"
        ]
    );
    assert_eq!(
        corpus
            .workloads
            .iter()
            .map(|workload| workload.id.as_str())
            .collect::<Vec<_>>(),
        [
            "filter_project",
            "aggregate",
            "distinct_aggregate",
            "inner_join",
            "left_join",
            "top_k",
            "fixed_window",
            "ranking",
            "chained_view"
        ]
    );
    for workload in &corpus.workloads {
        assert!(
            !workload.sql.is_empty() && workload.sql.iter().all(|sql| !sql.trim().is_empty()),
            "{} has no SQL",
            workload.id
        );
        assert!(
            !workload.expected_final.is_empty(),
            "{} has no expected rows",
            workload.id
        );
    }

    assert_eq!(
        corpus
            .scale_workloads
            .iter()
            .map(|workload| workload.id.as_str())
            .collect::<Vec<_>>(),
        [
            "aggregate_composite_high_cardinality",
            "aggregate_composite_hot_key_skew"
        ]
    );
    for workload in &corpus.scale_workloads {
        assert!(!workload.sql.trim().is_empty());
        assert!(workload.sql.contains("GROUP BY customer_id, category"));
        assert!(workload.total_rows > 0);
        assert!(workload.batch_rows > 0 && workload.total_rows % workload.batch_rows == 0);
        assert!(workload.distinct_groups > 1 && workload.distinct_groups <= workload.total_rows);
        match workload.distribution.as_str() {
            "high_cardinality" => {
                assert_eq!(workload.distinct_groups, workload.total_rows);
                assert_eq!(workload.hot_group_basis_points, 0);
            }
            "hot_key_skew" => {
                assert!((8_000..10_000).contains(&workload.hot_group_basis_points));
                assert!(workload.distinct_groups < workload.total_rows);
            }
            other => panic!("unsupported scale distribution {other}"),
        }
    }

    assert_eq!(
        corpus
            .join_scale_workloads
            .iter()
            .map(|workload| workload.distribution.as_str())
            .collect::<Vec<_>>(),
        [
            "one_to_one",
            "one_to_many",
            "many_to_many",
            "hot_key_skew",
            "unmatched"
        ]
    );
    for workload in &corpus.join_scale_workloads {
        assert!(!workload.id.trim().is_empty());
        assert!(workload.sql.contains("JOIN join_right"));
        assert!(workload.key_count > 1);
        assert!(workload.left_rows > 0 && workload.right_rows > 0);
        assert!(workload.samples >= 5);
        match workload.distribution.as_str() {
            "one_to_one" => {
                assert_eq!(workload.left_rows, workload.key_count);
                assert_eq!(workload.right_rows, workload.key_count);
                assert_eq!(workload.expected_groups, workload.key_count);
                assert_eq!(workload.expected_matches, workload.key_count);
                assert_eq!(workload.expected_max_group_matches, 1);
                assert_eq!(workload.hot_key_basis_points, 0);
            }
            "one_to_many" => {
                assert_eq!(workload.left_rows, workload.key_count);
                assert_eq!(workload.right_rows % workload.key_count, 0);
                assert_eq!(workload.expected_groups, workload.key_count);
                assert_eq!(workload.expected_matches, workload.right_rows);
                assert_eq!(workload.hot_key_basis_points, 0);
            }
            "many_to_many" => {
                assert_eq!(workload.left_rows % workload.key_count, 0);
                assert_eq!(workload.right_rows % workload.key_count, 0);
                assert_eq!(workload.expected_groups, workload.key_count);
                assert_eq!(workload.hot_key_basis_points, 0);
            }
            "hot_key_skew" => {
                assert!((8_000..10_000).contains(&workload.hot_key_basis_points));
                assert!(workload.expected_max_group_matches * 2 > workload.expected_matches);
            }
            "unmatched" => {
                assert_eq!(workload.expected_groups, 0);
                assert_eq!(workload.expected_matches, 0);
                assert_eq!(workload.expected_max_group_matches, 0);
                assert_eq!(workload.hot_key_basis_points, 0);
            }
            other => panic!("unsupported join scale distribution {other}"),
        }
    }

    let workload_ids = corpus
        .workloads
        .iter()
        .map(|workload| workload.id.as_str())
        .collect::<BTreeSet<_>>();
    let phase_names = corpus
        .phases
        .iter()
        .map(|phase| phase.name.as_str())
        .collect::<BTreeSet<_>>();
    let scale_workload_ids = corpus
        .scale_workloads
        .iter()
        .map(|workload| workload.id.as_str())
        .collect::<BTreeSet<_>>();
    let join_scale_workload_ids = corpus
        .join_scale_workloads
        .iter()
        .map(|workload| workload.id.as_str())
        .collect::<BTreeSet<_>>();
    for suite in [
        &corpus.suites.conformance,
        &corpus.suites.recovery,
        &corpus.suites.performance,
    ] {
        assert_eq!(
            suite.workload_ids.iter().collect::<BTreeSet<_>>().len(),
            suite.workload_ids.len(),
            "suite contains duplicate workloads"
        );
        assert!(suite
            .workload_ids
            .iter()
            .all(|workload| workload_ids.contains(workload.as_str())));
        assert!(suite
            .phase_names
            .iter()
            .all(|phase| phase_names.contains(phase.as_str())));
    }
    assert!(corpus.suites.conformance.scale_workload_ids.is_empty());
    assert!(corpus.suites.recovery.scale_workload_ids.is_empty());
    assert!(corpus.suites.conformance.join_scale_workload_ids.is_empty());
    assert!(corpus.suites.recovery.join_scale_workload_ids.is_empty());
    assert_eq!(
        corpus
            .suites
            .performance
            .scale_workload_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        scale_workload_ids
    );
    assert_eq!(
        corpus
            .suites
            .performance
            .join_scale_workload_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        join_scale_workload_ids
    );
    assert!(!corpus.suites.conformance.semantic_equivalence_required);
    assert!(!corpus.suites.recovery.semantic_equivalence_required);
    assert!(corpus.suites.performance.semantic_equivalence_required);
    assert!(corpus
        .suites
        .recovery
        .phase_names
        .iter()
        .all(|phase| !corpus.suites.performance.phase_names.contains(phase)));
    assert_eq!(
        corpus.suites.recovery.phase_names,
        ["checkpoint_restart", "replay_tail"]
    );
    assert!(!corpus
        .suites
        .performance
        .phase_names
        .iter()
        .any(|phase| phase.contains("checkpoint") || phase.contains("restart")));

    let schemas = corpus
        .relations
        .iter()
        .map(|relation| {
            validate_schema(relation);
            (relation.name.as_str(), relation)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(schemas["orders"].event_time.as_deref(), Some("event_time"));

    let mut seen_change_ids = BTreeSet::new();
    let mut state = schemas
        .keys()
        .map(|name| (*name, BTreeMap::<String, Map<String, Value>>::new()))
        .collect::<BTreeMap<_, _>>();

    for phase in &corpus.phases {
        for change in &phase.events {
            assert!(
                seen_change_ids.insert(change.change_id.as_str()),
                "duplicate change_id {}",
                change.change_id
            );
            let schema = schemas
                .get(change.relation.as_str())
                .unwrap_or_else(|| panic!("unknown relation {}", change.relation));
            apply_change(&mut state, schema, change);
        }

        for (relation, expected_rows) in &phase.expected {
            let schema = schemas
                .get(relation.as_str())
                .unwrap_or_else(|| panic!("unknown expected relation {relation}"));
            let expected = expected_rows
                .iter()
                .map(|row| {
                    validate_row(schema, row);
                    (row_key(schema, row), row.clone())
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(state[relation.as_str()], expected, "phase {}", phase.name);
        }
    }
}

fn validate_schema(relation: &Relation) {
    assert!(
        !relation.columns.is_empty(),
        "{} has no columns",
        relation.name
    );
    assert!(
        !relation.primary_key.is_empty(),
        "{} has no primary key",
        relation.name
    );
    let columns = relation
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(columns.len(), relation.columns.len());
    for key in &relation.primary_key {
        assert!(
            columns.contains(key.as_str()),
            "missing primary-key column {key}"
        );
    }
    if let Some(event_time) = &relation.event_time {
        assert!(columns.contains(event_time.as_str()));
    }
    for column in &relation.columns {
        assert!(matches!(
            column.data_type.as_str(),
            "utf8" | "int64" | "timestamp_rfc3339"
        ));
    }
}

fn apply_change<'a>(
    state: &mut BTreeMap<&'a str, BTreeMap<String, Map<String, Value>>>,
    schema: &'a Relation,
    change: &Change,
) {
    let rows = state.get_mut(schema.name.as_str()).unwrap();
    match change.op.as_str() {
        "insert" => {
            assert!(change.before.is_none());
            let after = change.after.as_ref().expect("insert must have after");
            validate_row(schema, after);
            assert!(rows.insert(row_key(schema, after), after.clone()).is_none());
        }
        "update" => {
            let before = change.before.as_ref().expect("update must have before");
            let after = change.after.as_ref().expect("update must have after");
            validate_row(schema, before);
            validate_row(schema, after);
            let key = row_key(schema, before);
            assert_eq!(key, row_key(schema, after), "update changed primary key");
            assert_eq!(rows.get(&key), Some(before), "update before mismatch");
            rows.insert(key, after.clone());
        }
        "delete" => {
            assert!(change.after.is_none());
            let before = change.before.as_ref().expect("delete must have before");
            validate_row(schema, before);
            assert_eq!(rows.remove(&row_key(schema, before)).as_ref(), Some(before));
        }
        other => panic!("unsupported change op {other}"),
    }
}

fn validate_row(schema: &Relation, row: &Map<String, Value>) {
    assert_eq!(row.len(), schema.columns.len(), "{} row width", schema.name);
    for column in &schema.columns {
        let value = row
            .get(&column.name)
            .unwrap_or_else(|| panic!("missing {}.{}", schema.name, column.name));
        if value.is_null() {
            assert!(
                column.nullable,
                "{}.{} is not nullable",
                schema.name, column.name
            );
            continue;
        }
        let valid = match column.data_type.as_str() {
            "utf8" => value.is_string(),
            "int64" => value.as_i64().is_some(),
            "timestamp_rfc3339" => value
                .as_str()
                .is_some_and(|timestamp| timestamp.ends_with('Z') && timestamp.contains('T')),
            _ => false,
        };
        assert!(
            valid,
            "invalid {}.{} value {value}",
            schema.name, column.name
        );
    }
}

fn row_key(schema: &Relation, row: &Map<String, Value>) -> String {
    serde_json::to_string(
        &schema
            .primary_key
            .iter()
            .map(|column| &row[column])
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

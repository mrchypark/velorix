use serde_json::json;
use velorix_core::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};
use velorix_core::operator::{
    filter_delta_batch, map_delta_batch, KeyedEquiJoin, KeyedSumCountAggregate, OperatorError,
};

#[test]
fn operators_map_delta_batch_transforms_records_while_preserving_signed_weights() {
    let input = DeltaBatch::from_records([
        record("order:1", json!({ "amount": 10 }), 2),
        record("order:2", json!({ "amount": 15 }), -1),
    ]);

    let output = map_delta_batch(&input, |record| {
        Ok((
            DeltaKey::from_json(json!({
                "mapped": record.key.as_json(),
            })),
            DeltaValue::from_json(json!({
                "seen": record.value.as_json(),
            })),
        ))
    })
    .unwrap();

    assert_eq!(
        output.net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!({ "mapped": "order:1" })),
                DeltaValue::from_json(json!({ "seen": { "amount": 10 } })),
                2,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!({ "mapped": "order:2" })),
                DeltaValue::from_json(json!({ "seen": { "amount": 15 } })),
                -1,
            ),
        ]
    );
}

#[test]
fn operators_filter_delta_batch_drops_records_without_mutating_surviving_deltas() {
    let input = DeltaBatch::from_records([
        record("order:1", json!({ "region": "us" }), 1),
        record("order:2", json!({ "region": "eu" }), -1),
    ]);

    let output = filter_delta_batch(
        &input,
        |record| Ok(record.value.as_json()["region"] == "us"),
    )
    .unwrap();

    assert_eq!(
        output.net_rows().unwrap(),
        vec![record("order:1", json!({ "region": "us" }), 1)]
    );
}

#[test]
fn operators_keyed_equi_join_emits_insertions_and_retractions_from_in_memory_side_state() {
    let mut join = KeyedEquiJoin::new(|left, right| {
        Ok(DeltaValue::from_json(json!({
            "left": left.as_json(),
            "right": right.as_json(),
        })))
    });

    let left_insert = DeltaBatch::from_records([record("acct:1", json!({ "name": "Ada" }), 1)]);
    assert!(join
        .apply_left(&left_insert)
        .unwrap()
        .net_rows()
        .unwrap()
        .is_empty());

    let right_insert = DeltaBatch::from_records([record("acct:1", json!({ "balance": 30 }), 1)]);
    let joined_insert = join.apply_right(&right_insert).unwrap();

    assert_eq!(
        joined_insert.net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("acct:1")),
            DeltaValue::from_json(json!({
                "left": { "name": "Ada" },
                "right": { "balance": 30 },
            })),
            1,
        )]
    );
    assert_eq!(
        join.left_state().net_rows().unwrap(),
        left_insert.net_rows().unwrap()
    );
    assert_eq!(
        join.right_state().net_rows().unwrap(),
        right_insert.net_rows().unwrap()
    );

    let right_retract = DeltaBatch::from_records([record("acct:1", json!({ "balance": 30 }), -1)]);
    let joined_retract = join.apply_right(&right_retract).unwrap();

    assert_eq!(
        joined_retract.net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("acct:1")),
            DeltaValue::from_json(json!({
                "left": { "name": "Ada" },
                "right": { "balance": 30 },
            })),
            -1,
        )]
    );
    assert!(join.right_state().net_rows().unwrap().is_empty());
}

#[test]
fn operators_keyed_equi_join_compacts_side_state_after_insert_retract_churn() {
    let mut join = KeyedEquiJoin::new(join_values);
    let left_insert = DeltaBatch::from_records([record("acct:1", json!({ "name": "Ada" }), 1)]);
    let left_retract = DeltaBatch::from_records([record("acct:1", json!({ "name": "Ada" }), -1)]);
    let right_insert = DeltaBatch::from_records([record("acct:1", json!({ "balance": 30 }), 1)]);
    let right_retract = DeltaBatch::from_records([record("acct:1", json!({ "balance": 30 }), -1)]);

    join.apply_left(&left_insert).unwrap();
    join.apply_left(&left_retract).unwrap();
    join.apply_right(&right_insert).unwrap();
    join.apply_right(&right_retract).unwrap();

    assert!(join.left_state().records().is_empty());
    assert!(join.right_state().records().is_empty());
}

#[test]
fn operators_keyed_equi_join_left_retractions_emit_joined_retractions() {
    let mut join = KeyedEquiJoin::new(join_values);
    let right_insert = DeltaBatch::from_records([record("acct:1", json!({ "balance": 30 }), 1)]);
    let left_insert = DeltaBatch::from_records([record("acct:1", json!({ "name": "Ada" }), 1)]);
    let left_retract = DeltaBatch::from_records([record("acct:1", json!({ "name": "Ada" }), -1)]);

    assert!(join
        .apply_right(&right_insert)
        .unwrap()
        .records()
        .is_empty());
    assert_eq!(
        join.apply_left(&left_insert).unwrap().net_rows().unwrap(),
        vec![joined_record(
            "acct:1",
            json!({ "name": "Ada" }),
            json!({ "balance": 30 }),
            1,
        )]
    );

    assert_eq!(
        join.apply_left(&left_retract).unwrap().net_rows().unwrap(),
        vec![joined_record(
            "acct:1",
            json!({ "name": "Ada" }),
            json!({ "balance": 30 }),
            -1,
        )]
    );
    assert!(join.left_state().records().is_empty());
    assert_eq!(join.right_state().records(), right_insert.records());
}

#[test]
fn operators_keyed_sum_count_aggregate_emits_changed_materialized_totals() {
    let mut aggregate = KeyedSumCountAggregate::new();

    let first = DeltaBatch::from_records([
        record("acct:1", json!(10), 1),
        record("acct:1", json!(5), 2),
    ]);
    let first_output = aggregate.apply(&first).unwrap();

    assert_eq!(
        first_output.net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("acct:1")),
            DeltaValue::from_json(json!({ "sum": 20, "count": 3 })),
            1,
        )]
    );
    assert_eq!(
        aggregate.state().net_rows().unwrap(),
        first_output.net_rows().unwrap()
    );

    let retract = DeltaBatch::from_records([record("acct:1", json!(5), -1)]);
    let retract_output = aggregate.apply(&retract).unwrap();

    assert_eq!(
        retract_output.net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("acct:1")),
                DeltaValue::from_json(json!({ "sum": 15, "count": 2 })),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("acct:1")),
                DeltaValue::from_json(json!({ "sum": 20, "count": 3 })),
                -1,
            ),
        ]
    );
    assert_eq!(
        aggregate.state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("acct:1")),
            DeltaValue::from_json(json!({ "sum": 15, "count": 2 })),
            1,
        )]
    );
}

#[test]
fn operators_keyed_sum_count_aggregate_preserves_signed_sum_when_count_is_zero() {
    let mut aggregate = KeyedSumCountAggregate::new();
    let input = DeltaBatch::from_records([
        record("acct:1", json!(10), 1),
        record("acct:1", json!(5), -1),
    ]);

    let output = aggregate.apply(&input).unwrap();

    assert_eq!(
        output.net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("acct:1")),
            DeltaValue::from_json(json!({ "sum": 5, "count": 0 })),
            1,
        )]
    );
    assert_eq!(
        aggregate.state().net_rows().unwrap(),
        output.net_rows().unwrap()
    );
}

#[test]
fn operators_keyed_sum_count_aggregate_preserves_negative_count_state() {
    let mut aggregate = KeyedSumCountAggregate::new();

    let output = aggregate
        .apply(&DeltaBatch::from_records([record("acct:1", json!(5), -1)]))
        .unwrap();

    assert_eq!(
        output.net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("acct:1")),
            DeltaValue::from_json(json!({ "sum": -5, "count": -1 })),
            1,
        )]
    );
    assert_eq!(
        aggregate.state().net_rows().unwrap(),
        output.net_rows().unwrap()
    );
}

#[test]
fn operators_keyed_sum_count_aggregate_hydrates_from_materialized_state() {
    let checkpointed_state = DeltaBatch::from_records([
        DeltaRecord::new(
            DeltaKey::from_json(json!("acct:1")),
            DeltaValue::from_json(json!({ "sum": 5, "count": 0 })),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!("acct:2")),
            DeltaValue::from_json(json!({ "sum": -7, "count": -1 })),
            1,
        ),
    ]);

    let mut aggregate = KeyedSumCountAggregate::from_state(&checkpointed_state).unwrap();
    assert_eq!(
        aggregate.state().net_rows().unwrap(),
        checkpointed_state.net_rows().unwrap()
    );

    aggregate
        .apply(&DeltaBatch::from_records([
            record("acct:1", json!(2), 1),
            record("acct:2", json!(3), -1),
        ]))
        .unwrap();

    assert_eq!(
        aggregate.state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("acct:1")),
                DeltaValue::from_json(json!({ "sum": 7, "count": 1 })),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("acct:2")),
                DeltaValue::from_json(json!({ "sum": -10, "count": -2 })),
                1,
            ),
        ]
    );
}

#[test]
fn operators_keyed_sum_count_aggregate_rejects_malformed_materialized_state() {
    let malformed = DeltaBatch::from_records([DeltaRecord::new(
        DeltaKey::from_json(json!("acct:1")),
        DeltaValue::from_json(json!({ "sum": 5, "missing_count": 1 })),
        1,
    )]);

    assert_eq!(
        KeyedSumCountAggregate::from_state(&malformed).unwrap_err(),
        OperatorError::InvalidAggregateStateValue
    );
}

#[test]
fn operators_keyed_sum_count_aggregate_non_integer_input_preserves_prior_state() {
    let mut aggregate = KeyedSumCountAggregate::new();
    let initial_output = aggregate
        .apply(&DeltaBatch::from_records([record("acct:1", json!(10), 1)]))
        .unwrap();

    let result = aggregate.apply(&DeltaBatch::from_records([record(
        "acct:1",
        json!({ "not": "integer" }),
        1,
    )]));

    assert_eq!(result, Err(OperatorError::NonIntegerAggregateValue));
    assert_eq!(
        aggregate.state().net_rows().unwrap(),
        initial_output.net_rows().unwrap()
    );
}

fn join_values(left: &DeltaValue, right: &DeltaValue) -> Result<DeltaValue, OperatorError> {
    Ok(DeltaValue::from_json(json!({
        "left": left.as_json(),
        "right": right.as_json(),
    })))
}

fn joined_record(
    key: &str,
    left: serde_json::Value,
    right: serde_json::Value,
    weight: i64,
) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(key)),
        DeltaValue::from_json(json!({
            "left": left,
            "right": right,
        })),
        weight,
    )
}

fn record(key: &str, value: serde_json::Value, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(key)),
        DeltaValue::from_json(value),
        weight,
    )
}

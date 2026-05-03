use serde_json::json;
use velorix_core::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};
use velorix_core::operator::{
    filter_delta_batch, map_delta_batch, KeyedEquiJoin, KeyedSumCountAggregate,
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

fn record(key: &str, value: serde_json::Value, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(key)),
        DeltaValue::from_json(value),
        weight,
    )
}

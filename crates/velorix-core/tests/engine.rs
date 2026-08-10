use serde_json::json;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{
        AggregateValueMode, EngineCheckpoint, EngineCheckpointPayload, IncrementalEngine,
        KeyedAggregateKernel, ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
    },
};

fn input_delta(account: &str, amount: i64, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!(amount)),
        weight,
    )
}

fn keyed_input_delta(key: serde_json::Value, amount: i64, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(key),
        DeltaValue::from_json(json!(amount)),
        weight,
    )
}

fn decimal_input_delta(account: &str, amount: &str, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!(amount)),
        weight,
    )
}

fn state_delta(account: &str, sum: serde_json::Value, count: serde_json::Value) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!({ "sum": sum, "count": count })),
        1,
    )
}

fn batch(records: impl IntoIterator<Item = DeltaRecord>) -> DeltaBatch {
    DeltaBatch::from_records(records)
}

fn net_state(engine: &KeyedAggregateKernel) -> Vec<DeltaRecord> {
    engine.materialized_state().net_rows().unwrap()
}

#[test]
fn keyed_aggregate_kernel_cancels_insert_and_retract_when_signed_changes_balance() {
    let mut engine = KeyedAggregateKernel::new();

    engine
        .push_changes(1, &batch([input_delta("account-a", 10, 1)]))
        .unwrap();
    engine
        .push_changes(2, &batch([input_delta("account-a", 10, -1)]))
        .unwrap();

    assert_eq!(engine.logical_epoch(), 2);
    assert!(net_state(&engine).is_empty());
}

#[test]
fn keyed_aggregate_kernel_materialized_state_is_invariant_to_input_chunking() {
    let mut one_batch = KeyedAggregateKernel::new();
    one_batch
        .push_changes(
            1,
            &batch([
                input_delta("account-a", 10, 1),
                input_delta("account-a", 5, 1),
                input_delta("account-b", 7, -1),
            ]),
        )
        .unwrap();

    let mut many_batches = KeyedAggregateKernel::new();
    many_batches
        .push_changes(1, &batch([input_delta("account-a", 10, 1)]))
        .unwrap();
    many_batches
        .push_changes(2, &batch([input_delta("account-a", 5, 1)]))
        .unwrap();
    many_batches
        .push_changes(3, &batch([input_delta("account-b", 7, -1)]))
        .unwrap();

    assert_eq!(net_state(&one_batch), net_state(&many_batches));
}

#[test]
fn keyed_aggregate_kernel_rejects_non_monotonic_logical_epochs() {
    let mut engine = KeyedAggregateKernel::new();
    engine
        .push_changes(1, &batch([input_delta("account-a", 10, 1)]))
        .unwrap();

    let err = engine
        .push_changes(1, &batch([input_delta("account-a", 5, 1)]))
        .unwrap_err();

    assert!(err.to_string().contains("logical epoch"));
}

#[test]
fn keyed_aggregate_kernel_checkpoint_plus_replay_matches_uninterrupted_run() {
    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
        input_delta("account-b", 7, -1),
    ]);
    let replay_input = batch([
        input_delta("account-a", 3, 1),
        input_delta("account-b", 2, -1),
    ]);

    let mut uninterrupted = KeyedAggregateKernel::new();
    uninterrupted.push_changes(1, &checkpoint_input).unwrap();
    uninterrupted.push_changes(2, &replay_input).unwrap();

    let mut checkpointed = KeyedAggregateKernel::new();
    checkpointed.push_changes(1, &checkpoint_input).unwrap();
    let checkpoint = checkpointed.checkpoint_state();
    let mut restored = KeyedAggregateKernel::from_checkpoint(checkpoint).unwrap();
    restored.push_changes(2, &replay_input).unwrap();

    assert_eq!(net_state(&restored), net_state(&uninterrupted));
}

#[test]
fn keyed_aggregate_kernel_restores_composite_null_group_state_from_wire_checkpoint() {
    let nullable_group = json!({ "region": "north", "category": null });
    let hardware_group = json!({ "region": "north", "category": "hardware" });
    let south_group = json!({ "region": "south", "category": null });
    let checkpoint_input = batch([
        keyed_input_delta(nullable_group.clone(), 10, 1),
        keyed_input_delta(nullable_group.clone(), 5, 1),
        keyed_input_delta(hardware_group.clone(), 7, 1),
    ]);
    let replay_input = batch([
        keyed_input_delta(nullable_group.clone(), 5, -1),
        keyed_input_delta(south_group.clone(), 3, 1),
    ]);

    let mut uninterrupted = KeyedAggregateKernel::new();
    uninterrupted.push_changes(1, &checkpoint_input).unwrap();
    uninterrupted.push_changes(2, &replay_input).unwrap();

    let mut checkpointed = KeyedAggregateKernel::new();
    checkpointed.push_changes(1, &checkpoint_input).unwrap();
    let encoded = serde_json::to_vec(&checkpointed.checkpoint_state().to_payload()).unwrap();
    assert!(
        String::from_utf8_lossy(&encoded).contains("\"category\":null"),
        "wire checkpoint must preserve a NULL composite-key component"
    );
    let decoded = serde_json::from_slice::<EngineCheckpointPayload>(&encoded).unwrap();
    let mut restored = KeyedAggregateKernel::from_checkpoint(decoded.into_checkpoint()).unwrap();
    restored.push_changes(2, &replay_input).unwrap();

    assert_eq!(restored.logical_epoch(), 2);
    assert_eq!(net_state(&restored), net_state(&uninterrupted));
    assert_eq!(
        net_state(&restored),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!({ "category": "hardware", "region": "north" })),
                DeltaValue::from_json(json!({ "sum": 7, "count": 1 })),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!({ "category": null, "region": "north" })),
                DeltaValue::from_json(json!({ "sum": 10, "count": 1 })),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!({ "category": null, "region": "south" })),
                DeltaValue::from_json(json!({ "sum": 3, "count": 1 })),
                1,
            ),
        ]
    );
}

#[test]
fn keyed_aggregate_kernel_hydrates_decimal128_checkpoint_with_selected_mode() {
    let checkpoint = EngineCheckpoint::new(
        1,
        batch([state_delta("account-a", json!("0.10"), json!(1))]),
    );
    let mut restored = KeyedAggregateKernel::from_checkpoint_with_aggregate_value_mode(
        checkpoint,
        AggregateValueMode::Decimal128 {
            precision: 38,
            scale: 2,
        },
    )
    .unwrap();

    restored
        .push_changes(2, &batch([decimal_input_delta("account-a", "0.20", 1)]))
        .unwrap();

    assert_eq!(
        net_state(&restored),
        vec![state_delta("account-a", json!("0.30"), json!(2))]
    );
}

#[test]
fn keyed_aggregate_kernel_rejects_malformed_checkpoint_state() {
    let malformed = EngineCheckpoint::new(
        7,
        batch([state_delta("account-a", json!(10), json!("not-an-integer"))]),
    );

    let err = KeyedAggregateKernel::from_checkpoint(malformed).unwrap_err();

    assert!(err.to_string().contains("aggregate state value"));
}

#[test]
fn engine_checkpoint_payload_preserves_logical_epoch_and_state_when_serialized() {
    let checkpoint =
        EngineCheckpoint::new(3, batch([state_delta("account-a", json!(10), json!(1))]));

    let encoded = serde_json::to_vec(&checkpoint.to_payload()).unwrap();
    let decoded = serde_json::from_slice::<EngineCheckpointPayload>(&encoded).unwrap();
    let restored = decoded.into_checkpoint();

    assert_eq!(restored.logical_epoch(), 3);
    assert_eq!(restored.state(), checkpoint.state());
    assert_eq!(
        checkpoint.to_payload().schema_version(),
        ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION
    );
}

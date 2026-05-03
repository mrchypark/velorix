use serde_json::json;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{
        EngineCheckpoint, EngineCheckpointPayload, IncrementalEngine, PrototypeIncrementalEngine,
        ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
    },
};

fn input_delta(account: &str, amount: i64, weight: i64) -> DeltaRecord {
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

fn net_state(engine: &PrototypeIncrementalEngine) -> Vec<DeltaRecord> {
    engine.materialized_state().net_rows().unwrap()
}

#[test]
fn prototype_incremental_engine_cancels_insert_and_retract_when_signed_changes_balance() {
    let mut engine = PrototypeIncrementalEngine::new();

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
fn prototype_incremental_engine_materialized_state_is_invariant_to_input_chunking() {
    let mut one_batch = PrototypeIncrementalEngine::new();
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

    let mut many_batches = PrototypeIncrementalEngine::new();
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
fn prototype_incremental_engine_rejects_non_monotonic_logical_epochs() {
    let mut engine = PrototypeIncrementalEngine::new();
    engine
        .push_changes(1, &batch([input_delta("account-a", 10, 1)]))
        .unwrap();

    let err = engine
        .push_changes(1, &batch([input_delta("account-a", 5, 1)]))
        .unwrap_err();

    assert!(err.to_string().contains("logical epoch"));
}

#[test]
fn prototype_incremental_engine_checkpoint_plus_replay_matches_uninterrupted_run() {
    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
        input_delta("account-b", 7, -1),
    ]);
    let replay_input = batch([
        input_delta("account-a", 3, 1),
        input_delta("account-b", 2, -1),
    ]);

    let mut uninterrupted = PrototypeIncrementalEngine::new();
    uninterrupted.push_changes(1, &checkpoint_input).unwrap();
    uninterrupted.push_changes(2, &replay_input).unwrap();

    let mut checkpointed = PrototypeIncrementalEngine::new();
    checkpointed.push_changes(1, &checkpoint_input).unwrap();
    let checkpoint = checkpointed.checkpoint_state();
    let mut restored = PrototypeIncrementalEngine::from_checkpoint(checkpoint).unwrap();
    restored.push_changes(2, &replay_input).unwrap();

    assert_eq!(net_state(&restored), net_state(&uninterrupted));
}

#[test]
fn prototype_incremental_engine_rejects_malformed_checkpoint_state() {
    let malformed = EngineCheckpoint::new(
        7,
        batch([state_delta("account-a", json!(10), json!("not-an-integer"))]),
    );

    let err = PrototypeIncrementalEngine::from_checkpoint(malformed).unwrap_err();

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

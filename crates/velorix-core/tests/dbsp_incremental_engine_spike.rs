#![cfg(feature = "dbsp-spike")]

use std::collections::{BTreeMap, BTreeSet};

use dbsp::{
    typed_batch::IndexedZSetReader, utils::Tup2, DBSPHandle, OrdIndexedZSet, OutputHandle, Runtime,
    ZSetHandle, ZWeight,
};
use feldera_macros::IsNone;
use rkyv::{Archive, Serialize};
use serde_json::json;
use size_of::SizeOf;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{
        EngineCheckpoint, EngineError, IncrementalEngine, LogicalEpoch, PrototypeIncrementalEngine,
    },
    operator::OperatorError,
};

#[derive(
    Clone,
    Default,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    SizeOf,
    Archive,
    Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct AccountAmount {
    account: String,
    amount: i64,
}

struct DbspAggregateEngineSpike {
    circuit: DBSPHandle,
    input: ZSetHandle<AccountAmount>,
    sum_output: OutputHandle<OrdIndexedZSet<String, ZWeight>>,
    count_output: OutputHandle<OrdIndexedZSet<String, ZWeight>>,
    logical_epoch: LogicalEpoch,
    materialized: BTreeMap<String, AggregateState>,
}

impl DbspAggregateEngineSpike {
    fn new() -> Self {
        let (circuit, (input, sum_output, count_output)) = Runtime::init_circuit(1, |circuit| {
            let (input_stream, input) = circuit.add_input_zset::<AccountAmount>();
            let indexed_amounts =
                input_stream.map_index(|record| (record.account.clone(), record.amount));
            let sum_output = indexed_amounts
                .aggregate_linear(|amount| *amount as ZWeight)
                .output();
            let count_output = input_stream
                .map_index(|record| (record.account.clone(), ()))
                .weighted_count()
                .output();

            Ok((input, sum_output, count_output))
        })
        .expect("dbsp aggregate spike circuit must build");

        Self {
            circuit,
            input,
            sum_output,
            count_output,
            logical_epoch: 0,
            materialized: BTreeMap::new(),
        }
    }

    fn hydrate_from_checkpoint(checkpoint: EngineCheckpoint) -> Result<Self, EngineError> {
        let mut engine = Self::new();
        let mut seed = Vec::new();

        for record in checkpoint.state().net_rows().map_err(OperatorError::from)? {
            let key = parse_key(&record.key)?;
            let state = parse_state_value(&record.value, record.weight)?;
            seed.extend(seed_records(&key, state));
        }

        if !seed.is_empty() {
            engine.apply_dbsp_input(seed)?;
        }
        engine.logical_epoch = checkpoint.logical_epoch();

        Ok(engine)
    }

    fn apply_dbsp_input(
        &mut self,
        mut records: Vec<Tup2<AccountAmount, ZWeight>>,
    ) -> Result<DeltaBatch, EngineError> {
        let before = self.materialized.clone();

        self.input.append(&mut records);
        self.circuit
            .transaction()
            .expect("dbsp aggregate spike transaction must run");

        apply_scalar_updates(
            &mut self.materialized,
            ScalarField::Sum,
            self.sum_output.consolidate(),
        );
        apply_scalar_updates(
            &mut self.materialized,
            ScalarField::Count,
            self.count_output.consolidate(),
        );
        self.materialized.retain(|_, state| !state.is_zero());

        Ok(delta_between_states(&before, &self.materialized))
    }
}

impl IncrementalEngine for DbspAggregateEngineSpike {
    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn push_changes(
        &mut self,
        _logical_epoch: LogicalEpoch,
        signed_input_changes: &DeltaBatch,
    ) -> Result<DeltaBatch, EngineError> {
        if _logical_epoch <= self.logical_epoch {
            return Err(EngineError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: _logical_epoch,
            });
        }

        let mut records = delta_batch_to_dbsp_records(signed_input_changes)?;
        let output = self.apply_dbsp_input(std::mem::take(&mut records))?;
        self.logical_epoch = _logical_epoch;
        Ok(output)
    }

    fn materialized_state(&self) -> DeltaBatch {
        batch(
            self.materialized
                .iter()
                .map(|(key, state)| state_delta(key, state.sum, state.count)),
        )
    }

    fn checkpoint_state(&self) -> EngineCheckpoint {
        EngineCheckpoint::new(self.logical_epoch, self.materialized_state())
    }

    fn from_checkpoint(checkpoint: EngineCheckpoint) -> Result<Self, EngineError>
    where
        Self: Sized,
    {
        Self::hydrate_from_checkpoint(checkpoint)
    }
}

#[test]
fn dbsp_spike_matches_prototype_incremental_engine_for_signed_aggregate_sequences() {
    let batches = [
        batch([input_delta("account-a", 10, 1)]),
        batch([
            input_delta("account-a", 5, 1),
            input_delta("account-b", 7, -1),
        ]),
        batch([
            input_delta("account-a", 10, -1),
            input_delta("account-b", 2, -1),
        ]),
    ];

    let mut prototype = PrototypeIncrementalEngine::new();
    let mut dbsp = DbspAggregateEngineSpike::new();

    for (index, input) in batches.iter().enumerate() {
        let epoch = u64::try_from(index + 1).unwrap();

        let prototype_output = prototype.push_changes(epoch, input).unwrap();
        let dbsp_output = dbsp.push_changes(epoch, input).unwrap();

        assert_eq!(net_rows(&dbsp_output), net_rows(&prototype_output));
        assert_eq!(
            net_rows(&dbsp.materialized_state()),
            net_rows(&prototype.materialized_state())
        );
        assert_eq!(dbsp.logical_epoch(), prototype.logical_epoch());
    }
}

#[test]
fn dbsp_spike_checkpoint_plus_replay_matches_prototype_checkpoint_plus_replay() {
    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
        input_delta("account-b", 7, -1),
    ]);
    let replay_input = batch([
        input_delta("account-a", 3, 1),
        input_delta("account-b", 2, -1),
    ]);

    let mut prototype = PrototypeIncrementalEngine::new();
    prototype.push_changes(1, &checkpoint_input).unwrap();
    let prototype_checkpoint = prototype.checkpoint_state();
    let mut restored_prototype =
        PrototypeIncrementalEngine::from_checkpoint(prototype_checkpoint).unwrap();
    let prototype_output = restored_prototype.push_changes(2, &replay_input).unwrap();

    let mut dbsp = DbspAggregateEngineSpike::new();
    dbsp.push_changes(1, &checkpoint_input).unwrap();
    let dbsp_checkpoint = dbsp.checkpoint_state();
    let mut restored_dbsp = DbspAggregateEngineSpike::from_checkpoint(dbsp_checkpoint).unwrap();
    let dbsp_output = restored_dbsp.push_changes(2, &replay_input).unwrap();

    assert_eq!(net_rows(&dbsp_output), net_rows(&prototype_output));
    assert_eq!(
        net_rows(&restored_dbsp.materialized_state()),
        net_rows(&restored_prototype.materialized_state())
    );
}

fn input_delta(account: &str, amount: i64, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!(amount)),
        weight,
    )
}

fn state_delta(account: &str, sum: i64, count: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!({ "sum": sum, "count": count })),
        1,
    )
}

fn batch(records: impl IntoIterator<Item = DeltaRecord>) -> DeltaBatch {
    DeltaBatch::from_records(records)
}

fn net_rows(batch: &DeltaBatch) -> Vec<DeltaRecord> {
    batch.net_rows().unwrap()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AggregateState {
    sum: i64,
    count: i64,
}

impl AggregateState {
    fn is_zero(self) -> bool {
        self.sum == 0 && self.count == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScalarField {
    Sum,
    Count,
}

fn delta_batch_to_dbsp_records(
    input: &DeltaBatch,
) -> Result<Vec<Tup2<AccountAmount, ZWeight>>, EngineError> {
    input
        .records()
        .iter()
        .map(|record| {
            Ok(Tup2(
                AccountAmount {
                    account: parse_key(&record.key)?,
                    amount: record
                        .value
                        .as_json()
                        .as_i64()
                        .ok_or(OperatorError::NonIntegerAggregateValue)?,
                },
                record.weight,
            ))
        })
        .collect()
}

fn parse_key(key: &DeltaKey) -> Result<String, EngineError> {
    key.as_json()
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| OperatorError::InvalidAggregateStateValue.into())
}

fn parse_state_value(value: &DeltaValue, weight: i64) -> Result<AggregateState, EngineError> {
    let object = value
        .as_json()
        .as_object()
        .ok_or(OperatorError::InvalidAggregateStateValue)?;
    let sum = object
        .get("sum")
        .and_then(|value| value.as_i64())
        .ok_or(OperatorError::InvalidAggregateStateValue)?;
    let count = object
        .get("count")
        .and_then(|value| value.as_i64())
        .ok_or(OperatorError::InvalidAggregateStateValue)?;

    Ok(AggregateState {
        sum: sum
            .checked_mul(weight)
            .ok_or(OperatorError::WeightOverflow)?,
        count: count
            .checked_mul(weight)
            .ok_or(OperatorError::WeightOverflow)?,
    })
}

fn seed_records(key: &str, state: AggregateState) -> Vec<Tup2<AccountAmount, ZWeight>> {
    vec![
        Tup2(
            AccountAmount {
                account: key.to_string(),
                amount: state.sum,
            },
            1,
        ),
        Tup2(
            AccountAmount {
                account: key.to_string(),
                amount: 0,
            },
            state.count - 1,
        ),
    ]
}

fn apply_scalar_updates(
    materialized: &mut BTreeMap<String, AggregateState>,
    field: ScalarField,
    updates: OrdIndexedZSet<String, ZWeight>,
) {
    for (key, value, weight) in updates.iter() {
        let state = materialized
            .entry(key.clone())
            .or_insert(AggregateState { sum: 0, count: 0 });

        if weight > 0 {
            match field {
                ScalarField::Sum => state.sum = value,
                ScalarField::Count => state.count = value,
            }
        } else if weight < 0 {
            match field {
                ScalarField::Sum if state.sum == value => state.sum = 0,
                ScalarField::Count if state.count == value => state.count = 0,
                _ => {}
            }
        }
    }
}

fn delta_between_states(
    before: &BTreeMap<String, AggregateState>,
    after: &BTreeMap<String, AggregateState>,
) -> DeltaBatch {
    let keys = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut records = Vec::new();

    for key in keys {
        let previous = before.get(&key).copied().filter(|state| !state.is_zero());
        let next = after.get(&key).copied().filter(|state| !state.is_zero());

        if previous == next {
            continue;
        }
        if let Some(previous) = previous {
            records.push(DeltaRecord::new(
                DeltaKey::from_json(json!(key)),
                DeltaValue::from_json(json!({ "sum": previous.sum, "count": previous.count })),
                -1,
            ));
        }
        if let Some(next) = next {
            records.push(state_delta(&key, next.sum, next.count));
        }
    }

    batch(records)
}

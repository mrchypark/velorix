use std::collections::BTreeMap;

use dbsp::{
    typed_batch::IndexedZSetReader, utils::Tup2, DBSPHandle, OrdIndexedZSet, OutputHandle, Runtime,
    ZSetHandle, ZWeight,
};
use feldera_macros::IsNone;
use rkyv::{Archive, Serialize};
use serde_json::{json, Value};
use size_of::SizeOf;

use crate::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{EngineCheckpoint, EngineError, IncrementalEngine, LogicalEpoch},
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
struct DbspSumCountInputRecord {
    key_json: String,
    value: i64,
}

pub struct DbspSingleKeySumCountEngine {
    circuit: Option<DBSPHandle>,
    input: Option<ZSetHandle<DbspSumCountInputRecord>>,
    sum_output: Option<OutputHandle<OrdIndexedZSet<String, ZWeight>>>,
    count_output: Option<OutputHandle<OrdIndexedZSet<String, ZWeight>>>,
    logical_epoch: LogicalEpoch,
    materialized: BTreeMap<String, AggregateState>,
}

impl DbspSingleKeySumCountEngine {
    pub fn new() -> Self {
        let (circuit, (input, sum_output, count_output)) = Runtime::init_circuit(1, |circuit| {
            let (input_stream, input) = circuit.add_input_zset::<DbspSumCountInputRecord>();
            let indexed_amounts =
                input_stream.map_index(|record| (record.key_json.clone(), record.value));
            let sum_output = indexed_amounts
                .aggregate_linear(|amount| *amount as ZWeight)
                .output();
            let count_output = input_stream
                .map_index(|record| (record.key_json.clone(), ()))
                .weighted_count()
                .output();

            Ok((input, sum_output, count_output))
        })
        .expect("DBSP single-key sum/count circuit must build");

        Self {
            circuit: Some(circuit),
            input: Some(input),
            sum_output: Some(sum_output),
            count_output: Some(count_output),
            logical_epoch: 0,
            materialized: BTreeMap::new(),
        }
    }

    pub fn hydrate_from_checkpoint(checkpoint: EngineCheckpoint) -> Result<Self, EngineError> {
        let mut engine = Self::new();
        let mut seed = Vec::new();

        for record in checkpoint.state().net_rows().map_err(OperatorError::from)? {
            let key = parse_key(&record.key);
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
        mut records: Vec<Tup2<DbspSumCountInputRecord, ZWeight>>,
    ) -> Result<DeltaBatch, EngineError> {
        let before = self.materialized.clone();

        self.input
            .as_mut()
            .expect("DBSP input handle is present until drop")
            .append(&mut records);
        self.circuit
            .as_mut()
            .expect("DBSP circuit handle is present until drop")
            .transaction()
            .expect("DBSP single-key sum/count transaction must run");

        apply_scalar_updates(
            &mut self.materialized,
            ScalarField::Sum,
            self.sum_output
                .as_ref()
                .expect("DBSP sum output handle is present until drop")
                .consolidate(),
        );
        apply_scalar_updates(
            &mut self.materialized,
            ScalarField::Count,
            self.count_output
                .as_ref()
                .expect("DBSP count output handle is present until drop")
                .consolidate(),
        );
        self.materialized.retain(|_, state| !state.is_zero());

        Ok(delta_between_states(&before, &self.materialized))
    }
}

impl Drop for DbspSingleKeySumCountEngine {
    fn drop(&mut self) {
        let handles = (
            self.count_output.take(),
            self.sum_output.take(),
            self.input.take(),
            self.circuit.take(),
        );
        let _ = std::thread::spawn(move || drop(handles)).join();
    }
}

impl Default for DbspSingleKeySumCountEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl IncrementalEngine for DbspSingleKeySumCountEngine {
    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn push_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        signed_input_changes: &DeltaBatch,
    ) -> Result<DeltaBatch, EngineError> {
        if logical_epoch <= self.logical_epoch {
            return Err(EngineError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }

        let mut records = delta_batch_to_dbsp_records(signed_input_changes)?;
        let output = self.apply_dbsp_input(std::mem::take(&mut records))?;
        self.logical_epoch = logical_epoch;
        Ok(output)
    }

    fn materialized_state(&self) -> DeltaBatch {
        DeltaBatch::from_records(self.materialized.iter().map(|(key, state)| {
            DeltaRecord::new(
                delta_key_from_canonical_json(key),
                DeltaValue::from_json(json!({
                    "count": state.count,
                    "sum": state.sum,
                })),
                1,
            )
        }))
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
) -> Result<Vec<Tup2<DbspSumCountInputRecord, ZWeight>>, EngineError> {
    input
        .records()
        .iter()
        .map(|record| {
            Ok(Tup2(
                DbspSumCountInputRecord {
                    key_json: canonical_json(record.key.as_json()),
                    value: record
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

fn parse_key(key: &DeltaKey) -> String {
    canonical_json(key.as_json())
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

fn seed_records(key: &str, state: AggregateState) -> Vec<Tup2<DbspSumCountInputRecord, ZWeight>> {
    vec![
        Tup2(
            DbspSumCountInputRecord {
                key_json: key.to_string(),
                value: state.sum,
            },
            1,
        ),
        Tup2(
            DbspSumCountInputRecord {
                key_json: key.to_string(),
                value: 0,
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
    let mut records = Vec::new();
    for (key, previous) in before {
        if Some(previous) != after.get(key) {
            records.push(DeltaRecord::new(
                delta_key_from_canonical_json(key),
                DeltaValue::from_json(json!({
                    "count": previous.count,
                    "sum": previous.sum,
                })),
                -1,
            ));
        }
    }
    for (key, current) in after {
        if Some(current) != before.get(key) {
            records.push(DeltaRecord::new(
                delta_key_from_canonical_json(key),
                DeltaValue::from_json(json!({
                    "count": current.count,
                    "sum": current.sum,
                })),
                1,
            ));
        }
    }
    DeltaBatch::from_records(records)
}

fn delta_key_from_canonical_json(key: &str) -> DeltaKey {
    DeltaKey::from_json(
        serde_json::from_str(key).expect("DBSP materialized key must be canonical JSON"),
    )
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("serializing JSON scalar cannot fail")
        }
        Value::Array(values) => {
            let items = values.iter().map(canonical_json).collect::<Vec<_>>();
            format!("[{}]", items.join(","))
        }
        Value::Object(object) => {
            let mut fields = object.iter().collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(right.0));
            let items = fields
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key)
                            .expect("serializing JSON object key cannot fail"),
                        canonical_json(value)
                    )
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", items.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn input_delta(account: &str, amount: i64, weight: i64) -> DeltaRecord {
        DeltaRecord::new(
            DeltaKey::from_json(json!(account)),
            DeltaValue::from_json(json!(amount)),
            weight,
        )
    }

    fn batch(records: impl IntoIterator<Item = DeltaRecord>) -> DeltaBatch {
        DeltaBatch::from_records(records)
    }

    #[test]
    fn dbsp_engine_removes_state_when_retraction_cancels_sum_and_count() {
        let mut engine = DbspSingleKeySumCountEngine::new();
        engine
            .push_changes(1, &batch([input_delta("account-b", 7, 1)]))
            .unwrap();

        engine
            .push_changes(2, &batch([input_delta("account-b", 7, -1)]))
            .unwrap();

        assert_eq!(engine.materialized_state().net_rows().unwrap(), Vec::new());
    }

    #[test]
    fn dbsp_engine_updates_count_to_zero_when_only_count_value_is_retracted() {
        let mut engine = DbspSingleKeySumCountEngine::new();
        engine
            .push_changes(1, &batch([input_delta("account-c", 7, 1)]))
            .unwrap();

        engine
            .push_changes(2, &batch([input_delta("account-c", 1, -1)]))
            .unwrap();

        assert_eq!(
            engine.materialized_state().net_rows().unwrap(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("account-c")),
                DeltaValue::from_json(json!({ "count": 0, "sum": 6 })),
                1
            )]
        );
    }
}

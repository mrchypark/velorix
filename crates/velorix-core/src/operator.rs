use std::collections::BTreeMap;

use serde_json::{json, Value};
use thiserror::Error;

use crate::delta::{DeltaBatch, DeltaError, DeltaKey, DeltaRecord, DeltaValue, DeltaWeight};

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum OperatorError {
    #[error(transparent)]
    Delta(#[from] DeltaError),
    #[error("delta weight arithmetic overflowed")]
    WeightOverflow,
    #[error("aggregate input value must be a signed integer")]
    NonIntegerAggregateValue,
}

pub fn map_delta_batch<F>(input: &DeltaBatch, mut transform: F) -> Result<DeltaBatch, OperatorError>
where
    F: FnMut(&DeltaRecord) -> Result<(DeltaKey, DeltaValue), OperatorError>,
{
    let records = input
        .records()
        .iter()
        .map(|record| {
            let (key, value) = transform(record)?;
            Ok::<_, OperatorError>(DeltaRecord::new(key, value, record.weight))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DeltaBatch::from_records(records))
}

pub fn filter_delta_batch<F>(
    input: &DeltaBatch,
    mut predicate: F,
) -> Result<DeltaBatch, OperatorError>
where
    F: FnMut(&DeltaRecord) -> Result<bool, OperatorError>,
{
    let records = input
        .records()
        .iter()
        .filter_map(|record| match predicate(record) {
            Ok(true) => Some(Ok(record.clone())),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DeltaBatch::from_records(records))
}

pub struct KeyedEquiJoin<F>
where
    F: FnMut(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
{
    left: SideState,
    right: SideState,
    join_values: F,
}

impl<F> KeyedEquiJoin<F>
where
    F: FnMut(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
{
    pub fn new(join_values: F) -> Self {
        Self {
            left: SideState::default(),
            right: SideState::default(),
            join_values,
        }
    }

    pub fn apply_left(&mut self, input: &DeltaBatch) -> Result<DeltaBatch, OperatorError> {
        let output = join_against(input, &self.right, &mut self.join_values)?;
        self.left = self.left.applied(input)?;
        Ok(output)
    }

    pub fn apply_right(&mut self, input: &DeltaBatch) -> Result<DeltaBatch, OperatorError> {
        let mut output = Vec::new();

        for record in input.records() {
            for left in self.left.net_records_for_key(&record.key)? {
                let weight = checked_weight_product(record.weight, left.weight)?;
                let value = (self.join_values)(&left.value, &record.value)?;
                output.push(DeltaRecord::new(record.key.clone(), value, weight));
            }
        }

        self.right = self.right.applied(input)?;
        Ok(DeltaBatch::from_records(output))
    }

    pub fn left_state(&self) -> DeltaBatch {
        self.left.batch()
    }

    pub fn right_state(&self) -> DeltaBatch {
        self.right.batch()
    }
}

#[derive(Clone, Debug, Default)]
pub struct KeyedSumCountAggregate {
    state: BTreeMap<String, AggregateEntry>,
}

impl KeyedSumCountAggregate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, input: &DeltaBatch) -> Result<DeltaBatch, OperatorError> {
        let mut changes: BTreeMap<String, AggregateChange> = BTreeMap::new();

        for record in input.records() {
            let amount = record
                .value
                .as_json()
                .as_i64()
                .ok_or(OperatorError::NonIntegerAggregateValue)?;
            let key = canonical_json(record.key.as_json());
            let change = changes
                .entry(key)
                .or_insert_with(|| AggregateChange::new(record.key.clone()));
            change.add(amount, record.weight)?;
        }

        let mut output = Vec::new();

        for (key, change) in changes {
            let before = self.state.get(&key).cloned();
            let after = change.apply_to(before.clone())?;

            if before == after {
                continue;
            }

            if let Some(before) = before {
                output.push(before.to_record(-1)?);
            }

            match after {
                Some(after) => {
                    output.push(after.to_record(1)?);
                    self.state.insert(key, after);
                }
                None => {
                    self.state.remove(&key);
                }
            }
        }

        Ok(DeltaBatch::from_records(output))
    }

    pub fn state(&self) -> DeltaBatch {
        DeltaBatch::from_records(
            self.state
                .values()
                .map(|entry| entry.to_record(1))
                .collect::<Result<Vec<_>, _>>()
                .expect("stored aggregate state must fit delta records"),
        )
    }
}

fn join_against<F>(
    input: &DeltaBatch,
    other: &SideState,
    join_values: &mut F,
) -> Result<DeltaBatch, OperatorError>
where
    F: FnMut(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
{
    let mut output = Vec::new();

    for record in input.records() {
        for right in other.net_records_for_key(&record.key)? {
            let weight = checked_weight_product(record.weight, right.weight)?;
            let value = join_values(&record.value, &right.value)?;
            output.push(DeltaRecord::new(record.key.clone(), value, weight));
        }
    }

    Ok(DeltaBatch::from_records(output))
}

#[derive(Clone, Debug, Default)]
struct SideState {
    records_by_key: BTreeMap<String, BTreeMap<String, SideStateRecord>>,
}

impl SideState {
    fn applied(&self, input: &DeltaBatch) -> Result<Self, OperatorError> {
        let mut next = self.clone();

        for record in input.records() {
            let key = canonical_json(record.key.as_json());
            let value = canonical_json(record.value.as_json());
            let values = next.records_by_key.entry(key.clone()).or_default();
            let weight = i128::from(values.get(&value).map_or(0, |entry| entry.weight))
                .checked_add(i128::from(record.weight))
                .ok_or(OperatorError::WeightOverflow)?;
            let weight: DeltaWeight = weight
                .try_into()
                .map_err(|_| OperatorError::WeightOverflow)?;

            if weight == 0 {
                values.remove(&value);
            } else {
                values.insert(
                    value,
                    SideStateRecord {
                        key: record.key.clone(),
                        value: record.value.clone(),
                        weight,
                    },
                );
            }

            if values.is_empty() {
                next.records_by_key.remove(&key);
            }
        }

        Ok(next)
    }

    fn batch(&self) -> DeltaBatch {
        DeltaBatch::from_records(
            self.records_by_key
                .values()
                .flat_map(|values| values.values())
                .map(SideStateRecord::to_record)
                .collect::<Result<Vec<_>, _>>()
                .expect("stored join side state must fit delta records"),
        )
    }

    fn net_records_for_key(&self, key: &DeltaKey) -> Result<Vec<DeltaRecord>, OperatorError> {
        let Some(records) = self.records_by_key.get(&canonical_json(key.as_json())) else {
            return Ok(Vec::new());
        };

        records
            .values()
            .map(SideStateRecord::to_record)
            .collect::<Result<Vec<_>, _>>()
    }
}

#[derive(Clone, Debug)]
struct SideStateRecord {
    key: DeltaKey,
    value: DeltaValue,
    weight: DeltaWeight,
}

impl SideStateRecord {
    fn to_record(&self) -> Result<DeltaRecord, OperatorError> {
        Ok(DeltaRecord::new(
            self.key.clone(),
            self.value.clone(),
            self.weight,
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AggregateEntry {
    key: DeltaKey,
    sum: i128,
    count: i128,
}

impl AggregateEntry {
    fn to_record(&self, weight: DeltaWeight) -> Result<DeltaRecord, OperatorError> {
        let sum: i64 = self
            .sum
            .try_into()
            .map_err(|_| OperatorError::WeightOverflow)?;
        let count: i64 = self
            .count
            .try_into()
            .map_err(|_| OperatorError::WeightOverflow)?;

        Ok(DeltaRecord::new(
            self.key.clone(),
            DeltaValue::from_json(json!({
                "sum": sum,
                "count": count,
            })),
            weight,
        ))
    }
}

#[derive(Clone, Debug)]
struct AggregateChange {
    key: DeltaKey,
    sum_delta: i128,
    count_delta: i128,
}

impl AggregateChange {
    fn new(key: DeltaKey) -> Self {
        Self {
            key,
            sum_delta: 0,
            count_delta: 0,
        }
    }

    fn add(&mut self, amount: i64, weight: DeltaWeight) -> Result<(), OperatorError> {
        let weighted_amount = i128::from(amount)
            .checked_mul(i128::from(weight))
            .ok_or(OperatorError::WeightOverflow)?;
        self.sum_delta = self
            .sum_delta
            .checked_add(weighted_amount)
            .ok_or(OperatorError::WeightOverflow)?;
        self.count_delta = self
            .count_delta
            .checked_add(i128::from(weight))
            .ok_or(OperatorError::WeightOverflow)?;
        Ok(())
    }

    fn apply_to(
        self,
        before: Option<AggregateEntry>,
    ) -> Result<Option<AggregateEntry>, OperatorError> {
        let sum = before.as_ref().map_or(0, |entry| entry.sum);
        let count = before.as_ref().map_or(0, |entry| entry.count);
        let sum = sum
            .checked_add(self.sum_delta)
            .ok_or(OperatorError::WeightOverflow)?;
        let count = count
            .checked_add(self.count_delta)
            .ok_or(OperatorError::WeightOverflow)?;

        if sum == 0 && count == 0 {
            Ok(None)
        } else {
            Ok(Some(AggregateEntry {
                key: before.map_or(self.key, |entry| entry.key),
                sum,
                count,
            }))
        }
    }
}

fn checked_weight_product(
    left: DeltaWeight,
    right: DeltaWeight,
) -> Result<DeltaWeight, OperatorError> {
    left.checked_mul(right).ok_or(OperatorError::WeightOverflow)
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("serializing JSON scalar cannot fail")
        }
        Value::Array(values) => {
            let items = values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{items}]")
        }
        Value::Object(values) => {
            let mut fields = values
                .iter()
                .map(|(key, value)| {
                    let key = serde_json::to_string(key).expect("serializing JSON key cannot fail");
                    format!("{key}:{}", canonical_json(value))
                })
                .collect::<Vec<_>>();
            fields.sort();
            let fields = fields.join(",");
            format!("{{{fields}}}")
        }
    }
}

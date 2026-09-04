use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

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
    #[error("aggregate input value must be a Decimal128 string")]
    NonDecimalAggregateValue,
    #[error("aggregate decimal value exceeds declared precision")]
    DecimalPrecisionOverflow,
    #[error("aggregate state value must contain integer `sum` and `count` fields")]
    InvalidAggregateStateValue,
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

#[derive(Debug)]
pub struct KeyedEquiJoin<F>
where
    F: FnMut(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
{
    left: SideState,
    right: SideState,
    join_values: F,
    instance_id: u64,
    revision: u64,
}

impl<F> Clone for KeyedEquiJoin<F>
where
    F: FnMut(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            right: self.right.clone(),
            join_values: self.join_values.clone(),
            instance_id: NEXT_JOIN_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            revision: self.revision,
        }
    }
}

static NEXT_JOIN_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

/// Selects a side of a keyed equi-join for a prepared epoch input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinInputSide {
    Left,
    Right,
}

/// An opaque, validated join epoch.  It contains only the cells first touched
/// by the epoch, bound to the join instance and its base revision.
#[derive(Debug)]
pub struct PreparedKeyedEquiJoinEpoch {
    base_revision: u64,
    join_instance_id: u64,
    left_overlay: SideOverlay,
    right_overlay: SideOverlay,
    touched_keys: BTreeMap<String, DeltaKey>,
    output: DeltaBatch,
}

impl PreparedKeyedEquiJoinEpoch {
    pub fn output_changes(&self) -> &DeltaBatch {
        &self.output
    }

    pub fn touched_keys(&self, side: JoinInputSide) -> Vec<DeltaKey> {
        self.touched_keys
            .values()
            .filter(|key| {
                self.overlay(side)
                    .contains_key(&canonical_json(key.as_json()))
            })
            .cloned()
            .collect()
    }

    fn overlay(&self, side: JoinInputSide) -> &SideOverlay {
        match side {
            JoinInputSide::Left => &self.left_overlay,
            JoinInputSide::Right => &self.right_overlay,
        }
    }
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
            instance_id: NEXT_JOIN_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            revision: 0,
        }
    }

    pub fn apply_left(&mut self, input: &DeltaBatch) -> Result<DeltaBatch, OperatorError> {
        let output = join_against(input, &self.right, &mut self.join_values)?;
        self.left = self.left.applied(input)?;
        self.revision = self.revision.wrapping_add(1);
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
        self.revision = self.revision.wrapping_add(1);
        Ok(DeltaBatch::from_records(output))
    }

    /// Prepares an ordered epoch without changing either join side.  Records
    /// read the base state merged with earlier inputs' per-key overlays.
    pub fn prepare_epoch(
        &self,
        _logical_epoch: u64,
        inputs: &[(JoinInputSide, &DeltaBatch)],
    ) -> Result<PreparedKeyedEquiJoinEpoch, OperatorError>
    where
        F: Fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
    {
        let mut left_overlay = SideOverlay::default();
        let mut right_overlay = SideOverlay::default();
        let mut touched_keys = BTreeMap::new();
        let mut output = Vec::new();
        for (side, input) in inputs {
            for record in input.records() {
                touched_keys
                    .entry(canonical_json(record.key.as_json()))
                    .or_insert_with(|| record.key.clone());
                let other = match side {
                    JoinInputSide::Left => {
                        merged_records_for_key(&self.right, &right_overlay, &record.key)?
                    }
                    JoinInputSide::Right => {
                        merged_records_for_key(&self.left, &left_overlay, &record.key)?
                    }
                };
                for other_record in other {
                    let weight = checked_weight_product(record.weight, other_record.weight)?;
                    let value = match side {
                        JoinInputSide::Left => {
                            (self.join_values)(&record.value, &other_record.value)?
                        }
                        JoinInputSide::Right => {
                            (self.join_values)(&other_record.value, &record.value)?
                        }
                    };
                    output.push(DeltaRecord::new(record.key.clone(), value, weight));
                }
                let (base, overlay) = match side {
                    JoinInputSide::Left => (&self.left, &mut left_overlay),
                    JoinInputSide::Right => (&self.right, &mut right_overlay),
                };
                apply_overlay_record(base, overlay, record)?;
            }
        }
        Ok(PreparedKeyedEquiJoinEpoch {
            base_revision: self.revision,
            join_instance_id: self.instance_id,
            left_overlay,
            right_overlay,
            touched_keys,
            output: DeltaBatch::from_records(output),
        })
    }

    /// Validates the opaque epoch before mutating, then applies precisely its
    /// touched cells.  All arithmetic has already completed in preparation.
    pub fn commit_prepared_epoch(
        &mut self,
        prepared: PreparedKeyedEquiJoinEpoch,
    ) -> Result<(), OperatorError> {
        self.validate_prepared_epoch(&prepared)?;
        apply_overlay(&mut self.left, prepared.left_overlay);
        apply_overlay(&mut self.right, prepared.right_overlay);
        self.revision = self.revision.wrapping_add(1);
        Ok(())
    }

    /// Rejects cross-instance and stale tokens before any caller commits a
    /// multi-operator epoch.
    pub fn validate_prepared_epoch(
        &self,
        prepared: &PreparedKeyedEquiJoinEpoch,
    ) -> Result<(), OperatorError> {
        if prepared.join_instance_id != self.instance_id || prepared.base_revision != self.revision
        {
            return Err(OperatorError::WeightOverflow);
        }
        Ok(())
    }

    pub fn prepared_side_records(
        &self,
        prepared: &PreparedKeyedEquiJoinEpoch,
        side: JoinInputSide,
        key: &DeltaKey,
        after: bool,
    ) -> Result<Vec<DeltaRecord>, OperatorError> {
        let base = match side {
            JoinInputSide::Left => &self.left,
            JoinInputSide::Right => &self.right,
        };
        if after {
            merged_records_for_key(base, prepared.overlay(side), key)
        } else {
            base.net_records_for_key(key)
        }
    }

    pub fn left_state(&self) -> DeltaBatch {
        self.left.batch()
    }

    pub fn right_state(&self) -> DeltaBatch {
        self.right.batch()
    }

    pub fn restore_state(
        &mut self,
        left: &DeltaBatch,
        right: &DeltaBatch,
    ) -> Result<(), OperatorError> {
        let restored_left = SideState::default().applied(left)?;
        let restored_right = SideState::default().applied(right)?;
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or(OperatorError::WeightOverflow)?;
        self.left = restored_left;
        self.right = restored_right;
        self.revision = next_revision;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AggregateValueMode {
    #[default]
    Integer,
    Decimal128 {
        precision: u8,
        scale: u8,
    },
}

impl AggregateValueMode {
    fn parse_input(self, value: &DeltaValue) -> Result<i128, OperatorError> {
        match self {
            Self::Integer => {
                let v = value.as_json();
                v.as_i64()
                    .map(i128::from)
                    .ok_or(OperatorError::NonIntegerAggregateValue)
            }
            Self::Decimal128 { precision, scale } => {
                parse_decimal128_value(value.as_json(), precision, scale)
            }
        }
    }

    fn parse_state_sum(self, value: &Value) -> Result<i128, OperatorError> {
        match self {
            Self::Integer => value
                .as_i64()
                .map(i128::from)
                .ok_or(OperatorError::InvalidAggregateStateValue),
            Self::Decimal128 { precision, scale } => {
                parse_decimal128_value_for_state(value, precision, scale)
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct KeyedSumCountAggregate {
    state: BTreeMap<String, AggregateEntry>,
    value_mode: AggregateValueMode,
    track_extrema: bool,
}

/// A validated, not-yet-applied aggregate update.  This intentionally keeps
/// only the keys touched by an input epoch; callers must commit it exactly
/// once, after all publication work that can fail has completed.
#[derive(Debug)]
pub(crate) struct PreparedAggregateChanges {
    changes: BTreeMap<String, Option<AggregateEntry>>,
    output: DeltaBatch,
}

impl PreparedAggregateChanges {
    pub(crate) fn output_changes(&self) -> &DeltaBatch {
        &self.output
    }
}

impl KeyedSumCountAggregate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_value_mode(value_mode: AggregateValueMode) -> Self {
        Self::with_value_mode_and_extrema(value_mode, false)
    }

    pub fn with_value_mode_and_extrema(
        value_mode: AggregateValueMode,
        track_extrema: bool,
    ) -> Self {
        Self {
            state: BTreeMap::new(),
            value_mode,
            track_extrema,
        }
    }

    pub fn from_state(state: &DeltaBatch) -> Result<Self, OperatorError> {
        Self::from_state_with_value_mode(state, AggregateValueMode::Integer)
    }

    pub fn from_state_with_value_mode(
        state: &DeltaBatch,
        value_mode: AggregateValueMode,
    ) -> Result<Self, OperatorError> {
        Self::from_state_with_value_mode_and_extrema(state, value_mode, false)
    }

    pub fn from_state_with_value_mode_and_extrema(
        state: &DeltaBatch,
        value_mode: AggregateValueMode,
        track_extrema: bool,
    ) -> Result<Self, OperatorError> {
        let mut aggregate = Self::with_value_mode_and_extrema(value_mode, track_extrema);

        for record in state.records() {
            let (sum, count, values) =
                aggregate_state_sum_count_values(&record.value, value_mode, track_extrema)?;
            let key = canonical_json(record.key.as_json());
            let entry = aggregate
                .state
                .entry(key.clone())
                .or_insert_with(|| AggregateEntry {
                    key: record.key.clone(),
                    sum: 0,
                    count: 0,
                    values: BTreeMap::new(),
                });

            entry.add_weighted(sum, count, record.weight)?;
            if track_extrema {
                for (order_value, value) in &values {
                    entry.add_value_weight(
                        *order_value,
                        &value.value,
                        value.weight,
                        record.weight,
                    )?;
                }
            }
            if entry.is_zero() {
                aggregate.state.remove(&key);
            }
        }
        validate_aggregate_entries(&aggregate.state, value_mode, track_extrema)?;

        Ok(aggregate)
    }

    pub(crate) fn prepare(
        &self,
        input: &DeltaBatch,
    ) -> Result<PreparedAggregateChanges, OperatorError> {
        let mut changes: BTreeMap<String, AggregateChange> = BTreeMap::new();

        for record in input.records() {
            let amount = self.value_mode.parse_input(&record.value)?;
            let value = aggregate_sum_json_value(amount, self.value_mode)?;
            let key = canonical_json(record.key.as_json());
            let change = changes
                .entry(key)
                .or_insert_with(|| AggregateChange::new(record.key.clone()));
            change.add(amount, value, record.weight)?;
        }

        let mut output = Vec::new();
        let mut prepared = BTreeMap::new();

        for (key, change) in changes {
            let before = self.state.get(&key).cloned();
            let after = change.apply_to(before.clone(), self.track_extrema)?;

            if before == after {
                continue;
            }

            if let Some(before) = before {
                output.push(before.to_record(-1, self.value_mode, self.track_extrema)?);
            }

            match after {
                Some(after) => {
                    output.push(after.to_record(1, self.value_mode, self.track_extrema)?);
                    prepared.insert(key, Some(after));
                }
                None => {
                    prepared.insert(key, None);
                }
            }
        }

        Ok(PreparedAggregateChanges {
            changes: prepared,
            output: DeltaBatch::from_records(output),
        })
    }

    /// Applies a change set produced by [`Self::prepare`].  Preparation has
    /// already performed every fallible calculation, so this operation is
    /// deliberately infallible.
    pub(crate) fn commit(&mut self, prepared: PreparedAggregateChanges) -> DeltaBatch {
        for (key, after) in prepared.changes {
            match after {
                Some(entry) => {
                    self.state.insert(key, entry);
                }
                None => {
                    self.state.remove(&key);
                }
            }
        }
        prepared.output
    }

    pub fn apply(&mut self, input: &DeltaBatch) -> Result<DeltaBatch, OperatorError> {
        let prepared = self.prepare(input)?;
        Ok(self.commit(prepared))
    }

    pub fn state(&self) -> DeltaBatch {
        DeltaBatch::from_records(
            self.state
                .values()
                .map(|entry| entry.to_record(1, self.value_mode, self.track_extrema))
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

/// A sparse replacement map.  An entry is created only when an epoch first
/// touches a key/value cell; `None` means that cell is pruned at commit.
type SideOverlay = BTreeMap<String, BTreeMap<String, Option<SideStateRecord>>>;

fn merged_records_for_key(
    base: &SideState,
    overlay: &SideOverlay,
    key: &DeltaKey,
) -> Result<Vec<DeltaRecord>, OperatorError> {
    let key_text = canonical_json(key.as_json());
    let mut values = base
        .records_by_key
        .get(&key_text)
        .cloned()
        .unwrap_or_default();
    if let Some(replacements) = overlay.get(&key_text) {
        for (value, replacement) in replacements {
            match replacement {
                Some(record) => {
                    values.insert(value.clone(), record.clone());
                }
                None => {
                    values.remove(value);
                }
            }
        }
    }
    values.values().map(SideStateRecord::to_record).collect()
}

fn apply_overlay_record(
    base: &SideState,
    overlay: &mut SideOverlay,
    record: &DeltaRecord,
) -> Result<(), OperatorError> {
    let key = canonical_json(record.key.as_json());
    let value = canonical_json(record.value.as_json());
    let previous = match overlay.get(&key).and_then(|values| values.get(&value)) {
        Some(entry) => entry.clone(),
        None => base
            .records_by_key
            .get(&key)
            .and_then(|values| values.get(&value))
            .cloned(),
    };
    let weight = i128::from(previous.as_ref().map_or(0, |entry| entry.weight))
        .checked_add(i128::from(record.weight))
        .ok_or(OperatorError::WeightOverflow)?;
    let weight: DeltaWeight = weight
        .try_into()
        .map_err(|_| OperatorError::WeightOverflow)?;
    let replacement = (weight != 0).then(|| SideStateRecord {
        key: record.key.clone(),
        value: record.value.clone(),
        weight,
    });
    overlay.entry(key).or_default().insert(value, replacement);
    Ok(())
}

fn apply_overlay(base: &mut SideState, overlay: SideOverlay) {
    for (key, replacements) in overlay {
        let values = base.records_by_key.entry(key.clone()).or_default();
        for (value, replacement) in replacements {
            match replacement {
                Some(record) => {
                    values.insert(value, record);
                }
                None => {
                    values.remove(&value);
                }
            }
        }
        if values.is_empty() {
            base.records_by_key.remove(&key);
        }
    }
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
    values: BTreeMap<i128, AggregateValueEntry>,
}

impl AggregateEntry {
    fn add_weighted(
        &mut self,
        sum: i128,
        count: i64,
        weight: DeltaWeight,
    ) -> Result<(), OperatorError> {
        let sum_delta = sum
            .checked_mul(i128::from(weight))
            .ok_or(OperatorError::WeightOverflow)?;
        let count_delta = i128::from(count)
            .checked_mul(i128::from(weight))
            .ok_or(OperatorError::WeightOverflow)?;
        self.sum = self
            .sum
            .checked_add(sum_delta)
            .ok_or(OperatorError::WeightOverflow)?;
        self.count = self
            .count
            .checked_add(count_delta)
            .ok_or(OperatorError::WeightOverflow)?;
        Ok(())
    }

    fn is_zero(&self) -> bool {
        self.sum == 0 && self.count == 0 && self.values.is_empty()
    }

    fn to_record(
        &self,
        weight: DeltaWeight,
        value_mode: AggregateValueMode,
        track_extrema: bool,
    ) -> Result<DeltaRecord, OperatorError> {
        let count: i64 = self
            .count
            .try_into()
            .map_err(|_| OperatorError::WeightOverflow)?;
        let sum = aggregate_sum_json_value(self.sum, value_mode)?;
        let mut value = serde_json::Map::new();
        value.insert("sum".to_string(), sum);
        value.insert("count".to_string(), json!(count));
        if track_extrema {
            let Some(min) = self.values.values().next().map(|entry| entry.value.clone()) else {
                return Err(OperatorError::InvalidAggregateStateValue);
            };
            let Some(max) = self
                .values
                .values()
                .next_back()
                .map(|entry| entry.value.clone())
            else {
                return Err(OperatorError::InvalidAggregateStateValue);
            };
            value.insert("min".to_string(), min);
            value.insert("max".to_string(), max);
            value.insert(
                "values".to_string(),
                Value::Array(
                    self.values
                        .values()
                        .map(|entry| {
                            let weight: i64 = entry
                                .weight
                                .try_into()
                                .map_err(|_| OperatorError::WeightOverflow)?;
                            Ok(json!({
                                "value": entry.value,
                                "weight": weight,
                            }))
                        })
                        .collect::<Result<Vec<_>, OperatorError>>()?,
                ),
            );
        }

        Ok(DeltaRecord::new(
            self.key.clone(),
            DeltaValue::from_json(Value::Object(value)),
            weight,
        ))
    }

    fn add_value_weight(
        &mut self,
        order_value: i128,
        value: &Value,
        value_weight: i128,
        record_weight: DeltaWeight,
    ) -> Result<(), OperatorError> {
        let delta = value_weight
            .checked_mul(i128::from(record_weight))
            .ok_or(OperatorError::WeightOverflow)?;
        let next_weight = self
            .values
            .get(&order_value)
            .map_or(0, |entry| entry.weight)
            .checked_add(delta)
            .ok_or(OperatorError::WeightOverflow)?;
        if next_weight < 0 {
            return Err(OperatorError::InvalidAggregateStateValue);
        }
        if next_weight == 0 {
            self.values.remove(&order_value);
        } else {
            self.values.insert(
                order_value,
                AggregateValueEntry {
                    value: value.clone(),
                    weight: next_weight,
                },
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AggregateValueEntry {
    value: Value,
    weight: i128,
}

fn aggregate_state_sum_count_values(
    value: &DeltaValue,
    value_mode: AggregateValueMode,
    track_extrema: bool,
) -> Result<(i128, i64, BTreeMap<i128, AggregateValueEntry>), OperatorError> {
    let value = value.as_json();
    let sum = value
        .get("sum")
        .ok_or(OperatorError::InvalidAggregateStateValue)
        .and_then(|sum| value_mode.parse_state_sum(sum))?;
    let count = value
        .get("count")
        .and_then(Value::as_i64)
        .ok_or(OperatorError::InvalidAggregateStateValue)?;

    let values = if track_extrema {
        let values = value
            .get("values")
            .and_then(Value::as_array)
            .ok_or(OperatorError::InvalidAggregateStateValue)?;
        let mut state_values = BTreeMap::new();
        for value in values {
            let value = value
                .as_object()
                .ok_or(OperatorError::InvalidAggregateStateValue)?;
            let aggregate_value = value
                .get("value")
                .cloned()
                .ok_or(OperatorError::InvalidAggregateStateValue)?;
            let order_value = value_mode.parse_state_sum(&aggregate_value)?;
            let weight = value
                .get("weight")
                .and_then(Value::as_i64)
                .ok_or(OperatorError::InvalidAggregateStateValue)?;
            if weight == 0 {
                return Err(OperatorError::InvalidAggregateStateValue);
            }
            state_values.insert(
                order_value,
                AggregateValueEntry {
                    value: aggregate_value,
                    weight: i128::from(weight),
                },
            );
        }
        if state_values.is_empty() {
            return Err(OperatorError::InvalidAggregateStateValue);
        }
        state_values
    } else {
        BTreeMap::new()
    };

    Ok((sum, count, values))
}

fn aggregate_sum_json_value(
    sum: i128,
    value_mode: AggregateValueMode,
) -> Result<Value, OperatorError> {
    match value_mode {
        AggregateValueMode::Integer => {
            let sum: i64 = sum.try_into().map_err(|_| OperatorError::WeightOverflow)?;
            Ok(json!(sum))
        }
        AggregateValueMode::Decimal128 { precision, scale } => {
            ensure_decimal128_precision(sum.unsigned_abs(), precision)?;
            Ok(json!(format_decimal128_value(sum, scale)))
        }
    }
}

fn validate_aggregate_entries(
    state: &BTreeMap<String, AggregateEntry>,
    value_mode: AggregateValueMode,
    track_extrema: bool,
) -> Result<(), OperatorError> {
    for entry in state.values() {
        entry.to_record(1, value_mode, track_extrema)?;
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct AggregateChange {
    key: DeltaKey,
    sum_delta: i128,
    count_delta: i128,
    value_deltas: BTreeMap<i128, AggregateValueEntry>,
}

impl AggregateChange {
    fn new(key: DeltaKey) -> Self {
        Self {
            key,
            sum_delta: 0,
            count_delta: 0,
            value_deltas: BTreeMap::new(),
        }
    }

    fn add(
        &mut self,
        amount: i128,
        value: Value,
        weight: DeltaWeight,
    ) -> Result<(), OperatorError> {
        let weighted_amount = amount
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
        let next_weight = self
            .value_deltas
            .get(&amount)
            .map_or(0, |entry| entry.weight)
            .checked_add(i128::from(weight))
            .ok_or(OperatorError::WeightOverflow)?;
        if next_weight == 0 {
            self.value_deltas.remove(&amount);
        } else {
            self.value_deltas.insert(
                amount,
                AggregateValueEntry {
                    value,
                    weight: next_weight,
                },
            );
        }
        Ok(())
    }

    fn apply_to(
        self,
        before: Option<AggregateEntry>,
        track_extrema: bool,
    ) -> Result<Option<AggregateEntry>, OperatorError> {
        let sum = before.as_ref().map_or(0, |entry| entry.sum);
        let count = before.as_ref().map_or(0, |entry| entry.count);
        let mut values = before
            .as_ref()
            .map_or_else(BTreeMap::new, |entry| entry.values.clone());
        let sum = sum
            .checked_add(self.sum_delta)
            .ok_or(OperatorError::WeightOverflow)?;
        let count = count
            .checked_add(self.count_delta)
            .ok_or(OperatorError::WeightOverflow)?;

        if track_extrema {
            for (order_value, value) in &self.value_deltas {
                add_value_weight_to_map(
                    &mut values,
                    *order_value,
                    &value.value,
                    value.weight,
                    true,
                )?;
            }
        }

        if sum == 0 && count == 0 && (!track_extrema || values.is_empty()) {
            Ok(None)
        } else {
            Ok(Some(AggregateEntry {
                key: before.map_or(self.key, |entry| entry.key),
                sum,
                count,
                values,
            }))
        }
    }
}

fn add_value_weight_to_map(
    values: &mut BTreeMap<i128, AggregateValueEntry>,
    order_value: i128,
    value: &Value,
    weight_delta: i128,
    reject_negative: bool,
) -> Result<(), OperatorError> {
    let next_weight = values
        .get(&order_value)
        .map_or(0, |entry| entry.weight)
        .checked_add(weight_delta)
        .ok_or(OperatorError::WeightOverflow)?;
    if reject_negative && next_weight < 0 {
        return Err(OperatorError::InvalidAggregateStateValue);
    }
    if next_weight == 0 {
        values.remove(&order_value);
    } else {
        values.insert(
            order_value,
            AggregateValueEntry {
                value: value.clone(),
                weight: next_weight,
            },
        );
    }
    Ok(())
}

fn parse_decimal128_value(value: &Value, precision: u8, scale: u8) -> Result<i128, OperatorError> {
    let value = value
        .as_str()
        .ok_or(OperatorError::NonDecimalAggregateValue)?;
    parse_decimal128_str(value, precision, scale).ok_or(OperatorError::NonDecimalAggregateValue)
}

fn parse_decimal128_value_for_state(
    value: &Value,
    precision: u8,
    scale: u8,
) -> Result<i128, OperatorError> {
    let value = value
        .as_str()
        .ok_or(OperatorError::InvalidAggregateStateValue)?;
    parse_decimal128_str(value, precision, scale).ok_or(OperatorError::InvalidAggregateStateValue)
}

fn parse_decimal128_str(value: &str, precision: u8, scale: u8) -> Option<i128> {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    if digits.is_empty() {
        return None;
    }

    let scale = usize::from(scale);
    let (whole, fractional) = match digits.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None if scale == 0 => (digits, ""),
        None => return None,
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fractional.len() != scale
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let mut magnitude = whole.parse::<i128>().ok()?;
    let factor = decimal128_scale_factor(scale)?;
    magnitude = magnitude.checked_mul(factor)?;
    if scale > 0 {
        magnitude = magnitude.checked_add(fractional.parse::<i128>().ok()?)?;
    }
    ensure_decimal128_precision(magnitude.unsigned_abs(), precision).ok()?;

    let signed_magnitude = if negative {
        magnitude.checked_neg()?
    } else {
        magnitude
    };
    if format_decimal128_value(signed_magnitude, scale as u8) != value {
        return None;
    }

    Some(signed_magnitude)
}

fn format_decimal128_value(value: i128, scale: u8) -> String {
    let mut digits = value.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let mut decimal = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        let leading_zeroes = "0".repeat(scale - digits.len());
        format!("0.{leading_zeroes}{digits}")
    } else {
        let fractional = digits.split_off(digits.len() - scale);
        format!("{digits}.{fractional}")
    };

    if value.is_negative() {
        decimal.insert(0, '-');
    }

    decimal
}

fn decimal128_scale_factor(scale: usize) -> Option<i128> {
    let mut factor = 1_i128;
    for _ in 0..scale {
        factor = factor.checked_mul(10)?;
    }
    Some(factor)
}

fn ensure_decimal128_precision(value: u128, precision: u8) -> Result<(), OperatorError> {
    let digits = if value == 0 {
        1
    } else {
        value.ilog10() as u8 + 1
    };
    if digits > precision {
        Err(OperatorError::DecimalPrecisionOverflow)
    } else {
        Ok(())
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

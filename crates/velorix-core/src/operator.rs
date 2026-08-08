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

#[derive(Clone, Debug)]
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
                    .or_else(|| v.as_f64().map(|f| f as i128))
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

    pub fn apply(&mut self, input: &DeltaBatch) -> Result<DeltaBatch, OperatorError> {
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
        let mut next_state = self.state.clone();

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
                    next_state.insert(key, after);
                }
                None => {
                    next_state.remove(&key);
                }
            }
        }

        self.state = next_state;
        Ok(DeltaBatch::from_records(output))
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

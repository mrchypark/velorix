use serde_json::Value;

pub type DeltaWeight = i64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaKey(Value);

impl DeltaKey {
    pub fn from_json(value: Value) -> Self {
        Self(value)
    }

    pub fn as_json(&self) -> &Value {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaValue(Value);

impl DeltaValue {
    pub fn from_json(value: Value) -> Self {
        Self(value)
    }

    pub fn as_json(&self) -> &Value {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaRecord {
    pub key: DeltaKey,
    pub value: DeltaValue,
    pub weight: DeltaWeight,
}

impl DeltaRecord {
    pub fn new(key: DeltaKey, value: DeltaValue, weight: DeltaWeight) -> Self {
        Self { key, value, weight }
    }

    pub fn inverse(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            weight: -self.weight,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeltaBatch {
    records: Vec<DeltaRecord>,
}

impl DeltaBatch {
    pub fn from_records(records: impl IntoIterator<Item = DeltaRecord>) -> Self {
        Self {
            records: records.into_iter().collect(),
        }
    }

    pub fn records(&self) -> &[DeltaRecord] {
        &self.records
    }

    pub fn combine(&self, other: &Self) -> Self {
        let mut records = Vec::with_capacity(self.records.len() + other.records.len());
        records.extend(self.records.iter().cloned());
        records.extend(other.records.iter().cloned());
        Self { records }
    }

    pub fn inverse(&self) -> Self {
        Self {
            records: self.records.iter().map(DeltaRecord::inverse).collect(),
        }
    }

    pub fn net_rows(&self) -> Vec<DeltaRecord> {
        let mut net: Vec<DeltaRecord> = Vec::new();

        for record in &self.records {
            if let Some(existing) = net
                .iter_mut()
                .find(|existing| existing.key == record.key && existing.value == record.value)
            {
                existing.weight += record.weight;
            } else {
                net.push(record.clone());
            }
        }

        net.into_iter()
            .filter(|record| record.weight != 0)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{json, Value};

    #[test]
    fn delta_batch_net_rows_inserts_positive_weighted_rows() {
        let batch = DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!("account:1")),
            DeltaValue::from_json(json!({ "balance": 100 })),
            1,
        )]);

        assert_eq!(
            batch.net_rows(),
            vec![DeltaRecord::new(
                DeltaKey::from_json(json!("account:1")),
                DeltaValue::from_json(json!({ "balance": 100 })),
                1,
            )]
        );
    }

    #[test]
    fn delta_batch_net_rows_retracts_positive_rows_with_negative_weight() {
        let inserted = DeltaRecord::new(
            DeltaKey::from_json(json!("account:1")),
            DeltaValue::from_json(json!({ "balance": 100 })),
            1,
        );
        let retracted = DeltaRecord::new(
            DeltaKey::from_json(json!("account:1")),
            DeltaValue::from_json(json!({ "balance": 100 })),
            -1,
        );

        let batch = DeltaBatch::from_records([inserted, retracted]);

        assert!(batch.net_rows().is_empty());
    }

    #[test]
    fn delta_batch_combine_preserves_net_row_weight() {
        let key = DeltaKey::from_json(json!("account:1"));
        let value = DeltaValue::from_json(json!({ "balance": 100 }));
        let first = DeltaBatch::from_records([
            DeltaRecord::new(key.clone(), value.clone(), 3),
            DeltaRecord::new(
                DeltaKey::from_json(json!("account:2")),
                DeltaValue::from_json(json!({ "balance": 50 })),
                1,
            ),
        ]);
        let second = DeltaBatch::from_records([
            DeltaRecord::new(key.clone(), value.clone(), -2),
            DeltaRecord::new(
                DeltaKey::from_json(json!("account:2")),
                DeltaValue::from_json(json!({ "balance": 50 })),
                -1,
            ),
        ]);

        let combined = first.combine(&second);

        assert_eq!(combined.net_rows(), vec![DeltaRecord::new(key, value, 1)]);
    }

    proptest! {
        #[test]
        fn delta_batch_combine_is_associative(
            left in batch_strategy(),
            middle in batch_strategy(),
            right in batch_strategy(),
        ) {
            let lhs = left.combine(&middle).combine(&right);
            let rhs = left.combine(&middle.combine(&right));

            prop_assert_eq!(lhs.net_rows(), rhs.net_rows());
        }

        #[test]
        fn delta_batch_combined_with_inverse_has_empty_net_result(batch in batch_strategy()) {
            let inverse = batch.inverse();

            prop_assert!(batch.combine(&inverse).net_rows().is_empty());
        }
    }

    fn batch_strategy() -> impl Strategy<Value = DeltaBatch> {
        prop::collection::vec(record_strategy(), 0..32).prop_map(DeltaBatch::from_records)
    }

    fn record_strategy() -> impl Strategy<Value = DeltaRecord> {
        (key_strategy(), value_strategy(), -8i64..=8)
            .prop_filter(
                "zero weight does not represent a delta",
                |(_, _, weight)| *weight != 0,
            )
            .prop_map(|(key, value, weight)| DeltaRecord::new(key, value, weight))
    }

    fn key_strategy() -> impl Strategy<Value = DeltaKey> {
        json_value_strategy().prop_map(DeltaKey::from_json)
    }

    fn value_strategy() -> impl Strategy<Value = DeltaValue> {
        json_value_strategy().prop_map(DeltaValue::from_json)
    }

    fn json_value_strategy() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<bool>().prop_map(Value::Bool),
            (-1000i64..=1000).prop_map(|number| json!(number)),
            "[a-z0-9:_-]{0,16}".prop_map(Value::String),
        ]
    }
}

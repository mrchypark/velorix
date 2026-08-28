use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub type DeltaWeight = i64;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DeltaError {
    #[error("delta weight arithmetic overflowed")]
    WeightOverflow,
}

/// Memcomparable binary key for ordered BTreeMap operations.
///
/// Stores key and value encodings as a single byte sequence with proper
/// JSON string escaping to prevent collision between different (key, value)
/// pairs. The single-buffer approach minimizes memory overhead while the
/// escaping ensures injective encoding.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TypedBinaryKey(Vec<u8>);

impl Ord for TypedBinaryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for TypedBinaryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TypedBinaryKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Encode a JSON value into a memcomparable binary format that preserves
/// the same sort order as canonical JSON strings (lexicographic).
///
/// This is equivalent to canonical_json() but produces bytes instead of strings,
/// enabling O(1) comparison in BTreeMap without parsing.
fn encode_json_ordered(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::Null => buf.extend_from_slice(b"null"),
        Value::Bool(false) => buf.extend_from_slice(b"false"),
        Value::Bool(true) => buf.extend_from_slice(b"true"),
        Value::Number(n) => {
            buf.extend_from_slice(n.to_string().as_bytes());
        }
        Value::String(s) => {
            buf.push(b'"');
            // Escape characters that could cause boundary ambiguity
            for byte in s.as_bytes() {
                match byte {
                    b'"' => buf.extend_from_slice(b"\\\""),
                    b'\\' => buf.extend_from_slice(b"\\\\"),
                    b'\n' => buf.extend_from_slice(b"\\n"),
                    b'\r' => buf.extend_from_slice(b"\\r"),
                    b'\t' => buf.extend_from_slice(b"\\t"),
                    b if b.is_ascii_control() => {
                        buf.extend_from_slice(format!("\\u{:04x}", b).as_bytes());
                    }
                    b => buf.push(*b),
                }
            }
            buf.push(b'"');
        }
        Value::Array(arr) => {
            buf.push(b'[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                encode_json_ordered(item, buf);
            }
            buf.push(b']');
        }
        Value::Object(map) => {
            buf.push(b'{');
            let mut fields: Vec<_> = map.iter().collect();
            fields.sort_by_key(|(k, _)| (*k).clone());
            for (i, (key, value)) in fields.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                buf.push(b'"');
                buf.extend_from_slice(key.as_bytes());
                buf.push(b'"');
                buf.push(b':');
                encode_json_ordered(value, buf);
            }
            buf.push(b'}');
        }
    }
}

/// Encode a key-value pair into an ordered binary key.
///
/// Uses proper JSON string escaping to ensure injective encoding —
/// different (key, value) pairs always produce different byte sequences.
pub fn encode_kv_ordered(key: &Value, value: &Value) -> TypedBinaryKey {
    let mut buf = Vec::with_capacity(64);
    encode_json_ordered(key, &mut buf);
    encode_json_ordered(value, &mut buf);
    TypedBinaryKey(buf)
}

/// Encode only the key part for pagination ordering.
///
/// Pagination uses key-only ordering so page tokens (which are
/// canonical JSON strings of keys only) align with the binary ordering.
pub fn encode_key_ordered(key: &Value) -> TypedBinaryKey {
    let mut buf = Vec::with_capacity(64);
    encode_json_ordered(key, &mut buf);
    TypedBinaryKey(buf)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaKey(Value);

impl DeltaKey {
    pub fn from_json(value: Value) -> Self {
        Self(value)
    }

    pub fn as_json(&self) -> &Value {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaValue(Value);

impl DeltaValue {
    pub fn from_json(value: Value) -> Self {
        Self(value)
    }

    pub fn as_json(&self) -> &Value {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaRecord {
    pub key: DeltaKey,
    pub value: DeltaValue,
    pub weight: DeltaWeight,
}

impl DeltaRecord {
    pub fn new(key: DeltaKey, value: DeltaValue, weight: DeltaWeight) -> Self {
        Self { key, value, weight }
    }

    pub fn inverse(&self) -> Result<Self, DeltaError> {
        Ok(Self {
            key: self.key.clone(),
            value: self.value.clone(),
            weight: self
                .weight
                .checked_neg()
                .ok_or(DeltaError::WeightOverflow)?,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
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

    pub fn inverse(&self) -> Result<Self, DeltaError> {
        let records = self
            .records
            .iter()
            .map(DeltaRecord::inverse)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { records })
    }

    pub fn diff(&self, other: &Self) -> Result<Self, DeltaError> {
        let mut records = Vec::with_capacity(self.records.len() + other.records.len());
        for record in &self.records {
            records.push(record.inverse()?);
        }
        records.extend(other.records.iter().cloned());
        Ok(Self { records })
    }

    pub fn net_rows(&self) -> Result<Vec<DeltaRecord>, DeltaError> {
        let mut net: BTreeMap<TypedBinaryKey, (DeltaKey, DeltaValue, i128)> = BTreeMap::new();

        for record in &self.records {
            // Consolidate by (key, value) pair. Records with the same key
            // but different values are separate entries.
            let binary_key = encode_kv_ordered(&record.key.0, &record.value.0);
            let entry = net
                .entry(binary_key)
                .or_insert_with(|| (record.key.clone(), record.value.clone(), 0));

            entry.2 = entry
                .2
                .checked_add(i128::from(record.weight))
                .ok_or(DeltaError::WeightOverflow)?;
        }

        net.into_values()
            .filter_map(|(key, value, weight)| {
                if weight == 0 {
                    None
                } else {
                    Some(
                        weight
                            .try_into()
                            .map(|weight| DeltaRecord::new(key, value, weight))
                            .map_err(|_| DeltaError::WeightOverflow),
                    )
                }
            })
            .collect()
    }
}

/// Encodes JSON values in a deterministic semantic order for net row keys.
///
/// Object fields are sorted here instead of relying on serde_json map iteration
/// order, which keeps checkpoint-facing net output stable across input order.
#[allow(dead_code)]
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
            batch.net_rows().unwrap(),
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

        assert!(batch.net_rows().unwrap().is_empty());
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

        assert_eq!(
            combined.net_rows().unwrap(),
            vec![DeltaRecord::new(key, value, 1)]
        );
    }

    #[test]
    fn delta_record_inverse_rejects_minimum_weight() {
        let record = DeltaRecord::new(
            DeltaKey::from_json(json!("account:1")),
            DeltaValue::from_json(json!({ "balance": 100 })),
            i64::MIN,
        );

        assert_eq!(record.inverse(), Err(DeltaError::WeightOverflow));
    }

    #[test]
    fn delta_batch_net_rows_rejects_weight_overflow() {
        let key = DeltaKey::from_json(json!("account:1"));
        let value = DeltaValue::from_json(json!({ "balance": 100 }));
        let batch = DeltaBatch::from_records([
            DeltaRecord::new(key.clone(), value.clone(), i64::MAX),
            DeltaRecord::new(key, value, 1),
        ]);

        assert_eq!(batch.net_rows(), Err(DeltaError::WeightOverflow));
    }

    #[test]
    fn delta_batch_net_rows_uses_canonical_ordering() {
        let first = DeltaRecord::new(
            DeltaKey::from_json(json!("account:1")),
            DeltaValue::from_json(json!({ "currency": "USD", "balance": 100 })),
            1,
        );
        let second = DeltaRecord::new(
            DeltaKey::from_json(json!("account:2")),
            DeltaValue::from_json(json!({ "currency": "USD", "balance": 50 })),
            1,
        );
        let forward = DeltaBatch::from_records([first.clone(), second.clone()]);
        let reverse = DeltaBatch::from_records([second, first]);

        assert_eq!(forward.net_rows().unwrap(), reverse.net_rows().unwrap());
    }

    #[test]
    fn delta_types_round_trip_through_json() {
        let batch = DeltaBatch::from_records([
            DeltaRecord::new(
                DeltaKey::from_json(json!("account:1")),
                DeltaValue::from_json(json!({ "currency": "USD", "balance": 100 })),
                2,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("account:2")),
                DeltaValue::from_json(json!(["open", "vip"])),
                -1,
            ),
        ]);

        let encoded = serde_json::to_string(&batch).unwrap();
        let decoded: DeltaBatch = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, batch);
    }

    #[test]
    fn delta_key_codec_fixture_round_trips_composite_null_key() {
        let batch = DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!([null, "tenant-a", 42])),
            DeltaValue::from_json(json!({"value": 7})),
            1,
        )]);
        let encoded = serde_json::to_string(&batch).unwrap();
        assert_eq!(
            encoded,
            r#"{"records":[{"key":[null,"tenant-a",42],"value":{"value":7},"weight":1}]}"#
        );
        assert_eq!(serde_json::from_str::<DeltaBatch>(&encoded).unwrap(), batch);
    }

    #[test]
    fn delta_batch_consolidates_composite_null_key_duplicates() {
        let key = DeltaKey::from_json(json!([null, "tenant-a"]));
        let value = DeltaValue::from_json(json!({"value": 7}));
        let batch = DeltaBatch::from_records([
            DeltaRecord::new(key.clone(), value.clone(), 1),
            DeltaRecord::new(key.clone(), value.clone(), 1),
            DeltaRecord::new(key.clone(), value.clone(), -1),
        ]);

        assert_eq!(
            batch.net_rows().unwrap(),
            vec![DeltaRecord::new(key, value, 1)]
        );
    }

    #[test]
    fn delta_batch_key_change_retracts_old_key_and_inserts_new_key() {
        let old = DeltaRecord::new(
            DeltaKey::from_json(json!(["tenant-a", "old"])),
            DeltaValue::from_json(json!({"value": 7})),
            -1,
        );
        let new = DeltaRecord::new(
            DeltaKey::from_json(json!(["tenant-a", "new"])),
            DeltaValue::from_json(json!({"value": 7})),
            1,
        );
        let net = DeltaBatch::from_records([new.clone(), old.clone()])
            .net_rows()
            .unwrap();

        assert_eq!(net, vec![new, old]);
    }

    #[test]
    fn delta_batch_final_duplicate_deletion_removes_the_row() {
        let key = DeltaKey::from_json(json!(["tenant-a", "row-1"]));
        let value = DeltaValue::from_json(json!({"value": 7}));
        let duplicates = DeltaBatch::from_records([
            DeltaRecord::new(key.clone(), value.clone(), 1),
            DeltaRecord::new(key.clone(), value.clone(), 1),
        ]);
        let one_deleted = duplicates.combine(&DeltaBatch::from_records([DeltaRecord::new(
            key.clone(),
            value.clone(),
            -1,
        )]));
        assert_eq!(
            one_deleted.net_rows().unwrap(),
            vec![DeltaRecord::new(key.clone(), value.clone(), 1)]
        );
        assert!(one_deleted
            .combine(&DeltaBatch::from_records([DeltaRecord::new(
                key, value, -1,
            )]))
            .net_rows()
            .unwrap()
            .is_empty());
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

            prop_assert_eq!(lhs.net_rows().unwrap(), rhs.net_rows().unwrap());
        }

        #[test]
        fn delta_batch_combined_with_inverse_has_empty_net_result(batch in batch_strategy()) {
            let inverse = batch.inverse().unwrap();

            prop_assert!(batch.combine(&inverse).net_rows().unwrap().is_empty());
        }

        #[test]
        fn delta_key_json_codec_round_trip_is_lossless(key in key_strategy()) {
            let encoded = serde_json::to_string(&key).unwrap();
            let decoded: DeltaKey = serde_json::from_str(&encoded).unwrap();

            prop_assert_eq!(decoded, key);
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

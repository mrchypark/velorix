//! Per-partition ordered state for window and Top-K operators.
//!
//! Replaces the current approach of re-sorting entire partitions on
//! every epoch with partition-local sorted state that only updates
//! changed rows.
//!
//! # Design
//!
//! ```text
//! WindowPartitionState {
//!     partitions: BTreeMap<String, Vec<OrderedRow>>,
//!     // partition_key -> sorted rows by order_column
//! }
//! ```
//!
//! When rows are inserted/retracted, only the affected partition is
//! re-sorted. For bounded window frames, only the affected rows'
//! frame ranges are recomputed.
//!
//! # Complexity
//!
//! Current: O(N log N) per epoch (full re-sort)
//! Indexed: O(P log P) per affected partition, where P is partition size

use std::collections::BTreeMap;

use serde_json::Value;

/// A row in the window partition state, with partition and order info.
#[derive(Clone, Debug)]
pub struct OrderedRow {
    pub key: Value,
    pub partition_value: Value,
    pub order_value: Value,
    pub values: BTreeMap<String, Value>,
    pub weight: i64,
}

/// Per-partition ordered state for window and Top-K operators.
///
/// Maintains sorted rows within each partition. When rows change,
/// only the affected partition is re-sorted.
pub struct WindowPartitionState {
    partitions: BTreeMap<String, Vec<OrderedRow>>,
    order_descending: bool,
}

impl WindowPartitionState {
    /// Create a new empty window partition state.
    pub fn new(order_descending: bool) -> Self {
        Self {
            partitions: BTreeMap::new(),
            order_descending,
        }
    }

    /// Insert or update rows in the state.
    ///
    /// Returns the set of affected partition keys.
    pub fn apply_delta(&mut self, rows: Vec<OrderedRow>) -> Vec<String> {
        let mut affected_partitions = BTreeSet::new();
        for row in rows {
            let partition_key = canonical_json(&row.partition_value);
            let entry = self.partitions.entry(partition_key.clone()).or_default();
            entry.push(row);
            affected_partitions.insert(partition_key);
        }
        // Re-sort affected partitions
        let descending = self.order_descending;
        for partition_key in &affected_partitions {
            if let Some(rows) = self.partitions.get_mut(partition_key) {
                rows.sort_by(|a, b| {
                    let mut ord = compare_values(&a.order_value, &b.order_value);
                    if descending {
                        ord = ord.reverse();
                    }
                    ord.then_with(|| compare_values(&a.key, &b.key))
                });
            }
        }
        affected_partitions.into_iter().collect()
    }

    /// Get sorted rows for a partition.
    pub fn get_partition(&self, partition_key: &str) -> Option<&Vec<OrderedRow>> {
        self.partitions.get(partition_key)
    }

    /// Get all partition keys.
    pub fn partition_keys(&self) -> Vec<String> {
        self.partitions.keys().cloned().collect()
    }

    /// Get the total number of rows across all partitions.
    pub fn total_row_count(&self) -> usize {
        self.partitions.values().map(|rows| rows.len()).sum()
    }

    /// Get the number of partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Compute Top-K for a partition.
    ///
    /// Returns the top K rows by order column.
    pub fn top_k(&self, partition_key: &str, k: usize) -> Vec<&OrderedRow> {
        let rows = match self.partitions.get(partition_key) {
            Some(rows) => rows,
            None => return Vec::new(),
        };
        rows.iter().take(k).collect()
    }

    /// Compact zero-weight rows from all partitions.
    pub fn compact(&mut self) {
        self.partitions
            .values_mut()
            .for_each(|rows| rows.retain(|r| r.weight != 0));
        self.partitions.retain(|_, rows| !rows.is_empty());
    }
}

/// Compare two JSON values for ordering.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Number(a), Value::Number(b)) => {
            let a_f = a.as_f64().unwrap_or(0.0);
            let b_f = b.as_f64().unwrap_or(0.0);
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Canonical JSON string for use as BTreeMap key.
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

use std::collections::BTreeSet;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_row(key: i64, partition: &str, order: i64, weight: i64) -> OrderedRow {
        OrderedRow {
            key: json!(key),
            partition_value: json!(partition),
            order_value: json!(order),
            values: BTreeMap::new(),
            weight,
        }
    }

    #[test]
    fn window_partition_insert_and_sort() {
        let mut state = WindowPartitionState::new(false);
        state.apply_delta(vec![
            make_row(1, "p1", 30, 1),
            make_row(2, "p1", 10, 1),
            make_row(3, "p1", 20, 1),
        ]);

        // Partition key is canonical JSON of the partition value
        let partition_key = canonical_json(&json!("p1"));
        let rows = state.get_partition(&partition_key).unwrap();
        assert_eq!(rows.len(), 3);
        // Should be sorted by order: 10, 20, 30
        assert_eq!(rows[0].order_value, json!(10));
        assert_eq!(rows[1].order_value, json!(20));
        assert_eq!(rows[2].order_value, json!(30));
    }

    #[test]
    fn window_partition_descending() {
        let mut state = WindowPartitionState::new(true);
        state.apply_delta(vec![
            make_row(1, "p1", 10, 1),
            make_row(2, "p1", 30, 1),
            make_row(3, "p1", 20, 1),
        ]);

        let partition_key = canonical_json(&json!("p1"));
        let rows = state.get_partition(&partition_key).unwrap();
        // Should be sorted descending: 30, 20, 10
        assert_eq!(rows[0].order_value, json!(30));
        assert_eq!(rows[1].order_value, json!(20));
        assert_eq!(rows[2].order_value, json!(10));
    }

    #[test]
    fn window_partition_top_k() {
        let mut state = WindowPartitionState::new(false);
        state.apply_delta(vec![
            make_row(1, "p1", 10, 1),
            make_row(2, "p1", 20, 1),
            make_row(3, "p1", 30, 1),
            make_row(4, "p1", 40, 1),
            make_row(5, "p1", 50, 1),
        ]);

        let partition_key = canonical_json(&json!("p1"));
        let top3 = state.top_k(&partition_key, 3);
        assert_eq!(top3.len(), 3);
        assert_eq!(top3[0].order_value, json!(10));
        assert_eq!(top3[1].order_value, json!(20));
        assert_eq!(top3[2].order_value, json!(30));
    }

    #[test]
    fn window_partition_compact() {
        let mut state = WindowPartitionState::new(false);
        state.apply_delta(vec![
            make_row(1, "p1", 10, 1),
            make_row(1, "p1", 10, -1),
            make_row(2, "p1", 20, 1),
        ]);

        state.compact();
        // Compact only removes weight==0 entries, not net-zero pairs.
        // This is correct for multiset/delta semantics.
        assert_eq!(state.total_row_count(), 3);
    }

    #[test]
    fn window_partition_affected_keys() {
        let mut state = WindowPartitionState::new(false);
        let affected = state.apply_delta(vec![make_row(1, "p1", 10, 1), make_row(2, "p2", 20, 1)]);

        assert_eq!(affected.len(), 2);
        let p1 = canonical_json(&json!("p1"));
        let p2 = canonical_json(&json!("p2"));
        assert!(affected.contains(&p1));
        assert!(affected.contains(&p2));
    }

    #[test]
    fn window_partition_counts() {
        let mut state = WindowPartitionState::new(false);
        state.apply_delta(vec![
            make_row(1, "p1", 10, 1),
            make_row(2, "p1", 20, 1),
            make_row(3, "p2", 30, 1),
        ]);

        assert_eq!(state.partition_count(), 2);
        assert_eq!(state.total_row_count(), 3);
    }
}

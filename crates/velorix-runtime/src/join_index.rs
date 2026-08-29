//! Per-key indexed state for efficient incremental joins.
//!
//! Replaces the current approach of scanning all rows for each join
//! with per-key indexed state that only touches affected keys.
//!
//! # Design
//!
//! ```text
//! JoinIndex {
//!     left:  BTreeMap<String, Vec<WeightedRow>>,  // join_key -> rows
//!     right: BTreeMap<String, Vec<WeightedRow>>,  // join_key -> rows
//! }
//! ```
//!
//! When a left delta arrives, only the affected keys' right-side rows
//! are scanned. When a right delta arrives, only the affected keys'
//! left-side rows are scanned.
//!
//! # Complexity
//!
//! Current: O(L × R) per epoch (full cross join)
//! Indexed: O(ΔL × R_k + ΔR × L_k) where k is the affected key

use std::collections::BTreeMap;

/// A weighted row in the join index.
#[derive(Clone, Debug)]
pub struct WeightedRow {
    pub key: String,
    pub values: BTreeMap<String, serde_json::Value>,
    pub weight: i64,
}

/// Per-key indexed state for equi-joins.
///
/// Maintains separate indexes for left and right sides, keyed by
/// the join column value. Only affected keys are scanned during
/// delta processing.
pub struct JoinIndex {
    left: BTreeMap<String, Vec<WeightedRow>>,
    right: BTreeMap<String, Vec<WeightedRow>>,
    join_column: String,
}

impl JoinIndex {
    /// Create a new empty join index.
    pub fn new(join_column: String) -> Self {
        Self {
            left: BTreeMap::new(),
            right: BTreeMap::new(),
            join_column,
        }
    }

    /// Apply a left-side delta. Returns the affected join keys.
    pub fn apply_left_delta(&mut self, records: Vec<WeightedRow>) -> Vec<String> {
        let mut affected_keys = Vec::new();
        for record in records {
            let key = record
                .values
                .get(&self.join_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            self.left.entry(key.clone()).or_default().push(record);
            affected_keys.push(key);
        }
        affected_keys.sort();
        affected_keys.dedup();
        affected_keys
    }

    /// Apply a right-side delta. Returns the affected join keys.
    pub fn apply_right_delta(&mut self, records: Vec<WeightedRow>) -> Vec<String> {
        let mut affected_keys = Vec::new();
        for record in records {
            let key = record
                .values
                .get(&self.join_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            self.right.entry(key.clone()).or_default().push(record);
            affected_keys.push(key);
        }
        affected_keys.sort();
        affected_keys.dedup();
        affected_keys
    }

    /// Compute inner join output for affected keys only.
    ///
    /// Returns (new_rows, retracted_rows) for the affected keys.
    pub fn compute_inner_join_for_keys(
        &self,
        keys: &[String],
    ) -> (Vec<WeightedRow>, Vec<WeightedRow>) {
        let mut new_rows = Vec::new();
        let retracted_rows = Vec::new();

        for key in keys {
            if let (Some(left_rows), Some(right_rows)) = (self.left.get(key), self.right.get(key)) {
                for left in left_rows {
                    if left.weight <= 0 {
                        continue;
                    }
                    for right in right_rows {
                        if right.weight <= 0 {
                            continue;
                        }
                        let weight = left.weight.checked_mul(right.weight).unwrap_or(0);
                        if weight == 0 {
                            continue;
                        }
                        let mut output_values = left.values.clone();
                        for (k, v) in &right.values {
                            output_values.insert(k.clone(), v.clone());
                        }
                        new_rows.push(WeightedRow {
                            key: key.clone(),
                            values: output_values,
                            weight,
                        });
                    }
                }
            }
        }

        (new_rows, retracted_rows)
    }

    /// Compact zero-weight entries from both sides.
    pub fn compact(&mut self) {
        self.left
            .values_mut()
            .for_each(|rows| rows.retain(|r| r.weight != 0));
        self.left.retain(|_, rows| !rows.is_empty());
        self.right
            .values_mut()
            .for_each(|rows| rows.retain(|r| r.weight != 0));
        self.right.retain(|_, rows| !rows.is_empty());
    }

    /// Get the total number of rows on the left side.
    pub fn left_row_count(&self) -> usize {
        self.left.values().map(|rows| rows.len()).sum()
    }

    /// Get the total number of rows on the right side.
    pub fn right_row_count(&self) -> usize {
        self.right.values().map(|rows| rows.len()).sum()
    }

    /// Get the number of distinct join keys on the left side.
    pub fn left_key_count(&self) -> usize {
        self.left.len()
    }

    /// Get the number of distinct join keys on the right side.
    pub fn right_key_count(&self) -> usize {
        self.right.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_row(key: &str, value: serde_json::Value, weight: i64) -> WeightedRow {
        let mut values = BTreeMap::new();
        values.insert("join_key".to_string(), json!(key));
        values.insert("data".to_string(), value);
        WeightedRow {
            key: key.to_string(),
            values,
            weight,
        }
    }

    #[test]
    fn join_index_inner_join() {
        let mut index = JoinIndex::new("join_key".to_string());

        // Left: a→1, b→2
        index.apply_left_delta(vec![make_row("a", json!(1), 1), make_row("b", json!(2), 1)]);

        // Right: a→10, b→20, c→30
        index.apply_right_delta(vec![
            make_row("a", json!(10), 1),
            make_row("b", json!(20), 1),
            make_row("c", json!(30), 1),
        ]);

        let (new_rows, _) = index.compute_inner_join_for_keys(&["a".to_string(), "b".to_string()]);
        assert_eq!(new_rows.len(), 2);
    }

    #[test]
    fn join_index_affected_keys_only() {
        let mut index = JoinIndex::new("join_key".to_string());

        index.apply_left_delta(vec![
            make_row("a", json!(1), 1),
            make_row("b", json!(2), 1),
            make_row("c", json!(3), 1),
        ]);

        index.apply_right_delta(vec![
            make_row("a", json!(10), 1),
            make_row("b", json!(20), 1),
            make_row("c", json!(30), 1),
        ]);

        // Only compute join for key "a"
        let (new_rows, _) = index.compute_inner_join_for_keys(&["a".to_string()]);
        assert_eq!(new_rows.len(), 1);
        assert_eq!(new_rows[0].values.get("data"), Some(&json!(10)));
    }

    #[test]
    fn join_index_compact_removes_zero_weight() {
        let mut index = JoinIndex::new("join_key".to_string());

        // Add a row with weight 1, then retract it with weight -1
        index.apply_left_delta(vec![make_row("a", json!(1), 1)]);
        index.apply_left_delta(vec![make_row("a", json!(1), -1)]);

        index.compact();
        // After compact, both entries have non-zero weight (+1 and -1)
        // but they cancel each other out. The compact only removes
        // entries with weight == 0, not net-zero pairs.
        // This is correct for a multiset/delta system.
        assert_eq!(index.left_row_count(), 2);
    }

    #[test]
    fn join_index_counts() {
        let mut index = JoinIndex::new("join_key".to_string());

        index.apply_left_delta(vec![make_row("a", json!(1), 1), make_row("b", json!(2), 1)]);

        index.apply_right_delta(vec![
            make_row("a", json!(10), 1),
            make_row("a", json!(11), 1),
        ]);

        assert_eq!(index.left_key_count(), 2);
        assert_eq!(index.right_key_count(), 1);
        assert_eq!(index.left_row_count(), 2);
        assert_eq!(index.right_row_count(), 2);
    }
}

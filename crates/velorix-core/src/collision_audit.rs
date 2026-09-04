//! Raw log collision audit for v1 encoding.
//!
//! Detects when the legacy v1 binary encoding produces the same byte
//! sequence for different (key, value) pairs. This is the root cause
//! of the P0-1 data corruption issue.
//!
//! # Usage
//!
//! ```text
//! let audit = CollisionAudit::new();
//! audit.check_pair(json!("a\""), json!("b"), json!("a"), json!("\"b"));
//! // audit.has_collisions() == true if collision detected
//! ```
//!
//! # Migration guidance
//!
//! After detecting collisions, affected views should:
//! 1. Stop writes to affected relations
//! 2. Replay from raw ingest log with v2 codec
//! 3. Verify all stateful views match replayed output
//! 4. Switch to v2 codec for new writes

use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Maximum distinct logical pairs retained for one encoded fingerprint.
///
/// Once a fingerprint reaches this limit, the audit stops reporting new
/// comparisons for that fingerprint and records the truncation instead.
pub const MAX_LOGICAL_PAIRS_PER_FINGERPRINT: usize = 64;
pub const MAX_FINGERPRINT_CLASSES: usize = 1_024;
pub const MAX_TOTAL_LOGICAL_PAIRS: usize = 4_096;
pub const MAX_TRACKED_COMPARISONS: usize = 16_384;
pub const MAX_COLLISIONS: usize = 8_192;
pub const MAX_ENCODED_FINGERPRINT_BYTES: usize = 64 * 1024;
pub const MAX_CANONICAL_JSON_BYTES: usize = 64 * 1024;
pub const MAX_LOGICAL_PAIR_BYTES: usize = 64 * 1024;
pub const MAX_RETAINED_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

/// A collision detected between two different (key, value) pairs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Collision {
    pub key_a: Value,
    pub value_a: Value,
    pub key_b: Value,
    pub value_b: Value,
    pub encoded_bytes: Vec<u8>,
}

/// Audit result for collision detection.
#[derive(Clone, Debug, Default)]
pub struct CollisionAudit {
    collisions: Vec<Collision>,
    checked_pairs: usize,
    seen: HashMap<Vec<u8>, FingerprintClass>,
    compared_pairs: HashSet<ComparisonKey>,
    retained_pairs: usize,
    retained_payload_bytes: usize,
    next_pair_id: u64,
    incomplete: bool,
    truncated_fingerprints: usize,
    truncated_records: usize,
    truncated_comparisons: usize,
    truncated_collisions: usize,
    truncated_oversized_records: usize,
}

#[derive(Clone, Debug, Default)]
struct FingerprintClass {
    logical_pairs: Vec<LogicalPair>,
}

#[derive(Clone, Debug)]
struct LogicalPair {
    key: Value,
    value: Value,
    id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ComparisonKey {
    first: u64,
    second: u64,
}

impl ComparisonKey {
    fn new(mut first: u64, mut second: u64) -> Self {
        if second < first {
            std::mem::swap(&mut first, &mut second);
        }
        Self { first, second }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComparisonOutcome {
    New,
    Seen,
    Truncated,
}

impl ComparisonOutcome {
    fn is_new(self) -> bool {
        matches!(self, Self::New)
    }
}

/// Snapshot of collision results and their completeness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollisionAuditStatus {
    pub complete: bool,
    pub checked_count: usize,
    pub collision_count: usize,
    pub truncated_fingerprint_count: usize,
    pub truncated_record_count: usize,
    pub truncated_comparison_count: usize,
    pub truncated_collision_count: usize,
    pub truncated_oversized_record_count: usize,
}

impl CollisionAudit {
    /// Create a new empty audit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if two (key, value) pairs produce the same v1 encoding.
    ///
    /// Uses the legacy `encode_kv_ordered` function to detect collisions.
    pub fn check_pair(&mut self, key_a: Value, value_a: Value, key_b: Value, value_b: Value) {
        let enc_a = encode_kv_ordered(&key_a, &value_a);
        let enc_b = encode_kv_ordered(&key_b, &value_b);
        let pair_a = self.observe_record(key_a, value_a);
        let pair_b = self.observe_record(key_b, value_b);

        if enc_a != enc_b {
            if let (Some(pair_a), Some(pair_b)) = (pair_a, pair_b) {
                self.record_comparison(pair_a, pair_b);
            }
        }
    }

    /// Check a batch of records for collisions.
    pub fn check_batch(
        &mut self,
        records: &[(Value, Value)], // (key, value) pairs
    ) {
        for (key, value) in records {
            self.observe_record(key.clone(), value.clone());
        }
    }

    fn observe_record(&mut self, key: Value, value: Value) -> Option<u64> {
        let key_bytes = canonical_json_bytes(&key);
        let value_bytes = canonical_json_bytes(&value);
        let pair_bytes = key_bytes.len().saturating_add(value_bytes.len());
        if key_bytes.len() > MAX_CANONICAL_JSON_BYTES
            || value_bytes.len() > MAX_CANONICAL_JSON_BYTES
            || pair_bytes > MAX_LOGICAL_PAIR_BYTES
        {
            self.mark_incomplete();
            increment_counter(&mut self.truncated_records);
            increment_counter(&mut self.truncated_oversized_records);
            return None;
        }

        // Rebuild from bounded serialized bytes so caller-owned String/Vec
        // capacities are never retained by the audit.
        let mut key: Value = serde_json::from_slice(&key_bytes).expect("canonical JSON is valid");
        let mut value: Value =
            serde_json::from_slice(&value_bytes).expect("canonical JSON is valid");
        compact_json_value(&mut key);
        compact_json_value(&mut value);
        let enc = encode_kv_ordered(&key, &value);
        if enc.len() > MAX_ENCODED_FINGERPRINT_BYTES {
            self.mark_incomplete();
            increment_counter(&mut self.truncated_records);
            increment_counter(&mut self.truncated_oversized_records);
            return None;
        }
        let existing = self.seen.get(&enc);
        if let Some(pair) = existing.and_then(|class| {
            class
                .logical_pairs
                .iter()
                .find(|pair| pair.key == key && pair.value == value)
        }) {
            return Some(pair.id);
        }

        let new_class = existing.is_none();
        let previous_pairs = existing
            .map(|class| class.logical_pairs.clone())
            .unwrap_or_default();
        let class_full = existing
            .is_some_and(|class| class.logical_pairs.len() == MAX_LOGICAL_PAIRS_PER_FINGERPRINT);
        let admission_bytes = pair_bytes.saturating_add(if new_class { enc.len() } else { 0 });
        if (new_class && self.seen.len() == MAX_FINGERPRINT_CLASSES)
            || class_full
            || self.retained_pairs == MAX_TOTAL_LOGICAL_PAIRS
            || self.retained_payload_bytes.saturating_add(admission_bytes)
                > MAX_RETAINED_PAYLOAD_BYTES
        {
            self.mark_incomplete();
            increment_counter(&mut self.truncated_records);
            if new_class || class_full {
                increment_counter(&mut self.truncated_fingerprints);
            }
            return None;
        }

        let id = self.next_pair_id;
        self.next_pair_id += 1;

        for pair in previous_pairs {
            if self.record_comparison(pair.id, id).is_new() {
                self.record_collision(Collision {
                    key_a: pair.key,
                    value_a: pair.value,
                    key_b: key.clone(),
                    value_b: value.clone(),
                    encoded_bytes: enc.clone(),
                });
            }
        }
        if let Some(class) = self.seen.get_mut(&enc) {
            class.logical_pairs.push(LogicalPair { key, value, id });
        } else {
            self.seen.insert(
                enc,
                FingerprintClass {
                    logical_pairs: vec![LogicalPair { key, value, id }],
                },
            );
        }
        self.retained_pairs += 1;
        self.retained_payload_bytes = self.retained_payload_bytes.saturating_add(admission_bytes);
        Some(id)
    }

    fn record_comparison(&mut self, first: u64, second: u64) -> ComparisonOutcome {
        let key = ComparisonKey::new(first, second);
        if self.compared_pairs.contains(&key) {
            ComparisonOutcome::Seen
        } else if self.compared_pairs.len() == MAX_TRACKED_COMPARISONS {
            self.mark_incomplete();
            increment_counter(&mut self.truncated_comparisons);
            ComparisonOutcome::Truncated
        } else {
            self.compared_pairs.insert(key);
            self.checked_pairs += 1;
            ComparisonOutcome::New
        }
    }

    fn record_collision(&mut self, collision: Collision) {
        let collision_bytes = canonical_json_bytes(&collision.key_a)
            .len()
            .saturating_add(canonical_json_bytes(&collision.value_a).len())
            .saturating_add(canonical_json_bytes(&collision.key_b).len())
            .saturating_add(canonical_json_bytes(&collision.value_b).len())
            .saturating_add(collision.encoded_bytes.len());
        if self.collisions.len() == MAX_COLLISIONS
            || self.retained_payload_bytes.saturating_add(collision_bytes)
                > MAX_RETAINED_PAYLOAD_BYTES
        {
            self.mark_incomplete();
            increment_counter(&mut self.truncated_collisions);
        } else {
            self.retained_payload_bytes =
                self.retained_payload_bytes.saturating_add(collision_bytes);
            self.collisions.push(collision);
        }
    }

    fn mark_incomplete(&mut self) {
        self.incomplete = true;
    }

    /// Check whether the audit found collisions or became incomplete.
    ///
    /// An incomplete audit is unsafe to treat as collision-free, so this
    /// returns `true` after any retention or reporting limit is reached.
    pub fn has_collisions(&self) -> bool {
        !self.collisions.is_empty() || !self.is_complete()
    }

    /// Get the number of retained collision reports.
    ///
    /// This is exact only when [`Self::is_complete`] is `true`; otherwise it
    /// is a lower bound. Use [`Self::status`] to obtain the count together
    /// with its completeness.
    pub fn collision_count(&self) -> usize {
        self.collisions.len()
    }

    /// Get the number of unique retained logical unordered comparisons completed.
    ///
    /// `check_batch` compares only records sharing an encoded fingerprint, and
    /// `check_pair` contributes a comparison only for distinct retained pairs.
    /// Identical logical pairs contribute zero. Repeated comparisons are
    /// counted once. When [`Self::is_complete`] is false this count is a lower
    /// bound because later comparisons were not retained.
    pub fn checked_count(&self) -> usize {
        self.checked_pairs
    }

    /// Get retained collision reports.
    ///
    /// The returned slice is complete only when [`Self::is_complete`] is true.
    pub fn collisions(&self) -> &[Collision] {
        &self.collisions
    }

    /// Whether every retained-state and reporting bound was respected.
    pub fn is_complete(&self) -> bool {
        !self.incomplete
    }

    /// Number of fingerprint classes for which collision reporting was truncated.
    pub fn truncated_fingerprint_count(&self) -> usize {
        self.truncated_fingerprints
    }

    /// Number of records skipped after a fingerprint class reached a bound.
    pub fn truncated_record_count(&self) -> usize {
        self.truncated_records
    }

    /// Number of unique comparisons omitted after the comparison cap was reached.
    pub fn truncated_comparison_count(&self) -> usize {
        self.truncated_comparisons
    }

    /// Number of collision reports omitted after the report cap was reached.
    pub fn truncated_collision_count(&self) -> usize {
        self.truncated_collisions
    }

    /// Number of records skipped because their retained fingerprint was too large.
    pub fn truncated_oversized_record_count(&self) -> usize {
        self.truncated_oversized_records
    }

    /// Return collision counts and completeness in one explicit snapshot.
    pub fn status(&self) -> CollisionAuditStatus {
        CollisionAuditStatus {
            complete: self.is_complete(),
            checked_count: self.checked_count(),
            collision_count: self.collision_count(),
            truncated_fingerprint_count: self.truncated_fingerprint_count(),
            truncated_record_count: self.truncated_record_count(),
            truncated_comparison_count: self.truncated_comparison_count(),
            truncated_collision_count: self.truncated_collision_count(),
            truncated_oversized_record_count: self.truncated_oversized_record_count(),
        }
    }

    #[cfg(test)]
    fn accounted_payload_bytes(&self) -> usize {
        let pair_bytes = self
            .seen
            .values()
            .flat_map(|class| &class.logical_pairs)
            .map(|pair| {
                canonical_json_bytes(&pair.key)
                    .len()
                    .saturating_add(canonical_json_bytes(&pair.value).len())
            })
            .sum::<usize>();
        let fingerprint_bytes = self.seen.keys().map(Vec::len).sum::<usize>();
        let collision_bytes = self
            .collisions
            .iter()
            .map(|collision| {
                canonical_json_bytes(&collision.key_a)
                    .len()
                    .saturating_add(canonical_json_bytes(&collision.value_a).len())
                    .saturating_add(canonical_json_bytes(&collision.key_b).len())
                    .saturating_add(canonical_json_bytes(&collision.value_b).len())
                    .saturating_add(collision.encoded_bytes.len())
            })
            .sum::<usize>();
        pair_bytes
            .saturating_add(fingerprint_bytes)
            .saturating_add(collision_bytes)
    }
}

/// Legacy v1 encoding function (for audit purposes only).
///
/// This is the function that produces collisions. It should NOT be
/// used for new code - use `encode_kv_v2` instead.
fn encode_kv_ordered(key: &Value, value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    encode_json_ordered(key, &mut buf);
    encode_json_ordered(value, &mut buf);
    buf.shrink_to_fit();
    buf
}

fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).expect("JSON values are serializable");
    bytes.shrink_to_fit();
    bytes
}

fn increment_counter(counter: &mut usize) {
    *counter = counter.saturating_add(1);
}

fn compact_json_value(value: &mut Value) {
    match value {
        Value::String(string) => string.shrink_to_fit(),
        Value::Array(values) => {
            for value in values.iter_mut() {
                compact_json_value(value);
            }
            values.shrink_to_fit();
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                compact_json_value(value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Legacy v1 JSON encoding (for audit purposes only).
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
            buf.extend_from_slice(s.as_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn collision_audit_no_collision() {
        let mut audit = CollisionAudit::new();
        audit.check_pair(json!("a"), json!(1), json!("b"), json!(2));
        assert!(!audit.has_collisions());
        assert_eq!(audit.collision_count(), 0);
    }

    #[test]
    fn collision_audit_detects_collision() {
        let mut audit = CollisionAudit::new();
        // The classic collision: key="a\"", value="b" vs key="a", value="\"b"
        audit.check_pair(json!("a\""), json!("b"), json!("a"), json!("\"b"));
        assert!(audit.has_collisions());
        assert_eq!(audit.collision_count(), 1);
    }

    #[test]
    fn collision_audit_same_pair_no_collision() {
        let mut audit = CollisionAudit::new();
        audit.check_pair(json!("a"), json!(1), json!("a"), json!(1));
        // Same pair should not be a collision
        assert!(!audit.has_collisions());
        assert_eq!(audit.checked_count(), 0);
    }

    #[test]
    fn collision_audit_multiple_pairs() {
        let mut audit = CollisionAudit::new();
        audit.check_pair(json!("a"), json!(1), json!("b"), json!(2));
        audit.check_pair(json!("c"), json!(3), json!("d"), json!(4));
        assert_eq!(audit.checked_count(), 2);
        assert!(!audit.has_collisions());
    }

    #[test]
    fn collision_audit_batch_records_distinct_colliding_pairs() {
        let mut audit = CollisionAudit::new();
        let records = [
            (json!("a\""), json!("b")),
            (json!("a"), json!("\"b")),
            (json!("a"), json!("\"b")),
        ];

        audit.check_batch(&records);

        assert_eq!(audit.checked_count(), 1);
        assert_eq!(audit.collision_count(), 1);
        assert_eq!(
            audit.collisions(),
            &[Collision {
                key_a: json!("a\""),
                value_a: json!("b"),
                key_b: json!("a"),
                value_b: json!("\"b"),
                encoded_bytes: encode_kv_ordered(&json!("a\""), &json!("b")),
            }]
        );
    }

    #[test]
    fn collision_audit_batch_reports_each_unique_pair_for_shared_encoding() {
        let mut audit = CollisionAudit::new();
        let records = [
            (json!("a\"\""), json!("b")),
            (json!("a\""), json!("\"b")),
            (json!("a"), json!("\"\"b")),
        ];

        audit.check_batch(&records);

        assert_eq!(audit.checked_count(), 3);
        assert_eq!(audit.collision_count(), 3);
        assert!(audit.has_collisions());
        let fingerprint = encode_kv_ordered(&json!("a\"\""), &json!("b"));
        assert_eq!(
            audit.collisions(),
            &[
                Collision {
                    key_a: json!("a\"\""),
                    value_a: json!("b"),
                    key_b: json!("a\""),
                    value_b: json!("\"b"),
                    encoded_bytes: fingerprint.clone(),
                },
                Collision {
                    key_a: json!("a\"\""),
                    value_a: json!("b"),
                    key_b: json!("a"),
                    value_b: json!("\"\"b"),
                    encoded_bytes: fingerprint.clone(),
                },
                Collision {
                    key_a: json!("a\""),
                    value_a: json!("\"b"),
                    key_b: json!("a"),
                    value_b: json!("\"\"b"),
                    encoded_bytes: fingerprint,
                },
            ]
        );
    }

    #[test]
    fn collision_audit_shares_state_across_batch_and_pair_checks() {
        let mut audit = CollisionAudit::new();
        let first = (json!("a\""), json!("b"));
        let second = (json!("a"), json!("\"b"));

        audit.check_batch(std::slice::from_ref(&first));
        audit.check_batch(std::slice::from_ref(&second));
        assert_eq!(audit.checked_count(), 1);
        assert_eq!(audit.collision_count(), 1);

        audit.check_pair(
            first.0.clone(),
            first.1.clone(),
            second.0.clone(),
            second.1.clone(),
        );
        audit.check_batch(&[first, second]);

        assert_eq!(audit.checked_count(), 1);
        assert_eq!(audit.collision_count(), 1);
        assert_eq!(audit.truncated_fingerprint_count(), 0);
        assert_eq!(audit.truncated_record_count(), 0);
        assert!(audit.is_complete());
    }

    #[test]
    fn collision_audit_truncates_a_fingerprint_class_at_the_payload_bound() {
        let mut audit = CollisionAudit::new();
        let quote_count = MAX_LOGICAL_PAIRS_PER_FINGERPRINT;
        let records: Vec<_> = (0..=quote_count)
            .map(|index| {
                (
                    json!(format!("a{}", "\"".repeat(index))),
                    json!(format!("{}b", "\"".repeat(quote_count - index))),
                )
            })
            .collect();

        audit.check_batch(&records);

        assert_eq!(audit.collision_count(), quote_count * (quote_count - 1) / 2);
        assert_eq!(audit.checked_count(), quote_count * (quote_count - 1) / 2);
        assert_eq!(audit.truncated_fingerprint_count(), 1);
        assert_eq!(audit.truncated_record_count(), 1);
        assert!(!audit.is_complete());

        audit.check_batch(std::slice::from_ref(
            records.last().expect("non-empty records"),
        ));
        assert_eq!(audit.truncated_record_count(), 2);
        assert_eq!(audit.collision_count(), quote_count * (quote_count - 1) / 2);

        audit.check_pair(
            records[0].0.clone(),
            records[0].1.clone(),
            records.last().expect("non-empty records").0.clone(),
            records.last().expect("non-empty records").1.clone(),
        );
        assert_eq!(audit.truncated_record_count(), 3);
        assert_eq!(audit.collision_count(), quote_count * (quote_count - 1) / 2);
    }

    fn records_for_fingerprint_class(prefix: &str) -> Vec<(Value, Value)> {
        (0..MAX_LOGICAL_PAIRS_PER_FINGERPRINT)
            .map(|index| {
                (
                    json!(format!("{prefix}{}", "\"".repeat(index))),
                    json!(format!("{}b", "\"".repeat(63 - index))),
                )
            })
            .collect()
    }

    #[test]
    fn collision_audit_fails_closed_when_fingerprint_class_cap_is_reached() {
        let mut audit = CollisionAudit::new();
        let records: Vec<_> = (0..=MAX_FINGERPRINT_CLASSES)
            .map(|index| (json!(format!("key-{index}")), json!("value")))
            .collect();

        audit.check_batch(&records);

        assert!(!audit.is_complete());
        assert!(audit.has_collisions());
        assert_eq!(audit.collision_count(), 0);
        assert_eq!(audit.truncated_fingerprint_count(), 1);
        assert_eq!(audit.truncated_record_count(), 1);
        assert_eq!(
            audit.status(),
            CollisionAuditStatus {
                complete: false,
                checked_count: 0,
                collision_count: 0,
                truncated_fingerprint_count: 1,
                truncated_record_count: 1,
                truncated_comparison_count: 0,
                truncated_collision_count: 0,
                truncated_oversized_record_count: 0,
            }
        );
    }

    #[test]
    fn collision_audit_fails_closed_for_an_oversized_retained_fingerprint() {
        let mut audit = CollisionAudit::new();
        audit.check_batch(&[(
            json!("key"),
            json!("x".repeat(MAX_ENCODED_FINGERPRINT_BYTES)),
        )]);

        assert!(!audit.is_complete());
        assert!(audit.has_collisions());
        assert_eq!(audit.truncated_record_count(), 1);
        assert_eq!(audit.truncated_oversized_record_count(), 1);
        assert_eq!(audit.collision_count(), 0);
    }

    #[test]
    fn collision_audit_compacts_caller_owned_value_allocations() {
        let mut oversized_capacity = String::with_capacity(MAX_ENCODED_FINGERPRINT_BYTES * 2);
        oversized_capacity.push_str("a\"");
        let mut oversized_array = Vec::with_capacity(MAX_ENCODED_FINGERPRINT_BYTES * 2);
        oversized_array.push(json!("short"));
        let mut audit = CollisionAudit::new();
        audit.check_batch(&[
            (Value::String(oversized_capacity), json!("b")),
            (json!("a"), json!("\"b")),
            (Value::Array(oversized_array), json!("array")),
        ]);

        assert!(audit.is_complete());
        assert_eq!(audit.collision_count(), 1);
        assert!(audit.retained_payload_bytes <= MAX_RETAINED_PAYLOAD_BYTES);
        assert_eq!(
            audit.retained_payload_bytes,
            audit.accounted_payload_bytes()
        );
        for class in audit.seen.values() {
            for pair in &class.logical_pairs {
                if let Value::String(key) = &pair.key {
                    assert!(key.capacity() <= key.len());
                }
                if let Value::String(value) = &pair.value {
                    assert!(value.capacity() <= value.len());
                }
                if let Value::Array(key) = &pair.key {
                    assert!(key.capacity() <= key.len());
                }
            }
        }
        for collision in audit.collisions() {
            if let Value::String(key) = &collision.key_a {
                assert!(key.capacity() <= key.len());
            }
            if let Value::String(value) = &collision.value_a {
                assert!(value.capacity() <= value.len());
            }
        }
    }

    #[test]
    fn collision_audit_bounds_total_payloads_comparisons_and_reports() {
        let mut audit = CollisionAudit::new();
        let mut records = Vec::new();
        for class in 0..(MAX_TOTAL_LOGICAL_PAIRS / MAX_LOGICAL_PAIRS_PER_FINGERPRINT) {
            records.extend(records_for_fingerprint_class(&format!("class-{class}-")));
        }
        records.push((json!("overflow"), json!("record")));

        audit.check_batch(&records);

        assert!(!audit.is_complete());
        assert!(audit.has_collisions());
        assert_eq!(audit.collision_count(), MAX_COLLISIONS);
        assert_eq!(audit.checked_count(), MAX_TRACKED_COMPARISONS);
        assert_eq!(audit.truncated_record_count(), 1);
        assert!(audit.truncated_comparison_count() > 0);
        assert!(audit.truncated_collision_count() > 0);
        assert_eq!(
            audit.retained_payload_bytes,
            audit.accounted_payload_bytes()
        );
        assert!(audit.retained_payload_bytes <= MAX_RETAINED_PAYLOAD_BYTES);
        let lower_bound = audit.checked_count();
        audit.check_batch(std::slice::from_ref(
            records.last().expect("overflow record exists"),
        ));
        assert_eq!(audit.checked_count(), lower_bound);
    }
}

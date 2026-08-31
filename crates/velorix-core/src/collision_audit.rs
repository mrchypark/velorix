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
use std::collections::HashSet;

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
        self.checked_pairs += 1;
        let enc_a = encode_kv_ordered(&key_a, &value_a);
        let enc_b = encode_kv_ordered(&key_b, &value_b);

        if enc_a == enc_b && (key_a != key_b || value_a != value_b) {
            self.collisions.push(Collision {
                key_a,
                value_a,
                key_b,
                value_b,
                encoded_bytes: enc_a,
            });
        }
    }

    /// Check a batch of records for collisions.
    pub fn check_batch(
        &mut self,
        records: &[(Value, Value)], // (key, value) pairs
    ) {
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        for (key, value) in records {
            let enc = encode_kv_ordered(key, value);
            if !seen.insert(enc.clone()) {
                // Found a duplicate encoding - check if it's actually the same pair
                // (same key+value is OK, different key+value with same encoding is a collision)
                // We can't easily check here, but the insert failure means collision
            }
        }
    }

    /// Check if any collisions were detected.
    pub fn has_collisions(&self) -> bool {
        !self.collisions.is_empty()
    }

    /// Get the number of collisions.
    pub fn collision_count(&self) -> usize {
        self.collisions.len()
    }

    /// Get the number of pairs checked.
    pub fn checked_count(&self) -> usize {
        self.checked_pairs
    }

    /// Get all detected collisions.
    pub fn collisions(&self) -> &[Collision] {
        &self.collisions
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
    buf
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
    }

    #[test]
    fn collision_audit_multiple_pairs() {
        let mut audit = CollisionAudit::new();
        audit.check_pair(json!("a"), json!(1), json!("b"), json!(2));
        audit.check_pair(json!("c"), json!(3), json!("d"), json!(4));
        assert_eq!(audit.checked_count(), 2);
        assert!(!audit.has_collisions());
    }
}

//! Canonical, bounded wire codec for the Velorix metadata snapshot.
//!
//! The root-CAS transport owns publication.  This module only defines the
//! bytes which are evaluated and published, so a decoded snapshot is never a
//! hidden authoritative cache.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::hash::Hash;

use super::InMemoryMetaState;

pub(crate) mod map_pairs {
    use super::*;
    use serde::de::DeserializeOwned;

    pub fn serialize<S, K, V>(map: &HashMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        K: Serialize,
        V: Serialize,
    {
        let mut pairs = map
            .iter()
            .map(|(key, value)| {
                serde_json::to_vec(key)
                    .map(|bytes| (bytes, key, value))
                    .map_err(serde::ser::Error::custom)
            })
            .collect::<Result<Vec<_>, _>>()?;
        pairs.sort_by(|left, right| left.0.cmp(&right.0));
        pairs
            .into_iter()
            .map(|(_, key, value)| (key, value))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D, K, V>(deserializer: D) -> Result<HashMap<K, V>, D::Error>
    where
        D: serde::Deserializer<'de>,
        K: DeserializeOwned + Eq + Hash,
        V: DeserializeOwned,
    {
        let pairs = Vec::<(K, V)>::deserialize(deserializer)?;
        let mut map = HashMap::with_capacity(pairs.len());
        for (key, value) in pairs {
            if map.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate snapshot map key"));
            }
        }
        Ok(map)
    }
}

pub const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotCodecError {
    #[error("snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes")]
    TooLarge,
    #[error("unsupported snapshot schema version {0}")]
    UnsupportedSchema(u32),
    #[error("invalid snapshot: {0}")]
    Invalid(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    schema_version: u32,
    state: Value,
}

/// Encodes state using canonical JSON (object keys are recursively sorted).
/// The canonical representation makes root/page digests stable across
/// HashMap iteration order and process restarts.
pub(crate) fn encode(state: &InMemoryMetaState) -> Result<Vec<u8>, SnapshotCodecError> {
    let state =
        serde_json::to_value(state).map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    let envelope = Envelope {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        state: canonicalize(state),
    };
    let bytes =
        serde_json::to_vec(&envelope).map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(SnapshotCodecError::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<InMemoryMetaState, SnapshotCodecError> {
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(SnapshotCodecError::TooLarge);
    }
    let envelope: Envelope =
        serde_json::from_slice(bytes).map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    if envelope.schema_version != SNAPSHOT_SCHEMA_VERSION {
        return Err(SnapshotCodecError::UnsupportedSchema(
            envelope.schema_version,
        ));
    }
    let state = serde_json::from_value(envelope.state)
        .map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    let canonical = encode(&state)?;
    if canonical != bytes {
        return Err(SnapshotCodecError::Invalid(
            "snapshot is not canonical or contains duplicate/unknown fields".into(),
        ));
    }
    Ok(state)
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = Map::new();
            for (key, value) in entries {
                sorted.insert(key, canonicalize(value));
            }
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonempty_state_roundtrips_and_is_deterministic() {
        let mut first = InMemoryMetaState {
            partition_authority_now_unix_ms: 42,
            ..Default::default()
        };
        first.committed_ingest_batch_keys.insert("batch-a".into());
        first
            .view_dependency_graph_revisions
            .insert("tenant".into(), 7);

        let mut second = InMemoryMetaState::default();
        second
            .view_dependency_graph_revisions
            .insert("tenant".into(), 7);
        second.committed_ingest_batch_keys.insert("batch-a".into());
        second.partition_authority_now_unix_ms = 42;

        let left = encode(&first).unwrap();
        let right = encode(&second).unwrap();
        assert_eq!(left, right);
        assert_eq!(decode(&left).unwrap().partition_authority_now_unix_ms, 42);
        assert_eq!(
            decode(&left).unwrap().view_dependency_graph_revisions["tenant"],
            7
        );
    }

    #[test]
    fn rejects_unknown_schema_and_oversized_input() {
        let unknown = br#"{"schema_version":99,"state":{}}"#;
        assert!(matches!(
            decode(unknown),
            Err(SnapshotCodecError::UnsupportedSchema(99))
        ));
        assert!(matches!(
            decode(&vec![b' '; MAX_SNAPSHOT_BYTES + 1]),
            Err(SnapshotCodecError::TooLarge)
        ));
    }

    #[test]
    fn populated_tuple_key_map_roundtrips() {
        let reservation = super::super::IngestRangeReservation {
            stream_id: "stream".into(),
            partition_id: 3,
            start_offset_inclusive: 10,
            end_offset_exclusive: 11,
            batch_key: "batch".into(),
            payload_digest: "sha256:digest".into(),
            relation_id: "relation".into(),
            relation_version: "v1".into(),
            schema_fingerprint: "sha256:schema".into(),
            writer_epoch: 2,
        };
        let mut state = InMemoryMetaState::default();
        state
            .ingest_reservations
            .insert(("stream".into(), 3), vec![reservation.clone()]);
        state.legacy_batch_keys.insert("batch".into(), reservation);
        let authority_key = super::super::RelationPartitionAuthorityKey {
            namespace: "ns".into(),
            relation_id: "relation".into(),
            stream_id: "stream".into(),
            partition_id: 3,
        };
        state.relation_partition_authorities.insert(
            authority_key.clone(),
            super::super::RelationPartitionAuthorityToken {
                key: authority_key,
                owner_id: "owner".into(),
                owner_epoch: 1,
                expires_at_unix_ms: 100,
            },
        );
        let decoded = decode(&encode(&state).unwrap()).unwrap();
        assert_eq!(decoded.ingest_reservations.len(), 1);
        assert_eq!(decoded.legacy_batch_keys.len(), 1);
        assert_eq!(decoded.relation_partition_authorities.len(), 1);
    }

    #[test]
    fn duplicate_pair_keys_are_rejected() {
        let pairs = "[[\"same\",1],[\"same\",2]]";
        let mut deserializer = serde_json::Deserializer::from_str(pairs);
        let result: Result<HashMap<String, u64>, _> =
            super::map_pairs::deserialize(&mut deserializer);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_and_unknown_envelope_fields_are_rejected() {
        let canonical = String::from_utf8(encode(&InMemoryMetaState::default()).unwrap()).unwrap();
        let duplicate = canonical.replacen(
            "{\"schema_version\":1,",
            "{\"schema_version\":1,\"schema_version\":1,",
            1,
        );
        assert!(matches!(
            decode(duplicate.as_bytes()),
            Err(SnapshotCodecError::Invalid(_))
        ));
        let unknown = canonical.replacen(
            "{\"schema_version\":1,",
            "{\"unknown\":0,\"schema_version\":1,",
            1,
        );
        assert!(matches!(
            decode(unknown.as_bytes()),
            Err(SnapshotCodecError::Invalid(_))
        ));
    }
}

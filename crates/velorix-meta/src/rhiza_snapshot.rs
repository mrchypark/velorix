//! Canonical, bounded wire codec for the Velorix metadata snapshot.
//!
//! The root-CAS transport owns publication.  This module only defines the
//! bytes which are evaluated and published, so a decoded snapshot is never a
//! hidden authoritative cache.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;

#[cfg(any(test, feature = "rhiza-backend"))]
use super::InMemoryMetaState;
#[cfg(any(test, feature = "rhiza-backend"))]
use serde_json::{Map, Value};

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

#[cfg(any(test, feature = "rhiza-backend"))]
pub const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
/// Bound compressed snapshot bytes independently of the expanded state budget.
#[cfg(any(test, feature = "rhiza-backend"))]
pub const MAX_SNAPSHOT_ENCODED_BYTES: usize = 16 * 1024 * 1024;
/// Arrow's logical typed-tree budget is independent of compressed IPC size.
#[cfg(any(test, feature = "rhiza-backend"))]
pub const MAX_SNAPSHOT_EXPANDED_BYTES: usize = 64 * 1024 * 1024;

#[cfg(any(test, feature = "rhiza-backend"))]
#[derive(Debug, thiserror::Error)]
pub enum SnapshotCodecError {
    #[error("snapshot exceeds its encoded or expanded byte limit")]
    TooLarge,
    #[error("unsupported snapshot schema version {0}")]
    UnsupportedSchema(u32),
    #[error("invalid snapshot: {0}")]
    Invalid(String),
}

#[cfg(any(test, feature = "rhiza-backend"))]
#[derive(Debug, Serialize, Deserialize)]
struct Envelope<T> {
    schema_version: u32,
    state: T,
}

#[cfg(any(test, feature = "rhiza-backend"))]
/// Encodes the normalized metadata state as bounded, ZSTD-compressed Arrow IPC.
/// Sorting map keys before encoding keeps snapshot bytes stable across restarts.
pub(crate) fn encode(state: &InMemoryMetaState) -> Result<Vec<u8>, SnapshotCodecError> {
    let state =
        serde_json::to_value(state).map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    let envelope = Envelope {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        state: canonicalize(state),
    };
    let expanded_bytes =
        velorix_core::arrow_state::ArrowState::measure_serializable(&envelope, usize::MAX)
            .map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    if expanded_bytes > MAX_SNAPSHOT_EXPANDED_BYTES {
        return Err(SnapshotCodecError::TooLarge);
    }
    let arrow_state = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
        &envelope,
        MAX_SNAPSHOT_EXPANDED_BYTES,
    )
    .map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    if arrow_state.as_bytes().len() > MAX_SNAPSHOT_ENCODED_BYTES {
        return Err(SnapshotCodecError::TooLarge);
    }
    Ok(arrow_state.as_bytes().to_vec())
}

#[cfg(any(test, feature = "rhiza-backend"))]
pub(crate) fn decode(bytes: &[u8]) -> Result<InMemoryMetaState, SnapshotCodecError> {
    decode_bounded(
        bytes,
        MAX_SNAPSHOT_ENCODED_BYTES,
        MAX_SNAPSHOT_EXPANDED_BYTES,
    )
}

#[cfg(any(test, feature = "rhiza-backend"))]
fn decode_bounded(
    bytes: &[u8],
    encoded_limit: usize,
    expanded_limit: usize,
) -> Result<InMemoryMetaState, SnapshotCodecError> {
    if bytes.len() > encoded_limit {
        return Err(SnapshotCodecError::TooLarge);
    }
    let arrow_state = velorix_core::arrow_state::ArrowState::from_ipc(bytes, expanded_limit)
        .map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    if arrow_state.len() > expanded_limit {
        return Err(SnapshotCodecError::TooLarge);
    }
    let envelope: Envelope<Value> = arrow_state
        .decode()
        .map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    if envelope.schema_version != SNAPSHOT_SCHEMA_VERSION {
        return Err(SnapshotCodecError::UnsupportedSchema(
            envelope.schema_version,
        ));
    }
    let had_active_view_registries = envelope
        .state
        .as_object()
        .is_some_and(|state| state.contains_key("active_view_registries"));
    let state: InMemoryMetaState = serde_json::from_value(envelope.state)
        .map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    // Older Arrow snapshots predate the active-view registry field. Accept
    // only that specific omission by normalizing it to the serde default
    // before applying the usual canonical-byte check.
    let mut canonical_state = serde_json::to_value(&state)
        .map_err(|error| SnapshotCodecError::Invalid(error.to_string()))?;
    if !had_active_view_registries {
        let Some(object) = canonical_state.as_object_mut() else {
            return Err(SnapshotCodecError::Invalid(
                "snapshot state is not an object".into(),
            ));
        };
        object.remove("active_view_registries");
    }
    let canonical_envelope = Envelope {
        schema_version: envelope.schema_version,
        state: canonicalize(canonical_state),
    };
    let canonical = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
        &canonical_envelope,
        MAX_SNAPSHOT_EXPANDED_BYTES,
    )
    .map_err(|e| SnapshotCodecError::Invalid(e.to_string()))?;
    if canonical.as_bytes() != bytes {
        return Err(SnapshotCodecError::Invalid(
            "snapshot is not canonical or contains duplicate/unknown fields".into(),
        ));
    }
    Ok(state)
}

#[cfg(any(test, feature = "rhiza-backend"))]
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
        let unknown = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
            &Envelope {
                schema_version: 99,
                state: Value::Object(Map::new()),
            },
            MAX_SNAPSHOT_EXPANDED_BYTES,
        )
        .unwrap();
        assert!(matches!(
            decode(unknown.as_bytes()),
            Err(SnapshotCodecError::UnsupportedSchema(99))
        ));
        assert!(matches!(
            decode(&vec![b' '; MAX_SNAPSHOT_ENCODED_BYTES + 1]),
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
    fn stored_snapshot_body_is_arrow_ipc_and_roundtrips_canonically() {
        let bytes = encode(&InMemoryMetaState::default()).unwrap();
        assert!(velorix_core::arrow_state::ArrowState::from_ipc(
            &bytes,
            MAX_SNAPSHOT_EXPANDED_BYTES
        )
        .is_ok());
        assert!(!bytes.starts_with(b"{"));
        assert_eq!(encode(&decode(&bytes).unwrap()).unwrap(), bytes);
    }

    #[test]
    fn older_arrow_snapshot_without_active_registry_defaults_empty() {
        let mut legacy_state = serde_json::to_value(InMemoryMetaState::default()).unwrap();
        legacy_state
            .as_object_mut()
            .unwrap()
            .remove("active_view_registries");
        let legacy_snapshot = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
            &Envelope {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                state: canonicalize(legacy_state),
            },
            MAX_SNAPSHOT_EXPANDED_BYTES,
        )
        .unwrap();

        let decoded = decode(legacy_snapshot.as_bytes()).unwrap();
        assert!(decoded.active_view_registries.is_empty());
    }

    #[test]
    fn rejects_highly_compressible_oversized_state_before_decompression() {
        let oversized = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
            &Envelope {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                state: "x".repeat(3 * 1024 * 1024),
            },
            4 * 1024 * 1024,
        )
        .unwrap();
        assert!(oversized.as_bytes().len() < 64 * 1024);
        assert!(
            velorix_core::arrow_state::ArrowState::metadata(oversized.as_bytes(), 1024 * 1024)
                .is_err()
        );
        assert!(matches!(
            decode_bounded(oversized.as_bytes(), 64 * 1024, 1024 * 1024),
            Err(SnapshotCodecError::Invalid(_))
        ));
    }
}

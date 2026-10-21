//! Identity envelopes for the shared runtime Arrow IPC state. Schema metadata
//! changes never decode or recompress the already-compressed runtime buffers.
use super::*;
use velorix_core::arrow_state::ArrowState;

pub(super) fn encoded_limit(state_bytes: usize) -> usize {
    state_bytes.saturating_mul(2).saturating_add(16 * 1024)
}
fn state_error(error: impl std::fmt::Display) -> ApiError {
    ApiError::bad_request(format!("invalid Arrow/ZSTD checkpoint body: {error}"))
}

fn invalid() -> ApiError {
    ApiError::bad_request("invalid Arrow/ZSTD checkpoint body")
}

pub(super) fn encode(record: &StandingRuntimeStatePayloadRecord) -> Result<Vec<u8>, ApiError> {
    let p = &record.payload.payload;
    let metadata = [
        ("schema_version", record.schema_version.to_string()),
        ("record_kind", record.record_kind.clone()),
        ("tenant_id", record.tenant_id.clone()),
        ("program_id", record.program_id.clone()),
        ("view_id", record.view_id.clone()),
        ("logical_epoch", record.logical_epoch.to_string()),
        (
            "checkpoint_codec_identity",
            record.checkpoint_codec_identity.clone(),
        ),
        ("state_content_hash", record.state_content_hash.clone()),
        ("source_kind", record.source_kind.clone()),
        (
            "payload_codec_identity",
            record.payload.codec_identity.clone(),
        ),
        ("state_bytes", p.len().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v))
    .collect();
    ArrowState::replace_metadata(p.as_bytes(), metadata, p.len()).map_err(state_error)
}
pub(super) fn decode(
    bytes: &[u8],
    max_state_bytes: usize,
) -> Result<StandingRuntimeStatePayloadRecord, ApiError> {
    let m = ArrowState::metadata(bytes, max_state_bytes).map_err(state_error)?;
    let get = |key: &str| m.get(key).cloned().ok_or_else(invalid);
    if m.len() != 12 {
        return Err(invalid());
    }
    let payload = ArrowState::from_ipc(bytes, max_state_bytes).map_err(state_error)?;
    if payload.len() != get("state_bytes")?.parse::<usize>().map_err(state_error)?
        || stable_bytes_hash(payload.as_bytes()) != get("state_content_hash")?
    {
        return Err(invalid());
    }
    Ok(StandingRuntimeStatePayloadRecord {
        schema_version: get("schema_version")?.parse().map_err(state_error)?,
        record_kind: get("record_kind")?,
        tenant_id: get("tenant_id")?,
        program_id: get("program_id")?,
        view_id: get("view_id")?,
        logical_epoch: get("logical_epoch")?.parse().map_err(state_error)?,
        checkpoint_codec_identity: get("checkpoint_codec_identity")?,
        state_content_hash: get("state_content_hash")?,
        source_kind: get("source_kind")?,
        payload: RuntimeCheckpointStatePayload {
            codec_identity: get("payload_codec_identity")?,
            payload,
        },
    })
}
pub(super) fn encode_retained_state(
    reference: &EventTimeStateRef,
    payload: &ArrowState,
) -> Result<Vec<u8>, ApiError> {
    let metadata = [
        ("record_kind", "retained-state-v2".into()),
        ("object_key", reference.state_root.object_key.clone()),
        ("window_key", reference.window_key.clone()),
        ("logical_epoch", reference.logical_epoch.to_string()),
        (
            "state_content_hash",
            reference.state_root.content_hash.clone(),
        ),
        ("state_bytes", payload.len().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v))
    .collect();
    ArrowState::replace_metadata(payload.as_bytes(), metadata, payload.len()).map_err(state_error)
}
pub(super) fn decode_retained_state(
    bytes: &[u8],
    reference: &EventTimeStateRef,
    max_state_bytes: usize,
) -> Result<ArrowState, ApiError> {
    let m = ArrowState::metadata(bytes, max_state_bytes).map_err(state_error)?;
    let epoch = reference.logical_epoch.to_string();
    for (key, expected) in [
        ("record_kind", "retained-state-v2"),
        ("object_key", reference.state_root.object_key.as_str()),
        ("window_key", reference.window_key.as_str()),
        ("logical_epoch", epoch.as_str()),
        (
            "state_content_hash",
            reference.state_root.content_hash.as_str(),
        ),
    ] {
        if m.get(key).is_none_or(|v| v != expected) {
            return Err(invalid());
        }
    }
    if m.len() != 7 {
        return Err(invalid());
    }
    let payload = ArrowState::from_ipc(bytes, max_state_bytes).map_err(state_error)?;
    if payload.len()
        != m.get("state_bytes")
            .ok_or_else(invalid)?
            .parse::<usize>()
            .map_err(state_error)?
        || stable_bytes_hash(payload.as_bytes()) != reference.state_root.content_hash
    {
        return Err(invalid());
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn envelope_reuses_runtime_buffers_and_preserves_commitment() {
        let checkpoint = crate::tests::test_runtime_checkpoint(Vec::new());
        let (_, record) =
            checkpoint_publication::standing_runtime_state_payload_record_for_checkpoint(
                &checkpoint,
                "purchases_by_user",
            )
            .unwrap();
        let bytes = encode(&record).unwrap();
        let restored = decode(&bytes, record.payload.payload.len()).unwrap();
        assert_eq!(restored, record);
        assert_eq!(
            restored.payload.payload.as_bytes(),
            record.payload.payload.as_bytes()
        );
        assert_eq!(encode(&record).unwrap(), bytes);
        assert!(serde_json::from_slice::<Value>(&bytes).is_err());
        assert!(decode(&bytes, record.payload.payload.len() - 1).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing, record.payload.payload.len()).is_err());
        let mut changed = record.clone();
        changed.payload.payload =
            velorix_core::arrow_state::to_state(&json!({"different":true})).unwrap();
        assert!(decode(&encode(&changed).unwrap(), 1024 * 1024).is_err());
    }
    #[test]
    fn compressed_retained_state_is_bounded_and_bound_to_reference() {
        let payload =
            velorix_core::arrow_state::to_state(&json!({"retained":"x".repeat(256*1024)})).unwrap();
        let reference = EventTimeStateRef {
            window_key: "synthetic-window".into(),
            logical_epoch: 19,
            state_root: velorix_core::standing_program::DurableStateRoot {
                object_key: "synthetic/state.arrow".into(),
                content_hash: stable_bytes_hash(payload.as_bytes()),
            },
        };
        let bytes = encode_retained_state(&reference, &payload).unwrap();
        assert!(bytes.len() < payload.len() / 10);
        assert_eq!(
            decode_retained_state(&bytes, &reference, payload.len()).unwrap(),
            payload
        );
        assert!(decode_retained_state(&bytes, &reference, 128).is_err());
        let mut foreign = reference.clone();
        foreign.logical_epoch += 1;
        assert!(decode_retained_state(&bytes, &foreign, payload.len()).is_err());
        assert!(decode_retained_state(b"{}", &reference, payload.len()).is_err());
    }
}

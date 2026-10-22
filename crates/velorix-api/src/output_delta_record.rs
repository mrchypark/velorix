//! Typed Arrow IPC envelope for persisted output deltas.
use super::*;

#[derive(Deserialize, Serialize)]
struct OutputDeltaArrowEnvelope {
    record: StandingRuntimeOutputDeltaRecord,
    delta: DeltaBatch,
}

pub(super) fn encode(
    record: &StandingRuntimeOutputDeltaRecord,
    max_state_bytes: usize,
) -> Result<Vec<u8>, ApiError> {
    let delta: DeltaBatch = serde_json::from_value(record.output_delta.clone())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let mut metadata = record.clone();
    metadata.output_delta = Value::Null;
    let state = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
        &OutputDeltaArrowEnvelope {
            record: metadata,
            delta,
        },
        max_state_bytes,
    )
    .map_err(ApiError::bad_request)?;
    let bytes = state.as_bytes().to_vec();
    Ok(bytes)
}

pub(super) fn decode(
    bytes: &[u8],
    max_state_bytes: usize,
) -> Result<StandingRuntimeOutputDeltaRecord, ApiError> {
    let state = velorix_core::arrow_state::ArrowState::from_ipc(bytes, max_state_bytes).map_err(
        |error| ApiError::bad_request(format!("invalid output delta Arrow body: {error}")),
    )?;
    let mut record = state
        .field::<StandingRuntimeOutputDeltaRecord>("record")
        .map_err(ApiError::bad_request)?
        .ok_or_else(|| ApiError::bad_request("invalid Arrow output delta envelope"))?;
    let delta = state
        .field::<DeltaBatch>("delta")
        .map_err(ApiError::bad_request)?
        .ok_or_else(|| ApiError::bad_request("invalid Arrow output delta envelope"))?;
    if !record.output_delta.is_null() {
        return Err(ApiError::bad_request("invalid Arrow output delta envelope"));
    }
    record.output_delta =
        serde_json::to_value(delta).map_err(|error| ApiError::internal(error.to_string()))?;
    record.output_delta.sort_all_objects();
    if encode(&record, max_state_bytes)?.as_slice() != bytes {
        return Err(ApiError::bad_request(
            "output delta Arrow record is not canonically encoded",
        ));
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_delta_round_trips_and_rejects_corruption() {
        let delta = DeltaBatch::from_records([velorix_core::delta::DeltaRecord {
            key: velorix_core::delta::DeltaKey::from_json(json!("k")),
            value: velorix_core::delta::DeltaValue::from_json(json!("v")),
            weight: -2,
        }]);
        let mut output_delta = serde_json::to_value(&delta).unwrap();
        output_delta.sort_all_objects();
        let record = StandingRuntimeOutputDeltaRecord {
            schema_version: 1,
            record_kind: "standing_runtime_output_delta_v1".into(),
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            logical_epoch: 4,
            schema_fingerprint: "schema".into(),
            delta_content_hash: stable_bytes_hash(&serde_json::to_vec(&output_delta).unwrap()),
            delta_encoding: "velorix-arrow-ipc-zstd3-v1".into(),
            delta_row_count: 1,
            source_kind: "standing_runtime_epoch_output_delta".into(),
            output_delta,
            producer_commit: None,
        };
        let bytes = encode(&record, 1024 * 1024).unwrap();
        assert!(serde_json::from_slice::<Value>(&bytes).is_err());
        let restored = decode(&bytes, 1024 * 1024).unwrap();
        assert_eq!(restored.output_delta, record.output_delta);
        assert_eq!(restored.delta_content_hash, record.delta_content_hash);
        assert_eq!(encode(&restored, 1024 * 1024).unwrap(), bytes);
        let mut corrupt = bytes;
        corrupt.push(0);
        assert!(decode(&corrupt, 1024 * 1024).is_err());
    }
}

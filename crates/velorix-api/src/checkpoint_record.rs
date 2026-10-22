//! Typed Arrow IPC envelope for the runtime checkpoint record.
use super::*;
use velorix_core::arrow_state::{from_state, ArrowState};

const RECORD_KIND: &str = "standing_runtime_checkpoint_v1";
const CODEC_IDENTITY: &str = "velorix-arrow-ipc-zstd3-v1";
pub(crate) const MAX_ENCODED_RECORD_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn encode(
    record: &StandingRuntimeCheckpointRecord,
    max_state_bytes: usize,
) -> Result<Vec<u8>, ApiError> {
    if record.schema_version != 1 || record.record_kind != RECORD_KIND {
        return Err(ApiError::bad_request(
            "invalid checkpoint record version or kind",
        ));
    }
    let state = ArrowState::from_serializable_bounded(record, max_state_bytes)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let metadata = [
        ("record_kind".into(), RECORD_KIND.into()),
        ("codec_identity".into(), CODEC_IDENTITY.into()),
    ]
    .into_iter()
    .collect();
    let bytes = ArrowState::replace_metadata(state.as_bytes(), metadata, max_state_bytes)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if bytes.len() > MAX_ENCODED_RECORD_BYTES {
        return Err(ApiError::payload_too_large(
            "standing runtime checkpoint record exceeds the 16 MiB envelope budget",
        ));
    }
    Ok(bytes)
}

pub(super) fn decode(
    bytes: &[u8],
    max_state_bytes: usize,
) -> Result<StandingRuntimeCheckpointRecord, ApiError> {
    if bytes.len() > MAX_ENCODED_RECORD_BYTES {
        return Err(ApiError::payload_too_large(
            "standing runtime checkpoint record exceeds the 16 MiB envelope budget",
        ));
    }
    let metadata = ArrowState::metadata(bytes, max_state_bytes).map_err(|error| {
        ApiError::bad_request(format!("invalid checkpoint Arrow body: {error}"))
    })?;
    if metadata.len() != 3
        || metadata.get("record_kind").map(String::as_str) != Some(RECORD_KIND)
        || metadata.get("codec_identity").map(String::as_str) != Some(CODEC_IDENTITY)
    {
        return Err(ApiError::bad_request("invalid checkpoint Arrow envelope"));
    }
    let state = ArrowState::from_ipc(bytes, max_state_bytes).map_err(|error| {
        ApiError::bad_request(format!("invalid checkpoint Arrow body: {error}"))
    })?;
    let mut record: StandingRuntimeCheckpointRecord = from_state(&state).map_err(|error| {
        ApiError::bad_request(format!("invalid checkpoint Arrow record: {error}"))
    })?;
    if record.schema_version != 1 || record.record_kind != RECORD_KIND {
        return Err(ApiError::bad_request(
            "invalid checkpoint record version or kind",
        ));
    }
    if let Some(demand) = &record.demand_materialization {
        demand.validate_checkpoint(&record.checkpoint)?;
    }
    if encode(&record, max_state_bytes)?.as_slice() != bytes {
        return Err(ApiError::bad_request(
            "checkpoint Arrow record is not canonically encoded",
        ));
    }
    record.manifest_hash = stable_bytes_hash(bytes);
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_demand_progress_round_trip_as_typed_arrow_and_reject_corruption() {
        let checkpoint = crate::tests::test_runtime_checkpoint(Vec::new());
        let output_schema_hash = checkpoint.identity.output_schema_hash.clone();
        let demand: demand_materialization::DemandCheckpoint = serde_json::from_value(json!({
            "schema_version": 1,
            "spec_hash": "spec",
            "plan_hash": "plan",
            "output_schema_hash": output_schema_hash,
            "paused": false,
            "order": "forward",
            "artifacts": {},
            "certificates": [],
            "full_complete": false
        }))
        .unwrap();
        let record = StandingRuntimeCheckpointRecord {
            demand_materialization: Some(demand),
            published_input_bootstrap: None,
            schema_version: 1,
            record_kind: RECORD_KIND.into(),
            view_id: checkpoint.identity.view_ids[0].clone(),
            checkpoint_key: "test.checkpoint.arrow".into(),
            previous_checkpoint: None,
            checkpoint,
            replay_checkpoints: Vec::new(),
            manifest_hash: String::new(),
        };
        let bytes = encode(&record, 16 * 1024 * 1024).unwrap();
        assert!(serde_json::from_slice::<Value>(&bytes).is_err());
        let restored = decode(&bytes, 16 * 1024 * 1024).unwrap();
        assert_eq!(
            restored.demand_materialization,
            record.demand_materialization
        );
        assert_eq!(restored.checkpoint, record.checkpoint);
        assert_eq!(restored.manifest_hash, stable_bytes_hash(&bytes));
        assert_eq!(encode(&restored, 16 * 1024 * 1024).unwrap(), bytes);
        assert!(decode(&bytes, 1).is_err());
        let mut unsupported = record;
        unsupported.schema_version = 2;
        let state = ArrowState::from_serializable_bounded(&unsupported, 16 * 1024 * 1024).unwrap();
        let metadata = [
            ("record_kind".into(), RECORD_KIND.into()),
            ("codec_identity".into(), CODEC_IDENTITY.into()),
        ]
        .into_iter()
        .collect();
        let unsupported =
            ArrowState::replace_metadata(state.as_bytes(), metadata, 16 * 1024 * 1024).unwrap();
        assert!(decode(&unsupported, 16 * 1024 * 1024).is_err());
        let mut corrupted = bytes;
        corrupted.push(0);
        assert!(decode(&corrupted, 16 * 1024 * 1024).is_err());
    }

    #[test]
    fn rejects_duplicate_demand_artifact_map_keys_in_arrow_record() {
        use arrow::{
            array::{Array, StringArray},
            ipc::{
                reader::StreamReader,
                writer::{IpcWriteOptions, StreamWriter},
                CompressionType,
            },
            record_batch::RecordBatch,
        };
        use std::{collections::BTreeMap, io::Cursor, sync::Arc};

        let checkpoint = crate::tests::test_runtime_checkpoint(Vec::new());
        let demand = demand_materialization::DemandCheckpoint {
            schema_version: 1,
            spec_hash: "spec".into(),
            plan_hash: "plan".into(),
            output_schema_hash: checkpoint.identity.output_schema_hash.clone(),
            paused: false,
            order: demand_materialization::DemandOrder::Forward,
            artifacts: ["alpha", "beta"]
                .into_iter()
                .map(|key| {
                    (
                        key.into(),
                        demand_materialization::DemandArtifactProgress {
                            artifact_key: key.into(),
                            artifact_hash: format!("hash-{key}"),
                            applied: velorix_core::processed_spans::ProcessedSpans::new(0),
                            inspected: BTreeMap::new(),
                            source_watermark: None,
                        },
                    )
                })
                .collect(),
            certificates: Vec::new(),
            full_complete: false,
            event_time_replay_id: None,
            event_time_output_readiness: Vec::new(),
        };
        let record = StandingRuntimeCheckpointRecord {
            demand_materialization: Some(demand),
            published_input_bootstrap: None,
            schema_version: 1,
            record_kind: RECORD_KIND.into(),
            view_id: checkpoint.identity.view_ids[0].clone(),
            checkpoint_key: "duplicate.checkpoint.arrow".into(),
            previous_checkpoint: None,
            checkpoint,
            replay_checkpoints: Vec::new(),
            manifest_hash: String::new(),
        };
        let bytes = encode(&record, 16 * 1024 * 1024).unwrap();

        // ArrowState's generic serializer canonicalizes maps, so introduce the
        // duplicate at the IPC node level after producing a valid two-key map.
        let mut reader = StreamReader::try_new(Cursor::new(&bytes), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let mut columns = batch.columns().to_vec();
        let text = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = (0..text.len())
            .map(|i| {
                if text.is_null(i) {
                    None
                } else {
                    Some(if text.value(i) == "beta" {
                        "alpha".into()
                    } else {
                        text.value(i).to_owned()
                    })
                }
            })
            .collect::<Vec<Option<String>>>();
        assert_eq!(
            values
                .iter()
                .flatten()
                .filter(|value| *value == "alpha")
                .count(),
            4
        );
        columns[6] = Arc::new(StringArray::from_iter(
            values.iter().map(|value| value.as_deref()),
        ));
        let duplicate_batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap()
            .try_with_compression_level(Some(3))
            .unwrap();
        let mut duplicate_bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new_with_options(
                &mut duplicate_bytes,
                &duplicate_batch.schema(),
                options,
            )
            .unwrap();
            writer.write(&duplicate_batch).unwrap();
            writer.finish().unwrap();
        }

        let error = decode(&duplicate_bytes, 16 * 1024 * 1024).unwrap_err();
        assert!(
            error.to_string().contains("canonically encoded"),
            "duplicate artifact map should reach canonical re-encode rejection, got: {error}"
        );
    }
}

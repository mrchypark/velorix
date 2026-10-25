use std::sync::Arc;

use anyhow::{bail, Context};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use serde::Deserialize;
use velorix_storage::ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest};

#[derive(Clone, Debug, Deserialize)]
struct DefaultScoreRow {
    user_id: String,
    score: i64,
    delta: i64,
}

pub fn encode_default_scores_payload(
    schema_fingerprint: &str,
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    rows_json: &str,
) -> anyhow::Result<Bytes> {
    let rows: Vec<DefaultScoreRow> = serde_json::from_str(rows_json)
        .with_context(|| "failed to parse --rows-json as validation fixture rows")?;
    if rows.is_empty() {
        bail!("validation fixture payload requires at least one row");
    }
    let end_offset_exclusive = start_offset_inclusive
        .checked_add(rows.len() as u64)
        .with_context(|| "validation fixture payload offset range overflow")?;
    let batch = default_scores_record_batch(&rows)?;
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: schema_fingerprint.to_string(),
            stream_id: stream_id.to_string(),
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: None,
        },
        &[batch],
    )
    .map_err(anyhow::Error::from)
}

fn default_scores_record_batch(rows: &[DefaultScoreRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("delta", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.user_id.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.score).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.delta).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .map_err(anyhow::Error::from)
}

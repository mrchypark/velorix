//! Materialized output pages. These are output-read proofs, not restore proofs.
use super::*;
use velorix_core::delta::encode_kv_ordered;
use velorix_core::query::{PageComparison, PagePredicate, PageScalar};

/// The general materialized output page codec.
pub(super) const CODEC: &str = "velorix-materialized-arrow-pages-v3";
/// The generic/demand bag page codec.
pub(super) const BAG_CODEC: &str = "velorix-materialized-bag-arrow-pages-v2";
pub(super) const GENERIC_ARROW_CODEC: &str = "velorix-delta-batch-arrow-v1";
//
pub(super) fn is_materialized_codec(codec: &str) -> bool {
    matches!(codec, CODEC | BAG_CODEC)
}
pub(super) fn is_bag_codec(codec: &str) -> bool {
    codec == BAG_CODEC
}

#[derive(Serialize, Deserialize)]
struct GenericPageArrowEnvelope {
    page: StandingRuntimeOutputPageRecord,
}

/// The published page identity of one codec.
///
/// The general materialized codec and the bag codec are distinct formats, not
/// aliases: they differ in record kind, schema version, IPC header and the
/// canonical column extraction order. Everything that writes or validates a
/// page derives those fields from this table so the two never drift.
struct CodecSpec {
    codec: &'static str,
    schema_version: u16,
    manifest_kind: &'static str,
    page_kind: &'static str,
    ipc_header: &'static str,
    /// The bag codec binds a family; the general codec has no family binding.
    bag: bool,
}

const GENERAL_CODEC_SPEC: CodecSpec = CodecSpec {
    codec: CODEC,
    schema_version: 3,
    manifest_kind: "standing_runtime_materialized_output_manifest_v3",
    page_kind: "standing_runtime_materialized_output_page_v3",
    ipc_header: "velorix.materialized.page.v3",
    bag: false,
};

const BAG_CODEC_SPEC: CodecSpec = CodecSpec {
    codec: BAG_CODEC,
    schema_version: 4,
    manifest_kind: "standing_runtime_native_output_manifest_v4",
    page_kind: "standing_runtime_native_output_page_v4",
    ipc_header: "velorix.native.page.v2",
    bag: true,
};

fn codec_spec(codec: &str) -> Option<&'static CodecSpec> {
    match codec {
        CODEC => Some(&GENERAL_CODEC_SPEC),
        BAG_CODEC => Some(&BAG_CODEC_SPEC),
        _ => None,
    }
}
const PAGE_ROWS: usize = 512;
// Reuse the API state-body byte budget for the compact index; a conservative
// 1KiB/ref preflight bounds page expansion before any page bodies are allocated.
const MAX_INDEX_BYTES: usize = DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES;
const MAX_PAGES: usize = MAX_INDEX_BYTES / 1024;
pub(super) fn fits_page_index(output: &DeltaBatch) -> Result<bool, ApiError> {
    let total = output
        .net_rows()
        .map_err(ApiError::bad_request)?
        .iter()
        .try_fold(0usize, |total, row| {
            let weight = usize::try_from(row.weight).map_err(ApiError::bad_request)?;
            total.checked_add(weight).ok_or_else(invalid)
        })?;
    Ok(total.div_ceil(PAGE_ROWS).max(1) <= MAX_PAGES)
}
const SOURCE: &str = "standing_runtime_checkpoint_published_output";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageIndex {
    /// The bag family that owns this output. Absent for the general
    /// materialized codec, whose pages carry no family binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    family: Option<String>,
    schema: RelationSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_stats: Option<Vec<Vec<ColumnStats>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ColumnStats {
    nulls: usize,
    min: Option<PageScalar>,
    max: Option<PageScalar>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageRows {
    /// The bag family that owns this output. Absent for the general
    /// materialized codec, whose pages carry no family binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    family: Option<String>,
    schema_fingerprint: String,
    checkpoint_content_hash: String,
    rows: DeltaBatch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema: Option<RelationSchema>,
}

#[track_caller]
fn invalid() -> ApiError {
    ApiError::bad_request(format!(
        "invalid native materialized output page identity, ordering or cardinality at {}",
        std::panic::Location::caller()
    ))
}

/// The two families whose runtime restore path yields a materialized delta
/// directly. The `set_ops` checkpoint binds its output under `output`, not
/// `published_output`, so the generic rule below cannot see it.
fn native_restore_family(family: &str) -> bool {
    matches!(family, "generic_set_ops_v1" | "typed_join_project_v1")
}

/// The families whose published output is a BAG. The runtime extracts the bag
/// output for these families, so their checkpoints publish the bag codec and
/// bind the family name. Every other admitted checkpoint publishes the general
/// materialized pages, which carry no family binding.
fn bag_publication_family(family: &str) -> bool {
    matches!(
        family,
        "generic_set_ops_v1" | "typed_join_project_v1" | "ordered_window" | "single_key_sum_count"
    )
}

pub(super) fn supports_checkpoint(checkpoint: &RuntimeCheckpoint) -> bool {
    let Some(state) = checkpoint.state_payload.as_ref().map(|p| &p.payload) else {
        return false;
    };
    match state.field::<String>("runtime_kind") {
        Ok(Some(family)) => {
            native_restore_family(&family)
                || (state.has_non_null_field("output_schema")
                    && state.has_non_null_field("published_output"))
        }
        Ok(None) => state.has_non_null_field("output_schema"),
        Err(_) => false,
    }
}

pub(super) fn generic_checkpoint_family(checkpoint: &RuntimeCheckpoint) -> bool {
    checkpoint
        .state_payload
        .as_ref()
        .and_then(|p| p.payload.field::<String>("runtime_kind").ok().flatten())
        .is_some_and(|family| !native_restore_family(&family))
}

fn checkpoint_index(checkpoint: &RuntimeCheckpoint) -> Result<PageIndex, ApiError> {
    if !supports_checkpoint(checkpoint) {
        return Err(invalid());
    }
    let Some(state) = checkpoint.state_payload.as_ref().map(|p| &p.payload) else {
        return Err(invalid());
    };
    let family = state
        .field::<String>("runtime_kind")
        .map_err(ApiError::bad_request)?;
    Ok(PageIndex {
        family: family.filter(|f| bag_publication_family(f)),
        schema: checkpoint_output_schema(checkpoint)?,
        page_stats: None,
    })
}

fn checkpoint_output_schema(checkpoint: &RuntimeCheckpoint) -> Result<RelationSchema, ApiError> {
    let payload = checkpoint.state_payload.as_ref().ok_or_else(invalid)?;
    payload
        .payload
        .field("output_schema")
        .map_err(ApiError::bad_request)?
        .ok_or_else(invalid)
}

/// Charge one object request against the query policy budget.
fn charge_request(policy: QueryPolicy, used: &mut usize) -> Result<(), ApiError> {
    *used = used.checked_add(1).ok_or_else(invalid)?;
    if policy.max_object_requests.is_some_and(|max| *used > max) {
        return Err(ApiError::bad_request(
            "materialized read exceeds object-request budget",
        ));
    }
    Ok(())
}

/// Find the newest checkpoint of a view when no metadata store is configured.
pub(super) async fn discover_checkpoint_head(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
    policy: QueryPolicy,
    requests: &mut usize,
) -> Result<Option<(String, StandingRuntimeCheckpointKeyParts)>, ApiError> {
    use futures::TryStreamExt;
    let prefix = ObjectPath::from(format!(
        "v1/standing-runtime-checkpoints/{}/{}/{view_id}/epochs",
        identity.tenant_id, identity.program_id,
    ));
    let mut stream = state.store.list(Some(&prefix));
    let mut latest: Option<(String, StandingRuntimeCheckpointKeyParts)> = None;
    let mut entries = 0usize;
    loop {
        // Charge each stream poll conservatively; a provider may paginate LIST.
        charge_request(policy, requests)?;
        let Some(meta) = stream.try_next().await.map_err(ApiError::internal)? else {
            break;
        };
        entries = entries.checked_add(1).ok_or_else(invalid)?;
        if entries > MAX_PAGES || meta.location.as_ref().len() > MAX_INDEX_BYTES {
            return Err(invalid());
        }
        let path = meta.location.to_string();
        if !path.ends_with(".checkpoint.arrow") {
            continue;
        }
        let (_, parts) = ObjectKey::parse_standing_runtime_checkpoint(path.clone())
            .map_err(ApiError::bad_request)?;
        if parts.tenant_id != identity.tenant_id
            || parts.program_id != identity.program_id
            || parts.view_id != view_id
        {
            return Err(invalid());
        }
        if let Some((_, previous)) = &latest {
            if parts.logical_epoch == previous.logical_epoch {
                return Err(invalid());
            }
            if parts.logical_epoch < previous.logical_epoch {
                continue;
            }
        }
        latest = Some((path, parts));
    }
    Ok(latest)
}

/// Normalize only the checkpoint's materialized output using its admitted projection.
pub(super) fn checkpoint_publication(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    key: &ObjectKey,
) -> Result<Option<StandingRuntimeOutputPublication>, ApiError> {
    checkpoint_publication_batched(checkpoint, view_id, key, PAGE_ROWS)
}

fn checkpoint_publication_batched(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    key: &ObjectKey,
    normalization_rows: usize,
) -> Result<Option<StandingRuntimeOutputPublication>, ApiError> {
    let Some(state) = checkpoint.state_payload.as_ref().map(|p| &p.payload) else {
        return Ok(None);
    };
    let Some(output) = state
        .field::<DeltaBatch>("published_output")
        .map_err(ApiError::bad_request)?
    else {
        return Ok(None);
    };
    let payload = state
        .project(&["runtime_kind", "plan", "output_schema"])
        .map_err(ApiError::bad_request)?;
    if payload.get("output_schema").is_none() {
        // Historical checkpoint-only outputs have no declared public schema.
        // Keep their existing reader rather than assigning an inferred page schema.
        return Ok(None);
    }
    let schema = checkpoint_output_schema(checkpoint)?;
    if schema.relation_id != view_id {
        return Err(invalid());
    }
    let aggregates =
        super::query_serving::standing_runtime_output_aggregate_outputs_for_checkpoint(checkpoint)?;
    let canonical = output.net_rows().map_err(ApiError::bad_request)?;
    let total = canonical.iter().try_fold(0usize, |n, row| {
        let copies = usize::try_from(row.weight)
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(invalid)?;
        n.checked_add(copies).ok_or_else(invalid)
    })?;
    let mut normalized = Vec::new();
    // Each normalization batch is bounded; the existing converter supplies aliases,
    // aggregate values and composite keys exactly as the public runtime query does.
    for chunk in canonical.chunks(normalization_rows.clamp(1, PAGE_ROWS)) {
        {
            let parts = chunk.iter().cloned().map(|mut row| {
                row.weight = 1;
                row
            });
            let page = velorix_runtime::materialized_view_runtime::materialized_delta_to_page_with_payload(
                &schema,
                &DeltaBatch::from_records(parts),
                ScopedViewId {
                    tenant_id: checkpoint.identity.tenant_id.clone(),
                    program_id: checkpoint.identity.program_id.clone(),
                    view_id: view_id.into(),
                },
                checkpoint.logical_epoch,
                SnapshotPageRequest {
                    max_rows: Some(chunk.len()),
                    ..Default::default()
                },
                aggregates.as_deref(),
                Some(&payload),
            )
            .map_err(ApiError::bad_request)?;
            if page.next_page_token.is_some() {
                return Err(invalid());
            }
            let mut converted = Vec::with_capacity(chunk.len());
            for batch in page.batches {
                for i in 0..batch.num_rows() {
                    let mut value = serde_json::Map::new();
                    for (column, array) in schema.columns.iter().zip(batch.columns()) {
                        let mut scalar = arrow_value_to_json(array, i)?;
                        if column.data_type == SqlDataType::Json && !scalar.is_null() {
                            scalar = serde_json::from_str(scalar.as_str().ok_or_else(invalid)?)
                                .map_err(ApiError::bad_request)?;
                        }
                        value.insert(column.name.clone(), scalar);
                    }
                    let row = chunk.get(converted.len()).ok_or_else(invalid)?;
                    converted.push(velorix_core::delta::DeltaRecord::new(
                        row.key.clone(),
                        velorix_core::delta::DeltaValue::from_json(Value::Object(value)),
                        row.weight,
                    ));
                }
            }
            if converted.len() != chunk.len() {
                return Err(invalid());
            }
            normalized.extend(
                DeltaBatch::from_records(converted)
                    .net_rows()
                    .map_err(ApiError::bad_request)?,
            );
        }
    }
    if total.div_ceil(PAGE_ROWS).max(1) > MAX_PAGES {
        // The durable payload retains the exact BAG after validating every compact row.
        return Ok(None);
    }
    publication(
        checkpoint,
        view_id,
        key,
        DeltaBatch::from_records(normalized),
    )
    .map(Some)
}

fn root_hash(record: &StandingRuntimeOutputManifestRecord) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(&(
        &record.output_encoding,
        record.schema_version,
        &record.record_kind,
        &record.tenant_id,
        &record.program_id,
        &record.view_id,
        &record.checkpoint_key,
        record.logical_epoch,
        &record.checkpoint_content_hash,
        &record.output_encoding,
        record.output_row_count,
        &record.source_kind,
        &record.pages,
        &record.published_output,
    ))
    .map_err(ApiError::internal)?;
    Ok(stable_bytes_hash(&bytes))
}

fn page_hash(record: &StandingRuntimeOutputPageRecord) -> Result<String, ApiError> {
    ipc_hash(record, &compact_batch(record)?)
}

const ROW_STATE_FIELD: &str = "__velorix_rows_arrow_state";

fn compact_batch(record: &StandingRuntimeOutputPageRecord) -> Result<RecordBatch, ApiError> {
    codec_spec(&record.output_encoding).ok_or_else(invalid)?;
    use arrow::array::BinaryArray;
    use arrow::datatypes::{DataType, Field, Schema};
    let payload: PageRows =
        serde_json::from_value(record.published_output.clone()).map_err(ApiError::bad_request)?;
    let schema = payload.schema.as_ref().ok_or_else(invalid)?;
    if schema.columns.iter().any(|c| c.name == ROW_STATE_FIELD) {
        return Err(invalid());
    }
    let rows = payload.rows.records();
    let single = DeltaBatch::from_records(rows.iter().map(|r| {
        let mut r = r.clone();
        r.weight = 1;
        r
    }));
    // The bag codec extracts columns from the key first, the general
    // materialized codec from the value first.
    let batch = if codec_spec(&record.output_encoding).is_some_and(|spec| spec.bag) {
        velorix_runtime::materialized_view_runtime::materialized_bag_canonical_batch(
            schema, &single,
        )
    } else {
        velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch(
            schema, &single,
        )
    }
    .map_err(ApiError::bad_request)?;
    let state = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
        &payload.rows,
        MAX_INDEX_BYTES,
    )
    .map_err(ApiError::bad_request)?;
    let mut row_states = vec![None; batch.num_rows()];
    if let Some(first) = row_states.first_mut() {
        *first = Some(state.as_bytes());
    }
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        ROW_STATE_FIELD,
        DataType::Binary,
        true,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(BinaryArray::from_iter(row_states)) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(ApiError::bad_request)
}

fn ipc_bytes(
    record: &StandingRuntimeOutputPageRecord,
    batch: &RecordBatch,
    _hashing: bool,
) -> Result<Vec<u8>, ApiError> {
    use arrow::datatypes::Schema;
    let spec = codec_spec(&record.output_encoding).ok_or_else(invalid)?;
    let mut header = record.clone();
    header.published_output["rows"] =
        serde_json::to_value(DeltaBatch::default()).map_err(ApiError::internal)?;
    // The manifest and object key bind these fields outside the raw payload digest.
    header.output_content_hash.clear();
    header.page_content_hash.clear();
    let metadata = [(
        spec.ipc_header.into(),
        serde_json::to_string(&header).map_err(ApiError::internal)?,
    )]
    .into_iter()
    .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            batch.schema().fields().clone(),
            metadata,
        )),
        batch.columns().to_vec(),
    )
    .map_err(ApiError::bad_request)?;
    let mut bytes = Vec::new();
    {
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
            .and_then(|options| options.try_with_compression_level(Some(3)))
            .map_err(ApiError::internal)?;
        let mut writer = arrow::ipc::writer::StreamWriter::try_new_with_options(
            &mut bytes,
            batch.schema().as_ref(),
            options,
        )
        .map_err(ApiError::internal)?;
        writer.write(&batch).map_err(ApiError::internal)?;
        writer.finish().map_err(ApiError::internal)?;
    }
    Ok(bytes)
}

fn ipc_hash(
    record: &StandingRuntimeOutputPageRecord,
    batch: &RecordBatch,
) -> Result<String, ApiError> {
    Ok(stable_bytes_hash(&ipc_bytes(record, batch, true)?))
}

pub(super) fn encode_page(record: &StandingRuntimeOutputPageRecord) -> Result<Vec<u8>, ApiError> {
    if record.output_encoding == GENERIC_ARROW_CODEC {
        let state = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
            &GenericPageArrowEnvelope {
                page: record.clone(),
            },
            MAX_INDEX_BYTES,
        )
        .map_err(ApiError::bad_request)?;
        return Ok(state.as_bytes().to_vec());
    }
    if codec_spec(&record.output_encoding).is_some() {
        ipc_bytes(record, &compact_batch(record)?, false)
    } else {
        Err(invalid())
    }
}

#[cfg(test)]
fn decode_ipc(bytes: &[u8]) -> Result<(StandingRuntimeOutputPageRecord, RecordBatch), ApiError> {
    decode_ipc_bounded(bytes, MAX_INDEX_BYTES)
}

fn decode_ipc_bounded(
    bytes: &[u8],
    max_expanded_bytes: usize,
) -> Result<(StandingRuntimeOutputPageRecord, RecordBatch), ApiError> {
    use arrow::array::{Array, BinaryArray};
    let compressed = validate_ipc_frames(bytes, max_expanded_bytes)?;
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(ApiError::bad_request)?;
    // The IPC header identifies the codec family; read it before the record
    // header so the expected record kind and schema version are known.
    let ipc_header = reader
        .schema()
        .metadata()
        .keys()
        .find(|key| {
            [GENERAL_CODEC_SPEC.ipc_header, BAG_CODEC_SPEC.ipc_header].contains(&key.as_str())
        })
        .cloned()
        .ok_or_else(invalid)?;
    let header = reader
        .schema()
        .metadata()
        .get(ipc_header.as_str())
        .cloned()
        .ok_or_else(invalid)?;
    let mut record: StandingRuntimeOutputPageRecord =
        serde_json::from_str(&header).map_err(ApiError::bad_request)?;
    let spec = codec_spec(&record.output_encoding).ok_or_else(invalid)?;
    if record.output_encoding != spec.codec
        || record.schema_version != spec.schema_version
        || record.record_kind != spec.page_kind
        || record.source_kind != SOURCE
        || !record.output_content_hash.is_empty()
        || !record.page_content_hash.is_empty()
        || reader.schema().metadata().len() != 1
        || !compressed
    {
        return Err(invalid());
    }
    let batch = reader
        .next()
        .transpose()
        .map_err(ApiError::bad_request)?
        .ok_or_else(invalid)?;
    if reader.next().is_some()
        || batch.num_rows() > PAGE_ROWS
        || ipc_bytes(&record, &batch, false)? != bytes
    {
        return Err(invalid());
    }
    let mut payload: PageRows =
        serde_json::from_value(record.published_output.clone()).map_err(ApiError::bad_request)?;
    if !payload.rows.records().is_empty() {
        return Err(invalid());
    }
    let schema = payload.schema.as_ref().ok_or_else(invalid)?;
    if schema.schema_fingerprint != payload.schema_fingerprint
        || schema.relation_id != record.view_id
    {
        return Err(invalid());
    }
    let output_schema = arrow_schema_from_incremental_relation_schema(schema)?;
    let n = schema.columns.len();
    if batch.num_columns() < n + 1 {
        return Err(invalid());
    }
    if batch.schema().fields()[..n] != output_schema.fields()[..] {
        return Err(invalid());
    }
    if batch.num_columns() != n + 1 || batch.schema().field(n).name() != ROW_STATE_FIELD {
        return Err(invalid());
    }
    let row_states = batch
        .column(n)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(invalid)?;
    if batch.num_rows() == 0 {
        payload.rows = DeltaBatch::default();
    } else {
        if row_states.is_null(0) || (1..batch.num_rows()).any(|i| !row_states.is_null(i)) {
            return Err(invalid());
        }
        let state = velorix_core::arrow_state::ArrowState::from_ipc(
            row_states.value(0),
            max_expanded_bytes,
        )
        .map_err(ApiError::bad_request)?;
        payload.rows =
            velorix_core::arrow_state::from_state(&state).map_err(ApiError::bad_request)?;
    }
    if canonical_count(&payload.rows)? != record.row_count {
        return Err(invalid());
    }
    record.published_output = serde_json::to_value(payload).map_err(ApiError::internal)?;
    // Proof rows and SQL columns must describe the same BAG, including nulls and
    // scalar normalization. Rebuild only for validation, never for SQL execution.
    let expected = compact_batch(&record)?;
    if expected.schema().fields() != batch.schema().fields()
        || expected.columns() != batch.columns()
    {
        return Err(invalid());
    }
    record.page_content_hash = stable_bytes_hash(bytes);
    let typed = RecordBatch::try_new(output_schema, batch.columns()[..n].to_vec())
        .map_err(ApiError::bad_request)?;
    Ok((record, typed))
}

fn validate_ipc_frames(mut bytes: &[u8], limit: usize) -> Result<bool, ApiError> {
    if bytes.len() > limit.saturating_mul(2).saturating_add(16 * 1024) {
        return Err(invalid());
    }
    let mut messages = 0usize;
    let mut expanded = 0usize;
    let mut compressed = false;
    loop {
        let prefix = bytes.get(..8).ok_or_else(invalid)?;
        if prefix[..4] != [255; 4] {
            return Err(invalid());
        }
        let length = u32::from_le_bytes(prefix[4..8].try_into().map_err(|_| invalid())?) as usize;
        bytes = &bytes[8..];
        if length == 0 {
            return if bytes.is_empty() && messages == 2 {
                Ok(compressed)
            } else {
                Err(invalid())
            };
        }
        let metadata = bytes.get(..length).ok_or_else(invalid)?;
        let message = arrow::ipc::root_as_message(metadata).map_err(ApiError::bad_request)?;
        if messages == 0 {
            if message.header_as_schema().is_none() || message.bodyLength() != 0 {
                return Err(invalid());
            }
        } else if messages == 1 {
            let batch = message.header_as_record_batch().ok_or_else(invalid)?;
            if batch.length() < 0
                || batch.length() > PAGE_ROWS as i64
                || batch.compression().is_some_and(|compression| {
                    compression.codec() != arrow::ipc::CompressionType::ZSTD
                        || compression.method() != arrow::ipc::BodyCompressionMethod::BUFFER
                })
            {
                return Err(invalid());
            }
            compressed = batch.compression().is_some();
            let body_len = usize::try_from(message.bodyLength()).map_err(ApiError::bad_request)?;
            let body = bytes
                .get(length..length.checked_add(body_len).ok_or_else(invalid)?)
                .ok_or_else(invalid)?;
            for buffer in batch.buffers().ok_or_else(invalid)? {
                let start = usize::try_from(buffer.offset()).map_err(ApiError::bad_request)?;
                let size = usize::try_from(buffer.length()).map_err(ApiError::bad_request)?;
                let frame = body
                    .get(start..start.checked_add(size).ok_or_else(invalid)?)
                    .ok_or_else(invalid)?;
                if size == 0 {
                    continue;
                }
                let size = if batch.compression().is_some() {
                    let expanded_size = i64::from_le_bytes(
                        frame
                            .get(..8)
                            .ok_or_else(invalid)?
                            .try_into()
                            .map_err(|_| invalid())?,
                    );
                    if expanded_size == -1 {
                        size - 8
                    } else {
                        usize::try_from(expanded_size).map_err(ApiError::bad_request)?
                    }
                } else {
                    size
                };
                expanded = expanded
                    .checked_add(size)
                    .filter(|expanded| *expanded <= limit)
                    .ok_or_else(invalid)?;
            }
        } else {
            return Err(invalid());
        }
        let body = usize::try_from(message.bodyLength()).map_err(ApiError::bad_request)?;
        let consumed = length.checked_add(body).ok_or_else(invalid)?;
        bytes = bytes.get(consumed..).ok_or_else(invalid)?;
        messages += 1;
    }
}

pub(super) fn decode_page(
    bytes: &[u8],
    codec: &str,
) -> Result<StandingRuntimeOutputPageRecord, ApiError> {
    decode_page_bounded(bytes, codec, MAX_INDEX_BYTES)
}

fn decode_page_bounded(
    bytes: &[u8],
    codec: &str,
    max_expanded_bytes: usize,
) -> Result<StandingRuntimeOutputPageRecord, ApiError> {
    if codec == GENERIC_ARROW_CODEC {
        let state = velorix_core::arrow_state::ArrowState::from_ipc(bytes, max_expanded_bytes)
            .map_err(ApiError::bad_request)?;
        let envelope = state
            .field::<StandingRuntimeOutputPageRecord>("page")
            .map_err(ApiError::bad_request)?
            .ok_or_else(invalid)?;
        let canonical = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
            &GenericPageArrowEnvelope {
                page: envelope.clone(),
            },
            max_expanded_bytes,
        )
        .map_err(ApiError::bad_request)?;
        if canonical.as_bytes() != bytes {
            return Err(invalid());
        }
        return Ok(envelope);
    }
    if codec_spec(codec).is_some() {
        decode_ipc_bounded(bytes, max_expanded_bytes).map(|(record, _)| record)
    } else {
        Err(invalid())
    }
}

fn decode_query_page(
    bytes: &[u8],
    manifest: &StandingRuntimeOutputManifestRecord,
    reference: &StandingRuntimeOutputPageRef,
    max_expanded_bytes: usize,
) -> Result<(StandingRuntimeOutputPageRecord, Option<RecordBatch>), ApiError> {
    let (record, batch) = if reference.output_encoding == GENERIC_ARROW_CODEC {
        (
            decode_page_bounded(bytes, &reference.output_encoding, max_expanded_bytes)?,
            None,
        )
    } else if codec_spec(&reference.output_encoding).is_some() {
        let (r, b) = decode_ipc_bounded(bytes, max_expanded_bytes)?;
        (r, Some(b))
    } else {
        (decode_page(bytes, &reference.output_encoding)?, None)
    };
    let (key, parts) = ObjectKey::parse_standing_runtime_output_page(&reference.page_key)
        .map_err(ApiError::bad_request)?;
    if batch.is_none() {
        super::checkpoint_publication::validate_standing_runtime_output_page_record(&key, &record)?;
    }
    if record.page_index != reference.page_index
        || record.page_content_hash != reference.page_content_hash
        || record.row_count != reference.row_count
        || record.output_encoding != reference.output_encoding
        || parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.page_index != record.page_index
        || parts.page_content_hash != record.page_content_hash
    {
        return Err(invalid());
    }
    validate_page_binding_metadata(manifest, &record)?;
    Ok((record, batch))
}

async fn read_query_page(
    state: &ApiState,
    manifest: &StandingRuntimeOutputManifestRecord,
    reference: &StandingRuntimeOutputPageRef,
    max_encoded_bytes: Option<u64>,
    max_expanded_bytes: usize,
) -> Result<(StandingRuntimeOutputPageRecord, Option<RecordBatch>, u64), ApiError> {
    let max_expanded_bytes = max_expanded_bytes.min(state.max_standing_runtime_expansion_bytes);
    read_query_page_with_decoder(state, manifest, reference, max_encoded_bytes, |bytes| {
        decode_query_page(bytes, manifest, reference, max_expanded_bytes)
    })
    .await
}

async fn read_query_page_with_decoder(
    state: &ApiState,
    manifest: &StandingRuntimeOutputManifestRecord,
    reference: &StandingRuntimeOutputPageRef,
    max_bytes: Option<u64>,
    decode: impl Fn(&[u8]) -> Result<(StandingRuntimeOutputPageRecord, Option<RecordBatch>), ApiError>,
) -> Result<(StandingRuntimeOutputPageRecord, Option<RecordBatch>, u64), ApiError> {
    // Per-read handoff only: cached bytes still undergo all proof/hash/stat checks.
    let decoded = std::sync::Mutex::new(None);
    let validate = |bytes: &[u8]| {
        if max_bytes.is_some_and(|max| bytes.len() as u64 > max) {
            return Err(ApiError::bad_request(
                "native cached page exceeds query byte budget",
            ));
        }
        let page = decode(bytes)?;
        *decoded.lock().map_err(|_| invalid())? = Some(page);
        Ok(())
    };
    let path = ObjectPath::from(reference.page_key.clone());
    let fetch =
        || super::checkpoint_publication::read_object_bytes_bounded(state, &path, max_bytes);
    let bytes = if let Some(cache) = &state.materialized_read_cache {
        let key = super::materialized_read_cache::MaterializedReadCacheKey {
            store_id: state.materialized_read_cache_store_id().into(),
            namespace: state.materialized_read_cache_namespace().into(),
            tenant_id: manifest.tenant_id.clone(),
            program_id: manifest.program_id.clone(),
            output_id: manifest.view_id.clone(),
            object_path: reference.page_key.clone(),
            content_hash: reference.page_content_hash.clone(),
            codec: reference.output_encoding.clone(),
            root_hash: manifest.output_content_hash.clone(),
            checkpoint_epoch: manifest.logical_epoch,
        };
        cache.read_through(key, fetch, validate).await?
    } else {
        let bytes = fetch().await?;
        validate(&bytes)?;
        bytes
    };
    let (record, batch) = decoded
        .into_inner()
        .map_err(|_| invalid())?
        .ok_or_else(invalid)?;
    Ok((record, batch, bytes.len() as u64))
}

async fn read_query_metadata(
    state: &ApiState,
    key: super::materialized_read_cache::MaterializedReadCacheKey,
    max_bytes: Option<u64>,
    validator: impl Fn(&[u8]) -> Result<(), ApiError>,
) -> Result<Vec<u8>, ApiError> {
    let path = ObjectPath::from(key.object_path.clone());
    let fetch =
        || super::checkpoint_publication::read_object_bytes_bounded(state, &path, max_bytes);
    let validate = |bytes: &[u8]| {
        if max_bytes.is_some_and(|max| bytes.len() as u64 > max) {
            return Err(ApiError::bad_request(
                "native metadata exceeds query byte budget",
            ));
        }
        validator(bytes)
    };
    if let Some(cache) = &state.materialized_read_cache {
        cache.read_through(key, fetch, validate).await
    } else {
        let bytes = fetch().await?;
        validate(&bytes)?;
        Ok(bytes)
    }
}

fn canonical_count(rows: &DeltaBatch) -> Result<usize, ApiError> {
    let mut previous = None;
    let mut count = 0usize;
    for row in rows.records() {
        let key = encode_kv_ordered(row.key.as_json(), row.value.as_json());
        if previous.as_ref().is_some_and(|last| last >= &key) {
            return Err(invalid());
        }
        previous = Some(key);
        let weight = usize::try_from(row.weight)
            .ok()
            .filter(|w| *w > 0)
            .ok_or_else(invalid)?;
        count = count
            .checked_add(weight)
            .filter(|n| *n <= PAGE_ROWS)
            .ok_or_else(invalid)?;
    }
    Ok(count)
}

fn scalar(value: &Value, data_type: &SqlDataType) -> Option<PageScalar> {
    match data_type {
        SqlDataType::Utf8 => value.as_str().map(|s| PageScalar::Utf8(s.into())),
        SqlDataType::Bool => value.as_bool().map(PageScalar::Bool),
        SqlDataType::Int8
        | SqlDataType::Int16
        | SqlDataType::Int32
        | SqlDataType::Int64
        | SqlDataType::UInt8
        | SqlDataType::UInt16
        | SqlDataType::UInt32 => value.as_i64().map(PageScalar::Int),
        SqlDataType::Float64 => value
            .as_f64()
            .filter(|n| n.is_finite())
            .map(PageScalar::Float),
        SqlDataType::Timestamp { timezone: None } => value.as_i64().map(PageScalar::Timestamp),
        // shortcut: timezone, decimals, UInt64 and Float32 bounds remain unknown until exact SQL coercions are verified.
        _ => None,
    }
}

fn compare(a: &PageScalar, b: &PageScalar) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (PageScalar::Utf8(a), PageScalar::Utf8(b)) => a.partial_cmp(b),
        (PageScalar::Bool(a), PageScalar::Bool(b)) => a.partial_cmp(b),
        (PageScalar::Int(a), PageScalar::Int(b)) => a.partial_cmp(b),
        (PageScalar::Timestamp(a), PageScalar::Timestamp(b)) => a.partial_cmp(b),
        (PageScalar::Float(a), PageScalar::Float(b)) if a.is_finite() && b.is_finite() => {
            a.partial_cmp(b)
        }
        _ => None,
    }
}

fn page_statistics(
    schema: &RelationSchema,
    rows: &DeltaBatch,
) -> Result<Vec<ColumnStats>, ApiError> {
    schema
        .columns
        .iter()
        .map(|column| {
            let mut stats = ColumnStats {
                nulls: 0,
                min: None,
                max: None,
            };
            let mut unknown = false;
            for row in rows.records() {
                let value = row
                    .key
                    .as_json()
                    .as_object()
                    .and_then(|k| k.get(&column.name))
                    .or_else(|| {
                        (schema.primary_key == [column.name.clone()]).then(|| row.key.as_json())
                    })
                    .or_else(|| {
                        row.value
                            .as_json()
                            .as_object()
                            .and_then(|v| v.get(&column.name))
                    })
                    .ok_or_else(invalid)?;
                if value.is_null() {
                    stats.nulls = stats
                        .nulls
                        .checked_add(usize::try_from(row.weight).map_err(ApiError::bad_request)?)
                        .ok_or_else(invalid)?;
                } else if let Some(value) = scalar(value, &column.data_type) {
                    if stats
                        .min
                        .as_ref()
                        .is_none_or(|m| compare(&value, m) == Some(std::cmp::Ordering::Less))
                    {
                        stats.min = Some(value.clone());
                    }
                    if stats
                        .max
                        .as_ref()
                        .is_none_or(|m| compare(&value, m) == Some(std::cmp::Ordering::Greater))
                    {
                        stats.max = Some(value);
                    }
                } else {
                    unknown = true;
                }
            }
            if unknown {
                stats.min = None;
                stats.max = None;
            }
            Ok(stats)
        })
        .collect()
}

fn may_match(
    predicate: &PagePredicate,
    schema: &RelationSchema,
    stats: &[ColumnStats],
    count: usize,
) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match predicate {
        PagePredicate::Unknown => true,
        PagePredicate::And(a, b) => {
            may_match(a, schema, stats, count) && may_match(b, schema, stats, count)
        }
        PagePredicate::Or(a, b) => {
            may_match(a, schema, stats, count) || may_match(b, schema, stats, count)
        }
        PagePredicate::Null { column, negated } => schema
            .columns
            .iter()
            .position(|c| c.name == *column)
            .and_then(|i| stats.get(i))
            .is_none_or(|s| {
                if *negated {
                    s.nulls < count
                } else {
                    s.nulls > 0
                }
            }),
        PagePredicate::Compare { column, op, value } => {
            let Some(s) = schema
                .columns
                .iter()
                .position(|c| c.name == *column)
                .and_then(|i| stats.get(i))
            else {
                return true;
            };
            if s.nulls == count {
                return false;
            }
            let Some((lo, hi)) = s
                .min
                .as_ref()
                .zip(s.max.as_ref())
                .and_then(|(a, b)| compare(a, value).zip(compare(b, value)))
            else {
                return true;
            };
            match op {
                PageComparison::Eq => lo != Greater && hi != Less,
                PageComparison::Ne => lo != Equal || hi != Equal,
                PageComparison::Lt => lo == Less,
                PageComparison::Le => lo != Greater,
                PageComparison::Gt => hi == Greater,
                PageComparison::Ge => hi != Less,
            }
        }
    }
}

pub(super) fn publication(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    checkpoint_key: &ObjectKey,
    output: DeltaBatch,
) -> Result<StandingRuntimeOutputPublication, ApiError> {
    let mut index = checkpoint_index(checkpoint)?;
    if index.schema.relation_id != view_id {
        return Err(invalid());
    }
    // A bag checkpoint publishes bag pages; every other admitted checkpoint
    // publishes the general materialized pages.
    let spec = if index.family.is_some() {
        &BAG_CODEC_SPEC
    } else {
        &GENERAL_CODEC_SPEC
    };
    let canonical = output.net_rows().map_err(ApiError::bad_request)?;
    let total = canonical.iter().try_fold(0usize, |total, row| {
        let weight = usize::try_from(row.weight)
            .ok()
            .filter(|w| *w > 0)
            .ok_or_else(invalid)?;
        total
            .checked_add(weight)
            .ok_or_else(|| ApiError::bad_request("native output cardinality overflow"))
    })?;
    if total.div_ceil(PAGE_ROWS).max(1) > MAX_PAGES {
        return Err(ApiError::bad_request(
            "native output exceeds bounded materialized page-index capacity",
        ));
    }
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut used = 0usize;
    for row in canonical {
        let mut remaining = usize::try_from(row.weight)
            .ok()
            .filter(|w| *w > 0)
            .ok_or_else(invalid)?;
        while remaining > 0 {
            let take = remaining.min(PAGE_ROWS - used);
            let mut part = row.clone();
            part.weight = i64::try_from(take).map_err(ApiError::bad_request)?;
            chunk.push(part);
            used += take;
            remaining -= take;
            if used == PAGE_ROWS {
                chunks.push(DeltaBatch::from_records(std::mem::take(&mut chunk)));
                used = 0;
            }
        }
    }
    if !chunk.is_empty() || chunks.is_empty() {
        chunks.push(DeltaBatch::from_records(chunk));
    }
    let mut pages = Vec::new();
    let mut page_records = Vec::new();
    index.page_stats = Some(Vec::new());
    for (ordinal, rows) in chunks.into_iter().enumerate() {
        let page_index = u32::try_from(ordinal).map_err(ApiError::bad_request)?;
        let row_count = canonical_count(&rows)?;
        index
            .page_stats
            .as_mut()
            .unwrap()
            .push(page_statistics(&index.schema, &rows)?);
        let mut page = StandingRuntimeOutputPageRecord {
            schema_version: spec.schema_version,
            record_kind: spec.page_kind.into(),
            tenant_id: checkpoint.identity.tenant_id.clone(),
            program_id: checkpoint.identity.program_id.clone(),
            view_id: view_id.into(),
            logical_epoch: checkpoint.logical_epoch,
            output_content_hash: String::new(),
            page_index,
            page_content_hash: String::new(),
            row_count,
            output_encoding: spec.codec.into(),
            source_kind: SOURCE.into(),
            published_output: serde_json::to_value(PageRows {
                family: index.family.clone(),
                schema_fingerprint: index.schema.schema_fingerprint.clone(),
                checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
                rows,
                schema: Some(index.schema.clone()),
            })
            .map_err(ApiError::internal)?,
        };
        page.page_content_hash = page_hash(&page)?;
        let key = ObjectKey::standing_runtime_output_page(
            &page.tenant_id,
            &page.program_id,
            view_id,
            page.logical_epoch,
            page_index,
            &page.page_content_hash,
        )
        .map_err(ApiError::bad_request)?;
        pages.push(StandingRuntimeOutputPageRef {
            page_index,
            page_key: key.as_str().into(),
            page_content_hash: page.page_content_hash.clone(),
            row_count,
            output_encoding: spec.codec.into(),
        });
        page_records.push((key, page));
    }
    let mut manifest = StandingRuntimeOutputManifestRecord {
        schema_version: spec.schema_version,
        record_kind: spec.manifest_kind.into(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.into(),
        checkpoint_key: checkpoint_key.as_str().into(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        output_content_hash: String::new(),
        output_encoding: spec.codec.into(),
        output_row_count: total,
        source_kind: SOURCE.into(),
        pages,
        published_output: serde_json::to_value(index).map_err(ApiError::internal)?,
    };
    manifest.output_content_hash = root_hash(&manifest)?;
    for (_, page) in &mut page_records {
        page.output_content_hash = manifest.output_content_hash.clone();
    }
    let manifest_key = ObjectKey::standing_runtime_output_manifest(
        &manifest.tenant_id,
        &manifest.program_id,
        view_id,
        manifest.logical_epoch,
        &manifest.output_content_hash,
    )
    .map_err(ApiError::bad_request)?;
    validate_manifest(&manifest_key, &manifest)?;
    Ok(StandingRuntimeOutputPublication {
        manifest_key,
        manifest_record: manifest,
        page_records,
    })
}

pub(super) fn validate_manifest(
    key: &ObjectKey,
    manifest: &StandingRuntimeOutputManifestRecord,
) -> Result<(), ApiError> {
    let index: PageIndex =
        serde_json::from_value(manifest.published_output.clone()).map_err(ApiError::bad_request)?;
    let (_, parts) = ObjectKey::parse_standing_runtime_output_manifest(key.as_str())
        .map_err(ApiError::bad_request)?;
    let spec = codec_spec(&manifest.output_encoding).ok_or_else(invalid)?;
    if manifest.schema_version != spec.schema_version
        || manifest.record_kind != spec.manifest_kind
        || manifest.source_kind != SOURCE
        || index
            .family
            .as_deref()
            .is_some_and(|family| family.is_empty())
        || spec.bag != index.family.is_some()
        || index.schema.relation_id != manifest.view_id
        || index.schema.schema_fingerprint.is_empty()
        || parts.tenant_id != manifest.tenant_id
        || parts.program_id != manifest.program_id
        || parts.view_id != manifest.view_id
        || parts.logical_epoch != manifest.logical_epoch
        || parts.output_content_hash != manifest.output_content_hash
        || root_hash(manifest)? != manifest.output_content_hash
        || manifest.pages.is_empty()
        || manifest.pages.len() > MAX_PAGES
        || serde_json::to_vec(manifest)
            .map_err(ApiError::internal)?
            .len()
            > MAX_INDEX_BYTES
    {
        return Err(invalid());
    }
    let mut total = 0usize;
    for (ordinal, page) in manifest.pages.iter().enumerate() {
        let (_, parts) = ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
            .map_err(ApiError::bad_request)?;
        if usize::try_from(page.page_index).map_err(ApiError::bad_request)? != ordinal
            || page.output_encoding != manifest.output_encoding
            || parts.tenant_id != manifest.tenant_id
            || parts.program_id != manifest.program_id
            || parts.view_id != manifest.view_id
            || parts.logical_epoch != manifest.logical_epoch
            || parts.page_index != page.page_index
            || parts.page_content_hash != page.page_content_hash
            || page.row_count > PAGE_ROWS
            || (ordinal + 1 < manifest.pages.len() && page.row_count != PAGE_ROWS)
            || (page.row_count == 0
                && (manifest.pages.len() != 1 || manifest.output_row_count != 0))
        {
            return Err(invalid());
        }
        total = total.checked_add(page.row_count).ok_or_else(invalid)?;
    }
    if total != manifest.output_row_count {
        return Err(invalid());
    }
    if let Some(pages) = &index.page_stats {
        if pages.len() != manifest.pages.len() {
            return Err(invalid());
        }
        for (stats, page) in pages.iter().zip(&manifest.pages) {
            if stats.len() != index.schema.columns.len() {
                return Err(invalid());
            }
            for (s, c) in stats.iter().zip(&index.schema.columns) {
                if s.nulls > page.row_count
                    || (!c.nullable && s.nulls != 0)
                    || s.min.is_some() != s.max.is_some()
                {
                    return Err(invalid());
                }
                if let Some((min, max)) = s.min.as_ref().zip(s.max.as_ref()) {
                    if s.nulls == page.row_count
                        || !matches!(
                            compare(min, max),
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                        )
                    {
                        return Err(invalid());
                    }
                    let valid_type = matches!(
                        (&c.data_type, min, max),
                        (SqlDataType::Utf8, PageScalar::Utf8(_), PageScalar::Utf8(_))
                            | (SqlDataType::Bool, PageScalar::Bool(_), PageScalar::Bool(_))
                            | (
                                SqlDataType::Float64,
                                PageScalar::Float(_),
                                PageScalar::Float(_)
                            )
                            | (
                                SqlDataType::Timestamp { timezone: None },
                                PageScalar::Timestamp(_),
                                PageScalar::Timestamp(_),
                            )
                            | (
                                SqlDataType::Int8
                                    | SqlDataType::Int16
                                    | SqlDataType::Int32
                                    | SqlDataType::Int64
                                    | SqlDataType::UInt8
                                    | SqlDataType::UInt16
                                    | SqlDataType::UInt32,
                                PageScalar::Int(_),
                                PageScalar::Int(_),
                            )
                    );
                    if !valid_type {
                        return Err(invalid());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Test publication without optional statistics using the current page codec.
#[cfg(test)]
pub(super) fn publication_without_stats(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    checkpoint_key: &ObjectKey,
    output: DeltaBatch,
) -> Result<StandingRuntimeOutputPublication, ApiError> {
    let mut p = publication(checkpoint, view_id, checkpoint_key, output)?;
    p.manifest_record
        .published_output
        .as_object_mut()
        .ok_or_else(invalid)?
        .remove("page_stats");
    p.manifest_record.output_content_hash = root_hash(&p.manifest_record)?;
    p.manifest_key = ObjectKey::standing_runtime_output_manifest(
        &p.manifest_record.tenant_id,
        &p.manifest_record.program_id,
        &p.manifest_record.view_id,
        p.manifest_record.logical_epoch,
        &p.manifest_record.output_content_hash,
    )
    .map_err(ApiError::bad_request)?;
    for (_, page) in &mut p.page_records {
        page.output_content_hash = p.manifest_record.output_content_hash.clone();
    }
    validate_manifest(&p.manifest_key, &p.manifest_record)?;
    Ok(p)
}

pub(super) fn validate_page(
    key: &ObjectKey,
    page: &StandingRuntimeOutputPageRecord,
) -> Result<(), ApiError> {
    let payload: PageRows =
        serde_json::from_value(page.published_output.clone()).map_err(ApiError::bad_request)?;
    let (_, parts) = ObjectKey::parse_standing_runtime_output_page(key.as_str())
        .map_err(ApiError::bad_request)?;
    let spec = codec_spec(&page.output_encoding).ok_or_else(invalid)?;
    if page.schema_version != spec.schema_version
        || page.record_kind != spec.page_kind
        || page.source_kind != SOURCE
        || payload
            .family
            .as_deref()
            .is_some_and(|family| family.is_empty())
        || spec.bag != payload.family.is_some()
        || payload.schema_fingerprint.is_empty()
        || parts.tenant_id != page.tenant_id
        || parts.program_id != page.program_id
        || parts.view_id != page.view_id
        || parts.logical_epoch != page.logical_epoch
        || parts.page_index != page.page_index
        || parts.page_content_hash != page.page_content_hash
        || page_hash(page)? != page.page_content_hash
        || canonical_count(&payload.rows)? != page.row_count
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn validate_page_binding(
    manifest: &StandingRuntimeOutputManifestRecord,
    page: &StandingRuntimeOutputPageRecord,
) -> Result<DeltaBatch, ApiError> {
    let rows = validate_page_binding_metadata(manifest, page)?;
    let index: PageIndex =
        serde_json::from_value(manifest.published_output.clone()).map_err(ApiError::bad_request)?;
    if codec_spec(&page.output_encoding).is_some_and(|spec| spec.bag) {
        velorix_runtime::materialized_view_runtime::materialized_bag_canonical_batch(
            &index.schema,
            &rows,
        )
        .map_err(ApiError::bad_request)?;
    } else {
        velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch(
            &index.schema,
            &rows,
        )
        .map_err(ApiError::bad_request)?;
    }
    Ok(rows)
}

fn validate_page_binding_metadata(
    manifest: &StandingRuntimeOutputManifestRecord,
    page: &StandingRuntimeOutputPageRecord,
) -> Result<DeltaBatch, ApiError> {
    let index: PageIndex =
        serde_json::from_value(manifest.published_output.clone()).map_err(ApiError::bad_request)?;
    let payload: PageRows =
        serde_json::from_value(page.published_output.clone()).map_err(ApiError::bad_request)?;
    // Only the Arrow codecs carry a decoded page root and the inlined schema;
    // the JSON codecs keep the whole payload in the record and bind the root
    // hash directly.
    let arrow = codec_spec(&page.output_encoding).is_some();
    if page.tenant_id != manifest.tenant_id
        || page.program_id != manifest.program_id
        || page.view_id != manifest.view_id
        || page.logical_epoch != manifest.logical_epoch
        || (page.output_content_hash != manifest.output_content_hash
            && !(arrow && page.output_content_hash.is_empty()))
        || payload.family != index.family
        || payload.schema_fingerprint != index.schema.schema_fingerprint
        || payload.checkpoint_content_hash != manifest.checkpoint_content_hash
        || (arrow && payload.schema.as_ref() != Some(&index.schema))
    {
        return Err(invalid());
    }
    if let Some(pages) = &index.page_stats {
        if pages.get(page.page_index as usize)
            != Some(&page_statistics(&index.schema, &payload.rows)?)
        {
            return Err(invalid());
        }
    }
    Ok(payload.rows)
}

pub(super) fn validate_checkpoint_binding(
    manifest: &StandingRuntimeOutputManifestRecord,
    checkpoint: &RuntimeCheckpoint,
) -> Result<(), ApiError> {
    // Some readers pass the lightweight pointer record without a hydrated state
    // payload. Their caller has already bound the manifest to the checkpoint
    // key, epoch, and content hash, so there is nothing left to compare here.
    let Some(_) = checkpoint.state_payload.as_ref() else {
        return Ok(());
    };
    let expected = checkpoint_index(checkpoint)?;
    let actual: PageIndex =
        serde_json::from_value(manifest.published_output.clone()).map_err(ApiError::bad_request)?;
    if expected.family != actual.family || expected.schema != actual.schema {
        return Err(invalid());
    }
    Ok(())
}

/// Readers pick the family explicitly only for the two legacy families. Every
/// other execution defers to the same checkpoint-bound extraction rule used for
/// publication; the manifest is validated against that checkpoint before any
/// page is decoded, so an ineligible (legacy JSON) view still falls back.
fn materialized_bag_family(active: &ActiveMaterializedView) -> Option<&'static str> {
    match &active.runtime.as_ref()?.logical_plan.as_ref()?.execution {
        VelorixLogicalViewExecutionV1::TypedMap { .. }
        | VelorixLogicalViewExecutionV1::Series { .. }
        | VelorixLogicalViewExecutionV1::UnionAll { .. } => Some("generic_set_ops_v1"),
        VelorixLogicalViewExecutionV1::TypedJoinProject { .. } => Some("typed_join_project_v1"),
        _ => Some(GENERIC_FAMILY),
    }
}

/// Sentinel for a family whose eligibility is decided by the checkpoint rather
/// than by the registered plan. Never equal to a real runtime kind.
const GENERIC_FAMILY: &str = "";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageToken {
    codec: String,
    tenant: String,
    program: String,
    output: String,
    schema: String,
    root: String,
    epoch: u64,
    offset: usize,
}

pub(super) async fn query_with_policy(
    state: &ApiState,
    active: &ActiveMaterializedView,
    identity: &StandingProgramIdentity,
    output_id: &str,
    request: SnapshotPageRequest,
    policy: QueryPolicy,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    let Some(family) = materialized_bag_family(active) else {
        return Ok(None);
    };
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(invalid)?;
    let query = query_bound_selected_policy(
        state,
        &active.spec.view_id,
        identity,
        output_id,
        family,
        schema,
        request,
        None,
        policy,
    );
    match policy.execution_timeout_ms {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), query)
            .await
            .map_err(|_| ApiError::bad_request("native read exceeded query execution timeout"))?,
        None => query.await,
    }
}

pub(super) async fn query_filtered(
    state: &ApiState,
    active: &ActiveMaterializedView,
    identity: &StandingProgramIdentity,
    output_id: &str,
    request: SnapshotPageRequest,
    predicate: &PagePredicate,
    policy: QueryPolicy,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    let Some(family) = materialized_bag_family(active) else {
        return Ok(None);
    };
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|s| s.relation_id == output_id)
        .ok_or_else(invalid)?;
    let query = query_bound_selected(
        state,
        &active.spec.view_id,
        identity,
        output_id,
        family,
        schema,
        request,
        Some((predicate, policy)),
    );
    match policy.execution_timeout_ms {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), query)
            .await
            .map_err(|_| {
                ApiError::bad_request("native filtered read exceeded query execution timeout")
            })?,
        None => query.await,
    }
}

#[cfg(test)]
async fn query_bound(
    state: &ApiState,
    view_id: &str,
    identity: &StandingProgramIdentity,
    output_id: &str,
    family: &str,
    schema: &RelationSchema,
    request: SnapshotPageRequest,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    query_bound_selected(
        state, view_id, identity, output_id, family, schema, request, None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn query_bound_selected(
    state: &ApiState,
    view_id: &str,
    identity: &StandingProgramIdentity,
    output_id: &str,
    family: &str,
    schema: &RelationSchema,
    request: SnapshotPageRequest,
    filter: Option<(&PagePredicate, QueryPolicy)>,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    query_bound_selected_policy(
        state,
        view_id,
        identity,
        output_id,
        family,
        schema,
        request,
        filter,
        filter.map(|(_, policy)| policy).unwrap_or_default(),
    )
    .await
}

pub(super) async fn query_filtered_record(
    state: &ApiState,
    active: &ActiveMaterializedView,
    record: &StandingRuntimeCheckpointRecord,
    output_id: &str,
    predicate: Option<&PagePredicate>,
    row_range: Option<(usize, usize)>,
    policy: QueryPolicy,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    let Some(family) = materialized_bag_family(active) else {
        return Ok(None);
    };
    let identity = active_standing_runtime_identity(active).ok_or_else(invalid)?;
    if record.checkpoint.identity != *identity
        || record.view_id != active.spec.view_id
        || record.manifest_hash.is_empty()
    {
        return Err(invalid());
    }
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(invalid)?;
    let pointer = standing_runtime_checkpoint_pointer_from_record(record);
    let query = query_bound_selected_policy_at_pointer(
        state,
        &active.spec.view_id,
        identity,
        output_id,
        family,
        schema,
        SnapshotPageRequest {
            committed_epoch: Some(record.checkpoint.logical_epoch),
            page_token: None,
            max_rows: row_range.map(|(_, limit)| limit),
        },
        predicate.map(|predicate| (predicate, policy)),
        policy,
        Some(pointer),
        row_range.map(|(offset, _)| offset),
    );
    match policy.execution_timeout_ms {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), query)
            .await
            .map_err(|_| {
                ApiError::bad_request("native filtered read exceeded query execution timeout")
            })?,
        None => query.await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn query_bound_selected_policy(
    state: &ApiState,
    view_id: &str,
    identity: &StandingProgramIdentity,
    output_id: &str,
    family: &str,
    schema: &RelationSchema,
    request: SnapshotPageRequest,
    filter: Option<(&PagePredicate, QueryPolicy)>,
    policy: QueryPolicy,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    query_bound_selected_policy_at_pointer(
        state, view_id, identity, output_id, family, schema, request, filter, policy, None, None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn query_bound_selected_policy_at_pointer(
    state: &ApiState,
    view_id: &str,
    identity: &StandingProgramIdentity,
    output_id: &str,
    family: &str,
    schema: &RelationSchema,
    request: SnapshotPageRequest,
    filter: Option<(&PagePredicate, QueryPolicy)>,
    policy: QueryPolicy,
    captured_pointer: Option<StandingRuntimeCheckpointPointer>,
    selected_row_offset: Option<usize>,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    let pinned = captured_pointer.is_some();
    let mut discovery_requests = 0;
    let mut discovered_bytes = None;
    let pointer = match captured_pointer {
        Some(pointer) => Some(pointer),
        None => match &state.meta_store {
            Some(meta) => meta
                .read_standing_runtime_checkpoint(
                    &identity.tenant_id,
                    &identity.program_id,
                    view_id,
                )
                .await
                .map_err(meta_error_to_api)?,
            None => {
                let Some((path, _)) = discover_checkpoint_head(
                    state,
                    identity,
                    view_id,
                    policy,
                    &mut discovery_requests,
                )
                .await?
                else {
                    return Ok(None);
                };
                charge_request(policy, &mut discovery_requests)?;
                let budget = policy
                    .max_scan_bytes
                    .unwrap_or(u64::MAX)
                    .min(policy.memory_limit_bytes.unwrap_or(u64::MAX) / 16)
                    .min(16 * 1024 * 1024);
                let bytes = super::checkpoint_publication::read_object_bytes_bounded(
                    state,
                    &ObjectPath::from(path.clone()),
                    Some(budget),
                )
                .await?;
                let mut record = standing_runtime_checkpoint_record_from_slice(&bytes)
                    .map_err(ApiError::bad_request)?;
                if record.checkpoint_key.is_empty() {
                    record.checkpoint_key = path.clone();
                }
                if record.checkpoint_key != path {
                    return Err(invalid());
                }
                let pointer = standing_runtime_checkpoint_pointer_from_record(&record);
                validate_standing_runtime_checkpoint_record(identity, view_id, &pointer, &record)?;
                discovered_bytes = Some(bytes);
                Some(pointer)
            }
        },
    };
    let Some(pointer) = pointer else {
        return Ok(None);
    };
    if policy.max_object_requests.is_some_and(|max| max < 2) {
        return Err(ApiError::bad_request(
            "native SQL read exceeds descriptor/manifest request budget",
        ));
    }
    let metadata_budget = |used: u64| {
        Some({
            let p = policy;
            p.max_scan_bytes
                .unwrap_or(u64::MAX)
                .saturating_sub(used)
                .min(
                    p.memory_limit_bytes
                        .unwrap_or(u64::MAX)
                        .saturating_sub(used.saturating_mul(16))
                        / 16,
                )
        })
    };
    // The checkpoint record is one object request against the read budget.
    // Charge it before the fetch so a restrictive policy is rejected before
    // any output page is read.
    let mut metadata_requests = discovery_requests;
    let bytes = if let Some(bytes) = discovered_bytes {
        bytes
    } else {
        charge_request(policy, &mut metadata_requests)?;
        read_query_metadata(
            state,
            super::materialized_read_cache::MaterializedReadCacheKey {
                store_id: state.materialized_read_cache_store_id().into(),
                namespace: state.materialized_read_cache_namespace().into(),
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                output_id: output_id.into(),
                object_path: pointer.checkpoint_key.clone(),
                content_hash: pointer.manifest_hash.clone(),
                codec: "velorix-arrow-ipc-zstd3-v1".into(),
                root_hash: pointer.content_hash.clone(),
                checkpoint_epoch: pointer.logical_epoch,
            },
            metadata_budget(0)
                .map(|n| n.min(16 * 1024 * 1024))
                .or(Some(16 * 1024 * 1024)),
            |bytes| {
                if stable_bytes_hash(bytes) != pointer.manifest_hash {
                    return Err(invalid());
                }
                let mut record = standing_runtime_checkpoint_record_from_slice(bytes)
                    .map_err(ApiError::bad_request)?;
                if record.checkpoint_key.is_empty() {
                    record.checkpoint_key = pointer.checkpoint_key.clone();
                }
                validate_standing_runtime_checkpoint_record(identity, view_id, &pointer, &record)
            },
        )
        .await?
    };
    if bytes.len() > 16 * 1024 * 1024 || stable_bytes_hash(&bytes) != pointer.manifest_hash {
        return Err(invalid());
    }
    let mut record =
        standing_runtime_checkpoint_record_from_slice(&bytes).map_err(ApiError::bad_request)?;
    if record.checkpoint_key.is_empty() {
        record.checkpoint_key = pointer.checkpoint_key.clone();
    }
    validate_standing_runtime_checkpoint_record(identity, view_id, &pointer, &record)?;
    let mut metadata_bytes = bytes.len() as u64;
    let manifest = {
        let mut matching = record
            .checkpoint
            .output_manifest_refs
            .iter()
            .filter_map(|r| r.strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
            .filter(|key| {
                ObjectKey::parse_standing_runtime_output_manifest(*key)
                    .is_ok_and(|(_, p)| p.view_id == output_id)
            });
        // A checkpoint without a published output manifest is not a malformed
        // page set: it is a checkpoint-only publication, which the explicit
        // legacy reader serves from the checkpoint's own published output.
        // The already-consumed metadata budget travels with the delegation so
        // the legacy reader cannot re-spend the same allowance.
        let Some(path) = matching.next() else {
            if pinned {
                return Ok(None);
            }
            return super::query_serving::legacy_materialized_page(
                state,
                view_id,
                identity,
                output_id,
                schema,
                request,
                filter.is_some(),
                policy,
                metadata_bytes,
                metadata_requests,
            )
            .await
            .map(Some);
        };
        if matching.next().is_some() {
            return Err(invalid());
        }
        let (key, parts) = ObjectKey::parse_standing_runtime_output_manifest(path)
            .map_err(ApiError::bad_request)?;
        charge_request(policy, &mut metadata_requests)?;
        let bytes = read_query_metadata(
            state,
            super::materialized_read_cache::MaterializedReadCacheKey {
                store_id: state.materialized_read_cache_store_id().into(),
                namespace: state.materialized_read_cache_namespace().into(),
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                output_id: output_id.into(),
                object_path: path.into(),
                content_hash: parts.output_content_hash.clone(),
                codec: "standing-runtime-output-manifest-json".into(),
                root_hash: parts.output_content_hash.clone(),
                checkpoint_epoch: pointer.logical_epoch,
            },
            metadata_budget(metadata_bytes).map(|n| n.min(MAX_INDEX_BYTES as u64)),
            |bytes| {
                let manifest = serde_json::from_slice(bytes).map_err(ApiError::bad_request)?;
                super::checkpoint_publication::validate_standing_runtime_output_manifest_record(
                    &key, &manifest,
                )
            },
        )
        .await?;
        metadata_bytes = metadata_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(invalid)?;
        let manifest = serde_json::from_slice(&bytes).map_err(ApiError::bad_request)?;
        super::checkpoint_publication::validate_standing_runtime_output_manifest_record(
            &key, &manifest,
        )?;
        manifest
    };
    if !is_materialized_codec(&manifest.output_encoding) {
        // A signed continuation must use the supplied immutable record, never
        // the legacy reader that resolves the latest head again.
        if pinned {
            return Ok(None);
        }
        return super::query_serving::legacy_materialized_page(
            state,
            view_id,
            identity,
            output_id,
            schema,
            request,
            filter.is_some(),
            policy,
            metadata_bytes,
            metadata_requests,
        )
        .await
        .map(Some);
    } // Explicit legacy reader remains responsible for v1.
    if manifest.checkpoint_key != pointer.checkpoint_key
        || manifest.logical_epoch != pointer.logical_epoch
        || manifest.checkpoint_content_hash != pointer.content_hash
        || manifest.tenant_id != identity.tenant_id
        || manifest.program_id != identity.program_id
        || manifest.view_id != output_id
    {
        return Err(invalid());
    }
    let index: PageIndex =
        serde_json::from_value(manifest.published_output.clone()).map_err(ApiError::bad_request)?;
    // The legacy families must match their registered plan. Any other family is
    // admitted only when the manifest still binds the checkpoint it was
    // published from, which is the exact extraction rule publication used.
    if index.schema != *schema {
        return Err(invalid());
    }
    if index.family.as_deref() != Some(family)
        && validate_checkpoint_binding(&manifest, &record.checkpoint).is_err()
    {
        return Err(invalid());
    }
    // Eligible SQL needs a complete bounded snapshot even when legacy statistics are absent.
    if filter.is_some() && request.page_token.is_some() {
        return Err(ApiError::bad_request(
            "filtered SQL cannot use a raw page cursor",
        ));
    }
    let candidates = manifest
        .pages
        .iter()
        .enumerate()
        .map(|(i, p)| {
            filter.is_none_or(|(predicate, _)| {
                index
                    .page_stats
                    .as_ref()
                    .is_none_or(|stats| may_match(predicate, schema, &stats[i], p.row_count))
            })
        })
        .collect::<Vec<_>>();
    let selected_count = manifest
        .pages
        .iter()
        .zip(&candidates)
        .filter(|(_, yes)| **yes)
        .try_fold(0usize, |n, (p, _)| {
            n.checked_add(p.row_count).ok_or_else(invalid)
        })?;
    if request.max_rows == Some(0)
        || request
            .committed_epoch
            .is_some_and(|epoch| epoch != pointer.logical_epoch)
    {
        return Err(invalid());
    }
    let offset = match (selected_row_offset, request.page_token) {
        (Some(offset), None) => offset,
        (Some(_), Some(_)) => return Err(invalid()),
        (None, None) => 0,
        (None, Some(token)) => {
            let token: PageToken = serde_json::from_str(&token).map_err(ApiError::bad_request)?;
            if token.codec != manifest.output_encoding
                || token.tenant != manifest.tenant_id
                || token.program != manifest.program_id
                || token.output != output_id
                || token.schema != schema.schema_fingerprint
                || token.root != manifest.output_content_hash
                || token.epoch != pointer.logical_epoch
            {
                return Err(invalid());
            }
            token.offset
        }
    };
    if offset > selected_count {
        return Err(invalid());
    }
    let canonical_batch = if is_bag_codec(&manifest.output_encoding) {
        velorix_runtime::materialized_view_runtime::materialized_bag_canonical_batch
    } else {
        velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch
    };
    let end = offset
        .saturating_add(if filter.is_some() {
            selected_count
        } else {
            request.max_rows.unwrap_or(selected_count)
        })
        .min(selected_count);
    let mut batches = Vec::new();
    let mut start = 0usize;
    let mut scan_bytes = metadata_bytes;
    let mut retained_bytes = metadata_bytes.saturating_mul(16);
    let mut candidate_start = 0usize;
    let mut files = 0usize;
    for (page, candidate) in manifest.pages.iter().zip(&candidates) {
        if !candidate {
            continue;
        }
        let stop = candidate_start
            .checked_add(page.row_count)
            .ok_or_else(invalid)?;
        if (candidate_start < end && stop > offset)
            || (filter.is_none() && manifest.output_row_count == 0)
        {
            files += 1;
        }
        candidate_start = stop;
    }
    let required_object_requests = files.checked_add(metadata_requests).ok_or_else(invalid)?;
    let exceeds_scan_files = policy.max_scan_files.is_some_and(|max| files > max);
    let exceeds_object_requests = policy
        .max_object_requests
        .is_some_and(|max| required_object_requests > max);
    if exceeds_scan_files || exceeds_object_requests {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "native read exceeds query file or object-request budget".into(),
            details: Some(serde_json::json!({
                "code": "NATIVE_QUERY_BUDGET_EXCEEDED",
                "required_scan_files": files,
                "max_scan_files": policy.max_scan_files,
                "required_object_requests": required_object_requests,
                "max_object_requests": policy.max_object_requests,
                "manifest_pages": manifest.pages.len(),
                "candidate_pages": candidates.iter().filter(|candidate| **candidate).count(),
                "requested_output_rows": request.max_rows,
                "filter_requires_complete_candidate_scan": filter.is_some(),
                "scan_files_exceeded": exceeds_scan_files,
                "object_requests_exceeded": exceeds_object_requests
            })),
            retryable_materialization: false,
        });
    }
    metadata_requests = required_object_requests;
    for (page_ref, candidate) in manifest.pages.iter().zip(candidates) {
        if !candidate {
            continue;
        }
        let count = page_ref.row_count;
        let stop = start.checked_add(count).ok_or_else(invalid)?;
        if (start < end && stop > offset) || (filter.is_none() && manifest.output_row_count == 0) {
            let scan_remaining = policy.max_scan_bytes.map(|n| n.saturating_sub(scan_bytes));
            // Charge encoded bytes to scan policy and independently bound
            // decompression by remaining query memory and the state ceiling.
            let expanded_remaining = policy
                .memory_limit_bytes
                .map(|n| n.saturating_sub(retained_bytes) / 16)
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(state.max_standing_runtime_expansion_bytes);
            let (page, typed, byte_count) = read_query_page(
                state,
                &manifest,
                page_ref,
                scan_remaining,
                expanded_remaining,
            )
            .await?;
            {
                scan_bytes = scan_bytes.checked_add(byte_count).ok_or_else(invalid)?;
                let payload: PageRows = serde_json::from_value(page.published_output.clone())
                    .map_err(ApiError::bad_request)?;
                let expanded = payload.rows.records().iter().try_fold(0u64, |n, row| {
                    let bytes = serde_json::to_vec(row).map_err(ApiError::internal)?.len() as u64;
                    n.checked_add(
                        bytes
                            .checked_mul(u64::try_from(row.weight).map_err(ApiError::bad_request)?)
                            .ok_or_else(invalid)?,
                    )
                    .ok_or_else(invalid)
                })?;
                let required = expanded
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(byte_count.saturating_mul(16)))
                    .and_then(|n| n.checked_add(retained_bytes))
                    .ok_or_else(invalid)?;
                if policy.memory_limit_bytes.is_some_and(|max| required > max) {
                    return Err(ApiError::bad_request(
                        "native filtered BAG expansion exceeds query memory budget",
                    ));
                }
            }
            let rows = if typed.is_some() {
                validate_page_binding_metadata(&manifest, &page)?
            } else {
                validate_page_binding(&manifest, &page)?
            };
            let mut position = start;
            let mut selected = Vec::new();
            let mut indices = Vec::new();
            for (i, row) in rows.records().iter().enumerate() {
                let next = position
                    .checked_add(usize::try_from(row.weight).map_err(ApiError::bad_request)?)
                    .ok_or_else(invalid)?;
                let take = next.min(end).saturating_sub(position.max(offset));
                if take > 0 {
                    if typed.is_some() {
                        indices.extend(std::iter::repeat_n(
                            u32::try_from(i).map_err(ApiError::bad_request)?,
                            take,
                        ));
                    } else {
                        let mut part = row.clone();
                        part.weight = i64::try_from(take).map_err(ApiError::bad_request)?;
                        selected.push(part);
                    }
                }
                position = next;
            }
            let batch = if let Some(batch) = typed {
                let indices = arrow::array::UInt32Array::from(indices);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|c| arrow::compute::take(c, &indices, None).map_err(ApiError::bad_request))
                    .collect::<Result<Vec<_>, _>>()?;
                RecordBatch::try_new(batch.schema(), columns).map_err(ApiError::bad_request)?
            } else {
                canonical_batch(schema, &DeltaBatch::from_records(selected))
                    .map_err(ApiError::bad_request)?
            };
            retained_bytes = retained_bytes
                .checked_add(batch.get_array_memory_size() as u64)
                .ok_or_else(invalid)?;
            if policy
                .memory_limit_bytes
                .is_some_and(|max| retained_bytes > max)
            {
                return Err(ApiError::bad_request(
                    "native filtered input exceeds query memory budget",
                ));
            }
            batches.push(batch);
        }
        start = stop;
        if start >= end {
            break;
        }
    }
    if batches.is_empty() {
        batches
            .push(canonical_batch(schema, &DeltaBatch::default()).map_err(ApiError::bad_request)?);
    }
    // The descriptor and selected pages describe one exact head, never a mixture.
    if !pinned {
        if let Some(meta) = &state.meta_store {
            let current = meta
                .read_standing_runtime_checkpoint(
                    &identity.tenant_id,
                    &identity.program_id,
                    view_id,
                )
                .await
                .map_err(meta_error_to_api)?;
            if current.as_ref() != Some(&pointer) {
                return Err(ApiError::conflict(
                    "materialized output head changed during paging",
                ));
            }
        } else {
            let current =
                discover_checkpoint_head(state, identity, view_id, policy, &mut metadata_requests)
                    .await?;
            if current.is_none_or(|(path, parts)| {
                path != pointer.checkpoint_key
                    || parts.logical_epoch != pointer.logical_epoch
                    || parts.content_hash != pointer.content_hash
            }) {
                return Err(ApiError::conflict(
                    "materialized output head changed during paging",
                ));
            }
        }
    }
    let next_page_token = if end < selected_count {
        Some(
            serde_json::to_string(&PageToken {
                codec: manifest.output_encoding.clone(),
                tenant: manifest.tenant_id,
                program: manifest.program_id,
                output: output_id.into(),
                schema: schema.schema_fingerprint.clone(),
                root: manifest.output_content_hash,
                epoch: pointer.logical_epoch,
                offset: end,
            })
            .map_err(ApiError::internal)?,
        )
    } else {
        None
    };
    Ok(Some(MaterializedViewPage {
        view: ScopedViewId {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: output_id.into(),
        },
        logical_epoch: pointer.logical_epoch,
        schema_fingerprint: schema.schema_fingerprint.clone(),
        batches,
        next_page_token,
    }))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use velorix_control::meta_admin::InMemoryMetaStore;
    use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};

    fn compare_normalization_batches(rows: usize) {
        let (mut checkpoint, schema) = fixture();
        let output = DeltaBatch::from_records((0..rows).map(|i| {
            DeltaRecord::new(
                DeltaKey::from_json(json!(format!("id-{:05}", i / 3))),
                DeltaValue::from_json(
                    json!({"power": if i.is_multiple_of(13) {Value::Null} else {json!(i as f64 + 0.5)}}),
                ),
                if i.is_multiple_of(17) { 3 } else { 1 },
            )
        }));
        let state = velorix_core::arrow_state::to_state(
            &json!({"output_schema":schema,"published_output":output}),
        )
        .unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(state.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = state;
        let key = key(&checkpoint);
        let start = Instant::now();
        let single = checkpoint_publication_batched(&checkpoint, &schema.relation_id, &key, 1)
            .unwrap()
            .unwrap();
        let single_time = start.elapsed();
        let start = Instant::now();
        let batched =
            checkpoint_publication_batched(&checkpoint, &schema.relation_id, &key, PAGE_ROWS)
                .unwrap()
                .unwrap();
        let batch_time = start.elapsed();
        assert_eq!(single.manifest_key, batched.manifest_key);
        assert_eq!(
            serde_json::to_value(single.manifest_record).unwrap(),
            serde_json::to_value(batched.manifest_record).unwrap()
        );
        assert_eq!(single.page_records.len(), batched.page_records.len());
        for ((old_key, old), (new_key, new)) in
            single.page_records.into_iter().zip(batched.page_records)
        {
            assert_eq!(old_key, new_key);
            assert_eq!(
                serde_json::to_value(old).unwrap(),
                serde_json::to_value(new).unwrap()
            );
        }
        eprintln!(
            "normalization_rows={rows} single_row_ms={:.3} batch_512_ms={:.3}",
            single_time.as_secs_f64() * 1000.0,
            batch_time.as_secs_f64() * 1000.0
        );
    }
    #[test]
    fn normalization_batches_preserve_keys_weights_nulls_and_page_boundaries() {
        compare_normalization_batches(1027);
    }
    #[test]
    #[ignore = "explicit performance measurement"]
    fn normalization_benchmark() {
        compare_normalization_batches(25000);
    }

    fn fixture() -> (RuntimeCheckpoint, RelationSchema) {
        let schema = RelationSchema {
            relation_id: "purchases_by_user".into(),
            relation_name: "purchases_by_user".into(),
            relation_version: "custom-version".into(),
            schema_fingerprint: stable_bytes_hash(b"typed-page-test"),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "power".into(),
                    data_type: SqlDataType::Float64,
                    nullable: true,
                },
            ],
            primary_key: vec!["id".into()],
        };
        let mut checkpoint = crate::tests::test_runtime_checkpoint(Vec::new());
        let payload = json!({"runtime_kind": "generic_set_ops_v1", "output_schema": schema});
        let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = payload;
        (checkpoint, schema)
    }

    fn key(checkpoint: &RuntimeCheckpoint) -> ObjectKey {
        ObjectKey::standing_runtime_checkpoint(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            &checkpoint.identity.view_ids[0],
            checkpoint.logical_epoch,
            &checkpoint.state_root.content_hash,
        )
        .unwrap()
    }

    fn row(value: Value, weight: i64) -> DeltaRecord {
        DeltaRecord::new(
            DeltaKey::from_json(json!("same")),
            DeltaValue::from_json(json!({"power":value})),
            weight,
        )
    }

    #[test]
    fn native_zone_maps_prove_only_typed_possible_matches() {
        let (_, mut schema) = fixture();
        schema.columns.push(ColumnSchema {
            name: "timestamp".into(),
            data_type: SqlDataType::Timestamp { timezone: None },
            nullable: true,
        });
        let rows =
            DeltaBatch::from_records([row(json!(2.), 5), row(json!(4.), 7), row(Value::Null, 3)]);
        let stats = page_statistics(&fixture().1, &rows).unwrap();
        assert_eq!(stats[1].nulls, 3);
        for (predicate, expected) in [
            ("id='other'", false),
            ("id IN ('other','same')", true),
            ("power BETWEEN 5 AND 8", false),
            ("power>=4", true),
            ("power<2", false),
            ("power<=2", true),
            ("power!=2", true),
            ("power IS NULL", true),
            ("power IS NOT NULL", true),
            ("id='other' AND abs(power)>0", false),
            ("id='other' OR abs(power)>0", true),
            ("NOT(id='same')", true),
            ("power='5'", true),
            ("timestamp > '2026-10-10'", true),
        ] {
            let sql = format!("SELECT count(*) FROM purchases_by_user WHERE {predicate}");
            let filter =
                velorix_core::query::materialized_page_predicate(&sql, &schema, &[]).unwrap();
            assert_eq!(
                may_match(&filter, &schema, &stats, 15),
                expected,
                "{predicate}"
            );
        }
        let stats = page_statistics(
            &fixture().1,
            &DeltaBatch::from_records([row(Value::Null, 15)]),
        )
        .unwrap();
        for (predicate, expected) in [
            ("power=2", false),
            ("power IS NOT NULL", false),
            ("power IS NULL", true),
        ] {
            let sql = format!("SELECT * FROM purchases_by_user WHERE {predicate}");
            assert_eq!(
                may_match(
                    &velorix_core::query::materialized_page_predicate(&sql, &schema, &[]).unwrap(),
                    &schema,
                    &stats,
                    15
                ),
                expected
            );
        }
    }

    #[test]
    fn native_filter_ast_rejects_nested_reads_and_preserves_unknown_leaves() {
        let (_, mut schema) = fixture();
        for sql in [
            "WITH purchases_by_user AS (SELECT 1) SELECT * FROM purchases_by_user WHERE id='same'",
            "SELECT * FROM purchases_by_user WHERE id='same' UNION SELECT * FROM purchases_by_user",
            "SELECT * FROM purchases_by_user a JOIN purchases_by_user b ON a.id=b.id WHERE a.id='same'",
            "SELECT (SELECT count(*) FROM purchases_by_user) FROM purchases_by_user WHERE id='same'",
            "SELECT * FROM purchases_by_user WHERE id='same' AND EXISTS (SELECT 1 FROM purchases_by_user)",
            "SELECT * FROM (SELECT * FROM purchases_by_user) t WHERE id='same'",
            "SELECT * FROM other WHERE id='same'",
        ] {
            assert!(
                velorix_core::query::materialized_page_predicate(sql, &schema, &[]).is_none(),
                "{sql}"
            );
        }
        let filter = velorix_core::query::materialized_page_predicate(
            "SELECT sum(power), count(*) FROM purchases_by_user p WHERE p.id=$1 AND abs(power)>0",
            &schema,
            &[QueryBindValue::Utf8("same".into())],
        )
        .unwrap();
        assert!(matches!(filter, PagePredicate::And(_, _)));
        assert_eq!(
            velorix_core::query::materialized_page_predicate(
                "SELECT * FROM purchases_by_user WHERE id=$1",
                &schema,
                &[QueryBindValue::Int64(1)]
            ),
            Some(PagePredicate::Unknown)
        );
        schema.columns[1].data_type = SqlDataType::Int64;
        assert!(matches!(
            velorix_core::query::materialized_page_predicate(
                "SELECT * FROM purchases_by_user WHERE power=9007199254740993",
                &schema,
                &[]
            ),
            Some(PagePredicate::Compare {
                value: PageScalar::Int(9007199254740993),
                ..
            })
        ));
        assert_eq!(
            velorix_core::query::materialized_page_predicate(
                "SELECT * FROM purchases_by_user WHERE power=1.5",
                &schema,
                &[]
            ),
            Some(PagePredicate::Unknown)
        );
    }

    #[tokio::test]
    async fn native_filtered_reads_prune_and_run_complete_weighted_residual_sql() {
        let output = DeltaBatch::from_records([
            DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                DeltaValue::from_json(json!({"power":1.})),
                12000,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("m")),
                DeltaValue::from_json(json!({"power":2.})),
                600,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("m")),
                DeltaValue::from_json(json!({"power":2.})),
                -1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("z")),
                DeltaValue::from_json(json!({"power":3.})),
                12000,
            ),
        ]);
        let (mut state, checkpoint, schema, publication, _) = installed(output).await;
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        for (sql, expected, reads) in [
            (
                "SELECT count(*) AS n, sum(power) AS total FROM purchases_by_user WHERE id='m'",
                json!({"n":599,"total":1198.}),
                2,
            ),
            (
                "SELECT count(*) AS n FROM purchases_by_user WHERE id='__missing'",
                json!({"n":0}),
                0,
            ),
            (
                "SELECT count(*) AS n FROM purchases_by_user WHERE power>=1",
                json!({"n":24599}),
                publication.page_records.len(),
            ),
            (
                "SELECT count(*) AS n FROM purchases_by_user WHERE id='m' AND abs(power)=99",
                json!({"n":0}),
                2,
            ),
            (
                "SELECT count(*) AS n FROM purchases_by_user WHERE id='missing' OR abs(power)=1",
                json!({"n":12000}),
                publication.page_records.len(),
            ),
        ] {
            counts.clear();
            let predicate =
                velorix_core::query::materialized_page_predicate(sql, &schema, &[]).unwrap();
            let page = query_bound_selected(
                &state,
                "purchases_by_user",
                &checkpoint.identity,
                "purchases_by_user",
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest {
                    max_rows: Some(10001),
                    ..Default::default()
                },
                Some((&predicate, QueryPolicy::default())),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(page.next_page_token.is_none());
            assert_eq!(
                counts
                    .get_paths()
                    .iter()
                    .filter(|p| p.starts_with("v1/standing-runtime-output-pages/"))
                    .count(),
                reads,
                "{sql}"
            );
            let result = query_record_batches_table_with_bindings_and_policy_and_limiter(
                "purchases_by_user",
                page.batches,
                sql,
                &[],
                QueryPolicy::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                record_batches_to_json_rows(&result).unwrap(),
                vec![expected],
                "{sql}"
            );
        }
        let predicate = PagePredicate::Unknown;
        let policy = QueryPolicy {
            max_scan_files: Some(1),
            ..QueryPolicy::default()
        };
        counts.clear();
        let error = query_bound_selected(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default(),
            Some((&predicate, policy)),
        )
        .await
        .unwrap_err();
        let details = error.details.as_ref().unwrap();
        assert_eq!(details["code"], "NATIVE_QUERY_BUDGET_EXCEEDED");
        assert!(details["required_scan_files"].as_u64().unwrap() > 1);
        assert_eq!(details["max_scan_files"], 1);
        assert_eq!(
            details["required_object_requests"].as_u64().unwrap(),
            details["required_scan_files"].as_u64().unwrap() + 2
        );
        assert_eq!(details["max_object_requests"], 1000);
        assert_eq!(details["manifest_pages"], details["required_scan_files"]);
        assert_eq!(details["candidate_pages"], details["required_scan_files"]);
        assert_eq!(details["scan_files_exceeded"], true);
        assert_eq!(details["object_requests_exceeded"], false);
        let required_files = details["required_scan_files"].clone();
        counts.clear();
        let smaller_response_limit = query_bound_selected(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest {
                max_rows: Some(500),
                ..SnapshotPageRequest::default()
            },
            Some((&predicate, policy)),
        )
        .await
        .unwrap_err();
        assert_eq!(
            smaller_response_limit.details.unwrap()["required_scan_files"],
            required_files,
            "max_rows caps result rows; it cannot skip pages needed for the exact SQL filter"
        );
        assert_eq!(
            counts.get_paths().len(),
            2,
            "reject page count before page GETs"
        );
        counts.clear();
        let first_unfiltered_page = query_bound_selected_policy(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest {
                max_rows: Some(500),
                ..SnapshotPageRequest::default()
            },
            None,
            QueryPolicy {
                max_scan_files: Some(1),
                max_object_requests: Some(3),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            first_unfiltered_page
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            500
        );
        assert!(first_unfiltered_page.next_page_token.is_some());
        assert_eq!(
            counts
                .get_paths()
                .iter()
                .filter(|path| path.starts_with("v1/standing-runtime-output-pages/"))
                .count(),
            1,
            "an unfiltered page reads only its requested output page"
        );
        let policy = QueryPolicy {
            max_scan_bytes: Some(1),
            ..QueryPolicy::default()
        };
        assert!(query_bound_selected(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default(),
            Some((&predicate, policy))
        )
        .await
        .is_err());
    }

    pub(crate) async fn public_native_fixture() -> (ApiState, ActiveMaterializedView) {
        public_native_fixture_with_codec(true).await
    }

    pub(crate) async fn public_native_fixture_with_codec(
        stats: bool,
    ) -> (ApiState, ActiveMaterializedView) {
        let output = DeltaBatch::from_records([
            DeltaRecord::new(
                DeltaKey::from_json(json!("a")),
                DeltaValue::from_json(json!({"power":1.})),
                12000,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("m")),
                DeltaValue::from_json(json!({"power":2.})),
                20,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("z")),
                DeltaValue::from_json(json!({"power":3.})),
                12000,
            ),
        ]);
        let (mut checkpoint, schema) = fixture();
        checkpoint.input_coverage = Some(
            velorix_core::standing_program::RuntimeCheckpointInputCoverageV1 {
                schema_version: 1,
                view_generation: 1,
                plan_hash: "sha256:query-fixture".into(),
                input_catalog_epoch: 0,
                relations: vec![
                    velorix_core::standing_program::RuntimeCheckpointRelationCoverageV1 {
                        relation_id: "input".into(),
                        relation_version: schema.relation_version.clone(),
                        relation_generation: 1,
                        schema_fingerprint: schema.schema_fingerprint.clone(),
                        partitions: vec![],
                    },
                ],
            },
        );
        let (state, checkpoint, schema, _, _) =
            install_checkpoint_with_stats(checkpoint, schema, output, stats).await;
        // Query-only durable snapshot: no runtime restoration or source ingestion.
        let logical_plan = serde_json::from_value(json!({
            "plan_version":1,"plan_hash":"sha256:query-fixture","view_sql":"select id,power from input",
            "capability_version":"fixture","key_semantics_version":"fixture","bag_semantics_version":"fixture",
            "input_relations":[],"output_relation":{"relation_id":schema.relation_id,"relation_name":schema.relation_name,"relation_version":schema.relation_version,"schema_fingerprint":schema.schema_fingerprint},
            "nodes":[],"operator_dag_contract":{"contract_version":"fixture","operators":[],"edges":[]},"state_requirements":[],"output_codec_version":"fixture",
            "execution":{"kind":"typed_map","plan":{"schema_version":1,"input_schema":schema,"projection":[],"output_columns":[],"predicate":null}}
        })).unwrap();
        let mut input = schema.clone();
        input.relation_id = "input".into();
        input.relation_name = "input".into();
        let spec = StandingViewSpec {
            view_id: schema.relation_id.clone(),
            sql: "select id,power from input".into(),
            dialect: SqlDialect::VelorixSql,
            source_kind: SqlSourceKind::StandingView,
            input_relations: vec![input],
            output_relations: vec![schema.clone()],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };
        let runtime = MaterializedViewRuntimeBinding {
            runtime_kind: "generic_set_ops_v1".into(),
            runtime_version: "fixture".into(),
            standing_program_identity: checkpoint.identity.clone(),
            logical_plan: Some(logical_plan),
            published_relations: vec![],
            input_bindings: vec![],
        };
        let api = MaterializedViewApiMetadata {
            sql_template: Some(
                "SELECT * FROM purchases_by_user WHERE id={{context.params.record_id}}".into(),
            ),
            request: vec![serde_json::from_value(
                json!({"fieldName":"record_id","fieldIn":"query","type":"string"}),
            )
            .unwrap()],
            ..Default::default()
        };
        MaterializedViewRegistry::new(state.store.clone())
            .register_with_api_metadata_runtime_execution(
                &spec,
                Some(api),
                runtime,
                Some(MaterializedViewLifecycleStatus::standing_runtime()),
            )
            .await
            .unwrap();
        state
            .meta_store
            .as_ref()
            .unwrap()
            .begin_view_bootstrap(BeginViewBootstrapRequest {
                tenant_id: checkpoint.identity.tenant_id.clone(),
                program_id: checkpoint.identity.program_id.clone(),
                view_id: spec.view_id.clone(),
                plan_hash: "sha256:query-fixture".into(),
                view_spec_json: serde_json::to_vec(&spec).unwrap(),
                relations: vec![
                    velorix_control::meta_admin::IngestSourceRelationIdentityV1 {
                        relation_id: "input".into(),
                        relation_version: schema.relation_version.clone(),
                        relation_generation: 1,
                        schema_fingerprint: schema.schema_fingerprint.clone(),
                    },
                ],
                view_inputs: vec![],
                expected_graph_revision: 0,
            })
            .await
            .unwrap();
        activate_authoritative_view_bootstrap(&state, &checkpoint.identity, &spec.view_id)
            .await
            .unwrap();
        let active = MaterializedViewRegistry::new(state.store.clone())
            .read_active(&spec.view_id)
            .await
            .unwrap();
        (state, active)
    }

    #[tokio::test]
    async fn native_public_sql_and_template_filter_large_output_under_default_policy() {
        use axum::http::Method;
        use tower::ServiceExt;
        let (mut state, _) = public_native_fixture().await;
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        let router = app(state);
        for (body, n, page_reads) in [
            (
                json!({"sql":"SELECT * FROM purchases_by_user WHERE id='m'"}),
                20,
                1,
            ),
            (json!({"parameters":{"record_id":"m"}}), 20, 1),
            (
                json!({"sql":"SELECT count(*) AS n FROM purchases_by_user WHERE power>=1"}),
                1,
                47,
            ),
            (
                json!({"sql":"SELECT * FROM purchases_by_user WHERE id='m' AND abs(power)=99"}),
                0,
                1,
            ),
        ] {
            counts.clear();
            let request = axum::http::Request::builder()
                .method(Method::POST)
                .uri("/v1/views/purchases_by_user/query")
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(result["rows"].as_array().unwrap().len(), n);
            if body["sql"]
                .as_str()
                .is_some_and(|sql| sql.starts_with("SELECT count"))
            {
                assert_eq!(result["rows"][0]["n"], 24020);
            }
            assert_eq!(
                counts
                    .get_paths()
                    .iter()
                    .filter(|p| p.starts_with("v1/standing-runtime-output-pages/"))
                    .count(),
                page_reads
            );
        }
        for body in [
            json!({"sql":"SELECT * FROM purchases_by_user WHERE power>=1"}),
            json!({"sql":"SELECT count(*) FROM purchases_by_user WHERE id='m'","page_token":"invalid"}),
        ] {
            let request = axum::http::Request::builder()
                .method(Method::POST)
                .uri("/v1/views/purchases_by_user/query")
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap();
            assert!(!router
                .clone()
                .oneshot(request)
                .await
                .unwrap()
                .status()
                .is_success());
        }
    }

    #[test]
    fn native_pages_consolidate_signed_bags_once_and_bound_capacity() {
        let (checkpoint, _) = fixture();
        let output = DeltaBatch::from_records([
            row(json!(2.5), 600),
            row(json!(2.5), -1),
            row(Value::Null, 20),
            row(json!(9.), 3),
            row(json!(9.), -3),
        ]);
        let publication = publication(
            &checkpoint,
            "purchases_by_user",
            &key(&checkpoint),
            output.clone(),
        )
        .unwrap();
        assert_eq!(publication.manifest_record.output_row_count, 619);
        assert_eq!(
            publication
                .manifest_record
                .pages
                .iter()
                .map(|p| p.row_count)
                .collect::<Vec<_>>(),
            [512, 107]
        );
        let combined =
            DeltaBatch::from_records(publication.page_records.iter().flat_map(|(_, p)| {
                validate_page_binding(&publication.manifest_record, p)
                    .unwrap()
                    .records()
                    .to_vec()
            }));
        assert_eq!(combined.net_rows().unwrap(), output.net_rows().unwrap());
        for (key, page) in &publication.page_records {
            validate_page(key, page).unwrap();
        }
        let empty = super::publication(
            &checkpoint,
            "purchases_by_user",
            &key(&checkpoint),
            DeltaBatch::default(),
        )
        .unwrap();
        assert_eq!(empty.page_records.len(), 1);
        assert_eq!(empty.manifest_record.output_row_count, 0);
        let started = std::time::Instant::now();
        for weights in [
            [i64::MAX, 0],
            [i64::MAX, i64::MAX],
            [MAX_PAGES as i64 * 512 + 1, 0],
        ] {
            let output = DeltaBatch::from_records(
                weights
                    .into_iter()
                    .filter(|w| *w != 0)
                    .enumerate()
                    .map(|(i, w)| row(json!(i), w)),
            );
            assert!(super::publication(
                &checkpoint,
                "purchases_by_user",
                &key(&checkpoint),
                output
            )
            .is_err());
        }
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        let overflow = super::publication(
            &checkpoint,
            "purchases_by_user",
            &key(&checkpoint),
            DeltaBatch::from_records((0..3).map(|i| row(json!(i), i64::MAX))),
        )
        .unwrap_err();
        assert!(overflow.to_string().contains("cardinality overflow"));
        assert!(super::publication(
            &checkpoint,
            "purchases_by_user",
            &key(&checkpoint),
            DeltaBatch::from_records([row(json!(1), i64::MAX), row(json!(1), 1)])
        )
        .is_err());
        assert!(super::publication(
            &checkpoint,
            "purchases_by_user",
            &key(&checkpoint),
            DeltaBatch::from_records([row(json!(1), -1)])
        )
        .is_err());
    }

    #[test]
    fn native_pages_hashes_bind_index_schema_scope_codec_and_order() {
        let (checkpoint, _) = fixture();
        let p = publication(
            &checkpoint,
            "purchases_by_user",
            &key(&checkpoint),
            DeltaBatch::from_records([row(json!(1), 600)]),
        )
        .unwrap();
        for mutation in 0..6 {
            let mut manifest = p.manifest_record.clone();
            match mutation {
                0 => manifest.published_output["schema"]["schema_fingerprint"] = json!("other"),
                1 => manifest.tenant_id = "other".into(),
                2 => manifest.logical_epoch += 1,
                3 => manifest.pages[0].row_count -= 1,
                4 => manifest.pages.swap(0, 1),
                _ => manifest.output_encoding = "unknown".into(),
            }
            assert!(validate_manifest(&p.manifest_key, &manifest).is_err());
        }
        for mutation in 0..6 {
            let (key, mut page) = p.page_records[0].clone();
            match mutation {
                0 => page.published_output["rows"]["records"][0]["weight"] = json!(1),
                1 => page.published_output["schema_fingerprint"] = json!("other"),
                2 => page.program_id = "other".into(),
                3 => page.logical_epoch += 1,
                4 => page.output_encoding = "unknown".into(),
                _ => page.row_count += 1,
            }
            assert!(validate_page(&key, &page).is_err());
        }
    }

    /// A synthetic checkpoint payload for a generic (non-legacy) family that
    /// binds an output schema and a published output, exactly like a real
    /// `ordered_window` or `single_key_sum_count` checkpoint does.
    fn generic_fixture(
        runtime_kind: &str,
        output: DeltaBatch,
    ) -> (RuntimeCheckpoint, RelationSchema) {
        let (mut checkpoint, schema) = fixture();
        let mut payload = json!({
            "runtime_kind": runtime_kind,
            "output_schema": schema,
            "published_output": output,
        });
        if runtime_kind == "ordered_window" {
            payload["plan"] = json!({"schema_version": 2});
        }
        let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = payload;
        (checkpoint, schema)
    }

    #[test]
    fn native_pages_publish_generic_families_and_round_trip() {
        for kind in ["ordered_window", "single_key_sum_count"] {
            let output = DeltaBatch::from_records([row(json!(2.5), 3), row(json!(9.), 7)]);
            let (checkpoint, schema) = generic_fixture(kind, output.clone());
            assert!(supports_checkpoint(&checkpoint), "{kind} must be eligible");
            assert_eq!(
                velorix_runtime::materialized_view_runtime::bag_checkpoint_published_output(
                    &checkpoint
                )
                .unwrap(),
                Some(output.clone())
            );
            let publication = publication(
                &checkpoint,
                &schema.relation_id,
                &key(&checkpoint),
                output.clone(),
            )
            .unwrap();
            validate_checkpoint_binding(&publication.manifest_record, &checkpoint).unwrap();
            assert_eq!(
                publication.manifest_record.published_output["family"],
                json!(kind)
            );
            let index: PageIndex =
                serde_json::from_value(publication.manifest_record.published_output.clone())
                    .unwrap();
            assert_eq!(index.schema, schema);
            let mut combined = Vec::new();
            for (page_key, page) in &publication.page_records {
                validate_page(page_key, page).unwrap();
                let bytes = encode_page(page).unwrap();
                let decoded = decode_page(&bytes, CODEC).unwrap();
                assert_eq!(decoded.published_output, page.published_output);
                combined.extend(
                    validate_page_binding(&publication.manifest_record, page)
                        .unwrap()
                        .records()
                        .iter()
                        .cloned(),
                );
            }
            assert_eq!(
                DeltaBatch::from_records(combined).net_rows().unwrap(),
                output.net_rows().unwrap()
            );
        }
    }

    #[test]
    fn generic_family_without_output_schema_uses_arrow_state_page() {
        let (mut checkpoint, schema) = fixture();
        let output = DeltaBatch::from_records([row(json!(1.), 2)]);
        let payload = json!({"runtime_kind": "ordered_window", "published_output": output});
        let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = payload;
        assert!(!supports_checkpoint(&checkpoint));
        assert!(
            velorix_runtime::materialized_view_runtime::bag_checkpoint_published_output(
                &checkpoint
            )
            .unwrap()
            .is_none()
        );
        let publication =
            crate::checkpoint_publication::standing_runtime_output_manifest_record_for_checkpoint(
                &checkpoint,
                &schema.relation_id,
                &key(&checkpoint),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            publication.manifest_record.output_encoding,
            GENERIC_ARROW_CODEC
        );
        let page = &publication.page_records[0].1;
        let encoded = encode_page(page).unwrap();
        let decoded = decode_page(&encoded, GENERIC_ARROW_CODEC).unwrap();
        assert_eq!(decoded.published_output, page.published_output);
        let mut malformed = encoded;
        let middle = malformed.len() / 2;
        malformed[middle] ^= 0x40;
        assert!(decode_page(&malformed, GENERIC_ARROW_CODEC).is_err());

        let output = DeltaBatch::from_records([row(json!(1.), 2)]);
        let payload = json!({
            "runtime_kind": "ordered_window",
            "output_schema": schema,
            "published_output": output,
            "plan": {"schema_version": 1}
        });
        let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = payload;
        assert!(
            velorix_runtime::materialized_view_runtime::bag_checkpoint_published_output(
                &checkpoint
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn generic_arrow_page_hashes_nested_maps_after_canonicalization() {
        let (checkpoint, schema) = fixture();
        let batch = DeltaBatch::from_records([row(json!(1.), 1)]);
        let mut published_output = serde_json::to_value(batch).unwrap();
        published_output["records"][0]["value"] = json!({"z": 1, "nested": {"y": 2, "a": 3}});
        let mut canonical = published_output.clone();
        canonical.sort_all_objects();
        let expected_hash = stable_bytes_hash(&serde_json::to_vec(&canonical).unwrap());

        let publication =
            super::super::checkpoint_publication::standing_runtime_output_publication(
                &checkpoint,
                &schema.relation_id,
                &key(&checkpoint),
                published_output,
            )
            .unwrap();
        assert_eq!(
            publication.manifest_record.output_content_hash,
            expected_hash
        );
        let (page_key, page) = &publication.page_records[0];
        let decoded = decode_page(&encode_page(page).unwrap(), GENERIC_ARROW_CODEC).unwrap();
        assert_eq!(decoded.published_output, canonical);
        super::super::checkpoint_publication::validate_standing_runtime_output_page_record(
            page_key, &decoded,
        )
        .unwrap();
    }

    async fn installed(
        output: DeltaBatch,
    ) -> (
        ApiState,
        RuntimeCheckpoint,
        RelationSchema,
        StandingRuntimeOutputPublication,
        StandingRuntimeCheckpointPointer,
    ) {
        let (checkpoint, schema) = fixture();
        install_checkpoint(checkpoint, schema, output).await
    }

    async fn install_checkpoint(
        checkpoint: RuntimeCheckpoint,
        schema: RelationSchema,
        output: DeltaBatch,
    ) -> (
        ApiState,
        RuntimeCheckpoint,
        RelationSchema,
        StandingRuntimeOutputPublication,
        StandingRuntimeCheckpointPointer,
    ) {
        install_checkpoint_with_stats(checkpoint, schema, output, true).await
    }

    async fn install_checkpoint_with_stats(
        mut checkpoint: RuntimeCheckpoint,
        schema: RelationSchema,
        output: DeltaBatch,
        stats: bool,
    ) -> (
        ApiState,
        RuntimeCheckpoint,
        RelationSchema,
        StandingRuntimeOutputPublication,
        StandingRuntimeCheckpointPointer,
    ) {
        let view_id = schema.relation_id.clone();
        let state_key = ObjectKey::standing_runtime_state_payload(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            &view_id,
            checkpoint.logical_epoch,
            &checkpoint.state_root.content_hash,
        )
        .unwrap();
        checkpoint.state_root.object_key = state_key.as_str().into();
        let publication = if stats {
            publication(&checkpoint, &view_id, &key(&checkpoint), output)
        } else {
            publication_without_stats(&checkpoint, &view_id, &key(&checkpoint), output)
        }
        .unwrap();
        checkpoint.output_manifest_refs = vec![format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            publication.manifest_key.as_str()
        )];
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let meta = Arc::new(InMemoryMetaStore::default());
        let state = crate::tests::test_api_state_with_store(store, "native-pages-test", false)
            .await
            .with_meta_store(meta.clone());
        let state_record = StandingRuntimeStatePayloadRecord {
            schema_version: 1,
            record_kind: "standing_runtime_state_payload_v1".into(),
            tenant_id: checkpoint.identity.tenant_id.clone(),
            program_id: checkpoint.identity.program_id.clone(),
            view_id: view_id.clone(),
            logical_epoch: checkpoint.logical_epoch,
            checkpoint_codec_identity: checkpoint.checkpoint_codec_identity.clone(),
            state_content_hash: checkpoint.state_root.content_hash.clone(),
            source_kind: "standing_runtime_checkpoint_state_payload".into(),
            payload: checkpoint.state_payload.clone().unwrap(),
        };
        persist_standing_runtime_state_payload(&state, &state_key, &state_record)
            .await
            .unwrap();
        for (key, page) in &publication.page_records {
            persist_standing_runtime_output_page(&state, key, page)
                .await
                .unwrap();
        }
        persist_standing_runtime_output_manifest(
            &state,
            &publication.manifest_key,
            &publication.manifest_record,
        )
        .await
        .unwrap();
        let record = StandingRuntimeCheckpointRecord {
            published_input_bootstrap: None,
            demand_materialization: None,
            schema_version: 1,
            record_kind: "standing_runtime_checkpoint_v1".into(),
            view_id: view_id.clone(),
            checkpoint_key: key(&checkpoint).as_str().into(),
            previous_checkpoint: None,
            checkpoint: {
                let mut stored = checkpoint.clone();
                stored.state_payload = None;
                stored
            },
            replay_checkpoints: vec![],
            manifest_hash: String::new(),
        };
        let bytes = checkpoint_record::encode(&record, checkpoint_record::MAX_ENCODED_RECORD_BYTES)
            .unwrap();
        state
            .store
            .put(
                &ObjectPath::from(record.checkpoint_key.clone()),
                bytes::Bytes::from(bytes).into(),
            )
            .await
            .unwrap();
        let pointer = standing_runtime_checkpoint_pointer_from_record(&record);
        let owner = state
            .acquire_standing_runtime_owner(&checkpoint.identity, &view_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            meta.publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: pointer.clone(),
                owner,
                expected_relation_source_cuts: None,
            })
            .await
            .unwrap(),
            PublishStandingRuntimeCheckpointOutcome::Published
        );
        (state, checkpoint, schema, publication, pointer)
    }

    fn rebind(p: &mut StandingRuntimeOutputPublication) {
        p.manifest_record.output_content_hash = root_hash(&p.manifest_record).unwrap();
        p.manifest_key = ObjectKey::standing_runtime_output_manifest(
            &p.manifest_record.tenant_id,
            &p.manifest_record.program_id,
            &p.manifest_record.view_id,
            p.manifest_record.logical_epoch,
            &p.manifest_record.output_content_hash,
        )
        .unwrap();
        for (_, page) in &mut p.page_records {
            page.output_content_hash = p.manifest_record.output_content_hash.clone();
        }
    }

    #[tokio::test]
    async fn native_legacy_without_stats_reads_all_rows_with_normal_bindings() {
        let (checkpoint, schema) = fixture();
        let (state, checkpoint, schema, _, _) = install_checkpoint_with_stats(
            checkpoint,
            schema,
            DeltaBatch::from_records([row(json!(1.), 12001)]),
            false,
        )
        .await;
        let sql = "SELECT count(*) AS n FROM purchases_by_user WHERE id=$1";
        let binds = [QueryBindValue::Utf8("same".into())];
        let predicate =
            velorix_core::query::materialized_page_predicate(sql, &schema, &binds).unwrap();
        let page = query_bound_selected(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest {
                max_rows: Some(10001),
                ..Default::default()
            },
            Some((&predicate, QueryPolicy::default())),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            page.batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            12001
        );
        let result = query_record_batches_table_with_bindings_and_policy_and_limiter(
            "purchases_by_user",
            page.batches,
            sql,
            &binds,
            QueryPolicy::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            record_batches_to_json_rows(&result).unwrap(),
            vec![json!({"n":12001})]
        );
    }

    #[tokio::test]
    async fn native_mixed_between_matches_datafusion_common_coercion() {
        let (mut checkpoint, mut schema) = fixture();
        schema.columns[1].data_type = SqlDataType::Int64;
        let payload = json!({"runtime_kind":"generic_set_ops_v1","output_schema":schema});
        let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = payload;
        let (state, checkpoint, schema, _, _) = install_checkpoint(
            checkpoint,
            schema,
            DeltaBatch::from_records([row(json!(9007199254740993i64), 1)]),
        )
        .await;
        let sql = "SELECT count(*) AS n FROM purchases_by_user WHERE power BETWEEN 0.0 AND 9007199254740992";
        let predicate =
            velorix_core::query::materialized_page_predicate(sql, &schema, &[]).unwrap();
        assert_eq!(predicate, PagePredicate::Unknown);
        let page = query_bound_selected(
            &state,
            &schema.relation_id,
            &checkpoint.identity,
            &schema.relation_id,
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default(),
            Some((&predicate, QueryPolicy::default())),
        )
        .await
        .unwrap()
        .unwrap();
        let result = query_record_batches_table_with_bindings_and_policy_and_limiter(
            &schema.relation_id,
            page.batches,
            sql,
            &[],
            QueryPolicy::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            record_batches_to_json_rows(&result).unwrap(),
            vec![json!({"n":1})]
        );
    }

    #[tokio::test]
    async fn native_timestamp_bounds_match_residual_sql_at_nanosecond_precision() {
        let (mut checkpoint, mut schema) = fixture();
        schema.columns.push(ColumnSchema {
            name: "timestamp".into(),
            data_type: SqlDataType::Timestamp { timezone: None },
            nullable: false,
        });
        let payload = json!({"runtime_kind":"generic_set_ops_v1","output_schema":schema});
        let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = payload;
        let output =
            DeltaBatch::from_records([("a", 0), ("m", 1), ("z", 2)].into_iter().map(|(id, t)| {
                DeltaRecord::new(
                    DeltaKey::from_json(json!(id)),
                    DeltaValue::from_json(json!({"power":2.,"timestamp":t})),
                    512,
                )
            }));
        let (mut state, checkpoint, mut schema, _, _) =
            install_checkpoint(checkpoint, schema, output).await;
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        for (sql, binds, expected, pages) in [
            (
                "SELECT count(*) AS n FROM purchases_by_user WHERE timestamp >= TIMESTAMP '1970-01-01 00:00:00.000000001' AND timestamp < TIMESTAMP '1970-01-01 00:00:00.000000002'",
                vec![],
                512,
                1,
            ),
            (
                "SELECT count(*) AS n FROM purchases_by_user WHERE timestamp=$1",
                vec![QueryBindValue::Timestamp(
                    "1970-01-01T00:00:00.000000001".into(),
                )],
                512,
                1,
            ),
            (
                "SELECT count(*) AS n FROM purchases_by_user WHERE id IN ('a','z')",
                vec![],
                1024,
                2,
            ),
        ] {
            counts.clear();
            let predicate =
                velorix_core::query::materialized_page_predicate(sql, &schema, &binds).unwrap();
            let page = query_bound_selected(
                &state,
                &schema.relation_id,
                &checkpoint.identity,
                &schema.relation_id,
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest::default(),
                Some((&predicate, QueryPolicy::default())),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                counts
                    .get_paths()
                    .iter()
                    .filter(|p| p.starts_with("v1/standing-runtime-output-pages/"))
                    .count(),
                pages
            );
            let result = query_record_batches_table_with_bindings_and_policy_and_limiter(
                &schema.relation_id,
                page.batches,
                sql,
                &binds,
                QueryPolicy::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                record_batches_to_json_rows(&result).unwrap(),
                vec![json!({"n":expected})]
            );
        }
        for predicate in [
            "timestamp >= '1970-01-01 00:00:00'",
            "timestamp >= TIMESTAMP '1970-01-01 00:00:00Z'",
            "timestamp >= TIMESTAMP '1970-01-01 00:00:00+09:00'",
            "timestamp >= TIMESTAMP '1970-01-01 00:00:60'",
        ] {
            assert_eq!(
                velorix_core::query::materialized_page_predicate(
                    &format!("SELECT * FROM purchases_by_user WHERE {predicate}"),
                    &schema,
                    &[]
                ),
                Some(PagePredicate::Unknown)
            );
        }
        schema.columns[2].data_type = SqlDataType::Timestamp {
            timezone: Some("UTC".into()),
        };
        assert_eq!(
            velorix_core::query::materialized_page_predicate(
                "SELECT * FROM purchases_by_user WHERE timestamp >= TIMESTAMP '1970-01-01 00:00:00'",
                &schema,
                &[]
            ),
            Some(PagePredicate::Unknown)
        );
    }

    #[tokio::test]
    async fn native_query_page_decodes_once_per_validated_read() {
        use super::super::materialized_read_cache::{
            MaterializedReadCache, MaterializedReadCacheConfig,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (mut state, _, _, publication, _) =
            installed(DeltaBatch::from_records([row(json!(2.), 7)])).await;
        let calls = AtomicUsize::new(0);
        let manifest = &publication.manifest_record;
        let reference = &manifest.pages[0];
        let max_expanded_bytes = state.max_standing_runtime_expansion_bytes;
        let decode = |bytes: &[u8]| {
            calls.fetch_add(1, Ordering::Relaxed);
            decode_query_page(bytes, manifest, reference, max_expanded_bytes)
        };
        read_query_page_with_decoder(&state, manifest, reference, None, decode)
            .await
            .unwrap();
        assert_eq!(calls.swap(0, Ordering::Relaxed), 1, "uncached read");
        let directory = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            MaterializedReadCache::open(MaterializedReadCacheConfig {
                directory: directory.path().into(),
                memory_bytes: 256 * 1024,
                disk_bytes: 8 * 1024 * 1024,
            })
            .await
            .unwrap(),
        );
        state = state.with_materialized_read_cache(cache.clone());
        for label in ["cold cache", "warm cache"] {
            let (record, batch, _) =
                read_query_page_with_decoder(&state, manifest, reference, None, decode)
                    .await
                    .unwrap();
            assert_eq!(calls.swap(0, Ordering::Relaxed), 1, "{label}");
            assert_eq!(record.row_count, 7);
            assert_eq!(batch.unwrap().num_rows(), 1);
        }
        assert!(
            read_query_page_with_decoder(&state, manifest, reference, Some(1), decode)
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0, "byte guard before decode");
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn compressed_page_uses_separate_scan_and_expansion_limits() {
        let output = DeltaBatch::from_records((0..PAGE_ROWS).map(|i| {
            DeltaRecord::new(
                DeltaKey::from_json(json!(format!("id-{i:04}"))),
                DeltaValue::from_json(json!({"power": 1.0})),
                1,
            )
        }));
        let (state, _, _, publication, _) = installed(output).await;
        let manifest = &publication.manifest_record;
        let reference = &manifest.pages[0];
        let encoded = encode_page(&publication.page_records[0].1).unwrap();
        let encoded_cap = u64::try_from(encoded.len()).unwrap();
        assert!(decode_ipc_bounded(&encoded, encoded.len()).is_err());

        read_query_page(
            &state,
            manifest,
            reference,
            Some(encoded_cap),
            state.max_standing_runtime_expansion_bytes,
        )
        .await
        .unwrap();

        assert!(read_query_page(
            &state,
            manifest,
            reference,
            Some(encoded_cap),
            encoded.len(),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn native_cached_pages_revalidate_bytes_and_keep_metadata_heads_fresh() {
        use super::super::materialized_read_cache::{
            MaterializedReadCache, MaterializedReadCacheConfig, MaterializedReadCacheKey,
        };
        let (mut state, checkpoint, schema, p, _) =
            installed(DeltaBatch::from_records([row(json!(2.), 600)])).await;
        let directory = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            MaterializedReadCache::open(MaterializedReadCacheConfig {
                directory: directory.path().into(),
                memory_bytes: 256 * 1024,
                disk_bytes: 8 * 1024 * 1024,
            })
            .await
            .unwrap(),
        );
        state = state.with_materialized_read_cache(cache.clone());
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        let predicate = PagePredicate::Unknown;
        for gets in [4, 0] {
            counts.clear();
            let page = query_bound_selected(
                &state,
                &schema.relation_id,
                &checkpoint.identity,
                &schema.relation_id,
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest::default(),
                Some((&predicate, QueryPolicy::default())),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                page.batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                600
            );
            assert_eq!(
                counts.get_paths().len(),
                gets,
                "cold immutable metadata/pages, then warm cache"
            );
        }
        let reference = &p.manifest_record.pages[0];
        counts.clear();
        let direct = query_bound_selected_policy(
            &state,
            &schema.relation_id,
            &checkpoint.identity,
            &schema.relation_id,
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest {
                max_rows: Some(512),
                ..Default::default()
            },
            None,
            QueryPolicy {
                max_scan_files: Some(1),
                max_object_requests: Some(3),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            direct
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            512
        );
        assert!(direct.next_page_token.is_some());
        assert_eq!(counts.get_paths().len(), 0);
        assert!(
            query_bound_selected_policy(
                &state,
                &schema.relation_id,
                &checkpoint.identity,
                &schema.relation_id,
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest {
                    max_rows: Some(512),
                    ..Default::default()
                },
                None,
                QueryPolicy {
                    max_object_requests: Some(2),
                    ..Default::default()
                },
            )
            .await
            .is_err(),
            "warm cache still charges source policy objects"
        );
        assert_eq!(counts.get_paths().len(), 0);
        let cache_key = MaterializedReadCacheKey {
            store_id: state.materialized_read_cache_store_id().into(),
            namespace: state.materialized_read_cache_namespace().into(),
            tenant_id: p.manifest_record.tenant_id.clone(),
            program_id: p.manifest_record.program_id.clone(),
            output_id: schema.relation_id.clone(),
            object_path: reference.page_key.clone(),
            content_hash: reference.page_content_hash.clone(),
            codec: reference.output_encoding.clone(),
            root_hash: p.manifest_record.output_content_hash.clone(),
            checkpoint_epoch: checkpoint.logical_epoch,
        };
        cache
            .insert(cache_key, b"corrupt".to_vec(), |_| Ok::<(), ApiError>(()))
            .unwrap();
        counts.clear();
        query_bound_selected(
            &state,
            &schema.relation_id,
            &checkpoint.identity,
            &schema.relation_id,
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default(),
            Some((&predicate, QueryPolicy::default())),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            counts.get_paths().len(),
            1,
            "bad cached page evicted and refetched, immutable metadata and second page stay warm"
        );
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn native_direct_pages_enforce_source_policy_without_full_snapshot() {
        let (mut state, checkpoint, schema, _, _) =
            installed(DeltaBatch::from_records([row(json!(1.), 600)])).await;
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        let read = |request, policy| {
            query_bound_selected_policy(
                &state,
                &schema.relation_id,
                &checkpoint.identity,
                &schema.relation_id,
                "generic_set_ops_v1",
                &schema,
                request,
                None,
                policy,
            )
        };
        for (policy, gets) in [
            (
                QueryPolicy {
                    max_object_requests: Some(1),
                    ..Default::default()
                },
                0,
            ),
            (
                QueryPolicy {
                    max_scan_bytes: Some(1),
                    ..Default::default()
                },
                1,
            ),
            (
                QueryPolicy {
                    memory_limit_bytes: Some(1),
                    ..Default::default()
                },
                1,
            ),
            (
                QueryPolicy {
                    max_scan_files: Some(0),
                    ..Default::default()
                },
                2,
            ),
            (
                QueryPolicy {
                    max_object_requests: Some(2),
                    ..Default::default()
                },
                2,
            ),
        ] {
            counts.clear();
            assert!(read(
                SnapshotPageRequest {
                    max_rows: Some(512),
                    ..Default::default()
                },
                policy
            )
            .await
            .is_err());
            assert_eq!(counts.get_paths().len(), gets);
        }
        let policy = QueryPolicy {
            max_scan_files: Some(1),
            max_object_requests: Some(3),
            ..Default::default()
        };
        let first = read(
            SnapshotPageRequest {
                max_rows: Some(512),
                ..Default::default()
            },
            policy,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            first
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            512
        );
        let last = read(
            SnapshotPageRequest {
                max_rows: Some(512),
                page_token: first.next_page_token,
                ..Default::default()
            },
            policy,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            last.batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            88
        );
        assert!(last.next_page_token.is_none());
    }

    #[tokio::test]
    async fn native_filtered_metadata_is_budgeted_even_when_no_pages_match() {
        let (mut state, checkpoint, schema, p, _) =
            installed(DeltaBatch::from_records([row(json!(1.), 20)])).await;
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        let predicate = PagePredicate::Compare {
            column: "id".into(),
            op: PageComparison::Eq,
            value: PageScalar::Utf8("__missing".into()),
        };
        for (policy, gets) in [
            (
                QueryPolicy {
                    max_object_requests: Some(1),
                    ..QueryPolicy::default()
                },
                0,
            ),
            (
                QueryPolicy {
                    max_scan_bytes: Some(1),
                    ..QueryPolicy::default()
                },
                1,
            ),
            (
                QueryPolicy {
                    memory_limit_bytes: Some(1),
                    ..QueryPolicy::default()
                },
                1,
            ),
        ] {
            counts.clear();
            assert!(query_bound_selected(
                &state,
                &schema.relation_id,
                &checkpoint.identity,
                &schema.relation_id,
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest::default(),
                Some((&predicate, policy))
            )
            .await
            .is_err());
            assert_eq!(counts.get_paths().len(), gets);
        }
        let descriptor_bytes = super::super::checkpoint_publication::read_object_bytes_bounded(
            &state,
            &ObjectPath::from(key(&checkpoint).as_str()),
            None,
        )
        .await
        .unwrap()
        .len() as u64;
        counts.clear();
        let policy = QueryPolicy {
            max_scan_bytes: Some(descriptor_bytes + 1),
            ..QueryPolicy::default()
        };
        assert!(query_bound_selected(
            &state,
            &schema.relation_id,
            &checkpoint.identity,
            &schema.relation_id,
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default(),
            Some((&predicate, policy))
        )
        .await
        .is_err());
        assert_eq!(
            counts.get_paths().len(),
            2,
            "descriptor then manifest, no candidate page"
        );
        assert!(counts
            .get_paths()
            .contains(&p.manifest_key.as_str().to_owned()));
    }

    #[test]
    fn native_ipc_codec_reads_persisted_bytes_in_fresh_process() {
        const CHILD_PATH: &str = "VELORIX_NATIVE_IPC_TEST_PAGE";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            let bytes = std::fs::read(path).unwrap();
            let (record, batch) = decode_ipc(&bytes).unwrap();
            assert_eq!(record.row_count, 7);
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(record.page_content_hash, stable_bytes_hash(&bytes));
            assert_eq!(encode_page(&record).unwrap(), bytes);
            return;
        }
        let (checkpoint, schema) = fixture();
        let publication = publication(
            &checkpoint,
            &schema.relation_id,
            &key(&checkpoint),
            DeltaBatch::from_records([row(json!(2.5), 7)]),
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("page.arrow");
        std::fs::write(&path, encode_page(&publication.page_records[0].1).unwrap()).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("native_output_pages::tests::native_ipc_codec_reads_persisted_bytes_in_fresh_process")
            .arg("--nocapture")
            .env(CHILD_PATH, &path)
            .status().unwrap();
        assert!(status.success());
    }

    #[test]
    fn native_ipc_bytes_preserve_compact_bag_and_bind_typed_columns() {
        let (checkpoint, schema) = fixture();
        let p = publication(
            &checkpoint,
            &schema.relation_id,
            &key(&checkpoint),
            DeltaBatch::from_records([
                row(json!(2.5), 600),
                row(json!(2.5), -1),
                row(Value::Null, 20),
            ]),
        )
        .unwrap();
        for (_, page) in &p.page_records {
            let bytes = encode_page(page).unwrap();
            assert_eq!(stable_bytes_hash(&bytes), page.page_content_hash);
            let mut padded = bytes.clone();
            padded.push(0);
            assert!(decode_ipc(&padded).is_err());
            let mut reader =
                arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)
                    .unwrap();
            let persisted = reader.next().unwrap().unwrap();
            let mut metadata = persisted.schema().metadata().clone();
            metadata.insert("unbound".into(), "extra".into());
            let altered_metadata = RecordBatch::try_new(
                Arc::new(arrow::datatypes::Schema::new_with_metadata(
                    persisted.schema().fields().clone(),
                    metadata,
                )),
                persisted.columns().to_vec(),
            )
            .unwrap();
            let mut extra = Vec::new();
            {
                let mut writer = arrow::ipc::writer::StreamWriter::try_new(
                    &mut extra,
                    altered_metadata.schema().as_ref(),
                )
                .unwrap();
                writer.write(&altered_metadata).unwrap();
                writer.finish().unwrap();
            }
            assert!(decode_ipc(&extra).is_err());
            assert!(
                serde_json::from_slice::<Value>(&bytes).is_err(),
                "durable object is binary IPC"
            );
            let (record, batch) = decode_ipc(&bytes).unwrap();
            assert!(
                batch.num_rows() <= 2,
                "compact weighted rows, not physical expanded BAG"
            );
            assert_eq!(record.published_output, page.published_output);
            validate_page_binding(&p.manifest_record, &record).unwrap();
            let n = page.published_output["schema"]["columns"]
                .as_array()
                .unwrap()
                .len();
            let truncated = RecordBatch::try_new(
                Arc::new(arrow::datatypes::Schema::new(
                    batch.schema().fields()[..n].to_vec(),
                )),
                batch.columns()[..n].to_vec(),
            )
            .unwrap();
            assert!(decode_ipc(&ipc_bytes(page, &truncated, false).unwrap()).is_err());
            let mut corrupt = bytes.clone();
            let end = corrupt.len() - 16;
            corrupt[end] ^= 1;
            assert!(decode_ipc(&corrupt).is_err());
            let altered = RecordBatch::try_new(
                batch.schema(),
                vec![
                    batch.column(0).clone(),
                    Arc::new(arrow::array::Float64Array::from(vec![
                        Some(77.);
                        batch.num_rows()
                    ])),
                ],
            )
            .unwrap();
            assert_ne!(altered, batch);
            let mut compact = compact_batch(page).unwrap();
            let mut columns = compact.columns().to_vec();
            columns[1] = altered.column(1).clone();
            compact = RecordBatch::try_new(compact.schema(), columns).unwrap();
            assert!(
                decode_ipc(&ipc_bytes(page, &compact, false).unwrap()).is_err(),
                "typed column alteration is content-bound"
            );
        }
        let without_stats = publication_without_stats(
            &checkpoint,
            &schema.relation_id,
            &key(&checkpoint),
            DeltaBatch::from_records([row(json!(1.), 1)]),
        )
        .unwrap();
        assert_eq!(without_stats.manifest_record.output_encoding, BAG_CODEC);
        assert!(without_stats
            .manifest_record
            .published_output
            .get("page_stats")
            .is_none());
        assert!(decode_page(
            &encode_page(&without_stats.page_records[0].1).unwrap(),
            BAG_CODEC
        )
        .is_ok());
    }

    #[tokio::test]
    async fn native_full_validation_rejects_self_hashed_wrong_zone_maps() {
        let (state, mut checkpoint, _, mut p, _) =
            installed(DeltaBatch::from_records([row(json!(1.), 600)])).await;
        p.manifest_record.published_output["page_stats"][0][1]["max"] = json!({"Float":2.});
        rebind(&mut p);
        validate_manifest(&p.manifest_key, &p.manifest_record).unwrap();
        for (key, page) in &p.page_records {
            put_standing_runtime_output_page(&state, key, page)
                .await
                .unwrap();
        }
        put_standing_runtime_output_manifest(&state, &p.manifest_key, &p.manifest_record)
            .await
            .unwrap();
        checkpoint.output_manifest_refs = vec![format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            p.manifest_key.as_str()
        )];
        let record = StandingRuntimeCheckpointRecord {
            published_input_bootstrap: None,
            demand_materialization: None,
            schema_version: 1,
            record_kind: "standing_runtime_checkpoint_v1".into(),
            view_id: "purchases_by_user".into(),
            checkpoint_key: key(&checkpoint).as_str().into(),
            previous_checkpoint: None,
            checkpoint,
            replay_checkpoints: vec![],
            manifest_hash: String::new(),
        };
        assert!(
            validate_standing_runtime_checkpoint_output_manifest_records(&state, &record)
                .await
                .is_err()
        );
        for value in [json!([]), json!([[{"nulls":513,"min":null,"max":null}]])] {
            let mut manifest = p.manifest_record.clone();
            manifest.published_output["page_stats"] = value;
            manifest.output_content_hash = root_hash(&manifest).unwrap();
            let key = ObjectKey::standing_runtime_output_manifest(
                &manifest.tenant_id,
                &manifest.program_id,
                &manifest.view_id,
                manifest.logical_epoch,
                &manifest.output_content_hash,
            )
            .unwrap();
            assert!(validate_manifest(&key, &manifest).is_err());
        }
    }

    #[tokio::test]
    async fn native_restore_rejects_self_hashed_global_page_reordering() {
        let (state, mut checkpoint, schema, mut p, _) =
            installed(DeltaBatch::from_records([row(json!(1), 600)])).await;
        p.page_records[1].1.published_output["rows"]["records"][0]["value"]["power"] = json!(0);
        p.page_records[1].1.page_content_hash = page_hash(&p.page_records[1].1).unwrap();
        let page = &p.page_records[1].1;
        p.page_records[1].0 = ObjectKey::standing_runtime_output_page(
            &page.tenant_id,
            &page.program_id,
            &page.view_id,
            page.logical_epoch,
            page.page_index,
            &page.page_content_hash,
        )
        .unwrap();
        p.manifest_record.pages[1].page_key = p.page_records[1].0.as_str().into();
        p.manifest_record.pages[1].page_content_hash =
            p.page_records[1].1.page_content_hash.clone();
        let rows: PageRows =
            serde_json::from_value(p.page_records[1].1.published_output.clone()).unwrap();
        p.manifest_record.published_output["page_stats"][1] =
            serde_json::to_value(page_statistics(&schema, &rows.rows).unwrap()).unwrap();
        p.manifest_record.output_content_hash = root_hash(&p.manifest_record).unwrap();
        p.manifest_key = ObjectKey::standing_runtime_output_manifest(
            &p.manifest_record.tenant_id,
            &p.manifest_record.program_id,
            &p.manifest_record.view_id,
            p.manifest_record.logical_epoch,
            &p.manifest_record.output_content_hash,
        )
        .unwrap();
        for (key, page) in &mut p.page_records {
            page.output_content_hash = p.manifest_record.output_content_hash.clone();
            put_standing_runtime_output_page(&state, key, page)
                .await
                .unwrap();
        }
        put_standing_runtime_output_manifest(&state, &p.manifest_key, &p.manifest_record)
            .await
            .unwrap();
        checkpoint.output_manifest_refs = vec![format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            p.manifest_key.as_str()
        )];
        let record = StandingRuntimeCheckpointRecord {
            published_input_bootstrap: None,
            demand_materialization: None,
            schema_version: 1,
            record_kind: "standing_runtime_checkpoint_v1".into(),
            view_id: "purchases_by_user".into(),
            checkpoint_key: key(&checkpoint).as_str().into(),
            previous_checkpoint: None,
            checkpoint,
            replay_checkpoints: vec![],
            manifest_hash: String::new(),
        };
        let error = validate_standing_runtime_checkpoint_output_manifest_records(&state, &record)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("globally ordered"), "{error}");
    }

    #[tokio::test]
    async fn native_empty_page_is_typed_and_integrity_checked() {
        let (state, checkpoint, schema, p, _) = installed(DeltaBatch::default()).await;
        let page = query_bound(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(page.batches[0].num_columns(), 2);
        assert_eq!(page.batches[0].num_rows(), 0);
        assert!(page.next_page_token.is_none());
        state
            .store
            .put(
                &ObjectPath::from(p.page_records[0].0.as_str()),
                bytes::Bytes::from_static(b"corrupt-empty-page").into(),
            )
            .await
            .unwrap();
        assert!(query_bound(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default()
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn native_page_query_detects_head_change_after_selected_page_read() {
        let (mut state, checkpoint, schema, _, pointer) =
            installed(DeltaBatch::from_records([row(json!(1), 600)])).await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        state.store = Arc::new(
            crate::tests::CountingObjectStore::new(state.store.clone(), Default::default())
                .with_page_get_gate(entered.clone(), resume.clone()),
        );
        let query_state = state.clone();
        let identity = checkpoint.identity.clone();
        let task = tokio::spawn(async move {
            query_bound(
                &query_state,
                "purchases_by_user",
                &identity,
                "purchases_by_user",
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest {
                    max_rows: Some(1),
                    ..Default::default()
                },
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        let owner = state
            .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .unwrap();
        let mut candidate = pointer.clone();
        candidate.logical_epoch += 1;
        candidate.checkpoint_key = ObjectKey::standing_runtime_checkpoint(
            &candidate.tenant_id,
            &candidate.program_id,
            &candidate.view_id,
            candidate.logical_epoch,
            &candidate.content_hash,
        )
        .unwrap()
        .as_str()
        .into();
        candidate.previous_checkpoint_key = pointer.checkpoint_key.clone();
        candidate.previous_manifest_hash = pointer.manifest_hash.clone();
        candidate.manifest_hash = stable_bytes_hash(b"new-authoritative-head");
        candidate.output_manifest_refs.clear();
        assert_eq!(
            state
                .meta_store
                .as_ref()
                .unwrap()
                .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                    expected_previous: Some(pointer),
                    candidate,
                    owner,
                    expected_relation_source_cuts: None,
                })
                .await
                .unwrap(),
            PublishStandingRuntimeCheckpointOutcome::Published
        );
        resume.notify_one();
        assert_eq!(
            task.await.unwrap().unwrap_err().status,
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn native_page_query_preserves_bag_tokens_and_detects_selected_corruption() {
        let output = DeltaBatch::from_records([row(json!(2.5), 600), row(Value::Null, 20)]);
        let (mut state, checkpoint, schema, publication, _) = installed(output.clone()).await;
        assert!(read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "purchases_by_user"
        )
        .await
        .unwrap()
        .is_some());
        let expected =
            velorix_runtime::materialized_view_runtime::materialized_delta_to_page_with_checkpoint(
                &schema,
                &output,
                ScopedViewId {
                    tenant_id: checkpoint.identity.tenant_id.clone(),
                    program_id: checkpoint.identity.program_id.clone(),
                    view_id: schema.relation_id.clone(),
                },
                checkpoint.logical_epoch,
                SnapshotPageRequest::default(),
                None,
                &checkpoint,
            )
            .unwrap();
        let expected =
            record_batches_to_json_rows_for_view_schema(&schema, &expected.batches).unwrap();
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        let mut token = None;
        let mut count = 0;
        let mut values = Vec::new();
        loop {
            counts.clear();
            let page = query_bound(
                &state,
                "purchases_by_user",
                &checkpoint.identity,
                "purchases_by_user",
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest {
                    max_rows: Some(37),
                    page_token: token,
                    committed_epoch: Some(checkpoint.logical_epoch),
                },
            )
            .await
            .unwrap()
            .unwrap();
            count += page
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            values.extend(
                record_batches_to_json_rows_for_view_schema(&schema, &page.batches).unwrap(),
            );
            let paths = counts.get_paths();
            assert!(
                (3..=4).contains(&paths.len()),
                "only descriptor/index and intersecting pages are read: {paths:?}"
            );
            assert!(!paths
                .iter()
                .any(|path| path.contains("state-payload") || path.contains("output-deltas")));
            token = page.next_page_token;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(count, 620);
        assert_eq!(values, expected);
        let first = query_bound(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest {
                max_rows: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
        let token = first.next_page_token.unwrap();
        let mut at_end: Value = serde_json::from_str(&token).unwrap();
        at_end["offset"] = json!(620);
        let end = query_bound(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest {
                page_token: Some(at_end.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(end.batches[0].num_rows(), 0);
        assert!(end.next_page_token.is_none());
        for field in [
            "epoch", "root", "output", "schema", "tenant", "program", "offset", "codec",
        ] {
            let mut bad: Value = serde_json::from_str(&token).unwrap();
            bad[field] = if field == "epoch" || field == "offset" {
                json!(99999)
            } else {
                json!("other")
            };
            assert!(query_bound(
                &state,
                "purchases_by_user",
                &checkpoint.identity,
                "purchases_by_user",
                "generic_set_ops_v1",
                &schema,
                SnapshotPageRequest {
                    page_token: Some(bad.to_string()),
                    ..Default::default()
                }
            )
            .await
            .is_err());
        }
        for request in [
            SnapshotPageRequest {
                max_rows: Some(0),
                ..Default::default()
            },
            SnapshotPageRequest {
                committed_epoch: Some(0),
                ..Default::default()
            },
        ] {
            assert!(query_bound(
                &state,
                "purchases_by_user",
                &checkpoint.identity,
                "purchases_by_user",
                "generic_set_ops_v1",
                &schema,
                request
            )
            .await
            .is_err());
        }
        // An unselected corrupt page does not force a full-output read. Restore
        // validation still rejects it; requesting that page also rejects it.
        let path = ObjectPath::from(publication.page_records[1].0.as_str());
        state
            .store
            .put(&path, bytes::Bytes::from_static(b"corrupt").into())
            .await
            .unwrap();
        assert!(query_bound(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest {
                max_rows: Some(1),
                ..Default::default()
            }
        )
        .await
        .is_ok());
        assert!(read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "purchases_by_user"
        )
        .await
        .is_err());
        assert!(query_bound(
            &state,
            "purchases_by_user",
            &checkpoint.identity,
            "purchases_by_user",
            "generic_set_ops_v1",
            &schema,
            SnapshotPageRequest::default()
        )
        .await
        .is_err());
    }
}

//! Native materialized output pages. These are output-read proofs, not restore proofs.
use super::*;
use velorix_core::delta::encode_kv_ordered;
use velorix_core::query::{PageComparison, PagePredicate, PageScalar};

pub(super) const CODEC: &str = "velorix-materialized-arrow-pages-v2";
pub(super) const LEGACY_CODEC: &str = "velorix-delta-batch-json-v1";
pub(super) fn is_materialized_codec(codec: &str) -> bool {
    codec == CODEC
}
const PAGE_ROWS: usize = 512;
// Reuse the API state-body byte budget for the compact index; a conservative
// 1KiB/ref preflight bounds page expansion before any page bodies are allocated.
const MAX_INDEX_BYTES: usize = DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES;
const MAX_PAGES: usize = MAX_INDEX_BYTES / 1024;
const SOURCE: &str = "standing_runtime_checkpoint_published_output";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageIndex {
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
    schema_fingerprint: String,
    checkpoint_content_hash: String,
    rows: DeltaBatch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema: Option<RelationSchema>,
}

#[derive(Deserialize, Serialize)]
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

fn invalid() -> ApiError {
    ApiError::bad_request(
        "invalid native materialized output page identity, ordering or cardinality",
    )
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
    if record.output_encoding == CODEC {
        return ipc_hash(record, &compact_batch(record)?);
    }
    // Exclude the manifest root to avoid a circular root -> page -> root digest.
    // The root binds this page digest, and readers require the page's root equality.
    let bytes = serde_json::to_vec(&(
        LEGACY_CODEC,
        record.schema_version,
        &record.record_kind,
        &record.tenant_id,
        &record.program_id,
        &record.view_id,
        record.logical_epoch,
        record.page_index,
        record.row_count,
        &record.output_encoding,
        &record.source_kind,
        &record.published_output,
    ))
    .map_err(ApiError::internal)?;
    Ok(stable_bytes_hash(&bytes))
}

const IPC_HEADER: &str = "velorix.materialized.page";
const BAG_FIELDS: [&str; 3] = [
    "__velorix_key_json",
    "__velorix_value_json",
    "__velorix_weight",
];

fn compact_batch(record: &StandingRuntimeOutputPageRecord) -> Result<RecordBatch, ApiError> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let payload: PageRows =
        serde_json::from_value(record.published_output.clone()).map_err(ApiError::bad_request)?;
    let schema = payload.schema.as_ref().ok_or_else(invalid)?;
    if schema
        .columns
        .iter()
        .any(|c| BAG_FIELDS.contains(&c.name.as_str()))
    {
        return Err(invalid());
    }
    let rows = payload.rows.records();
    let single = DeltaBatch::from_records(rows.iter().map(|r| {
        let mut r = r.clone();
        r.weight = 1;
        r
    }));
    let batch = velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch(
        schema, &single,
    )
    .map_err(ApiError::bad_request)?;
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.extend(
        [
            Field::new(BAG_FIELDS[0], DataType::Utf8, false),
            Field::new(BAG_FIELDS[1], DataType::Utf8, false),
            Field::new(BAG_FIELDS[2], DataType::Int64, false),
        ]
        .into_iter()
        .map(Arc::new),
    );
    let keys = rows
        .iter()
        .map(|r| serde_json::to_string(r.key.as_json()).map_err(ApiError::internal))
        .collect::<Result<Vec<_>, _>>()?;
    let values = rows
        .iter()
        .map(|r| serde_json::to_string(r.value.as_json()).map_err(ApiError::internal))
        .collect::<Result<Vec<_>, _>>()?;
    let mut columns = batch.columns().to_vec();
    columns.extend([
        Arc::new(StringArray::from(keys)) as ArrayRef,
        Arc::new(StringArray::from(values)) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.weight))) as ArrayRef,
    ]);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(ApiError::bad_request)
}

fn ipc_bytes(
    record: &StandingRuntimeOutputPageRecord,
    batch: &RecordBatch,
    _hashing: bool,
) -> Result<Vec<u8>, ApiError> {
    use arrow::datatypes::Schema;
    let mut header = record.clone();
    header.published_output["rows"] =
        serde_json::to_value(DeltaBatch::default()).map_err(ApiError::internal)?;
    // The manifest and object key bind these fields outside the raw payload digest.
    header.output_content_hash.clear();
    header.page_content_hash.clear();
    let metadata = [(
        IPC_HEADER.into(),
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
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut bytes, batch.schema().as_ref())
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
    if record.output_encoding == CODEC {
        ipc_bytes(record, &compact_batch(record)?, false)
    } else {
        serde_json::to_vec(record).map_err(ApiError::internal)
    }
}

fn decode_ipc(bytes: &[u8]) -> Result<(StandingRuntimeOutputPageRecord, RecordBatch), ApiError> {
    use arrow::array::{Array, Int64Array, StringArray};
    use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};
    validate_ipc_frames(bytes)?;
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(ApiError::bad_request)?;
    let header = reader
        .schema()
        .metadata()
        .get(IPC_HEADER)
        .cloned()
        .ok_or_else(invalid)?;
    let mut record: StandingRuntimeOutputPageRecord =
        serde_json::from_str(&header).map_err(ApiError::bad_request)?;
    if record.output_encoding != CODEC
        || record.schema_version != 2
        || record.record_kind != "standing_runtime_materialized_output_page_v2"
        || record.source_kind != SOURCE
        || !record.output_content_hash.is_empty()
        || !record.page_content_hash.is_empty()
        || reader.schema().metadata().len() != 1
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
    if batch.num_columns() != n + 3 || batch.schema().fields()[..n] != output_schema.fields()[..] {
        return Err(invalid());
    }
    let keys = batch
        .column(n)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(invalid)?;
    let values = batch
        .column(n + 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(invalid)?;
    let weights = batch
        .column(n + 2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(invalid)?;
    if keys.null_count() != 0
        || values.null_count() != 0
        || weights.null_count() != 0
        || (0..3).any(|i| batch.schema().field(n + i).name() != BAG_FIELDS[i])
    {
        return Err(invalid());
    }
    payload.rows = DeltaBatch::from_records(
        (0..batch.num_rows())
            .map(|i| {
                let key = serde_json::from_str(keys.value(i)).map_err(ApiError::bad_request)?;
                let value = serde_json::from_str(values.value(i)).map_err(ApiError::bad_request)?;
                Ok(DeltaRecord::new(
                    DeltaKey::from_json(key),
                    DeltaValue::from_json(value),
                    weights.value(i),
                ))
            })
            .collect::<Result<Vec<_>, ApiError>>()?,
    );
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

fn validate_ipc_frames(mut bytes: &[u8]) -> Result<(), ApiError> {
    let mut messages = 0usize;
    loop {
        let prefix = bytes.get(..8).ok_or_else(invalid)?;
        if prefix[..4] != [255; 4] {
            return Err(invalid());
        }
        let length = u32::from_le_bytes(prefix[4..8].try_into().map_err(|_| invalid())?) as usize;
        bytes = &bytes[8..];
        if length == 0 {
            return if bytes.is_empty() && messages == 2 {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        let metadata = bytes.get(..length).ok_or_else(invalid)?;
        let message = arrow::ipc::root_as_message(metadata).map_err(ApiError::bad_request)?;
        if messages == 0 {
            if message.header_as_schema().is_none() {
                return Err(invalid());
            }
        } else if messages == 1 {
            let batch = message.header_as_record_batch().ok_or_else(invalid)?;
            if batch.length() < 0
                || batch.length() > PAGE_ROWS as i64
                || batch.compression().is_some()
            {
                return Err(invalid());
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
    if codec == CODEC {
        decode_ipc(bytes).map(|(record, _)| record)
    } else {
        serde_json::from_slice(bytes).map_err(ApiError::bad_request)
    }
}

fn decode_query_page(
    bytes: &[u8],
    manifest: &StandingRuntimeOutputManifestRecord,
    reference: &StandingRuntimeOutputPageRef,
) -> Result<(StandingRuntimeOutputPageRecord, Option<RecordBatch>), ApiError> {
    let (record, batch) = if reference.output_encoding == CODEC {
        let (r, b) = decode_ipc(bytes)?;
        (r, Some(b))
    } else {
        (decode_page(bytes, &reference.output_encoding)?, None)
    };
    let (key, parts) = ObjectKey::parse_standing_runtime_output_page(&reference.page_key)
        .map_err(ApiError::bad_request)?;
    if batch.is_none() {
        validate_page(&key, &record)?;
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
    max_bytes: Option<u64>,
) -> Result<(StandingRuntimeOutputPageRecord, Option<RecordBatch>, u64), ApiError> {
    read_query_page_with_decoder(state, manifest, reference, max_bytes, |bytes| {
        decode_query_page(bytes, manifest, reference)
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
    let mut index = PageIndex {
        schema: checkpoint_output_schema(checkpoint)?,
        page_stats: None,
    };
    if index.schema.relation_id != view_id {
        return Err(invalid());
    }
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
            schema_version: 2,
            record_kind: "standing_runtime_materialized_output_page_v2".into(),
            tenant_id: checkpoint.identity.tenant_id.clone(),
            program_id: checkpoint.identity.program_id.clone(),
            view_id: view_id.into(),
            logical_epoch: checkpoint.logical_epoch,
            output_content_hash: String::new(),
            page_index,
            page_content_hash: String::new(),
            row_count,
            output_encoding: CODEC.into(),
            source_kind: SOURCE.into(),
            published_output: serde_json::to_value(PageRows {
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
            output_encoding: CODEC.into(),
        });
        page_records.push((key, page));
    }
    let mut manifest = StandingRuntimeOutputManifestRecord {
        schema_version: 2,
        record_kind: "standing_runtime_materialized_output_manifest_v2".into(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.into(),
        checkpoint_key: checkpoint_key.as_str().into(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        output_content_hash: String::new(),
        output_encoding: CODEC.into(),
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
    let (version, kind) = (2, "standing_runtime_materialized_output_manifest_v2");
    if manifest.schema_version != version
        || manifest.record_kind != kind
        || !is_materialized_codec(&manifest.output_encoding)
        || manifest.source_kind != SOURCE
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

pub(super) fn validate_page(
    key: &ObjectKey,
    page: &StandingRuntimeOutputPageRecord,
) -> Result<(), ApiError> {
    let payload: PageRows =
        serde_json::from_value(page.published_output.clone()).map_err(ApiError::bad_request)?;
    let (_, parts) = ObjectKey::parse_standing_runtime_output_page(key.as_str())
        .map_err(ApiError::bad_request)?;
    let (version, kind) = (2, "standing_runtime_materialized_output_page_v2");
    if page.schema_version != version
        || page.record_kind != kind
        || !is_materialized_codec(&page.output_encoding)
        || page.source_kind != SOURCE
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
    velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch(
        &index.schema,
        &rows,
    )
    .map_err(ApiError::bad_request)?;
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
    if page.tenant_id != manifest.tenant_id
        || page.program_id != manifest.program_id
        || page.view_id != manifest.view_id
        || page.logical_epoch != manifest.logical_epoch
        || (page.output_content_hash != manifest.output_content_hash
            && !(page.output_encoding == CODEC && page.output_content_hash.is_empty()))
        || payload.schema_fingerprint != index.schema.schema_fingerprint
        || payload.checkpoint_content_hash != manifest.checkpoint_content_hash
        || (page.output_encoding == CODEC && payload.schema.as_ref() != Some(&index.schema))
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
    let expected = checkpoint_output_schema(checkpoint)?;
    let actual: PageIndex =
        serde_json::from_value(manifest.published_output.clone()).map_err(ApiError::bad_request)?;
    if expected != actual.schema {
        return Err(invalid());
    }
    Ok(())
}

pub(super) async fn query_with_policy(
    state: &ApiState,
    active: &ActiveMaterializedView,
    identity: &StandingProgramIdentity,
    output_id: &str,
    request: SnapshotPageRequest,
    policy: QueryPolicy,
) -> Result<Option<MaterializedViewPage>, ApiError> {
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

#[allow(clippy::too_many_arguments)]
async fn query_bound_selected(
    state: &ApiState,
    view_id: &str,
    identity: &StandingProgramIdentity,
    output_id: &str,
    schema: &RelationSchema,
    request: SnapshotPageRequest,
    filter: Option<(&PagePredicate, QueryPolicy)>,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    query_bound_selected_policy(
        state,
        view_id,
        identity,
        output_id,
        schema,
        request,
        filter,
        filter.map(|(_, policy)| policy).unwrap_or_default(),
    )
    .await
}

fn charge_request(policy: QueryPolicy, used: &mut usize) -> Result<(), ApiError> {
    *used = used.checked_add(1).ok_or_else(invalid)?;
    if policy.max_object_requests.is_some_and(|max| *used > max) {
        return Err(ApiError::bad_request(
            "materialized read exceeds object-request budget",
        ));
    }
    Ok(())
}

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
        if !path.ends_with(".checkpoint.json") {
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

#[allow(clippy::too_many_arguments)]
async fn query_bound_selected_policy(
    state: &ApiState,
    view_id: &str,
    identity: &StandingProgramIdentity,
    output_id: &str,
    schema: &RelationSchema,
    request: SnapshotPageRequest,
    filter: Option<(&PagePredicate, QueryPolicy)>,
    policy: QueryPolicy,
) -> Result<Option<MaterializedViewPage>, ApiError> {
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
    let mut metadata_requests = 0usize;
    let mut discovered_bytes = None;
    let pointer = if let Some(meta) = &state.meta_store {
        let Some(pointer) = meta
            .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view_id)
            .await
            .map_err(meta_error_to_api)?
        else {
            return Ok(None);
        };
        pointer
    } else {
        let Some((path, parts)) =
            discover_checkpoint_head(state, identity, view_id, policy, &mut metadata_requests)
                .await?
        else {
            return Ok(None);
        };
        charge_request(policy, &mut metadata_requests)?;
        let bytes = super::checkpoint_publication::read_object_bytes_bounded(
            state,
            &ObjectPath::from(path.clone()),
            metadata_budget(0).map(|n| n.min(16 * 1024 * 1024)),
        )
        .await?;
        let mut record =
            standing_runtime_checkpoint_record_from_slice(&bytes).map_err(ApiError::bad_request)?;
        if record.checkpoint_key.is_empty() {
            record.checkpoint_key = path.clone();
        }
        let mut pointer =
            super::checkpoint_publication::standing_runtime_checkpoint_pointer_from_record(&record);
        pointer.manifest_hash = stable_bytes_hash(&bytes);
        if pointer.checkpoint_key != path
            || pointer.logical_epoch != parts.logical_epoch
            || pointer.content_hash != parts.content_hash
        {
            return Err(invalid());
        }
        validate_standing_runtime_checkpoint_record(identity, view_id, &pointer, &record)?;
        discovered_bytes = Some(bytes);
        pointer
    };
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
                codec: "standing-runtime-checkpoint-json".into(),
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
        let Some(path) = matching.next() else {
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
    if index.schema != *schema {
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
    let offset = match request.page_token {
        None => 0,
        Some(token) => {
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
    if policy.max_scan_files.is_some_and(|max| files > max)
        || policy.max_object_requests.is_some_and(|max| {
            files
                .saturating_add(metadata_requests)
                .saturating_add(usize::from(state.meta_store.is_none()))
                > max
        })
    {
        return Err(ApiError::bad_request(
            "native read exceeds query file or object-request budget",
        ));
    }
    for (page_ref, candidate) in manifest.pages.iter().zip(candidates) {
        if !candidate {
            continue;
        }
        let count = page_ref.row_count;
        let stop = start.checked_add(count).ok_or_else(invalid)?;
        if (start < end && stop > offset) || (filter.is_none() && manifest.output_row_count == 0) {
            let budget = {
                let scan_remaining = policy.max_scan_bytes.map(|n| n.saturating_sub(scan_bytes));
                // JSON decoding, BAG copies and Arrow expansion need room before allocation.
                let memory_remaining = policy
                    .memory_limit_bytes
                    .map(|n| n.saturating_sub(retained_bytes) / 16);
                match (scan_remaining, memory_remaining) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                }
            };
            let (page, typed, byte_count) =
                read_query_page(state, &manifest, page_ref, budget).await?;
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
                velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch(
                    schema,
                    &DeltaBatch::from_records(selected),
                )
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
        batches.push(
            velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch(
                schema,
                &DeltaBatch::default(),
            )
            .map_err(ApiError::bad_request)?,
        );
    }
    // The descriptor and selected pages describe one exact head, never a mixture.
    let unchanged = if let Some(meta) = &state.meta_store {
        meta.read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view_id)
            .await
            .map_err(meta_error_to_api)?
            .as_ref()
            == Some(&pointer)
    } else {
        metadata_requests = metadata_requests.checked_add(files).ok_or_else(invalid)?;
        discover_checkpoint_head(state, identity, view_id, policy, &mut metadata_requests)
            .await?
            .is_some_and(|(path, parts)| {
                path == pointer.checkpoint_key
                    && parts.logical_epoch == pointer.logical_epoch
                    && parts.content_hash == pointer.content_hash
            })
    };
    if !unchanged {
        return Err(ApiError::conflict(
            "materialized output head changed during paging",
        ));
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

fn checkpoint_output_schema(checkpoint: &RuntimeCheckpoint) -> Result<RelationSchema, ApiError> {
    let payload = checkpoint.state_payload.as_ref().ok_or_else(invalid)?;
    let value: Value = serde_json::from_str(&payload.payload).map_err(ApiError::bad_request)?;
    serde_json::from_value(value.get("output_schema").cloned().ok_or_else(invalid)?)
        .map_err(ApiError::bad_request)
}

/// Normalize only the checkpoint's materialized output using its admitted projection.
pub(super) fn checkpoint_publication(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    key: &ObjectKey,
) -> Result<Option<StandingRuntimeOutputPublication>, ApiError> {
    let Some(value) =
        super::checkpoint_publication::standing_runtime_checkpoint_published_output(checkpoint)
    else {
        return Ok(None);
    };
    let payload: Value = serde_json::from_str(
        &checkpoint
            .state_payload
            .as_ref()
            .ok_or_else(invalid)?
            .payload,
    )
    .map_err(ApiError::bad_request)?;
    if payload.get("output_schema").is_none() {
        // Historical checkpoint-only outputs have no declared public schema.
        // Keep their existing reader rather than assigning an inferred page schema.
        return Ok(None);
    }
    let schema = checkpoint_output_schema(checkpoint)?;
    let output: DeltaBatch = serde_json::from_value(value).map_err(ApiError::bad_request)?;
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
    if total.div_ceil(PAGE_ROWS).max(1) > MAX_PAGES {
        return Err(invalid());
    }
    let mut normalized = Vec::new();
    // Each normalization batch is bounded; the existing converter supplies aliases,
    // aggregate values and composite keys exactly as the public runtime query does.
    for row in canonical {
        {
            let copies = 1;
            let mut part = row.clone();
            part.weight = 1;
            let page = velorix_runtime::materialized_view_runtime::materialized_delta_to_page(
                &schema,
                &DeltaBatch::from_records([part]),
                ScopedViewId {
                    tenant_id: checkpoint.identity.tenant_id.clone(),
                    program_id: checkpoint.identity.program_id.clone(),
                    view_id: view_id.into(),
                },
                checkpoint.logical_epoch,
                SnapshotPageRequest {
                    max_rows: Some(copies),
                    ..Default::default()
                },
                aggregates.as_deref(),
            )
            .map_err(ApiError::bad_request)?;
            let mut converted = Vec::new();
            for batch in page.batches {
                for i in 0..batch.num_rows() {
                    let mut value = serde_json::Map::new();
                    for (column, array) in schema.columns.iter().zip(batch.columns()) {
                        let scalar = arrow_value_to_json(array, i)?;
                        value.insert(column.name.clone(), scalar);
                    }
                    converted.push(velorix_core::delta::DeltaRecord::new(
                        row.key.clone(),
                        velorix_core::delta::DeltaValue::from_json(Value::Object(value)),
                        row.weight,
                    ));
                }
            }
            if converted.len() != copies {
                return Err(invalid());
            }
            normalized.extend(
                DeltaBatch::from_records(converted)
                    .net_rows()
                    .map_err(ApiError::bad_request)?,
            );
        }
    }
    publication(
        checkpoint,
        view_id,
        key,
        DeltaBatch::from_records(normalized),
    )
    .map(Some)
}

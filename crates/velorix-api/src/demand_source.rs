//! Authenticated physical publications for demand replay. No input is applied here.
use super::*;
use futures::StreamExt;

const MAX_ARTIFACT_BYTES: usize = 512 * 1024 * 1024;

fn decoded_size_overflow() -> ApiError {
    ApiError::bad_request("decoded source size overflow")
}

fn account_buffer(
    buffer: &arrow::buffer::Buffer,
    allocations: &mut BTreeMap<usize, usize>,
    total: &mut usize,
) -> Result<(), ApiError> {
    // Foreign buffers have no capacity; include the visible extent from their base.
    let extent = buffer
        .ptr_offset()
        .checked_add(buffer.len())
        .ok_or_else(decoded_size_overflow)?;
    let size = buffer.capacity().max(extent);
    let previous = allocations
        .entry(buffer.data_ptr().as_ptr() as usize)
        .or_default();
    if size > *previous {
        *total = total
            .checked_add(size - *previous)
            .ok_or_else(decoded_size_overflow)?;
        *previous = size;
    }
    Ok(())
}

fn account_array_data(
    data: &arrow::array::ArrayData,
    allocations: &mut BTreeMap<usize, usize>,
    total: &mut usize,
) -> Result<(), ApiError> {
    // Charge bounded metadata per node, never the arrays' recursive buffer capacities.
    let handles = data
        .buffers()
        .len()
        .checked_mul(std::mem::size_of::<arrow::buffer::Buffer>())
        .ok_or_else(decoded_size_overflow)?;
    *total = total
        .checked_add(std::mem::size_of::<arrow::array::ArrayData>())
        .and_then(|size| size.checked_add(handles))
        .ok_or_else(decoded_size_overflow)?;
    for buffer in data.buffers() {
        account_buffer(buffer, allocations, total)?;
    }
    if let Some(nulls) = data.nulls() {
        account_buffer(nulls.buffer(), allocations, total)?;
    }
    for child in data.child_data() {
        account_array_data(child, allocations, total)?;
    }
    Ok(())
}

fn decoded_source_size(batches: &[RecordBatch]) -> Result<usize, ApiError> {
    let mut allocations = BTreeMap::new();
    let mut total = 0usize;
    for batch in batches {
        total = total
            .checked_add(std::mem::size_of::<RecordBatch>())
            .ok_or_else(decoded_size_overflow)?;
        for array in batch.columns() {
            total = total
                .checked_add(std::mem::size_of_val(array.as_ref()))
                .and_then(|size| size.checked_add(std::mem::size_of::<arrow::array::ArrayRef>()))
                .ok_or_else(decoded_size_overflow)?;
            account_array_data(&array.to_data(), &mut allocations, &mut total)?;
        }
    }
    Ok(total)
}

fn validate_decoded_source_budget(
    batches: &[RecordBatch],
    fetched: usize,
    max_bytes: usize,
) -> Result<(), ApiError> {
    let decoded = decoded_source_size(batches)?;
    if decoded > max_bytes.min(MAX_ARTIFACT_BYTES) {
        return Err(budget_error(
            decoded.max(fetched) as u64,
            max_bytes,
            "decoded_acceptance",
            false,
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandSourceArtifact {
    pub(super) relation: IngestSourceRelationIdentityV1,
    pub(super) stream_id: String,
    pub(super) partition_id: u32,
    pub(super) publication: RelationIngestPublicationRefV1,
}

fn validate_artifact(artifact: &DemandSourceArtifact) -> Result<(), ApiError> {
    let p = &artifact.publication;
    let (_, parts) =
        ObjectKey::parse_ingest_staging(&p.object_key).map_err(ApiError::bad_request)?;
    let expected = ObjectKey::ingest_batch(
        &artifact.stream_id,
        artifact.partition_id,
        p.start_offset_inclusive,
        p.end_offset_exclusive,
    )
    .map_err(ApiError::bad_request)?;
    if artifact.relation.relation_id.is_empty()
        || artifact.relation.relation_version.is_empty()
        || artifact.relation.relation_generation != INGEST_SOURCE_IDENTITY_GENERATION_V1
        || artifact.relation.schema_fingerprint.is_empty()
        || p.start_offset_inclusive >= p.end_offset_exclusive
        || parts.stream_id != artifact.stream_id
        || parts.partition_id != artifact.partition_id
        || parts.start_offset_inclusive != p.start_offset_inclusive
        || parts.end_offset_exclusive != p.end_offset_exclusive
        || parts.staging_id != p.request_id
        || expected.as_str() != p.batch_key
        || p.object_digest != p.payload_digest
        || !p
            .object_digest
            .strip_prefix("sha256:")
            .is_some_and(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err(ApiError::bad_request(
            "invalid demand source artifact identity",
        ));
    }
    Ok(())
}

pub(super) fn demand_source_artifact_key(
    artifact: &DemandSourceArtifact,
) -> Result<String, ApiError> {
    validate_artifact(artifact)?;
    let bytes =
        serde_json::to_vec(&("velorix-demand-source-v1", artifact)).map_err(ApiError::internal)?;
    Ok(stable_bytes_hash(&bytes))
}

pub(super) async fn capture_demand_source_artifacts(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<
    (
        Vec<RelationIngestSourceIdentityCutV1>,
        Vec<DemandSourceArtifact>,
    ),
    ApiError,
> {
    let relations = active
        .spec
        .input_relations
        .iter()
        .filter(|input| {
            !active.runtime.as_ref().is_some_and(|runtime| {
                runtime.input_bindings.iter().any(|binding| {
            matches!(binding, StandingInputBindingV1::PublishedView { published_relation, .. }
                if published_relation.relation.relation_id == input.relation_id
                && published_relation.relation.relation_version == input.relation_version)
        })
            })
        })
        .map(|input| IngestSourceRelationIdentityV1 {
            relation_id: input.relation_id.clone(),
            relation_version: input.relation_version.clone(),
            relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
            schema_fingerprint: input.schema_fingerprint.clone(),
        })
        .collect::<Vec<_>>();
    if relations.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let config = state.relation_ingest_config.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("authoritative ingest configuration unavailable")
    })?;
    let meta = state
        .meta_store
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("authoritative metadata unavailable"))?;
    let cuts = meta
        .capture_relation_ingest_source_cuts(CaptureRelationIngestSourceCutsRequest {
            namespace: config.namespace.clone(),
            relations: relations.clone(),
        })
        .await
        .map_err(meta_error_to_api)?;
    if cuts.len() != relations.len() {
        return Err(ApiError::bad_request("source cut count mismatch"));
    }
    let mut seen = BTreeSet::new();
    let mut artifacts = Vec::new();
    for cut in &cuts {
        if !relations.contains(&cut.relation)
            || !seen.insert(cut.relation.relation_id.clone())
            || cut.cut.namespace != config.namespace
            || cut.cut.relation_id != cut.relation.relation_id
            || cut.cut.schema_version != RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1
        {
            return Err(ApiError::bad_request("source cut identity mismatch"));
        }
        let mut partitions = BTreeSet::new();
        for partition in &cut.cut.partitions {
            if !partitions.insert((&partition.stream_id, partition.partition_id)) {
                return Err(ApiError::bad_request("duplicate source partition"));
            }
            let mut frontier = partition.base_offset_inclusive;
            for publication in &partition.publications {
                if publication.start_offset_inclusive != frontier {
                    return Err(ApiError::bad_request("noncontiguous source publications"));
                }
                let artifact = DemandSourceArtifact {
                    relation: cut.relation.clone(),
                    stream_id: partition.stream_id.clone(),
                    partition_id: partition.partition_id,
                    publication: publication.clone(),
                };
                validate_artifact(&artifact)?;
                frontier = publication.end_offset_exclusive;
                artifacts.push(artifact);
            }
            if frontier != partition.committed_offset_exclusive {
                return Err(ApiError::bad_request("source frontier mismatch"));
            }
        }
    }
    Ok((cuts, artifacts))
}

fn budget_error(required: u64, supplied: usize, phase: &str, lower_bound: bool) -> ApiError {
    ApiError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        message: "demand source requires a larger whole-artifact verification budget".into(),
        details: Some(
            serde_json::json!({"code":"DEMAND_SOURCE_BUDGET_REQUIRED", "required_min_bytes":required, "supplied_max_bytes":supplied, "max_supported_bytes":MAX_ARTIFACT_BYTES, "budget_phase":phase, "required_min_bytes_is_lower_bound":lower_bound, "retry_with_larger_budget_possible":required <= MAX_ARTIFACT_BYTES as u64}),
        ),
        retryable_materialization: false,
    }
}

pub(super) async fn load_demand_source_artifact(
    state: &ApiState,
    artifact: &DemandSourceArtifact,
    max_bytes: usize,
) -> Result<
    (
        RelationInputBatch,
        usize,
        recovery::ValidatedRelationPublication,
    ),
    ApiError,
> {
    validate_artifact(artifact)?;
    let limit = max_bytes.min(MAX_ARTIFACT_BYTES);
    let path = ObjectPath::from(artifact.publication.object_key.clone());
    // shortcut: source envelopes need publication/schema proof, not checkpoint hashes; revisit with a proof-bound source cache.
    let head = state.store.head(&path).await.map_err(ApiError::internal)?;
    if head.size > limit as u64 {
        return Err(budget_error(head.size, max_bytes, "serialized_read", true));
    }
    let object = state.store.get(&path).await.map_err(ApiError::internal)?;
    if object.meta.size != head.size {
        return Err(ApiError::bad_request(
            "source object size changed after HEAD",
        ));
    }
    let mut stream = object.into_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ApiError::internal)?;
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| ApiError::bad_request("source read overflow"))?;
        if next > limit || next as u64 > head.size {
            return Err(budget_error(
                next as u64,
                max_bytes,
                "serialized_read",
                true,
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != head.size {
        return Err(ApiError::bad_request("source object length mismatch"));
    }
    let fetched = bytes.len();
    let (envelope, proof) = recovery::validate_demand_source_payload(artifact, bytes.into())?;
    let h = envelope.header();
    let batches = envelope.record_batches().map_err(ApiError::bad_request)?;
    validate_decoded_source_budget(&batches, fetched, max_bytes)?;
    Ok((
        RelationInputBatch {
            encoding: RelationInputEncodingV1::SourceRelationV1,
            relation_id: h.relation_id.clone(),
            relation_version: h.relation_version.clone(),
            stream_id: h.stream_id.clone(),
            partition_id: h.partition_id,
            schema_fingerprint: h.schema_fingerprint.clone(),
            start_offset_inclusive: h.start_offset_inclusive,
            end_offset_exclusive: h.end_offset_exclusive,
            event_time_watermark: h.event_time_watermark.clone(),
            batches,
        },
        fetched,
        proof,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared_columns_ipc() -> bytes::Bytes {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let values = Arc::new(StringArray::from(
            (0..128)
                .map(|i| (i % 3 != 0).then(|| "x".repeat(256)))
                .collect::<Vec<_>>(),
        ));
        let schema = Arc::new(Schema::new(
            (0..4)
                .map(|i| Field::new(format!("column_{i}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![values.clone(), values.clone(), values.clone(), values],
        )
        .unwrap();
        IngestEnvelope::encode_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: "events".into(),
                relation_version: "v1".into(),
                schema_fingerprint: format!("sha256:{}", "a".repeat(64)),
                stream_id: "stream".into(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 128,
                event_time_watermark: None,
            },
            &[batch],
        )
        .unwrap()
    }

    #[test]
    fn decoded_budget_deduplicates_ipc_columns_and_sliced_batches() {
        let bytes = shared_columns_ipc();
        let budget = bytes.len() * 2;
        let batches = IngestEnvelope::decode(bytes.clone())
            .unwrap()
            .record_batches()
            .unwrap();
        assert!(
            batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>()
                > budget
        );
        validate_decoded_source_budget(&batches, bytes.len(), budget).unwrap();
        let slices = vec![batches[0].slice(0, 64), batches[0].slice(64, 64)];
        validate_decoded_source_budget(&slices, bytes.len(), budget).unwrap();
        assert_eq!(slices.iter().map(RecordBatch::num_rows).sum::<usize>(), 128);
        let size = decoded_source_size(&slices).unwrap();
        validate_decoded_source_budget(&slices, bytes.len(), size).unwrap();
        assert_eq!(
            validate_decoded_source_budget(&slices, bytes.len(), size - 1)
                .unwrap_err()
                .status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn decoded_budget_rejects_independent_ipc_allocations() {
        let bytes = shared_columns_ipc();
        let mut batches = Vec::new();
        for _ in 0..3 {
            batches.extend(
                IngestEnvelope::decode(bytes.clone())
                    .unwrap()
                    .record_batches()
                    .unwrap(),
            );
        }
        let error =
            validate_decoded_source_budget(&batches, bytes.len(), bytes.len() * 2).unwrap_err();
        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
        let details = error.details.unwrap();
        assert_eq!(details["budget_phase"], "decoded_acceptance");
        assert_eq!(
            details["required_min_bytes"],
            decoded_source_size(&batches).unwrap()
        );
    }

    #[test]
    fn nested_struct_accounts_shared_child_and_null_bitmaps_once() {
        use arrow::array::{Array, ArrayData, Int64Array, StructArray};
        use arrow::buffer::{Buffer, NullBuffer};
        use arrow::datatypes::{DataType, Field};

        let child = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let parent_nulls = NullBuffer::from(vec![true, true, false]);
        let child_data = child.to_data();
        let expected = [
            &child_data.buffers()[0],
            child_data.nulls().unwrap().buffer(),
            parent_nulls.buffer(),
        ]
        .into_iter()
        .map(|buffer| (buffer.data_ptr().as_ptr() as usize, buffer.capacity()))
        .collect::<BTreeMap<_, _>>();
        assert_eq!(expected.len(), 3);
        assert!(expected.values().all(|size| *size > 0));
        let nested = StructArray::new(
            vec![
                Field::new("left", DataType::Int64, true),
                Field::new("right", DataType::Int64, true),
            ]
            .into(),
            vec![child.clone(), child],
            Some(parent_nulls),
        );
        let mut allocations = BTreeMap::new();
        let mut total = 0;
        account_array_data(&nested.to_data(), &mut allocations, &mut total).unwrap();
        assert_eq!(allocations, expected);
        // Three array nodes and two child buffer handles; shared payloads count once.
        let metadata = 3 * std::mem::size_of::<ArrayData>() + 2 * std::mem::size_of::<Buffer>();
        assert_eq!(total, metadata + expected.values().sum::<usize>());
    }

    #[test]
    fn foreign_buffer_slices_account_for_visible_extent() {
        let mut storage = vec![0u8; 128];
        let pointer = std::ptr::NonNull::new(storage.as_mut_ptr()).unwrap();
        let length = storage.len();
        // SAFETY: pointer covers length initialized bytes; the moved Vec owns the
        // stable allocation until the last Buffer/slice drops, with no mutable alias.
        let buffer = unsafe {
            arrow::buffer::Buffer::from_custom_allocation(pointer, length, Arc::new(storage))
        };
        // Arrow 59 reports the supplied length as custom allocation capacity.
        assert_eq!(buffer.capacity(), length);
        let mut allocations = BTreeMap::new();
        let mut total = 0;
        account_buffer(
            &buffer.slice_with_length(32, 16),
            &mut allocations,
            &mut total,
        )
        .unwrap();
        assert_eq!(total, length);
        account_buffer(
            &buffer.slice_with_length(64, 32),
            &mut allocations,
            &mut total,
        )
        .unwrap();
        assert_eq!(total, length);
        account_buffer(
            &buffer.slice_with_length(32, 16),
            &mut allocations,
            &mut total,
        )
        .unwrap();
        assert_eq!(total, length);
        assert_eq!(allocations.len(), 1);
    }

    #[test]
    fn decoded_buffer_size_overflow_fails_closed() {
        let buffer = arrow::buffer::Buffer::from_slice_ref([1u8]);
        let mut allocations = BTreeMap::new();
        let mut total = usize::MAX;
        assert!(account_buffer(&buffer, &mut allocations, &mut total).is_err());
        assert_eq!(total, usize::MAX);
    }

    fn artifact() -> DemandSourceArtifact {
        let digest = format!("sha256:{}", "a".repeat(64));
        DemandSourceArtifact {
            relation: IngestSourceRelationIdentityV1 {
                relation_id: "events".into(),
                relation_version: "v1".into(),
                relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                schema_fingerprint: digest.clone(),
            },
            stream_id: "stream".into(),
            partition_id: 0,
            publication: RelationIngestPublicationRefV1 {
                request_id: "request".into(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                batch_key: ObjectKey::ingest_batch("stream", 0, 0, 2)
                    .unwrap()
                    .as_str()
                    .into(),
                object_key: ObjectKey::ingest_staging("stream", 0, 0, 2, "request")
                    .unwrap()
                    .as_str()
                    .into(),
                payload_digest: digest.clone(),
                object_digest: digest,
            },
        }
    }

    #[test]
    fn key_binds_relation_schema_and_digest() {
        let original = artifact();
        let key = demand_source_artifact_key(&original).unwrap();
        let mut changed = original.clone();
        changed.relation.relation_id = "other".into();
        assert_ne!(key, demand_source_artifact_key(&changed).unwrap());
        changed = original.clone();
        changed.relation.schema_fingerprint = format!("sha256:{}", "b".repeat(64));
        assert_ne!(key, demand_source_artifact_key(&changed).unwrap());
        changed = original;
        changed.publication.object_digest = format!("sha256:{}", "c".repeat(64));
        changed.publication.payload_digest = changed.publication.object_digest.clone();
        assert_ne!(key, demand_source_artifact_key(&changed).unwrap());
    }

    #[test]
    fn mismatched_publication_identity_fails_closed() {
        let mut changed = artifact();
        changed.publication.request_id = "other".into();
        assert!(demand_source_artifact_key(&changed).is_err());
        changed = artifact();
        changed.partition_id = 1;
        assert!(demand_source_artifact_key(&changed).is_err());
        changed = artifact();
        changed.publication.end_offset_exclusive = 3;
        assert!(demand_source_artifact_key(&changed).is_err());
    }
    #[test]
    fn required_budget_is_structured_and_not_retryable() {
        let error = budget_error(100, 99, "serialized_read", true);
        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(!error.retryable_materialization);
        assert_eq!(error.details.unwrap()["required_min_bytes"], 100);
        let unsupported = budget_error(
            MAX_ARTIFACT_BYTES as u64 + 1,
            99,
            "decoded_acceptance",
            false,
        );
        assert_eq!(
            unsupported.details.unwrap()["retry_with_larger_budget_possible"],
            false
        );
    }
}

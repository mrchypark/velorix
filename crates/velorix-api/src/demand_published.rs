//! Budgeted immutable published-input inventory. No producer state hydration.
//! Metadata inventory does not fetch pages to learn signed-record counts.
use super::checkpoint_publication as publication;
use super::view_dependencies::ConsumerViewDependencyInputV1;
use super::*;

/// Retained request-local proof: every header was hash-bound to its predecessor
/// from this authoritative head. Payload validation happens only on selection.
#[derive(Clone, Debug)]
struct CapturedProof {
    head: StandingRuntimeCheckpointPointer,
    lineage: Vec<StandingRuntimeCheckpointRecord>,
}

#[derive(Clone, Debug)]
enum ArtifactPayload {
    /// The consumer's authenticated checkpoint already includes this baseline.
    /// The saved bootstrap entry supplies its persisted summary, while the
    /// matching input frontier ensures this marker can only be skipped.
    CoveredBaseline {
        manifest_ref: String,
    },
    Empty {
        manifest_ref: String,
    },
    Page {
        manifest: Arc<StandingRuntimeOutputManifestRecord>,
        page: StandingRuntimeOutputPageRef,
        previous: Option<StandingRuntimeOutputPageRef>,
    },
    Delta {
        object_key: String,
    },
}

#[derive(Clone, Debug)]
pub(super) struct DemandPublishedArtifact {
    pub(super) edge: ViewDependencyEdgeV1,
    pub(super) binding: PublishedRelationBindingV1,
    pub(super) cursor: CausalViewCursorV1,
    baseline_summary: Option<Arc<(String, String, u64, u64)>>,
    proof: Arc<CapturedProof>,
    header_index: usize,
    payload: ArtifactPayload,
}

/// Exact request-local identity for coalescing adjacent positive baseline
/// pages. The serialized binding and cursor avoid relying on a relation name
/// or a partial subset of the producer lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PositiveBaselinePageGroup {
    edge_id: String,
    manifest_ref: String,
    manifest_hash: String,
    manifest_pages: u64,
    manifest_rows: u64,
    binding_and_cursor: Vec<u8>,
}

impl DemandPublishedArtifact {
    pub(super) fn is_baseline(&self) -> bool {
        !matches!(&self.payload, ArtifactPayload::Delta { .. })
    }

    /// Manifest reference, canonical manifest hash, page count, and expanded
    /// materialized multiplicity. The last value is NOT a sparse ordinal count.
    pub(super) fn baseline_manifest_summary(&self) -> Option<(&str, &str, u64, u64)> {
        self.baseline_summary
            .as_deref()
            .map(|summary| (summary.0.as_str(), summary.1.as_str(), summary.2, summary.3))
    }

    pub(super) fn positive_baseline_page_group(
        &self,
    ) -> Result<Option<PositiveBaselinePageGroup>, ApiError> {
        if !matches!(&self.payload, ArtifactPayload::Page { .. }) {
            return Ok(None);
        }
        let Some((manifest_ref, manifest_hash, manifest_pages, manifest_rows)) =
            self.baseline_manifest_summary()
        else {
            return Err(invalid("published page is missing its manifest identity"));
        };
        Ok(Some(PositiveBaselinePageGroup {
            edge_id: self.edge.edge_id.clone(),
            manifest_ref: manifest_ref.to_owned(),
            manifest_hash: manifest_hash.to_owned(),
            manifest_pages,
            manifest_rows,
            binding_and_cursor: serde_json::to_vec(&(&self.binding, &self.cursor))
                .map_err(ApiError::internal)?,
        }))
    }
}

fn invalid(message: &str) -> ApiError {
    ApiError::bad_request(message)
}

fn required_budget(required: u64, supplied: usize) -> ApiError {
    ApiError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        message: "published input requires a larger authenticated-object budget".into(),
        details: Some(json!({"code":"DEMAND_PUBLISHED_BUDGET_REQUIRED",
            "required_min_bytes":required,"supplied_max_bytes":supplied})),
        retryable_materialization: false,
    }
}

struct ReadBudget {
    supplied: usize,
    used: usize,
}
impl ReadBudget {
    async fn read(&mut self, state: &ApiState, key: &str) -> Result<Vec<u8>, ApiError> {
        let path = ObjectPath::from(key);
        let size = match publication::cached_checkpoint_size(state, &path).await {
            Some(size) => size,
            None => {
                state
                    .store
                    .head(&path)
                    .await
                    .map_err(ApiError::internal)?
                    .size
            }
        };
        let required = (self.used as u64)
            .checked_add(size)
            .ok_or_else(|| invalid("published read size overflow"))?;
        if required > self.supplied as u64 {
            return Err(required_budget(required, self.supplied));
        }
        let bytes = publication::read_object_bytes_bounded(state, &path, Some(size)).await?;
        if bytes.len() as u64 != size {
            return Err(invalid("published object size changed"));
        }
        self.used = self
            .used
            .checked_add(bytes.len())
            .ok_or_else(|| invalid("published budget overflow"))?;
        Ok(bytes)
    }
}

fn validate_input(input: &ConsumerViewDependencyInputV1) -> Result<(), ApiError> {
    validate_published_relation_binding_v1(&input.binding).map_err(ApiError::bad_request)?;
    input
        .bootstrap_cursor
        .validate()
        .map_err(ApiError::bad_request)?;
    let e = &input.edge;
    let b = &input.binding;
    let c = &input.bootstrap_cursor;
    if e.edge_id != c.input_edge
        || e.tenant_id != c.producer_tenant_id
        || e.producer_program_id != c.producer_program_id
        || e.producer_view_id != b.producer_view_id
        || c.producer_view_id != b.producer_view_id
        || e.producer_generation != b.producer_view_generation
        || c.producer_generation != b.producer_view_generation
        || e.producer_plan_hash != b.producer_plan_hash
        || e.output_stream_id != b.output_stream_id
        || c.output_stream != b.output_stream_id
        || e.input_relation_id != b.relation.relation_id
        || e.input_relation_version != b.relation.relation_version
        || e.output_schema_hash != b.output_schema_hash
        || e.key_descriptor_hash != b.key_descriptor_hash
        || e.delta_codec_identity != b.delta_codec_identity
        || e.frontier_kind != b.frontier_kind
    {
        return Err(invalid("published artifact edge/binding/cursor mismatch"));
    }
    Ok(())
}

async fn capture_proof(
    state: &ApiState,
    input: &ConsumerViewDependencyInputV1,
    budget: &mut ReadBudget,
) -> Result<CapturedProof, ApiError> {
    validate_input(input)?;
    let c = &input.bootstrap_cursor;
    // Metadata authority is required; never discover an unauthenticated head by LIST.
    let meta = state
        .meta_store
        .as_ref()
        .ok_or_else(|| invalid("published demand requires authoritative metadata"))?;
    let head = meta
        .read_standing_runtime_checkpoint(
            &c.producer_tenant_id,
            &c.producer_program_id,
            &c.producer_view_id,
        )
        .await
        .map_err(meta_error_to_api)?
        .ok_or_else(|| invalid("published demand producer checkpoint missing"))?;
    let mut pointer = head.clone();
    let mut lineage = Vec::new();
    loop {
        if lineage.len() >= 4096
            || pointer.logical_epoch < c.output_epoch
            || pointer.output_manifest_refs.len() > 64
        {
            return Err(invalid(
                "published demand lineage exceeds bounds or misses captured cursor",
            ));
        }
        if pointer.tenant_id != c.producer_tenant_id
            || pointer.program_id != c.producer_program_id
            || pointer.view_id != c.producer_view_id
        {
            return Err(invalid("published head scope mismatch"));
        }
        let bytes = budget.read(state, &pointer.checkpoint_key).await?;
        if stable_bytes_hash(&bytes) != pointer.manifest_hash {
            return Err(invalid("published header commitment mismatch"));
        }
        let mut record =
            standing_runtime_checkpoint_record_from_slice(&bytes).map_err(ApiError::bad_request)?;
        if record.checkpoint_key.is_empty() {
            record.checkpoint_key = pointer.checkpoint_key.clone();
        }
        publication::validate_standing_runtime_checkpoint_record(
            &record.checkpoint.identity,
            &c.producer_view_id,
            &pointer,
            &record,
        )?;
        publication::validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
        let digest = published_header_digest(&record, &input.binding)?;
        let reached = pointer.logical_epoch == c.output_epoch;
        if reached && digest.as_deref() != Some(c.commit_digest.as_str()) {
            return Err(invalid("captured cursor digest mismatch"));
        }
        let previous = record.previous_checkpoint.clone();
        lineage.push(record);
        if reached {
            break;
        }
        let previous = previous.ok_or_else(|| invalid("captured cursor is absent from lineage"))?;
        if previous.logical_epoch >= pointer.logical_epoch {
            return Err(invalid("non-decreasing published lineage"));
        }
        pointer = previous;
    }
    lineage.reverse();
    Ok(CapturedProof { head, lineage })
}

/// Partial demand checkpoints authenticate lineage but publish no consumer
/// commit. Never derive a cursor from their speculative output or coverage.
fn published_header_digest(
    header: &StandingRuntimeCheckpointRecord,
    binding: &PublishedRelationBindingV1,
) -> Result<Option<String>, ApiError> {
    let partial = header
        .published_input_bootstrap
        .as_ref()
        .is_some_and(|p| !p.is_complete())
        || header
            .demand_materialization
            .as_ref()
            .is_some_and(|p| !p.full_complete);
    let has_commit = header
        .checkpoint
        .output_manifest_refs
        .iter()
        .any(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX));
    if partial {
        if has_commit {
            return Err(invalid(
                "partial published checkpoint claims a producer commit",
            ));
        }
        return Ok(None);
    }
    publication::producer_cursor_digest_from_header(header, binding).map(Some)
}

pub(super) async fn capture_demand_published_artifacts(
    state: &ApiState,
    inputs: &[ConsumerViewDependencyInputV1],
    max_bytes: usize,
    previous_consumer: Option<&StandingRuntimeCheckpointRecord>,
    consumer_binding: &PublishedRelationBindingV1,
) -> Result<(Vec<DemandPublishedArtifact>, usize), ApiError> {
    let mut budget = ReadBudget {
        supplied: max_bytes,
        used: 0,
    };
    let mut artifacts = Vec::new();
    let mut ordered = inputs.iter().collect::<Vec<_>>();
    ordered.sort_by(|a, b| a.edge.edge_id.cmp(&b.edge.edge_id));
    if ordered
        .windows(2)
        .any(|pair| pair[0].edge.edge_id == pair[1].edge.edge_id)
    {
        return Err(invalid("duplicate published input edge"));
    }
    for input in ordered {
        let proof = Arc::new(capture_proof(state, input, &mut budget).await?);
        for (index, header) in proof.lineage.iter().enumerate() {
            let Some(digest) = published_header_digest(header, &input.binding)? else {
                continue;
            };
            let mut cursor = input.bootstrap_cursor.clone();
            cursor.output_epoch = header.checkpoint.logical_epoch;
            cursor.commit_digest = digest;
            let base = DemandPublishedArtifact {
                edge: input.edge.clone(),
                binding: input.binding.clone(),
                cursor,
                baseline_summary: None,
                proof: proof.clone(),
                header_index: index,
                payload: ArtifactPayload::Delta {
                    object_key: String::new(),
                },
            };
            if index == 0 {
                if let Some((manifest_ref, manifest_hash, pages, rows)) = previous_consumer
                    .map(|record| applied_baseline_summary(record, input, consumer_binding))
                    .transpose()?
                    .flatten()
                {
                    let mut artifact = base;
                    artifact.baseline_summary =
                        Some(Arc::new((manifest_ref.clone(), manifest_hash, pages, rows)));
                    artifact.payload = ArtifactPayload::CoveredBaseline { manifest_ref };
                    artifacts.push(artifact);
                    continue;
                }
                let refs = header
                    .checkpoint
                    .output_manifest_refs
                    .iter()
                    .filter_map(|r| r.strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
                    .collect::<Vec<_>>();
                let [key] = refs.as_slice() else {
                    return Err(invalid(
                        "published baseline requires one immutable output manifest; state hydration is not a demand fallback",
                    ));
                };
                let bytes = budget.read(state, key).await?;
                let manifest: StandingRuntimeOutputManifestRecord =
                    serde_json::from_slice(&bytes).map_err(ApiError::bad_request)?;
                let (object_key, _) =
                    ObjectKey::parse_standing_runtime_output_manifest((*key).to_string())
                        .map_err(ApiError::bad_request)?;
                publication::validate_standing_runtime_output_manifest_record(
                    &object_key,
                    &manifest,
                )?;
                if manifest.tenant_id != header.checkpoint.identity.tenant_id
                    || manifest.program_id != header.checkpoint.identity.program_id
                    || manifest.view_id != header.view_id
                    || manifest.checkpoint_key != header.checkpoint_key
                    || manifest.logical_epoch != header.checkpoint.logical_epoch
                    || manifest.checkpoint_content_hash != header.checkpoint.state_root.content_hash
                {
                    return Err(invalid("published manifest/header binding mismatch"));
                }
                if native_output_pages::is_native_codec(&manifest.output_encoding) {
                    let schema: RelationSchema =
                        serde_json::from_value(manifest.published_output["schema"].clone())
                            .map_err(ApiError::bad_request)?;
                    if schema != input.binding.relation {
                        return Err(invalid("published manifest schema mismatch"));
                    }
                }
                let summary = Arc::new((
                    format!("{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{key}"),
                    stable_bytes_hash(&serde_json::to_vec(&manifest).map_err(ApiError::internal)?),
                    u64::try_from(manifest.pages.len()).map_err(ApiError::bad_request)?,
                    u64::try_from(manifest.output_row_count).map_err(ApiError::bad_request)?,
                ));
                let manifest = Arc::new(manifest);
                if manifest.pages.is_empty() {
                    if manifest.output_row_count != 0 {
                        return Err(invalid("nonempty snapshot has no pages"));
                    }
                    let mut artifact = base.clone();
                    artifact.baseline_summary = Some(summary.clone());
                    artifact.payload = ArtifactPayload::Empty {
                        manifest_ref: (*key).to_string(),
                    };
                    artifacts.push(artifact);
                }
                for (ordinal, page) in manifest.pages.iter().enumerate() {
                    if page.page_index as usize != ordinal {
                        return Err(invalid("published page index is not contiguous"));
                    }
                    let mut artifact = base.clone();
                    artifact.baseline_summary = Some(summary.clone());
                    artifact.payload = ArtifactPayload::Page {
                        manifest: manifest.clone(),
                        page: page.clone(),
                        previous: ordinal.checked_sub(1).map(|i| manifest.pages[i].clone()),
                    };
                    artifacts.push(artifact);
                }
            } else {
                let refs = header
                    .checkpoint
                    .output_manifest_refs
                    .iter()
                    .filter_map(|r| r.strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
                    .collect::<Vec<_>>();
                let [key] = refs.as_slice() else {
                    return Err(invalid("published checkpoint requires one signed commit"));
                };
                let mut artifact = base;
                artifact.payload = ArtifactPayload::Delta {
                    object_key: (*key).to_string(),
                };
                artifacts.push(artifact);
            }
        }
    }
    Ok((artifacts, budget.used))
}

/// Reuse the baseline only when a validated consumer checkpoint persists the
/// exact captured bootstrap and its authoritative frontier proves that prefix
/// is already applied. Producer state restoration is never evidence here.
fn applied_baseline_summary(
    consumer: &StandingRuntimeCheckpointRecord,
    input: &ConsumerViewDependencyInputV1,
    consumer_binding: &PublishedRelationBindingV1,
) -> Result<Option<(String, String, u64, u64)>, ApiError> {
    let Some(progress) = consumer.published_input_bootstrap.as_ref() else {
        return Ok(None);
    };
    progress.validate().map_err(meta_error_to_api)?;
    if !progress.is_complete()
        || consumer.view_id != consumer_binding.producer_view_id
        || progress.bootstrap_generation != consumer_binding.producer_view_generation
        || progress.plan_hash != consumer_binding.producer_plan_hash
    {
        return Ok(None);
    }
    let mut matches = progress.inputs.iter().filter(|saved| {
        saved.input_edge == input.edge.edge_id
            && saved.captured_cursor == input.bootstrap_cursor
            && saved.output_schema_hash == input.binding.output_schema_hash
    });
    let Some(saved) = matches.next() else {
        return Ok(None);
    };
    if saved.next_page_index != saved.total_pages || saved.applied_rows != saved.total_rows {
        return Ok(None);
    }
    if matches.next().is_some() {
        return Err(invalid("duplicate persisted published baseline edge"));
    }
    let frontier_count = consumer
        .checkpoint
        .input_frontiers
        .iter()
        .filter(|frontier| {
            frontier.relation_id == input.binding.relation.relation_id
                && frontier.relation_version == input.binding.relation.relation_version
                && frontier.stream_id == input.binding.output_stream_id
                && frontier.partition_id == 0
                && frontier.committed_offset_exclusive >= input.bootstrap_cursor.output_epoch
        })
        .count();
    if frontier_count != 1 {
        return Ok(None);
    }
    Ok(Some((
        saved.output_manifest_ref.clone(),
        saved.output_manifest_hash.clone(),
        saved.total_pages,
        saved.total_rows,
    )))
}

pub(super) fn demand_published_artifact_key(
    artifact: &DemandPublishedArtifact,
) -> Result<String, ApiError> {
    let reference = match &artifact.payload {
        ArtifactPayload::CoveredBaseline { manifest_ref } => manifest_ref,
        ArtifactPayload::Empty { manifest_ref } => manifest_ref,
        ArtifactPayload::Page { page, .. } => &page.page_key,
        ArtifactPayload::Delta { object_key } => object_key,
    };
    let header = &artifact.proof.lineage[artifact.header_index];
    Ok(stable_bytes_hash(
        &serde_json::to_vec(&json!({"version":1,"edge":artifact.edge,
        "binding":artifact.binding,"cursor":artifact.cursor,"checkpoint":header.checkpoint_key,
        "manifest_hash":header.manifest_hash,"reference":reference}))
        .map_err(ApiError::internal)?,
    ))
}

async fn load_page(
    state: &ApiState,
    manifest: &StandingRuntimeOutputManifestRecord,
    reference: &StandingRuntimeOutputPageRef,
    budget: &mut ReadBudget,
) -> Result<DeltaBatch, ApiError> {
    let bytes = budget.read(state, &reference.page_key).await?;
    let page = native_output_pages::decode_page(&bytes, &reference.output_encoding)?;
    let (key, _) = ObjectKey::parse_standing_runtime_output_page(reference.page_key.clone())
        .map_err(ApiError::bad_request)?;
    publication::validate_standing_runtime_output_page_record(&key, &page)?;
    if page.page_index != reference.page_index
        || page.page_content_hash != reference.page_content_hash
        || page.row_count != reference.row_count
        || page.output_encoding != reference.output_encoding
        || page.tenant_id != manifest.tenant_id
        || page.program_id != manifest.program_id
        || page.view_id != manifest.view_id
        || page.logical_epoch != manifest.logical_epoch
    {
        return Err(invalid("published page binding mismatch"));
    }
    if native_output_pages::is_native_codec(&manifest.output_encoding) {
        native_output_pages::validate_page_binding(manifest, &page)
    } else {
        // Current aggregate/ordered codec is one whole snapshot page. It must
        // equal the manifest payload; no hydration or assumed small page size.
        if manifest.pages.len() != 1
            || page.output_content_hash != manifest.output_content_hash
            || page.published_output != manifest.published_output
        {
            return Err(invalid("nonpaged published snapshot differs from manifest"));
        }
        serde_json::from_value(page.published_output).map_err(ApiError::bad_request)
    }
}

pub(super) async fn load_demand_published_artifact(
    state: &ApiState,
    artifact: &DemandPublishedArtifact,
    max_bytes: usize,
) -> Result<(RelationInputBatch, usize), ApiError> {
    let mut budget = ReadBudget {
        supplied: max_bytes,
        used: 0,
    };
    let header = &artifact.proof.lineage[artifact.header_index];
    if artifact.proof.head.logical_epoch < artifact.cursor.output_epoch
        || publication::producer_cursor_digest_from_header(header, &artifact.binding)?
            != artifact.cursor.commit_digest
    {
        return Err(invalid("published artifact proof/cursor mismatch"));
    }
    let delta = match &artifact.payload {
        ArtifactPayload::CoveredBaseline { .. } => {
            return Err(invalid(
                "already-applied published baseline was selected for replay",
            ));
        }
        ArtifactPayload::Empty { .. } => DeltaBatch::from_records([]),
        ArtifactPayload::Page {
            manifest,
            page,
            previous,
        } => {
            let prior = if let Some(reference) = previous {
                Some(load_page(state, manifest, reference, &mut budget).await?)
            } else {
                None
            };
            let delta = load_page(state, manifest, page, &mut budget).await?;
            let mut last = None;
            let mut weight = 0i64;
            for row in prior
                .iter()
                .flat_map(|d| d.records())
                .chain(delta.records())
            {
                if row.weight <= 0 {
                    return Err(invalid("published baseline has nonpositive multiplicity"));
                }
                let key =
                    velorix_core::delta::encode_kv_ordered(row.key.as_json(), row.value.as_json());
                if native_output_pages::is_native_codec(&manifest.output_encoding)
                    && last.as_ref().is_some_and(|p| p > &key)
                {
                    return Err(invalid("published page order mismatch"));
                }
                weight = if last.as_ref() == Some(&key) {
                    weight
                        .checked_add(row.weight)
                        .ok_or_else(|| invalid("published multiplicity overflow"))?
                } else {
                    row.weight
                };
                last = Some(key);
            }
            delta
        }
        ArtifactPayload::Delta { object_key } => {
            let bytes = budget.read(state, object_key).await?;
            let record: StandingRuntimeOutputDeltaRecord =
                serde_json::from_slice(&bytes).map_err(ApiError::bad_request)?;
            let (key, _) = ObjectKey::parse_standing_runtime_output_delta(object_key.clone())
                .map_err(ApiError::bad_request)?;
            publication::validate_standing_runtime_output_delta_record(&key, &record)?;
            let commit = record
                .producer_commit
                .as_ref()
                .ok_or_else(|| invalid("published signed commit missing"))?;
            if record.logical_epoch != artifact.cursor.output_epoch
                || record.schema_fingerprint != artifact.binding.relation.schema_fingerprint
                || commit.checkpoint_key != header.checkpoint_key
                || commit.checkpoint_content_hash != header.checkpoint.state_root.content_hash
                || commit.producer_commit_digest != artifact.cursor.commit_digest
            {
                return Err(invalid("published commit/header mismatch"));
            }
            serde_json::from_value(record.output_delta).map_err(ApiError::bad_request)?
        }
    };
    // Convert separately: the existing batch converter consolidates bags, which
    // must not alter sparse artifact ordinals. Restore zero weights afterwards.
    let mut batches = Vec::with_capacity(delta.records().len());
    for row in delta.records() {
        let mut single = row.clone();
        if single.weight == 0 {
            single.weight = 1;
        }
        let batch = published_delta_to_record_batch(
            &artifact.binding,
            &DeltaBatch::from_records([single]),
        )?;
        let mut arrays = batch.columns().to_vec();
        arrays[artifact.binding.relation.columns.len()] =
            Arc::new(Int64Array::from(vec![row.weight]));
        batches.push(RecordBatch::try_new(batch.schema(), arrays).map_err(ApiError::bad_request)?);
    }
    if batches.is_empty() {
        batches.push(published_delta_to_record_batch(
            &artifact.binding,
            &DeltaBatch::from_records([]),
        )?);
    }
    let binding = &artifact.binding;
    Ok((
        RelationInputBatch {
            relation_id: binding.relation.relation_id.clone(),
            relation_version: binding.relation.relation_version.clone(),
            schema_fingerprint: binding.relation.schema_fingerprint.clone(),
            stream_id: binding.output_stream_id.clone(),
            partition_id: 0,
            start_offset_inclusive: artifact.proof.lineage[..artifact.header_index]
                .iter()
                .rev()
                .find(|header| {
                    header
                        .checkpoint
                        .output_manifest_refs
                        .iter()
                        .any(|reference| {
                            reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
                        })
                })
                .map_or(0, |header| header.checkpoint.logical_epoch),
            end_offset_exclusive: artifact.cursor.output_epoch,
            event_time_watermark: None,
            encoding: RelationInputEncodingV1::PublishedRelationDeltaV1 {
                delta_codec_identity: binding.delta_codec_identity.clone(),
                output_schema_hash: binding.output_schema_hash.clone(),
                weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.into(),
                weight_field_index: binding.relation.columns.len(),
            },
            batches,
        },
        budget.used,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_read_cache::{NativeReadCache, NativeReadCacheConfig};
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn checkpoint_cache_preserves_published_budgets_and_commitments() {
        let mut state = crate::tests::test_api_state_with_store(
            Arc::new(InMemory::new()),
            "published-cache-test",
            false,
        )
        .await;
        let mut checkpoint = crate::tests::test_runtime_checkpoint(Vec::new());
        checkpoint.identity.tenant_id = "tenant-a.fragment-cache".into();
        publication::persist_standing_runtime_checkpoint(
            &state,
            "purchases_by_user",
            &checkpoint,
            &[],
            StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
            None,
        )
        .await
        .unwrap();
        let key = ObjectKey::standing_runtime_checkpoint(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            "purchases_by_user",
            checkpoint.logical_epoch,
            &checkpoint.state_root.content_hash,
        )
        .unwrap();
        let path = ObjectPath::from(key.as_str());
        let size = state.store.head(&path).await.unwrap().size as usize;
        let mut budget = ReadBudget {
            supplied: size,
            used: 0,
        };
        let bytes = budget.read(&state, key.as_str()).await.unwrap();
        assert_eq!(budget.used, size, "uncached read charges serialized bytes");

        let directory = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            NativeReadCache::open(NativeReadCacheConfig {
                directory: directory.path().into(),
                memory_bytes: 256 * 1024,
                disk_bytes: 8 * 1024 * 1024,
            })
            .await
            .unwrap(),
        );
        state = state.with_native_read_cache(cache.clone());
        budget = ReadBudget {
            supplied: 2 * size,
            used: 0,
        };
        assert_eq!(budget.read(&state, key.as_str()).await.unwrap(), bytes);
        assert_eq!(
            publication::cached_checkpoint_size(&state, &path).await,
            Some(size as u64)
        );
        assert!(
            publication::read_object_bytes_bounded(&state, &path, Some(size as u64 - 1))
                .await
                .is_err()
        );

        let mut record = standing_runtime_checkpoint_record_from_slice(&bytes).unwrap();
        publication::hydrate_standing_runtime_checkpoint_state_payload(&state, &mut record)
            .await
            .unwrap();
        assert_eq!(record.checkpoint.state_payload, checkpoint.state_payload);

        // An empty backing store makes any accidental warm HEAD/GET fail.
        state.store = Arc::new(InMemory::new());
        budget.supplied = 2 * size - 1;
        let error = budget.read(&state, key.as_str()).await.unwrap_err();
        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            error.details.unwrap()["required_min_bytes"],
            json!(2 * size)
        );
        assert_eq!(budget.used, size, "rejected reads do not charge the budget");
        budget.supplied = 2 * size;
        assert_eq!(budget.read(&state, key.as_str()).await.unwrap(), bytes);
        assert_eq!(budget.used, 2 * size, "warm reads still consume the budget");
        record.checkpoint.state_payload = None;
        publication::hydrate_standing_runtime_checkpoint_state_payload(&state, &mut record)
            .await
            .unwrap();
        assert_eq!(record.checkpoint.state_payload, checkpoint.state_payload);

        let mut pointer = publication::standing_runtime_checkpoint_pointer_from_record(&record);
        pointer.manifest_hash = stable_bytes_hash(b"wrong commitment");
        let error = publication::read_standing_runtime_checkpoint_header_from_pointer(
            &state,
            &pointer.tenant_id,
            &pointer.program_id,
            &pointer.view_id,
            &pointer,
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("manifest hash mismatch"));
        cache.close().await.unwrap();
    }
}

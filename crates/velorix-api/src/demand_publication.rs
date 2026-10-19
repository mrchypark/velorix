//! Publish the signed changes since the last producer commit, without hydrating state.
use super::checkpoint_publication::{
    producer_cursor_digest_from_header, standing_runtime_checkpoint_pointer_from_record,
    validate_standing_runtime_checkpoint_record,
    validate_standing_runtime_checkpoint_replay_frontiers,
    validate_standing_runtime_output_delta_record,
};
use super::*;
use futures::StreamExt;

#[derive(Clone, Copy, Debug)]
pub(super) struct DemandPublicationLimits {
    /// Aggregate serialized bytes, including current deltas and authenticated headers.
    pub(super) max_bytes: usize,
    /// Signed records before consolidation, including the current delta.
    pub(super) max_delta_records: usize,
}

#[derive(Debug)]
pub(super) struct DemandPublishedBaseline {
    pub(super) pointer: StandingRuntimeCheckpointPointer,
    pub(super) commit_digest: String,
}

#[derive(Debug)]
pub(super) struct DemandPublicationDelta {
    pub(super) output_deltas: Vec<ViewOutputDelta>,
    pub(super) last_published: Option<DemandPublishedBaseline>,
    pub(super) fetched_bytes: usize,
    pub(super) visited_checkpoints: usize,
}

fn invalid(message: &str) -> ApiError {
    ApiError::bad_request(format!("demand publication: {message}"))
}

fn budget_required(resource: &str, limit: u64, consumed: u64, needed: u64) -> ApiError {
    ApiError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        message: "demand publication requires a larger resource budget".into(),
        details: Some(json!({
            "code": "DEMAND_PUBLICATION_BUDGET_REQUIRED", "resource": resource,
            "limit": limit, "consumed": consumed, "min_needed_lower_bound": needed,
            "requirement_is_lower_bound": true,
        })),
        retryable_materialization: false,
    }
}

async fn read_budgeted(
    state: &ApiState,
    path: &ObjectPath,
    limit: usize,
    remaining: usize,
    header: bool,
) -> Result<Vec<u8>, ApiError> {
    // shortcut: publication reads enforce header/aggregate budgets against GET metadata; revisit with a cache preserving those checks and lineage commitments.
    let object = state.store.get(path).await.map_err(ApiError::internal)?;
    let size = object.meta.size;
    const HEADER_LIMIT: u64 = 16 * 1024 * 1024;
    if header && size > HEADER_LIMIT {
        return Err(budget_required(
            "checkpoint_header_bytes",
            HEADER_LIMIT,
            0,
            size,
        ));
    }
    let consumed = (limit - remaining) as u64;
    if size > remaining as u64 {
        return Err(budget_required(
            "serialized_bytes",
            limit as u64,
            consumed,
            consumed.saturating_add(size),
        ));
    }
    let mut bytes = Vec::new();
    let mut stream = object.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ApiError::internal)?;
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| invalid("object length overflow"))?;
        if next as u64 > size {
            return Err(invalid("object exceeds declared length"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != size {
        return Err(invalid("object is truncated"));
    }
    Ok(bytes)
}

fn visit_lineage(
    seen: &mut std::collections::HashSet<String>,
    key: &str,
    epoch: u64,
    next_epoch: u64,
) -> Result<(), ApiError> {
    if !seen.insert(key.to_owned()) || epoch >= next_epoch {
        return Err(invalid("cyclic or nondecreasing checkpoint lineage"));
    }
    Ok(())
}

fn append_delta(
    records: &mut Vec<velorix_core::delta::DeltaRecord>,
    delta: &DeltaBatch,
    limit: usize,
) -> Result<(), ApiError> {
    let count = records
        .len()
        .checked_add(delta.records().len())
        .ok_or_else(|| invalid("delta record count overflow"))?;
    if count > limit {
        return Err(budget_required(
            "signed_delta_records",
            limit as u64,
            records.len() as u64,
            count as u64,
        ));
    }
    records.extend_from_slice(delta.records());
    Ok(())
}

/// `immediate_previous` must be the predecessor used for the publication CAS.
/// Its persisted bytes are reauthenticated here; the returned baseline must never
/// replace that predecessor in the CAS. No published baseline means offset zero,
/// rather than a fabricated producer cursor. The caller constructs its edge cursor
/// from the returned pointer's epoch and commit digest.
pub(super) async fn consolidate_demand_publication_deltas(
    state: &ApiState,
    immediate_previous: Option<&StandingRuntimeCheckpointRecord>,
    current: &RuntimeCheckpoint,
    view_id: &str,
    binding: &PublishedRelationBindingV1,
    current_deltas: &[ViewOutputDelta],
    limits: DemandPublicationLimits,
) -> Result<DemandPublicationDelta, ApiError> {
    validate_published_relation_binding_v1(binding).map_err(ApiError::bad_request)?;
    if binding.producer_view_id != view_id
        || !current.identity.view_ids.iter().any(|id| id == view_id)
    {
        return Err(invalid("current checkpoint/binding identity mismatch"));
    }
    if current.input_coverage.as_ref().is_some_and(|coverage| {
        coverage.view_generation != binding.producer_view_generation
            || coverage.plan_hash != binding.producer_plan_hash
    }) {
        return Err(invalid("current checkpoint plan/generation mismatch"));
    }
    let mut records = Vec::new();
    // Serialize through a counting writer: no extra full delta byte buffer.
    let mut counter = ByteBudget {
        used: 0,
        limit: limits.max_bytes,
        required: None,
    };
    for delta in current_deltas {
        if delta.view_id != view_id
            || delta.schema_fingerprint != binding.relation.schema_fingerprint
        {
            return Err(invalid("current delta identity/schema mismatch"));
        }
        serde_json::to_writer(&mut counter, &delta.delta).map_err(|error| {
            counter.required.map_or_else(
                || ApiError::internal(error),
                |needed| {
                    budget_required(
                        "serialized_bytes",
                        counter.limit as u64,
                        counter.used as u64,
                        needed as u64,
                    )
                },
            )
        })?;
        append_delta(&mut records, &delta.delta, limits.max_delta_records)?;
    }
    let mut remaining = limits.max_bytes - counter.used;
    let mut fetched_bytes = 0usize;
    let mut visited_checkpoints = 0usize;
    let mut last_published = None;
    let mut pointer = immediate_previous.map(standing_runtime_checkpoint_pointer_from_record);
    let mut seen = std::collections::HashSet::new();
    let mut next_epoch = current.logical_epoch;
    while let Some(previous) = pointer {
        visit_lineage(
            &mut seen,
            &previous.checkpoint_key,
            previous.logical_epoch,
            next_epoch,
        )?;
        if previous.tenant_id != current.identity.tenant_id
            || previous.program_id != current.identity.program_id
            || previous.view_id != view_id
        {
            return Err(invalid("checkpoint lineage identity mismatch"));
        }
        let bytes = read_budgeted(
            state,
            &ObjectPath::from(previous.checkpoint_key.clone()),
            limits.max_bytes,
            remaining,
            true,
        )
        .await?;
        charge(&mut remaining, &mut fetched_bytes, bytes.len())?;
        if stable_bytes_hash(&bytes) != previous.manifest_hash {
            return Err(invalid("checkpoint manifest hash mismatch"));
        }
        let mut header = standing_runtime_checkpoint_record_from_slice(&bytes)
            .map_err(|error| invalid(&error.to_string()))?;
        drop(bytes);
        if header.checkpoint_key.is_empty() {
            header.checkpoint_key = previous.checkpoint_key.clone();
        }
        validate_standing_runtime_checkpoint_record(
            &current.identity,
            view_id,
            &previous,
            &header,
        )?;
        validate_standing_runtime_checkpoint_replay_frontiers(&header)?;
        if let Some(demand) = &header.demand_materialization {
            demand.validate_checkpoint(&header.checkpoint)?;
        }
        visited_checkpoints += 1;
        next_epoch = previous.logical_epoch;
        let commits = header
            .checkpoint
            .output_manifest_refs
            .iter()
            .filter_map(|r| r.strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
            .collect::<Vec<_>>();
        let partial = header
            .demand_materialization
            .as_ref()
            .is_some_and(|d| !d.full_complete)
            || header
                .published_input_bootstrap
                .as_ref()
                .is_some_and(|p| !p.is_complete());
        let published = !commits.is_empty();
        let keys = if published {
            if partial || commits.len() != 1 {
                return Err(invalid(
                    "partial or ambiguous authoritative producer commit",
                ));
            }
            commits
        } else {
            if !partial {
                return Err(invalid(
                    "unpublished checkpoint has no partial progress proof",
                ));
            }
            if header
                .demand_materialization
                .as_ref()
                .is_some_and(|d| d.plan_hash != binding.producer_plan_hash)
                || header
                    .published_input_bootstrap
                    .as_ref()
                    .is_some_and(|p| p.plan_hash != binding.producer_plan_hash)
            {
                return Err(invalid("partial checkpoint plan/generation mismatch"));
            }
            header
                .checkpoint
                .output_manifest_refs
                .iter()
                .filter_map(|r| r.strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX))
                .collect::<Vec<_>>()
        };
        if keys.len() != 1 {
            return Err(invalid(
                "checkpoint requires exactly one signed output delta",
            ));
        }
        let (key, _) = ObjectKey::parse_standing_runtime_output_delta(keys[0].to_string())
            .map_err(ApiError::bad_request)?;
        let bytes = read_budgeted(
            state,
            &ObjectPath::from(key.as_str()),
            limits.max_bytes,
            remaining,
            false,
        )
        .await?;
        charge(&mut remaining, &mut fetched_bytes, bytes.len())?;
        let delta: StandingRuntimeOutputDeltaRecord =
            serde_json::from_slice(&bytes).map_err(|error| invalid(&error.to_string()))?;
        drop(bytes);
        validate_standing_runtime_output_delta_record(&key, &delta)?;
        if delta.tenant_id != current.identity.tenant_id
            || delta.program_id != current.identity.program_id
            || delta.view_id != view_id
            || delta.logical_epoch != previous.logical_epoch
            || delta.schema_fingerprint != binding.relation.schema_fingerprint
        {
            return Err(invalid("output delta scope/schema mismatch"));
        }
        if published {
            let digest = producer_cursor_digest_from_header(&header, binding)?;
            let commit = delta
                .producer_commit
                .as_ref()
                .ok_or_else(|| invalid("producer commit missing"))?;
            if commit.producer_commit_digest != digest
                || commit.checkpoint_key != header.checkpoint_key
                || commit.checkpoint_content_hash != header.checkpoint.state_root.content_hash
            {
                return Err(invalid("producer commit/header mismatch"));
            }
            last_published = Some(DemandPublishedBaseline {
                pointer: previous,
                commit_digest: digest,
            });
            break;
        }
        if delta.producer_commit.is_some() {
            return Err(invalid(
                "partial delta unexpectedly contains a producer commit",
            ));
        }
        let batch: DeltaBatch = serde_json::from_value(delta.output_delta)
            .map_err(|error| invalid(&error.to_string()))?;
        append_delta(&mut records, &batch, limits.max_delta_records)?;
        pointer = header.previous_checkpoint;
    }
    let delta = consolidate(records)?;
    Ok(DemandPublicationDelta {
        output_deltas: vec![ViewOutputDelta {
            view_id: view_id.to_string(),
            schema_fingerprint: binding.relation.schema_fingerprint.clone(),
            delta,
        }],
        last_published,
        fetched_bytes,
        visited_checkpoints,
    })
}

fn consolidate(records: Vec<velorix_core::delta::DeltaRecord>) -> Result<DeltaBatch, ApiError> {
    DeltaBatch::from_records(records)
        .net_rows()
        .map(DeltaBatch::from_records)
        .map_err(|error| invalid(&format!("signed delta consolidation failed: {error}")))
}

fn charge(remaining: &mut usize, fetched: &mut usize, bytes: usize) -> Result<(), ApiError> {
    *remaining = remaining
        .checked_sub(bytes)
        .ok_or_else(|| invalid("serialized byte budget exceeded"))?;
    *fetched = fetched
        .checked_add(bytes)
        .ok_or_else(|| invalid("read byte count overflow"))?;
    Ok(())
}

struct ByteBudget {
    used: usize,
    limit: usize,
    required: Option<usize>,
}
impl std::io::Write for ByteBudget {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let used = self
            .used
            .checked_add(buf.len())
            .ok_or_else(|| std::io::Error::other("demand publication byte count overflow"))?;
        if used > self.limit {
            self.required = Some(used);
            return Err(std::io::Error::other(
                "demand publication serialized byte budget exceeded",
            ));
        }
        self.used = used;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};
    fn row(key: i64, weight: i64) -> DeltaRecord {
        DeltaRecord::new(
            DeltaKey::from_json(json!(key)),
            DeltaValue::from_json(json!([key])),
            weight,
        )
    }
    #[test]
    fn partial_and_final_deltas_keep_multiplicity_and_retractions() {
        let delta = consolidate(vec![
            row(1, 3),
            row(2, -2),
            row(1, -1),
            row(3, 1),
            row(3, -1),
        ])
        .unwrap();
        assert_eq!(delta.records().len(), 2);
        assert!(delta.records().contains(&row(1, 2)));
        assert!(delta.records().contains(&row(2, -2)));
    }
    #[test]
    fn negative_only_and_initial_empty_are_valid() {
        assert_eq!(
            consolidate(vec![row(1, -3)]).unwrap().records(),
            &[row(1, -3)]
        );
        assert!(consolidate(vec![]).unwrap().records().is_empty());
    }
    #[test]
    fn signed_overflow_and_resource_exhaustion_fail_closed() {
        assert!(consolidate(vec![row(1, i64::MAX), row(1, 1)]).is_err());
        let mut records = vec![row(1, 1)];
        assert!(append_delta(&mut records, &DeltaBatch::from_records([row(2, 1)]), 1).is_err());
        assert_eq!(records.len(), 1);
        let mut remaining = 2;
        let mut fetched = 0;
        assert!(charge(&mut remaining, &mut fetched, 3).is_err());
        assert_eq!((remaining, fetched), (2, 0));
    }

    #[test]
    fn persisted_negative_delta_authentication_rejects_corruption() {
        let output_delta = serde_json::to_value(DeltaBatch::from_records([row(7, -2)])).unwrap();
        let delta_content_hash = stable_bytes_hash(&serde_json::to_vec(&output_delta).unwrap());
        let key = ObjectKey::standing_runtime_output_delta(
            "tenant",
            "program",
            "stage",
            2,
            &delta_content_hash,
        )
        .unwrap();
        let record = StandingRuntimeOutputDeltaRecord {
            schema_version: 1,
            record_kind: "standing_runtime_output_delta_v1".into(),
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "stage".into(),
            logical_epoch: 2,
            schema_fingerprint: "neutral-schema".into(),
            delta_content_hash,
            delta_encoding: "velorix-delta-batch-json-v1".into(),
            delta_row_count: 1,
            source_kind: "standing_runtime_epoch_output_delta".into(),
            output_delta,
            producer_commit: None,
        };
        let bytes = serde_json::to_vec(&record).unwrap();
        let mut restored: StandingRuntimeOutputDeltaRecord =
            serde_json::from_slice(&bytes).unwrap();
        validate_standing_runtime_output_delta_record(&key, &restored).unwrap();
        restored.output_delta =
            serde_json::to_value(DeltaBatch::from_records([row(7, -1)])).unwrap();
        assert!(validate_standing_runtime_output_delta_record(&key, &restored).is_err());
    }

    #[test]
    fn current_delta_serialization_obeys_same_byte_budget() {
        let batch = DeltaBatch::from_records([row(1, 1)]);
        let exact = serde_json::to_vec(&batch).unwrap().len();
        let mut budget = ByteBudget {
            used: 0,
            limit: exact,
            required: None,
        };
        serde_json::to_writer(&mut budget, &batch).unwrap();
        assert_eq!(budget.used, exact);
        let mut insufficient = ByteBudget {
            used: 0,
            limit: exact - 1,
            required: None,
        };
        assert!(serde_json::to_writer(&mut insufficient, &batch).is_err());
        assert!(insufficient.required.unwrap() > insufficient.limit);
    }

    #[test]
    fn lineage_over_4096_is_limited_by_bytes_not_history_count() {
        let mut seen = std::collections::HashSet::new();
        let mut remaining = 5_000 * 128;
        let mut fetched = 0;
        let mut next_epoch = 5_001;
        for epoch in (1..=5_000).rev() {
            visit_lineage(&mut seen, &format!("checkpoint/{epoch}"), epoch, next_epoch).unwrap();
            charge(&mut remaining, &mut fetched, 128).unwrap();
            next_epoch = epoch;
        }
        assert_eq!(seen.len(), 5_000);
        assert_eq!((remaining, fetched), (0, 640_000));
        assert!(charge(&mut remaining, &mut fetched, 1).is_err());
        assert!(visit_lineage(&mut seen, "checkpoint/1", 0, 1).is_err());
        assert!(visit_lineage(&mut seen, "future", 2, 1).is_err());
    }

    #[test]
    fn resource_errors_are_actionable_and_distinct_from_corruption() {
        let mut records = vec![row(1, 1)];
        let error =
            append_delta(&mut records, &DeltaBatch::from_records([row(2, 1)]), 1).unwrap_err();
        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(!error.retryable_materialization);
        let details = error.details.unwrap();
        assert_eq!(details["code"], "DEMAND_PUBLICATION_BUDGET_REQUIRED");
        assert_eq!(details["resource"], "signed_delta_records");
        assert_eq!(details["limit"], 1);
        assert_eq!(details["consumed"], 1);
        assert_eq!(details["min_needed_lower_bound"], 2);
        assert_eq!(
            invalid("checkpoint manifest hash mismatch").status,
            StatusCode::BAD_REQUEST
        );
    }
}

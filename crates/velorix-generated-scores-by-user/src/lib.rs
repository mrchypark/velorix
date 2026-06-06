use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use velorix_core::{
    engine::LogicalEpoch,
    feldera_artifact::{feldera_artifact_bytes_hash, ColumnSchema, RelationSchema, SqlDataType},
    feldera_package_runtime::{FelderaExecutableProgram, FelderaPackageRuntime},
    standing_program::{
        DurableStateRoot, EpochCommit, EpochIdempotencyKey, MaterializedViewPage, RelationFrontier,
        RelationInputBatch, RuntimeCheckpoint, RuntimeCheckpointStatePayload, ScopedViewId,
        SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
        StandingProgramRuntimeError, ViewFrontier, ViewOutputBatch,
    },
};

pub const CRATE_NAME: &str = "scores_by_user_generated";

pub fn create_standing_runtime(
    identity: &StandingProgramIdentity,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    create_package_runtime(identity)
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|error| error.to_string())
}

pub fn restore_standing_runtime(
    checkpoint: RuntimeCheckpoint,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    let runtime =
        ScoresByUserGeneratedRuntime::restore(checkpoint).map_err(|error| error.to_string())?;
    let identity = StandingProgramRuntime::program_identity(&runtime).clone();
    FelderaPackageRuntime::new(identity, ScoresByUserGeneratedExecutable::new(runtime))
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|error| error.to_string())
}

pub fn create_package_runtime(
    identity: &StandingProgramIdentity,
) -> Result<FelderaPackageRuntime<ScoresByUserGeneratedExecutable>, StandingProgramRuntimeError> {
    identity.validate()?;
    if !identity
        .runtime_packages
        .iter()
        .any(|package| package.name == CRATE_NAME)
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "runtime_packages",
        });
    }

    FelderaPackageRuntime::new(
        identity.clone(),
        ScoresByUserGeneratedExecutable::new(ScoresByUserGeneratedRuntime::new(identity.clone())),
    )
}

#[derive(Clone, Debug)]
pub struct ScoresByUserGeneratedRuntime {
    identity: StandingProgramIdentity,
    logical_epoch: LogicalEpoch,
    input_frontiers: Vec<RelationFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    materialized: BTreeMap<String, UserScore>,
}

#[derive(Clone, Debug)]
pub struct ScoresByUserGeneratedExecutable {
    runtime: ScoresByUserGeneratedRuntime,
}

impl ScoresByUserGeneratedExecutable {
    fn new(runtime: ScoresByUserGeneratedRuntime) -> Self {
        Self { runtime }
    }
}

#[derive(Clone, Debug, Default)]
struct UserScore {
    sum: i64,
    count: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScoresCheckpointPayload {
    materialized: Vec<ScoresCheckpointRow>,
    applied_epochs: Vec<ScoresCheckpointAppliedEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScoresCheckpointRow {
    user_id: String,
    sum: i64,
    count: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScoresCheckpointAppliedEpoch {
    idempotency_key: String,
    logical_epoch: LogicalEpoch,
}

impl ScoresByUserGeneratedRuntime {
    fn new(identity: StandingProgramIdentity) -> Self {
        Self {
            identity,
            logical_epoch: 0,
            input_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
            materialized: BTreeMap::new(),
        }
    }

    fn apply_batch_to(
        materialized: &mut BTreeMap<String, UserScore>,
        batch: &RecordBatch,
    ) -> Result<(), StandingProgramRuntimeError> {
        let user_id_index = batch
            .schema()
            .index_of("user_id")
            .map_err(|_| invalid_scores_batch())?;
        let score_index = batch
            .schema()
            .index_of("score")
            .map_err(|_| invalid_scores_batch())?;
        let delta_index = batch
            .schema()
            .index_of("delta")
            .map_err(|_| invalid_scores_batch())?;

        let user_ids = batch
            .column(user_id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid_scores_batch)?;
        let scores = batch
            .column(score_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(invalid_scores_batch)?;
        let deltas = batch
            .column(delta_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(invalid_scores_batch)?;

        for row in 0..batch.num_rows() {
            if user_ids.is_null(row) || scores.is_null(row) || deltas.is_null(row) {
                return Err(invalid_scores_batch());
            }
            let score = scores.value(row);
            if score <= 0 {
                continue;
            }
            let entry = materialized
                .entry(user_ids.value(row).to_string())
                .or_default();
            let delta = deltas.value(row);
            entry.sum = entry.sum.saturating_add(score.saturating_mul(delta));
            entry.count = entry.count.saturating_add(delta);
        }

        materialized.retain(|_, score| score.count != 0);
        Ok(())
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        Self::materialized_batch_from_rows(self.materialized.iter())
    }

    fn materialized_batch_from_rows<'a>(
        rows: impl IntoIterator<Item = (&'a String, &'a UserScore)>,
    ) -> Result<RecordBatch, StandingProgramRuntimeError> {
        let mut user_ids = Vec::new();
        let mut sums = Vec::new();
        let mut counts = Vec::new();
        for (user_id, score) in rows {
            user_ids.push(user_id.as_str());
            sums.push(score.sum);
            counts.push(score.count);
        }

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("sum", DataType::Int64, false),
                Field::new("count", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(user_ids)) as ArrayRef,
                Arc::new(Int64Array::from(sums)) as ArrayRef,
                Arc::new(Int64Array::from(counts)) as ArrayRef,
            ],
        )
        .map_err(|_| invalid_scores_batch())
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        if let Some(requested) = page.committed_epoch {
            if requested != self.logical_epoch {
                return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                    requested,
                    current: self.logical_epoch,
                });
            }
        }
        let limit = page.max_rows.unwrap_or(self.materialized.len());
        if limit == 0 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "snapshot_page.max_rows",
            });
        }
        let mut rows = self
            .materialized
            .iter()
            .filter(|(user_id, _)| match &page.page_token {
                Some(token) => user_id.as_str() > token.as_str(),
                None => true,
            })
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_next = rows.len() > limit;
        if has_next {
            rows.truncate(limit);
        }
        let next_page_token = if has_next {
            rows.last().map(|(user_id, _)| (*user_id).clone())
        } else {
            None
        };
        Ok((Self::materialized_batch_from_rows(rows)?, next_page_token))
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = ScoresCheckpointPayload {
            materialized: self
                .materialized
                .iter()
                .map(|(user_id, score)| ScoresCheckpointRow {
                    user_id: user_id.clone(),
                    sum: score.sum,
                    count: score.count,
                })
                .collect(),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(
                    |(idempotency_key, logical_epoch)| ScoresCheckpointAppliedEpoch {
                        idempotency_key: idempotency_key.clone(),
                        logical_epoch: *logical_epoch,
                    },
                )
                .collect(),
        };
        serde_json::to_string(&payload).map_err(|_| invalid_scores_checkpoint())
    }

    fn restore_materialized(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<BTreeMap<String, UserScore>, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_scores_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: ScoresCheckpointPayload = serde_json::from_str(&state_payload.payload)
            .map_err(|_| invalid_scores_checkpoint())?;
        Ok(payload
            .materialized
            .into_iter()
            .map(|row| {
                (
                    row.user_id,
                    UserScore {
                        sum: row.sum,
                        count: row.count,
                    },
                )
            })
            .collect())
    }

    fn restore_applied_epochs(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<BTreeMap<String, LogicalEpoch>, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_scores_checkpoint());
        };
        let payload: ScoresCheckpointPayload = serde_json::from_str(&state_payload.payload)
            .map_err(|_| invalid_scores_checkpoint())?;
        Ok(payload
            .applied_epochs
            .into_iter()
            .map(|entry| (entry.idempotency_key, entry.logical_epoch))
            .collect())
    }

    fn validate_input_identity(
        &self,
        input: &RelationInputBatch,
    ) -> Result<(), StandingProgramRuntimeError> {
        let expected = scores_input_schema(&self.identity.input_catalog_hash);
        if input.relation_id != expected.relation_id
            || input.relation_version != expected.relation_version
            || input.schema_fingerprint != expected.schema_fingerprint
        {
            return Err(invalid_scores_input_relation());
        }
        Ok(())
    }

    fn output_schema_fingerprint(&self) -> String {
        scores_output_schema(
            &self.identity.view_ids[0],
            &self.identity.input_catalog_hash,
        )
        .schema_fingerprint
    }

    fn validate_checkpoint_frontiers(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<(), StandingProgramRuntimeError> {
        let expected_input = scores_input_schema(&checkpoint.identity.input_catalog_hash);
        if checkpoint.input_frontiers.len() != 1 {
            return Err(invalid_scores_checkpoint_input_frontier());
        }
        for frontier in &checkpoint.input_frontiers {
            if frontier.relation_id != expected_input.relation_id
                || frontier.relation_version != expected_input.relation_version
            {
                return Err(invalid_scores_checkpoint_input_frontier());
            }
        }

        let expected_outputs = checkpoint.identity.view_ids.iter().collect::<BTreeSet<_>>();
        let actual_outputs = checkpoint
            .output_frontiers
            .iter()
            .map(|frontier| &frontier.view_id)
            .collect::<BTreeSet<_>>();
        if checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len()
            || actual_outputs != expected_outputs
        {
            return Err(invalid_scores_checkpoint_output_frontier());
        }
        for frontier in &checkpoint.output_frontiers {
            if frontier.committed_epoch != checkpoint.logical_epoch {
                return Err(invalid_scores_checkpoint_output_frontier());
            }
        }
        Ok(())
    }

    pub fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        Self::validate_checkpoint_frontiers(&checkpoint)?;
        let materialized = Self::restore_materialized(&checkpoint)?;
        let applied_epochs = Self::restore_applied_epochs(&checkpoint)?;
        Ok(Self {
            materialized,
            input_frontiers: checkpoint.input_frontiers,
            applied_epochs,
            identity: checkpoint.identity,
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}

impl StandingProgramRuntime for ScoresByUserGeneratedRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![scores_input_schema(&self.identity.input_catalog_hash)]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![scores_output_schema(
            &self.identity.view_ids[0],
            &self.identity.input_catalog_hash,
        )]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let idempotency_key_text = idempotency_key.as_str().to_string();
        if let Some(applied_epoch) = self.applied_epochs.get(&idempotency_key_text) {
            if *applied_epoch == logical_epoch {
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: self.input_frontiers.clone(),
                    output_batches: vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema_fingerprint(),
                        batches: vec![self.materialized_batch()?],
                    }],
                });
            }
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key_text,
                first_epoch: *applied_epoch,
                attempted_epoch: logical_epoch,
            });
        }
        if logical_epoch <= self.logical_epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }

        let mut next_materialized = self.materialized.clone();
        let mut input_frontiers = self.input_frontiers.clone();
        for input in input_changes {
            self.validate_input_identity(&input)?;
            for batch in &input.batches {
                Self::apply_batch_to(&mut next_materialized, batch)?;
            }
            if let Some(frontier) = input_frontiers.iter_mut().find(|frontier| {
                frontier.relation_id == input.relation_id
                    && frontier.relation_version == input.relation_version
            }) {
                frontier.committed_offset_exclusive = frontier
                    .committed_offset_exclusive
                    .max(input.end_offset_exclusive);
            } else {
                input_frontiers.push(RelationFrontier {
                    relation_id: input.relation_id,
                    relation_version: input.relation_version,
                    committed_offset_exclusive: input.end_offset_exclusive,
                });
            }
        }
        self.materialized = next_materialized;
        self.input_frontiers = input_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);
        self.logical_epoch = logical_epoch;

        let view_id = self.identity.view_ids[0].clone();
        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers,
            output_batches: vec![ViewOutputBatch {
                view_id,
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.materialized_batch()?],
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.identity.tenant_id
            || view.program_id != self.identity.program_id
            || !self
                .identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }

        let (batch, next_page_token) = self.materialized_page_batch(page)?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.checkpoint_payload()?;
        let content_hash = feldera_artifact_bytes_hash(payload.as_bytes());
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.logical_epoch,
            input_frontiers: self.input_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.logical_epoch,
                })
                .collect(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: format!("v1/state/generated/{}/checkpoint", self.identity.program_id),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: self.identity.checkpoint_codec_identity.clone(),
                payload,
            }),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        Self::restore(checkpoint)
    }
}

impl FelderaExecutableProgram for ScoresByUserGeneratedExecutable {
    fn program_identity(&self) -> &StandingProgramIdentity {
        self.runtime.program_identity()
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.runtime.input_schemas()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        self.runtime.output_schemas()
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.runtime.logical_epoch()
    }

    fn apply_epoch(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        self.runtime
            .apply_changes(logical_epoch, idempotency_key, input_changes)
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        self.runtime.materialized_view_page(view, page)
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        self.runtime.checkpoint()
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        Ok(Self::new(ScoresByUserGeneratedRuntime::restore(
            checkpoint,
        )?))
    }
}

fn invalid_scores_batch() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "scores_input_batch",
    }
}

fn invalid_scores_input_relation() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "scores_input_relation",
    }
}

fn invalid_scores_checkpoint() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "scores_checkpoint_payload",
    }
}

fn invalid_scores_checkpoint_input_frontier() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "scores_checkpoint_input_frontier",
    }
}

fn invalid_scores_checkpoint_output_frontier() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "scores_checkpoint_output_frontier",
    }
}

fn scores_input_schema(schema_fingerprint: &str) -> RelationSchema {
    RelationSchema {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: schema_fingerprint.to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "delta".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_output_schema(view_id: &str, schema_fingerprint: &str) -> RelationSchema {
    RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: schema_fingerprint.to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velorix_core::standing_program::{
        FelderaRuntimePackageIdentity, NativeCodePolicy, ScopedViewId,
    };

    #[test]
    fn generated_scores_runtime_checkpoint_restore_preserves_materialized_state_and_frontiers() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();

        let checkpoint = runtime.checkpoint().unwrap();
        assert_eq!(checkpoint.logical_epoch, 7);
        assert_eq!(
            checkpoint.input_frontiers,
            vec![RelationFrontier {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                committed_offset_exclusive: 3,
            }]
        );
        assert_eq!(
            checkpoint.output_frontiers,
            vec![ViewFrontier {
                view_id: "scores_by_user".to_string(),
                committed_epoch: 7,
            }]
        );
        assert!(checkpoint.state_payload.is_some());

        let restored = ScoresByUserGeneratedRuntime::restore(checkpoint).unwrap();
        assert_eq!(restored.program_identity(), &identity);
        assert_eq!(restored.logical_epoch(), 7);

        let page = restored
            .materialized_view_page(
                ScopedViewId {
                    tenant_id: identity.tenant_id.clone(),
                    program_id: identity.program_id.clone(),
                    view_id: "scores_by_user".to_string(),
                },
                SnapshotPageRequest::default(),
            )
            .unwrap();
        assert_eq!(page.batches[0].num_rows(), 1);
        let user_ids = page.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = page.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = page.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(user_ids.value(0), "u1");
        assert_eq!(sums.value(0), 12);
        assert_eq!(counts.value(0), 2);
    }

    #[test]
    fn generated_scores_runtime_checkpoint_restore_uses_alias_view_identity() {
        let identity = identity("pending_scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-4").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    batches: vec![scores_input_batch_with_zero()],
                }],
            )
            .unwrap();

        let checkpoint = runtime.checkpoint().unwrap();
        assert_eq!(
            checkpoint.output_frontiers,
            vec![ViewFrontier {
                view_id: "pending_scores_by_user".to_string(),
                committed_epoch: 7,
            }]
        );

        let restored = ScoresByUserGeneratedRuntime::restore(checkpoint).unwrap();
        let page = restored
            .materialized_view_page(scoped_view(&identity), SnapshotPageRequest::default())
            .unwrap();
        assert_scores_batch_rows(&page.batches[0], &[("u1", 12, 2)]);

        let wrong_view = restored.materialized_view_page(
            ScopedViewId {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: "scores_by_user".to_string(),
            },
            SnapshotPageRequest::default(),
        );
        assert!(matches!(
            wrong_view,
            Err(StandingProgramRuntimeError::UnknownView { .. })
        ));
    }

    #[test]
    fn generated_scores_runtime_restore_rejects_tampered_checkpoint_payload() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity);
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: format!("sha256:{}", "b".repeat(64)),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();

        let mut checkpoint = runtime.checkpoint().unwrap();
        checkpoint.state_payload.as_mut().unwrap().payload.push(' ');

        let error = ScoresByUserGeneratedRuntime::restore(checkpoint).unwrap_err();

        assert_eq!(
            error,
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "state_payload.content_hash"
            }
        );
    }

    #[test]
    fn generated_scores_runtime_restore_rejects_checkpoint_frontier_identity_drift() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity);
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: format!("sha256:{}", "b".repeat(64)),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();

        let mut input_drift = runtime.checkpoint().unwrap();
        input_drift.input_frontiers[0].relation_id = "orders".to_string();
        let error = ScoresByUserGeneratedRuntime::restore(input_drift).unwrap_err();
        assert_eq!(
            error,
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scores_checkpoint_input_frontier"
            }
        );

        let mut output_drift = runtime.checkpoint().unwrap();
        output_drift.output_frontiers[0].view_id = "other_view".to_string();
        let error = ScoresByUserGeneratedRuntime::restore(output_drift).unwrap_err();
        assert_eq!(
            error,
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scores_checkpoint_output_frontier"
            }
        );
    }

    #[test]
    fn generated_scores_runtime_restore_rejects_checkpoint_missing_required_frontiers() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity);
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: format!("sha256:{}", "b".repeat(64)),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();

        let mut missing_input = runtime.checkpoint().unwrap();
        missing_input.input_frontiers.clear();
        let error = ScoresByUserGeneratedRuntime::restore(missing_input).unwrap_err();
        assert_eq!(
            error,
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scores_checkpoint_input_frontier"
            }
        );

        let mut missing_output = runtime.checkpoint().unwrap();
        missing_output.output_frontiers.clear();
        let error = ScoresByUserGeneratedRuntime::restore(missing_output).unwrap_err();
        assert_eq!(
            error,
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scores_checkpoint_output_frontier"
            }
        );
    }

    #[test]
    fn generated_scores_runtime_replays_same_epoch_idempotently_after_restore() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        let input = RelationInputBatch {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: identity.input_catalog_hash.clone(),
            start_offset_inclusive: 0,
            end_offset_exclusive: 3,
            batches: vec![scores_input_batch()],
        };
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![input.clone()],
            )
            .unwrap();
        let checkpoint = runtime.checkpoint().unwrap();
        let mut restored = ScoresByUserGeneratedRuntime::restore(checkpoint).unwrap();

        let duplicate_commit = restored
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![input],
            )
            .unwrap();

        assert_eq!(duplicate_commit.logical_epoch, 7);
        assert_eq!(
            duplicate_commit.input_frontiers,
            vec![RelationFrontier {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                committed_offset_exclusive: 3,
            }]
        );
        assert_scores_page(&restored, &identity, "u1", 12, 2);
    }

    #[test]
    fn generated_scores_package_runtime_keeps_idempotent_epoch_replay_boundary() {
        let identity = identity("scores_by_user");
        let mut runtime = create_package_runtime(&identity).unwrap();
        let input = RelationInputBatch {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: identity.input_catalog_hash.clone(),
            start_offset_inclusive: 0,
            end_offset_exclusive: 3,
            batches: vec![scores_input_batch()],
        };

        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![input.clone()],
            )
            .unwrap();
        let duplicate = runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![input],
            )
            .unwrap();

        assert_eq!(duplicate.logical_epoch, 7);
        let page = runtime
            .materialized_view_page(scoped_view(&identity), SnapshotPageRequest::default())
            .unwrap();
        assert_scores_batch_rows(&page.batches[0], &[("u1", 12, 2)]);
    }

    #[test]
    fn generated_scores_runtime_rejects_same_idempotency_key_for_different_epoch() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        let input = RelationInputBatch {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: identity.input_catalog_hash.clone(),
            start_offset_inclusive: 0,
            end_offset_exclusive: 3,
            batches: vec![scores_input_batch()],
        };
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![input.clone()],
            )
            .unwrap();

        let error = runtime
            .apply_changes(
                8,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![input],
            )
            .unwrap_err();

        assert_eq!(
            error,
            StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: "scores-0-3".to_string(),
                first_epoch: 7,
                attempted_epoch: 8,
            }
        );
        assert_scores_page(&runtime, &identity, "u1", 12, 2);
    }

    #[test]
    fn generated_scores_runtime_rejects_invalid_epoch_atomically_without_state_or_idempotency_mutation(
    ) {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();

        let error = runtime
            .apply_changes(
                8,
                EpochIdempotencyKey::new("scores-invalid").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 3,
                    end_offset_exclusive: 5,
                    batches: vec![
                        additional_scores_input_batch(),
                        invalid_scores_input_batch_missing_delta(),
                    ],
                }],
            )
            .unwrap_err();

        assert_eq!(
            error,
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scores_input_batch"
            }
        );
        assert_eq!(runtime.logical_epoch(), 7);
        assert_scores_page(&runtime, &identity, "u1", 12, 2);

        runtime
            .apply_changes(
                8,
                EpochIdempotencyKey::new("scores-invalid").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 3,
                    end_offset_exclusive: 4,
                    batches: vec![additional_scores_input_batch()],
                }],
            )
            .unwrap();
        assert_scores_page(&runtime, &identity, "u1", 112, 3);
    }

    #[test]
    fn generated_scores_runtime_rejects_wrong_input_relation_identity_without_state_mutation() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();

        let error = runtime
            .apply_changes(
                8,
                EpochIdempotencyKey::new("orders-0-1").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "orders".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 1,
                    batches: vec![additional_scores_input_batch()],
                }],
            )
            .unwrap_err();

        assert_eq!(
            error,
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scores_input_relation"
            }
        );
        assert_eq!(runtime.logical_epoch(), 7);
        assert_scores_page(&runtime, &identity, "u1", 12, 2);
    }

    #[test]
    fn generated_scores_runtime_materialized_view_pages_rows_by_stable_cursor() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new("scores-0-4").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    batches: vec![multi_user_scores_input_batch()],
                }],
            )
            .unwrap();

        let first = runtime
            .materialized_view_page(
                scoped_view(&identity),
                SnapshotPageRequest {
                    page_token: None,
                    max_rows: Some(2),
                    ..SnapshotPageRequest::default()
                },
            )
            .unwrap();
        assert_eq!(first.next_page_token, Some("u2".to_string()));
        assert_scores_batch_rows(&first.batches[0], &[("u1", 5, 1), ("u2", 9, 1)]);

        let second = runtime
            .materialized_view_page(
                scoped_view(&identity),
                SnapshotPageRequest {
                    page_token: first.next_page_token,
                    max_rows: Some(2),
                    ..SnapshotPageRequest::default()
                },
            )
            .unwrap();
        assert_eq!(second.next_page_token, None);
        assert_scores_batch_rows(&second.batches[0], &[("u3", 13, 1)]);
    }

    #[test]
    fn generated_scores_runtime_materialized_view_requires_available_committed_epoch() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();

        let current = runtime
            .materialized_view_page(
                scoped_view(&identity),
                SnapshotPageRequest {
                    committed_epoch: Some(7),
                    ..SnapshotPageRequest::default()
                },
            )
            .unwrap();
        assert_eq!(current.logical_epoch, 7);

        let error = runtime
            .materialized_view_page(
                scoped_view(&identity),
                SnapshotPageRequest {
                    committed_epoch: Some(6),
                    ..SnapshotPageRequest::default()
                },
            )
            .unwrap_err();
        assert_eq!(
            error,
            StandingProgramRuntimeError::UnavailableCommittedEpoch {
                requested: 6,
                current: 7,
            }
        );
    }

    #[test]
    fn generated_scores_runtime_commit_and_page_use_declared_output_schema_fingerprint() {
        let identity = identity("scores_by_user");
        let mut runtime = ScoresByUserGeneratedRuntime::new(identity.clone());
        let expected_output_schema = runtime.output_schemas().remove(0);

        let commit = runtime
            .apply_changes(
                7,
                EpochIdempotencyKey::new("scores-0-3").unwrap(),
                vec![RelationInputBatch {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                    schema_fingerprint: identity.input_catalog_hash.clone(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    batches: vec![scores_input_batch()],
                }],
            )
            .unwrap();
        assert_eq!(
            commit.output_batches[0].schema_fingerprint,
            expected_output_schema.schema_fingerprint
        );

        let page = runtime
            .materialized_view_page(scoped_view(&identity), SnapshotPageRequest::default())
            .unwrap();
        assert_eq!(
            page.schema_fingerprint,
            expected_output_schema.schema_fingerprint
        );
    }

    fn identity(view_id: &str) -> StandingProgramIdentity {
        StandingProgramIdentity {
            tenant_id: "default".to_string(),
            program_id: view_id.to_string(),
            view_ids: vec![view_id.to_string()],
            sql_hash: format!("sha256:{}", "a".repeat(64)),
            input_catalog_hash: format!("sha256:{}", "b".repeat(64)),
            output_schema_hash: format!("sha256:{}", "c".repeat(64)),
            compiler_identity: "feldera-sql-compiler:builtin-default".to_string(),
            runtime_packages: vec![FelderaRuntimePackageIdentity {
                name: CRATE_NAME.to_string(),
                version: "feldera-generated-rust-abi-v1".to_string(),
            }],
            package_feature_set: vec!["static_release_artifact".to_string()],
            dbsp_runtime_compatibility: "feldera-generated-rust-abi-v1".to_string(),
            checkpoint_codec_identity: "feldera-dbsp-state-v1".to_string(),
            native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        }
    }

    fn scores_input_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("score", DataType::Int64, false),
                Field::new("delta", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["u1", "u1", "u2"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 7, -1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn scores_input_batch_with_zero() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("score", DataType::Int64, false),
                Field::new("delta", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["u1", "u1", "u2", "u3"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 7, -1, 0])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn multi_user_scores_input_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("score", DataType::Int64, false),
                Field::new("delta", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["u1", "u2", "u3"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 9, 13])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn additional_scores_input_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("score", DataType::Int64, false),
                Field::new("delta", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn invalid_scores_input_batch_missing_delta() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("score", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn assert_scores_page(
        runtime: &ScoresByUserGeneratedRuntime,
        identity: &StandingProgramIdentity,
        expected_user_id: &str,
        expected_sum: i64,
        expected_count: i64,
    ) {
        let page = runtime
            .materialized_view_page(
                ScopedViewId {
                    tenant_id: identity.tenant_id.clone(),
                    program_id: identity.program_id.clone(),
                    view_id: "scores_by_user".to_string(),
                },
                SnapshotPageRequest::default(),
            )
            .unwrap();
        assert_eq!(page.batches[0].num_rows(), 1);
        assert_scores_batch_rows(
            &page.batches[0],
            &[(expected_user_id, expected_sum, expected_count)],
        );
    }

    fn assert_scores_batch_rows(batch: &RecordBatch, expected: &[(&str, i64, i64)]) {
        assert_eq!(batch.num_rows(), expected.len());
        let user_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for (row, (expected_user_id, expected_sum, expected_count)) in expected.iter().enumerate() {
            assert_eq!(user_ids.value(row), *expected_user_id);
            assert_eq!(sums.value(row), *expected_sum);
            assert_eq!(counts.value(row), *expected_count);
        }
    }

    fn scoped_view(identity: &StandingProgramIdentity) -> ScopedViewId {
        ScopedViewId {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: identity.view_ids[0].clone(),
        }
    }
}

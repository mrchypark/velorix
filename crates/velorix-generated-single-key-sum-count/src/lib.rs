use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        StringArray, TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use velorix_core::{
    dbsp_view_plan::{
        validate_supported_dbsp_join_view_sql, validate_supported_dbsp_view_sql, DbspPredicateOp,
        DbspRowPredicate, SupportedDbspJoinViewPlan, SupportedDbspViewPlan,
    },
    delta::{DeltaBatch, DeltaRecord},
    engine::{
        AggregateValueMode, EngineCheckpointPayload, IncrementalEngine, LogicalEpoch,
        PrototypeIncrementalEngine,
    },
    feldera_artifact::{
        catalog_input_relation_schema, feldera_artifact_bytes_hash, RelationSchema, SqlDataType,
        SqlStructField,
    },
    relation::{
        arrow_record_batches_to_single_key_sum_count_delta_batch, ArrowPhysicalTypeV1,
        RelationSemanticRoleV1, VelorixRelationCatalogV1,
    },
    standing_program::{
        DurableStateRoot, EpochCommit, EpochIdempotencyKey, MaterializedViewPage, RelationFrontier,
        RelationInputBatch, RuntimeCheckpoint, RuntimeCheckpointStatePayload, ScopedViewId,
        SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
        StandingProgramRuntimeError, ViewFrontier, ViewOutputBatch,
    },
};

pub const CRATE_NAME: &str = "single_key_sum_count_generated";

const CHECKPOINT_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const JOIN_RUNTIME_KIND: &str = "two_input_join_sum_count";

pub fn create_standing_runtime(
    identity: &StandingProgramIdentity,
    catalog: &VelorixRelationCatalogV1,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    SingleKeySumCountGeneratedRuntime::new(
        identity.clone(),
        catalog.clone(),
        only_schema(input_schemas, "input_schemas").map_err(|error| error.to_string())?,
        only_schema(output_schemas, "output_schemas").map_err(|error| error.to_string())?,
    )
    .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
    .map_err(|error| error.to_string())
}

pub fn create_standing_runtime_with_sql(
    identity: &StandingProgramIdentity,
    catalog: &VelorixRelationCatalogV1,
    sql: &str,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    create_standing_runtime_with_sql_and_catalogs(
        identity,
        std::slice::from_ref(catalog),
        sql,
        input_schemas,
        output_schemas,
    )
}

pub fn create_standing_runtime_with_sql_and_catalogs(
    identity: &StandingProgramIdentity,
    catalogs: &[VelorixRelationCatalogV1],
    sql: &str,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    if input_schemas.len() == 2 {
        let plan = validate_supported_dbsp_join_view_sql(sql, catalogs)
            .map_err(|error| error.to_string())?;
        return TwoInputJoinGeneratedRuntime::new_with_plan(
            identity.clone(),
            catalogs.to_vec(),
            input_schemas.to_vec(),
            only_schema(output_schemas, "output_schemas").map_err(|error| error.to_string())?,
            sql.to_string(),
            plan,
        )
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|error| error.to_string());
    }
    let [catalog] = catalogs else {
        return Err(
            "single-key sum/count runtime requires exactly one relation catalog".to_string(),
        );
    };
    let plan = validate_supported_dbsp_view_sql(sql, catalog).map_err(|error| error.to_string())?;
    SingleKeySumCountGeneratedRuntime::new_with_plan(
        identity.clone(),
        catalog.clone(),
        only_schema(input_schemas, "input_schemas").map_err(|error| error.to_string())?,
        only_schema(output_schemas, "output_schemas").map_err(|error| error.to_string())?,
        sql.to_string(),
        plan,
    )
    .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
    .map_err(|error| error.to_string())
}

pub fn restore_standing_runtime(
    checkpoint: RuntimeCheckpoint,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    if checkpoint_has_join_payload(&checkpoint) {
        return TwoInputJoinGeneratedRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    SingleKeySumCountGeneratedRuntime::restore(checkpoint)
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|error| error.to_string())
}

#[derive(Clone, Debug)]
pub struct SingleKeySumCountGeneratedRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedDbspViewPlan,
    engine: PrototypeIncrementalEngine,
    input_frontiers: Vec<RelationFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericCheckpointPayload {
    schema_version: u32,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    #[serde(default)]
    view_sql: Option<String>,
    #[serde(default)]
    plan: Option<SupportedDbspViewPlan>,
    #[serde(default)]
    input_frontiers: Option<Vec<RelationFrontier>>,
    engine: EngineCheckpointPayload,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericAppliedEpoch {
    idempotency_key: String,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug)]
pub struct TwoInputJoinGeneratedRuntime {
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedDbspJoinViewPlan,
    engine: PrototypeIncrementalEngine,
    left_state: DeltaBatch,
    right_state: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct JoinCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedDbspJoinViewPlan,
    input_frontiers: Vec<RelationFrontier>,
    left_state: DeltaBatch,
    right_state: DeltaBatch,
    engine: EngineCheckpointPayload,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

impl SingleKeySumCountGeneratedRuntime {
    pub fn new(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
    ) -> Result<Self, StandingProgramRuntimeError> {
        let view_sql = default_sql_for_catalog(&catalog)?;
        let plan = validate_supported_dbsp_view_sql(view_sql.as_str(), &catalog).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan",
            }
        })?;
        Self::new_with_plan(
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
        )
    }

    pub fn new_with_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedDbspViewPlan,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_runtime_package(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_supported_schemas(&catalog, &input_schema, &output_schema)?;
        let compiled_plan =
            validate_supported_dbsp_view_sql(view_sql.as_str(), &catalog).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan",
                }
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan",
            });
        }
        validate_plan_matches_catalog(&plan, &catalog)?;
        let value_mode = aggregate_value_mode_for_catalog(&catalog)?;
        Ok(Self {
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            engine: PrototypeIncrementalEngine::with_aggregate_value_mode(value_mode),
            input_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_delta_to_record_batch(&self.output_schema, &self.engine.materialized_state())
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        if let Some(requested) = page.committed_epoch {
            if requested != self.engine.logical_epoch() {
                return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                    requested,
                    current: self.engine.logical_epoch(),
                });
            }
        }
        let mut rows = self
            .engine
            .materialized_state()
            .net_rows()
            .map_err(|_| invalid_runtime_state())?;
        rows.sort_by(|left, right| {
            canonical_json(left.key.as_json()).cmp(&canonical_json(right.key.as_json()))
        });
        if let Some(page_token) = &page.page_token {
            rows.retain(|row| canonical_json(row.key.as_json()) > *page_token);
        }

        let limit = page.max_rows.unwrap_or(rows.len());
        if limit == 0 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "snapshot_page.max_rows",
            });
        }
        let has_next = rows.len() > limit;
        if has_next {
            rows.truncate(limit);
        }
        let next_page_token = if has_next {
            rows.last().map(|row| canonical_json(row.key.as_json()))
        } else {
            None
        };
        materialized_delta_to_record_batch(&self.output_schema, &DeltaBatch::from_records(rows))
            .map(|batch| (batch, next_page_token))
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = GenericCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: Some(self.view_sql.clone()),
            plan: Some(self.plan.clone()),
            input_frontiers: Some(self.input_frontiers.clone()),
            engine: self.engine.checkpoint_state().to_payload(),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
        };
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<GenericCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: GenericCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION {
            return Err(invalid_checkpoint());
        }
        validate_supported_schemas(
            &payload.catalog,
            &payload.input_schema,
            &payload.output_schema,
        )?;
        Ok(payload)
    }

    fn validate_input_identity(
        &self,
        input: &RelationInputBatch,
    ) -> Result<(), StandingProgramRuntimeError> {
        if input.relation_id != self.input_schema.relation_id
            || input.relation_version != self.input_schema.relation_version
            || input.schema_fingerprint != self.input_schema.schema_fingerprint
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_input_relation",
            });
        }
        Ok(())
    }
}

impl StandingProgramRuntime for SingleKeySumCountGeneratedRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![self.input_schema.clone()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.engine.logical_epoch()
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
        if logical_epoch <= self.engine.logical_epoch() {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.engine.logical_epoch(),
                attempted: logical_epoch,
            });
        }

        let mut combined = DeltaBatch::default();
        let mut input_frontiers = self.input_frontiers.clone();
        for input in input_changes {
            self.validate_input_identity(&input)?;
            let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
                &self.catalog,
                &input.relation_id,
                &input.relation_version,
                &input.schema_fingerprint,
                &input.batches,
            )
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_input_batch",
            })?;
            let delta = filter_delta_batch_for_plan(&delta, &self.plan, &self.catalog)?;
            combined = combined.combine(&delta);
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

        self.engine
            .push_changes(logical_epoch, &combined)
            .map_err(|_| invalid_runtime_state())?;
        self.input_frontiers = input_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers,
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
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
            logical_epoch: self.engine.logical_epoch(),
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
            logical_epoch: self.engine.logical_epoch(),
            input_frontiers: self.input_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.engine.logical_epoch(),
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
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        validate_checkpoint_frontiers(&checkpoint, &payload)?;
        let engine_checkpoint = payload.engine.into_checkpoint();
        if engine_checkpoint.logical_epoch() != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
        let value_mode = aggregate_value_mode_for_catalog(&payload.catalog)?;
        if let Some(payload_frontiers) = &payload.input_frontiers {
            if payload_frontiers != &checkpoint.input_frontiers {
                return Err(invalid_checkpoint());
            }
        }
        let view_sql = match payload.view_sql {
            Some(view_sql) => {
                validate_view_sql_hash(&checkpoint.identity, view_sql.as_str())?;
                view_sql
            }
            None => default_sql_for_catalog(&payload.catalog)?,
        };
        let plan = match payload.plan {
            Some(plan) => {
                let compiled =
                    validate_supported_dbsp_view_sql(view_sql.as_str(), &payload.catalog)
                        .map_err(|_| invalid_checkpoint())?;
                if compiled != plan {
                    return Err(invalid_checkpoint());
                }
                plan
            }
            None => validate_supported_dbsp_view_sql(view_sql.as_str(), &payload.catalog)
                .map_err(|_| invalid_checkpoint())?,
        };
        validate_plan_matches_catalog(&plan, &payload.catalog)?;
        let engine = PrototypeIncrementalEngine::from_checkpoint_with_aggregate_value_mode(
            engine_checkpoint,
            value_mode,
        )
        .map_err(|_| invalid_checkpoint())?;
        Ok(Self {
            identity: checkpoint.identity,
            catalog: payload.catalog,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql,
            plan,
            engine,
            input_frontiers: checkpoint.input_frontiers,
            applied_epochs: payload
                .applied_epochs
                .into_iter()
                .map(|entry| (entry.idempotency_key, entry.logical_epoch))
                .collect(),
        })
    }
}

impl TwoInputJoinGeneratedRuntime {
    pub fn new_with_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedDbspJoinViewPlan,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_runtime_package(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_join_supported_schemas(&catalogs, &input_schemas, &output_schema)?;
        let compiled_plan = validate_supported_dbsp_join_view_sql(view_sql.as_str(), &catalogs)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan",
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan",
            });
        }
        validate_join_plan_matches_catalogs(&plan, &catalogs)?;
        let left_catalog = join_left_catalog(&plan, &catalogs)?;
        let value_mode = aggregate_value_mode_for_catalog(left_catalog)?;
        Ok(Self {
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            engine: PrototypeIncrementalEngine::with_aggregate_value_mode(value_mode),
            left_state: DeltaBatch::default(),
            right_state: DeltaBatch::default(),
            input_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_delta_to_record_batch(&self.output_schema, &self.engine.materialized_state())
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        if let Some(requested) = page.committed_epoch {
            if requested != self.engine.logical_epoch() {
                return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                    requested,
                    current: self.engine.logical_epoch(),
                });
            }
        }
        let mut rows = self
            .engine
            .materialized_state()
            .net_rows()
            .map_err(|_| invalid_runtime_state())?;
        rows.sort_by(|left, right| {
            canonical_json(left.key.as_json()).cmp(&canonical_json(right.key.as_json()))
        });
        if let Some(page_token) = &page.page_token {
            rows.retain(|row| canonical_json(row.key.as_json()) > *page_token);
        }

        let limit = page.max_rows.unwrap_or(rows.len());
        if limit == 0 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "snapshot_page.max_rows",
            });
        }
        let has_next = rows.len() > limit;
        if has_next {
            rows.truncate(limit);
        }
        let next_page_token = if has_next {
            rows.last().map(|row| canonical_json(row.key.as_json()))
        } else {
            None
        };
        materialized_delta_to_record_batch(&self.output_schema, &DeltaBatch::from_records(rows))
            .map(|batch| (batch, next_page_token))
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = JoinCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: JOIN_RUNTIME_KIND.to_string(),
            catalogs: self.catalogs.clone(),
            input_schemas: self.input_schemas.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            left_state: self.left_state.clone(),
            right_state: self.right_state.clone(),
            engine: self.engine.checkpoint_state().to_payload(),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
        };
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<JoinCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: JoinCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != JOIN_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_join_supported_schemas(
            &payload.catalogs,
            &payload.input_schemas,
            &payload.output_schema,
        )?;
        Ok(payload)
    }

    fn validate_input_identity(
        &self,
        input: &RelationInputBatch,
    ) -> Result<(), StandingProgramRuntimeError> {
        if self
            .input_schema_for_relation(&input.relation_id)
            .is_some_and(|schema| {
                input.relation_version == schema.relation_version
                    && input.schema_fingerprint == schema.schema_fingerprint
            })
        {
            Ok(())
        } else {
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_input_relation",
            })
        }
    }

    fn input_schema_for_relation(&self, relation_id: &str) -> Option<&RelationSchema> {
        self.input_schemas
            .iter()
            .find(|schema| schema.relation_id == relation_id)
    }
}

impl StandingProgramRuntime for TwoInputJoinGeneratedRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.engine.logical_epoch()
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
        if logical_epoch <= self.engine.logical_epoch() {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.engine.logical_epoch(),
                attempted: logical_epoch,
            });
        }

        let mut joined_changes = DeltaBatch::default();
        let mut input_frontiers = self.input_frontiers.clone();
        for input in input_changes {
            self.validate_input_identity(&input)?;
            let catalog = join_catalog_for_relation(&self.catalogs, &input.relation_id)?;
            let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
                catalog,
                &input.relation_id,
                &input.relation_version,
                &input.schema_fingerprint,
                &input.batches,
            )
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_input_batch",
            })?;
            if input.relation_id == self.plan.left_input_relation_id {
                let joined = join_delta_against_state(&delta, &self.right_state)?;
                joined_changes = joined_changes.combine(&joined);
                self.left_state = self.left_state.combine(&delta);
            } else if input.relation_id == self.plan.right_input_relation_id {
                let joined =
                    join_delta_against_state_with_value_source(&delta, &self.left_state, false)?;
                joined_changes = joined_changes.combine(&joined);
                self.right_state = self.right_state.combine(&delta);
            } else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_input_relation",
                });
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

        self.engine
            .push_changes(logical_epoch, &joined_changes)
            .map_err(|_| invalid_runtime_state())?;
        self.input_frontiers = input_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers,
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
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
            logical_epoch: self.engine.logical_epoch(),
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
            logical_epoch: self.engine.logical_epoch(),
            input_frontiers: self.input_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.engine.logical_epoch(),
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
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        validate_join_checkpoint_frontiers(&checkpoint, &payload)?;
        let engine_checkpoint = payload.engine.into_checkpoint();
        if engine_checkpoint.logical_epoch() != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        let compiled =
            validate_supported_dbsp_join_view_sql(payload.view_sql.as_str(), &payload.catalogs)
                .map_err(|_| invalid_checkpoint())?;
        if compiled != payload.plan {
            return Err(invalid_checkpoint());
        }
        validate_join_plan_matches_catalogs(&payload.plan, &payload.catalogs)?;
        let left_catalog = join_left_catalog(&payload.plan, &payload.catalogs)?;
        let value_mode = aggregate_value_mode_for_catalog(left_catalog)?;
        let engine = PrototypeIncrementalEngine::from_checkpoint_with_aggregate_value_mode(
            engine_checkpoint,
            value_mode,
        )
        .map_err(|_| invalid_checkpoint())?;
        Ok(Self {
            identity: checkpoint.identity,
            catalogs: payload.catalogs,
            input_schemas: payload.input_schemas,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            engine,
            left_state: payload.left_state,
            right_state: payload.right_state,
            input_frontiers: checkpoint.input_frontiers,
            applied_epochs: payload
                .applied_epochs
                .into_iter()
                .map(|entry| (entry.idempotency_key, entry.logical_epoch))
                .collect(),
        })
    }
}

fn validate_checkpoint_frontiers(
    checkpoint: &RuntimeCheckpoint,
    payload: &GenericCheckpointPayload,
) -> Result<(), StandingProgramRuntimeError> {
    if checkpoint.input_frontiers.len() > 1 {
        return Err(invalid_checkpoint());
    }
    for frontier in &checkpoint.input_frontiers {
        if frontier.relation_id != payload.input_schema.relation_id
            || frontier.relation_version != payload.input_schema.relation_version
        {
            return Err(invalid_checkpoint());
        }
    }
    if checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len() {
        return Err(invalid_checkpoint());
    }
    for view_id in &checkpoint.identity.view_ids {
        let Some(frontier) = checkpoint
            .output_frontiers
            .iter()
            .find(|frontier| &frontier.view_id == view_id)
        else {
            return Err(invalid_checkpoint());
        };
        if frontier.committed_epoch != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
    }
    Ok(())
}

fn validate_join_checkpoint_frontiers(
    checkpoint: &RuntimeCheckpoint,
    payload: &JoinCheckpointPayload,
) -> Result<(), StandingProgramRuntimeError> {
    if checkpoint.input_frontiers.len() > payload.input_schemas.len() {
        return Err(invalid_checkpoint());
    }
    for frontier in &checkpoint.input_frontiers {
        if !payload.input_schemas.iter().any(|schema| {
            schema.relation_id == frontier.relation_id
                && schema.relation_version == frontier.relation_version
        }) {
            return Err(invalid_checkpoint());
        }
    }
    if checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len() {
        return Err(invalid_checkpoint());
    }
    for view_id in &checkpoint.identity.view_ids {
        let Some(frontier) = checkpoint
            .output_frontiers
            .iter()
            .find(|frontier| &frontier.view_id == view_id)
        else {
            return Err(invalid_checkpoint());
        };
        if frontier.committed_epoch != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
    }
    Ok(())
}

fn validate_join_supported_schemas(
    catalogs: &[VelorixRelationCatalogV1],
    inputs: &[RelationSchema],
    output: &RelationSchema,
) -> Result<(), StandingProgramRuntimeError> {
    let [left_catalog, right_catalog] = catalogs else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalogs" });
    };
    let expected_inputs = catalogs
        .iter()
        .map(|catalog| {
            catalog.validate().map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalog" }
            })?;
            catalog_input_relation_schema(catalog).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "input_schema",
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if expected_inputs != inputs {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schemas",
        });
    }
    let [key, sum, count] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    let right_key = catalog_primary_key_column(right_catalog)?;
    let expected_key_type = sql_type_from_catalog_column(right_key)?;
    let expected_sum_type = aggregate_sum_sql_type_for_catalog(left_catalog)?;
    if output.primary_key != vec![key.name.clone()]
        || key.name != right_key.name
        || key.data_type != expected_key_type
        || sum.data_type != expected_sum_type
        || !matches!(count.data_type, SqlDataType::Int64)
        || sum.name != "sum"
        || count.name != "count"
        || key.nullable
        || sum.nullable
        || count.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    Ok(())
}

fn validate_join_plan_matches_catalogs(
    plan: &SupportedDbspJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<(), StandingProgramRuntimeError> {
    let left = join_left_catalog(plan, catalogs)?;
    let right = join_right_catalog(plan, catalogs)?;
    if plan.left_join_key_column_id != catalog_primary_key_column(left)?.column_id
        || plan.right_join_key_column_id != catalog_primary_key_column(right)?.column_id
        || plan.group_key_relation_id != right.relation_schema.relation_id
        || plan.group_key_column_id != catalog_primary_key_column(right)?.column_id
        || plan.sum_value_relation_id != left.relation_schema.relation_id
        || plan.sum_value_column_id != aggregate_value_column(left)?.column_id
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan",
        });
    }
    Ok(())
}

fn join_left_catalog<'a>(
    plan: &SupportedDbspJoinViewPlan,
    catalogs: &'a [VelorixRelationCatalogV1],
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    join_catalog_for_relation(catalogs, &plan.left_input_relation_id)
}

fn join_right_catalog<'a>(
    plan: &SupportedDbspJoinViewPlan,
    catalogs: &'a [VelorixRelationCatalogV1],
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    join_catalog_for_relation(catalogs, &plan.right_input_relation_id)
}

fn join_catalog_for_relation<'a>(
    catalogs: &'a [VelorixRelationCatalogV1],
    relation_id: &str,
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == relation_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_catalog",
        })
}

fn join_delta_against_state(
    input: &DeltaBatch,
    other_state: &DeltaBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    join_delta_against_state_with_value_source(input, other_state, true)
}

fn join_delta_against_state_with_value_source(
    input: &DeltaBatch,
    other_state: &DeltaBatch,
    input_carries_aggregate_value: bool,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let other_rows = other_state
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    let mut output = Vec::new();
    for input_record in input.records() {
        for other in other_rows
            .iter()
            .filter(|other| other.key.as_json() == input_record.key.as_json())
        {
            let weight = checked_weight_product(input_record.weight, other.weight)?;
            let value = if input_carries_aggregate_value {
                input_record.value.clone()
            } else {
                other.value.clone()
            };
            output.push(DeltaRecord::new(input_record.key.clone(), value, weight));
        }
    }
    Ok(DeltaBatch::from_records(output))
}

fn checked_weight_product(left: i64, right: i64) -> Result<i64, StandingProgramRuntimeError> {
    i128::from(left)
        .checked_mul(i128::from(right))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(invalid_runtime_state)
}

fn only_schema(
    schemas: &[RelationSchema],
    field: &'static str,
) -> Result<RelationSchema, StandingProgramRuntimeError> {
    let [schema] = schemas else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
    };
    Ok(schema.clone())
}

fn validate_runtime_package(
    identity: &StandingProgramIdentity,
) -> Result<(), StandingProgramRuntimeError> {
    if identity
        .runtime_packages
        .iter()
        .any(|package| package.name == CRATE_NAME)
    {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "runtime_packages",
        })
    }
}

fn validate_supported_schemas(
    catalog: &VelorixRelationCatalogV1,
    input: &RelationSchema,
    output: &RelationSchema,
) -> Result<(), StandingProgramRuntimeError> {
    catalog
        .validate()
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalog" })?;
    let expected_input = catalog_input_relation_schema(catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        }
    })?;
    if &expected_input != input {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        });
    }
    let [key, sum, count] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    let key_column = catalog_primary_key_column(catalog)?;
    let expected_key_type = sql_type_from_catalog_column(key_column)?;
    let expected_sum_type = aggregate_sum_sql_type_for_catalog(catalog)?;
    if output.primary_key != vec![key.name.clone()]
        || key.name != key_column.name
        || key.data_type != expected_key_type
        || sum.data_type != expected_sum_type
        || !matches!(count.data_type, SqlDataType::Int64)
        || sum.name != "sum"
        || count.name != "count"
        || key.nullable
        || sum.nullable
        || count.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    Ok(())
}

fn validate_plan_matches_catalog(
    plan: &SupportedDbspViewPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.input_relation_id != catalog.relation_schema.relation_id
        || plan.group_key_column_id != catalog_primary_key_column(catalog)?.column_id
        || plan.sum_value_column_id != aggregate_value_column(catalog)?.column_id
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan",
        });
    }
    if let Some(predicate) = &plan.predicate {
        let column = catalog_column(catalog, &predicate.column_id)?;
        if column.column_id != catalog_primary_key_column(catalog)?.column_id
            && column.column_id != aggregate_value_column(catalog)?.column_id
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.column",
            });
        }
    }
    Ok(())
}

fn validate_view_sql_hash(
    identity: &StandingProgramIdentity,
    view_sql: &str,
) -> Result<(), StandingProgramRuntimeError> {
    let sql_hash = feldera_artifact_bytes_hash(view_sql.as_bytes());
    if sql_hash == identity.sql_hash {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_sql",
        })
    }
}

fn default_sql_for_catalog(
    catalog: &VelorixRelationCatalogV1,
) -> Result<String, StandingProgramRuntimeError> {
    let key_column = catalog_primary_key_column(catalog)?;
    let value_column = aggregate_value_column(catalog)?;
    Ok(format!(
        "select {}, sum({}) as sum, count(*) as count from {} group by {}",
        key_column.name, value_column.name, catalog.datafusion_registration.name, key_column.name
    ))
}

fn catalog_column<'a>(
    catalog: &'a VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<&'a velorix_core::relation::RelationColumnV1, StandingProgramRuntimeError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate",
        })
}

fn predicate_matches_record(
    predicate: &DbspRowPredicate,
    catalog: &VelorixRelationCatalogV1,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let column = catalog_column(catalog, &predicate.column_id)?;
    let actual = if column.column_id == catalog_primary_key_column(catalog)?.column_id {
        record.key.as_json()
    } else if column.column_id == aggregate_value_column(catalog)?.column_id {
        record.value.as_json()
    } else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.column",
        });
    };
    compare_catalog_scalar(column, actual, predicate.op, &predicate.literal)
}

fn filter_delta_batch_for_plan(
    delta: &DeltaBatch,
    plan: &SupportedDbspViewPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(predicate) = &plan.predicate else {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        if predicate_matches_record(predicate, catalog, record)? {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

fn compare_catalog_scalar(
    column: &velorix_core::relation::RelationColumnV1,
    actual: &Value,
    op: DbspPredicateOp,
    literal: &Value,
) -> Result<bool, StandingProgramRuntimeError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            return Ok(compare_ord(
                actual_i128(actual)?,
                op,
                literal_i128(literal)?,
            ));
        }
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            let actual = decimal_value_i128(actual, *precision, *scale)?;
            let expected = decimal_value_i128(literal, *precision, *scale)?;
            return Ok(compare_ord(actual, op, expected));
        }
        ArrowPhysicalTypeV1::Float64 => {
            return Ok(compare_ord(actual_f64(actual)?, op, literal_f64(literal)?));
        }
        _ => {}
    }

    match (actual, literal) {
        (Value::Number(actual), Value::Number(expected)) => Ok(compare_ord(
            actual.as_i64().ok_or_else(invalid_runtime_state)?,
            op,
            expected
                .as_i64()
                .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })?,
        )),
        (Value::String(actual), Value::String(expected)) => Ok(compare_ord(actual, op, expected)),
        (Value::Bool(actual), Value::Bool(expected)) => Ok(match op {
            DbspPredicateOp::Eq => actual == expected,
            DbspPredicateOp::NotEq => actual != expected,
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.op",
                })
            }
        }),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn actual_i128(value: &Value) -> Result<i128, StandingProgramRuntimeError> {
    value
        .as_i64()
        .map(i128::from)
        .ok_or_else(invalid_runtime_state)
}

fn literal_i128(value: &Value) -> Result<i128, StandingProgramRuntimeError> {
    match value {
        Value::Number(value) => value.as_i64().map(i128::from).ok_or(
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.literal",
            },
        ),
        Value::String(value) => {
            value
                .parse::<i128>()
                .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn actual_f64(value: &Value) -> Result<f64, StandingProgramRuntimeError> {
    value.as_f64().ok_or_else(invalid_runtime_state)
}

fn literal_f64(value: &Value) -> Result<f64, StandingProgramRuntimeError> {
    match value {
        Value::Number(value) => {
            value
                .as_f64()
                .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })
        }
        Value::String(value) => {
            value
                .parse::<f64>()
                .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn decimal_value_i128(
    value: &Value,
    precision: u8,
    scale: u8,
) -> Result<i128, StandingProgramRuntimeError> {
    match value {
        Value::String(value) => parse_decimal128(value, precision, scale).ok_or(
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.literal",
            },
        ),
        Value::Number(value) => {
            let Some(value) = value.as_i64() else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                });
            };
            let factor = 10_i128.checked_pow(u32::from(scale)).ok_or(
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                },
            )?;
            i128::from(value).checked_mul(factor).ok_or(
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                },
            )
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn compare_ord<T: PartialOrd + PartialEq>(actual: T, op: DbspPredicateOp, expected: T) -> bool {
    match op {
        DbspPredicateOp::Eq => actual == expected,
        DbspPredicateOp::NotEq => actual != expected,
        DbspPredicateOp::Gt => actual > expected,
        DbspPredicateOp::GtEq => actual >= expected,
        DbspPredicateOp::Lt => actual < expected,
        DbspPredicateOp::LtEq => actual <= expected,
    }
}

fn catalog_primary_key_column(
    catalog: &VelorixRelationCatalogV1,
) -> Result<&velorix_core::relation::RelationColumnV1, StandingProgramRuntimeError> {
    let [primary_key] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.primary_key",
        });
    };
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == primary_key)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.primary_key",
        })
}

fn aggregate_value_column(
    catalog: &VelorixRelationCatalogV1,
) -> Result<&velorix_core::relation::RelationColumnV1, StandingProgramRuntimeError> {
    let mut columns = catalog
        .relation_schema
        .columns
        .iter()
        .filter(|column| column.semantic_role == RelationSemanticRoleV1::Value);
    let column = columns
        .next()
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.value_column",
        })?;
    if columns.next().is_some() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.value_columns",
        });
    }
    Ok(column)
}

fn aggregate_value_mode_for_catalog(
    catalog: &VelorixRelationCatalogV1,
) -> Result<AggregateValueMode, StandingProgramRuntimeError> {
    match &aggregate_value_column(catalog)?.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(AggregateValueMode::Integer),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            Ok(AggregateValueMode::Decimal128 {
                precision: *precision,
                scale: *scale,
            })
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.value_column",
        }),
    }
}

fn aggregate_sum_sql_type_for_catalog(
    catalog: &VelorixRelationCatalogV1,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    match &aggregate_value_column(catalog)?.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Int64),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => Ok(SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.value_column",
        }),
    }
}

fn sql_type_from_catalog_column(
    column: &velorix_core::relation::RelationColumnV1,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    Ok(match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => SqlDataType::Bool,
        ArrowPhysicalTypeV1::Int8 => SqlDataType::Int8,
        ArrowPhysicalTypeV1::Int16 => SqlDataType::Int16,
        ArrowPhysicalTypeV1::Int32 => SqlDataType::Int32,
        ArrowPhysicalTypeV1::Int64 => SqlDataType::Int64,
        ArrowPhysicalTypeV1::UInt8 => SqlDataType::UInt8,
        ArrowPhysicalTypeV1::UInt16 => SqlDataType::UInt16,
        ArrowPhysicalTypeV1::UInt32 => SqlDataType::UInt32,
        ArrowPhysicalTypeV1::UInt64 => SqlDataType::UInt64,
        ArrowPhysicalTypeV1::Float32 => SqlDataType::Float32,
        ArrowPhysicalTypeV1::Float64 => SqlDataType::Float64,
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::DictionaryUtf8 { .. } => SqlDataType::Utf8,
        ArrowPhysicalTypeV1::Binary => SqlDataType::Varbinary,
        ArrowPhysicalTypeV1::JsonUtf8 => SqlDataType::Json,
        ArrowPhysicalTypeV1::Date32 => SqlDataType::Date,
        ArrowPhysicalTypeV1::Time64Nanosecond => SqlDataType::Time,
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => SqlDataType::Timestamp {
            timezone: timezone.clone(),
        },
        ArrowPhysicalTypeV1::List { element_type } => SqlDataType::Array {
            element_type: Box::new(sql_type_from_arrow_physical_type(element_type)?),
        },
        ArrowPhysicalTypeV1::Struct { fields } => SqlDataType::Struct {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(SqlStructField {
                        name: field.name.clone(),
                        data_type: sql_type_from_arrow_physical_type(&field.physical_arrow_type)?,
                        nullable: field.nullable,
                    })
                })
                .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?,
        },
        ArrowPhysicalTypeV1::Map {
            key_type,
            value_type,
        } => SqlDataType::Map {
            key_type: Box::new(sql_type_from_arrow_physical_type(key_type)?),
            value_type: Box::new(sql_type_from_arrow_physical_type(value_type)?),
        },
    })
}

fn sql_type_from_arrow_physical_type(
    physical_type: &ArrowPhysicalTypeV1,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    let column = velorix_core::relation::RelationColumnV1 {
        column_id: "__type".to_string(),
        name: "__type".to_string(),
        logical_type: velorix_core::relation::VelorixLogicalTypeV1::Utf8,
        physical_arrow_type: physical_type.clone(),
        nullable: true,
        ordinal: 0,
        semantic_role: RelationSemanticRoleV1::Metadata,
    };
    sql_type_from_catalog_column(&column)
}

fn materialized_delta_to_record_batch(
    output_schema: &RelationSchema,
    state: &DeltaBatch,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let [key_column, sum_column, count_column] = output_schema.columns.as_slice() else {
        return Err(invalid_runtime_state());
    };
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let mut keys = Vec::new();
    let mut sums = Vec::new();
    let mut counts = Vec::new();
    for row in rows {
        if row.weight != 1 {
            return Err(invalid_runtime_state());
        }
        let value = row
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        keys.push(row.key.as_json().clone());
        sums.push(
            value
                .get("sum")
                .cloned()
                .ok_or_else(invalid_runtime_state)?,
        );
        counts.push(
            value
                .get("count")
                .and_then(Value::as_i64)
                .ok_or_else(invalid_runtime_state)?,
        );
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                key_column.name.as_str(),
                arrow_data_type(&key_column.data_type)?,
                false,
            ),
            Field::new(
                sum_column.name.as_str(),
                arrow_data_type(&sum_column.data_type)?,
                false,
            ),
            Field::new(count_column.name.as_str(), DataType::Int64, false),
        ])),
        vec![
            key_array(&key_column.data_type, &keys)?,
            sum_array(&sum_column.data_type, &sums)?,
            Arc::new(Int64Array::from(counts)) as ArrayRef,
        ],
    )
    .map_err(|_| invalid_runtime_state())
}

fn key_array(
    data_type: &SqlDataType,
    values: &[Value],
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.as_str().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| value.as_f64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Bool => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.as_bool().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Decimal { precision, scale } => Ok(Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .and_then(|value| parse_decimal128(value, *precision, *scale))
                            .ok_or_else(invalid_runtime_state)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_precision_and_scale(
                *precision,
                i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
            )
            .map_err(|_| invalid_runtime_state())?,
        )),
        SqlDataType::Json => Ok(Arc::new(StringArray::from(
            values.iter().map(canonical_json).collect::<Vec<_>>(),
        ))),
        SqlDataType::Date => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|value| {
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(invalid_runtime_state)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Timestamp { timezone } => Ok(Arc::new(
            TimestampNanosecondArray::from(
                values
                    .iter()
                    .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_timezone_opt(timezone.clone()),
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.key",
        }),
    }
}

fn sum_array(
    data_type: &SqlDataType,
    values: &[Value],
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Decimal { precision, scale } => Ok(Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .and_then(|value| parse_decimal128(value, *precision, *scale))
                            .ok_or_else(invalid_runtime_state)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_precision_and_scale(
                *precision,
                i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
            )
            .map_err(|_| invalid_runtime_state())?,
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.sum",
        }),
    }
}

fn arrow_data_type(data_type: &SqlDataType) -> Result<DataType, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Utf8 => Ok(DataType::Utf8),
        SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Bool => Ok(DataType::Boolean),
        SqlDataType::Decimal { precision, scale } => Ok(DataType::Decimal128(
            *precision,
            i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
        )),
        SqlDataType::Json => Ok(DataType::Utf8),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Timestamp { timezone } => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            timezone.clone().map(Into::into),
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        }),
    }
}

fn parse_decimal128(value: &str, precision: u8, scale: u8) -> Option<i128> {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let scale = usize::from(scale);
    let (whole, fractional) = match digits.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None if scale == 0 => (digits, ""),
        None => return None,
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fractional.len() != scale
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let mut magnitude = whole.parse::<i128>().ok()?;
    let factor = 10_i128.checked_pow(scale.try_into().ok()?)?;
    magnitude = magnitude.checked_mul(factor)?;
    if scale > 0 {
        magnitude = magnitude.checked_add(fractional.parse::<i128>().ok()?)?;
    }
    if magnitude.unsigned_abs().to_string().len() > usize::from(precision) {
        return None;
    }
    if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    }
}

fn invalid_checkpoint() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_checkpoint_payload",
    }
}

fn invalid_runtime_state() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_runtime_state",
    }
}

fn checkpoint_has_join_payload(checkpoint: &RuntimeCheckpoint) -> bool {
    checkpoint
        .state_payload
        .as_ref()
        .and_then(|payload| serde_json::from_str::<Value>(&payload.payload).ok())
        .and_then(|payload| {
            payload
                .get("runtime_kind")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(JOIN_RUNTIME_KIND)
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("serializing JSON scalar cannot fail")
        }
        Value::Array(values) => {
            let items = values.iter().map(canonical_json).collect::<Vec<_>>();
            format!("[{}]", items.join(","))
        }
        Value::Object(object) => {
            let mut fields = object.iter().collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(right.0));
            let items = fields
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key)
                            .expect("serializing JSON object key cannot fail"),
                        canonical_json(value)
                    )
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", items.join(","))
        }
    }
}

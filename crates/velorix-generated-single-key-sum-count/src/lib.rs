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
    delta::DeltaBatch,
    engine::{
        AggregateValueMode, EngineCheckpointPayload, IncrementalEngine, LogicalEpoch,
        PrototypeIncrementalEngine,
    },
    feldera_artifact::{
        catalog_input_relation_schema, feldera_artifact_bytes_hash, RelationSchema, SqlDataType,
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

pub fn restore_standing_runtime(
    checkpoint: RuntimeCheckpoint,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
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
    engine: EngineCheckpointPayload,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericAppliedEpoch {
    idempotency_key: String,
    logical_epoch: LogicalEpoch,
}

impl SingleKeySumCountGeneratedRuntime {
    pub fn new(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_runtime_package(&identity)?;
        validate_supported_schemas(&catalog, &input_schema, &output_schema)?;
        let value_mode = aggregate_value_mode_for_catalog(&catalog)?;
        Ok(Self {
            identity,
            catalog,
            input_schema,
            output_schema,
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
        ArrowPhysicalTypeV1::Int64 => SqlDataType::Int64,
        ArrowPhysicalTypeV1::Float64 => SqlDataType::Float64,
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::DictionaryUtf8 { .. } => SqlDataType::Utf8,
        ArrowPhysicalTypeV1::JsonUtf8 => SqlDataType::Json,
        ArrowPhysicalTypeV1::Date32 => SqlDataType::Date,
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => SqlDataType::Timestamp {
            timezone: timezone.clone(),
        },
    })
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

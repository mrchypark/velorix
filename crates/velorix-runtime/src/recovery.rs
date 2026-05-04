use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array, StringArray, StringViewArray},
    datatypes::DataType,
};
use object_store::ObjectStore;
use serde_json::json;
use thiserror::Error;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{
        EngineCheckpoint, EngineCheckpointPayload, EngineError, IncrementalEngine, LogicalEpoch,
        PrototypeIncrementalEngine, ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
    },
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSchemaError, RelationSemanticRoleV1, SchemaFingerprintV1,
        VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_storage::{
    ingest_envelope::IngestEnvelope,
    log::{IngestLog, IngestLogError, ReplayCheckpoint},
    manifest::CheckpointManifest,
    state::{CheckpointPublishError, CheckpointPublisher},
};

pub const ORDERS_SUM_COUNT_OWNER: &str = "orders_sum_count";
pub const ORDERS_SUM_COUNT_RELATION_ID: &str = "orders";
pub const ORDERS_SUM_COUNT_RELATION_VERSION: &str = "2026-05-05.v1";
pub const ORDERS_SUM_COUNT_ADAPTER_ID: &str = "incremental-adapter-orders-sum-count-v1";

#[derive(Clone, Debug)]
pub struct RecoveredRuntime {
    materialized: PrototypeIncrementalEngine,
    replay_checkpoints: Vec<ReplayCheckpoint>,
    replayed_batch_count: usize,
    latest_checkpoint_version: Option<u64>,
}

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Checkpoint(#[from] CheckpointPublishError),
    #[error(transparent)]
    Ingest(#[from] IngestLogError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("unexpected state object owner `{actual}`, expected `{expected}`")]
    UnexpectedStateOwner { actual: String, expected: String },
    #[error("unsupported engine checkpoint payload schema version {0}")]
    UnsupportedEngineCheckpointPayloadSchema(u32),
    #[error(
        "state objects disagree on checkpoint logical epoch: expected={expected}, actual={actual}"
    )]
    InconsistentCheckpointLogicalEpoch {
        expected: LogicalEpoch,
        actual: LogicalEpoch,
    },
    #[error("logical epoch overflowed during recovery replay")]
    LogicalEpochOverflow,
    #[error(transparent)]
    RelationCatalog(#[from] RelationSchemaError),
    #[error("ingest relation mismatch for {field}: expected `{expected}`, actual `{actual}`")]
    IngestRelationMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("unsupported incremental adapter `{adapter_id}`")]
    UnsupportedIncrementalAdapter { adapter_id: String },
    #[error("malformed prototype Arrow ingest envelope: {reason}")]
    MalformedPrototypeArrowIngest { reason: String },
}

impl RecoveredRuntime {
    pub async fn recover(store: Arc<dyn ObjectStore>) -> Result<Self, RecoveryError> {
        Self::recover_with_owner(store, ORDERS_SUM_COUNT_OWNER).await
    }

    pub async fn recover_with_owner(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
    ) -> Result<Self, RecoveryError> {
        Self::recover_with_owner_and_relation_catalog(
            store,
            expected_owner,
            orders_sum_count_relation_catalog()?,
        )
        .await
    }

    pub async fn recover_with_owner_and_relation_catalog(
        store: Arc<dyn ObjectStore>,
        expected_owner: &str,
        relation_catalog: VelorixRelationCatalogV1,
    ) -> Result<Self, RecoveryError> {
        relation_catalog.validate()?;
        let publisher = CheckpointPublisher::new(Arc::clone(&store));
        let latest_manifest = publisher.latest_manifest().await?;
        let mut materialized = PrototypeIncrementalEngine::new();

        if let Some(manifest) = latest_manifest.as_ref() {
            let mut checkpointed_state = DeltaBatch::default();
            let mut checkpoint_logical_epoch = None;
            for state_ref in &manifest.state_objects {
                if state_ref.owner != expected_owner {
                    return Err(RecoveryError::UnexpectedStateOwner {
                        actual: state_ref.owner.clone(),
                        expected: expected_owner.to_string(),
                    });
                }

                let bytes = publisher.read_state_object(state_ref).await?;
                match decode_checkpoint_state(&bytes)? {
                    DecodedCheckpointState::Versioned(checkpoint) => {
                        let logical_epoch = checkpoint.logical_epoch();
                        if let Some(expected) = checkpoint_logical_epoch {
                            if expected != logical_epoch {
                                return Err(RecoveryError::InconsistentCheckpointLogicalEpoch {
                                    expected,
                                    actual: logical_epoch,
                                });
                            }
                        } else {
                            checkpoint_logical_epoch = Some(logical_epoch);
                        }

                        checkpointed_state = checkpointed_state.combine(checkpoint.state());
                    }
                    DecodedCheckpointState::Legacy(state) => {
                        checkpointed_state = checkpointed_state.combine(&state);
                    }
                }
            }
            let logical_epoch = checkpoint_logical_epoch.unwrap_or(manifest.checkpoint_version);
            materialized = PrototypeIncrementalEngine::from_checkpoint(EngineCheckpoint::new(
                logical_epoch,
                checkpointed_state,
            ))?;
        }

        let replay_checkpoints = replay_checkpoints(latest_manifest.as_ref());
        let ingest_log = IngestLog::new(store);
        let replayed = ingest_log
            .replay_validated_envelopes_from(&replay_checkpoints)
            .await?;
        let replayed_batch_count = replayed.len();
        let mut logical_epoch = materialized.logical_epoch();

        for batch in replayed {
            let envelope =
                IngestEnvelope::decode(batch.payload().clone()).map_err(IngestLogError::from)?;
            let input = prototype_delta_batch_from_arrow_envelope(&envelope, &relation_catalog)?;
            logical_epoch = logical_epoch
                .checked_add(1)
                .ok_or(RecoveryError::LogicalEpochOverflow)?;
            materialized.push_changes(logical_epoch, &input)?;
        }

        Ok(Self {
            materialized,
            replay_checkpoints,
            replayed_batch_count,
            latest_checkpoint_version: latest_manifest.map(|manifest| manifest.checkpoint_version),
        })
    }

    pub fn materialized_state(&self) -> DeltaBatch {
        self.materialized.materialized_state()
    }

    pub fn logical_epoch(&self) -> LogicalEpoch {
        self.materialized.logical_epoch()
    }

    pub fn replay_checkpoints(&self) -> &[ReplayCheckpoint] {
        &self.replay_checkpoints
    }

    pub fn replayed_batch_count(&self) -> usize {
        self.replayed_batch_count
    }

    pub fn latest_checkpoint_version(&self) -> Option<u64> {
        self.latest_checkpoint_version
    }
}

pub fn orders_sum_count_relation_catalog() -> Result<VelorixRelationCatalogV1, RelationSchemaError>
{
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
        relation_name: "orders".to_string(),
        relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)?;

    Ok(VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: ORDERS_SUM_COUNT_ADAPTER_ID.to_string(),
        },
    })
}

enum DecodedCheckpointState {
    Versioned(EngineCheckpoint),
    Legacy(DeltaBatch),
}

fn decode_checkpoint_state(bytes: &[u8]) -> Result<DecodedCheckpointState, RecoveryError> {
    // Checkpoint state has a separate compatibility lifecycle from durable
    // ingest; the legacy DeltaBatch fallback remains intentionally scoped here.
    match serde_json::from_slice::<EngineCheckpointPayload>(bytes) {
        Ok(payload) => {
            if payload.schema_version() != ENGINE_CHECKPOINT_PAYLOAD_SCHEMA_VERSION {
                return Err(RecoveryError::UnsupportedEngineCheckpointPayloadSchema(
                    payload.schema_version(),
                ));
            }

            Ok(DecodedCheckpointState::Versioned(payload.into_checkpoint()))
        }
        Err(versioned_error) => match serde_json::from_slice::<DeltaBatch>(bytes) {
            Ok(state) => Ok(DecodedCheckpointState::Legacy(state)),
            Err(_) => Err(RecoveryError::Json(versioned_error)),
        },
    }
}

fn replay_checkpoints(manifest: Option<&CheckpointManifest>) -> Vec<ReplayCheckpoint> {
    manifest
        .into_iter()
        .flat_map(|manifest| &manifest.input_ranges)
        .map(|range| {
            ReplayCheckpoint::new(
                range.stream_id.clone(),
                range.partition_id,
                range.end_offset_exclusive,
            )
        })
        .collect()
}

fn prototype_delta_batch_from_arrow_envelope(
    envelope: &IngestEnvelope,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, RecoveryError> {
    validate_envelope_relation(envelope, catalog)?;
    if catalog.incremental_adapter.adapter_id != ORDERS_SUM_COUNT_ADAPTER_ID {
        return Err(RecoveryError::UnsupportedIncrementalAdapter {
            adapter_id: catalog.incremental_adapter.adapter_id.clone(),
        });
    }

    let key_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.primary_key_column_ids[0].as_str(),
    )?;
    let value_column = single_value_column(&catalog.relation_schema)?;
    let weight_column = relation_column(
        &catalog.relation_schema,
        catalog.relation_schema.weight_column_id.as_str(),
    )?;
    let mut records = Vec::new();

    for batch in envelope.record_batches().map_err(IngestLogError::from)? {
        validate_batch_schema_matches_relation(&batch, &catalog.relation_schema)?;
        let key = string_column(&batch, key_column.name.as_str())?;
        let value = int64_column(&batch, value_column.name.as_str())?;
        let weight = int64_column(&batch, weight_column.name.as_str())?;

        for row in 0..batch.num_rows() {
            if key.is_null(row) || value.is_null(row) || weight.is_null(row) {
                return Err(RecoveryError::MalformedPrototypeArrowIngest {
                    reason: "prototype ingest columns must be non-null".to_string(),
                });
            }

            records.push(DeltaRecord::new(
                DeltaKey::from_json(json!(string_value(&key, row)?)),
                DeltaValue::from_json(json!(value.value(row))),
                weight.value(row),
            ));
        }
    }

    Ok(DeltaBatch::from_records(records))
}

fn validate_envelope_relation(
    envelope: &IngestEnvelope,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), RecoveryError> {
    let header = envelope.header();
    if header.relation_id != catalog.relation_schema.relation_id {
        return Err(RecoveryError::IngestRelationMismatch {
            field: "relation_id",
            expected: catalog.relation_schema.relation_id.clone(),
            actual: header.relation_id.clone(),
        });
    }
    if header.relation_version != catalog.relation_schema.relation_version {
        return Err(RecoveryError::IngestRelationMismatch {
            field: "relation_version",
            expected: catalog.relation_schema.relation_version.clone(),
            actual: header.relation_version.clone(),
        });
    }
    if header.schema_fingerprint != catalog.schema_fingerprint.as_str() {
        return Err(RecoveryError::IngestRelationMismatch {
            field: "schema_fingerprint",
            expected: catalog.schema_fingerprint.to_string(),
            actual: header.schema_fingerprint.clone(),
        });
    }

    Ok(())
}

fn validate_batch_schema_matches_relation(
    batch: &arrow::record_batch::RecordBatch,
    relation_schema: &VelorixRelationSchemaV1,
) -> Result<(), RecoveryError> {
    if batch.num_columns() != relation_schema.columns.len() {
        return Err(RecoveryError::MalformedPrototypeArrowIngest {
            reason: format!(
                "relation column count mismatch: expected={}, actual={}",
                relation_schema.columns.len(),
                batch.num_columns()
            ),
        });
    }

    let schema = batch.schema();
    for column in &relation_schema.columns {
        let field = schema.field(column.ordinal as usize);
        if field.name() != &column.name {
            return Err(RecoveryError::MalformedPrototypeArrowIngest {
                reason: format!(
                    "relation column `{}` must appear at ordinal {}",
                    column.name, column.ordinal
                ),
            });
        }
        if field.is_nullable() != column.nullable {
            return Err(RecoveryError::MalformedPrototypeArrowIngest {
                reason: format!("relation column `{}` nullability mismatch", column.name),
            });
        }
        if !physical_arrow_type_matches(&column.physical_arrow_type, field.data_type()) {
            return Err(RecoveryError::MalformedPrototypeArrowIngest {
                reason: format!(
                    "relation column `{}` physical Arrow type mismatch",
                    column.name
                ),
            });
        }
    }

    Ok(())
}

fn physical_arrow_type_matches(expected: &ArrowPhysicalTypeV1, actual: &DataType) -> bool {
    match expected {
        ArrowPhysicalTypeV1::Boolean => actual == &DataType::Boolean,
        ArrowPhysicalTypeV1::Int64 => actual == &DataType::Int64,
        ArrowPhysicalTypeV1::Float64 => actual == &DataType::Float64,
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::JsonUtf8 => actual == &DataType::Utf8,
        ArrowPhysicalTypeV1::Date32 => actual == &DataType::Date32,
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => i8::try_from(*scale)
            .is_ok_and(|scale| actual == &DataType::Decimal128(*precision, scale)),
        ArrowPhysicalTypeV1::TimestampNanosecond { .. }
        | ArrowPhysicalTypeV1::DictionaryUtf8 { .. } => false,
    }
}

fn relation_column<'a>(
    schema: &'a VelorixRelationSchemaV1,
    column_id: &str,
) -> Result<&'a RelationColumnV1, RecoveryError> {
    schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or_else(|| RecoveryError::MalformedPrototypeArrowIngest {
            reason: format!("relation catalog is missing column `{column_id}`"),
        })
}

fn single_value_column(
    schema: &VelorixRelationSchemaV1,
) -> Result<&RelationColumnV1, RecoveryError> {
    let mut values = schema
        .columns
        .iter()
        .filter(|column| column.semantic_role == RelationSemanticRoleV1::Value);
    let Some(column) = values.next() else {
        return Err(RecoveryError::MalformedPrototypeArrowIngest {
            reason: "relation catalog must define one value column".to_string(),
        });
    };
    if values.next().is_some() {
        return Err(RecoveryError::MalformedPrototypeArrowIngest {
            reason: "prototype adapter supports exactly one value column".to_string(),
        });
    }

    Ok(column)
}

enum StringColumn<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl StringColumn<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(array) => array.is_null(row),
            Self::Utf8View(array) => array.is_null(row),
        }
    }
}

fn string_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<StringColumn<'a>, RecoveryError> {
    let column =
        batch
            .column_by_name(name)
            .ok_or_else(|| RecoveryError::MalformedPrototypeArrowIngest {
                reason: format!("missing `{name}` column"),
            })?;

    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(StringColumn::Utf8(array));
    }

    if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        return Ok(StringColumn::Utf8View(array));
    }

    Err(RecoveryError::MalformedPrototypeArrowIngest {
        reason: format!("`{name}` column must be Utf8"),
    })
}

fn string_value(column: &StringColumn<'_>, row: usize) -> Result<String, RecoveryError> {
    Ok(match column {
        StringColumn::Utf8(array) => array.value(row).to_string(),
        StringColumn::Utf8View(array) => array.value(row).to_string(),
    })
}

fn int64_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, RecoveryError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| RecoveryError::MalformedPrototypeArrowIngest {
            reason: format!("missing `{name}` column"),
        })?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| RecoveryError::MalformedPrototypeArrowIngest {
            reason: format!("`{name}` column must be Int64"),
        })
}

use std::sync::Arc;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use velorix_core::{
    delta::DeltaBatch,
    engine::LogicalEpoch,
    standing_program::{
        BuiltinRuntimeIdentity, DurableStateRoot, EpochCommit, EpochIdempotencyKey,
        NativeCodePolicy, RelationFrontier, RelationInputBatch, RuntimeCheckpoint, ScopedViewId,
        SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
        StandingProgramRuntimeError, ViewFrontier, ViewOutputBatch, ViewOutputDelta,
    },
    view_contract::{ColumnSchema, RelationSchema, SqlDataType},
};

fn valid_identity() -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant-a".to_string(),
        program_id: "program-orders".to_string(),
        view_ids: vec!["orders_by_region".to_string()],
        sql_hash: format!("sha256:{}", "1".repeat(64)),
        input_catalog_hash: format!("sha256:{}", "2".repeat(64)),
        output_schema_hash: format!("sha256:{}", "3".repeat(64)),
        planner_identity: "velorix-logical-view-planner@1".to_string(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: "velorix_native_materialized_runtime".to_string(),
            version: "builtin-v1".to_string(),
        }],
        runtime_capabilities: vec!["materialized-view-runtime".to_string()],
        runtime_compatibility: "velorix-native-materialized-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    }
}

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["apac"])) as _,
            Arc::new(Int64Array::from(vec![7])) as _,
        ],
    )
    .unwrap()
}

#[test]
fn standing_program_identity_rejects_missing_runtime_identity() {
    let mut identity = valid_identity();
    identity.builtin_runtime_identities.clear();

    let error = identity.validate().unwrap_err();

    assert!(matches!(
        error,
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "builtin_runtime_identities"
        }
    ));
}

#[test]
fn standing_program_identity_rejects_native_code_or_external_dependencies() {
    let mut identity = valid_identity();
    identity.native_code_policy = NativeCodePolicy::NativeCodeOrExternalDependenciesPresent {
        reason: "rust udf".to_string(),
    };

    let error = identity.validate().unwrap_err();

    assert!(matches!(
        error,
        StandingProgramRuntimeError::UnsupportedNativeCodePolicy { .. }
    ));
}

#[test]
fn runtime_checkpoint_rejects_program_identity_mismatch() {
    let identity = valid_identity();
    let mut other = identity.clone();
    other.program_id = "different-program".to_string();
    let checkpoint = RuntimeCheckpoint {
        identity: other,
        logical_epoch: 42,
        input_frontiers: vec![RelationFrontier {
            relation_id: "orders".to_string(),
            relation_version: "2026-05-05.v1".to_string(),
            stream_id: "test-stream".to_string(),
            partition_id: 0,
            committed_offset_exclusive: 10,
        }],
        input_event_time_frontiers: Vec::new(),
        output_frontiers: vec![ViewFrontier {
            view_id: "orders_by_region".to_string(),
            committed_epoch: 42,
        }],
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        state_root: DurableStateRoot {
            object_key: "v1/state/program-orders/root".to_string(),
            content_hash: format!("sha256:{}", "4".repeat(64)),
        },
        state_payload: None,
        output_manifest_refs: vec!["v1/outputs/orders_by_region/42.json".to_string()],
        owner_epoch: Some(9),
    };

    let error = checkpoint.validate_identity(&identity).unwrap_err();

    assert!(matches!(
        error,
        StandingProgramRuntimeError::ProgramIdentityMismatch { .. }
    ));
}

struct FakeStandingProgramRuntime {
    identity: StandingProgramIdentity,
    epoch: LogicalEpoch,
}

impl StandingProgramRuntime for FakeStandingProgramRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![orders_input_schema()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![orders_output_schema()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.epoch
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        self.identity.validate()?;
        if logical_epoch <= self.epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.epoch,
                attempted: logical_epoch,
            });
        }
        assert_eq!(idempotency_key.as_str(), "epoch-1");
        assert_eq!(input_changes[0].relation_id, "orders");
        self.epoch = logical_epoch;
        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: vec![RelationFrontier {
                relation_id: "orders".to_string(),
                relation_version: "2026-05-05.v1".to_string(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                committed_offset_exclusive: 1,
            }],
            input_event_time_frontiers: Vec::new(),
            output_deltas: vec![ViewOutputDelta {
                view_id: "orders_by_region".to_string(),
                schema_fingerprint: format!("sha256:{}", "5".repeat(64)),
                delta: DeltaBatch::default(),
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: "orders_by_region".to_string(),
                schema_fingerprint: format!("sha256:{}", "5".repeat(64)),
                batches: input_changes[0].batches.clone(),
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        _page: SnapshotPageRequest,
    ) -> Result<velorix_core::standing_program::MaterializedViewPage, StandingProgramRuntimeError>
    {
        assert_eq!(view.program_id, self.identity.program_id);
        Ok(velorix_core::standing_program::MaterializedViewPage {
            view,
            logical_epoch: self.epoch,
            schema_fingerprint: format!("sha256:{}", "5".repeat(64)),
            batches: Vec::new(),
            next_page_token: None,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.epoch,
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            output_frontiers: Vec::new(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: "v1/state/program-orders/root".to_string(),
                content_hash: format!("sha256:{}", "4".repeat(64)),
            },
            state_payload: None,
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.identity.validate()?;
        Ok(Self {
            identity: checkpoint.identity,
            epoch: checkpoint.logical_epoch,
        })
    }
}

#[test]
fn epoch_idempotency_keys_have_a_checkpoint_safe_size_bound() {
    assert!(EpochIdempotencyKey::new("k".repeat(EpochIdempotencyKey::MAX_BYTES)).is_ok());
    assert!(EpochIdempotencyKey::new("k".repeat(EpochIdempotencyKey::MAX_BYTES + 1)).is_err());
}

#[test]
fn standing_program_runtime_applies_relation_scoped_epoch_and_emits_view_scoped_commit() {
    let identity = valid_identity();
    let mut runtime = FakeStandingProgramRuntime {
        identity: identity.clone(),
        epoch: 0,
    };

    let commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                relation_id: "orders".to_string(),
                relation_version: "2026-05-05.v1".to_string(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
                start_offset_inclusive: 0,
                end_offset_exclusive: 1,
                event_time_watermark: None,
                batches: vec![sample_batch()],
            }],
        )
        .unwrap();

    assert_eq!(commit.logical_epoch, 1);
    assert_eq!(commit.output_deltas[0].view_id, "orders_by_region");
    assert_eq!(commit.output_batches[0].view_id, "orders_by_region");
    assert_eq!(commit.output_batches[0].batches[0].num_rows(), 1);

    let checkpoint = runtime.checkpoint().unwrap();
    checkpoint.validate_identity(&identity).unwrap();
    let restored = FakeStandingProgramRuntime::restore(checkpoint).unwrap();
    assert_eq!(restored.logical_epoch(), 1);
}

fn orders_input_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
        columns: vec![
            ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["region".to_string()],
    }
}

fn orders_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "orders_by_region".to_string(),
        relation_name: "orders_by_region".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
        columns: vec![
            ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["region".to_string()],
    }
}

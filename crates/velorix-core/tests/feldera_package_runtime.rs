#![cfg(feature = "feldera-package-compat")]

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use velorix_core::{
    feldera_artifact::{ColumnSchema, RelationSchema, SqlDataType},
    feldera_package_runtime::{FelderaExecutableProgram, FelderaPackageRuntime},
    standing_program::{
        DurableStateRoot, EpochCommit, EpochIdempotencyKey, FelderaRuntimePackageIdentity,
        MaterializedViewPage, NativeCodePolicy, RelationFrontier, RelationInputBatch,
        RuntimeCheckpoint, ScopedViewId, SnapshotPageRequest, StandingProgramIdentity,
        StandingProgramRuntime, StandingProgramRuntimeError, ViewFrontier, ViewOutputBatch,
    },
};

#[test]
fn feldera_package_runtime_delegates_relation_scoped_epoch_to_executable_program() {
    let identity = identity("scores_program");
    let executable = FakeExecutableProgram::new(identity.clone());
    let mut runtime = FelderaPackageRuntime::new(identity.clone(), executable).unwrap();
    let idempotency_key = EpochIdempotencyKey::new("scores-0-3").unwrap();

    let commit = runtime
        .apply_changes(
            1,
            idempotency_key.clone(),
            vec![RelationInputBatch {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                batches: vec![scores_input_batch()],
            }],
        )
        .unwrap();

    assert_eq!(runtime.program_identity(), &identity);
    assert_eq!(runtime.input_schemas(), vec![scores_input_schema()]);
    assert_eq!(runtime.output_schemas(), vec![scores_output_schema()]);
    assert_eq!(runtime.logical_epoch(), 1);
    assert_eq!(commit.logical_epoch, 1);
    assert_eq!(commit.idempotency_key, idempotency_key);
    assert_eq!(
        commit.input_frontiers,
        vec![RelationFrontier {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            committed_offset_exclusive: 3,
        }]
    );
    assert_eq!(commit.output_batches[0].view_id, "scores_by_user");
    assert_eq!(commit.output_batches[0].batches[0].num_rows(), 1);
}

#[test]
fn feldera_package_runtime_fails_closed_when_executable_identity_does_not_match() {
    let expected = identity("scores_program");
    let executable = FakeExecutableProgram::new(identity("other_program"));

    let error = FelderaPackageRuntime::new(expected, executable).unwrap_err();

    assert_eq!(
        error,
        StandingProgramRuntimeError::ProgramIdentityMismatch {
            expected_program_id: "scores_program".to_string(),
            actual_program_id: "other_program".to_string(),
        }
    );
}

#[test]
fn feldera_package_runtime_rejects_unknown_view_without_committing_candidate_state() {
    let identity = identity("scores_program");
    let executable =
        FakeExecutableProgram::new(identity.clone()).with_output_view("unexpected_view");
    let mut runtime = FelderaPackageRuntime::new(identity.clone(), executable).unwrap();

    let error = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("scores-0-3").unwrap(),
            vec![RelationInputBatch {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                batches: vec![scores_input_batch()],
            }],
        )
        .unwrap_err();

    assert_eq!(
        error,
        StandingProgramRuntimeError::UnknownView {
            view_id: "unexpected_view".to_string(),
        }
    );
    assert_eq!(runtime.logical_epoch(), 0);
    assert!(runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: "scores_by_user".to_string(),
            },
            SnapshotPageRequest::default(),
        )
        .unwrap()
        .batches
        .is_empty());
}

#[derive(Clone, Debug)]
struct FakeExecutableProgram {
    identity: StandingProgramIdentity,
    logical_epoch: u64,
    materialized: Option<RecordBatch>,
    output_view_id: String,
}

impl FakeExecutableProgram {
    fn new(identity: StandingProgramIdentity) -> Self {
        Self {
            identity,
            logical_epoch: 0,
            materialized: None,
            output_view_id: "scores_by_user".to_string(),
        }
    }

    fn with_output_view(mut self, output_view_id: &str) -> Self {
        self.output_view_id = output_view_id.to_string();
        self
    }
}

impl FelderaExecutableProgram for FakeExecutableProgram {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![scores_input_schema()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![scores_output_schema()]
    }

    fn logical_epoch(&self) -> u64 {
        self.logical_epoch
    }

    fn apply_epoch(
        &mut self,
        logical_epoch: u64,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let input = input_changes.first().unwrap();
        self.logical_epoch = logical_epoch;
        self.materialized = Some(scores_view_batch());

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: vec![RelationFrontier {
                relation_id: input.relation_id.clone(),
                relation_version: input.relation_version.clone(),
                committed_offset_exclusive: input.end_offset_exclusive,
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: self.output_view_id.clone(),
                schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
                batches: vec![scores_view_batch()],
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        _page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
            batches: self.materialized.iter().cloned().collect(),
            next_page_token: None,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.logical_epoch,
            input_frontiers: vec![RelationFrontier {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                committed_offset_exclusive: 3,
            }],
            output_frontiers: vec![ViewFrontier {
                view_id: "scores_by_user".to_string(),
                committed_epoch: self.logical_epoch,
            }],
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: "v1/state/fake".to_string(),
                content_hash: format!("sha256:{}", "4".repeat(64)),
            },
            state_payload: None,
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        Ok(Self {
            identity: checkpoint.identity,
            logical_epoch: checkpoint.logical_epoch,
            materialized: None,
            output_view_id: "scores_by_user".to_string(),
        })
    }
}

fn identity(program_id: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant-a".to_string(),
        program_id: program_id.to_string(),
        view_ids: vec!["scores_by_user".to_string()],
        sql_hash: format!("sha256:{}", "a".repeat(64)),
        input_catalog_hash: format!("sha256:{}", "b".repeat(64)),
        output_schema_hash: format!("sha256:{}", "c".repeat(64)),
        compiler_identity: "feldera-sql-compiler:0.299.0".to_string(),
        runtime_packages: vec![
            FelderaRuntimePackageIdentity {
                name: "dbsp".to_string(),
                version: "0.299.0".to_string(),
            },
            FelderaRuntimePackageIdentity {
                name: "feldera-sqllib".to_string(),
                version: "0.299.0".to_string(),
            },
        ],
        package_feature_set: vec!["feldera-package-compat".to_string()],
        dbsp_runtime_compatibility: "dbsp-0.299.0".to_string(),
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
            Arc::new(Int64Array::from(vec![5, 7, 11])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn scores_view_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("sum", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![12])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn scores_input_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
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

fn scores_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user".to_string(),
        relation_name: "scores_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
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

use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{datasource::MemTable, prelude::SessionContext};
use serde_json::json;
use velorix_core::{
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochIdempotencyKey, NativeCodePolicy, RelationInputBatch,
        ScopedViewId, SnapshotPageRequest, StandingInputChangeV1, StandingProgramIdentity,
        StandingProgramRuntime,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
};
use velorix_runtime::{
    frontier_conformance::{
        CommittedFrontierEvidenceV1, FrontierConformanceVerifierV1, WeightedCanonicalRowV1,
    },
    materialized_view_runtime::{
        create_standing_runtime_with_sql_and_catalogs, restore_standing_runtime, CRATE_NAME,
    },
};

#[tokio::test]
async fn committed_velorix_frontiers_match_independent_datafusion_delta_and_snapshot() {
    let sql = "select user_id, score from scores where score > 0";
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_output_schema();
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &standing_identity(sql),
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let mut source = BTreeMap::new();
    let mut verifier = FrontierConformanceVerifierV1::default();

    source.insert("alice".to_string(), 10);
    source.insert("bob".to_string(), -3);
    let first = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![input(&catalog, 0, 2, &[("alice", 10, 1), ("bob", -3, 1)])],
        )
        .unwrap();
    verify_frontier(
        &mut verifier,
        runtime.as_ref(),
        &source,
        &first.output_deltas[0].delta,
        1,
    )
    .await;

    source.insert("alice".to_string(), 12);
    source.insert("carol".to_string(), 7);
    let second = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![input(
                &catalog,
                2,
                5,
                &[("alice", 10, -1), ("alice", 12, 1), ("carol", 7, 1)],
            )],
        )
        .unwrap();
    verify_frontier(
        &mut verifier,
        runtime.as_ref(),
        &source,
        &second.output_deltas[0].delta,
        2,
    )
    .await;

    let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    source.remove("carol");
    let third = runtime
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![input(&catalog, 5, 6, &[("carol", 7, -1)])],
        )
        .unwrap();
    verify_frontier(
        &mut verifier,
        runtime.as_ref(),
        &source,
        &third.output_deltas[0].delta,
        3,
    )
    .await;

    assert_eq!(verifier.last_verified_frontier(), Some(3));
}

async fn verify_frontier(
    verifier: &mut FrontierConformanceVerifierV1,
    runtime: &(dyn StandingProgramRuntime + Send),
    source: &BTreeMap<String, i64>,
    delta: &velorix_core::delta::DeltaBatch,
    frontier: u64,
) {
    verifier
        .verify_committed_frontier(CommittedFrontierEvidenceV1 {
            frontier,
            oracle_snapshot: batch_oracle_snapshot(source).await,
            observed_delta: delta
                .net_rows()
                .unwrap()
                .into_iter()
                .map(|record| WeightedCanonicalRowV1 {
                    row: canonical_row(
                        record.key.as_json().as_str().unwrap(),
                        record.value.as_json()["score"].as_i64().unwrap(),
                    ),
                    weight: record.weight,
                })
                .collect(),
            observed_snapshot: runtime_snapshot(runtime, frontier),
        })
        .unwrap();
}

async fn batch_oracle_snapshot(source: &BTreeMap<String, i64>) -> Vec<WeightedCanonicalRowV1> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from_iter_values(source.keys())) as _,
            Arc::new(Int64Array::from_iter_values(source.values().copied())) as _,
        ],
    )
    .unwrap();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    let context = SessionContext::new();
    context.register_table("scores", Arc::new(table)).unwrap();
    let batches = context
        .sql("select user_id, score from scores where score > 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(canonical_rows_from_batch)
        .map(|row| WeightedCanonicalRowV1 { row, weight: 1 })
        .collect()
}

fn runtime_snapshot(
    runtime: &(dyn StandingProgramRuntime + Send),
    frontier: u64,
) -> Vec<WeightedCanonicalRowV1> {
    runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-conformance".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(frontier),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap()
        .batches
        .iter()
        .flat_map(canonical_rows_from_batch)
        .map(|row| WeightedCanonicalRowV1 { row, weight: 1 })
        .collect()
}

fn canonical_rows_from_batch(batch: &RecordBatch) -> Vec<String> {
    let users = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let scores = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|index| canonical_row(users.value(index), scores.value(index)))
        .collect()
}

fn canonical_row(user_id: &str, score: i64) -> String {
    json!([user_id, score]).to_string()
}

fn input(
    catalog: &VelorixRelationCatalogV1,
    start: u64,
    end: u64,
    rows: &[(&str, i64, i64)],
) -> StandingInputChangeV1 {
    StandingInputChangeV1::Source(RelationInputBatch {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: "scores-stream".to_string(),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive: start,
        end_offset_exclusive: end,
        event_time_watermark: None,
        batches: vec![RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("score", DataType::Int64, false),
                Field::new("delta", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(user, _, _)| *user),
                )) as _,
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|(_, score, _)| *score),
                )) as _,
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|(_, _, weight)| *weight),
                )) as _,
            ],
        )
        .unwrap()],
    })
}

fn scores_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["user_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "scores".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "scores".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn scores_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: "positive-scores-v1".to_string(),
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
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn standing_identity(sql: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant-a".to_string(),
        program_id: "program-conformance".to_string(),
        view_ids: vec!["positive_scores".to_string()],
        sql_hash: stable_bytes_hash(sql.as_bytes()),
        input_catalog_hash: format!("sha256:{}", "1".repeat(64)),
        output_schema_hash: format!("sha256:{}", "2".repeat(64)),
        planner_identity: "velorix-logical-view-planner@1".to_string(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: CRATE_NAME.to_string(),
            version: "0.1.0".to_string(),
        }],
        runtime_capabilities: vec!["materialized-view-runtime-v1".to_string()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        dependency_binding_digest: String::new(),
        authenticated_tenant_id: "default".to_string(),
    }
}

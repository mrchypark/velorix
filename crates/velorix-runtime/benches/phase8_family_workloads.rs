//! Phase 8 family workload benchmarks (interval join, recursive fixpoint,
//! cross join). These exercise the runtime families end-to-end through the
//! same admission and runtime path as the public API, with the resource
//! contracts enforced by the runtimes themselves.

use std::{error::Error, sync::Arc, time::Duration, time::Instant};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use velorix_core::{
    relation::{
        ArrowPhysicalTypeV1, RelationColumnV1, RelationOperationV1, RelationSemanticRoleV1,
        VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochIdempotencyKey, NativeCodePolicy, RelationInputBatch,
        RelationInputEncodingV1, ScopedViewId, SnapshotPageRequest, StandingProgramIdentity,
        StandingProgramRuntime,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
};
use velorix_runtime::materialized_view_runtime::{
    create_standing_runtime_with_sql_and_catalogs, CRATE_NAME,
};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(super) struct Phase8WorkloadMeasurement {
    pub(super) name: String,
    pub(super) samples: Vec<Duration>,
}

pub(super) fn run() -> BenchResult<Vec<Phase8WorkloadMeasurement>> {
    Ok(vec![
        interval_join_workload()?,
        recursive_fixpoint_workload()?,
        cross_join_workload()?,
    ])
}

const INTERVAL_JOIN_SQL: &str =
    "select l.ride_id, l.booking_start, l.booking_end_time from rides l join vehicles v on l.booking_start < v.capacity_end and v.capacity_start_time < l.booking_end_time";

fn interval_join_workload() -> BenchResult<Phase8WorkloadMeasurement> {
    let rides = interval_side_catalog("rides", "ride_id", "booking_start", "booking_end_time");
    let vehicles = interval_side_catalog(
        "vehicles",
        "vehicle_id",
        "capacity_start_time",
        "capacity_end",
    );
    let catalogs = vec![rides.clone(), vehicles.clone()];
    let input_schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let output_schema = RelationSchema {
        relation_id: "interval_matches".to_string(),
        relation_name: "interval_matches".to_string(),
        relation_version: "2026-08-13.v1".to_string(),
        schema_fingerprint: "interval-bench-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "ride_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "booking_start".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "booking_end_time".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "vehicle_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
        ],
        primary_key: vec![
            "ride_id".to_string(),
            "booking_start".to_string(),
            "booking_end_time".to_string(),
            "vehicle_id".to_string(),
        ],
    };
    let identity = standing_identity(INTERVAL_JOIN_SQL, "interval_matches");

    const SIDE_ROWS: u64 = 2_000;
    const SAMPLES: u32 = 2;
    let mut samples = Vec::new();
    for sample in 0..SAMPLES {
        let start = sample as u64 * SIDE_ROWS;
        let rides_batch = interval_side_batch(
            &rides,
            "ride_id",
            "booking_start",
            "booking_end_time",
            start,
            start + SIDE_ROWS,
        );
        let vehicles_batch = interval_side_batch(
            &vehicles,
            "vehicle_id",
            "capacity_start_time",
            "capacity_end",
            start,
            start + SIDE_ROWS,
        );
        let epoch = sample as u64 + 1;
        let identity = standing_identity(INTERVAL_JOIN_SQL, "interval_matches");
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            &catalogs,
            INTERVAL_JOIN_SQL,
            &input_schemas,
            std::slice::from_ref(&output_schema),
        )
        .map_err(std::io::Error::other)?;
        let started = Instant::now();
        runtime.apply_changes(
            epoch,
            EpochIdempotencyKey::new(format!("interval-bench-{epoch}"))?,
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: rides.relation_schema.relation_id.clone(),
                    relation_version: rides.relation_schema.relation_version.clone(),
                    stream_id: "bench-rides".to_string(),
                    partition_id: 0,
                    schema_fingerprint: rides.schema_fingerprint.to_string(),
                    start_offset_inclusive: start,
                    end_offset_exclusive: start + SIDE_ROWS,
                    event_time_watermark: None,
                    batches: vec![rides_batch],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: vehicles.relation_schema.relation_id.clone(),
                    relation_version: vehicles.relation_schema.relation_version.clone(),
                    stream_id: "bench-vehicles".to_string(),
                    partition_id: 0,
                    schema_fingerprint: vehicles.schema_fingerprint.to_string(),
                    start_offset_inclusive: start,
                    end_offset_exclusive: start + SIDE_ROWS,
                    event_time_watermark: None,
                    batches: vec![vehicles_batch],
                },
            ],
        )?;
        assert_materialized_row_count(runtime.as_ref(), "interval_matches", epoch, SIDE_ROWS)?;
        samples.push(started.elapsed());
    }
    Ok(Phase8WorkloadMeasurement {
        name: "interval_join_epoch_apply".to_string(),
        samples,
    })
}

const RECURSIVE_FIXPOINT_SQL: &str =
    "with recursive reach as (select src, dst from edges union distinct select r.src, e.dst from reach r join edges e on r.dst = e.src) select src, dst from reach";

fn recursive_fixpoint_workload() -> BenchResult<Phase8WorkloadMeasurement> {
    let edges = edge_catalog();
    let input_schema = catalog_input_relation_schema(&edges).unwrap();
    let output_schema = RelationSchema {
        relation_id: "reachability".to_string(),
        relation_name: "reachability".to_string(),
        relation_version: "2026-08-13.v1".to_string(),
        schema_fingerprint: "recursive-bench-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "src".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "dst".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
        ],
        primary_key: vec!["src".to_string(), "dst".to_string()],
    };
    let identity = standing_identity(RECURSIVE_FIXPOINT_SQL, "reachability");
    let _ = &input_schema;

    const STAR_ROWS: u64 = 500;
    const CHAIN_ROWS: u64 = 15;
    const SAMPLES: u32 = 2;
    let mut samples = Vec::new();
    for sample in 0..SAMPLES {
        let mut sample_runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            std::slice::from_ref(&edges),
            RECURSIVE_FIXPOINT_SQL,
            &[catalog_input_relation_schema(&edges).unwrap()],
            std::slice::from_ref(&output_schema),
        )
        .map_err(std::io::Error::other)?;
        let start = sample as u64 * (STAR_ROWS + CHAIN_ROWS);
        // Star edges (hub -> every leaf) keep the closure shallow; the
        // chain (n0->n1->...->n29) exercises multi-iteration fixpoint
        // propagation within the resource contract.
        let mut ids = Vec::new();
        let mut srcs = Vec::new();
        let mut dsts = Vec::new();
        for index in 0..STAR_ROWS {
            ids.push(format!("s{start}-{index}"));
            srcs.push("hub".to_string());
            dsts.push(format!("leaf{start}-{index}"));
        }
        for index in 0..CHAIN_ROWS {
            ids.push(format!("c{start}-{index}"));
            srcs.push(format!("n{start}-{index}"));
            dsts.push(format!("n{start}-{}", index + 1));
        }
        let batch = edge_id_src_dst_batch(&ids, &srcs, &dsts);
        let epoch = sample as u64 + 1;
        let started = Instant::now();
        sample_runtime.apply_changes(
            epoch,
            EpochIdempotencyKey::new(format!("recursive-bench-{epoch}"))?,
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: edges.relation_schema.relation_id.clone(),
                relation_version: edges.relation_schema.relation_version.clone(),
                stream_id: "bench-edges".to_string(),
                partition_id: 0,
                schema_fingerprint: edges.schema_fingerprint.to_string(),
                start_offset_inclusive: start,
                end_offset_exclusive: start + STAR_ROWS + CHAIN_ROWS,
                event_time_watermark: None,
                batches: vec![batch],
            }],
        )?;
        // Star closure (1000) plus chain closure (30*31/2 = 465).
        assert_materialized_row_count(
            sample_runtime.as_ref(),
            "reachability",
            epoch,
            STAR_ROWS + CHAIN_ROWS * (CHAIN_ROWS + 1) / 2,
        )?;
        samples.push(started.elapsed());
    }
    Ok(Phase8WorkloadMeasurement {
        name: "recursive_fixpoint_epoch_apply".to_string(),
        samples,
    })
}

const CROSS_JOIN_SQL: &str =
    "select s.user_id, a.account_id, s.score, a.tier from scores s cross join accounts a";

fn cross_join_workload() -> BenchResult<Phase8WorkloadMeasurement> {
    let scores = simple_side_catalog("scores", "user_id", &["score"]);
    let accounts = simple_side_catalog("accounts", "account_id", &["tier"]);
    let catalogs = vec![scores.clone(), accounts.clone()];
    let input_schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let output_schema = RelationSchema {
        relation_id: "pair_matches".to_string(),
        relation_name: "pair_matches".to_string(),
        relation_version: "2026-08-13.v1".to_string(),
        schema_fingerprint: "cross-bench-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "account_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "tier".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
        ],
        primary_key: vec![
            "user_id".to_string(),
            "account_id".to_string(),
            "score".to_string(),
            "tier".to_string(),
        ],
    };
    const SIDE_ROWS: u64 = 400;
    const SAMPLES: u32 = 2;
    let mut samples = Vec::new();
    for sample in 0..SAMPLES {
        let start = sample as u64 * SIDE_ROWS;
        let epoch = sample as u64 + 1;
        let identity = standing_identity(CROSS_JOIN_SQL, "pair_matches");
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            &catalogs,
            CROSS_JOIN_SQL,
            &input_schemas,
            std::slice::from_ref(&output_schema),
        )
        .map_err(std::io::Error::other)?;
        let started = Instant::now();
        runtime.apply_changes(
            epoch,
            EpochIdempotencyKey::new(format!("cross-bench-{epoch}"))?,
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "bench-scores".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: start,
                    end_offset_exclusive: start + SIDE_ROWS,
                    event_time_watermark: None,
                    batches: vec![simple_side_batch(&scores, start, start + SIDE_ROWS)],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "bench-accounts".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: start,
                    end_offset_exclusive: start + SIDE_ROWS,
                    event_time_watermark: None,
                    batches: vec![simple_side_batch(&accounts, start, start + SIDE_ROWS)],
                },
            ],
        )?;
        assert_materialized_row_count(
            runtime.as_ref(),
            "pair_matches",
            epoch,
            SIDE_ROWS * SIDE_ROWS,
        )?;
        samples.push(started.elapsed());
    }
    Ok(Phase8WorkloadMeasurement {
        name: "cross_join_epoch_apply".to_string(),
        samples,
    })
}

fn standing_identity(sql: &str, view_id: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant-a".to_string(),
        program_id: "program-phase8-bench".to_string(),
        view_ids: vec![view_id.to_string()],
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
    }
}

fn interval_side_catalog(
    relation_id: &str,
    key_column_id: &str,
    start_column_id: &str,
    end_column_id: &str,
) -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: relation_id.to_string(),
        relation_name: relation_id.to_string(),
        relation_version: "2026-08-13.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: key_column_id.to_string(),
                name: key_column_id.to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: start_column_id.to_string(),
                name: start_column_id.to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: end_column_id.to_string(),
                name: end_column_id.to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec![key_column_id.to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint =
        velorix_core::relation::SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        relation_source: velorix_core::relation::VelorixRelationSourceV1::SourceRelation,
        schema_version: velorix_core::relation::RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: velorix_core::relation::DataFusionRegistrationV1 {
            name: relation_id.to_string(),
            mode: velorix_core::relation::DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: velorix_core::relation::IncrementalRelationBindingV1 {
            relation_id: relation_id.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: velorix_core::relation::IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn interval_side_batch(
    catalog: &VelorixRelationCatalogV1,
    key_column: &str,
    start_column: &str,
    end_column: &str,
    start: u64,
    end: u64,
) -> RecordBatch {
    let rows = (start..end).collect::<Vec<_>>();
    let keys = rows
        .iter()
        .map(|index| format!("k{index}"))
        .collect::<Vec<_>>();
    let starts = rows
        .iter()
        .map(|index| (*index as i64) * 1000)
        .collect::<Vec<_>>();
    let ends = rows
        .iter()
        .map(|index| (*index as i64) * 1000 + 500)
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(key_column, DataType::Utf8, false),
            Field::new(start_column, DataType::Int64, false),
            Field::new(end_column, DataType::Int64, false),
            Field::new(
                catalog.relation_schema.weight_column_id.as_str(),
                DataType::Int64,
                false,
            ),
        ])),
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(starts)) as ArrayRef,
            Arc::new(Int64Array::from(ends)) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; rows.len()])) as ArrayRef,
        ],
    )
    .map_err(std::io::Error::other)
    .unwrap()
}

fn edge_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "edges".to_string(),
        relation_name: "edges".to_string(),
        relation_version: "2026-08-13.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "edge_id".to_string(),
                name: "edge_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "src".to_string(),
                name: "src".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "dst".to_string(),
                name: "dst".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["edge_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint =
        velorix_core::relation::SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        relation_source: velorix_core::relation::VelorixRelationSourceV1::SourceRelation,
        schema_version: velorix_core::relation::RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: velorix_core::relation::DataFusionRegistrationV1 {
            name: "edges".to_string(),
            mode: velorix_core::relation::DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: velorix_core::relation::IncrementalRelationBindingV1 {
            relation_id: "edges".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: velorix_core::relation::IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}


fn edge_id_src_dst_batch(ids: &[String], srcs: &[String], dsts: &[String]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::Utf8, false),
            Field::new("src", DataType::Utf8, false),
            Field::new("dst", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(srcs.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(dsts.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; ids.len()])) as ArrayRef,
        ],
    )
    .map_err(std::io::Error::other)
    .unwrap()
}

fn assert_materialized_row_count(
    runtime: &dyn StandingProgramRuntime,
    view_id: &str,
    epoch: u64,
    expected_rows: u64,
) -> BenchResult<()> {
    let page = runtime.materialized_view_page(
        ScopedViewId {
            tenant_id: "tenant-a".into(),
            program_id: "program-phase8-bench".into(),
            view_id: view_id.into(),
        },
        SnapshotPageRequest {
            committed_epoch: Some(epoch),
            page_token: None,
            max_rows: None,
        },
    )?;
    let actual = page
        .batches
        .iter()
        .map(|batch| batch.num_rows() as u64)
        .sum::<u64>();
    if actual != expected_rows {
        return Err(format!(
            "benchmark output cardinality regression: {view_id} epoch {epoch} expected {expected_rows} rows, got {actual}"
        )
        .into());
    }
    Ok(())
}

fn simple_side_catalog(
    relation_id: &str,
    key_column_id: &str,
    value_column_ids: &[&str],
) -> VelorixRelationCatalogV1 {
    let mut columns = vec![RelationColumnV1 {
        column_id: key_column_id.to_string(),
        name: key_column_id.to_string(),
        logical_type: VelorixLogicalTypeV1::Utf8,
        physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
        nullable: false,
        ordinal: 0,
        semantic_role: RelationSemanticRoleV1::PrimaryKey,
    }];
    for (index, value_column_id) in value_column_ids.iter().enumerate() {
        columns.push(RelationColumnV1 {
            column_id: value_column_id.to_string(),
            name: value_column_id.to_string(),
            logical_type: if *value_column_id == "score" {
                VelorixLogicalTypeV1::Int64
            } else {
                VelorixLogicalTypeV1::Utf8
            },
            physical_arrow_type: if *value_column_id == "score" {
                ArrowPhysicalTypeV1::Int64
            } else {
                ArrowPhysicalTypeV1::Utf8
            },
            nullable: false,
            ordinal: (index + 1) as u32,
            semantic_role: RelationSemanticRoleV1::Value,
        });
    }
    columns.push(RelationColumnV1 {
        column_id: "delta".to_string(),
        name: "delta".to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: false,
        ordinal: columns.len() as u32,
        semantic_role: RelationSemanticRoleV1::Weight,
    });
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: relation_id.to_string(),
        relation_name: relation_id.to_string(),
        relation_version: "2026-08-13.v1".to_string(),
        columns,
        primary_key_column_ids: vec![key_column_id.to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint =
        velorix_core::relation::SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        relation_source: velorix_core::relation::VelorixRelationSourceV1::SourceRelation,
        schema_version: velorix_core::relation::RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: velorix_core::relation::DataFusionRegistrationV1 {
            name: relation_id.to_string(),
            mode: velorix_core::relation::DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: velorix_core::relation::IncrementalRelationBindingV1 {
            relation_id: relation_id.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: velorix_core::relation::IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn simple_side_batch(catalog: &VelorixRelationCatalogV1, start: u64, end: u64) -> RecordBatch {
    let rows = (start..end).collect::<Vec<_>>();
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for column in catalog
        .relation_schema
        .columns
        .iter()
        .filter(|column| column.column_id != catalog.relation_schema.weight_column_id)
    {
        match column.physical_arrow_type {
            ArrowPhysicalTypeV1::Utf8 => {
                fields.push(Field::new(
                    column.name.as_str(),
                    DataType::Utf8,
                    column.nullable,
                ));
                arrays.push(Arc::new(StringArray::from(
                    rows.iter()
                        .map(|index| format!("{}_{index}", column.column_id))
                        .collect::<Vec<_>>(),
                )) as ArrayRef);
            }
            ArrowPhysicalTypeV1::Int64 => {
                fields.push(Field::new(
                    column.name.as_str(),
                    DataType::Int64,
                    column.nullable,
                ));
                arrays.push(Arc::new(Int64Array::from(
                    rows.iter().map(|index| *index as i64).collect::<Vec<_>>(),
                )) as ArrayRef);
            }
            _ => unreachable!("benchmark side batch only uses utf8 and int64"),
        }
    }
    fields.push(Field::new(
        catalog.relation_schema.weight_column_id.as_str(),
        DataType::Int64,
        false,
    ));
    arrays.push(Arc::new(Int64Array::from(vec![1_i64; rows.len()])) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(std::io::Error::other)
        .unwrap()
}

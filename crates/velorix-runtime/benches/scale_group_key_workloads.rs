use std::{error::Error, sync::Arc, time::Duration, time::Instant};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde::Deserialize;
use velorix_core::{
    relation::{
        ArrowPhysicalTypeV1, RelationColumnV1, RelationOperationV1, RelationSemanticRoleV1,
        VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochIdempotencyKey, NativeCodePolicy, RelationInputBatch,
        ScopedViewId, SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
};
use velorix_runtime::materialized_view_runtime::{
    create_standing_runtime_with_sql_and_catalogs, CRATE_NAME,
};

const CORPUS: &str = include_str!("fixtures/incremental_sql_corpus_v1.json");
const RELATION_ID: &str = "scale_orders";
const VIEW_ID: &str = "scale_order_groups";

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(super) struct ScaleWorkloadMeasurement {
    pub(super) name: String,
    pub(super) samples: Vec<Duration>,
}

#[derive(Deserialize)]
struct Corpus {
    scale_workloads: Vec<ScaleWorkload>,
}

#[derive(Deserialize)]
struct ScaleWorkload {
    id: String,
    sql: String,
    distribution: Distribution,
    total_rows: u64,
    batch_rows: u64,
    distinct_groups: u64,
    hot_group_basis_points: u16,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Distribution {
    HighCardinality,
    HotKeySkew,
}

pub(super) fn run() -> BenchResult<Vec<ScaleWorkloadMeasurement>> {
    let corpus: Corpus = serde_json::from_str(CORPUS)?;
    corpus.scale_workloads.iter().map(run_workload).collect()
}

fn run_workload(workload: &ScaleWorkload) -> BenchResult<ScaleWorkloadMeasurement> {
    validate(workload)?;
    let catalog = scale_orders_catalog()?;
    let input_schema = catalog_input_relation_schema(&catalog)?;
    let output_schema = output_schema();
    let identity = identity(workload);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        &workload.sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .map_err(std::io::Error::other)?;

    let mut samples = Vec::new();
    for (epoch, start) in (0..workload.total_rows)
        .step_by(workload.batch_rows as usize)
        .enumerate()
    {
        let end = start + workload.batch_rows;
        let started = Instant::now();
        runtime.apply_changes(
            epoch as u64 + 1,
            EpochIdempotencyKey::new(format!("{}-epoch-{}", workload.id, epoch + 1))?,
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: workload.id.clone(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: start,
                end_offset_exclusive: end,
                event_time_watermark: None,
                batches: vec![input_batch(workload, start, end)?],
            }],
        )?;
        samples.push(started.elapsed());
    }

    assert_final_snapshot(runtime.as_ref(), &identity, workload)?;
    Ok(ScaleWorkloadMeasurement {
        name: workload.id.clone(),
        samples,
    })
}

fn validate(workload: &ScaleWorkload) -> BenchResult<()> {
    let valid = !workload.id.trim().is_empty()
        && !workload.sql.trim().is_empty()
        && workload.total_rows > 0
        && workload.batch_rows > 0
        && workload.total_rows % workload.batch_rows == 0
        && workload.distinct_groups > 1
        && workload.distinct_groups <= workload.total_rows
        && match workload.distribution {
            Distribution::HighCardinality => {
                workload.distinct_groups == workload.total_rows
                    && workload.hot_group_basis_points == 0
            }
            Distribution::HotKeySkew => {
                (8_000..10_000).contains(&workload.hot_group_basis_points)
                    && workload.distinct_groups < workload.total_rows
            }
        };
    if valid {
        Ok(())
    } else {
        Err(
            std::io::Error::other(format!("invalid scale workload contract `{}`", workload.id))
                .into(),
        )
    }
}

fn group_index(workload: &ScaleWorkload, row: u64) -> u64 {
    match workload.distribution {
        Distribution::HighCardinality => row,
        Distribution::HotKeySkew => {
            let hot_rows =
                workload.total_rows * u64::from(workload.hot_group_basis_points) / 10_000;
            if row < hot_rows {
                0
            } else {
                1 + (row - hot_rows) % (workload.distinct_groups - 1)
            }
        }
    }
}

fn input_batch(workload: &ScaleWorkload, start: u64, end: u64) -> BenchResult<RecordBatch> {
    let rows = start..end;
    let order_ids = rows
        .clone()
        .map(|row| format!("order-{row:08}"))
        .collect::<Vec<_>>();
    let groups = rows
        .clone()
        .map(|row| group_index(workload, row))
        .collect::<Vec<_>>();
    let customer_ids = groups
        .iter()
        .map(|group| format!("customer-{:06}", group / 64))
        .collect::<Vec<_>>();
    let categories = groups
        .iter()
        .map(|group| format!("category-{:02}", group % 64))
        .collect::<Vec<_>>();
    let amounts = rows
        .clone()
        .map(|row| (row % 101 + 1) as i64)
        .collect::<Vec<_>>();
    let weights = vec![1_i64; (end - start) as usize];

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(order_ids)) as ArrayRef,
            Arc::new(StringArray::from(customer_ids)) as ArrayRef,
            Arc::new(StringArray::from(categories)) as ArrayRef,
            Arc::new(Int64Array::from(amounts)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )?)
}

fn assert_final_snapshot(
    runtime: &(dyn StandingProgramRuntime + Send),
    identity: &StandingProgramIdentity,
    workload: &ScaleWorkload,
) -> BenchResult<()> {
    let page = runtime.materialized_view_page(
        ScopedViewId {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: VIEW_ID.to_string(),
        },
        SnapshotPageRequest {
            committed_epoch: Some(workload.total_rows / workload.batch_rows),
            page_token: None,
            max_rows: None,
        },
    )?;
    let batch = page
        .batches
        .first()
        .ok_or_else(|| std::io::Error::other("scale workload produced no output batch"))?;
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| std::io::Error::other("scale workload count output is not int64"))?;
    let observed_rows = counts.values().iter().sum::<i64>();
    let observed_max = counts.values().iter().copied().max().unwrap_or(0);
    let expected_max = match workload.distribution {
        Distribution::HighCardinality => 1,
        Distribution::HotKeySkew => {
            (workload.total_rows * u64::from(workload.hot_group_basis_points) / 10_000) as i64
        }
    };
    if batch.num_rows() != workload.distinct_groups as usize
        || observed_rows != workload.total_rows as i64
        || observed_max != expected_max
    {
        return Err(std::io::Error::other(format!(
            "scale workload `{}` output mismatch: groups={}, rows={}, max_group={}",
            workload.id,
            batch.num_rows(),
            observed_rows,
            observed_max
        ))
        .into());
    }
    Ok(())
}

fn scale_orders_catalog() -> BenchResult<VelorixRelationCatalogV1> {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: RELATION_ID.to_string(),
        relation_name: RELATION_ID.to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        columns: vec![
            relation_column(
                "order_id",
                VelorixLogicalTypeV1::Utf8,
                ArrowPhysicalTypeV1::Utf8,
                0,
                RelationSemanticRoleV1::PrimaryKey,
            ),
            relation_column(
                "customer_id",
                VelorixLogicalTypeV1::Utf8,
                ArrowPhysicalTypeV1::Utf8,
                1,
                RelationSemanticRoleV1::Metadata,
            ),
            relation_column(
                "category",
                VelorixLogicalTypeV1::Utf8,
                ArrowPhysicalTypeV1::Utf8,
                2,
                RelationSemanticRoleV1::Metadata,
            ),
            relation_column(
                "amount",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                3,
                RelationSemanticRoleV1::Value,
            ),
            relation_column(
                "delta",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                4,
                RelationSemanticRoleV1::Weight,
            ),
        ],
        primary_key_column_ids: vec!["order_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    Ok(VelorixRelationCatalogV1::from_relation_schema(
        relation_schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
    )?)
}

fn relation_column(
    name: &str,
    logical_type: VelorixLogicalTypeV1,
    physical_arrow_type: ArrowPhysicalTypeV1,
    ordinal: u32,
    semantic_role: RelationSemanticRoleV1,
) -> RelationColumnV1 {
    RelationColumnV1 {
        column_id: name.to_string(),
        name: name.to_string(),
        logical_type,
        physical_arrow_type,
        nullable: false,
        ordinal,
        semantic_role,
    }
}

fn output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: VIEW_ID.to_string(),
        relation_name: VIEW_ID.to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        schema_fingerprint: stable_bytes_hash(b"scale-order-groups-output-v1"),
        columns: vec![
            ColumnSchema {
                name: "customer_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "category".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "total".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "order_count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["customer_id".to_string(), "category".to_string()],
    }
}

fn identity(workload: &ScaleWorkload) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "local-benchmark".to_string(),
        program_id: workload.id.clone(),
        view_ids: vec![VIEW_ID.to_string()],
        sql_hash: stable_bytes_hash(workload.sql.as_bytes()),
        input_catalog_hash: stable_bytes_hash(b"scale-orders-catalog-v1"),
        output_schema_hash: stable_bytes_hash(b"scale-order-groups-output-v1"),
        planner_identity: "velorix-logical-view-planner@1".to_string(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: CRATE_NAME.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }],
        runtime_capabilities: vec!["materialized-view-runtime-v1".to_string()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        dependency_binding_digest: String::new(),
        authenticated_tenant_id: "default".to_string(),
    }
}

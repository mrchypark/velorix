use std::{collections::BTreeMap, error::Error, sync::Arc, time::Duration, time::Instant};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
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
const LEFT_RELATION_ID: &str = "join_left";
const RIGHT_RELATION_ID: &str = "join_right";
const VIEW_ID: &str = "join_distribution_counts";
const OUTER_LEFT_RELATION_ID: &str = "outer_left";
const OUTER_RIGHT_RELATION_ID: &str = "outer_right";
const OUTER_VIEW_ID: &str = "outer_join_distribution";
const OUTER_KEY_COUNT: i64 = 512;
const OUTER_SAMPLES: u32 = 5;
const OUTER_SQL: &str = "SELECT COALESCE(l.join_key, r.join_key) AS bucket, SUM(l.payload) AS sum, COUNT(*) AS count FROM outer_left l FULL OUTER JOIN outer_right r ON l.join_key = r.join_key GROUP BY COALESCE(l.join_key, r.join_key)";

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(super) struct JoinScaleWorkloadMeasurement {
    pub(super) name: String,
    pub(super) samples: Vec<Duration>,
}

#[derive(Deserialize)]
struct Corpus {
    join_scale_workloads: Vec<JoinScaleWorkload>,
}

#[derive(Deserialize)]
struct JoinScaleWorkload {
    id: String,
    sql: String,
    distribution: Distribution,
    key_count: u64,
    left_rows: u64,
    right_rows: u64,
    hot_key_basis_points: u16,
    expected_groups: u64,
    expected_matches: u64,
    expected_max_group_matches: u64,
    samples: u32,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Distribution {
    OneToOne,
    OneToMany,
    ManyToMany,
    HotKeySkew,
    Unmatched,
}

pub(super) fn run() -> BenchResult<Vec<JoinScaleWorkloadMeasurement>> {
    let corpus: Corpus = serde_json::from_str(CORPUS)?;
    let mut measurements = corpus
        .join_scale_workloads
        .iter()
        .map(run_workload)
        .collect::<BenchResult<Vec<_>>>()?;
    measurements.push(run_full_join_high_unmatched_ratio()?);
    measurements.push(run_full_join_match_transitions()?);
    Ok(measurements)
}

fn run_full_join_high_unmatched_ratio() -> BenchResult<JoinScaleWorkloadMeasurement> {
    let overlap = OUTER_KEY_COUNT / 20;
    let left_rows = (0..OUTER_KEY_COUNT)
        .map(|key| (key, key + 1, 1))
        .collect::<Vec<_>>();
    let right_rows = ((OUTER_KEY_COUNT - overlap)..(OUTER_KEY_COUNT * 2 - overlap))
        .map(|key| (key, key + 1, 1))
        .collect::<Vec<_>>();
    let expected = outer_expected(&left_rows, &right_rows)?;
    let unmatched = expected.values().filter(|(sum, _)| sum.is_none()).count()
        + left_rows
            .iter()
            .filter(|(key, _, _)| !right_rows.iter().any(|(right, _, _)| right == key))
            .count();
    if unmatched * 100 < expected.len() * 95 {
        return Err(std::io::Error::other("full join unmatched benchmark is below 95%").into());
    }
    run_outer_samples(
        "full_join_high_unmatched_ratio",
        &left_rows,
        &right_rows,
        None,
        &expected,
    )
}

fn run_full_join_match_transitions() -> BenchResult<JoinScaleWorkloadMeasurement> {
    let left_rows = (0..OUTER_KEY_COUNT)
        .map(|key| (key, key + 1, 1))
        .collect::<Vec<_>>();
    let initial_right = (OUTER_KEY_COUNT..OUTER_KEY_COUNT * 2)
        .map(|key| (key, key + 1, 1))
        .collect::<Vec<_>>();
    let mut transition = initial_right
        .iter()
        .map(|(key, payload, _)| (*key, *payload, -1))
        .collect::<Vec<_>>();
    transition.extend((0..OUTER_KEY_COUNT).map(|key| (key, key + 1, 1)));
    let final_right = (0..OUTER_KEY_COUNT)
        .map(|key| (key, key + 1, 1))
        .collect::<Vec<_>>();
    let expected = outer_expected(&left_rows, &final_right)?;
    run_outer_samples(
        "full_join_match_transitions",
        &left_rows,
        &initial_right,
        Some(&transition),
        &expected,
    )
}

fn run_outer_samples(
    name: &str,
    left_rows: &[(i64, i64, i64)],
    initial_right: &[(i64, i64, i64)],
    transition: Option<&[(i64, i64, i64)]>,
    expected: &BTreeMap<i64, (Option<i64>, i64)>,
) -> BenchResult<JoinScaleWorkloadMeasurement> {
    let left_catalog = outer_catalog(OUTER_LEFT_RELATION_ID)?;
    let right_catalog = outer_catalog(OUTER_RIGHT_RELATION_ID)?;
    let catalogs = vec![left_catalog.clone(), right_catalog.clone()];
    let input_schemas = catalogs
        .iter()
        .map(catalog_input_relation_schema)
        .collect::<Result<Vec<_>, _>>()?;
    let output_schema = outer_output_schema();
    let identity = outer_identity(name);
    let mut samples = Vec::with_capacity(OUTER_SAMPLES as usize);

    for sample in 0..OUTER_SAMPLES {
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            &catalogs,
            OUTER_SQL,
            &input_schemas,
            std::slice::from_ref(&output_schema),
        )
        .map_err(std::io::Error::other)?;
        let initial = vec![
            outer_relation_input(&left_catalog, name, 0, left_rows)?,
            outer_relation_input(&right_catalog, name, 0, initial_right)?,
        ];
        if let Some(transition) = transition {
            runtime.apply_changes(
                1,
                EpochIdempotencyKey::new(format!("{name}-initial-{sample}"))?,
                initial,
            )?;
            let started = Instant::now();
            runtime.apply_changes(
                2,
                EpochIdempotencyKey::new(format!("{name}-transition-{sample}"))?,
                vec![outer_relation_input(
                    &right_catalog,
                    name,
                    initial_right.len() as u64,
                    transition,
                )?],
            )?;
            samples.push(started.elapsed());
            assert_outer_snapshot(runtime.as_ref(), &identity, 2, expected)?;
        } else {
            let started = Instant::now();
            runtime.apply_changes(
                1,
                EpochIdempotencyKey::new(format!("{name}-initial-{sample}"))?,
                initial,
            )?;
            samples.push(started.elapsed());
            assert_outer_snapshot(runtime.as_ref(), &identity, 1, expected)?;
        }
    }
    Ok(JoinScaleWorkloadMeasurement {
        name: name.to_string(),
        samples,
    })
}

fn run_workload(workload: &JoinScaleWorkload) -> BenchResult<JoinScaleWorkloadMeasurement> {
    let left_keys = keys(workload, Side::Left);
    let right_keys = keys(workload, Side::Right);
    validate(workload, &left_keys, &right_keys)?;

    let left_catalog = catalog(LEFT_RELATION_ID)?;
    let right_catalog = catalog(RIGHT_RELATION_ID)?;
    let catalogs = vec![left_catalog.clone(), right_catalog.clone()];
    let input_schemas = catalogs
        .iter()
        .map(catalog_input_relation_schema)
        .collect::<Result<Vec<_>, _>>()?;
    let output_schema = output_schema();
    let identity = identity(workload);
    let mut samples = Vec::with_capacity(workload.samples as usize);

    for sample in 0..workload.samples {
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            &catalogs,
            &workload.sql,
            &input_schemas,
            std::slice::from_ref(&output_schema),
        )
        .map_err(std::io::Error::other)?;
        let started = Instant::now();
        runtime.apply_changes(
            1,
            EpochIdempotencyKey::new(format!("{}-sample-{sample}", workload.id))?,
            vec![
                relation_input(&left_catalog, &workload.id, &left_keys)?,
                relation_input(&right_catalog, &workload.id, &right_keys)?,
            ],
        )?;
        samples.push(started.elapsed());
        assert_final_snapshot(
            runtime.as_ref(),
            &identity,
            workload,
            &left_keys,
            &right_keys,
        )?;
    }

    Ok(JoinScaleWorkloadMeasurement {
        name: workload.id.clone(),
        samples,
    })
}

#[derive(Clone, Copy)]
enum Side {
    Left,
    Right,
}

fn keys(workload: &JoinScaleWorkload, side: Side) -> Vec<i64> {
    let rows = match side {
        Side::Left => workload.left_rows,
        Side::Right => workload.right_rows,
    };
    match workload.distribution {
        Distribution::OneToOne | Distribution::OneToMany | Distribution::ManyToMany => (0..rows)
            .map(|row| (row % workload.key_count) as i64)
            .collect(),
        Distribution::HotKeySkew => {
            let hot_rows = rows * u64::from(workload.hot_key_basis_points) / 10_000;
            (0..rows)
                .map(|row| {
                    if row < hot_rows {
                        0
                    } else {
                        (1 + (row - hot_rows) % (workload.key_count - 1)) as i64
                    }
                })
                .collect()
        }
        Distribution::Unmatched => {
            let side_offset = match side {
                Side::Left => 0,
                Side::Right => workload.key_count,
            };
            (0..rows)
                .map(|row| (side_offset + row % workload.key_count) as i64)
                .collect()
        }
    }
}

fn validate(
    workload: &JoinScaleWorkload,
    left_keys: &[i64],
    right_keys: &[i64],
) -> BenchResult<()> {
    if workload.id.trim().is_empty()
        || workload.sql.trim().is_empty()
        || workload.key_count < 2
        || workload.left_rows == 0
        || workload.right_rows == 0
        || workload.samples < 5
        || left_keys.len() != workload.left_rows as usize
        || right_keys.len() != workload.right_rows as usize
    {
        return Err(invalid_contract(workload));
    }

    let counts = |keys: &[i64]| {
        let mut counts = BTreeMap::<i64, u64>::new();
        for key in keys {
            *counts.entry(*key).or_default() += 1;
        }
        counts
    };
    let left_counts = counts(left_keys);
    let right_counts = counts(right_keys);
    let group_matches = left_counts
        .iter()
        .filter_map(|(key, left)| right_counts.get(key).map(|right| left * right))
        .collect::<Vec<_>>();
    let expected_groups = group_matches.len() as u64;
    let expected_matches = group_matches.iter().sum::<u64>();
    let expected_max = group_matches.iter().copied().max().unwrap_or(0);
    let shape_is_valid = match workload.distribution {
        Distribution::OneToOne => {
            left_counts.values().all(|count| *count == 1)
                && right_counts.values().all(|count| *count == 1)
        }
        Distribution::OneToMany => {
            left_counts.values().all(|count| *count == 1)
                && right_counts.values().any(|count| *count > 1)
        }
        Distribution::ManyToMany => {
            left_counts.values().all(|count| *count > 1)
                && right_counts.values().all(|count| *count > 1)
        }
        Distribution::HotKeySkew => {
            (8_000..10_000).contains(&workload.hot_key_basis_points)
                && expected_max.saturating_mul(2) > expected_matches
        }
        Distribution::Unmatched => left_counts
            .keys()
            .all(|key| !right_counts.contains_key(key)),
    };
    if !shape_is_valid
        || expected_groups != workload.expected_groups
        || expected_matches != workload.expected_matches
        || expected_max != workload.expected_max_group_matches
    {
        return Err(invalid_contract(workload));
    }
    Ok(())
}

fn invalid_contract(workload: &JoinScaleWorkload) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::other(format!(
        "invalid join scale workload contract `{}`",
        workload.id
    )))
}

fn relation_input(
    catalog: &VelorixRelationCatalogV1,
    workload_id: &str,
    keys: &[i64],
) -> BenchResult<RelationInputBatch> {
    Ok(RelationInputBatch {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: format!("{workload_id}-{}", catalog.relation_schema.relation_id),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive: 0,
        end_offset_exclusive: keys.len() as u64,
        event_time_watermark: None,
        batches: vec![input_batch(catalog, keys)?],
    })
}

fn input_batch(catalog: &VelorixRelationCatalogV1, keys: &[i64]) -> BenchResult<RecordBatch> {
    let ids = (0..keys.len())
        .map(|row| format!("{}-{row:08}", catalog.relation_schema.relation_id))
        .collect::<Vec<_>>();
    let payloads = (0..keys.len())
        .map(|row| i64::try_from(row + 1))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Utf8, false),
            Field::new("join_key", DataType::Int64, false),
            Field::new("payload", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(payloads)) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; keys.len()])) as ArrayRef,
        ],
    )?)
}

fn assert_final_snapshot(
    runtime: &(dyn StandingProgramRuntime + Send),
    identity: &StandingProgramIdentity,
    workload: &JoinScaleWorkload,
    left_keys: &[i64],
    right_keys: &[i64],
) -> BenchResult<()> {
    let checkpoint = runtime.checkpoint()?;
    let checkpoint_payload: serde_json::Value = serde_json::from_str(
        &checkpoint
            .state_payload
            .as_ref()
            .ok_or_else(|| std::io::Error::other("join scale checkpoint has no state payload"))?
            .payload,
    )?;
    let state_records = |side: &str| -> BenchResult<usize> {
        checkpoint_payload[side]["records"]
            .as_array()
            .map(Vec::len)
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "join scale checkpoint has no `{side}.records` array"
                ))
                .into()
            })
    };
    let left_state_records = state_records("left_state")?;
    let right_state_records = state_records("right_state")?;
    if left_state_records != left_keys.len() || right_state_records != right_keys.len() {
        return Err(std::io::Error::other(format!(
            "join scale workload `{}` collapsed per-key multiset state: left={left_state_records}/{}, right={right_state_records}/{}",
            workload.id,
            left_keys.len(),
            right_keys.len()
        ))
        .into());
    }

    let page = runtime.materialized_view_page(
        ScopedViewId {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: VIEW_ID.to_string(),
        },
        SnapshotPageRequest {
            committed_epoch: Some(1),
            page_token: None,
            max_rows: None,
        },
    )?;
    let mut left = BTreeMap::<i64, (i64, i64)>::new();
    for (row, key) in left_keys.iter().enumerate() {
        let entry = left.entry(*key).or_default();
        entry.0 = entry
            .0
            .checked_add(i64::try_from(row + 1)?)
            .ok_or_else(|| std::io::Error::other("join scale expected sum overflow"))?;
        entry.1 += 1;
    }
    let mut right = BTreeMap::<i64, i64>::new();
    for key in right_keys {
        *right.entry(*key).or_default() += 1;
    }
    let expected = left
        .iter()
        .filter_map(|(key, (left_sum, left_count))| {
            right
                .get(key)
                .map(|right_count| (*key, (left_sum * right_count, left_count * right_count)))
        })
        .collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::<i64, (i64, i64)>::new();
    for batch in &page.batches {
        let buckets = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| std::io::Error::other("join scale bucket output is not int64"))?;
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| std::io::Error::other("join scale sum output is not int64"))?;
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| std::io::Error::other("join scale count output is not int64"))?;
        for row in 0..batch.num_rows() {
            if actual
                .insert(buckets.value(row), (sums.value(row), counts.value(row)))
                .is_some()
            {
                return Err(std::io::Error::other("duplicate join scale output bucket").into());
            }
        }
    }
    if actual != expected {
        return Err(std::io::Error::other(format!(
            "join scale workload `{}` output mismatch: expected {} groups, observed {}",
            workload.id,
            expected.len(),
            actual.len()
        ))
        .into());
    }
    Ok(())
}

fn outer_expected(
    left_rows: &[(i64, i64, i64)],
    right_rows: &[(i64, i64, i64)],
) -> BenchResult<BTreeMap<i64, (Option<i64>, i64)>> {
    let mut left = BTreeMap::new();
    for (key, payload, weight) in left_rows {
        if *weight > 0 {
            left.insert(*key, *payload);
        }
    }
    let mut right = BTreeMap::new();
    for (key, _, weight) in right_rows {
        if *weight > 0 {
            right.insert(*key, ());
        }
    }
    let mut expected = BTreeMap::new();
    for (key, payload) in &left {
        expected.insert(*key, (Some(*payload), 1));
    }
    for key in right.keys() {
        expected.entry(*key).or_insert((None, 1));
    }
    Ok(expected)
}

fn outer_relation_input(
    catalog: &VelorixRelationCatalogV1,
    workload_id: &str,
    start_offset: u64,
    rows: &[(i64, i64, i64)],
) -> BenchResult<RelationInputBatch> {
    Ok(RelationInputBatch {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: format!("{workload_id}-{}", catalog.relation_schema.relation_id),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive: start_offset,
        end_offset_exclusive: start_offset + rows.len() as u64,
        event_time_watermark: None,
        batches: vec![RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("join_key", DataType::Int64, false),
                Field::new("payload", DataType::Int64, false),
                Field::new("delta", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(key, _, _)| *key).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|(_, payload, _)| *payload)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|(_, _, weight)| *weight)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )?],
    })
}

fn assert_outer_snapshot(
    runtime: &(dyn StandingProgramRuntime + Send),
    identity: &StandingProgramIdentity,
    epoch: u64,
    expected: &BTreeMap<i64, (Option<i64>, i64)>,
) -> BenchResult<()> {
    let page = runtime.materialized_view_page(
        ScopedViewId {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: OUTER_VIEW_ID.to_string(),
        },
        SnapshotPageRequest {
            committed_epoch: Some(epoch),
            page_token: None,
            max_rows: None,
        },
    )?;
    let mut actual = BTreeMap::new();
    for batch in &page.batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| std::io::Error::other("outer benchmark key is not int64"))?;
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| std::io::Error::other("outer benchmark sum is not int64"))?;
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| std::io::Error::other("outer benchmark count is not int64"))?;
        for row in 0..batch.num_rows() {
            actual.insert(
                keys.value(row),
                (
                    (!sums.is_null(row)).then(|| sums.value(row)),
                    counts.value(row),
                ),
            );
        }
    }
    if &actual != expected {
        return Err(std::io::Error::other(format!(
            "outer benchmark snapshot mismatch: expected {} groups, observed {}",
            expected.len(),
            actual.len()
        ))
        .into());
    }
    Ok(())
}

fn outer_catalog(relation_id: &str) -> BenchResult<VelorixRelationCatalogV1> {
    Ok(VelorixRelationCatalogV1::from_relation_schema(
        VelorixRelationSchemaV1 {
            relation_id: relation_id.to_string(),
            relation_name: relation_id.to_string(),
            relation_version: "2026-08-10.v1".to_string(),
            columns: vec![
                relation_column(
                    "join_key",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                    0,
                    RelationSemanticRoleV1::PrimaryKey,
                ),
                relation_column(
                    "payload",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                    1,
                    RelationSemanticRoleV1::Value,
                ),
                relation_column(
                    "delta",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                    2,
                    RelationSemanticRoleV1::Weight,
                ),
            ],
            primary_key_column_ids: vec!["join_key".to_string()],
            weight_column_id: "delta".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        },
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
    )?)
}

fn outer_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: OUTER_VIEW_ID.to_string(),
        relation_name: OUTER_VIEW_ID.to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        schema_fingerprint: stable_bytes_hash(b"outer-join-distribution-output-v1"),
        columns: vec![
            ColumnSchema {
                name: "bucket".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: true,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["bucket".to_string()],
    }
}

fn outer_identity(name: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "local-benchmark".to_string(),
        program_id: name.to_string(),
        view_ids: vec![OUTER_VIEW_ID.to_string()],
        sql_hash: stable_bytes_hash(OUTER_SQL.as_bytes()),
        input_catalog_hash: stable_bytes_hash(b"outer-join-scale-catalogs-v1"),
        output_schema_hash: stable_bytes_hash(b"outer-join-distribution-output-v1"),
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

fn catalog(relation_id: &str) -> BenchResult<VelorixRelationCatalogV1> {
    Ok(VelorixRelationCatalogV1::from_relation_schema(
        VelorixRelationSchemaV1 {
            relation_id: relation_id.to_string(),
            relation_name: relation_id.to_string(),
            relation_version: "2026-08-10.v1".to_string(),
            columns: vec![
                relation_column(
                    "row_id",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                    0,
                    RelationSemanticRoleV1::PrimaryKey,
                ),
                relation_column(
                    "join_key",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                    1,
                    RelationSemanticRoleV1::Metadata,
                ),
                relation_column(
                    "payload",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                    2,
                    RelationSemanticRoleV1::Value,
                ),
                relation_column(
                    "delta",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                    3,
                    RelationSemanticRoleV1::Weight,
                ),
            ],
            primary_key_column_ids: vec!["row_id".to_string()],
            weight_column_id: "delta".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        },
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
        schema_fingerprint: stable_bytes_hash(b"join-distribution-counts-output-v1"),
        columns: vec![
            ColumnSchema {
                name: "bucket".to_string(),
                data_type: SqlDataType::Int64,
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
        primary_key: vec!["bucket".to_string()],
    }
}

fn identity(workload: &JoinScaleWorkload) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "local-benchmark".to_string(),
        program_id: workload.id.clone(),
        view_ids: vec![VIEW_ID.to_string()],
        sql_hash: stable_bytes_hash(workload.sql.as_bytes()),
        input_catalog_hash: stable_bytes_hash(b"join-scale-catalogs-v1"),
        output_schema_hash: stable_bytes_hash(b"join-distribution-counts-output-v1"),
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

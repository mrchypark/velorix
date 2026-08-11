use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::PathBuf,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array},
    compute::cast,
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::{datasource::MemTable, prelude::SessionContext};
use serde::Deserialize;
use serde_json::{Map, Number, Value};
use velorix_core::{
    delta::DeltaBatch,
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
        RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochIdempotencyKey, NativeCodePolicy, RelationInputBatch,
        ScopedViewId, SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
    view_plan::{lower_supported_sql_to_logical_plan, VelorixLogicalViewPlanV1},
};
use velorix_runtime::{
    frontier_conformance::{
        CommittedFrontierEvidenceV1, FrontierConformanceVerifierV1, WeightedCanonicalRowV1,
    },
    incremental_sql_comparison::{
        ComparisonEngineV1, ComparisonPlanEvidenceV1, ComparisonProtocolV1, CorrectnessOutcomeV2,
        CorrectnessStatusV2, IncrementalSqlComparisonResultV2, NativeIdentityEvidenceV1,
    },
    materialized_view_runtime::{
        create_standing_runtime_with_logical_plan_and_catalogs, restore_standing_runtime,
        CRATE_NAME,
    },
};

const CORPUS: &str = include_str!("../benches/fixtures/incremental_sql_corpus_v1.json");

type DynError = Box<dyn Error + Send + Sync>;
type SourceState = BTreeMap<String, BTreeMap<String, Map<String, Value>>>;

#[derive(Deserialize)]
struct Corpus {
    workloads: Vec<Workload>,
    phases: Vec<Phase>,
}

#[derive(Deserialize)]
struct Workload {
    id: String,
    sql: Vec<String>,
    expected_final: Vec<Map<String, Value>>,
}

#[derive(Deserialize)]
struct Phase {
    name: String,
    events: Vec<Change>,
}

#[derive(Clone, Deserialize)]
struct Change {
    relation: String,
    op: String,
    before: Option<Map<String, Value>>,
    after: Option<Map<String, Value>>,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let (output, source_revision) = arguments()?;
    let corpus: Corpus = serde_json::from_str(CORPUS)?;
    let catalogs = corpus_catalogs()?;
    let mut correctness = Vec::with_capacity(corpus.workloads.len());

    for workload in &corpus.workloads {
        let outcome = run_workload(workload, &corpus.phases, &catalogs).await?;
        correctness.push(CorrectnessOutcomeV2 {
            workload_id: workload.id.clone(),
            outcome,
        });
    }

    let result = IncrementalSqlComparisonResultV2 {
        schema_version: 2,
        corpus_version: "incremental-sql-corpus-v1".to_string(),
        engine: ComparisonEngineV1 {
            name: "velorix".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            source_revision,
            configuration: BTreeMap::from([
                (
                    "runner".to_string(),
                    "velorix-runtime-example-v1".to_string(),
                ),
                ("workers".to_string(), "1".to_string()),
                ("batch_oracle".to_string(), "datafusion".to_string()),
            ]),
            durability_mode: "in_memory_runtime_checkpoint_restore".to_string(),
            input_semantics: "primary_key_updates_lowered_to_signed_rows".to_string(),
            state_retention_policy: "all_history_unless_plan_declares_watermark_bound".to_string(),
        },
        protocol: ComparisonProtocolV1 {
            warm_up_iterations: 0,
            measured_iterations: 1,
            initial_rows: 7,
            change_events: 4,
            change_mix: BTreeMap::from([
                ("insert".to_string(), 2),
                ("update".to_string(), 1),
                ("delete".to_string(), 1),
            ]),
        },
        correctness,
        performance: Vec::new(),
    };
    let workload_ids = corpus
        .workloads
        .iter()
        .map(|workload| workload.id.as_str())
        .collect::<Vec<_>>();
    result.validate_for_workloads(&workload_ids)?;
    if result
        .correctness
        .iter()
        .any(|outcome| matches!(outcome.outcome, CorrectnessStatusV2::Failed { .. }))
    {
        return Err("Velorix corpus baseline contains a correctness failure".into());
    }

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec_pretty(&result)?)?;
    println!("{}", output.display());
    Ok(())
}

async fn run_workload(
    workload: &Workload,
    phases: &[Phase],
    all_catalogs: &[VelorixRelationCatalogV1],
) -> Result<CorrectnessStatusV2, DynError> {
    if workload.sql.len() != 1 {
        return Ok(CorrectnessStatusV2::Unsupported {
            reason: "Velorix does not yet admit chained materialized-view statements into one runtime DAG"
                .to_string(),
        });
    }
    let sql = &workload.sql[0];
    let output_schema = output_schema(&workload.id)?;
    let admission_catalogs = corpus_input_catalogs(&workload.id, all_catalogs)?;
    let plan = match lower_supported_sql_to_logical_plan(sql, &admission_catalogs, &output_schema) {
        Ok(plan) => plan,
        Err(error) => {
            return Ok(CorrectnessStatusV2::Unsupported {
                reason: error.to_string(),
            });
        }
    };
    let plan_evidence = plan_evidence(&plan)?;
    let catalogs = plan
        .input_relations
        .iter()
        .map(|input| {
            all_catalogs
                .iter()
                .find(|catalog| catalog.relation_schema.relation_id == input.relation_id)
                .cloned()
                .ok_or_else(|| format!("missing catalog {}", input.relation_id))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let input_schemas = catalogs
        .iter()
        .map(catalog_input_relation_schema)
        .collect::<Result<Vec<_>, _>>()?;
    let identity = standing_identity(&workload.id, sql);
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &catalogs,
        plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )?;
    let input_ids = catalogs
        .iter()
        .map(|catalog| catalog.relation_schema.relation_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut source = SourceState::new();
    for catalog in all_catalogs {
        source.insert(catalog.relation_schema.relation_id.clone(), BTreeMap::new());
    }
    let mut offsets = input_ids
        .iter()
        .map(|relation| ((*relation).to_string(), 0_u64))
        .collect::<BTreeMap<_, _>>();
    let mut logical_epoch = 0_u64;
    let mut verifier = FrontierConformanceVerifierV1::default();
    let mut verified_phases = Vec::with_capacity(phases.len());

    for (phase_index, phase) in phases.iter().enumerate() {
        for change in &phase.events {
            apply_source_change(&mut source, change)?;
        }
        let relevant = phase
            .events
            .iter()
            .filter(|change| input_ids.contains(change.relation.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let observed_delta = if phase.name == "checkpoint_restart" {
            runtime = restore_standing_runtime(runtime.checkpoint()?)?;
            Vec::new()
        } else if relevant.is_empty() {
            Vec::new()
        } else {
            logical_epoch += 1;
            let batches = relation_input_batches(&catalogs, &relevant, &mut offsets)?;
            let commit = runtime.apply_changes(
                logical_epoch,
                EpochIdempotencyKey::new(format!("{}-{logical_epoch}", workload.id))?,
                batches,
            )?;
            if commit.output_deltas.len() != 1 {
                return Err(format!(
                    "{} epoch {} emitted {} output deltas",
                    workload.id,
                    logical_epoch,
                    commit.output_deltas.len()
                )
                .into());
            }
            canonical_delta(&output_schema, &commit.output_deltas[0].delta)?
        };
        let oracle_snapshot = oracle_snapshot(sql, &catalogs, &source).await?;
        let runtime_snapshot = runtime_snapshot(
            runtime.as_ref(),
            &identity,
            &workload.id,
            &output_schema,
            logical_epoch,
        )?;
        verifier.verify_committed_frontier(CommittedFrontierEvidenceV1 {
            frontier: phase_index as u64 + 1,
            oracle_snapshot,
            observed_delta,
            observed_snapshot: runtime_snapshot,
        })?;
        verified_phases.push(phase.name.clone());
    }

    let observed = runtime_snapshot(
        runtime.as_ref(),
        &identity,
        &workload.id,
        &output_schema,
        logical_epoch,
    )?;
    let expected = workload
        .expected_final
        .iter()
        .map(|row| WeightedCanonicalRowV1 {
            row: canonical_json(&Value::Object(row.clone())),
            weight: 1,
        })
        .collect::<Vec<_>>();
    let expected_digest = bag_digest(&expected)?;
    let observed_digest = bag_digest(&observed)?;
    if expected_digest != observed_digest {
        return Ok(CorrectnessStatusV2::Failed {
            reason: format!(
                "final corpus result digest mismatch: expected {expected_digest}, observed {observed_digest}"
            ),
        });
    }
    Ok(CorrectnessStatusV2::Passed {
        expected_digest,
        observed_digest,
        verified_phases,
        plan_evidence,
    })
}

fn corpus_input_catalogs(
    workload: &str,
    all_catalogs: &[VelorixRelationCatalogV1],
) -> Result<Vec<VelorixRelationCatalogV1>, DynError> {
    let relation_ids: &[&str] = match workload {
        "filter_project" | "aggregate" | "distinct_aggregate" | "top_k" | "fixed_window"
        | "ranking" => &["orders"],
        "inner_join" | "left_join" => &["customers", "orders"],
        other => return Err(format!("no single-runtime input binding for {other}").into()),
    };
    relation_ids
        .iter()
        .map(|relation_id| {
            all_catalogs
                .iter()
                .find(|catalog| catalog.relation_schema.relation_id == *relation_id)
                .cloned()
                .ok_or_else(|| format!("missing corpus catalog {relation_id}").into())
        })
        .collect()
}

fn relation_input_batches(
    catalogs: &[VelorixRelationCatalogV1],
    changes: &[Change],
    offsets: &mut BTreeMap<String, u64>,
) -> Result<Vec<RelationInputBatch>, DynError> {
    let mut batches = Vec::new();
    for catalog in catalogs {
        let relation_id = &catalog.relation_schema.relation_id;
        let relation_changes = changes
            .iter()
            .filter(|change| &change.relation == relation_id)
            .collect::<Vec<_>>();
        if relation_changes.is_empty() {
            continue;
        }
        let mut rows = Vec::new();
        for change in &relation_changes {
            match change.op.as_str() {
                "insert" => rows.push((change.after.as_ref().unwrap().clone(), 1)),
                "delete" => rows.push((change.before.as_ref().unwrap().clone(), -1)),
                "update" => {
                    rows.push((change.before.as_ref().unwrap().clone(), -1));
                    rows.push((change.after.as_ref().unwrap().clone(), 1));
                }
                other => return Err(format!("unsupported corpus operation {other}").into()),
            }
        }
        let start = *offsets.get(relation_id).unwrap_or(&0);
        let end = start + relation_changes.len() as u64;
        offsets.insert(relation_id.clone(), end);
        batches.push(RelationInputBatch {
            relation_id: relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            stream_id: format!("{relation_id}-corpus-stream"),
            partition_id: 0,
            schema_fingerprint: catalog.schema_fingerprint.to_string(),
            start_offset_inclusive: start,
            end_offset_exclusive: end,
            event_time_watermark: None,
            batches: vec![catalog_batch(catalog, &rows, true)?],
        });
    }
    Ok(batches)
}

async fn oracle_snapshot(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    source: &SourceState,
) -> Result<Vec<WeightedCanonicalRowV1>, DynError> {
    let context = SessionContext::new();
    for catalog in catalogs {
        let rows = source
            .get(&catalog.relation_schema.relation_id)
            .unwrap()
            .values()
            .cloned()
            .map(|row| (row, 1))
            .collect::<Vec<_>>();
        let batch = catalog_batch(catalog, &rows, false)?;
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        context.register_table(&catalog.datafusion_registration.name, Arc::new(table))?;
    }
    let batches = context.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        rows.extend(canonical_record_batch(batch)?);
    }
    Ok(rows
        .into_iter()
        .map(|row| WeightedCanonicalRowV1 { row, weight: 1 })
        .collect())
}

fn runtime_snapshot(
    runtime: &(dyn StandingProgramRuntime + Send),
    identity: &StandingProgramIdentity,
    view_id: &str,
    _output_schema: &RelationSchema,
    logical_epoch: u64,
) -> Result<Vec<WeightedCanonicalRowV1>, DynError> {
    let page = runtime.materialized_view_page(
        ScopedViewId {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: view_id.to_string(),
        },
        SnapshotPageRequest {
            committed_epoch: Some(logical_epoch),
            page_token: None,
            max_rows: None,
        },
    )?;
    let mut rows = Vec::new();
    for batch in &page.batches {
        rows.extend(canonical_record_batch(batch)?);
    }
    Ok(rows
        .into_iter()
        .map(|row| WeightedCanonicalRowV1 { row, weight: 1 })
        .collect())
}

fn canonical_delta(
    output_schema: &RelationSchema,
    delta: &DeltaBatch,
) -> Result<Vec<WeightedCanonicalRowV1>, DynError> {
    delta
        .net_rows()?
        .into_iter()
        .map(|record| {
            let mut row = Map::new();
            if output_schema.primary_key.len() == 1 {
                row.insert(
                    output_schema.primary_key[0].clone(),
                    record.key.as_json().clone(),
                );
            } else {
                let keys = record
                    .key
                    .as_json()
                    .as_object()
                    .ok_or("composite output key is not an object")?;
                for key in &output_schema.primary_key {
                    row.insert(
                        key.clone(),
                        keys.get(key)
                            .cloned()
                            .ok_or("composite output key column is missing")?,
                    );
                }
            }
            if let Some(values) = record.value.as_json().as_object() {
                for (name, value) in values {
                    row.insert(name.clone(), value.clone());
                }
            }
            for column in &output_schema.columns {
                if !row.contains_key(&column.name) {
                    return Err(format!("output delta is missing column {}", column.name).into());
                }
            }
            Ok(WeightedCanonicalRowV1 {
                row: canonical_json(&Value::Object(row)),
                weight: record.weight,
            })
        })
        .collect()
}

fn canonical_record_batch(batch: &RecordBatch) -> Result<Vec<String>, DynError> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let mut row = Map::new();
        for (column_index, field) in batch.schema().fields().iter().enumerate() {
            row.insert(
                field.name().clone(),
                arrow_value(batch.column(column_index), row_index)?,
            );
        }
        rows.push(canonical_json(&Value::Object(row)));
    }
    Ok(rows)
}

fn arrow_value(array: &ArrayRef, row: usize) -> Result<Value, DynError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Int64 => Ok(Value::Number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
                .into(),
        )),
        DataType::UInt64 => Ok(Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row)
                .into(),
        )),
        DataType::Float64 => Number::from_f64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        )
        .map(Value::Number)
        .ok_or_else(|| "non-finite float output".into()),
        other => Err(format!("baseline runner does not encode Arrow output type {other}").into()),
    }
}

fn catalog_batch(
    catalog: &VelorixRelationCatalogV1,
    rows: &[(Map<String, Value>, i64)],
    include_weight: bool,
) -> Result<RecordBatch, DynError> {
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for column in &catalog.relation_schema.columns {
        if column.semantic_role == RelationSemanticRoleV1::Weight && !include_weight {
            continue;
        }
        let (field, array): (Field, ArrayRef) = match &column.physical_arrow_type {
            ArrowPhysicalTypeV1::Utf8 => (
                Field::new(&column.name, DataType::Utf8, column.nullable),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(row, _)| row[&column.name].as_str())
                        .collect::<Vec<_>>(),
                )),
            ),
            ArrowPhysicalTypeV1::Int64 => {
                let values = if column.semantic_role == RelationSemanticRoleV1::Weight {
                    rows.iter().map(|(_, weight)| *weight).collect::<Vec<_>>()
                } else {
                    rows.iter()
                        .map(|(row, _)| row[&column.name].as_i64().unwrap())
                        .collect::<Vec<_>>()
                };
                (
                    Field::new(&column.name, DataType::Int64, column.nullable),
                    Arc::new(Int64Array::from(values)),
                )
            }
            ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => {
                let strings = StringArray::from(
                    rows.iter()
                        .map(|(row, _)| row[&column.name].as_str())
                        .collect::<Vec<_>>(),
                );
                let data_type =
                    DataType::Timestamp(TimeUnit::Nanosecond, timezone.clone().map(Into::into));
                (
                    Field::new(&column.name, data_type.clone(), column.nullable),
                    cast(&strings, &data_type)?,
                )
            }
            other => return Err(format!("unsupported corpus Arrow type {other:?}").into()),
        };
        fields.push(field);
        arrays.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn apply_source_change(source: &mut SourceState, change: &Change) -> Result<(), DynError> {
    let relation = source
        .get_mut(&change.relation)
        .ok_or_else(|| format!("unknown relation {}", change.relation))?;
    match change.op.as_str() {
        "insert" => {
            let row = change.after.as_ref().unwrap();
            relation.insert(primary_key(&change.relation, row)?, row.clone());
        }
        "update" => {
            let before = change.before.as_ref().unwrap();
            let after = change.after.as_ref().unwrap();
            relation.remove(&primary_key(&change.relation, before)?);
            relation.insert(primary_key(&change.relation, after)?, after.clone());
        }
        "delete" => {
            let before = change.before.as_ref().unwrap();
            relation.remove(&primary_key(&change.relation, before)?);
        }
        other => return Err(format!("unsupported corpus operation {other}").into()),
    }
    Ok(())
}

fn primary_key(relation: &str, row: &Map<String, Value>) -> Result<String, DynError> {
    let column = match relation {
        "orders" => "order_id",
        "customers" => "customer_id",
        "products" => "product_id",
        other => return Err(format!("unknown corpus relation {other}").into()),
    };
    Ok(canonical_json(&row[column]))
}

fn plan_evidence(plan: &VelorixLogicalViewPlanV1) -> Result<ComparisonPlanEvidenceV1, DynError> {
    let implementation = plan
        .execution_implementation
        .as_ref()
        .ok_or("admitted plan has no execution implementation")?;
    Ok(ComparisonPlanEvidenceV1 {
        native_logical_plan: NativeIdentityEvidenceV1::Available {
            identity: plan
                .plan_hash
                .clone()
                .ok_or("admitted plan has no plan hash")?,
        },
        native_physical_dag: NativeIdentityEvidenceV1::Available {
            identity: implementation.physical_operator_dag_hash.clone(),
        },
        diagnostic_explain_digest: None,
    })
}

fn bag_digest(rows: &[WeightedCanonicalRowV1]) -> Result<String, DynError> {
    let mut bag = BTreeMap::<String, i64>::new();
    for row in rows {
        *bag.entry(row.row.clone()).or_default() += row.weight;
    }
    bag.retain(|_, weight| *weight != 0);
    Ok(stable_bytes_hash(&serde_json::to_vec(&bag)?))
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).unwrap()
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(values) => {
            let mut fields = values
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        canonical_json(value)
                    )
                })
                .collect::<Vec<_>>();
            fields.sort();
            format!("{{{}}}", fields.join(","))
        }
    }
}

fn corpus_catalogs() -> Result<Vec<VelorixRelationCatalogV1>, DynError> {
    Ok(vec![
        relation_catalog(
            "orders",
            "order_id",
            vec![
                (
                    "order_id",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                ),
                (
                    "customer_id",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                ),
                (
                    "product_id",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                ),
                (
                    "amount",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                ),
                (
                    "event_time",
                    VelorixLogicalTypeV1::Timestamp { timezone: None },
                    ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
                ),
            ],
            Some("event_time"),
        )?,
        relation_catalog(
            "customers",
            "customer_id",
            vec![
                (
                    "customer_id",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                ),
                (
                    "region",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                ),
            ],
            None,
        )?,
        relation_catalog(
            "products",
            "product_id",
            vec![
                (
                    "product_id",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                ),
                (
                    "category",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                ),
            ],
            None,
        )?,
    ])
}

fn relation_catalog(
    relation_id: &str,
    primary_key: &str,
    columns: Vec<(&str, VelorixLogicalTypeV1, ArrowPhysicalTypeV1)>,
    event_time: Option<&str>,
) -> Result<VelorixRelationCatalogV1, DynError> {
    let mut relation_columns = columns
        .into_iter()
        .enumerate()
        .map(
            |(ordinal, (name, logical_type, physical_arrow_type))| RelationColumnV1 {
                column_id: name.to_string(),
                name: name.to_string(),
                logical_type,
                physical_arrow_type,
                nullable: false,
                ordinal: ordinal as u32,
                semantic_role: if name == primary_key {
                    RelationSemanticRoleV1::PrimaryKey
                } else if Some(name) == event_time {
                    RelationSemanticRoleV1::EventTime
                } else {
                    RelationSemanticRoleV1::Value
                },
            },
        )
        .collect::<Vec<_>>();
    relation_columns.push(RelationColumnV1 {
        column_id: "delta".to_string(),
        name: "delta".to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: false,
        ordinal: relation_columns.len() as u32,
        semantic_role: RelationSemanticRoleV1::Weight,
    });
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: relation_id.to_string(),
        relation_name: relation_id.to_string(),
        relation_version: "corpus-v1".to_string(),
        columns: relation_columns,
        primary_key_column_ids: vec![primary_key.to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: event_time.map(str::to_string),
    };
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)?;
    Ok(VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: relation_id.to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: relation_id.to_string(),
            schema_fingerprint: fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    })
}

fn output_schema(workload: &str) -> Result<RelationSchema, DynError> {
    let (columns, primary_key) = match workload {
        "filter_project" => (
            vec![
                ("order_id", SqlDataType::Utf8),
                ("doubled", SqlDataType::Int64),
            ],
            vec!["order_id"],
        ),
        "aggregate" => (
            vec![
                ("customer_id", SqlDataType::Utf8),
                ("total", SqlDataType::Int64),
                ("order_count", SqlDataType::Int64),
                ("minimum", SqlDataType::Int64),
                ("maximum", SqlDataType::Int64),
                ("average", SqlDataType::Float64),
            ],
            vec!["customer_id"],
        ),
        "distinct_aggregate" => (
            vec![
                ("customer_id", SqlDataType::Utf8),
                ("product_count", SqlDataType::Int64),
            ],
            vec!["customer_id"],
        ),
        "inner_join" => (
            vec![("region", SqlDataType::Utf8), ("total", SqlDataType::Int64)],
            vec!["region"],
        ),
        "left_join" => (
            vec![
                ("customer_id", SqlDataType::Utf8),
                ("order_count", SqlDataType::Int64),
            ],
            vec!["customer_id"],
        ),
        "top_k" => (
            vec![
                ("customer_id", SqlDataType::Utf8),
                ("total", SqlDataType::Int64),
            ],
            vec!["customer_id"],
        ),
        "fixed_window" => (
            vec![
                ("customer_id", SqlDataType::Utf8),
                ("window_start", SqlDataType::Int64),
                ("total", SqlDataType::Int64),
            ],
            vec!["customer_id", "window_start"],
        ),
        "ranking" => (
            vec![
                ("customer_id", SqlDataType::Utf8),
                ("order_id", SqlDataType::Utf8),
                ("rank", SqlDataType::Int64),
            ],
            vec!["order_id"],
        ),
        "chained_view" => return Err("chained view has multiple output schemas".into()),
        other => return Err(format!("unknown workload {other}").into()),
    };
    Ok(RelationSchema {
        relation_id: format!("corpus_{workload}"),
        relation_name: format!("corpus_{workload}"),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("corpus-{workload}-v1"),
        columns: columns
            .into_iter()
            .map(|(name, data_type)| ColumnSchema {
                name: name.to_string(),
                data_type,
                nullable: false,
            })
            .collect(),
        primary_key: primary_key.into_iter().map(str::to_string).collect(),
    })
}

fn standing_identity(workload: &str, sql: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "corpus".to_string(),
        program_id: "incremental-sql-baseline".to_string(),
        view_ids: vec![workload.to_string()],
        sql_hash: stable_bytes_hash(sql.as_bytes()),
        input_catalog_hash: format!("sha256:{}", "1".repeat(64)),
        output_schema_hash: format!("sha256:{}", "2".repeat(64)),
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

fn arguments() -> Result<(PathBuf, String), DynError> {
    let mut output = None;
    let mut source_revision = None;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--output" => output = args.next().map(PathBuf::from),
            "--source-revision" => source_revision = args.next(),
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    Ok((
        output.ok_or("--output is required")?,
        source_revision.ok_or("--source-revision is required")?,
    ))
}

//! Pure output-dependency projection helpers for scoped materialization.
//!
//! A `ScopeDependencies` value is derived from an admitted active view's
//! logical plan and answers, for each pending published-input record, which
//! **output** columns of the view can be certified as known values, known
//! nulls, or unknown. Certification is conservative: a column is only present
//! in a [`DependencyRow`] when this module can prove its value; anything
//! absent means unknown and therefore MAY_MATCH under
//! [`predicate_may_match`]. Aggregate outputs are never certified.
//!
//! This is dependency evidence, not a coverage certificate. Unsupported plans
//! return `Ok(None)` so the caller computes broader dependencies. Only a single
//! input is eligible. Physical sources require a verified catalog mapping and
//! support row-local/group-key and fixed tumbling projections; mixed inputs
//! remain deferred. Tumbling projection does not prove watermark readiness or
//! authorize advancing source offsets across omitted rows.
//!
//! Row-local input filters declared in the plan are intentionally NOT applied;
//! every input record is projected, which only widens the MAY_MATCH set.

use super::*;
#[cfg(test)]
use velorix_core::delta::DeltaRecord;
use velorix_core::query::{materialized_page_predicate, PageComparison, PagePredicate, PageScalar};
use velorix_core::view_plan::{
    supported_view_plan_group_keys, SupportedEventTimeWindowKind, SupportedProjectionBinaryOp,
    SupportedTypedMapPlanV1, TypedExprProgramV1,
};
use velorix_runtime::compiled_expression::CompiledTypedExpr;
use velorix_runtime::materialized_view_runtime::expression_eval::RuntimeScalarValue;

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// One dependency row per input record ordinal.
///
/// * missing key           => unknown (MAY_MATCH)
/// * stored `None`         => known NULL
/// * stored `Some(scalar)` => known value
pub(super) type DependencyRow = BTreeMap<String, Option<PageScalar>>;

#[derive(Clone, Debug)]
pub(super) enum ScopeKeySource {
    Column { input_column_id: String },
    Int64Expression(SupportedProjectionExpr),
}

#[derive(Clone, Debug)]
pub(super) struct ScopeKeyProjection {
    pub(super) output_name: String,
    pub(super) source: ScopeKeySource,
}

#[derive(Clone, Debug)]
pub(super) enum ScopeRowColumn {
    Column {
        output_name: String,
        input_column_id: String,
    },
    Int64Expression {
        output_name: String,
        expr: SupportedProjectionExpr,
    },
    TypedExpression {
        output_name: String,
        program: TypedExprProgramV1,
    },
}

#[derive(Clone, Debug)]
pub(super) enum ScopeDependenciesPlan {
    SingleKeyGroup {
        input_relation_id: String,
        keys: Vec<ScopeKeyProjection>,
    },
    TumblingWindow {
        input_relation_id: String,
        key: ScopeKeyProjection,
        event_time_column_id: String,
        window_size_ns: i64,
        window_start_output: String,
        window_end_output: String,
    },
    RowLocalProject {
        input_relation_id: String,
        key: ScopeKeyProjection,
        value_columns: Vec<ScopeRowColumn>,
    },
    /// Single-branch typed bag projection: every output column is a typed
    /// expression over the one published input; the optional row predicate is
    /// not applied (all rows projected, conservative MAY_MATCH).
    TypedBag {
        input_relation_id: String,
        value_columns: Vec<ScopeRowColumn>,
    },
}

impl ScopeDependenciesPlan {
    fn input_relation_id(&self) -> &str {
        match self {
            Self::SingleKeyGroup {
                input_relation_id, ..
            }
            | Self::TumblingWindow {
                input_relation_id, ..
            }
            | Self::RowLocalProject {
                input_relation_id, ..
            }
            | Self::TypedBag {
                input_relation_id, ..
            } => input_relation_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ScopeDependencies {
    plan: ScopeDependenciesPlan,
    output_schema: RelationSchema,
    #[cfg(test)]
    input_binding: Option<PublishedRelationBindingV1>,
    source_schema: Option<RelationSchema>,
    source_catalog: Option<VelorixRelationCatalogV1>,
    eligible_columns: Vec<String>,
}

impl ScopeDependencies {
    /// Builds dependency projection state for an active view, or `Ok(None)`
    /// when the view is not an eligible single-input consumer of a
    /// supported plan shape (fail closed by absence, never by guessing).
    pub(super) fn for_active(
        active: &ActiveMaterializedView,
    ) -> Result<Option<ScopeDependencies>, ApiError> {
        let Some(runtime) = active.runtime.as_ref() else {
            return Ok(None);
        };
        let [input] = runtime.input_bindings.as_slice() else {
            return Ok(None);
        };
        let (input_schema, source_schema) = match input {
            StandingInputBindingV1::Source { relation, .. } => (relation, Some(relation.clone())),
            StandingInputBindingV1::PublishedView {
                published_relation, ..
            } => (&published_relation.relation, None),
        };
        let [output_schema] = active.spec.output_relations.as_slice() else {
            return Ok(None);
        };
        let Some(logical_plan) = runtime.logical_plan.as_ref() else {
            return Ok(None);
        };
        let plan = match &logical_plan.execution {
            VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } => {
                single_key_group_plan(plan)?
            }
            VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan } => {
                tumbling_window_plan(plan)?
            }
            VelorixLogicalViewExecutionV1::FilterProject { plan } => row_local_plan(plan)?,
            VelorixLogicalViewExecutionV1::TypedMap { plan } => typed_map_plan(plan)?,
            _ => return Ok(None),
        };
        let Some(plan) = plan else {
            return Ok(None);
        };
        if plan.input_relation_id() != input_schema.relation_id {
            return Ok(None);
        }
        if let VelorixLogicalViewExecutionV1::TypedMap { plan } = &logical_plan.execution {
            if plan.input_schema != *input_schema {
                return Ok(None);
            }
        }
        let eligible_columns = eligible_columns_for(&plan);
        Ok(Some(ScopeDependencies {
            plan,
            output_schema: output_schema.clone(),
            #[cfg(test)]
            input_binding: match input {
                StandingInputBindingV1::PublishedView {
                    published_relation, ..
                } => Some(published_relation.clone()),
                StandingInputBindingV1::Source { .. } => None,
            },
            source_schema,
            source_catalog: None,
            eligible_columns,
        }))
    }

    /// Output columns this projection can ever certify. Aggregate outputs are
    /// never included: their values are unknown, not false.
    pub(super) fn eligible_known_columns(&self) -> &[String] {
        &self.eligible_columns
    }

    /// Projects one dependency row per input record ordinal. The published
    /// relation must be the consumer's bound input; records are converted
    /// through the existing published-delta Arrow path per record so row
    /// ordinals are preserved exactly and codec-wrapped values are never
    /// assumed to be raw JSON objects.
    // Published demand currently computes full closure. Keep this adapter only
    // for projection/ordinal tests until published narrowing is wired deliberately.
    #[cfg(test)]
    pub(super) fn dependency_rows(
        &self,
        published_relation: &PublishedRelationBindingV1,
        records: &[DeltaRecord],
    ) -> Result<Vec<DependencyRow>, ApiError> {
        let input_relation_id = self.plan.input_relation_id();
        if self.input_binding.as_ref() != Some(published_relation) {
            return Err(ApiError::bad_request(format!(
                "scope dependency input relation `{}` does not match the admitted plan input `{}`",
                published_relation.relation.relation_id, input_relation_id
            )));
        }
        // A published commit whose net output delta is empty (for example a
        // producer filter that drops every source row) has zero ordinals to
        // project. Converting it through the canonical zero-row Arrow batch
        // would push one fabricated empty row, breaking the rows==records
        // length invariant that every scoped reader fails closed on.
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let compiled_typed = self.compiled_typed_columns()?;
        let delta = DeltaBatch::from_records(records.iter().cloned());
        let net = delta
            .net_rows()
            .map_err(|_| ApiError::bad_request("published output delta is malformed"))?;
        // Canonical unique input converts through ONE Arrow batch (row
        // ordinals align with input ordinals); only non-normalized records
        // (canceling duplicates, zero weights) fall back per record.
        let canonical = net.len() == records.len() && net.iter().eq(records.iter());
        let batches: Vec<RecordBatch> = if canonical {
            let batch = published_delta_to_record_batch(published_relation, &delta)?;
            if batch.num_rows() != records.len() {
                return Err(ApiError::bad_request(
                    "published delta batch row count does not match the input records",
                ));
            }
            vec![batch]
        } else {
            let mut batches = Vec::with_capacity(records.len());
            for record in records {
                batches.push(published_delta_to_record_batch(
                    published_relation,
                    &DeltaBatch::from_records(std::iter::once(record.clone())),
                )?);
            }
            batches
        };
        let mut rows = Vec::with_capacity(records.len());
        for batch in &batches {
            if batch.num_rows() == 0 {
                // A zero-weight record nets to no published row; its output
                // influence is unknown, never fabricated.
                rows.push(DependencyRow::new());
                continue;
            }
            for row_index in 0..batch.num_rows() {
                let row = self.project_row(batch, row_index, &compiled_typed);
                rows.push(row);
            }
        }
        if rows.len() != records.len() {
            return Err(ApiError::internal("dependency row ordinal count changed"));
        }
        Ok(rows)
    }

    fn project_row(
        &self,
        batch: &RecordBatch,
        row_index: usize,
        compiled_typed: &[Option<CompiledTypedExpr<'_>>],
    ) -> DependencyRow {
        match &self.plan {
            ScopeDependenciesPlan::SingleKeyGroup { keys, .. } => {
                project_keys_row(batch, row_index, keys, &self.output_schema)
            }
            ScopeDependenciesPlan::TumblingWindow {
                key,
                event_time_column_id,
                window_size_ns,
                window_start_output,
                window_end_output,
                ..
            } => {
                let mut row = project_keys_row(
                    batch,
                    row_index,
                    std::slice::from_ref(key),
                    &self.output_schema,
                );
                if let Some(start_ns) = event_time_ns(batch, row_index, event_time_column_id)
                    .and_then(|event| fixed_window_start(event, *window_size_ns))
                {
                    let column_type = |name: &str| {
                        self.output_schema
                            .columns
                            .iter()
                            .find(|column| column.name == name)
                            .map(|column| &column.data_type)
                    };
                    if let Some(scalar) =
                        output_scalar_from_integer(start_ns, column_type(window_start_output))
                    {
                        row.insert(window_start_output.clone(), Some(scalar));
                    }
                    if let Some(end_ns) = start_ns.checked_add(*window_size_ns) {
                        if let Some(scalar) =
                            output_scalar_from_integer(end_ns, column_type(window_end_output))
                        {
                            row.insert(window_end_output.clone(), Some(scalar));
                        }
                    }
                }
                row
            }
            ScopeDependenciesPlan::RowLocalProject {
                key, value_columns, ..
            } => {
                let mut row = project_keys_row(
                    batch,
                    row_index,
                    std::slice::from_ref(key),
                    &self.output_schema,
                );
                for (index, column) in value_columns.iter().enumerate() {
                    match column {
                        ScopeRowColumn::Column {
                            output_name,
                            input_column_id,
                        } => {
                            insert_batch_cell(
                                &mut row,
                                batch,
                                row_index,
                                input_column_id,
                                output_name,
                            );
                        }
                        ScopeRowColumn::Int64Expression { output_name, expr } => {
                            if let Some(cell) = eval_int64_projection(expr, batch, row_index) {
                                let data_type = self
                                    .output_schema
                                    .columns
                                    .iter()
                                    .find(|column| &column.name == output_name)
                                    .map(|column| &column.data_type);
                                match cell {
                                    None => {
                                        row.insert(output_name.clone(), None);
                                    }
                                    Some(value) => {
                                        if let Some(scalar) =
                                            output_scalar_from_integer(value, data_type)
                                        {
                                            row.insert(output_name.clone(), Some(scalar));
                                        }
                                    }
                                }
                            }
                        }
                        ScopeRowColumn::TypedExpression { output_name, .. } => {
                            let Some(Some(compiled)) = compiled_typed.get(index) else {
                                continue;
                            };
                            let get_column = |column_id: &str| {
                                batch.schema().index_of(column_id).ok().and_then(|index| {
                                    runtime_scalar_from_array(batch.column(index), row_index)
                                })
                            };
                            if let Ok(value) = compiled.evaluate(&get_column) {
                                if let Some(cell) = page_scalar_from_runtime(value) {
                                    row.insert(output_name.clone(), cell);
                                }
                            }
                        }
                    }
                }
                row
            }
            ScopeDependenciesPlan::TypedBag { value_columns, .. } => {
                let mut row = DependencyRow::new();
                for (index, column) in value_columns.iter().enumerate() {
                    let ScopeRowColumn::TypedExpression { output_name, .. } = column else {
                        continue;
                    };
                    let Some(Some(compiled)) = compiled_typed.get(index) else {
                        continue;
                    };
                    let get_column = |column_id: &str| {
                        batch.schema().index_of(column_id).ok().and_then(|index| {
                            runtime_scalar_from_array(batch.column(index), row_index)
                        })
                    };
                    if let Ok(value) = compiled.evaluate(&get_column) {
                        if let Some(cell) = page_scalar_from_runtime(value) {
                            row.insert(output_name.clone(), cell);
                        }
                    }
                }
                row
            }
        }
    }

    /// Bind the captured catalog before source narrowing. The source batch reader
    /// must separately authenticate generation, version, fingerprint and offsets.
    pub(super) fn bind_source_catalog(
        &mut self,
        catalog: &VelorixRelationCatalogV1,
    ) -> Result<(), ApiError> {
        let schema = velorix_core::view_contract::catalog_input_relation_schema(catalog)
            .map_err(ApiError::bad_request)?;
        if self.source_schema.as_ref() != Some(&schema) {
            return Err(ApiError::bad_request(
                "scope source catalog does not match the captured schema",
            ));
        }
        catalog
            .validate_ingest_adapter_scope()
            .map_err(ApiError::bad_request)?;
        self.source_catalog = Some(catalog.clone());
        Ok(())
    }

    /// One dependency row per Arrow row, in batch/row order. No consolidation,
    /// weight expansion or predicate filtering is allowed on this path.
    /// Without a catalog ID-to-physical-name mapping every row remains unknown.
    pub(super) fn source_dependency_rows(
        &self,
        relation_id: &str,
        batches: &[RecordBatch],
    ) -> Result<Vec<DependencyRow>, ApiError> {
        if self
            .source_schema
            .as_ref()
            .map(|schema| schema.relation_id.as_str())
            != Some(relation_id)
        {
            return Err(ApiError::bad_request(
                "scope source relation does not match the captured input",
            ));
        }
        let compiled = self.compiled_typed_columns()?;
        let mut rows = Vec::new();
        for batch in batches {
            let Some(catalog) = &self.source_catalog else {
                rows.extend((0..batch.num_rows()).map(|_| DependencyRow::new()));
                continue;
            };
            velorix_core::relation::validate_record_batch_matches_catalog(catalog, batch)
                .map_err(ApiError::bad_request)?;
            // Runtime decoding addresses physical fields by name, then exposes
            // values under catalog column IDs. Reproduce that mapping without
            // netting deltas, so canceled and zero-weight rows keep their ordinals.
            let mut fields = Vec::new();
            let mut arrays = Vec::new();
            for column in &catalog.relation_schema.columns {
                let index = batch
                    .schema()
                    .index_of(&column.name)
                    .map_err(ApiError::bad_request)?;
                fields.push(
                    batch
                        .schema()
                        .field(index)
                        .clone()
                        .with_name(&column.column_id),
                );
                arrays.push(batch.column(index).clone());
            }
            let normalized = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                .map_err(ApiError::bad_request)?;
            for index in 0..normalized.num_rows() {
                rows.push(self.project_row(&normalized, index, &compiled));
            }
        }
        Ok(rows)
    }

    fn compiled_typed_columns(&self) -> Result<Vec<Option<CompiledTypedExpr<'_>>>, ApiError> {
        let value_columns = match &self.plan {
            ScopeDependenciesPlan::RowLocalProject { value_columns, .. }
            | ScopeDependenciesPlan::TypedBag { value_columns, .. } => value_columns,
            _ => return Ok(Vec::new()),
        };
        value_columns
            .iter()
            .map(|column| match column {
                ScopeRowColumn::TypedExpression { program, .. } => {
                    CompiledTypedExpr::compile(program)
                        .map(Some)
                        .map_err(|error| {
                            ApiError::internal(format!(
                                "admitted typed dependency projection failed to compile: {error}"
                            ))
                        })
                }
                _ => Ok(None),
            })
            .collect()
    }
}

fn single_key_group_plan(
    plan: &SupportedViewPlan,
) -> Result<Option<ScopeDependenciesPlan>, ApiError> {
    if plan.top_k.is_some() {
        return Ok(None);
    }
    let keys = supported_view_plan_group_keys(plan);
    if keys.is_empty() {
        // Global (singleton) aggregates have no key columns to certify.
        return Ok(None);
    }
    let mut projections = Vec::with_capacity(keys.len());
    for key in keys {
        projections.push(match (&key.expression, &key.input_column_id) {
            (Some(expression), None) => ScopeKeyProjection {
                output_name: key.output_column_id.clone(),
                source: ScopeKeySource::Int64Expression(expression.clone()),
            },
            (None, Some(column_id)) => ScopeKeyProjection {
                output_name: key.output_column_id.clone(),
                source: ScopeKeySource::Column {
                    input_column_id: column_id.clone(),
                },
            },
            _ => return Ok(None),
        });
    }
    Ok(Some(ScopeDependenciesPlan::SingleKeyGroup {
        input_relation_id: plan.input_relation_id.clone(),
        keys: projections,
    }))
}

fn tumbling_window_plan(
    plan: &SupportedTumblingWindowPlan,
) -> Result<Option<ScopeDependenciesPlan>, ApiError> {
    if plan.top_k.is_some()
        || plan.hop_slide_ns.is_some()
        || plan.session_gap_ns.is_some()
        || plan.window_kind != SupportedEventTimeWindowKind::Tumbling
        || plan.window_size_ns <= 0
    {
        return Ok(None);
    }
    let key_output = if plan.output_key_column_id.is_empty() {
        plan.group_key_column_id.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    Ok(Some(ScopeDependenciesPlan::TumblingWindow {
        input_relation_id: plan.input_relation_id.clone(),
        key: ScopeKeyProjection {
            output_name: key_output,
            source: ScopeKeySource::Column {
                input_column_id: plan.group_key_column_id.clone(),
            },
        },
        event_time_column_id: plan.event_time_column_id.clone(),
        window_size_ns: plan.window_size_ns,
        window_start_output: plan.window_start_output_column_id.clone(),
        window_end_output: plan.window_end_output_column_id.clone(),
    }))
}

fn row_local_plan(
    plan: &SupportedFilterProjectPlan,
) -> Result<Option<ScopeDependenciesPlan>, ApiError> {
    if plan.top_k.is_some() {
        return Ok(None);
    }
    let key_input = plan
        .output_key_input_column_id
        .clone()
        .unwrap_or_else(|| plan.key_column_id.clone());
    let key_output = if plan.output_key_column_id.is_empty() {
        key_input.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    let mut value_columns =
        Vec::with_capacity(plan.value_columns.len() + plan.typed_value_columns.len());
    for column in &plan.value_columns {
        value_columns.push(match &column.expression {
            Some(expression) => ScopeRowColumn::Int64Expression {
                output_name: column.output_column_id.clone(),
                expr: expression.clone(),
            },
            None => ScopeRowColumn::Column {
                output_name: column.output_column_id.clone(),
                input_column_id: column.input_column_id.clone(),
            },
        });
    }
    for column in &plan.typed_value_columns {
        value_columns.push(ScopeRowColumn::TypedExpression {
            output_name: column.output_column_id.clone(),
            program: column.program.clone(),
        });
    }
    Ok(Some(ScopeDependenciesPlan::RowLocalProject {
        input_relation_id: plan.input_relation_id.clone(),
        key: ScopeKeyProjection {
            output_name: key_output,
            source: ScopeKeySource::Column {
                input_column_id: key_input,
            },
        },
        value_columns,
    }))
}

/// Row-local typed maps need no semantic row key. Series, unions, joins and
/// ordered windows require additional dependency closure and remain ineligible.
fn typed_map_plan(
    plan: &SupportedTypedMapPlanV1,
) -> Result<Option<ScopeDependenciesPlan>, ApiError> {
    if plan.projection.len() != plan.output_columns.len() {
        return Ok(None);
    }
    let value_columns = plan
        .projection
        .iter()
        .zip(&plan.output_columns)
        .map(|(program, column)| ScopeRowColumn::TypedExpression {
            output_name: column.name.clone(),
            program: program.clone(),
        })
        .collect();
    Ok(Some(ScopeDependenciesPlan::TypedBag {
        input_relation_id: plan.input_schema.relation_id.clone(),
        value_columns,
    }))
}

fn eligible_columns_for(plan: &ScopeDependenciesPlan) -> Vec<String> {
    match plan {
        ScopeDependenciesPlan::SingleKeyGroup { keys, .. } => {
            keys.iter().map(|key| key.output_name.clone()).collect()
        }
        ScopeDependenciesPlan::TumblingWindow {
            key,
            window_start_output,
            window_end_output,
            ..
        } => vec![
            key.output_name.clone(),
            window_start_output.clone(),
            window_end_output.clone(),
        ],
        ScopeDependenciesPlan::RowLocalProject {
            key, value_columns, ..
        } => {
            let mut columns = Vec::with_capacity(value_columns.len() + 1);
            columns.push(key.output_name.clone());
            for column in value_columns {
                columns.push(match column {
                    ScopeRowColumn::Column { output_name, .. }
                    | ScopeRowColumn::Int64Expression { output_name, .. }
                    | ScopeRowColumn::TypedExpression { output_name, .. } => output_name.clone(),
                });
            }
            columns
        }
        ScopeDependenciesPlan::TypedBag { value_columns, .. } => value_columns
            .iter()
            .map(|column| match column {
                ScopeRowColumn::Column { output_name, .. }
                | ScopeRowColumn::Int64Expression { output_name, .. }
                | ScopeRowColumn::TypedExpression { output_name, .. } => output_name.clone(),
            })
            .collect(),
    }
}

fn project_keys_row(
    batch: &RecordBatch,
    row_index: usize,
    keys: &[ScopeKeyProjection],
    output_schema: &RelationSchema,
) -> DependencyRow {
    let mut row = DependencyRow::new();
    for key in keys {
        match &key.source {
            ScopeKeySource::Column { input_column_id } => {
                insert_batch_cell(
                    &mut row,
                    batch,
                    row_index,
                    input_column_id,
                    &key.output_name,
                );
            }
            ScopeKeySource::Int64Expression(expression) => {
                if let Some(Some(value)) = eval_int64_projection(expression, batch, row_index) {
                    let data_type = output_column_type(output_schema, &key.output_name);
                    if let Some(scalar) = output_scalar_from_integer(value, data_type) {
                        row.insert(key.output_name.clone(), Some(scalar));
                    }
                }
            }
        }
    }
    row
}

fn output_column_type<'a>(schema: &'a RelationSchema, name: &str) -> Option<&'a SqlDataType> {
    schema
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| &column.data_type)
}

fn insert_batch_cell(
    row: &mut DependencyRow,
    batch: &RecordBatch,
    row_index: usize,
    input_column_id: &str,
    output_name: &str,
) {
    let Ok(index) = batch.schema().index_of(input_column_id) else {
        return;
    };
    if let Some(cell) = page_scalar_from_array(batch.column(index), row_index) {
        row.insert(output_name.to_string(), cell);
    }
}

/// `None` => unrepresentable (unknown); `Some(None)` => known null;
/// `Some(Some)` => known value. Physical types outside the `PageScalar`
/// families (Date32, decimals, binaries, nested, timezone'd timestamps,
/// non-finite floats) stay unknown.
fn page_scalar_from_array(array: &ArrayRef, row: usize) -> Option<Option<PageScalar>> {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    if array.is_null(row) {
        return Some(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(Some(PageScalar::Bool(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(Some(PageScalar::Int(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(Some(PageScalar::Int(i64::from(array.value(row)))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        return Some(Some(PageScalar::Int(i64::from(array.value(row)))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        return Some(Some(PageScalar::Int(i64::from(array.value(row)))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(array.value(row))
            .ok()
            .map(|value| Some(PageScalar::Int(value)));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        return Some(Some(PageScalar::Int(i64::from(array.value(row)))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        return Some(Some(PageScalar::Int(i64::from(array.value(row)))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        return Some(Some(PageScalar::Int(i64::from(array.value(row)))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        let value = array.value(row);
        return value.is_finite().then_some(Some(PageScalar::Float(value)));
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        let value = f64::from(array.value(row));
        return value.is_finite().then_some(Some(PageScalar::Float(value)));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Some(Some(PageScalar::Utf8(array.value(row).to_string())));
    }
    if matches!(
        array.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    ) {
        if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(Some(PageScalar::Timestamp(array.value(row))));
        }
    }
    None
}

/// Reads an event-time cell in nanoseconds, mirroring the runtime's units:
/// Int64 raw, Date32 days => ns, Timestamp(Nanosecond, no tz) raw.
fn event_time_ns(batch: &RecordBatch, row: usize, column_id: &str) -> Option<i64> {
    use arrow::array::{Date32Array, Int64Array, TimestampNanosecondArray};
    let index = batch.schema().index_of(column_id).ok()?;
    let array = batch.column(index);
    if array.is_null(row) {
        return None;
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
        return i64::from(array.value(row)).checked_mul(NS_PER_DAY);
    }
    if matches!(
        array.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    ) {
        if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(array.value(row));
        }
    }
    None
}

/// Negative-epoch-safe fixed tumbling window start: euclidean floor to the
/// window grid, matching the runtime's `div_euclid` assignment.
fn fixed_window_start(event_time_ns: i64, window_size_ns: i64) -> Option<i64> {
    if window_size_ns <= 0 {
        return None;
    }
    event_time_ns
        .div_euclid(window_size_ns)
        .checked_mul(window_size_ns)
}

/// Emits a nanosecond-domain integer under the admitted output column type:
/// integer columns carry the raw integer, timezone-less timestamp columns
/// carry nanoseconds; anything else (including Date, which `PageScalar`
/// cannot represent) stays unknown.
fn output_scalar_from_integer(value: i64, data_type: Option<&SqlDataType>) -> Option<PageScalar> {
    match data_type? {
        SqlDataType::Int8
        | SqlDataType::Int16
        | SqlDataType::Int32
        | SqlDataType::Int64
        | SqlDataType::UInt8
        | SqlDataType::UInt16
        | SqlDataType::UInt32
        | SqlDataType::UInt64 => Some(PageScalar::Int(value)),
        SqlDataType::Timestamp { timezone: None } => Some(PageScalar::Timestamp(value)),
        _ => None,
    }
}

fn int64_from_array(array: &ArrayRef, row: usize) -> Option<Option<i64>> {
    use arrow::array::{
        Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    if array.is_null(row) {
        return Some(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(Some(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        return Some(Some(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        return Some(Some(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(array.value(row)).ok().map(Some);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        return Some(Some(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        return Some(Some(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        return Some(Some(i64::from(array.value(row))));
    }
    None
}

/// Evaluates the legacy Int64 projection expression family.
/// `None` => unknown (unrepresentable, unsupported, or overflow);
/// `Some(None)` => known null; `Some(Some)` => known integer.
fn eval_int64_projection(
    expression: &SupportedProjectionExpr,
    batch: &RecordBatch,
    row: usize,
) -> Option<Option<i64>> {
    use SupportedProjectionBinaryOp as Op;
    match expression {
        SupportedProjectionExpr::Typed { .. } => None,
        SupportedProjectionExpr::Column { column_id } => {
            let index = batch.schema().index_of(column_id).ok()?;
            int64_from_array(batch.column(index), row)
        }
        SupportedProjectionExpr::LiteralInt64 { value } => Some(Some(*value)),
        SupportedProjectionExpr::BinaryInt64 { op, left, right } => {
            // `?` propagates unknown; the inner Option is the null cell.
            let (Some(left), Some(right)) = (
                eval_int64_projection(left, batch, row)?,
                eval_int64_projection(right, batch, row)?,
            ) else {
                return Some(None);
            };
            // Overflow and division by zero stay unknown (uncertifiable).
            match op {
                Op::Add => left.checked_add(right).map(Some),
                Op::Subtract => left.checked_sub(right).map(Some),
                Op::Multiply => left.checked_mul(right).map(Some),
                Op::Divide => left.checked_div(right).map(Some),
                Op::Modulo => left.checked_rem(right).map(Some),
            }
        }
        SupportedProjectionExpr::AbsInt64 { expr } => {
            match eval_int64_projection(expr, batch, row)? {
                None => Some(None),
                // Overflow stays unknown (outer None).
                Some(value) => value.checked_abs().map(Some),
            }
        }
        SupportedProjectionExpr::GreatestInt64 { exprs } => {
            reduce_extreme_int64(exprs, batch, row, true)
        }
        SupportedProjectionExpr::LeastInt64 { exprs } => {
            reduce_extreme_int64(exprs, batch, row, false)
        }
        SupportedProjectionExpr::CoalesceInt64 {
            column_id,
            fallback,
        } => {
            let index = batch.schema().index_of(column_id).ok()?;
            match int64_from_array(batch.column(index), row)? {
                Some(value) => Some(Some(value)),
                None => Some(Some(*fallback)),
            }
        }
        // CaseInt64 depends on a full row predicate evaluation that this
        // helper does not share; stay unknown (MAY_MATCH) instead of guessing.
        SupportedProjectionExpr::CaseInt64 { .. } => None,
    }
}

fn reduce_extreme_int64(
    exprs: &[SupportedProjectionExpr],
    batch: &RecordBatch,
    row: usize,
    greatest: bool,
) -> Option<Option<i64>> {
    if exprs.is_empty() {
        return None;
    }
    let mut best: Option<i64> = None;
    for expr in exprs {
        match eval_int64_projection(expr, batch, row)? {
            None => return Some(None),
            Some(value) => {
                best = Some(match best {
                    Some(current) if greatest => current.max(value),
                    Some(current) => current.min(value),
                    None => value,
                });
            }
        }
    }
    best.map(Some)
}

fn runtime_scalar_from_array(array: &ArrayRef, row: usize) -> Option<RuntimeScalarValue> {
    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    if array.is_null(row) {
        return None;
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(RuntimeScalarValue::Boolean(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(RuntimeScalarValue::Int64(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(RuntimeScalarValue::Int64(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        return Some(RuntimeScalarValue::Int64(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        return Some(RuntimeScalarValue::Int64(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(array.value(row))
            .ok()
            .map(RuntimeScalarValue::Int64);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        return Some(RuntimeScalarValue::Int64(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        return Some(RuntimeScalarValue::Int64(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        return Some(RuntimeScalarValue::Int64(i64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        let value = array.value(row);
        return value
            .is_finite()
            .then_some(RuntimeScalarValue::Float64(value));
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        let value = f64::from(array.value(row));
        return value
            .is_finite()
            .then_some(RuntimeScalarValue::Float64(value));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Some(RuntimeScalarValue::Utf8(array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
        return Some(RuntimeScalarValue::Date32(array.value(row)));
    }
    if matches!(
        array.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    ) {
        if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(RuntimeScalarValue::TimestampNanosecond(array.value(row)));
        }
    }
    None
}

fn page_scalar_from_runtime(value: RuntimeScalarValue) -> Option<Option<PageScalar>> {
    match value {
        RuntimeScalarValue::Null(_) => Some(None),
        RuntimeScalarValue::Boolean(value) => Some(Some(PageScalar::Bool(value))),
        RuntimeScalarValue::Int64(value) => Some(Some(PageScalar::Int(value))),
        RuntimeScalarValue::Float64(value) if value.is_finite() => {
            Some(Some(PageScalar::Float(value)))
        }
        RuntimeScalarValue::Float64(_) => None,
        RuntimeScalarValue::Utf8(value) => Some(Some(PageScalar::Utf8(value))),
        RuntimeScalarValue::TimestampNanosecond(value) => Some(Some(PageScalar::Timestamp(value))),
        RuntimeScalarValue::Date32(_) | RuntimeScalarValue::Decimal128 { .. } => None,
    }
}

/// Parses an explicit scope/query SQL string into a row predicate. Accepts a
/// full single-table SELECT or a bare WHERE predicate (wrapped against
/// `schema.relation_id`). Fails closed with 400 on anything that does not
/// parse to a predicate whose every leaf references a known field and a
/// supported typed comparison.
pub(super) fn parse_predicate(
    sql_or_scope: &str,
    binds: &[QueryBindValue],
    schema: &RelationSchema,
) -> Result<PagePredicate, ApiError> {
    let raw = materialized_page_predicate(sql_or_scope, schema, binds);
    let predicate = raw.or_else(|| {
        let wrapped = format!(
            "SELECT * FROM \"{}\" WHERE {}",
            schema.relation_id.replace('"', "\"\""),
            sql_or_scope
        );
        materialized_page_predicate(&wrapped, schema, binds)
    });
    let Some(predicate) = predicate else {
        return Err(ApiError::bad_request(
            "scope predicate must be a single-table SELECT or a bare WHERE predicate over the output relation",
        ));
    };
    if predicate_contains_unknown(&predicate) {
        return Err(ApiError::bad_request(
            "scope predicate contains an unsupported expression, unknown field, or uncomparable literal",
        ));
    }
    Ok(predicate)
}

fn predicate_contains_unknown(predicate: &PagePredicate) -> bool {
    match predicate {
        PagePredicate::Unknown => true,
        PagePredicate::Compare { .. } | PagePredicate::Null { .. } => false,
        PagePredicate::And(left, right) | PagePredicate::Or(left, right) => {
            predicate_contains_unknown(left) || predicate_contains_unknown(right)
        }
    }
}

/// Conservative row-level predicate match against a dependency row. Unknown
/// fields and unrepresentable comparisons always MAY_MATCH; only certified
/// values can ever exclude a row.
pub(super) fn predicate_may_match(predicate: &PagePredicate, row: &DependencyRow) -> bool {
    match predicate {
        PagePredicate::Unknown => true,
        PagePredicate::And(left, right) => {
            predicate_may_match(left, row) && predicate_may_match(right, row)
        }
        PagePredicate::Or(left, right) => {
            predicate_may_match(left, row) || predicate_may_match(right, row)
        }
        PagePredicate::Null { column, negated } => match row.get(column) {
            None => true,
            Some(None) => !negated,
            Some(Some(_)) => *negated,
        },
        PagePredicate::Compare { column, op, value } => match row.get(column) {
            None => true,
            Some(None) => false,
            Some(Some(actual)) => match compare_page_scalars(actual, value) {
                None => true,
                Some(ordering) => comparison_holds(*op, ordering),
            },
        },
    }
}

fn compare_page_scalars(left: &PageScalar, right: &PageScalar) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (PageScalar::Utf8(left), PageScalar::Utf8(right)) => left.partial_cmp(right),
        (PageScalar::Bool(left), PageScalar::Bool(right)) => left.partial_cmp(right),
        (PageScalar::Int(left), PageScalar::Int(right)) => left.partial_cmp(right),
        (PageScalar::Timestamp(left), PageScalar::Timestamp(right)) => left.partial_cmp(right),
        (PageScalar::Float(left), PageScalar::Float(right))
            if left.is_finite() && right.is_finite() =>
        {
            left.partial_cmp(right)
        }
        _ => None,
    }
}

fn comparison_holds(op: PageComparison, ordering: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match op {
        PageComparison::Eq => ordering == Equal,
        PageComparison::Ne => ordering != Equal,
        PageComparison::Lt => ordering == Less,
        PageComparison::Le => ordering != Greater,
        PageComparison::Gt => ordering == Greater,
        PageComparison::Ge => ordering != Less,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velorix_core::delta::{DeltaKey, DeltaValue};
    use velorix_core::view_plan::{
        BuiltinScalarFunctionV1, RuntimeScalarTypeV1, SupportedTumblingWindowPlan, TypedExprKindV1,
        TypedExprNodeV1, TypedExprProgramV1,
    };

    fn column(name: &str, data_type: SqlDataType, nullable: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            data_type,
            nullable,
        }
    }

    fn relation_schema(columns: Vec<ColumnSchema>, primary_key: Vec<String>) -> RelationSchema {
        RelationSchema {
            relation_id: "in_rel".to_string(),
            relation_name: "in_rel".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "fp".to_string(),
            columns,
            primary_key,
        }
    }

    fn binding(schema: RelationSchema) -> PublishedRelationBindingV1 {
        PublishedRelationBindingV1 {
            schema_version: 1,
            producer_view_id: "producer".to_string(),
            producer_view_generation: 1,
            producer_plan_hash: "plan".to_string(),
            relation: schema,
            output_schema_hash: "hash".to_string(),
            key_descriptor_hash: "key".to_string(),
            output_stream_id: "stream".to_string(),
            delta_codec_identity: "codec".to_string(),
            frontier_kind: "frontier".to_string(),
        }
    }

    fn record(key: Value, value: Value, weight: i64) -> DeltaRecord {
        DeltaRecord::new(
            DeltaKey::from_json(key),
            DeltaValue::from_json(value),
            weight,
        )
    }

    fn dependencies(
        plan: ScopeDependenciesPlan,
        output_schema: RelationSchema,
        input_schema: RelationSchema,
    ) -> ScopeDependencies {
        let eligible_columns = eligible_columns_for(&plan);
        ScopeDependencies {
            plan,
            output_schema,
            input_binding: Some(binding(input_schema)),
            source_schema: None,
            source_catalog: None,
            eligible_columns,
        }
    }

    #[test]
    fn tumbling_renamed_key_negative_epoch_window_bounds() {
        let schema = relation_schema(
            vec![
                column("kid", SqlDataType::Int64, false),
                column("evt", SqlDataType::Int64, false),
            ],
            vec!["kid".to_string()],
        );
        let output_schema = relation_schema(
            vec![
                column("grp", SqlDataType::Int64, false),
                column("ws", SqlDataType::Int64, false),
                column("we", SqlDataType::Int64, false),
            ],
            vec!["grp".to_string(), "ws".to_string(), "we".to_string()],
        );
        let mut plan = SupportedTumblingWindowPlan {
            input_relation_id: "in_rel".to_string(),
            group_key_column_id: "kid".to_string(),
            output_key_column_id: "grp".to_string(),
            event_time_column_id: "evt".to_string(),
            window_kind: SupportedEventTimeWindowKind::Tumbling,
            window_size_ns: 60_000_000_000,
            hop_slide_ns: None,
            session_gap_ns: None,
            sum_value_column_id: "evt".to_string(),
            aggregate_outputs: Vec::new(),
            predicate_expr: None,
            aggregate_filter_exprs: BTreeMap::new(),
            having_expr: None,
            top_k: None,
            window_start_output_column_id: "ws".to_string(),
            window_end_output_column_id: "we".to_string(),
            late_row_policy: None,
            retention_contract: None,
        };
        let scope = dependencies(
            tumbling_window_plan(&plan)
                .unwrap()
                .expect("tumbling plan must be eligible"),
            output_schema,
            schema.clone(),
        );
        let rows = scope
            .dependency_rows(
                &binding(schema),
                &[record(json!({"kid": 1}), json!({"evt": -1_i64}), 1)],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("grp"), Some(&Some(PageScalar::Int(1))));
        assert_eq!(
            rows[0].get("ws"),
            Some(&Some(PageScalar::Int(-60_000_000_000)))
        );
        assert_eq!(rows[0].get("we"), Some(&Some(PageScalar::Int(0))));
        assert_eq!(rows[0].len(), 3);
        assert_eq!(
            scope.eligible_known_columns(),
            ["grp", "ws", "we"].map(String::from).as_slice()
        );
        plan.hop_slide_ns = Some(1);
        assert!(tumbling_window_plan(&plan).unwrap().is_none());
        plan.hop_slide_ns = None;
        plan.session_gap_ns = Some(1);
        assert!(tumbling_window_plan(&plan).unwrap().is_none());
        plan.session_gap_ns = None;
        plan.window_size_ns = 0;
        assert!(tumbling_window_plan(&plan).unwrap().is_none());
    }

    #[test]
    fn single_key_group_keys_known_aggregate_outputs_unknown() {
        let schema = relation_schema(
            vec![column("k", SqlDataType::Utf8, false)],
            vec!["k".to_string()],
        );
        let output_schema = relation_schema(
            vec![
                column("grp", SqlDataType::Utf8, false),
                column("sum", SqlDataType::Float64, false),
                column("count", SqlDataType::Int64, false),
            ],
            vec!["grp".to_string()],
        );
        let plan = ScopeDependenciesPlan::SingleKeyGroup {
            input_relation_id: "in_rel".to_string(),
            keys: vec![ScopeKeyProjection {
                output_name: "grp".to_string(),
                source: ScopeKeySource::Column {
                    input_column_id: "k".to_string(),
                },
            }],
        };
        let scope = dependencies(plan, output_schema, schema.clone());
        let rows = scope
            .dependency_rows(&binding(schema), &[record(json!("a"), json!({}), 1)])
            .unwrap();
        assert_eq!(
            rows[0].get("grp"),
            Some(&Some(PageScalar::Utf8("a".into())))
        );
        assert!(!rows[0].contains_key("sum"));
        assert!(!rows[0].contains_key("count"));
        assert_eq!(
            scope.eligible_known_columns(),
            ["grp"].map(String::from).as_slice()
        );
    }

    #[test]
    fn single_key_null_group_key_is_known_null() {
        let schema = relation_schema(
            vec![column("k", SqlDataType::Int64, true)],
            vec!["k".to_string()],
        );
        let output_schema = relation_schema(
            vec![column("grp", SqlDataType::Int64, true)],
            vec!["grp".to_string()],
        );
        let plan = ScopeDependenciesPlan::SingleKeyGroup {
            input_relation_id: "in_rel".to_string(),
            keys: vec![ScopeKeyProjection {
                output_name: "grp".to_string(),
                source: ScopeKeySource::Column {
                    input_column_id: "k".to_string(),
                },
            }],
        };
        let scope = dependencies(plan, output_schema, schema.clone());
        let rows = scope
            .dependency_rows(
                &binding(schema),
                &[record(json!({"k": null}), json!({}), 1)],
            )
            .unwrap();
        assert_eq!(rows[0].get("grp"), Some(&None));
    }

    #[test]
    fn filter_project_direct_renamed_and_typed_columns_known() {
        let schema = relation_schema(
            vec![
                column("kid", SqlDataType::Int64, false),
                column("amt", SqlDataType::Int64, false),
                column("tag", SqlDataType::Utf8, false),
            ],
            vec!["kid".to_string()],
        );
        let output_schema = relation_schema(
            vec![
                column("grp", SqlDataType::Int64, false),
                column("amount", SqlDataType::Int64, false),
                column("double", SqlDataType::Int64, false),
                column("tag_len", SqlDataType::Int64, false),
            ],
            vec!["grp".to_string()],
        );
        let text_node = |column_id: &str| TypedExprNodeV1 {
            result_type: RuntimeScalarTypeV1::Utf8,
            nullable: false,
            kind: TypedExprKindV1::Column {
                column_id: column_id.to_string(),
            },
        };
        let root = TypedExprNodeV1 {
            result_type: RuntimeScalarTypeV1::Int64,
            nullable: false,
            kind: TypedExprKindV1::Call {
                function: BuiltinScalarFunctionV1::Length,
                args: vec![text_node("tag")],
            },
        };
        let program = TypedExprProgramV1 {
            encoding_version: 1,
            root,
        };
        let plan = ScopeDependenciesPlan::RowLocalProject {
            input_relation_id: "in_rel".to_string(),
            key: ScopeKeyProjection {
                output_name: "grp".to_string(),
                source: ScopeKeySource::Column {
                    input_column_id: "kid".to_string(),
                },
            },
            value_columns: vec![
                ScopeRowColumn::Column {
                    output_name: "amount".to_string(),
                    input_column_id: "amt".to_string(),
                },
                ScopeRowColumn::Int64Expression {
                    output_name: "double".to_string(),
                    expr: SupportedProjectionExpr::BinaryInt64 {
                        op: SupportedProjectionBinaryOp::Add,
                        left: Box::new(SupportedProjectionExpr::Column {
                            column_id: "amt".to_string(),
                        }),
                        right: Box::new(SupportedProjectionExpr::Column {
                            column_id: "amt".to_string(),
                        }),
                    },
                },
                ScopeRowColumn::TypedExpression {
                    output_name: "tag_len".to_string(),
                    program,
                },
            ],
        };
        let scope = dependencies(plan, output_schema, schema.clone());
        let rows = scope
            .dependency_rows(
                &binding(schema),
                &[record(
                    json!({"kid": 7}),
                    json!({"amt": 3, "tag": "abc"}),
                    1,
                )],
            )
            .unwrap();
        assert_eq!(rows[0].get("grp"), Some(&Some(PageScalar::Int(7))));
        assert_eq!(rows[0].get("amount"), Some(&Some(PageScalar::Int(3))));
        assert_eq!(rows[0].get("double"), Some(&Some(PageScalar::Int(6))));
        assert_eq!(rows[0].get("tag_len"), Some(&Some(PageScalar::Int(3))));
    }

    #[test]
    fn predicate_may_match_or_unknown_is_conservative() {
        let row: DependencyRow = BTreeMap::from([("k".to_string(), Some(PageScalar::Int(2)))]);
        let mismatch = PagePredicate::Compare {
            column: "k".to_string(),
            op: PageComparison::Eq,
            value: PageScalar::Int(1),
        };
        let unknown = PagePredicate::Unknown;
        assert!(predicate_may_match(
            &PagePredicate::Or(Box::new(mismatch.clone()), Box::new(unknown.clone())),
            &row
        ));
        assert!(!predicate_may_match(
            &PagePredicate::And(Box::new(mismatch), Box::new(unknown)),
            &row
        ));
    }

    #[test]
    fn typed_map_preserves_signed_duplicate_and_zero_record_ordinals() {
        let schema = relation_schema(vec![column("k", SqlDataType::Int64, false)], vec![]);
        let plan = SupportedTypedMapPlanV1 {
            schema_version: 1,
            input_schema: schema.clone(),
            projection: vec![TypedExprProgramV1 {
                encoding_version: 1,
                root: TypedExprNodeV1 {
                    result_type: RuntimeScalarTypeV1::Int64,
                    nullable: false,
                    kind: TypedExprKindV1::Column {
                        column_id: "k".into(),
                    },
                },
            }],
            output_columns: vec![column("renamed", SqlDataType::Int64, false)],
            predicate: None,
        };
        let scope = dependencies(
            typed_map_plan(&plan).unwrap().unwrap(),
            relation_schema(plan.output_columns.clone(), vec![]),
            schema.clone(),
        );
        let input = binding(schema);
        let records = [
            record(json!({"k": 7}), Value::Null, 1),
            record(json!({"k": 7}), Value::Null, -1),
            record(json!({"k": 9}), Value::Null, 0),
        ];
        let rows = scope.dependency_rows(&input, &records).unwrap();
        assert_eq!(rows.len(), records.len());
        assert_eq!(rows[0].get("renamed"), Some(&Some(PageScalar::Int(7))));
        assert_eq!(rows[1], rows[0]);
        assert!(rows[2].is_empty());
        assert!(scope.dependency_rows(&input, &[]).unwrap().is_empty());

        let mut rebound = input.clone();
        rebound.producer_view_generation += 1;
        assert!(scope.dependency_rows(&rebound, &records).is_err());
        rebound = input;
        rebound.relation.columns[0].nullable = true;
        assert!(scope.dependency_rows(&rebound, &records).is_err());
    }

    #[test]
    fn incomparable_values_and_arithmetic_overflow_remain_unknown() {
        let row = BTreeMap::from([("k".to_string(), Some(PageScalar::Int(i64::MAX)))]);
        assert!(predicate_may_match(
            &PagePredicate::Compare {
                column: "k".into(),
                op: PageComparison::Eq,
                value: PageScalar::Float(i64::MAX as f64),
            },
            &row
        ));
        let batch = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
        let expression = SupportedProjectionExpr::BinaryInt64 {
            op: SupportedProjectionBinaryOp::Add,
            left: Box::new(SupportedProjectionExpr::LiteralInt64 { value: i64::MAX }),
            right: Box::new(SupportedProjectionExpr::LiteralInt64 { value: 1 }),
        };
        assert_eq!(eval_int64_projection(&expression, &batch, 0), None);
        assert_eq!(fixed_window_start(i64::MIN, 3), None);
    }

    #[test]
    fn source_projection_uses_catalog_ids_and_preserves_artifact_ordinals() {
        use crate::example_fixtures::orders_sum_count_relation_catalog;
        use velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID;
        let mut physical = orders_sum_count_relation_catalog().unwrap().relation_schema;
        physical.columns[0].name = "physical_key".into();
        let catalog = VelorixRelationCatalogV1::from_relation_schema(
            physical,
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap();
        let schema = velorix_core::view_contract::catalog_input_relation_schema(&catalog).unwrap();
        let plan = ScopeDependenciesPlan::SingleKeyGroup {
            input_relation_id: schema.relation_id.clone(),
            keys: vec![ScopeKeyProjection {
                output_name: "group".into(),
                source: ScopeKeySource::Column {
                    input_column_id: "account_id".into(),
                },
            }],
        };
        let mut scope = dependencies(
            plan,
            relation_schema(vec![column("group", SqlDataType::Utf8, false)], vec![]),
            schema.clone(),
        );
        scope.input_binding = None;
        scope.source_schema = Some(schema.clone());
        let batch = RecordBatch::try_new(
            datafusion_schema_from_catalog(&catalog).unwrap(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![1, -1, 0])),
            ],
        )
        .unwrap();
        let unknown = scope
            .source_dependency_rows(&schema.relation_id, std::slice::from_ref(&batch))
            .unwrap();
        assert_eq!(unknown.len(), 3);
        assert!(unknown.iter().all(BTreeMap::is_empty));
        scope.bind_source_catalog(&catalog).unwrap();
        let rows = scope
            .source_dependency_rows(
                &schema.relation_id,
                &[batch.slice(0, 2), batch.slice(2, 1), batch.slice(0, 0)],
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].get("group"),
            Some(&Some(PageScalar::Utf8("a".into())))
        );
        assert_eq!(rows[1], rows[0]);
        assert_eq!(
            rows[2].get("group"),
            Some(&Some(PageScalar::Utf8("b".into())))
        );
        assert!(scope.source_dependency_rows("other", &[batch]).is_err());
    }

    #[test]
    fn physical_tumbling_bounds_preserve_ordinals_and_unknown_null_time() {
        use crate::example_fixtures::orders_sum_count_relation_catalog;
        use velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID;
        let mut physical = orders_sum_count_relation_catalog().unwrap().relation_schema;
        let key_id = physical.columns[0].column_id.clone();
        let event_id = physical.columns[1].column_id.clone();
        physical.columns[0].name = "physical_group".into();
        physical.columns[1].name = "physical_time".into();
        physical.columns[1].nullable = true;
        let catalog = VelorixRelationCatalogV1::from_relation_schema(
            physical,
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap();
        let schema = velorix_core::view_contract::catalog_input_relation_schema(&catalog).unwrap();
        let mut scope = dependencies(
            ScopeDependenciesPlan::TumblingWindow {
                input_relation_id: schema.relation_id.clone(),
                key: ScopeKeyProjection {
                    output_name: "group".into(),
                    source: ScopeKeySource::Column {
                        input_column_id: key_id,
                    },
                },
                event_time_column_id: event_id,
                window_size_ns: 10,
                window_start_output: "start".into(),
                window_end_output: "end".into(),
            },
            relation_schema(
                vec![
                    column("group", SqlDataType::Utf8, false),
                    column("start", SqlDataType::Int64, false),
                    column("end", SqlDataType::Int64, false),
                ],
                vec![],
            ),
            schema.clone(),
        );
        scope.input_binding = None;
        scope.source_schema = Some(schema.clone());
        scope.bind_source_catalog(&catalog).unwrap();
        let batch = RecordBatch::try_new(
            datafusion_schema_from_catalog(&catalog).unwrap(),
            vec![
                Arc::new(StringArray::from(vec!["a"; 6])),
                Arc::new(Int64Array::from(vec![
                    Some(-11),
                    Some(-10),
                    Some(-1),
                    Some(0),
                    Some(10),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![1, -1, 0, 1, 1, 1])),
            ],
        )
        .unwrap();
        let rows = scope
            .source_dependency_rows(&schema.relation_id, &[batch])
            .unwrap();
        assert_eq!(rows.len(), 6);
        for (row, start) in rows.iter().zip([-20, -10, -10, 0, 10]) {
            assert_eq!(row.get("group"), Some(&Some(PageScalar::Utf8("a".into()))));
            assert_eq!(row.get("start"), Some(&Some(PageScalar::Int(start))));
            assert_eq!(row.get("end"), Some(&Some(PageScalar::Int(start + 10))));
        }
        // A missing event time cannot prove exclusion, even for a range which
        // excludes every known event above. Signed/zero weights keep ordinals.
        assert!(!rows[5].contains_key("start"));
        assert!(!rows[5].contains_key("end"));
        assert!(predicate_may_match(
            &PagePredicate::Compare {
                column: "start".into(),
                op: PageComparison::Eq,
                value: PageScalar::Int(100),
            },
            &rows[5]
        ));
    }

    #[test]
    fn predicate_may_match_null_and_compare_semantics() {
        let row: DependencyRow = BTreeMap::from([
            ("a".to_string(), None),
            ("b".to_string(), Some(PageScalar::Int(5))),
        ]);
        assert!(predicate_may_match(
            &PagePredicate::Null {
                column: "a".to_string(),
                negated: false
            },
            &row
        ));
        assert!(!predicate_may_match(
            &PagePredicate::Null {
                column: "a".to_string(),
                negated: true
            },
            &row
        ));
        assert!(!predicate_may_match(
            &PagePredicate::Compare {
                column: "a".to_string(),
                op: PageComparison::Eq,
                value: PageScalar::Int(1),
            },
            &row
        ));
        assert!(predicate_may_match(
            &PagePredicate::Compare {
                column: "missing".to_string(),
                op: PageComparison::Eq,
                value: PageScalar::Int(1),
            },
            &row
        ));
        assert!(predicate_may_match(
            &PagePredicate::Compare {
                column: "b".to_string(),
                op: PageComparison::Gt,
                value: PageScalar::Int(4),
            },
            &row
        ));
        assert!(!predicate_may_match(
            &PagePredicate::Compare {
                column: "b".to_string(),
                op: PageComparison::Lt,
                value: PageScalar::Int(4),
            },
            &row
        ));
    }

    #[test]
    fn parse_predicate_wraps_bare_where_and_rejects_garbage() {
        let schema = relation_schema(vec![column("k", SqlDataType::Int64, false)], vec![]);
        let schema = RelationSchema {
            relation_id: "out_rel".to_string(),
            ..schema
        };
        let bare = parse_predicate("k = 1", &[], &schema).unwrap();
        assert_eq!(
            bare,
            PagePredicate::Compare {
                column: "k".to_string(),
                op: PageComparison::Eq,
                value: PageScalar::Int(1),
            }
        );
        let full = parse_predicate("SELECT * FROM out_rel WHERE k > 2", &[], &schema).unwrap();
        assert!(matches!(
            full,
            PagePredicate::Compare {
                op: PageComparison::Gt,
                ..
            }
        ));
        assert!(parse_predicate("not sql at all", &[], &schema).is_err());
        assert!(parse_predicate("SELECT * FROM other WHERE k = 1", &[], &schema).is_err());
        assert!(parse_predicate("k = missing_field", &[], &schema).is_err());
    }
}

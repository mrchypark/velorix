//! Materialized view runtime using GeneralCircuitRuntime.
//!
//! This module provides the public API for creating and managing materialized view runtimes.
//! All SQL is compiled to a Circuit IR, incrementalized, and executed via GeneralCircuitRuntime.

use velorix_core::{
    delta::DeltaBatch,
    relation::VelorixRelationCatalogV1,
    standing_program::{
        MaterializedViewPage, RuntimeCheckpoint, ScopedViewId,
        SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
        StandingProgramRuntimeError,
    },
    view_contract::RelationSchema,
    view_plan::VelorixLogicalViewPlanV1,
};

pub use crate::runtime_contract::MATERIALIZED_VIEW_RUNTIME_NAME as CRATE_NAME;

use crate::general_circuit_runtime::GeneralStandingRuntime;
use crate::disk_state::DiskStateConfig;
use velorix_core::circuit::Circuit;
use velorix_core::incrementalize::incrementalize;
use velorix_core::sql_to_circuit::{sql_to_circuit, TableSchema};

// ---------------------------------------------------------------------------
// Public API: Create runtimes
// ---------------------------------------------------------------------------

/// Create a standing runtime from a SQL string.
pub fn create_standing_runtime(
    _identity: &StandingProgramIdentity,
    _catalog: &VelorixRelationCatalogV1,
    _input_schemas: &[RelationSchema],
    _output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    Err("materialized view runtime requires SQL and catalog metadata".to_string())
}

/// Create a standing runtime from SQL and a single catalog.
pub fn create_standing_runtime_with_sql(
    identity: &StandingProgramIdentity,
    catalog: &VelorixRelationCatalogV1,
    sql: &str,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    create_standing_runtime_with_sql_and_catalogs(
        identity,
        std::slice::from_ref(catalog),
        sql,
        input_schemas,
        output_schemas,
    )
}

/// Create a standing runtime from SQL and multiple catalogs.
pub fn create_standing_runtime_with_sql_and_catalogs(
    identity: &StandingProgramIdentity,
    catalogs: &[VelorixRelationCatalogV1],
    sql: &str,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    create_general_circuit_runtime(identity, catalogs, input_schemas, output_schemas, sql)
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
}

/// Create a standing runtime from a logical plan and catalogs.
pub fn create_standing_runtime_with_logical_plan_and_catalogs(
    identity: &StandingProgramIdentity,
    catalogs: &[VelorixRelationCatalogV1],
    logical_plan: VelorixLogicalViewPlanV1,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    create_general_circuit_runtime(
        identity,
        catalogs,
        input_schemas,
        output_schemas,
        &logical_plan.view_sql,
    )
    .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
}

// ---------------------------------------------------------------------------
// GeneralCircuitRuntime factory
// ---------------------------------------------------------------------------

/// Create a GeneralCircuitRuntime from SQL and catalogs.
fn create_general_circuit_runtime(
    identity: &StandingProgramIdentity,
    catalogs: &[VelorixRelationCatalogV1],
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
    sql: &str,
) -> Result<GeneralStandingRuntime, String> {
    // Build table schemas from catalogs
    let table_schemas: Vec<TableSchema> = catalogs
        .iter()
        .map(|catalog| {
            let columns: Vec<String> = catalog
                .relation_schema
                .columns
                .iter()
                .map(|col| col.column_id.clone())
                .collect();
            TableSchema {
                name: catalog.relation_schema.relation_id.clone(),
                columns,
            }
        })
        .collect();

    // Parse SQL to Circuit
    let circuit: Circuit = sql_to_circuit(sql, &table_schemas)
        .map_err(|e| format!("SQL to circuit conversion failed: {}", e))?;

    // Incrementalize the circuit
    let incremental_circuit = incrementalize(&circuit);

    // Create state config for foyer-backed state
    let state_dir = std::env::temp_dir().join(format!(
        "velorix-general-state-{}",
        identity.program_id
    ));
    let state_config = DiskStateConfig::new(
        state_dir,
        64 * 1024 * 1024,  // 64MB in-memory
        512 * 1024 * 1024, // 512MB on disk
    );

    // Create the runtime on a dedicated OS thread to avoid
    // "Cannot start a runtime from within a runtime" when called inside tokio.
    let identity_clone = identity.clone();
    let input_schemas_owned = input_schemas.to_vec();
    let output_schemas_owned = output_schemas.to_vec();
    let runtime = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to build init runtime: {}", e))?;
        rt.block_on(GeneralStandingRuntime::new(
            identity_clone,
            input_schemas_owned,
            output_schemas_owned,
            incremental_circuit,
            &state_config,
        ))
        .map_err(|e| format!("Failed to create general circuit runtime: {}", e))
    })
    .join()
    .map_err(|e| format!("Thread join failed: {:?}", e))?
    .map_err(|e| e)?;

    Ok(runtime)
}

// ---------------------------------------------------------------------------
// Public API: Materialized view pages
// ---------------------------------------------------------------------------

/// Convert a materialized delta batch to a page for serving.
pub fn materialized_delta_to_page(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    scoped_view: ScopedViewId,
    logical_epoch: u64,
    _page: SnapshotPageRequest,
) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
    use velorix_core::delta_to_arrow::delta_batch_to_record_batch;

    let batch = delta_batch_to_record_batch(output_schema, published_output)
        .map_err(|e| StandingProgramRuntimeError::ExternalRuntime {
            reason: format!("delta-to-arrow conversion error: {}", e),
        })?;

    Ok(MaterializedViewPage {
        view: scoped_view,
        logical_epoch,
        schema_fingerprint: output_schema.schema_fingerprint.clone(),
        batches: vec![batch],
        next_page_token: None,
    })
}

// ---------------------------------------------------------------------------
// Public API: Restore runtimes
// ---------------------------------------------------------------------------

/// Restore a standing runtime from a checkpoint.
pub fn restore_standing_runtime(
    checkpoint: RuntimeCheckpoint,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    GeneralStandingRuntime::restore(checkpoint)
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|e| format!("restore failed: {}", e))
}





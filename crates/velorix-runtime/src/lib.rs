//! Stateless execution runtime boundaries for Velorix.

#![forbid(unsafe_code)]

pub mod arrow_batch_operator;
pub mod benchmark_gate;
pub mod compiled_expression;
pub mod epoch_overlay;
pub mod frontier_conformance;
pub mod incremental_sql_comparison;
pub mod join_index;
pub mod json_api_boundary;
pub mod materialized_view_runtime;
pub mod query_policy_catalog;
pub mod recursive_frontier;
pub mod runtime_contract;
pub mod window_partition_state;

/// Stable classification for the one legacy checkpoint shape that the
/// explicit startup migration is permitted to rebuild. Keep this exact: API
/// recovery must not turn unrelated restore failures into migrations.
pub const LEGACY_SINGLE_KEY_GROUP_COUNTS_MISSING_FIELD: &str =
    "generic_checkpoint_payload:legacy_single_key_group_counts_missing";

pub fn is_legacy_single_key_group_counts_missing(error: &str) -> bool {
    error
        == format!(
        "invalid standing program identity field: {LEGACY_SINGLE_KEY_GROUP_COUNTS_MISSING_FIELD}"
    )
}

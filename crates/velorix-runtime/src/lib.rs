//! Stateless execution runtime boundaries for Velorix.

#![forbid(unsafe_code)]

pub mod benchmark_gate;
pub mod epoch_overlay;
pub mod frontier_conformance;
pub mod incremental_sql_comparison;
pub mod join_index;
pub mod materialized_view_runtime;
pub mod query_policy_catalog;
pub mod recursive_frontier;
pub mod runtime_contract;
pub mod window_partition_state;

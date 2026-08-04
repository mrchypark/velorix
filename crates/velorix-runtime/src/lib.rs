//! Stateless execution runtime boundaries for Velorix.

#![forbid(unsafe_code)]

pub use velorix_control::benchmark_gate;
pub mod cache;
pub mod disk_state;
pub mod general_circuit_runtime;
pub mod leased_checkpoint;
pub use velorix_control::materialized_view_runtime;
mod object_meter;
#[cfg(feature = "legacy-source-scan-surfaces")]
pub mod persisted_query;
#[cfg(feature = "legacy-source-scan-surfaces")]
pub mod persisted_table;
#[cfg(feature = "legacy-source-scan-surfaces")]
pub mod persisted_view;
pub mod query;
pub mod query_policy_catalog;
mod query_runtime;
pub use velorix_control::readiness;
pub mod recovery;
pub mod storage_registry;

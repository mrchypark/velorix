//! Stateless execution runtime boundaries for Velorix.

#![forbid(unsafe_code)]

pub mod benchmark_gate;
pub mod cache;
pub mod leased_checkpoint;
pub mod materialized_view_runtime;
mod object_meter;
pub mod persisted_query;
pub mod persisted_table;
pub mod persisted_view;
pub mod query;
pub mod query_policy_catalog;
mod query_runtime;
pub mod readiness;
pub mod recovery;
pub mod storage_registry;

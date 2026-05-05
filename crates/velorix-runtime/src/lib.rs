//! Stateless execution runtime boundaries for Velorix.

#![forbid(unsafe_code)]

pub mod benchmark_gate;
pub mod cache;
pub mod leased_checkpoint;
pub mod persisted_query;
pub mod persisted_table;
pub mod persisted_view;
pub mod query;
mod query_runtime;
pub mod recovery;
pub mod storage_registry;

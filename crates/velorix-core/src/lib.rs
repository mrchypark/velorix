//! Core domain types and incremental computation boundaries for Velorix.

#![forbid(unsafe_code)]

pub mod circuit;
pub mod delta;
pub mod delta_to_arrow;
pub mod engine;
pub mod incrementalize;
pub mod operator;
pub mod query;
pub mod relation;
pub mod resource_policy;
pub mod sql_to_circuit;
pub mod standing_program;
pub mod view_contract;
pub mod view_plan;

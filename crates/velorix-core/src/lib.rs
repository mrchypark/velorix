//! Core domain types and incremental computation boundaries for Velorix.

#![forbid(unsafe_code)]

pub mod collision_audit;
pub mod delta;
pub mod delta_to_arrow;
pub mod engine;
pub mod native_operator;
pub mod operator;
pub mod operator_contract;
pub mod query;
pub mod relation;
pub mod resource_policy;
pub mod standing_program;
pub mod state_replay_plan;
pub mod view_contract;
pub mod view_plan;

//! Core domain types and incremental computation boundaries for Velorix.

#![forbid(unsafe_code)]

#[cfg(feature = "dbsp-spike")]
pub mod dbsp_engine;
pub mod dbsp_view_plan;
pub mod delta;
pub mod engine;
pub mod feldera_artifact;
pub mod feldera_package_runtime;
#[cfg(feature = "feldera-package-compat")]
pub mod feldera_program_descriptor;
pub mod generated_view_descriptor;
pub mod operator;
pub mod query;
pub mod relation;
pub mod resource_policy;
pub mod standing_program;

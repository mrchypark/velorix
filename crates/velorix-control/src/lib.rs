//! Pure control-plane domain boundaries for Velorix.
//!
//! This crate uses Kubernetes-native vocabulary for ownership, but it does not
//! depend on Kubernetes and does not make Kubernetes or etcd the durable
//! database authority. Lease grants only produce storage-compatible owner
//! claims; object storage remains responsible for durable progress publication.

#![forbid(unsafe_code)]

pub mod benchmark_gate;
pub mod control_plane_contract;
pub mod ingest_writer_runtime;
pub mod lease;
pub mod materialized_view_runtime;
pub mod meta_admin;
pub mod operator_authority;
pub mod query_policy_catalog;
pub mod readiness;
pub mod reconcile_plan;
pub mod runtime_contract;
pub mod storage_admin;

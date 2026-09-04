//! Pure control-plane domain boundaries for Velorix.
//!
//! This crate uses Kubernetes-native vocabulary for ownership, but it does not
//! depend on Kubernetes and does not make Kubernetes or etcd the durable
//! database authority. Lease grants only produce storage-compatible owner
//! claims; object storage remains responsible for durable progress publication.

#![forbid(unsafe_code)]

pub mod control_plane_contract;
pub mod ingest_writer_runtime;
pub mod lease;
pub mod leased_checkpoint;
pub mod meta_admin;
pub mod operator_authority;
pub mod readiness;
pub mod reconcile_plan;
pub mod relation_ingest_publisher;
pub mod storage_admin;

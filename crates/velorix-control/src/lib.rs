//! Pure control-plane domain boundaries for Velorix.
//!
//! This crate uses Kubernetes-native vocabulary for ownership, but it does not
//! depend on Kubernetes and does not make Kubernetes or etcd the durable
//! database authority. Lease grants only produce storage-compatible owner
//! claims; object storage remains responsible for durable progress publication.

#![forbid(unsafe_code)]

pub mod control_plane_contract;
pub mod lease;
pub mod reconcile_plan;

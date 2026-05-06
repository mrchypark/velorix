//! Kubernetes-facing API types for Velorix.
//!
//! This crate owns Kubernetes dependencies and generated CRDs. It must not make
//! Kubernetes or etcd the durable database authority; CRDs express desired
//! intent and observed status while Velorix object-store records remain
//! authoritative.

#![forbid(unsafe_code)]

pub mod controller;
pub mod crd;
pub mod lease;
pub mod startup;
pub mod status;
pub mod stream_watch;
pub mod worker_shard;

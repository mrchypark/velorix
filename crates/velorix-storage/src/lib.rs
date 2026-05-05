//! Object-storage interfaces and manifest boundaries for Velorix.

#![forbid(unsafe_code)]

pub mod capability;
pub mod checkpoint_index;
pub mod feldera_artifact_registry;
pub mod gc;
pub mod ingest_envelope;
pub mod log;
pub mod manifest;
pub mod object_key;
pub mod ownership;
pub mod relation_catalog_registry;
pub mod state;
pub mod state_store;

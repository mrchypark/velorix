use crate::{MetaStoreError, ViewBootstrapControlV1};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use velorix_core::standing_program::CausalViewCursorV1;

pub const PUBLISHED_INPUT_BOOTSTRAP_SCHEMA_VERSION_V1: u32 = 1;

/// Committed atomically with runtime state through its checkpoint pointer.
/// Counters describe complete immutable manifest pages, never producer epochs.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedInputBootstrapProgressV1 {
    pub schema_version: u32,
    pub bootstrap_generation: u64,
    pub plan_hash: String,
    pub inputs: Vec<PublishedInputBootstrapCursorV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedInputBootstrapCursorV1 {
    pub input_edge: String,
    /// Frozen target, not an applied causal cursor. The runtime checkpoint
    /// must not claim this cursor until the complete baseline is committed.
    pub captured_cursor: CausalViewCursorV1,
    /// Immutable manifest identity, validated against the producer checkpoint
    /// by the publisher before first publication; fixed by subsequent CASes.
    pub output_manifest_ref: String,
    pub output_manifest_hash: String,
    pub output_schema_hash: String,
    pub total_pages: u64,
    /// Expanded bag multiplicity, not the number of distinct stored rows.
    pub total_rows: u64,
    pub next_page_index: u64,
    pub applied_rows: u64,
}

fn invalid() -> MetaStoreError {
    MetaStoreError::Serialization("invalid published input bootstrap progress".into())
}
fn digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

impl PublishedInputBootstrapProgressV1 {
    pub fn validate(&self) -> Result<(), MetaStoreError> {
        if self.schema_version != PUBLISHED_INPUT_BOOTSTRAP_SCHEMA_VERSION_V1
            || self.bootstrap_generation == 0
            || self.plan_hash.trim().is_empty()
            || self.inputs.is_empty()
        {
            return Err(invalid());
        }
        let mut edges = BTreeSet::new();
        for input in &self.inputs {
            input.captured_cursor.validate().map_err(|_| invalid())?;
            if input.input_edge != input.captured_cursor.input_edge
                || !edges.insert(&input.input_edge)
                || input.output_manifest_ref.trim().is_empty()
                || !digest(&input.output_manifest_hash)
                || !digest(&input.output_schema_hash)
                || input.next_page_index > input.total_pages
                || input.applied_rows > input.total_rows
                || (input.next_page_index == 0 && input.applied_rows != 0)
                || (input.total_pages == 0 && input.total_rows != 0)
                || (input.next_page_index == input.total_pages
                    && input.applied_rows != input.total_rows)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.validate().is_ok()
            && self.inputs.iter().all(|input| {
                input.next_page_index == input.total_pages && input.applied_rows == input.total_rows
            })
    }

    pub(crate) fn matches_control(&self, control: &ViewBootstrapControlV1) -> bool {
        self.validate().is_ok()
            && self.bootstrap_generation == control.bootstrap_generation
            && self.plan_hash == control.plan_hash
            && self.inputs.len() == control.view_inputs.len()
            && control.view_inputs.iter().all(|edge| {
                self.inputs.iter().any(|input| {
                    input.input_edge == edge.edge_id
                        && edge
                            .bootstrap_cursor
                            .as_ref()
                            .is_none_or(|cursor| &input.captured_cursor == cursor)
                        && input.captured_cursor.producer_tenant_id == control.tenant_id
                        && input.captured_cursor.producer_program_id == edge.producer_program_id
                        && input.captured_cursor.producer_view_id == edge.producer_view_id
                        && input.captured_cursor.producer_generation == edge.producer_generation
                        && input.captured_cursor.output_stream == edge.output_stream_id
                        && input.output_schema_hash == edge.output_schema_hash
                })
            })
    }

    pub(crate) fn follows(&self, previous: &Self) -> bool {
        self.schema_version == previous.schema_version
            && self.bootstrap_generation == previous.bootstrap_generation
            && self.plan_hash == previous.plan_hash
            && self.inputs.len() == previous.inputs.len()
            && previous.inputs.iter().all(|old| {
                self.inputs.iter().any(|new| {
                    new.input_edge == old.input_edge
                        && new.captured_cursor == old.captured_cursor
                        && new.output_manifest_ref == old.output_manifest_ref
                        && new.output_manifest_hash == old.output_manifest_hash
                        && new.output_schema_hash == old.output_schema_hash
                        && new.total_pages == old.total_pages
                        && new.total_rows == old.total_rows
                        && new.next_page_index >= old.next_page_index
                        && new.applied_rows >= old.applied_rows
                        && (new.next_page_index != old.next_page_index
                            || new.applied_rows == old.applied_rows)
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        standing_runtime_checkpoint_pointer_from_proto,
        standing_runtime_checkpoint_pointer_to_proto, StandingRuntimeCheckpointPointer,
    };

    fn progress() -> PublishedInputBootstrapProgressV1 {
        PublishedInputBootstrapProgressV1 {
            schema_version: 1,
            bootstrap_generation: 1,
            plan_hash: "plan".into(),
            inputs: vec![PublishedInputBootstrapCursorV1 {
                input_edge: "edge".into(),
                captured_cursor: CausalViewCursorV1 {
                    input_edge: "edge".into(),
                    producer_tenant_id: "tenant".into(),
                    producer_program_id: "producer".into(),
                    producer_view_id: "producer".into(),
                    producer_generation: 1,
                    output_stream: "rows/v1".into(),
                    output_epoch: 7,
                    commit_digest: format!("sha256:{}", "a".repeat(64)),
                },
                output_manifest_ref: "immutable-manifest".into(),
                output_manifest_hash: format!("sha256:{}", "b".repeat(64)),
                output_schema_hash: format!("sha256:{}", "c".repeat(64)),
                total_pages: 2,
                total_rows: 5,
                next_page_index: 1,
                applied_rows: 3,
            }],
        }
    }

    #[test]
    fn progress_rejects_ambiguous_or_impossible_completion() {
        let valid = progress();
        valid.validate().unwrap();
        assert!(!valid.is_complete());
        for mutation in 0..7 {
            let mut bad = valid.clone();
            match mutation {
                0 => bad.schema_version = 2,
                1 => bad.inputs.push(bad.inputs[0].clone()),
                2 => bad.inputs[0].next_page_index = 3,
                3 => bad.inputs[0].next_page_index = 2,
                4 => bad.inputs[0].applied_rows = 6,
                5 => bad.inputs[0].input_edge = "other".into(),
                _ => bad.inputs[0].output_manifest_hash = "not-a-digest".into(),
            }
            assert!(bad.validate().is_err(), "mutation {mutation}");
        }
        let mut complete = valid.clone();
        complete.inputs[0].next_page_index = 2;
        complete.inputs[0].applied_rows = 5;
        assert!(complete.is_complete());
        assert!(complete.follows(&valid));
        assert!(!valid.follows(&complete));
    }

    #[test]
    fn empty_snapshots_finish_only_after_all_manifest_pages() {
        let mut empty = progress();
        empty.inputs[0].total_rows = 0;
        empty.inputs[0].applied_rows = 0;
        empty.inputs[0].next_page_index = 0;
        empty.validate().unwrap();
        assert!(!empty.is_complete());
        empty.inputs[0].next_page_index = 2;
        assert!(empty.is_complete());
        empty.inputs[0].total_pages = 0;
        empty.inputs[0].next_page_index = 0;
        assert!(empty.is_complete());
        empty.inputs[0].total_rows = 1;
        assert!(empty.validate().is_err());
    }

    #[test]
    fn pointer_progress_round_trips_json_and_proto_without_losing_legacy_none() {
        let pointer = StandingRuntimeCheckpointPointer {
            tenant_id: "tenant".into(),
            program_id: "consumer".into(),
            view_id: "consumer".into(),
            checkpoint_key: "key".into(),
            logical_epoch: 1,
            content_hash: "hash".into(),
            manifest_hash: "manifest".into(),
            output_manifest_refs: vec![],
            bootstrap_generation: 1,
            plan_hash: "plan".into(),
            coverage_hash: "coverage".into(),
            input_coverage: None,
            previous_checkpoint_key: String::new(),
            previous_manifest_hash: String::new(),
            published_input_bootstrap: Some(progress()),
        };
        let encoded = serde_json::to_vec(&pointer).unwrap();
        assert_eq!(
            serde_json::from_slice::<StandingRuntimeCheckpointPointer>(&encoded).unwrap(),
            pointer
        );
        assert_eq!(
            standing_runtime_checkpoint_pointer_from_proto(
                standing_runtime_checkpoint_pointer_to_proto(pointer.clone())
            )
            .unwrap(),
            pointer
        );
        let mut legacy = serde_json::to_value(&pointer).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("published_input_bootstrap");
        let legacy: StandingRuntimeCheckpointPointer = serde_json::from_value(legacy).unwrap();
        assert!(legacy.published_input_bootstrap.is_none());
        assert_eq!(
            standing_runtime_checkpoint_pointer_from_proto(
                standing_runtime_checkpoint_pointer_to_proto(legacy.clone())
            )
            .unwrap(),
            legacy
        );
    }
}

use std::{collections::HashSet, fmt};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::object_key::ObjectKey;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub schema_version: u16,
    pub checkpoint_version: u64,
    pub input_ranges: Vec<InputRange>,
    pub state_objects: Vec<StateObjectRef>,
    pub output_objects: Vec<OutputObjectRef>,
    pub parent_checkpoint: Option<u64>,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionOwnerClaim {
    pub owner_id: String,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRange {
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateObjectRef {
    pub object_id: String,
    pub object_key: ObjectKey,
    pub owner: String,
    pub partition_id: u32,
    pub checkpoint_version: u64,
    #[serde(default = "legacy_raw_state_ref_type")]
    pub ref_type: StateRefType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_claim: Option<PartitionOwnerClaim>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateRefType {
    LegacyRawObject,
    RawObject,
    SlateDbCheckpoint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputObjectRef {
    pub object_id: String,
    pub object_key: ObjectKey,
    pub stream_id: String,
    pub partition_id: u32,
    #[serde(default = "legacy_missing_output_checkpoint_version")]
    pub checkpoint_version: u64,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_claim: Option<PartitionOwnerClaim>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ManifestError {
    #[error("unsupported manifest schema version {0}")]
    UnsupportedSchemaVersion(u16),
    #[error("manifest must include at least one input range")]
    MissingInputProgress,
    #[error("manifest must include at least one state object reference")]
    MissingStateObjects,
    #[error("input range for {stream_id}/p={partition_id} must be nonempty: start={start_offset_inclusive}, end={end_offset_exclusive}")]
    InvalidInputRange {
        stream_id: String,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    },
    #[error("duplicate input range for {stream_id}/p={partition_id}")]
    DuplicateInputRange {
        stream_id: String,
        partition_id: u32,
    },
    #[error("manifest input ranges must be ordered by stream and partition")]
    InputRangesNotSorted,
    #[error("invalid parent checkpoint {parent_checkpoint} for checkpoint {checkpoint_version}")]
    InvalidParentCheckpoint {
        parent_checkpoint: u64,
        checkpoint_version: u64,
    },
    #[error("non-genesis checkpoint {0} must declare its parent checkpoint")]
    MissingParentCheckpoint(u64),
    #[error("genesis checkpoint must not declare a parent checkpoint")]
    UnexpectedGenesisParent,
    #[error("state object `{object_id}` belongs to checkpoint {state_checkpoint_version}, expected {manifest_checkpoint_version}")]
    StateObjectCheckpointMismatch {
        object_id: String,
        state_checkpoint_version: u64,
        manifest_checkpoint_version: u64,
    },
    #[error("state object `{object_id}` key does not match its metadata: expected `{expected}`, actual `{actual}`")]
    StateObjectKeyMismatch {
        object_id: String,
        expected: ObjectKey,
        actual: ObjectKey,
    },
    #[error("state object `{object_id}` must carry an owner claim")]
    MissingOwnerClaim { object_id: String },
    #[error(
        "state object `{object_id}` owner claim mismatch: expected `{expected}`, actual `{actual}`"
    )]
    OwnerClaimMismatch {
        object_id: String,
        expected: PartitionOwnerClaim,
        actual: PartitionOwnerClaim,
    },
    #[error("output object `{object_id}` must carry an owner claim")]
    MissingOutputOwnerClaim { object_id: String },
    #[error(
        "output object `{object_id}` owner claim mismatch: expected `{expected}`, actual `{actual}`"
    )]
    OutputOwnerClaimMismatch {
        object_id: String,
        expected: PartitionOwnerClaim,
        actual: PartitionOwnerClaim,
    },
    #[error("output object range for {object_id} must be nonempty: start={start_offset_inclusive}, end={end_offset_exclusive}")]
    InvalidOutputRange {
        object_id: String,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    },
    #[error("output object `{object_id}` belongs to checkpoint {output_checkpoint_version}, expected {manifest_checkpoint_version}")]
    OutputObjectCheckpointMismatch {
        object_id: String,
        output_checkpoint_version: u64,
        manifest_checkpoint_version: u64,
    },
    #[error("output object `{object_id}` key does not match its metadata: expected `{expected}`, actual `{actual}`")]
    OutputObjectKeyMismatch {
        object_id: String,
        expected: ObjectKey,
        actual: ObjectKey,
    },
    #[error("duplicate object id `{0}`")]
    DuplicateObjectId(String),
    #[error("duplicate object key `{0}`")]
    DuplicateObjectKey(ObjectKey),
    #[error("manifest creation timestamp must be provided by the caller")]
    MissingCreationTimestamp,
}

impl fmt::Display for PartitionOwnerClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.owner_id, self.owner_epoch)
    }
}

impl CheckpointManifest {
    pub fn object_key(&self) -> ObjectKey {
        ObjectKey::checkpoint_manifest(self.checkpoint_version)
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.schema_version != 1 {
            return Err(ManifestError::UnsupportedSchemaVersion(self.schema_version));
        }

        if self.created_at.is_empty() {
            return Err(ManifestError::MissingCreationTimestamp);
        }

        self.validate_parent_checkpoint()?;
        self.validate_input_ranges()?;
        self.validate_state_objects()?;
        self.validate_unique_object_refs()?;
        self.validate_output_objects()?;

        Ok(())
    }

    pub fn validate_owner_claim(
        &self,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), ManifestError> {
        for state_object in &self.state_objects {
            match &state_object.owner_claim {
                Some(actual) if actual == owner_claim => {}
                Some(actual) => {
                    return Err(ManifestError::OwnerClaimMismatch {
                        object_id: state_object.object_id.clone(),
                        expected: owner_claim.clone(),
                        actual: actual.clone(),
                    });
                }
                None => {
                    return Err(ManifestError::MissingOwnerClaim {
                        object_id: state_object.object_id.clone(),
                    });
                }
            }
        }

        for output_object in &self.output_objects {
            match &output_object.owner_claim {
                Some(actual) if actual == owner_claim => {}
                Some(actual) => {
                    return Err(ManifestError::OutputOwnerClaimMismatch {
                        object_id: output_object.object_id.clone(),
                        expected: owner_claim.clone(),
                        actual: actual.clone(),
                    });
                }
                None => {
                    return Err(ManifestError::MissingOutputOwnerClaim {
                        object_id: output_object.object_id.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    fn validate_parent_checkpoint(&self) -> Result<(), ManifestError> {
        match (self.checkpoint_version, self.parent_checkpoint) {
            (0, None) => Ok(()),
            (0, Some(_)) => Err(ManifestError::UnexpectedGenesisParent),
            (checkpoint_version, Some(parent_checkpoint))
                if checkpoint_version
                    .checked_sub(1)
                    .is_some_and(|expected_parent| expected_parent == parent_checkpoint) =>
            {
                Ok(())
            }
            (checkpoint_version, Some(parent_checkpoint)) => {
                Err(ManifestError::InvalidParentCheckpoint {
                    parent_checkpoint,
                    checkpoint_version,
                })
            }
            (checkpoint_version, None) => {
                Err(ManifestError::MissingParentCheckpoint(checkpoint_version))
            }
        }
    }

    fn validate_input_ranges(&self) -> Result<(), ManifestError> {
        if self.input_ranges.is_empty() {
            return Err(ManifestError::MissingInputProgress);
        }

        let mut seen = HashSet::new();
        let mut previous: Option<(&str, u32)> = None;

        for range in &self.input_ranges {
            if range.start_offset_inclusive >= range.end_offset_exclusive {
                return Err(ManifestError::InvalidInputRange {
                    stream_id: range.stream_id.clone(),
                    partition_id: range.partition_id,
                    start_offset_inclusive: range.start_offset_inclusive,
                    end_offset_exclusive: range.end_offset_exclusive,
                });
            }

            let current = (range.stream_id.as_str(), range.partition_id);
            if let Some(previous) = previous {
                if previous > current {
                    return Err(ManifestError::InputRangesNotSorted);
                }
            }
            previous = Some(current);

            if !seen.insert((range.stream_id.as_str(), range.partition_id)) {
                return Err(ManifestError::DuplicateInputRange {
                    stream_id: range.stream_id.clone(),
                    partition_id: range.partition_id,
                });
            }
        }

        Ok(())
    }

    fn validate_state_objects(&self) -> Result<(), ManifestError> {
        if self.state_objects.is_empty() {
            return Err(ManifestError::MissingStateObjects);
        }

        for state_object in &self.state_objects {
            if state_object.checkpoint_version != self.checkpoint_version {
                return Err(ManifestError::StateObjectCheckpointMismatch {
                    object_id: state_object.object_id.clone(),
                    state_checkpoint_version: state_object.checkpoint_version,
                    manifest_checkpoint_version: self.checkpoint_version,
                });
            }

            let expected = ObjectKey::state_object(
                &state_object.owner,
                state_object.partition_id,
                state_object.checkpoint_version,
                &state_object.object_id,
            )
            .map_err(|_| ManifestError::StateObjectKeyMismatch {
                object_id: state_object.object_id.clone(),
                expected: state_object.object_key.clone(),
                actual: state_object.object_key.clone(),
            })?;

            if state_object.object_key != expected {
                return Err(ManifestError::StateObjectKeyMismatch {
                    object_id: state_object.object_id.clone(),
                    expected,
                    actual: state_object.object_key.clone(),
                });
            }
        }

        Ok(())
    }

    fn validate_output_objects(&self) -> Result<(), ManifestError> {
        for output_object in &self.output_objects {
            if output_object.start_offset_inclusive >= output_object.end_offset_exclusive {
                return Err(ManifestError::InvalidOutputRange {
                    object_id: output_object.object_id.clone(),
                    start_offset_inclusive: output_object.start_offset_inclusive,
                    end_offset_exclusive: output_object.end_offset_exclusive,
                });
            }

            let expected = ObjectKey::output_object(
                &output_object.stream_id,
                output_object.partition_id,
                output_object.checkpoint_version,
                output_object.start_offset_inclusive,
                output_object.end_offset_exclusive,
                &output_object.object_id,
            )
            .map_err(|_| ManifestError::OutputObjectKeyMismatch {
                object_id: output_object.object_id.clone(),
                expected: output_object.object_key.clone(),
                actual: output_object.object_key.clone(),
            })?;

            if output_object.object_key != expected {
                return Err(ManifestError::OutputObjectKeyMismatch {
                    object_id: output_object.object_id.clone(),
                    expected,
                    actual: output_object.object_key.clone(),
                });
            }

            if output_object.checkpoint_version != self.checkpoint_version {
                return Err(ManifestError::OutputObjectCheckpointMismatch {
                    object_id: output_object.object_id.clone(),
                    output_checkpoint_version: output_object.checkpoint_version,
                    manifest_checkpoint_version: self.checkpoint_version,
                });
            }
        }

        Ok(())
    }

    fn validate_unique_object_refs(&self) -> Result<(), ManifestError> {
        let mut object_ids = HashSet::new();
        let mut object_keys = HashSet::new();

        for object_ref in self
            .state_objects
            .iter()
            .map(ObjectRef::State)
            .chain(self.output_objects.iter().map(ObjectRef::Output))
        {
            if !object_ids.insert(object_ref.object_id()) {
                return Err(ManifestError::DuplicateObjectId(
                    object_ref.object_id().to_string(),
                ));
            }

            if !object_keys.insert(object_ref.object_key()) {
                return Err(ManifestError::DuplicateObjectKey(
                    object_ref.object_key().clone(),
                ));
            }
        }

        Ok(())
    }
}

fn legacy_missing_output_checkpoint_version() -> u64 {
    u64::MAX
}

fn legacy_raw_state_ref_type() -> StateRefType {
    StateRefType::LegacyRawObject
}

enum ObjectRef<'a> {
    State(&'a StateObjectRef),
    Output(&'a OutputObjectRef),
}

impl<'a> ObjectRef<'a> {
    fn object_id(&self) -> &'a str {
        match self {
            Self::State(object_ref) => &object_ref.object_id,
            Self::Output(object_ref) => &object_ref.object_id,
        }
    }

    fn object_key(&self) -> &'a ObjectKey {
        match self {
            Self::State(object_ref) => &object_ref.object_key,
            Self::Output(object_ref) => &object_ref.object_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CheckpointManifest, InputRange, ManifestError, OutputObjectRef, PartitionOwnerClaim,
        StateObjectRef, StateRefType,
    };
    use crate::object_key::ObjectKey;

    fn state_ref(object_id: &str, object_key: ObjectKey) -> StateObjectRef {
        StateObjectRef {
            object_id: object_id.to_string(),
            object_key,
            owner: "balances_by_account".to_string(),
            partition_id: 0,
            checkpoint_version: 1,
            ref_type: StateRefType::LegacyRawObject,
            owner_claim: None,
        }
    }

    fn output_ref(object_id: &str, object_key: ObjectKey) -> OutputObjectRef {
        OutputObjectRef {
            object_id: object_id.to_string(),
            object_key,
            stream_id: "settlements".to_string(),
            partition_id: 0,
            checkpoint_version: 1,
            start_offset_inclusive: 20,
            end_offset_exclusive: 25,
            owner_claim: None,
        }
    }

    fn valid_manifest() -> CheckpointManifest {
        CheckpointManifest {
            schema_version: 1,
            checkpoint_version: 1,
            input_ranges: vec![InputRange {
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 10,
                end_offset_exclusive: 20,
            }],
            state_objects: vec![state_ref(
                "state-0001",
                ObjectKey::state_object("balances_by_account", 0, 1, "state-0001").unwrap(),
            )],
            output_objects: vec![output_ref(
                "out-0001",
                ObjectKey::output_object("settlements", 0, 1, 20, 25, "out-0001").unwrap(),
            )],
            parent_checkpoint: Some(0),
            created_at: "2026-05-03T00:00:00Z".to_string(),
        }
    }

    fn owner_claim(owner_id: &str, owner_epoch: u64) -> PartitionOwnerClaim {
        PartitionOwnerClaim {
            owner_id: owner_id.to_string(),
            owner_epoch,
        }
    }

    #[test]
    fn checkpoint_manifest_accepts_valid_structural_contract() {
        let manifest = valid_manifest();

        assert_eq!(manifest.validate(), Ok(()));
    }

    #[test]
    fn checkpoint_manifest_rejects_missing_input_progress() {
        let mut manifest = valid_manifest();
        manifest.input_ranges.clear();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::MissingInputProgress)
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_missing_state_object_references() {
        let mut manifest = valid_manifest();
        manifest.state_objects.clear();

        assert_eq!(manifest.validate(), Err(ManifestError::MissingStateObjects));
    }

    #[test]
    fn checkpoint_manifest_rejects_state_ref_object_key_mismatch() {
        let mut manifest = valid_manifest();
        manifest.state_objects[0].object_key =
            ObjectKey::state_object("other_owner", 0, 1, "state-0001").unwrap();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::StateObjectKeyMismatch {
                object_id: "state-0001".to_string(),
                expected: ObjectKey::state_object("balances_by_account", 0, 1, "state-0001")
                    .unwrap(),
                actual: ObjectKey::state_object("other_owner", 0, 1, "state-0001").unwrap(),
            })
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_output_ref_object_key_mismatch() {
        let mut manifest = valid_manifest();
        manifest.output_objects[0].object_key =
            ObjectKey::output_object("settlements", 1, 1, 20, 25, "out-0001").unwrap();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::OutputObjectKeyMismatch {
                object_id: "out-0001".to_string(),
                expected: ObjectKey::output_object("settlements", 0, 1, 20, 25, "out-0001")
                    .unwrap(),
                actual: ObjectKey::output_object("settlements", 1, 1, 20, 25, "out-0001").unwrap(),
            })
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_output_ref_in_ingest_namespace() {
        let mut manifest = valid_manifest();
        manifest.output_objects[0].object_key =
            ObjectKey::ingest_batch("settlements", 0, 20, 25).unwrap();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::OutputObjectKeyMismatch {
                object_id: "out-0001".to_string(),
                expected: ObjectKey::output_object("settlements", 0, 1, 20, 25, "out-0001")
                    .unwrap(),
                actual: ObjectKey::ingest_batch("settlements", 0, 20, 25).unwrap(),
            })
        );
    }

    #[test]
    fn checkpoint_manifest_deserializes_old_output_ref_without_checkpoint_version_then_rejects_ingest_key(
    ) {
        let value = serde_json::json!({
            "schema_version": 1,
            "checkpoint_version": 1,
            "input_ranges": [{
                "stream_id": "orders",
                "partition_id": 0,
                "start_offset_inclusive": 10,
                "end_offset_exclusive": 20
            }],
            "state_objects": [{
                "object_id": "state-0001",
                "object_key": "v1/state/balances_by_account/p=0000000000/chk=00000000000000000001/state-0001.state",
                "owner": "balances_by_account",
                "partition_id": 0,
                "checkpoint_version": 1
            }],
            "output_objects": [{
                "object_id": "out-0001",
                "object_key": "v1/ingest/settlements/p=0000000000/00000000000000000020-00000000000000000025.batch",
                "stream_id": "settlements",
                "partition_id": 0,
                "start_offset_inclusive": 20,
                "end_offset_exclusive": 25
            }],
            "parent_checkpoint": 0,
            "created_at": "2026-05-03T00:00:00Z"
        });

        let manifest: CheckpointManifest = serde_json::from_value(value).unwrap();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::OutputObjectKeyMismatch {
                object_id: "out-0001".to_string(),
                expected: ObjectKey::output_object(
                    "settlements",
                    0,
                    manifest.output_objects[0].checkpoint_version,
                    20,
                    25,
                    "out-0001"
                )
                .unwrap(),
                actual: ObjectKey::ingest_batch("settlements", 0, 20, 25).unwrap(),
            })
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_output_ref_from_different_checkpoint() {
        let mut manifest = valid_manifest();
        manifest.output_objects[0].checkpoint_version = 2;
        manifest.output_objects[0].object_key =
            ObjectKey::output_object("settlements", 0, 2, 20, 25, "out-0001").unwrap();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::OutputObjectCheckpointMismatch {
                object_id: "out-0001".to_string(),
                output_checkpoint_version: 2,
                manifest_checkpoint_version: 1,
            })
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_duplicate_object_identifiers_across_refs() {
        let mut manifest = valid_manifest();
        manifest.output_objects[0].object_id = "state-0001".to_string();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::DuplicateObjectId("state-0001".to_string()))
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_duplicate_object_keys_across_refs() {
        let mut manifest = valid_manifest();
        manifest.output_objects[0].object_key = manifest.state_objects[0].object_key.clone();

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::DuplicateObjectKey(
                manifest.state_objects[0].object_key.clone()
            ))
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_non_monotonic_checkpoint_versions() {
        let mut manifest = valid_manifest();
        manifest.parent_checkpoint = Some(1);

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::InvalidParentCheckpoint {
                parent_checkpoint: 1,
                checkpoint_version: 1,
            })
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_overflowing_parent_checkpoint_without_panicking() {
        let mut manifest = valid_manifest();
        manifest.parent_checkpoint = Some(u64::MAX);

        assert_eq!(
            manifest.validate(),
            Err(ManifestError::InvalidParentCheckpoint {
                parent_checkpoint: u64::MAX,
                checkpoint_version: 1,
            })
        );
    }

    #[test]
    fn checkpoint_manifest_rejects_state_ref_with_mismatched_owner_claim() {
        let expected_claim = owner_claim("worker-a", 7);
        let mut manifest = valid_manifest();
        manifest.state_objects[0].owner_claim = Some(owner_claim("worker-b", 7));

        assert_eq!(
            manifest.validate_owner_claim(&expected_claim),
            Err(ManifestError::OwnerClaimMismatch {
                object_id: "state-0001".to_string(),
                expected: expected_claim,
                actual: owner_claim("worker-b", 7),
            })
        );
    }
}

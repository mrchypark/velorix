use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

const PARTITION_WIDTH: usize = 6;
const CHECKPOINT_WIDTH: usize = 20;
const OFFSET_WIDTH: usize = 20;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectKey(String);

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ObjectKeyError {
    #[error("object key segment `{0}` is empty")]
    EmptySegment(&'static str),
    #[error("object key segment `{name}` contains path-unsafe value `{value}`")]
    UnsafeSegment { name: &'static str, value: String },
    #[error(
        "offset range must be nonempty: start={start_offset_inclusive}, end={end_offset_exclusive}"
    )]
    InvalidOffsetRange {
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    },
    #[error("object key must use the v1 namespace and have no leading slash: {0}")]
    InvalidExternalKey(String),
}

impl ObjectKey {
    pub fn ingest_batch(
        stream_id: &str,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("stream_id", stream_id)?;
        validate_offset_range(start_offset_inclusive, end_offset_exclusive)?;

        Ok(Self(format!(
            "v1/ingest/{stream_id}/p={partition_id:0PARTITION_WIDTH$}/{start_offset_inclusive:0OFFSET_WIDTH$}-{end_offset_exclusive:0OFFSET_WIDTH$}.batch"
        )))
    }

    pub fn state_object(
        owner: &str,
        partition_id: u32,
        checkpoint_version: u64,
        object_id: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("owner", owner)?;
        validate_segment("object_id", object_id)?;

        Ok(Self(format!(
            "v1/state/{owner}/p={partition_id:0PARTITION_WIDTH$}/chk={checkpoint_version:0CHECKPOINT_WIDTH$}/{object_id}.state"
        )))
    }

    pub fn temp_publish(
        checkpoint_version: u64,
        attempt_or_object_id: &str,
        kind: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("attempt_or_object_id", attempt_or_object_id)?;
        validate_segment("kind", kind)?;

        Ok(Self(format!(
            "v1/tmp/{checkpoint_version:0CHECKPOINT_WIDTH$}/{attempt_or_object_id}/{kind}"
        )))
    }

    pub fn checkpoint_manifest(checkpoint_version: u64) -> Self {
        Self(format!(
            "v1/checkpoints/{checkpoint_version:0CHECKPOINT_WIDTH$}.manifest"
        ))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, ObjectKeyError> {
        let value = value.into();
        if value.starts_with('/')
            || !value.starts_with("v1/")
            || value.split('/').any(str::is_empty)
        {
            return Err(ObjectKeyError::InvalidExternalKey(value));
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn validate_offset_range(
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> Result<(), ObjectKeyError> {
    if start_offset_inclusive >= end_offset_exclusive {
        return Err(ObjectKeyError::InvalidOffsetRange {
            start_offset_inclusive,
            end_offset_exclusive,
        });
    }

    Ok(())
}

fn validate_segment(name: &'static str, value: &str) -> Result<(), ObjectKeyError> {
    if value.is_empty() {
        return Err(ObjectKeyError::EmptySegment(name));
    }

    if value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ObjectKeyError::UnsafeSegment {
            name,
            value: value.to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ObjectKey;

    #[test]
    fn ingest_batch_key_is_deterministic_and_lexicographically_ordered() {
        let key = ObjectKey::ingest_batch("orders", 7, 42, 100).unwrap();
        let restarted = ObjectKey::ingest_batch("orders", 7, 42, 100).unwrap();

        assert_eq!(
            key.as_str(),
            "v1/ingest/orders/p=000007/00000000000000000042-00000000000000000100.batch"
        );
        assert_eq!(key, restarted);
        assert_eq!(key.to_string(), key.as_str());
    }

    #[test]
    fn state_object_key_is_deterministic_and_names_checkpoint_context() {
        let key = ObjectKey::state_object("balances_by_account", 12, 9, "state-0001").unwrap();
        let restarted =
            ObjectKey::state_object("balances_by_account", 12, 9, "state-0001").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/state/balances_by_account/p=000012/chk=00000000000000000009/state-0001.state"
        );
        assert_eq!(key, restarted);
    }

    #[test]
    fn temp_publish_key_uses_caller_supplied_attempt_id() {
        let key = ObjectKey::temp_publish(9, "attempt-abc", "manifest").unwrap();
        let restarted = ObjectKey::temp_publish(9, "attempt-abc", "manifest").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/tmp/00000000000000000009/attempt-abc/manifest"
        );
        assert_eq!(key, restarted);
    }

    #[test]
    fn checkpoint_manifest_key_is_deterministic_and_version_ordered() {
        let key = ObjectKey::checkpoint_manifest(9);
        let restarted = ObjectKey::checkpoint_manifest(9);

        assert_eq!(key.as_str(), "v1/checkpoints/00000000000000000009.manifest");
        assert_eq!(key, restarted);
    }
}

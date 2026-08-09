use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::object_key::{ObjectKey, ObjectKeyError};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipEpochRecord {
    pub stream_id: String,
    pub partition_id: u32,
    pub owner_id: String,
    pub owner_epoch: u64,
    pub lease_identity: String,
    pub created_at: String,
    pub previous_epoch: Option<u64>,
    pub previous_checkpoint_version: Option<u64>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OwnershipEpochRecordError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error("ownership epoch record owner id must be provided")]
    MissingOwnerId,
    #[error("ownership epoch record lease identity must be provided")]
    MissingLeaseIdentity,
    #[error("ownership epoch record creation timestamp must be provided")]
    MissingCreationTimestamp,
    #[error(
        "ownership epoch record key `{object_key}` does not match record body key `{body_key}`"
    )]
    ObjectKeyMismatch {
        object_key: ObjectKey,
        body_key: ObjectKey,
    },
    #[error("ownership epoch record `{object_key}` owner mismatch: expected {expected_owner_id}@{expected_owner_epoch}, actual {actual_owner_id}@{actual_owner_epoch}")]
    OwnerClaimMismatch {
        object_key: ObjectKey,
        expected_owner_id: String,
        expected_owner_epoch: u64,
        actual_owner_id: String,
        actual_owner_epoch: u64,
    },
}

impl OwnershipEpochRecord {
    pub fn object_key(&self) -> Result<ObjectKey, ObjectKeyError> {
        ObjectKey::ownership_epoch_record(&self.stream_id, self.partition_id, self.owner_epoch)
    }

    pub fn validate(&self) -> Result<(), OwnershipEpochRecordError> {
        self.object_key()?;

        if self.owner_id.is_empty() {
            return Err(OwnershipEpochRecordError::MissingOwnerId);
        }

        if self.lease_identity.is_empty() {
            return Err(OwnershipEpochRecordError::MissingLeaseIdentity);
        }

        if self.created_at.is_empty() {
            return Err(OwnershipEpochRecordError::MissingCreationTimestamp);
        }

        Ok(())
    }

    pub fn validate_object_key(
        &self,
        object_key: &ObjectKey,
    ) -> Result<(), OwnershipEpochRecordError> {
        self.validate()?;
        let body_key = self.object_key()?;
        if &body_key != object_key {
            return Err(OwnershipEpochRecordError::ObjectKeyMismatch {
                object_key: object_key.clone(),
                body_key,
            });
        }

        Ok(())
    }

    pub fn validate_owner_claim(
        &self,
        object_key: &ObjectKey,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), OwnershipEpochRecordError> {
        self.validate_object_key(object_key)?;

        if self.owner_id != owner_id || self.owner_epoch != owner_epoch {
            return Err(OwnershipEpochRecordError::OwnerClaimMismatch {
                object_key: object_key.clone(),
                expected_owner_id: owner_id.to_string(),
                expected_owner_epoch: owner_epoch,
                actual_owner_id: self.owner_id.clone(),
                actual_owner_epoch: self.owner_epoch,
            });
        }

        Ok(())
    }
}

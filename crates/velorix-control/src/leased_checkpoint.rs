use std::sync::Arc;

use crate::lease::PartitionLeaseGrant;
use object_store::ObjectStore;
use thiserror::Error;
use velorix_storage::{
    capability::{AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1},
    manifest::{
        CheckpointManifest, ManifestError, OutputObjectRef, PartitionOwnerClaim, StateObjectRef,
        StateRefType,
    },
    object_key::ObjectKey,
    state::{CheckpointPublishError, CheckpointPublisher, OutputObjectWrite, StateObjectWrite},
};

#[derive(Clone, Debug)]
pub struct LeasedCheckpointPublisher {
    publisher: CheckpointPublisher,
    production_authority_validated: bool,
}

#[derive(Debug, Error)]
pub enum LeasedCheckpointError {
    #[error("partition lease grant expired at {expires_at_unix_ms}, now is {now_unix_ms}")]
    ExpiredGrant {
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("checkpoint manifest spans multiple partitions: {partitions:?}")]
    MultiPartitionManifest { partitions: Vec<u32> },
    #[error(
        "checkpoint {kind} `{object_id}` partition mismatch: expected {expected}, actual {actual}"
    )]
    PartitionMismatch {
        kind: &'static str,
        object_id: String,
        expected: u32,
        actual: u32,
    },
    #[error("checkpoint input stream mismatch: expected `{expected}`, actual `{actual}`")]
    InputStreamMismatch { expected: String, actual: String },
    #[error("state object `{object_key}` owner claim mismatch: expected `{expected}`, actual `{actual:?}`")]
    StateOwnerClaimMismatch {
        object_key: ObjectKey,
        expected: PartitionOwnerClaim,
        actual: Option<PartitionOwnerClaim>,
    },
    #[error("output object `{object_key}` owner claim mismatch: expected `{expected}`, actual `{actual:?}`")]
    OutputOwnerClaimMismatch {
        object_key: ObjectKey,
        expected: PartitionOwnerClaim,
        actual: Option<PartitionOwnerClaim>,
    },
    #[error("manifest state refs do not match requested state writes")]
    StateRefsMismatch {
        expected: Vec<StateObjectRef>,
        actual: Vec<StateObjectRef>,
    },
    #[error("manifest output refs do not match requested output writes")]
    OutputRefsMismatch {
        expected: Vec<OutputObjectRef>,
        actual: Vec<OutputObjectRef>,
    },
    #[error("production state store produces `{actual:?}` state refs, expected `{expected:?}`")]
    ProductionStateStoreRefTypeMismatch {
        expected: StateRefType,
        actual: StateRefType,
    },
    #[error(
        "production leased checkpoint publisher requires shared startup object-store capability evidence"
    )]
    MissingProductionAuthorityEvidence,
    #[error(transparent)]
    Publish(#[from] CheckpointPublishError),
}

impl LeasedCheckpointPublisher {
    pub fn new(publisher: CheckpointPublisher) -> Self {
        Self {
            publisher,
            production_authority_validated: false,
        }
    }

    /// Constructs the production leased checkpoint publisher from shared
    /// startup capability evidence instead of accepting an unchecked
    /// checkpoint publisher.
    pub async fn with_slatedb_state_store_authoritative(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<object_store::path::Path>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, LeasedCheckpointError> {
        capabilities
            .validate_namespace(AuthoritativeNamespace::Output)
            .map_err(CheckpointPublishError::from)?;
        capabilities
            .validate_namespace(AuthoritativeNamespace::Ownership)
            .map_err(CheckpointPublishError::from)?;
        let publisher = CheckpointPublisher::with_slatedb_state_store_authoritative(
            store,
            db_path,
            capabilities,
        )
        .await?;

        Ok(Self {
            publisher,
            production_authority_validated: true,
        })
    }

    pub async fn publish(
        &self,
        grant: PartitionLeaseGrant,
        now_unix_ms: u64,
        state_objects: Vec<StateObjectWrite>,
        output_objects: Vec<OutputObjectWrite>,
        manifest: CheckpointManifest,
    ) -> Result<(), LeasedCheckpointError> {
        validate_grant_unexpired(&grant, now_unix_ms)?;
        manifest.validate()?;

        let owner_claim = PartitionOwnerClaim::from(grant.clone());
        validate_single_grant_partition(&grant, &manifest)?;
        validate_input_stream(&grant, &manifest)?;
        validate_state_writes(&grant, &owner_claim, &state_objects)?;
        validate_output_writes(&grant, &owner_claim, &output_objects)?;
        validate_manifest_refs_match_writes(&manifest, &state_objects, &output_objects)?;
        self.publisher
            .preflight_manifest_publication(&manifest)
            .await?;

        for state in &state_objects {
            self.publisher
                .write_state_object_fenced(state, &owner_claim)
                .await?;
        }

        for output in &output_objects {
            self.publisher
                .write_output_object_fenced(output, &owner_claim)
                .await?;
        }

        self.publisher
            .publish_manifest_fenced(&manifest, &owner_claim)
            .await?;

        Ok(())
    }

    pub async fn publish_production(
        &self,
        grant: PartitionLeaseGrant,
        now_unix_ms: u64,
        state_objects: Vec<StateObjectWrite>,
        output_objects: Vec<OutputObjectWrite>,
        manifest: CheckpointManifest,
    ) -> Result<(), LeasedCheckpointError> {
        if !self.production_authority_validated {
            return Err(LeasedCheckpointError::MissingProductionAuthorityEvidence);
        }

        validate_grant_unexpired(&grant, now_unix_ms)?;
        manifest.validate()?;

        let owner_claim = PartitionOwnerClaim::from(grant.clone());
        validate_single_grant_partition(&grant, &manifest)?;
        validate_input_stream(&grant, &manifest)?;
        validate_state_writes(&grant, &owner_claim, &state_objects)?;
        validate_output_writes(&grant, &owner_claim, &output_objects)?;
        validate_production_state_store_ref_type(self.publisher.produced_state_ref_type())?;
        validate_manifest_refs_match_writes_with_state_ref_type(
            &manifest,
            &state_objects,
            &output_objects,
            StateRefType::SlateDbCheckpoint,
        )?;
        self.publisher
            .preflight_manifest_publication(&manifest)
            .await?;

        let mut produced_state_refs = Vec::with_capacity(state_objects.len());
        for state in &state_objects {
            let state_ref = self
                .publisher
                .write_state_object_fenced_production(state, &grant.key.stream_id, &owner_claim)
                .await?;
            produced_state_refs.push(state_ref);
        }

        for output in &output_objects {
            self.publisher
                .write_output_object_fenced_production(output, &owner_claim)
                .await?;
        }

        let mut published_manifest = manifest;
        published_manifest.state_objects = produced_state_refs;
        self.publisher
            .publish_manifest_fenced_production(&published_manifest, &owner_claim)
            .await?;

        Ok(())
    }
}

fn validate_grant_unexpired(
    grant: &PartitionLeaseGrant,
    now_unix_ms: u64,
) -> Result<(), LeasedCheckpointError> {
    if grant.expires_at_unix_ms <= now_unix_ms {
        Err(LeasedCheckpointError::ExpiredGrant {
            expires_at_unix_ms: grant.expires_at_unix_ms,
            now_unix_ms,
        })
    } else {
        Ok(())
    }
}

fn validate_single_grant_partition(
    grant: &PartitionLeaseGrant,
    manifest: &CheckpointManifest,
) -> Result<(), LeasedCheckpointError> {
    let mut partitions = manifest
        .input_ranges
        .iter()
        .map(|range| range.partition_id)
        .chain(
            manifest
                .state_objects
                .iter()
                .map(|state_ref| state_ref.partition_id),
        )
        .chain(
            manifest
                .output_objects
                .iter()
                .map(|output_ref| output_ref.partition_id),
        )
        .collect::<Vec<_>>();
    partitions.sort_unstable();
    partitions.dedup();

    if partitions.len() > 1 {
        return Err(LeasedCheckpointError::MultiPartitionManifest { partitions });
    }

    if let Some(actual) = partitions.first().copied() {
        if actual != grant.key.partition_id {
            return Err(LeasedCheckpointError::PartitionMismatch {
                kind: "manifest",
                object_id: manifest.checkpoint_version.to_string(),
                expected: grant.key.partition_id,
                actual,
            });
        }
    }

    Ok(())
}

fn validate_input_stream(
    grant: &PartitionLeaseGrant,
    manifest: &CheckpointManifest,
) -> Result<(), LeasedCheckpointError> {
    for input_range in &manifest.input_ranges {
        if input_range.stream_id != grant.key.stream_id {
            return Err(LeasedCheckpointError::InputStreamMismatch {
                expected: grant.key.stream_id.clone(),
                actual: input_range.stream_id.clone(),
            });
        }
    }

    Ok(())
}

fn validate_state_writes(
    grant: &PartitionLeaseGrant,
    owner_claim: &PartitionOwnerClaim,
    state_objects: &[StateObjectWrite],
) -> Result<(), LeasedCheckpointError> {
    for state in state_objects {
        if state.partition_id() != grant.key.partition_id {
            return Err(LeasedCheckpointError::PartitionMismatch {
                kind: "state",
                object_id: state.object_id().to_string(),
                expected: grant.key.partition_id,
                actual: state.partition_id(),
            });
        }

        if state.owner_claim() != Some(owner_claim) {
            return Err(LeasedCheckpointError::StateOwnerClaimMismatch {
                object_key: state.object_key().clone(),
                expected: owner_claim.clone(),
                actual: state.owner_claim().cloned(),
            });
        }
    }

    Ok(())
}

fn validate_output_writes(
    grant: &PartitionLeaseGrant,
    owner_claim: &PartitionOwnerClaim,
    output_objects: &[OutputObjectWrite],
) -> Result<(), LeasedCheckpointError> {
    for output in output_objects {
        if output.partition_id() != grant.key.partition_id {
            return Err(LeasedCheckpointError::PartitionMismatch {
                kind: "output",
                object_id: output.object_id().to_string(),
                expected: grant.key.partition_id,
                actual: output.partition_id(),
            });
        }

        if output.owner_claim() != Some(owner_claim) {
            return Err(LeasedCheckpointError::OutputOwnerClaimMismatch {
                object_key: output.object_key().clone(),
                expected: owner_claim.clone(),
                actual: output.owner_claim().cloned(),
            });
        }
    }

    Ok(())
}

fn validate_manifest_refs_match_writes(
    manifest: &CheckpointManifest,
    state_objects: &[StateObjectWrite],
    output_objects: &[OutputObjectWrite],
) -> Result<(), LeasedCheckpointError> {
    validate_manifest_refs_match_writes_with_state_ref_type(
        manifest,
        state_objects,
        output_objects,
        StateRefType::RawObject,
    )
}

fn validate_manifest_refs_match_writes_with_state_ref_type(
    manifest: &CheckpointManifest,
    state_objects: &[StateObjectWrite],
    output_objects: &[OutputObjectWrite],
    state_ref_type: StateRefType,
) -> Result<(), LeasedCheckpointError> {
    let expected_state_refs = state_objects
        .iter()
        .map(|state| state_object_ref(state, state_ref_type))
        .collect::<Vec<_>>();
    if manifest.state_objects != expected_state_refs {
        return Err(LeasedCheckpointError::StateRefsMismatch {
            expected: expected_state_refs,
            actual: manifest.state_objects.clone(),
        });
    }

    let expected_output_refs = output_objects
        .iter()
        .map(output_object_ref)
        .collect::<Vec<_>>();
    if manifest.output_objects != expected_output_refs {
        return Err(LeasedCheckpointError::OutputRefsMismatch {
            expected: expected_output_refs,
            actual: manifest.output_objects.clone(),
        });
    }

    Ok(())
}

fn validate_production_state_store_ref_type(
    actual: StateRefType,
) -> Result<(), LeasedCheckpointError> {
    let expected = StateRefType::SlateDbCheckpoint;
    if actual != expected {
        return Err(LeasedCheckpointError::ProductionStateStoreRefTypeMismatch {
            expected,
            actual,
        });
    }

    Ok(())
}

fn state_object_ref(state: &StateObjectWrite, ref_type: StateRefType) -> StateObjectRef {
    StateObjectRef {
        object_id: state.object_id().to_string(),
        object_key: state.object_key().clone(),
        owner: state.owner().to_string(),
        partition_id: state.partition_id(),
        checkpoint_version: state.checkpoint_version(),
        ref_type,
        slatedb: None,
        owner_claim: state.owner_claim().cloned(),
    }
}

fn output_object_ref(output: &OutputObjectWrite) -> OutputObjectRef {
    OutputObjectRef {
        object_id: output.object_id().to_string(),
        object_key: output.object_key().clone(),
        stream_id: output.stream_id().to_string(),
        partition_id: output.partition_id(),
        checkpoint_version: output.checkpoint_version(),
        start_offset_inclusive: output.start_offset_inclusive(),
        end_offset_exclusive: output.end_offset_exclusive(),
        owner_claim: output.owner_claim().cloned(),
    }
}

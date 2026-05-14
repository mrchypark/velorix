use std::collections::{BTreeMap, BTreeSet};

use crate::crd::{
    CheckpointRef, ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
    VelorixCondition, VelorixStream,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthoritySnapshot {
    authorities: BTreeSet<ObjectStoreAuthorityRef>,
    relation_fingerprints: BTreeMap<RelationEvidenceKey, String>,
    latest_stream_checkpoints: BTreeMap<StreamRelationEvidenceKey, CheckpointRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub action: ControllerAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControllerAction {
    WriteStreamStatus(StreamStatus),
}

impl AuthoritySnapshot {
    pub fn with_authority(mut self, authority: ObjectStoreAuthorityRef) -> Self {
        self.authorities.insert(authority);
        self
    }

    pub fn with_relation(self, relation: &RelationVersionRef) -> Self {
        self.with_relation_for_authority(&ObjectStoreAuthorityRef::default(), relation)
    }

    pub fn with_relation_for_authority(
        mut self,
        authority: &ObjectStoreAuthorityRef,
        relation: &RelationVersionRef,
    ) -> Self {
        self.relation_fingerprints.insert(
            RelationEvidenceKey {
                authority: authority.clone(),
                relation_id: relation.relation_id.clone(),
                relation_version: relation.relation_version,
            },
            relation.schema_fingerprint.clone(),
        );
        self
    }

    pub fn with_latest_stream_checkpoint(
        self,
        stream_id: impl Into<String>,
        relation: &RelationVersionRef,
        checkpoint: CheckpointRef,
    ) -> Self {
        self.with_latest_stream_checkpoint_for_authority(
            &ObjectStoreAuthorityRef::default(),
            stream_id,
            relation,
            checkpoint,
        )
    }

    pub fn with_latest_stream_checkpoint_for_authority(
        mut self,
        authority: &ObjectStoreAuthorityRef,
        stream_id: impl Into<String>,
        relation: &RelationVersionRef,
        checkpoint: CheckpointRef,
    ) -> Self {
        self.latest_stream_checkpoints.insert(
            StreamRelationEvidenceKey {
                authority: authority.clone(),
                stream_id: stream_id.into(),
                relation_id: relation.relation_id.clone(),
                relation_version: relation.relation_version,
                schema_fingerprint: relation.schema_fingerprint.clone(),
            },
            checkpoint,
        );
        self
    }

    fn has_authority(&self, authority: &ObjectStoreAuthorityRef) -> bool {
        self.authorities.contains(authority)
    }

    fn relation_fingerprint(
        &self,
        authority: &ObjectStoreAuthorityRef,
        relation: &RelationVersionRef,
    ) -> Option<&str> {
        self.relation_fingerprints
            .get(&RelationEvidenceKey {
                authority: authority.clone(),
                relation_id: relation.relation_id.clone(),
                relation_version: relation.relation_version,
            })
            .map(String::as_str)
    }

    fn latest_stream_checkpoint(
        &self,
        authority: &ObjectStoreAuthorityRef,
        stream_id: &str,
        relation: &RelationVersionRef,
    ) -> Option<CheckpointRef> {
        self.latest_stream_checkpoints
            .get(&StreamRelationEvidenceKey {
                authority: authority.clone(),
                stream_id: stream_id.to_string(),
                relation_id: relation.relation_id.clone(),
                relation_version: relation.relation_version,
                schema_fingerprint: relation.schema_fingerprint.clone(),
            })
            .cloned()
    }
}

pub fn reconcile_stream(stream: &VelorixStream, snapshot: &AuthoritySnapshot) -> ReconcileOutcome {
    let status = stream_status_from_authority(stream, snapshot);

    ReconcileOutcome {
        action: ControllerAction::WriteStreamStatus(status),
    }
}

fn stream_status_from_authority(
    stream: &VelorixStream,
    snapshot: &AuthoritySnapshot,
) -> StreamStatus {
    let observed_generation = stream.metadata.generation;

    if !snapshot.has_authority(&stream.spec.authority) {
        return StreamStatus {
            observed_generation,
            last_accepted_relation_schema_fingerprint: None,
            latest_published_checkpoint: None,
            readiness: Some(condition(
                ConditionState::False,
                "MissingAuthorityRecord",
                "object-store authority record is not visible",
            )),
        };
    }

    let Some(catalog_fingerprint) =
        snapshot.relation_fingerprint(&stream.spec.authority, &stream.spec.relation)
    else {
        return StreamStatus {
            observed_generation,
            last_accepted_relation_schema_fingerprint: None,
            latest_published_checkpoint: None,
            readiness: Some(condition(
                ConditionState::False,
                "MissingRelationCatalogRecord",
                "relation catalog record is not visible",
            )),
        };
    };

    if catalog_fingerprint != stream.spec.relation.schema_fingerprint {
        return StreamStatus {
            observed_generation,
            last_accepted_relation_schema_fingerprint: Some(catalog_fingerprint.to_string()),
            latest_published_checkpoint: None,
            readiness: Some(condition(
                ConditionState::False,
                "RelationFingerprintMismatch",
                "stream spec relation fingerprint does not match object-store catalog",
            )),
        };
    }

    let latest_published_checkpoint = snapshot.latest_stream_checkpoint(
        &stream.spec.authority,
        &stream.spec.stream_id,
        &stream.spec.relation,
    );

    StreamStatus {
        observed_generation,
        last_accepted_relation_schema_fingerprint: Some(catalog_fingerprint.to_string()),
        latest_published_checkpoint,
        readiness: Some(condition(
            ConditionState::True,
            "AuthorityValidated",
            "object-store authority and relation catalog records validated",
        )),
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RelationEvidenceKey {
    authority: ObjectStoreAuthorityRef,
    relation_id: String,
    relation_version: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StreamRelationEvidenceKey {
    authority: ObjectStoreAuthorityRef,
    stream_id: String,
    relation_id: String,
    relation_version: u64,
    schema_fingerprint: String,
}

fn condition(status: ConditionState, reason: &str, message: &str) -> VelorixCondition {
    VelorixCondition {
        type_: "Ready".to_string(),
        status,
        reason: reason.to_string(),
        message: message.to_string(),
    }
}

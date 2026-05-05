use velorix_k8s::{
    controller::{reconcile_stream, AuthoritySnapshot, ControllerAction},
    crd::{
        CheckpointRef, ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
        VelorixCondition, VelorixStream, VelorixStreamSpec,
    },
};

#[test]
fn reconcile_stream_reports_missing_authority_without_using_kubernetes_status_as_authority() {
    let mut stream = stream();
    stream.status = Some(StreamStatus {
        observed_generation: Some(1),
        last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
        latest_published_checkpoint: Some(checkpoint(9)),
        readiness: Some(ready_condition()),
    });

    let outcome = reconcile_stream(&stream, &AuthoritySnapshot::default());

    assert_eq!(
        outcome.action,
        ControllerAction::WriteStreamStatus(StreamStatus {
            observed_generation: Some(1),
            last_accepted_relation_schema_fingerprint: None,
            latest_published_checkpoint: None,
            readiness: Some(VelorixCondition {
                type_: "Ready".to_string(),
                status: ConditionState::False,
                reason: "MissingAuthorityRecord".to_string(),
                message: "object-store authority record is not visible".to_string(),
            }),
        })
    );
}

#[test]
fn reconcile_stream_reports_relation_fingerprint_mismatch_from_authority_snapshot() {
    let stream = stream();
    let stale_relation = RelationVersionRef {
        schema_fingerprint: format!("sha256:{}", "0".repeat(64)),
        ..relation()
    };
    let snapshot = AuthoritySnapshot::default()
        .with_authority(authority())
        .with_relation_for_authority(&authority(), &stale_relation);

    let outcome = reconcile_stream(&stream, &snapshot);

    assert_eq!(
        outcome.action,
        ControllerAction::WriteStreamStatus(StreamStatus {
            observed_generation: Some(1),
            last_accepted_relation_schema_fingerprint: Some(stale_relation.schema_fingerprint),
            latest_published_checkpoint: None,
            readiness: Some(VelorixCondition {
                type_: "Ready".to_string(),
                status: ConditionState::False,
                reason: "RelationFingerprintMismatch".to_string(),
                message: "stream spec relation fingerprint does not match object-store catalog"
                    .to_string(),
            }),
        })
    );
}

#[test]
fn reconcile_stream_replaces_stale_checkpoint_status_with_authoritative_checkpoint() {
    let mut stream = stream();
    stream.status = Some(StreamStatus {
        observed_generation: Some(1),
        last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
        latest_published_checkpoint: Some(checkpoint(99)),
        readiness: Some(ready_condition()),
    });
    let snapshot = AuthoritySnapshot::default()
        .with_authority(authority())
        .with_relation_for_authority(&authority(), &relation())
        .with_latest_stream_checkpoint_for_authority(&authority(), "deposits", checkpoint(7));

    let outcome = reconcile_stream(&stream, &snapshot);

    assert_eq!(
        outcome.action,
        ControllerAction::WriteStreamStatus(StreamStatus {
            observed_generation: Some(1),
            last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
            latest_published_checkpoint: Some(checkpoint(7)),
            readiness: Some(ready_condition()),
        })
    );
}

#[test]
fn reconcile_stream_reports_ready_only_after_authority_and_relation_validate() {
    let stream = stream();
    let snapshot = AuthoritySnapshot::default()
        .with_authority(authority())
        .with_relation_for_authority(&authority(), &relation());

    let outcome = reconcile_stream(&stream, &snapshot);

    assert_eq!(
        outcome.action,
        ControllerAction::WriteStreamStatus(StreamStatus {
            observed_generation: Some(1),
            last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
            latest_published_checkpoint: None,
            readiness: Some(ready_condition()),
        })
    );
}

#[test]
fn reconcile_stream_ignores_relation_evidence_from_other_authority() {
    let stream = stream();
    let snapshot = AuthoritySnapshot::default()
        .with_authority(authority())
        .with_relation_for_authority(&other_authority(), &relation());

    let outcome = reconcile_stream(&stream, &snapshot);

    assert_eq!(
        outcome.action,
        ControllerAction::WriteStreamStatus(StreamStatus {
            observed_generation: Some(1),
            last_accepted_relation_schema_fingerprint: None,
            latest_published_checkpoint: None,
            readiness: Some(VelorixCondition {
                type_: "Ready".to_string(),
                status: ConditionState::False,
                reason: "MissingRelationCatalogRecord".to_string(),
                message: "relation catalog record is not visible".to_string(),
            }),
        })
    );
}

#[test]
fn reconcile_stream_ignores_checkpoint_evidence_from_other_authority() {
    let stream = stream();
    let snapshot = AuthoritySnapshot::default()
        .with_authority(authority())
        .with_relation_for_authority(&authority(), &relation())
        .with_latest_stream_checkpoint_for_authority(&other_authority(), "deposits", checkpoint(7));

    let outcome = reconcile_stream(&stream, &snapshot);

    assert_eq!(
        outcome.action,
        ControllerAction::WriteStreamStatus(StreamStatus {
            observed_generation: Some(1),
            last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
            latest_published_checkpoint: None,
            readiness: Some(ready_condition()),
        })
    );
}

fn stream() -> VelorixStream {
    let mut stream = VelorixStream::new(
        "deposits",
        VelorixStreamSpec {
            stream_id: "deposits".to_string(),
            database_id: "analytics".to_string(),
            relation: relation(),
            authority: authority(),
        },
    );
    stream.metadata.generation = Some(1);
    stream
}

fn authority() -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: "analytics".to_string(),
    }
}

fn other_authority() -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "secondary".to_string(),
        namespace: "analytics".to_string(),
    }
}

fn relation() -> RelationVersionRef {
    RelationVersionRef {
        relation_id: "deposits".to_string(),
        relation_version: 1,
        schema_fingerprint: format!("sha256:{}", "1".repeat(64)),
    }
}

fn checkpoint(checkpoint_version: u64) -> CheckpointRef {
    CheckpointRef {
        checkpoint_version,
        manifest_digest: format!("sha256:{checkpoint_version:064x}"),
    }
}

fn ready_condition() -> VelorixCondition {
    VelorixCondition {
        type_: "Ready".to_string(),
        status: ConditionState::True,
        reason: "AuthorityValidated".to_string(),
        message: "object-store authority and relation catalog records validated".to_string(),
    }
}

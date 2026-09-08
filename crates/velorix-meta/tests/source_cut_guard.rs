use velorix_meta::{
    AcquireRelationPartitionAuthorityOutcome, AcquireRelationPartitionAuthorityRequest,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    CaptureRelationIngestSourceCutsRequest, InMemoryMetaStore, IngestSourceRelationIdentityV1,
    MetaStore, PublishIngestReservationOutcome, PublishRelationIngestReservationRequest,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    RelationPartitionAuthorityKey, ReserveIngestRangeOutcome,
    ReserveRelationAuthoritativeIngestRangeRequest, StandingRuntimeCheckpointPointer,
};

fn identity() -> IngestSourceRelationIdentityV1 {
    IngestSourceRelationIdentityV1 {
        relation_id: "orders".into(),
        relation_version: "v1".into(),
        relation_generation: 1,
        schema_fingerprint: "sha256:schema".into(),
    }
}

fn reservation(start: u64, end: u64, batch: &str) -> velorix_meta::IngestRangeReservation {
    velorix_meta::IngestRangeReservation {
        stream_id: "orders".into(),
        partition_id: 0,
        start_offset_inclusive: start,
        end_offset_exclusive: end,
        batch_key: format!("batch-{batch}"),
        payload_digest: format!("sha256:payload-{batch}"),
        relation_id: "orders".into(),
        relation_version: "v1".into(),
        schema_fingerprint: "sha256:schema".into(),
        writer_epoch: 1,
    }
}

fn pointer(epoch: u64, seed: char) -> StandingRuntimeCheckpointPointer {
    let hash = seed.to_string().repeat(64);
    StandingRuntimeCheckpointPointer {
        tenant_id: "default".into(),
        program_id: "program".into(),
        view_id: "view".into(),
        checkpoint_key: format!(
            "v1/standing-runtime-checkpoints/default/program/view/epochs/{epoch:020}/sha256/{hash}.checkpoint.json"
        ),
        logical_epoch: epoch,
        content_hash: format!("sha256:{hash}"),
        manifest_hash: format!("sha256:{hash}"),
        output_manifest_refs: Vec::new(),
        bootstrap_generation: 0,
        plan_hash: String::new(),
        coverage_hash: String::new(),
        input_coverage: None,
        previous_checkpoint_key: String::new(),
        previous_manifest_hash: String::new(),
    }
}

async fn owner(store: &InMemoryMetaStore) -> velorix_meta::StandingRuntimeOwnerToken {
    match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: "default".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: "owner".into(),
            ttl_ms: 60_000,
        })
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => {
            velorix_meta::StandingRuntimeOwnerToken {
                tenant_id: claim.tenant_id,
                program_id: claim.program_id,
                view_id: claim.view_id,
                owner_id: claim.owner_id,
                owner_epoch: claim.owner_epoch,
            }
        }
        other => panic!("unexpected owner outcome: {other:?}"),
    }
}

async fn publish_range(
    store: &InMemoryMetaStore,
    authority: &velorix_meta::RelationPartitionAuthorityToken,
    range: velorix_meta::IngestRangeReservation,
    request_id: &str,
) {
    assert_eq!(
        store
            .reserve_relation_authoritative_ingest_range(
                ReserveRelationAuthoritativeIngestRangeRequest {
                    reservation: range.clone(),
                    authority: authority.clone(),
                },
            )
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    assert_eq!(
        store
            .publish_relation_ingest_reservation(PublishRelationIngestReservationRequest {
                reservation: range,
                authority: authority.clone(),
                request_id: request_id.into(),
                request_digest: format!("sha256:{request_id}"),
                object_key: format!("objects/{request_id}"),
                object_digest: format!("sha256:object-{request_id}"),
            })
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Committed
    );
}

async fn capture(
    store: &InMemoryMetaStore,
) -> Vec<velorix_meta::RelationIngestSourceIdentityCutV1> {
    store
        .capture_relation_ingest_source_cuts(CaptureRelationIngestSourceCutsRequest {
            namespace: "default".into(),
            relations: vec![identity()],
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn guarded_publish_accepts_unchanged_full_source_cut() {
    let store = InMemoryMetaStore::default();
    let key = RelationPartitionAuthorityKey {
        namespace: "default".into(),
        relation_id: "orders".into(),
        stream_id: "orders".into(),
        partition_id: 0,
    };
    let authority = match store
        .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
            key,
            owner_id: "writer".into(),
            current_token: None,
            ttl_ms: 60_000,
        })
        .await
        .unwrap()
    {
        AcquireRelationPartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected authority outcome: {other:?}"),
    };
    publish_range(&store, &authority, reservation(0, 10, "one"), "one").await;
    let cuts = capture(&store).await;
    let result = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer(1, 'a'),
            owner: owner(&store).await,
            expected_relation_source_cuts: Some(cuts),
        })
        .await
        .unwrap();
    assert_eq!(result, PublishStandingRuntimeCheckpointOutcome::Published);
}

#[tokio::test]
async fn guarded_publish_rejects_source_change_and_leaves_pointer_unchanged() {
    let store = InMemoryMetaStore::default();
    let key = RelationPartitionAuthorityKey {
        namespace: "default".into(),
        relation_id: "orders".into(),
        stream_id: "orders".into(),
        partition_id: 0,
    };
    let authority = match store
        .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
            key,
            owner_id: "writer".into(),
            current_token: None,
            ttl_ms: 60_000,
        })
        .await
        .unwrap()
    {
        AcquireRelationPartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected authority outcome: {other:?}"),
    };
    publish_range(&store, &authority, reservation(0, 10, "one"), "one").await;
    let cuts = capture(&store).await;
    let first = pointer(1, 'a');
    let token = owner(&store).await;
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: first.clone(),
                owner: token.clone(),
                expected_relation_source_cuts: Some(cuts.clone()),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    publish_range(&store, &authority, reservation(10, 20, "two"), "two").await;

    let mut second = pointer(2, 'b');
    second.previous_checkpoint_key = first.checkpoint_key.clone();
    second.previous_manifest_hash = first.manifest_hash.clone();
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: Some(first.clone()),
                candidate: second,
                owner: token,
                expected_relation_source_cuts: Some(cuts),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::SourceCutChanged
    );
    assert_eq!(
        store
            .read_standing_runtime_checkpoint("default", "program", "view")
            .await
            .unwrap(),
        Some(first)
    );
}

#[tokio::test]
async fn guarded_retry_of_current_candidate_is_duplicate_after_later_ingest() {
    let store = InMemoryMetaStore::default();
    let key = RelationPartitionAuthorityKey {
        namespace: "default".into(),
        relation_id: "orders".into(),
        stream_id: "orders".into(),
        partition_id: 0,
    };
    let authority = match store
        .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
            key,
            owner_id: "writer".into(),
            current_token: None,
            ttl_ms: 60_000,
        })
        .await
        .unwrap()
    {
        AcquireRelationPartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected authority outcome: {other:?}"),
    };
    publish_range(&store, &authority, reservation(0, 10, "one"), "one").await;
    let cuts = capture(&store).await;
    let token = owner(&store).await;
    let candidate = pointer(1, 'a');
    let request = PublishStandingRuntimeCheckpointRequest {
        expected_previous: None,
        candidate: candidate.clone(),
        owner: token,
        expected_relation_source_cuts: Some(cuts),
    };
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(request.clone())
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    publish_range(&store, &authority, reservation(10, 20, "two"), "two").await;
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(request)
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Duplicate
    );
}

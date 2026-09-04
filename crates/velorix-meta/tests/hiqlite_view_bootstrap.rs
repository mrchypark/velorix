#![cfg(feature = "hiqlite-backend")]

use std::{net::TcpListener, sync::Arc, time::Duration};

use tempfile::TempDir;
use velorix_core::standing_program::{
    RuntimeCheckpointInputCoverageV1, RuntimeCheckpointPartitionCoverageV1,
    RuntimeCheckpointRelationCoverageV1, RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
};
use velorix_meta::{
    AcquirePartitionAuthorityOutcome, AcquirePartitionAuthorityRequest,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    BeginViewBootstrapOutcome, BeginViewBootstrapRequest, BeginViewDependencyEdgeV1,
    CommitIngestRangeOutcome, FixViewBootstrapActivationCutOutcome,
    FixViewBootstrapActivationCutRequest, HiqliteMetaStore, IngestRangeReservation,
    IngestSourceCutV1, IngestSourceRelationIdentityV1, MetaStore, MetaStoreError,
    PartitionAuthorityKey, PartitionAuthorityToken, PromoteViewBootstrapOutcome,
    PromoteViewBootstrapRequest, PublishIngestReservationOutcome, PublishIngestReservationRequest,
    PublishPartitionCheckpointPointerOutcome, PublishPartitionCheckpointPointerRequest,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    ReserveAuthoritativeIngestRangeRequest, ReserveIngestRangeOutcome,
    StandingRuntimeCheckpointPointer, StandingRuntimeOwnerToken, ViewBootstrapLifecycleV1,
};

#[tokio::test]
async fn hiqlite_partition_authority_acquire_retry_renew_takeover_and_read() {
    let (_dir, store, client) = start_store().await;
    let request = partition_authority_request("worker-a", None);
    let first = match store
        .acquire_partition_authority(request.clone())
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected first authority outcome: {other:?}"),
    };
    assert_eq!(first.owner_epoch, 1);
    assert_eq!(
        store.acquire_partition_authority(request).await.unwrap(),
        AcquirePartitionAuthorityOutcome::Acquired(first.clone())
    );
    let renewed = match store
        .acquire_partition_authority(partition_authority_request("worker-a", Some(first.clone())))
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Renewed(token) => token,
        other => panic!("unexpected renewal outcome: {other:?}"),
    };
    assert_eq!(renewed.owner_epoch, 1);
    let mut stale_renewal = partition_authority_request("worker-a", Some(first));
    stale_renewal.ttl_ms += 1;
    assert!(matches!(
        store
            .acquire_partition_authority(stale_renewal)
            .await
            .unwrap(),
        AcquirePartitionAuthorityOutcome::Conflict(_)
    ));
    client.execute(
        "UPDATE velorix_partition_authorities SET expires_at_unix_ms = 0 WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4",
        vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders"), hiqlite::Param::from(0_i64)],
    ).await.unwrap();
    let takeover = match store
        .acquire_partition_authority(partition_authority_request("worker-b", None))
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected takeover outcome: {other:?}"),
    };
    assert_eq!(takeover.owner_epoch, 2);
    assert_eq!(
        store
            .read_partition_authority(&partition_authority_key())
            .await
            .unwrap(),
        Some(takeover)
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_partition_authority_concurrent_claim_has_one_winner() {
    let (_dir, store, client) = start_store().await;
    let store = Arc::new(store);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for owner_id in ["worker-a", "worker-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .acquire_partition_authority(partition_authority_request(owner_id, None))
                .await
                .unwrap()
        }));
    }
    barrier.wait().await;
    let outcomes = [
        workers.remove(0).await.unwrap(),
        workers.remove(0).await.unwrap(),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcquirePartitionAuthorityOutcome::Acquired(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcquirePartitionAuthorityOutcome::Conflict(_)))
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_authoritative_ingest_publication_is_bound_idempotent_and_recoverable() {
    let (_dir, store, client) = start_store().await;
    let authority = match store
        .acquire_partition_authority(partition_authority_request("worker-a", None))
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected authority outcome: {other:?}"),
    };
    let reservation = reservation(0, 0, 100, "sha256:payload");
    assert_eq!(
        store
            .reserve_authoritative_ingest_range(ReserveAuthoritativeIngestRangeRequest {
                reservation: reservation.clone(),
                authority: authority.clone(),
            })
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    let request = PublishIngestReservationRequest {
        reservation: reservation.clone(),
        authority: authority.clone(),
        request_id: "publication-one".to_string(),
        request_digest: "sha256:request-one".to_string(),
        object_key: "objects/one".to_string(),
        object_digest: "sha256:object-one".to_string(),
    };
    assert_eq!(
        store
            .publish_ingest_reservation(request.clone())
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Committed
    );
    client
        .execute(
            "UPDATE velorix_partition_authorities SET expires_at_unix_ms = 0 WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4",
            vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders"), hiqlite::Param::from(0_i64)],
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .publish_ingest_reservation(request.clone())
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Duplicate
    );
    let mut conflict = request;
    conflict.request_digest = "sha256:request-two".to_string();
    assert_eq!(
        store.publish_ingest_reservation(conflict).await.unwrap(),
        PublishIngestReservationOutcome::Conflict
    );
    let publication = store
        .read_authoritative_ingest_publication("publication-one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(publication.reservation, reservation);
    assert_eq!(publication.object_key, "objects/one");
    assert_eq!(
        store
            .list_authoritative_ingest_publications(&authority.key)
            .await
            .unwrap(),
        vec![publication]
    );
    client.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hiqlite_concurrent_authoritative_publication_has_one_commit() {
    let (_dir, store, client) = start_store().await;
    let authority = match store
        .acquire_partition_authority(partition_authority_request("worker-a", None))
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected authority outcome: {other:?}"),
    };
    let reservation = reservation(0, 0, 100, "sha256:payload");
    store
        .reserve_authoritative_ingest_range(ReserveAuthoritativeIngestRangeRequest {
            reservation: reservation.clone(),
            authority: authority.clone(),
        })
        .await
        .unwrap();
    let request = PublishIngestReservationRequest {
        reservation,
        authority,
        request_id: "publication-concurrent".to_string(),
        request_digest: "sha256:request".to_string(),
        object_key: "objects/concurrent".to_string(),
        object_digest: "sha256:object".to_string(),
    };
    let store = Arc::new(store);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let request = request.clone();
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            store.publish_ingest_reservation(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let outcomes = [
        workers.remove(0).await.unwrap(),
        workers.remove(0).await.unwrap(),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublishIngestReservationOutcome::Committed)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublishIngestReservationOutcome::Duplicate)
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hiqlite_competing_authoritative_publications_have_one_commit_and_no_loser_phantom() {
    let (_dir, store, client) = start_store().await;
    let authority = match store
        .acquire_partition_authority(partition_authority_request("worker-a", None))
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected authority outcome: {other:?}"),
    };
    let reservation = reservation(0, 0, 100, "sha256:payload");
    store
        .reserve_authoritative_ingest_range(ReserveAuthoritativeIngestRangeRequest {
            reservation: reservation.clone(),
            authority: authority.clone(),
        })
        .await
        .unwrap();
    let winner_candidate = PublishIngestReservationRequest {
        reservation: reservation.clone(),
        authority: authority.clone(),
        request_id: "publication-winner".to_string(),
        request_digest: "sha256:winner-request".to_string(),
        object_key: "objects/winner".to_string(),
        object_digest: "sha256:winner-object".to_string(),
    };
    let loser_candidate = PublishIngestReservationRequest {
        reservation,
        authority,
        request_id: "publication-loser".to_string(),
        request_digest: "sha256:loser-request".to_string(),
        object_key: "objects/loser".to_string(),
        object_digest: "sha256:loser-object".to_string(),
    };
    let store = Arc::new(store);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let left_store = Arc::clone(&store);
    let left_barrier = Arc::clone(&barrier);
    let left = tokio::spawn(async move {
        left_barrier.wait().await;
        left_store
            .publish_ingest_reservation(winner_candidate)
            .await
            .unwrap()
    });
    let right_store = Arc::clone(&store);
    let right_barrier = Arc::clone(&barrier);
    let right = tokio::spawn(async move {
        right_barrier.wait().await;
        right_store
            .publish_ingest_reservation(loser_candidate)
            .await
            .unwrap()
    });
    barrier.wait().await;
    let left_outcome = left.await.unwrap();
    let right_outcome = right.await.unwrap();
    let outcomes = [left_outcome.clone(), right_outcome.clone()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublishIngestReservationOutcome::Committed)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublishIngestReservationOutcome::Conflict)
            .count(),
        1
    );
    let winner = store
        .read_authoritative_ingest_publication("publication-winner")
        .await
        .unwrap();
    let loser = store
        .read_authoritative_ingest_publication("publication-loser")
        .await
        .unwrap();
    match (left_outcome, right_outcome) {
        (PublishIngestReservationOutcome::Committed, PublishIngestReservationOutcome::Conflict) => {
            assert_eq!(winner.unwrap().object_key, "objects/winner");
            assert_eq!(loser, None);
        }
        (PublishIngestReservationOutcome::Conflict, PublishIngestReservationOutcome::Committed) => {
            assert_eq!(winner, None);
            assert_eq!(loser.unwrap().object_key, "objects/loser");
        }
        outcomes => panic!("unexpected competing outcomes: {outcomes:?}"),
    }
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_partition_checkpoint_publish_is_fenced_and_idempotent() {
    let (_dir, store, client) = start_store().await;
    let authority = match store
        .acquire_partition_authority(partition_authority_request("worker-a", None))
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected authority outcome: {other:?}"),
    };
    let candidate = velorix_meta::PartitionCheckpointPointer {
        key: partition_authority_key(),
        checkpoint_key: "checkpoints/one".to_string(),
    };
    let request = PublishPartitionCheckpointPointerRequest {
        expected_previous: None,
        candidate: candidate.clone(),
        authority: authority.clone(),
    };
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(request.clone())
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Published
    );
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(request)
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Published
    );
    let duplicate = PublishPartitionCheckpointPointerRequest {
        expected_previous: Some(candidate.clone()),
        candidate: candidate.clone(),
        authority: authority.clone(),
    };
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(duplicate)
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Duplicate
    );
    client.execute("UPDATE velorix_partition_authorities SET expires_at_unix_ms = 0 WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4", vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders"), hiqlite::Param::from(0_i64)]).await.unwrap();
    let stale = PublishPartitionCheckpointPointerRequest {
        expected_previous: None,
        candidate: velorix_meta::PartitionCheckpointPointer {
            key: candidate.key,
            checkpoint_key: "checkpoints/two".to_string(),
        },
        authority,
    };
    let stale_result = store.publish_partition_checkpoint_pointer(stale).await;
    assert!(
        matches!(
            stale_result,
            Err(MetaStoreError::PartitionAuthorityInvalidToken)
        ),
        "{stale_result:?}"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_partition_authority_epoch_overflow_fails_closed_without_mutation() {
    let (_dir, store, client) = start_store().await;
    client.execute(
        "INSERT INTO velorix_partition_authorities (namespace, view_id, stream_id, partition_id, owner_id, owner_epoch, expires_at_unix_ms, last_request_id, last_outcome) VALUES ($1, $2, $3, $4, $5, $6, $7, '', '')",
        vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders"), hiqlite::Param::from(1_i64), hiqlite::Param::from("worker-a"), hiqlite::Param::from(i64::MAX), hiqlite::Param::from(i64::MAX)],
    ).await.unwrap();
    let live_max = PartitionAuthorityToken {
        key: PartitionAuthorityKey {
            partition_id: 1,
            ..partition_authority_key()
        },
        owner_id: "worker-a".to_string(),
        owner_epoch: i64::MAX as u64,
        expires_at_unix_ms: i64::MAX as u64,
    };
    let mut renew_max = partition_authority_request("worker-a", Some(live_max));
    renew_max.key.partition_id = 1;
    assert!(
        matches!(store.acquire_partition_authority(renew_max).await.unwrap(), AcquirePartitionAuthorityOutcome::Renewed(token) if token.owner_epoch == i64::MAX as u64)
    );
    client.execute(
        "INSERT INTO velorix_partition_authorities (namespace, view_id, stream_id, partition_id, owner_id, owner_epoch, expires_at_unix_ms, last_request_id, last_outcome) VALUES ($1, $2, $3, $4, $5, $6, 0, '', '')",
        vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders"), hiqlite::Param::from(0_i64), hiqlite::Param::from("worker-a"), hiqlite::Param::from(i64::MAX)],
    ).await.unwrap();
    assert!(matches!(
        store
            .acquire_partition_authority(partition_authority_request("worker-b", None))
            .await,
        Err(MetaStoreError::AuthorityEpochOverflow)
    ));
    let mut rows = client.query_consistent("SELECT owner_id, owner_epoch, expires_at_unix_ms FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4", vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders"), hiqlite::Param::from(0_i64)]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>("owner_id"), "worker-a");
    assert_eq!(rows[0].get::<i64>("owner_epoch"), i64::MAX);
    assert_eq!(rows[0].get::<i64>("expires_at_unix_ms"), 0);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_partition_authority_max_minus_one_takeover_never_wraps() {
    let (_dir, store, client) = start_store().await;
    client.execute(
        "INSERT INTO velorix_partition_authorities (namespace, view_id, stream_id, partition_id, owner_id, owner_epoch, expires_at_unix_ms, last_request_id, last_outcome) VALUES ($1, $2, $3, 2, 'old', $4, 0, '', '')",
        vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders"), hiqlite::Param::from(i64::MAX - 1)],
    ).await.unwrap();
    let store = Arc::new(store);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for owner_id in ["worker-a", "worker-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut request = partition_authority_request(owner_id, None);
            request.key.partition_id = 2;
            store.acquire_partition_authority(request).await
        }));
    }
    barrier.wait().await;
    let outcomes = [
        workers.remove(0).await.unwrap(),
        workers.remove(0).await.unwrap(),
    ];
    assert_eq!(outcomes.iter().filter(|outcome| matches!(outcome, Ok(AcquirePartitionAuthorityOutcome::Acquired(token)) if token.owner_epoch == i64::MAX as u64)).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                Ok(AcquirePartitionAuthorityOutcome::Conflict(_))
                    | Err(MetaStoreError::AuthorityEpochOverflow)
            ))
            .count(),
        1
    );
    let mut rows = client.query_consistent("SELECT owner_epoch FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = 2", vec![hiqlite::Param::from("default"), hiqlite::Param::from("view"), hiqlite::Param::from("orders")]).await.unwrap();
    assert_eq!(rows[0].get::<i64>("owner_epoch"), i64::MAX);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_partition_authority_reopen_repairs_legacy_tables_and_replays_status() {
    let (_dir, _store, client) = start_store().await;
    client
        .execute("DROP TABLE velorix_partition_authority_requests", vec![])
        .await
        .unwrap();
    client
        .execute("DROP TABLE velorix_partition_authorities", vec![])
        .await
        .unwrap();
    client
        .execute("DROP TABLE velorix_partition_checkpoint_requests", vec![])
        .await
        .unwrap();
    client
        .execute("DROP TABLE velorix_partition_checkpoint_pointers", vec![])
        .await
        .unwrap();
    client.execute("CREATE TABLE velorix_partition_authorities (namespace TEXT NOT NULL, view_id TEXT NOT NULL, stream_id TEXT NOT NULL, partition_id INTEGER NOT NULL, owner_id TEXT NOT NULL, owner_epoch INTEGER NOT NULL, expires_at_unix_ms INTEGER NOT NULL, PRIMARY KEY(namespace, view_id, stream_id, partition_id))", vec![]).await.unwrap();
    client.execute("CREATE TABLE velorix_partition_authority_requests (request_id TEXT NOT NULL PRIMARY KEY)", vec![]).await.unwrap();
    let store = HiqliteMetaStore::new(client.clone()).await.unwrap();
    assert_eq!(
        store
            .read_partition_checkpoint_pointer(&partition_authority_key())
            .await
            .unwrap(),
        None
    );
    let request = partition_authority_request("worker-a", None);
    let first = store
        .acquire_partition_authority(request.clone())
        .await
        .unwrap();
    assert_eq!(
        store.acquire_partition_authority(request).await.unwrap(),
        first
    );
    let authority = match first {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected repaired authority outcome: {other:?}"),
    };
    let pointer = velorix_meta::PartitionCheckpointPointer {
        key: partition_authority_key(),
        checkpoint_key: "checkpoints/repaired".to_string(),
    };
    let publish = PublishPartitionCheckpointPointerRequest {
        expected_previous: None,
        candidate: pointer.clone(),
        authority,
    };
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(publish.clone())
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Published
    );
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(publish)
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Published
    );
    assert_eq!(
        store
            .read_partition_checkpoint_pointer(&partition_authority_key())
            .await
            .unwrap(),
        Some(pointer)
    );
    client
        .query_consistent(
            "SELECT outcome, expires_at_unix_ms FROM velorix_partition_authority_requests LIMIT 0",
            vec![],
        )
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_view_input_bootstrap_retries_do_not_advance_graph_revision() {
    let (_dir, store, client) = start_store().await;
    let request = view_input_bootstrap_request("dependent-view", 0);

    assert!(matches!(
        store.begin_view_bootstrap(request.clone()).await.unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        1
    );

    assert!(matches!(
        store.begin_view_bootstrap(request.clone()).await.unwrap(),
        BeginViewBootstrapOutcome::Duplicate(_)
    ));
    let mut retry_at_current_revision = request;
    retry_at_current_revision.expected_graph_revision = 1;
    assert!(matches!(
        store
            .begin_view_bootstrap(retry_at_current_revision)
            .await
            .unwrap(),
        BeginViewBootstrapOutcome::Duplicate(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        1
    );

    assert!(matches!(
        store
            .begin_view_bootstrap(view_input_bootstrap_request("next-dependent-view", 1))
            .await
            .unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        2
    );

    assert_eq!(
        store
            .begin_view_bootstrap(view_input_bootstrap_request("stale-dependent-view", 0))
            .await
            .unwrap(),
        BeginViewBootstrapOutcome::Conflict
    );
    assert!(store
        .read_view_bootstrap("default", "stale-dependent-view", "stale-dependent-view")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        2
    );

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_view_input_bootstrap_reads_legacy_blob_edge_json() {
    let (_dir, store, client) = start_store().await;
    let request = view_input_bootstrap_request("legacy-blob-dependent-view", 0);
    assert!(matches!(
        store.begin_view_bootstrap(request.clone()).await.unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));

    // Previous releases bound `serde_json::to_vec` directly, leaving valid
    // JSON in a SQLite BLOB even though the column is declared TEXT.
    let legacy_edge_json = serde_json::to_vec(&request.view_inputs[0]).unwrap();
    client
        .execute(
            "UPDATE velorix_view_bootstrap_view_inputs SET edge_json = $1
             WHERE tenant_id = $2 AND program_id = $3 AND view_id = $4",
            vec![
                hiqlite::Param::from(legacy_edge_json),
                hiqlite::Param::from("default"),
                hiqlite::Param::from("legacy-blob-dependent-view"),
                hiqlite::Param::from("legacy-blob-dependent-view"),
            ],
        )
        .await
        .unwrap();
    let mut rows = client
        .query_consistent(
            "SELECT typeof(edge_json) AS storage_type
             FROM velorix_view_bootstrap_view_inputs
             WHERE tenant_id = $1 AND program_id = $2 AND view_id = $3",
            vec![
                hiqlite::Param::from("default"),
                hiqlite::Param::from("legacy-blob-dependent-view"),
                hiqlite::Param::from("legacy-blob-dependent-view"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(rows.pop().unwrap().get::<String>("storage_type"), "blob");

    assert!(matches!(
        store.begin_view_bootstrap(request).await.unwrap(),
        BeginViewBootstrapOutcome::Duplicate(_)
    ));
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_concurrent_view_input_bootstraps_allow_one_graph_cas_winner() {
    let (_dir, store, client) = start_store().await;
    let store = Arc::new(store);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for view_id in ["race-dependent-a", "race-dependent-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let request = view_input_bootstrap_request(view_id, 0);
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            store.begin_view_bootstrap(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let outcomes = [
        workers.remove(0).await.unwrap(),
        workers.remove(0).await.unwrap(),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, BeginViewBootstrapOutcome::Created(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, BeginViewBootstrapOutcome::Conflict))
            .count(),
        1
    );
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        1
    );

    let mut controls = 0;
    for view_id in ["race-dependent-a", "race-dependent-b"] {
        if store
            .read_view_bootstrap("default", view_id, view_id)
            .await
            .unwrap()
            .is_some()
        {
            controls += 1;
        }
    }
    assert_eq!(controls, 1);
    let mut rows = client
        .query_consistent(
            "SELECT program_id FROM velorix_view_bootstrap_view_inputs
             WHERE tenant_id = $1 AND program_id IN ($2, $3)",
            vec![
                hiqlite::Param::from("default"),
                hiqlite::Param::from("race-dependent-a"),
                hiqlite::Param::from("race-dependent-b"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let persisted_edge_program = rows.pop().unwrap().get::<String>("program_id");
    assert!(store
        .read_view_bootstrap("default", &persisted_edge_program, &persisted_edge_program)
        .await
        .unwrap()
        .is_some());

    drop(store);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn hiqlite_activation_cas_fences_expired_owner_pointer_change_and_concurrent_workers() {
    let (_dir, store, client) = start_store().await;
    let first = reservation(0, 0, 10, "sha256:first");
    assert_eq!(
        store.reserve_ingest_range(first.clone()).await.unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    assert_eq!(
        store.commit_ingest_range(first).await.unwrap(),
        CommitIngestRangeOutcome::Committed
    );
    let in_flight_new_partition = reservation(1, 0, 1, "sha256:new-partition");
    assert_eq!(
        store
            .reserve_ingest_range(in_flight_new_partition.clone())
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    let control = match store
        .begin_view_bootstrap(bootstrap_request())
        .await
        .unwrap()
    {
        BeginViewBootstrapOutcome::Created(control) => control,
        other => panic!("unexpected begin outcome: {other:?}"),
    };
    assert_eq!(control.bootstrap_cut.relations[0].partitions.len(), 2);
    assert_eq!(
        control.bootstrap_cut.relations[0]
            .partitions
            .iter()
            .find(|partition| partition.partition_id == 1)
            .unwrap()
            .committed_offset_exclusive,
        0
    );
    assert_eq!(
        store
            .commit_ingest_range(in_flight_new_partition)
            .await
            .unwrap(),
        CommitIngestRangeOutcome::Committed
    );

    let owner_a = acquire_owner(&store, "owner-a", 1_000).await;
    let pointer_1 = checkpoint_pointer(1, "a", &control.bootstrap_cut, &control.plan_hash, "", "");
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: pointer_1.clone(),
                owner: owner_a.clone(),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let expired_fix_without_takeover = store
        .fix_view_bootstrap_activation_cut(fix_request(&control, owner_a.clone()))
        .await
        .unwrap_err();
    assert!(expired_fix_without_takeover
        .to_string()
        .contains("owner token"));

    let owner_a_renewed = acquire_owner(&store, "owner-a", 1_000).await;
    assert!(owner_a_renewed.owner_epoch > owner_a.owner_epoch);
    assert!(matches!(
        store
            .fix_view_bootstrap_activation_cut(fix_request(&control, owner_a_renewed.clone(),))
            .await
            .unwrap(),
        FixViewBootstrapActivationCutOutcome::Fixed(_)
    ));
    let activation_cut = store
        .read_view_bootstrap("default", "orders-view", "orders-view")
        .await
        .unwrap()
        .unwrap()
        .activation_cut
        .unwrap();
    let pointer_2 = checkpoint_pointer(
        2,
        "b",
        &activation_cut,
        &control.plan_hash,
        &pointer_1.checkpoint_key,
        &pointer_1.manifest_hash,
    );
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: Some(pointer_1.clone()),
                candidate: pointer_2.clone(),
                owner: owner_a_renewed.clone(),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let expired_promote_without_takeover = store
        .promote_view_bootstrap(promote_request(
            &control,
            pointer_2.clone(),
            owner_a_renewed.clone(),
        ))
        .await
        .unwrap_err();
    assert!(expired_promote_without_takeover
        .to_string()
        .contains("owner token"));

    let owner_b = acquire_owner(&store, "owner-b", 30_000).await;
    assert!(owner_b.owner_epoch > owner_a_renewed.owner_epoch);
    assert!(matches!(
        store
            .fix_view_bootstrap_activation_cut(fix_request(&control, owner_b.clone()))
            .await
            .unwrap(),
        FixViewBootstrapActivationCutOutcome::Duplicate(_)
    ));

    assert_eq!(
        store
            .promote_view_bootstrap(promote_request(&control, pointer_1, owner_b.clone(),))
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Conflict
    );

    let store = Arc::new(store);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let request = promote_request(&control, pointer_2.clone(), owner_b.clone());
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            store.promote_view_bootstrap(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let outcomes = [
        workers.remove(0).await.unwrap(),
        workers.remove(0).await.unwrap(),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PromoteViewBootstrapOutcome::Promoted(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PromoteViewBootstrapOutcome::Duplicate(_)))
            .count(),
        1
    );
    let active = store
        .read_view_bootstrap("default", "orders-view", "orders-view")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.lifecycle, ViewBootstrapLifecycleV1::Active);
    assert_eq!(active.active_checkpoint, Some(pointer_2));
    drop(store);
    client.shutdown().await.unwrap();
}

#[allow(clippy::field_reassign_with_default)]
async fn start_store() -> (TempDir, HiqliteMetaStore, hiqlite::Client) {
    let dir = TempDir::new().unwrap();
    let raft_addr = free_addr();
    let api_addr = free_addr();
    let mut config = hiqlite::NodeConfig::default();
    config.node_id = 1;
    config.nodes = vec![hiqlite::Node {
        id: 1,
        addr_raft: raft_addr.clone(),
        addr_api: api_addr.clone(),
    }];
    config.listen_addr_raft = "127.0.0.1".into();
    config.listen_addr_api = "127.0.0.1".into();
    config.data_dir = dir.path().to_string_lossy().into_owned().into();
    config.filename_db = "velorix-meta.db".into();
    config.secret_raft = "velorix-test-raft-secret".to_string();
    config.secret_api = "velorix-test-api-secret".to_string();
    config
        .enc_keys
        .append_new_random_with_id("velorix-test-key".to_string())
        .unwrap();
    config.health_check_delay_secs = 0;
    config.raft_config = hiqlite::NodeConfig::default_raft_config(100);
    let client = hiqlite::start_node(config).await.unwrap();
    let store = HiqliteMetaStore::new(client.clone()).await.unwrap();
    (dir, store, client)
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn partition_authority_key() -> PartitionAuthorityKey {
    PartitionAuthorityKey {
        namespace: "default".to_string(),
        view_id: "view".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 0,
    }
}

fn partition_authority_request(
    owner_id: &str,
    current_token: Option<PartitionAuthorityToken>,
) -> AcquirePartitionAuthorityRequest {
    AcquirePartitionAuthorityRequest {
        key: partition_authority_key(),
        owner_id: owner_id.to_string(),
        current_token,
        ttl_ms: 60_000,
    }
}

fn relation() -> IngestSourceRelationIdentityV1 {
    IngestSourceRelationIdentityV1 {
        relation_id: "orders".to_string(),
        relation_version: "v1".to_string(),
        relation_generation: 1,
        schema_fingerprint: format!("sha256:{}", "c".repeat(64)),
    }
}

fn reservation(partition_id: u32, start: u64, end: u64, digest: &str) -> IngestRangeReservation {
    IngestRangeReservation {
        stream_id: "orders".to_string(),
        partition_id,
        start_offset_inclusive: start,
        end_offset_exclusive: end,
        batch_key: format!("v1/ingest/orders/p={partition_id:010}/{start:020}-{end:020}.batch"),
        payload_digest: digest.to_string(),
        relation_id: "orders".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "c".repeat(64)),
        writer_epoch: 1,
    }
}

fn bootstrap_request() -> BeginViewBootstrapRequest {
    BeginViewBootstrapRequest {
        tenant_id: "default".to_string(),
        program_id: "orders-view".to_string(),
        view_id: "orders-view".to_string(),
        plan_hash: "sha256:plan".to_string(),
        view_spec_json: br#"{"view_id":"orders-view"}"#.to_vec(),
        relations: vec![relation()],
        view_inputs: Vec::new(),
        expected_graph_revision: 0,
    }
}

fn view_input_bootstrap_request(
    view_id: &str,
    expected_graph_revision: u64,
) -> BeginViewBootstrapRequest {
    BeginViewBootstrapRequest {
        tenant_id: "default".to_string(),
        program_id: view_id.to_string(),
        view_id: view_id.to_string(),
        plan_hash: "sha256:dependent-plan".to_string(),
        view_spec_json: format!(r#"{{"view_id":"{view_id}"}}"#).into_bytes(),
        relations: vec![relation()],
        view_inputs: vec![BeginViewDependencyEdgeV1 {
            edge_id: "producer-to-dependent".to_string(),
            producer_program_id: "producer".to_string(),
            producer_view_id: "producer".to_string(),
            producer_generation: 1,
            producer_plan_hash: "sha256:producer-plan".to_string(),
            input_relation_id: "producer-output".to_string(),
            input_relation_version: "v1".to_string(),
            output_stream_id: "producer-output/v1".to_string(),
            output_schema_hash: "sha256:output-schema".to_string(),
            key_descriptor_hash: "sha256:key-descriptor".to_string(),
            delta_codec_identity: "velorix-published-delta-v1".to_string(),
            frontier_kind: "producer_commit_epoch".to_string(),
            bootstrap_cursor: velorix_core::standing_program::CausalViewCursorV1 {
                input_edge: "producer-to-dependent".to_string(),
                producer_tenant_id: "default".to_string(),
                producer_program_id: "producer".to_string(),
                producer_view_id: "producer".to_string(),
                producer_generation: 1,
                output_stream: "producer-output/v1".to_string(),
                output_epoch: 3,
                commit_digest: format!("sha256:{}", "a".repeat(64)),
            },
        }],
        expected_graph_revision,
    }
}

async fn acquire_owner(
    store: &HiqliteMetaStore,
    owner_id: &str,
    ttl_ms: u64,
) -> StandingRuntimeOwnerToken {
    match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: "default".to_string(),
            program_id: "orders-view".to_string(),
            view_id: "orders-view".to_string(),
            owner_id: owner_id.to_string(),
            ttl_ms,
        })
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => StandingRuntimeOwnerToken {
            tenant_id: claim.tenant_id,
            program_id: claim.program_id,
            view_id: claim.view_id,
            owner_id: claim.owner_id,
            owner_epoch: claim.owner_epoch,
        },
        AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
            panic!("unexpected owner conflict: {claim:?}")
        }
    }
}

fn checkpoint_pointer(
    epoch: u64,
    hash_seed: &str,
    cut: &IngestSourceCutV1,
    plan_hash: &str,
    previous_checkpoint_key: &str,
    previous_manifest_hash: &str,
) -> StandingRuntimeCheckpointPointer {
    let coverage = RuntimeCheckpointInputCoverageV1 {
        schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
        view_generation: 1,
        plan_hash: plan_hash.to_string(),
        input_catalog_epoch: cut.input_catalog_epoch,
        relations: cut
            .relations
            .iter()
            .map(|relation| RuntimeCheckpointRelationCoverageV1 {
                relation_id: relation.relation.relation_id.clone(),
                relation_version: relation.relation.relation_version.clone(),
                relation_generation: relation.relation.relation_generation,
                schema_fingerprint: relation.relation.schema_fingerprint.clone(),
                partitions: relation
                    .partitions
                    .iter()
                    .map(|partition| RuntimeCheckpointPartitionCoverageV1 {
                        stream_id: partition.stream_id.clone(),
                        stream_generation: partition.stream_generation,
                        partition_id: partition.partition_id,
                        partition_generation: partition.partition_generation,
                        covered_from_offset_inclusive: partition.base_offset_inclusive,
                        processed_offset_exclusive: partition.committed_offset_exclusive,
                    })
                    .collect(),
            })
            .collect(),
    };
    let hash = hash_seed.repeat(64);
    StandingRuntimeCheckpointPointer {
        tenant_id: "default".to_string(),
        program_id: "orders-view".to_string(),
        view_id: "orders-view".to_string(),
        checkpoint_key: format!(
            "v1/standing-runtime-checkpoints/default/orders-view/orders-view/epochs/{epoch:020}/sha256/{hash}.checkpoint.json"
        ),
        logical_epoch: epoch,
        content_hash: format!("sha256:{hash}"),
        manifest_hash: format!("sha256:{hash}"),
        output_manifest_refs: Vec::new(),
        bootstrap_generation: 1,
        plan_hash: plan_hash.to_string(),
        coverage_hash: coverage.stable_hash().unwrap(),
        input_coverage: Some(coverage),
        previous_checkpoint_key: previous_checkpoint_key.to_string(),
        previous_manifest_hash: previous_manifest_hash.to_string(),
    }
}

fn fix_request(
    control: &velorix_meta::ViewBootstrapControlV1,
    owner: StandingRuntimeOwnerToken,
) -> FixViewBootstrapActivationCutRequest {
    FixViewBootstrapActivationCutRequest {
        tenant_id: control.tenant_id.clone(),
        program_id: control.program_id.clone(),
        view_id: control.view_id.clone(),
        bootstrap_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash.clone(),
        owner,
    }
}

fn promote_request(
    control: &velorix_meta::ViewBootstrapControlV1,
    checkpoint: StandingRuntimeCheckpointPointer,
    owner: StandingRuntimeOwnerToken,
) -> PromoteViewBootstrapRequest {
    PromoteViewBootstrapRequest {
        tenant_id: control.tenant_id.clone(),
        program_id: control.program_id.clone(),
        view_id: control.view_id.clone(),
        bootstrap_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash.clone(),
        checkpoint,
        owner,
    }
}

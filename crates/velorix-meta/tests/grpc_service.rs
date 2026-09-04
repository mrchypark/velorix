use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    time::Duration,
};

use object_store::memory::InMemory as InMemoryObjectStore;
#[cfg(feature = "hiqlite-backend")]
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{metadata::MetadataValue, transport::Server, Request};
use velorix_core::standing_program::{
    RuntimeCheckpointInputCoverageV1, RuntimeCheckpointPartitionCoverageV1,
    RuntimeCheckpointRelationCoverageV1, RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
};
#[cfg(feature = "hiqlite-backend")]
use velorix_meta::HiqliteMetaStore;
use velorix_meta::{
    proto::{
        velorix_meta_server::{VelorixMeta, VelorixMetaServer},
        AcquireStandingRuntimeOwnerRequest as ProtoAcquireStandingRuntimeOwnerRequest,
        BeginViewBootstrapRequest as ProtoBeginViewBootstrapRequest,
        CaptureIngestSourceCutRequest as ProtoCaptureIngestSourceCutRequest,
        PublishStandingRuntimeCheckpointRequest, ReadMetaStoreCapabilitiesRequest,
        ReadRelationCatalogRequest, ReadStandingRuntimeCheckpointRequest,
        ReadStandingRuntimeOwnerRequest, ReadViewBootstrapRequest as ProtoReadViewBootstrapRequest,
        ReserveIngestRangeRequest, StandingRuntimeCheckpointPointer as ProtoCheckpointPointer,
        StandingRuntimeOwnerToken as ProtoOwnerToken, StoreRelationCatalogRequest,
    },
    validate_bearer_token, AcquireRelationPartitionAuthorityOutcome,
    AcquireRelationPartitionAuthorityRequest, AcquireStandingRuntimeOwnerOutcome,
    BeginViewBootstrapOutcome, BeginViewBootstrapRequest, CaptureIngestSourceCutRequest,
    CaptureRelationIngestSourceCutRequest, CommitIngestRangeOutcome,
    FixViewBootstrapActivationCutOutcome, FixViewBootstrapActivationCutRequest, GrpcMetaStore,
    InMemoryMetaStore, IngestRangeReservation, IngestSourceRelationIdentityV1, MetaGrpcService,
    MetaStore, OssMetaStore, PartitionAuthorityKey, PartitionCheckpointPointer,
    PromoteViewBootstrapOutcome, PromoteViewBootstrapRequest, PublishIngestReservationOutcome,
    PublishIngestReservationRequest, PublishPartitionCheckpointPointerOutcome,
    PublishPartitionCheckpointPointerRequest, PublishRelationIngestReservationRequest,
    PublishStandingRuntimeCheckpointOutcome, RelationPartitionAuthorityKey,
    ReserveAuthoritativeIngestRangeRequest, ReserveIngestRangeOutcome,
    ReserveRelationAuthoritativeIngestRangeRequest, StandingRuntimeCheckpointPointer,
    StandingRuntimeOwnerToken, StoreRelationCatalogOutcome,
};

mod common;

#[tokio::test]
async fn grpc_service_exposes_meta_store_capabilities() {
    let service = MetaGrpcService::new(InMemoryMetaStore::default());

    let response = service
        .read_meta_store_capabilities(Request::new(ReadMetaStoreCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner();
    let capability = response
        .standing_runtime_fencing
        .expect("capability should be present");

    assert_eq!(capability.backend_name, "in-memory");
    assert!(!capability.production_multi_writer_safe);
    assert!(capability.linearizable_owner_lease);
}

#[tokio::test]
async fn grpc_service_stores_reads_catalog_and_reserves_ingest_range() {
    let service = MetaGrpcService::new(InMemoryMetaStore::default());
    let catalog = common::orders_relation_catalog("v1");
    let catalog_json = serde_json::to_vec(&catalog).unwrap();

    let store_response = service
        .store_relation_catalog(Request::new(StoreRelationCatalogRequest {
            catalog_json: catalog_json.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    let read_response = service
        .read_relation_catalog(Request::new(ReadRelationCatalogRequest {
            relation_id: "orders".to_string(),
            relation_version: "v1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    let reserve_response = service
        .reserve_ingest_range(Request::new(ReserveIngestRangeRequest {
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 100,
            batch_key:
                "v1/ingest/orders/p=0000000000/00000000000000000000-00000000000000000100.batch"
                    .to_string(),
            payload_digest: "sha256:first".to_string(),
            relation_id: "orders".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            writer_epoch: 7,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(store_response.outcome, "created");
    assert_eq!(read_response.catalog_json, catalog_json);
    assert_eq!(reserve_response.outcome, "reserved");
}

#[tokio::test]
async fn grpc_service_requires_bearer_token_when_auth_is_configured() {
    let service =
        MetaGrpcService::with_bearer_token(InMemoryMetaStore::default(), "secret").unwrap();
    let unauthenticated = service
        .read_meta_store_capabilities(Request::new(ReadMetaStoreCapabilitiesRequest {}))
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), tonic::Code::Unauthenticated);

    let mut request = Request::new(ReadMetaStoreCapabilitiesRequest {});
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer secret").unwrap(),
    );
    let capability = service
        .read_meta_store_capabilities(request)
        .await
        .unwrap()
        .into_inner()
        .standing_runtime_fencing
        .unwrap();
    assert!(capability.control_plane_auth_enforced);
    assert!(!capability.multi_writer_fencing_safe);
    assert!(!capability.bounded_wall_clock_failover);
    assert!(!capability.production_multi_writer_safe);
}

#[tokio::test]
async fn grpc_service_rejects_every_rpc_without_valid_bearer_token() {
    let service =
        MetaGrpcService::with_bearer_token(InMemoryMetaStore::default(), "secret").unwrap();
    let catalog = common::orders_relation_catalog("v1");
    let catalog_json = serde_json::to_vec(&catalog).unwrap();

    assert_eq!(
        service
            .read_meta_store_capabilities(Request::new(ReadMetaStoreCapabilitiesRequest {}))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .store_relation_catalog(malformed_authorization_request(
                StoreRelationCatalogRequest {
                    catalog_json: catalog_json.clone(),
                }
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .read_relation_catalog(wrong_bearer_request(ReadRelationCatalogRequest {
                relation_id: "orders".to_string(),
                relation_version: "v1".to_string(),
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .reserve_ingest_range(wrong_bearer_request(reserve_ingest_range_request(&catalog)))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .commit_ingest_range(wrong_bearer_request(reserve_ingest_range_request(&catalog)))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .capture_ingest_source_cut(wrong_bearer_request(ProtoCaptureIngestSourceCutRequest {
                request_json: serde_json::to_vec(&source_cut_request(&catalog)).unwrap(),
            },))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .begin_view_bootstrap(wrong_bearer_request(ProtoBeginViewBootstrapRequest {
                request_json: serde_json::to_vec(&view_bootstrap_request(&catalog)).unwrap(),
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .read_view_bootstrap(wrong_bearer_request(ProtoReadViewBootstrapRequest {
                tenant_id: "default".to_string(),
                program_id: "orders-view".to_string(),
                view_id: "orders-view".to_string(),
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .acquire_standing_runtime_owner(wrong_bearer_request(proto_owner_request("owner-a")))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .read_standing_runtime_owner(wrong_bearer_request(proto_read_owner_request()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .publish_standing_runtime_checkpoint(wrong_bearer_request(
                proto_publish_checkpoint_request(1, "a", "owner-a", 1)
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .read_standing_runtime_checkpoint(wrong_bearer_request(proto_read_checkpoint_request()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );

    let store_response = service
        .store_relation_catalog(bearer_request(
            StoreRelationCatalogRequest {
                catalog_json: catalog_json.clone(),
            },
            "secret",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(store_response.outcome, "created");
    service
        .read_relation_catalog(bearer_request(
            ReadRelationCatalogRequest {
                relation_id: "orders".to_string(),
                relation_version: "v1".to_string(),
            },
            "secret",
        ))
        .await
        .unwrap();
    service
        .reserve_ingest_range(bearer_request(
            reserve_ingest_range_request(&catalog),
            "secret",
        ))
        .await
        .unwrap();
    let claim = service
        .acquire_standing_runtime_owner(bearer_request(proto_owner_request("owner-a"), "secret"))
        .await
        .unwrap()
        .into_inner()
        .claim
        .unwrap();
    service
        .read_standing_runtime_owner(bearer_request(proto_read_owner_request(), "secret"))
        .await
        .unwrap();
    service
        .publish_standing_runtime_checkpoint(bearer_request(
            proto_publish_checkpoint_request(1, "a", "owner-a", claim.owner_epoch),
            "secret",
        ))
        .await
        .unwrap();
    service
        .read_standing_runtime_checkpoint(bearer_request(proto_read_checkpoint_request(), "secret"))
        .await
        .unwrap();
}

#[test]
fn bearer_token_validation_rejects_empty_whitespace_and_control_characters() {
    assert!(validate_bearer_token("secret").is_ok());
    assert!(validate_bearer_token("").is_err());
    assert!(validate_bearer_token("   ").is_err());
    assert!(validate_bearer_token(" secret").is_err());
    assert!(validate_bearer_token("sec ret").is_err());
    assert!(validate_bearer_token("secret\n").is_err());
    assert!(MetaGrpcService::with_bearer_token(InMemoryMetaStore::default(), "   ").is_err());
}

#[tokio::test]
async fn grpc_meta_store_round_trips_through_service_endpoint() {
    let endpoint = spawn_meta_service().await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let catalog = common::orders_relation_catalog("v1");

    let created = store.store_relation_catalog(catalog.clone()).await.unwrap();
    let read = store.read_relation_catalog("orders", "v1").await.unwrap();
    let capability = store
        .read_meta_store_capabilities()
        .await
        .unwrap()
        .standing_runtime_fencing;

    assert_eq!(created, StoreRelationCatalogOutcome::Created);
    assert_eq!(read, catalog);
    assert_eq!(capability.backend_name, "in-memory");
    assert_eq!(capability.backend_time_source_kind, "process_clock");
    assert_eq!(
        capability.backend_time_blocked_reason,
        "in_memory_process_clock_not_backend_authority"
    );
    assert_eq!(capability.lease_authority_kind, "process_local");
    assert_eq!(capability.lease_expiry_semantics, "process_clock_ttl");
    assert!(!capability.multi_writer_fencing_safe);
    assert!(!capability.bounded_wall_clock_failover);
    assert!(!capability.production_multi_writer_safe);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_meta_store_allows_independent_rpcs_to_enter_concurrently() {
    let (endpoint, entered) = spawn_barrier_meta_service().await;
    let store = Arc::new(GrpcMetaStore::connect(endpoint).await.unwrap());

    let first_store = Arc::clone(&store);
    let first = tokio::spawn(async move { first_store.read_meta_store_capabilities().await });
    let second_store = Arc::clone(&store);
    let second = tokio::spawn(async move { second_store.read_meta_store_capabilities().await });

    let (first, second) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(first, second)
    })
    .await
    .expect("independent RPCs should not be serialized by the client");
    first.unwrap().unwrap();
    second.unwrap().unwrap();
    assert_eq!(entered.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn grpc_meta_store_reserves_commits_and_captures_ingest_source_cut() {
    let endpoint = spawn_meta_service().await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let catalog = common::orders_relation_catalog("v1");
    let reservation = ingest_range_reservation(&catalog);

    assert_eq!(
        store
            .reserve_ingest_range(reservation.clone())
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    assert_eq!(
        store.commit_ingest_range(reservation).await.unwrap(),
        CommitIngestRangeOutcome::Committed
    );
    let cut = store
        .capture_ingest_source_cut(source_cut_request(&catalog))
        .await
        .unwrap();

    assert_eq!(cut.input_catalog_epoch, 1);
    assert_eq!(cut.relations.len(), 1);
    assert_eq!(cut.relations[0].partitions.len(), 1);
    assert_eq!(cut.relations[0].partitions[0].base_offset_inclusive, 0);
    assert_eq!(
        cut.relations[0].partitions[0].committed_offset_exclusive,
        100
    );
    let bootstrap = match store
        .begin_view_bootstrap(view_bootstrap_request(&catalog))
        .await
        .unwrap()
    {
        BeginViewBootstrapOutcome::Created(control) => control,
        other => panic!("unexpected bootstrap outcome: {other:?}"),
    };
    assert_eq!(bootstrap.bootstrap_cut, cut);
    assert_eq!(
        store
            .read_view_bootstrap("default", "orders-view", "orders-view")
            .await
            .unwrap(),
        Some(bootstrap.clone())
    );

    let owner = match store
        .acquire_standing_runtime_owner(velorix_meta::AcquireStandingRuntimeOwnerRequest {
            tenant_id: "default".to_string(),
            program_id: "orders-view".to_string(),
            view_id: "orders-view".to_string(),
            owner_id: "owner-a".to_string(),
            ttl_ms: 30_000,
        })
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => StandingRuntimeOwnerToken {
            tenant_id: claim.tenant_id,
            program_id: claim.program_id,
            view_id: claim.view_id,
            owner_id: claim.owner_id,
            owner_epoch: claim.owner_epoch,
        },
        other => panic!("unexpected owner outcome: {other:?}"),
    };
    let mut pointer = checkpoint_pointer(1, "a");
    pointer.program_id = "orders-view".to_string();
    pointer.view_id = "orders-view".to_string();
    pointer.checkpoint_key = format!(
        "v1/standing-runtime-checkpoints/default/orders-view/orders-view/epochs/{:020}/sha256/{}.checkpoint.json",
        pointer.logical_epoch,
        pointer.content_hash.strip_prefix("sha256:").unwrap()
    );
    let coverage = RuntimeCheckpointInputCoverageV1 {
        schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
        view_generation: bootstrap.bootstrap_generation,
        plan_hash: bootstrap.plan_hash.clone(),
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
    pointer.bootstrap_generation = bootstrap.bootstrap_generation;
    pointer.plan_hash = bootstrap.plan_hash.clone();
    pointer.coverage_hash = coverage.stable_hash().unwrap();
    pointer.input_coverage = Some(coverage);
    store
        .publish_standing_runtime_checkpoint(
            velorix_meta::PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: pointer.clone(),
                owner: owner.clone(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .fix_view_bootstrap_activation_cut(FixViewBootstrapActivationCutRequest {
                tenant_id: "default".to_string(),
                program_id: "orders-view".to_string(),
                view_id: "orders-view".to_string(),
                bootstrap_generation: bootstrap.bootstrap_generation,
                plan_hash: bootstrap.plan_hash.clone(),
                owner: owner.clone(),
            })
            .await
            .unwrap(),
        FixViewBootstrapActivationCutOutcome::Fixed(_)
    ));
    assert!(matches!(
        store
            .promote_view_bootstrap(PromoteViewBootstrapRequest {
                tenant_id: "default".to_string(),
                program_id: "orders-view".to_string(),
                view_id: "orders-view".to_string(),
                bootstrap_generation: bootstrap.bootstrap_generation,
                plan_hash: bootstrap.plan_hash,
                checkpoint: pointer,
                owner,
            })
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Promoted(_)
    ));
}

#[tokio::test]
async fn grpc_meta_store_sends_bearer_token_to_authenticated_service() {
    let endpoint = spawn_authenticated_meta_service("secret").await;
    let unauthenticated = GrpcMetaStore::connect(endpoint.clone()).await.unwrap();
    let error = unauthenticated
        .read_meta_store_capabilities()
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .to_ascii_lowercase()
        .contains("valid authentication credentials"));

    let authenticated = GrpcMetaStore::connect_with_bearer_token(endpoint, "secret")
        .await
        .unwrap();
    let capability = authenticated
        .read_meta_store_capabilities()
        .await
        .unwrap()
        .standing_runtime_fencing;
    assert!(capability.control_plane_auth_enforced);
}

#[tokio::test]
async fn grpc_meta_store_publishes_standing_runtime_checkpoint_pointer() {
    let endpoint = spawn_meta_service().await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let mut pointer = checkpoint_pointer(1, "a");
    let output_hash = "b".repeat(64);
    pointer.output_manifest_refs = vec![format!(
        "{}v1/standing-runtime-output-manifests/default/program/view/epochs/00000000000000000001/sha256/{output_hash}.output-manifest.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
    )];
    let owner = match store
        .acquire_standing_runtime_owner(owner_request("owner-a"))
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => StandingRuntimeOwnerToken {
            tenant_id: claim.tenant_id,
            program_id: claim.program_id,
            view_id: claim.view_id,
            owner_id: claim.owner_id,
            owner_epoch: claim.owner_epoch,
        },
        other => panic!("unexpected owner outcome: {other:?}"),
    };

    let outcome = store
        .publish_standing_runtime_checkpoint(
            velorix_meta::PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: pointer.clone(),
                owner: owner.clone(),
            },
        )
        .await
        .unwrap();
    let mut successor = checkpoint_pointer(2, "c");
    successor.previous_checkpoint_key = pointer.checkpoint_key.clone();
    successor.previous_manifest_hash = pointer.manifest_hash.clone();
    let successor_outcome = store
        .publish_standing_runtime_checkpoint(
            velorix_meta::PublishStandingRuntimeCheckpointRequest {
                expected_previous: Some(pointer),
                candidate: successor.clone(),
                owner,
            },
        )
        .await
        .unwrap();
    let read = store
        .read_standing_runtime_checkpoint("default", "program", "view")
        .await
        .unwrap();

    assert_eq!(outcome, PublishStandingRuntimeCheckpointOutcome::Published);
    assert_eq!(
        successor_outcome,
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert_eq!(read, Some(successor));
}

#[tokio::test]
async fn grpc_service_rejects_mismatched_standing_runtime_checkpoint_scope() {
    let service = MetaGrpcService::new(InMemoryMetaStore::default());
    let mut pointer = proto_checkpoint_pointer(1, "a");
    pointer.view_id = "other".to_string();
    let owner = proto_owner_token("view", "owner-a", 1);

    let error = service
        .publish_standing_runtime_checkpoint(Request::new(
            PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: Some(pointer),
                owner: Some(owner),
            },
        ))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_partition_authority_round_trips_acquire_renew_and_fenced_pointer_publish() {
    let backend = InMemoryMetaStore::default();
    backend.set_partition_authority_clock_for_test(100).await;
    let endpoint = spawn_meta_service_for(backend.clone()).await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let key = PartitionAuthorityKey {
        namespace: "tenant-a".to_string(),
        view_id: "view-a".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 3,
    };

    let acquired = match store
        .acquire_partition_authority(velorix_meta::AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "writer-a".to_string(),
            current_token: None,
            ttl_ms: 10,
        })
        .await
        .unwrap()
    {
        velorix_meta::AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        outcome => panic!("expected acquire, got {outcome:?}"),
    };
    assert!(matches!(
        store
            .acquire_partition_authority(velorix_meta::AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: "writer-a".to_string(),
                current_token: None,
                ttl_ms: 10,
            })
            .await
            .unwrap(),
        velorix_meta::AcquirePartitionAuthorityOutcome::Conflict(_)
    ));
    let renewed = match store
        .acquire_partition_authority(velorix_meta::AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "writer-a".to_string(),
            current_token: Some(acquired.clone()),
            ttl_ms: 20,
        })
        .await
        .unwrap()
    {
        velorix_meta::AcquirePartitionAuthorityOutcome::Renewed(token) => token,
        outcome => panic!("expected renew, got {outcome:?}"),
    };
    assert_eq!(
        store.read_partition_authority(&key).await.unwrap(),
        Some(renewed.clone())
    );

    let first = PartitionCheckpointPointer {
        key: key.clone(),
        checkpoint_key: "checkpoints/first".to_string(),
    };
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: None,
                candidate: first.clone(),
                authority: renewed.clone(),
            })
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Published
    );
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: None,
                candidate: first.clone(),
                authority: renewed.clone(),
            })
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Duplicate
    );
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: None,
                candidate: PartitionCheckpointPointer {
                    key: key.clone(),
                    checkpoint_key: "checkpoints/conflict".to_string(),
                },
                authority: renewed.clone(),
            })
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Conflict
    );
    assert_eq!(
        store.read_partition_checkpoint_pointer(&key).await.unwrap(),
        Some(first)
    );

    backend.set_partition_authority_clock_for_test(121).await;
    let takeover = match store
        .acquire_partition_authority(velorix_meta::AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "writer-b".to_string(),
            current_token: None,
            ttl_ms: 10,
        })
        .await
        .unwrap()
    {
        velorix_meta::AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        outcome => panic!("expected takeover, got {outcome:?}"),
    };
    assert_ne!(takeover.owner_epoch, renewed.owner_epoch);
    let stale = store
        .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
            expected_previous: None,
            candidate: PartitionCheckpointPointer {
                key,
                checkpoint_key: "checkpoints/stale".to_string(),
            },
            authority: renewed,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        velorix_meta::MetaStoreError::PartitionAuthorityInvalidToken
    ));
    assert!(
        store
            .read_partition_authority_capability()
            .await
            .unwrap()
            .partition_scoped_authority
    );
}

#[tokio::test]
async fn grpc_authoritative_ingest_publication_round_trips_and_recovers() {
    let backend = InMemoryMetaStore::default();
    backend.set_partition_authority_clock_for_test(100).await;
    let endpoint = spawn_meta_service_for(backend).await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let key = PartitionAuthorityKey {
        namespace: "tenant-a".to_string(),
        view_id: "view-a".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 0,
    };
    let authority = match store
        .acquire_partition_authority(velorix_meta::AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "writer-a".to_string(),
            current_token: None,
            ttl_ms: 100,
        })
        .await
        .unwrap()
    {
        velorix_meta::AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        outcome => panic!("expected acquire, got {outcome:?}"),
    };
    let catalog = common::orders_relation_catalog("v1");
    let reservation = ingest_range_reservation(&catalog);
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
        reservation,
        authority,
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
    assert_eq!(
        store
            .publish_ingest_reservation(request.clone())
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Duplicate
    );
    let publication = store
        .read_authoritative_ingest_publication("publication-one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(publication.object_key, "objects/one");
    assert_eq!(
        store
            .list_authoritative_ingest_publications(&key)
            .await
            .unwrap(),
        vec![publication]
    );
}

#[tokio::test]
async fn grpc_relation_authority_round_trips_scope_fencing_and_publication() {
    let backend = InMemoryMetaStore::default();
    backend.set_partition_authority_clock_for_test(100).await;
    let endpoint = spawn_meta_service_for(backend.clone()).await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let key = RelationPartitionAuthorityKey {
        namespace: "tenant-a".into(),
        relation_id: "orders".into(),
        stream_id: "orders-stream".into(),
        partition_id: 2,
    };
    let authority = match store
        .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "writer-a".into(),
            current_token: None,
            ttl_ms: 10,
        })
        .await
        .unwrap()
    {
        AcquireRelationPartitionAuthorityOutcome::Acquired(token) => token,
        outcome => panic!("expected relation acquire, got {outcome:?}"),
    };
    let reservation = IngestRangeReservation {
        stream_id: "orders-stream".into(),
        partition_id: 2,
        start_offset_inclusive: 0,
        end_offset_exclusive: 10,
        batch_key: "batches/orders-0-10".into(),
        payload_digest: "sha256:orders".into(),
        relation_id: "orders".into(),
        relation_version: "v1".into(),
        schema_fingerprint: "sha256:schema".into(),
        writer_epoch: 1,
    };
    assert_eq!(
        store
            .reserve_relation_authoritative_ingest_range(
                ReserveRelationAuthoritativeIngestRangeRequest {
                    reservation: reservation.clone(),
                    authority: authority.clone(),
                },
            )
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    let mut collision = reservation.clone();
    collision.start_offset_inclusive = 10;
    collision.end_offset_exclusive = 20;
    collision.payload_digest = "sha256:other".into();
    assert_eq!(
        store
            .reserve_relation_authoritative_ingest_range(
                ReserveRelationAuthoritativeIngestRangeRequest {
                    reservation: collision,
                    authority: authority.clone(),
                },
            )
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Conflict
    );
    let mut hole = reservation.clone();
    hole.start_offset_inclusive = 10;
    hole.end_offset_exclusive = 20;
    hole.batch_key = "batches/relation-hole".into();
    store
        .reserve_relation_authoritative_ingest_range(
            ReserveRelationAuthoritativeIngestRangeRequest {
                reservation: hole,
                authority: authority.clone(),
            },
        )
        .await
        .unwrap();
    let mut third = reservation.clone();
    third.start_offset_inclusive = 20;
    third.end_offset_exclusive = 30;
    third.batch_key = "batches/relation-third".into();
    store
        .reserve_relation_authoritative_ingest_range(
            ReserveRelationAuthoritativeIngestRangeRequest {
                reservation: third.clone(),
                authority: authority.clone(),
            },
        )
        .await
        .unwrap();
    let request = PublishRelationIngestReservationRequest {
        reservation: reservation.clone(),
        authority: authority.clone(),
        request_id: "relation-publication".into(),
        request_digest: "sha256:request".into(),
        object_key: "objects/relation".into(),
        object_digest: "sha256:object".into(),
    };
    assert_eq!(
        store
            .publish_relation_ingest_reservation(request.clone())
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Committed
    );
    assert_eq!(
        store
            .publish_relation_ingest_reservation(request.clone())
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Duplicate
    );
    let third_request = PublishRelationIngestReservationRequest {
        reservation: third,
        authority: authority.clone(),
        request_id: "relation-publication-third".into(),
        request_digest: "sha256:request-third".into(),
        object_key: "objects/relation-third".into(),
        object_digest: "sha256:object-third".into(),
    };
    assert_eq!(
        store
            .publish_relation_ingest_reservation(third_request)
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Committed
    );
    let publication = store
        .read_relation_authoritative_ingest_publication("relation-publication")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(publication.object_key, "objects/relation");
    let publications = store
        .list_relation_authoritative_ingest_publications(&key)
        .await
        .unwrap();
    assert_eq!(publications.len(), 2);
    assert_eq!(publications[0], publication);
    assert_eq!(publications[1].request_id, "relation-publication-third");
    let cut = store
        .capture_relation_ingest_source_cut(CaptureRelationIngestSourceCutRequest {
            authority: key.clone(),
            relation_version: "v1".into(),
            schema_fingerprint: "sha256:schema".into(),
        })
        .await
        .unwrap();
    assert_eq!(cut.partitions[0].committed_offset_exclusive, 10);
    assert_eq!(cut.partitions[0].publications.len(), 1);
    assert_eq!(
        cut.partitions[0].publications[0].object_key,
        "objects/relation"
    );
    backend.set_partition_authority_clock_for_test(200).await;
    let takeover = store
        .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
            key,
            owner_id: "writer-b".into(),
            current_token: None,
            ttl_ms: 10,
        })
        .await
        .unwrap();
    assert!(matches!(
        takeover,
        AcquireRelationPartitionAuthorityOutcome::Acquired(_)
    ));
    let stale_result = store.publish_relation_ingest_reservation(request).await;
    assert!(matches!(
        stale_result,
        Ok(PublishIngestReservationOutcome::InvalidAuthority)
    ));
}

#[cfg(feature = "hiqlite-backend")]
#[tokio::test]
#[allow(clippy::field_reassign_with_default)]
async fn grpc_relation_authority_round_trips_through_hiqlite_backend() {
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
    config.secret_raft = "velorix-test-raft-secret".into();
    config.secret_api = "velorix-test-api-secret".into();
    config
        .enc_keys
        .append_new_random_with_id("velorix-test-key".into())
        .unwrap();
    config.health_check_delay_secs = 0;
    config.raft_config = hiqlite::NodeConfig::default_raft_config(100);
    let client = hiqlite::start_node(config).await.unwrap();
    let backend = HiqliteMetaStore::new(client.clone()).await.unwrap();
    let endpoint = spawn_meta_service_for(backend).await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let key = RelationPartitionAuthorityKey {
        namespace: "default".into(),
        relation_id: "orders".into(),
        stream_id: "orders-stream".into(),
        partition_id: 0,
    };
    let authority = match store
        .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "writer".into(),
            current_token: None,
            ttl_ms: 60_000,
        })
        .await
        .unwrap()
    {
        AcquireRelationPartitionAuthorityOutcome::Acquired(token) => token,
        outcome => panic!("expected relation authority, got {outcome:?}"),
    };
    let reservation = IngestRangeReservation {
        stream_id: "orders-stream".into(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive: 10,
        batch_key: "batch/grpc-hiqlite".into(),
        payload_digest: "sha256:payload".into(),
        relation_id: "orders".into(),
        relation_version: "v1".into(),
        schema_fingerprint: "sha256:schema".into(),
        writer_epoch: 1,
    };
    assert_eq!(
        store
            .reserve_relation_authoritative_ingest_range(
                ReserveRelationAuthoritativeIngestRangeRequest {
                    reservation: reservation.clone(),
                    authority: authority.clone(),
                },
            )
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    let request = PublishRelationIngestReservationRequest {
        reservation,
        authority,
        request_id: "grpc-hiqlite-publication".into(),
        request_digest: "sha256:request".into(),
        object_key: "objects/grpc-hiqlite".into(),
        object_digest: "sha256:object".into(),
    };
    assert_eq!(
        store
            .publish_relation_ingest_reservation(request)
            .await
            .unwrap(),
        PublishIngestReservationOutcome::Committed
    );
    assert_eq!(
        store
            .read_relation_authoritative_ingest_publication("grpc-hiqlite-publication")
            .await
            .unwrap()
            .unwrap()
            .object_key,
        "objects/grpc-hiqlite"
    );
    assert_eq!(
        store
            .list_relation_authoritative_ingest_publications(&key)
            .await
            .unwrap()
            .len(),
        1
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_partition_authority_unsupported_backend_fails_closed() {
    let endpoint =
        spawn_meta_service_for(OssMetaStore::new(Arc::new(InMemoryObjectStore::new()))).await;
    let store = GrpcMetaStore::connect(endpoint).await.unwrap();

    assert!(matches!(
        store.read_partition_authority_capability().await,
        Err(velorix_meta::MetaStoreError::UnsupportedCapability(
            "partition_authority"
        ))
    ));
}

async fn spawn_meta_service() -> String {
    spawn_meta_service_for(InMemoryMetaStore::default()).await
}

#[cfg(feature = "hiqlite-backend")]
fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

async fn spawn_meta_service_for<S>(store: S) -> String
where
    S: MetaStore,
{
    let service = MetaGrpcService::new(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(VelorixMetaServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    format!("http://{addr}")
}

async fn spawn_barrier_meta_service() -> (String, Arc<AtomicUsize>) {
    let entered = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let interceptor_entered = Arc::clone(&entered);
    let interceptor_barrier = Arc::clone(&barrier);
    let interceptor = move |request: Request<()>| {
        interceptor_entered.fetch_add(1, Ordering::SeqCst);
        interceptor_barrier.wait();
        Ok(request)
    };
    let service = VelorixMetaServer::with_interceptor(
        MetaGrpcService::new(InMemoryMetaStore::default()),
        interceptor,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    (format!("http://{addr}"), entered)
}

async fn spawn_authenticated_meta_service(token: &'static str) -> String {
    let service = MetaGrpcService::with_bearer_token(InMemoryMetaStore::default(), token).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(VelorixMetaServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    format!("http://{addr}")
}

fn checkpoint_pointer(epoch: u64, hash_seed: &str) -> StandingRuntimeCheckpointPointer {
    let proto = proto_checkpoint_pointer(epoch, hash_seed);
    StandingRuntimeCheckpointPointer {
        tenant_id: proto.tenant_id,
        program_id: proto.program_id,
        view_id: proto.view_id,
        checkpoint_key: proto.checkpoint_key,
        logical_epoch: proto.logical_epoch,
        content_hash: proto.content_hash,
        manifest_hash: proto.manifest_hash,
        output_manifest_refs: proto.output_manifest_refs,
        bootstrap_generation: proto.bootstrap_generation,
        plan_hash: proto.plan_hash,
        coverage_hash: proto.coverage_hash,
        input_coverage: None,
        previous_checkpoint_key: proto.previous_checkpoint_key,
        previous_manifest_hash: proto.previous_manifest_hash,
    }
}

fn proto_checkpoint_pointer(epoch: u64, hash_seed: &str) -> ProtoCheckpointPointer {
    let hash = format!("{hash_seed:0<64}");
    ProtoCheckpointPointer {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
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
        input_coverage_json: Vec::new(),
        previous_checkpoint_key: String::new(),
        previous_manifest_hash: String::new(),
    }
}

fn owner_request(owner_id: &str) -> velorix_meta::AcquireStandingRuntimeOwnerRequest {
    velorix_meta::AcquireStandingRuntimeOwnerRequest {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
        owner_id: owner_id.to_string(),
        ttl_ms: 30_000,
    }
}

fn proto_owner_token(view_id: &str, owner_id: &str, owner_epoch: u64) -> ProtoOwnerToken {
    ProtoOwnerToken {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: view_id.to_string(),
        owner_id: owner_id.to_string(),
        owner_epoch,
    }
}

fn bearer_request<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    request
}

fn wrong_bearer_request<T>(message: T) -> Request<T> {
    bearer_request(message, "wrong")
}

fn malformed_authorization_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Basic secret").unwrap(),
    );
    request
}

fn reserve_ingest_range_request(
    catalog: &velorix_core::relation::VelorixRelationCatalogV1,
) -> ReserveIngestRangeRequest {
    ReserveIngestRangeRequest {
        stream_id: "orders".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive: 100,
        batch_key: "v1/ingest/orders/p=0000000000/00000000000000000000-00000000000000000100.batch"
            .to_string(),
        payload_digest: "sha256:first".to_string(),
        relation_id: "orders".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
        writer_epoch: 7,
    }
}

fn ingest_range_reservation(
    catalog: &velorix_core::relation::VelorixRelationCatalogV1,
) -> IngestRangeReservation {
    IngestRangeReservation {
        stream_id: "orders".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive: 100,
        batch_key: "v1/ingest/orders/p=0000000000/00000000000000000000-00000000000000000100.batch"
            .to_string(),
        payload_digest: "sha256:first".to_string(),
        relation_id: "orders".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
        writer_epoch: 7,
    }
}

fn source_cut_request(
    catalog: &velorix_core::relation::VelorixRelationCatalogV1,
) -> CaptureIngestSourceCutRequest {
    CaptureIngestSourceCutRequest {
        relations: vec![IngestSourceRelationIdentityV1 {
            relation_id: "orders".to_string(),
            relation_version: "v1".to_string(),
            relation_generation: 1,
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
        }],
    }
}

fn view_bootstrap_request(
    catalog: &velorix_core::relation::VelorixRelationCatalogV1,
) -> BeginViewBootstrapRequest {
    BeginViewBootstrapRequest {
        tenant_id: "default".to_string(),
        program_id: "orders-view".to_string(),
        view_id: "orders-view".to_string(),
        plan_hash: "sha256:plan".to_string(),
        view_spec_json: br#"{"view_id":"orders-view"}"#.to_vec(),
        relations: source_cut_request(catalog).relations,
        view_inputs: Vec::new(),
        expected_graph_revision: 0,
    }
}

fn proto_owner_request(owner_id: &str) -> ProtoAcquireStandingRuntimeOwnerRequest {
    ProtoAcquireStandingRuntimeOwnerRequest {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
        owner_id: owner_id.to_string(),
        ttl_ms: 30_000,
    }
}

fn proto_read_owner_request() -> ReadStandingRuntimeOwnerRequest {
    ReadStandingRuntimeOwnerRequest {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
    }
}

fn proto_read_checkpoint_request() -> ReadStandingRuntimeCheckpointRequest {
    ReadStandingRuntimeCheckpointRequest {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
    }
}

fn proto_publish_checkpoint_request(
    epoch: u64,
    hash_seed: &str,
    owner_id: &str,
    owner_epoch: u64,
) -> PublishStandingRuntimeCheckpointRequest {
    PublishStandingRuntimeCheckpointRequest {
        expected_previous: None,
        candidate: Some(proto_checkpoint_pointer(epoch, hash_seed)),
        owner: Some(proto_owner_token("view", owner_id, owner_epoch)),
    }
}

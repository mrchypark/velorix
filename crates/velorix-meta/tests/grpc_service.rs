use tokio_stream::wrappers::TcpListenerStream;
use tonic::{metadata::MetadataValue, transport::Server, Request};
use velorix_meta::{
    proto::{
        velorix_meta_server::{VelorixMeta, VelorixMetaServer},
        AcquireStandingRuntimeOwnerRequest as ProtoAcquireStandingRuntimeOwnerRequest,
        PublishStandingRuntimeCheckpointRequest, ReadMetaStoreCapabilitiesRequest,
        ReadRelationCatalogRequest, ReadStandingRuntimeCheckpointRequest,
        ReadStandingRuntimeOwnerRequest, ReserveIngestRangeRequest,
        StandingRuntimeCheckpointPointer as ProtoCheckpointPointer,
        StandingRuntimeOwnerToken as ProtoOwnerToken, StoreRelationCatalogRequest,
    },
    validate_bearer_token, AcquireStandingRuntimeOwnerOutcome, GrpcMetaStore, InMemoryMetaStore,
    MetaGrpcService, MetaStore, PublishStandingRuntimeCheckpointOutcome,
    StandingRuntimeCheckpointPointer, StandingRuntimeOwnerToken, StoreRelationCatalogOutcome,
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
    assert_eq!(read, Some(pointer));
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

async fn spawn_meta_service() -> String {
    let service = MetaGrpcService::new(InMemoryMetaStore::default());
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
        output_manifest_refs: proto.output_manifest_refs,
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
        output_manifest_refs: Vec::new(),
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

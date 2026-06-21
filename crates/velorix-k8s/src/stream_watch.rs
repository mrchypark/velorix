use async_trait::async_trait;
use futures::StreamExt;
use kube::{
    api::Api,
    runtime::watcher::{self, Event},
    Client,
};
use std::sync::Arc;
use thiserror::Error;
use velorix_control::storage_admin::{
    manifest_body_digest, AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
    CheckpointManifest, CheckpointPublishError, CheckpointPublisher, IngestAdmissionCoordinator,
    IngestAdmissionReconstructionReport, ObjectStoreCapabilityProfile, RelationCatalogRegistry,
    RelationCatalogRegistryError,
};

use crate::{
    controller::{reconcile_stream, AuthoritySnapshot, ControllerAction},
    crd::{CheckpointRef, ObjectStoreAuthorityRef, RelationVersionRef, VelorixStream},
    startup::{OperatorAuthorityStartupComponents, ValidatedStartupAuthorityToken},
    status::{KubeStreamStatusApi, KubernetesStatusError, StreamStatusApi, StreamStatusWriter},
};

#[derive(Clone, Debug)]
pub enum StreamWatchEvent {
    Applied(VelorixStream),
    Deleted(VelorixStream),
}

#[async_trait]
pub trait AuthoritySnapshotProvider: Clone + Send + Sync + 'static {
    async fn snapshot_for_stream(
        &self,
        stream: &VelorixStream,
    ) -> Result<AuthoritySnapshot, StreamWatchError>;
}

#[derive(Clone, Debug)]
pub struct RelationCatalogSnapshotProvider {
    authority: ObjectStoreAuthorityRef,
    store: Arc<dyn object_store::ObjectStore>,
    capabilities: Arc<AuthoritativeObjectStoreCapabilitiesV1>,
}

#[derive(Clone, Debug)]
pub struct IngestAdmissionCoordinatorProvider {
    authority: ObjectStoreAuthorityRef,
    store: Arc<dyn object_store::ObjectStore>,
    capabilities: Arc<AuthoritativeObjectStoreCapabilitiesV1>,
}

impl RelationCatalogSnapshotProvider {
    pub(crate) fn from_authority_parts(
        _token: ValidatedStartupAuthorityToken,
        authority: ObjectStoreAuthorityRef,
        store: Arc<dyn object_store::ObjectStore>,
        capabilities: Arc<AuthoritativeObjectStoreCapabilitiesV1>,
    ) -> Self {
        Self {
            authority,
            store,
            capabilities,
        }
    }

    pub fn capabilities(&self) -> &AuthoritativeObjectStoreCapabilitiesV1 {
        &self.capabilities
    }
}

impl IngestAdmissionCoordinatorProvider {
    pub(crate) fn from_authority_parts(
        _token: ValidatedStartupAuthorityToken,
        authority: ObjectStoreAuthorityRef,
        store: Arc<dyn object_store::ObjectStore>,
        capabilities: Arc<AuthoritativeObjectStoreCapabilitiesV1>,
    ) -> Self {
        Self {
            authority,
            store,
            capabilities,
        }
    }

    pub fn authority(&self) -> &ObjectStoreAuthorityRef {
        &self.authority
    }

    pub fn capabilities(&self) -> &AuthoritativeObjectStoreCapabilitiesV1 {
        &self.capabilities
    }

    fn coordinator(&self) -> Result<IngestAdmissionCoordinator, StreamWatchError> {
        IngestAdmissionCoordinator::new_checked(Arc::clone(&self.store), &self.capabilities)
            .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }

    pub fn coordinator_without_startup_reconstruction(
        &self,
    ) -> Result<IngestAdmissionCoordinator, StreamWatchError> {
        self.coordinator()
    }

    pub async fn coordinator_after_startup_reconstruction(
        &self,
    ) -> Result<
        (
            IngestAdmissionCoordinator,
            IngestAdmissionReconstructionReport,
        ),
        StreamWatchError,
    > {
        let coordinator = self.coordinator()?;
        let report = coordinator
            .reconstruct_active_admissions()
            .await
            .map_err(|error| StreamWatchError::snapshot(error.to_string()))?;

        Ok((coordinator, report))
    }

    pub async fn startup(&self) -> Result<IngestAdmissionReconstructionReport, StreamWatchError> {
        self.coordinator_after_startup_reconstruction()
            .await
            .map(|(_, report)| report)
    }
}

#[async_trait]
impl AuthoritySnapshotProvider for RelationCatalogSnapshotProvider {
    async fn snapshot_for_stream(
        &self,
        stream: &VelorixStream,
    ) -> Result<AuthoritySnapshot, StreamWatchError> {
        if stream.spec.authority != self.authority {
            return Ok(AuthoritySnapshot::default());
        }

        let mut snapshot = AuthoritySnapshot::default().with_authority(self.authority.clone());
        match self
            .relation_catalog_registry()?
            .read(
                &stream.spec.relation.relation_id,
                &stream.spec.relation.relation_version.to_string(),
            )
            .await
        {
            Ok(catalog) => {
                let catalog_relation = RelationVersionRef {
                    relation_id: stream.spec.relation.relation_id.clone(),
                    relation_version: stream.spec.relation.relation_version,
                    schema_fingerprint: catalog.schema_fingerprint.to_string(),
                };
                snapshot = snapshot.with_relation_for_authority(&self.authority, &catalog_relation);
            }
            Err(RelationCatalogRegistryError::ObjectStore(object_store::Error::NotFound {
                ..
            })) => {}
            Err(error) => return Err(StreamWatchError::snapshot(error.to_string())),
        }

        if let Some(checkpoint) = self.latest_checkpoint_for_stream(stream).await? {
            snapshot = snapshot.with_latest_stream_checkpoint_for_authority(
                &self.authority,
                &stream.spec.stream_id,
                &stream.spec.relation,
                checkpoint,
            );
        }

        Ok(snapshot)
    }
}

impl RelationCatalogSnapshotProvider {
    fn relation_catalog_registry(&self) -> Result<RelationCatalogRegistry, StreamWatchError> {
        RelationCatalogRegistry::new_checked(
            Arc::clone(&self.store),
            self.profile_for(AuthoritativeNamespace::RelationCatalog)?,
        )
        .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }

    fn checkpoint_publisher(&self) -> Result<CheckpointPublisher, StreamWatchError> {
        CheckpointPublisher::new_checked(
            Arc::clone(&self.store),
            self.profile_for(AuthoritativeNamespace::Checkpoint)?,
        )
        .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }

    fn profile_for(
        &self,
        namespace: AuthoritativeNamespace,
    ) -> Result<&ObjectStoreCapabilityProfile, StreamWatchError> {
        self.capabilities.profiles.get(&namespace).ok_or_else(|| {
            StreamWatchError::snapshot(format!(
                "validated authority missing `{namespace}` capability evidence"
            ))
        })
    }

    async fn latest_checkpoint_for_stream(
        &self,
        stream: &VelorixStream,
    ) -> Result<Option<CheckpointRef>, StreamWatchError> {
        let publisher = self.checkpoint_publisher()?;
        let selected_manifest = publisher
            .list_published_manifests()
            .await
            .map_err(snapshot_error)?
            .into_iter()
            .filter(|manifest| {
                manifest
                    .input_ranges
                    .iter()
                    .any(|range| range.stream_id == stream.spec.stream_id)
                    && checkpoint_manifest_proves_relation_identity(manifest, &stream.spec.relation)
            })
            .max_by_key(|manifest| manifest.checkpoint_version);

        let Some(manifest) = selected_manifest else {
            return Ok(None);
        };

        match publisher
            .read_checkpoint_lifecycle_record(manifest.checkpoint_version)
            .await
        {
            Ok(record) if record.manifest_digest != digest_manifest(&manifest)? => Ok(None),
            Ok(record) => Ok(Some(CheckpointRef {
                checkpoint_version: manifest.checkpoint_version,
                manifest_digest: record.manifest_digest,
            })),
            Err(CheckpointPublishError::ObjectStore(object_store::Error::NotFound { .. })) => {
                Ok(None)
            }
            Err(error) => Err(snapshot_error(error)),
        }
    }
}

fn checkpoint_manifest_proves_relation_identity(
    _manifest: &CheckpointManifest,
    _relation: &RelationVersionRef,
) -> bool {
    // CheckpointManifest v1 has no relation id/version/fingerprint fields. A matching
    // input stream alone is not enough evidence for Kubernetes relation-scoped status.
    false
}

fn snapshot_error(error: CheckpointPublishError) -> StreamWatchError {
    StreamWatchError::snapshot(error.to_string())
}

fn digest_manifest(manifest: &CheckpointManifest) -> Result<String, StreamWatchError> {
    serde_json::to_vec(manifest)
        .map(|bytes| manifest_body_digest(&bytes))
        .map_err(|error| StreamWatchError::snapshot(error.to_string()))
}

pub async fn handle_stream_event<P, A>(
    event: StreamWatchEvent,
    snapshot_provider: &P,
    status_writer: &StreamStatusWriter<A>,
) -> Result<(), StreamWatchError>
where
    P: AuthoritySnapshotProvider,
    A: StreamStatusApi,
{
    let StreamWatchEvent::Applied(stream) = event else {
        return Ok(());
    };

    let snapshot = snapshot_provider.snapshot_for_stream(&stream).await?;
    match reconcile_stream(&stream, &snapshot).action {
        ControllerAction::WriteStreamStatus(status) => {
            status_writer.write_stream_status(&stream, status).await?;
        }
    }
    Ok(())
}

pub async fn watch_streams<P, A>(
    client: Client,
    namespace: &str,
    snapshot_provider: P,
    status_writer: StreamStatusWriter<A>,
) -> Result<(), StreamWatchError>
where
    P: AuthoritySnapshotProvider,
    A: StreamStatusApi,
{
    let api: Api<VelorixStream> = Api::namespaced(client, namespace);
    let mut events = Box::pin(watcher::watcher(api, watcher::Config::default()));

    while let Some(event) = events.next().await {
        let event = event.map_err(StreamWatchError::watcher)?;
        if let Some(event) = stream_watch_event(event) {
            handle_stream_event(event, &snapshot_provider, &status_writer).await?;
        }
    }
    Ok(())
}

pub async fn watch_streams_with_kubernetes_runtime(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
) -> Result<(), StreamWatchError> {
    let snapshot_provider = startup_components.relation_snapshot_provider();
    let status_writer = StreamStatusWriter::new(KubeStreamStatusApi::new(client.clone()));

    watch_streams(client, namespace, snapshot_provider, status_writer).await
}

fn stream_watch_event(event: Event<VelorixStream>) -> Option<StreamWatchEvent> {
    match event {
        Event::Apply(stream) | Event::InitApply(stream) => Some(StreamWatchEvent::Applied(stream)),
        Event::Delete(stream) => Some(StreamWatchEvent::Deleted(stream)),
        Event::Init | Event::InitDone => None,
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum StreamWatchError {
    #[error("authority snapshot failed: {message}")]
    Snapshot { message: String },
    #[error(transparent)]
    Status(#[from] KubernetesStatusError),
    #[error("kubernetes stream watcher failed: {message}")]
    Watcher { message: String },
}

impl StreamWatchError {
    pub fn snapshot(message: impl Into<String>) -> Self {
        Self::Snapshot {
            message: message.into(),
        }
    }

    fn watcher(error: watcher::Error) -> Self {
        Self::Watcher {
            message: error.to_string(),
        }
    }
}

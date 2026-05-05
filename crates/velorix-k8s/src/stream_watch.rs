use async_trait::async_trait;
use futures::StreamExt;
use kube::{
    api::Api,
    runtime::watcher::{self, Event},
    Client,
};
use thiserror::Error;

use crate::{
    controller::{reconcile_stream, AuthoritySnapshot, ControllerAction},
    crd::VelorixStream,
    status::{KubernetesStatusError, StreamStatusApi, StreamStatusWriter},
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

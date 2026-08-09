use async_trait::async_trait;
use kube::{
    api::{Api, Patch, PatchParams},
    Client, ResourceExt,
};
use serde_json::{json, Value};
use thiserror::Error;

use crate::crd::{StreamStatus, VelorixStream};

#[derive(Clone, Debug)]
pub struct StreamStatusWriter<A> {
    api: A,
}

impl<A> StreamStatusWriter<A> {
    pub fn new(api: A) -> Self {
        Self { api }
    }
}

impl<A> StreamStatusWriter<A>
where
    A: StreamStatusApi,
{
    pub async fn write_stream_status(
        &self,
        stream: &VelorixStream,
        status: StreamStatus,
    ) -> Result<(), KubernetesStatusError> {
        let namespace = stream
            .namespace()
            .ok_or(KubernetesStatusError::MissingObjectField {
                field: "metadata.namespace",
            })?;
        let name = stream.name_any();

        self.api
            .patch_status(&namespace, &name, json!({ "status": status }))
            .await
    }
}

#[async_trait]
pub trait StreamStatusApi: Clone + Send + Sync + 'static {
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        patch: Value,
    ) -> Result<(), KubernetesStatusError>;
}

#[derive(Clone)]
pub struct KubeStreamStatusApi {
    client: Client,
}

impl KubeStreamStatusApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl StreamStatusApi for KubeStreamStatusApi {
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        patch: Value,
    ) -> Result<(), KubernetesStatusError> {
        let api: Api<VelorixStream> = Api::namespaced(self.client.clone(), namespace);
        api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map(|_| ())
            .map_err(|error| KubernetesStatusError::api("patch_status", error))
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum KubernetesStatusError {
    #[error("missing kubernetes stream object field `{field}`")]
    MissingObjectField { field: &'static str },
    #[error("kubernetes status API {operation} failed: {message}")]
    Api {
        operation: &'static str,
        message: String,
    },
}

impl KubernetesStatusError {
    fn api(operation: &'static str, error: kube::Error) -> Self {
        Self::Api {
            operation,
            message: error.to_string(),
        }
    }
}

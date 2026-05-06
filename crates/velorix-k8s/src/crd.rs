use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const API_GROUP: &str = "control.velorix.io";
const API_VERSION: &str = "v1alpha1";

#[derive(
    Clone, Debug, Default, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreAuthorityRef {
    pub store_id: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationVersionRef {
    pub relation_id: String,
    pub relation_version: u64,
    pub schema_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRef {
    pub checkpoint_version: u64,
    pub manifest_digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkArtifactRef {
    pub object_key: String,
    pub digest: String,
    pub schema_version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerEpochStatus {
    pub stream_id: String,
    pub partition_id: u32,
    pub owner_id: String,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum ConditionState {
    True,
    False,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: ConditionState,
    pub reason: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseStatus {
    pub observed_generation: Option<i64>,
    pub readiness: Option<VelorixCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamStatus {
    pub observed_generation: Option<i64>,
    pub last_accepted_relation_schema_fingerprint: Option<String>,
    pub latest_published_checkpoint: Option<CheckpointRef>,
    pub readiness: Option<VelorixCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableStatus {
    pub observed_generation: Option<i64>,
    pub last_accepted_relation_schema_fingerprint: Option<String>,
    pub readiness: Option<VelorixCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerShardStatus {
    pub observed_generation: Option<i64>,
    pub current_owner_epoch: Option<OwnerEpochStatus>,
    pub readiness: Option<VelorixCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointPolicyStatus {
    pub observed_generation: Option<i64>,
    pub latest_published_checkpoint: Option<CheckpointRef>,
    pub readiness: Option<VelorixCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkGateStatus {
    pub observed_generation: Option<i64>,
    pub latest_result: Option<BenchmarkArtifactRef>,
    pub readiness: Option<VelorixCondition>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, CustomResource)]
#[kube(
    group = "control.velorix.io",
    version = "v1alpha1",
    kind = "VelorixDatabase",
    plural = "velorixdatabases",
    namespaced,
    status = "DatabaseStatus"
)]
#[serde(deny_unknown_fields)]
pub struct VelorixDatabaseSpec {
    pub database_id: String,
    pub authority: ObjectStoreAuthorityRef,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, CustomResource)]
#[kube(
    group = "control.velorix.io",
    version = "v1alpha1",
    kind = "VelorixStream",
    plural = "velorixstreams",
    namespaced,
    status = "StreamStatus"
)]
#[serde(deny_unknown_fields)]
pub struct VelorixStreamSpec {
    pub stream_id: String,
    pub database_id: String,
    pub relation: RelationVersionRef,
    pub authority: ObjectStoreAuthorityRef,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, CustomResource)]
#[kube(
    group = "control.velorix.io",
    version = "v1alpha1",
    kind = "VelorixTable",
    plural = "velorixtables",
    namespaced,
    status = "TableStatus"
)]
#[serde(deny_unknown_fields)]
pub struct VelorixTableSpec {
    pub table_id: String,
    pub tenant_id: String,
    pub relation: RelationVersionRef,
    pub authority: ObjectStoreAuthorityRef,
    pub query_policy_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, CustomResource)]
#[kube(
    group = "control.velorix.io",
    version = "v1alpha1",
    kind = "VelorixWorkerShard",
    plural = "velorixworkershards",
    namespaced,
    status = "WorkerShardStatus"
)]
#[serde(deny_unknown_fields)]
pub struct VelorixWorkerShardSpec {
    pub worker_id: String,
    pub view_id: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub desired_owner_id: String,
    pub authority: ObjectStoreAuthorityRef,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, CustomResource)]
#[kube(
    group = "control.velorix.io",
    version = "v1alpha1",
    kind = "VelorixCheckpointPolicy",
    plural = "velorixcheckpointpolicies",
    namespaced,
    status = "CheckpointPolicyStatus"
)]
#[serde(deny_unknown_fields)]
pub struct VelorixCheckpointPolicySpec {
    pub policy_id: String,
    pub database_id: String,
    pub stream_id: String,
    pub authority: ObjectStoreAuthorityRef,
    pub min_interval_ms: u64,
    pub retain_checkpoints: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, CustomResource)]
#[kube(
    group = "control.velorix.io",
    version = "v1alpha1",
    kind = "VelorixBenchmarkGate",
    plural = "velorixbenchmarkgates",
    namespaced,
    status = "BenchmarkGateStatus"
)]
#[serde(deny_unknown_fields)]
pub struct VelorixBenchmarkGateSpec {
    pub gate_id: String,
    pub gate_level: String,
    pub backend: String,
    pub authority: ObjectStoreAuthorityRef,
    pub baseline_ref: BenchmarkArtifactRef,
    pub result_ref: BenchmarkArtifactRef,
}

pub fn api_group() -> &'static str {
    API_GROUP
}

pub fn api_version() -> &'static str {
    API_VERSION
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixView {
    pub api_version: String,
    pub kind: String,
    pub metadata: ContractMetadata,
    pub spec_version: u32,
    pub spec: VelorixViewSpec,
    pub status: VelorixViewStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractMetadata {
    pub name: String,
    pub namespace: String,
    pub generation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixViewSpec {
    pub view_id: String,
    pub worker: WorkerIntent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIntent {
    pub stream_id: String,
    pub partition_id: u32,
    pub owner_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixViewStatus {
    pub observed_generation: Option<u64>,
    pub observed_checkpoint_version: Option<u64>,
    pub observed_owner_epoch: Option<u64>,
    pub conditions: Vec<VelorixCondition>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: ConditionStatus,
    pub reason: String,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

use crate::object_key::ObjectKey;
use serde::{Deserialize, Serialize};

pub const GC_RUN_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GarbageCollectionPolicy {
    pub retain_latest_manifests: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GarbageCollectionPlan {
    pub retained_manifest_versions: Vec<u64>,
    pub candidates: Vec<GarbageCollectionCandidate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GarbageCollectionReport {
    pub deleted: Vec<GarbageCollectionCandidate>,
    pub skipped: Vec<GarbageCollectionCandidate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GarbageCollectionRunV1 {
    pub schema_version: u16,
    pub run_id: String,
    pub policy: GarbageCollectionPolicy,
    pub plan: GarbageCollectionPlan,
    pub report: GarbageCollectionReport,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GarbageCollectionCandidate {
    pub object_key: ObjectKey,
    pub kind: GarbageCollectionCandidateKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GarbageCollectionCandidateKind {
    RawStateObject,
    SlateDbStateRef,
    OutputObject,
}

impl GarbageCollectionCandidateKind {
    pub(crate) fn matches_key(self, object_key: &ObjectKey) -> bool {
        match self {
            Self::RawStateObject => object_key.as_str().starts_with("v1/state/"),
            Self::SlateDbStateRef => object_key.as_str().starts_with("v1/state/"),
            Self::OutputObject => object_key.as_str().starts_with("v1/outputs/"),
        }
    }
}

use crate::object_key::ObjectKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GarbageCollectionPolicy {
    pub retain_latest_manifests: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GarbageCollectionPlan {
    pub retained_manifest_versions: Vec<u64>,
    pub candidates: Vec<GarbageCollectionCandidate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GarbageCollectionReport {
    pub deleted: Vec<GarbageCollectionCandidate>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GarbageCollectionCandidate {
    pub object_key: ObjectKey,
    pub kind: GarbageCollectionCandidateKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GarbageCollectionCandidateKind {
    RawStateObject,
    OutputObject,
}

impl GarbageCollectionCandidateKind {
    pub(crate) fn matches_key(self, object_key: &ObjectKey) -> bool {
        match self {
            Self::RawStateObject => object_key.as_str().starts_with("v1/state/"),
            Self::OutputObject => object_key.as_str().starts_with("v1/outputs/"),
        }
    }
}

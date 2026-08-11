use serde::{Deserialize, Serialize};

use crate::{
    require_non_empty, CaptureIngestSourceCutRequest, IngestSourceCutV1,
    IngestSourceRelationIdentityV1, MetaStoreError, StandingRuntimeCheckpointPointer,
    StandingRuntimeOwnerToken,
};

pub const VIEW_BOOTSTRAP_CONTROL_SCHEMA_VERSION_V1: u32 = 1;
pub const INITIAL_VIEW_BOOTSTRAP_GENERATION: u64 = 1;
pub const DEPENDENCY_GRAPH_SCHEMA_VERSION_V1: u32 = 1;

/// Tenant-scoped dependency graph with monotonically increasing revision.
///
/// Each mutation (view admission, deletion, edge change) increments the revision
/// via a CAS transaction. This prevents TOCTOU races and ensures that concurrent
/// admissions cannot create cycles.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyGraphV1 {
    pub schema_version: u32,
    pub tenant_id: String,
    pub graph_revision: u64,
    pub edges: Vec<DependencyGraphEdgeV1>,
}

/// An immutable edge in the dependency graph.
///
/// Created during view admission under a specific graph revision.
/// Each edge captures the full producer identity at admission time.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyGraphEdgeV1 {
    /// Unique identifier for this edge within the graph.
    pub input_edge_id: String,
    /// Graph revision when this edge was created.
    pub graph_revision: u64,
    /// Consumer view's input port identifier.
    pub consumer_input_port: String,
    /// Producer view's tenant ID.
    pub producer_tenant_id: String,
    /// Producer view's program ID.
    pub producer_program_id: String,
    /// Producer view's view ID.
    pub producer_view_id: String,
    /// Producer view's generation at admission time.
    pub producer_generation: u64,
    /// Producer view's logical plan hash.
    pub producer_plan_hash: String,
    /// Output schema hash from the producer's PublishedRelationBindingV1.
    pub output_schema_hash: String,
    /// Key descriptor hash from the producer's PublishedRelationBindingV1.
    pub key_descriptor_hash: String,
    /// Output stream ID from the producer's PublishedRelationBindingV1.
    pub output_stream_id: String,
    /// Delta codec identity from the producer's PublishedRelationBindingV1.
    pub delta_codec_identity: String,
    /// Frontier kind from the producer's PublishedRelationBindingV1.
    pub frontier_kind: String,
    /// Timestamp when this edge was created.
    pub created_at_ms: u64,
}

/// Outcome of a dependency graph CAS operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishDependencyGraphOutcome {
    /// Graph was successfully published with new revision.
    Published(DependencyGraphV1),
    /// CAS conflict: expected revision didn't match current.
    Conflict {
        current_revision: u64,
        expected_revision: u64,
    },
}

impl DependencyGraphV1 {
    /// Creates a new empty dependency graph for a tenant.
    pub fn new(tenant_id: String) -> Self {
        Self {
            schema_version: DEPENDENCY_GRAPH_SCHEMA_VERSION_V1,
            tenant_id,
            graph_revision: 0,
            edges: Vec::new(),
        }
    }

    /// Returns the current graph revision.
    pub fn revision(&self) -> u64 {
        self.graph_revision
    }

    /// Returns all edges in the graph.
    pub fn edges(&self) -> &[DependencyGraphEdgeV1] {
        &self.edges
    }

    /// Checks if adding the given edges would create a cycle.
    /// Returns Ok(()) if acyclic, Err(cycle_path) if cyclic.
    pub fn validate_acyclicity(&self, new_edges: &[DependencyGraphEdgeV1]) -> Result<(), Vec<String>> {
        // Build adjacency list: producer_view_id -> set of consumer_view_ids
        let mut graph: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();

        // Add existing edges
        for edge in &self.edges {
            graph.entry(edge.producer_view_id.clone())
                .or_default()
                .push(edge.consumer_input_port.clone());
        }

        // Add new edges
        for edge in new_edges {
            graph.entry(edge.producer_view_id.clone())
                .or_default()
                .push(edge.consumer_input_port.clone());
        }

        // DFS cycle detection
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        for node in graph.keys() {
            if !visited.contains(node) {
                if let Some(cycle) = dfs_cycle_detection(node, &graph, &mut visited, &mut rec_stack) {
                    return Err(cycle);
                }
            }
        }

        Ok(())
    }

    /// Creates a new revision with the given edges added.
    pub fn with_edges_added(&self, new_edges: Vec<DependencyGraphEdgeV1>) -> Self {
        let mut graph = self.clone();
        graph.edges.extend(new_edges);
        graph.graph_revision += 1;
        graph
    }
}

/// DFS-based cycle detection.
/// Returns Some(cycle_path) if a cycle is found, None otherwise.
fn dfs_cycle_detection(
    node: &str,
    graph: &std::collections::HashMap<String, Vec<String>>,
    visited: &mut std::collections::HashSet<String>,
    rec_stack: &mut std::collections::HashSet<String>,
) -> Option<Vec<String>> {
    visited.insert(node.to_string());
    rec_stack.insert(node.to_string());

    if let Some(neighbors) = graph.get(node) {
        for neighbor in neighbors {
            if !visited.contains(neighbor) {
                if let Some(cycle) = dfs_cycle_detection(neighbor, graph, visited, rec_stack) {
                    let mut result = vec![node.to_string()];
                    result.extend(cycle);
                    return Some(result);
                }
            } else if rec_stack.contains(neighbor) {
                return Some(vec![node.to_string(), neighbor.to_string()]);
            }
        }
    }

    rec_stack.remove(node);
    None
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum ViewBootstrapLifecycleV1 {
    Bootstrapping,
    Active,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BeginViewBootstrapRequest {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub plan_hash: String,
    pub view_spec_json: Vec<u8>,
    pub relations: Vec<IngestSourceRelationIdentityV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewBootstrapControlV1 {
    pub schema_version: u32,
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub bootstrap_generation: u64,
    pub plan_hash: String,
    pub view_spec_json: Vec<u8>,
    pub lifecycle: ViewBootstrapLifecycleV1,
    pub bootstrap_cut: IngestSourceCutV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_cut: Option<IngestSourceCutV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_checkpoint: Option<StandingRuntimeCheckpointPointer>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixViewBootstrapActivationCutRequest {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub bootstrap_generation: u64,
    pub plan_hash: String,
    pub owner: StandingRuntimeOwnerToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixViewBootstrapActivationCutOutcome {
    Fixed(ViewBootstrapControlV1),
    Duplicate(ViewBootstrapControlV1),
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PromoteViewBootstrapRequest {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub bootstrap_generation: u64,
    pub plan_hash: String,
    pub checkpoint: StandingRuntimeCheckpointPointer,
    pub owner: StandingRuntimeOwnerToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromoteViewBootstrapOutcome {
    Promoted(ViewBootstrapControlV1),
    Duplicate(ViewBootstrapControlV1),
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeginViewBootstrapOutcome {
    Created(ViewBootstrapControlV1),
    Duplicate(ViewBootstrapControlV1),
    Conflict,
}

impl BeginViewBootstrapRequest {
    pub(crate) fn validate(&self) -> Result<(), MetaStoreError> {
        require_non_empty("tenant_id", &self.tenant_id)?;
        require_non_empty("program_id", &self.program_id)?;
        require_non_empty("view_id", &self.view_id)?;
        require_non_empty("plan_hash", &self.plan_hash)?;
        if self.view_spec_json.is_empty() {
            return Err(MetaStoreError::EmptyField {
                field: "view_spec_json",
            });
        }
        if self.relations.is_empty() {
            return Err(MetaStoreError::EmptyField { field: "relations" });
        }
        CaptureIngestSourceCutRequest {
            relations: self.relations.clone(),
        }
        .validate()
    }

    pub(crate) fn matches(&self, control: &ViewBootstrapControlV1) -> bool {
        self.tenant_id == control.tenant_id
            && self.program_id == control.program_id
            && self.view_id == control.view_id
            && self.plan_hash == control.plan_hash
            && self.view_spec_json == control.view_spec_json
            && self.relations
                == control
                    .bootstrap_cut
                    .relations
                    .iter()
                    .map(|cut| cut.relation.clone())
                    .collect::<Vec<_>>()
    }
}

impl FixViewBootstrapActivationCutRequest {
    pub(crate) fn validate(&self) -> Result<(), MetaStoreError> {
        validate_activation_scope(
            &self.tenant_id,
            &self.program_id,
            &self.view_id,
            self.bootstrap_generation,
            &self.plan_hash,
        )?;
        self.owner.validate()
    }
}

impl PromoteViewBootstrapRequest {
    pub(crate) fn validate(&self) -> Result<(), MetaStoreError> {
        validate_activation_scope(
            &self.tenant_id,
            &self.program_id,
            &self.view_id,
            self.bootstrap_generation,
            &self.plan_hash,
        )?;
        self.owner.validate()?;
        self.checkpoint.validate()
    }
}

fn validate_activation_scope(
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    bootstrap_generation: u64,
    plan_hash: &str,
) -> Result<(), MetaStoreError> {
    require_non_empty("tenant_id", tenant_id)?;
    require_non_empty("program_id", program_id)?;
    require_non_empty("view_id", view_id)?;
    require_non_empty("plan_hash", plan_hash)?;
    if bootstrap_generation == 0 {
        return Err(MetaStoreError::IntegerOutOfRange {
            field: "bootstrap_generation",
            value: bootstrap_generation,
        });
    }
    Ok(())
}

pub(crate) fn bootstrap_control(
    request: BeginViewBootstrapRequest,
    bootstrap_cut: IngestSourceCutV1,
) -> ViewBootstrapControlV1 {
    ViewBootstrapControlV1 {
        schema_version: VIEW_BOOTSTRAP_CONTROL_SCHEMA_VERSION_V1,
        tenant_id: request.tenant_id,
        program_id: request.program_id,
        view_id: request.view_id,
        bootstrap_generation: INITIAL_VIEW_BOOTSTRAP_GENERATION,
        plan_hash: request.plan_hash,
        view_spec_json: request.view_spec_json,
        lifecycle: ViewBootstrapLifecycleV1::Bootstrapping,
        bootstrap_cut,
        activation_cut: None,
        active_checkpoint: None,
    }
}

pub(crate) fn checkpoint_covers_source_cut(
    checkpoint: &StandingRuntimeCheckpointPointer,
    cut: &IngestSourceCutV1,
) -> bool {
    let Some(coverage) = checkpoint.input_coverage.as_ref() else {
        return false;
    };
    if coverage.input_catalog_epoch < cut.input_catalog_epoch {
        return false;
    }
    cut.relations.iter().all(|cut_relation| {
        coverage.relations.iter().any(|covered_relation| {
            covered_relation.relation_id == cut_relation.relation.relation_id
                && covered_relation.relation_version == cut_relation.relation.relation_version
                && covered_relation.relation_generation == cut_relation.relation.relation_generation
                && covered_relation.schema_fingerprint == cut_relation.relation.schema_fingerprint
                && cut_relation.partitions.iter().all(|cut_partition| {
                    covered_relation.partitions.iter().any(|covered_partition| {
                        covered_partition.stream_id == cut_partition.stream_id
                            && covered_partition.stream_generation
                                == cut_partition.stream_generation
                            && covered_partition.partition_id == cut_partition.partition_id
                            && covered_partition.partition_generation
                                == cut_partition.partition_generation
                            && covered_partition.covered_from_offset_inclusive
                                == cut_partition.base_offset_inclusive
                            && covered_partition.processed_offset_exclusive
                                >= cut_partition.committed_offset_exclusive
                    })
                })
        })
    })
}

pub(crate) fn source_cut_covers(
    candidate: &IngestSourceCutV1,
    required: &IngestSourceCutV1,
) -> bool {
    candidate.input_catalog_epoch >= required.input_catalog_epoch
        && required.relations.iter().all(|required_relation| {
            candidate.relations.iter().any(|candidate_relation| {
                candidate_relation.relation == required_relation.relation
                    && required_relation
                        .partitions
                        .iter()
                        .all(|required_partition| {
                            candidate_relation
                                .partitions
                                .iter()
                                .any(|candidate_partition| {
                                    candidate_partition.stream_id == required_partition.stream_id
                                        && candidate_partition.stream_generation
                                            == required_partition.stream_generation
                                        && candidate_partition.partition_id
                                            == required_partition.partition_id
                                        && candidate_partition.partition_generation
                                            == required_partition.partition_generation
                                        && candidate_partition.base_offset_inclusive
                                            == required_partition.base_offset_inclusive
                                        && candidate_partition.committed_offset_exclusive
                                            >= required_partition.committed_offset_exclusive
                                })
                        })
            })
        })
}

#[cfg(test)]
mod tests {
    use velorix_core::standing_program::{
        RuntimeCheckpointInputCoverageV1, RuntimeCheckpointPartitionCoverageV1,
        RuntimeCheckpointRelationCoverageV1, RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
    };

    use super::*;
    use crate::{IngestSourcePartitionCutV1, IngestSourceRelationCutV1};

    fn cut() -> IngestSourceCutV1 {
        IngestSourceCutV1 {
            schema_version: 1,
            input_catalog_epoch: 7,
            relations: vec![IngestSourceRelationCutV1 {
                relation: IngestSourceRelationIdentityV1 {
                    relation_id: "orders".to_string(),
                    relation_version: "v1".to_string(),
                    relation_generation: 2,
                    schema_fingerprint: "sha256:schema".to_string(),
                },
                partitions: vec![IngestSourcePartitionCutV1 {
                    stream_id: "orders".to_string(),
                    stream_generation: 3,
                    partition_id: 4,
                    partition_generation: 5,
                    base_offset_inclusive: 10,
                    committed_offset_exclusive: 20,
                }],
            }],
        }
    }

    fn pointer() -> StandingRuntimeCheckpointPointer {
        let coverage = RuntimeCheckpointInputCoverageV1 {
            schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
            view_generation: 6,
            plan_hash: "sha256:plan".to_string(),
            input_catalog_epoch: 7,
            relations: vec![RuntimeCheckpointRelationCoverageV1 {
                relation_id: "orders".to_string(),
                relation_version: "v1".to_string(),
                relation_generation: 2,
                schema_fingerprint: "sha256:schema".to_string(),
                partitions: vec![RuntimeCheckpointPartitionCoverageV1 {
                    stream_id: "orders".to_string(),
                    stream_generation: 3,
                    partition_id: 4,
                    partition_generation: 5,
                    covered_from_offset_inclusive: 10,
                    processed_offset_exclusive: 20,
                }],
            }],
        };
        let coverage_hash = coverage.stable_hash().unwrap();
        StandingRuntimeCheckpointPointer {
            tenant_id: "default".to_string(),
            program_id: "program".to_string(),
            view_id: "view".to_string(),
            checkpoint_key: format!(
                "v1/standing-runtime-checkpoints/default/program/view/epochs/{:020}/sha256/{}.checkpoint.json",
                1,
                "a".repeat(64)
            ),
            logical_epoch: 1,
            content_hash: format!("sha256:{}", "a".repeat(64)),
            manifest_hash: format!("sha256:{}", "b".repeat(64)),
            output_manifest_refs: Vec::new(),
            bootstrap_generation: 6,
            plan_hash: "sha256:plan".to_string(),
            coverage_hash,
            input_coverage: Some(coverage),
            previous_checkpoint_key: String::new(),
            previous_manifest_hash: String::new(),
        }
    }

    #[test]
    fn checkpoint_coverage_requires_every_cut_identity_and_frontier() {
        let required = cut();
        let valid = pointer();
        assert!(checkpoint_covers_source_cut(&valid, &required));

        let mut cases = Vec::new();
        let mut candidate = valid.clone();
        candidate
            .input_coverage
            .as_mut()
            .unwrap()
            .input_catalog_epoch = 6;
        cases.push(candidate);
        let mut candidate = valid.clone();
        candidate.input_coverage.as_mut().unwrap().relations[0].relation_generation = 1;
        cases.push(candidate);
        let mut candidate = valid.clone();
        candidate.input_coverage.as_mut().unwrap().relations[0].schema_fingerprint =
            "sha256:other".to_string();
        cases.push(candidate);
        let mut candidate = valid.clone();
        candidate.input_coverage.as_mut().unwrap().relations[0].partitions[0].stream_generation = 2;
        cases.push(candidate);
        let mut candidate = valid.clone();
        candidate.input_coverage.as_mut().unwrap().relations[0].partitions[0]
            .partition_generation = 4;
        cases.push(candidate);
        let mut candidate = valid.clone();
        candidate.input_coverage.as_mut().unwrap().relations[0].partitions[0]
            .covered_from_offset_inclusive = 11;
        cases.push(candidate);
        let mut candidate = valid.clone();
        candidate.input_coverage.as_mut().unwrap().relations[0].partitions[0]
            .processed_offset_exclusive = 19;
        cases.push(candidate);
        let mut candidate = valid;
        candidate.input_coverage.as_mut().unwrap().relations.clear();
        cases.push(candidate);

        for candidate in cases {
            assert!(!checkpoint_covers_source_cut(&candidate, &required));
        }
    }

    #[test]
    fn activation_cut_must_preserve_bootstrap_identity_and_base() {
        let required = cut();
        assert!(source_cut_covers(&required, &required));

        let mut lower_catalog = required.clone();
        lower_catalog.input_catalog_epoch -= 1;
        assert!(!source_cut_covers(&lower_catalog, &required));

        let mut changed_identity = required.clone();
        changed_identity.relations[0].relation.relation_generation += 1;
        assert!(!source_cut_covers(&changed_identity, &required));

        let mut changed_base = required.clone();
        changed_base.relations[0].partitions[0].base_offset_inclusive += 1;
        assert!(!source_cut_covers(&changed_base, &required));

        let mut lower_frontier = required.clone();
        lower_frontier.relations[0].partitions[0].committed_offset_exclusive -= 1;
        assert!(!source_cut_covers(&lower_frontier, &required));
    }
}

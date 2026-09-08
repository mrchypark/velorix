use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    require_non_empty, IngestRangeReservation, MetaStoreError,
    RelationAuthoritativeIngestPublication, RelationPartitionAuthorityKey,
};

pub const INGEST_SOURCE_CUT_SCHEMA_VERSION_V1: u32 = 1;
pub const INGEST_SOURCE_IDENTITY_GENERATION_V1: u64 = 1;
pub const RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationIngestPublicationRefV1 {
    pub request_id: String,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub batch_key: String,
    pub payload_digest: String,
    pub object_key: String,
    pub object_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationIngestPartitionCutV1 {
    pub stream_id: String,
    pub partition_id: u32,
    pub base_offset_inclusive: u64,
    pub committed_offset_exclusive: u64,
    pub publications: Vec<RelationIngestPublicationRefV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationIngestSourceCutV1 {
    pub schema_version: u32,
    pub namespace: String,
    pub relation_id: String,
    pub partitions: Vec<RelationIngestPartitionCutV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationIngestSourceIdentityCutV1 {
    pub relation: IngestSourceRelationIdentityV1,
    pub cut: RelationIngestSourceCutV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureRelationIngestSourceCutRequest {
    pub authority: RelationPartitionAuthorityKey,
    pub relation_version: String,
    pub schema_fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureRelationIngestSourceCutsRequest {
    pub namespace: String,
    pub relations: Vec<IngestSourceRelationIdentityV1>,
}

impl CaptureRelationIngestSourceCutsRequest {
    pub(crate) fn validate(&self) -> Result<(), MetaStoreError> {
        require_non_empty("namespace", &self.namespace)?;
        if self.relations.is_empty() {
            return Err(MetaStoreError::EmptyField { field: "relations" });
        }
        let mut seen = BTreeSet::new();
        for relation in &self.relations {
            relation.validate()?;
            if relation.relation_generation != INGEST_SOURCE_IDENTITY_GENERATION_V1 {
                return Err(MetaStoreError::UnsupportedRelationGeneration {
                    generation: relation.relation_generation,
                });
            }
            if !seen.insert(relation) {
                return Err(MetaStoreError::DuplicateSourceCutRelation {
                    relation_id: relation.relation_id.clone(),
                    relation_version: relation.relation_version.clone(),
                });
            }
        }
        Ok(())
    }
}

impl CaptureRelationIngestSourceCutRequest {
    pub(crate) fn validate(&self) -> Result<(), MetaStoreError> {
        self.authority.validate()?;
        require_non_empty("relation_version", &self.relation_version)?;
        require_non_empty("schema_fingerprint", &self.schema_fingerprint)
    }
}

pub(crate) fn build_relation_ingest_source_cut(
    request: &CaptureRelationIngestSourceCutRequest,
    publications: impl IntoIterator<Item = RelationAuthoritativeIngestPublication>,
) -> Result<RelationIngestSourceCutV1, MetaStoreError> {
    request.validate()?;
    let mut publications = publications
        .into_iter()
        .filter(|publication| {
            publication.authority_key == request.authority
                && publication.reservation.relation_version == request.relation_version
                && publication.reservation.schema_fingerprint == request.schema_fingerprint
        })
        .collect::<Vec<_>>();
    publications.sort_by_key(|publication| {
        (
            publication.reservation.start_offset_inclusive,
            publication.reservation.end_offset_exclusive,
            publication.request_id.clone(),
        )
    });
    let Some(first) = publications.first() else {
        return Ok(RelationIngestSourceCutV1 {
            schema_version: RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1,
            namespace: request.authority.namespace.clone(),
            relation_id: request.authority.relation_id.clone(),
            partitions: Vec::new(),
        });
    };
    let mut frontier = first.reservation.start_offset_inclusive;
    let base = frontier;
    let mut refs = Vec::new();
    for publication in &publications {
        let reservation = &publication.reservation;
        if reservation.start_offset_inclusive != frontier {
            break;
        }
        frontier = reservation.end_offset_exclusive;
        refs.push(RelationIngestPublicationRefV1 {
            request_id: publication.request_id.clone(),
            start_offset_inclusive: reservation.start_offset_inclusive,
            end_offset_exclusive: reservation.end_offset_exclusive,
            batch_key: reservation.batch_key.clone(),
            payload_digest: reservation.payload_digest.clone(),
            object_key: publication.object_key.clone(),
            object_digest: publication.object_digest.clone(),
        });
    }
    Ok(RelationIngestSourceCutV1 {
        schema_version: RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1,
        namespace: request.authority.namespace.clone(),
        relation_id: request.authority.relation_id.clone(),
        partitions: vec![RelationIngestPartitionCutV1 {
            stream_id: request.authority.stream_id.clone(),
            partition_id: request.authority.partition_id,
            base_offset_inclusive: base,
            committed_offset_exclusive: frontier,
            publications: refs,
        }],
    })
}

pub(crate) fn build_relation_ingest_source_cuts(
    request: &CaptureRelationIngestSourceCutsRequest,
    reservations: impl IntoIterator<Item = (RelationPartitionAuthorityKey, IngestRangeReservation)>,
    publications: impl IntoIterator<Item = RelationAuthoritativeIngestPublication>,
) -> Result<Vec<RelationIngestSourceIdentityCutV1>, MetaStoreError> {
    request.validate()?;
    let requested = request.relations.iter().cloned().collect::<BTreeSet<_>>();
    let mut grouped = BTreeMap::<
        (IngestSourceRelationIdentityV1, String, u32),
        Vec<(RelationPartitionAuthorityKey, IngestRangeReservation)>,
    >::new();
    for (authority, reservation) in reservations {
        reservation.validate()?;
        let requested_relation = request.relations.iter().find(|candidate| {
            candidate.relation_id == reservation.relation_id
                && candidate.relation_version == reservation.relation_version
                && candidate.schema_fingerprint == reservation.schema_fingerprint
        });
        if requested_relation.is_none() {
            continue;
        }
        if authority.namespace != request.namespace
            || authority.relation_id != reservation.relation_id
            || authority.stream_id != reservation.stream_id
            || authority.partition_id != reservation.partition_id
        {
            return Err(MetaStoreError::IncompleteRelationSourceCut {
                relation_id: reservation.relation_id.clone(),
                stream_id: reservation.stream_id.clone(),
                partition_id: reservation.partition_id,
                reason: "reservation authority scope mismatch",
            });
        }
        let relation = requested_relation.expect("checked above").clone();
        if requested.contains(&relation) {
            grouped
                .entry((
                    relation,
                    reservation.stream_id.clone(),
                    reservation.partition_id,
                ))
                .or_default()
                .push((authority, reservation));
        }
    }

    let mut publication_map = BTreeMap::new();
    for publication in publications {
        let key = relation_source_entry_key(&publication.authority_key, &publication.reservation);
        if publication_map.insert(key, publication).is_some() {
            return Err(MetaStoreError::Serialization(
                "duplicate relation ingest publication in source-cut snapshot".into(),
            ));
        }
    }

    let mut cuts = Vec::with_capacity(request.relations.len());
    for relation in &request.relations {
        let mut partitions = Vec::new();
        for ((group_relation, stream_id, partition_id), mut ranges) in grouped
            .iter()
            .filter(|((candidate, _, _), _)| candidate == relation)
            .map(|(key, ranges)| (key.clone(), ranges.clone()))
        {
            ranges.sort_by_key(|(_, reservation)| {
                (
                    reservation.start_offset_inclusive,
                    reservation.end_offset_exclusive,
                    reservation.batch_key.clone(),
                )
            });
            let Some((_, first)) = ranges.first() else {
                continue;
            };
            let base_offset_inclusive = first.start_offset_inclusive;
            let mut committed_offset_exclusive = base_offset_inclusive;
            let mut refs = Vec::with_capacity(ranges.len());
            for (authority, reservation) in ranges {
                if reservation.start_offset_inclusive < committed_offset_exclusive {
                    return Err(MetaStoreError::OverlappingSourceCutRange {
                        stream_id: stream_id.clone(),
                        partition_id,
                    });
                }
                if reservation.start_offset_inclusive != committed_offset_exclusive {
                    return Err(MetaStoreError::IncompleteRelationSourceCut {
                        relation_id: group_relation.relation_id.clone(),
                        stream_id: stream_id.clone(),
                        partition_id,
                        reason: "reservation hole",
                    });
                }
                let key = relation_source_entry_key(&authority, &reservation);
                let Some(publication) = publication_map.remove(&key) else {
                    return Err(MetaStoreError::IncompleteRelationSourceCut {
                        relation_id: group_relation.relation_id.clone(),
                        stream_id: stream_id.clone(),
                        partition_id,
                        reason: "missing authoritative publication",
                    });
                };
                committed_offset_exclusive = reservation.end_offset_exclusive;
                refs.push(RelationIngestPublicationRefV1 {
                    request_id: publication.request_id,
                    start_offset_inclusive: reservation.start_offset_inclusive,
                    end_offset_exclusive: reservation.end_offset_exclusive,
                    batch_key: reservation.batch_key.clone(),
                    payload_digest: reservation.payload_digest.clone(),
                    object_key: publication.object_key,
                    object_digest: publication.object_digest,
                });
            }
            partitions.push(RelationIngestPartitionCutV1 {
                stream_id,
                partition_id,
                base_offset_inclusive,
                committed_offset_exclusive,
                publications: refs,
            });
        }
        cuts.push(RelationIngestSourceIdentityCutV1 {
            relation: relation.clone(),
            cut: RelationIngestSourceCutV1 {
                schema_version: RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1,
                namespace: request.namespace.clone(),
                relation_id: relation.relation_id.clone(),
                partitions,
            },
        });
    }

    if let Some(publication) = publication_map.values().find(|publication| {
        publication.authority_key.namespace == request.namespace
            && request.relations.iter().any(|relation| {
                relation.relation_id == publication.reservation.relation_id
                    && relation.relation_version == publication.reservation.relation_version
                    && relation.schema_fingerprint == publication.reservation.schema_fingerprint
            })
    }) {
        return Err(MetaStoreError::IncompleteRelationSourceCut {
            relation_id: publication.reservation.relation_id.clone(),
            stream_id: publication.reservation.stream_id.clone(),
            partition_id: publication.reservation.partition_id,
            reason: "publication has no matching reservation",
        });
    }

    Ok(cuts)
}

/// Validates and canonicalizes a guarded relation source-cut snapshot.
///
/// The builder above emits this shape, but guarded publication also accepts
/// snapshots arriving over the wire.  Validate the identity and every
/// publication reference before comparing it with a freshly rebuilt snapshot;
/// otherwise a caller could exploit alternate ordering or incomplete frontier
/// data to bypass the source-cut precondition.
pub(crate) fn canonicalize_relation_ingest_source_cuts(
    cuts: &[RelationIngestSourceIdentityCutV1],
) -> Result<Vec<RelationIngestSourceIdentityCutV1>, MetaStoreError> {
    if cuts.is_empty() {
        return Err(MetaStoreError::EmptyField {
            field: "expected_relation_source_cuts",
        });
    }
    let mut canonical = cuts.to_vec();
    let namespace = canonical[0].cut.namespace.clone();
    require_non_empty("source_cut.namespace", &namespace)?;
    let mut seen_relations = BTreeSet::new();
    for entry in &mut canonical {
        entry.relation.validate()?;
        if entry.relation.relation_generation != INGEST_SOURCE_IDENTITY_GENERATION_V1 {
            return Err(MetaStoreError::UnsupportedRelationGeneration {
                generation: entry.relation.relation_generation,
            });
        }
        if !seen_relations.insert(entry.relation.clone()) {
            return Err(MetaStoreError::DuplicateSourceCutRelation {
                relation_id: entry.relation.relation_id.clone(),
                relation_version: entry.relation.relation_version.clone(),
            });
        }
        if entry.cut.schema_version != RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1 {
            return Err(MetaStoreError::Serialization(
                "unsupported relation source cut schema version".into(),
            ));
        }
        require_non_empty("source_cut.namespace", &entry.cut.namespace)?;
        if entry.cut.namespace != namespace || entry.cut.relation_id != entry.relation.relation_id {
            return Err(MetaStoreError::IncompleteRelationSourceCut {
                relation_id: entry.relation.relation_id.clone(),
                stream_id: String::new(),
                partition_id: 0,
                reason: "source cut identity mismatch",
            });
        }
        validate_relation_source_cut_partitions(&entry.cut)?;
    }
    canonical.sort_by(|left, right| left.relation.cmp(&right.relation));
    Ok(canonical)
}

fn validate_relation_source_cut_partitions(
    cut: &RelationIngestSourceCutV1,
) -> Result<(), MetaStoreError> {
    let mut previous_partition = None;
    for partition in &cut.partitions {
        require_non_empty("source_cut.stream_id", &partition.stream_id)?;
        if partition.base_offset_inclusive > partition.committed_offset_exclusive {
            return Err(MetaStoreError::Serialization(
                "relation source cut partition offsets are not monotonic".into(),
            ));
        }
        let partition_key = (&partition.stream_id, partition.partition_id);
        if previous_partition.is_some_and(|previous| previous >= partition_key) {
            return Err(MetaStoreError::Serialization(
                "relation source cut partitions are not canonical".into(),
            ));
        }
        previous_partition = Some(partition_key);
        let mut frontier = partition.base_offset_inclusive;
        for publication in &partition.publications {
            require_non_empty("source_cut.request_id", &publication.request_id)?;
            require_non_empty("source_cut.batch_key", &publication.batch_key)?;
            require_non_empty("source_cut.payload_digest", &publication.payload_digest)?;
            require_non_empty("source_cut.object_key", &publication.object_key)?;
            require_non_empty("source_cut.object_digest", &publication.object_digest)?;
            if publication.start_offset_inclusive != frontier
                || publication.start_offset_inclusive >= publication.end_offset_exclusive
            {
                return Err(MetaStoreError::Serialization(
                    "relation source cut publications are not contiguous".into(),
                ));
            }
            frontier = publication.end_offset_exclusive;
        }
        if frontier != partition.committed_offset_exclusive {
            return Err(MetaStoreError::Serialization(
                "relation source cut frontier does not match publications".into(),
            ));
        }
    }
    Ok(())
}

fn relation_source_entry_key(
    authority: &RelationPartitionAuthorityKey,
    reservation: &IngestRangeReservation,
) -> (
    String,
    String,
    String,
    u32,
    String,
    String,
    u64,
    u64,
    String,
    String,
    u64,
) {
    (
        authority.namespace.clone(),
        authority.relation_id.clone(),
        authority.stream_id.clone(),
        authority.partition_id,
        reservation.relation_version.clone(),
        reservation.schema_fingerprint.clone(),
        reservation.start_offset_inclusive,
        reservation.end_offset_exclusive,
        reservation.batch_key.clone(),
        reservation.payload_digest.clone(),
        reservation.writer_epoch,
    )
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSourceRelationIdentityV1 {
    pub relation_id: String,
    pub relation_version: String,
    #[serde(default = "default_ingest_source_identity_generation")]
    pub relation_generation: u64,
    pub schema_fingerprint: String,
}

fn default_ingest_source_identity_generation() -> u64 {
    INGEST_SOURCE_IDENTITY_GENERATION_V1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation_identity() -> IngestSourceRelationIdentityV1 {
        IngestSourceRelationIdentityV1 {
            relation_id: "orders".into(),
            relation_version: "v1".into(),
            relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
            schema_fingerprint: "sha256:schema".into(),
        }
    }

    fn authority(relation_id: &str) -> RelationPartitionAuthorityKey {
        RelationPartitionAuthorityKey {
            namespace: "default".into(),
            relation_id: relation_id.into(),
            stream_id: "orders-stream".into(),
            partition_id: 0,
        }
    }

    fn reservation() -> IngestRangeReservation {
        IngestRangeReservation {
            stream_id: "orders-stream".into(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 10,
            batch_key: "batch-1".into(),
            payload_digest: "sha256:payload".into(),
            relation_id: "orders".into(),
            relation_version: "v1".into(),
            schema_fingerprint: "sha256:schema".into(),
            writer_epoch: 1,
        }
    }

    fn publication(
        authority_key: RelationPartitionAuthorityKey,
        reservation: IngestRangeReservation,
        request_id: &str,
    ) -> RelationAuthoritativeIngestPublication {
        RelationAuthoritativeIngestPublication {
            reservation,
            authority_key,
            request_id: request_id.into(),
            request_digest: format!("sha256:{request_id}"),
            object_key: format!("objects/{request_id}"),
            object_digest: format!("sha256:object-{request_id}"),
        }
    }

    fn request() -> CaptureRelationIngestSourceCutsRequest {
        CaptureRelationIngestSourceCutsRequest {
            namespace: "default".into(),
            relations: vec![relation_identity()],
        }
    }

    #[test]
    fn legacy_relation_source_cut_wire_shape_decodes_without_identity_fields() {
        let json = r#"{
            "schema_version": 1,
            "namespace": "default",
            "relation_id": "orders",
            "partitions": [{
                "stream_id": "orders-stream",
                "partition_id": 0,
                "base_offset_inclusive": 0,
                "committed_offset_exclusive": 10,
                "publications": [{
                    "request_id": "request-1",
                    "start_offset_inclusive": 0,
                    "end_offset_exclusive": 10,
                    "batch_key": "batch-1",
                    "payload_digest": "sha256:payload",
                    "object_key": "objects/request-1",
                    "object_digest": "sha256:object"
                }]
            }]
        }"#;
        let cut: RelationIngestSourceCutV1 = serde_json::from_str(json).unwrap();
        assert_eq!(cut.partitions[0].publications[0].request_id, "request-1");
        let encoded = serde_json::to_value(cut).unwrap();
        let publication = &encoded["partitions"][0]["publications"][0];
        assert!(publication.get("relation_version").is_none());
        assert!(publication.get("schema_fingerprint").is_none());
    }

    #[test]
    fn relation_source_cut_rejects_authority_scope_mismatch() {
        let reservation = reservation();
        let error = build_relation_ingest_source_cuts(
            &request(),
            [(authority("other"), reservation.clone())],
            [publication(authority("other"), reservation, "request-1")],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            MetaStoreError::IncompleteRelationSourceCut {
                reason: "reservation authority scope mismatch",
                ..
            }
        ));
    }

    #[test]
    fn relation_source_cut_rejects_orphan_and_duplicate_publications() {
        let reservation = reservation();
        let orphan = build_relation_ingest_source_cuts(
            &request(),
            [],
            [publication(
                authority("orders"),
                reservation.clone(),
                "request-1",
            )],
        )
        .unwrap_err();
        assert!(matches!(
            orphan,
            MetaStoreError::IncompleteRelationSourceCut {
                reason: "publication has no matching reservation",
                ..
            }
        ));

        let mut mismatched_reservation = reservation.clone();
        mismatched_reservation.payload_digest = "sha256:other-payload".into();
        let mismatched = build_relation_ingest_source_cuts(
            &request(),
            [(authority("orders"), reservation.clone())],
            [publication(
                authority("orders"),
                mismatched_reservation,
                "request-mismatched",
            )],
        )
        .unwrap_err();
        assert!(matches!(
            mismatched,
            MetaStoreError::IncompleteRelationSourceCut {
                reason: "missing authoritative publication",
                ..
            }
        ));

        let duplicate = build_relation_ingest_source_cuts(
            &request(),
            [(authority("orders"), reservation.clone())],
            [
                publication(authority("orders"), reservation.clone(), "request-1"),
                publication(authority("orders"), reservation, "request-2"),
            ],
        )
        .unwrap_err();
        assert!(matches!(duplicate, MetaStoreError::Serialization(_)));
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSourcePartitionCutV1 {
    pub stream_id: String,
    pub stream_generation: u64,
    pub partition_id: u32,
    pub partition_generation: u64,
    pub base_offset_inclusive: u64,
    pub committed_offset_exclusive: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSourceRelationCutV1 {
    pub relation: IngestSourceRelationIdentityV1,
    pub partitions: Vec<IngestSourcePartitionCutV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSourceCutV1 {
    pub schema_version: u32,
    pub input_catalog_epoch: u64,
    pub relations: Vec<IngestSourceRelationCutV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureIngestSourceCutRequest {
    pub relations: Vec<IngestSourceRelationIdentityV1>,
}

impl CaptureIngestSourceCutRequest {
    pub(crate) fn validate(&self) -> Result<(), MetaStoreError> {
        let mut seen = BTreeSet::new();
        for relation in &self.relations {
            relation.validate()?;
            if !seen.insert(relation) {
                return Err(MetaStoreError::DuplicateSourceCutRelation {
                    relation_id: relation.relation_id.clone(),
                    relation_version: relation.relation_version.clone(),
                });
            }
        }
        Ok(())
    }
}

impl IngestSourceRelationIdentityV1 {
    fn validate(&self) -> Result<(), MetaStoreError> {
        require_non_empty("relation_id", &self.relation_id)?;
        require_non_empty("relation_version", &self.relation_version)?;
        if self.relation_generation == 0 {
            return Err(MetaStoreError::IntegerOutOfRange {
                field: "relation_generation",
                value: self.relation_generation,
            });
        }
        require_non_empty("schema_fingerprint", &self.schema_fingerprint)
    }
}

pub(crate) fn build_ingest_source_cut(
    request: &CaptureIngestSourceCutRequest,
    input_catalog_epoch: u64,
    reservations: impl IntoIterator<Item = IngestRangeReservation>,
    committed_batch_keys: &BTreeSet<String>,
) -> Result<IngestSourceCutV1, MetaStoreError> {
    request.validate()?;
    let requested = request.relations.iter().cloned().collect::<BTreeSet<_>>();
    let mut grouped = BTreeMap::<
        (IngestSourceRelationIdentityV1, String, u32),
        Vec<IngestRangeReservation>,
    >::new();
    for reservation in reservations {
        reservation.validate()?;
        let relation = IngestSourceRelationIdentityV1 {
            relation_id: reservation.relation_id.clone(),
            relation_version: reservation.relation_version.clone(),
            relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
            schema_fingerprint: reservation.schema_fingerprint.clone(),
        };
        if requested.contains(&relation) {
            grouped
                .entry((
                    relation,
                    reservation.stream_id.clone(),
                    reservation.partition_id,
                ))
                .or_default()
                .push(reservation);
        }
    }

    let mut relation_partitions =
        BTreeMap::<IngestSourceRelationIdentityV1, Vec<IngestSourcePartitionCutV1>>::new();
    for ((relation, stream_id, partition_id), mut ranges) in grouped {
        ranges.sort_by_key(|range| (range.start_offset_inclusive, range.end_offset_exclusive));
        let base_offset_inclusive = ranges[0].start_offset_inclusive;
        let mut committed_offset_exclusive = base_offset_inclusive;
        for range in ranges {
            if range.start_offset_inclusive < committed_offset_exclusive {
                return Err(MetaStoreError::OverlappingSourceCutRange {
                    stream_id,
                    partition_id,
                });
            }
            if range.start_offset_inclusive != committed_offset_exclusive
                || !committed_batch_keys.contains(&range.batch_key)
            {
                break;
            }
            committed_offset_exclusive = range.end_offset_exclusive;
        }
        relation_partitions
            .entry(relation)
            .or_default()
            .push(IngestSourcePartitionCutV1 {
                stream_id,
                stream_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                partition_id,
                partition_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                base_offset_inclusive,
                committed_offset_exclusive,
            });
    }

    let relations = request
        .relations
        .iter()
        .cloned()
        .map(|relation| IngestSourceRelationCutV1 {
            partitions: relation_partitions.remove(&relation).unwrap_or_default(),
            relation,
        })
        .collect();
    Ok(IngestSourceCutV1 {
        schema_version: INGEST_SOURCE_CUT_SCHEMA_VERSION_V1,
        input_catalog_epoch,
        relations,
    })
}

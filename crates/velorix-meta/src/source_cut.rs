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
    #[serde(default)]
    pub relation_version: String,
    #[serde(default)]
    pub schema_fingerprint: String,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureRelationIngestSourceCutRequest {
    pub authority: RelationPartitionAuthorityKey,
    pub relation_version: String,
    pub schema_fingerprint: String,
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
            relation_version: reservation.relation_version.clone(),
            schema_fingerprint: reservation.schema_fingerprint.clone(),
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

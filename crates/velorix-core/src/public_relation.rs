//! Public relation registration types and their conversion to the internal
//! signed catalog representation.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::relation::{
    ArrowPhysicalTypeV1, RelationColumnV1, RelationOperationV1, RelationSchemaError,
    RelationSemanticRoleV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
    VelorixRelationSchemaV1, CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRelationCatalogV1 {
    pub relation_schema: PublicRelationSchemaV1,
    #[serde(default = "default_public_incremental_adapter")]
    pub adapter_id: String,
}

fn default_public_incremental_adapter() -> String {
    CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRelationSchemaV1 {
    pub relation_id: String,
    pub relation_name: String,
    pub relation_version: String,
    pub columns: Vec<PublicRelationColumnV1>,
    pub primary_key_column_ids: Vec<String>,
    pub allowed_operations: Vec<RelationOperationV1>,
    #[serde(default)]
    pub event_time_column_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRelationColumnV1 {
    pub column_id: String,
    pub name: String,
    pub logical_type: VelorixLogicalTypeV1,
    pub physical_arrow_type: ArrowPhysicalTypeV1,
    pub nullable: bool,
    pub ordinal: u32,
}

impl PublicRelationCatalogV1 {
    /// Convert the public business schema into the internal signed catalog.
    /// The internal weight ID and name are deterministic and independently
    /// collision safe against caller supplied column IDs and names.
    pub fn into_internal(self) -> Result<VelorixRelationCatalogV1, RelationSchemaError> {
        let business_ids: BTreeSet<&str> = self
            .relation_schema
            .columns
            .iter()
            .map(|column| column.column_id.as_str())
            .collect();
        if self
            .relation_schema
            .primary_key_column_ids
            .iter()
            .any(|column_id| !business_ids.contains(column_id.as_str()))
        {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "primary_key_column_ids",
            });
        }
        if self
            .relation_schema
            .event_time_column_id
            .as_deref()
            .is_some_and(|column_id| !business_ids.contains(column_id))
        {
            return Err(RelationSchemaError::InvalidRelationSchema {
                field: "event_time_column_id",
            });
        }
        let primary_keys: BTreeSet<String> = self
            .relation_schema
            .primary_key_column_ids
            .iter()
            .cloned()
            .collect();
        let event_time = self.relation_schema.event_time_column_id.clone();
        let mut columns: Vec<RelationColumnV1> = self
            .relation_schema
            .columns
            .into_iter()
            .map(|column| {
                let semantic_role = if primary_keys.contains(&column.column_id) {
                    RelationSemanticRoleV1::PrimaryKey
                } else if event_time.as_deref() == Some(column.column_id.as_str()) {
                    RelationSemanticRoleV1::EventTime
                } else {
                    RelationSemanticRoleV1::Value
                };
                RelationColumnV1 {
                    column_id: column.column_id,
                    name: column.name,
                    logical_type: column.logical_type,
                    physical_arrow_type: column.physical_arrow_type,
                    nullable: column.nullable,
                    ordinal: column.ordinal,
                    semantic_role,
                }
            })
            .collect();

        let weight_name = collision_safe_name(
            "__velorix_internal_weight",
            columns.iter().map(|column| column.name.as_str()),
        );
        let weight_id = collision_safe_name(
            "__velorix_internal_weight",
            columns.iter().map(|column| column.column_id.as_str()),
        );
        columns.push(RelationColumnV1 {
            column_id: weight_id.clone(),
            name: weight_name,
            logical_type: VelorixLogicalTypeV1::Int64,
            physical_arrow_type: ArrowPhysicalTypeV1::Int64,
            nullable: false,
            ordinal: columns.len() as u32,
            semantic_role: RelationSemanticRoleV1::Weight,
        });

        let schema = VelorixRelationSchemaV1 {
            relation_id: self.relation_schema.relation_id,
            relation_name: self.relation_schema.relation_name,
            relation_version: self.relation_schema.relation_version,
            columns,
            primary_key_column_ids: self.relation_schema.primary_key_column_ids,
            weight_column_id: weight_id,
            allowed_operations: self.relation_schema.allowed_operations,
            event_time_column_id: self.relation_schema.event_time_column_id,
        };
        VelorixRelationCatalogV1::from_relation_schema(schema, self.adapter_id)
    }

    pub fn to_internal(&self) -> Result<VelorixRelationCatalogV1, RelationSchemaError> {
        self.clone().into_internal()
    }
}

fn collision_safe_name<'a>(base: &str, existing: impl Iterator<Item = &'a str>) -> String {
    let existing: BTreeSet<&str> = existing.collect();
    if !existing.contains(base) {
        return base.to_string();
    }
    let mut suffix = 1_u64;
    loop {
        let candidate = format!("{base}_{suffix}");
        if !existing.contains(candidate.as_str()) {
            return candidate;
        }
        suffix += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn public_schema_with_engine_name_collision() -> PublicRelationCatalogV1 {
        PublicRelationCatalogV1 {
            relation_schema: PublicRelationSchemaV1 {
                relation_id: "measurements".into(),
                relation_name: "measurements".into(),
                relation_version: "v1".into(),
                columns: vec![
                    PublicRelationColumnV1 {
                        column_id: "id".into(),
                        name: "id".into(),
                        logical_type: VelorixLogicalTypeV1::Utf8,
                        physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                        nullable: false,
                        ordinal: 0,
                    },
                    PublicRelationColumnV1 {
                        column_id: "delta".into(),
                        name: "delta".into(),
                        logical_type: VelorixLogicalTypeV1::Int64,
                        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                        nullable: false,
                        ordinal: 1,
                    },
                    PublicRelationColumnV1 {
                        column_id: "__velorix_internal_weight_1".into(),
                        name: "__velorix_internal_weight".into(),
                        logical_type: VelorixLogicalTypeV1::Int64,
                        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                        nullable: false,
                        ordinal: 2,
                    },
                ],
                primary_key_column_ids: vec!["id".into()],
                allowed_operations: vec![RelationOperationV1::Insert],
                event_time_column_id: None,
            },
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        }
    }

    #[test]
    fn public_schema_keeps_business_weight_names_and_derives_collision_safe_engine_field() {
        let catalog = public_schema_with_engine_name_collision()
            .into_internal()
            .unwrap();
        let weight = catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == catalog.relation_schema.weight_column_id)
            .unwrap();
        assert_eq!(weight.column_id, "__velorix_internal_weight");
        assert_eq!(weight.name, "__velorix_internal_weight_1");
        assert!(catalog
            .relation_schema
            .columns
            .iter()
            .any(|column| column.name == "delta"));
        assert!(catalog
            .relation_schema
            .columns
            .iter()
            .any(|column| column.name == "__velorix_internal_weight"
                && column.column_id == "__velorix_internal_weight_1"));
    }

    #[test]
    fn public_schema_rejects_dangling_key_and_event_ids_before_engine_field_injection() {
        let mut public = public_schema_with_engine_name_collision();
        public.relation_schema.primary_key_column_ids = vec!["__velorix_internal_weight".into()];
        let error = public.into_internal().unwrap_err();
        assert_eq!(
            error,
            RelationSchemaError::InvalidRelationSchema {
                field: "primary_key_column_ids"
            }
        );
        assert!(!error.to_string().contains("__velorix_internal_weight"));

        let mut public = public_schema_with_engine_name_collision();
        public.relation_schema.event_time_column_id = Some("__velorix_internal_weight".into());
        let error = public.into_internal().unwrap_err();
        assert_eq!(
            error,
            RelationSchemaError::InvalidRelationSchema {
                field: "event_time_column_id"
            }
        );
        assert!(!error.to_string().contains("__velorix_internal_weight"));
    }
}

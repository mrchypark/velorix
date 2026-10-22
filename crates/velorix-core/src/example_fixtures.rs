use crate::relation::{
    ArrowPhysicalTypeV1, RelationColumnV1, RelationOperationV1, RelationSchemaError,
    RelationSemanticRoleV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
    VelorixRelationSchemaV1,
};

pub const ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID: &str = "incremental-adapter-orders-sum-count-v1";
pub const ORDERS_SUM_COUNT_RELATION_ID: &str = "orders";
pub const ORDERS_SUM_COUNT_RELATION_VERSION: &str = "2026-05-05.v1";
pub const ORDERS_SUM_COUNT_ADAPTER_ID: &str = ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID;

pub fn orders_sum_count_relation_catalog() -> Result<VelorixRelationCatalogV1, RelationSchemaError>
{
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
        relation_name: "orders".to_string(),
        relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    VelorixRelationCatalogV1::from_relation_schema(
        relation_schema,
        ORDERS_SUM_COUNT_ADAPTER_ID.to_string(),
    )
}

#![cfg(feature = "feldera-package-compat")]

use std::collections::HashMap;

use feldera_ir::Dataflow;
use feldera_sqllib::{SqlString, Weight};
use feldera_types::program_schema::{ProgramSchema, Relation, SqlIdentifier};

#[test]
fn feldera_package_compat_gate_exposes_runtime_type_descriptor_and_ir_crates() {
    let weight: Weight = 1;
    let sql_string = SqlString::from("orders");
    assert_eq!(weight, 1);
    assert_eq!(sql_string.to_string(), "orders");

    let schema = ProgramSchema {
        inputs: vec![Relation::new(
            SqlIdentifier::from("orders"),
            Vec::new(),
            false,
            Default::default(),
        )],
        outputs: Vec::new(),
    };
    assert_eq!(schema.inputs[0].name.name(), "orders");

    let dataflow = Dataflow::new(HashMap::new(), HashMap::new());
    assert!(dataflow.diff(&dataflow).is_empty());
}

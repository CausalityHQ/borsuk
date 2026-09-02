use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

fn vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("element", DataType::Float32, false)),
        96,
    )
}

pub fn v26_construction_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
    ])
}

pub fn v26_source_map_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("dataset_ordinal", DataType::UInt64, false),
    ])
}

pub fn v26_tree_schema() -> Schema {
    Schema::new(vec![
        Field::new("node_ordinal", DataType::UInt32, false),
        Field::new("left", DataType::UInt32, true),
        Field::new("right", DataType::UInt32, true),
        Field::new("direction_ordinal", DataType::UInt8, false),
        Field::new("threshold", DataType::Float32, false),
        Field::new("split_gap", DataType::Float32, false),
        Field::new("leaf_page", DataType::UInt32, true),
    ])
}

pub fn v26_page_assignments_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

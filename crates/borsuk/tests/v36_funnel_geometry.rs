//! V36 geometry construction work and admission contracts.

use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field};
use borsuk::{
    load_v36_prefix_source_feature_ids, project_v36_exact_assignment_preflight,
    v36_prefix_source_schema, write_v36_prefix_source_parquet,
};

const SOURCE_DIMENSIONS: usize = 768;

fn source_batch(feature_ids: Vec<u64>) -> RecordBatch {
    let rows = feature_ids.len();
    let child = Arc::new(Field::new("item", DataType::Float32, false));
    let mut values = vec![0.0_f32; rows * SOURCE_DIMENSIONS];
    for row in 0..rows {
        values[row * SOURCE_DIMENSIONS + row % SOURCE_DIMENSIONS] = 1.0;
    }
    let embeddings = FixedSizeListArray::try_new(
        child,
        SOURCE_DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )
    .unwrap();
    RecordBatch::try_new(
        Arc::new(v36_prefix_source_schema()),
        vec![
            Arc::new(UInt64Array::from(feature_ids)) as ArrayRef,
            Arc::new(embeddings),
        ],
    )
    .unwrap()
}

#[test]
fn v36_geometry_inputs_reuse_strict_prefix_source_membership_scan() {
    // Break caught: geometry invents a second source schema or decodes every
    // embedding merely to establish the authenticated corpus ID order.
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let feature_ids = (7_u64..108).collect::<Vec<_>>();
    write_v36_prefix_source_parquet(&source, &feature_ids, [source_batch(feature_ids.clone())])
        .unwrap();
    assert_eq!(
        load_v36_prefix_source_feature_ids(&source, 101).unwrap(),
        feature_ids
    );
    assert!(load_v36_prefix_source_feature_ids(&source, 100).is_err());
    assert!(load_v36_prefix_source_feature_ids(&source, 102).is_err());
}

#[test]
fn v36_geometry_preflight_projects_exact_assignment_work_before_corpus_execution() {
    // Break caught: a long geometry cell starts without first projecting the
    // exact global assignment work from a bounded measured kernel sample.
    let one_million = project_v36_exact_assignment_preflight(
        1_000_000, 192, 4_096, 1_024, 245, 1_000_000, 43_200,
    )
    .unwrap();
    assert_eq!(one_million.posting_count, 245);
    assert_eq!(one_million.distance_evaluations, 245_000_000);
    assert_eq!(one_million.component_terms, 47_040_000_000);
    assert_eq!(one_million.projected_active_ns, 976_562_500);
    assert!(one_million.within_active_wall_cap);

    let ten_million = project_v36_exact_assignment_preflight(
        10_000_000, 192, 4_096, 1_024, 245, 1_000_000, 43_200,
    )
    .unwrap();
    assert_eq!(ten_million.posting_count, 2_442);
    assert_eq!(ten_million.component_terms, 4_688_640_000_000);

    let hundred_million = project_v36_exact_assignment_preflight(
        100_000_000,
        192,
        4_096,
        1_024,
        245,
        1_000_000,
        43_200,
    )
    .unwrap();
    assert_eq!(hundred_million.posting_count, 24_415);
    assert_eq!(hundred_million.component_terms, 468_768_000_000_000);
    assert_eq!(hundred_million.projected_active_ns, 9_731_744_260_205);
    assert!(hundred_million.within_active_wall_cap);

    let b8192 =
        project_v36_exact_assignment_preflight(100_000_000, 192, 8_192, 1_024, 123, 1_000_000, 1)
            .unwrap();
    assert_eq!(b8192.posting_count, 12_208);
    assert_eq!(b8192.component_terms, 234_393_600_000_000);
    assert!(!b8192.within_active_wall_cap);
}

#[test]
fn v36_geometry_preflight_rejects_unbounded_or_ambiguous_measurements() {
    for request in [
        (0, 192, 4_096, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 0, 4_096, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 192, 0, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 0, 245, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 1_024, 0, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 1_024, 245, 0, 43_200),
        (1_000_000, 192, 4_096, 1_024, 245, 1_000_000, 0),
    ] {
        assert!(
            project_v36_exact_assignment_preflight(
                request.0, request.1, request.2, request.3, request.4, request.5, request.6,
            )
            .is_err()
        );
    }
    assert!(
        project_v36_exact_assignment_preflight(
            u64::MAX,
            u32::MAX,
            1,
            u64::MAX,
            u32::MAX,
            u64::MAX,
            u64::MAX,
        )
        .is_err()
    );
}

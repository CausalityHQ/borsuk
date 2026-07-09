#![allow(missing_docs)]

use borsuk::{
    BorsukError, BorsukIndex, CompactionOptions, IndexConfig, RecordId, VectorMetric, VectorRecord,
};

fn index_config(uri: String, sparse: bool, segment_max_vectors: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors,
        ram_budget_bytes: None,
        sparse,
    }
}

fn sparse_record(
    id: impl Into<RecordId>,
    vector: Vec<f32>,
    indices: Vec<u32>,
    values: Vec<f32>,
) -> VectorRecord {
    VectorRecord::new(id.into(), vector)
        .with_sparse(indices, values)
        .unwrap()
}

#[test]
fn sparse_vectors_round_trip_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri.clone(), true, 2)).unwrap();
    index
        .add(vec![
            sparse_record("a", vec![0.0, 0.0], vec![10, 2], vec![1.5, 0.5]),
            sparse_record("b", vec![1.0, 0.0], vec![7, 3], vec![2.5, 3.5]),
        ])
        .unwrap();
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();

    assert!(reopened.stats().sparse);
    assert_eq!(
        reopened.get_sparse(&RecordId::from("a")).unwrap(),
        Some((vec![2, 10], vec![0.5, 1.5]))
    );
    assert_eq!(
        reopened.get_sparse(&RecordId::from("b")).unwrap(),
        Some((vec![3, 7], vec![3.5, 2.5]))
    );
}

#[test]
fn sparse_records_are_rejected_when_index_is_not_sparse_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, false, 2)).unwrap();
    let err = index
        .add(vec![sparse_record(
            "blocked",
            vec![0.0, 0.0],
            vec![1],
            vec![2.0],
        )])
        .unwrap_err();

    assert!(
        matches!(err, BorsukError::InvalidMetricInput(ref message) if message.contains("sparse")),
        "{err:?}"
    );
}

#[test]
fn compaction_preserves_sparse_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 1)).unwrap();
    index
        .add(
            (0..12)
                .map(|id| {
                    sparse_record(
                        format!("v{id}"),
                        vec![id as f32, 0.0],
                        vec![100 + id as u32, id as u32],
                        vec![id as f32 + 0.25, id as f32 + 0.75],
                    )
                })
                .collect(),
        )
        .unwrap();

    let compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(12),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(compaction.compacted);

    for id in [0, 3, 7, 11] {
        assert_eq!(
            index.get_sparse(&RecordId::from(format!("v{id}"))).unwrap(),
            Some((
                vec![id as u32, 100 + id as u32],
                vec![id as f32 + 0.75, id as f32 + 0.25],
            ))
        );
    }
}

#[test]
fn sparse_enabled_index_accepts_records_without_sparse_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    index
        .add(vec![
            sparse_record("with-sparse", vec![0.0, 0.0], vec![4], vec![1.25]),
            VectorRecord::new("dense-only", vec![1.0, 0.0]),
        ])
        .unwrap();

    assert_eq!(
        index.get_sparse(&RecordId::from("with-sparse")).unwrap(),
        Some((vec![4], vec![1.25]))
    );
    assert_eq!(
        index.get_sparse(&RecordId::from("dense-only")).unwrap(),
        None
    );
}

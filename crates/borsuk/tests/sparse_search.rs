#![allow(missing_docs)]

use borsuk::{
    BorsukError, BorsukIndex, IndexConfig, RecordId, SearchHit, SparseVector, VectorMetric,
    VectorRecord, sparse_dot,
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

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).unwrap()
}

fn hit_ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter()
        .map(|hit| hit.id.to_utf8_string().unwrap())
        .collect()
}

fn brute_force_top_k(
    rows: &[(String, SparseVector)],
    query: &SparseVector,
    k: usize,
) -> Vec<(String, f32)> {
    let mut scored: Vec<_> = rows
        .iter()
        .filter_map(|(id, vector)| {
            let score = sparse_dot(query, vector);
            (score != 0.0).then(|| (id.clone(), score))
        })
        .collect();
    scored.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored.truncate(k);
    scored
}

#[test]
fn search_sparse_matches_bruteforce_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let rows = vec![
        ("doc-00".to_string(), sparse(&[1, 3], &[0.9, 1.5])),
        ("doc-01".to_string(), sparse(&[2, 4], &[2.0, 0.25])),
        ("doc-02".to_string(), sparse(&[1, 4, 8], &[1.25, -0.4, 2.0])),
        ("doc-03".to_string(), sparse(&[3, 8], &[0.5, 1.75])),
        ("doc-04".to_string(), sparse(&[2, 9], &[1.5, 3.0])),
        ("doc-05".to_string(), sparse(&[1, 2, 3], &[-0.5, 0.75, 1.0])),
    ];
    let records: Vec<_> = rows
        .iter()
        .enumerate()
        .map(|(offset, (id, sparse))| {
            sparse_record(
                id.as_str(),
                vec![offset as f32, 0.0],
                sparse.indices().to_vec(),
                sparse.values().to_vec(),
            )
        })
        .collect();

    let mut index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    index.add(records).unwrap();
    assert!(
        index.stats().segments >= 2,
        "test setup must create multiple segments"
    );

    for query in [
        sparse(&[1, 3], &[1.0, 0.5]),
        sparse(&[2, 4, 9], &[0.5, -1.0, 2.0]),
        sparse(&[8], &[1.25]),
    ] {
        let expected = brute_force_top_k(&rows, &query, 4);
        let report = index.search_sparse(&query, 4).unwrap();
        let expected_ids: Vec<_> = expected.iter().map(|(id, _)| id.clone()).collect();

        assert_eq!(hit_ids(&report.hits), expected_ids);
        assert_eq!(report.segments_searched, index.stats().segments);
        assert!(report.bytes_read > 0);
        assert_eq!(report.records_considered, 0);
        assert_eq!(report.records_scored, 0);
        for (hit, (_, score)) in report.hits.iter().zip(expected) {
            assert_eq!(hit.distance, -score);
            assert!(hit.metadata.is_none());
        }
    }
}

#[test]
fn deleted_record_never_appears_in_search_sparse_results() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 1)).unwrap();
    index
        .add(vec![
            sparse_record("deleted-best", vec![0.0, 0.0], vec![7], vec![10.0]),
            sparse_record("live-next", vec![1.0, 0.0], vec![7], vec![3.0]),
            sparse_record("live-last", vec![2.0, 0.0], vec![7], vec![1.0]),
        ])
        .unwrap();
    assert_eq!(index.delete(["deleted-best"]).unwrap(), 1);

    let report = index.search_sparse(&sparse(&[7], &[1.0]), 3).unwrap();

    assert_eq!(hit_ids(&report.hits), vec!["live-next", "live-last"]);
}

#[test]
fn search_sparse_rejects_disabled_index_and_zero_k() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut dense_index = BorsukIndex::create(index_config(uri, false, 2)).unwrap();
    dense_index
        .add(vec![VectorRecord::new("dense", vec![0.0, 0.0])])
        .unwrap();

    let disabled = dense_index
        .search_sparse(&sparse(&[1], &[1.0]), 1)
        .unwrap_err();
    assert!(
        matches!(disabled, BorsukError::InvalidMetricInput(ref message) if message.contains("sparse=false")),
        "{disabled:?}"
    );

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut sparse_index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    sparse_index
        .add(vec![sparse_record(
            "sparse",
            vec![0.0, 0.0],
            vec![1],
            vec![1.0],
        )])
        .unwrap();

    let zero_k = sparse_index
        .search_sparse(&sparse(&[1], &[1.0]), 0)
        .unwrap_err();
    assert!(
        matches!(zero_k, BorsukError::InvalidSearchOptions(ref message) if message == "k must be greater than zero"),
        "{zero_k:?}"
    );
}

#[test]
fn records_without_sparse_data_are_absent_from_sparse_results() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    index
        .add(vec![
            sparse_record("with-sparse", vec![0.0, 0.0], vec![5], vec![2.0]),
            VectorRecord::new("dense-only", vec![1.0, 0.0]),
        ])
        .unwrap();

    let report = index.search_sparse(&sparse(&[5], &[1.0]), 10).unwrap();

    assert_eq!(hit_ids(&report.hits), vec!["with-sparse"]);
}

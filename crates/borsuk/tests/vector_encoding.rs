#![allow(missing_docs)]

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, SearchHit, SearchOptions, StorageEncoding,
    VectorMetric, VectorRecord,
};

const DIMENSIONS: usize = 8;

fn index_config(uri: String, segment_max_vectors: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: DIMENSIONS,
        segment_max_vectors,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

fn vectors() -> Vec<(String, Vec<f32>)> {
    vec![
        (
            "v00".to_string(),
            vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
        (
            "v01".to_string(),
            vec![0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
        (
            "v02".to_string(),
            vec![0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
        (
            "v03".to_string(),
            vec![0.0, 0.0, 0.0, 4.0, 0.0, 0.0, 0.0, 0.0],
        ),
        (
            "v04".to_string(),
            vec![0.5, 0.4, 0.3, 0.2, 0.1, 0.0, 0.0, 0.0],
        ),
        (
            "v05".to_string(),
            vec![0.0, 0.0, 0.0, 0.0, -1.0, 0.0, 0.0, 2.0],
        ),
        (
            "v06".to_string(),
            vec![3.0, 2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0],
        ),
        (
            "v07".to_string(),
            vec![-1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
        (
            "v08".to_string(),
            vec![0.0, -2.0, 0.0, 0.0, 0.0, 1.5, 0.0, 0.0],
        ),
        (
            "v09".to_string(),
            vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
    ]
}

fn records_with_storage(storage: StorageEncoding) -> Vec<VectorRecord> {
    vectors()
        .into_iter()
        .map(|(id, vector)| VectorRecord::new(id, vector).with_storage(storage))
        .collect()
}

fn queries() -> Vec<Vec<f32>> {
    vec![
        vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        vec![0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        vec![0.2, 0.1, 0.0, 0.0, -0.4, 0.0, 0.0, 0.9],
        vec![3.0, 2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0],
        vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    ]
}

fn hit_ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter()
        .map(|hit| hit.id.to_utf8_string().unwrap())
        .collect()
}

fn assert_reports_same(left: &[SearchHit], right: &[SearchHit]) {
    assert_eq!(hit_ids(left), hit_ids(right));
    assert_eq!(left.len(), right.len());
    for (left, right) in left.iter().zip(right) {
        assert!(
            (left.distance - right.distance).abs() <= 1e-6,
            "distance mismatch for {}: {} vs {}",
            left.id,
            left.distance,
            right.distance
        );
    }
}

#[test]
fn forced_sparse_storage_searches_identically_to_forced_dense_storage() {
    let dense_dir = tempfile::tempdir().unwrap();
    let sparse_dir = tempfile::tempdir().unwrap();
    let dense_uri = dense_dir.path().to_string_lossy().into_owned();
    let sparse_uri = sparse_dir.path().to_string_lossy().into_owned();

    let mut dense = BorsukIndex::create(index_config(dense_uri, 3)).unwrap();
    let mut sparse = BorsukIndex::create(index_config(sparse_uri, 3)).unwrap();
    dense
        .add(records_with_storage(StorageEncoding::Dense))
        .unwrap();
    sparse
        .add(records_with_storage(StorageEncoding::Sparse))
        .unwrap();

    assert_eq!(dense.stats().dense_encoded_vectors, vectors().len());
    assert_eq!(dense.stats().sparse_encoded_vectors, 0);
    assert_eq!(sparse.stats().dense_encoded_vectors, 0);
    assert_eq!(sparse.stats().sparse_encoded_vectors, vectors().len());

    for query in queries() {
        let dense_ids = dense.search_ids(&query, SearchOptions::exact(6)).unwrap();
        let sparse_ids = sparse.search_ids(&query, SearchOptions::exact(6)).unwrap();
        assert_eq!(dense_ids, sparse_ids);

        let dense_report = dense
            .search_with_report(&query, SearchOptions::exact(6))
            .unwrap();
        let sparse_report = sparse
            .search_with_report(&query, SearchOptions::exact(6))
            .unwrap();
        assert_reports_same(&dense_report.hits, &sparse_report.hits);
    }
}

#[test]
fn auto_storage_uses_sparse_for_mostly_zero_vectors_and_dense_for_dense_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri, 10)).unwrap();

    index
        .add(vec![
            VectorRecord::new("mostly-zero", vec![0.0, 0.0, 2.5, 0.0, 0.0, 0.0, 0.0, 0.0]),
            VectorRecord::new("dense", vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
        ])
        .unwrap();

    let stats = index.stats();
    assert_eq!(stats.sparse_encoded_vectors, 1);
    assert_eq!(stats.dense_encoded_vectors, 1);
}

#[test]
fn from_sparse_searches_identically_to_equivalent_dense_record() {
    let dense_dir = tempfile::tempdir().unwrap();
    let sparse_input_dir = tempfile::tempdir().unwrap();
    let dense_uri = dense_dir.path().to_string_lossy().into_owned();
    let sparse_input_uri = sparse_input_dir.path().to_string_lossy().into_owned();

    let mut dense = BorsukIndex::create(index_config(dense_uri, 2)).unwrap();
    let mut sparse_input = BorsukIndex::create(index_config(sparse_input_uri, 2)).unwrap();
    dense
        .add(vec![
            VectorRecord::new("target", vec![0.0, 0.0, 3.0, 0.0, 0.0, -1.5, 0.0, 0.0]),
            VectorRecord::new("other", vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ])
        .unwrap();
    sparse_input
        .add(vec![
            VectorRecord::from_sparse("target", vec![2, 5], vec![3.0, -1.5], DIMENSIONS).unwrap(),
            VectorRecord::new("other", vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ])
        .unwrap();

    let query = vec![0.0, 0.0, 3.0, 0.0, 0.0, -1.5, 0.0, 0.0];
    let dense_report = dense
        .search_with_report(&query, SearchOptions::exact(2))
        .unwrap();
    let sparse_input_report = sparse_input
        .search_with_report(&query, SearchOptions::exact(2))
        .unwrap();

    assert_reports_same(&dense_report.hits, &sparse_input_report.hits);
}

#[test]
fn sparse_encoded_vectors_survive_reopen_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri.clone(), 1)).unwrap();
    index
        .add(records_with_storage(StorageEncoding::Sparse))
        .unwrap();
    drop(index);

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.stats().sparse_encoded_vectors, vectors().len());
    for query in queries() {
        let expected = BorsukIndex::open(&uri)
            .unwrap()
            .search_with_report(&query, SearchOptions::exact(6))
            .unwrap()
            .hits;
        let actual = reopened
            .search_with_report(&query, SearchOptions::exact(6))
            .unwrap()
            .hits;
        assert_reports_same(&expected, &actual);
    }

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(10),
            min_segments: 2,
            target_segment_max_vectors: Some(3),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(compaction.compacted);

    let dense_dir = tempfile::tempdir().unwrap();
    let dense_uri = dense_dir.path().to_string_lossy().into_owned();
    let mut dense = BorsukIndex::create(index_config(dense_uri, 3)).unwrap();
    dense
        .add(records_with_storage(StorageEncoding::Dense))
        .unwrap();

    for query in queries() {
        let dense_report = dense
            .search_with_report(&query, SearchOptions::exact(6))
            .unwrap();
        let compacted_report = reopened
            .search_with_report(&query, SearchOptions::exact(6))
            .unwrap();
        assert_reports_same(&dense_report.hits, &compacted_report.hits);
    }
}

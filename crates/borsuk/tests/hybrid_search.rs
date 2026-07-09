#![allow(missing_docs)]

use borsuk::{
    BorsukError, BorsukIndex, Fusion, HybridOptions, HybridQuery, IndexConfig, RecallGuarantee,
    SearchHit, SearchOptions, SearchTerminationReason, SparseVector, VectorMetric, VectorRecord,
};

fn index_config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        sparse: true,
        text: true,
    }
}

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).unwrap()
}

fn repeated_text(term_count: usize) -> String {
    std::iter::repeat_n("needle", term_count)
        .collect::<Vec<_>>()
        .join(" ")
}

fn hybrid_record(id: &str, dense_x: f32, sparse_weight: f32, text_terms: usize) -> VectorRecord {
    VectorRecord::new(id, vec![dense_x, 0.0])
        .with_sparse(vec![7], vec![sparse_weight])
        .unwrap()
        .with_text(repeated_text(text_terms))
}

fn build_index() -> (BorsukIndex, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri)).unwrap();
    index
        .add(vec![
            // Dense: A, B, C, D. Sparse: C, B, A, D. Text: D, B, C, A.
            hybrid_record("doc-a", 0.0, 2.0, 1),
            hybrid_record("doc-b", 1.0, 3.0, 3),
            hybrid_record("doc-c", 2.0, 4.0, 2),
            hybrid_record("doc-d", 3.0, 1.0, 4),
        ])
        .unwrap();
    assert!(
        index.stats().segments >= 2,
        "test setup must create multiple segments"
    );
    (index, dir)
}

fn hit_ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter()
        .map(|hit| hit.id.to_utf8_string().unwrap())
        .collect()
}

fn hybrid_options(k: usize, fusion: Fusion) -> HybridOptions {
    let mut options = HybridOptions::new(k);
    options.fusion = fusion;
    options.candidate_depth = 4;
    options.dense_options = SearchOptions::exact(4);
    options
}

fn hybrid_query() -> HybridQuery {
    HybridQuery::new()
        .with_dense(vec![0.0, 0.0])
        .with_sparse(sparse(&[7], &[1.0]))
        .with_text("needle")
}

fn rrf_score(ranks: &[usize], k: usize) -> f32 {
    ranks
        .iter()
        .map(|rank| 1.0 / (k as f32 + *rank as f32))
        .sum()
}

#[test]
fn rrf_fuses_dense_sparse_and_bm25_rankings() {
    let (index, _dir) = build_index();

    let report = index
        .search_hybrid(&hybrid_query(), hybrid_options(3, Fusion::Rrf { k: 60 }))
        .unwrap();

    assert_eq!(hit_ids(&report.hits), vec!["doc-b", "doc-c", "doc-a"]);
    assert_eq!(report.leaf_mode, "hybrid");
    assert_eq!(report.termination_reason, SearchTerminationReason::Complete);
    assert_eq!(report.recall_guarantee, RecallGuarantee::Approximate);

    let expected = [
        ("doc-b", rrf_score(&[1, 1, 1], 60)),
        ("doc-c", rrf_score(&[2, 0, 2], 60)),
        ("doc-a", rrf_score(&[0, 2, 3], 60)),
    ];
    for (hit, (expected_id, expected_score)) in report.hits.iter().zip(expected) {
        assert_eq!(hit.id.as_str(), expected_id);
        assert!(
            (hit.distance + expected_score).abs() <= 1e-6,
            "hit {expected_id} distance {} expected fused score {expected_score}",
            hit.distance
        );
    }
}

#[test]
fn weighted_single_modality_weights_reproduce_that_modality_ordering() {
    let (index, _dir) = build_index();
    let sparse_query = sparse(&[7], &[1.0]);

    let dense_only = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(4))
        .unwrap();
    let dense_weighted = index
        .search_hybrid(
            &hybrid_query(),
            hybrid_options(
                4,
                Fusion::Weighted {
                    dense: 1.0,
                    sparse: 0.0,
                    text: 0.0,
                },
            ),
        )
        .unwrap();
    assert_eq!(hit_ids(&dense_weighted.hits), hit_ids(&dense_only.hits));

    let sparse_only = index.search_sparse(&sparse_query, 4).unwrap();
    let sparse_weighted = index
        .search_hybrid(
            &hybrid_query(),
            hybrid_options(
                4,
                Fusion::Weighted {
                    dense: 0.0,
                    sparse: 1.0,
                    text: 0.0,
                },
            ),
        )
        .unwrap();
    assert_eq!(hit_ids(&sparse_weighted.hits), hit_ids(&sparse_only.hits));
}

#[test]
fn sparse_only_hybrid_query_returns_sparse_top_k() {
    let (index, _dir) = build_index();
    let sparse_query = sparse(&[7], &[1.0]);

    let report = index
        .search_hybrid(
            &HybridQuery::new().with_sparse(sparse_query.clone()),
            hybrid_options(2, Fusion::Rrf { k: 60 }),
        )
        .unwrap();
    let sparse_only = index.search_sparse(&sparse_query, 2).unwrap();

    assert_eq!(hit_ids(&report.hits), hit_ids(&sparse_only.hits));
}

#[test]
fn search_hybrid_rejects_empty_query_and_zero_k() {
    let (index, _dir) = build_index();

    let empty_query = index
        .search_hybrid(&HybridQuery::new(), HybridOptions::new(3))
        .unwrap_err();
    assert!(
        matches!(
            empty_query,
            BorsukError::InvalidSearchOptions(ref message)
                if message == "hybrid query must set at least one of dense, sparse, text"
        ),
        "{empty_query:?}"
    );

    let zero_k = index
        .search_hybrid(
            &HybridQuery::new().with_sparse(sparse(&[7], &[1.0])),
            HybridOptions::new(0),
        )
        .unwrap_err();
    assert!(
        matches!(
            zero_k,
            BorsukError::InvalidSearchOptions(ref message)
                if message == "k must be greater than zero"
        ),
        "{zero_k:?}"
    );
}

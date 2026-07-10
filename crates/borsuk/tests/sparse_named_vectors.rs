#![allow(missing_docs)]

//! End-to-end coverage for high-dimensional sparse named vectors served by the
//! inverted-index backend. Nothing here densifies: the named vector spans a
//! 100k-term vocabulary while every record and query carries only ~15
//! non-zeros. Results are cross-checked against an exact brute-force sparse dot.

use std::collections::{BTreeMap, BTreeSet};

use borsuk::{
    BorsukIndex, Fusion, HybridOptions, HybridQuery, IndexConfig, SearchOptions, SparseVector,
    VectorKind, VectorMetric, VectorRecord, VectorSpec, sparse_dot,
};

const VOCAB: u32 = 100_000;
const NNZ: usize = 15;

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::from([(
            "lexical".to_string(),
            VectorSpec {
                dimensions: VOCAB as usize,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::Sparse,
            },
        )]),
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn random_sparse(seed: u64) -> (Vec<u32>, Vec<f32>) {
    let mut indices = BTreeSet::new();
    let mut state = seed;
    while indices.len() < NNZ {
        state = splitmix64(state);
        indices.insert((state % u64::from(VOCAB)) as u32);
    }
    let indices: Vec<u32> = indices.into_iter().collect();
    let mut vstate = seed ^ 0xABCD;
    let values = indices
        .iter()
        .map(|&i| {
            vstate = splitmix64(vstate ^ u64::from(i));
            (vstate >> 40) as f32 / f32::from(1u16 << 12) + 0.1
        })
        .collect();
    (indices, values)
}

fn brute_force(rows: &[(String, SparseVector)], query: &SparseVector, k: usize) -> Vec<String> {
    let mut scored = rows
        .iter()
        .enumerate()
        .filter_map(|(row, (id, vector))| {
            let score = sparse_dot(query, vector);
            (score > 0.0).then_some((row, id.clone(), score))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    scored.truncate(k);
    scored.into_iter().map(|(_, id, _)| id).collect()
}

fn ids(hits: Vec<borsuk::SearchHit>) -> Vec<String> {
    hits.into_iter().map(|hit| hit.id.to_string()).collect()
}

#[test]
fn sparse_named_search_matches_brute_force_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();

    let mut rows: Vec<(String, SparseVector)> = Vec::new();
    for i in 0..60u64 {
        let id = format!("doc-{i}");
        let (indices, values) = random_sparse(1000 + i);
        rows.push((
            id.clone(),
            SparseVector::new(indices.clone(), values.clone()).unwrap(),
        ));
        index
            .add(vec![
                VectorRecord::new(id, vec![i as f32, 0.0])
                    .with_named_sparse_vector("lexical", indices, values)
                    .unwrap(),
            ])
            .unwrap();
    }

    for q in 0..12u64 {
        let (qi, qv) = random_sparse(9000 + q);
        let query = SparseVector::new(qi.clone(), qv.clone()).unwrap();
        let got = ids(index.search_sparse_named("lexical", qi, qv, 5).unwrap());
        assert_eq!(got, brute_force(&rows, &query, 5), "query {q}");
    }

    // The inverted index rebuilds from the persisted rows on reopen.
    let reopened = BorsukIndex::open(&uri).unwrap();
    let (qi, qv) = random_sparse(9001);
    let query = SparseVector::new(qi.clone(), qv.clone()).unwrap();
    assert_eq!(
        ids(reopened.search_sparse_named("lexical", qi, qv, 5).unwrap()),
        brute_force(&rows, &query, 5),
    );
}

#[test]
fn deleting_records_drops_them_from_the_sparse_index() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    // Three docs that all share term 7 so every one is a candidate.
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0])
                .with_named_sparse_vector("lexical", vec![7], vec![1.0])
                .unwrap(),
            VectorRecord::new("b", vec![1.0, 0.0])
                .with_named_sparse_vector("lexical", vec![7], vec![3.0])
                .unwrap(),
            VectorRecord::new("c", vec![2.0, 0.0])
                .with_named_sparse_vector("lexical", vec![7], vec![2.0])
                .unwrap(),
        ])
        .unwrap();

    assert_eq!(
        ids(index
            .search_sparse_named("lexical", vec![7], vec![1.0], 3)
            .unwrap()),
        ["b", "c", "a"],
    );

    index.delete(["b"]).unwrap();

    assert_eq!(
        ids(index
            .search_sparse_named("lexical", vec![7], vec![1.0], 3)
            .unwrap()),
        ["c", "a"],
    );
}

#[test]
fn hybrid_fuses_a_sparse_named_leg_with_the_primary_vector() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    // "b" is the best match on BOTH legs: nearest primary vector and the
    // strongest term-1 weight, so it must top the fused ranking regardless of
    // fusion ties on the weaker docs.
    index
        .add(vec![
            VectorRecord::new("a", vec![5.0, 0.0])
                .with_named_sparse_vector("lexical", vec![1], vec![0.5])
                .unwrap(),
            VectorRecord::new("b", vec![0.1, 0.0])
                .with_named_sparse_vector("lexical", vec![1], vec![5.0])
                .unwrap(),
            VectorRecord::new("c", vec![10.0, 0.0])
                .with_named_sparse_vector("lexical", vec![2], vec![5.0])
                .unwrap(),
        ])
        .unwrap();

    let query = HybridQuery::new()
        .with_vector("", vec![0.0, 0.0])
        .with_named_sparse_query("lexical", vec![1], vec![1.0]);
    let report = index
        .search_hybrid(
            &query,
            HybridOptions {
                k: 3,
                fusion: Fusion::Rrf { k: 60 },
                candidate_depth: 3,
                dense_options: SearchOptions::exact(3),
            },
        )
        .unwrap();

    assert_eq!(report.hits[0].id.to_string(), "b");
    // "c" shares no query term, so it never enters the sparse leg, but the
    // dense leg still surfaces it.
    let fused: Vec<String> = report.hits.iter().map(|h| h.id.to_string()).collect();
    assert!(fused.contains(&"a".to_string()) && fused.contains(&"c".to_string()));
}

#[test]
fn sparse_data_on_dense_named_vector_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut config = config(uri);
    config.named_vectors.insert(
        "dense".to_string(),
        VectorSpec {
            dimensions: 4,
            metric: VectorMetric::Euclidean,
            kind: VectorKind::Dense,
        },
    );
    let mut index = BorsukIndex::create(config).unwrap();

    let err = index
        .add(vec![
            VectorRecord::new("x", vec![0.0, 0.0])
                .with_named_sparse_vector("dense", vec![1], vec![1.0])
                .unwrap(),
        ])
        .unwrap_err();
    assert!(
        err.to_string().contains("dense named vector `dense`"),
        "{err}"
    );
}

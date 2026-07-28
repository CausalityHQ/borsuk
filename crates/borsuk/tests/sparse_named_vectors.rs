#![allow(missing_docs)]

//! End-to-end coverage for high-dimensional sparse named vectors served by the
//! inverted-index backend. Nothing here densifies: the named vector spans a
//! 100k-term vocabulary while every record and query carries only ~15
//! non-zeros. Results are cross-checked against an exact brute-force sparse dot.

#[allow(dead_code)]
mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use borsuk::{
    BorsukIndex, Fusion, HybridOptions, HybridQuery, IndexConfig, LeafCapability, SearchOptions,
    SparseVector, VectorElementType, VectorKind, VectorMetric, VectorRecord, VectorSpec,
    sparse_dot,
};
use object_store::{ObjectStore, memory::InMemory};

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
                element_type: Default::default(),
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
fn sparse_float16_values_survive_wal_flush_reopen_and_upsert() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index_config = config(uri.clone());
    index_config
        .named_vectors
        .get_mut("lexical")
        .unwrap()
        .element_type = VectorElementType::Float16;
    let mut index = BorsukIndex::create(index_config).unwrap();

    let old_value = 1.000_1;
    let expected_old = f32::from(half::f16::from_f32(old_value));
    index
        .add(vec![
            VectorRecord::new("doc", vec![0.0, 0.0])
                .with_named_sparse_vector("lexical", vec![7], vec![old_value])
                .unwrap(),
        ])
        .unwrap();
    let before_flush = index
        .search_sparse_named("lexical", vec![7], vec![1.0], 1)
        .unwrap();
    assert_eq!(before_flush[0].id.as_str(), "doc");
    assert_eq!(before_flush[0].distance, -expected_old);

    index.flush().unwrap();
    drop(index);
    let mut reopened = BorsukIndex::open(&uri).unwrap();
    let after_reopen = reopened
        .search_sparse_named("lexical", vec![7], vec![1.0], 1)
        .unwrap();
    assert_eq!(after_reopen[0].distance, -expected_old);

    let new_value = 2.000_1;
    let expected_new = f32::from(half::f16::from_f32(new_value));
    reopened
        .upsert(vec![
            VectorRecord::new("doc", vec![1.0, 0.0])
                .with_named_sparse_vector("lexical", vec![7], vec![new_value])
                .unwrap(),
        ])
        .unwrap();
    let after_upsert = reopened
        .search_sparse_named("lexical", vec![7], vec![1.0], 1)
        .unwrap();
    assert_eq!(after_upsert[0].distance, -expected_new);
}

#[test]
fn sparse_block_bounds_skip_low_scoring_parquet_ranges_without_losing_exactness() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index_config = config(uri);
    index_config.segment_max_vectors = 1;
    let mut index = BorsukIndex::create(index_config).unwrap();
    index
        .add(
            (0..40)
                .map(|row| {
                    VectorRecord::new(format!("doc-{row:02}"), vec![row as f32, 0.0])
                        .with_named_sparse_vector("lexical", vec![7], vec![100.0 - row as f32])
                        .unwrap()
                })
                .collect(),
        )
        .unwrap();
    index.flush().unwrap();

    let report = index
        .search_hybrid(
            &HybridQuery::new().with_named_sparse_query("lexical", vec![7], vec![1.0]),
            HybridOptions {
                k: 2,
                fusion: Fusion::Rrf { k: 60 },
                candidate_depth: 2,
                dense_options: SearchOptions::exact(2),
            },
        )
        .unwrap();

    assert_eq!(ids(report.hits), ["doc-00", "doc-01"]);
    assert!(report.segments_skipped > 0);
    assert_eq!(
        report.segments_searched + report.segments_skipped,
        report.segments_total
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
    assert!(report.bytes_read > 0);
    assert!(report.backing_bytes_read > 0);
    assert!(report.backing_reads > 0);
    assert!(report.requests.gets > 0);
    // "c" shares no query term, so it never enters the sparse leg, but the
    // dense leg still surfaces it.
    let fused: Vec<String> = report.hits.iter().map(|h| h.id.to_string()).collect();
    assert!(fused.contains(&"a".to_string()) && fused.contains(&"c".to_string()));
}

#[test]
fn sparse_only_hybrid_reports_logical_and_physical_parquet_reads() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0])
                .with_named_sparse_vector("lexical", vec![7], vec![1.0])
                .unwrap(),
            VectorRecord::new("b", vec![1.0, 0.0])
                .with_named_sparse_vector("lexical", vec![7], vec![2.0])
                .unwrap(),
        ])
        .unwrap();
    index.flush().unwrap();

    let report = index
        .search_hybrid(
            &HybridQuery::new().with_named_sparse_query("lexical", vec![7], vec![1.0]),
            HybridOptions {
                k: 2,
                fusion: Fusion::Rrf { k: 60 },
                candidate_depth: 2,
                dense_options: SearchOptions::exact(2),
            },
        )
        .unwrap();

    assert_eq!(ids(report.hits.clone()), ["b", "a"]);
    assert!(report.segments_searched > 0);
    assert!(report.bytes_read > 0);
    assert!(report.backing_bytes_read > 0);
    assert!(report.backing_reads > 0);
    assert!(report.requests.gets > 0);
}

#[test]
fn sparse_text_hybrid_distinguishes_backing_and_disk_cached_reads() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index_config = config(uri.clone());
    index_config.text = true;
    let mut writer = BorsukIndex::create(index_config).unwrap();
    writer
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0])
                .with_text("alpha")
                .with_named_sparse_vector("lexical", vec![7], vec![1.0])
                .unwrap(),
            VectorRecord::new("b", vec![1.0, 0.0])
                .with_text("alpha alpha")
                .with_named_sparse_vector("lexical", vec![7], vec![2.0])
                .unwrap(),
        ])
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let index = BorsukIndex::open_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();
    let query = HybridQuery::new()
        .with_named_sparse_query("lexical", vec![7], vec![1.0])
        .with_text("alpha");
    let options = HybridOptions {
        k: 2,
        fusion: Fusion::Rrf { k: 60 },
        candidate_depth: 2,
        dense_options: SearchOptions::exact(2),
    };

    let uncached = index.search_hybrid(&query, options.clone()).unwrap();
    assert!(uncached.backing_bytes_read > 0);
    assert!(uncached.backing_reads > 0);

    let cached = index.search_hybrid(&query, options).unwrap();
    assert!(cached.disk_cache_bytes_read > 0);
    assert_eq!(cached.backing_bytes_read, 0);
    assert!(cached.disk_cache_reads > 0);
    assert_eq!(cached.backing_reads, 0);
}

#[test]
fn sparse_and_text_parquet_ranges_overlap_slow_object_reads() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory://parallel-sidecars".to_string();
    let mut index_config = config(uri.clone());
    index_config.segment_max_vectors = 1;
    index_config.text = true;
    let mut writer = BorsukIndex::create_with_object_store_and_leaf_capability(
        Arc::clone(&inner),
        index_config,
        LeafCapability::PqScanOnly,
    )
    .unwrap();
    writer
        .add(
            (0..8)
                .map(|row| {
                    VectorRecord::new(format!("row-{row}"), vec![row as f32, 0.0])
                        .with_text("needle")
                        .with_named_sparse_vector("lexical", vec![7], vec![1.0 + row as f32])
                        .unwrap()
                })
                .collect(),
        )
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let open_slow = || {
        let slow: Arc<dyn ObjectStore> = Arc::new(
            common::FaultInjectingObjectStore::new(Arc::clone(&inner))
                .with_latency(Duration::from_millis(40)),
        );
        let index = BorsukIndex::open_with_object_store(slow, &uri).unwrap();
        index.prepare_serving_metadata().unwrap();
        index
    };

    let sparse_index = open_slow();
    let started = Instant::now();
    let sparse = sparse_index
        .search_hybrid(
            &HybridQuery::new().with_named_sparse_query("lexical", vec![7], vec![1.0]),
            HybridOptions {
                k: 2,
                fusion: Fusion::Rrf { k: 60 },
                candidate_depth: 2,
                dense_options: SearchOptions::exact(2),
            },
        )
        .unwrap();
    let sparse_elapsed = started.elapsed();
    assert!(sparse.backing_reads >= 8);
    assert!(
        sparse_elapsed < Duration::from_millis(340),
        "eight Parquet range plans over 40 ms GETs should overlap, took {sparse_elapsed:?}"
    );

    let text_index = open_slow();
    let started = Instant::now();
    let text = text_index.search_text("needle", 2).unwrap();
    let text_elapsed = started.elapsed();
    assert!(text.backing_reads >= 8);
    assert!(
        text_elapsed < Duration::from_millis(340),
        "eight BM25 Parquet range plans over 40 ms GETs should overlap, took {text_elapsed:?}"
    );

    // Use a fresh handle so this is a cold-versus-cold comparison. Reusing the
    // individual handles would intentionally hit the bounded decoded lexical
    // caches and measure retention instead of cross-leg I/O overlap.
    let combined_index = open_slow();
    let started = Instant::now();
    let combined = combined_index
        .search_hybrid(
            &HybridQuery::new()
                .with_named_sparse_query("lexical", vec![7], vec![1.0])
                .with_text("needle"),
            HybridOptions {
                k: 2,
                fusion: Fusion::Rrf { k: 1 },
                candidate_depth: 2,
                dense_options: SearchOptions::exact(2),
            },
        )
        .unwrap();
    let combined_elapsed = started.elapsed();
    assert_eq!(
        combined.backing_reads,
        sparse.backing_reads + text.backing_reads
    );
    // Require material overlap while leaving scheduler headroom on shared CI
    // hosts. A serial execution is approximately sparse + text; the 3/4
    // allowance still rejects that path without turning sub-millisecond
    // wake-up jitter at a two-wave boundary into a failure.
    let parallel_ceiling =
        sparse_elapsed.max(text_elapsed) + sparse_elapsed.min(text_elapsed) * 3 / 4;
    assert!(
        combined_elapsed < parallel_ceiling,
        "independent sparse and text legs should overlap: sparse={sparse_elapsed:?}, \
         text={text_elapsed:?}, combined={combined_elapsed:?}"
    );
}

#[test]
fn concurrent_users_share_decoded_immutable_lexical_chunks() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory://shared-lexical-chunks".to_string();
    let mut index_config = config(uri.clone());
    index_config.segment_max_vectors = 1;
    let mut writer = BorsukIndex::create_with_object_store_and_leaf_capability(
        Arc::clone(&inner),
        index_config,
        LeafCapability::PqScanOnly,
    )
    .unwrap();
    writer
        .add(
            (0..8)
                .map(|row| {
                    VectorRecord::new(format!("row-{row}"), vec![row as f32, 0.0])
                        .with_named_sparse_vector("lexical", vec![7], vec![1.0 + row as f32])
                        .unwrap()
                })
                .collect(),
        )
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let slow: Arc<dyn ObjectStore> = Arc::new(
        common::FaultInjectingObjectStore::new(inner).with_latency(Duration::from_millis(20)),
    );
    let index = BorsukIndex::open_with_object_store(slow, &uri).unwrap();
    index.prepare_serving_metadata().unwrap();
    let query = HybridQuery::new().with_named_sparse_query("lexical", vec![7], vec![1.0]);
    let options = HybridOptions {
        k: 2,
        fusion: Fusion::Rrf { k: 60 },
        candidate_depth: 2,
        dense_options: SearchOptions::exact(2),
    };
    let baseline = index
        .search_hybrid(&query, options.clone())
        .unwrap()
        .backing_reads;
    assert!(baseline > 0);

    let callers = 4;
    let start = Arc::new(Barrier::new(callers));
    let reports = (0..callers)
        .map(|_| {
            let index = index.clone();
            let query = query.clone();
            let options = options.clone();
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                index.search_hybrid(&query, options).unwrap()
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|caller| caller.join().unwrap())
        .collect::<Vec<_>>();

    assert!(
        reports
            .iter()
            .all(|report| ids(report.hits.clone()) == ["row-7", "row-6"])
    );
    assert!(
        reports
            .iter()
            .map(|report| report.decoded_cache_hits)
            .sum::<usize>()
            > 0
    );
    let shared_reads = reports
        .iter()
        .map(|report| report.backing_reads)
        .sum::<u64>();
    assert!(
        shared_reads < baseline.saturating_mul(2),
        "four overlapping callers used {shared_reads} backing reads; one caller used {baseline}"
    );
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
            element_type: Default::default(),
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

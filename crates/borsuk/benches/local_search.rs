#![allow(missing_docs)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use borsuk::{
    BorsukIndex, IndexConfig, LeafMode, SearchOptions, SearchReport, VectorMetric, VectorRecord,
    recall_at_k, tie_aware_recall_at_k,
};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

const HIGH_RECALL_MIN_TIE_AWARE_RECALL_AT_10: f32 = 0.95;
const BASELINE_MIN_TIE_AWARE_RECALL_AT_10: f32 = 0.10;

struct BuiltIndex {
    _dir: tempfile::TempDir,
    uri: String,
    index: BorsukIndex,
}

#[derive(Debug, Clone, Copy)]
enum Dataset {
    Uniform,
    Clustered,
    Adversarial,
}

impl Dataset {
    fn vector(self, seed: usize, dimensions: usize) -> Vec<f32> {
        match self {
            Self::Uniform => deterministic_vector(seed, dimensions),
            Self::Clustered => clustered_vector(seed, dimensions),
            Self::Adversarial => adversarial_vector(seed, dimensions),
        }
    }
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| {
            let value = (seed.wrapping_mul(31) + dim.wrapping_mul(17)) % 997;
            value as f32 / 997.0
        })
        .collect()
}

fn clustered_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    let cluster = seed % 16;
    (0..dimensions)
        .map(|dim| {
            let center = if dim % 16 == cluster { 8.0 } else { 0.0 };
            let jitter = (seed.wrapping_mul(37) + dim.wrapping_mul(19)) % 101;
            center + (jitter as f32 - 50.0) / 500.0
        })
        .collect()
}

fn adversarial_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| {
            let sign = if (seed + dim).is_multiple_of(2) {
                1.0
            } else {
                -1.0
            };
            let perturbation = (seed.wrapping_mul(13) + dim.wrapping_mul(7)) % 17;
            sign + perturbation as f32 / 10_000.0
        })
        .collect()
}

fn approx_options(leaf_mode: LeafMode) -> SearchOptions {
    SearchOptions::approx(10, leaf_mode).with_max_candidates_per_segment(64)
}

fn build_index(record_count: usize, dimensions: usize) -> BuiltIndex {
    build_index_with_dataset(record_count, dimensions, Dataset::Uniform)
}

fn build_index_with_dataset(
    record_count: usize,
    dimensions: usize,
    dataset: Dataset,
) -> BuiltIndex {
    let dir = tempfile::tempdir().expect("temp dir");
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors: 256,
        ram_budget_bytes: None,
    })
    .expect("create index");

    let records = (0..record_count)
        .map(|idx| VectorRecord::new(format!("doc-{idx}"), dataset.vector(idx, dimensions)))
        .collect::<Vec<_>>();
    index.add(records).expect("insert vectors");
    BuiltIndex {
        _dir: dir,
        uri,
        index,
    }
}

/// Latency of appending a single new record into a warm index. Each iteration
/// inserts a unique id (re-adding a deleted/existing id is rejected), so the
/// counter starts above the warm-up range and never repeats.
fn bench_single_insert_latency(c: &mut Criterion) {
    let warm = 5_000;
    let dimensions = 64;
    let dir = tempfile::tempdir().expect("temp dir");
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors: 256,
        ram_budget_bytes: None,
    })
    .expect("create index");
    index
        .add(
            (0..warm)
                .map(|idx| {
                    VectorRecord::new(format!("doc-{idx}"), deterministic_vector(idx, dimensions))
                })
                .collect::<Vec<_>>(),
        )
        .expect("warm index");

    let next = AtomicUsize::new(warm);
    c.bench_function("local_single_insert_latency_64", |b| {
        b.iter(|| {
            let seed = next.fetch_add(1, Ordering::Relaxed);
            index
                .add(vec![VectorRecord::new(
                    format!("ins-{seed}"),
                    deterministic_vector(seed, dimensions),
                )])
                .expect("insert one");
        });
    });
}

/// Latency of appending a batch of 256 new records. Each iteration works on a
/// fresh index (built in the setup closure) so batches never collide on ids and
/// the measured time is a clean append of one full segment's worth of vectors.
fn bench_batch_insert_latency(c: &mut Criterion) {
    let batch = 256;
    let dimensions = 64;
    c.bench_function("local_batch_insert_256_64", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("temp dir");
                let uri = dir.path().to_string_lossy().into_owned();
                let index = BorsukIndex::create(IndexConfig {
                    uri,
                    metric: VectorMetric::Euclidean,
                    dimensions,
                    segment_max_vectors: 256,
                    ram_budget_bytes: None,
                })
                .expect("create index");
                let records = (0..batch)
                    .map(|idx| {
                        VectorRecord::new(
                            format!("doc-{idx}"),
                            deterministic_vector(idx, dimensions),
                        )
                    })
                    .collect::<Vec<_>>();
                // Keep the temp dir alive for the whole measured insert.
                (dir, index, records)
            },
            |(dir, mut index, records)| {
                index.add(records).expect("insert batch");
                drop(index);
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_exact_search(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);

    c.bench_function("local_exact_search_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_ids(&query, SearchOptions::exact(10))
                .expect("search")
        });
    });
}

fn bench_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, LeafMode::Graph, false);

    c.bench_function("local_approx_report_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options(LeafMode::Graph))
                .expect("approx search")
        });
    });
}

fn bench_flat_scan_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, LeafMode::FlatScan, false);

    c.bench_function("local_flat_scan_approx_report_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options(LeafMode::FlatScan))
                .expect("flat-scan approx search")
        });
    });
}

fn bench_sq_scan_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, LeafMode::SqScan, false);

    c.bench_function("local_sq_scan_approx_report_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options(LeafMode::SqScan))
                .expect("sq-scan approx search")
        });
    });
}

fn bench_pq_scan_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, LeafMode::PqScan, false);

    c.bench_function("local_pq_scan_approx_report_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options(LeafMode::PqScan))
                .expect("pq-scan approx search")
        });
    });
}

fn bench_vamana_pq_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, LeafMode::VamanaPq, false);

    c.bench_function("local_vamana_pq_approx_report_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options(LeafMode::VamanaPq))
                .expect("vamana-pq approx search")
        });
    });
}

fn bench_hybrid_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, LeafMode::Hybrid, false);

    c.bench_function("local_hybrid_approx_report_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options(LeafMode::Hybrid))
                .expect("hybrid approx search")
        });
    });
}

fn bench_warm_cache_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let cache = tempfile::tempdir().expect("cache dir");
    let index =
        BorsukIndex::open_with_cache(&built.uri, Some(PathBuf::from(cache.path()))).expect("open");
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");

    assert_approx_report(&index, &query, &exact, LeafMode::Graph, false);
    assert_approx_report(&index, &query, &exact, LeafMode::Graph, true);

    c.bench_function("local_warm_cache_approx_report_10k_x_64", |b| {
        b.iter(|| {
            index
                .search_with_report(&query, approx_options(LeafMode::Graph))
                .expect("warm cache approx search")
        });
    });
}

fn bench_clustered_approx_report(c: &mut Criterion) {
    bench_dataset_approx_report(
        c,
        "local_clustered_approx_report_10k_x_64",
        Dataset::Clustered,
        42,
    );
}

fn bench_adversarial_approx_report(c: &mut Criterion) {
    bench_dataset_approx_report(
        c,
        "local_adversarial_approx_report_10k_x_64",
        Dataset::Adversarial,
        0,
    );
}

fn bench_dataset_approx_report(
    c: &mut Criterion,
    bench_name: &'static str,
    dataset: Dataset,
    query_seed: usize,
) {
    let built = build_index_with_dataset(10_000, 64, dataset);
    let query = dataset.vector(query_seed, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, LeafMode::Graph, false);

    c.bench_function(bench_name, |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options(LeafMode::Graph))
                .expect("approx search")
        });
    });
}

fn assert_approx_report(
    index: &BorsukIndex,
    query: &[f32],
    exact: &SearchReport,
    leaf_mode: LeafMode,
    warm: bool,
) {
    let report = index
        .search_with_report(query, approx_options(leaf_mode))
        .expect("approx report");
    let exact_ids = exact
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    let approx_ids = report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();

    let id_recall = recall_at_k(&exact_ids, &approx_ids, 10).expect("id recall");
    let tie_aware_recall =
        tie_aware_recall_at_k(&hit_distances(exact), &hit_distances(&report), 10)
            .expect("tie-aware recall");
    let min_recall = minimum_tie_aware_recall(leaf_mode);
    assert!(
        tie_aware_recall >= min_recall,
        "{leaf_mode} tie-aware recall@10 was {tie_aware_recall:.3}, below {min_recall:.3}; id recall@10 was {id_recall:.3}"
    );
    assert!(report.records_scored < report.records_considered);
    assert_eq!(report.leaf_mode, leaf_mode.to_string());
    if matches!(
        leaf_mode,
        LeafMode::Graph | LeafMode::VamanaPq | LeafMode::Hybrid
    ) {
        assert!(report.graph_bytes_read > 0);
    } else {
        assert_eq!(report.graph_bytes_read, 0);
    }

    if warm {
        assert!(report.object_cache_hits > 0);
        assert_eq!(report.object_cache_misses, 0);
    }
}

fn minimum_tie_aware_recall(leaf_mode: LeafMode) -> f32 {
    match leaf_mode {
        LeafMode::PqScan | LeafMode::VamanaPq | LeafMode::Hybrid => {
            HIGH_RECALL_MIN_TIE_AWARE_RECALL_AT_10
        }
        _ => BASELINE_MIN_TIE_AWARE_RECALL_AT_10,
    }
}

fn hit_distances(report: &SearchReport) -> Vec<f32> {
    report.hits.iter().map(|hit| hit.distance).collect()
}

criterion_group!(
    benches,
    bench_single_insert_latency,
    bench_batch_insert_latency,
    bench_exact_search,
    bench_approx_report,
    bench_flat_scan_approx_report,
    bench_sq_scan_approx_report,
    bench_pq_scan_approx_report,
    bench_vamana_pq_approx_report,
    bench_hybrid_approx_report,
    bench_warm_cache_approx_report,
    bench_clustered_approx_report,
    bench_adversarial_approx_report
);
criterion_main!(benches);

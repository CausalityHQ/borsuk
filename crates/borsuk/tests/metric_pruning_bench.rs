#![allow(missing_docs)]

//! Exact-search pruning across metrics — the RAG-fitness benchmark.
//!
//! RAG stacks overwhelmingly rank by cosine similarity, so the question that
//! matters for them is: does BORSUK prune segments on cosine the way it does on
//! Euclidean, or does exact cosine search read the whole index? This benchmark
//! builds the same clustered dataset under several metrics, runs exact top-k
//! queries, and records how much of the index each query could skip
//! (`prune_pct`) alongside the object reads and latency.
//!
//! It shows that the two metrics RAG leans on — `cosine` and `angular` — prune
//! just like the Lp family (`euclidean`, `manhattan`), while a metric with no
//! sound lower bound (`inner-product`) must scan every candidate segment. Exact
//! results are identical to an independent brute-force top-k for every metric.
//!
//! Fast test (`metric_pruning_is_sound`) is the correctness gate; the ignored
//! `metric_pruning_gate` runs a larger sweep and writes
//! `docs/web/assets/benchmarks/metric-pruning.csv` when `BORSUK_METRIC_PRUNING_OUTPUT`
//! is set.

use std::{env, fs, path::Path, time::Instant};

use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric};

/// A named metric constructor, so the sweep can list metrics as data.
type NamedMetric = (&'static str, fn() -> VectorMetric);

/// Metrics that carry a sound geometric lower bound and therefore prune.
const PRUNABLE: [NamedMetric; 4] = [
    ("cosine", || VectorMetric::Cosine),
    ("angular", || VectorMetric::Angular),
    ("euclidean", || VectorMetric::Euclidean),
    ("manhattan", || VectorMetric::Manhattan),
];

/// A metric with no lower bound: exact search must read every candidate segment.
const SCAN_ONLY: NamedMetric = ("inner-product", || VectorMetric::InnerProduct);

struct BenchConfig {
    dimensions: usize,
    records: usize,
    clusters: usize,
    segment_max_vectors: usize,
    routing_page_fanout: usize,
    queries: usize,
    /// How many times the timed query sweep is repeated. The pruned/recall/bytes
    /// columns are deterministic for fixed queries, so only latency varies across
    /// repetitions — we report its mean and sample standard deviation.
    repetitions: usize,
}

struct MetricRow {
    metric: String,
    prunable: bool,
    segments_total: usize,
    avg_segments_searched: f64,
    prune_pct: f64,
    recall_at_k: f64,
    avg_bytes_read: f64,
    p50_ms_mean: f64,
    p50_ms_std: f64,
}

/// Sample standard deviation (Bessel-corrected). Zero for fewer than two samples.
fn std_dev(values: &[f64], mean: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| (value - mean) * (value - mean))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt()
}

const K: usize = 10;

fn random_unit(state: &mut u64) -> f32 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^= value >> 31;
    (value as f64 / u64::MAX as f64) as f32
}

fn random_signed(state: &mut u64) -> f32 {
    random_unit(state) * 2.0 - 1.0
}

fn cluster_centers(clusters: usize, dimensions: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut state = seed;
    (0..clusters)
        .map(|_| {
            (0..dimensions)
                .map(|_| random_signed(&mut state))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn clustered_records(config: &BenchConfig, seed: u64) -> Vec<borsuk::VectorRecord> {
    let centers = cluster_centers(config.clusters, config.dimensions, seed);
    let per_cluster = config.records.div_ceil(config.clusters);
    let mut state = seed ^ 0xA1_91_71_51;
    let mut records = Vec::with_capacity(config.records);
    for (cluster, center) in centers.iter().enumerate() {
        for _ in 0..per_cluster {
            if records.len() == config.records {
                break;
            }
            // Non-unit scale on purpose: exercises the original-vector path.
            let scale = 0.2 + 5.0 * random_unit(&mut state);
            let vector = center
                .iter()
                .map(|coordinate| (coordinate + 0.02 * random_signed(&mut state)) * scale)
                .collect();
            records.push(borsuk::VectorRecord::new(
                format!("record-{cluster:02}-{:04}", records.len()),
                vector,
            ));
        }
    }
    records
}

fn brute_force_ids(
    records: &[borsuk::VectorRecord],
    query: &[f32],
    metric: &VectorMetric,
    k: usize,
) -> Vec<String> {
    let mut distances = records
        .iter()
        .map(|record| {
            (
                metric.distance(query, &record.vector).unwrap(),
                record.id.to_string(),
            )
        })
        .collect::<Vec<_>>();
    distances.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    distances.into_iter().take(k).map(|(_, id)| id).collect()
}

fn recall_at_k(hits: &[String], expected: &[String]) -> f64 {
    if expected.is_empty() {
        return 1.0;
    }
    let matched = hits.iter().filter(|id| expected.contains(id)).count();
    matched as f64 / expected.len() as f64
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn run_metric(name: &str, metric: VectorMetric, prunable: bool, config: &BenchConfig) -> MetricRow {
    let dir = tempfile::tempdir().unwrap();
    let records = clustered_records(config, 0xB0_25_5E_ED);
    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri: dir.path().to_string_lossy().into_owned(),
            metric: metric.clone(),
            dimensions: config.dimensions,
            segment_max_vectors: config.segment_max_vectors,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        config.routing_page_fanout,
    )
    .unwrap();
    index.add(records.clone()).unwrap();
    let segments_total = index.stats().segments;

    let centers = cluster_centers(config.clusters, config.dimensions, 0xB0_25_5E_ED);
    let queries = config.queries as f64;
    let repetitions = config.repetitions.max(1);

    // The query set is identical each repetition (the RNG seed is reset), so the
    // pruned/recall/bytes results are deterministic — captured once — while the
    // p50 latency is remeasured every repetition to expose timing variance.
    let mut avg_searched = 0.0;
    let mut avg_bytes = 0.0;
    let mut recall = 0.0;
    let mut p50_reps: Vec<f64> = Vec::with_capacity(repetitions);

    for rep in 0..repetitions {
        let mut state = 0xC0_51_4E_u64;
        let mut searched_samples = Vec::new();
        let mut bytes_samples = Vec::new();
        let mut latency_samples = Vec::new();
        let mut recall_sum = 0.0;
        // The pruned/recall/bytes results are identical every repetition, so the
        // expensive independent brute-force check runs only on the first pass;
        // later passes just re-time the same search set.
        let verify = rep == 0;

        for query_index in 0..config.queries {
            let center = &centers[query_index % centers.len()];
            let scale = 0.25 + 4.0 * random_unit(&mut state);
            let query = center
                .iter()
                .map(|coordinate| (coordinate + 0.025 * random_signed(&mut state)) * scale)
                .collect::<Vec<_>>();

            let started = Instant::now();
            let report = index
                .search_with_report(&query, SearchOptions::exact(K))
                .unwrap();
            latency_samples.push(started.elapsed().as_secs_f64() * 1000.0);

            if verify {
                let expected = brute_force_ids(&records, &query, &metric, K);
                let hits = report
                    .hits
                    .iter()
                    .map(|hit| hit.id.to_string())
                    .collect::<Vec<_>>();
                recall_sum += recall_at_k(&hits, &expected);
                searched_samples.push(report.segments_searched as f64);
                bytes_samples.push(report.bytes_read as f64);
            }
        }

        p50_reps.push(median(&mut latency_samples));
        if verify {
            avg_searched = searched_samples.iter().sum::<f64>() / queries;
            avg_bytes = bytes_samples.iter().sum::<f64>() / queries;
            recall = recall_sum / queries;
        }
    }

    let prune_pct = if segments_total > 0 {
        (1.0 - avg_searched / segments_total as f64) * 100.0
    } else {
        0.0
    };
    let p50_ms_mean = p50_reps.iter().sum::<f64>() / p50_reps.len() as f64;
    let p50_ms_std = std_dev(&p50_reps, p50_ms_mean);
    MetricRow {
        metric: name.to_string(),
        prunable,
        segments_total,
        avg_segments_searched: avg_searched,
        prune_pct,
        recall_at_k: recall,
        avg_bytes_read: avg_bytes,
        p50_ms_mean,
        p50_ms_std,
    }
}

fn run_sweep(config: &BenchConfig) -> Vec<MetricRow> {
    let mut rows = Vec::new();
    for (name, metric) in PRUNABLE {
        rows.push(run_metric(name, metric(), true, config));
    }
    let (name, metric) = SCAN_ONLY;
    rows.push(run_metric(name, metric(), false, config));
    rows
}

fn metric_pruning_csv(rows: &[MetricRow]) -> String {
    let mut csv = String::from(
        "metric,prunable,segments_total,avg_segments_searched,prune_pct,recall_at_k,avg_bytes_read,p50_ms_mean,p50_ms_std\n",
    );
    for row in rows {
        csv.push_str(&format!(
            "{},{},{},{:.2},{:.1},{:.4},{:.0},{:.3},{:.3}\n",
            row.metric,
            row.prunable,
            row.segments_total,
            row.avg_segments_searched,
            row.prune_pct,
            row.recall_at_k,
            row.avg_bytes_read,
            row.p50_ms_mean,
            row.p50_ms_std,
        ));
    }
    csv
}

#[test]
fn metric_pruning_is_sound() {
    let config = BenchConfig {
        dimensions: 16,
        records: 1_500,
        clusters: 16,
        segment_max_vectors: 16,
        routing_page_fanout: 8,
        queries: 20,
        repetitions: 3,
    };
    let rows = run_sweep(&config);

    // Every metric — pruned or scanned — returns the exact top-k.
    for row in &rows {
        assert!(
            (row.recall_at_k - 1.0).abs() < f64::EPSILON,
            "{} exact search must be exact (recall {:.4})",
            row.metric,
            row.recall_at_k
        );
        assert!(
            row.segments_total > 1,
            "need a multi-segment index to test pruning"
        );
    }

    // The RAG metrics (cosine, angular) prune, like the Lp family.
    for row in rows.iter().filter(|row| row.prunable) {
        assert!(
            row.prune_pct > 0.0,
            "{} should prune at least some segments, pruned {:.1}%",
            row.metric,
            row.prune_pct
        );
    }

    // Cosine specifically must prune — that is the whole point of the change.
    let cosine = rows.iter().find(|row| row.metric == "cosine").unwrap();
    assert!(
        cosine.avg_segments_searched < cosine.segments_total as f64,
        "cosine exact search must skip segments"
    );

    // The metric with no lower bound scans every segment (nothing to prove skippable).
    let scan_only = rows.iter().find(|row| !row.prunable).unwrap();
    assert!(
        scan_only.prune_pct <= f64::EPSILON,
        "{} has no lower bound and should scan all segments, pruned {:.1}%",
        scan_only.metric,
        scan_only.prune_pct
    );
}

#[test]
#[ignore = "benchmark gate; run explicitly to regenerate metric-pruning.csv"]
fn metric_pruning_gate() {
    let config = BenchConfig {
        dimensions: 32,
        records: 3_000,
        clusters: 24,
        segment_max_vectors: 24,
        routing_page_fanout: 8,
        queries: 40,
        repetitions: 8,
    };
    let rows = run_sweep(&config);
    let csv = metric_pruning_csv(&rows);
    eprintln!("{csv}");
    if let Ok(output) = env::var("BORSUK_METRIC_PRUNING_OUTPUT") {
        fs::write(Path::new(&output), csv).unwrap();
    }
}

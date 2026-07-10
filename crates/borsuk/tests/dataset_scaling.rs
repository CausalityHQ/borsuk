#![allow(missing_docs)]

//! Dataset-scaling sweep: how recall, latency, and resident memory move as the
//! collection grows from ten thousand to ten million vectors. Every point runs
//! the production path — batched ingest, compaction to L1, obsolete-segment GC,
//! then paged (near-zero-RAM) `pq-scan` approximate search graded against exact
//! search. The heavy gate is `#[ignore]`; `dataset_scaling_point_is_sound`
//! keeps a fast version in the normal test run.

use std::{
    env, fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
    time::{Duration, Instant},
};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, LeafMode, SearchOptions,
    VectorMetric, recall_at_k, tie_aware_recall_at_k,
};
use memory_stats::memory_stats;

const DEFAULT_RECORDS: &[usize] = &[10_000, 100_000, 1_000_000, 10_000_000];
const DEFAULT_DIMENSIONS: usize = 16;
// Segments are scanned in full (max candidates == segment size, so no PQ
// approximation on the rows we read). With clustered data (below), routing
// pinpoints the query's cluster, so a modest budget holds recall at 1.0 while
// resident memory stays flat and cold latency stays sub-second.
const DEFAULT_SEGMENT_MAX_VECTORS: usize = 256;
const DEFAULT_BATCH_RECORDS: usize = 8_192;
const DEFAULT_QUERIES: usize = 32;
const DEFAULT_MAX_SEGMENTS: usize = 128;
const DEFAULT_ROUTING_PAGE_OVERFETCH: usize = 16;
const DEFAULT_MAX_CANDIDATES_PER_SEGMENT: usize = 256;
const K: usize = 10;
// Real embeddings are clustered on a manifold, not uniform noise. Each vector
// belongs to a well-separated cluster whose members are its true neighbours;
// ~this many vectors per cluster. Uniform-random data is a pathological ANN
// worst case (neighbours barely separated) and is not representative.
const CLUSTER_SIZE: usize = 256;

struct ScalingConfig {
    dimensions: usize,
    segment_max_vectors: usize,
    batch_records: usize,
    queries: usize,
    max_segments: usize,
    routing_page_overfetch: usize,
    max_candidates_per_segment: usize,
}

struct ScalingPoint {
    records: usize,
    dimensions: usize,
    queries: usize,
    segment_max_vectors: usize,
    max_segments: usize,
    tie_aware_recall_at_10: f32,
    id_recall_at_10: f32,
    p50_ms: u128,
    p95_ms: u128,
    resident_bytes: u64,
    rss_before: Option<u64>,
    rss_peak: Option<u64>,
    avg_bytes_read: u64,
    avg_segments_searched: usize,
    ingest_ms: u128,
    compaction_ms: u128,
}

#[test]
fn dataset_scaling_csv_has_stable_header() {
    let point = ScalingPoint {
        records: 1_000_000,
        dimensions: 16,
        queries: 20,
        segment_max_vectors: 4_096,
        max_segments: 64,
        tie_aware_recall_at_10: 1.0,
        id_recall_at_10: 1.0,
        p50_ms: 6,
        p95_ms: 11,
        resident_bytes: 61_000,
        rss_before: Some(1_000_000),
        rss_peak: Some(1_250_000),
        avg_bytes_read: 14_460_000,
        avg_segments_searched: 64,
        ingest_ms: 142_000,
        compaction_ms: 93_200,
    };
    let csv = dataset_scaling_csv(&[point]);
    assert!(csv.starts_with(
        "records,dimensions,queries,segment_max_vectors,max_segments,tie_aware_recall_at_10,id_recall_at_10,p50_ms,p95_ms,resident_bytes,rss_before,rss_peak,rss_peak_delta,avg_bytes_read,avg_segments_searched,ingest_ms,compaction_ms\n"
    ));
    assert!(csv.contains(
        "\n1000000,16,20,4096,64,1.000000,1.000000,6,11,61000,1000000,1250000,250000,14460000,64,142000,93200\n"
    ));
}

#[test]
fn dataset_scaling_point_is_sound() {
    // A tiny, fast version of the sweep that still exercises the whole path:
    // recall stays valid in [0, 1], resident memory stays small, and a larger
    // dataset never uses less resident memory disproportionately.
    let config = ScalingConfig {
        dimensions: 8,
        segment_max_vectors: 128,
        batch_records: 256,
        queries: 5,
        max_segments: 16,
        routing_page_overfetch: 4,
        max_candidates_per_segment: 64,
    };
    let points = run_sweep(&[512, 2_048], &config);
    assert_eq!(points.len(), 2);
    for point in &points {
        assert!(
            (0.0..=1.0).contains(&point.tie_aware_recall_at_10),
            "recall out of range: {}",
            point.tie_aware_recall_at_10
        );
        assert!((0.0..=1.0).contains(&point.id_recall_at_10));
        assert!(point.resident_bytes < 8 * 1024 * 1024);
        assert!(point.avg_segments_searched >= 1);
    }
    assert!(points[1].records > points[0].records);
}

#[test]
#[ignore = "heavy release gate; run explicitly for records-vs-recall/latency/memory scaling"]
fn dataset_scaling_gate() {
    let records = env_records("BORSUK_SCALING_RECORDS", DEFAULT_RECORDS);
    let config = ScalingConfig {
        dimensions: env_usize("BORSUK_SCALING_DIMENSIONS", DEFAULT_DIMENSIONS),
        segment_max_vectors: env_usize(
            "BORSUK_SCALING_SEGMENT_MAX_VECTORS",
            DEFAULT_SEGMENT_MAX_VECTORS,
        ),
        batch_records: env_usize("BORSUK_SCALING_BATCH_RECORDS", DEFAULT_BATCH_RECORDS),
        queries: env_usize("BORSUK_SCALING_QUERIES", DEFAULT_QUERIES),
        max_segments: env_usize("BORSUK_SCALING_MAX_SEGMENTS", DEFAULT_MAX_SEGMENTS),
        routing_page_overfetch: env_usize(
            "BORSUK_SCALING_ROUTING_PAGE_OVERFETCH",
            DEFAULT_ROUTING_PAGE_OVERFETCH,
        ),
        max_candidates_per_segment: env_usize(
            "BORSUK_SCALING_MAX_CANDIDATES_PER_SEGMENT",
            DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
        ),
    };

    let points = run_sweep(&records, &config);
    for point in &points {
        eprintln!(
            "dataset_scaling records={} recall@10={:.3} id_recall@10={:.3} p50_ms={} p95_ms={} resident_bytes={} rss_peak_delta={} avg_bytes_read={} avg_segments={} ingest_ms={} compaction_ms={}",
            point.records,
            point.tie_aware_recall_at_10,
            point.id_recall_at_10,
            point.p50_ms,
            point.p95_ms,
            point.resident_bytes,
            format_optional_i128(rss_delta(point.rss_before, point.rss_peak)),
            point.avg_bytes_read,
            point.avg_segments_searched,
            point.ingest_ms,
            point.compaction_ms,
        );
    }

    if let Ok(output_path) = env::var("BORSUK_SCALING_OUTPUT") {
        write_dataset_scaling_csv(Path::new(&output_path), &points).unwrap();
    }
}

fn run_sweep(records: &[usize], config: &ScalingConfig) -> Vec<ScalingPoint> {
    records
        .iter()
        .map(|&count| run_point(count, config))
        .collect()
}

fn run_point(record_count: usize, config: &ScalingConfig) -> ScalingPoint {
    let num_clusters = (record_count / CLUSTER_SIZE).max(64);
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: config.dimensions,
        segment_max_vectors: config.segment_max_vectors,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })
    .unwrap();

    let ingest_started = Instant::now();
    let mut inserted = 0_usize;
    while inserted < record_count {
        let end = inserted
            .saturating_add(config.batch_records)
            .min(record_count);
        let vectors = (inserted..end)
            .map(|seed| deterministic_vector(seed, config.dimensions, num_clusters))
            .collect::<Vec<_>>();
        let ids = index.add_vectors(vectors).unwrap();
        assert_eq!(ids.len(), end - inserted);
        inserted = end;
    }
    let ingest_ms = ingest_started.elapsed().as_millis();

    let pre_segments = index.stats().segments;
    let compaction_started = Instant::now();
    if pre_segments > 1 {
        index
            .compact(CompactionOptions {
                source_level: 0,
                target_level: 1,
                max_segments: None,
                min_segments: 1,
                target_segment_max_vectors: Some(config.segment_max_vectors),
                target_segment_max_radius: None,
            })
            .unwrap();
        index
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .unwrap();
    }
    let compaction_ms = compaction_started.elapsed().as_millis();

    let stats = index.stats();
    assert_eq!(stats.records, record_count);
    let resident_bytes = stats.resident_bytes_estimate;

    // Deterministic query seeds spread across the id space, and one approximate
    // configuration used for both warm-up and timing.
    let seed_for = |query_index: usize| {
        query_index
            .wrapping_mul(record_count / config.queries.max(1))
            .min(record_count.saturating_sub(1))
    };
    let run_approx = |query: &[f32]| {
        index
            .search_with_report(
                query,
                SearchOptions::approx(K, LeafMode::PqScan)
                    .with_max_segments(config.max_segments)
                    .with_routing_page_overfetch(config.routing_page_overfetch)
                    .with_max_candidates_per_segment(config.max_candidates_per_segment),
            )
            .unwrap()
    };

    // Warm the routing-page and segment caches so the timed loop measures
    // steady-state latency, not first-touch cold-start noise (which is what made
    // the per-size p50 ordering jitter across runs).
    for query_index in 0..config.queries {
        let query = deterministic_vector(seed_for(query_index), config.dimensions, num_clusters);
        run_approx(&query);
    }

    // Sample resident set size across the whole query loop to capture the true
    // working-memory peak, not just the flat resident-metadata estimate.
    let rss_before = current_rss_bytes();
    let peak_rss = Arc::new(AtomicU64::new(rss_before.unwrap_or(0)));
    let running = Arc::new(AtomicBool::new(true));
    let sampler_running = Arc::clone(&running);
    let sampler_peak = Arc::clone(&peak_rss);
    let sampler = thread::spawn(move || {
        while sampler_running.load(AtomicOrdering::Relaxed) {
            if let Some(rss) = current_rss_bytes() {
                update_peak(&sampler_peak, rss);
            }
            thread::sleep(Duration::from_millis(2));
        }
    });

    let mut latencies_ms = Vec::with_capacity(config.queries);
    let mut tie_recall_sum = 0.0_f32;
    let mut id_recall_sum = 0.0_f32;
    let mut bytes_read_sum = 0_u64;
    let mut segments_sum = 0_usize;
    for query_index in 0..config.queries {
        let query = deterministic_vector(seed_for(query_index), config.dimensions, num_clusters);

        let exact = index
            .search_with_report(&query, SearchOptions::exact(K))
            .unwrap();

        let approx_started = Instant::now();
        let approx = run_approx(&query);
        latencies_ms.push(approx_started.elapsed().as_millis());

        tie_recall_sum +=
            tie_aware_recall_at_k(&hit_distances(&exact), &hit_distances(&approx), K).unwrap();
        id_recall_sum += recall_at_k(&hit_ids(&exact), &hit_ids(&approx), K).unwrap();
        bytes_read_sum += approx.bytes_read;
        segments_sum += approx.segments_searched;
    }

    running.store(false, AtomicOrdering::Relaxed);
    sampler.join().expect("rss sampler should not panic");
    if let Some(rss) = current_rss_bytes() {
        update_peak(&peak_rss, rss);
    }
    let rss_peak = match peak_rss.load(AtomicOrdering::Relaxed) {
        0 => None,
        value => Some(value),
    };

    let queries = config.queries.max(1);
    ScalingPoint {
        records: record_count,
        dimensions: config.dimensions,
        queries: config.queries,
        segment_max_vectors: config.segment_max_vectors,
        max_segments: config.max_segments,
        tie_aware_recall_at_10: tie_recall_sum / queries as f32,
        id_recall_at_10: id_recall_sum / queries as f32,
        p50_ms: percentile(&mut latencies_ms.clone(), 0.50),
        p95_ms: percentile(&mut latencies_ms, 0.95),
        resident_bytes,
        rss_before,
        rss_peak,
        avg_bytes_read: bytes_read_sum / queries as u64,
        avg_segments_searched: segments_sum / queries,
        ingest_ms,
        compaction_ms,
    }
}

fn write_dataset_scaling_csv(path: &Path, points: &[ScalingPoint]) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, dataset_scaling_csv(points))
}

fn dataset_scaling_csv(points: &[ScalingPoint]) -> String {
    let mut csv = String::from(
        "records,dimensions,queries,segment_max_vectors,max_segments,tie_aware_recall_at_10,id_recall_at_10,p50_ms,p95_ms,resident_bytes,rss_before,rss_peak,rss_peak_delta,avg_bytes_read,avg_segments_searched,ingest_ms,compaction_ms\n",
    );
    for point in points {
        csv.push_str(&format!(
            "{},{},{},{},{},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{}\n",
            point.records,
            point.dimensions,
            point.queries,
            point.segment_max_vectors,
            point.max_segments,
            point.tie_aware_recall_at_10,
            point.id_recall_at_10,
            point.p50_ms,
            point.p95_ms,
            point.resident_bytes,
            format_optional_u64(point.rss_before),
            format_optional_u64(point.rss_peak),
            format_optional_i128(rss_delta(point.rss_before, point.rss_peak)),
            point.avg_bytes_read,
            point.avg_segments_searched,
            point.ingest_ms,
            point.compaction_ms,
        ));
    }
    csv
}

fn percentile(values: &mut [u128], quantile: f32) -> u128 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let rank = (quantile * (values.len() - 1) as f32).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn current_rss_bytes() -> Option<u64> {
    memory_stats().map(|stats| stats.physical_mem as u64)
}

fn update_peak(peak: &AtomicU64, candidate: u64) {
    let mut current = peak.load(AtomicOrdering::Relaxed);
    while candidate > current {
        match peak.compare_exchange_weak(
            current,
            candidate,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => break,
            Err(value) => current = value,
        }
    }
}

fn rss_delta(before: Option<u64>, peak: Option<u64>) -> Option<i128> {
    Some(i128::from(peak?) - i128::from(before?))
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_i128(value: Option<i128>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn hit_distances(report: &borsuk::SearchReport) -> Vec<f32> {
    report.hits.iter().map(|hit| hit.distance).collect()
}

fn hit_ids(report: &borsuk::SearchReport) -> Vec<String> {
    report.hits.iter().map(|hit| hit.id.to_string()).collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("{name} must be a usize: {error}"))
        })
        .unwrap_or(default)
}

fn env_records(name: &str, default: &[usize]) -> Vec<usize> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .filter(|count| *count > 0)
                .collect::<Vec<_>>()
        })
        .filter(|counts| !counts.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

// Clustered generator: a vector's cluster id = hash(seed) % num_clusters, so
// cluster-mates are scattered through the insert order (routing, not insert
// locality, has to find them). The vector is its cluster centre plus small
// noise, with the noise scale far below the typical inter-cluster distance so
// clusters stay well separated — the structure real embeddings have.
fn deterministic_vector(seed: usize, dimensions: usize, num_clusters: usize) -> Vec<f32> {
    let cluster = (splitmix64(seed as u64) % num_clusters.max(1) as u64) as usize;
    (0..dimensions)
        .map(|dimension| cluster_center(cluster, dimension) + 0.02 * centered_unit(seed, dimension))
        .collect()
}

fn cluster_center(cluster: usize, dimension: usize) -> f32 {
    // Distinct, deterministic centre per cluster, spread across [-0.5, 0.5].
    centered_unit(cluster.wrapping_mul(0x9E37_79B9).wrapping_add(1), dimension)
}

fn centered_unit(seed: usize, dimension: usize) -> f32 {
    let mixed = splitmix64(
        (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (dimension as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9),
    );
    let unit = (mixed >> 40) as f32 / (1_u64 << 24) as f32;
    unit - 0.5
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

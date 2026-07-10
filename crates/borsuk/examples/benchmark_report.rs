#![allow(missing_docs)]

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
    time::{Duration, Instant},
};

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LeafMode, SearchHit, SearchOptions, SearchReport,
    VectorMetric, VectorRecord, recall_at_k, tie_aware_recall_at_k,
};
use memory_stats::memory_stats;

const DEFAULT_SYNTHETIC_RECORDS: usize = 10_000;
const DEFAULT_DIMENSIONS: usize = 64;
const DEFAULT_QUERIES: usize = 20;
const DEFAULT_SEGMENT_MAX_VECTORS: usize = 256;
const DEFAULT_MAX_SEGMENTS: usize = 8;
const DEFAULT_ROUTING_PAGE_OVERFETCH: usize = 8;
const DEFAULT_MAX_CANDIDATES_PER_SEGMENT: usize = 64;
const ROUTING_OVERFETCH_SWEEP: &[usize] = &[1, 2, 4, 8, 16, 32];
const HIGH_RECALL_MIN_TIE_AWARE_RECALL_AT_10: f64 = 0.95;
const HIGH_RECALL_MODES: &[&str] = &["pq-scan", "vamana-pq", "hybrid"];
const SERIAL_PREFETCH_DEPTH: usize = 1;
const PIPELINED_PREFETCH_DEPTH: usize = 8;
const READ_STRATIFICATION_PREFETCH_DEPTHS: &[usize] =
    &[SERIAL_PREFETCH_DEPTH, PIPELINED_PREFETCH_DEPTH];
const CACHE_STRATIFICATION_CSV_COLUMNS: &str = "prefetch_depth_1_cold_p50_ms,prefetch_depth_1_cold_p95_ms,prefetch_depth_1_warm_p50_ms,prefetch_depth_1_warm_p95_ms,prefetch_depth_1_cold_avg_cache_hits,prefetch_depth_1_cold_avg_cache_misses,prefetch_depth_1_warm_avg_cache_hits,prefetch_depth_1_warm_avg_cache_misses,prefetch_depth_8_cold_p50_ms,prefetch_depth_8_cold_p95_ms,prefetch_depth_8_warm_p50_ms,prefetch_depth_8_warm_p95_ms,prefetch_depth_8_cold_avg_cache_hits,prefetch_depth_8_cold_avg_cache_misses,prefetch_depth_8_warm_avg_cache_hits,prefetch_depth_8_warm_avg_cache_misses";

#[derive(Debug, Clone, Copy)]
enum SyntheticDataset {
    Uniform,
    Clustered,
    Adversarial,
}

impl SyntheticDataset {
    fn name(self) -> &'static str {
        match self {
            Self::Uniform => "synthetic-uniform",
            Self::Clustered => "synthetic-clustered",
            Self::Adversarial => "synthetic-adversarial",
        }
    }

    fn vector(self, seed: usize, dimensions: usize) -> Vec<f32> {
        match self {
            Self::Uniform => deterministic_vector(seed, dimensions),
            Self::Clustered => clustered_vector(seed, dimensions),
            Self::Adversarial => adversarial_vector(seed, dimensions),
        }
    }
}

#[derive(Debug, Clone)]
struct Dataset {
    name: String,
    metric: VectorMetric,
    dimensions: usize,
    records: Vec<VectorRecord>,
    queries: Vec<Vec<f32>>,
}

#[derive(Debug, Clone)]
struct Args {
    synthetic_records: usize,
    synthetic_record_counts: Vec<usize>,
    dimensions: usize,
    segment_max_vectors: usize,
    max_segments: usize,
    routing_page_overfetch: usize,
    max_candidates_per_segment: usize,
    queries: usize,
    csv: Option<String>,
    csv_name: String,
    csv_dimensions: Option<usize>,
    artifacts_dir: Option<PathBuf>,
    parallelism: Vec<usize>,
}

#[derive(Debug)]
struct ModeSummary {
    dataset: String,
    mode: String,
    records: usize,
    dimensions: usize,
    segment_max_vectors: usize,
    max_segments: usize,
    routing_page_overfetch: usize,
    max_candidates_per_segment: usize,
    queries: usize,
    recall_sum: f64,
    id_recall_sum: f64,
    durations: Vec<Duration>,
    bytes_read: u128,
    graph_bytes_read: u128,
    routing_page_indexes_read: u128,
    routing_pages_read: u128,
    segments_searched: u128,
    records_considered: u128,
    records_scored: u128,
    resident_bytes_estimate: u128,
    object_cache_hits: u128,
    object_cache_misses: u128,
    termination_reasons: BTreeMap<String, usize>,
    prefetch_depth_1_cache: CacheDepthSummary,
    prefetch_depth_8_cache: CacheDepthSummary,
}

#[derive(Debug, Clone, Copy)]
enum CachePhase {
    Cold,
    Warm,
}

#[derive(Debug)]
struct CachePhaseSummary {
    durations: Vec<Duration>,
    object_cache_hits: u128,
    object_cache_misses: u128,
}

#[derive(Debug)]
struct CacheDepthSummary {
    cold: CachePhaseSummary,
    warm: CachePhaseSummary,
}

#[derive(Debug, Clone, Copy)]
struct SummaryBudgets {
    segment_max_vectors: usize,
    max_segments: usize,
    routing_page_overfetch: usize,
    max_candidates_per_segment: usize,
}

#[derive(Debug)]
struct ParallelSummary {
    dataset: String,
    mode: String,
    records: usize,
    dimensions: usize,
    segment_max_vectors: usize,
    max_segments: usize,
    routing_page_overfetch: usize,
    max_candidates_per_segment: usize,
    parallelism: usize,
    queries: usize,
    recall_sum: f64,
    id_recall_sum: f64,
    durations: Vec<Duration>,
    wall_duration: Duration,
    bytes_read: u128,
    graph_bytes_read: u128,
    routing_page_indexes_read: u128,
    routing_pages_read: u128,
    resident_bytes_estimate: u128,
    object_cache_hits: u128,
    object_cache_misses: u128,
    termination_reasons: BTreeMap<String, usize>,
    rss_before: Option<u64>,
    rss_peak: Option<u64>,
    rss_after: Option<u64>,
}

#[derive(Debug)]
struct LifecycleSummary {
    dataset: String,
    records: usize,
    dimensions: usize,
    segment_max_vectors: usize,
    ingest_duration: Duration,
    compaction_duration: Duration,
    pre_compaction_segments: usize,
    post_compaction_segments: usize,
    compacted_segments_read: usize,
    compacted_segments_written: usize,
    records_rewritten: usize,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    routing_page_indexes_written: usize,
    routing_pages_written: usize,
    graph_payloads_read: usize,
    graph_bytes_read: u64,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
}

#[derive(Debug)]
struct DatasetBenchmarkSuite {
    sequential: Vec<ModeSummary>,
    parallel: Vec<ParallelSummary>,
    routing_overfetch: Vec<ModeSummary>,
    lifecycle: LifecycleSummary,
}

#[derive(Debug)]
struct QueryOutcome {
    duration: Duration,
    report: SearchReport,
}

#[derive(Debug, Clone, Copy)]
enum ModeSpec {
    Exact,
    Approx(LeafMode),
}

impl ModeSpec {
    fn all() -> &'static [Self] {
        &[
            Self::Exact,
            Self::Approx(LeafMode::FlatScan),
            Self::Approx(LeafMode::SqScan),
            Self::Approx(LeafMode::PqScan),
            Self::Approx(LeafMode::Graph),
            Self::Approx(LeafMode::VamanaPq),
            Self::Approx(LeafMode::Hybrid),
        ]
    }

    fn name(self) -> String {
        match self {
            Self::Exact => "exact".to_string(),
            Self::Approx(leaf_mode) => leaf_mode.to_string(),
        }
    }

    fn options(self, args: &Args) -> SearchOptions {
        self.options_with_routing_page_overfetch(args, args.routing_page_overfetch)
    }

    fn options_with_routing_page_overfetch(
        self,
        args: &Args,
        routing_page_overfetch: usize,
    ) -> SearchOptions {
        match self {
            Self::Exact => SearchOptions::exact(10),
            Self::Approx(leaf_mode) => SearchOptions::approx(10, leaf_mode)
                .with_max_segments(args.max_segments)
                .with_routing_page_overfetch(routing_page_overfetch)
                .with_max_candidates_per_segment(args.max_candidates_per_segment),
        }
    }

    fn high_recall() -> &'static [Self] {
        &[
            Self::Approx(LeafMode::PqScan),
            Self::Approx(LeafMode::VamanaPq),
            Self::Approx(LeafMode::Hybrid),
        ]
    }
}

impl ModeSummary {
    #[cfg(test)]
    fn new(dataset: &str, mode: &str, queries: usize, records: usize, dimensions: usize) -> Self {
        Self::new_with_budgets(
            dataset,
            mode,
            queries,
            records,
            dimensions,
            SummaryBudgets::default_test(),
        )
    }

    fn new_with_args(
        dataset: &str,
        mode: &str,
        queries: usize,
        records: usize,
        dimensions: usize,
        args: &Args,
    ) -> Self {
        Self::new_with_budgets(
            dataset,
            mode,
            queries,
            records,
            dimensions,
            SummaryBudgets::from_args(args),
        )
    }

    fn new_with_budgets(
        dataset: &str,
        mode: &str,
        queries: usize,
        records: usize,
        dimensions: usize,
        budgets: SummaryBudgets,
    ) -> Self {
        Self {
            dataset: dataset.to_string(),
            mode: mode.to_string(),
            records,
            dimensions,
            segment_max_vectors: budgets.segment_max_vectors,
            max_segments: budgets.max_segments,
            routing_page_overfetch: budgets.routing_page_overfetch,
            max_candidates_per_segment: budgets.max_candidates_per_segment,
            queries,
            recall_sum: 0.0,
            id_recall_sum: 0.0,
            durations: Vec::with_capacity(queries),
            bytes_read: 0,
            graph_bytes_read: 0,
            routing_page_indexes_read: 0,
            routing_pages_read: 0,
            segments_searched: 0,
            records_considered: 0,
            records_scored: 0,
            resident_bytes_estimate: 0,
            object_cache_hits: 0,
            object_cache_misses: 0,
            termination_reasons: BTreeMap::new(),
            prefetch_depth_1_cache: CacheDepthSummary::new(queries),
            prefetch_depth_8_cache: CacheDepthSummary::new(queries),
        }
    }

    fn push(&mut self, recall: f32, id_recall: f32, duration: Duration, report: &SearchReport) {
        self.recall_sum += f64::from(recall);
        self.id_recall_sum += f64::from(id_recall);
        self.durations.push(duration);
        self.bytes_read += u128::from(report.bytes_read);
        self.graph_bytes_read += u128::from(report.graph_bytes_read);
        self.routing_page_indexes_read += report.routing_page_indexes_read as u128;
        self.routing_pages_read += report.routing_pages_read as u128;
        self.segments_searched += report.segments_searched as u128;
        self.records_considered += report.records_considered as u128;
        self.records_scored += report.records_scored as u128;
        self.resident_bytes_estimate += u128::from(report.resident_bytes_estimate);
        self.object_cache_hits += report.object_cache_hits as u128;
        self.object_cache_misses += report.object_cache_misses as u128;
        *self
            .termination_reasons
            .entry(report.termination_reason.to_string())
            .or_insert(0) += 1;
    }

    fn record_cache_stratification(
        &mut self,
        prefetch_depth: usize,
        phase: CachePhase,
        duration: Duration,
        report: &SearchReport,
    ) {
        let cache_summary = match prefetch_depth {
            SERIAL_PREFETCH_DEPTH => &mut self.prefetch_depth_1_cache,
            PIPELINED_PREFETCH_DEPTH => &mut self.prefetch_depth_8_cache,
            _ => panic!("unsupported benchmark prefetch depth {prefetch_depth}"),
        };
        cache_summary.record(phase, duration, report);
    }

    fn mean_recall(&self) -> f64 {
        self.recall_sum / self.queries as f64
    }

    fn mean_id_recall(&self) -> f64 {
        self.id_recall_sum / self.queries as f64
    }

    fn p50_ms(&self) -> f64 {
        percentile_ms(&self.durations, 0.50)
    }

    fn p95_ms(&self) -> f64 {
        percentile_ms(&self.durations, 0.95)
    }

    fn avg_bytes_read(&self) -> f64 {
        self.bytes_read as f64 / self.queries as f64
    }

    fn avg_graph_bytes_read(&self) -> f64 {
        self.graph_bytes_read as f64 / self.queries as f64
    }

    fn avg_routing_page_indexes_read(&self) -> f64 {
        self.routing_page_indexes_read as f64 / self.queries as f64
    }

    fn avg_routing_pages_read(&self) -> f64 {
        self.routing_pages_read as f64 / self.queries as f64
    }

    fn avg_segments_searched(&self) -> f64 {
        self.segments_searched as f64 / self.queries as f64
    }

    fn avg_records_considered(&self) -> f64 {
        self.records_considered as f64 / self.queries as f64
    }

    fn avg_records_scored(&self) -> f64 {
        self.records_scored as f64 / self.queries as f64
    }

    fn avg_resident_bytes_estimate(&self) -> f64 {
        self.resident_bytes_estimate as f64 / self.queries as f64
    }

    fn avg_cache_hits(&self) -> f64 {
        self.object_cache_hits as f64 / self.queries as f64
    }

    fn avg_cache_misses(&self) -> f64 {
        self.object_cache_misses as f64 / self.queries as f64
    }

    fn termination_reasons(&self) -> String {
        format_termination_reasons(&self.termination_reasons)
    }
}

impl CachePhaseSummary {
    fn new(queries: usize) -> Self {
        Self {
            durations: Vec::with_capacity(queries),
            object_cache_hits: 0,
            object_cache_misses: 0,
        }
    }

    fn push(&mut self, duration: Duration, report: &SearchReport) {
        self.durations.push(duration);
        self.object_cache_hits += report.object_cache_hits as u128;
        self.object_cache_misses += report.object_cache_misses as u128;
    }

    fn p50_ms(&self) -> f64 {
        percentile_ms(&self.durations, 0.50)
    }

    fn p95_ms(&self) -> f64 {
        percentile_ms(&self.durations, 0.95)
    }

    fn avg_cache_hits(&self) -> f64 {
        self.avg_cache_counter(self.object_cache_hits)
    }

    fn avg_cache_misses(&self) -> f64 {
        self.avg_cache_counter(self.object_cache_misses)
    }

    fn avg_cache_counter(&self, counter: u128) -> f64 {
        if self.durations.is_empty() {
            return 0.0;
        }
        counter as f64 / self.durations.len() as f64
    }
}

impl CacheDepthSummary {
    fn new(queries: usize) -> Self {
        Self {
            cold: CachePhaseSummary::new(queries),
            warm: CachePhaseSummary::new(queries),
        }
    }

    fn record(&mut self, phase: CachePhase, duration: Duration, report: &SearchReport) {
        match phase {
            CachePhase::Cold => self.cold.push(duration, report),
            CachePhase::Warm => self.warm.push(duration, report),
        }
    }
}

impl SummaryBudgets {
    fn from_args(args: &Args) -> Self {
        Self {
            segment_max_vectors: args.segment_max_vectors,
            max_segments: args.max_segments,
            routing_page_overfetch: args.routing_page_overfetch,
            max_candidates_per_segment: args.max_candidates_per_segment,
        }
    }

    #[cfg(test)]
    fn default_test() -> Self {
        Self {
            segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
            max_segments: DEFAULT_MAX_SEGMENTS,
            routing_page_overfetch: DEFAULT_ROUTING_PAGE_OVERFETCH,
            max_candidates_per_segment: DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
        }
    }
}

impl ParallelSummary {
    fn mean_recall(&self) -> f64 {
        self.recall_sum / self.queries as f64
    }

    fn mean_id_recall(&self) -> f64 {
        self.id_recall_sum / self.queries as f64
    }

    fn p50_ms(&self) -> f64 {
        percentile_ms(&self.durations, 0.50)
    }

    fn p95_ms(&self) -> f64 {
        percentile_ms(&self.durations, 0.95)
    }

    fn qps(&self) -> f64 {
        self.queries as f64 / self.wall_duration.as_secs_f64()
    }

    fn avg_bytes_read(&self) -> f64 {
        self.bytes_read as f64 / self.queries as f64
    }

    fn avg_graph_bytes_read(&self) -> f64 {
        self.graph_bytes_read as f64 / self.queries as f64
    }

    fn avg_routing_page_indexes_read(&self) -> f64 {
        self.routing_page_indexes_read as f64 / self.queries as f64
    }

    fn avg_routing_pages_read(&self) -> f64 {
        self.routing_pages_read as f64 / self.queries as f64
    }

    fn avg_resident_bytes_estimate(&self) -> f64 {
        self.resident_bytes_estimate as f64 / self.queries as f64
    }

    fn avg_cache_hits(&self) -> f64 {
        self.object_cache_hits as f64 / self.queries as f64
    }

    fn avg_cache_misses(&self) -> f64 {
        self.object_cache_misses as f64 / self.queries as f64
    }

    fn termination_reasons(&self) -> String {
        format_termination_reasons(&self.termination_reasons)
    }

    fn rss_delta(&self) -> Option<i128> {
        Some(i128::from(self.rss_peak?) - i128::from(self.rss_before?))
    }
}

impl LifecycleSummary {
    fn ingest_ms(&self) -> f64 {
        duration_ms(self.ingest_duration)
    }

    fn compaction_ms(&self) -> f64 {
        duration_ms(self.compaction_duration)
    }

    fn ingest_vectors_per_sec(&self) -> f64 {
        throughput_per_sec(self.records, self.ingest_duration)
    }

    fn compaction_vectors_per_sec(&self) -> f64 {
        throughput_per_sec(self.records_rewritten, self.compaction_duration)
    }

    fn compaction_read_bytes_per_sec(&self) -> f64 {
        byte_throughput_per_sec(self.compaction_bytes_read, self.compaction_duration)
    }

    fn compaction_write_bytes_per_sec(&self) -> f64 {
        byte_throughput_per_sec(self.compaction_bytes_written, self.compaction_duration)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse(env::args().skip(1))?;
    let mut datasets = synthetic_datasets(&args);

    if let Some(csv) = &args.csv {
        datasets.push(csv_dataset(
            &args.csv_name,
            Path::new(csv),
            args.csv_dimensions.unwrap_or(args.dimensions),
            args.queries,
        )?);
    }

    println!("# BORSUK Benchmark Report");
    println!();
    println!("Generated with `cargo run --release -p borsuk --example benchmark_report -- ...`.");
    println!();
    println!(
        "Approximate modes use `max_segments={}`, `routing_page_overfetch={}`, \
         and `max_candidates_per_segment={}`.",
        args.max_segments, args.routing_page_overfetch, args.max_candidates_per_segment
    );
    println!(
        "Datasets are bulk inserted through the append path, explicitly compacted into \
         vector-local L1 blobs, then queried. Compaction time is not included in query latencies."
    );
    println!(
        "Headline recall is tie-aware: any hit at or inside the exact kth distance counts, \
         so duplicate vectors with different ids are not penalized. Id recall is reported separately."
    );
    println!();
    let mut sequential_summaries = Vec::new();
    let mut parallel_summaries = Vec::new();
    let mut lifecycle_summaries = Vec::new();
    let mut routing_overfetch_summaries = Vec::new();
    for dataset in &datasets {
        let suite = run_dataset_suite(
            dataset,
            &args,
            &args.parallelism,
            args.artifacts_dir.is_some(),
        )?;
        sequential_summaries.extend(suite.sequential);
        lifecycle_summaries.push(suite.lifecycle);
        parallel_summaries.extend(suite.parallel);
        routing_overfetch_summaries.extend(suite.routing_overfetch);
    }

    validate_high_recall_modes(&sequential_summaries)?;

    print_lifecycle_table(&lifecycle_summaries);
    print_sequential_table(&sequential_summaries);
    print_parallel_table(&parallel_summaries);

    if let Some(artifacts_dir) = &args.artifacts_dir {
        fs::create_dir_all(artifacts_dir)?;
        write_lifecycle_csv(&artifacts_dir.join("lifecycle.csv"), &lifecycle_summaries)?;
        write_sequential_csv(&artifacts_dir.join("sequential.csv"), &sequential_summaries)?;
        write_parallel_csv(&artifacts_dir.join("parallel.csv"), &parallel_summaries)?;
        write_scale_csv(&artifacts_dir.join("scale.csv"), &sequential_summaries)?;
        write_routing_overfetch_csv(
            &artifacts_dir.join("routing-overfetch.csv"),
            &routing_overfetch_summaries,
        )?;
    }

    Ok(())
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut parsed = Self {
            synthetic_records: DEFAULT_SYNTHETIC_RECORDS,
            synthetic_record_counts: vec![DEFAULT_SYNTHETIC_RECORDS],
            dimensions: DEFAULT_DIMENSIONS,
            segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
            max_segments: DEFAULT_MAX_SEGMENTS,
            routing_page_overfetch: DEFAULT_ROUTING_PAGE_OVERFETCH,
            max_candidates_per_segment: DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
            queries: DEFAULT_QUERIES,
            csv: None,
            csv_name: "real-csv".to_string(),
            csv_dimensions: None,
            artifacts_dir: None,
            parallelism: vec![1, 2, 4, 8],
        };
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--synthetic-records" => {
                    parsed.synthetic_records = parse_value(&arg, args.next())?;
                    parsed.synthetic_record_counts = vec![parsed.synthetic_records];
                }
                "--synthetic-records-list" => {
                    parsed.synthetic_record_counts =
                        parse_record_counts(&required_value(&arg, args.next())?)?;
                    parsed.synthetic_records = *parsed
                        .synthetic_record_counts
                        .first()
                        .ok_or("--synthetic-records-list must contain at least one value")?;
                }
                "--dimensions" => {
                    parsed.dimensions = parse_value(&arg, args.next())?;
                }
                "--segment-max-vectors" => {
                    parsed.segment_max_vectors = parse_value(&arg, args.next())?;
                }
                "--max-segments" => {
                    parsed.max_segments = parse_value(&arg, args.next())?;
                }
                "--routing-page-overfetch" => {
                    parsed.routing_page_overfetch = parse_value(&arg, args.next())?;
                }
                "--max-candidates-per-segment" => {
                    parsed.max_candidates_per_segment = parse_value(&arg, args.next())?;
                }
                "--queries" => {
                    parsed.queries = parse_value(&arg, args.next())?;
                }
                "--csv" => {
                    parsed.csv = Some(required_value(&arg, args.next())?);
                }
                "--csv-name" => {
                    parsed.csv_name = required_value(&arg, args.next())?;
                }
                "--csv-dimensions" => {
                    parsed.csv_dimensions = Some(parse_value(&arg, args.next())?);
                }
                "--artifacts-dir" => {
                    parsed.artifacts_dir = Some(PathBuf::from(required_value(&arg, args.next())?));
                }
                "--parallelism" => {
                    parsed.parallelism = parse_parallelism(&required_value(&arg, args.next())?)?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => {
                    return Err(format!("unknown argument `{arg}`").into());
                }
            }
        }
        if parsed.queries == 0 {
            return Err("--queries must be greater than zero".into());
        }
        if parsed.dimensions == 0 {
            return Err("--dimensions must be greater than zero".into());
        }
        if parsed.segment_max_vectors == 0 {
            return Err("--segment-max-vectors must be greater than zero".into());
        }
        if parsed.max_segments == 0 {
            return Err("--max-segments must be greater than zero".into());
        }
        if parsed.routing_page_overfetch == 0 {
            return Err("--routing-page-overfetch must be greater than zero".into());
        }
        if parsed.max_candidates_per_segment == 0 {
            return Err("--max-candidates-per-segment must be greater than zero".into());
        }
        if parsed
            .synthetic_record_counts
            .iter()
            .any(|record_count| *record_count < parsed.queries)
        {
            return Err("--synthetic-records values must be at least --queries".into());
        }
        if parsed.parallelism.is_empty() {
            return Err("--parallelism must contain at least one value".into());
        }
        Ok(parsed)
    }
}

fn print_usage() {
    println!("Usage:");
    println!("  cargo run --release -p borsuk --example benchmark_report -- [options]");
    println!();
    println!("Options:");
    println!("  --synthetic-records N   Synthetic records per generated dataset");
    println!("  --synthetic-records-list LIST");
    println!("                           Comma-separated synthetic record counts for scale sweeps");
    println!("  --dimensions N          Synthetic vector dimensions");
    println!("  --segment-max-vectors N Segment size used for ingest and compaction");
    println!("  --max-segments N        Approximate segment payload budget");
    println!("  --routing-page-overfetch N");
    println!("                           Routing metadata overfetch multiplier");
    println!("  --max-candidates-per-segment N");
    println!("                           Per-segment exact rerank candidate budget");
    println!("  --queries N             Query count per dataset");
    println!("  --csv PATH              Optional real-data CSV; rows are vectors");
    println!("  --csv-name NAME         Display name for the real-data CSV");
    println!("  --csv-dimensions N      Feature columns to read from the CSV");
    println!(
        "  --artifacts-dir PATH    Write lifecycle.csv, sequential.csv, parallel.csv, scale.csv, and routing-overfetch.csv"
    );
    println!("  --parallelism LIST      Comma-separated parallel query counts, default 1,2,4,8");
}

fn synthetic_datasets(args: &Args) -> Vec<Dataset> {
    let include_record_count = args.synthetic_record_counts.len() > 1;
    args.synthetic_record_counts
        .iter()
        .flat_map(|record_count| {
            [
                SyntheticDataset::Uniform,
                SyntheticDataset::Clustered,
                SyntheticDataset::Adversarial,
            ]
            .into_iter()
            .map(move |kind| {
                let mut dataset =
                    synthetic_dataset(kind, *record_count, args.dimensions, args.queries);
                if include_record_count {
                    dataset.name = format!("{}-n{}", dataset.name, record_count);
                }
                dataset
            })
        })
        .collect()
}

fn synthetic_dataset(
    kind: SyntheticDataset,
    record_count: usize,
    dimensions: usize,
    query_count: usize,
) -> Dataset {
    let records = (0..record_count)
        .map(|idx| VectorRecord::new(format!("doc-{idx}"), kind.vector(idx, dimensions)))
        .collect::<Vec<_>>();
    let queries = (0..query_count)
        .map(|idx| kind.vector(idx, dimensions))
        .collect::<Vec<_>>();
    Dataset {
        name: kind.name().to_string(),
        metric: VectorMetric::Euclidean,
        dimensions,
        records,
        queries,
    }
}

fn validate_high_recall_modes(summaries: &[ModeSummary]) -> Result<(), Box<dyn Error>> {
    for summary in summaries
        .iter()
        .filter(|summary| HIGH_RECALL_MODES.contains(&summary.mode.as_str()))
    {
        let recall = summary.mean_recall();
        if recall < HIGH_RECALL_MIN_TIE_AWARE_RECALL_AT_10 {
            return Err(format!(
                "{} {} tie-aware recall@10 {recall:.3} is below {:.3}",
                summary.dataset, summary.mode, HIGH_RECALL_MIN_TIE_AWARE_RECALL_AT_10
            )
            .into());
        }
    }
    Ok(())
}

fn csv_dataset(
    name: &str,
    path: &Path,
    dimensions: usize,
    query_count: usize,
) -> Result<Dataset, Box<dyn Error>> {
    if dimensions == 0 {
        return Err("--csv-dimensions must be greater than zero".into());
    }
    let text = fs::read_to_string(path)?;
    let mut records = Vec::new();
    for (line_index, line) in text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        let values = line
            .split(',')
            .map(|value| value.trim().parse::<f32>())
            .collect::<Result<Vec<_>, _>>()?;
        if values.len() < dimensions {
            return Err(format!(
                "{} line {} has {} columns, expected at least {}",
                path.display(),
                line_index + 1,
                values.len(),
                dimensions
            )
            .into());
        }
        records.push(VectorRecord::new(
            format!("row-{line_index}"),
            values.into_iter().take(dimensions).collect(),
        ));
    }
    if records.len() < query_count {
        return Err(format!(
            "{} has {} rows, but --queries requested {}",
            path.display(),
            records.len(),
            query_count
        )
        .into());
    }
    let queries = records
        .iter()
        .take(query_count)
        .map(|record| record.vector.clone())
        .collect();
    Ok(Dataset {
        name: name.to_string(),
        metric: VectorMetric::Euclidean,
        dimensions,
        records,
        queries,
    })
}

fn run_dataset_suite(
    dataset: &Dataset,
    args: &Args,
    parallelisms: &[usize],
    include_routing_overfetch: bool,
) -> Result<DatasetBenchmarkSuite, Box<dyn Error>> {
    let (dir, index, lifecycle) = build_query_benchmark_index(dataset, args)?;
    let uri = dir.path().to_string_lossy().into_owned();
    let exact_reports = exact_reports_for_index(dataset, &index)?;
    let sequential =
        run_sequential_modes_with_exact_reports(dataset, &index, &uri, &exact_reports, args)?;
    let exact_hits = exact_reports
        .iter()
        .map(|(_, report)| report.hits.clone())
        .collect::<Vec<_>>();
    let parallel =
        run_parallel_modes_with_exact_hits(dataset, &index, &exact_hits, parallelisms, args)?;
    let routing_overfetch = if include_routing_overfetch {
        run_routing_overfetch_modes_with_exact_reports(dataset, &index, &uri, &exact_reports, args)?
    } else {
        Vec::new()
    };

    Ok(DatasetBenchmarkSuite {
        sequential,
        parallel,
        routing_overfetch,
        lifecycle,
    })
}

fn exact_reports_for_index(
    dataset: &Dataset,
    index: &BorsukIndex,
) -> Result<Vec<(Duration, SearchReport)>, Box<dyn Error>> {
    Ok(dataset
        .queries
        .iter()
        .map(|query| timed_report(index, query, SearchOptions::exact(10)))
        .collect::<Result<Vec<_>, _>>()?)
}

fn run_sequential_modes_with_exact_reports(
    dataset: &Dataset,
    index: &BorsukIndex,
    uri: &str,
    exact_reports: &[(Duration, SearchReport)],
    args: &Args,
) -> Result<Vec<ModeSummary>, Box<dyn Error>> {
    let exact_ids = exact_reports
        .iter()
        .map(|(_, report)| hit_ids(report))
        .collect::<borsuk::Result<Vec<_>>>()?;

    let mut summaries = Vec::new();
    let mut exact_summary = ModeSummary::new_with_args(
        &dataset.name,
        "exact",
        dataset.queries.len(),
        dataset.records.len(),
        dataset.dimensions,
        args,
    );
    for (duration, report) in exact_reports {
        exact_summary.push(1.0, 1.0, *duration, report);
    }
    record_cache_stratification_for_mode(
        &mut exact_summary,
        uri,
        &dataset.queries,
        ModeSpec::Exact,
        args,
        args.routing_page_overfetch,
    )?;
    summaries.push(exact_summary);

    for mode in &ModeSpec::all()[1..] {
        let mut summary = ModeSummary::new_with_args(
            &dataset.name,
            &mode.name(),
            dataset.queries.len(),
            dataset.records.len(),
            dataset.dimensions,
            args,
        );
        for (query, ((_, exact_report), exact_ids)) in dataset
            .queries
            .iter()
            .zip(exact_reports.iter().zip(&exact_ids))
        {
            let (duration, report) = timed_report(index, query, mode.options(args))?;
            let ids = hit_ids(&report)?;
            let id_recall = recall_at_k(exact_ids, &ids, 10)?;
            let recall = hit_tie_aware_recall_at_k(&exact_report.hits, &report.hits, 10)?;
            summary.push(recall, id_recall, duration, &report);
        }
        record_cache_stratification_for_mode(
            &mut summary,
            uri,
            &dataset.queries,
            *mode,
            args,
            args.routing_page_overfetch,
        )?;
        summaries.push(summary);
    }

    Ok(summaries)
}

fn run_parallel_modes_with_exact_hits(
    dataset: &Dataset,
    index: &BorsukIndex,
    exact_hits: &[Vec<SearchHit>],
    parallelisms: &[usize],
    args: &Args,
) -> Result<Vec<ParallelSummary>, Box<dyn Error>> {
    let mut summaries = Vec::new();
    for mode in ModeSpec::all() {
        for parallelism in parallelisms {
            summaries.push(run_parallel_mode(
                dataset,
                index,
                exact_hits,
                *mode,
                *parallelism,
                args,
            )?);
        }
    }
    Ok(summaries)
}

fn run_routing_overfetch_modes_with_exact_reports(
    dataset: &Dataset,
    index: &BorsukIndex,
    uri: &str,
    exact_reports: &[(Duration, SearchReport)],
    args: &Args,
) -> Result<Vec<ModeSummary>, Box<dyn Error>> {
    let exact_ids = exact_reports
        .iter()
        .map(|(_, report)| hit_ids(report))
        .collect::<borsuk::Result<Vec<_>>>()?;

    let mut summaries = Vec::new();
    for mode in ModeSpec::high_recall() {
        for routing_page_overfetch in ROUTING_OVERFETCH_SWEEP {
            let mut summary = ModeSummary::new_with_args(
                &dataset.name,
                &mode.name(),
                dataset.queries.len(),
                dataset.records.len(),
                dataset.dimensions,
                args,
            );
            summary.routing_page_overfetch = *routing_page_overfetch;
            for (query, ((_, exact_report), exact_ids)) in dataset
                .queries
                .iter()
                .zip(exact_reports.iter().zip(&exact_ids))
            {
                let (duration, report) = timed_report(
                    index,
                    query,
                    mode.options_with_routing_page_overfetch(args, *routing_page_overfetch),
                )?;
                let ids = hit_ids(&report)?;
                let id_recall = recall_at_k(exact_ids, &ids, 10)?;
                let recall = hit_tie_aware_recall_at_k(&exact_report.hits, &report.hits, 10)?;
                summary.push(recall, id_recall, duration, &report);
            }
            record_cache_stratification_for_mode(
                &mut summary,
                uri,
                &dataset.queries,
                *mode,
                args,
                *routing_page_overfetch,
            )?;
            summaries.push(summary);
        }
    }

    Ok(summaries)
}

fn record_cache_stratification_for_mode(
    summary: &mut ModeSummary,
    uri: &str,
    queries: &[Vec<f32>],
    mode: ModeSpec,
    args: &Args,
    routing_page_overfetch: usize,
) -> Result<(), Box<dyn Error>> {
    for prefetch_depth in READ_STRATIFICATION_PREFETCH_DEPTHS {
        let cache_dir = tempfile::tempdir()?;
        let reader = BorsukIndex::open_with_cache(uri, Some(cache_dir.path().to_path_buf()))?;
        for phase in [CachePhase::Cold, CachePhase::Warm] {
            for query in queries {
                let options = mode
                    .options_with_routing_page_overfetch(args, routing_page_overfetch)
                    .with_prefetch_depth(*prefetch_depth);
                let (duration, report) = timed_report(&reader, query, options)?;
                summary.record_cache_stratification(*prefetch_depth, phase, duration, &report);
            }
        }
    }
    Ok(())
}

fn build_query_benchmark_index(
    dataset: &Dataset,
    args: &Args,
) -> Result<(tempfile::TempDir, BorsukIndex, LifecycleSummary), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: dataset.metric.clone(),
        dimensions: dataset.dimensions,
        segment_max_vectors: args.segment_max_vectors,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })?;

    let ingest_started = Instant::now();
    index.add(dataset.records.clone())?;
    let ingest_duration = ingest_started.elapsed();
    let pre_compaction_segments = index.stats().segments;

    let compaction_started = Instant::now();
    let compaction = compact_for_query_benchmark(&mut index, args)?;
    let compaction_duration = compaction_started.elapsed();
    let post_compaction_segments = index.stats().segments;

    Ok((
        dir,
        index,
        LifecycleSummary {
            dataset: dataset.name.clone(),
            records: dataset.records.len(),
            dimensions: dataset.dimensions,
            segment_max_vectors: args.segment_max_vectors,
            ingest_duration,
            compaction_duration,
            pre_compaction_segments,
            post_compaction_segments,
            compacted_segments_read: compaction.segments_read,
            compacted_segments_written: compaction.segments_written,
            records_rewritten: compaction.records_rewritten,
            routing_page_indexes_read: compaction.routing_page_indexes_read,
            routing_pages_read: compaction.routing_pages_read,
            routing_page_indexes_written: compaction.routing_page_indexes_written,
            routing_pages_written: compaction.routing_pages_written,
            graph_payloads_read: compaction.graph_payloads_read,
            graph_bytes_read: compaction.graph_bytes_read,
            compaction_bytes_read: compaction.bytes_read,
            compaction_bytes_written: compaction.bytes_written,
        },
    ))
}

fn compact_for_query_benchmark(
    index: &mut BorsukIndex,
    args: &Args,
) -> borsuk::Result<borsuk::CompactionReport> {
    index.compact(CompactionOptions {
        source_level: 0,
        target_level: 1,
        max_segments: None,
        min_segments: 2,
        target_segment_max_vectors: Some(args.segment_max_vectors),
        target_segment_max_radius: None,
    })
}

fn run_parallel_mode(
    dataset: &Dataset,
    index: &BorsukIndex,
    exact_hits: &[Vec<SearchHit>],
    mode: ModeSpec,
    parallelism: usize,
    args: &Args,
) -> Result<ParallelSummary, Box<dyn Error>> {
    if parallelism == 0 {
        return Err("parallelism values must be greater than zero".into());
    }

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
        if let Some(rss) = current_rss_bytes() {
            update_peak(&sampler_peak, rss);
        }
    });

    let started = Instant::now();
    let mut handles = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let worker_index = index.clone();
        let queries = dataset.queries.clone();
        let worker_args = args.clone();
        handles.push(thread::spawn(
            move || -> Result<Vec<QueryOutcome>, String> {
                queries
                    .iter()
                    .map(|query| {
                        timed_report(&worker_index, query, mode.options(&worker_args))
                            .map(|(duration, report)| QueryOutcome { duration, report })
                            .map_err(|error| error.to_string())
                    })
                    .collect()
            },
        ));
    }

    let mut outcomes = Vec::with_capacity(parallelism * dataset.queries.len());
    for handle in handles {
        let worker_outcomes = handle
            .join()
            .map_err(|_| "parallel benchmark worker panicked")?
            .map_err(|error| format!("parallel benchmark worker failed: {error}"))?;
        outcomes.extend(worker_outcomes);
    }
    let wall_duration = started.elapsed();
    running.store(false, AtomicOrdering::Relaxed);
    sampler
        .join()
        .map_err(|_| "parallel benchmark memory sampler panicked")?;
    let rss_after = current_rss_bytes();
    if let Some(rss) = rss_after {
        update_peak(&peak_rss, rss);
    }
    let rss_peak = match peak_rss.load(AtomicOrdering::Relaxed) {
        0 => None,
        value => Some(value),
    };

    let mut recall_sum = 0.0_f64;
    let mut id_recall_sum = 0.0_f64;
    let mut durations = Vec::with_capacity(outcomes.len());
    let mut bytes_read = 0_u128;
    let mut graph_bytes_read = 0_u128;
    let mut routing_page_indexes_read = 0_u128;
    let mut routing_pages_read = 0_u128;
    let mut resident_bytes_estimate = 0_u128;
    let mut object_cache_hits = 0_u128;
    let mut object_cache_misses = 0_u128;
    let mut termination_reasons = BTreeMap::<String, usize>::new();
    for (outcome_index, outcome) in outcomes.into_iter().enumerate() {
        let query_index = outcome_index % dataset.queries.len();
        let exact_ids = hit_ids_from_hits(&exact_hits[query_index])?;
        let ids = hit_ids(&outcome.report)?;
        recall_sum += f64::from(hit_tie_aware_recall_at_k(
            &exact_hits[query_index],
            &outcome.report.hits,
            10,
        )?);
        id_recall_sum += f64::from(recall_at_k(&exact_ids, &ids, 10)?);
        durations.push(outcome.duration);
        bytes_read += u128::from(outcome.report.bytes_read);
        graph_bytes_read += u128::from(outcome.report.graph_bytes_read);
        routing_page_indexes_read += outcome.report.routing_page_indexes_read as u128;
        routing_pages_read += outcome.report.routing_pages_read as u128;
        resident_bytes_estimate += u128::from(outcome.report.resident_bytes_estimate);
        object_cache_hits += outcome.report.object_cache_hits as u128;
        object_cache_misses += outcome.report.object_cache_misses as u128;
        *termination_reasons
            .entry(outcome.report.termination_reason.to_string())
            .or_insert(0) += 1;
    }

    Ok(ParallelSummary {
        dataset: dataset.name.clone(),
        mode: mode.name(),
        records: dataset.records.len(),
        dimensions: dataset.dimensions,
        segment_max_vectors: args.segment_max_vectors,
        max_segments: args.max_segments,
        routing_page_overfetch: args.routing_page_overfetch,
        max_candidates_per_segment: args.max_candidates_per_segment,
        parallelism,
        queries: parallelism * dataset.queries.len(),
        recall_sum,
        id_recall_sum,
        durations,
        wall_duration,
        bytes_read,
        graph_bytes_read,
        routing_page_indexes_read,
        routing_pages_read,
        resident_bytes_estimate,
        object_cache_hits,
        object_cache_misses,
        termination_reasons,
        rss_before,
        rss_peak,
        rss_after,
    })
}

fn print_lifecycle_table(summaries: &[LifecycleSummary]) {
    println!("## Ingest and Compaction");
    println!();
    println!(
        "| Dataset | Records | Dimensions | Segment max | Ingest ms | Ingest vectors/sec | Compaction ms | Compaction vectors/sec | Pre segments | Post segments | Segments read | Segments written | Records rewritten | Routing indexes read | Routing pages read | Routing indexes written | Routing pages written | Graph payloads read | Graph bytes read | Compaction bytes read | Compaction bytes written |"
    );
    println!(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for summary in summaries {
        println!(
            "| {} | {} | {} | {} | {:.3} | {:.1} | {:.3} | {:.1} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            summary.dataset,
            summary.records,
            summary.dimensions,
            summary.segment_max_vectors,
            summary.ingest_ms(),
            summary.ingest_vectors_per_sec(),
            summary.compaction_ms(),
            summary.compaction_vectors_per_sec(),
            summary.pre_compaction_segments,
            summary.post_compaction_segments,
            summary.compacted_segments_read,
            summary.compacted_segments_written,
            summary.records_rewritten,
            summary.routing_page_indexes_read,
            summary.routing_pages_read,
            summary.routing_page_indexes_written,
            summary.routing_pages_written,
            summary.graph_payloads_read,
            summary.graph_bytes_read,
            summary.compaction_bytes_read,
            summary.compaction_bytes_written,
        );
    }
    println!();
}

fn print_sequential_table(summaries: &[ModeSummary]) {
    println!("## Query Modes");
    println!();
    println!(
        "| Dataset | Mode | Records | Dimensions | Queries | Tie-aware Recall@10 | Id Recall@10 | p50 ms | p95 ms | Cold p95 d1 | Warm p95 d1 | Cold p95 d8 | Warm p95 d8 | Avg bytes | Avg graph bytes | Avg routing indexes | Avg routing pages | Avg resident bytes | Avg segments | Avg considered | Avg scored | Avg cache hits/misses |"
    );
    println!(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for summary in summaries {
        println!(
            "| {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.0} | {:.0} | {:.1} | {:.1} | {:.0} | {:.1} | {:.0} | {:.0} | {:.1}/{:.1} |",
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.prefetch_depth_1_cache.cold.p95_ms(),
            summary.prefetch_depth_1_cache.warm.p95_ms(),
            summary.prefetch_depth_8_cache.cold.p95_ms(),
            summary.prefetch_depth_8_cache.warm.p95_ms(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
            summary.avg_routing_page_indexes_read(),
            summary.avg_routing_pages_read(),
            summary.avg_resident_bytes_estimate(),
            summary.avg_segments_searched(),
            summary.avg_records_considered(),
            summary.avg_records_scored(),
            summary.avg_cache_hits(),
            summary.avg_cache_misses(),
        );
    }
}

fn print_parallel_table(summaries: &[ParallelSummary]) {
    println!();
    println!("## Parallel Query Pressure");
    println!();
    println!(
        "| Dataset | Mode | Records | Dimensions | Parallelism | Queries | Tie-aware Recall@10 | Id Recall@10 | p50 ms | p95 ms | QPS | Avg bytes | Avg graph bytes | Avg routing indexes | Avg routing pages | Avg resident bytes | Avg cache hits/misses | RSS before | RSS peak | RSS after | RSS peak delta |"
    );
    println!(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for summary in summaries {
        println!(
            "| {} | {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.1} | {:.0} | {:.0} | {:.1} | {:.1} | {:.0} | {:.1}/{:.1} | {} | {} | {} | {} |",
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.parallelism,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.qps(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
            summary.avg_routing_page_indexes_read(),
            summary.avg_routing_pages_read(),
            summary.avg_resident_bytes_estimate(),
            summary.avg_cache_hits(),
            summary.avg_cache_misses(),
            format_optional_u64(summary.rss_before),
            format_optional_u64(summary.rss_peak),
            format_optional_u64(summary.rss_after),
            format_optional_i128(summary.rss_delta()),
        );
    }
}

fn write_lifecycle_csv(path: &Path, summaries: &[LifecycleSummary]) -> Result<(), Box<dyn Error>> {
    let mut csv = String::from(
        "dataset,records,dimensions,segment_max_vectors,ingest_ms,ingest_vectors_per_sec,compaction_ms,compaction_vectors_per_sec,pre_compaction_segments,post_compaction_segments,compacted_segments_read,compacted_segments_written,records_rewritten,routing_page_indexes_read,routing_pages_read,routing_page_indexes_written,routing_pages_written,graph_payloads_read,graph_bytes_read,compaction_bytes_read,compaction_bytes_written,compaction_read_bytes_per_sec,compaction_write_bytes_per_sec\n",
    );
    for summary in summaries {
        csv.push_str(&format!(
            "{},{},{},{},{:.6},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6}\n",
            summary.dataset,
            summary.records,
            summary.dimensions,
            summary.segment_max_vectors,
            summary.ingest_ms(),
            summary.ingest_vectors_per_sec(),
            summary.compaction_ms(),
            summary.compaction_vectors_per_sec(),
            summary.pre_compaction_segments,
            summary.post_compaction_segments,
            summary.compacted_segments_read,
            summary.compacted_segments_written,
            summary.records_rewritten,
            summary.routing_page_indexes_read,
            summary.routing_pages_read,
            summary.routing_page_indexes_written,
            summary.routing_pages_written,
            summary.graph_payloads_read,
            summary.graph_bytes_read,
            summary.compaction_bytes_read,
            summary.compaction_bytes_written,
            summary.compaction_read_bytes_per_sec(),
            summary.compaction_write_bytes_per_sec(),
        ));
    }
    fs::write(path, csv)?;
    Ok(())
}

fn write_sequential_csv(path: &Path, summaries: &[ModeSummary]) -> Result<(), Box<dyn Error>> {
    let mut csv = format!(
        "dataset,mode,records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment,queries,tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,avg_routing_pages_read,avg_resident_bytes,avg_segments,avg_records_considered,avg_records_scored,avg_cache_hits,avg_cache_misses,{CACHE_STRATIFICATION_CSV_COLUMNS}\n"
    );
    for summary in summaries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{}\n",
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.segment_max_vectors,
            summary.max_segments,
            summary.routing_page_overfetch,
            summary.max_candidates_per_segment,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.termination_reasons(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
            summary.avg_routing_page_indexes_read(),
            summary.avg_routing_pages_read(),
            summary.avg_resident_bytes_estimate(),
            summary.avg_segments_searched(),
            summary.avg_records_considered(),
            summary.avg_records_scored(),
            summary.avg_cache_hits(),
            summary.avg_cache_misses(),
            cache_stratification_csv_values(summary),
        ));
    }
    fs::write(path, csv)?;
    Ok(())
}

fn write_parallel_csv(path: &Path, summaries: &[ParallelSummary]) -> Result<(), Box<dyn Error>> {
    let mut csv = String::from(
        "dataset,mode,records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment,parallelism,queries,tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,qps,avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,avg_routing_pages_read,avg_resident_bytes,avg_cache_hits,avg_cache_misses,rss_before,rss_peak,rss_after,rss_peak_delta\n",
    );
    for summary in summaries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{},{}\n",
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.segment_max_vectors,
            summary.max_segments,
            summary.routing_page_overfetch,
            summary.max_candidates_per_segment,
            summary.parallelism,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.termination_reasons(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.qps(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
            summary.avg_routing_page_indexes_read(),
            summary.avg_routing_pages_read(),
            summary.avg_resident_bytes_estimate(),
            summary.avg_cache_hits(),
            summary.avg_cache_misses(),
            format_optional_u64(summary.rss_before),
            format_optional_u64(summary.rss_peak),
            format_optional_u64(summary.rss_after),
            format_optional_i128(summary.rss_delta()),
        ));
    }
    fs::write(path, csv)?;
    Ok(())
}

fn write_scale_csv(path: &Path, summaries: &[ModeSummary]) -> Result<(), Box<dyn Error>> {
    let mut csv = format!(
        "family,dataset,mode,records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment,queries,tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,avg_routing_pages_read,avg_resident_bytes,avg_segments,avg_records_considered,avg_records_scored,avg_cache_hits,avg_cache_misses,{CACHE_STRATIFICATION_CSV_COLUMNS}\n"
    );
    for summary in summaries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{}\n",
            scale_family_name(&summary.dataset),
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.segment_max_vectors,
            summary.max_segments,
            summary.routing_page_overfetch,
            summary.max_candidates_per_segment,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.termination_reasons(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
            summary.avg_routing_page_indexes_read(),
            summary.avg_routing_pages_read(),
            summary.avg_resident_bytes_estimate(),
            summary.avg_segments_searched(),
            summary.avg_records_considered(),
            summary.avg_records_scored(),
            summary.avg_cache_hits(),
            summary.avg_cache_misses(),
            cache_stratification_csv_values(summary),
        ));
    }
    fs::write(path, csv)?;
    Ok(())
}

fn write_routing_overfetch_csv(
    path: &Path,
    summaries: &[ModeSummary],
) -> Result<(), Box<dyn Error>> {
    write_sequential_csv(path, summaries)
}

fn cache_stratification_csv_values(summary: &ModeSummary) -> String {
    format!(
        "{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3}",
        summary.prefetch_depth_1_cache.cold.p50_ms(),
        summary.prefetch_depth_1_cache.cold.p95_ms(),
        summary.prefetch_depth_1_cache.warm.p50_ms(),
        summary.prefetch_depth_1_cache.warm.p95_ms(),
        summary.prefetch_depth_1_cache.cold.avg_cache_hits(),
        summary.prefetch_depth_1_cache.cold.avg_cache_misses(),
        summary.prefetch_depth_1_cache.warm.avg_cache_hits(),
        summary.prefetch_depth_1_cache.warm.avg_cache_misses(),
        summary.prefetch_depth_8_cache.cold.p50_ms(),
        summary.prefetch_depth_8_cache.cold.p95_ms(),
        summary.prefetch_depth_8_cache.warm.p50_ms(),
        summary.prefetch_depth_8_cache.warm.p95_ms(),
        summary.prefetch_depth_8_cache.cold.avg_cache_hits(),
        summary.prefetch_depth_8_cache.cold.avg_cache_misses(),
        summary.prefetch_depth_8_cache.warm.avg_cache_hits(),
        summary.prefetch_depth_8_cache.warm.avg_cache_misses(),
    )
}

fn format_termination_reasons(reasons: &BTreeMap<String, usize>) -> String {
    reasons
        .iter()
        .map(|(reason, count)| format!("{reason}={count}"))
        .collect::<Vec<_>>()
        .join("|")
}

fn scale_family_name(dataset: &str) -> &str {
    let Some((family, count)) = dataset.rsplit_once("-n") else {
        return dataset;
    };
    if !family.is_empty() && count.chars().all(|character| character.is_ascii_digit()) {
        family
    } else {
        dataset
    }
}

fn timed_report(
    index: &BorsukIndex,
    query: &[f32],
    options: SearchOptions,
) -> borsuk::Result<(Duration, SearchReport)> {
    let started = Instant::now();
    let report = index.search_with_report(query, options)?;
    Ok((started.elapsed(), report))
}

fn hit_ids(report: &SearchReport) -> borsuk::Result<Vec<String>> {
    hit_ids_from_hits(&report.hits)
}

fn hit_ids_from_hits(hits: &[SearchHit]) -> borsuk::Result<Vec<String>> {
    hits.iter().map(|hit| hit.id.to_utf8_string()).collect()
}

fn hit_tie_aware_recall_at_k(
    exact_hits: &[SearchHit],
    actual_hits: &[SearchHit],
    k: usize,
) -> Result<f32, Box<dyn Error>> {
    Ok(tie_aware_recall_at_k(
        &hit_distances(exact_hits),
        &hit_distances(actual_hits),
        k,
    )?)
}

fn hit_distances(hits: &[SearchHit]) -> Vec<f32> {
    hits.iter().map(|hit| hit.distance).collect()
}

fn percentile_ms(durations: &[Duration], percentile: f64) -> f64 {
    if durations.is_empty() {
        return 0.0;
    }
    let mut micros = durations
        .iter()
        .map(|duration| duration.as_secs_f64() * 1_000_000.0)
        .collect::<Vec<_>>();
    micros.sort_by(f64::total_cmp);
    let index = ((micros.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(micros.len() - 1);
    micros[index] / 1_000.0
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn throughput_per_sec(items: usize, duration: Duration) -> f64 {
    if duration.is_zero() {
        return items as f64;
    }
    items as f64 / duration.as_secs_f64()
}

fn byte_throughput_per_sec(bytes: u64, duration: Duration) -> f64 {
    if duration.is_zero() {
        return bytes as f64;
    }
    bytes as f64 / duration.as_secs_f64()
}

fn parse_value<T>(flag: &str, value: Option<String>) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    Ok(required_value(flag, value)?.parse()?)
}

fn required_value(flag: &str, value: Option<String>) -> Result<String, Box<dyn Error>> {
    value.ok_or_else(|| format!("{flag} requires a value").into())
}

fn parse_parallelism(value: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    value
        .split(',')
        .map(|part| {
            let parsed = part.trim().parse::<usize>()?;
            if parsed == 0 {
                return Err("parallelism values must be greater than zero".into());
            }
            Ok(parsed)
        })
        .collect()
}

fn parse_record_counts(value: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    let counts = value
        .split(',')
        .map(|part| {
            let parsed = part.trim().parse::<usize>()?;
            if parsed == 0 {
                return Err("synthetic record counts must be greater than zero".into());
            }
            Ok(parsed)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if counts.is_empty() {
        return Err("--synthetic-records-list must contain at least one value".into());
    }
    Ok(counts)
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

#[cfg(test)]
mod tests {
    use super::*;
    use borsuk::SearchHit;

    #[test]
    fn tie_aware_recall_counts_equal_distance_hits_with_different_ids() {
        let exact = vec![hit("exact-a", 0.0), hit("exact-b", 0.0)];
        let actual = vec![hit("other-a", 0.0), hit("other-b", 0.0)];

        assert_eq!(hit_tie_aware_recall_at_k(&exact, &actual, 2).unwrap(), 1.0);
    }

    #[test]
    fn tie_aware_recall_rejects_hits_outside_exact_k_distance() {
        let exact = vec![hit("exact-a", 0.0), hit("exact-b", 0.0)];
        let actual = vec![hit("same-vector", 0.0), hit("outside-tie", 0.1)];

        assert_eq!(hit_tie_aware_recall_at_k(&exact, &actual, 2).unwrap(), 0.5);
    }

    #[test]
    fn sequential_csv_includes_dataset_size_and_budget_columns() {
        let mut summary = ModeSummary::new("synthetic-uniform", "exact", 1, 10_000, 64);
        summary.push(
            1.0,
            1.0,
            Duration::from_millis(1),
            &SearchReport {
                hits: vec![hit("doc-0", 0.0)],
                leaf_mode: "flat-scan".to_string(),
                termination_reason: borsuk::SearchTerminationReason::Complete,
                recall_guarantee: borsuk::RecallGuarantee::Exact,
                segments_total: 1,
                segments_searched: 1,
                segments_skipped: 0,
                routing_page_indexes_read: 0,
                routing_pages_read: 0,
                bytes_read: 1,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 1,
                cache_repairs: 0,
                records_considered: 1,
                records_scored: 1,
                graph_candidates_added: 0,
                resident_bytes_estimate: 1,
                elapsed_ms: 1,
                requests: Default::default(),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
            },
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sequential.csv");
        write_sequential_csv(&path, &[summary]).unwrap();
        let csv = fs::read_to_string(path).unwrap();

        assert!(csv.starts_with("dataset,mode,records,dimensions,"));
        assert!(csv.contains(
            "segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment"
        ));
        assert!(csv.contains("tie_aware_recall_at_10,id_recall_at_10,termination_reasons"));
        assert!(csv.contains("10000,64,256,8,8,64"));
        assert!(csv.contains(",1.000000,1.000000,complete=1,"));
    }

    #[test]
    fn sequential_csv_includes_cold_warm_cache_and_prefetch_columns() {
        let mut summary = ModeSummary::new("synthetic-uniform", "pq-scan", 2, 10_000, 64);
        summary.record_cache_stratification(
            1,
            CachePhase::Cold,
            Duration::from_millis(10),
            &cache_report(0, 6),
        );
        summary.record_cache_stratification(
            1,
            CachePhase::Cold,
            Duration::from_millis(20),
            &cache_report(0, 4),
        );
        summary.record_cache_stratification(
            1,
            CachePhase::Warm,
            Duration::from_millis(3),
            &cache_report(5, 1),
        );
        summary.record_cache_stratification(
            1,
            CachePhase::Warm,
            Duration::from_millis(5),
            &cache_report(7, 1),
        );
        summary.record_cache_stratification(
            8,
            CachePhase::Cold,
            Duration::from_millis(8),
            &cache_report(0, 8),
        );
        summary.record_cache_stratification(
            8,
            CachePhase::Warm,
            Duration::from_millis(2),
            &cache_report(8, 0),
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sequential.csv");
        write_sequential_csv(&path, &[summary]).unwrap();
        let csv = fs::read_to_string(path).unwrap();

        assert!(csv.contains("prefetch_depth_1_cold_p50_ms"));
        assert!(csv.contains("prefetch_depth_1_warm_p95_ms"));
        assert!(csv.contains("prefetch_depth_8_cold_avg_cache_misses"));
        assert!(csv.contains("prefetch_depth_8_warm_avg_cache_hits"));
        assert!(csv.contains(",10.000000,20.000000,3.000000,5.000000,"));
        assert!(csv.contains(",0.000,5.000,6.000,1.000,"));
        assert!(csv.contains(",8.000000,8.000000,2.000000,2.000000,"));
        assert!(csv.contains(",0.000,8.000,8.000,0.000"));
    }

    #[test]
    fn mode_summary_counts_each_searched_segment_once() {
        let mut summary = ModeSummary::new("synthetic-uniform", "pq-scan", 1, 10_000, 64);
        summary.push(
            1.0,
            1.0,
            Duration::from_millis(1),
            &SearchReport {
                hits: vec![hit("doc-0", 0.0)],
                leaf_mode: "pq-scan".to_string(),
                termination_reason: borsuk::SearchTerminationReason::MaxSegments,
                recall_guarantee: borsuk::RecallGuarantee::Degraded,
                segments_total: 40,
                segments_searched: 8,
                segments_skipped: 32,
                routing_page_indexes_read: 1,
                routing_pages_read: 2,
                bytes_read: 115_000,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 8,
                cache_repairs: 0,
                records_considered: 2048,
                records_scored: 512,
                graph_candidates_added: 0,
                resident_bytes_estimate: 267,
                elapsed_ms: 1,
                requests: Default::default(),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
            },
        );

        assert_eq!(summary.avg_segments_searched(), 8.0);
    }

    #[test]
    fn lifecycle_csv_includes_ingest_and_compaction_columns() {
        let summary = LifecycleSummary {
            dataset: "synthetic-uniform".to_string(),
            records: 10_000,
            dimensions: 64,
            segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
            ingest_duration: Duration::from_millis(250),
            compaction_duration: Duration::from_millis(500),
            pre_compaction_segments: 40,
            post_compaction_segments: 40,
            compacted_segments_read: 40,
            compacted_segments_written: 40,
            records_rewritten: 10_000,
            routing_page_indexes_read: 1,
            routing_pages_read: 4,
            routing_page_indexes_written: 1,
            routing_pages_written: 3,
            graph_payloads_read: 0,
            graph_bytes_read: 0,
            compaction_bytes_read: 1_000_000,
            compaction_bytes_written: 2_000_000,
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lifecycle.csv");
        write_lifecycle_csv(&path, &[summary]).unwrap();
        let csv = fs::read_to_string(path).unwrap();

        assert!(csv.starts_with("dataset,records,dimensions,segment_max_vectors,"));
        assert!(csv.contains("ingest_ms,ingest_vectors_per_sec,compaction_ms"));
        assert!(csv.contains("routing_page_indexes_read,routing_pages_read"));
        assert!(csv.contains("graph_payloads_read,graph_bytes_read"));
        assert!(csv.contains("synthetic-uniform,10000,64,256"));
    }

    #[test]
    fn high_recall_modes_below_threshold_fail_the_report_gate() {
        let mut summary = ModeSummary::new("synthetic-uniform", "pq-scan", 1, 10_000, 64);
        summary.push(
            0.90,
            0.90,
            Duration::from_millis(1),
            &SearchReport {
                hits: vec![hit("doc-0", 0.0)],
                leaf_mode: "pq-scan".to_string(),
                termination_reason: borsuk::SearchTerminationReason::Complete,
                recall_guarantee: borsuk::RecallGuarantee::BudgetComplete,
                segments_total: 1,
                segments_searched: 1,
                segments_skipped: 0,
                routing_page_indexes_read: 0,
                routing_pages_read: 0,
                bytes_read: 1,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 1,
                cache_repairs: 0,
                records_considered: 1,
                records_scored: 1,
                graph_candidates_added: 0,
                resident_bytes_estimate: 1,
                elapsed_ms: 1,
                requests: Default::default(),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
            },
        );

        let err = validate_high_recall_modes(&[summary]).unwrap_err();

        assert!(err.to_string().contains("pq-scan"), "{err}");
        assert!(err.to_string().contains("0.900"), "{err}");
    }

    #[test]
    fn args_parse_accepts_synthetic_record_count_sweeps() {
        let args = Args::parse(
            [
                "--synthetic-records-list",
                "1000,10000,1000000",
                "--queries",
                "10",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(args.synthetic_record_counts, vec![1000, 10_000, 1_000_000]);
    }

    #[test]
    fn args_parse_accepts_benchmark_budget_knobs() {
        let args = Args::parse(
            [
                "--segment-max-vectors",
                "512",
                "--max-segments",
                "16",
                "--routing-page-overfetch",
                "32",
                "--max-candidates-per-segment",
                "128",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(args.segment_max_vectors, 512);
        assert_eq!(args.max_segments, 16);
        assert_eq!(args.routing_page_overfetch, 32);
        assert_eq!(args.max_candidates_per_segment, 128);
    }

    #[test]
    fn mode_options_use_parsed_benchmark_budgets() {
        let args = Args::parse(
            [
                "--max-segments",
                "16",
                "--routing-page-overfetch",
                "32",
                "--max-candidates-per-segment",
                "128",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        let borsuk::SearchMode::Approx {
            max_segments,
            routing_page_overfetch,
            max_candidates_per_segment,
            ..
        } = ModeSpec::Approx(LeafMode::PqScan).options(&args).mode
        else {
            panic!("expected approximate search options");
        };

        assert_eq!(max_segments, Some(16));
        assert_eq!(routing_page_overfetch, Some(32));
        assert_eq!(max_candidates_per_segment, Some(128));
    }

    #[test]
    fn scale_sweep_dataset_names_include_record_counts() {
        let args = Args::parse(
            ["--synthetic-records-list", "1000,10000", "--queries", "10"]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        let datasets = synthetic_datasets(&args);

        assert!(
            datasets
                .iter()
                .any(|dataset| dataset.name == "synthetic-uniform-n1000")
        );
        assert!(
            datasets
                .iter()
                .any(|dataset| dataset.name == "synthetic-uniform-n10000")
        );
    }

    #[test]
    fn scale_csv_normalizes_synthetic_family_and_keeps_record_counts() {
        let mut summary = ModeSummary::new("synthetic-uniform-n10000", "pq-scan", 1, 10_000, 64);
        summary.push(
            1.0,
            0.9,
            Duration::from_millis(7),
            &SearchReport {
                hits: vec![hit("doc-0", 0.0)],
                leaf_mode: "pq-scan".to_string(),
                termination_reason: borsuk::SearchTerminationReason::MaxSegments,
                recall_guarantee: borsuk::RecallGuarantee::Degraded,
                segments_total: 40,
                segments_searched: 8,
                segments_skipped: 32,
                routing_page_indexes_read: 1,
                routing_pages_read: 2,
                bytes_read: 115_000,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 8,
                cache_repairs: 0,
                records_considered: 2048,
                records_scored: 512,
                graph_candidates_added: 0,
                resident_bytes_estimate: 61_000,
                elapsed_ms: 7,
                requests: Default::default(),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
            },
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scale.csv");
        write_scale_csv(&path, &[summary]).unwrap();
        let csv = fs::read_to_string(path).unwrap();

        assert!(csv.starts_with("family,dataset,mode,records,dimensions,"));
        assert!(csv.contains(
            "segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment"
        ));
        assert!(csv.contains("avg_routing_page_indexes_read,avg_routing_pages_read"));
        assert!(csv.contains("avg_cache_hits,avg_cache_misses"));
        assert!(csv.contains("synthetic-uniform,synthetic-uniform-n10000,pq-scan,10000,64"));
        assert!(csv.contains("10000,64,256,8,8,64"));
        assert!(csv.contains(",1.000000,0.900000,max-segments=1,"));
        assert!(csv.contains(",1.000,2.000,61000.000,8.000,2048.000"));
    }

    #[test]
    fn parallel_csv_includes_cache_counters_for_parallel_pressure() {
        let mut termination_reasons = BTreeMap::new();
        termination_reasons.insert("max-segments".to_string(), 2);
        let summary = ParallelSummary {
            dataset: "synthetic-uniform".to_string(),
            mode: "vamana-pq".to_string(),
            records: 10_000,
            dimensions: 64,
            segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
            max_segments: DEFAULT_MAX_SEGMENTS,
            routing_page_overfetch: DEFAULT_ROUTING_PAGE_OVERFETCH,
            max_candidates_per_segment: DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
            parallelism: 2,
            queries: 2,
            recall_sum: 2.0,
            id_recall_sum: 1.8,
            durations: vec![Duration::from_millis(4), Duration::from_millis(6)],
            wall_duration: Duration::from_millis(10),
            bytes_read: 230_000,
            graph_bytes_read: 12_000,
            routing_page_indexes_read: 2,
            routing_pages_read: 4,
            resident_bytes_estimate: 122_000,
            object_cache_hits: 6,
            object_cache_misses: 10,
            termination_reasons,
            rss_before: Some(1_000_000),
            rss_peak: Some(1_100_000),
            rss_after: Some(1_050_000),
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("parallel.csv");
        write_parallel_csv(&path, &[summary]).unwrap();
        let csv = fs::read_to_string(path).unwrap();

        assert!(
            csv.contains("avg_routing_page_indexes_read,avg_routing_pages_read,avg_resident_bytes")
        );
        assert!(csv.contains("segment_max_vectors,max_segments,routing_page_overfetch"));
        assert!(csv.contains("synthetic-uniform,vamana-pq,10000,64,256,8,8,64,2,2"));
        assert!(csv.contains(",1.000,2.000,61000.000,3.000,5.000,1000000"));
    }

    #[test]
    fn routing_overfetch_csv_exposes_recall_and_metadata_io_sweep() {
        let mut low = ModeSummary::new("synthetic-uniform", "pq-scan", 1, 10_000, 64);
        low.routing_page_overfetch = 1;
        low.push(
            0.80,
            0.80,
            Duration::from_millis(5),
            &SearchReport {
                hits: vec![hit("doc-0", 0.0)],
                leaf_mode: "pq-scan".to_string(),
                termination_reason: borsuk::SearchTerminationReason::MaxSegments,
                recall_guarantee: borsuk::RecallGuarantee::Degraded,
                segments_total: 40,
                segments_searched: 8,
                segments_skipped: 32,
                routing_page_indexes_read: 1,
                routing_pages_read: 1,
                bytes_read: 100_000,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 9,
                cache_repairs: 0,
                records_considered: 2048,
                records_scored: 512,
                graph_candidates_added: 0,
                resident_bytes_estimate: 267,
                elapsed_ms: 5,
                requests: Default::default(),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
            },
        );
        let mut high = ModeSummary::new("synthetic-uniform", "pq-scan", 1, 10_000, 64);
        high.routing_page_overfetch = 8;
        high.push(
            1.0,
            1.0,
            Duration::from_millis(7),
            &SearchReport {
                hits: vec![hit("doc-0", 0.0)],
                leaf_mode: "pq-scan".to_string(),
                termination_reason: borsuk::SearchTerminationReason::MaxSegments,
                recall_guarantee: borsuk::RecallGuarantee::Degraded,
                segments_total: 40,
                segments_searched: 8,
                segments_skipped: 32,
                routing_page_indexes_read: 1,
                routing_pages_read: 3,
                bytes_read: 120_000,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 11,
                cache_repairs: 0,
                records_considered: 2048,
                records_scored: 512,
                graph_candidates_added: 0,
                resident_bytes_estimate: 267,
                elapsed_ms: 7,
                requests: Default::default(),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
            },
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing-overfetch.csv");
        write_routing_overfetch_csv(&path, &[low, high]).unwrap();
        let csv = fs::read_to_string(path).unwrap();

        assert!(csv.starts_with("dataset,mode,records,dimensions,"));
        assert!(csv.contains(
            "segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment"
        ));
        assert!(csv.contains("avg_routing_page_indexes_read,avg_routing_pages_read"));
        assert!(csv.contains("avg_cache_hits,avg_cache_misses"));
        assert!(csv.contains("synthetic-uniform,pq-scan,10000,64,256,8,1,64"));
        assert!(csv.contains("synthetic-uniform,pq-scan,10000,64,256,8,8,64"));
        assert!(csv.contains(",0.800000,0.800000,max-segments=1,"));
        assert!(csv.contains(",1.000000,1.000000,max-segments=1,"));
    }

    #[test]
    fn dataset_suite_reuses_one_built_index_for_all_artifact_families() {
        let dataset = synthetic_dataset(SyntheticDataset::Uniform, 4, 2, 2);
        let args = Args::parse([].into_iter().map(str::to_string)).unwrap();

        let suite = run_dataset_suite(&dataset, &args, &[1], true).unwrap();

        assert_eq!(suite.lifecycle.records, 4);
        assert!(
            suite
                .sequential
                .iter()
                .any(|summary| summary.mode == "exact")
        );
        assert!(
            suite
                .parallel
                .iter()
                .any(|summary| summary.mode == "pq-scan" && summary.parallelism == 1)
        );
        assert!(
            suite
                .routing_overfetch
                .iter()
                .any(|summary| summary.mode == "pq-scan" && summary.routing_page_overfetch == 32)
        );
    }

    fn hit(id: &str, distance: f32) -> SearchHit {
        SearchHit {
            id: id.into(),
            distance,
            metadata: None,
        }
    }

    fn cache_report(object_cache_hits: usize, object_cache_misses: usize) -> SearchReport {
        SearchReport {
            hits: vec![hit("doc-0", 0.0)],
            leaf_mode: "pq-scan".to_string(),
            termination_reason: borsuk::SearchTerminationReason::Complete,
            recall_guarantee: borsuk::RecallGuarantee::BudgetComplete,
            segments_total: 1,
            segments_searched: 1,
            segments_skipped: 0,
            routing_page_indexes_read: 0,
            routing_pages_read: 0,
            bytes_read: 1,
            prefetched_bytes_unused: 0,
            graph_bytes_read: 0,
            object_cache_hits,
            object_cache_misses,
            cache_repairs: 0,
            records_considered: 1,
            records_scored: 1,
            graph_candidates_added: 0,
            resident_bytes_estimate: 1,
            elapsed_ms: 1,
            requests: Default::default(),
            rows_evaluated: 0,
            rows_passed_filter: 0,
            segments_pruned_by_filter: 0,
        }
    }
}

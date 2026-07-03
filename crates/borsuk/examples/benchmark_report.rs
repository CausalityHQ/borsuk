#![allow(missing_docs)]

use std::{
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
    VectorMetric, VectorRecord, recall_at_k,
};
use memory_stats::memory_stats;

const DEFAULT_SYNTHETIC_RECORDS: usize = 10_000;
const DEFAULT_DIMENSIONS: usize = 64;
const DEFAULT_QUERIES: usize = 20;
const DEFAULT_SEGMENT_MAX_VECTORS: usize = 256;
const DEFAULT_MAX_SEGMENTS: usize = 8;
const DEFAULT_MAX_CANDIDATES_PER_SEGMENT: usize = 64;

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
    dimensions: usize,
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
    max_candidates_per_segment: usize,
    queries: usize,
    recall_sum: f64,
    id_recall_sum: f64,
    durations: Vec<Duration>,
    bytes_read: u128,
    graph_bytes_read: u128,
    segments_searched: u128,
    records_considered: u128,
    records_scored: u128,
    resident_bytes_estimate: u128,
    object_cache_hits: u128,
    object_cache_misses: u128,
}

#[derive(Debug)]
struct ParallelSummary {
    dataset: String,
    mode: String,
    records: usize,
    dimensions: usize,
    segment_max_vectors: usize,
    max_segments: usize,
    max_candidates_per_segment: usize,
    parallelism: usize,
    queries: usize,
    recall_sum: f64,
    id_recall_sum: f64,
    durations: Vec<Duration>,
    wall_duration: Duration,
    bytes_read: u128,
    graph_bytes_read: u128,
    resident_bytes_estimate: u128,
    rss_before: Option<u64>,
    rss_peak: Option<u64>,
    rss_after: Option<u64>,
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

    fn options(self) -> SearchOptions {
        match self {
            Self::Exact => SearchOptions::exact(10),
            Self::Approx(leaf_mode) => SearchOptions::approx(10, leaf_mode)
                .with_max_segments(DEFAULT_MAX_SEGMENTS)
                .with_max_candidates_per_segment(DEFAULT_MAX_CANDIDATES_PER_SEGMENT),
        }
    }
}

impl ModeSummary {
    fn new(dataset: &str, mode: &str, queries: usize, records: usize, dimensions: usize) -> Self {
        Self {
            dataset: dataset.to_string(),
            mode: mode.to_string(),
            records,
            dimensions,
            segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
            max_segments: DEFAULT_MAX_SEGMENTS,
            max_candidates_per_segment: DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
            queries,
            recall_sum: 0.0,
            id_recall_sum: 0.0,
            durations: Vec::with_capacity(queries),
            bytes_read: 0,
            graph_bytes_read: 0,
            segments_searched: 0,
            records_considered: 0,
            records_scored: 0,
            resident_bytes_estimate: 0,
            object_cache_hits: 0,
            object_cache_misses: 0,
        }
    }

    fn push(&mut self, recall: f32, id_recall: f32, duration: Duration, report: &SearchReport) {
        self.recall_sum += f64::from(recall);
        self.id_recall_sum += f64::from(id_recall);
        self.durations.push(duration);
        self.bytes_read += u128::from(report.bytes_read);
        self.graph_bytes_read += u128::from(report.graph_bytes_read);
        self.segments_searched += report.segments_searched as u128;
        self.records_considered += report.records_considered as u128;
        self.records_scored += report.records_scored as u128;
        self.resident_bytes_estimate += u128::from(report.resident_bytes_estimate);
        self.object_cache_hits += report.object_cache_hits as u128;
        self.object_cache_misses += report.object_cache_misses as u128;
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

    fn avg_resident_bytes_estimate(&self) -> f64 {
        self.resident_bytes_estimate as f64 / self.queries as f64
    }

    fn rss_delta(&self) -> Option<i128> {
        Some(i128::from(self.rss_peak?) - i128::from(self.rss_before?))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse(env::args().skip(1))?;
    let mut datasets = vec![
        synthetic_dataset(
            SyntheticDataset::Uniform,
            args.synthetic_records,
            args.dimensions,
            args.queries,
        ),
        synthetic_dataset(
            SyntheticDataset::Clustered,
            args.synthetic_records,
            args.dimensions,
            args.queries,
        ),
        synthetic_dataset(
            SyntheticDataset::Adversarial,
            args.synthetic_records,
            args.dimensions,
            args.queries,
        ),
    ];

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
        "Approximate modes use `max_segments={DEFAULT_MAX_SEGMENTS}` and \
         `max_candidates_per_segment={DEFAULT_MAX_CANDIDATES_PER_SEGMENT}`."
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
    for dataset in &datasets {
        sequential_summaries.extend(run_dataset(dataset)?);
        parallel_summaries.extend(run_parallel_dataset(dataset, &args.parallelism)?);
    }

    print_sequential_table(&sequential_summaries);
    print_parallel_table(&parallel_summaries);

    if let Some(artifacts_dir) = &args.artifacts_dir {
        fs::create_dir_all(artifacts_dir)?;
        write_sequential_csv(&artifacts_dir.join("sequential.csv"), &sequential_summaries)?;
        write_parallel_csv(&artifacts_dir.join("parallel.csv"), &parallel_summaries)?;
    }

    Ok(())
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut parsed = Self {
            synthetic_records: DEFAULT_SYNTHETIC_RECORDS,
            dimensions: DEFAULT_DIMENSIONS,
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
                }
                "--dimensions" => {
                    parsed.dimensions = parse_value(&arg, args.next())?;
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
        if parsed.synthetic_records < parsed.queries {
            return Err("--synthetic-records must be at least --queries".into());
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
    println!("  --dimensions N          Synthetic vector dimensions");
    println!("  --queries N             Query count per dataset");
    println!("  --csv PATH              Optional real-data CSV; rows are vectors");
    println!("  --csv-name NAME         Display name for the real-data CSV");
    println!("  --csv-dimensions N      Feature columns to read from the CSV");
    println!("  --artifacts-dir PATH    Write sequential.csv and parallel.csv");
    println!("  --parallelism LIST      Comma-separated parallel query counts, default 1,2,4,8");
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

fn run_dataset(dataset: &Dataset) -> Result<Vec<ModeSummary>, Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: dataset.metric.clone(),
        dimensions: dataset.dimensions,
        segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
        ram_budget_bytes: None,
    })?;
    index.add(dataset.records.clone())?;
    compact_for_query_benchmark(&mut index)?;

    let exact_reports = dataset
        .queries
        .iter()
        .map(|query| timed_report(&index, query, SearchOptions::exact(10)))
        .collect::<Result<Vec<_>, _>>()?;
    let exact_ids = exact_reports
        .iter()
        .map(|(_, report)| hit_ids(report))
        .collect::<Vec<_>>();

    let mut summaries = Vec::new();
    let mut exact_summary = ModeSummary::new(
        &dataset.name,
        "exact",
        dataset.queries.len(),
        dataset.records.len(),
        dataset.dimensions,
    );
    for (duration, report) in &exact_reports {
        exact_summary.push(1.0, 1.0, *duration, report);
    }
    summaries.push(exact_summary);

    for mode in &ModeSpec::all()[1..] {
        let mut summary = ModeSummary::new(
            &dataset.name,
            &mode.name(),
            dataset.queries.len(),
            dataset.records.len(),
            dataset.dimensions,
        );
        for (query, ((_, exact_report), exact_ids)) in dataset
            .queries
            .iter()
            .zip(exact_reports.iter().zip(&exact_ids))
        {
            let (duration, report) = timed_report(&index, query, mode.options())?;
            let ids = hit_ids(&report);
            let id_recall = recall_at_k(exact_ids, &ids, 10)?;
            let recall = tie_aware_recall_at_k(&exact_report.hits, &report.hits, 10)?;
            summary.push(recall, id_recall, duration, &report);
        }
        summaries.push(summary);
    }

    Ok(summaries)
}

fn run_parallel_dataset(
    dataset: &Dataset,
    parallelisms: &[usize],
) -> Result<Vec<ParallelSummary>, Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: dataset.metric.clone(),
        dimensions: dataset.dimensions,
        segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
        ram_budget_bytes: None,
    })?;
    index.add(dataset.records.clone())?;
    compact_for_query_benchmark(&mut index)?;

    let exact_hits = dataset
        .queries
        .iter()
        .map(|query| {
            index
                .search_with_report(query, SearchOptions::exact(10))
                .map(|report| report.hits)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut summaries = Vec::new();
    for mode in ModeSpec::all() {
        for parallelism in parallelisms {
            summaries.push(run_parallel_mode(
                dataset,
                &index,
                &exact_hits,
                *mode,
                *parallelism,
            )?);
        }
    }
    Ok(summaries)
}

fn compact_for_query_benchmark(index: &mut BorsukIndex) -> borsuk::Result<()> {
    index.compact(CompactionOptions {
        source_level: 0,
        target_level: 1,
        max_segments: None,
        min_segments: 2,
        target_segment_max_vectors: Some(DEFAULT_SEGMENT_MAX_VECTORS),
    })?;
    Ok(())
}

fn run_parallel_mode(
    dataset: &Dataset,
    index: &BorsukIndex,
    exact_hits: &[Vec<SearchHit>],
    mode: ModeSpec,
    parallelism: usize,
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
        handles.push(thread::spawn(
            move || -> Result<Vec<QueryOutcome>, String> {
                queries
                    .iter()
                    .map(|query| {
                        timed_report(&worker_index, query, mode.options())
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
    let mut resident_bytes_estimate = 0_u128;
    for (outcome_index, outcome) in outcomes.into_iter().enumerate() {
        let query_index = outcome_index % dataset.queries.len();
        let exact_ids = hit_ids_from_hits(&exact_hits[query_index]);
        let ids = hit_ids(&outcome.report);
        recall_sum += f64::from(tie_aware_recall_at_k(
            &exact_hits[query_index],
            &outcome.report.hits,
            10,
        )?);
        id_recall_sum += f64::from(recall_at_k(&exact_ids, &ids, 10)?);
        durations.push(outcome.duration);
        bytes_read += u128::from(outcome.report.bytes_read);
        graph_bytes_read += u128::from(outcome.report.graph_bytes_read);
        resident_bytes_estimate += u128::from(outcome.report.resident_bytes_estimate);
    }

    Ok(ParallelSummary {
        dataset: dataset.name.clone(),
        mode: mode.name(),
        records: dataset.records.len(),
        dimensions: dataset.dimensions,
        segment_max_vectors: DEFAULT_SEGMENT_MAX_VECTORS,
        max_segments: DEFAULT_MAX_SEGMENTS,
        max_candidates_per_segment: DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
        parallelism,
        queries: parallelism * dataset.queries.len(),
        recall_sum,
        id_recall_sum,
        durations,
        wall_duration,
        bytes_read,
        graph_bytes_read,
        resident_bytes_estimate,
        rss_before,
        rss_peak,
        rss_after,
    })
}

fn print_sequential_table(summaries: &[ModeSummary]) {
    println!(
        "| Dataset | Mode | Records | Dimensions | Queries | Tie-aware Recall@10 | Id Recall@10 | p50 ms | p95 ms | Avg bytes | Avg graph bytes | Avg resident bytes | Avg segments | Avg considered | Avg scored | Avg cache hits/misses |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for summary in summaries {
        println!(
            "| {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.0} | {:.0} | {:.0} | {:.1} | {:.0} | {:.0} | {:.1}/{:.1} |",
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
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
        "| Dataset | Mode | Records | Dimensions | Parallelism | Queries | Tie-aware Recall@10 | Id Recall@10 | p50 ms | p95 ms | QPS | Avg bytes | Avg graph bytes | Avg resident bytes | RSS before | RSS peak | RSS after | RSS peak delta |"
    );
    println!(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for summary in summaries {
        println!(
            "| {} | {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.1} | {:.0} | {:.0} | {:.0} | {} | {} | {} | {} |",
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
            summary.avg_resident_bytes_estimate(),
            format_optional_u64(summary.rss_before),
            format_optional_u64(summary.rss_peak),
            format_optional_u64(summary.rss_after),
            format_optional_i128(summary.rss_delta()),
        );
    }
}

fn write_sequential_csv(path: &Path, summaries: &[ModeSummary]) -> Result<(), Box<dyn Error>> {
    let mut csv = String::from(
        "dataset,mode,records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,queries,tie_aware_recall_at_10,id_recall_at_10,p50_ms,p95_ms,avg_bytes_read,avg_graph_bytes_read,avg_resident_bytes,avg_segments,avg_records_considered,avg_records_scored,avg_cache_hits,avg_cache_misses\n",
    );
    for summary in summaries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}\n",
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.segment_max_vectors,
            summary.max_segments,
            summary.max_candidates_per_segment,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
            summary.avg_resident_bytes_estimate(),
            summary.avg_segments_searched(),
            summary.avg_records_considered(),
            summary.avg_records_scored(),
            summary.avg_cache_hits(),
            summary.avg_cache_misses(),
        ));
    }
    fs::write(path, csv)?;
    Ok(())
}

fn write_parallel_csv(path: &Path, summaries: &[ParallelSummary]) -> Result<(), Box<dyn Error>> {
    let mut csv = String::from(
        "dataset,mode,records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,parallelism,queries,tie_aware_recall_at_10,id_recall_at_10,p50_ms,p95_ms,qps,avg_bytes_read,avg_graph_bytes_read,avg_resident_bytes,rss_before,rss_peak,rss_after,rss_peak_delta\n",
    );
    for summary in summaries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{},{},{},{}\n",
            summary.dataset,
            summary.mode,
            summary.records,
            summary.dimensions,
            summary.segment_max_vectors,
            summary.max_segments,
            summary.max_candidates_per_segment,
            summary.parallelism,
            summary.queries,
            summary.mean_recall(),
            summary.mean_id_recall(),
            summary.p50_ms(),
            summary.p95_ms(),
            summary.qps(),
            summary.avg_bytes_read(),
            summary.avg_graph_bytes_read(),
            summary.avg_resident_bytes_estimate(),
            format_optional_u64(summary.rss_before),
            format_optional_u64(summary.rss_peak),
            format_optional_u64(summary.rss_after),
            format_optional_i128(summary.rss_delta()),
        ));
    }
    fs::write(path, csv)?;
    Ok(())
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

fn hit_ids(report: &SearchReport) -> Vec<String> {
    hit_ids_from_hits(&report.hits)
}

fn hit_ids_from_hits(hits: &[SearchHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.id.clone()).collect()
}

fn tie_aware_recall_at_k(
    exact_hits: &[SearchHit],
    actual_hits: &[SearchHit],
    k: usize,
) -> Result<f32, Box<dyn Error>> {
    if k == 0 {
        return Err("k must be greater than zero".into());
    }

    let exact_top = exact_hits.iter().take(k).collect::<Vec<_>>();
    if exact_top.is_empty() {
        return Ok(0.0);
    }

    let kth_distance = exact_top.last().expect("exact_top is non-empty").distance;
    let tolerance = kth_distance.abs().max(1.0) * 1.0e-6;
    let accepted = actual_hits
        .iter()
        .take(k)
        .filter(|hit| hit.distance <= kth_distance + tolerance)
        .count();

    Ok(accepted as f32 / exact_top.len() as f32)
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

        assert_eq!(tie_aware_recall_at_k(&exact, &actual, 2).unwrap(), 1.0);
    }

    #[test]
    fn tie_aware_recall_rejects_hits_outside_exact_k_distance() {
        let exact = vec![hit("exact-a", 0.0), hit("exact-b", 0.0)];
        let actual = vec![hit("same-vector", 0.0), hit("outside-tie", 0.1)];

        assert_eq!(tie_aware_recall_at_k(&exact, &actual, 2).unwrap(), 0.5);
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
                segments_total: 1,
                segments_searched: 1,
                segments_skipped: 0,
                bytes_read: 1,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 1,
                records_considered: 1,
                records_scored: 1,
                graph_candidates_added: 0,
                resident_bytes_estimate: 1,
                elapsed_ms: 1,
            },
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sequential.csv");
        write_sequential_csv(&path, &[summary]).unwrap();
        let csv = fs::read_to_string(path).unwrap();

        assert!(csv.starts_with("dataset,mode,records,dimensions,"));
        assert!(csv.contains("tie_aware_recall_at_10,id_recall_at_10"));
        assert!(csv.contains("10000,64,256,8,64"));
    }

    fn hit(id: &str, distance: f32) -> SearchHit {
        SearchHit {
            id: id.to_string(),
            distance,
        }
    }
}

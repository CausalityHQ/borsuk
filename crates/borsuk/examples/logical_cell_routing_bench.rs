//! Preregistered paired logical-cell write-routing benchmark.
//!
//! `build` creates the frozen logical-cell catalog. `run` executes one matrix
//! cell and writes complete per-operation evidence to a fresh output directory.

use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver},
    },
    thread,
    time::{Duration, Instant},
};

use borsuk::{
    BorsukIndex, IndexConfig, OpenOptions, VectorMetric, VectorRecord,
    recommended_segment_max_vectors,
};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
struct RunConfig {
    uri: String,
    output: PathBuf,
    mode: String,
    cell_count: usize,
    writers: usize,
    repetition: usize,
    operations: usize,
    warmup: usize,
    dimensions: usize,
    seed: u64,
    source_sha256: String,
    manifest_sha256: String,
    architecture: String,
    instance_type: String,
    instance_id: String,
    availability_zone: String,
    purchase_option: String,
    mutation_protocol: String,
    metric: String,
}

#[derive(Clone)]
struct Sample {
    writer: usize,
    operation: usize,
    record_id: String,
    latency_ms: f64,
    selected_cell: u32,
    routing_path: &'static str,
    vector_blake3: String,
    storage_requests: u64,
}

enum WriterReady {
    Ready,
    Failed(String),
}

#[derive(Default)]
struct Progress {
    opens_started: AtomicUsize,
    opens_completed: AtomicUsize,
    warmups_started: AtomicUsize,
    warmups_completed: AtomicUsize,
    routes_started: AtomicUsize,
    routes_completed: AtomicUsize,
    appends_started: AtomicUsize,
    appends_completed: AtomicUsize,
    writers_ready: AtomicUsize,
    writers_done: AtomicUsize,
}

impl Progress {
    fn snapshot(&self) -> String {
        format!(
            "opens={}/{} warmups={}/{} routes={}/{} appends={}/{} writers_ready={} writers_done={}",
            self.opens_completed.load(Ordering::Relaxed),
            self.opens_started.load(Ordering::Relaxed),
            self.warmups_completed.load(Ordering::Relaxed),
            self.warmups_started.load(Ordering::Relaxed),
            self.routes_completed.load(Ordering::Relaxed),
            self.routes_started.load(Ordering::Relaxed),
            self.appends_completed.load(Ordering::Relaxed),
            self.appends_started.load(Ordering::Relaxed),
            self.writers_ready.load(Ordering::Relaxed),
            self.writers_done.load(Ordering::Relaxed),
        )
    }
}

struct ProgressReporter {
    stop_tx: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ProgressReporter {
    fn start(progress: Arc<Progress>) -> Self {
        let (stop_tx, stop_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let started = Instant::now();
            eprintln!("routing_progress elapsed_seconds=0 {}", progress.snapshot());
            loop {
                match stop_rx.recv_timeout(Duration::from_secs(30)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                        eprintln!(
                            "routing_progress final=true elapsed_seconds={} {}",
                            started.elapsed().as_secs(),
                            progress.snapshot()
                        );
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => eprintln!(
                        "routing_progress elapsed_seconds={} {}",
                        started.elapsed().as_secs(),
                        progress.snapshot()
                    ),
                }
            }
        });
        Self {
            stop_tx,
            handle: Some(handle),
        }
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn await_writer_readiness(ready_rx: &Receiver<WriterReady>, writers: usize) -> BenchResult<()> {
    for received in 1..=writers {
        match ready_rx.recv() {
            Ok(WriterReady::Ready) => {}
            Ok(WriterReady::Failed(error)) => {
                return Err(format!(
                    "{error}; received {received} of {writers} writer readiness reports"
                )
                .into());
            }
            Err(error) => {
                return Err(format!(
                    "writer readiness channel closed after {} of {writers} reports: {error}",
                    received - 1
                )
                .into());
            }
        }
    }
    Ok(())
}

fn required(name: &str) -> BenchResult<String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn number<T: std::str::FromStr>(name: &str) -> BenchResult<T>
where
    T::Err: std::fmt::Display,
{
    let value = required(name)?;
    value
        .parse()
        .map_err(|error| format!("invalid {name}={value:?}: {error}").into())
}

fn smoke_mode() -> bool {
    is_smoke_value(env::var("BORSUK_ROUTING_SMOKE").ok().as_deref())
}

fn is_smoke_value(value: Option<&str>) -> bool {
    value == Some("1")
}

fn vector(seed: u64, ordinal: u64, dimensions: usize) -> Vec<f32> {
    let mut state = seed ^ ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..dimensions)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            let bits = state.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 40;
            2.0 * (bits as f32 / (1_u64 << 24) as f32) - 1.0
        })
        .collect()
}

fn vector_blake3(vector: &[f32]) -> String {
    let mut digest = blake3::Hasher::new();
    for value in vector {
        digest.update(&value.to_le_bytes());
    }
    digest.finalize().to_hex().to_string()
}

#[cfg(test)]
fn cohort_digest(entries: &[(usize, usize, String, Vec<f32>)]) -> String {
    let mut digest = blake3::Hasher::new();
    for (writer, operation, record_id, vector) in entries {
        digest.update(&writer.to_le_bytes());
        digest.update(&operation.to_le_bytes());
        digest.update(&(record_id.len() as u64).to_le_bytes());
        digest.update(record_id.as_bytes());
        digest.update(vector_blake3(vector).as_bytes());
    }
    digest.finalize().to_hex().to_string()
}

fn measured_cohort_digest(samples: &[Sample]) -> String {
    let mut digest = blake3::Hasher::new();
    for sample in samples {
        digest.update(&sample.writer.to_le_bytes());
        digest.update(&sample.operation.to_le_bytes());
        digest.update(&(sample.record_id.len() as u64).to_le_bytes());
        digest.update(sample.record_id.as_bytes());
        digest.update(sample.vector_blake3.as_bytes());
    }
    digest.finalize().to_hex().to_string()
}

fn protocol_shape_is_valid(
    smoke: bool,
    cell_count: usize,
    writers: usize,
    repetition: usize,
    operations: usize,
    warmup: usize,
    dimensions: usize,
) -> bool {
    if smoke {
        cell_count == 2_000
            && writers == 1
            && repetition == 1
            && operations == 2
            && warmup == 1
            && dimensions == 768
    } else {
        matches!(cell_count, 2_000 | 16_000)
            && matches!(writers, 1 | 8 | 32)
            && (1..=5).contains(&repetition)
            && operations == 100
            && warmup == 20
            && dimensions == 768
    }
}

fn routing_path_is_valid_for_arm(mode: &str, path: &str, distinct_cells: usize) -> bool {
    path == mode && distinct_cells > 1
}

fn build() -> BenchResult<()> {
    let uri = required("BORSUK_ROUTING_INDEX_URI")?;
    let cell_count: usize = number("BORSUK_ROUTING_CELL_COUNT")?;
    let dimensions: usize = number("BORSUK_ROUTING_DIMENSIONS")?;
    let smoke = smoke_mode();
    if (!smoke && (!matches!(cell_count, 2_000 | 16_000) || dimensions != 768))
        || (smoke && (cell_count != 2_000 || dimensions != 768))
    {
        return Err("build differs from its frozen logical-cell protocol".into());
    }
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Cosine,
        dimensions,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })?;
    let records = (0..cell_count)
        .map(|ordinal| {
            VectorRecord::new(
                format!("base-{ordinal:05}"),
                vector(0x626f_7273_756b, ordinal as u64, dimensions),
            )
        })
        .collect();
    index.add(records)?;
    index.finish_bulk_load()?;
    if index.manifest().logical_cells().len() != cell_count {
        return Err("built logical-cell count differs from frozen protocol".into());
    }
    index.set_segment_max_vectors(recommended_segment_max_vectors(dimensions))?;
    Ok(())
}

fn run_config() -> BenchResult<RunConfig> {
    let config = RunConfig {
        uri: required("BORSUK_ROUTING_INDEX_URI")?,
        output: PathBuf::from(required("BORSUK_ROUTING_OUTPUT")?),
        mode: required("BORSUK_ROUTING_MODE")?,
        cell_count: number("BORSUK_ROUTING_CELL_COUNT")?,
        writers: number("BORSUK_ROUTING_WRITERS")?,
        repetition: number("BORSUK_ROUTING_REPETITION")?,
        operations: number("BORSUK_ROUTING_OPERATIONS_PER_WRITER")?,
        warmup: number("BORSUK_ROUTING_WARMUP_OPERATIONS_PER_WRITER")?,
        dimensions: number("BORSUK_ROUTING_DIMENSIONS")?,
        seed: number("BORSUK_ROUTING_MASTER_SEED")?,
        source_sha256: required("BORSUK_SOURCE_SHA256")?,
        manifest_sha256: required("BORSUK_ROUTING_MANIFEST_SHA256")?,
        architecture: required("BORSUK_ARCHITECTURE")?,
        instance_type: required("BORSUK_INSTANCE_TYPE")?,
        instance_id: required("BORSUK_INSTANCE_ID")?,
        availability_zone: required("BORSUK_AVAILABILITY_ZONE")?,
        purchase_option: required("BORSUK_PURCHASE_OPTION")?,
        mutation_protocol: required("BORSUK_MUTATION_PROTOCOL")?,
        metric: required("BORSUK_ROUTING_METRIC")?,
    };
    let smoke = smoke_mode();
    if !protocol_shape_is_valid(
        smoke,
        config.cell_count,
        config.writers,
        config.repetition,
        config.operations,
        config.warmup,
        config.dimensions,
    ) || !matches!(config.mode.as_str(), "flat" | "quantizer")
        || config.purchase_option != "spot" && !smoke
        || config.mutation_protocol != "positioned-v12"
        || config.metric != "cosine"
    {
        return Err("run cell differs from the frozen routing manifest".into());
    }
    for digest in [&config.source_sha256, &config.manifest_sha256] {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("campaign identity is not a SHA-256 digest".into());
        }
    }
    if config.output.exists() {
        return Err(format!("refusing to replace output {}", config.output.display()).into());
    }
    Ok(config)
}

fn open(config: &RunConfig) -> BenchResult<BorsukIndex> {
    Ok(BorsukIndex::open_with_options(
        &config.uri,
        OpenOptions {
            flat_logical_cell_routing: config.mode == "flat",
            ..OpenOptions::default()
        },
    )?)
}

fn process_cpu_seconds() -> BenchResult<f64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let close = stat
        .rfind(')')
        .ok_or("/proc/self/stat has no command terminator")?;
    let fields = stat[close + 2..].split_whitespace().collect::<Vec<_>>();
    let user: u64 = fields
        .get(11)
        .ok_or("missing process user ticks")?
        .parse()?;
    let system: u64 = fields
        .get(12)
        .ok_or("missing process system ticks")?
        .parse()?;
    let output = Command::new("getconf").arg("CLK_TCK").output()?;
    if !output.status.success() {
        return Err("getconf CLK_TCK failed".into());
    }
    let ticks: f64 = String::from_utf8(output.stdout)?.trim().parse()?;
    Ok((user + system) as f64 / ticks)
}

fn run() -> BenchResult<()> {
    let config = Arc::new(run_config()?);
    let progress = Arc::new(Progress::default());
    let _progress_reporter = ProgressReporter::start(Arc::clone(&progress));
    let (ready_tx, ready_rx) = mpsc::channel();
    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let workers = (0..config.writers)
        .map(|writer| {
            let config = Arc::clone(&config);
            let samples = Arc::clone(&samples);
            let progress = Arc::clone(&progress);
            let ready_tx = ready_tx.clone();
            let (start_tx, start_rx) = mpsc::sync_channel(0);
            let handle = thread::spawn(move || -> BenchResult<()> {
                let prepared = (|| -> BenchResult<_> {
                    progress.opens_started.fetch_add(1, Ordering::Relaxed);
                    let mut index = open(&config)?;
                    progress.opens_completed.fetch_add(1, Ordering::Relaxed);
                    for operation in 0..config.warmup {
                        let ordinal = ((writer * config.warmup + operation) as u64) | (1_u64 << 62);
                        progress.warmups_started.fetch_add(1, Ordering::Relaxed);
                        index.add(vec![VectorRecord::new(
                            format!(
                                "warm-c{}-r{}-w{writer:02}-o{operation:03}",
                                config.cell_count, config.repetition
                            ),
                            vector(config.seed, ordinal, config.dimensions),
                        )])?;
                        progress.warmups_completed.fetch_add(1, Ordering::Relaxed);
                    }
                    let cohort = (0..config.operations)
                        .map(|operation| {
                            let ordinal = writer * config.operations + operation;
                            let value = vector(
                                config.seed.wrapping_add(config.repetition as u64),
                                ordinal as u64,
                                config.dimensions,
                            );
                            progress.routes_started.fetch_add(1, Ordering::Relaxed);
                            let (cell, routing_path) = index.logical_cell_for_research(&value)?;
                            progress.routes_completed.fetch_add(1, Ordering::Relaxed);
                            Ok((operation, value, cell.cell_ordinal, routing_path))
                        })
                        .collect::<BenchResult<Vec<_>>>()?;
                    Ok((index, cohort))
                })();
                let (mut index, cohort) = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let message = format!("writer {writer} preflight failed: {error}");
                        let _ = ready_tx.send(WriterReady::Failed(message.clone()));
                        return Err(message.into());
                    }
                };
                progress.writers_ready.fetch_add(1, Ordering::Relaxed);
                ready_tx.send(WriterReady::Ready).map_err(|error| {
                    format!("writer {writer} could not report readiness: {error}")
                })?;
                drop(ready_tx);
                start_rx.recv().map_err(|error| {
                    format!("writer {writer} start was cancelled after readiness: {error}")
                })?;
                let mut local = Vec::with_capacity(config.operations);
                for (operation, value, selected_cell, routing_path) in cohort {
                    let record_id = format!(
                        "measure-c{}-r{}-w{}-writer{writer:02}-o{operation:03}",
                        config.cell_count, config.repetition, config.writers
                    );
                    let vector_blake3 = vector_blake3(&value);
                    progress.appends_started.fetch_add(1, Ordering::Relaxed);
                    let started = Instant::now();
                    let (_, report) =
                        index.add_with_report(vec![value], Some(vec![record_id.clone()]))?;
                    local.push(Sample {
                        writer,
                        operation,
                        record_id,
                        latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                        selected_cell,
                        routing_path,
                        vector_blake3,
                        storage_requests: report.requests.total(),
                    });
                    progress.appends_completed.fetch_add(1, Ordering::Relaxed);
                }
                samples.lock().unwrap().extend(local);
                progress.writers_done.fetch_add(1, Ordering::Relaxed);
                Ok(())
            });
            (start_tx, handle)
        })
        .collect::<Vec<_>>();
    drop(ready_tx);
    let (start_senders, handles): (Vec<_>, Vec<_>) = workers.into_iter().unzip();
    if let Err(error) = await_writer_readiness(&ready_rx, config.writers) {
        drop(start_senders);
        for handle in handles {
            let _ = handle.join();
        }
        return Err(error);
    }
    let cpu_before = process_cpu_seconds()?;
    let started = Instant::now();
    let mut start_error = None;
    for (writer, start_tx) in start_senders.into_iter().enumerate() {
        if let Err(error) = start_tx.send(()) {
            start_error = Some(format!(
                "writer {writer} exited before measurement start: {error}"
            ));
            break;
        }
    }
    if let Some(error) = start_error {
        for handle in handles {
            let _ = handle.join();
        }
        return Err(error.into());
    }
    for handle in handles {
        handle.join().map_err(|_| "routing writer panicked")??;
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let cpu_seconds = process_cpu_seconds()? - cpu_before;
    let mut samples = Arc::try_unwrap(samples)
        .map_err(|_| "sample owners remain")?
        .into_inner()
        .unwrap();
    samples.sort_by_key(|sample| (sample.writer, sample.operation));
    write_results(&config, &samples, elapsed_ms, cpu_seconds)
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[((ordered.len() - 1) as f64 * quantile).round() as usize]
}

fn write_results(
    config: &RunConfig,
    samples: &[Sample],
    elapsed_ms: f64,
    cpu_seconds: f64,
) -> BenchResult<()> {
    let staging = config.output.with_extension("staging");
    if staging.exists() {
        return Err(format!("staging output already exists: {}", staging.display()).into());
    }
    fs::create_dir_all(&staging)?;
    let distinct = samples
        .iter()
        .map(|sample| sample.selected_cell)
        .collect::<BTreeSet<_>>()
        .len();
    let routing_paths = samples
        .iter()
        .map(|sample| sample.routing_path)
        .collect::<BTreeSet<_>>();
    let routing_path = routing_paths
        .iter()
        .copied()
        .next()
        .ok_or("routing arm contains no samples")?;
    if routing_paths.len() != 1
        || !routing_path_is_valid_for_arm(&config.mode, routing_path, distinct)
    {
        return Err(format!(
            "routing arm did not exercise its labeled path: mode={} paths={routing_paths:?} distinct_cells={distinct}",
            config.mode
        )
        .into());
    }
    let cohort_blake3 = measured_cohort_digest(samples);
    let identity = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        config.source_sha256,
        config.manifest_sha256,
        config.architecture,
        config.instance_type,
        config.instance_id,
        config.availability_zone,
        config.purchase_option,
        config.mutation_protocol,
        config.dimensions,
        config.metric,
        config.mode,
        routing_path,
        config.cell_count,
        config.writers,
        config.repetition,
        cohort_blake3,
    );
    let latencies = samples
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    let requests = samples
        .iter()
        .map(|sample| sample.storage_requests)
        .sum::<u64>();
    let mut summary = BufWriter::new(File::create(staging.join("summary.csv"))?);
    writeln!(
        summary,
        "source_sha256,manifest_sha256,architecture,instance_type,instance_id,availability_zone,purchase_option,mutation_protocol,dimensions,metric,routing_mode,routing_path,cell_count,writers,repetition,cohort_blake3,operations,elapsed_ms,cpu_seconds,p50_ms,p95_ms,throughput_ops_per_second,storage_requests,distinct_cells"
    )?;
    writeln!(
        summary,
        "{identity},{},{elapsed_ms:.9},{cpu_seconds:.9},{:.9},{:.9},{:.9},{requests},{distinct}",
        samples.len(),
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        samples.len() as f64 / (elapsed_ms / 1_000.0),
    )?;
    summary.flush()?;
    let mut raw = BufWriter::new(File::create(staging.join("samples.csv"))?);
    writeln!(
        raw,
        "source_sha256,manifest_sha256,architecture,instance_type,instance_id,availability_zone,purchase_option,mutation_protocol,dimensions,metric,routing_mode,routing_path,cell_count,writers,repetition,cohort_blake3,writer,operation,record_id,vector_blake3,latency_ms,selected_cell,storage_requests"
    )?;
    for sample in samples {
        writeln!(
            raw,
            "{identity},{},{},{},{},{:.9},{},{}",
            sample.writer,
            sample.operation,
            sample.record_id,
            sample.vector_blake3,
            sample.latency_ms,
            sample.selected_cell,
            sample.storage_requests,
        )?;
    }
    raw.flush()?;
    fs::rename(staging, &config.output)?;
    Ok(())
}

fn main() -> BenchResult<()> {
    match env::args().nth(1).as_deref() {
        Some("build") => build(),
        Some("run") => run(),
        _ => Err("usage: logical_cell_routing_bench <build|run>".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_vectors_are_finite_and_cohort_stable() {
        let first = vector(7, 11, 768);
        assert_eq!(first, vector(7, 11, 768));
        assert_ne!(first, vector(7, 12, 768));
        assert!(first.iter().all(|value| value.is_finite()));
        assert!(first.iter().any(|value| *value < 0.0));
        assert!(first.iter().any(|value| *value > 0.0));
        let mean = first.iter().copied().sum::<f32>() / first.len() as f32;
        assert!(mean.abs() < 0.1, "centered cosine fixture mean={mean}");
    }

    #[test]
    fn cohort_digest_commits_to_actual_vector_bytes_and_order() {
        let first = vec![
            (0, 0, "a".to_owned(), vec![1.0, -2.0]),
            (1, 0, "b".to_owned(), vec![3.0, -4.0]),
        ];
        let mut changed_value = first.clone();
        changed_value[1].3[0] = 3.5;
        let mut changed_order = first.clone();
        changed_order.swap(0, 1);

        assert_eq!(cohort_digest(&first), cohort_digest(&first));
        assert_ne!(cohort_digest(&first), cohort_digest(&changed_value));
        assert_ne!(cohort_digest(&first), cohort_digest(&changed_order));
    }

    #[test]
    fn percentile_uses_nearest_rank_from_sorted_samples() {
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.50), 3.0);
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.95), 4.0);
    }

    #[test]
    fn production_zero_is_not_smoke_mode() {
        assert!(!is_smoke_value(Some("0")));
        assert!(is_smoke_value(Some("1")));
        assert!(!is_smoke_value(None));
    }

    #[test]
    fn positioned_v12_protocol_requires_realistic_dimensions() {
        assert!(protocol_shape_is_valid(false, 2_000, 1, 1, 100, 20, 768));
        assert!(protocol_shape_is_valid(false, 16_000, 32, 5, 100, 20, 768));
        assert!(!protocol_shape_is_valid(false, 2_000, 1, 1, 100, 20, 96));
        assert!(!protocol_shape_is_valid(false, 2_000, 2, 1, 100, 20, 768));
    }

    #[test]
    fn functional_smoke_is_realistic_but_claim_ineligible() {
        assert!(protocol_shape_is_valid(true, 2_000, 1, 1, 2, 1, 768));
        assert!(!protocol_shape_is_valid(true, 64, 1, 1, 2, 1, 8));
    }

    #[test]
    fn arm_label_must_match_the_observed_routing_path() {
        assert!(routing_path_is_valid_for_arm("flat", "flat", 2));
        assert!(routing_path_is_valid_for_arm("quantizer", "quantizer", 2));
        assert!(!routing_path_is_valid_for_arm("quantizer", "flat", 2));
        assert!(!routing_path_is_valid_for_arm("quantizer", "bootstrap", 1));
        assert!(!routing_path_is_valid_for_arm("flat", "flat", 1));
    }

    #[test]
    fn preflight_failure_cancels_ready_writers_without_a_barrier() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        ready_tx.send(WriterReady::Ready).unwrap();
        ready_tx
            .send(WriterReady::Failed("writer 1 preflight failed".into()))
            .unwrap();
        drop(ready_tx);

        let error = await_writer_readiness(&ready_rx, 3).unwrap_err();
        assert!(error.to_string().contains("writer 1 preflight failed"));
        assert!(error.to_string().contains("2 of 3"));
    }

    #[test]
    fn dropping_start_senders_releases_every_ready_writer() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let mut start_senders = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..2 {
            let ready_tx = ready_tx.clone();
            let (start_tx, start_rx) = std::sync::mpsc::sync_channel::<()>(0);
            start_senders.push(start_tx);
            handles.push(std::thread::spawn(move || {
                ready_tx.send(WriterReady::Ready).unwrap();
                drop(ready_tx);
                start_rx.recv().is_err()
            }));
        }
        ready_tx
            .send(WriterReady::Failed("writer 2 preflight failed".into()))
            .unwrap();
        drop(ready_tx);

        let error = await_writer_readiness(&ready_rx, 3).unwrap_err();
        assert!(error.to_string().contains("writer 2 preflight failed"));
        drop(start_senders);
        for handle in handles {
            assert!(handle.join().unwrap());
        }
    }

    #[test]
    fn progress_snapshot_localizes_the_active_remote_stage() {
        let progress = Progress::default();
        progress.opens_started.store(8, Ordering::Relaxed);
        progress.opens_completed.store(8, Ordering::Relaxed);
        progress.warmups_started.store(160, Ordering::Relaxed);
        progress.warmups_completed.store(160, Ordering::Relaxed);
        progress.routes_started.store(800, Ordering::Relaxed);
        progress.routes_completed.store(800, Ordering::Relaxed);
        progress.appends_started.store(17, Ordering::Relaxed);
        progress.appends_completed.store(9, Ordering::Relaxed);

        assert_eq!(
            progress.snapshot(),
            "opens=8/8 warmups=160/160 routes=800/800 appends=9/17 writers_ready=0 writers_done=0"
        );
    }
}

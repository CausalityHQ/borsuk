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
    sync::{Arc, Barrier, Mutex},
    thread,
    time::Instant,
};

use borsuk::{BorsukIndex, IndexConfig, OpenOptions, VectorMetric, VectorRecord};

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
    cohort_sha256: String,
    architecture: String,
    instance_type: String,
}

#[derive(Clone)]
struct Sample {
    writer: usize,
    operation: usize,
    record_id: String,
    latency_ms: f64,
    selected_cell: u32,
    storage_requests: u64,
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

fn vector(seed: u64, ordinal: u64, dimensions: usize) -> Vec<f32> {
    let mut state = seed ^ ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..dimensions)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            let bits = state.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 40;
            bits as f32 / (1_u64 << 24) as f32
        })
        .collect()
}

fn build() -> BenchResult<()> {
    let uri = required("BORSUK_ROUTING_INDEX_URI")?;
    let cell_count: usize = number("BORSUK_ROUTING_CELL_COUNT")?;
    let dimensions: usize = number("BORSUK_ROUTING_DIMENSIONS")?;
    let smoke = env::var_os("BORSUK_ROUTING_SMOKE").is_some();
    if (!smoke && (!matches!(cell_count, 2_000 | 16_000) || dimensions != 96))
        || (smoke && (cell_count != 64 || dimensions != 8))
    {
        return Err("build differs from the frozen 2K/16K-cell, 96D protocol".into());
    }
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
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
        cohort_sha256: required("BORSUK_ROUTING_COHORT_SHA256")?,
        architecture: required("BORSUK_ARCHITECTURE")?,
        instance_type: required("BORSUK_INSTANCE_TYPE")?,
    };
    let smoke = env::var_os("BORSUK_ROUTING_SMOKE").is_some();
    let production_shape = matches!(config.cell_count, 2_000 | 16_000)
        && matches!(config.writers, 1 | 8 | 32)
        && (1..=5).contains(&config.repetition)
        && config.operations == 100
        && config.warmup == 20
        && config.dimensions == 96;
    let smoke_shape = config.cell_count == 64
        && config.writers == 1
        && config.repetition == 1
        && config.operations == 2
        && config.warmup == 1
        && config.dimensions == 8;
    if ((!smoke && !production_shape) || (smoke && !smoke_shape))
        || !matches!(config.mode.as_str(), "flat" | "quantizer")
    {
        return Err("run cell differs from the frozen routing manifest".into());
    }
    for digest in [
        &config.source_sha256,
        &config.manifest_sha256,
        &config.cohort_sha256,
    ] {
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
    let barrier = Arc::new(Barrier::new(config.writers + 1));
    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let handles = (0..config.writers)
        .map(|writer| {
            let config = Arc::clone(&config);
            let barrier = Arc::clone(&barrier);
            let samples = Arc::clone(&samples);
            thread::spawn(move || -> BenchResult<()> {
                let mut index = open(&config)?;
                for operation in 0..config.warmup {
                    let ordinal = ((writer * config.warmup + operation) as u64) | (1_u64 << 62);
                    index.add(vec![VectorRecord::new(
                        format!(
                            "warm-r{}-c{}-w{writer:02}-o{operation:03}",
                            config.repetition, config.writers
                        ),
                        vector(config.seed, ordinal, config.dimensions),
                    )])?;
                }
                let cohort = (0..config.operations)
                    .map(|operation| {
                        let ordinal = writer * config.operations + operation;
                        let value = vector(
                            config.seed.wrapping_add(config.repetition as u64),
                            ordinal as u64,
                            config.dimensions,
                        );
                        let cell = index.logical_cell_for_research(&value)?;
                        Ok((operation, value, cell.cell_ordinal))
                    })
                    .collect::<BenchResult<Vec<_>>>()?;
                barrier.wait();
                let mut local = Vec::with_capacity(config.operations);
                for (operation, value, selected_cell) in cohort {
                    let record_id = format!(
                        "measure-c{}-r{}-c{}-w{writer:02}-o{operation:03}",
                        config.cell_count, config.repetition, config.writers
                    );
                    let started = Instant::now();
                    let (_, report) =
                        index.add_with_report(vec![value], Some(vec![record_id.clone()]))?;
                    local.push(Sample {
                        writer,
                        operation,
                        record_id,
                        latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                        selected_cell,
                        storage_requests: report.requests.total(),
                    });
                }
                samples.lock().unwrap().extend(local);
                Ok(())
            })
        })
        .collect::<Vec<_>>();
    let cpu_before = process_cpu_seconds()?;
    let started = Instant::now();
    barrier.wait();
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
    let identity = format!(
        "{},{},{},{},{},{},{},{},{}",
        config.source_sha256,
        config.manifest_sha256,
        config.architecture,
        config.instance_type,
        config.mode,
        config.cell_count,
        config.writers,
        config.repetition,
        config.cohort_sha256,
    );
    let latencies = samples
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    let requests = samples
        .iter()
        .map(|sample| sample.storage_requests)
        .sum::<u64>();
    let distinct = samples
        .iter()
        .map(|sample| sample.selected_cell)
        .collect::<BTreeSet<_>>()
        .len();
    let mut summary = BufWriter::new(File::create(staging.join("summary.csv"))?);
    writeln!(
        summary,
        "source_sha256,manifest_sha256,architecture,instance_type,routing_mode,cell_count,writers,repetition,cohort_sha256,operations,elapsed_ms,cpu_seconds,p50_ms,p95_ms,throughput_ops_per_second,storage_requests,distinct_cells"
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
        "source_sha256,manifest_sha256,architecture,instance_type,routing_mode,cell_count,writers,repetition,cohort_sha256,writer,operation,record_id,latency_ms,selected_cell,storage_requests"
    )?;
    for sample in samples {
        writeln!(
            raw,
            "{identity},{},{},{},{:.9},{},{}",
            sample.writer,
            sample.operation,
            sample.record_id,
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
        let first = vector(7, 11, 96);
        assert_eq!(first, vector(7, 11, 96));
        assert_ne!(first, vector(7, 12, 96));
        assert!(first.iter().all(|value| value.is_finite()));
    }

    #[test]
    fn percentile_uses_nearest_rank_from_sorted_samples() {
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.50), 3.0);
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.95), 4.0);
    }
}

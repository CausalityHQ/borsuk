//! Bounded group-commit ingest qualification.

use std::{
    collections::{BTreeMap, VecDeque},
    env,
    error::Error,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Barrier, Mutex},
    thread,
    time::{Duration, Instant},
};

use arrow_array::{Array, FixedSizeListArray, Float32Array, LargeListArray, ListArray};
use borsuk::{
    BorsukIndex, GROUP_COMMIT_STRIPE_COUNT, GroupCommitConfig, GroupCommitLaneReceipt,
    GroupCommitTicket, GroupCommitWriter, LeafMode, OpenOptions, RequestCounts, SearchOptions,
    StorageAccessTrace, VectorRecord, install_storage_access_trace,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
struct Sample {
    process_id: u32,
    writer: usize,
    writer_instance: usize,
    operation: usize,
    record_ids: Vec<String>,
    batch_records: usize,
    latency_ms: f64,
    commit_lane: usize,
    commit_sequence: u64,
    committed_records: usize,
    acknowledgement_bytes: u64,
    group_requests: RequestCounts,
    lane_receipts: Vec<GroupCommitLaneReceipt>,
}

struct ReadSample {
    query: usize,
    record_id: String,
    hit_id: String,
    hit_ids: String,
    contains_record_id: bool,
    latency_ms: f64,
    requests: RequestCounts,
    bytes_read: u64,
    disk_cache_bytes_read: u64,
    backing_bytes_read: u64,
    segments_searched: usize,
    global_base_approximate_us: u64,
    global_base_exact_rerank_us: u64,
    global_delta_approximate_us: u64,
    global_delta_exact_rerank_us: u64,
    global_delta_wait_us: u64,
}

struct ReadMeasurement {
    samples: Vec<ReadSample>,
    latencies: Vec<f64>,
    requests: RequestCounts,
    bytes: u64,
    disk_cache_bytes: u64,
    backing_bytes: u64,
    segments_searched: usize,
    hits: usize,
}

#[derive(Clone, Copy)]
struct ReadConfig {
    operations: usize,
    records_per_operation: usize,
    query_count: usize,
    diagnostic_protocol: bool,
    max_read_segments: usize,
    refresh_before_each: bool,
}

#[derive(Clone, Copy)]
struct ReadQualificationShape {
    writers: usize,
    operations: usize,
    records_per_operation: usize,
    dimensions: usize,
    query_count: usize,
    max_read_segments: usize,
    stripe_bytes: usize,
    repetition: usize,
}

#[derive(Clone, Copy)]
struct ReadHedgeQualificationShape {
    read: ReadQualificationShape,
    read_writer: usize,
    hedge_after_ms: Option<u64>,
}

struct PendingAppend {
    operation: usize,
    record_ids: Vec<String>,
    started: Instant,
    ticket: GroupCommitTicket,
}

fn required(name: &str) -> BenchResult<String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn open_benchmark_index_with_stripe(
    uri: &str,
    global_pq_prefetch_stripe_bytes: usize,
) -> borsuk::Result<BorsukIndex> {
    let cache_dir = env::var_os("BORSUK_GROUP_COMMIT_CACHE_DIR").map(PathBuf::from);
    BorsukIndex::open_with_options(
        uri,
        OpenOptions {
            // Keep repeated post-drain probes local after the first decode;
            // this is the bounded production read profile for object storage.
            cache_dir,
            cache_max_bytes: Some(256 * 1024 * 1024),
            global_pq_prefetch_stripe_bytes,
            segment_cache_max_bytes: Some(64 * 1024 * 1024),
            ..OpenOptions::default()
        },
    )
}

fn read_hedge_open_options(
    global_pq_prefetch_stripe_bytes: usize,
    hedge_after_ms: Option<u64>,
) -> OpenOptions {
    OpenOptions {
        cache_dir: None,
        global_pq_prefetch_stripe_bytes,
        global_pq_slow_read_hedge_after: hedge_after_ms.map(Duration::from_millis),
        segment_cache_max_bytes: Some(64 * 1024 * 1024),
        ..OpenOptions::default()
    }
}

fn open_read_hedge_index(
    uri: &str,
    global_pq_prefetch_stripe_bytes: usize,
    hedge_after_ms: Option<u64>,
) -> borsuk::Result<BorsukIndex> {
    BorsukIndex::open_with_options(
        uri,
        read_hedge_open_options(global_pq_prefetch_stripe_bytes, hedge_after_ms),
    )
}

fn open_benchmark_index(uri: &str) -> borsuk::Result<BorsukIndex> {
    open_benchmark_index_with_stripe(uri, OpenOptions::default().global_pq_prefetch_stripe_bytes)
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

fn lane_receipts_field(receipts: &[GroupCommitLaneReceipt]) -> String {
    receipts
        .iter()
        .map(|receipt| {
            format!(
                "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
                receipt.commit_lane,
                receipt.commit_sequence,
                receipt.lease_epoch,
                receipt.committed_records,
                receipt.acknowledgement_bytes,
                receipt.requests.total(),
                receipt.requests.gets,
                receipt.requests.puts,
                receipt.requests.deletes,
                receipt.requests.heads,
                receipt.requests.lists,
                blake3::Hash::from_bytes(receipt.extent_checksum).to_hex(),
                blake3::Hash::from_bytes(receipt.published_head_checksum).to_hex(),
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn parquet_vector_row(array: &dyn Array, row: usize, dimensions: usize) -> BenchResult<Vec<f32>> {
    let values = if let Some(vectors) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        vectors.value(row)
    } else if let Some(vectors) = array.as_any().downcast_ref::<ListArray>() {
        vectors.value(row)
    } else if let Some(vectors) = array.as_any().downcast_ref::<LargeListArray>() {
        vectors.value(row)
    } else {
        return Err(format!(
            "Parquet `emb` must be a float32 list, got {:?}",
            array.data_type()
        )
        .into());
    };
    let floats = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            format!(
                "Parquet `emb` values must be float32, got {:?}",
                values.data_type()
            )
        })?;
    if floats.null_count() != 0 || floats.len() != dimensions {
        return Err(format!(
            "Parquet `emb` row has {} values/nulls; expected {dimensions} non-null values",
            floats.len()
        )
        .into());
    }
    Ok(floats.values().to_vec())
}

fn read_parquet_vectors(
    dataset: &Path,
    rows: usize,
    dimensions: usize,
) -> BenchResult<Vec<Vec<f32>>> {
    read_parquet_vector_range(dataset, 0, rows, dimensions)
}

fn read_parquet_vector_range(
    dataset: &Path,
    offset: usize,
    rows: usize,
    dimensions: usize,
) -> BenchResult<Vec<Vec<f32>>> {
    let path = dataset.join("train.parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?
        .with_offset(offset)
        .with_limit(rows)
        .with_batch_size(rows.clamp(1, 1024))
        .build()?;
    let mut vectors = Vec::with_capacity(rows);
    for batch in reader {
        let batch = batch?;
        let column = batch
            .column_by_name("emb")
            .ok_or_else(|| format!("{} has no `emb` column", path.display()))?;
        for row in 0..batch.num_rows() {
            if vectors.len() == rows {
                break;
            }
            vectors.push(parquet_vector_row(column.as_ref(), row, dimensions)?);
        }
        if vectors.len() == rows {
            break;
        }
    }
    if vectors.len() != rows {
        return Err(format!(
            "dataset vectors ended after {} rows; expected {rows}",
            vectors.len()
        )
        .into());
    }
    Ok(vectors)
}

fn write_samples(path: &Path, samples: &[Sample]) -> BenchResult<()> {
    let mut raw = BufWriter::new(File::create(path)?);
    writeln!(
        raw,
        "writer,writer_instance,process_id,operation,batch_records,first_record_id,record_ids,latency_ms,commit_lane,commit_sequence,committed_records,acknowledgement_bytes,group_requests,group_gets,group_puts,group_heads,lane_receipts"
    )?;
    for sample in samples {
        writeln!(
            raw,
            "{},{},{},{},{},{},{},{:.9},{},{},{},{},{},{},{},{},{}",
            sample.writer,
            sample.writer_instance,
            sample.process_id,
            sample.operation,
            sample.batch_records,
            sample.record_ids[0],
            sample.record_ids.join("|"),
            sample.latency_ms,
            sample.commit_lane,
            sample.commit_sequence,
            sample.committed_records,
            sample.acknowledgement_bytes,
            sample.group_requests.total(),
            sample.group_requests.gets,
            sample.group_requests.puts,
            sample.group_requests.heads,
            lane_receipts_field(&sample.lane_receipts),
        )?;
    }
    raw.flush()?;
    Ok(())
}

fn parse_lane_receipts(encoded: &str) -> BenchResult<Vec<GroupCommitLaneReceipt>> {
    encoded
        .split(';')
        .map(|entry| {
            let fields = entry.split(':').collect::<Vec<_>>();
            if fields.len() != 13 {
                return Err("invalid child lane receipt evidence".into());
            }
            let numbers = fields[..11]
                .iter()
                .map(|field| field.parse::<u64>())
                .collect::<Result<Vec<_>, _>>()?;
            let extent_checksum = *blake3::Hash::from_hex(fields[11])?.as_bytes();
            let published_head_checksum = *blake3::Hash::from_hex(fields[12])?.as_bytes();
            Ok(GroupCommitLaneReceipt {
                commit_lane: usize::try_from(numbers[0])?,
                commit_sequence: numbers[1],
                lease_epoch: numbers[2],
                records: usize::try_from(numbers[3])?,
                committed_records: usize::try_from(numbers[3])?,
                acknowledgement_bytes: numbers[4],
                extent_checksum,
                published_head_checksum,
                requests: RequestCounts {
                    gets: numbers[6],
                    puts: numbers[7],
                    deletes: numbers[8],
                    heads: numbers[9],
                    lists: numbers[10],
                },
            })
        })
        .collect()
}

fn read_samples(path: &Path) -> BenchResult<Vec<Sample>> {
    let contents = fs::read_to_string(path)?;
    contents
        .lines()
        .skip(1)
        .map(|line| -> BenchResult<Sample> {
            let fields = line.split(',').collect::<Vec<_>>();
            if fields.len() != 17 {
                return Err(format!("invalid child sample row in {}", path.display()).into());
            }
            let lane_receipts = parse_lane_receipts(fields[16])?;
            Ok(Sample {
                writer: fields[0].parse()?,
                writer_instance: fields[1].parse()?,
                process_id: fields[2].parse()?,
                operation: fields[3].parse()?,
                batch_records: fields[4].parse()?,
                record_ids: fields[6].split('|').map(str::to_owned).collect(),
                latency_ms: fields[7].parse()?,
                commit_lane: fields[8].parse()?,
                commit_sequence: fields[9].parse()?,
                committed_records: fields[10].parse()?,
                acknowledgement_bytes: fields[11].parse()?,
                group_requests: RequestCounts {
                    gets: fields[13].parse()?,
                    puts: fields[14].parse()?,
                    heads: fields[15].parse()?,
                    ..RequestCounts::default()
                },
                lane_receipts,
            })
        })
        .collect()
}

fn cohort_seed(diagnostic_protocol: bool, writers: usize, repetition: usize) -> u64 {
    const MASTER_SEED: u64 = 76_412_031;
    if diagnostic_protocol {
        return MASTER_SEED;
    }
    MASTER_SEED
        .wrapping_add(repetition as u64)
        .wrapping_add((writers as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[((ordered.len() - 1) as f64 * quantile).round() as usize]
}

fn vector_mib_per_second(records_per_second: f64, dimensions: usize) -> f64 {
    records_per_second * dimensions as f64 * size_of::<f32>() as f64 / (1024.0 * 1024.0)
}

fn validate_throughput_concurrency(
    writers: usize,
    pipeline_depth: usize,
    records_per_operation: usize,
    min_records_per_second: f64,
    max_p95_ms: f64,
) -> BenchResult<()> {
    if min_records_per_second <= 0.0 {
        return Ok(());
    }
    let outstanding_records = writers
        .checked_mul(pipeline_depth)
        .and_then(|value| value.checked_mul(records_per_operation))
        .ok_or("group-commit outstanding-record count exceeds usize")?;
    let required = min_records_per_second * max_p95_ms / 1_000.0;
    if outstanding_records as f64 >= required {
        return Ok(());
    }
    Err(format!(
        "group-commit workload exposes {outstanding_records} outstanding records but needs at least {required:.3} to express {min_records_per_second:.3} records/s at {max_p95_ms:.3} ms p95"
    )
    .into())
}

fn validate_writer_topology(
    writers: usize,
    writer_instances: usize,
    worker_lanes: usize,
    persisted_stripes: usize,
) -> BenchResult<()> {
    if writer_instances == 0 {
        return Err("group-commit writer instances must be positive".into());
    }
    if writer_instances > writers {
        return Err("group-commit writer instances cannot exceed producer writers".into());
    }
    if !writers.is_multiple_of(writer_instances) {
        return Err("group-commit writer instances must divide producer writers evenly".into());
    }
    let required_stripes = writer_instances
        .checked_mul(worker_lanes)
        .ok_or("group-commit writer topology exceeds usize")?;
    if required_stripes > persisted_stripes {
        return Err(format!(
            "group-commit writer topology requires {required_stripes} persisted writer stripes but the index provides {persisted_stripes}"
        )
        .into());
    }
    Ok(())
}

fn production_record_id(ordinal: usize) -> String {
    format!("group-o{ordinal:08}")
}

fn validate_read_qualification_shape(
    shape: ReadQualificationShape,
    structural_smoke: bool,
) -> BenchResult<()> {
    const MIB: usize = 1024 * 1024;
    let common =
        shape.max_read_segments == 4 && [MIB, 2 * MIB, 4 * MIB].contains(&shape.stripe_bytes);
    let expected = if structural_smoke {
        shape.writers == 2
            && shape.operations == 2
            && shape.records_per_operation == 1
            && shape.dimensions == 8
            && shape.query_count == 4
            && shape.repetition == 1
    } else {
        shape.writers == 8
            && shape.operations == 1_000
            && shape.records_per_operation == 16
            && shape.dimensions == 768
            && shape.query_count == 100
            && (1..=5).contains(&shape.repetition)
    };
    if !common || !expected {
        return Err("read qualification differs from the preregistered shape".into());
    }
    Ok(())
}

fn validate_read_confirmation_shape(
    shape: ReadQualificationShape,
    structural_smoke: bool,
) -> BenchResult<()> {
    const MIB: usize = 1024 * 1024;
    let common = shape.max_read_segments == 4 && [MIB, 4 * MIB].contains(&shape.stripe_bytes);
    let expected = if structural_smoke {
        shape.writers == 2
            && shape.operations == 2
            && shape.records_per_operation == 1
            && shape.dimensions == 8
            && shape.query_count == 4
            && shape.repetition == 1
    } else {
        shape.writers == 8
            && shape.operations == 1_000
            && shape.records_per_operation == 16
            && shape.dimensions == 768
            && shape.query_count == 500
            && (1..=5).contains(&shape.repetition)
    };
    if !common || !expected {
        return Err("read stripe confirmation differs from the preregistered shape".into());
    }
    Ok(())
}

fn validate_read_hedge_qualification_shape(
    shape: ReadHedgeQualificationShape,
    structural_smoke: bool,
) -> BenchResult<()> {
    const MIB: usize = 1024 * 1024;
    let common = shape.read.max_read_segments == 4
        && shape.read.stripe_bytes == MIB
        && shape.read_writer == 0;
    let expected = if structural_smoke {
        shape.read.writers == 2
            && shape.read.operations == 2
            && shape.read.records_per_operation == 1
            && shape.read.dimensions == 8
            && shape.read.query_count == 4
            && shape.read.repetition == 1
            && matches!(shape.hedge_after_ms, None | Some(5))
    } else {
        shape.read.writers == 8
            && shape.read.operations == 1_000
            && shape.read.records_per_operation == 16
            && shape.read.dimensions == 768
            && shape.read.query_count == 500
            && (1..=5).contains(&shape.read.repetition)
            && matches!(shape.hedge_after_ms, None | Some(20) | Some(35) | Some(75))
    };
    if !common || !expected {
        return Err("read hedge qualification differs from the preregistered shape".into());
    }
    Ok(())
}

fn validate_read_hedge_qualification_inputs(
    shape: ReadHedgeQualificationShape,
    structural_smoke: bool,
    cache_dir_present: bool,
) -> BenchResult<()> {
    validate_read_hedge_qualification_shape(shape, structural_smoke)?;
    if cache_dir_present {
        return Err("read hedge qualification forbids a disk cache".into());
    }
    Ok(())
}

fn parse_read_hedge_after_ms(value: &str, structural_smoke: bool) -> BenchResult<Option<u64>> {
    match (value, structural_smoke) {
        ("none", _) => Ok(None),
        ("5", true) => Ok(Some(5)),
        ("20", false) => Ok(Some(20)),
        ("35", false) => Ok(Some(35)),
        ("75", false) => Ok(Some(75)),
        _ => Err("read hedge delay differs from the preregistration".into()),
    }
}

fn read_hedge_order_position(
    repetition: usize,
    hedge_after_ms: Option<u64>,
    structural_smoke: bool,
) -> BenchResult<usize> {
    let repetition_is_valid = if structural_smoke {
        repetition == 1
    } else {
        (1..=5).contains(&repetition)
    };
    let registered_delay = if structural_smoke {
        matches!(hedge_after_ms, None | Some(5))
    } else {
        matches!(hedge_after_ms, None | Some(20) | Some(35) | Some(75))
    };
    if !repetition_is_valid || !registered_delay {
        return Err("read hedge arm order differs from the preregistration".into());
    }
    let control_first = structural_smoke || repetition % 2 == 1;
    Ok(match (control_first, hedge_after_ms.is_some()) {
        (true, false) | (false, true) => 0,
        (true, true) | (false, false) => 1,
    })
}

fn read_hedge_query_positions(
    writers: usize,
    operations: usize,
    read_writer: usize,
    query_count: usize,
) -> BenchResult<Vec<usize>> {
    if writers == 0 || operations == 0 || query_count == 0 || read_writer >= writers {
        return Err("read hedge query cohort is invalid".into());
    }
    let writer_start = read_writer
        .checked_mul(operations)
        .ok_or("read hedge writer offset exceeds usize")?;
    (0..query_count)
        .map(|query| {
            let offset = query
                .checked_mul(operations)
                .ok_or("read hedge query offset exceeds usize")?
                / query_count;
            writer_start
                .checked_add(offset)
                .ok_or_else(|| "read hedge query position exceeds usize".into())
        })
        .collect()
}

fn read_qualification_query_ordinal(
    sample: &Sample,
    operations: usize,
    records_per_operation: usize,
) -> BenchResult<usize> {
    if sample.operation >= operations
        || sample.batch_records != records_per_operation
        || sample.record_ids.len() != records_per_operation
    {
        return Err("read qualification sample shape is invalid".into());
    }
    let ordinal = sample
        .writer
        .checked_mul(operations)
        .and_then(|value| value.checked_add(sample.operation))
        .and_then(|value| value.checked_mul(records_per_operation))
        .ok_or("read qualification dataset ordinal exceeds usize")?;
    if sample.record_ids[0] != production_record_id(ordinal) {
        return Err("read qualification sample ID does not match its dataset ordinal".into());
    }
    Ok(ordinal)
}

fn read_qualification_order_position(repetition: usize, stripe_bytes: usize) -> BenchResult<usize> {
    const MIB: usize = 1024 * 1024;
    if !(1..=5).contains(&repetition) {
        return Err("read qualification repetition is outside 1..=5".into());
    }
    let order = match (repetition - 1) % 3 {
        0 => [MIB, 2 * MIB, 4 * MIB],
        1 => [2 * MIB, 4 * MIB, MIB],
        _ => [4 * MIB, MIB, 2 * MIB],
    };
    order
        .iter()
        .position(|candidate| *candidate == stripe_bytes)
        .ok_or_else(|| "read qualification stripe is not preregistered".into())
}

fn read_confirmation_order_position(repetition: usize, stripe_bytes: usize) -> BenchResult<usize> {
    const MIB: usize = 1024 * 1024;
    if !(1..=5).contains(&repetition) {
        return Err("read stripe confirmation repetition is outside 1..=5".into());
    }
    let order = if repetition % 2 == 1 {
        [MIB, 4 * MIB]
    } else {
        [4 * MIB, MIB]
    };
    order
        .iter()
        .position(|candidate| *candidate == stripe_bytes)
        .ok_or_else(|| "read stripe confirmation arm is not preregistered".into())
}

fn measure_reads(
    index: &mut BorsukIndex,
    samples: &[Sample],
    input_vectors: &[Vec<f32>],
    config: ReadConfig,
) -> BenchResult<ReadMeasurement> {
    let mut measurement = ReadMeasurement {
        samples: Vec::with_capacity(config.query_count),
        latencies: Vec::with_capacity(config.query_count),
        requests: RequestCounts::default(),
        bytes: 0,
        disk_cache_bytes: 0,
        backing_bytes: 0,
        segments_searched: 0,
        hits: 0,
    };
    for (query_index, sample) in samples.iter().take(config.query_count).enumerate() {
        let ordinal =
            (sample.writer * config.operations + sample.operation) * config.records_per_operation;
        let record_id = &sample.record_ids[0];
        let read_started = Instant::now();
        if config.refresh_before_each {
            index.refresh_wal_tail()?;
        }
        let report = index.search_with_report(
            &input_vectors[ordinal],
            if config.diagnostic_protocol {
                SearchOptions::exact(1)
            } else {
                SearchOptions::approx(10, LeafMode::SrhtPqScan)
                    .with_max_segments(config.max_read_segments)
                    // The production gate is k=10; a 16-candidate rerank
                    // budget preserves headroom while avoiding needless
                    // object-store vector fetches on the post-drain path.
                    .with_max_candidates_per_segment(16)
            },
        )?;
        let latency_ms = read_started.elapsed().as_secs_f64() * 1_000.0;
        measurement.latencies.push(latency_ms);
        measurement.requests.gets += report.requests.gets;
        measurement.requests.puts += report.requests.puts;
        measurement.requests.deletes += report.requests.deletes;
        measurement.requests.heads += report.requests.heads;
        measurement.requests.lists += report.requests.lists;
        measurement.bytes = measurement.bytes.saturating_add(report.bytes_read);
        measurement.disk_cache_bytes = measurement
            .disk_cache_bytes
            .saturating_add(report.disk_cache_bytes_read);
        measurement.backing_bytes = measurement
            .backing_bytes
            .saturating_add(report.backing_bytes_read);
        measurement.segments_searched = measurement
            .segments_searched
            .saturating_add(report.segments_searched);
        let hit_id = report
            .hits
            .first()
            .map_or_else(String::new, |hit| hit.id.as_str().to_string());
        let hit_ids = report
            .hits
            .iter()
            .take(10)
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>()
            .join("|");
        let contains_record_id = report.hits.iter().any(|hit| hit.id.as_str() == record_id);
        measurement.samples.push(ReadSample {
            query: query_index,
            record_id: record_id.clone(),
            hit_id,
            hit_ids,
            contains_record_id,
            latency_ms,
            requests: report.requests,
            bytes_read: report.bytes_read,
            disk_cache_bytes_read: report.disk_cache_bytes_read,
            backing_bytes_read: report.backing_bytes_read,
            segments_searched: report.segments_searched,
            global_base_approximate_us: report.global_base_approximate_us,
            global_base_exact_rerank_us: report.global_base_exact_rerank_us,
            global_delta_approximate_us: report.global_delta_approximate_us,
            global_delta_exact_rerank_us: report.global_delta_exact_rerank_us,
            global_delta_wait_us: report.global_delta_wait_us,
        });
        measurement.hits += usize::from(contains_record_id);
    }
    Ok(measurement)
}

fn write_read_samples(path: &Path, samples: &[ReadSample]) -> BenchResult<()> {
    let mut reads = BufWriter::new(File::create(path)?);
    writeln!(
        reads,
        "query,record_id,hit_id,contains_record_id,latency_ms,requests,gets,puts,deletes,heads,lists,bytes_read,segments_searched,global_base_approximate_us,global_base_exact_rerank_us,global_delta_approximate_us,global_delta_exact_rerank_us,global_delta_wait_us"
    )?;
    for sample in samples {
        writeln!(
            reads,
            "{},{},{},{},{:.9},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            sample.query,
            sample.record_id,
            sample.hit_id,
            sample.contains_record_id,
            sample.latency_ms,
            sample.requests.total(),
            sample.requests.gets,
            sample.requests.puts,
            sample.requests.deletes,
            sample.requests.heads,
            sample.requests.lists,
            sample.bytes_read,
            sample.segments_searched,
            sample.global_base_approximate_us,
            sample.global_base_exact_rerank_us,
            sample.global_delta_approximate_us,
            sample.global_delta_exact_rerank_us,
            sample.global_delta_wait_us,
        )?;
    }
    reads.flush()?;
    Ok(())
}

fn write_read_hedge_samples(path: &Path, samples: &[ReadSample]) -> BenchResult<()> {
    let mut reads = BufWriter::new(File::create(path)?);
    writeln!(
        reads,
        "query,record_id,hit_id,hit_ids,contains_record_id,latency_ms,requests,gets,puts,deletes,heads,lists,bytes_read,disk_cache_bytes_read,backing_bytes_read,segments_searched,global_base_approximate_us,global_base_exact_rerank_us,global_delta_approximate_us,global_delta_exact_rerank_us,global_delta_wait_us"
    )?;
    for sample in samples {
        writeln!(
            reads,
            "{},{},{},{},{},{:.9},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            sample.query,
            sample.record_id,
            sample.hit_id,
            sample.hit_ids,
            sample.contains_record_id,
            sample.latency_ms,
            sample.requests.total(),
            sample.requests.gets,
            sample.requests.puts,
            sample.requests.deletes,
            sample.requests.heads,
            sample.requests.lists,
            sample.bytes_read,
            sample.disk_cache_bytes_read,
            sample.backing_bytes_read,
            sample.segments_searched,
            sample.global_base_approximate_us,
            sample.global_base_exact_rerank_us,
            sample.global_delta_approximate_us,
            sample.global_delta_exact_rerank_us,
            sample.global_delta_wait_us,
        )?;
    }
    reads.flush()?;
    Ok(())
}

#[derive(Clone, Copy)]
struct PerformanceObservation {
    p95_ms: f64,
    records_per_second: f64,
    end_to_end_records_per_second: f64,
    read_p95_ms: f64,
    active_tail_read_p95_ms: f64,
    inserted_id_recall_at_10: f64,
}

#[derive(Clone, Copy)]
struct PerformanceThresholds {
    max_p95_ms: f64,
    min_records_per_second: f64,
    min_end_to_end_records_per_second: f64,
    max_read_p95_ms: f64,
    min_inserted_id_recall_at_10: f64,
}

fn production_performance_gate_failures(
    observed: PerformanceObservation,
    thresholds: PerformanceThresholds,
) -> Vec<&'static str> {
    let mut failures = Vec::new();
    if observed.p95_ms >= thresholds.max_p95_ms {
        failures.push("PRODUCTION_WRITE_P95_FAILED");
    }
    if thresholds.min_records_per_second > 0.0
        && observed.records_per_second < thresholds.min_records_per_second
    {
        failures.push("PRODUCTION_WRITE_THROUGHPUT_FAILED");
    }
    if thresholds.min_end_to_end_records_per_second > 0.0
        && observed.end_to_end_records_per_second < thresholds.min_end_to_end_records_per_second
    {
        failures.push("PRODUCTION_END_TO_END_THROUGHPUT_FAILED");
    }
    if observed.read_p95_ms >= thresholds.max_read_p95_ms {
        failures.push("PRODUCTION_READ_P95_FAILED");
    }
    if observed.active_tail_read_p95_ms >= thresholds.max_read_p95_ms {
        failures.push("PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED");
    }
    if observed.inserted_id_recall_at_10 < thresholds.min_inserted_id_recall_at_10 {
        failures.push("PRODUCTION_INSERTED_ID_RECALL_AT_10_FAILED");
    }
    failures
}

#[allow(clippy::too_many_arguments)]
fn run_process_worker(
    uri: &str,
    protocol: &str,
    operations: usize,
    dimensions: usize,
    max_delay_ms: u64,
    max_records: usize,
    worker_lanes: usize,
    pipeline_depth: usize,
    records_per_operation: usize,
    vector_seed: u64,
) -> BenchResult<()> {
    let writer_ordinal: usize = number("BORSUK_GROUP_COMMIT_WRITER_ORDINAL")?;
    let process_dir = PathBuf::from(required("BORSUK_GROUP_COMMIT_PROCESS_OUTPUT")?);
    let start_marker = PathBuf::from(required("BORSUK_GROUP_COMMIT_START_MARKER")?);
    fs::create_dir_all(&process_dir)?;
    let vector_count = operations * records_per_operation;
    let input_vectors = if matches!(protocol, "scalability" | "local") {
        let dataset = PathBuf::from(required("BORSUK_GROUP_COMMIT_DATASET")?);
        read_parquet_vector_range(
            &dataset,
            writer_ordinal * vector_count,
            vector_count,
            dimensions,
        )?
    } else {
        let first = writer_ordinal * vector_count;
        (0..vector_count)
            .map(|local| vector(vector_seed, (first + local) as u64, dimensions))
            .collect()
    };
    let writer = GroupCommitWriter::new(
        open_benchmark_index(uri)?,
        GroupCommitConfig {
            max_delay: Duration::from_millis(max_delay_ms),
            max_records,
            worker_lanes,
        },
    )?;
    fs::write(process_dir.join("READY"), b"ready\n")?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !start_marker.is_file() {
        if Instant::now() >= deadline {
            return Err("timed out waiting for the multi-process start barrier".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    let mut samples = Vec::with_capacity(operations);
    let mut pending = VecDeque::<PendingAppend>::with_capacity(pipeline_depth);
    for operation in 0..operations {
        let global_first = (writer_ordinal * operations + operation) * records_per_operation;
        let local_first = operation * records_per_operation;
        let record_ids = (0..records_per_operation)
            .map(|batch| {
                if protocol == "diagnostic" {
                    format!("group-w{writer_ordinal:02}-o{operation:03}-b{batch:03}")
                } else {
                    production_record_id(global_first + batch)
                }
            })
            .collect::<Vec<_>>();
        let records = record_ids
            .iter()
            .enumerate()
            .map(|(batch, id)| {
                VectorRecord::new(id.clone(), input_vectors[local_first + batch].clone())
            })
            .collect();
        let started = Instant::now();
        let ticket = writer.append_async(records)?;
        pending.push_back(PendingAppend {
            operation,
            record_ids,
            started,
            ticket,
        });
        if pending.len() < pipeline_depth {
            continue;
        }
        let completed = pending.pop_front().expect("non-empty pipeline");
        samples.push(finish_sample(writer_ordinal, completed)?);
    }
    while let Some(completed) = pending.pop_front() {
        samples.push(finish_sample(writer_ordinal, completed)?);
    }
    samples.sort_by_key(|sample| sample.operation);
    write_samples(&process_dir.join("writer-samples.csv.tmp"), &samples)?;
    fs::rename(
        process_dir.join("writer-samples.csv.tmp"),
        process_dir.join("writer-samples.csv"),
    )?;
    fs::write(process_dir.join("COMPLETE"), b"complete\n")?;
    Ok(())
}

fn finish_sample(writer_ordinal: usize, completed: PendingAppend) -> BenchResult<Sample> {
    let PendingAppend {
        operation,
        record_ids,
        started,
        ticket,
    } = completed;
    let receipt = ticket.wait()?;
    Ok(Sample {
        process_id: std::process::id(),
        writer: writer_ordinal,
        writer_instance: writer_ordinal,
        operation,
        batch_records: record_ids.len(),
        record_ids,
        latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
        commit_lane: receipt.commit_lane,
        commit_sequence: receipt.commit_sequence,
        committed_records: receipt.committed_records,
        acknowledgement_bytes: receipt.acknowledgement_bytes,
        group_requests: receipt.requests,
        lane_receipts: receipt.lane_receipts,
    })
}

fn run_process_coordinator(output: &Path, writers: usize) -> BenchResult<(Vec<Sample>, f64)> {
    let process_root = output.join("processes");
    fs::create_dir_all(&process_root)?;
    let start_marker = process_root.join("START");
    let executable = env::current_exe()?;
    let mut children = Vec::with_capacity(writers);
    for writer in 0..writers {
        let process_dir = process_root.join(format!("w{writer:02}"));
        fs::create_dir_all(&process_dir)?;
        let stdout = File::create(process_dir.join("stdout.log"))?;
        let stderr = File::create(process_dir.join("stderr.log"))?;
        let child = Command::new(&executable)
            .env("BORSUK_GROUP_COMMIT_ROLE", "writer-process")
            .env("BORSUK_GROUP_COMMIT_WRITER_ORDINAL", writer.to_string())
            .env("BORSUK_GROUP_COMMIT_PROCESS_OUTPUT", &process_dir)
            .env("BORSUK_GROUP_COMMIT_START_MARKER", &start_marker)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        children.push((writer, process_dir, child));
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let mut ready = 0;
        for (writer, process_dir, child) in &mut children {
            if process_dir.join("READY").is_file() {
                ready += 1;
            } else if let Some(status) = child.try_wait()? {
                return Err(
                    format!("writer process {writer} exited before ready: {status}").into(),
                );
            }
        }
        if ready == writers {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!("only {ready}/{writers} writer processes reached ready").into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    let started = Instant::now();
    fs::write(&start_marker, b"start\n")?;
    for (writer, process_dir, mut child) in children {
        let status = child.wait()?;
        if !status.success() || !process_dir.join("COMPLETE").is_file() {
            return Err(format!("writer process {writer} failed: {status}").into());
        }
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let mut samples = Vec::new();
    for writer in 0..writers {
        samples.extend(read_samples(
            &process_root.join(format!("w{writer:02}/writer-samples.csv")),
        )?);
    }
    Ok((samples, elapsed_ms))
}

fn required_sha256(name: &str) -> BenchResult<String> {
    let value = required(name)?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{name} must be a 64-character SHA-256").into());
    }
    Ok(value.to_ascii_lowercase())
}

fn run_read_qualification(protocol: &str) -> BenchResult<()> {
    let confirmation = protocol == "read-stripe-confirmation";
    let hedge_qualification = protocol == "read-hedge-qualification";
    let uri = required("BORSUK_GROUP_COMMIT_INDEX_URI")?;
    let structural_smoke = env::var("BORSUK_GROUP_COMMIT_READ_SMOKE").as_deref() == Ok("1");
    if !uri.starts_with("s3://") && !structural_smoke {
        return Err("read qualification requires an s3:// index outside structural smoke".into());
    }
    let output = PathBuf::from(required("BORSUK_GROUP_COMMIT_OUTPUT")?);
    let cache_dir = env::var_os("BORSUK_GROUP_COMMIT_CACHE_DIR").map(PathBuf::from);
    if !hedge_qualification && cache_dir.is_none() {
        return Err("missing required environment variable BORSUK_GROUP_COMMIT_CACHE_DIR".into());
    }
    if output.exists() {
        return Err(format!("refusing to replace output {}", output.display()).into());
    }
    if let Some(cache_dir) = cache_dir.as_deref()
        && cache_dir.exists()
    {
        return Err(format!("refusing to reuse cache {}", cache_dir.display()).into());
    }
    let source_sha = required_sha256("BORSUK_SOURCE_SHA256")?;
    let manifest_sha = required_sha256("BORSUK_GROUP_COMMIT_MANIFEST_SHA256")?;
    let base_source_sha = required_sha256("BORSUK_GROUP_COMMIT_BASE_SOURCE_SHA256")?;
    let base_manifest_sha = required_sha256("BORSUK_GROUP_COMMIT_BASE_MANIFEST_SHA256")?;
    let base_samples_sha = required_sha256("BORSUK_GROUP_COMMIT_BASE_SAMPLES_SHA256")?;
    let dataset_sha = required_sha256("BORSUK_GROUP_COMMIT_DATASET_SHA256")?;
    let base_cell = required("BORSUK_GROUP_COMMIT_BASE_CELL")?;
    if (!structural_smoke && base_cell != "c2000/r01/l1/w8")
        || (structural_smoke && base_cell != "smoke")
    {
        return Err("read qualification base cell differs from the preregistration".into());
    }
    let shape = ReadQualificationShape {
        writers: number("BORSUK_GROUP_COMMIT_WRITERS")?,
        operations: number("BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER")?,
        records_per_operation: number("BORSUK_GROUP_COMMIT_RECORDS_PER_OPERATION")?,
        dimensions: number("BORSUK_GROUP_COMMIT_DIMENSIONS")?,
        query_count: number("BORSUK_GROUP_COMMIT_READ_QUERIES")?,
        max_read_segments: number("BORSUK_GROUP_COMMIT_MAX_READ_SEGMENTS")?,
        stripe_bytes: number("BORSUK_GROUP_COMMIT_PREFETCH_STRIPE_BYTES")?,
        repetition: number("BORSUK_GROUP_COMMIT_READ_REPETITION")?,
    };
    let hedge_after_ms = if hedge_qualification {
        parse_read_hedge_after_ms(
            &required("BORSUK_GROUP_COMMIT_HEDGE_AFTER_MS")?,
            structural_smoke,
        )?
    } else {
        None
    };
    let read_writer = if hedge_qualification {
        number("BORSUK_GROUP_COMMIT_READ_WRITER")?
    } else {
        0
    };
    if hedge_qualification {
        validate_read_hedge_qualification_inputs(
            ReadHedgeQualificationShape {
                read: shape,
                read_writer,
                hedge_after_ms,
            },
            structural_smoke,
            cache_dir.is_some(),
        )?;
    } else if confirmation {
        validate_read_confirmation_shape(shape, structural_smoke)?;
    } else {
        validate_read_qualification_shape(shape, structural_smoke)?;
    }
    let order_position: usize = number("BORSUK_GROUP_COMMIT_READ_ORDER_POSITION")?;
    let expected_order_position = if structural_smoke {
        if hedge_qualification {
            read_hedge_order_position(shape.repetition, hedge_after_ms, true)?
        } else {
            0
        }
    } else if hedge_qualification {
        read_hedge_order_position(shape.repetition, hedge_after_ms, false)?
    } else if confirmation {
        read_confirmation_order_position(shape.repetition, shape.stripe_bytes)?
    } else {
        read_qualification_order_position(shape.repetition, shape.stripe_bytes)?
    };
    if order_position != expected_order_position {
        return Err("read qualification arm order differs from the preregistration".into());
    }
    let samples_path = PathBuf::from(required("BORSUK_GROUP_COMMIT_BASE_SAMPLES")?);
    let dataset = if structural_smoke {
        None
    } else {
        Some(PathBuf::from(required("BORSUK_GROUP_COMMIT_DATASET")?))
    };
    let samples = read_samples(&samples_path)?;
    let expected_samples = shape
        .writers
        .checked_mul(shape.operations)
        .ok_or("read qualification sample count exceeds usize")?;
    if samples.len() != expected_samples {
        return Err(format!(
            "read qualification requires {expected_samples} base samples, got {}",
            samples.len()
        )
        .into());
    }
    for (row, sample) in samples.iter().enumerate() {
        let expected_writer = row / shape.operations;
        let expected_operation = row % shape.operations;
        if sample.writer != expected_writer || sample.operation != expected_operation {
            return Err(
                "read qualification base samples are not in canonical writer/operation order"
                    .into(),
            );
        }
        read_qualification_query_ordinal(sample, shape.operations, shape.records_per_operation)?;
    }
    let query_samples = if hedge_qualification {
        read_hedge_query_positions(
            shape.writers,
            shape.operations,
            read_writer,
            shape.query_count,
        )?
        .into_iter()
        .map(|position| samples[position].clone())
        .collect::<Vec<_>>()
    } else {
        (0..shape.query_count)
            .map(|query| {
                let position = query * samples.len() / shape.query_count;
                samples[position].clone()
            })
            .collect::<Vec<_>>()
    };
    let required_dataset_rows = query_samples.iter().try_fold(0_usize, |rows, sample| {
        read_qualification_query_ordinal(sample, shape.operations, shape.records_per_operation)
            .map(|ordinal| rows.max(ordinal.saturating_add(1)))
    })?;
    let input_vectors = if let Some(dataset) = dataset.as_deref() {
        read_parquet_vectors(dataset, required_dataset_rows, shape.dimensions)?
    } else {
        let seed = cohort_seed(false, shape.writers, shape.repetition);
        (0..required_dataset_rows)
            .map(|ordinal| vector(seed, ordinal as u64, shape.dimensions))
            .collect()
    };

    fs::create_dir_all(&output)?;
    let query_trace = if hedge_qualification {
        env::var_os("BORSUK_GROUP_COMMIT_READ_STORAGE_TRACE")
            .map(StorageAccessTrace::create)
            .transpose()?
    } else {
        None
    };
    if let Some(trace) = query_trace.as_ref() {
        install_storage_access_trace(trace.clone())?;
    }
    let mut index = if hedge_qualification {
        open_read_hedge_index(&uri, shape.stripe_bytes, hedge_after_ms)?
    } else {
        open_benchmark_index_with_stripe(&uri, shape.stripe_bytes)?
    };
    if let Some(trace) = query_trace.as_ref() {
        trace.reset()?;
    }
    let reads = measure_reads(
        &mut index,
        &query_samples,
        &input_vectors,
        ReadConfig {
            operations: shape.operations,
            records_per_operation: shape.records_per_operation,
            query_count: shape.query_count,
            diagnostic_protocol: false,
            max_read_segments: shape.max_read_segments,
            refresh_before_each: false,
        },
    )?;
    if reads.hits != shape.query_count {
        return Err(format!(
            "read qualification inserted-ID recall failed: {}/{}",
            reads.hits, shape.query_count
        )
        .into());
    }
    if reads.requests.puts != 0 || reads.requests.deletes != 0 {
        return Err("read qualification performed a write-like storage request".into());
    }
    if hedge_qualification && reads.disk_cache_bytes != 0 {
        return Err("read hedge qualification observed disk-cache bytes".into());
    }
    if hedge_qualification {
        write_read_hedge_samples(&output.join("reads.csv"), &reads.samples)?;
    } else {
        write_read_samples(&output.join("reads.csv"), &reads.samples)?;
    }
    let read_p50_ms = percentile(&reads.latencies, 0.50);
    let read_p95_ms = percentile(&reads.latencies, 0.95);
    let recall = reads.hits as f64 / shape.query_count as f64;
    let mut summary = BufWriter::new(File::create(output.join("summary.csv"))?);
    if hedge_qualification {
        let hedge_after_label =
            hedge_after_ms.map_or_else(|| "none".to_string(), |ms| ms.to_string());
        writeln!(
            summary,
            "protocol_kind,source_sha256,manifest_sha256,base_source_sha256,base_manifest_sha256,base_samples_sha256,dataset_sha256,base_cell,index_uri,repetition,order_position,writers,operations_per_writer,records_per_operation,dimensions,read_writer,stripe_bytes,hedge_after_ms,cache_enabled,queries,max_read_segments,inserted_id_recall_at_10,read_p50_ms,read_p95_ms,read_storage_requests,read_storage_gets,read_storage_puts,read_storage_deletes,read_storage_heads,read_storage_lists,read_bytes,read_disk_cache_bytes,read_backing_bytes,read_segments_searched"
        )?;
        writeln!(
            summary,
            "{},{source_sha},{manifest_sha},{base_source_sha},{base_manifest_sha},{base_samples_sha},{dataset_sha},{base_cell},{uri},{},{order_position},{},{},{},{},{read_writer},{},{hedge_after_label},false,{},{},{recall:.9},{read_p50_ms:.9},{read_p95_ms:.9},{},{},{},{},{},{},{},{},{},{}",
            if structural_smoke {
                "structural-smoke"
            } else if hedge_after_ms.is_some() {
                "range-hedge-candidate"
            } else {
                "range-hedge-control"
            },
            shape.repetition,
            shape.writers,
            shape.operations,
            shape.records_per_operation,
            shape.dimensions,
            shape.stripe_bytes,
            shape.query_count,
            shape.max_read_segments,
            reads.requests.total(),
            reads.requests.gets,
            reads.requests.puts,
            reads.requests.deletes,
            reads.requests.heads,
            reads.requests.lists,
            reads.bytes,
            reads.disk_cache_bytes,
            reads.backing_bytes,
            reads.segments_searched,
        )?;
    } else {
        writeln!(
            summary,
            "protocol_kind,source_sha256,manifest_sha256,base_source_sha256,base_manifest_sha256,base_samples_sha256,dataset_sha256,base_cell,index_uri,repetition,order_position,stripe_bytes,queries,inserted_id_recall_at_10,read_p50_ms,read_p95_ms,read_storage_requests,read_storage_gets,read_storage_puts,read_storage_deletes,read_storage_heads,read_storage_lists,read_bytes,read_segments_searched"
        )?;
        writeln!(
            summary,
            "{},{source_sha},{manifest_sha},{base_source_sha},{base_manifest_sha},{base_samples_sha},{dataset_sha},{base_cell},{uri},{},{order_position},{},{},{recall:.9},{read_p50_ms:.9},{read_p95_ms:.9},{},{},{},{},{},{},{},{}",
            if structural_smoke {
                "structural-smoke"
            } else if confirmation {
                "stripe-confirmation"
            } else {
                "production-diagnostic"
            },
            shape.repetition,
            shape.stripe_bytes,
            shape.query_count,
            reads.requests.total(),
            reads.requests.gets,
            reads.requests.puts,
            reads.requests.deletes,
            reads.requests.heads,
            reads.requests.lists,
            reads.bytes,
            reads.segments_searched,
        )?;
    }
    summary.flush()?;
    let cache_dir_label = cache_dir
        .as_deref()
        .map_or_else(|| "none".to_string(), |path| path.display().to_string());
    let hedge_after_label = hedge_after_ms.map_or_else(|| "none".to_string(), |ms| ms.to_string());
    fs::write(
        output.join("environment.txt"),
        format!(
            "source_sha256={source_sha}\nmanifest_sha256={manifest_sha}\nbase_source_sha256={base_source_sha}\nbase_manifest_sha256={base_manifest_sha}\nbase_samples_sha256={base_samples_sha}\ndataset_sha256={dataset_sha}\nbase_cell={base_cell}\nindex_uri={uri}\ncache_dir={cache_dir_label}\ncache_enabled={}\nrepetition={}\nwriters={}\noperations_per_writer={}\nrecords_per_operation={}\ndimensions={}\nread_writer={read_writer}\nqueries={}\nmax_read_segments={}\nstripe_bytes={}\nhedge_after_ms={hedge_after_label}\norder_position={order_position}\n",
            !hedge_qualification,
            shape.repetition,
            shape.writers,
            shape.operations,
            shape.records_per_operation,
            shape.dimensions,
            shape.query_count,
            shape.max_read_segments,
            shape.stripe_bytes,
        ),
    )?;
    fs::write(
        output.join(if hedge_qualification {
            "READ_HEDGE_QUALIFICATION_COMPLETE"
        } else {
            "READ_QUALIFICATION_COMPLETE"
        }),
        b"complete\n",
    )?;
    Ok(())
}

fn main() -> BenchResult<()> {
    let protocol =
        env::var("BORSUK_GROUP_COMMIT_PROTOCOL").unwrap_or_else(|_| "diagnostic".to_string());
    if matches!(
        protocol.as_str(),
        "read-qualification" | "read-stripe-confirmation" | "read-hedge-qualification"
    ) {
        return run_read_qualification(&protocol);
    }
    let uri = required("BORSUK_GROUP_COMMIT_INDEX_URI")?;
    let output = PathBuf::from(required("BORSUK_GROUP_COMMIT_OUTPUT")?);
    let source_sha = required("BORSUK_SOURCE_SHA256")?;
    let manifest_sha = required("BORSUK_GROUP_COMMIT_MANIFEST_SHA256")?;
    let writers: usize = number("BORSUK_GROUP_COMMIT_WRITERS")?;
    let writer_instance_count: usize = number("BORSUK_GROUP_COMMIT_WRITER_INSTANCES")?;
    let operations: usize = number("BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER")?;
    let dimensions: usize = number("BORSUK_GROUP_COMMIT_DIMENSIONS")?;
    let max_delay_ms: u64 = number("BORSUK_GROUP_COMMIT_MAX_DELAY_MS")?;
    let max_records: usize = number("BORSUK_GROUP_COMMIT_MAX_RECORDS")?;
    let realistic_protocol = matches!(protocol.as_str(), "scalability" | "local");
    let worker_lanes = match protocol.as_str() {
        "scalability" | "local" => number("BORSUK_GROUP_COMMIT_WORKER_LANES")?,
        "diagnostic" => 8,
        _ => 1,
    };
    let pipeline_depth = if realistic_protocol {
        number("BORSUK_GROUP_COMMIT_PIPELINE_DEPTH")?
    } else {
        1
    };
    let records_per_operation = if realistic_protocol || protocol == "smoke" {
        number("BORSUK_GROUP_COMMIT_RECORDS_PER_OPERATION")?
    } else {
        1
    };
    if pipeline_depth == 0 {
        return Err("group-commit pipeline depth must be positive".into());
    }
    if records_per_operation == 0 {
        return Err("group-commit records per operation must be positive".into());
    }
    validate_writer_topology(
        writers,
        writer_instance_count,
        worker_lanes,
        usize::from(GROUP_COMMIT_STRIPE_COUNT),
    )?;
    let (_cell_count, repetition, performance_gate) = match protocol.as_str() {
        "diagnostic" => {
            if writers != 8
                || writer_instance_count != 1
                || operations != 20
                || dimensions != 96
                || max_delay_ms != 5
                || max_records != 64
                || worker_lanes != 8
            {
                return Err("group-commit cell differs from the frozen diagnostic".into());
            }
            (2_000, 0, None)
        }
        "scalability" => {
            let cell_count: usize = number("BORSUK_GROUP_COMMIT_CELL_COUNT")?;
            let repetition: usize = number("BORSUK_GROUP_COMMIT_REPETITION")?;
            if !matches!(cell_count, 2_000 | 16_000)
                || !matches!(writers, 1 | 8 | 32)
                || writer_instance_count != writers
                || !(1..=5).contains(&repetition)
                || operations != 1_000
                || dimensions != 768
                || max_delay_ms != 5
                || max_records != 1_024
                || !matches!(worker_lanes, 1 | 2 | 4 | 8)
                || pipeline_depth != 4
                || !matches!(records_per_operation, 1 | 16)
            {
                return Err(
                    "group-commit cell differs from the frozen scalability protocol".into(),
                );
            }
            let max_p95_ms: f64 = number("BORSUK_GROUP_COMMIT_MAX_P95_MS")?;
            let min_records_per_second: f64 = number("BORSUK_GROUP_COMMIT_MIN_RECORDS_PER_SECOND")?;
            let min_end_to_end_records_per_second: f64 =
                number("BORSUK_GROUP_COMMIT_MIN_END_TO_END_RECORDS_PER_SECOND")?;
            let max_read_p95_ms: f64 = number("BORSUK_GROUP_COMMIT_MAX_READ_P95_MS")?;
            let min_inserted_id_recall_at_10: f64 =
                number("BORSUK_GROUP_COMMIT_MIN_INSERTED_ID_RECALL_AT_10")?;
            if max_p95_ms <= 0.0
                || min_records_per_second < 0.0
                || min_end_to_end_records_per_second < 0.0
                || max_read_p95_ms <= 0.0
                || !(0.0..=1.0).contains(&min_inserted_id_recall_at_10)
            {
                return Err("production performance thresholds must be positive".into());
            }
            validate_throughput_concurrency(
                writers,
                pipeline_depth,
                records_per_operation,
                min_records_per_second.max(min_end_to_end_records_per_second),
                max_p95_ms,
            )?;
            (
                cell_count,
                repetition,
                Some(PerformanceThresholds {
                    max_p95_ms,
                    min_records_per_second,
                    min_end_to_end_records_per_second,
                    max_read_p95_ms,
                    min_inserted_id_recall_at_10,
                }),
            )
        }
        "local" => {
            let cell_count: usize = number("BORSUK_GROUP_COMMIT_CELL_COUNT")?;
            let repetition: usize = number("BORSUK_GROUP_COMMIT_REPETITION")?;
            if cell_count != 2_000
                || !matches!(writers, 1 | 8 | 32)
                || writer_instance_count > writers
                || repetition != 1
                || operations != 32
                || dimensions != 768
                || max_delay_ms != 5
                || max_records != 1_024
                || !matches!(worker_lanes, 1 | 2 | 4 | 8)
                || pipeline_depth != 4
                || !matches!(records_per_operation, 1 | 16)
            {
                return Err(
                    "group-commit cell differs from the bounded local qualification".into(),
                );
            }
            (cell_count, repetition, None)
        }
        "smoke" => {
            let cell_count: usize = number("BORSUK_GROUP_COMMIT_CELL_COUNT")?;
            let repetition: usize = number("BORSUK_GROUP_COMMIT_REPETITION")?;
            if cell_count != 2_000
                || !matches!(writers, 1 | 8)
                || writer_instance_count != writers
                || repetition != 1
                || operations != 2
                || dimensions != 768
                || max_delay_ms != 5
                || max_records != 1_024
                || !matches!(records_per_operation, 1 | 16)
            {
                return Err("group-commit cell differs from the structural smoke".into());
            }
            (cell_count, repetition, None)
        }
        _ => return Err(format!("unknown group-commit protocol {protocol:?}").into()),
    };
    let diagnostic_protocol = protocol == "diagnostic";
    let vector_seed = cohort_seed(diagnostic_protocol, writers, repetition);
    if env::var("BORSUK_GROUP_COMMIT_ROLE").as_deref() == Ok("writer-process") {
        return run_process_worker(
            &uri,
            &protocol,
            operations,
            dimensions,
            max_delay_ms,
            max_records,
            worker_lanes,
            pipeline_depth,
            records_per_operation,
            vector_seed,
        );
    }
    if output.exists() {
        return Err(format!("refusing to replace output {}", output.display()).into());
    }
    fs::create_dir_all(&output)?;

    let dataset_sha = if realistic_protocol {
        required("BORSUK_GROUP_COMMIT_DATASET_SHA256")?
    } else {
        String::new()
    };
    let input_vectors = if realistic_protocol {
        let dataset = PathBuf::from(required("BORSUK_GROUP_COMMIT_DATASET")?);
        if !dataset.is_dir()
            || dataset_sha.len() != 64
            || !dataset_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("invalid group-commit dataset identity".into());
        }
        read_parquet_vectors(
            &dataset,
            writers * operations * records_per_operation,
            dimensions,
        )?
    } else {
        (0..writers * operations * records_per_operation)
            .map(|ordinal| vector(vector_seed, ordinal as u64, dimensions))
            .collect()
    };
    // Production invariant: dataset vectors must be decoded before durable timing.
    let input_vectors = Arc::new(input_vectors);

    let process_execution = env::var("BORSUK_GROUP_COMMIT_EXECUTION").as_deref() == Ok("processes");
    let (writer_instances, mut samples, elapsed_ms) = if process_execution {
        let (samples, elapsed_ms) = run_process_coordinator(&output, writers)?;
        (Vec::new(), samples, elapsed_ms)
    } else {
        let writer_instances = (0..writer_instance_count)
            .map(|_| {
                GroupCommitWriter::new(
                    open_benchmark_index(&uri)?,
                    GroupCommitConfig {
                        max_delay: Duration::from_millis(max_delay_ms),
                        max_records,
                        worker_lanes,
                    },
                )
            })
            .collect::<borsuk::Result<Vec<_>>>()?;
        let barrier = Arc::new(Barrier::new(writers));
        let samples = Arc::new(Mutex::new(Vec::with_capacity(writers * operations)));
        let started = Instant::now();
        let handles = (0..writers)
            .map(|writer_ordinal| {
                let writer_instance = writer_ordinal % writer_instances.len();
                let writer = writer_instances[writer_instance].clone();
                let barrier = Arc::clone(&barrier);
                let samples = Arc::clone(&samples);
                let input_vectors = Arc::clone(&input_vectors);
                thread::spawn(move || -> BenchResult<()> {
                    barrier.wait();
                    let mut local = Vec::with_capacity(operations);
                    let mut pending = VecDeque::<PendingAppend>::with_capacity(pipeline_depth);
                    for operation in 0..operations {
                        let first_ordinal =
                            (writer_ordinal * operations + operation) * records_per_operation;
                        let record_ids = (0..records_per_operation)
                            .map(|batch| {
                                let ordinal = first_ordinal + batch;
                                if diagnostic_protocol {
                                    format!(
                                        "group-w{writer_ordinal:02}-o{operation:03}-b{batch:03}"
                                    )
                                } else {
                                    production_record_id(ordinal)
                                }
                            })
                            .collect::<Vec<_>>();
                        let records = record_ids
                            .iter()
                            .enumerate()
                            .map(|(batch, id)| {
                                VectorRecord::new(
                                    id.clone(),
                                    input_vectors[first_ordinal + batch].clone(),
                                )
                            })
                            .collect();
                        let started = Instant::now();
                        let ticket = writer.append_async(records)?;
                        pending.push_back(PendingAppend {
                            operation,
                            record_ids,
                            started,
                            ticket,
                        });
                        if pending.len() >= pipeline_depth {
                            let completed = pending.pop_front().expect("non-empty pipeline");
                            let mut sample = finish_sample(writer_ordinal, completed)?;
                            sample.writer_instance = writer_instance;
                            local.push(sample);
                        }
                    }
                    while let Some(completed) = pending.pop_front() {
                        let mut sample = finish_sample(writer_ordinal, completed)?;
                        sample.writer_instance = writer_instance;
                        local.push(sample);
                    }
                    samples.lock().unwrap().extend(local);
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle
                .join()
                .map_err(|_| "group commit writer panicked")??;
        }
        let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let samples = Arc::try_unwrap(samples)
            .map_err(|_| "sample owners remain")?
            .into_inner()
            .unwrap();
        (writer_instances, samples, elapsed_ms)
    };
    fs::write(output.join("INGEST_COMPLETE"), b"complete\n")?;
    samples.sort_by_key(|sample| (sample.writer, sample.operation));
    let max_read_segments = if diagnostic_protocol { 0 } else { 4 };
    let recall_queries = if diagnostic_protocol {
        20
    } else if realistic_protocol {
        number("BORSUK_GROUP_COMMIT_READ_QUERIES")?
    } else {
        1
    }
    .min(samples.len());
    let mut active_tail_index = open_benchmark_index(&uri)?;
    let active_tail_reads = measure_reads(
        &mut active_tail_index,
        &samples,
        &input_vectors,
        ReadConfig {
            operations,
            records_per_operation,
            query_count: recall_queries,
            diagnostic_protocol,
            max_read_segments,
            refresh_before_each: true,
        },
    )?;
    if active_tail_reads.hits != recall_queries {
        let missed_samples = active_tail_reads
            .samples
            .iter()
            .filter(|sample| !sample.contains_record_id)
            .take(8)
            .collect::<Vec<_>>();
        let missed_ids = missed_samples
            .iter()
            .map(|sample| sample.record_id.as_str())
            .collect::<Vec<_>>();
        let point_states = active_tail_index.get_records(&missed_ids)?;
        let misses = missed_samples
            .into_iter()
            .zip(point_states)
            .map(|(sample, point)| {
                format!(
                    "{}:{}->{}({})",
                    sample.query,
                    sample.record_id,
                    sample.hit_id,
                    if point.is_some() {
                        "point-visible"
                    } else {
                        "point-missing"
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "active-tail inserted-ID recall gate failed: {}/{} hits; first misses: {misses}",
            active_tail_reads.hits, recall_queries
        )
        .into());
    }
    write_read_samples(
        &output.join("active-tail-reads.csv"),
        &active_tail_reads.samples,
    )?;
    fs::write(
        output.join("ACTIVE_TAIL_READ_QUALIFICATION_COMPLETE"),
        b"complete\n",
    )?;
    let drain_started = Instant::now();
    if writer_instances.is_empty() {
        let maintenance = GroupCommitWriter::new(
            open_benchmark_index(&uri)?,
            GroupCommitConfig {
                max_delay: Duration::from_millis(max_delay_ms),
                max_records,
                worker_lanes,
            },
        )?;
        maintenance.drain()?;
    } else {
        for writer in &writer_instances {
            writer.drain()?;
        }
    }
    let drain_ms = drain_started.elapsed().as_secs_f64() * 1_000.0;
    let total_record_count = writers * operations * records_per_operation;
    let end_to_end_records_per_second =
        total_record_count as f64 / ((elapsed_ms + drain_ms) / 1_000.0);
    fs::write(output.join("DRAIN_COMPLETE"), b"complete\n")?;
    drop(writer_instances);

    let mut groups = BTreeMap::<(usize, u64), (usize, u64, RequestCounts)>::new();
    for sample in &samples {
        for receipt in &sample.lane_receipts {
            let evidence = (
                receipt.committed_records,
                receipt.acknowledgement_bytes,
                receipt.requests,
            );
            match groups.insert((receipt.commit_lane, receipt.commit_sequence), evidence) {
                Some(previous) if previous != evidence => {
                    return Err("callers disagree about shared lane-group evidence".into());
                }
                _ => {}
            }
        }
    }
    let request_totals =
        groups
            .values()
            .fold(RequestCounts::default(), |mut totals, (_, _, requests)| {
                totals.gets += requests.gets;
                totals.puts += requests.puts;
                totals.deletes += requests.deletes;
                totals.heads += requests.heads;
                totals.lists += requests.lists;
                totals
            });
    let total_requests = request_totals.total();
    let committed_records = groups
        .values()
        .map(|(records, _, _)| *records)
        .sum::<usize>();
    let acknowledgement_bytes = groups
        .values()
        .map(|(_, bytes, _)| *bytes)
        .collect::<Vec<_>>();
    let max_acknowledgement_bytes = acknowledgement_bytes.iter().copied().max().unwrap_or(0);
    let total_acknowledgement_bytes = acknowledgement_bytes.iter().sum::<u64>();
    if committed_records != total_record_count {
        return Err("group record totals do not reconcile with caller samples".into());
    }

    let mut reopened = open_benchmark_index(&uri)?;
    let point_ids = samples
        .iter()
        .flat_map(|sample| sample.record_ids.iter().map(String::as_str))
        .collect::<Vec<_>>();
    let point_records = reopened.get_records(&point_ids)?;
    let mut visible = 0_usize;
    let mut mismatches = Vec::new();
    for (ordinal, point_record) in point_records.into_iter().enumerate() {
        let expected = &input_vectors[ordinal];
        if point_record
            .as_ref()
            .is_some_and(|(stored, _)| stored == expected)
        {
            visible += 1;
        } else if mismatches.len() < 8 {
            mismatches.push(format!(
                "{}:{}",
                point_ids[ordinal],
                if point_record.is_some() {
                    "wrong-vector"
                } else {
                    "missing"
                }
            ));
        }
    }
    if visible != total_record_count {
        return Err(format!(
            "post-reopen point visibility gate failed: {visible}/{total_record_count} exact; first mismatches: {}",
            mismatches.join(", ")
        )
        .into());
    }
    fs::write(output.join("POINT_VISIBILITY_COMPLETE"), b"complete\n")?;
    let reads = measure_reads(
        &mut reopened,
        &samples,
        &input_vectors,
        ReadConfig {
            operations,
            records_per_operation,
            query_count: recall_queries,
            diagnostic_protocol,
            max_read_segments,
            refresh_before_each: false,
        },
    )?;
    if reads.hits != recall_queries {
        let misses = reads
            .samples
            .iter()
            .filter(|sample| !sample.contains_record_id)
            .take(8)
            .map(|sample| format!("{}:{}->{}", sample.query, sample.record_id, sample.hit_id))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "post-reopen exact recall gate failed: {}/{} hits; first misses: {misses}",
            reads.hits, recall_queries
        )
        .into());
    }
    fs::write(output.join("READ_QUALIFICATION_COMPLETE"), b"complete\n")?;

    let latencies = samples
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    let p50_ms = percentile(&latencies, 0.50);
    let p95_ms = percentile(&latencies, 0.95);
    let operations_per_second = samples.len() as f64 / (elapsed_ms / 1_000.0);
    let records_per_second = total_record_count as f64 / (elapsed_ms / 1_000.0);
    let vector_mib_per_second = vector_mib_per_second(records_per_second, dimensions);
    let read_p50_ms = percentile(&reads.latencies, 0.50);
    let read_p95_ms = percentile(&reads.latencies, 0.95);
    let active_tail_read_p50_ms = percentile(&active_tail_reads.latencies, 0.50);
    let active_tail_read_p95_ms = percentile(&active_tail_reads.latencies, 0.95);
    let inserted_id_recall_at_10 = reads.hits as f64 / recall_queries as f64;
    let mut summary = BufWriter::new(File::create(output.join("summary.csv"))?);
    writeln!(
        summary,
        "source_sha256,dataset_sha256,manifest_sha256,writers,writer_instances,operations,records_per_operation,pipeline_depth,worker_lanes,records,groups,mean_group_records,elapsed_ms,drain_ms,end_to_end_records_per_second,p50_ms,p95_ms,operations_per_second,records_per_second,vector_mib_per_second,storage_requests,storage_gets,storage_puts,storage_heads,requests_per_record,total_acknowledgement_bytes,max_acknowledgement_bytes,visible_records,recall_queries,max_read_segments,inserted_id_recall_at_10,active_tail_read_p50_ms,active_tail_read_p95_ms,read_p50_ms,read_p95_ms,read_storage_requests,read_storage_gets,read_storage_puts,read_storage_deletes,read_storage_heads,read_storage_lists,read_bytes,read_segments_searched"
    )?;
    writeln!(
        summary,
        "{source_sha},{dataset_sha},{manifest_sha},{writers},{},{operations},{records_per_operation},{pipeline_depth},{worker_lanes},{},{},{:.9},{elapsed_ms:.9},{drain_ms:.9},{end_to_end_records_per_second:.9},{:.9},{:.9},{operations_per_second:.9},{:.9},{vector_mib_per_second:.9},{total_requests},{},{},{},{:.9},{total_acknowledgement_bytes},{max_acknowledgement_bytes},{visible},{recall_queries},{max_read_segments},{:.9},{active_tail_read_p50_ms:.9},{active_tail_read_p95_ms:.9},{:.9},{:.9},{},{},{},{},{},{},{},{}",
        writer_instance_count,
        total_record_count,
        groups.len(),
        total_record_count as f64 / groups.len() as f64,
        p50_ms,
        p95_ms,
        records_per_second,
        request_totals.gets,
        request_totals.puts,
        request_totals.heads,
        total_requests as f64 / total_record_count as f64,
        inserted_id_recall_at_10,
        read_p50_ms,
        read_p95_ms,
        reads.requests.total(),
        reads.requests.gets,
        reads.requests.puts,
        reads.requests.deletes,
        reads.requests.heads,
        reads.requests.lists,
        reads.bytes,
        reads.segments_searched,
    )?;
    write_samples(&output.join("samples.csv"), &samples)?;
    write_read_samples(&output.join("reads.csv"), &reads.samples)?;
    summary.flush()?;
    if let Some(thresholds) = performance_gate {
        let failures = production_performance_gate_failures(
            PerformanceObservation {
                p95_ms,
                records_per_second,
                end_to_end_records_per_second,
                read_p95_ms,
                active_tail_read_p95_ms,
                inserted_id_recall_at_10,
            },
            thresholds,
        );
        if failures.is_empty() {
            File::create(output.join("PRODUCTION_PERFORMANCE_GATE_COMPLETE"))?;
        } else {
            for marker in failures {
                File::create(output.join(marker))?;
            }
            File::create(output.join("PRODUCTION_PERFORMANCE_GATE_FAILED"))?;
            return Err("production performance gate failed".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_hedge_qualification_accepts_only_the_preregistered_shape() {
        const MIB: usize = 1024 * 1024;
        let shape = ReadHedgeQualificationShape {
            read: ReadQualificationShape {
                writers: 8,
                operations: 1_000,
                records_per_operation: 16,
                dimensions: 768,
                query_count: 500,
                max_read_segments: 4,
                stripe_bytes: MIB,
                repetition: 3,
            },
            read_writer: 0,
            hedge_after_ms: Some(75),
        };
        validate_read_hedge_qualification_shape(shape, false).unwrap();
        validate_read_hedge_qualification_shape(
            ReadHedgeQualificationShape {
                hedge_after_ms: None,
                ..shape
            },
            false,
        )
        .unwrap();
        validate_read_hedge_qualification_shape(
            ReadHedgeQualificationShape {
                hedge_after_ms: Some(35),
                ..shape
            },
            false,
        )
        .unwrap();
        validate_read_hedge_qualification_shape(
            ReadHedgeQualificationShape {
                hedge_after_ms: Some(20),
                ..shape
            },
            false,
        )
        .unwrap();

        for invalid in [
            ReadHedgeQualificationShape {
                read_writer: 1,
                ..shape
            },
            ReadHedgeQualificationShape {
                hedge_after_ms: Some(74),
                ..shape
            },
            ReadHedgeQualificationShape {
                read: ReadQualificationShape {
                    query_count: 499,
                    ..shape.read
                },
                ..shape
            },
            ReadHedgeQualificationShape {
                read: ReadQualificationShape {
                    stripe_bytes: 2 * MIB,
                    ..shape.read
                },
                ..shape
            },
            ReadHedgeQualificationShape {
                read: ReadQualificationShape {
                    repetition: 0,
                    ..shape.read
                },
                ..shape
            },
        ] {
            assert!(validate_read_hedge_qualification_shape(invalid, false).is_err());
        }
    }

    #[test]
    fn read_hedge_qualification_spreads_writer_zero_without_crossing_writers() {
        let positions = read_hedge_query_positions(8, 1_000, 0, 500).unwrap();
        assert_eq!(positions.len(), 500);
        assert_eq!(positions.first(), Some(&0));
        assert_eq!(positions.last(), Some(&998));
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(positions.iter().all(|position| *position < 1_000));
        assert_eq!(positions[1], 2);
        assert_eq!(positions[250], 500);
    }

    #[test]
    fn read_hedge_qualification_rejects_cache_and_unregistered_delay_text() {
        const MIB: usize = 1024 * 1024;
        let shape = ReadHedgeQualificationShape {
            read: ReadQualificationShape {
                writers: 8,
                operations: 1_000,
                records_per_operation: 16,
                dimensions: 768,
                query_count: 500,
                max_read_segments: 4,
                stripe_bytes: MIB,
                repetition: 1,
            },
            read_writer: 0,
            hedge_after_ms: None,
        };

        validate_read_hedge_qualification_inputs(shape, false, false).unwrap();
        assert!(validate_read_hedge_qualification_inputs(shape, false, true).is_err());
        assert_eq!(parse_read_hedge_after_ms("none", false).unwrap(), None);
        assert_eq!(parse_read_hedge_after_ms("20", false).unwrap(), Some(20));
        assert_eq!(parse_read_hedge_after_ms("35", false).unwrap(), Some(35));
        assert_eq!(parse_read_hedge_after_ms("75", false).unwrap(), Some(75));
        assert_eq!(parse_read_hedge_after_ms("5", true).unwrap(), Some(5));
        for invalid in ["", "0", "5", "74", "076", "75.0", "none "] {
            assert!(parse_read_hedge_after_ms(invalid, false).is_err());
        }
    }

    #[test]
    fn read_hedge_qualification_alternates_control_and_candidate_order() {
        assert_eq!(read_hedge_order_position(1, None, false).unwrap(), 0);
        assert_eq!(read_hedge_order_position(1, Some(20), false).unwrap(), 1);
        assert_eq!(read_hedge_order_position(2, Some(20), false).unwrap(), 0);
        assert_eq!(read_hedge_order_position(1, Some(35), false).unwrap(), 1);
        assert_eq!(read_hedge_order_position(2, Some(35), false).unwrap(), 0);
        assert_eq!(read_hedge_order_position(1, Some(75), false).unwrap(), 1);
        assert_eq!(read_hedge_order_position(2, Some(75), false).unwrap(), 0);
        assert_eq!(read_hedge_order_position(2, None, false).unwrap(), 1);
        assert_eq!(read_hedge_order_position(1, None, true).unwrap(), 0);
        assert_eq!(read_hedge_order_position(1, Some(5), true).unwrap(), 1);
        assert!(read_hedge_order_position(0, None, false).is_err());
        assert!(read_hedge_order_position(1, Some(5), false).is_err());
    }

    #[test]
    fn read_hedge_qualification_opens_uncached_with_the_selected_delay() {
        let options = read_hedge_open_options(1024 * 1024, Some(75));
        assert_eq!(options.cache_dir, None);
        assert_eq!(options.global_pq_prefetch_stripe_bytes, 1024 * 1024);
        assert_eq!(
            options.global_pq_slow_read_hedge_after,
            Some(Duration::from_millis(75))
        );

        let control = read_hedge_open_options(1024 * 1024, None);
        assert_eq!(control.cache_dir, None);
        assert_eq!(control.global_pq_slow_read_hedge_after, None);
    }

    #[test]
    fn read_hedge_qualification_csv_preserves_physical_byte_telemetry() {
        let path = env::temp_dir().join(format!(
            "borsuk-read-hedge-samples-{}.csv",
            std::process::id()
        ));
        let sample = ReadSample {
            query: 0,
            record_id: "group-o00000000".to_string(),
            hit_id: "group-o00000000".to_string(),
            hit_ids: "group-o00000000|neighbor-1|neighbor-2".to_string(),
            contains_record_id: true,
            latency_ms: 12.5,
            requests: RequestCounts {
                gets: 3,
                ..RequestCounts::default()
            },
            bytes_read: 100,
            disk_cache_bytes_read: 123,
            backing_bytes_read: 456,
            segments_searched: 4,
            global_base_approximate_us: 1,
            global_base_exact_rerank_us: 2,
            global_delta_approximate_us: 3,
            global_delta_exact_rerank_us: 4,
            global_delta_wait_us: 5,
        };

        write_read_hedge_samples(&path, &[sample]).unwrap();
        let csv = fs::read_to_string(&path).unwrap();
        fs::remove_file(&path).unwrap();
        assert!(csv.starts_with(
            "query,record_id,hit_id,hit_ids,contains_record_id,latency_ms,requests,gets,puts,deletes,heads,lists,bytes_read,disk_cache_bytes_read,backing_bytes_read,"
        ));
        assert!(csv.contains(",100,123,456,4,1,2,3,4,5\n"));
    }

    #[test]
    fn read_qualification_accepts_only_the_preregistered_shape() {
        let shape = ReadQualificationShape {
            writers: 8,
            operations: 1_000,
            records_per_operation: 16,
            dimensions: 768,
            query_count: 100,
            max_read_segments: 4,
            stripe_bytes: 2 * 1024 * 1024,
            repetition: 3,
        };
        validate_read_qualification_shape(shape, false).unwrap();

        for invalid in [
            ReadQualificationShape {
                stripe_bytes: 0,
                ..shape
            },
            ReadQualificationShape {
                stripe_bytes: 3 * 1024 * 1024,
                ..shape
            },
            ReadQualificationShape {
                repetition: 0,
                ..shape
            },
            ReadQualificationShape {
                repetition: 6,
                ..shape
            },
            ReadQualificationShape {
                query_count: 20,
                ..shape
            },
        ] {
            assert!(validate_read_qualification_shape(invalid, false).is_err());
        }
    }

    #[test]
    fn read_qualification_smoke_is_small_but_keeps_the_same_read_policy() {
        validate_read_qualification_shape(
            ReadQualificationShape {
                writers: 2,
                operations: 2,
                records_per_operation: 1,
                dimensions: 8,
                query_count: 4,
                max_read_segments: 4,
                stripe_bytes: 4 * 1024 * 1024,
                repetition: 1,
            },
            true,
        )
        .unwrap();
    }

    #[test]
    fn read_qualification_arm_order_matches_the_preregistration() {
        const MIB: usize = 1024 * 1024;
        assert_eq!(read_qualification_order_position(1, MIB).unwrap(), 0);
        assert_eq!(read_qualification_order_position(1, 2 * MIB).unwrap(), 1);
        assert_eq!(read_qualification_order_position(2, 2 * MIB).unwrap(), 0);
        assert_eq!(read_qualification_order_position(3, MIB).unwrap(), 1);
        assert!(read_qualification_order_position(0, MIB).is_err());
        assert!(read_qualification_order_position(1, 3 * MIB).is_err());
    }

    #[test]
    fn read_confirmation_accepts_only_the_preregistered_shape() {
        const MIB: usize = 1024 * 1024;
        let shape = ReadQualificationShape {
            writers: 8,
            operations: 1_000,
            records_per_operation: 16,
            dimensions: 768,
            query_count: 500,
            max_read_segments: 4,
            stripe_bytes: 4 * MIB,
            repetition: 5,
        };
        validate_read_confirmation_shape(shape, false).unwrap();
        validate_read_confirmation_shape(
            ReadQualificationShape {
                stripe_bytes: MIB,
                ..shape
            },
            false,
        )
        .unwrap();

        for invalid in [
            ReadQualificationShape {
                stripe_bytes: 2 * MIB,
                ..shape
            },
            ReadQualificationShape {
                query_count: 100,
                ..shape
            },
            ReadQualificationShape {
                query_count: 499,
                ..shape
            },
            ReadQualificationShape {
                repetition: 0,
                ..shape
            },
            ReadQualificationShape {
                repetition: 6,
                ..shape
            },
            ReadQualificationShape {
                max_read_segments: 3,
                ..shape
            },
        ] {
            assert!(validate_read_confirmation_shape(invalid, false).is_err());
        }
    }

    #[test]
    fn read_confirmation_preserves_the_structural_smoke_shape() {
        const MIB: usize = 1024 * 1024;
        let smoke = ReadQualificationShape {
            writers: 2,
            operations: 2,
            records_per_operation: 1,
            dimensions: 8,
            query_count: 4,
            max_read_segments: 4,
            stripe_bytes: MIB,
            repetition: 1,
        };
        validate_read_confirmation_shape(smoke, true).unwrap();
        validate_read_confirmation_shape(
            ReadQualificationShape {
                stripe_bytes: 4 * MIB,
                ..smoke
            },
            true,
        )
        .unwrap();
        assert!(
            validate_read_confirmation_shape(
                ReadQualificationShape {
                    stripe_bytes: 2 * MIB,
                    ..smoke
                },
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn read_confirmation_arm_order_matches_the_preregistration() {
        const MIB: usize = 1024 * 1024;
        assert_eq!(read_confirmation_order_position(1, MIB).unwrap(), 0);
        assert_eq!(read_confirmation_order_position(1, 4 * MIB).unwrap(), 1);
        assert_eq!(read_confirmation_order_position(2, 4 * MIB).unwrap(), 0);
        assert_eq!(read_confirmation_order_position(2, MIB).unwrap(), 1);
        assert_eq!(read_confirmation_order_position(5, MIB).unwrap(), 0);
        assert!(read_confirmation_order_position(0, MIB).is_err());
        assert!(read_confirmation_order_position(6, MIB).is_err());
        assert!(read_confirmation_order_position(1, 2 * MIB).is_err());
    }

    #[test]
    fn read_qualification_reconstructs_the_original_dataset_row() {
        let sample = Sample {
            process_id: 1,
            writer: 3,
            writer_instance: 3,
            operation: 17,
            batch_records: 16,
            record_ids: (0..16)
                .map(|batch| production_record_id((3 * 1_000 + 17) * 16 + batch))
                .collect(),
            latency_ms: 1.0,
            commit_lane: 0,
            commit_sequence: 1,
            committed_records: 16,
            acknowledgement_bytes: 1,
            group_requests: RequestCounts::default(),
            lane_receipts: Vec::new(),
        };

        assert_eq!(
            read_qualification_query_ordinal(&sample, 1_000, 16).unwrap(),
            48_272
        );
        let mut wrong_id = sample.clone();
        wrong_id.record_ids[0] = "wrong".to_string();
        assert!(read_qualification_query_ordinal(&wrong_id, 1_000, 16).is_err());
    }

    #[test]
    fn writer_topology_rejects_thread_only_or_unrepresentable_assignments() {
        validate_writer_topology(8, 8, 1, 8).unwrap();
        validate_writer_topology(32, 8, 1, 8).unwrap();
        validate_writer_topology(32, 32, 1, 64).unwrap();

        assert!(
            validate_writer_topology(8, 0, 1, 8)
                .unwrap_err()
                .to_string()
                .contains("writer instances must be positive")
        );
        assert!(
            validate_writer_topology(8, 9, 1, 16)
                .unwrap_err()
                .to_string()
                .contains("cannot exceed producer writers")
        );
        assert!(
            validate_writer_topology(8, 3, 1, 8)
                .unwrap_err()
                .to_string()
                .contains("must divide producer writers evenly")
        );
        assert!(
            validate_writer_topology(32, 32, 3, 64)
                .unwrap_err()
                .to_string()
                .contains("requires 96 persisted writer stripes")
        );
    }

    #[test]
    fn percentile_is_deterministic() {
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.95), 4.0);
    }

    #[test]
    fn lane_receipt_evidence_round_trips_authenticated_object_checksums() {
        let receipt = GroupCommitLaneReceipt {
            commit_lane: 7,
            commit_sequence: 11,
            lease_epoch: 13,
            records: 17,
            committed_records: 17,
            acknowledgement_bytes: 19,
            extent_checksum: [0xab; 32],
            published_head_checksum: [0xcd; 32],
            requests: RequestCounts {
                gets: 0,
                puts: 2,
                deletes: 0,
                heads: 0,
                lists: 0,
            },
        };

        let encoded = lane_receipts_field(&[receipt]);
        assert_eq!(encoded.split(':').count(), 13);
        assert_eq!(parse_lane_receipts(&encoded).unwrap(), vec![receipt]);
    }

    #[test]
    fn vector_throughput_reports_payload_mib_per_second() {
        assert_eq!(vector_mib_per_second(10_000.0, 768), 29.296875);
    }

    #[test]
    fn throughput_preflight_rejects_scalar_but_accepts_preregistered_bulk_concurrency() {
        let scalar = validate_throughput_concurrency(32, 4, 1, 10_000.0, 200.0)
            .unwrap_err()
            .to_string();
        assert!(scalar.contains("128 outstanding records"), "{scalar}");
        validate_throughput_concurrency(32, 4, 16, 10_000.0, 200.0).unwrap();
    }

    #[test]
    fn scalability_cohorts_pair_cell_counts_but_separate_writer_counts() {
        let one_writer = cohort_seed(false, 1, 3);
        assert_eq!(one_writer, cohort_seed(false, 1, 3));
        assert_ne!(one_writer, cohort_seed(false, 8, 3));
        assert_ne!(one_writer, cohort_seed(false, 1, 4));
        assert_eq!(cohort_seed(true, 1, 0), 76_412_031);
        assert_eq!(cohort_seed(true, 32, 5), 76_412_031);
    }

    #[test]
    fn production_record_ids_are_treatment_independent() {
        assert_eq!(production_record_id(0), "group-o00000000");
        assert_eq!(production_record_id(3_199), "group-o00003199");
    }

    #[test]
    fn production_gate_requires_both_latency_and_scaled_throughput() {
        let thresholds = PerformanceThresholds {
            max_p95_ms: 200.0,
            min_records_per_second: 160.0,
            min_end_to_end_records_per_second: 120.0,
            max_read_p95_ms: 200.0,
            min_inserted_id_recall_at_10: 1.0,
        };
        assert!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 160.0,
                    end_to_end_records_per_second: 120.0,
                    read_p95_ms: 199.999,
                    active_tail_read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 1.0,
                },
                thresholds,
            )
            .is_empty()
        );
        assert_eq!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 200.0,
                    records_per_second: 160.0,
                    end_to_end_records_per_second: 120.0,
                    read_p95_ms: 199.999,
                    active_tail_read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 1.0
                },
                thresholds,
            ),
            vec!["PRODUCTION_WRITE_P95_FAILED"]
        );
        assert_eq!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 159.999,
                    end_to_end_records_per_second: 120.0,
                    read_p95_ms: 199.999,
                    active_tail_read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 1.0
                },
                thresholds,
            ),
            vec!["PRODUCTION_WRITE_THROUGHPUT_FAILED"]
        );
        assert!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 1.0,
                    end_to_end_records_per_second: 120.0,
                    read_p95_ms: 199.999,
                    active_tail_read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 1.0,
                },
                PerformanceThresholds {
                    min_records_per_second: 0.0,
                    ..thresholds
                },
            )
            .is_empty()
        );
        assert_eq!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 160.0,
                    end_to_end_records_per_second: 119.999,
                    read_p95_ms: 199.999,
                    active_tail_read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 1.0,
                },
                thresholds,
            ),
            vec!["PRODUCTION_END_TO_END_THROUGHPUT_FAILED"]
        );
        assert_eq!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 160.0,
                    end_to_end_records_per_second: 120.0,
                    read_p95_ms: 200.0,
                    active_tail_read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 1.0
                },
                thresholds,
            ),
            vec!["PRODUCTION_READ_P95_FAILED"]
        );
        assert_eq!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 160.0,
                    end_to_end_records_per_second: 120.0,
                    read_p95_ms: 199.999,
                    active_tail_read_p95_ms: 200.0,
                    inserted_id_recall_at_10: 1.0,
                },
                thresholds,
            ),
            vec!["PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED"]
        );
        assert_eq!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 160.0,
                    end_to_end_records_per_second: 120.0,
                    read_p95_ms: 199.999,
                    active_tail_read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 0.99
                },
                thresholds,
            ),
            vec!["PRODUCTION_INSERTED_ID_RECALL_AT_10_FAILED"]
        );
    }
}

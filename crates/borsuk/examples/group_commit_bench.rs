//! Bounded group-commit ingest qualification.

use std::{
    collections::{BTreeMap, VecDeque},
    env,
    error::Error,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Barrier, Mutex},
    thread,
    time::{Duration, Instant},
};

use arrow_array::{Array, FixedSizeListArray, Float32Array, LargeListArray, ListArray};
use borsuk::{
    BorsukIndex, GroupCommitConfig, GroupCommitLaneReceipt, GroupCommitTicket, GroupCommitWriter,
    LeafMode, RequestCounts, SearchOptions, VectorRecord,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
struct Sample {
    writer: usize,
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
    contains_record_id: bool,
    latency_ms: f64,
    requests: RequestCounts,
    bytes_read: u64,
    segments_searched: usize,
}

struct ReadMeasurement {
    samples: Vec<ReadSample>,
    latencies: Vec<f64>,
    requests: RequestCounts,
    bytes: u64,
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

struct PendingAppend {
    operation: usize,
    record_ids: Vec<String>,
    started: Instant,
    ticket: GroupCommitTicket,
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

fn lane_receipts_field(receipts: &[GroupCommitLaneReceipt]) -> String {
    receipts
        .iter()
        .map(|receipt| {
            format!(
                "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
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
    let path = dataset.join("train.parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?
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

fn production_record_id(ordinal: usize) -> String {
    format!("group-o{ordinal:08}")
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
        measurement.segments_searched = measurement
            .segments_searched
            .saturating_add(report.segments_searched);
        let hit_id = report
            .hits
            .first()
            .map_or_else(String::new, |hit| hit.id.as_str().to_string());
        let contains_record_id = report.hits.iter().any(|hit| hit.id.as_str() == record_id);
        measurement.samples.push(ReadSample {
            query: query_index,
            record_id: record_id.clone(),
            hit_id,
            contains_record_id,
            latency_ms,
            requests: report.requests,
            bytes_read: report.bytes_read,
            segments_searched: report.segments_searched,
        });
        measurement.hits += usize::from(contains_record_id);
    }
    Ok(measurement)
}

fn write_read_samples(path: &Path, samples: &[ReadSample]) -> BenchResult<()> {
    let mut reads = BufWriter::new(File::create(path)?);
    writeln!(
        reads,
        "query,record_id,hit_id,contains_record_id,latency_ms,requests,gets,puts,deletes,heads,lists,bytes_read,segments_searched"
    )?;
    for sample in samples {
        writeln!(
            reads,
            "{},{},{},{},{:.9},{},{},{},{},{},{},{},{}",
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

fn main() -> BenchResult<()> {
    let protocol =
        env::var("BORSUK_GROUP_COMMIT_PROTOCOL").unwrap_or_else(|_| "diagnostic".to_string());
    let uri = required("BORSUK_GROUP_COMMIT_INDEX_URI")?;
    let output = PathBuf::from(required("BORSUK_GROUP_COMMIT_OUTPUT")?);
    let source_sha = required("BORSUK_SOURCE_SHA256")?;
    let manifest_sha = required("BORSUK_GROUP_COMMIT_MANIFEST_SHA256")?;
    let writers: usize = number("BORSUK_GROUP_COMMIT_WRITERS")?;
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
    let (_cell_count, repetition, performance_gate) = match protocol.as_str() {
        "diagnostic" => {
            if writers != 8
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
            if cell_count != 64
                || writers != 1
                || repetition != 1
                || operations != 2
                || dimensions != 8
                || max_delay_ms != 1
                || max_records != 8
                || !matches!(records_per_operation, 1 | 16)
            {
                return Err("group-commit cell differs from the structural smoke".into());
            }
            (cell_count, repetition, None)
        }
        _ => return Err(format!("unknown group-commit protocol {protocol:?}").into()),
    };
    if output.exists() {
        return Err(format!("refusing to replace output {}", output.display()).into());
    }
    fs::create_dir_all(&output)?;

    let diagnostic_protocol = protocol == "diagnostic";
    let vector_seed = cohort_seed(diagnostic_protocol, writers, repetition);
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

    let index = BorsukIndex::open(&uri)?;
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: Duration::from_millis(max_delay_ms),
            max_records,
            worker_lanes,
        },
    )?;
    let barrier = Arc::new(Barrier::new(writers));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(writers * operations)));
    let started = Instant::now();
    let handles = (0..writers)
        .map(|writer_ordinal| {
            let writer = writer.clone();
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
                        .map(|batch_ordinal| {
                            let ordinal = first_ordinal + batch_ordinal;
                            if diagnostic_protocol {
                                format!(
                                    "group-w{writer_ordinal:02}-o{operation:03}-b{batch_ordinal:03}"
                                )
                            } else {
                                production_record_id(ordinal)
                            }
                        })
                        .collect::<Vec<_>>();
                    let records = record_ids
                        .iter()
                        .enumerate()
                        .map(|(batch_ordinal, record_id)| {
                            VectorRecord::new(
                                record_id.clone(),
                                input_vectors[first_ordinal + batch_ordinal].clone(),
                            )
                        })
                        .collect();
                    let append_started = Instant::now();
                    let ticket = writer.append_async(records)?;
                    pending.push_back(PendingAppend {
                        operation,
                        record_ids,
                        started: append_started,
                        ticket,
                    });
                    if pending.len() < pipeline_depth {
                        continue;
                    }
                    let completed = pending.pop_front().expect("non-empty pipeline");
                    let receipt = completed.ticket.wait()?;
                    local.push(Sample {
                        writer: writer_ordinal,
                        operation: completed.operation,
                        batch_records: completed.record_ids.len(),
                        record_ids: completed.record_ids,
                        latency_ms: completed.started.elapsed().as_secs_f64() * 1_000.0,
                        commit_lane: receipt.commit_lane,
                        commit_sequence: receipt.commit_sequence,
                        committed_records: receipt.committed_records,
                        acknowledgement_bytes: receipt.acknowledgement_bytes,
                        group_requests: receipt.requests,
                        lane_receipts: receipt.lane_receipts,
                    });
                }
                while let Some(completed) = pending.pop_front() {
                    let receipt = completed.ticket.wait()?;
                    local.push(Sample {
                        writer: writer_ordinal,
                        operation: completed.operation,
                        batch_records: completed.record_ids.len(),
                        record_ids: completed.record_ids,
                        latency_ms: completed.started.elapsed().as_secs_f64() * 1_000.0,
                        commit_lane: receipt.commit_lane,
                        commit_sequence: receipt.commit_sequence,
                        committed_records: receipt.committed_records,
                        acknowledgement_bytes: receipt.acknowledgement_bytes,
                        group_requests: receipt.requests,
                        lane_receipts: receipt.lane_receipts,
                    });
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
    fs::write(output.join("INGEST_COMPLETE"), b"complete\n")?;
    let mut samples = Arc::try_unwrap(samples)
        .map_err(|_| "sample owners remain")?
        .into_inner()
        .unwrap();
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
    let mut active_tail_index = BorsukIndex::open(&uri)?;
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
        return Err("active-tail inserted-ID recall gate failed".into());
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
    writer.drain()?;
    let drain_ms = drain_started.elapsed().as_secs_f64() * 1_000.0;
    let total_record_count = writers * operations * records_per_operation;
    let end_to_end_records_per_second =
        total_record_count as f64 / ((elapsed_ms + drain_ms) / 1_000.0);
    fs::write(output.join("DRAIN_COMPLETE"), b"complete\n")?;
    drop(writer);

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

    let mut reopened = BorsukIndex::open(&uri)?;
    let point_ids = samples
        .iter()
        .flat_map(|sample| sample.record_ids.iter().map(String::as_str))
        .collect::<Vec<_>>();
    let point_records = reopened.get_records(&point_ids)?;
    let mut visible = 0_usize;
    for (ordinal, point_record) in point_records.into_iter().enumerate() {
        let expected = &input_vectors[ordinal];
        visible += usize::from(point_record.is_some_and(|(stored, _)| &stored == expected));
    }
    if visible != total_record_count {
        return Err("post-reopen point visibility gate failed".into());
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
        return Err("post-reopen exact recall gate failed".into());
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
        "source_sha256,dataset_sha256,manifest_sha256,writers,operations,records_per_operation,pipeline_depth,worker_lanes,records,groups,mean_group_records,elapsed_ms,drain_ms,end_to_end_records_per_second,p50_ms,p95_ms,operations_per_second,records_per_second,vector_mib_per_second,storage_requests,storage_gets,storage_puts,storage_heads,requests_per_record,total_acknowledgement_bytes,max_acknowledgement_bytes,visible_records,recall_queries,max_read_segments,inserted_id_recall_at_10,active_tail_read_p50_ms,active_tail_read_p95_ms,read_p50_ms,read_p95_ms,read_storage_requests,read_storage_gets,read_storage_puts,read_storage_deletes,read_storage_heads,read_storage_lists,read_bytes,read_segments_searched"
    )?;
    writeln!(
        summary,
        "{source_sha},{dataset_sha},{manifest_sha},{writers},{operations},{records_per_operation},{pipeline_depth},{worker_lanes},{},{},{:.9},{elapsed_ms:.9},{drain_ms:.9},{end_to_end_records_per_second:.9},{:.9},{:.9},{operations_per_second:.9},{:.9},{vector_mib_per_second:.9},{total_requests},{},{},{},{:.9},{total_acknowledgement_bytes},{max_acknowledgement_bytes},{visible},{recall_queries},{max_read_segments},{:.9},{active_tail_read_p50_ms:.9},{active_tail_read_p95_ms:.9},{:.9},{:.9},{},{},{},{},{},{},{},{}",
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
    let mut raw = BufWriter::new(File::create(output.join("samples.csv"))?);
    writeln!(
        raw,
        "writer,operation,batch_records,first_record_id,record_ids,latency_ms,commit_lane,commit_sequence,committed_records,acknowledgement_bytes,group_requests,group_gets,group_puts,group_heads,lane_receipts"
    )?;
    for sample in samples {
        writeln!(
            raw,
            "{},{},{},{},{},{:.9},{},{},{},{},{},{},{},{},{}",
            sample.writer,
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
    write_read_samples(&output.join("reads.csv"), &reads.samples)?;
    summary.flush()?;
    raw.flush()?;
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
    fn percentile_is_deterministic() {
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.95), 4.0);
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

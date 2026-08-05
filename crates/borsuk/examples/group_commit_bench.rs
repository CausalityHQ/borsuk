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
    BorsukIndex, GroupCommitConfig, GroupCommitTicket, GroupCommitWriter, LeafMode, RequestCounts,
    SearchOptions, VectorRecord,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
struct Sample {
    writer: usize,
    operation: usize,
    record_id: String,
    latency_ms: f64,
    commit_lane: usize,
    commit_sequence: u64,
    committed_records: usize,
    group_requests: RequestCounts,
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

struct PendingAppend {
    operation: usize,
    record_id: String,
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

#[derive(Clone, Copy)]
struct PerformanceObservation {
    p95_ms: f64,
    records_per_second: f64,
    read_p95_ms: f64,
    inserted_id_recall_at_10: f64,
}

#[derive(Clone, Copy)]
struct PerformanceThresholds {
    max_p95_ms: f64,
    min_records_per_second: f64,
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
    if observed.read_p95_ms >= thresholds.max_read_p95_ms {
        failures.push("PRODUCTION_READ_P95_FAILED");
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
    let worker_lanes = match protocol.as_str() {
        "scalability" => number("BORSUK_GROUP_COMMIT_WORKER_LANES")?,
        "diagnostic" => 8,
        _ => 1,
    };
    let pipeline_depth = if protocol == "scalability" {
        number("BORSUK_GROUP_COMMIT_PIPELINE_DEPTH")?
    } else {
        1
    };
    if pipeline_depth == 0 {
        return Err("group-commit pipeline depth must be positive".into());
    }
    let (cell_count, repetition, performance_gate) = match protocol.as_str() {
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
                || operations != 100
                || dimensions != 768
                || max_delay_ms != 5
                || max_records != 1_024
                || !matches!(worker_lanes, 1 | 2 | 4)
                || pipeline_depth != 4
            {
                return Err(
                    "group-commit cell differs from the frozen scalability protocol".into(),
                );
            }
            let max_p95_ms: f64 = number("BORSUK_GROUP_COMMIT_MAX_P95_MS")?;
            let min_records_per_second: f64 = number("BORSUK_GROUP_COMMIT_MIN_RECORDS_PER_SECOND")?;
            let max_read_p95_ms: f64 = number("BORSUK_GROUP_COMMIT_MAX_READ_P95_MS")?;
            let min_inserted_id_recall_at_10: f64 =
                number("BORSUK_GROUP_COMMIT_MIN_INSERTED_ID_RECALL_AT_10")?;
            if max_p95_ms <= 0.0
                || min_records_per_second < 0.0
                || max_read_p95_ms <= 0.0
                || !(0.0..=1.0).contains(&min_inserted_id_recall_at_10)
            {
                return Err("production performance thresholds must be positive".into());
            }
            (
                cell_count,
                repetition,
                Some(PerformanceThresholds {
                    max_p95_ms,
                    min_records_per_second,
                    max_read_p95_ms,
                    min_inserted_id_recall_at_10,
                }),
            )
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
    let dataset_sha = if protocol == "scalability" {
        required("BORSUK_GROUP_COMMIT_DATASET_SHA256")?
    } else {
        String::new()
    };
    let input_vectors = if protocol == "scalability" {
        let dataset = PathBuf::from(required("BORSUK_GROUP_COMMIT_DATASET")?);
        if !dataset.is_dir()
            || dataset_sha.len() != 64
            || !dataset_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("invalid group-commit dataset identity".into());
        }
        read_parquet_vectors(&dataset, writers * operations, dimensions)?
    } else {
        (0..writers * operations)
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
                    let ordinal = writer_ordinal * operations + operation;
                    let record_id = if diagnostic_protocol {
                        format!("group-w{writer_ordinal:02}-o{operation:03}")
                    } else {
                        format!(
                            "group-c{cell_count}-r{repetition:02}-l{worker_lanes}-w{writers}-p{writer_ordinal:02}-o{operation:03}"
                        )
                    };
                    let record_vector = input_vectors[ordinal].clone();
                    let append_started = Instant::now();
                    let ticket = writer.append_async(vec![VectorRecord::new(
                        record_id.clone(),
                        record_vector,
                    )])?;
                    pending.push_back(PendingAppend {
                        operation,
                        record_id,
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
                        record_id: completed.record_id,
                        latency_ms: completed.started.elapsed().as_secs_f64() * 1_000.0,
                        commit_lane: receipt.commit_lane,
                        commit_sequence: receipt.commit_sequence,
                        committed_records: receipt.committed_records,
                        group_requests: receipt.requests,
                    });
                }
                while let Some(completed) = pending.pop_front() {
                    let receipt = completed.ticket.wait()?;
                    local.push(Sample {
                        writer: writer_ordinal,
                        operation: completed.operation,
                        record_id: completed.record_id,
                        latency_ms: completed.started.elapsed().as_secs_f64() * 1_000.0,
                        commit_lane: receipt.commit_lane,
                        commit_sequence: receipt.commit_sequence,
                        committed_records: receipt.committed_records,
                        group_requests: receipt.requests,
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
    let drain_started = Instant::now();
    writer.drain()?;
    let drain_ms = drain_started.elapsed().as_secs_f64() * 1_000.0;
    fs::write(output.join("DRAIN_COMPLETE"), b"complete\n")?;
    drop(writer);

    let mut samples = Arc::try_unwrap(samples)
        .map_err(|_| "sample owners remain")?
        .into_inner()
        .unwrap();
    samples.sort_by_key(|sample| (sample.writer, sample.operation));
    let mut groups = BTreeMap::<(usize, u64), (usize, RequestCounts)>::new();
    for sample in &samples {
        match groups.insert(
            (sample.commit_lane, sample.commit_sequence),
            (sample.committed_records, sample.group_requests),
        ) {
            Some(previous) if previous != (sample.committed_records, sample.group_requests) => {
                return Err("callers disagree about shared group evidence".into());
            }
            _ => {}
        }
    }
    let request_totals =
        groups
            .values()
            .fold(RequestCounts::default(), |mut totals, (_, requests)| {
                totals.gets += requests.gets;
                totals.puts += requests.puts;
                totals.deletes += requests.deletes;
                totals.heads += requests.heads;
                totals.lists += requests.lists;
                totals
            });
    let total_requests = request_totals.total();
    let committed_records = groups.values().map(|(records, _)| *records).sum::<usize>();
    if committed_records != samples.len() {
        return Err("group record totals do not reconcile with caller samples".into());
    }

    let reopened = BorsukIndex::open(&uri)?;
    let point_records = reopened.get_records(
        &samples
            .iter()
            .map(|sample| sample.record_id.as_str())
            .collect::<Vec<_>>(),
    )?;
    let mut visible = 0_usize;
    for (sample, point_record) in samples.iter().zip(point_records) {
        let ordinal = sample.writer * operations + sample.operation;
        let expected = &input_vectors[ordinal];
        visible += usize::from(point_record.is_some_and(|(stored, _)| &stored == expected));
    }
    if visible != samples.len() {
        return Err("post-reopen point visibility gate failed".into());
    }
    fs::write(output.join("POINT_VISIBILITY_COMPLETE"), b"complete\n")?;
    let mut recall_hits = 0_usize;
    let max_read_segments = if diagnostic_protocol { 0 } else { 4 };
    let recall_queries = if diagnostic_protocol {
        20
    } else if protocol == "scalability" {
        number("BORSUK_GROUP_COMMIT_READ_QUERIES")?
    } else {
        1
    }
    .min(samples.len());
    let mut read_latencies = Vec::with_capacity(recall_queries);
    let mut read_requests = RequestCounts::default();
    let mut read_bytes = 0_u64;
    let mut read_segments_searched = 0_usize;
    let mut read_samples = Vec::with_capacity(recall_queries);
    for (query_index, sample) in samples.iter().take(recall_queries).enumerate() {
        let ordinal = sample.writer * operations + sample.operation;
        let read_started = Instant::now();
        let report = reopened.search_with_report(
            &input_vectors[ordinal],
            if diagnostic_protocol {
                SearchOptions::exact(1)
            } else {
                SearchOptions::approx(10, LeafMode::SrhtPqScan)
                    .with_max_segments(max_read_segments)
                    .with_max_candidates_per_segment(64)
            },
        )?;
        let latency_ms = read_started.elapsed().as_secs_f64() * 1_000.0;
        read_latencies.push(latency_ms);
        read_requests.gets += report.requests.gets;
        read_requests.puts += report.requests.puts;
        read_requests.deletes += report.requests.deletes;
        read_requests.heads += report.requests.heads;
        read_requests.lists += report.requests.lists;
        read_bytes = read_bytes.saturating_add(report.bytes_read);
        read_segments_searched = read_segments_searched.saturating_add(report.segments_searched);
        let hit_id = report
            .hits
            .first()
            .map_or_else(String::new, |hit| hit.id.as_str().to_string());
        let contains_record_id = report
            .hits
            .iter()
            .any(|hit| hit.id.as_str() == sample.record_id);
        read_samples.push(ReadSample {
            query: query_index,
            record_id: sample.record_id.clone(),
            hit_id,
            contains_record_id,
            latency_ms,
            requests: report.requests,
            bytes_read: report.bytes_read,
            segments_searched: report.segments_searched,
        });
        recall_hits += usize::from(contains_record_id);
    }
    if recall_hits != recall_queries {
        return Err("post-reopen exact recall gate failed".into());
    }
    fs::write(output.join("READ_QUALIFICATION_COMPLETE"), b"complete\n")?;

    let latencies = samples
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    let p50_ms = percentile(&latencies, 0.50);
    let p95_ms = percentile(&latencies, 0.95);
    let records_per_second = samples.len() as f64 / (elapsed_ms / 1_000.0);
    let vector_mib_per_second = vector_mib_per_second(records_per_second, dimensions);
    let read_p50_ms = percentile(&read_latencies, 0.50);
    let read_p95_ms = percentile(&read_latencies, 0.95);
    let inserted_id_recall_at_10 = recall_hits as f64 / recall_queries as f64;
    let mut summary = BufWriter::new(File::create(output.join("summary.csv"))?);
    writeln!(
        summary,
        "source_sha256,dataset_sha256,manifest_sha256,writers,operations,pipeline_depth,worker_lanes,records,groups,mean_group_records,elapsed_ms,drain_ms,p50_ms,p95_ms,records_per_second,vector_mib_per_second,storage_requests,storage_gets,storage_puts,storage_heads,requests_per_record,visible_records,recall_queries,max_read_segments,inserted_id_recall_at_10,read_p50_ms,read_p95_ms,read_storage_requests,read_storage_gets,read_storage_puts,read_storage_deletes,read_storage_heads,read_storage_lists,read_bytes,read_segments_searched"
    )?;
    writeln!(
        summary,
        "{source_sha},{dataset_sha},{manifest_sha},{writers},{operations},{pipeline_depth},{worker_lanes},{},{},{:.9},{elapsed_ms:.9},{drain_ms:.9},{:.9},{:.9},{:.9},{vector_mib_per_second:.9},{total_requests},{},{},{},{:.9},{visible},{recall_queries},{max_read_segments},{:.9},{:.9},{:.9},{},{},{},{},{},{},{read_bytes},{read_segments_searched}",
        samples.len(),
        groups.len(),
        samples.len() as f64 / groups.len() as f64,
        p50_ms,
        p95_ms,
        records_per_second,
        request_totals.gets,
        request_totals.puts,
        request_totals.heads,
        total_requests as f64 / samples.len() as f64,
        inserted_id_recall_at_10,
        read_p50_ms,
        read_p95_ms,
        read_requests.total(),
        read_requests.gets,
        read_requests.puts,
        read_requests.deletes,
        read_requests.heads,
        read_requests.lists,
    )?;
    let mut raw = BufWriter::new(File::create(output.join("samples.csv"))?);
    writeln!(
        raw,
        "writer,operation,record_id,latency_ms,commit_lane,commit_sequence,committed_records,group_requests,group_gets,group_puts,group_heads"
    )?;
    for sample in samples {
        writeln!(
            raw,
            "{},{},{},{:.9},{},{},{},{},{},{},{}",
            sample.writer,
            sample.operation,
            sample.record_id,
            sample.latency_ms,
            sample.commit_lane,
            sample.commit_sequence,
            sample.committed_records,
            sample.group_requests.total(),
            sample.group_requests.gets,
            sample.group_requests.puts,
            sample.group_requests.heads,
        )?;
    }
    let mut reads = BufWriter::new(File::create(output.join("reads.csv"))?);
    writeln!(
        reads,
        "query,record_id,hit_id,contains_record_id,latency_ms,requests,gets,puts,deletes,heads,lists,bytes_read,segments_searched"
    )?;
    for sample in read_samples {
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
    summary.flush()?;
    raw.flush()?;
    if let Some(thresholds) = performance_gate {
        let failures = production_performance_gate_failures(
            PerformanceObservation {
                p95_ms,
                records_per_second,
                read_p95_ms,
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
    fn scalability_cohorts_pair_cell_counts_but_separate_writer_counts() {
        let one_writer = cohort_seed(false, 1, 3);
        assert_eq!(one_writer, cohort_seed(false, 1, 3));
        assert_ne!(one_writer, cohort_seed(false, 8, 3));
        assert_ne!(one_writer, cohort_seed(false, 1, 4));
        assert_eq!(cohort_seed(true, 1, 0), 76_412_031);
        assert_eq!(cohort_seed(true, 32, 5), 76_412_031);
    }

    #[test]
    fn production_gate_requires_both_latency_and_scaled_throughput() {
        let thresholds = PerformanceThresholds {
            max_p95_ms: 200.0,
            min_records_per_second: 160.0,
            max_read_p95_ms: 200.0,
            min_inserted_id_recall_at_10: 1.0,
        };
        assert!(
            production_performance_gate_failures(
                PerformanceObservation {
                    p95_ms: 199.999,
                    records_per_second: 160.0,
                    read_p95_ms: 199.999,
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
                    read_p95_ms: 199.999,
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
                    read_p95_ms: 199.999,
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
                    read_p95_ms: 199.999,
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
                    read_p95_ms: 200.0,
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
                    read_p95_ms: 199.999,
                    inserted_id_recall_at_10: 0.99
                },
                thresholds,
            ),
            vec!["PRODUCTION_INSERTED_ID_RECALL_AT_10_FAILED"]
        );
    }
}

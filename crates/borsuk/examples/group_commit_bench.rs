//! Bounded group-commit ingest qualification.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Barrier, Mutex},
    thread,
    time::{Duration, Instant},
};

use borsuk::{
    BorsukIndex, GroupCommitConfig, GroupCommitWriter, RequestCounts, SearchOptions, VectorRecord,
};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
struct Sample {
    writer: usize,
    operation: usize,
    record_id: String,
    latency_ms: f64,
    commit_sequence: u64,
    committed_records: usize,
    group_requests: RequestCounts,
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

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[((ordered.len() - 1) as f64 * quantile).round() as usize]
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
    let (cell_count, repetition) = match protocol.as_str() {
        "diagnostic" => {
            if writers != 8
                || operations != 20
                || dimensions != 96
                || max_delay_ms != 5
                || max_records != 64
            {
                return Err("group-commit cell differs from the frozen diagnostic".into());
            }
            (2_000, 0)
        }
        "scalability" => {
            let cell_count: usize = number("BORSUK_GROUP_COMMIT_CELL_COUNT")?;
            let repetition: usize = number("BORSUK_GROUP_COMMIT_REPETITION")?;
            if !matches!(cell_count, 2_000 | 16_000)
                || !matches!(writers, 1 | 8 | 32)
                || !(1..=5).contains(&repetition)
                || operations != 100
                || dimensions != 96
                || max_delay_ms != 5
                || max_records != 64
            {
                return Err(
                    "group-commit cell differs from the frozen scalability protocol".into(),
                );
            }
            (cell_count, repetition)
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
            (cell_count, repetition)
        }
        _ => return Err(format!("unknown group-commit protocol {protocol:?}").into()),
    };
    if output.exists() {
        return Err(format!("refusing to replace output {}", output.display()).into());
    }

    let index = BorsukIndex::open(&uri)?;
    let diagnostic_protocol = protocol == "diagnostic";
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: Duration::from_millis(max_delay_ms),
            max_records,
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
            thread::spawn(move || -> BenchResult<()> {
                barrier.wait();
                let mut local = Vec::with_capacity(operations);
                for operation in 0..operations {
                    let ordinal = writer_ordinal * operations + operation;
                    let record_id = if diagnostic_protocol {
                        format!("group-w{writer_ordinal:02}-o{operation:03}")
                    } else {
                        format!(
                            "group-c{cell_count}-r{repetition:02}-w{writers}-p{writer_ordinal:02}-o{operation:03}"
                        )
                    };
                    let append_started = Instant::now();
                    let receipt = writer.append(vec![VectorRecord::new(
                        record_id.clone(),
                        vector(
                            76412031_u64.wrapping_add(repetition as u64),
                            ordinal as u64,
                            dimensions,
                        ),
                    )])?;
                    local.push(Sample {
                        writer: writer_ordinal,
                        operation,
                        record_id,
                        latency_ms: append_started.elapsed().as_secs_f64() * 1_000.0,
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
    drop(writer);

    let mut samples = Arc::try_unwrap(samples)
        .map_err(|_| "sample owners remain")?
        .into_inner()
        .unwrap();
    samples.sort_by_key(|sample| (sample.writer, sample.operation));
    let mut groups = BTreeMap::<u64, (usize, RequestCounts)>::new();
    for sample in &samples {
        match groups.insert(
            sample.commit_sequence,
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
    let mut visible = 0_usize;
    for sample in &samples {
        visible += usize::from(reopened.get_record(&sample.record_id)?.is_some());
    }
    let mut recall_hits = 0_usize;
    let recall_queries = 20_usize.min(samples.len());
    for sample in samples.iter().take(recall_queries) {
        let ordinal = sample.writer * operations + sample.operation;
        let report = reopened.search_with_report(
            &vector(
                76412031_u64.wrapping_add(repetition as u64),
                ordinal as u64,
                dimensions,
            ),
            SearchOptions::exact(1),
        )?;
        recall_hits += usize::from(
            report
                .hits
                .first()
                .is_some_and(|hit| hit.id.as_str() == sample.record_id),
        );
    }
    if visible != samples.len() || recall_hits != recall_queries {
        return Err("post-reopen visibility or exact recall gate failed".into());
    }

    fs::create_dir_all(&output)?;
    let latencies = samples
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    let mut summary = BufWriter::new(File::create(output.join("summary.csv"))?);
    writeln!(
        summary,
        "source_sha256,manifest_sha256,writers,operations,records,groups,mean_group_records,elapsed_ms,p50_ms,p95_ms,records_per_second,storage_requests,storage_gets,storage_puts,storage_heads,requests_per_record,visible_records,exact_recall"
    )?;
    writeln!(
        summary,
        "{source_sha},{manifest_sha},{writers},{operations},{},{},{:.9},{elapsed_ms:.9},{:.9},{:.9},{:.9},{total_requests},{},{},{},{:.9},{visible},{:.9}",
        samples.len(),
        groups.len(),
        samples.len() as f64 / groups.len() as f64,
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        samples.len() as f64 / (elapsed_ms / 1_000.0),
        request_totals.gets,
        request_totals.puts,
        request_totals.heads,
        total_requests as f64 / samples.len() as f64,
        recall_hits as f64 / recall_queries as f64,
    )?;
    let mut raw = BufWriter::new(File::create(output.join("samples.csv"))?);
    writeln!(
        raw,
        "writer,operation,record_id,latency_ms,commit_sequence,committed_records,group_requests,group_gets,group_puts,group_heads"
    )?;
    for sample in samples {
        writeln!(
            raw,
            "{},{},{},{:.9},{},{},{},{},{},{}",
            sample.writer,
            sample.operation,
            sample.record_id,
            sample.latency_ms,
            sample.commit_sequence,
            sample.committed_records,
            sample.group_requests.total(),
            sample.group_requests.gets,
            sample.group_requests.puts,
            sample.group_requests.heads,
        )?;
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
}

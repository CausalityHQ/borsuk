//! Sealed local-only PQ4 holdout qualification over Parquet query and truth inputs.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::Instant,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{Pq4Index, Pq4OpenOptions};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use sha2::{Digest, Sha256};

const AGGREGATE_RECALL_GATE_PPM: u32 = 995_000;
const QUERY_FLOOR_PPM: u32 = 800_000;
const FLOOR_COMPLIANCE_GATE_PPM: u32 = 997_500;
const P99_LATENCY_GATE_NS: u64 = 15_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pq4QualifyMode {
    Development,
    SealedHoldout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pq4QualifyRequest {
    mode: Pq4QualifyMode,
    snapshot: PathBuf,
    query_parquet: PathBuf,
    truth_parquet: PathBuf,
    result_json: PathBuf,
    samples_parquet: PathBuf,
    binary_sha256: String,
    binary_bytes: u64,
    snapshot_manifest_sha256: String,
    snapshot_manifest_bytes: u64,
    query_sha256: String,
    query_bytes: u64,
    truth_sha256: String,
    truth_bytes: u64,
    source_commit: String,
    query_start: u32,
    query_count: u32,
    candidate_depth: usize,
    query_threads: usize,
    memory_budget_bytes: usize,
    admission_timeout_ms: u64,
    warmup_queries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HoldoutSample {
    query_ordinal: u32,
    match_source_ordinals: [u64; 10],
    hits: u32,
    recall_ppm: u32,
    latency_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HoldoutSummary {
    aggregate_recall_ppm: u32,
    floor_compliance_ppm: u32,
    minimum_recall_ppm: u32,
    p99_latency_ns: u64,
    maximum_latency_ns: u64,
    passed: bool,
}

fn take(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn positive<T>(values: &mut BTreeMap<String, String>, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr + PartialOrd + Default,
{
    take(values, flag)?
        .parse::<T>()
        .ok()
        .filter(|value| *value > T::default())
        .ok_or_else(|| format!("invalid {flag}"))
}

fn digest(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    let value = take(values, flag)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("invalid {flag}"));
    }
    Ok(value)
}

fn parse_pq4_qualify_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Pq4QualifyRequest, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut values = BTreeMap::new();
    let mut mode = None;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-sealed-holdout" || flag == "--execute-development" {
            let next = if flag == "--execute-sealed-holdout" {
                Pq4QualifyMode::SealedHoldout
            } else {
                Pq4QualifyMode::Development
            };
            if mode.replace(next).is_some() {
                return Err("duplicate PQ4 execution mode".to_owned());
            }
            continue;
        }
        if !flag.starts_with("--") {
            return Err(format!("unexpected positional argument {flag}"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        if value.starts_with("--") || values.insert(flag.clone(), value).is_some() {
            return Err(format!("invalid or duplicate flag {flag}"));
        }
    }
    let mode = mode.ok_or_else(|| "PQ4 execution mode is absent".to_owned())?;
    let request = Pq4QualifyRequest {
        mode,
        snapshot: PathBuf::from(take(&mut values, "--snapshot")?),
        query_parquet: PathBuf::from(take(&mut values, "--query-parquet")?),
        truth_parquet: PathBuf::from(take(&mut values, "--truth-parquet")?),
        result_json: PathBuf::from(take(&mut values, "--result-json")?),
        samples_parquet: PathBuf::from(take(&mut values, "--samples-parquet")?),
        binary_sha256: digest(&mut values, "--binary-sha256")?,
        binary_bytes: positive(&mut values, "--binary-bytes")?,
        snapshot_manifest_sha256: digest(&mut values, "--snapshot-manifest-sha256")?,
        snapshot_manifest_bytes: positive(&mut values, "--snapshot-manifest-bytes")?,
        query_sha256: digest(&mut values, "--query-sha256")?,
        query_bytes: positive(&mut values, "--query-bytes")?,
        truth_sha256: digest(&mut values, "--truth-sha256")?,
        truth_bytes: positive(&mut values, "--truth-bytes")?,
        source_commit: take(&mut values, "--source-commit")?,
        query_start: take(&mut values, "--query-start")?
            .parse::<u32>()
            .map_err(|_| "invalid --query-start".to_owned())?,
        query_count: positive(&mut values, "--query-count")?,
        candidate_depth: positive(&mut values, "--candidate-depth")?,
        query_threads: positive(&mut values, "--query-threads")?,
        memory_budget_bytes: positive(&mut values, "--memory-budget-bytes")?,
        admission_timeout_ms: positive(&mut values, "--admission-timeout-ms")?,
        warmup_queries: positive(&mut values, "--warmup-queries")?,
    };
    let mode_matches = match request.mode {
        Pq4QualifyMode::SealedHoldout => {
            request.query_start == 512 && request.query_count == 480 && request.query_threads == 16
        }
        Pq4QualifyMode::Development => {
            request.query_start == 0
                && request.query_count == 512
                && [4, 8, 16].contains(&request.query_threads)
        }
    };
    if !values.is_empty()
        || request.source_commit.len() != 40
        || !request
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || request.snapshot.as_os_str().is_empty()
        || request.query_parquet.as_os_str().is_empty()
        || request.truth_parquet.as_os_str().is_empty()
        || request.result_json.as_os_str().is_empty()
        || request.samples_parquet.as_os_str().is_empty()
        || !mode_matches
        || request.candidate_depth != 3_072
        || request.memory_budget_bytes != 3_221_225_472
        || request.admission_timeout_ms != 1_000
        || request.warmup_queries != 32
    {
        return Err("PQ4 qualification arguments differ".to_owned());
    }
    Ok(request)
}

fn summarize_holdout(
    samples: &[HoldoutSample],
    first_query_ordinal: u32,
) -> Result<HoldoutSummary, String> {
    if samples.is_empty()
        || samples.iter().enumerate().any(|(index, sample)| {
            sample.query_ordinal != first_query_ordinal + u32::try_from(index).unwrap()
                || sample.hits > 10
                || sample.recall_ppm != sample.hits * 100_000
                || sample.latency_ns == 0
        })
    {
        return Err("PQ4 holdout samples differ".to_owned());
    }
    let count = u64::try_from(samples.len()).unwrap();
    let aggregate_recall_ppm = u32::try_from(
        samples
            .iter()
            .map(|sample| u64::from(sample.hits))
            .sum::<u64>()
            * 100_000
            / count,
    )
    .unwrap();
    let floor_count = samples
        .iter()
        .filter(|sample| sample.recall_ppm >= QUERY_FLOOR_PPM)
        .count();
    let floor_compliance_ppm =
        u32::try_from(u64::try_from(floor_count).unwrap() * 1_000_000 / count).unwrap();
    let minimum_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .unwrap();
    let mut latencies = samples
        .iter()
        .map(|sample| sample.latency_ns)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let p99_index = (samples.len() * 99).div_ceil(100).saturating_sub(1);
    let p99_latency_ns = latencies[p99_index];
    let maximum_latency_ns = *latencies.last().unwrap();
    Ok(HoldoutSummary {
        aggregate_recall_ppm,
        floor_compliance_ppm,
        minimum_recall_ppm,
        p99_latency_ns,
        maximum_latency_ns,
        passed: aggregate_recall_ppm >= AGGREGATE_RECALL_GATE_PPM
            && floor_compliance_ppm >= FLOOR_COMPLIANCE_GATE_PPM
            && p99_latency_ns <= P99_LATENCY_GATE_NS,
    })
}

fn query_schema() -> Schema {
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        ),
        false,
    )])
}

fn truth_schema() -> Schema {
    Schema::new(vec![Field::new(
        "neighbors_id",
        DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int32, false)), 100),
        false,
    )])
}

fn read_queries(path: &Path) -> Result<Vec<[f32; 96]>, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(path).map_err(|error| format!("query open failed: {error}"))?,
    )
    .map_err(|error| format!("query metadata failed: {error}"))?;
    if builder.schema().as_ref() != &query_schema()
        || builder.metadata().file_metadata().num_rows() != 10_000
    {
        return Err("query Parquet authority differs".to_owned());
    }
    let mut output = Vec::with_capacity(10_000);
    for batch in builder
        .with_batch_size(8_192)
        .build()
        .map_err(|error| format!("query reader failed: {error}"))?
    {
        let batch = batch.map_err(|error| format!("query read failed: {error}"))?;
        let vectors = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| "query array differs".to_owned())?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| "query values differ".to_owned())?;
        let (rows, remainder) = values.values().as_chunks::<96>();
        if vectors.null_count() != 0
            || values.null_count() != 0
            || !remainder.is_empty()
            || rows.len() != batch.num_rows()
            || rows.iter().any(|row| {
                row.iter().any(|value| !value.is_finite())
                    || row.iter().map(|value| value * value).sum::<f32>() <= 0.0
            })
        {
            return Err("query values differ".to_owned());
        }
        output.extend_from_slice(rows);
    }
    if output.len() != 10_000 {
        return Err("query row count differs".to_owned());
    }
    Ok(output)
}

fn read_truth(path: &Path) -> Result<Vec<[u64; 10]>, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(path).map_err(|error| format!("truth open failed: {error}"))?,
    )
    .map_err(|error| format!("truth metadata failed: {error}"))?;
    if builder.schema().as_ref() != &truth_schema()
        || builder.metadata().file_metadata().num_rows() != 10_000
    {
        return Err("truth Parquet authority differs".to_owned());
    }
    let mut output = Vec::with_capacity(10_000);
    for batch in builder
        .with_batch_size(8_192)
        .build()
        .map_err(|error| format!("truth reader failed: {error}"))?
    {
        let batch = batch.map_err(|error| format!("truth read failed: {error}"))?;
        let neighbors = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| "truth array differs".to_owned())?;
        let values = neighbors
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "truth values differ".to_owned())?;
        if neighbors.null_count() != 0
            || values.null_count() != 0
            || values.len() != batch.num_rows() * 100
        {
            return Err("truth values differ".to_owned());
        }
        for row in 0..batch.num_rows() {
            let mut first_ten = [0_u64; 10];
            let mut unique = BTreeSet::new();
            for (position, slot) in first_ten.iter_mut().enumerate() {
                let value = values.value(row * 100 + position);
                if value < 0 || !unique.insert(value) {
                    return Err("truth neighbor authority differs".to_owned());
                }
                *slot = u64::try_from(value).unwrap();
            }
            output.push(first_ten);
        }
    }
    if output.len() != 10_000 {
        return Err("truth row count differs".to_owned());
    }
    Ok(output)
}

fn sha256_file(path: &Path) -> Result<(String, u64), String> {
    let mut file =
        fs::File::open(path).map_err(|error| format!("artifact open failed: {error}"))?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)
        .map_err(|error| format!("artifact digest failed: {error}"))?;
    let bytes = file
        .metadata()
        .map_err(|error| format!("artifact metadata failed: {error}"))?
        .len();
    Ok((format!("{:x}", digest.finalize()), bytes))
}

fn authenticate(path: &Path, digest: &str, bytes: u64, role: &str) -> Result<(), String> {
    let observed = sha256_file(path)?;
    if observed != (digest.to_owned(), bytes) {
        return Err(format!("{role} authority differs"));
    }
    Ok(())
}

fn decode_source_ordinal(id: &[u8]) -> Result<u64, String> {
    let bytes: [u8; 8] = id
        .try_into()
        .map_err(|_| "PQ4 result ID is not a source ordinal".to_owned())?;
    Ok(u64::from_le_bytes(bytes))
}

fn samples_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt32, false),
        Field::new("latency_ns", DataType::UInt64, false),
        Field::new(
            "match_source_ordinals",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt64, false)), 10),
            false,
        ),
    ])
}

fn write_samples(path: &Path, samples: &[HoldoutSample]) -> Result<(), String> {
    let schema = Arc::new(samples_schema());
    let match_ordinals = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt64, false)),
        10,
        Arc::new(UInt64Array::from_iter_values(
            samples.iter().flat_map(|row| row.match_source_ordinals),
        )),
        None,
    )
    .map_err(|error| format!("sample match array failed: {error}"))?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|row| row.query_ordinal),
            )) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|row| row.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|row| row.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|row| row.latency_ns),
            )),
            Arc::new(match_ordinals),
        ],
    )
    .map_err(|error| format!("sample batch failed: {error}"))?;
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(path).map_err(|error| format!("sample create failed: {error}"))?,
        schema,
        Some(properties),
    )
    .map_err(|error| format!("sample writer failed: {error}"))?;
    writer
        .write(&batch)
        .map_err(|error| format!("sample write failed: {error}"))?;
    writer
        .close()
        .map_err(|error| format!("sample close failed: {error}"))?;
    Ok(())
}

fn peak_rss_bytes() -> Result<u64, String> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| format!("process status failed: {error}"))?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| "process peak RSS is absent".to_owned())?;
    kib.checked_mul(1_024)
        .ok_or_else(|| "process peak RSS overflows".to_owned())
}

fn warmup_query_range(request: &Pq4QualifyRequest) -> Result<std::ops::Range<usize>, String> {
    if request.mode == Pq4QualifyMode::SealedHoldout && request.warmup_queries > request.query_start
    {
        return Err("query warmup range differs".to_owned());
    }
    let end = usize::try_from(request.warmup_queries)
        .map_err(|_| "query warmup range overflows".to_owned())?;
    Ok(0..end)
}

fn run() -> Result<(), String> {
    let request = parse_pq4_qualify_args(env::args())?;
    if request.result_json.exists() || request.samples_parquet.exists() {
        return Err("PQ4 qualification output already exists".to_owned());
    }
    authenticate(
        &env::current_exe().map_err(|error| format!("binary path failed: {error}"))?,
        &request.binary_sha256,
        request.binary_bytes,
        "binary",
    )?;
    authenticate(
        &request.snapshot.join("manifest.json"),
        &request.snapshot_manifest_sha256,
        request.snapshot_manifest_bytes,
        "snapshot manifest",
    )?;
    authenticate(
        &request.query_parquet,
        &request.query_sha256,
        request.query_bytes,
        "query",
    )?;
    authenticate(
        &request.truth_parquet,
        &request.truth_sha256,
        request.truth_bytes,
        "truth",
    )?;
    let queries = read_queries(&request.query_parquet)?;
    let truth = read_truth(&request.truth_parquet)?;
    let query_end = request
        .query_start
        .checked_add(request.query_count)
        .ok_or_else(|| "query range overflows".to_owned())?;
    let warmup_range = warmup_query_range(&request)?;
    if usize::try_from(query_end).unwrap() > queries.len() || warmup_range.end > queries.len() {
        return Err("query range differs".to_owned());
    }
    let index = Pq4Index::open(
        &request.snapshot,
        Pq4OpenOptions {
            shard_ordinal: 0,
            memory_budget_bytes: u64::try_from(request.memory_budget_bytes).unwrap(),
            query_threads: request.query_threads,
            admission_timeout_ms: request.admission_timeout_ms,
        },
    )
    .map_err(|error| error.to_string())?;
    for query in &queries[warmup_range] {
        index.search(query, 10).map_err(|error| error.to_string())?;
    }
    let mut samples = Vec::with_capacity(request.query_count as usize);
    for query_ordinal in request.query_start..query_end {
        let started = Instant::now();
        let matches = index
            .search(&queries[query_ordinal as usize], 10)
            .map_err(|error| error.to_string())?;
        let latency_ns = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| "query latency overflows".to_owned())?;
        let returned_ordered = matches
            .iter()
            .map(|item| decode_source_ordinal(&item.id))
            .collect::<Result<Vec<_>, _>>()?;
        let returned = returned_ordered.iter().copied().collect::<BTreeSet<_>>();
        if returned.len() != 10 {
            return Err("PQ4 result cardinality differs".to_owned());
        }
        let hits = u32::try_from(
            truth[query_ordinal as usize]
                .iter()
                .filter(|source| returned.contains(source))
                .count(),
        )
        .unwrap();
        samples.push(HoldoutSample {
            query_ordinal,
            match_source_ordinals: returned_ordered
                .try_into()
                .map_err(|_| "PQ4 result cardinality differs".to_owned())?,
            hits,
            recall_ppm: hits * 100_000,
            latency_ns,
        });
    }
    let summary = summarize_holdout(&samples, request.query_start)?;
    write_samples(&request.samples_parquet, &samples)?;
    let (samples_sha256, samples_bytes) = sha256_file(&request.samples_parquet)?;
    let result = BTreeMap::from([
        (
            "aggregate_recall_gate_ppm",
            serde_json::json!(AGGREGATE_RECALL_GATE_PPM),
        ),
        (
            "aggregate_recall_ppm",
            serde_json::json!(summary.aggregate_recall_ppm),
        ),
        ("binary_sha256", serde_json::json!(request.binary_sha256)),
        ("binary_bytes", serde_json::json!(request.binary_bytes)),
        (
            "candidate_depth",
            serde_json::json!(request.candidate_depth),
        ),
        ("claim_eligible", serde_json::json!(false)),
        (
            "floor_compliance_gate_ppm",
            serde_json::json!(FLOOR_COMPLIANCE_GATE_PPM),
        ),
        (
            "floor_compliance_ppm",
            serde_json::json!(summary.floor_compliance_ppm),
        ),
        (
            "maximum_latency_ns",
            serde_json::json!(summary.maximum_latency_ns),
        ),
        (
            "minimum_recall_ppm",
            serde_json::json!(summary.minimum_recall_ppm),
        ),
        (
            "p99_latency_gate_ns",
            serde_json::json!(P99_LATENCY_GATE_NS),
        ),
        ("p99_latency_ns", serde_json::json!(summary.p99_latency_ns)),
        ("passed", serde_json::json!(summary.passed)),
        ("peak_rss_bytes", serde_json::json!(peak_rss_bytes()?)),
        ("query_bytes", serde_json::json!(request.query_bytes)),
        ("query_count", serde_json::json!(request.query_count)),
        ("query_floor_ppm", serde_json::json!(QUERY_FLOOR_PPM)),
        ("query_sha256", serde_json::json!(request.query_sha256)),
        ("query_start", serde_json::json!(request.query_start)),
        ("samples_bytes", serde_json::json!(samples_bytes)),
        ("samples_sha256", serde_json::json!(samples_sha256)),
        (
            "schema",
            serde_json::json!(match request.mode {
                Pq4QualifyMode::Development => "borsuk-v26-pq4-development-v1",
                Pq4QualifyMode::SealedHoldout => "borsuk-v26-pq4-sealed-holdout-v1",
            }),
        ),
        (
            "snapshot_manifest_bytes",
            serde_json::json!(request.snapshot_manifest_bytes),
        ),
        (
            "snapshot_manifest_sha256",
            serde_json::json!(request.snapshot_manifest_sha256),
        ),
        ("source_commit", serde_json::json!(request.source_commit)),
        ("truth_bytes", serde_json::json!(request.truth_bytes)),
        ("truth_sha256", serde_json::json!(request.truth_sha256)),
        ("warmup_queries", serde_json::json!(request.warmup_queries)),
    ]);
    let mut bytes = serde_json::to_vec(&result).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    fs::File::create(&request.result_json)
        .and_then(|mut file| file.write_all(&bytes).and_then(|()| file.sync_all()))
        .map_err(|error| format!("result write failed: {error}"))?;
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| format!("stdout write failed: {error}"))
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    use super::{
        HoldoutSample, parse_pq4_qualify_args, read_queries, read_truth, summarize_holdout,
        warmup_query_range,
    };

    fn arguments() -> Vec<String> {
        [
            "pq4-qualify",
            "--snapshot",
            "/data/shard-0000",
            "--query-parquet",
            "/data/test.parquet",
            "--truth-parquet",
            "/data/neighbors.parquet",
            "--result-json",
            "/data/result.json",
            "--samples-parquet",
            "/data/samples.parquet",
            "--binary-sha256",
            "a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef00112233445566778",
            "--binary-bytes",
            "9999999",
            "--snapshot-manifest-sha256",
            "b1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef00112233445566778",
            "--snapshot-manifest-bytes",
            "2048",
            "--query-sha256",
            "c1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef00112233445566778",
            "--query-bytes",
            "3843448",
            "--truth-sha256",
            "d1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef00112233445566778",
            "--truth-bytes",
            "4003585",
            "--source-commit",
            "238aad7823220243d271b2c883168d9f7eb29716",
            "--query-start",
            "512",
            "--query-count",
            "480",
            "--candidate-depth",
            "3072",
            "--query-threads",
            "16",
            "--memory-budget-bytes",
            "3221225472",
            "--admission-timeout-ms",
            "1000",
            "--warmup-queries",
            "32",
            "--execute-sealed-holdout",
        ]
        .map(str::to_owned)
        .to_vec()
    }

    fn set_flag(arguments: &mut [String], flag: &str, value: &str) {
        let index = arguments.iter().position(|item| item == flag).unwrap();
        arguments[index + 1] = value.to_owned();
    }

    #[test]
    fn pq4_qualify_cli_is_explicit_local_parquet_only_and_freezes_the_holdout() {
        let request = parse_pq4_qualify_args(arguments()).unwrap();
        assert_eq!(request.query_start, 512);
        assert_eq!(request.query_count, 480);
        assert_eq!(request.candidate_depth, 3_072);
        assert_eq!(request.query_threads, 16);
        assert_eq!(request.memory_budget_bytes, 3_221_225_472);
        assert_eq!(request.warmup_queries, 32);
        assert_eq!(request.query_bytes, 3_843_448);
        assert_eq!(request.truth_bytes, 4_003_585);

        let mut forbidden = arguments();
        forbidden.splice(
            forbidden.len() - 1..forbidden.len() - 1,
            ["--bucket".to_owned(), "forbidden".to_owned()],
        );
        assert!(parse_pq4_qualify_args(forbidden).is_err());
        let mut duplicate = arguments();
        duplicate.splice(
            duplicate.len() - 1..duplicate.len() - 1,
            ["--query-count".to_owned(), "32".to_owned()],
        );
        assert!(parse_pq4_qualify_args(duplicate).is_err());
        let mut missing_execute = arguments();
        missing_execute.pop();
        assert!(parse_pq4_qualify_args(missing_execute).is_err());
    }

    #[test]
    fn pq4_qualify_development_mode_is_burned_and_freezes_the_thread_ladder() {
        for threads in [4, 8, 16] {
            let mut development = arguments();
            set_flag(&mut development, "--query-start", "0");
            set_flag(&mut development, "--query-count", "512");
            set_flag(&mut development, "--query-threads", &threads.to_string());
            *development.last_mut().unwrap() = "--execute-development".to_owned();
            let request = parse_pq4_qualify_args(development).unwrap();
            assert_eq!(request.query_start, 0);
            assert_eq!(request.query_count, 512);
            assert_eq!(request.query_threads, threads);
            assert_eq!(warmup_query_range(&request).unwrap(), 0..32);
        }

        for threads in [1, 2, 3, 5, 32] {
            let mut development = arguments();
            set_flag(&mut development, "--query-start", "0");
            set_flag(&mut development, "--query-count", "512");
            set_flag(&mut development, "--query-threads", &threads.to_string());
            *development.last_mut().unwrap() = "--execute-development".to_owned();
            assert!(parse_pq4_qualify_args(development).is_err());
        }

        let mut leaked_holdout = arguments();
        *leaked_holdout.last_mut().unwrap() = "--execute-development".to_owned();
        assert!(parse_pq4_qualify_args(leaked_holdout).is_err());
    }

    fn sample(query_ordinal: u32, hits: u32, latency_ns: u64) -> HoldoutSample {
        HoldoutSample {
            query_ordinal,
            match_source_ordinals: std::array::from_fn(|index| index as u64),
            hits,
            recall_ppm: hits * 100_000,
            latency_ns,
        }
    }

    #[test]
    fn pq4_qualify_reads_the_complete_frozen_cross_language_parquet_contract() {
        let directory = tempfile::tempdir().unwrap();
        let queries = directory.path().join("test.parquet");
        let query_values = (0..10_000 * 96)
            .map(|index| if index % 96 == 0 { 1.0 } else { 0.0 })
            .collect::<Vec<_>>();
        let query_vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from(query_values)),
            None,
        )
        .unwrap();
        let query_schema = Arc::new(Schema::new(vec![Field::new(
            "emb",
            query_vectors.data_type().clone(),
            false,
        )]));
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&queries).unwrap(),
            query_schema.clone(),
            None,
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(query_schema, vec![Arc::new(query_vectors) as ArrayRef])
                    .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();

        let truth = directory.path().join("neighbors.parquet");
        let truth_values = (0..10_000 * 100)
            .map(|index| i32::try_from(index % 100).unwrap())
            .collect::<Vec<_>>();
        let truth_lists = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Int32, false)),
            100,
            Arc::new(Int32Array::from(truth_values)),
            None,
        )
        .unwrap();
        let truth_schema = Arc::new(Schema::new(vec![Field::new(
            "neighbors_id",
            truth_lists.data_type().clone(),
            false,
        )]));
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&truth).unwrap(),
            truth_schema.clone(),
            None,
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(truth_schema, vec![Arc::new(truth_lists) as ArrayRef])
                    .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();

        let observed_queries = read_queries(&queries).unwrap();
        let observed_truth = read_truth(&truth).unwrap();
        assert_eq!(observed_queries.len(), 10_000);
        assert_eq!(observed_queries[9_999][0], 1.0);
        assert_eq!(observed_truth.len(), 10_000);
        assert_eq!(
            observed_truth[9_999],
            std::array::from_fn(|index| index as u64)
        );
    }

    #[test]
    fn pq4_qualify_summary_recomputes_literal_release_gates_and_nearest_rank_p99() {
        let mut samples = (0..400)
            .map(|ordinal| sample(512 + ordinal, 10, 10_000_000 + u64::from(ordinal)))
            .collect::<Vec<_>>();
        samples[399].latency_ns = 15_000_000;
        samples[0].hits = 9;
        samples[0].recall_ppm = 900_000;
        let summary = summarize_holdout(&samples, 512).unwrap();
        assert_eq!(summary.aggregate_recall_ppm, 999_750);
        assert_eq!(summary.floor_compliance_ppm, 1_000_000);
        assert_eq!(summary.minimum_recall_ppm, 900_000);
        assert_eq!(summary.p99_latency_ns, 10_000_395);
        assert!(summary.passed);

        let mut quality_failure = samples.clone();
        quality_failure[0].hits = 7;
        quality_failure[0].recall_ppm = 700_000;
        quality_failure[1].hits = 7;
        quality_failure[1].recall_ppm = 700_000;
        assert!(!summarize_holdout(&quality_failure, 512).unwrap().passed);

        let mut latency_failure = samples;
        for row in latency_failure.iter_mut().skip(395) {
            row.latency_ns = 15_000_001;
        }
        assert!(!summarize_holdout(&latency_failure, 512).unwrap().passed);
    }
}

//! Local-only qualification of ten authenticated PQ4 shards.

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
use borsuk::{Pq4ShardedIndex, Pq4ShardedOpenOptions};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use sha2::{Digest, Sha256};

const AGGREGATE_RECALL_GATE_PPM: u32 = 995_000;
const QUERY_RECALL_FLOOR_PPM: u32 = 800_000;
const P99_LATENCY_GATE_NS: u64 = 15_000_000;
const ROWS_SCANNED_PER_QUERY: u64 = 100_000_000;
const CANDIDATES_RERANKED_PER_QUERY: u32 = 30_720;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardAuthority {
    ordinal: u32,
    snapshot: PathBuf,
    manifest_sha256: String,
    manifest_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pq4ShardedQualifyRequest {
    shards: Vec<ShardAuthority>,
    query_parquet: PathBuf,
    query_sha256: String,
    query_bytes: u64,
    truth_parquet: PathBuf,
    truth_sha256: String,
    truth_bytes: u64,
    result_json: PathBuf,
    samples_parquet: PathBuf,
    source_commit: String,
    binary_sha256: String,
    binary_bytes: u64,
    query_start: u32,
    query_count: u32,
    candidate_depth: usize,
    fanout_threads: usize,
    shard_query_threads: usize,
    memory_budget_bytes: u64,
    admission_timeout_ms: u64,
    warmup_queries: u32,
    claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardedHoldoutSample {
    query_ordinal: u32,
    match_source_ordinals: [u64; 10],
    hits: u32,
    recall_ppm: u32,
    latency_ns: u64,
    shard_searches: u32,
    rows_scanned: u64,
    candidates_reranked: u32,
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
        || builder.metadata().file_metadata().num_rows() != 100
    {
        return Err("query Parquet authority differs".to_owned());
    }
    let mut output = Vec::with_capacity(100);
    for batch in builder
        .with_batch_size(100)
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
    if output.len() != 100 {
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
        || builder.metadata().file_metadata().num_rows() != 100
    {
        return Err("truth Parquet authority differs".to_owned());
    }
    let mut output = Vec::with_capacity(100);
    for batch in builder
        .with_batch_size(100)
        .build()
        .map_err(|error| format!("truth reader failed: {error}"))?
    {
        let batch = batch.map_err(|error| format!("truth read failed: {error}"))?;
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| "truth array differs".to_owned())?;
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "truth values differ".to_owned())?;
        if lists.null_count() != 0
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
    if output.len() != 100 {
        return Err("truth row count differs".to_owned());
    }
    Ok(output)
}

fn sha256_file(path: &Path) -> Result<(String, u64), String> {
    let mut file = fs::File::open(path).map_err(|error| format!("open failed: {error}"))?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest).map_err(|error| format!("hash failed: {error}"))?;
    let bytes = file.metadata().map_err(|error| error.to_string())?.len();
    Ok((format!("{:x}", digest.finalize()), bytes))
}

fn authenticate(path: &Path, digest: &str, bytes: u64, role: &str) -> Result<(), String> {
    if sha256_file(path)? != (digest.to_owned(), bytes) {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardedHoldoutSummary {
    aggregate_recall_ppm: u32,
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

fn parse_pq4_sharded_qualify_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Pq4ShardedQualifyRequest, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut values = BTreeMap::new();
    let mut claim_eligible = None;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-reduced" || flag == "--execute-sealed-100m" {
            let value = flag == "--execute-sealed-100m";
            if claim_eligible.replace(value).is_some() {
                return Err("duplicate PQ4 sharded execution mode".to_owned());
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
    let claim_eligible = claim_eligible.ok_or_else(|| "execution mode is absent".to_owned())?;
    let mut shards = Vec::with_capacity(10);
    for ordinal in 0..10 {
        shards.push(ShardAuthority {
            ordinal,
            snapshot: PathBuf::from(take(&mut values, &format!("--snapshot-{ordinal}"))?),
            manifest_sha256: digest(
                &mut values,
                &format!("--snapshot-manifest-sha256-{ordinal}"),
            )?,
            manifest_bytes: positive(&mut values, &format!("--snapshot-manifest-bytes-{ordinal}"))?,
        });
    }
    let request = Pq4ShardedQualifyRequest {
        shards,
        query_parquet: PathBuf::from(take(&mut values, "--query-parquet")?),
        query_sha256: digest(&mut values, "--query-sha256")?,
        query_bytes: positive(&mut values, "--query-bytes")?,
        truth_parquet: PathBuf::from(take(&mut values, "--truth-parquet")?),
        truth_sha256: digest(&mut values, "--truth-sha256")?,
        truth_bytes: positive(&mut values, "--truth-bytes")?,
        result_json: PathBuf::from(take(&mut values, "--result-json")?),
        samples_parquet: PathBuf::from(take(&mut values, "--samples-parquet")?),
        source_commit: take(&mut values, "--source-commit")?,
        binary_sha256: digest(&mut values, "--binary-sha256")?,
        binary_bytes: positive(&mut values, "--binary-bytes")?,
        query_start: take(&mut values, "--query-start")?
            .parse()
            .map_err(|_| "invalid --query-start".to_owned())?,
        query_count: positive(&mut values, "--query-count")?,
        candidate_depth: positive(&mut values, "--candidate-depth")?,
        fanout_threads: positive(&mut values, "--fanout-threads")?,
        shard_query_threads: positive(&mut values, "--shard-query-threads")?,
        memory_budget_bytes: positive(&mut values, "--memory-budget-bytes")?,
        admission_timeout_ms: positive(&mut values, "--admission-timeout-ms")?,
        warmup_queries: positive(&mut values, "--warmup-queries")?,
        claim_eligible,
    };
    if !values.is_empty()
        || request.source_commit.len() != 40
        || !request
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || request
            .shards
            .iter()
            .any(|shard| shard.snapshot.as_os_str().is_empty())
        || request.query_parquet.as_os_str().is_empty()
        || request.truth_parquet.as_os_str().is_empty()
        || request.result_json.as_os_str().is_empty()
        || request.samples_parquet.as_os_str().is_empty()
        || request.query_start != 0
        || request.query_count != 100
        || request.candidate_depth != 3_072
        || request.fanout_threads != 10
        || request.shard_query_threads == 0
        || request.memory_budget_bytes != 3 * 1024 * 1024 * 1024
        || request.admission_timeout_ms != 1_000
        || request.warmup_queries != 32
    {
        return Err("PQ4 sharded qualification arguments differ".to_owned());
    }
    Ok(request)
}

fn summarize_sharded_holdout(
    samples: &[ShardedHoldoutSample],
    first_query_ordinal: u32,
) -> Result<ShardedHoldoutSummary, String> {
    if samples.is_empty()
        || samples.iter().enumerate().any(|(index, sample)| {
            sample.query_ordinal != first_query_ordinal + u32::try_from(index).unwrap()
                || sample.hits > 10
                || sample.recall_ppm != sample.hits * 100_000
                || sample.latency_ns == 0
                || sample.shard_searches != 10
                || sample.rows_scanned != ROWS_SCANNED_PER_QUERY
                || sample.candidates_reranked != CANDIDATES_RERANKED_PER_QUERY
        })
    {
        return Err("PQ4 sharded holdout samples differ".to_owned());
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
    let p99_latency_ns = latencies[(samples.len() * 99).div_ceil(100) - 1];
    let maximum_latency_ns = *latencies.last().unwrap();
    Ok(ShardedHoldoutSummary {
        aggregate_recall_ppm,
        minimum_recall_ppm,
        p99_latency_ns,
        maximum_latency_ns,
        passed: aggregate_recall_ppm >= AGGREGATE_RECALL_GATE_PPM
            && minimum_recall_ppm >= QUERY_RECALL_FLOOR_PPM
            && p99_latency_ns <= P99_LATENCY_GATE_NS,
    })
}

fn write_samples(path: &Path, samples: &[ShardedHoldoutSample]) -> Result<(), String> {
    let matches = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt64, false)),
        10,
        Arc::new(UInt64Array::from_iter_values(
            samples
                .iter()
                .flat_map(|sample| sample.match_source_ordinals),
        )),
        None,
    )
    .map_err(|error| error.to_string())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt32, false),
        Field::new("latency_ns", DataType::UInt64, false),
        Field::new("shard_searches", DataType::UInt32, false),
        Field::new("rows_scanned", DataType::UInt64, false),
        Field::new("candidates_reranked", DataType::UInt32, false),
        Field::new("match_source_ordinals", matches.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.latency_ns),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.shard_searches),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.rows_scanned),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.candidates_reranked),
            )),
            Arc::new(matches),
        ],
    )
    .map_err(|error| error.to_string())?;
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(path).map_err(|error| error.to_string())?,
        schema,
        Some(properties),
    )
    .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(())
}

fn peak_rss_bytes() -> Result<u64, String> {
    let status = fs::read_to_string("/proc/self/status").map_err(|error| error.to_string())?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| "process peak RSS is absent".to_owned())?;
    kib.checked_mul(1_024)
        .ok_or_else(|| "process peak RSS overflows".to_owned())
}

fn run() -> Result<(), String> {
    let request = parse_pq4_sharded_qualify_args(env::args())?;
    if request.result_json.exists() || request.samples_parquet.exists() {
        return Err("PQ4 sharded output already exists".to_owned());
    }
    authenticate(
        &env::current_exe().map_err(|error| error.to_string())?,
        &request.binary_sha256,
        request.binary_bytes,
        "binary",
    )?;
    for shard in &request.shards {
        authenticate(
            &shard.snapshot.join("manifest.json"),
            &shard.manifest_sha256,
            shard.manifest_bytes,
            "snapshot manifest",
        )?;
    }
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
    let shards = request
        .shards
        .iter()
        .map(|shard| (shard.ordinal, shard.snapshot.clone()))
        .collect::<Vec<_>>();
    let index = Pq4ShardedIndex::open(
        &shards,
        Pq4ShardedOpenOptions {
            memory_budget_bytes: request.memory_budget_bytes,
            fanout_threads: request.fanout_threads,
            shard_query_threads: request.shard_query_threads,
            admission_timeout_ms: request.admission_timeout_ms,
        },
    )
    .map_err(|error| error.to_string())?;
    for query in queries.iter().take(request.warmup_queries as usize) {
        index.search(query, 10).map_err(|error| error.to_string())?;
    }
    let query_end = request
        .query_start
        .checked_add(request.query_count)
        .ok_or_else(|| "query range overflows".to_owned())?;
    if query_end as usize > queries.len() {
        return Err("query range differs".to_owned());
    }
    let mut samples = Vec::with_capacity(request.query_count as usize);
    for query_ordinal in request.query_start..query_end {
        let started = Instant::now();
        let matches = index
            .search(&queries[query_ordinal as usize], 10)
            .map_err(|error| error.to_string())?;
        let latency_ns = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| "query latency overflows".to_owned())?;
        let ordered = matches
            .iter()
            .map(|item| decode_source_ordinal(&item.id))
            .collect::<Result<Vec<_>, _>>()?;
        let returned = ordered.iter().copied().collect::<BTreeSet<_>>();
        if returned.len() != 10 {
            return Err("PQ4 sharded result cardinality differs".to_owned());
        }
        let hits = u32::try_from(
            truth[query_ordinal as usize]
                .iter()
                .filter(|source| returned.contains(source))
                .count(),
        )
        .unwrap();
        samples.push(ShardedHoldoutSample {
            query_ordinal,
            match_source_ordinals: ordered
                .try_into()
                .map_err(|_| "PQ4 sharded result cardinality differs".to_owned())?,
            hits,
            recall_ppm: hits * 100_000,
            latency_ns,
            shard_searches: 10,
            rows_scanned: ROWS_SCANNED_PER_QUERY,
            candidates_reranked: CANDIDATES_RERANKED_PER_QUERY,
        });
    }
    let summary = summarize_sharded_holdout(&samples, request.query_start)?;
    write_samples(&request.samples_parquet, &samples)?;
    let (samples_sha256, samples_bytes) = sha256_file(&request.samples_parquet)?;
    let peak_rss_bytes = peak_rss_bytes()?;
    let manifests = request
        .shards
        .iter()
        .map(|shard| {
            serde_json::json!({
                "bytes": shard.manifest_bytes,
                "ordinal": shard.ordinal,
                "sha256": shard.manifest_sha256,
            })
        })
        .collect::<Vec<_>>();
    let result = BTreeMap::from([
        (
            "aggregate_recall_gate_ppm",
            serde_json::json!(AGGREGATE_RECALL_GATE_PPM),
        ),
        (
            "aggregate_recall_ppm",
            serde_json::json!(summary.aggregate_recall_ppm),
        ),
        ("binary_bytes", serde_json::json!(request.binary_bytes)),
        ("binary_sha256", serde_json::json!(request.binary_sha256)),
        (
            "candidate_depth",
            serde_json::json!(request.candidate_depth),
        ),
        ("claim_eligible", serde_json::json!(request.claim_eligible)),
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
        (
            "passed",
            serde_json::json!(summary.passed && peak_rss_bytes < 3 * 1024 * 1024 * 1024),
        ),
        ("peak_rss_bytes", serde_json::json!(peak_rss_bytes)),
        ("query_bytes", serde_json::json!(request.query_bytes)),
        ("query_count", serde_json::json!(request.query_count)),
        ("query_sha256", serde_json::json!(request.query_sha256)),
        (
            "rows_scanned_per_query",
            serde_json::json!(ROWS_SCANNED_PER_QUERY),
        ),
        ("samples_bytes", serde_json::json!(samples_bytes)),
        ("samples_sha256", serde_json::json!(samples_sha256)),
        (
            "schema",
            serde_json::json!("borsuk-v26-pq4-sharded-qualification-v1"),
        ),
        ("snapshot_manifests", serde_json::json!(manifests)),
        ("source_commit", serde_json::json!(request.source_commit)),
        ("truth_bytes", serde_json::json!(request.truth_bytes)),
        ("truth_sha256", serde_json::json!(request.truth_sha256)),
    ]);
    let mut bytes = serde_json::to_vec(&result).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    fs::File::create(&request.result_json)
        .and_then(|mut file| file.write_all(&bytes).and_then(|()| file.sync_all()))
        .map_err(|error| error.to_string())?;
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| error.to_string())
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
    use super::{ShardedHoldoutSample, parse_pq4_sharded_qualify_args, summarize_sharded_holdout};

    fn arguments() -> Vec<String> {
        let mut values = vec!["pq4-sharded-qualify".to_owned()];
        for ordinal in 0..10 {
            values.extend([
                format!("--snapshot-{ordinal}"),
                format!("/data/shard-{ordinal:04}"),
                format!("--snapshot-manifest-sha256-{ordinal}"),
                format!("{ordinal:x}").repeat(64),
                format!("--snapshot-manifest-bytes-{ordinal}"),
                "4096".to_owned(),
            ]);
        }
        values.extend(
            [
                "--query-parquet",
                "/data/test.parquet",
                "--query-sha256",
                "a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef00112233445566778",
                "--query-bytes",
                "3843448",
                "--truth-parquet",
                "/data/neighbors.parquet",
                "--truth-sha256",
                "b1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef00112233445566778",
                "--truth-bytes",
                "4003585",
                "--result-json",
                "/data/result.json",
                "--samples-parquet",
                "/data/samples.parquet",
                "--source-commit",
                "238aad7823220243d271b2c883168d9f7eb29716",
                "--binary-sha256",
                "c1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef00112233445566778",
                "--binary-bytes",
                "9999999",
                "--query-start",
                "0",
                "--query-count",
                "100",
                "--candidate-depth",
                "3072",
                "--fanout-threads",
                "10",
                "--shard-query-threads",
                "1",
                "--memory-budget-bytes",
                "3221225472",
                "--admission-timeout-ms",
                "1000",
                "--warmup-queries",
                "32",
                "--execute-reduced",
            ]
            .map(str::to_owned),
        );
        values
    }

    #[test]
    fn v26_pq4_100m_qualify_cli_is_explicit_local_ten_shard_only() {
        let request = parse_pq4_sharded_qualify_args(arguments()).unwrap();
        assert_eq!(request.shards.len(), 10);
        assert_eq!(request.query_count, 100);
        assert_eq!(request.fanout_threads, 10);
        assert_eq!(request.shard_query_threads, 1);
        assert!(!request.claim_eligible);

        let mut forbidden = arguments();
        forbidden.splice(
            forbidden.len() - 1..forbidden.len() - 1,
            ["--bucket".to_owned(), "forbidden".to_owned()],
        );
        assert!(parse_pq4_sharded_qualify_args(forbidden).is_err());

        let mut missing = arguments();
        let index = missing
            .iter()
            .position(|item| item == "--snapshot-7")
            .unwrap();
        missing.drain(index..index + 2);
        assert!(parse_pq4_sharded_qualify_args(missing).is_err());
    }

    #[test]
    fn v26_pq4_100m_qualify_recomputes_quality_latency_and_work_gates() {
        let mut samples = (0..100)
            .map(|ordinal| ShardedHoldoutSample {
                query_ordinal: ordinal,
                match_source_ordinals: std::array::from_fn(|value| value as u64),
                hits: 10,
                recall_ppm: 1_000_000,
                latency_ns: 10_000_000 + u64::from(ordinal),
                shard_searches: 10,
                rows_scanned: 100_000_000,
                candidates_reranked: 30_720,
            })
            .collect::<Vec<_>>();
        samples[0].hits = 9;
        samples[0].recall_ppm = 900_000;
        let summary = summarize_sharded_holdout(&samples, 0).unwrap();
        assert_eq!(summary.aggregate_recall_ppm, 999_000);
        assert_eq!(summary.minimum_recall_ppm, 900_000);
        assert_eq!(summary.p99_latency_ns, 10_000_098);
        assert!(summary.passed);

        samples[0].rows_scanned = 99_999_999;
        assert!(summarize_sharded_holdout(&samples, 0).is_err());
    }
}

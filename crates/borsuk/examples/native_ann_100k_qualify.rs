//! Authenticated local-Parquet qualification for the production native ANN path.

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
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, IndexStats, LeafMode, RequestCounts,
    SearchOptions, VectorMetric, VectorRecord, recommended_segment_max_vectors,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use serde::Serialize;
use sha2::{Digest, Sha256};

const ROWS: usize = 100_000;
const DIMENSIONS: usize = 768;
const QUERY_COUNT: usize = 1_000;
const NEIGHBORS: usize = 100;
const BULK_LOAD_BATCH_ROWS: usize = 4_096;
const AVERAGE_RECALL_AT_10_GATE_PPM: u32 = 960_000;
const AVERAGE_RECALL_AT_100_GATE_PPM: u32 = 975_000;
const P05_RECALL_AT_100_GATE_PPM: u32 = 900_000;
const RESULT_SCHEMA: &str = "borsuk-bounded-native-100k-qualification-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ArtifactAuthority {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    encoded_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QualificationRequest {
    source: ArtifactAuthority,
    queries: ArtifactAuthority,
    truth: ArtifactAuthority,
    index_uri: PathBuf,
    samples: PathBuf,
    result: PathBuf,
    source_commit: String,
    rows: usize,
    dimensions: usize,
    query_count: usize,
    neighbors: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SampleEvidence {
    query_ordinal: u32,
    returned_ids: Vec<u64>,
    hits_at_10: u32,
    hits_at_100: u32,
    recall_at_10_ppm: u32,
    recall_at_100_ppm: u32,
    physical_gets: u64,
    pages_read: u64,
    bytes_read: u64,
    records_scored: u64,
    latency_ns: u64,
}

#[cfg(test)]
impl SampleEvidence {
    fn literal(
        query_ordinal: u32,
        hits_at_10: u32,
        hits_at_100: u32,
        physical_gets: u64,
        pages_read: u64,
        bytes_read: u64,
        latency_ns: u64,
    ) -> Self {
        Self {
            query_ordinal,
            returned_ids: Vec::new(),
            hits_at_10,
            hits_at_100,
            recall_at_10_ppm: hits_at_10 * 100_000,
            recall_at_100_ppm: hits_at_100 * 10_000,
            physical_gets,
            pages_read,
            bytes_read,
            records_scored: 1,
            latency_ns,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct QualificationSummary {
    average_recall_at_10_ppm: u32,
    average_recall_at_100_ppm: u32,
    p05_recall_at_100_ppm: u32,
    worst_recall_at_100_ppm: u32,
    p50_latency_ns: u64,
    p95_latency_ns: u64,
    p99_latency_ns: u64,
    total_gets: u64,
    total_pages_read: u64,
    total_bytes: u64,
    total_records_scored: u64,
    passed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SemanticEquivalence {
    one_run: bool,
    ten_run: bool,
    hundred_run: bool,
    pending_put: bool,
    pending_delete: bool,
    reopen: bool,
    compaction: bool,
}

#[derive(Debug, Serialize)]
struct OutputIdentity {
    role: &'static str,
    sha256: String,
    encoded_bytes: u64,
}

#[derive(Debug, Serialize)]
struct QualificationResult {
    schema: &'static str,
    claim_eligible: bool,
    source_commit: String,
    rows: usize,
    dimensions: usize,
    query_count: usize,
    neighbors: usize,
    average_recall_at_10_gate_ppm: u32,
    average_recall_at_100_gate_ppm: u32,
    p05_recall_at_100_gate_ppm: u32,
    inputs: [ArtifactAuthority; 3],
    samples: OutputIdentity,
    build_wall_ns: u64,
    query_wall_ns: u64,
    peak_rss_bytes: u64,
    index_stats: IndexStats,
    summary: QualificationSummary,
    equivalence: SemanticEquivalence,
}

fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn next_value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    arguments
        .next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_role(
    arguments: &mut impl Iterator<Item = String>,
    role: &str,
) -> Result<ArtifactAuthority, String> {
    let path = PathBuf::from(next_value(arguments, role)?);
    let uri = next_value(arguments, role)?;
    let sha256 = next_value(arguments, role)?;
    let encoded_bytes = next_value(arguments, role)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("invalid {role} byte length"))?;
    let parsed_uri = url::Url::parse(&uri).map_err(|_| format!("invalid {role} URI"))?;
    if path.as_os_str().is_empty()
        || parsed_uri.scheme().is_empty()
        || parsed_uri.path().is_empty()
        || parsed_uri.query().is_some()
        || parsed_uri.fragment().is_some()
        || !valid_hex(&sha256, 64)
    {
        return Err(format!("invalid {role} authority"));
    }
    Ok(ArtifactAuthority {
        role: role.trim_start_matches("--").to_owned(),
        path,
        uri,
        sha256,
        encoded_bytes,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T, flag: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("duplicate flag {flag}"));
    }
    Ok(())
}

fn parse_args(arguments: impl IntoIterator<Item = String>) -> Result<QualificationRequest, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut source = None;
    let mut queries = None;
    let mut truth = None;
    let mut singles = BTreeMap::<String, String>::new();
    let mut execute = false;
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--source" => {
                let value = parse_role(&mut arguments, &flag)?;
                set_once(&mut source, value, &flag)?;
            }
            "--queries" => {
                let value = parse_role(&mut arguments, &flag)?;
                set_once(&mut queries, value, &flag)?;
            }
            "--truth" => {
                let value = parse_role(&mut arguments, &flag)?;
                set_once(&mut truth, value, &flag)?;
            }
            "--index-uri" | "--samples" | "--result" | "--source-commit" => {
                let value = next_value(&mut arguments, &flag)?;
                if singles.insert(flag.clone(), value).is_some() {
                    return Err(format!("duplicate flag {flag}"));
                }
            }
            "--execute-native-ann-100k" if !execute => execute = true,
            _ => return Err(format!("unknown or duplicate flag {flag}")),
        }
    }
    let mut take = |flag: &str| {
        singles
            .remove(flag)
            .ok_or_else(|| format!("missing required flag {flag}"))
    };
    let index_uri = PathBuf::from(take("--index-uri")?);
    let samples = PathBuf::from(take("--samples")?);
    let result = PathBuf::from(take("--result")?);
    let source_commit = take("--source-commit")?;
    if !execute
        || !singles.is_empty()
        || index_uri.as_os_str().is_empty()
        || samples.as_os_str().is_empty()
        || result.as_os_str().is_empty()
        || !valid_hex(&source_commit, 40)
    {
        return Err("native ANN 100k qualification arguments differ".to_owned());
    }
    Ok(QualificationRequest {
        source: source.ok_or_else(|| "source authority is absent".to_owned())?,
        queries: queries.ok_or_else(|| "query authority is absent".to_owned())?,
        truth: truth.ok_or_else(|| "truth authority is absent".to_owned())?,
        index_uri,
        samples,
        result,
        source_commit,
        rows: ROWS,
        dimensions: DIMENSIONS,
        query_count: QUERY_COUNT,
        neighbors: NEIGHBORS,
    })
}

fn rounded_ppm(numerator: u64, denominator: u64) -> Result<u32, String> {
    let scaled = numerator
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(denominator / 2))
        .ok_or_else(|| "recall aggregation overflows".to_owned())?;
    u32::try_from(scaled / denominator).map_err(|_| "recall aggregation overflows".to_owned())
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    sorted[(sorted.len() * percentile).div_ceil(100).saturating_sub(1)]
}

fn summarize_samples(samples: &[SampleEvidence]) -> Result<QualificationSummary, String> {
    if samples.is_empty() {
        return Err("native ANN sample evidence is empty".to_owned());
    }
    for (index, sample) in samples.iter().enumerate() {
        if sample.query_ordinal != u32::try_from(index).unwrap_or(u32::MAX)
            || sample.hits_at_10 > 10
            || sample.hits_at_100 > 100
            || sample.recall_at_10_ppm != sample.hits_at_10 * 100_000
            || sample.recall_at_100_ppm != sample.hits_at_100 * 10_000
            || sample.physical_gets == 0
            || sample.pages_read == 0
            || sample.bytes_read == 0
            || sample.records_scored == 0
            || sample.latency_ns == 0
        {
            return Err(format!(
                "native ANN sample evidence differs at row {index}: {sample:?}"
            ));
        }
    }
    let count = u64::try_from(samples.len()).map_err(|_| "sample count overflows".to_owned())?;
    let average_recall_at_10_ppm = rounded_ppm(
        samples
            .iter()
            .map(|sample| u64::from(sample.hits_at_10))
            .sum(),
        count * 10,
    )?;
    let average_recall_at_100_ppm = rounded_ppm(
        samples
            .iter()
            .map(|sample| u64::from(sample.hits_at_100))
            .sum(),
        count * 100,
    )?;
    let mut recall_at_100 = samples
        .iter()
        .map(|sample| sample.recall_at_100_ppm)
        .collect::<Vec<_>>();
    recall_at_100.sort_unstable();
    let p05_recall_at_100_ppm = recall_at_100[(samples.len() * 5).div_ceil(100).saturating_sub(1)];
    let worst_recall_at_100_ppm = recall_at_100[0];
    let mut latencies = samples
        .iter()
        .map(|sample| sample.latency_ns)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let total_gets = samples.iter().map(|sample| sample.physical_gets).sum();
    let total_pages_read = samples.iter().map(|sample| sample.pages_read).sum();
    let total_bytes = samples.iter().map(|sample| sample.bytes_read).sum();
    let total_records_scored = samples.iter().map(|sample| sample.records_scored).sum();
    Ok(QualificationSummary {
        average_recall_at_10_ppm,
        average_recall_at_100_ppm,
        p05_recall_at_100_ppm,
        worst_recall_at_100_ppm,
        p50_latency_ns: percentile(&latencies, 50),
        p95_latency_ns: percentile(&latencies, 95),
        p99_latency_ns: percentile(&latencies, 99),
        total_gets,
        total_pages_read,
        total_bytes,
        total_records_scored,
        passed: average_recall_at_10_ppm >= AVERAGE_RECALL_AT_10_GATE_PPM
            && average_recall_at_100_ppm >= AVERAGE_RECALL_AT_100_GATE_PPM
            && p05_recall_at_100_ppm >= P05_RECALL_AT_100_GATE_PPM,
    })
}

fn recall_hits(returned_ids: &[u64], expected: &[u64; NEIGHBORS]) -> Result<(u32, u32), String> {
    let returned_all = returned_ids.iter().copied().collect::<BTreeSet<_>>();
    let expected_all = expected.iter().copied().collect::<BTreeSet<_>>();
    if returned_ids.len() != NEIGHBORS
        || returned_all.len() != NEIGHBORS
        || expected_all.len() != NEIGHBORS
    {
        return Err("native ANN recall evidence cardinality differs".to_owned());
    }
    let returned_at_10 = returned_ids[..10].iter().copied().collect::<BTreeSet<_>>();
    let hits_at_10 = expected[..10]
        .iter()
        .filter(|id| returned_at_10.contains(id))
        .count() as u32;
    let hits_at_100 = expected
        .iter()
        .filter(|id| returned_all.contains(id))
        .count() as u32;
    Ok((hits_at_10, hits_at_100))
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

fn authenticate(authority: &ArtifactAuthority) -> Result<(), String> {
    if sha256_file(&authority.path)? != (authority.sha256.clone(), authority.encoded_bytes) {
        return Err(format!("{} authority differs", authority.role));
    }
    Ok(())
}

fn vector_type(child_name: &str) -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new(child_name, DataType::Float32, false)),
        DIMENSIONS as i32,
    )
}

fn source_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("embedding", vector_type("element"), false),
    ])
}

fn query_schema() -> Schema {
    Schema::new(vec![
        Field::new("query", DataType::UInt32, false),
        Field::new("vector", vector_type("element"), false),
    ])
}

fn truth_schema() -> Schema {
    Schema::new(vec![
        Field::new("query", DataType::UInt32, false),
        Field::new(
            "neighbors",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Int64, false)),
                NEIGHBORS as i32,
            ),
            false,
        ),
    ])
}

fn vector_rows(array: &dyn Array, rows: usize, role: &str) -> Result<Vec<Vec<f32>>, String> {
    let vectors = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| format!("{role} vectors differ"))?;
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| format!("{role} vector values differ"))?;
    if vectors.null_count() != 0
        || values.null_count() != 0
        || values.len() != rows * DIMENSIONS
        || values.values().iter().any(|value| !value.is_finite())
    {
        return Err(format!("{role} vector values differ"));
    }
    Ok(values
        .values()
        .chunks_exact(DIMENSIONS)
        .map(<[f32]>::to_vec)
        .collect())
}

fn read_source(path: &Path) -> Result<Vec<VectorRecord>, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(path).map_err(|error| format!("source open failed: {error}"))?,
    )
    .map_err(|error| format!("source metadata failed: {error}"))?;
    if builder.schema().as_ref() != &source_schema()
        || builder.metadata().file_metadata().num_rows() != ROWS as i64
    {
        return Err("source Parquet authority differs".to_owned());
    }
    let mut records = Vec::with_capacity(ROWS);
    let mut unique = BTreeSet::new();
    for batch in builder
        .with_batch_size(4_096)
        .build()
        .map_err(|error| format!("source reader failed: {error}"))?
    {
        let batch = batch.map_err(|error| format!("source read failed: {error}"))?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| "source IDs differ".to_owned())?;
        let vectors = vector_rows(batch.column(1).as_ref(), batch.num_rows(), "source")?;
        if ids.null_count() != 0 {
            return Err("source IDs differ".to_owned());
        }
        for (id, vector) in ids.values().iter().copied().zip(vectors) {
            if id > i64::MAX as u64 || !unique.insert(id) {
                return Err("source ID authority differs".to_owned());
            }
            records.push(VectorRecord::new(id.to_string(), vector));
        }
    }
    if records.len() != ROWS {
        return Err("source row count differs".to_owned());
    }
    Ok(records)
}

fn read_queries(path: &Path) -> Result<Vec<Vec<f32>>, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(path).map_err(|error| format!("query open failed: {error}"))?,
    )
    .map_err(|error| format!("query metadata failed: {error}"))?;
    if builder.schema().as_ref() != &query_schema()
        || builder.metadata().file_metadata().num_rows() != QUERY_COUNT as i64
    {
        return Err("query Parquet authority differs".to_owned());
    }
    let mut output = Vec::with_capacity(QUERY_COUNT);
    for batch in builder
        .with_batch_size(1_000)
        .build()
        .map_err(|error| format!("query reader failed: {error}"))?
    {
        let batch = batch.map_err(|error| format!("query read failed: {error}"))?;
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| "query ordinals differ".to_owned())?;
        if ordinals.null_count() != 0
            || ordinals
                .values()
                .iter()
                .copied()
                .enumerate()
                .any(|(row, value)| value != u32::try_from(output.len() + row).unwrap_or(u32::MAX))
        {
            return Err("query ordinals differ".to_owned());
        }
        output.extend(vector_rows(
            batch.column(1).as_ref(),
            batch.num_rows(),
            "query",
        )?);
    }
    if output.len() != QUERY_COUNT {
        return Err("query row count differs".to_owned());
    }
    Ok(output)
}

fn read_truth(path: &Path) -> Result<Vec<[u64; NEIGHBORS]>, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(path).map_err(|error| format!("truth open failed: {error}"))?,
    )
    .map_err(|error| format!("truth metadata failed: {error}"))?;
    if builder.schema().as_ref() != &truth_schema()
        || builder.metadata().file_metadata().num_rows() != QUERY_COUNT as i64
    {
        return Err("truth Parquet authority differs".to_owned());
    }
    let mut output = Vec::with_capacity(QUERY_COUNT);
    for batch in builder
        .with_batch_size(1_000)
        .build()
        .map_err(|error| format!("truth reader failed: {error}"))?
    {
        let batch = batch.map_err(|error| format!("truth read failed: {error}"))?;
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| "truth ordinals differ".to_owned())?;
        let neighbors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| "truth neighbors differ".to_owned())?;
        let values = neighbors
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "truth neighbor values differ".to_owned())?;
        if ordinals.null_count() != 0
            || neighbors.null_count() != 0
            || values.null_count() != 0
            || values.len() != batch.num_rows() * NEIGHBORS
        {
            return Err("truth values differ".to_owned());
        }
        for row in 0..batch.num_rows() {
            if ordinals.value(row) != u32::try_from(output.len()).unwrap_or(u32::MAX) {
                return Err("truth ordinals differ".to_owned());
            }
            let mut ids = [0_u64; NEIGHBORS];
            let mut unique = BTreeSet::new();
            for (position, slot) in ids.iter_mut().enumerate() {
                let value = values.value(row * NEIGHBORS + position);
                if value < 0 || !unique.insert(value) {
                    return Err("truth neighbor authority differs".to_owned());
                }
                *slot = value as u64;
            }
            output.push(ids);
        }
    }
    if output.len() != QUERY_COUNT {
        return Err("truth row count differs".to_owned());
    }
    Ok(output)
}

fn samples_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("hits_at_10", DataType::UInt32, false),
        Field::new("hits_at_100", DataType::UInt32, false),
        Field::new("recall_at_10_ppm", DataType::UInt32, false),
        Field::new("recall_at_100_ppm", DataType::UInt32, false),
        Field::new("physical_gets", DataType::UInt64, false),
        Field::new("pages_read", DataType::UInt64, false),
        Field::new("bytes_read", DataType::UInt64, false),
        Field::new("records_scored", DataType::UInt64, false),
        Field::new("latency_ns", DataType::UInt64, false),
        Field::new(
            "returned_ids",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::UInt64, false)),
                NEIGHBORS as i32,
            ),
            false,
        ),
    ])
}

fn write_samples(path: &Path, samples: &[SampleEvidence]) -> Result<(), String> {
    let schema = Arc::new(samples_schema());
    let returned_ids = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt64, false)),
        NEIGHBORS as i32,
        Arc::new(UInt64Array::from_iter_values(
            samples
                .iter()
                .flat_map(|sample| sample.returned_ids.iter().copied()),
        )),
        None,
    )
    .map_err(|error| format!("sample IDs failed: {error}"))?;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits_at_10),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits_at_100),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_at_10_ppm),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_at_100_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.physical_gets),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.pages_read),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.bytes_read),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.records_scored),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.latency_ns),
            )),
            Arc::new(returned_ids),
        ],
    )
    .map_err(|error| format!("sample batch failed: {error}"))?;
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(Default::default()))
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
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|line| line.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|kib| kib.checked_mul(1_024))
        .ok_or_else(|| "process peak RSS is absent".to_owned())
}

fn elapsed_ns(started: Instant) -> Result<u64, String> {
    u64::try_from(started.elapsed().as_nanos()).map_err(|_| "elapsed time overflows".to_owned())
}

fn native_route_io(requests: &RequestCounts, bytes_read: u64) -> Result<(u64, u64), String> {
    if requests.gets == 0 || bytes_read == 0 {
        return Err("native ANN route I/O evidence differs".to_owned());
    }
    Ok((requests.gets, bytes_read))
}

fn bounded_search_ids(index: &BorsukIndex, query: &[f32], k: usize) -> Result<Vec<u64>, String> {
    let report = index
        .search_with_report(query, SearchOptions::approx(k, LeafMode::PqScan))
        .map_err(|error| error.to_string())?;
    if report.leaf_mode != "native-bounded-sq8" || report.hits.len() != k {
        return Err("bounded native semantic dispatch differs".to_owned());
    }
    report
        .hits
        .iter()
        .map(|hit| {
            hit.id
                .parse::<u64>()
                .map_err(|_| "bounded native semantic result ID differs".to_owned())
        })
        .collect()
}

fn repeated_query_matches(
    index: &BorsukIndex,
    query: &[f32],
    expected: &[u64],
    repetitions: usize,
) -> Result<bool, String> {
    for _ in 0..repetitions {
        if bounded_search_ids(index, query, expected.len())? != expected {
            return Ok(false);
        }
    }
    Ok(true)
}

fn verify_semantic_equivalence(
    index: &mut BorsukIndex,
    query: &[f32],
    index_uri: &Path,
) -> Result<SemanticEquivalence, String> {
    let baseline = bounded_search_ids(index, query, NEIGHBORS.min(100))?;
    let one_run = repeated_query_matches(index, query, &baseline, 1)?;
    let ten_run = repeated_query_matches(index, query, &baseline, 10)?;
    let hundred_run = repeated_query_matches(index, query, &baseline, 100)?;

    let mutation_id = 10_000_000_000_u64;
    let mutation_query = vec![-100.0_f32; query.len()];
    index
        .add(vec![VectorRecord::new(
            mutation_id.to_string(),
            mutation_query.clone(),
        )])
        .map_err(|error| error.to_string())?;
    let pending_put = bounded_search_ids(index, &mutation_query, 1)? == [mutation_id];
    index
        .delete([mutation_id.to_string()])
        .map_err(|error| error.to_string())?;
    let pending_delete = bounded_search_ids(index, &mutation_query, 1)? != [mutation_id];
    index
        .add(vec![VectorRecord::new(
            mutation_id.to_string(),
            mutation_query.clone(),
        )])
        .map_err(|error| error.to_string())?;
    index.flush().map_err(|error| error.to_string())?;

    let index_uri = index_uri
        .to_str()
        .ok_or_else(|| "bounded native index path is not UTF-8".to_owned())?;
    let mut reopened = BorsukIndex::open(index_uri).map_err(|error| error.to_string())?;
    let before_compaction = bounded_search_ids(&reopened, &mutation_query, 1)?;
    let reopen = before_compaction == [mutation_id];
    let report = reopened
        .compact(CompactionOptions::default())
        .map_err(|error| error.to_string())?;
    let after_compaction = bounded_search_ids(&reopened, &mutation_query, 1)?;
    let compaction = report.compacted && after_compaction == before_compaction;

    Ok(SemanticEquivalence {
        one_run,
        ten_run,
        hundred_run,
        pending_put,
        pending_delete,
        reopen,
        compaction,
    })
}

fn run() -> Result<(), String> {
    let request = parse_args(env::args())?;
    if request.index_uri.exists() || request.samples.exists() || request.result.exists() {
        return Err("native ANN qualification output already exists".to_owned());
    }
    for authority in [&request.source, &request.queries, &request.truth] {
        authenticate(authority)?;
    }
    let queries = read_queries(&request.queries.path)?;
    let truth = read_truth(&request.truth.path)?;
    let records = read_source(&request.source.path)?;

    let build_started = Instant::now();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: request.index_uri.to_string_lossy().into_owned(),
        metric: VectorMetric::SquaredEuclidean,
        dimensions: DIMENSIONS,
        segment_max_vectors: recommended_segment_max_vectors(DIMENSIONS),
        ram_budget_bytes: Some(3 * 1_024 * 1_024 * 1_024),
        text: false,
        named_vectors: Default::default(),
    })
    .map_err(|error| error.to_string())?;
    let mut records = records.into_iter();
    for source_shard in 0_u8.. {
        let batch = records
            .by_ref()
            .take(BULK_LOAD_BATCH_ROWS)
            .collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        let ids = batch
            .iter()
            .map(|record| {
                record
                    .id
                    .to_utf8_string()
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let vectors = batch.into_iter().map(|record| record.vector).collect();
        index
            .bulk_load_vectors_with_unique_ids_on_source_shard(source_shard, vectors, ids)
            .map_err(|error| error.to_string())?;
    }
    index
        .finish_bulk_load()
        .map_err(|error| error.to_string())?;
    let build_wall_ns = elapsed_ns(build_started)?;

    let query_started = Instant::now();
    let mut samples = Vec::with_capacity(QUERY_COUNT);
    for (query_ordinal, (query, expected)) in queries.iter().zip(&truth).enumerate() {
        let started = Instant::now();
        let report = index
            .search_with_report(query, SearchOptions::approx(NEIGHBORS, LeafMode::PqScan))
            .map_err(|error| error.to_string())?;
        let latency_ns = elapsed_ns(started)?;
        if report.leaf_mode != "native-bounded-sq8" || report.hits.len() != NEIGHBORS {
            return Err("native ANN dispatch or result cardinality differs".to_owned());
        }
        let returned_ids = report
            .hits
            .iter()
            .map(|hit| {
                hit.id
                    .parse::<u64>()
                    .map_err(|_| "native ANN result ID differs".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (hits_at_10, hits_at_100) = recall_hits(&returned_ids, expected)?;
        let (physical_gets, bytes_read) = native_route_io(&report.requests, report.bytes_read)?;
        samples.push(SampleEvidence {
            query_ordinal: query_ordinal as u32,
            returned_ids,
            hits_at_10,
            hits_at_100,
            recall_at_10_ppm: hits_at_10 * 100_000,
            recall_at_100_ppm: hits_at_100 * 10_000,
            physical_gets,
            pages_read: report.global_leaf_pages_read as u64,
            bytes_read,
            records_scored: report.records_scored as u64,
            latency_ns,
        });
    }
    let query_wall_ns = elapsed_ns(query_started)?;
    write_samples(&request.samples, &samples)?;
    let summary = summarize_samples(&samples)?;
    let equivalence = verify_semantic_equivalence(
        &mut index,
        queries
            .first()
            .ok_or_else(|| "query authority is empty".to_owned())?,
        &request.index_uri,
    )?;
    let (samples_sha256, samples_bytes) = sha256_file(&request.samples)?;
    let result = QualificationResult {
        schema: RESULT_SCHEMA,
        claim_eligible: false,
        source_commit: request.source_commit,
        rows: request.rows,
        dimensions: request.dimensions,
        query_count: request.query_count,
        neighbors: request.neighbors,
        average_recall_at_10_gate_ppm: AVERAGE_RECALL_AT_10_GATE_PPM,
        average_recall_at_100_gate_ppm: AVERAGE_RECALL_AT_100_GATE_PPM,
        p05_recall_at_100_gate_ppm: P05_RECALL_AT_100_GATE_PPM,
        inputs: [request.source, request.queries, request.truth],
        samples: OutputIdentity {
            role: "per-query-samples",
            sha256: samples_sha256,
            encoded_bytes: samples_bytes,
        },
        build_wall_ns,
        query_wall_ns,
        peak_rss_bytes: peak_rss_bytes()?,
        index_stats: index.stats(),
        summary,
        equivalence,
    };
    let canonical = serde_json::to_value(&result).map_err(|error| error.to_string())?;
    let mut bytes = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    fs::File::create(&request.result)
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
    use super::*;

    fn args() -> Vec<String> {
        vec![
            "native-ann-100k".into(),
            "--source".into(),
            "/data/source.parquet".into(),
            "s3://authority/source.parquet".into(),
            "a".repeat(64),
            "123".into(),
            "--queries".into(),
            "/data/queries.parquet".into(),
            "s3://authority/queries.parquet".into(),
            "b".repeat(64),
            "456".into(),
            "--truth".into(),
            "/data/truth.parquet".into(),
            "s3://authority/truth.parquet".into(),
            "c".repeat(64),
            "789".into(),
            "--index-uri".into(),
            "/data/index".into(),
            "--samples".into(),
            "/data/samples.parquet".into(),
            "--result".into(),
            "/data/result.json".into(),
            "--source-commit".into(),
            "d".repeat(40),
            "--execute-native-ann-100k".into(),
        ]
    }

    #[test]
    fn native_ann_100k_cli_freezes_three_roles_and_has_no_tuning_or_storage_surface() {
        let request = parse_args(args()).unwrap();

        assert_eq!(request.source.role, "source");
        assert_eq!(request.queries.role, "queries");
        assert_eq!(request.truth.role, "truth");
        assert_eq!(request.rows, 100_000);
        assert_eq!(request.dimensions, 768);
        assert_eq!(request.query_count, 1_000);
        assert_eq!(request.neighbors, 100);

        for forbidden in [
            "--bucket",
            "--endpoint",
            "--page-prefix",
            "--rows",
            "--dimensions",
            "--queries-count",
            "--neighbors",
            "--max-segments",
            "--candidate-depth",
        ] {
            let mut changed = args();
            changed.extend([forbidden.into(), "1".into()]);
            assert!(parse_args(changed).is_err(), "accepted {forbidden}");
        }
        let mut duplicate = args();
        duplicate.extend([
            "--source".into(),
            "/other".into(),
            "s3://authority/other".into(),
            "e".repeat(64),
            "1".into(),
        ]);
        assert!(parse_args(duplicate).is_err());
    }

    #[test]
    fn native_ann_100k_summary_recomputes_literal_quality_gates() {
        let samples = vec![
            SampleEvidence::literal(0, 10, 98, 11, 2, 3_000_000, 20_000),
            SampleEvidence::literal(1, 9, 90, 13, 3, 4_000_000, 21_000),
            SampleEvidence::literal(2, 10, 100, 11, 1, 2_000_000, 19_000),
        ];

        let summary = summarize_samples(&samples).unwrap();

        assert_eq!(summary.average_recall_at_10_ppm, 966_667);
        assert_eq!(summary.average_recall_at_100_ppm, 960_000);
        assert_eq!(summary.p05_recall_at_100_ppm, 900_000);
        assert_eq!(summary.total_gets, 35);
        assert_eq!(summary.total_bytes, 9_000_000);
        assert!(!summary.passed);

        let mut noncontiguous = samples;
        noncontiguous[2].query_ordinal = 3;
        assert!(summarize_samples(&noncontiguous).is_err());
    }

    #[test]
    fn native_ann_100k_recall_at_10_does_not_credit_hits_below_returned_rank_10() {
        let mut returned = (100_u64..200).collect::<Vec<_>>();
        returned[50] = 0;
        let truth = std::array::from_fn(|index| index as u64);

        assert_eq!(recall_hits(&returned, &truth).unwrap(), (0, 1));
    }

    #[test]
    fn native_ann_100k_source_schema_matches_authenticated_arrow_child_name() {
        assert_eq!(
            source_schema(),
            Schema::new(vec![
                Field::new("feature_row_id", DataType::UInt64, false),
                Field::new("embedding", vector_type("element"), false),
            ])
        );
    }

    #[test]
    fn native_ann_100k_samples_logical_route_io_not_local_backing_cache() {
        let requests = RequestCounts {
            gets: 3,
            ..RequestCounts::default()
        };

        assert_eq!(native_route_io(&requests, 123_456).unwrap(), (3, 123_456));
        assert!(native_route_io(&RequestCounts::default(), 123_456).is_err());
        assert!(native_route_io(&requests, 0).is_err());
    }

    #[test]
    fn native_ann_100k_semantic_gate_proves_repetition_mutation_reopen_and_compaction() {
        let directory = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create(IndexConfig {
            uri: directory.path().to_string_lossy().into_owned(),
            metric: VectorMetric::SquaredEuclidean,
            dimensions: 64,
            segment_max_vectors: 128,
            ram_budget_bytes: None,
            text: false,
            named_vectors: BTreeMap::new(),
        })
        .unwrap();
        index
            .add(
                (0..520)
                    .map(|row| VectorRecord::new(row.to_string(), vec![row as f32 / 17.0; 64]))
                    .collect(),
            )
            .unwrap();
        index.finish_bulk_load().unwrap();

        let evidence =
            verify_semantic_equivalence(&mut index, &[0.0; 64], directory.path()).unwrap();

        assert_eq!(
            evidence,
            SemanticEquivalence {
                one_run: true,
                ten_run: true,
                hundred_run: true,
                pending_put: true,
                pending_delete: true,
                reopen: true,
                compaction: true,
            }
        );
        assert_eq!(RESULT_SCHEMA, "borsuk-bounded-native-100k-qualification-v1");
    }
}

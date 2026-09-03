//! Search a local authenticated PQ4 Arrow snapshot with one Parquet query.

use std::{collections::BTreeMap, env, io::Write, path::PathBuf, process::ExitCode, sync::Arc};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{Pq4Index, Pq4OpenOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pq4SearchRequest {
    snapshot: PathBuf,
    query_parquet: PathBuf,
    options: Pq4OpenOptions,
    k: usize,
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

fn parse_pq4_search_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Pq4SearchRequest, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut values = BTreeMap::new();
    while let Some(flag) = arguments.next() {
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
    let snapshot = PathBuf::from(take(&mut values, "--snapshot")?);
    let query_parquet = PathBuf::from(take(&mut values, "--query-parquet")?);
    let options = Pq4OpenOptions {
        shard_ordinal: take(&mut values, "--shard-ordinal")?
            .parse::<u32>()
            .map_err(|_| "invalid --shard-ordinal".to_owned())?,
        memory_budget_bytes: positive(&mut values, "--memory-budget-bytes")?,
        query_threads: positive(&mut values, "--query-threads")?,
        admission_timeout_ms: positive(&mut values, "--admission-timeout-ms")?,
    };
    let k = positive(&mut values, "--k")?;
    if snapshot.as_os_str().is_empty() || query_parquet.as_os_str().is_empty() || !values.is_empty()
    {
        return Err("PQ4 search arguments differ".to_owned());
    }
    Ok(Pq4SearchRequest {
        snapshot,
        query_parquet,
        options,
        k,
    })
}

fn query_schema() -> Schema {
    Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        ),
        false,
    )])
}

fn read_query(path: &std::path::Path) -> Result<[f32; 96], String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path).map_err(|error| format!("query open failed: {error}"))?,
    )
    .map_err(|error| format!("query metadata failed: {error}"))?;
    if builder.schema().as_ref() != &query_schema()
        || builder.metadata().file_metadata().num_rows() != 1
    {
        return Err("query Parquet authority differs".to_owned());
    }
    let mut reader = builder
        .with_batch_size(1)
        .build()
        .map_err(|error| format!("query reader failed: {error}"))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|error| format!("query read failed: {error}"))?
        .ok_or_else(|| "query row is absent".to_owned())?;
    if reader.next().is_some() || batch.num_rows() != 1 {
        return Err("query row count differs".to_owned());
    }
    let vectors = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| "query vector array differs".to_owned())?;
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| "query vector values differ".to_owned())?;
    if vectors.null_count() != 0 || values.null_count() != 0 || values.len() != 96 {
        return Err("query vector shape differs".to_owned());
    }
    values
        .values()
        .as_ref()
        .try_into()
        .map_err(|_| "query vector width differs".to_owned())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}

fn run() -> Result<(), String> {
    let request = parse_pq4_search_args(env::args())?;
    let query = read_query(&request.query_parquet)?;
    let index =
        Pq4Index::open(&request.snapshot, request.options).map_err(|error| error.to_string())?;
    let matches = index
        .search(&query, request.k)
        .map_err(|error| error.to_string())?;
    let rows = matches
        .into_iter()
        .map(|item| {
            BTreeMap::from([
                ("id_hex", serde_json::Value::String(hex(&item.id))),
                ("shard_ordinal", serde_json::Value::from(item.shard_ordinal)),
                (
                    "source_ordinal",
                    serde_json::Value::from(item.source_ordinal),
                ),
                (
                    "squared_distance",
                    serde_json::Value::from(item.squared_distance),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let mut bytes = serde_json::to_vec(&rows).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
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
    use super::parse_pq4_search_args;

    fn arguments() -> Vec<String> {
        [
            "pq4-search",
            "--snapshot",
            "/data/shard-0007",
            "--query-parquet",
            "/data/query.parquet",
            "--shard-ordinal",
            "7",
            "--memory-budget-bytes",
            "3221225472",
            "--query-threads",
            "4",
            "--admission-timeout-ms",
            "1000",
            "--k",
            "10",
        ]
        .map(str::to_owned)
        .to_vec()
    }

    #[test]
    fn pq4_search_cli_requires_local_arrow_and_typed_parquet_only() {
        // Break caught: search invents serving defaults or gains a bucket/page/network surface
        // instead of opening one authenticated Arrow shard and one typed Parquet query.
        let request = parse_pq4_search_args(arguments()).unwrap();
        assert_eq!(request.snapshot.to_str(), Some("/data/shard-0007"));
        assert_eq!(request.query_parquet.to_str(), Some("/data/query.parquet"));
        assert_eq!(request.options.shard_ordinal, 7);
        assert_eq!(request.options.memory_budget_bytes, 3_221_225_472);
        assert_eq!(request.options.query_threads, 4);
        assert_eq!(request.options.admission_timeout_ms, 1_000);
        assert_eq!(request.k, 10);
        let mut zero_shard = arguments();
        zero_shard[6] = "0".to_owned();
        assert_eq!(
            parse_pq4_search_args(zero_shard)
                .unwrap()
                .options
                .shard_ordinal,
            0
        );

        let mut duplicate = arguments();
        duplicate.extend(["--k".to_owned(), "11".to_owned()]);
        assert!(parse_pq4_search_args(duplicate).is_err());
        let mut forbidden = arguments();
        forbidden.extend(["--page-prefix".to_owned(), "pages/".to_owned()]);
        assert!(parse_pq4_search_args(forbidden).is_err());
        let mut missing = arguments();
        missing.drain(3..5);
        assert!(parse_pq4_search_args(missing).is_err());
        let mut invalid = arguments();
        invalid[8] = "lots".to_owned();
        assert!(parse_pq4_search_args(invalid).is_err());
    }
}

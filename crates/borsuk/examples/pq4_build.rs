//! Build a local authenticated PQ4 Arrow snapshot from cross-language Parquet input.

use std::{collections::BTreeMap, env, io::Write, path::PathBuf, process::ExitCode};

use borsuk::{Pq4BuildConfig, Pq4Builder};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pq4BuildRequest {
    input: PathBuf,
    output: PathBuf,
    config: Pq4BuildConfig,
}

fn take(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn positive_usize(values: &mut BTreeMap<String, String>, flag: &str) -> Result<usize, String> {
    take(values, flag)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("invalid {flag}"))
}

fn parse_pq4_build_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Pq4BuildRequest, String> {
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
    let input = PathBuf::from(take(&mut values, "--input")?);
    let output = PathBuf::from(take(&mut values, "--output")?);
    let worker_count = positive_usize(&mut values, "--workers")?;
    let batch_rows = positive_usize(&mut values, "--batch-rows")?;
    let generation = take(&mut values, "--generation")?;
    let source_uri = take(&mut values, "--source-uri")?;
    if input.as_os_str().is_empty()
        || output.as_os_str().is_empty()
        || generation.is_empty()
        || source_uri.is_empty()
        || !values.is_empty()
    {
        return Err("PQ4 build arguments differ".to_owned());
    }
    Ok(Pq4BuildRequest {
        input,
        output,
        config: Pq4BuildConfig {
            worker_count,
            batch_rows,
            generation,
            source_uri,
        },
    })
}

fn run() -> Result<(), String> {
    let request = parse_pq4_build_args(env::args())?;
    let report = Pq4Builder::build_parquet(&request.input, &request.output, &request.config)
        .map_err(|error| error.to_string())?;
    let output = BTreeMap::from([
        ("maximum_buffered_rows", report.maximum_buffered_rows as u64),
        ("row_count", report.row_count),
        ("sample_rows", report.sample_rows as u64),
        ("worker_count", report.worker_count as u64),
    ]);
    let mut bytes = serde_json::to_vec(&output).map_err(|error| error.to_string())?;
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
    use super::parse_pq4_build_args;

    fn arguments() -> Vec<String> {
        [
            "pq4-build",
            "--input",
            "/data/input.parquet",
            "--output",
            "/data/shard-0007",
            "--workers",
            "8",
            "--batch-rows",
            "8192",
            "--generation",
            "generation-0001",
            "--source-uri",
            "s3://frozen/input.parquet",
        ]
        .map(str::to_owned)
        .to_vec()
    }

    #[test]
    fn pq4_build_cli_requires_explicit_local_cross_language_contract() {
        // Break caught: the example invents defaults, accepts ambiguous flags, or adds a remote
        // loader/page surface instead of invoking the public Parquet-to-Arrow builder directly.
        let request = parse_pq4_build_args(arguments()).unwrap();
        assert_eq!(request.input.to_str(), Some("/data/input.parquet"));
        assert_eq!(request.output.to_str(), Some("/data/shard-0007"));
        assert_eq!(request.config.worker_count, 8);
        assert_eq!(request.config.batch_rows, 8_192);
        assert_eq!(request.config.generation, "generation-0001");
        assert_eq!(request.config.source_uri, "s3://frozen/input.parquet");

        let mut duplicate = arguments();
        duplicate.extend(["--workers".to_owned(), "4".to_owned()]);
        assert!(parse_pq4_build_args(duplicate).is_err());
        let mut unknown = arguments();
        unknown.extend(["--bucket".to_owned(), "forbidden".to_owned()]);
        assert!(parse_pq4_build_args(unknown).is_err());
        let mut missing = arguments();
        missing.drain(1..3);
        assert!(parse_pq4_build_args(missing).is_err());
        let mut invalid = arguments();
        invalid[6] = "zero".to_owned();
        assert!(parse_pq4_build_args(invalid).is_err());
    }
}

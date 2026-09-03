//! Fresh-process V26 PQ16 serving benchmark with explicit local artifact authority.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26LocalObjectPath, V26ObjectIdentity, V26Pq16GlobalPreflightRequest,
    V26Pq16ServingBenchmarkRequest, V26Pq16ServingRuntimeRequest, run_v26_pq16_global_preflight,
    run_v26_pq16_serving_benchmark,
};

enum V26ServingMode {
    Benchmark(V26Pq16ServingBenchmarkRequest),
    GlobalPreflight(V26Pq16GlobalPreflightRequest),
}

fn take(values: &mut BTreeMap<String, String>, key: &str) -> Result<String, String> {
    values.remove(key).ok_or_else(|| format!("missing {key}"))
}

fn registered(
    values: &mut BTreeMap<String, String>,
    prefix: &str,
    role: &str,
    generation: &str,
) -> Result<V26LocalObjectPath, String> {
    let path = PathBuf::from(take(values, &format!("--{prefix}-path"))?);
    let uri = take(values, &format!("--{prefix}-uri"))?;
    let digest = take(values, &format!("--{prefix}-sha256"))?;
    let encoded_bytes = take(values, &format!("--{prefix}-bytes"))?
        .parse::<u64>()
        .map_err(|_| format!("invalid --{prefix}-bytes"))?;
    if !uri.starts_with("s3://")
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || encoded_bytes == 0
    {
        return Err(format!("invalid --{prefix} authority"));
    }
    Ok(V26LocalObjectPath {
        identity: V26ObjectIdentity {
            role: role.to_owned(),
            uri,
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: generation.to_owned(),
        },
        path,
    })
}

fn parse_args(args: Vec<String>) -> Result<V26ServingMode, String> {
    let mut values = BTreeMap::new();
    let mut mode = None;
    let mut args = args.into_iter();
    while let Some(key) = args.next() {
        if matches!(
            key.as_str(),
            "--execute-pq16-serving" | "--execute-pq16-global-preflight"
        ) {
            if mode.is_some() {
                return Err("duplicate execution mode".to_owned());
            }
            mode = Some(key);
            continue;
        }
        if !key.starts_with("--") {
            return Err(format!("unexpected argument {key}"));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        if values.insert(key.clone(), value).is_some() {
            return Err(format!("duplicate {key}"));
        }
    }
    let mode = mode.ok_or_else(|| "missing execution mode".to_owned())?;
    let generation = take(&mut values, "--generation")?;
    if generation.is_empty() {
        return Err("invalid --generation".to_owned());
    }
    let serving_dir = PathBuf::from(take(&mut values, "--serving-dir")?);
    let latency_output_path = PathBuf::from(take(&mut values, "--latency-output-path")?);
    let latency_output_uri = take(&mut values, "--latency-output-uri")?;
    let serving_manifest = registered(
        &mut values,
        "serving-manifest",
        "pq16-serving-manifest",
        &generation,
    )?;
    let layout_terminal = registered(
        &mut values,
        "layout-terminal",
        "layout-terminal",
        &generation,
    )?;
    let primary_tree = registered(
        &mut values,
        "primary-tree",
        "primary-tree-parquet",
        &generation,
    )?;
    let replica_tree = registered(
        &mut values,
        "replica-tree",
        "replica-tree-parquet",
        &generation,
    )?;
    let external_queries = registered(
        &mut values,
        "external-queries",
        "external-queries-parquet",
        &generation,
    )?;
    if !values.is_empty()
        || !latency_output_uri.starts_with("s3://")
        || !latency_output_uri.ends_with(".parquet")
    {
        return Err("unknown or invalid argument".to_owned());
    }
    let runtime = V26Pq16ServingRuntimeRequest {
        serving_manifest,
        serving_dir,
        layout_terminal,
        primary_tree,
        replica_tree,
        external_queries,
        expected_queries: 512,
    };
    match mode.as_str() {
        "--execute-pq16-serving" => Ok(V26ServingMode::Benchmark(V26Pq16ServingBenchmarkRequest {
            runtime,
            latency_output_path,
            latency_output_uri,
        })),
        "--execute-pq16-global-preflight" => Ok(V26ServingMode::GlobalPreflight(
            V26Pq16GlobalPreflightRequest {
                runtime,
                latency_output_path,
                latency_output_uri,
            },
        )),
        _ => unreachable!(),
    }
}

fn run() -> Result<(), String> {
    let mode = parse_args(std::env::args().skip(1).collect())?;
    let bytes = match mode {
        V26ServingMode::Benchmark(request) => {
            run_v26_pq16_serving_benchmark(&request).map_err(|error| error.to_string())?
        }
        V26ServingMode::GlobalPreflight(request) => {
            run_v26_pq16_global_preflight(&request).map_err(|error| error.to_string())?
        }
    };
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| format!("stdout write failed: {error}"))
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    fn valid_args() -> Vec<String> {
        let mut args = vec![
            "--execute-pq16-serving".to_owned(),
            "--generation".to_owned(),
            "v26-test".to_owned(),
            "--serving-dir".to_owned(),
            "/tmp/serving".to_owned(),
            "--latency-output-path".to_owned(),
            "/tmp/latency.parquet".to_owned(),
            "--latency-output-uri".to_owned(),
            "s3://v26/latency.parquet".to_owned(),
        ];
        for role in [
            "serving-manifest",
            "layout-terminal",
            "primary-tree",
            "replica-tree",
            "external-queries",
        ] {
            args.extend([
                format!("--{role}-path"),
                format!("/tmp/{role}"),
                format!("--{role}-uri"),
                format!("s3://v26/{role}"),
                format!("--{role}-sha256"),
                "a".repeat(64),
                format!("--{role}-bytes"),
                "1024".to_owned(),
            ]);
        }
        args
    }

    #[test]
    fn v26_pq16_serving_cli_requires_explicit_authority_and_mode() {
        let super::V26ServingMode::Benchmark(request) = super::parse_args(valid_args()).unwrap()
        else {
            panic!("serving benchmark mode differs");
        };
        assert_eq!(request.runtime.expected_queries, 512);
        assert_eq!(
            request.runtime.serving_manifest.identity.role,
            "pq16-serving-manifest"
        );
        assert_eq!(
            request.runtime.external_queries.identity.role,
            "external-queries-parquet"
        );
        assert_eq!(request.latency_output_uri, "s3://v26/latency.parquet");
    }

    #[test]
    fn v26_pq16_serving_cli_fails_closed_on_missing_duplicate_and_unknown_flags() {
        let mut missing = valid_args();
        missing.remove(0);
        assert!(super::parse_args(missing).is_err());

        let mut duplicate = valid_args();
        duplicate.extend(["--generation".to_owned(), "other".to_owned()]);
        assert!(super::parse_args(duplicate).is_err());

        let mut unknown = valid_args();
        unknown.extend(["--bucket".to_owned(), "forbidden".to_owned()]);
        assert!(super::parse_args(unknown).is_err());
    }

    #[test]
    fn v26_pq16_serving_cli_rejects_invalid_digest_length_and_numeric_values() {
        let mut digest = valid_args();
        let index = digest
            .iter()
            .position(|value| value == "--primary-tree-sha256")
            .unwrap();
        digest[index + 1] = "a".repeat(63);
        assert!(super::parse_args(digest).is_err());

        let mut bytes = valid_args();
        let index = bytes
            .iter()
            .position(|value| value == "--replica-tree-bytes")
            .unwrap();
        bytes[index + 1] = "zero".to_owned();
        assert!(super::parse_args(bytes).is_err());
    }

    #[test]
    fn v26_pq16_global_preflight_cli_reuses_only_authenticated_serving_artifacts() {
        let mut args = valid_args();
        args[0] = "--execute-pq16-global-preflight".to_owned();
        let super::V26ServingMode::GlobalPreflight(request) = super::parse_args(args).unwrap()
        else {
            panic!("global preflight mode differs");
        };
        assert_eq!(request.runtime.expected_queries, 512);
        assert_eq!(
            request.runtime.serving_manifest.identity.role,
            "pq16-serving-manifest"
        );
        assert_eq!(request.latency_output_uri, "s3://v26/latency.parquet");
    }
}

//! Claim-ineligible, file-backed V36 one-million-row geometry diagnostic.

use std::{
    collections::BTreeMap, env, fs::OpenOptions, io::Write, path::PathBuf, process::ExitCode,
    time::Instant,
};

use borsuk::{
    V36GeometryStop, V36PrefixGeometryConstructionLocalRequest,
    V36PrefixGeometryDevelopmentLocalRequest, evaluate_v36_prefix_geometry_development_scores,
    load_v36_prefix_geometry_construction_local_files,
    load_v36_prefix_geometry_development_local_files, load_v36_prefix_source_feature_ids,
    project_v36_prefix_source_resident, run_v36_resident_projected_posting_diagnostic,
    train_v36_resident_posting_score_model,
};

#[derive(Debug, PartialEq)]
struct Args {
    authority: PathBuf,
    closure_epsilon: Option<f64>,
    development_ground_truth: PathBuf,
    development_query: PathBuf,
    execution_authority: PathBuf,
    output: PathBuf,
    receipt: PathBuf,
    source: PathBuf,
    source_registry: PathBuf,
    workers: u8,
}

fn parse_args(values: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut values = values.into_iter();
    if values.next().as_deref() != Some("--execute-resident-1m-geometry") {
        return Err("V36 geometry execution capability differs".into());
    }
    let mut fields = BTreeMap::new();
    while let Some(flag) = values.next() {
        if !matches!(
            flag.as_str(),
            "--authority"
                | "--closure-epsilon"
                | "--execution-authority"
                | "--development-ground-truth"
                | "--development-query"
                | "--receipt"
                | "--source-registry"
                | "--source"
                | "--output"
                | "--workers"
        ) || fields.contains_key(&flag)
        {
            return Err("V36 geometry argument differs".into());
        }
        let value = values
            .next()
            .ok_or_else(|| "V36 geometry argument value is missing".to_owned())?;
        fields.insert(flag, value);
    }
    let mut take = |flag: &str| {
        fields
            .remove(flag)
            .ok_or_else(|| format!("V36 geometry {flag} is missing"))
    };
    let authority = take("--authority")?.into();
    let closure_epsilon = match take("--closure-epsilon")?.as_str() {
        "none" => None,
        "0.05" => Some(0.05),
        "0.15" => Some(0.15),
        "0.30" => Some(0.30),
        _ => return Err("V36 geometry closure epsilon differs".into()),
    };
    let development_ground_truth = take("--development-ground-truth")?.into();
    let development_query = take("--development-query")?.into();
    let execution_authority = take("--execution-authority")?.into();
    let output = take("--output")?.into();
    let receipt = take("--receipt")?.into();
    let source = take("--source")?.into();
    let source_registry = take("--source-registry")?.into();
    let workers = take("--workers")?
        .parse::<u8>()
        .ok()
        .filter(|workers| matches!(workers, 1 | 2 | 4 | 8 | 16 | 32))
        .ok_or_else(|| "V36 geometry worker count differs".to_owned())?;
    if !fields.is_empty() {
        return Err("V36 geometry arguments differ".into());
    }
    Ok(Args {
        authority,
        closure_epsilon,
        development_ground_truth,
        development_query,
        execution_authority,
        output,
        receipt,
        source,
        source_registry,
        workers,
    })
}

fn stop_name(stop: Option<V36GeometryStop>) -> Option<&'static str> {
    stop.map(|stop| match stop {
        V36GeometryStop::MeanReplication => "mean-replication",
        V36GeometryStop::PrimaryP99 => "primary-p99",
        V36GeometryStop::PrimaryMaximum => "primary-maximum",
        V36GeometryStop::StoredP99 => "stored-p99",
        V36GeometryStop::StoredMaximum => "stored-maximum",
    })
}

fn run(args: Args) -> borsuk::Result<()> {
    const MAXIMUM_BLOCK_ROWS: usize = 65_536;
    const TARGET_PRIMARY_ROWS: u64 = 8_192;

    let whole = Instant::now();
    let authentication = Instant::now();
    let files = load_v36_prefix_geometry_construction_local_files(
        V36PrefixGeometryConstructionLocalRequest {
            authority: args.authority,
            execution_authority: args.execution_authority,
            receipt: args.receipt,
            source: args.source,
            source_registry: args.source_registry,
        },
    )?;
    let construction = files.inputs().construction();
    if construction.corpus_rows() != 1_000_000 {
        return Err(borsuk::BorsukError::InvalidStorage(
            "V36 resident geometry requires exactly one million rows".into(),
        ));
    }
    let source_identity = construction.source().clone();
    let authentication_elapsed_ns = authentication.elapsed().as_nanos();
    let feature_ids =
        load_v36_prefix_source_feature_ids(files.source_path(), construction.corpus_rows())?;
    let projection = Instant::now();
    let projected =
        project_v36_prefix_source_resident(files.source_path(), &feature_ids, MAXIMUM_BLOCK_ROWS)?;
    let projected_corpus_sha256 = projected.projected_corpus_sha256().to_owned();
    let projection_elapsed_ns = projection.elapsed().as_nanos();
    let geometry = Instant::now();
    let diagnostic = run_v36_resident_projected_posting_diagnostic(
        projected,
        construction.corpus_rows(),
        MAXIMUM_BLOCK_ROWS,
        &projected_corpus_sha256,
        TARGET_PRIMARY_ROWS,
        args.closure_epsilon,
        usize::from(args.workers),
    )?;
    let geometry_elapsed_ns = geometry.elapsed().as_nanos();
    let admission = diagnostic.assignments().admission();
    let (containment, evaluation_elapsed_ns) = if admission.stop.is_none() {
        let evaluation = Instant::now();
        let score_model =
            train_v36_resident_posting_score_model(&diagnostic, usize::from(args.workers))?;
        let development = load_v36_prefix_geometry_development_local_files(
            files.inputs(),
            V36PrefixGeometryDevelopmentLocalRequest {
                development_ground_truth: args.development_ground_truth,
                development_query: args.development_query,
            },
        )?;
        let containment = evaluate_v36_prefix_geometry_development_scores(
            &development,
            &feature_ids,
            &diagnostic,
            &score_model,
        )?;
        (Some(containment), evaluation.elapsed().as_nanos())
    } else {
        (None, 0)
    };
    let mut result = BTreeMap::new();
    result.insert("claim_eligible", serde_json::json!(false));
    result.insert(
        "closure_epsilon",
        serde_json::to_value(args.closure_epsilon).unwrap(),
    );
    result.insert(
        "construction_passed",
        serde_json::json!(admission.stop.is_none()),
    );
    result.insert(
        "construction_stop",
        serde_json::to_value(stop_name(admission.stop)).unwrap(),
    );
    result.insert("corpus_rows", serde_json::json!(construction.corpus_rows()));
    result.insert(
        "development_score_containment",
        serde_json::to_value(containment).unwrap(),
    );
    result.insert(
        "mean_replication_ppm",
        serde_json::json!(admission.mean_replication_ppm),
    );
    result.insert(
        "posting_count",
        serde_json::json!(diagnostic.centroids().len()),
    );
    result.insert(
        "primary_maximum",
        serde_json::json!(admission.primary_maximum),
    );
    result.insert("primary_p99", serde_json::json!(admission.primary_p99));
    result.insert(
        "projected_corpus_sha256",
        serde_json::json!(projected_corpus_sha256),
    );
    result.insert("projection", serde_json::json!("srht192-seed36"));
    result.insert(
        "schema",
        serde_json::json!("borsuk-v36-resident-1m-geometry-result-v4"),
    );
    result.insert("source", serde_json::to_value(source_identity).unwrap());
    result.insert(
        "stored_maximum",
        serde_json::json!(admission.stored_maximum),
    );
    result.insert("stored_p99", serde_json::json!(admission.stored_p99));
    result.insert(
        "target_primary_rows",
        serde_json::json!(TARGET_PRIMARY_ROWS),
    );
    result.insert(
        "timing_ns",
        serde_json::json!({
            "authentication": authentication_elapsed_ns,
            "evaluation": evaluation_elapsed_ns,
            "geometry": geometry_elapsed_ns,
            "projection": projection_elapsed_ns,
            "whole": whole.elapsed().as_nanos(),
        }),
    );
    result.insert("workers", serde_json::json!(args.workers));
    let mut bytes = serde_json::to_vec(&result)
        .map_err(|_| borsuk::BorsukError::InvalidStorage("V36 geometry result differs".into()))?;
    bytes.push(b'\n');
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)
        .map_err(|source| borsuk::BorsukError::Io {
            path: args.output.clone(),
            source,
        })?;
    output
        .write_all(&bytes)
        .map_err(|source| borsuk::BorsukError::Io {
            path: args.output,
            source,
        })?;
    Ok(())
}

fn main() -> ExitCode {
    let args = match parse_args(env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    match run(args) {
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
    use std::path::PathBuf;

    fn valid() -> Vec<String> {
        [
            "--execute-resident-1m-geometry",
            "--authority",
            "authority.json",
            "--closure-epsilon",
            "none",
            "--development-ground-truth",
            "development-gt100.parquet",
            "--development-query",
            "development-query.parquet",
            "--execution-authority",
            "execution.json",
            "--receipt",
            "receipt.json",
            "--source-registry",
            "registry.json",
            "--source",
            "source.parquet",
            "--output",
            "result.json",
            "--workers",
            "4",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v36_geometry_oracle_cli_is_explicit_one_million_only_and_storage_free() {
        let parsed = parse_args(valid()).unwrap();
        assert_eq!(parsed.source, PathBuf::from("source.parquet"));
        assert_eq!(parsed.closure_epsilon, None);
        assert_eq!(parsed.workers, 4);
        let mut parallel = valid();
        let index = parallel.iter().position(|value| value == "4").unwrap();
        parallel[index] = "32".into();
        assert_eq!(parse_args(parallel).unwrap().workers, 32);

        for value in ["0.05", "0.15", "0.30"] {
            let mut closure = valid();
            let index = closure
                .iter()
                .position(|candidate| candidate == "none")
                .unwrap();
            closure[index] = value.into();
            assert_eq!(
                parse_args(closure).unwrap().closure_epsilon,
                value.parse().ok()
            );
        }
        let mut closure = valid();
        let index = closure
            .iter()
            .position(|candidate| candidate == "none")
            .unwrap();
        closure[index] = "0.20".into();
        assert!(parse_args(closure).is_err());

        for forbidden in ["--bucket", "--page-prefix", "--endpoint", "--d3"] {
            let mut args = valid();
            args.extend([forbidden.to_owned(), "forbidden".to_owned()]);
            assert!(parse_args(args).is_err());
        }
        let mut duplicate = valid();
        duplicate.extend(["--source".into(), "other.parquet".into()]);
        assert!(parse_args(duplicate).is_err());
        let mut missing = valid();
        missing.drain(1..3);
        assert!(parse_args(missing).is_err());
        let mut workers = valid();
        let index = workers.iter().position(|value| value == "4").unwrap();
        workers[index] = "3".into();
        assert!(parse_args(workers).is_err());
    }
}

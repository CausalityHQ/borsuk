//! One-million-row algorithm probe over immutable local Parquet artifacts.

use std::{env, io::Write, path::PathBuf};

use borsuk::{V41BurnedDiagnosticRequest, run_v41_burned_diagnostic};

fn value(args: &[String], flag: &str) -> Result<String, String> {
    let mut values = args
        .windows(2)
        .filter(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone());
    let value = values.next().ok_or_else(|| format!("missing {flag}"))?;
    if values.next().is_some() {
        return Err(format!("duplicate {flag}"));
    }
    Ok(value)
}

fn run() -> Result<Vec<u8>, String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let expected = [
        "--development-query",
        "--development-gt",
        "--spill-relation",
        "--spill-postings",
        "--v40-direct-selection",
        "--v40-ownership-tree",
        "--training-state",
        "--epochs",
        "--workers",
    ];
    if args.len() != expected.len() * 2
        || args
            .chunks_exact(2)
            .any(|pair| !expected.contains(&pair[0].as_str()))
    {
        return Err("V41 diagnostic arguments differ".to_owned());
    }
    run_v41_burned_diagnostic(V41BurnedDiagnosticRequest {
        development_query: PathBuf::from(value(&args, "--development-query")?),
        development_gt: PathBuf::from(value(&args, "--development-gt")?),
        spill_relation: PathBuf::from(value(&args, "--spill-relation")?),
        spill_postings: PathBuf::from(value(&args, "--spill-postings")?),
        v40_direct_selection: PathBuf::from(value(&args, "--v40-direct-selection")?),
        v40_ownership_tree: PathBuf::from(value(&args, "--v40-ownership-tree")?),
        training_state: PathBuf::from(value(&args, "--training-state")?),
        epochs: value(&args, "--epochs")?
            .parse()
            .map_err(|_| "invalid --epochs".to_owned())?,
        workers: value(&args, "--workers")?
            .parse()
            .map_err(|_| "invalid --workers".to_owned())?,
    })
    .map_err(|error| error.to_string())
}

fn main() {
    match run() {
        Ok(bytes) => {
            if std::io::stdout().write_all(&bytes).is_err() {
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

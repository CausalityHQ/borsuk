//! Throwaway one-million-row hyperplane-forest algorithm probe.

use borsuk::{V41ForestProbeRequest, run_v41_forest_probe};
use std::{env, io::Write, path::PathBuf};

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
        "--source",
        "--training-query",
        "--training-ground-truth",
        "--query",
        "--ground-truth",
        "--spill-relation",
        "--spill-postings",
        "--initial-model",
        "--workers",
    ];
    if args.len() != expected.len() * 2
        || args
            .chunks_exact(2)
            .any(|pair| !expected.contains(&pair[0].as_str()))
    {
        return Err("V41 forest probe arguments differ".to_owned());
    }
    run_v41_forest_probe(V41ForestProbeRequest {
        source: PathBuf::from(value(&args, "--source")?),
        training_query: PathBuf::from(value(&args, "--training-query")?),
        training_ground_truth: PathBuf::from(value(&args, "--training-ground-truth")?),
        query: PathBuf::from(value(&args, "--query")?),
        ground_truth: PathBuf::from(value(&args, "--ground-truth")?),
        spill_relation: PathBuf::from(value(&args, "--spill-relation")?),
        spill_postings: PathBuf::from(value(&args, "--spill-postings")?),
        initial_model: PathBuf::from(value(&args, "--initial-model")?),
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

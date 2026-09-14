//! Disposable ReLAION2B one-million-row PQ4 algorithm probe.

use borsuk::{V42Pq4RelaionProbeRequest, run_v42_pq4_relaion_probe};
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
        "--development-query",
        "--development-ground-truth",
        "--spill-relation",
        "--spill-postings",
        "--scratch",
        "--workers",
    ];
    if args.len() != expected.len() * 2
        || args
            .chunks_exact(2)
            .any(|pair| !expected.contains(&pair[0].as_str()))
    {
        return Err("V42 PQ4 ReLAION probe arguments differ".to_owned());
    }
    run_v42_pq4_relaion_probe(V42Pq4RelaionProbeRequest {
        source: PathBuf::from(value(&args, "--source")?),
        development_query: PathBuf::from(value(&args, "--development-query")?),
        development_ground_truth: PathBuf::from(value(&args, "--development-ground-truth")?),
        spill_relation: PathBuf::from(value(&args, "--spill-relation")?),
        spill_postings: PathBuf::from(value(&args, "--spill-postings")?),
        scratch: PathBuf::from(value(&args, "--scratch")?),
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

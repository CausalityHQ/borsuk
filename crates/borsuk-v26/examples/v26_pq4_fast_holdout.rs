//! Local sealed V26 PQ4 holdout with no page or network execution surface.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26ColdVectorManifest, V26LocalObjectPath, V26ObjectIdentity, V26Pq4HoldoutRequest,
    run_v26_pq4_holdout,
};

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

fn parse_args(args: Vec<String>) -> Result<V26Pq4HoldoutRequest, String> {
    let mut values = BTreeMap::new();
    let mut execute = false;
    let mut args = args.into_iter();
    while let Some(key) = args.next() {
        if key == "--execute-pq4-holdout" {
            if execute {
                return Err("duplicate --execute-pq4-holdout".to_owned());
            }
            execute = true;
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
    if !execute {
        return Err("missing --execute-pq4-holdout".to_owned());
    }
    let generation = take(&mut values, "--generation")?;
    let pq4_dir = PathBuf::from(take(&mut values, "--pq4-dir")?);
    let cold_row_count = take(&mut values, "--cold-row-count")?
        .parse::<u64>()
        .map_err(|_| "invalid --cold-row-count".to_owned())?;
    let cold_batch_rows = take(&mut values, "--cold-batch-rows")?
        .parse::<u32>()
        .map_err(|_| "invalid --cold-batch-rows".to_owned())?;
    let evidence_output_path = PathBuf::from(take(&mut values, "--evidence-output-path")?);
    let evidence_output_uri = take(&mut values, "--evidence-output-uri")?;
    let pq4_manifest = registered(
        &mut values,
        "pq4-manifest",
        "pq4-fast-manifest",
        &generation,
    )?;
    let cold_vectors = registered(
        &mut values,
        "cold-vectors",
        "cold-vectors-arrow",
        &generation,
    )?;
    let layout_terminal = registered(
        &mut values,
        "layout-terminal",
        "layout-terminal",
        &generation,
    )?;
    let external_queries = registered(
        &mut values,
        "external-queries",
        "external-queries-parquet",
        &generation,
    )?;
    let truth = registered(&mut values, "truth", "truth-parquet", &generation)?;
    let frontier_result = registered(
        &mut values,
        "frontier-result",
        "pq4-fast-quality-result",
        &generation,
    )?;
    let development_serving_result = registered(
        &mut values,
        "development-serving-result",
        "pq4-fast-serving-result",
        &generation,
    )?;
    if generation.is_empty()
        || cold_row_count == 0
        || cold_batch_rows == 0
        || !evidence_output_uri.starts_with("s3://")
        || !evidence_output_uri.ends_with(".parquet")
        || !values.is_empty()
    {
        return Err("unknown or invalid argument".to_owned());
    }
    let cold_vectors_manifest = V26ColdVectorManifest {
        row_count: cold_row_count,
        batch_rows: cold_batch_rows,
        encoded_bytes: cold_vectors.identity.encoded_bytes,
        sha256: cold_vectors.identity.digest.clone(),
    };
    Ok(V26Pq4HoldoutRequest {
        pq4_manifest,
        pq4_dir,
        cold_vectors,
        cold_vectors_manifest,
        layout_terminal,
        external_queries,
        truth,
        frontier_result,
        development_serving_result,
        evidence_output_path,
        evidence_output_uri,
    })
}

fn run() -> Result<(), String> {
    let request = parse_args(std::env::args().skip(1).collect())?;
    let bytes = run_v26_pq4_holdout(&request).map_err(|error| error.to_string())?;
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| format!("stdout write failed: {error}"))
}

#[cfg(test)]
mod tests {
    fn valid_args() -> Vec<String> {
        let mut args = vec![
            "--execute-pq4-holdout".to_owned(),
            "--generation".to_owned(),
            "v26-pq4-a0001".to_owned(),
            "--pq4-dir".to_owned(),
            "/tmp/pq4".to_owned(),
            "--cold-row-count".to_owned(),
            "9990000".to_owned(),
            "--cold-batch-rows".to_owned(),
            "65536".to_owned(),
            "--evidence-output-path".to_owned(),
            "/tmp/holdout.parquet".to_owned(),
            "--evidence-output-uri".to_owned(),
            "s3://v26/pq4/holdout.parquet".to_owned(),
        ];
        for role in [
            "pq4-manifest",
            "cold-vectors",
            "layout-terminal",
            "external-queries",
            "truth",
            "frontier-result",
            "development-serving-result",
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
    fn v26_pq4_holdout_cli_requires_both_frozen_development_results() {
        let request = super::parse_args(valid_args()).unwrap();
        assert_eq!(request.cold_vectors_manifest.row_count, 9_990_000);
        assert_eq!(
            request.development_serving_result.identity.role,
            "pq4-fast-serving-result"
        );
        assert_eq!(request.evidence_output_uri, "s3://v26/pq4/holdout.parquet");
    }

    #[test]
    fn v26_pq4_holdout_cli_rejects_cohort_tuning_storage_page_and_d3_surface() {
        for flag in [
            "--ranked-row-limit",
            "--query-start",
            "--query-count",
            "--bucket",
            "--page-prefix",
            "--endpoint",
            "--aws-region",
            "--execute-d3",
        ] {
            let mut args = valid_args();
            args.extend([flag.to_owned(), "forbidden".to_owned()]);
            assert!(super::parse_args(args).is_err(), "accepted {flag}");
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

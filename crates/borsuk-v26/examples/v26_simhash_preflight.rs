//! Strict offline CLI for the V26 SimHash/PQ16 truth-bound preflight.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26LocalObjectPath, V26ObjectIdentity, V26SimHashPreflightRequest, run_v26_simhash_preflight,
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
        || !uri.contains('/')
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

fn parse_args(args: Vec<String>) -> Result<V26SimHashPreflightRequest, String> {
    let mut values = BTreeMap::new();
    let mut execute = false;
    let mut args = args.into_iter();
    while let Some(key) = args.next() {
        if key == "--execute-simhash-preflight" {
            if execute {
                return Err("duplicate --execute-simhash-preflight".to_owned());
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
        return Err("missing --execute-simhash-preflight".to_owned());
    }
    let generation = take(&mut values, "--generation")?;
    let serving_dir = PathBuf::from(take(&mut values, "--serving-dir")?);
    let evidence_output_path = PathBuf::from(take(&mut values, "--evidence-output-path")?);
    let evidence_output_uri = take(&mut values, "--evidence-output-uri")?;
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
    let external_queries = registered(
        &mut values,
        "external-queries",
        "external-queries-parquet",
        &generation,
    )?;
    let truth = registered(&mut values, "truth", "truth-parquet", &generation)?;
    if !values.is_empty()
        || generation.is_empty()
        || !evidence_output_uri.starts_with("s3://")
        || !evidence_output_uri.ends_with(".parquet")
    {
        return Err("unknown or invalid argument".to_owned());
    }
    Ok(V26SimHashPreflightRequest {
        serving_manifest,
        serving_dir,
        layout_terminal,
        external_queries,
        truth,
        evidence_output_path,
        evidence_output_uri,
    })
}

fn run() -> Result<(), String> {
    let request = parse_args(std::env::args().skip(1).collect())?;
    let bytes = run_v26_simhash_preflight(&request).map_err(|error| error.to_string())?;
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
            "--execute-simhash-preflight".to_owned(),
            "--generation".to_owned(),
            "v26-test".to_owned(),
            "--serving-dir".to_owned(),
            "/tmp/serving".to_owned(),
            "--evidence-output-path".to_owned(),
            "/tmp/simhash-preflight.parquet".to_owned(),
            "--evidence-output-uri".to_owned(),
            "s3://v26/simhash-preflight.parquet".to_owned(),
        ];
        for role in [
            "serving-manifest",
            "layout-terminal",
            "external-queries",
            "truth",
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
    fn v26_simhash_preflight_cli_requires_exact_offline_authority() {
        let request = super::parse_args(valid_args()).unwrap();
        assert_eq!(
            request.serving_manifest.identity.role,
            "pq16-serving-manifest"
        );
        assert_eq!(
            request.external_queries.identity.role,
            "external-queries-parquet"
        );
        assert_eq!(request.truth.identity.role, "truth-parquet");
        assert_eq!(
            request.evidence_output_uri,
            "s3://v26/simhash-preflight.parquet"
        );
    }

    #[test]
    fn v26_simhash_preflight_cli_fails_closed_on_missing_duplicate_and_invalid_values() {
        let mut missing = valid_args();
        missing.remove(0);
        assert!(super::parse_args(missing).is_err());

        let mut duplicate = valid_args();
        duplicate.extend(["--generation".to_owned(), "other".to_owned()]);
        assert!(super::parse_args(duplicate).is_err());

        let mut invalid = valid_args();
        let digest = invalid
            .iter()
            .position(|value| value == "--truth-sha256")
            .unwrap();
        invalid[digest + 1] = "f".repeat(63);
        assert!(super::parse_args(invalid).is_err());
    }

    #[test]
    fn v26_simhash_preflight_cli_rejects_storage_d3_and_tuning_surface() {
        for forbidden in [
            "--bucket",
            "--endpoint",
            "--page-prefix",
            "--d3",
            "--bucket-limit",
            "--ranked-row-limit",
        ] {
            let mut args = valid_args();
            args.extend([forbidden.to_owned(), "forbidden".to_owned()]);
            assert!(super::parse_args(args).is_err(), "accepted {forbidden}");
        }
    }
}

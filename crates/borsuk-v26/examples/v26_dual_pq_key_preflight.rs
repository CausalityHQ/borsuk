//! Strict offline CLI for the V26 dual-PQ-key truth-bound preflight.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26DualPqKeyPreflightRequest, V26LocalObjectPath, V26ObjectIdentity,
    build_v26_dual_pq_key_index_from_serving, run_v26_dual_pq_key_preflight,
};

#[derive(Debug)]
struct Args {
    serving_manifest: V26LocalObjectPath,
    serving_dir: PathBuf,
    layout_terminal: V26LocalObjectPath,
    external_queries: V26LocalObjectPath,
    truth: V26LocalObjectPath,
    dual_index_dir: PathBuf,
    offsets_uri: String,
    ordinals_uri: String,
    evidence_output_path: PathBuf,
    evidence_output_uri: String,
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

fn parse_args(args: Vec<String>) -> Result<Args, String> {
    let mut values = BTreeMap::new();
    let mut execute = false;
    let mut args = args.into_iter();
    while let Some(key) = args.next() {
        if key == "--execute-dual-pq-key-preflight" {
            if execute {
                return Err("duplicate --execute-dual-pq-key-preflight".to_owned());
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
        return Err("missing --execute-dual-pq-key-preflight".to_owned());
    }
    let generation = take(&mut values, "--generation")?;
    let serving_dir = PathBuf::from(take(&mut values, "--serving-dir")?);
    let dual_index_dir = PathBuf::from(take(&mut values, "--dual-index-dir")?);
    let offsets_uri = take(&mut values, "--offsets-uri")?;
    let ordinals_uri = take(&mut values, "--ordinals-uri")?;
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
        || !offsets_uri.starts_with("s3://")
        || !offsets_uri.ends_with("pq16-dual-key-offsets.arrow")
        || !ordinals_uri.starts_with("s3://")
        || !ordinals_uri.ends_with("pq16-dual-key-ordinals.arrow")
        || !evidence_output_uri.starts_with("s3://")
        || !evidence_output_uri.ends_with(".parquet")
    {
        return Err("unknown or invalid argument".to_owned());
    }
    Ok(Args {
        serving_manifest,
        serving_dir,
        layout_terminal,
        external_queries,
        truth,
        dual_index_dir,
        offsets_uri,
        ordinals_uri,
        evidence_output_path,
        evidence_output_uri,
    })
}

fn run() -> Result<(), String> {
    let args = parse_args(std::env::args().skip(1).collect())?;
    let dual_index = build_v26_dual_pq_key_index_from_serving(
        &args.serving_manifest,
        &args.serving_dir,
        &args.dual_index_dir,
    )
    .map_err(|error| error.to_string())?;
    let bytes = run_v26_dual_pq_key_preflight(&V26DualPqKeyPreflightRequest {
        serving_manifest: args.serving_manifest,
        serving_dir: args.serving_dir,
        layout_terminal: args.layout_terminal,
        external_queries: args.external_queries,
        truth: args.truth,
        dual_index_dir: args.dual_index_dir,
        dual_index,
        offsets_uri: args.offsets_uri,
        ordinals_uri: args.ordinals_uri,
        evidence_output_path: args.evidence_output_path,
        evidence_output_uri: args.evidence_output_uri,
    })
    .map_err(|error| error.to_string())?;
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
            "--execute-dual-pq-key-preflight".to_owned(),
            "--generation".to_owned(),
            "v26-test".to_owned(),
            "--serving-dir".to_owned(),
            "/tmp/serving".to_owned(),
            "--dual-index-dir".to_owned(),
            "/tmp/dual-index".to_owned(),
            "--offsets-uri".to_owned(),
            "s3://v26/dual-index/pq16-dual-key-offsets.arrow".to_owned(),
            "--ordinals-uri".to_owned(),
            "s3://v26/dual-index/pq16-dual-key-ordinals.arrow".to_owned(),
            "--evidence-output-path".to_owned(),
            "/tmp/dual-preflight.parquet".to_owned(),
            "--evidence-output-uri".to_owned(),
            "s3://v26/dual-preflight.parquet".to_owned(),
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
    fn v26_dual_pq_key_preflight_cli_requires_exact_offline_authority() {
        let request = super::parse_args(valid_args()).unwrap();
        assert_eq!(
            request.serving_manifest.identity.role,
            "pq16-serving-manifest"
        );
        assert_eq!(request.dual_index_dir.to_str(), Some("/tmp/dual-index"));
        assert!(request.offsets_uri.ends_with("pq16-dual-key-offsets.arrow"));
        assert!(
            request
                .ordinals_uri
                .ends_with("pq16-dual-key-ordinals.arrow")
        );
        assert_eq!(
            request.evidence_output_uri,
            "s3://v26/dual-preflight.parquet"
        );
    }

    #[test]
    fn v26_dual_pq_key_preflight_cli_fails_closed_on_missing_duplicate_and_invalid_values() {
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
    fn v26_dual_pq_key_preflight_cli_rejects_storage_d3_and_tuning_surface() {
        for forbidden in [
            "--bucket",
            "--endpoint",
            "--page-prefix",
            "--d3",
            "--key-limit",
            "--ranked-row-limit",
            "--selected-page-count",
        ] {
            let mut args = valid_args();
            args.extend([forbidden.to_owned(), "forbidden".to_owned()]);
            assert!(super::parse_args(args).is_err(), "accepted {forbidden}");
        }
    }
}

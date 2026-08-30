//! Local authenticated, claim-ineligible V23 global-ADC diagnostic.
//!
//! This executable reads only registered local artifacts and exposes no page-body or network
//! execution surface.

use std::{collections::BTreeMap, env, io::Write, path::PathBuf, process::ExitCode};

use borsuk::{
    V23GlobalAdcEvidenceIdentity, V23GlobalAdcLocalArtifactPaths, V23GlobalAdcLocalRunRequest,
    V23GlobalAdcObjectIdentity, run_v23_global_adc_local_request,
};

const V23_GLOBAL_ADC_ROLES: [(&str, &str); 7] = [
    ("d1-report", "sha256"),
    ("d2-terminal", "sha256"),
    ("d2-result", "sha256"),
    ("d2-report", "sha256"),
    ("page-roster", "sha256"),
    ("query-parquet", "sha256"),
    ("selector", "blake3"),
];

fn valid_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn take_required(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn parse_object_identity(
    values: &mut BTreeMap<String, String>,
    role: &str,
    digest_algorithm: &str,
) -> Result<V23GlobalAdcObjectIdentity, String> {
    let uri = take_required(values, &format!("--{role}-uri"))?;
    if !uri.starts_with("s3://") || uri.trim_start_matches("s3://").is_empty() {
        return Err(format!("invalid --{role}-uri"));
    }
    let digest = take_required(values, &format!("--{role}-{digest_algorithm}"))?;
    if !valid_lower_hex(&digest, 64) {
        return Err(format!("invalid --{role}-{digest_algorithm}"));
    }
    let encoded_bytes = take_required(values, &format!("--{role}-bytes"))?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("invalid --{role}-bytes"))?;
    Ok(V23GlobalAdcObjectIdentity {
        role: role.to_string(),
        uri,
        digest_algorithm: digest_algorithm.to_string(),
        digest,
        encoded_bytes,
    })
}

fn parse_v23_global_adc_diagnostic_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V23GlobalAdcLocalRunRequest, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "program name is absent".to_string())?;
    let mut values = BTreeMap::new();
    let mut execute_global_adc = false;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-global-adc" {
            if execute_global_adc {
                return Err("duplicate --execute-global-adc".to_string());
            }
            execute_global_adc = true;
            continue;
        }
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
    if !execute_global_adc {
        return Err("missing required flag --execute-global-adc".to_string());
    }

    let paths = V23GlobalAdcLocalArtifactPaths {
        d1_report: PathBuf::from(take_required(&mut values, "--d1-report")?),
        d2_terminal: PathBuf::from(take_required(&mut values, "--d2-terminal")?),
        d2_result: PathBuf::from(take_required(&mut values, "--d2-result")?),
        d2_report: PathBuf::from(take_required(&mut values, "--d2-report")?),
        roster: PathBuf::from(take_required(&mut values, "--page-roster")?),
        query: PathBuf::from(take_required(&mut values, "--query-parquet")?),
        selector: PathBuf::from(take_required(&mut values, "--selector")?),
    };
    if [
        &paths.d1_report,
        &paths.d2_terminal,
        &paths.d2_result,
        &paths.d2_report,
        &paths.roster,
        &paths.query,
        &paths.selector,
    ]
    .iter()
    .any(|path| path.as_os_str().is_empty())
    {
        return Err("local artifact path is empty".to_string());
    }
    let source_commit = take_required(&mut values, "--source-commit")?;
    let source_archive_sha256 = take_required(&mut values, "--source-archive-sha256")?;
    let index_id = take_required(&mut values, "--index-id")?;
    if !valid_lower_hex(&source_commit, 40)
        || !valid_lower_hex(&source_archive_sha256, 64)
        || index_id.is_empty()
    {
        return Err("registered source identity is invalid".to_string());
    }
    let mut identities = Vec::with_capacity(V23_GLOBAL_ADC_ROLES.len());
    for (role, digest_algorithm) in V23_GLOBAL_ADC_ROLES {
        identities.push(parse_object_identity(&mut values, role, digest_algorithm)?);
    }
    if let Some(unknown) = values.keys().next() {
        return Err(format!("unknown flag {unknown}"));
    }
    let mut identities = identities.into_iter();
    let registered_identity = V23GlobalAdcEvidenceIdentity {
        source_commit,
        source_archive_sha256,
        index_id,
        d1_report: identities.next().unwrap(),
        d2_terminal: identities.next().unwrap(),
        d2_result: identities.next().unwrap(),
        d2_report: identities.next().unwrap(),
        roster: identities.next().unwrap(),
        query: identities.next().unwrap(),
        selector: identities.next().unwrap(),
    };
    Ok(V23GlobalAdcLocalRunRequest {
        paths,
        registered_identity,
        execute_global_adc,
    })
}

fn run() -> Result<(), String> {
    let request = parse_v23_global_adc_diagnostic_args(env::args())?;
    let bytes = run_v23_global_adc_local_request(request).map_err(|error| error.to_string())?;
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
    use std::path::PathBuf;

    use super::parse_v23_global_adc_diagnostic_args;
    use borsuk::{V23GlobalAdcLocalRunRequest, run_v23_global_adc_local_request};

    const SOURCE_COMMIT: &str = "1111111111111111111111111111111111111111";
    const SOURCE_ARCHIVE_SHA256: &str =
        "2222222222222222222222222222222222222222222222222222222222222222";

    const ROLES: [(&str, &str, &str); 7] = [
        ("d1-report", "sha256", "3"),
        ("d2-terminal", "sha256", "4"),
        ("d2-result", "sha256", "5"),
        ("d2-report", "sha256", "6"),
        ("page-roster", "sha256", "7"),
        ("query-parquet", "sha256", "8"),
        ("selector", "blake3", "9"),
    ];

    fn arguments() -> Vec<String> {
        let mut arguments = vec!["v23-global-adc-diagnostic".to_string()];
        for (flag, value) in [
            ("--d1-report", "/fixtures/bench_v23_d1_report.json"),
            ("--d2-terminal", "/fixtures/terminal.json"),
            ("--d2-result", "/fixtures/RESULT_COMPLETE.json"),
            ("--d2-report", "/fixtures/bench_v23_d2_report.json"),
            ("--page-roster", "/fixtures/bench_v23_pages.json"),
            ("--query-parquet", "/fixtures/query.parquet"),
            ("--selector", "/fixtures/selector.bvs"),
            ("--source-commit", SOURCE_COMMIT),
            ("--source-archive-sha256", SOURCE_ARCHIVE_SHA256),
            ("--index-id", "synthetic-v23-index"),
        ] {
            arguments.extend([flag.to_string(), value.to_string()]);
        }
        for (role, digest_algorithm, marker) in ROLES {
            arguments.extend([
                format!("--{role}-uri"),
                format!("s3://frozen-v23/{role}"),
                format!("--{role}-{digest_algorithm}"),
                marker.repeat(64),
                format!("--{role}-bytes"),
                "4096".to_string(),
            ]);
        }
        arguments.push("--execute-global-adc".to_string());
        arguments
    }

    fn flag_width(arguments: &[String], index: usize) -> usize {
        if arguments[index] == "--execute-global-adc" {
            1
        } else {
            2
        }
    }

    #[test]
    fn v23_global_adc_example_parses_exact_local_paths_identities_and_execute_gate() {
        let request = parse_v23_global_adc_diagnostic_args(arguments()).unwrap();
        assert_eq!(
            request.paths.d1_report,
            PathBuf::from("/fixtures/bench_v23_d1_report.json")
        );
        assert_eq!(
            request.paths.d2_terminal,
            PathBuf::from("/fixtures/terminal.json")
        );
        assert_eq!(
            request.paths.d2_result,
            PathBuf::from("/fixtures/RESULT_COMPLETE.json")
        );
        assert_eq!(
            request.paths.d2_report,
            PathBuf::from("/fixtures/bench_v23_d2_report.json")
        );
        assert_eq!(
            request.paths.roster,
            PathBuf::from("/fixtures/bench_v23_pages.json")
        );
        assert_eq!(
            request.paths.query,
            PathBuf::from("/fixtures/query.parquet")
        );
        assert_eq!(
            request.paths.selector,
            PathBuf::from("/fixtures/selector.bvs")
        );
        assert_eq!(request.registered_identity.source_commit, SOURCE_COMMIT);
        assert_eq!(
            request.registered_identity.source_archive_sha256,
            SOURCE_ARCHIVE_SHA256
        );
        assert_eq!(request.registered_identity.index_id, "synthetic-v23-index");
        assert!(request.execute_global_adc);

        let typed: V23GlobalAdcLocalRunRequest = request;
        let _future_run_boundary = run_v23_global_adc_local_request;
        drop(typed);
    }

    #[test]
    fn v23_global_adc_example_rejects_missing_duplicate_unknown_and_invalid_values() {
        let baseline = arguments();
        for index in 1..baseline.len() {
            if !baseline[index].starts_with("--") {
                continue;
            }
            let mut missing = baseline.clone();
            let width = flag_width(&baseline, index);
            missing.drain(index..index + width);
            assert!(parse_v23_global_adc_diagnostic_args(missing).is_err());
        }

        for flag in ["--d1-report", "--selector-blake3", "--execute-global-adc"] {
            let mut duplicate = baseline.clone();
            duplicate.push(flag.to_string());
            if flag != "--execute-global-adc" {
                duplicate.push("duplicate".to_string());
            }
            assert!(parse_v23_global_adc_diagnostic_args(duplicate).is_err());
        }

        let mut unknown = baseline.clone();
        unknown.extend(["--unknown".to_string(), "value".to_string()]);
        assert!(parse_v23_global_adc_diagnostic_args(unknown).is_err());

        for (flag, value) in [
            ("--source-commit", "not-a-commit"),
            ("--source-archive-sha256", "not-a-digest"),
            ("--selector-blake3", "not-a-digest"),
            ("--d1-report-bytes", "0"),
            ("--query-parquet-bytes", "not-a-number"),
            ("--selector-uri", "not-an-s3-uri"),
        ] {
            let mut invalid = baseline.clone();
            let index = invalid
                .iter()
                .position(|argument| argument == flag)
                .unwrap();
            invalid[index + 1] = value.to_string();
            assert!(parse_v23_global_adc_diagnostic_args(invalid).is_err());
        }
    }

    #[test]
    fn v23_global_adc_example_refuses_page_storage_endpoint_and_d3_surfaces() {
        for forbidden in [
            "--bucket",
            "--page-prefix",
            "--page-uri",
            "--storage-uri",
            "--storage-endpoint",
            "--s3-endpoint",
            "--aws-profile",
            "--d3",
            "--execute-d3",
        ] {
            let mut changed = arguments();
            changed.extend([forbidden.to_string(), "forbidden".to_string()]);
            assert!(parse_v23_global_adc_diagnostic_args(changed).is_err());
        }
    }
}

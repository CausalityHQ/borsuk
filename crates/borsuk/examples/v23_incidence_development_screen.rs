//! Direct local executable for the claim-ineligible V23 incidence development screen.
//!
//! The executable accepts exactly seven authenticated local artifacts and exposes no network,
//! storage, page-body, holdout, or D3 execution surface.

use std::{collections::BTreeMap, env, path::PathBuf, process::ExitCode};

use borsuk::{
    V23IncidenceScreenAuthority, V23IncidenceScreenLocalPaths, V23IncidenceScreenLocalRunRequest,
    V23IncidenceScreenObjectIdentity, run_v23_incidence_development_screen_local,
};

const ROLES: [(&str, &str); 7] = [
    ("tree-receipt", "sha256"),
    ("incidence-tree", "blake3"),
    ("posting-receipt", "sha256"),
    ("incidence-postings-one", "blake3"),
    ("incidence-postings-two", "blake3"),
    ("d2-report", "sha256"),
    ("query-parquet", "sha256"),
];

struct ParsedScreenRequest {
    request: V23IncidenceScreenLocalRunRequest,
    output: PathBuf,
}

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

fn parse_identity(
    values: &mut BTreeMap<String, String>,
    role: &str,
    algorithm: &str,
) -> Result<V23IncidenceScreenObjectIdentity, String> {
    let uri = take_required(values, &format!("--{role}-uri"))?;
    let digest = take_required(values, &format!("--{role}-{algorithm}"))?;
    let encoded_bytes = take_required(values, &format!("--{role}-bytes"))?
        .parse::<u64>()
        .ok()
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| format!("invalid --{role}-bytes"))?;
    if !uri.starts_with("s3://")
        || uri.trim_start_matches("s3://").is_empty()
        || !valid_lower_hex(&digest, 64)
    {
        return Err(format!("invalid {role} identity"));
    }
    Ok(V23IncidenceScreenObjectIdentity {
        role: role.to_string(),
        uri,
        digest_algorithm: algorithm.to_string(),
        digest,
        encoded_bytes,
    })
}

fn parse_v23_incidence_screen_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<ParsedScreenRequest, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "program name is absent".to_string())?;
    let mut values = BTreeMap::new();
    let mut execute = false;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-development-screen" {
            if execute {
                return Err("duplicate --execute-development-screen".to_string());
            }
            execute = true;
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
    if !execute {
        return Err("missing required flag --execute-development-screen".to_string());
    }

    let paths = V23IncidenceScreenLocalPaths {
        tree_receipt: PathBuf::from(take_required(&mut values, "--tree-receipt")?),
        incidence_tree: PathBuf::from(take_required(&mut values, "--incidence-tree")?),
        posting_receipt: PathBuf::from(take_required(&mut values, "--posting-receipt")?),
        incidence_postings_one: PathBuf::from(take_required(
            &mut values,
            "--incidence-postings-one",
        )?),
        incidence_postings_two: PathBuf::from(take_required(
            &mut values,
            "--incidence-postings-two",
        )?),
        d2_report: PathBuf::from(take_required(&mut values, "--d2-report")?),
        query_parquet: PathBuf::from(take_required(&mut values, "--query-parquet")?),
    };
    if [
        &paths.tree_receipt,
        &paths.incidence_tree,
        &paths.posting_receipt,
        &paths.incidence_postings_one,
        &paths.incidence_postings_two,
        &paths.d2_report,
        &paths.query_parquet,
    ]
    .iter()
    .any(|path| !path.is_absolute())
    {
        return Err("local artifact path is not absolute".to_string());
    }
    let output = PathBuf::from(take_required(&mut values, "--output")?);
    if !output.is_absolute() {
        return Err("output path is not absolute".to_string());
    }
    let source_commit = take_required(&mut values, "--source-commit")?;
    let source_archive_sha256 = take_required(&mut values, "--source-archive-sha256")?;
    let index_id = take_required(&mut values, "--index-id")?;
    if !valid_lower_hex(&source_commit, 40)
        || !valid_lower_hex(&source_archive_sha256, 64)
        || index_id.is_empty()
    {
        return Err("source identity differs".to_string());
    }
    let mut objects = Vec::with_capacity(ROLES.len());
    for (role, algorithm) in ROLES {
        objects.push(parse_identity(&mut values, role, algorithm)?);
    }
    if let Some(unknown) = values.keys().next() {
        return Err(format!("unknown flag {unknown}"));
    }
    Ok(ParsedScreenRequest {
        request: V23IncidenceScreenLocalRunRequest {
            paths,
            authority: V23IncidenceScreenAuthority {
                source_commit,
                source_archive_sha256,
                index_id,
                objects,
            },
            execute_development_screen: true,
        },
        output,
    })
}

fn run() -> Result<(), String> {
    let parsed = parse_v23_incidence_screen_args(env::args())?;
    let bytes = run_v23_incidence_development_screen_local(parsed.request)
        .map_err(|error| error.to_string())?;
    std::fs::write(&parsed.output, bytes)
        .map_err(|error| format!("failed to write {}: {error}", parsed.output.display()))
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

    use super::parse_v23_incidence_screen_args;
    use borsuk::{V23IncidenceScreenLocalRunRequest, run_v23_incidence_development_screen_local};

    const ROLES: [(&str, &str, char); 7] = [
        ("tree-receipt", "sha256", '3'),
        ("incidence-tree", "blake3", '4'),
        ("posting-receipt", "sha256", '5'),
        ("incidence-postings-one", "blake3", '6'),
        ("incidence-postings-two", "blake3", '7'),
        ("d2-report", "sha256", '8'),
        ("query-parquet", "sha256", '9'),
    ];

    fn arguments() -> Vec<String> {
        let mut arguments = vec!["v23-incidence-development-screen".to_string()];
        for role in ROLES.map(|(role, _, _)| role) {
            arguments.extend([format!("--{role}"), format!("/fixtures/{role}")]);
        }
        arguments.extend([
            "--source-commit".to_string(),
            "1".repeat(40),
            "--source-archive-sha256".to_string(),
            "2".repeat(64),
            "--index-id".to_string(),
            "index-fixture".to_string(),
            "--output".to_string(),
            "/output/screen.json".to_string(),
        ]);
        for (role, algorithm, marker) in ROLES {
            arguments.extend([
                format!("--{role}-uri"),
                format!("s3://frozen/{role}"),
                format!("--{role}-{algorithm}"),
                marker.to_string().repeat(64),
                format!("--{role}-bytes"),
                "4096".to_string(),
            ]);
        }
        arguments.push("--execute-development-screen".to_string());
        arguments
    }

    #[test]
    fn v23_incidence_screen_example_parses_exact_local_authority_and_output() {
        let parsed = parse_v23_incidence_screen_args(arguments()).unwrap();
        assert_eq!(
            parsed.request.paths.incidence_tree,
            PathBuf::from("/fixtures/incidence-tree")
        );
        assert_eq!(parsed.request.authority.objects.len(), 7);
        assert!(parsed.request.execute_development_screen);
        assert_eq!(parsed.output, PathBuf::from("/output/screen.json"));

        let typed: V23IncidenceScreenLocalRunRequest = parsed.request;
        let _runner = run_v23_incidence_development_screen_local;
        drop(typed);
    }

    #[test]
    fn v23_incidence_screen_example_rejects_ambiguous_or_forbidden_surface() {
        let baseline = arguments();
        for flag in [
            "--incidence-tree",
            "--incidence-tree-blake3",
            "--execute-development-screen",
            "--output",
        ] {
            let index = baseline
                .iter()
                .position(|argument| argument == flag)
                .unwrap();
            let mut missing = baseline.clone();
            missing.drain(index..index + usize::from(flag != "--execute-development-screen") + 1);
            assert!(parse_v23_incidence_screen_args(missing).is_err());
        }
        for flag in ["--incidence-tree", "--execute-development-screen"] {
            let mut duplicate = baseline.clone();
            duplicate.push(flag.to_string());
            if flag != "--execute-development-screen" {
                duplicate.push("/duplicate".to_string());
            }
            assert!(parse_v23_incidence_screen_args(duplicate).is_err());
        }
        for forbidden in [
            "--bucket",
            "--endpoint",
            "--page-prefix",
            "--neighbors",
            "--holdout",
            "--d3",
        ] {
            let mut arguments = baseline.clone();
            arguments.extend([forbidden.to_string(), "forbidden".to_string()]);
            assert!(parse_v23_incidence_screen_args(arguments).is_err());
        }
        let mut invalid = baseline;
        let index = invalid
            .iter()
            .position(|argument| argument == "--query-parquet-bytes")
            .unwrap();
        invalid[index + 1] = "0".to_string();
        assert!(parse_v23_incidence_screen_args(invalid).is_err());
    }
}

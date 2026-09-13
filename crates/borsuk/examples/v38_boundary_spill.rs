//! Strict local-file command boundary for the claim-ineligible V38 diagnostic.

use std::{
    collections::BTreeMap,
    env,
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
};

use borsuk::{
    V38LocalArtifact, V38LocalOutput, V38LocalRunMode, V38LocalRunRequest,
    run_v38_local_request_with_progress,
};

fn valid_lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn take(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn parse_v38_boundary_spill_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V38LocalRunRequest, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut execute = false;
    let mut values = BTreeMap::new();
    while let Some(flag) = arguments.next() {
        if flag == "--execute-v38-local" {
            if execute {
                return Err("duplicate --execute-v38-local".to_owned());
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
        return Err("missing required flag --execute-v38-local".to_owned());
    }
    let mode = match take(&mut values, "--mode")?.as_str() {
        "preflight-spill" => V38LocalRunMode::PreflightSpill,
        "build-spill" => V38LocalRunMode::BuildSpill,
        "evaluate-ceiling" => V38LocalRunMode::EvaluateCeiling,
        _ => return Err("V38 local mode differs".to_owned()),
    };
    let workers = take(&mut values, "--workers")?
        .parse::<u32>()
        .map_err(|_| "V38 local worker count differs".to_owned())?;
    let input_roles: &[&str] = match mode {
        V38LocalRunMode::PreflightSpill => &["v38-authority"],
        V38LocalRunMode::BuildSpill => &[
            "v38-authority",
            "v37-authority",
            "v37-build-manifest",
            "v37-local-result",
            "v37-terminal",
            "v37-tree",
            "v37-ownership",
            "source",
        ],
        V38LocalRunMode::EvaluateCeiling => &[
            "v38-ceiling-authority",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
            "gt100",
        ],
    };
    let output_roles: &[&str] = match mode {
        V38LocalRunMode::PreflightSpill => &[],
        V38LocalRunMode::BuildSpill => &["spill-relation", "spill-postings"],
        V38LocalRunMode::EvaluateCeiling => &["ceiling"],
    };
    let mut inputs = Vec::with_capacity(input_roles.len());
    for role in input_roles {
        let path = PathBuf::from(take(&mut values, &format!("--{role}"))?);
        let uri = take(&mut values, &format!("--{role}-uri"))?;
        let sha256 = take(&mut values, &format!("--{role}-sha256"))?;
        let blake3 = take(&mut values, &format!("--{role}-blake3"))?;
        if !valid_lower_hex(&sha256) || !valid_lower_hex(&blake3) {
            return Err(format!("invalid {role} digest"));
        }
        let encoded_bytes = take(&mut values, &format!("--{role}-bytes"))?
            .parse::<u64>()
            .map_err(|_| format!("invalid {role} byte length"))?;
        inputs.push(
            V38LocalArtifact::try_new((*role).to_owned(), path, uri, sha256, blake3, encoded_bytes)
                .map_err(|error| error.to_string())?,
        );
    }
    let mut outputs = Vec::with_capacity(output_roles.len());
    for role in output_roles {
        outputs.push(
            V38LocalOutput::try_new(
                (*role).to_owned(),
                PathBuf::from(take(&mut values, &format!("--{role}-output"))?),
            )
            .map_err(|error| error.to_string())?,
        );
    }
    if let Some(unknown) = values.keys().next() {
        return Err(format!("unknown flag {unknown}"));
    }
    V38LocalRunRequest::try_new(mode, inputs, outputs, workers).map_err(|error| error.to_string())
}

fn main() -> ExitCode {
    let request = match parse_v38_boundary_spill_args(env::args()) {
        Ok(request) => request,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let mut stderr = io::stderr().lock();
    match run_v38_local_request_with_progress(request, |bytes| {
        stderr.write_all(bytes).and_then(|()| stderr.flush())
    }) {
        Ok(bytes) => match io::stdout().write_all(&bytes) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("V38 stdout write failed: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_v38_boundary_spill_args;
    use borsuk::V38LocalRunMode;

    const BUILD_ROLES: [&str; 8] = [
        "v38-authority",
        "v37-authority",
        "v37-build-manifest",
        "v37-local-result",
        "v37-terminal",
        "v37-tree",
        "v37-ownership",
        "source",
    ];
    const CEILING_ROLES: [&str; 5] = [
        "v38-ceiling-authority",
        "v38-construction-result",
        "spill-relation",
        "spill-postings",
        "gt100",
    ];

    fn input_arguments(role: &str, marker: char) -> Vec<String> {
        vec![
            format!("--{role}"),
            format!("/fixtures/{role}"),
            format!("--{role}-uri"),
            format!("s3://frozen-v38/{role}"),
            format!("--{role}-sha256"),
            marker.to_string().repeat(64),
            format!("--{role}-blake3"),
            marker.to_string().repeat(64),
            format!("--{role}-bytes"),
            "4096".to_owned(),
        ]
    }

    fn arguments(mode: &str, roles: &[&str], outputs: &[&str]) -> Vec<String> {
        let mut arguments = vec![
            "v38-boundary-spill".to_owned(),
            "--execute-v38-local".to_owned(),
            "--mode".to_owned(),
            mode.to_owned(),
            "--workers".to_owned(),
            "4".to_owned(),
        ];
        for (index, role) in roles.iter().enumerate() {
            arguments.extend(input_arguments(role, char::from(b'1' + index as u8)));
        }
        for role in outputs {
            arguments.extend([format!("--{role}-output"), format!("/output/{role}")]);
        }
        arguments
    }

    #[test]
    fn v38_boundary_cli_parses_exact_three_phase_capabilities() {
        let preflight =
            parse_v38_boundary_spill_args(arguments("preflight-spill", &["v38-authority"], &[]))
                .unwrap();
        assert_eq!(preflight.mode(), V38LocalRunMode::PreflightSpill);
        assert_eq!(preflight.input_roles(), ["v38-authority"]);
        assert!(preflight.output_roles().is_empty());

        let build = parse_v38_boundary_spill_args(arguments(
            "build-spill",
            &BUILD_ROLES,
            &["spill-relation", "spill-postings"],
        ))
        .unwrap();
        assert_eq!(build.mode(), V38LocalRunMode::BuildSpill);
        assert_eq!(build.input_roles(), BUILD_ROLES);
        assert_eq!(build.output_roles(), ["spill-relation", "spill-postings"]);

        let ceiling = parse_v38_boundary_spill_args(arguments(
            "evaluate-ceiling",
            &CEILING_ROLES,
            &["ceiling"],
        ))
        .unwrap();
        assert_eq!(ceiling.mode(), V38LocalRunMode::EvaluateCeiling);
        assert_eq!(ceiling.input_roles(), CEILING_ROLES);
        assert_eq!(ceiling.output_roles(), ["ceiling"]);
        assert_eq!(ceiling.workers(), 4);
    }

    #[test]
    fn v38_boundary_cli_rejects_missing_duplicate_unknown_and_remote_capabilities() {
        let baseline = arguments(
            "build-spill",
            &BUILD_ROLES,
            &["spill-relation", "spill-postings"],
        );

        for index in 1..baseline.len() {
            if !baseline[index].starts_with("--") {
                continue;
            }
            let width = usize::from(baseline[index] != "--execute-v38-local") + 1;
            let mut missing = baseline.clone();
            missing.drain(index..index + width);
            assert!(parse_v38_boundary_spill_args(missing).is_err());
        }

        let mut duplicate = baseline.clone();
        duplicate.extend(input_arguments("source", 'f'));
        assert!(parse_v38_boundary_spill_args(duplicate).is_err());

        for (flag, value) in [
            ("--unknown", "value"),
            ("--bucket", "forbidden"),
            ("--endpoint", "forbidden"),
            ("--page-prefix", "forbidden"),
            ("--execute-d3", "forbidden"),
            ("--population", "10000000"),
            ("--validation", "/fixtures/validation.parquet"),
            ("--holdout", "/fixtures/holdout.parquet"),
        ] {
            let mut changed = baseline.clone();
            changed.extend([flag.to_owned(), value.to_owned()]);
            assert!(parse_v38_boundary_spill_args(changed).is_err(), "{flag}");
        }

        for (flag, invalid) in [
            ("--mode", "combined"),
            ("--workers", "3"),
            ("--source-uri", "file:///tmp/source"),
            ("--source-sha256", "not-a-digest"),
            ("--source-blake3", "not-a-digest"),
            ("--source-bytes", "0"),
        ] {
            let mut changed = baseline.clone();
            let index = changed.iter().position(|value| value == flag).unwrap();
            changed[index + 1] = invalid.to_owned();
            assert!(parse_v38_boundary_spill_args(changed).is_err(), "{flag}");
        }
    }

    #[test]
    fn v38_boundary_cli_rejects_cross_phase_source_query_ground_truth_and_page_flags() {
        let cases = [
            ("build-spill", BUILD_ROLES.as_slice(), "--query"),
            ("build-spill", BUILD_ROLES.as_slice(), "--gt100"),
            ("evaluate-ceiling", CEILING_ROLES.as_slice(), "--source"),
            (
                "evaluate-ceiling",
                CEILING_ROLES.as_slice(),
                "--page-object",
            ),
        ];
        for (mode, roles, forbidden) in cases {
            let outputs = if mode == "build-spill" {
                &["spill-relation", "spill-postings"][..]
            } else {
                &["ceiling"][..]
            };
            let mut changed = arguments(mode, roles, outputs);
            changed.extend([forbidden.to_owned(), "/forbidden".to_owned()]);
            assert!(
                parse_v38_boundary_spill_args(changed).is_err(),
                "{mode} accepted {forbidden}"
            );
        }
    }
}

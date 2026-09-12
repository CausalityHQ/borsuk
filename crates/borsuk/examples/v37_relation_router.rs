//! Local authenticated, claim-ineligible V37 balanced-hyperplane diagnostic.

use std::{
    collections::BTreeMap,
    env,
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
};

use borsuk::{
    V37LocalArtifact, V37LocalOutput, V37LocalRunMode, V37LocalRunRequest, run_v37_local_request,
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

fn parse_v37_relation_router_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V37LocalRunRequest, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut execute = false;
    let mut values = BTreeMap::new();
    while let Some(flag) = arguments.next() {
        if flag == "--execute-v37-local" {
            if execute {
                return Err("duplicate --execute-v37-local".to_owned());
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
        return Err("missing required flag --execute-v37-local".to_owned());
    }
    let mode = match take(&mut values, "--mode")?.as_str() {
        "build-ownership" => V37LocalRunMode::BuildOwnership,
        "evaluate-ceiling" => V37LocalRunMode::EvaluateCeiling,
        _ => return Err("V37 local mode differs".to_owned()),
    };
    let workers = take(&mut values, "--workers")?
        .parse::<u32>()
        .map_err(|_| "V37 local worker count differs".to_owned())?;
    let input_roles: &[&str] = match mode {
        V37LocalRunMode::BuildOwnership => &[
            "v36-authority",
            "v36-execution-authority",
            "v36-receipt",
            "v36-source-registry",
            "v37-authority",
            "source",
        ],
        V37LocalRunMode::EvaluateCeiling => &[
            "ceiling-authority",
            "development-ground-truth",
            "ownership-tree",
            "ownership",
        ],
    };
    let output_roles: &[&str] = match mode {
        V37LocalRunMode::BuildOwnership => &["ownership-tree", "ownership"],
        V37LocalRunMode::EvaluateCeiling => &["ceiling"],
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
            V37LocalArtifact::try_new((*role).to_owned(), path, uri, sha256, blake3, encoded_bytes)
                .map_err(|error| error.to_string())?,
        );
    }
    let mut outputs = Vec::with_capacity(output_roles.len());
    for role in output_roles {
        outputs.push(
            V37LocalOutput::try_new(
                (*role).to_owned(),
                PathBuf::from(take(&mut values, &format!("--{role}-output"))?),
            )
            .map_err(|error| error.to_string())?,
        );
    }
    if let Some(unknown) = values.keys().next() {
        return Err(format!("unknown flag {unknown}"));
    }
    V37LocalRunRequest::try_new(mode, inputs, outputs, workers).map_err(|error| error.to_string())
}

fn main() -> ExitCode {
    let request = match parse_v37_relation_router_args(env::args()) {
        Ok(request) => request,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    match run_v37_local_request(request) {
        Ok(bytes) => match io::stdout().write_all(&bytes) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("V37 stdout write failed: {error}");
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
    use super::{parse_v37_relation_router_args, run_v37_local_request};
    use borsuk::V37LocalRunMode;

    const COMMON_ROLES: [&str; 5] = [
        "v36-authority",
        "v36-execution-authority",
        "v36-receipt",
        "v36-source-registry",
        "v37-authority",
    ];

    #[test]
    fn v37_relation_cli_exposes_only_the_high_level_local_runner() {
        let _runner = run_v37_local_request;
    }

    fn input_arguments(role: &str, marker: char) -> Vec<String> {
        vec![
            format!("--{role}"),
            format!("/fixtures/{role}"),
            format!("--{role}-uri"),
            format!("s3://frozen-v37/{role}"),
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
            "v37-relation-router".to_owned(),
            "--execute-v37-local".to_owned(),
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
    fn v37_relation_cli_parses_exact_build_ownership_capabilities() {
        let mut roles = COMMON_ROLES.to_vec();
        roles.push("source");
        let request = parse_v37_relation_router_args(arguments(
            "build-ownership",
            &roles,
            &["ownership-tree", "ownership"],
        ))
        .unwrap();

        assert_eq!(request.mode(), V37LocalRunMode::BuildOwnership);
        assert_eq!(request.input_roles(), roles);
        assert_eq!(request.output_roles(), ["ownership-tree", "ownership"]);
        assert_eq!(request.workers(), 4);
    }

    #[test]
    fn v37_relation_cli_parses_exact_evaluate_ceiling_capabilities() {
        let roles = vec![
            "ceiling-authority",
            "development-ground-truth",
            "ownership-tree",
            "ownership",
        ];
        let request =
            parse_v37_relation_router_args(arguments("evaluate-ceiling", &roles, &["ceiling"]))
                .unwrap();

        assert_eq!(request.mode(), V37LocalRunMode::EvaluateCeiling);
        assert_eq!(request.input_roles(), roles);
        assert_eq!(request.output_roles(), ["ceiling"]);
        assert_eq!(request.workers(), 4);
    }

    #[test]
    fn v37_relation_cli_rejects_duplicate_unknown_remote_and_phase_inappropriate_capabilities() {
        let mut build_roles = COMMON_ROLES.to_vec();
        build_roles.push("source");
        let baseline = arguments(
            "build-ownership",
            &build_roles,
            &["ownership-tree", "ownership"],
        );

        let mut duplicate = baseline.clone();
        duplicate.extend(input_arguments("source", 'f'));
        assert!(parse_v37_relation_router_args(duplicate).is_err());

        for (flag, value) in [
            ("--unknown", "value"),
            ("--bucket", "forbidden"),
            ("--endpoint", "forbidden"),
            ("--page-prefix", "forbidden"),
            ("--execute-d3", "forbidden"),
            ("--development-query", "/fixtures/query.parquet"),
        ] {
            let mut changed = baseline.clone();
            changed.extend([flag.to_owned(), value.to_owned()]);
            assert!(parse_v37_relation_router_args(changed).is_err(), "{flag}");
        }

        for index in 1..baseline.len() {
            if !baseline[index].starts_with("--") {
                continue;
            }
            let width = usize::from(baseline[index] != "--execute-v37-local") + 1;
            let mut missing = baseline.clone();
            missing.drain(index..index + width);
            assert!(parse_v37_relation_router_args(missing).is_err());
        }

        for (flag, invalid) in [
            ("--mode", "route-direct"),
            ("--workers", "3"),
            ("--source-uri", "file:///tmp/source"),
            ("--source-sha256", "not-a-digest"),
            ("--source-blake3", "not-a-digest"),
            ("--source-bytes", "0"),
        ] {
            let mut changed = baseline.clone();
            let index = changed.iter().position(|value| value == flag).unwrap();
            changed[index + 1] = invalid.to_owned();
            assert!(parse_v37_relation_router_args(changed).is_err(), "{flag}");
        }
    }
}

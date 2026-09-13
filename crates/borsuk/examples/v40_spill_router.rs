//! Strict local-file boundary for the claim-ineligible V40 direct router.

use std::{collections::BTreeMap, env, io::Write, path::PathBuf, process::ExitCode};

use borsuk::{
    V40LocalArtifact, V40LocalOutput, V40LocalRunMode, V40LocalRunRequest, run_v40_local_request,
};

fn take(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn parse_v40_spill_router_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V40LocalRunRequest, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut execute = false;
    let mut values = BTreeMap::new();
    while let Some(flag) = arguments.next() {
        if flag == "--execute-v40" {
            if execute {
                return Err("duplicate --execute-v40".to_owned());
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
        return Err("missing required flag --execute-v40".to_owned());
    }
    let mode = match take(&mut values, "--mode")?.as_str() {
        "select-direct" => V40LocalRunMode::SelectDirect,
        "build-spill-summary" => V40LocalRunMode::BuildSpillSummary,
        "select-accepted-spill" => V40LocalRunMode::SelectAcceptedSpill,
        "evaluate-direct" => V40LocalRunMode::EvaluateDirect,
        "evaluate-accepted-spill" => V40LocalRunMode::EvaluateAcceptedSpill,
        _ => return Err("V40 local mode differs".to_owned()),
    };
    let workers = take(&mut values, "--workers")?
        .parse::<u32>()
        .map_err(|_| "V40 worker count differs".to_owned())?;
    let input_roles = match mode {
        V40LocalRunMode::SelectDirect => &[
            "cohort-authority",
            "v37-authority",
            "ownership-tree",
            "development-query",
        ][..],
        V40LocalRunMode::BuildSpillSummary => &[
            "cohort-authority",
            "direct-result",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
        ],
        V40LocalRunMode::SelectAcceptedSpill => &[
            "cohort-authority",
            "direct-result",
            "v37-authority",
            "ownership-tree",
            "development-query",
            "spill-summary-result",
            "spill-summary",
        ],
        V40LocalRunMode::EvaluateDirect => &[
            "cohort-authority",
            "v38-ceiling-authority",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
            "development-ground-truth",
            "direct-selection-result",
            "direct-selection",
        ],
        V40LocalRunMode::EvaluateAcceptedSpill => &[
            "cohort-authority",
            "direct-result",
            "v38-ceiling-authority",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
            "development-ground-truth",
            "spill-summary-result",
            "spill-summary",
            "accepted-selection-result",
            "accepted-selection",
        ],
    };
    let output_roles: &[&str] = match mode {
        V40LocalRunMode::SelectDirect => &["direct-selection"],
        V40LocalRunMode::BuildSpillSummary => {
            &["spill-counts", "spill-summary", "spill-summary-result"]
        }
        V40LocalRunMode::SelectAcceptedSpill => {
            &["accepted-selection", "accepted-selection-result"]
        }
        V40LocalRunMode::EvaluateDirect => &["direct-result"],
        V40LocalRunMode::EvaluateAcceptedSpill => &["accepted-result"],
    };
    let mut inputs = Vec::with_capacity(input_roles.len());
    for role in input_roles {
        inputs.push(
            V40LocalArtifact::try_new(
                (*role).to_owned(),
                PathBuf::from(take(&mut values, &format!("--{role}"))?),
                take(&mut values, &format!("--{role}-uri"))?,
                take(&mut values, &format!("--{role}-sha256"))?,
                take(&mut values, &format!("--{role}-blake3"))?,
                take(&mut values, &format!("--{role}-bytes"))?
                    .parse::<u64>()
                    .map_err(|_| format!("invalid {role} byte length"))?,
            )
            .map_err(|error| error.to_string())?,
        );
    }
    let outputs = output_roles
        .iter()
        .map(|role| {
            V40LocalOutput::try_new(
                (*role).to_owned(),
                PathBuf::from(take(&mut values, &format!("--{role}-output"))?),
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(unknown) = values.keys().next() {
        return Err(format!("unknown flag {unknown}"));
    }
    V40LocalRunRequest::try_new(mode, inputs, outputs, workers).map_err(|error| error.to_string())
}

fn main() -> ExitCode {
    let request = match parse_v40_spill_router_args(env::args()) {
        Ok(request) => request,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    match run_v40_local_request(request) {
        Ok(bytes) => match std::io::stdout().write_all(&bytes) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("V40 stdout write failed: {error}");
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
    use super::parse_v40_spill_router_args;
    use borsuk::{V40LocalRunMode, run_v40_local_request};

    const SELECT_ROLES: [&str; 4] = [
        "cohort-authority",
        "v37-authority",
        "ownership-tree",
        "development-query",
    ];
    const EVALUATE_ROLES: [&str; 8] = [
        "cohort-authority",
        "v38-ceiling-authority",
        "v38-construction-result",
        "spill-relation",
        "spill-postings",
        "development-ground-truth",
        "direct-selection-result",
        "direct-selection",
    ];
    const BUILD_SPILL_ROLES: [&str; 5] = [
        "cohort-authority",
        "direct-result",
        "v38-construction-result",
        "spill-relation",
        "spill-postings",
    ];
    const SELECT_SPILL_ROLES: [&str; 7] = [
        "cohort-authority",
        "direct-result",
        "v37-authority",
        "ownership-tree",
        "development-query",
        "spill-summary-result",
        "spill-summary",
    ];
    const EVALUATE_SPILL_ROLES: [&str; 11] = [
        "cohort-authority",
        "direct-result",
        "v38-ceiling-authority",
        "v38-construction-result",
        "spill-relation",
        "spill-postings",
        "development-ground-truth",
        "spill-summary-result",
        "spill-summary",
        "accepted-selection-result",
        "accepted-selection",
    ];

    fn input_arguments(role: &str, marker: char) -> Vec<String> {
        vec![
            format!("--{role}"),
            format!("/fixtures/{role}"),
            format!("--{role}-uri"),
            format!("s3://frozen-v40/{role}"),
            format!("--{role}-sha256"),
            marker.to_string().repeat(64),
            format!("--{role}-blake3"),
            marker.to_string().repeat(64),
            format!("--{role}-bytes"),
            "4096".to_owned(),
        ]
    }

    fn arguments(mode: &str, roles: &[&str], outputs: &[&str]) -> Vec<String> {
        const DIGEST_MARKERS: &[u8] = b"123456789abcdef";
        let mut arguments = vec![
            "v40-spill-router".to_owned(),
            "--execute-v40".to_owned(),
            "--mode".to_owned(),
            mode.to_owned(),
            "--workers".to_owned(),
            "4".to_owned(),
        ];
        for (index, role) in roles.iter().enumerate() {
            arguments.extend(input_arguments(role, char::from(DIGEST_MARKERS[index])));
        }
        for output in outputs {
            arguments.extend([format!("--{output}-output"), format!("/output/{output}")]);
        }
        arguments
    }

    #[test]
    fn v40_cli_parses_only_the_two_direct_capabilities() {
        let selection = parse_v40_spill_router_args(arguments(
            "select-direct",
            &SELECT_ROLES,
            &["direct-selection"],
        ))
        .unwrap();
        assert_eq!(selection.mode(), V40LocalRunMode::SelectDirect);
        assert_eq!(selection.input_roles(), SELECT_ROLES);
        assert_eq!(selection.output_roles(), ["direct-selection"]);

        let evaluation = parse_v40_spill_router_args(arguments(
            "evaluate-direct",
            &EVALUATE_ROLES,
            &["direct-result"],
        ))
        .unwrap();
        assert_eq!(evaluation.mode(), V40LocalRunMode::EvaluateDirect);
        assert_eq!(evaluation.input_roles(), EVALUATE_ROLES);
        assert_eq!(evaluation.output_roles(), ["direct-result"]);
        assert_eq!(evaluation.workers(), 4);
        let _runner = run_v40_local_request;
    }

    #[test]
    fn v40_cli_parses_the_three_capability_separated_challenger_phases() {
        for (mode, roles, outputs, expected_mode) in [
            (
                "build-spill-summary",
                BUILD_SPILL_ROLES.as_slice(),
                ["spill-counts", "spill-summary", "spill-summary-result"].as_slice(),
                V40LocalRunMode::BuildSpillSummary,
            ),
            (
                "select-accepted-spill",
                SELECT_SPILL_ROLES.as_slice(),
                ["accepted-selection", "accepted-selection-result"].as_slice(),
                V40LocalRunMode::SelectAcceptedSpill,
            ),
            (
                "evaluate-accepted-spill",
                EVALUATE_SPILL_ROLES.as_slice(),
                ["accepted-result"].as_slice(),
                V40LocalRunMode::EvaluateAcceptedSpill,
            ),
        ] {
            let request = parse_v40_spill_router_args(arguments(mode, roles, outputs)).unwrap();
            assert_eq!(request.mode(), expected_mode);
            assert_eq!(request.input_roles(), roles);
            assert_eq!(request.output_roles(), outputs);
        }
    }

    #[test]
    fn v40_cli_fails_closed_on_argument_and_remote_capability_drift() {
        let baseline = arguments("select-direct", &SELECT_ROLES, &["direct-selection"]);
        for forbidden in [
            "--bucket",
            "--page-prefix",
            "--endpoint",
            "--region",
            "--d3",
        ] {
            let mut drifted = baseline.clone();
            drifted.extend([forbidden.to_owned(), "forbidden".to_owned()]);
            assert!(parse_v40_spill_router_args(drifted).is_err());
        }

        let mut missing_execute = baseline.clone();
        missing_execute.remove(1);
        assert!(parse_v40_spill_router_args(missing_execute).is_err());

        let mut duplicate = baseline.clone();
        duplicate.push("--execute-v40".to_owned());
        assert!(parse_v40_spill_router_args(duplicate).is_err());

        let mut bad_workers = baseline;
        let index = bad_workers
            .iter()
            .position(|value| value == "--workers")
            .unwrap();
        bad_workers[index + 1] = "many".to_owned();
        assert!(parse_v40_spill_router_args(bad_workers).is_err());
    }
}

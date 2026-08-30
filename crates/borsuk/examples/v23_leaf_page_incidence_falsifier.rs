//! Local-only executable boundary for the claim-ineligible V23 incidence falsifier.

use borsuk::{
    V23IncidenceLocalPhaseRequest, V23IncidenceLocalRolePath, V23IncidenceObjectIdentity,
    V23IncidencePhase, V23IncidenceRunMode, run_v23_incidence_local_phase,
};
use std::path::PathBuf;

fn lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn take_value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires one value"))
}

fn take_named_value(
    arguments: &mut impl Iterator<Item = String>,
    expected: &str,
) -> Result<String, String> {
    let flag = arguments
        .next()
        .ok_or_else(|| format!("{expected} is absent"))?;
    if flag != expected {
        return Err(format!("expected {expected}, got {flag}"));
    }
    take_value(arguments, expected)
}

fn set_once<T>(slot: &mut Option<T>, value: T, flag: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("{flag} is duplicated"));
    }
    Ok(())
}

fn gate(flag: &str) -> Option<V23IncidenceRunMode> {
    let (execute, phase) = if let Some(phase) = flag.strip_prefix("--preflight-") {
        (false, phase)
    } else {
        (true, flag.strip_prefix("--execute-")?)
    };
    let phase = match phase {
        "tree-training" => V23IncidencePhase::TreeTraining,
        "posting-construction" => V23IncidencePhase::PostingConstruction,
        "development-evaluation" => V23IncidencePhase::DevelopmentEvaluation,
        "holdout-binding" => V23IncidencePhase::HoldoutBinding,
        "holdout-evaluation" => V23IncidencePhase::HoldoutEvaluation,
        _ => return None,
    };
    Some(if execute {
        V23IncidenceRunMode::Execute(phase)
    } else {
        V23IncidenceRunMode::Preflight(phase)
    })
}

fn parse_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V23IncidenceLocalPhaseRequest, String> {
    let mut arguments = arguments.into_iter();
    let mut mode = None;
    let mut manifest_path = None;
    let mut parent_receipt_path = None;
    let mut preflight_receipt_path = None;
    let mut scratch_path = None;
    let mut output_path = None;
    let mut executable_sha256 = None;
    let mut input_paths = Vec::new();

    while let Some(flag) = arguments.next() {
        if let Some(value) = gate(&flag) {
            set_once(&mut mode, value, "phase gate")?;
            continue;
        }
        match flag.as_str() {
            "--manifest" => {
                let value = take_value(&mut arguments, &flag)?;
                set_once(&mut manifest_path, PathBuf::from(value), &flag)?;
            }
            "--parent-receipt" => {
                let value = take_value(&mut arguments, &flag)?;
                set_once(&mut parent_receipt_path, PathBuf::from(value), &flag)?;
            }
            "--preflight-receipt" => {
                let value = take_value(&mut arguments, &flag)?;
                set_once(&mut preflight_receipt_path, PathBuf::from(value), &flag)?;
            }
            "--scratch" => {
                let value = take_value(&mut arguments, &flag)?;
                set_once(&mut scratch_path, PathBuf::from(value), &flag)?;
            }
            "--output" => {
                let value = take_value(&mut arguments, &flag)?;
                set_once(&mut output_path, PathBuf::from(value), &flag)?;
            }
            "--executable-sha256" => {
                let value = take_value(&mut arguments, &flag)?;
                if !lower_hex_digest(&value) {
                    return Err("--executable-sha256 differs".to_string());
                }
                set_once(&mut executable_sha256, value, &flag)?;
            }
            "--input-role" => {
                let role = take_value(&mut arguments, &flag)?;
                let path = PathBuf::from(take_named_value(&mut arguments, "--input-path")?);
                let uri = take_named_value(&mut arguments, "--input-uri")?;
                let digest_algorithm =
                    take_named_value(&mut arguments, "--input-digest-algorithm")?;
                let digest = take_named_value(&mut arguments, "--input-digest")?;
                let encoded_bytes = take_named_value(&mut arguments, "--input-bytes")?
                    .parse::<u64>()
                    .map_err(|_| "--input-bytes differs".to_string())?;
                let generation = take_named_value(&mut arguments, "--input-generation")?;
                if !lower_hex_digest(&digest) || encoded_bytes == 0 {
                    return Err("input identity differs".to_string());
                }
                input_paths.push(V23IncidenceLocalRolePath {
                    identity: V23IncidenceObjectIdentity {
                        role,
                        uri,
                        digest_algorithm,
                        digest,
                        encoded_bytes,
                        generation,
                    },
                    path,
                });
            }
            _ => return Err(format!("unknown argument {flag}")),
        }
    }

    let request = V23IncidenceLocalPhaseRequest {
        mode: mode.ok_or_else(|| "phase gate is absent".to_string())?,
        manifest_path: manifest_path.ok_or_else(|| "--manifest is absent".to_string())?,
        parent_receipt_path,
        preflight_receipt_path,
        input_paths,
        scratch_path: scratch_path.ok_or_else(|| "--scratch is absent".to_string())?,
        output_path: output_path.ok_or_else(|| "--output is absent".to_string())?,
        executable_sha256: executable_sha256
            .ok_or_else(|| "--executable-sha256 is absent".to_string())?,
    };
    request.validate().map_err(|error| error.to_string())?;
    Ok(request)
}

#[cfg(not(test))]
fn main() {
    let request = parse_args(std::env::args().skip(1)).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(2);
    });
    let output_path = request.output_path.clone();
    let bytes = run_v23_incidence_local_phase(request).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    std::fs::write(&output_path, bytes).unwrap_or_else(|error| {
        eprintln!("failed to write {}: {error}", output_path.display());
        std::process::exit(1);
    });
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const PHASES: [V23IncidencePhase; 5] = [
        V23IncidencePhase::TreeTraining,
        V23IncidencePhase::PostingConstruction,
        V23IncidencePhase::DevelopmentEvaluation,
        V23IncidencePhase::HoldoutBinding,
        V23IncidencePhase::HoldoutEvaluation,
    ];

    fn phase_name(phase: V23IncidencePhase) -> &'static str {
        match phase {
            V23IncidencePhase::TreeTraining => "tree-training",
            V23IncidencePhase::PostingConstruction => "posting-construction",
            V23IncidencePhase::DevelopmentEvaluation => "development-evaluation",
            V23IncidencePhase::HoldoutBinding => "holdout-binding",
            V23IncidencePhase::HoldoutEvaluation => "holdout-evaluation",
        }
    }

    fn input_roles(
        phase: V23IncidencePhase,
        execute: bool,
    ) -> &'static [(&'static str, &'static str)] {
        match (phase, execute) {
            (V23IncidencePhase::TreeTraining, true) => &[
                ("construction-manifest", "sha256"),
                ("dataset-meta", "sha256"),
                ("training-shard-0000", "sha256"),
            ],
            (V23IncidencePhase::TreeTraining, false) => &[
                ("construction-manifest", "sha256"),
                ("training-shard-0000", "sha256"),
            ],
            (V23IncidencePhase::PostingConstruction, _) => &[
                ("phase-manifest", "sha256"),
                ("parent-receipt", "sha256"),
                ("incidence-tree", "blake3"),
                ("page-roster", "sha256"),
                ("page-body-0000", "blake3"),
            ],
            (V23IncidencePhase::DevelopmentEvaluation, true) => &[
                ("phase-manifest", "sha256"),
                ("parent-receipt", "sha256"),
                ("incidence-tree", "blake3"),
                ("incidence-postings-one", "blake3"),
                ("incidence-postings-two", "blake3"),
                ("d2-report", "sha256"),
                ("query-parquet", "sha256"),
            ],
            (V23IncidencePhase::DevelopmentEvaluation, false) => &[
                ("phase-manifest", "sha256"),
                ("parent-receipt", "sha256"),
                ("incidence-tree", "blake3"),
                ("incidence-postings-one", "blake3"),
                ("incidence-postings-two", "blake3"),
            ],
            (V23IncidencePhase::HoldoutBinding, true) => &[
                ("phase-manifest", "sha256"),
                ("parent-receipt", "sha256"),
                ("development-result", "sha256"),
                ("page-roster", "sha256"),
                ("neighbors-parquet", "sha256"),
                ("page-body-0000", "blake3"),
            ],
            (V23IncidencePhase::HoldoutBinding, false) => &[
                ("phase-manifest", "sha256"),
                ("parent-receipt", "sha256"),
                ("page-roster", "sha256"),
                ("page-body-0000", "blake3"),
            ],
            (V23IncidencePhase::HoldoutEvaluation, true) => &[
                ("phase-manifest", "sha256"),
                ("parent-receipt", "sha256"),
                ("development-result", "sha256"),
                ("development-latency", "blake3"),
                ("incidence-tree", "blake3"),
                ("incidence-postings-one", "blake3"),
                ("incidence-postings-two", "blake3"),
                ("query-parquet", "sha256"),
                ("holdout-truth", "sha256"),
            ],
            (V23IncidencePhase::HoldoutEvaluation, false) => &[
                ("phase-manifest", "sha256"),
                ("parent-receipt", "sha256"),
                ("incidence-tree", "blake3"),
                ("incidence-postings-one", "blake3"),
                ("incidence-postings-two", "blake3"),
            ],
        }
    }

    fn arguments(phase: V23IncidencePhase, execute: bool) -> Vec<String> {
        let mut arguments = vec![
            format!(
                "--{}-{}",
                if execute { "execute" } else { "preflight" },
                phase_name(phase)
            ),
            "--manifest".to_string(),
            if phase == V23IncidencePhase::TreeTraining {
                "/inputs/construction-manifest".to_string()
            } else {
                "/inputs/phase-manifest".to_string()
            },
            "--scratch".to_string(),
            "/scratch".to_string(),
            "--output".to_string(),
            "/output/receipt.json".to_string(),
            "--executable-sha256".to_string(),
            "11".repeat(32),
        ];
        if phase != V23IncidencePhase::TreeTraining {
            arguments.extend([
                "--parent-receipt".to_string(),
                "/inputs/parent-receipt".to_string(),
            ]);
        }
        if execute {
            arguments.extend([
                "--preflight-receipt".to_string(),
                "/inputs/preflight-receipt".to_string(),
            ]);
        }
        for (index, (role, algorithm)) in input_roles(phase, execute).iter().enumerate() {
            arguments.extend([
                "--input-role".to_string(),
                (*role).to_string(),
                "--input-path".to_string(),
                format!("/inputs/{role}"),
                "--input-uri".to_string(),
                format!("s3://borsuk-evidence/{role}"),
                "--input-digest-algorithm".to_string(),
                (*algorithm).to_string(),
                "--input-digest".to_string(),
                format!("{:02x}", index + 32).repeat(32),
                "--input-bytes".to_string(),
                (index + 1).to_string(),
                "--input-generation".to_string(),
                format!("generation-{index:04}"),
            ]);
        }
        if execute {
            arguments.extend([
                "--input-role".to_string(),
                "preflight-receipt".to_string(),
                "--input-path".to_string(),
                "/inputs/preflight-receipt".to_string(),
                "--input-uri".to_string(),
                "s3://borsuk-evidence/preflight-receipt".to_string(),
                "--input-digest-algorithm".to_string(),
                "sha256".to_string(),
                "--input-digest".to_string(),
                "fe".repeat(32),
                "--input-bytes".to_string(),
                "1".to_string(),
                "--input-generation".to_string(),
                "generation-preflight".to_string(),
            ]);
        }
        arguments
    }

    #[test]
    fn v23_incidence_example_requires_one_phase_and_exact_local_roles() {
        for phase in PHASES {
            let preflight = parse_args(arguments(phase, false)).unwrap();
            assert_eq!(preflight.mode, V23IncidenceRunMode::Preflight(phase));
            assert_eq!(
                preflight.manifest_path,
                if phase == V23IncidencePhase::TreeTraining {
                    Path::new("/inputs/construction-manifest")
                } else {
                    Path::new("/inputs/phase-manifest")
                }
            );
            assert_eq!(preflight.input_paths.len(), input_roles(phase, false).len());

            let execute = parse_args(arguments(phase, true)).unwrap();
            assert_eq!(execute.mode, V23IncidenceRunMode::Execute(phase));
            assert!(execute.preflight_receipt_path.is_some());
        }
    }

    #[test]
    fn v23_incidence_example_rejects_missing_duplicate_unknown_and_invalid_arguments() {
        let valid = arguments(V23IncidencePhase::TreeTraining, false);
        for changed in [
            valid[1..].to_vec(),
            [valid.clone(), vec![valid[0].clone()]].concat(),
            [
                valid.clone(),
                vec!["--unknown".to_string(), "value".to_string()],
            ]
            .concat(),
        ] {
            assert!(parse_args(changed).is_err());
        }

        let mut changed = valid.clone();
        *changed
            .iter_mut()
            .find(|value| value.as_str() == "1")
            .unwrap() = "0".to_string();
        assert!(parse_args(changed).is_err());

        let mut changed = valid;
        let digest = changed
            .iter()
            .position(|value| value == "--input-digest")
            .unwrap()
            + 1;
        changed[digest] = "AA".repeat(32);
        assert!(parse_args(changed).is_err());
    }

    #[test]
    fn v23_incidence_example_refuses_network_storage_query_leak_and_d3_flags() {
        for flag in [
            "--bucket",
            "--aws-profile",
            "--endpoint",
            "--page-uri",
            "--storage-uri",
            "--d3",
            "--query",
            "--neighbors",
        ] {
            let mut changed = arguments(V23IncidencePhase::TreeTraining, false);
            changed.extend([flag.to_string(), "forbidden".to_string()]);
            assert!(
                parse_args(changed).is_err(),
                "accepted forbidden flag {flag}"
            );
        }

        let development =
            parse_args(arguments(V23IncidencePhase::DevelopmentEvaluation, false)).unwrap();
        assert!(
            development
                .input_paths
                .iter()
                .all(|input| input.identity.role != "query-parquet")
        );
        let development_execute =
            parse_args(arguments(V23IncidencePhase::DevelopmentEvaluation, true)).unwrap();
        assert!(
            development_execute
                .input_paths
                .iter()
                .any(|input| input.identity.role == "query-parquet")
        );
        let holdout = parse_args(arguments(V23IncidencePhase::HoldoutBinding, false)).unwrap();
        assert!(
            holdout
                .input_paths
                .iter()
                .all(|input| input.identity.role != "neighbors-parquet")
        );
        let holdout_execute =
            parse_args(arguments(V23IncidencePhase::HoldoutBinding, true)).unwrap();
        assert!(
            holdout_execute
                .input_paths
                .iter()
                .any(|input| input.identity.role == "neighbors-parquet")
        );
    }

    #[test]
    fn v23_incidence_example_calls_only_the_local_high_level_runner() {
        let request: V23IncidenceLocalPhaseRequest =
            parse_args(arguments(V23IncidencePhase::TreeTraining, false)).unwrap();
        let error = run_v23_incidence_local_phase(request).unwrap_err();
        assert!(error.to_string().contains("sandbox probes are absent"));
    }
}

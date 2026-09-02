//! Offline local-file boundary for the claim-ineligible V24 witness-router campaign.

#[cfg(not(test))]
use std::io::Write;
use std::{collections::BTreeSet, path::PathBuf};

use borsuk::{BorsukError, V24LocalPhase};
#[cfg(not(test))]
use borsuk::{V24LocalRunRequest, run_v24_local_request};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V24CliPhase {
    TrainWitnesses,
    BuildPostings,
    EvaluatePseudoqueries,
    EvaluateDevelopment,
    BindHoldout,
    EvaluateHoldout,
}

impl From<V24CliPhase> for V24LocalPhase {
    fn from(value: V24CliPhase) -> Self {
        match value {
            V24CliPhase::TrainWitnesses => Self::TrainWitnesses,
            V24CliPhase::BuildPostings => Self::BuildPostings,
            V24CliPhase::EvaluatePseudoqueries => Self::EvaluatePseudoqueries,
            V24CliPhase::EvaluateDevelopment => Self::EvaluateDevelopment,
            V24CliPhase::BindHoldout => Self::BindHoldout,
            V24CliPhase::EvaluateHoldout => Self::EvaluateHoldout,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct V24Cli {
    manifest: PathBuf,
    input_dir: PathBuf,
    output_dir: PathBuf,
    phase: V24CliPhase,
    execute: bool,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn parse_v24_cli(arguments: Vec<String>) -> borsuk::Result<V24Cli> {
    let mut manifest = None;
    let mut input_dir = None;
    let mut output_dir = None;
    let mut phase = None;
    let mut execute = false;
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| invalid("V24 CLI program is missing"))?;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--manifest" | "--input-dir" | "--output-dir" => {
                let value = PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| invalid("V24 CLI flag value is missing"))?,
                );
                let target = match argument.as_str() {
                    "--manifest" => &mut manifest,
                    "--input-dir" => &mut input_dir,
                    _ => &mut output_dir,
                };
                if target.replace(value).is_some() {
                    return Err(invalid("V24 CLI flag is duplicated"));
                }
            }
            "--train-witnesses"
            | "--build-postings"
            | "--evaluate-pseudoqueries"
            | "--evaluate-development"
            | "--bind-holdout"
            | "--evaluate-holdout" => {
                let parsed = match argument.as_str() {
                    "--train-witnesses" => V24CliPhase::TrainWitnesses,
                    "--build-postings" => V24CliPhase::BuildPostings,
                    "--evaluate-pseudoqueries" => V24CliPhase::EvaluatePseudoqueries,
                    "--evaluate-development" => V24CliPhase::EvaluateDevelopment,
                    "--bind-holdout" => V24CliPhase::BindHoldout,
                    _ => V24CliPhase::EvaluateHoldout,
                };
                if phase.replace(parsed).is_some() {
                    return Err(invalid("V24 CLI phase is ambiguous"));
                }
            }
            "--execute" if !execute => execute = true,
            _ => return Err(invalid("V24 CLI flag is unknown or forbidden")),
        }
    }
    let parsed = V24Cli {
        manifest: manifest.ok_or_else(|| invalid("V24 CLI manifest is missing"))?,
        input_dir: input_dir.ok_or_else(|| invalid("V24 CLI input directory is missing"))?,
        output_dir: output_dir.ok_or_else(|| invalid("V24 CLI output directory is missing"))?,
        phase: phase.ok_or_else(|| invalid("V24 CLI phase is missing"))?,
        execute,
    };
    if !parsed.execute {
        return Err(invalid("V24 CLI execution boundary differs"));
    }
    Ok(parsed)
}

fn validate_v24_cli_environment<I>(environment: I) -> borsuk::Result<()>
where
    I: IntoIterator<Item = (String, String)>,
{
    let forbidden = environment
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("AWS_"))
        .collect::<BTreeSet<_>>();
    if !forbidden.is_empty() {
        return Err(invalid("V24 CLI AWS environment is forbidden"));
    }
    Ok(())
}

#[cfg(not(test))]
fn main() {
    let result = (|| {
        validate_v24_cli_environment(std::env::vars())?;
        let cli = parse_v24_cli(std::env::args().collect())?;
        run_v24_local_request(V24LocalRunRequest {
            manifest: cli.manifest,
            input_dir: cli.input_dir,
            output_dir: cli.output_dir,
            phase: cli.phase.into(),
        })
    })();
    match result {
        Ok(bytes) => {
            if let Err(error) = std::io::stdout().write_all(&bytes) {
                eprintln!("V24 stdout failed: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use borsuk::{V24LocalRunRequest, run_v24_local_request};

    use super::{V24CliPhase, parse_v24_cli, validate_v24_cli_environment};

    fn args(phase: &str) -> Vec<String> {
        [
            "v24_witness_page_router",
            "--manifest",
            "/tmp/manifest.json",
            "--input-dir",
            "/tmp/v24-input",
            "--output-dir",
            "/tmp/v24-output",
            phase,
            "--execute",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v24_witness_cli_requires_one_explicit_offline_phase_and_execute() {
        for (flag, phase) in [
            ("--train-witnesses", V24CliPhase::TrainWitnesses),
            ("--build-postings", V24CliPhase::BuildPostings),
            (
                "--evaluate-pseudoqueries",
                V24CliPhase::EvaluatePseudoqueries,
            ),
            ("--evaluate-development", V24CliPhase::EvaluateDevelopment),
            ("--bind-holdout", V24CliPhase::BindHoldout),
            ("--evaluate-holdout", V24CliPhase::EvaluateHoldout),
        ] {
            let parsed = parse_v24_cli(args(flag)).unwrap();
            assert_eq!(parsed.manifest, PathBuf::from("/tmp/manifest.json"));
            assert_eq!(parsed.input_dir, PathBuf::from("/tmp/v24-input"));
            assert_eq!(parsed.output_dir, PathBuf::from("/tmp/v24-output"));
            assert_eq!(parsed.phase, phase);
            assert!(parsed.execute);
        }

        let request_type_lock: fn(V24LocalRunRequest) -> borsuk::Result<Vec<u8>> =
            run_v24_local_request;
        let _ = request_type_lock;
    }

    #[test]
    fn v24_witness_cli_rejects_ambiguous_network_storage_and_legacy_surface() {
        let invalid = [
            vec!["--train-witnesses"],
            vec!["--execute"],
            vec!["--train-witnesses", "--evaluate-holdout"],
            vec!["--manifest", "/a", "--manifest", "/b"],
            vec!["--bucket", "bucket"],
            vec!["--endpoint", "https://example.invalid"],
            vec!["--page-prefix", "pages/"],
            vec!["--storage", "s3"],
            vec!["--v23"],
            vec!["--d3"],
            vec!["--unknown"],
        ];
        for suffix in invalid {
            let mut candidate = args("--train-witnesses");
            candidate.extend(suffix.into_iter().map(str::to_owned));
            assert!(parse_v24_cli(candidate).is_err());
        }
        let without_execute = args("--train-witnesses")
            .into_iter()
            .filter(|value| value != "--execute")
            .collect::<Vec<_>>();
        assert!(parse_v24_cli(without_execute).is_err());
    }

    #[test]
    fn v24_witness_cli_rejects_credentials_and_nonempty_output_before_execution() {
        assert!(validate_v24_cli_environment(Vec::<(String, String)>::new()).is_ok());
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
        ] {
            assert!(
                validate_v24_cli_environment(vec![(name.to_owned(), "present".to_owned())])
                    .is_err()
            );
        }

        let root = std::env::temp_dir().join(format!("borsuk-v24-cli-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("input")).unwrap();
        fs::create_dir_all(root.join("output")).unwrap();
        fs::write(root.join("manifest.json"), b"{}\n").unwrap();
        let mut candidate = vec![
            "v24_witness_page_router".to_owned(),
            "--manifest".to_owned(),
            root.join("manifest.json").display().to_string(),
            "--input-dir".to_owned(),
            root.join("input").display().to_string(),
            "--output-dir".to_owned(),
            root.join("output").display().to_string(),
            "--train-witnesses".to_owned(),
            "--execute".to_owned(),
        ];
        assert!(parse_v24_cli(candidate.clone()).is_ok());
        fs::write(root.join("output/existing"), b"owned").unwrap();
        let parsed = parse_v24_cli(candidate.clone()).unwrap();
        let error = run_v24_local_request(V24LocalRunRequest {
            manifest: parsed.manifest,
            input_dir: parsed.input_dir,
            output_dir: parsed.output_dir,
            phase: parsed.phase.into(),
        })
        .unwrap_err();
        assert!(error.to_string().contains("path authority"));
        candidate.push("--bucket".to_owned());
        candidate.push("forbidden".to_owned());
        assert!(parse_v24_cli(candidate).is_err());
        fs::remove_file(root.join("output/existing")).unwrap();
        fs::remove_file(root.join("manifest.json")).unwrap();
        fs::remove_dir(root.join("input")).unwrap();
        fs::remove_dir(root.join("output")).unwrap();
        fs::remove_dir(root).unwrap();
    }
}

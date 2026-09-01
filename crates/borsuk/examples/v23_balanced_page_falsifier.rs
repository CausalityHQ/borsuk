//! Local-only V23 balanced-page falsifier with no storage or page-body surface.

use std::{collections::BTreeMap, env, ffi::OsString, io::Write, path::PathBuf, process::ExitCode};

use borsuk::{V23BalancedLocalMode, V23BalancedLocalRequest, run_v23_balanced_local_request};

fn required(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn parse_v23_balanced_page_falsifier_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V23BalancedLocalRequest, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut values = BTreeMap::new();
    let mut mode = None;
    while let Some(flag) = arguments.next() {
        if matches!(flag.as_str(), "--preflight" | "--execute") {
            let parsed = if flag == "--preflight" {
                V23BalancedLocalMode::Preflight
            } else {
                V23BalancedLocalMode::Execute
            };
            if mode.replace(parsed).is_some() {
                return Err("duplicate or ambiguous run mode".to_owned());
            }
            continue;
        }
        if !matches!(
            flag.as_str(),
            "--manifest" | "--input-directory" | "--output-directory"
        ) {
            return Err(format!("unknown or forbidden flag {flag}"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        if value.starts_with("--") || values.insert(flag.clone(), value).is_some() {
            return Err(format!("invalid or duplicate flag {flag}"));
        }
    }
    Ok(V23BalancedLocalRequest {
        manifest: PathBuf::from(required(&mut values, "--manifest")?),
        input_directory: PathBuf::from(required(&mut values, "--input-directory")?),
        output_directory: PathBuf::from(required(&mut values, "--output-directory")?),
        mode: mode.ok_or_else(|| "missing required run mode".to_owned())?,
    })
}

fn parse_v23_balanced_page_falsifier_os_args(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<V23BalancedLocalRequest, String> {
    let arguments = arguments
        .into_iter()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| "argument is not valid UTF-8".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    parse_v23_balanced_page_falsifier_args(arguments)
}

fn run() -> Result<(), String> {
    let request = parse_v23_balanced_page_falsifier_os_args(env::args_os())?;
    let bytes = run_v23_balanced_local_request(request).map_err(|error| error.to_string())?;
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
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

    use super::{
        parse_v23_balanced_page_falsifier_args, parse_v23_balanced_page_falsifier_os_args,
    };
    use borsuk::{V23BalancedLocalMode, V23BalancedLocalRequest, run_v23_balanced_local_request};

    fn arguments(mode: &str) -> Vec<String> {
        vec![
            "v23-balanced-page-falsifier".to_owned(),
            "--manifest".to_owned(),
            "/inputs/manifest.json".to_owned(),
            "--input-directory".to_owned(),
            "/inputs".to_owned(),
            "--output-directory".to_owned(),
            "/outputs".to_owned(),
            mode.to_owned(),
        ]
    }

    #[test]
    fn v23_balanced_cli_parses_exact_local_directories_and_one_run_mode() {
        let preflight = parse_v23_balanced_page_falsifier_args(arguments("--preflight")).unwrap();
        assert_eq!(preflight.manifest, PathBuf::from("/inputs/manifest.json"));
        assert_eq!(preflight.input_directory, PathBuf::from("/inputs"));
        assert_eq!(preflight.output_directory, PathBuf::from("/outputs"));
        assert_eq!(preflight.mode, V23BalancedLocalMode::Preflight);

        let execute = parse_v23_balanced_page_falsifier_args(arguments("--execute")).unwrap();
        assert_eq!(execute.mode, V23BalancedLocalMode::Execute);

        let typed: V23BalancedLocalRequest = execute;
        let _single_run_boundary = run_v23_balanced_local_request;
        drop(typed);
    }

    #[test]
    fn v23_balanced_cli_rejects_missing_duplicate_unknown_and_ambiguous_flags() {
        let baseline = arguments("--preflight");
        for index in [1, 3, 5, 7] {
            let mut missing = baseline.clone();
            let width = usize::from(index != 7) + 1;
            missing.drain(index..index + width);
            assert!(parse_v23_balanced_page_falsifier_args(missing).is_err());
        }

        for (flag, value) in [
            ("--manifest", Some("/duplicate/manifest.json")),
            ("--input-directory", Some("/duplicate/inputs")),
            ("--output-directory", Some("/duplicate/outputs")),
            ("--preflight", None),
        ] {
            let mut duplicate = baseline.clone();
            duplicate.push(flag.to_owned());
            if let Some(value) = value {
                duplicate.push(value.to_owned());
            }
            assert!(parse_v23_balanced_page_falsifier_args(duplicate).is_err());
        }

        let mut both_modes = baseline.clone();
        both_modes.push("--execute".to_owned());
        assert!(parse_v23_balanced_page_falsifier_args(both_modes).is_err());

        let mut unknown = baseline;
        unknown.extend(["--unknown".to_owned(), "value".to_owned()]);
        assert!(parse_v23_balanced_page_falsifier_args(unknown).is_err());
    }

    #[test]
    fn v23_balanced_cli_refuses_storage_page_d3_holdout_loader_and_mount_surfaces() {
        for forbidden in [
            "--bucket",
            "--endpoint",
            "--storage-uri",
            "--aws-profile",
            "--credential",
            "--page-body",
            "--page-prefix",
            "--d3",
            "--holdout",
            "--loader",
            "--mount",
        ] {
            let mut changed = arguments("--execute");
            changed.extend([forbidden.to_owned(), "forbidden".to_owned()]);
            assert!(parse_v23_balanced_page_falsifier_args(changed).is_err());
        }
    }

    #[test]
    fn v23_balanced_cli_rejects_non_utf8_arguments_without_panicking() {
        let mut changed = arguments("--preflight")
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        changed[2] = OsString::from_vec(vec![b'/', 0xff]);
        assert!(parse_v23_balanced_page_falsifier_os_args(changed).is_err());
    }
}

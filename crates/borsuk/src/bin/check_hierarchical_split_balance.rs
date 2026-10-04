//! Frozen source-only split falsifier. Usage: CONFIG SHA256 NEW_OUTPUT.
//! Caller must supply CPU1/512Mi/noSwap cgroup controls and a 180s unit timeout.
//! A PASS screens source-neighborhood geometry; it is not recall qualification.
//! Report bodies require a matching original supervisor exit/resource receipt.
use borsuk::hierarchical_semantic_cells::split_balance_diagnostic;
use std::{ffi::OsString, path::PathBuf};

fn arguments(args: Vec<OsString>) -> Result<(PathBuf, String, PathBuf), &'static str> {
    if args.len() != 3 {
        return Err("usage: check_hierarchical_split_balance CONFIG SHA256 NEW_OUTPUT");
    }
    let sha = args[1].to_str().ok_or("SHA256 must be UTF-8")?;
    if sha.len() != 64
        || !sha
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
    {
        return Err("SHA256 must be 64 lowercase hexadecimal characters");
    }
    let config = PathBuf::from(&args[0]);
    let output = PathBuf::from(&args[2]);
    if !config.is_absolute() || !output.is_absolute() || config == output {
        return Err("CONFIG and distinct NEW_OUTPUT must be absolute local paths");
    }
    Ok((config, sha.to_owned(), output))
}

fn main() {
    let status = arguments(std::env::args_os().skip(1).collect())
        .map_err(|error| error.to_owned())
        .and_then(|(config, sha, output)| {
            split_balance_diagnostic::execute(&config, &sha, &output)
                .map_err(|error| error.to_string())
        });
    let code = exit_code(&status);
    match status {
        Ok(status) => {
            println!("{status}: source-neighborhood diagnostic; not recall or product quality");
        }
        Err(error) => {
            eprintln!("INVALID: {error}");
        }
    }
    std::process::exit(code);
}
fn exit_code(status: &Result<String, String>) -> i32 {
    match status.as_ref().map(String::as_str) {
        Ok("PASS") => 0,
        Ok("REJECT") => 1,
        Ok("INCONCLUSIVE" | "INPUT_UNAVAILABLE") => 3,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Vec<OsString> {
        vec![
            "/tmp/config".into(),
            "a".repeat(64).into(),
            "/tmp/new-output".into(),
        ]
    }

    #[test]
    fn strict_source_only_cli() {
        assert!(arguments(valid()).is_ok());
        for flag in [
            "--query",
            "--truth",
            "--gt",
            "--download",
            "--sample-rows",
            "--retrain",
        ] {
            let mut args = valid();
            args.push(flag.into());
            assert!(arguments(args).is_err());
        }
        let mut args = valid();
        args[1] = "A".repeat(64).into();
        assert!(arguments(args).is_err());
        let mut args = valid();
        args[0] = "relative-config".into();
        assert!(arguments(args).is_err());
        let mut args = valid();
        args[2] = args[0].clone();
        assert!(arguments(args).is_err());
        assert_eq!(exit_code(&Err("file fsync failed".into())), 2);
        assert_eq!(exit_code(&Err("directory fsync failed".into())), 2);
        assert_eq!(exit_code(&Ok("PASS".into())), 0);
        assert_eq!(exit_code(&Ok("REJECT".into())), 1);
        assert_eq!(exit_code(&Ok("INCONCLUSIVE".into())), 3);
    }
}

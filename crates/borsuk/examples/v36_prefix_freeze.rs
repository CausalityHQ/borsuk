//! Bounded, claim-ineligible V36 diagnostic population freezer.

use std::{collections::BTreeMap, env, path::PathBuf, process::ExitCode};

use borsuk::{V36PrefixFreezeRequest, run_v36_prefix_freeze};

#[derive(Debug, PartialEq, Eq)]
struct Args {
    authority: PathBuf,
    execution_authority: PathBuf,
    source_registry: PathBuf,
    source_archive: PathBuf,
    output: PathBuf,
    scratch: PathBuf,
}

fn parse_args(values: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut values = values.into_iter();
    if values.next().as_deref() != Some("--execute-prefix-freeze") {
        return Err("V36 prefix freezer execution capability differs".into());
    }
    let mut fields = BTreeMap::new();
    while let Some(flag) = values.next() {
        if !matches!(
            flag.as_str(),
            "--authority"
                | "--execution-authority"
                | "--source-registry"
                | "--source-archive"
                | "--output"
                | "--scratch"
        ) || fields.contains_key(&flag)
        {
            return Err("V36 prefix freezer argument differs".into());
        }
        let value = values
            .next()
            .ok_or_else(|| "V36 prefix freezer argument value is missing".to_owned())?;
        fields.insert(flag, value);
    }
    let mut take = |flag: &str| {
        fields
            .remove(flag)
            .ok_or_else(|| format!("V36 prefix freezer {flag} is missing"))
    };
    let authority = take("--authority")?.into();
    let execution_authority = take("--execution-authority")?.into();
    let source_registry = take("--source-registry")?.into();
    let source_archive = take("--source-archive")?.into();
    let output = take("--output")?.into();
    let scratch = take("--scratch")?.into();
    if !fields.is_empty() {
        return Err("V36 prefix freezer arguments differ".into());
    }
    Ok(Args {
        authority,
        execution_authority,
        source_registry,
        source_archive,
        output,
        scratch,
    })
}

fn main() -> ExitCode {
    let parsed = parse_args(env::args().skip(1));
    let result = parsed.and_then(|args| {
        let executable = env::current_exe()
            .map_err(|error| format!("V36 prefix freezer executable differs: {error}"))?;
        run_v36_prefix_freeze(V36PrefixFreezeRequest {
            authority: args.authority,
            executable,
            execution_authority: args.execution_authority,
            source_registry: args.source_registry,
            source_archive: args.source_archive,
            output: args.output,
            scratch: args.scratch,
        })
        .map_err(|error| error.to_string())
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Vec<String> {
        [
            "--execute-prefix-freeze",
            "--authority",
            "authority.json",
            "--execution-authority",
            "execution-authority.json",
            "--source-registry",
            "registry.json",
            "--source-archive",
            "source.tar.zst",
            "--output",
            "output",
            "--scratch",
            "scratch",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v36_prefix_freezer_cli_is_explicit_and_bounded() {
        let parsed = parse_args(valid()).unwrap();
        assert_eq!(
            parsed.execution_authority,
            PathBuf::from("execution-authority.json")
        );
        assert_eq!(parsed.scratch, PathBuf::from("scratch"));

        let mut missing = valid();
        missing.drain(2..4);
        assert!(parse_args(missing).is_err());
        let mut duplicate = valid();
        duplicate.extend(["--output".into(), "other".into()]);
        assert!(parse_args(duplicate).is_err());
        let mut unknown = valid();
        unknown.extend(["--bucket".into(), "forbidden".into()]);
        assert!(parse_args(unknown).is_err());
    }
}

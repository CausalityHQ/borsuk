//! Bounded, claim-ineligible V36 diagnostic population freezer.

use std::{collections::BTreeMap, env, path::PathBuf, process::ExitCode};

use borsuk::{V36PrefixFreezeRequest, run_v36_prefix_freeze};

#[derive(Debug, PartialEq, Eq)]
struct Args {
    authority: PathBuf,
    checkpoint_outbox: PathBuf,
    execution_authority: PathBuf,
    producer_instance_id: String,
    source_registry: PathBuf,
    source_archive: PathBuf,
    output: PathBuf,
    resume_checkpoint: Option<PathBuf>,
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
                | "--checkpoint-outbox"
                | "--producer-instance-id"
                | "--resume-checkpoint"
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
    let checkpoint_outbox = take("--checkpoint-outbox")?.into();
    let execution_authority = take("--execution-authority")?.into();
    let producer_instance_id = take("--producer-instance-id")?;
    let source_registry = take("--source-registry")?.into();
    let source_archive = take("--source-archive")?.into();
    let output = take("--output")?.into();
    let scratch = take("--scratch")?.into();
    let resume_checkpoint = fields.remove("--resume-checkpoint").map(PathBuf::from);
    if !fields.is_empty() {
        return Err("V36 prefix freezer arguments differ".into());
    }
    Ok(Args {
        authority,
        checkpoint_outbox,
        execution_authority,
        producer_instance_id,
        source_registry,
        source_archive,
        output,
        resume_checkpoint,
        scratch,
    })
}

fn exit_code_for_error(error: &borsuk::BorsukError) -> ExitCode {
    match error {
        borsuk::BorsukError::V36PrefixSourceInsufficient => ExitCode::from(42),
        _ => ExitCode::FAILURE,
    }
}

fn main() -> ExitCode {
    let args = match parse_args(env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let executable = match env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            eprintln!("V36 prefix freezer executable differs: {error}");
            return ExitCode::FAILURE;
        }
    };
    match run_v36_prefix_freeze(V36PrefixFreezeRequest {
        authority: args.authority,
        checkpoint_outbox: args.checkpoint_outbox,
        executable,
        execution_authority: args.execution_authority,
        source_registry: args.source_registry,
        source_archive: args.source_archive,
        output: args.output,
        producer_instance_id: args.producer_instance_id,
        resume_checkpoint: args.resume_checkpoint,
        scratch: args.scratch,
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            exit_code_for_error(&error)
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
            "--checkpoint-outbox",
            "outbox",
            "--producer-instance-id",
            "i-fixture",
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
        assert_eq!(parsed.checkpoint_outbox, PathBuf::from("outbox"));
        assert_eq!(parsed.producer_instance_id, "i-fixture");
        assert_eq!(parsed.resume_checkpoint, None);

        let mut resumed = valid();
        resumed.extend(["--resume-checkpoint".into(), "resume".into()]);
        assert_eq!(
            parse_args(resumed).unwrap().resume_checkpoint,
            Some(PathBuf::from("resume"))
        );

        let mut missing = valid();
        missing.drain(2..4);
        assert!(parse_args(missing).is_err());
        let mut duplicate = valid();
        duplicate.extend(["--output".into(), "other".into()]);
        assert!(parse_args(duplicate).is_err());
        let mut unknown = valid();
        unknown.extend(["--bucket".into(), "forbidden".into()]);
        assert!(parse_args(unknown).is_err());
        let mut duplicate_resume = valid();
        duplicate_resume.extend([
            "--resume-checkpoint".into(),
            "resume-a".into(),
            "--resume-checkpoint".into(),
            "resume-b".into(),
        ]);
        assert!(parse_args(duplicate_resume).is_err());
        let mut missing_resume_value = valid();
        missing_resume_value.push("--resume-checkpoint".into());
        assert!(parse_args(missing_resume_value).is_err());
    }

    #[test]
    fn v36_prefix_freezer_source_insufficiency_has_a_closed_exit_code() {
        assert_eq!(
            exit_code_for_error(&borsuk::BorsukError::V36PrefixSourceInsufficient),
            ExitCode::from(42),
        );
    }
}

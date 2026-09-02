//! Offline authenticated conversion of immutable source artifacts into V24 Parquet.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk::{V24PreparationRunRequest, run_v24_preparation_request};

fn parse_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V24PreparationRunRequest, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments.next().ok_or("program name is absent")?;
    let mut values = BTreeMap::new();
    let mut execute = false;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-preparation" {
            if execute {
                return Err("duplicate execution flag".to_owned());
            }
            execute = true;
            continue;
        }
        if !matches!(
            flag.as_str(),
            "--manifest"
                | "--manifest-sha256"
                | "--input-dir"
                | "--output-dir"
                | "--construction-uri"
                | "--page-rows-uri"
        ) || values.contains_key(&flag)
        {
            return Err("unknown or duplicate flag".to_owned());
        }
        let value = arguments
            .next()
            .filter(|value| !value.is_empty() && !value.starts_with("--"))
            .ok_or_else(|| "flag value is absent".to_owned())?;
        values.insert(flag, value);
    }
    if !execute || values.len() != 6 {
        return Err("complete explicit preparation flags are required".to_owned());
    }
    let take = |values: &mut BTreeMap<String, String>, key: &str| {
        values
            .remove(key)
            .ok_or_else(|| format!("{key} is required"))
    };
    Ok(V24PreparationRunRequest {
        manifest: PathBuf::from(take(&mut values, "--manifest")?),
        manifest_sha256: take(&mut values, "--manifest-sha256")?,
        input_dir: PathBuf::from(take(&mut values, "--input-dir")?),
        output_dir: PathBuf::from(take(&mut values, "--output-dir")?),
        construction_uri: take(&mut values, "--construction-uri")?,
        page_rows_uri: take(&mut values, "--page-rows-uri")?,
    })
}

#[cfg(not(test))]
fn main() {
    let result = parse_args(std::env::args())
        .map_err(|error| error.to_string())
        .and_then(|request| {
            run_v24_preparation_request(request).map_err(|error| error.to_string())
        });
    match result {
        Ok(bytes) => {
            if std::io::stdout().write_all(&bytes).is_err() {
                std::process::exit(1);
            }
        }
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "{error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_args;

    fn args() -> Vec<String> {
        [
            "v24_prepare_witness_inputs",
            "--manifest",
            "/inputs/preparation-manifest.json",
            "--manifest-sha256",
            &"11".repeat(32),
            "--input-dir",
            "/inputs/staged",
            "--output-dir",
            "/outputs",
            "--construction-uri",
            "s3://borsuk-v24/run/construction-rows.parquet",
            "--page-rows-uri",
            "s3://borsuk-v24/run/page-rows.parquet",
            "--execute-preparation",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v24_prepare_cli_requires_exact_offline_file_boundary() {
        let request = parse_args(args()).unwrap();
        assert_eq!(
            request.manifest.to_str(),
            Some("/inputs/preparation-manifest.json")
        );
        assert_eq!(request.input_dir.to_str(), Some("/inputs/staged"));
        assert_eq!(request.output_dir.to_str(), Some("/outputs"));
    }

    #[test]
    fn v24_prepare_cli_rejects_storage_query_and_duplicate_flags() {
        for forbidden in [
            "--bucket",
            "--endpoint",
            "--page-prefix",
            "--query-parquet",
            "--neighbors-parquet",
            "--development-result",
            "--holdout-result",
            "--d3",
        ] {
            let mut candidate = args();
            candidate.extend([forbidden.to_owned(), "value".to_owned()]);
            assert!(parse_args(candidate).is_err(), "accepted {forbidden}");
        }
        let mut duplicate = args();
        duplicate.extend(["--input-dir".to_owned(), "/other".to_owned()]);
        assert!(parse_args(duplicate).is_err());
        let mut missing_execute = args();
        missing_execute.pop();
        assert!(parse_args(missing_execute).is_err());
    }
}

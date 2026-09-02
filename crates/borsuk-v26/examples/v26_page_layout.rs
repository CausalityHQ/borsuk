//! Offline V26 layout construction and evaluation executable.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26LocalObjectPath, V26ObjectIdentity, canonical_v26_layout_build_output_bytes,
    run_v26_layout_build_directory,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegisteredFile {
    path: PathBuf,
    uri: String,
    sha256: String,
    encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuildRequest {
    generation: String,
    manifest: RegisteredFile,
    input_dir: PathBuf,
    output_dir: PathBuf,
    output_uri_prefix: String,
    worker_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum V26CliMode {
    Build(BuildRequest),
}

fn take(values: &mut BTreeMap<String, String>, key: &str) -> Result<String, String> {
    values.remove(key).ok_or_else(|| format!("missing {key}"))
}

fn exact_lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_v26_args(args: Vec<String>) -> Result<V26CliMode, String> {
    let mut arguments = args.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "program is absent".to_owned())?;
    let mut build = false;
    let mut evaluate = false;
    let mut execute = false;
    let mut values = BTreeMap::new();
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--build-layout" => {
                if build {
                    return Err("duplicate --build-layout".to_owned());
                }
                build = true;
            }
            "--evaluate-layout" => {
                if evaluate {
                    return Err("duplicate --evaluate-layout".to_owned());
                }
                evaluate = true;
            }
            "--execute" => {
                if execute {
                    return Err("duplicate --execute".to_owned());
                }
                execute = true;
            }
            "--generation"
            | "--manifest-path"
            | "--manifest-uri"
            | "--manifest-sha256"
            | "--manifest-bytes"
            | "--input-dir"
            | "--output-dir"
            | "--output-uri-prefix"
            | "--workers" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| format!("missing value for {flag}"))?;
                if value.starts_with("--") || values.insert(flag.clone(), value).is_some() {
                    return Err(format!("invalid or duplicate {flag}"));
                }
            }
            _ => return Err(format!("unknown or forbidden flag {flag}")),
        }
    }
    if !execute || build == evaluate || !build {
        return Err("exactly one executable phase is required".to_owned());
    }
    let generation = take(&mut values, "--generation")?;
    let manifest = RegisteredFile {
        path: PathBuf::from(take(&mut values, "--manifest-path")?),
        uri: take(&mut values, "--manifest-uri")?,
        sha256: take(&mut values, "--manifest-sha256")?,
        encoded_bytes: take(&mut values, "--manifest-bytes")?
            .parse()
            .map_err(|_| "invalid --manifest-bytes".to_owned())?,
    };
    let input_dir = PathBuf::from(take(&mut values, "--input-dir")?);
    let output_dir = PathBuf::from(take(&mut values, "--output-dir")?);
    let output_uri_prefix = take(&mut values, "--output-uri-prefix")?;
    let worker_count = take(&mut values, "--workers")?
        .parse()
        .map_err(|_| "invalid --workers".to_owned())?;
    if !values.is_empty()
        || generation.is_empty()
        || manifest.encoded_bytes == 0
        || !manifest.uri.starts_with("s3://")
        || !exact_lower_hex(&manifest.sha256)
        || input_dir.as_os_str().is_empty()
        || output_dir.as_os_str().is_empty()
        || !output_uri_prefix.starts_with("s3://")
        || !output_uri_prefix.ends_with('/')
        || worker_count == 0
    {
        return Err("V26 build arguments differ".to_owned());
    }
    Ok(V26CliMode::Build(BuildRequest {
        generation,
        manifest,
        input_dir,
        output_dir,
        output_uri_prefix,
        worker_count,
    }))
}

fn execute_v26_mode(mode: V26CliMode) -> Result<Vec<u8>, String> {
    match mode {
        V26CliMode::Build(request) => {
            let manifest = V26LocalObjectPath {
                identity: V26ObjectIdentity {
                    role: "layout-manifest".to_owned(),
                    uri: request.manifest.uri,
                    digest_algorithm: "sha256".to_owned(),
                    digest: request.manifest.sha256,
                    encoded_bytes: request.manifest.encoded_bytes,
                    generation: request.generation,
                },
                path: request.manifest.path,
            };
            let (build_request, output) = run_v26_layout_build_directory(
                manifest,
                &request.input_dir,
                request.output_dir,
                request.output_uri_prefix,
                request.worker_count,
            )
            .map_err(|error| error.to_string())?;
            canonical_v26_layout_build_output_bytes(&build_request, &output)
                .map_err(|error| error.to_string())
        }
    }
}

fn run() -> Result<(), String> {
    let mode = parse_v26_args(std::env::args().collect())?;
    let bytes = execute_v26_mode(mode)?;
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| format!("stdout write failed: {error}"))
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{V26CliMode, execute_v26_mode, parse_v26_args};

    fn build_args() -> Vec<String> {
        [
            "v26_page_layout",
            "--build-layout",
            "--execute",
            "--generation",
            "v26-generation",
            "--manifest-path",
            "/input/layout-manifest.json",
            "--manifest-uri",
            "s3://bucket/layout-manifest.json",
            "--manifest-sha256",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "--manifest-bytes",
            "1024",
            "--input-dir",
            "/input",
            "--output-dir",
            "/output",
            "--output-uri-prefix",
            "s3://bucket/v26/layout/",
            "--workers",
            "4",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v26_page_layout_cli_parses_explicit_build_authority() {
        // Break caught: a hidden loader or implicit identity enters the scientific process.
        let parsed = parse_v26_args(build_args()).unwrap();
        let V26CliMode::Build(request) = parsed;
        assert_eq!(request.generation, "v26-generation");
        assert_eq!(request.manifest.encoded_bytes, 1024);
        assert_eq!(request.worker_count, 4);
        assert_eq!(request.output_uri_prefix, "s3://bucket/v26/layout/");
    }

    #[test]
    fn v26_page_layout_cli_fails_closed_before_execution() {
        // Break caught: duplicate, unknown, incomplete, or network/storage flags are accepted.
        for mutation in [
            vec!["--workers", "8"],
            vec!["--unknown", "value"],
            vec!["--bucket", "forbidden"],
            vec!["--endpoint", "https://forbidden"],
            vec!["--page-prefix", "forbidden"],
            vec!["--d3"],
            vec!["--evaluate-layout"],
        ] {
            let mut args = build_args();
            args.extend(mutation.into_iter().map(str::to_owned));
            assert!(parse_v26_args(args).is_err());
        }
        let mut missing_execute = build_args();
        missing_execute.retain(|argument| argument != "--execute");
        assert!(parse_v26_args(missing_execute).is_err());
    }

    #[test]
    fn v26_page_layout_cli_enters_only_the_authenticated_library_boundary() {
        // Break caught: the thin executable parses or constructs scientific data itself.
        let mode = parse_v26_args(build_args()).unwrap();
        let error = execute_v26_mode(mode).unwrap_err();
        assert!(error.contains("local object open failed"));
    }
}

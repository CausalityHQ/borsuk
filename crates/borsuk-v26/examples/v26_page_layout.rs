//! Offline V26 layout construction and evaluation executable.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26ExactGlobalRequest, V26LayoutEvaluationRequest, V26LocalObjectPath, V26ObjectIdentity,
    V26TruthBuildRequest, canonical_v26_layout_build_output_bytes,
    canonical_v26_layout_result_bytes, canonical_v26_object_identity_bytes,
    evaluate_v26_layout_oracle, run_v26_exact_global, run_v26_layout_build_directory,
    run_v26_truth_build,
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
struct EvaluationRequest {
    generation: String,
    layout_terminal: RegisteredFile,
    page_assignments: RegisteredFile,
    external_queries: RegisteredFile,
    truth: RegisteredFile,
    expected_queries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExactGlobalRequest {
    generation: String,
    construction: RegisteredFile,
    layout: EvaluationRequest,
    ranked_row_limits: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TruthRequest {
    generation: String,
    construction: RegisteredFile,
    external_queries: RegisteredFile,
    expected_rows: u64,
    expected_queries: u32,
    output_path: PathBuf,
    output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum V26CliMode {
    Build(BuildRequest),
    Truth(TruthRequest),
    Evaluate(EvaluationRequest),
    ExactGlobal(ExactGlobalRequest),
}

fn take(values: &mut BTreeMap<String, String>, key: &str) -> Result<String, String> {
    values.remove(key).ok_or_else(|| format!("missing {key}"))
}

fn take_registered(
    values: &mut BTreeMap<String, String>,
    prefix: &str,
) -> Result<RegisteredFile, String> {
    Ok(RegisteredFile {
        path: PathBuf::from(take(values, &format!("--{prefix}-path"))?),
        uri: take(values, &format!("--{prefix}-uri"))?,
        sha256: take(values, &format!("--{prefix}-sha256"))?,
        encoded_bytes: take(values, &format!("--{prefix}-bytes"))?
            .parse()
            .map_err(|_| format!("invalid --{prefix}-bytes"))?,
    })
}

fn valid_registered(file: &RegisteredFile) -> bool {
    !file.path.as_os_str().is_empty()
        && file.uri.starts_with("s3://")
        && exact_lower_hex(&file.sha256)
        && file.encoded_bytes > 0
}

fn local_object(role: &str, generation: &str, file: RegisteredFile) -> V26LocalObjectPath {
    V26LocalObjectPath {
        identity: V26ObjectIdentity {
            role: role.to_owned(),
            uri: file.uri,
            digest_algorithm: "sha256".to_owned(),
            digest: file.sha256,
            encoded_bytes: file.encoded_bytes,
            generation: generation.to_owned(),
        },
        path: file.path,
    }
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
    let mut truth_build = false;
    let mut evaluate = false;
    let mut exact_global = false;
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
            "--build-truth" => {
                if truth_build {
                    return Err("duplicate --build-truth".to_owned());
                }
                truth_build = true;
            }
            "--evaluate-layout" => {
                if evaluate {
                    return Err("duplicate --evaluate-layout".to_owned());
                }
                evaluate = true;
            }
            "--exact-global" => {
                if exact_global {
                    return Err("duplicate --exact-global".to_owned());
                }
                exact_global = true;
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
            | "--workers"
            | "--layout-terminal-path"
            | "--layout-terminal-uri"
            | "--layout-terminal-sha256"
            | "--layout-terminal-bytes"
            | "--page-assignments-path"
            | "--page-assignments-uri"
            | "--page-assignments-sha256"
            | "--page-assignments-bytes"
            | "--external-queries-path"
            | "--external-queries-uri"
            | "--external-queries-sha256"
            | "--external-queries-bytes"
            | "--truth-path"
            | "--truth-uri"
            | "--truth-sha256"
            | "--truth-bytes"
            | "--truth-output-path"
            | "--truth-output-uri"
            | "--expected-rows"
            | "--expected-queries" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| format!("missing value for {flag}"))?;
                if value.starts_with("--") || values.insert(flag.clone(), value).is_some() {
                    return Err(format!("invalid or duplicate {flag}"));
                }
            }
            "--construction-path"
            | "--construction-uri"
            | "--construction-sha256"
            | "--construction-bytes"
            | "--ranked-row-limits" => {
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
    if !execute
        || u8::from(build) + u8::from(truth_build) + u8::from(evaluate) + u8::from(exact_global)
            != 1
    {
        return Err("exactly one executable phase is required".to_owned());
    }
    let generation = take(&mut values, "--generation")?;
    if generation.is_empty() {
        return Err("V26 generation differs".to_owned());
    }
    if build {
        let manifest = take_registered(&mut values, "manifest")?;
        let input_dir = PathBuf::from(take(&mut values, "--input-dir")?);
        let output_dir = PathBuf::from(take(&mut values, "--output-dir")?);
        let output_uri_prefix = take(&mut values, "--output-uri-prefix")?;
        let worker_count = take(&mut values, "--workers")?
            .parse()
            .map_err(|_| "invalid --workers".to_owned())?;
        if !values.is_empty()
            || !valid_registered(&manifest)
            || input_dir.as_os_str().is_empty()
            || output_dir.as_os_str().is_empty()
            || !output_uri_prefix.starts_with("s3://")
            || !output_uri_prefix.ends_with('/')
            || worker_count == 0
        {
            return Err("V26 build arguments differ".to_owned());
        }
        return Ok(V26CliMode::Build(BuildRequest {
            generation,
            manifest,
            input_dir,
            output_dir,
            output_uri_prefix,
            worker_count,
        }));
    }
    if truth_build {
        let construction = take_registered(&mut values, "construction")?;
        let external_queries = take_registered(&mut values, "external-queries")?;
        let expected_rows = take(&mut values, "--expected-rows")?
            .parse()
            .map_err(|_| "invalid --expected-rows".to_owned())?;
        let expected_queries = take(&mut values, "--expected-queries")?
            .parse()
            .map_err(|_| "invalid --expected-queries".to_owned())?;
        let output_path = PathBuf::from(take(&mut values, "--truth-output-path")?);
        let output_uri = take(&mut values, "--truth-output-uri")?;
        if !values.is_empty()
            || !valid_registered(&construction)
            || !valid_registered(&external_queries)
            || expected_rows < 10
            || expected_queries != 512
            || output_path.as_os_str().is_empty()
            || !output_uri.starts_with("s3://")
        {
            return Err("V26 truth arguments differ".to_owned());
        }
        return Ok(V26CliMode::Truth(TruthRequest {
            generation,
            construction,
            external_queries,
            expected_rows,
            expected_queries,
            output_path,
            output_uri,
        }));
    }
    let layout_terminal = take_registered(&mut values, "layout-terminal")?;
    let page_assignments = take_registered(&mut values, "page-assignments")?;
    let external_queries = take_registered(&mut values, "external-queries")?;
    let truth = take_registered(&mut values, "truth")?;
    let expected_queries = take(&mut values, "--expected-queries")?
        .parse()
        .map_err(|_| "invalid --expected-queries".to_owned())?;
    if evaluate && !values.is_empty()
        || [
            &layout_terminal,
            &page_assignments,
            &external_queries,
            &truth,
        ]
        .into_iter()
        .any(|file| !valid_registered(file))
        || expected_queries != 512
    {
        return Err("V26 evaluation arguments differ".to_owned());
    }
    let layout = EvaluationRequest {
        generation,
        layout_terminal,
        page_assignments,
        external_queries,
        truth,
        expected_queries,
    };
    if evaluate {
        if !values.is_empty() {
            return Err("V26 evaluation arguments differ".to_owned());
        }
        return Ok(V26CliMode::Evaluate(layout));
    }
    let construction = take_registered(&mut values, "construction")?;
    let ranked_row_limits = take(&mut values, "--ranked-row-limits")?
        .split(',')
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| "invalid --ranked-row-limits".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !values.is_empty()
        || !valid_registered(&construction)
        || ranked_row_limits != [10, 32, 128, 512, 2_048, 4_096]
    {
        return Err("V26 exact-global arguments differ".to_owned());
    }
    Ok(V26CliMode::ExactGlobal(ExactGlobalRequest {
        generation: layout.generation.clone(),
        construction,
        layout,
        ranked_row_limits,
    }))
}

fn evaluation_request(request: EvaluationRequest) -> V26LayoutEvaluationRequest {
    let generation = request.generation;
    V26LayoutEvaluationRequest {
        layout_terminal: local_object("layout-terminal", &generation, request.layout_terminal),
        page_assignments: local_object(
            "page-assignments-parquet",
            &generation,
            request.page_assignments,
        ),
        external_queries: local_object(
            "external-queries-parquet",
            &generation,
            request.external_queries,
        ),
        truth: local_object("truth-parquet", &generation, request.truth),
        expected_queries: request.expected_queries,
    }
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
        V26CliMode::Truth(request) => {
            let output = run_v26_truth_build(&V26TruthBuildRequest {
                construction_rows: local_object(
                    "construction-parquet",
                    &request.generation,
                    request.construction,
                ),
                external_queries: local_object(
                    "external-queries-parquet",
                    &request.generation,
                    request.external_queries,
                ),
                expected_rows: request.expected_rows,
                expected_queries: request.expected_queries,
                output_path: request.output_path,
                output_uri: request.output_uri,
            })
            .map_err(|error| error.to_string())?;
            canonical_v26_object_identity_bytes(&output.identity).map_err(|error| error.to_string())
        }
        V26CliMode::Evaluate(request) => {
            let evaluation = evaluation_request(request);
            let (truths, samples, result) =
                evaluate_v26_layout_oracle(&evaluation).map_err(|error| error.to_string())?;
            canonical_v26_layout_result_bytes(&result, &truths, &samples)
                .map_err(|error| error.to_string())
        }
        V26CliMode::ExactGlobal(request) => run_v26_exact_global(&V26ExactGlobalRequest {
            construction_rows: local_object(
                "construction-parquet",
                &request.generation,
                request.construction,
            ),
            layout: evaluation_request(request.layout),
            ranked_row_limits: request.ranked_row_limits,
        })
        .map_err(|error| error.to_string()),
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

    fn evaluation_args() -> Vec<String> {
        let mut args = vec![
            "v26_page_layout".to_owned(),
            "--evaluate-layout".to_owned(),
            "--execute".to_owned(),
            "--generation".to_owned(),
            "v26-generation".to_owned(),
        ];
        for (role, byte) in [
            ("layout-terminal", '1'),
            ("page-assignments", '2'),
            ("external-queries", '3'),
            ("truth", '4'),
        ] {
            args.extend([
                format!("--{role}-path"),
                format!("/input/{role}.bin"),
                format!("--{role}-uri"),
                format!("s3://bucket/{role}.bin"),
                format!("--{role}-sha256"),
                byte.to_string().repeat(64),
                format!("--{role}-bytes"),
                "1024".to_owned(),
            ]);
        }
        args.extend(["--expected-queries".to_owned(), "512".to_owned()]);
        args
    }

    fn exact_global_args() -> Vec<String> {
        let mut args = evaluation_args();
        args.retain(|argument| argument != "--evaluate-layout");
        args.insert(1, "--exact-global".to_owned());
        args.extend([
            "--construction-path".to_owned(),
            "/input/construction.parquet".to_owned(),
            "--construction-uri".to_owned(),
            "s3://bucket/construction.parquet".to_owned(),
            "--construction-sha256".to_owned(),
            "5".repeat(64),
            "--construction-bytes".to_owned(),
            "4096".to_owned(),
            "--ranked-row-limits".to_owned(),
            "10,32,128,512,2048,4096".to_owned(),
        ]);
        args
    }

    fn truth_args() -> Vec<String> {
        let mut args = vec![
            "v26_page_layout".to_owned(),
            "--build-truth".to_owned(),
            "--execute".to_owned(),
            "--generation".to_owned(),
            "v26-generation".to_owned(),
        ];
        for (role, byte) in [("construction", '5'), ("external-queries", '6')] {
            args.extend([
                format!("--{role}-path"),
                format!("/input/{role}.parquet"),
                format!("--{role}-uri"),
                format!("s3://bucket/{role}.parquet"),
                format!("--{role}-sha256"),
                byte.to_string().repeat(64),
                format!("--{role}-bytes"),
                "4096".to_owned(),
            ]);
        }
        args.extend([
            "--expected-rows".to_owned(),
            "1409".to_owned(),
            "--expected-queries".to_owned(),
            "512".to_owned(),
            "--truth-output-path".to_owned(),
            "/output/external-truth.parquet".to_owned(),
            "--truth-output-uri".to_owned(),
            "s3://bucket/output/external-truth.parquet".to_owned(),
        ]);
        args
    }

    #[test]
    fn v26_external_query_truth_cli_has_only_explicit_local_parquet_roles() {
        // Break caught: truth construction discovers data or receives layout/page/network access.
        let parsed = parse_v26_args(truth_args()).unwrap();
        let V26CliMode::Truth(request) = parsed else {
            panic!("truth mode differs");
        };
        assert_eq!(request.generation, "v26-generation");
        assert_eq!(request.expected_rows, 1_409);
        assert_eq!(request.expected_queries, 512);
        assert_eq!(request.construction.sha256, "5".repeat(64));
        assert_eq!(request.external_queries.sha256, "6".repeat(64));
        assert_eq!(
            request.output_path,
            std::path::Path::new("/output/external-truth.parquet")
        );
        let error = execute_v26_mode(V26CliMode::Truth(request)).unwrap_err();
        assert!(error.contains("local object open failed"));
        for forbidden in [
            "--layout-terminal-path",
            "--page-assignments-path",
            "--bucket",
            "--d3",
        ] {
            let mut args = truth_args();
            args.extend([forbidden.to_owned(), "forbidden".to_owned()]);
            assert!(parse_v26_args(args).is_err());
        }
    }

    #[test]
    fn v26_page_layout_cli_parses_explicit_build_authority() {
        // Break caught: a hidden loader or implicit identity enters the scientific process.
        let parsed = parse_v26_args(build_args()).unwrap();
        let V26CliMode::Build(request) = parsed else {
            panic!("build mode differs");
        };
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

    #[test]
    fn v26_page_layout_cli_parses_explicit_evaluation_authority() {
        // Break caught: layout evaluation silently discovers roles or accepts storage access.
        let parsed = parse_v26_args(evaluation_args()).unwrap();
        let V26CliMode::Evaluate(request) = parsed else {
            panic!("evaluation mode differs");
        };
        assert_eq!(request.generation, "v26-generation");
        assert_eq!(request.layout_terminal.encoded_bytes, 1024);
        assert_eq!(request.page_assignments.sha256, "2".repeat(64));
        assert_eq!(
            request.external_queries.uri,
            "s3://bucket/external-queries.bin"
        );
        assert_eq!(request.truth.path, std::path::Path::new("/input/truth.bin"));
        assert_eq!(request.expected_queries, 512);

        let error = execute_v26_mode(V26CliMode::Evaluate(request)).unwrap_err();
        assert!(error.contains("local object open failed"));
    }

    #[test]
    fn v26_page_layout_cli_rejects_incomplete_or_storage_evaluation_authority() {
        // Break caught: an incomplete evaluation or hidden network flag reaches science.
        for mutation in [
            vec!["--bucket".to_owned(), "forbidden".to_owned()],
            vec!["--page-prefix".to_owned(), "forbidden".to_owned()],
            vec!["--workers".to_owned(), "4".to_owned()],
            vec!["--build-layout".to_owned()],
        ] {
            let mut args = evaluation_args();
            args.extend(mutation);
            assert!(parse_v26_args(args).is_err());
        }
        let mut missing = evaluation_args();
        let index = missing
            .iter()
            .position(|value| value == "--truth-sha256")
            .unwrap();
        missing.drain(index..=index + 1);
        assert!(parse_v26_args(missing).is_err());
    }

    #[test]
    fn v26_exact_global_cli_requires_explicit_offline_authority_and_fixed_limits() {
        // Break caught: exact-global discovers an input, accepts a tunable rank ladder, or gains
        // network/page access inside the scientific process.
        let parsed = parse_v26_args(exact_global_args()).unwrap();
        let V26CliMode::ExactGlobal(request) = parsed else {
            panic!("exact-global mode differs");
        };
        assert_eq!(request.generation, "v26-generation");
        assert_eq!(request.construction.sha256, "5".repeat(64));
        assert_eq!(request.ranked_row_limits, [10, 32, 128, 512, 2_048, 4_096]);

        for mutation in [
            vec!["--bucket", "forbidden"],
            vec!["--endpoint", "https://forbidden"],
            vec!["--page-prefix", "forbidden"],
            vec!["--d3"],
            vec!["--ranked-row-limits", "10,32"],
        ] {
            let mut args = exact_global_args();
            args.extend(mutation.into_iter().map(str::to_owned));
            assert!(parse_v26_args(args).is_err());
        }
    }
}

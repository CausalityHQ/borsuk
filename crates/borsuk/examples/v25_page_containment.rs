//! Offline V25 rank-sharp page-containment diagnostic.
//!
//! This executable accepts only local authenticated artifacts. It has no page,
//! object-storage, AWS, endpoint, or compatibility execution surface.

use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs,
    io::{self, Write},
    path::{Component, Path, PathBuf},
};

use borsuk_v25::{
    V25ContainmentLocalRequest, V25LocalObjectPath, V25ObjectIdentity,
    run_v25_containment_local_request, write_v25_containment_evidence,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq)]
struct Cli {
    manifest: PathBuf,
    input_dir: PathBuf,
    output_dir: PathBuf,
    evaluate_containment: bool,
    execute: bool,
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Cli, String> {
    let mut arguments = arguments.into_iter();
    let _program = arguments
        .next()
        .ok_or_else(|| "V25 program name is missing".to_owned())?;
    let mut manifest = None;
    let mut input_dir = None;
    let mut output_dir = None;
    let mut evaluate_containment = false;
    let mut execute = false;
    while let Some(argument) = arguments.next() {
        let flag = argument
            .to_str()
            .ok_or_else(|| "V25 flag is not UTF-8".to_owned())?;
        match flag {
            "--manifest" | "--input-dir" | "--output-dir" => {
                let value = arguments
                    .next()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| format!("V25 {flag} value is missing"))?;
                let slot = match flag {
                    "--manifest" => &mut manifest,
                    "--input-dir" => &mut input_dir,
                    "--output-dir" => &mut output_dir,
                    _ => unreachable!(),
                };
                if slot.replace(PathBuf::from(value)).is_some() {
                    return Err(format!("V25 {flag} repeats"));
                }
            }
            "--evaluate-containment" => {
                if evaluate_containment {
                    return Err("V25 --evaluate-containment repeats".to_owned());
                }
                evaluate_containment = true;
            }
            "--execute" => {
                if execute {
                    return Err("V25 --execute repeats".to_owned());
                }
                execute = true;
            }
            _ => return Err(format!("V25 flag is unknown: {flag}")),
        }
    }
    Ok(Cli {
        manifest: manifest.ok_or_else(|| "V25 --manifest is missing".to_owned())?,
        input_dir: input_dir.ok_or_else(|| "V25 --input-dir is missing".to_owned())?,
        output_dir: output_dir.ok_or_else(|| "V25 --output-dir is missing".to_owned())?,
        evaluate_containment,
        execute,
    })
    .and_then(|cli| {
        if !cli.evaluate_containment || !cli.execute {
            Err("V25 execution acknowledgement is missing".to_owned())
        } else {
            Ok(cli)
        }
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestObject {
    file_name: String,
    identity: V25ObjectIdentity,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    source_commit: String,
    source_archive_sha256: String,
    index_sha256: String,
    generation: String,
    construction_rows: ManifestObject,
    page_assignments: ManifestObject,
    pseudoqueries: ManifestObject,
    truth: ManifestObject,
    ranked_row_limits: Vec<u32>,
    page_budget: u32,
    expected_source_rows: u64,
    expected_page_count: u32,
    expected_queries: u32,
    construction_batch_rows: usize,
    evidence_uri: String,
}

#[derive(Debug, Serialize)]
struct SmokeReceipt {
    schema: &'static str,
    claim_eligible: bool,
    source_commit: String,
    source_archive_sha256: String,
    index_sha256: String,
    generation: String,
    scanned_rows: u64,
    peak_construction_batch_rows: u64,
    peak_ranked_rows_retained: u64,
    page_body_reads: u64,
    evidence: V25ObjectIdentity,
}

fn exact_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn resolve_input(input_dir: &Path, object: ManifestObject) -> Result<V25LocalObjectPath, String> {
    let path = Path::new(&object.file_name);
    if path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err("V25 input file name differs".to_owned());
    }
    let path = input_dir.join(path);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| format!("V25 input metadata failed: {error}"))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err("V25 input file type differs".to_owned());
    }
    Ok(V25LocalObjectPath {
        identity: object.identity,
        path,
    })
}

fn run_cli(arguments: impl IntoIterator<Item = OsString>) -> Result<Vec<u8>, String> {
    let cli = parse_args(arguments)?;
    let input_dir = cli
        .input_dir
        .canonicalize()
        .map_err(|error| format!("V25 input directory failed: {error}"))?;
    let output_dir = cli
        .output_dir
        .canonicalize()
        .map_err(|error| format!("V25 output directory failed: {error}"))?;
    if !input_dir.is_dir() || !output_dir.is_dir() {
        return Err("V25 local directory type differs".to_owned());
    }
    if fs::read_dir(&output_dir)
        .map_err(|error| format!("V25 output inventory failed: {error}"))?
        .next()
        .is_some()
    {
        return Err("V25 output directory is not empty".to_owned());
    }
    let manifest_path = cli
        .manifest
        .canonicalize()
        .map_err(|error| format!("V25 manifest path failed: {error}"))?;
    if manifest_path.parent() != Some(input_dir.as_path())
        || fs::symlink_metadata(&manifest_path)
            .map_err(|error| format!("V25 manifest metadata failed: {error}"))?
            .file_type()
            .is_symlink()
    {
        return Err("V25 manifest authority differs".to_owned());
    }
    let manifest: Manifest = serde_json::from_slice(
        &fs::read(&manifest_path).map_err(|error| format!("V25 manifest read failed: {error}"))?,
    )
    .map_err(|error| format!("V25 manifest parse failed: {error}"))?;
    if manifest.schema != "borsuk-v25-containment-smoke-manifest-v1"
        || !exact_hex(&manifest.source_commit, 40)
        || !exact_hex(&manifest.source_archive_sha256, 64)
        || !exact_hex(&manifest.index_sha256, 64)
        || manifest.generation.is_empty()
    {
        return Err("V25 manifest identity differs".to_owned());
    }
    let manifest_name = manifest_path
        .file_name()
        .ok_or_else(|| "V25 manifest name differs".to_owned())?
        .to_owned();
    let expected_names = [
        manifest_name,
        OsString::from(&manifest.construction_rows.file_name),
        OsString::from(&manifest.page_assignments.file_name),
        OsString::from(&manifest.pseudoqueries.file_name),
        OsString::from(&manifest.truth.file_name),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let observed_names = fs::read_dir(&input_dir)
        .map_err(|error| format!("V25 input inventory failed: {error}"))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|error| format!("V25 input inventory failed: {error}"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if expected_names.len() != 5 || observed_names != expected_names {
        return Err("V25 input inventory differs".to_owned());
    }

    let request = V25ContainmentLocalRequest {
        construction_rows: resolve_input(&input_dir, manifest.construction_rows)?,
        page_assignments: resolve_input(&input_dir, manifest.page_assignments)?,
        pseudoqueries: resolve_input(&input_dir, manifest.pseudoqueries)?,
        truth: resolve_input(&input_dir, manifest.truth)?,
        ranked_row_limits: manifest.ranked_row_limits,
        page_budget: manifest.page_budget,
        expected_source_rows: manifest.expected_source_rows,
        expected_page_count: manifest.expected_page_count,
        expected_queries: manifest.expected_queries,
        construction_batch_rows: manifest.construction_batch_rows,
    };
    let result = run_v25_containment_local_request(&request).map_err(|error| error.to_string())?;
    let evidence = write_v25_containment_evidence(
        &output_dir.join("containment-evidence.parquet"),
        &manifest.evidence_uri,
        &manifest.generation,
        request.page_budget,
        &result.samples,
    )
    .map_err(|error| error.to_string())?;
    let receipt = SmokeReceipt {
        schema: "borsuk-v25-containment-smoke-receipt-v1",
        claim_eligible: false,
        source_commit: manifest.source_commit,
        source_archive_sha256: manifest.source_archive_sha256,
        index_sha256: manifest.index_sha256,
        generation: manifest.generation,
        scanned_rows: result.scanned_rows,
        peak_construction_batch_rows: result.peak_construction_batch_rows,
        peak_ranked_rows_retained: result.peak_ranked_rows_retained,
        page_body_reads: result.page_body_reads,
        evidence: evidence.identity,
    };
    let value = serde_json::to_value(&receipt)
        .map_err(|error| format!("V25 receipt serialization failed: {error}"))?;
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|error| format!("V25 receipt serialization failed: {error}"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn main() {
    match run_cli(std::env::args_os()) {
        Ok(stdout) => {
            if let Err(error) = io::stdout().lock().write_all(&stdout) {
                eprintln!("V25 stdout write failed: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, fs, path::Path, path::PathBuf, sync::Arc, time::Instant};

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::{parse_args, run_cli};

    fn valid_args() -> Vec<OsString> {
        [
            "v25-page-containment",
            "--manifest",
            "/input/manifest.json",
            "--input-dir",
            "/input",
            "--output-dir",
            "/output",
            "--evaluate-containment",
            "--execute",
        ]
        .into_iter()
        .map(OsString::from)
        .collect()
    }

    fn vector(first: f32, second: f32) -> [f32; 96] {
        let norm = first.hypot(second);
        let mut vector = [0.0; 96];
        vector[0] = first / norm;
        vector[1] = second / norm;
        vector
    }

    fn vector_array(vectors: &[[f32; 96]]) -> FixedSizeListArray {
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from_iter_values(
                vectors.iter().flat_map(|vector| vector.iter().copied()),
            )),
            None,
        )
        .unwrap()
    }

    fn list_u32(values: &[u32], width: i32) -> FixedSizeListArray {
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::UInt32, false)),
            width,
            Arc::new(UInt32Array::from(values.to_vec())),
            None,
        )
        .unwrap()
    }

    fn list_u64(values: &[u64], width: i32) -> FixedSizeListArray {
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::UInt64, false)),
            width,
            Arc::new(UInt64Array::from(values.to_vec())),
            None,
        )
        .unwrap()
    }

    fn write_batch(path: &Path, batch: RecordBatch) {
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn primary_page(source: u64) -> u32 {
        match source {
            0 => 0,
            1 | 2 => 1,
            3 | 4 => 2,
            5..=10 => u32::try_from(source - 2).unwrap(),
            _ => u32::try_from(source).unwrap(),
        }
    }

    fn identity(role: &str, file_name: &str, path: &Path) -> serde_json::Value {
        let bytes = fs::read(path).unwrap();
        json!({
            "file_name": file_name,
            "identity": {
                "role": role,
                "uri": format!("s3://borsuk-v25/smoke/{file_name}"),
                "digest_algorithm": "sha256",
                "digest": format!("{:x}", Sha256::digest(&bytes)),
                "encoded_bytes": bytes.len(),
                "generation": "v25-smoke-test"
            }
        })
    }

    fn write_smoke_inputs(input: &Path) -> PathBuf {
        let construction = input.join("construction.parquet");
        let pages = input.join("pages.parquet");
        let queries = input.join("queries.parquet");
        let truth = input.join("truth.parquet");
        let vectors = (0..20_u64)
            .map(|source| vector(20.0 - source as f32, source as f32 + 1.0))
            .collect::<Vec<_>>();
        let vector_type = || {
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            )
        };
        write_batch(
            &construction,
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("source_ordinal", DataType::UInt64, false),
                    Field::new("vector", vector_type(), false),
                ])),
                vec![
                    Arc::new(UInt64Array::from_iter_values(0..20_u64)) as ArrayRef,
                    Arc::new(vector_array(&vectors)),
                ],
            )
            .unwrap(),
        );
        write_batch(
            &pages,
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("source_ordinal", DataType::UInt64, false),
                    Field::new("primary_page", DataType::UInt32, false),
                    Field::new("replica_page", DataType::UInt32, false),
                ])),
                vec![
                    Arc::new(UInt64Array::from_iter_values(0..20_u64)) as ArrayRef,
                    Arc::new(UInt32Array::from_iter_values((0..20_u64).map(primary_page))),
                    Arc::new(UInt32Array::from_iter_values(
                        (0..20_u32).map(|source| if source == 0 { 19 } else { u32::MAX }),
                    )),
                ],
            )
            .unwrap(),
        );
        write_batch(
            &queries,
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("query_ordinal", DataType::UInt32, false),
                    Field::new("source_ordinal", DataType::UInt64, false),
                    Field::new("vector", vector_type(), false),
                ])),
                vec![
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![0])),
                    Arc::new(vector_array(&[vector(1.0, 0.0)])),
                ],
            )
            .unwrap(),
        );
        write_batch(
            &truth,
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("query_ordinal", DataType::UInt32, false),
                    Field::new(
                        "neighbor_source_ordinals",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("element", DataType::UInt64, false)),
                            10,
                        ),
                        false,
                    ),
                    Field::new(
                        "primary_pages",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("element", DataType::UInt32, false)),
                            10,
                        ),
                        false,
                    ),
                    Field::new(
                        "replica_pages",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("element", DataType::UInt32, false)),
                            10,
                        ),
                        false,
                    ),
                    Field::new(
                        "oracle_pages",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("element", DataType::UInt32, false)),
                            8,
                        ),
                        false,
                    ),
                ])),
                vec![
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(list_u64(&(1..=10).collect::<Vec<_>>(), 10)),
                    Arc::new(list_u32(&[1, 1, 2, 2, 3, 4, 5, 6, 7, 8], 10)),
                    Arc::new(list_u32(&[u32::MAX; 10], 10)),
                    Arc::new(list_u32(&(1..=8).collect::<Vec<_>>(), 8)),
                ],
            )
            .unwrap(),
        );
        let manifest = json!({
            "schema": "borsuk-v25-containment-smoke-manifest-v1",
            "source_commit": "a".repeat(40),
            "source_archive_sha256": "b".repeat(64),
            "index_sha256": "c".repeat(64),
            "generation": "v25-smoke-test",
            "construction_rows": identity("construction-rows-parquet", "construction.parquet", &construction),
            "page_assignments": identity("page-assignments-parquet", "pages.parquet", &pages),
            "pseudoqueries": identity("pseudoqueries-parquet", "queries.parquet", &queries),
            "truth": identity("truth-parquet", "truth.parquet", &truth),
            "ranked_row_limits": [10, 32],
            "page_budget": 8,
            "expected_source_rows": 20,
            "expected_page_count": 20,
            "expected_queries": 1,
            "construction_batch_rows": 8,
            "evidence_uri": "s3://borsuk-v25/smoke/evidence.parquet"
        });
        let manifest_path = input.join("manifest.json");
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        manifest_path
    }

    #[test]
    fn v25_containment_cli_accepts_only_the_offline_execution_surface() {
        let parsed = parse_args(valid_args()).unwrap();
        assert_eq!(parsed.manifest, PathBuf::from("/input/manifest.json"));
        assert_eq!(parsed.input_dir, PathBuf::from("/input"));
        assert_eq!(parsed.output_dir, PathBuf::from("/output"));
        assert!(parsed.evaluate_containment);
        assert!(parsed.execute);

        for forbidden in [
            "--bucket",
            "--endpoint",
            "--page-prefix",
            "--aws-profile",
            "--page-body",
            "--v24",
            "--d3",
            "--compatibility",
        ] {
            let mut args = valid_args();
            args.push(OsString::from(forbidden));
            args.push(OsString::from("forbidden"));
            assert!(parse_args(args).is_err(), "accepted forbidden {forbidden}");
        }
    }

    #[test]
    fn v25_containment_cli_rejects_missing_duplicate_unknown_and_malformed_flags() {
        for missing in [
            "--manifest",
            "--input-dir",
            "--output-dir",
            "--evaluate-containment",
            "--execute",
        ] {
            let mut args = valid_args();
            let index = args.iter().position(|value| value == missing).unwrap();
            args.remove(index);
            if !matches!(missing, "--evaluate-containment" | "--execute") {
                args.remove(index);
            }
            assert!(parse_args(args).is_err(), "accepted missing {missing}");
        }

        let mut duplicate = valid_args();
        duplicate.extend([OsString::from("--execute")]);
        assert!(parse_args(duplicate).is_err());

        let mut unknown = valid_args();
        unknown.extend([OsString::from("--other")]);
        assert!(parse_args(unknown).is_err());

        let mut malformed = valid_args();
        let input = malformed
            .iter()
            .position(|value| value == "--input-dir")
            .unwrap();
        malformed[input + 1] = OsString::from("");
        assert!(parse_args(malformed).is_err());
    }

    #[test]
    fn v25_containment_cli_smoke_runs_authenticated_parquet_without_page_bodies() {
        let temporary = tempfile::tempdir().unwrap();
        let input = temporary.path().join("input");
        let output = temporary.path().join("output");
        fs::create_dir(&input).unwrap();
        fs::create_dir(&output).unwrap();
        let manifest = write_smoke_inputs(&input);
        let arguments = [
            OsString::from("v25-page-containment"),
            OsString::from("--manifest"),
            manifest.into_os_string(),
            OsString::from("--input-dir"),
            input.into_os_string(),
            OsString::from("--output-dir"),
            output.clone().into_os_string(),
            OsString::from("--evaluate-containment"),
            OsString::from("--execute"),
        ];
        let started = Instant::now();
        let stdout = run_cli(arguments).unwrap();
        assert!(started.elapsed().as_secs() < 90);
        assert_eq!(stdout.last(), Some(&b'\n'));
        assert_eq!(
            stdout[..stdout.len() - 1]
                .iter()
                .filter(|byte| **byte == b'\n')
                .count(),
            0
        );
        let receipt: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
        let mut canonical = serde_json::to_vec(&receipt).unwrap();
        canonical.push(b'\n');
        assert_eq!(stdout, canonical);
        assert_eq!(receipt["schema"], "borsuk-v25-containment-smoke-receipt-v1");
        assert_eq!(receipt["claim_eligible"], false);
        assert_eq!(receipt["scanned_rows"], 20);
        assert_eq!(receipt["page_body_reads"], 0);
        assert_eq!(receipt["evidence"]["role"], "containment-evidence-parquet");
        assert_eq!(
            fs::read_dir(output)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            vec![OsString::from("containment-evidence.parquet")]
        );
    }
}

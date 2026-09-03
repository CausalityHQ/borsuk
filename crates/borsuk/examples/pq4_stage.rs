//! Deterministically stage frozen ANN Parquet shards into the public PQ4 input schema.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pq4StageRequest {
    manifest: PathBuf,
    manifest_sha256: String,
    manifest_bytes: u64,
    input_dir: PathBuf,
    output: PathBuf,
    expected_rows: u64,
    ordinal_start: u64,
    ordinal_end: u64,
    dimensions: usize,
    batch_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pq4StageReport {
    rows: u64,
    shards: usize,
    output_sha256: String,
    output_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrainingManifest {
    schema: String,
    dataset_id: String,
    shard_ordinal: u32,
    ordinal_start: u64,
    ordinal_end: u64,
    ordered_inputs: Vec<ManifestInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    authority_kind: String,
    dimensions: usize,
    identity: ManifestIdentity,
    metric: String,
    ordinal_end: Option<u64>,
    ordinal_start: Option<u64>,
    physical_schema: String,
    rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestIdentity {
    digest: String,
    digest_algorithm: String,
    encoded_bytes: u64,
    role: String,
    uri: String,
}

fn take(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn positive<T>(values: &mut BTreeMap<String, String>, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr + PartialOrd + Default,
{
    take(values, flag)?
        .parse::<T>()
        .ok()
        .filter(|value| *value > T::default())
        .ok_or_else(|| format!("invalid {flag}"))
}

fn lower_digest(value: String, flag: &str) -> Result<String, String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("invalid {flag}"));
    }
    Ok(value)
}

fn parse_pq4_stage_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Pq4StageRequest, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| "program name is absent".to_owned())?;
    let mut values = BTreeMap::new();
    let mut execute = false;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-stage" {
            if execute {
                return Err("duplicate --execute-stage".to_owned());
            }
            execute = true;
            continue;
        }
        if !flag.starts_with("--") {
            return Err(format!("unexpected positional argument {flag}"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        if value.starts_with("--") || values.insert(flag.clone(), value).is_some() {
            return Err(format!("invalid or duplicate flag {flag}"));
        }
    }
    let request = Pq4StageRequest {
        manifest: PathBuf::from(take(&mut values, "--manifest")?),
        manifest_sha256: lower_digest(
            take(&mut values, "--manifest-sha256")?,
            "--manifest-sha256",
        )?,
        manifest_bytes: positive(&mut values, "--manifest-bytes")?,
        input_dir: PathBuf::from(take(&mut values, "--input-dir")?),
        output: PathBuf::from(take(&mut values, "--output")?),
        expected_rows: positive(&mut values, "--expected-rows")?,
        ordinal_start: take(&mut values, "--ordinal-start")?
            .parse()
            .map_err(|_| "invalid --ordinal-start".to_owned())?,
        ordinal_end: positive(&mut values, "--ordinal-end")?,
        dimensions: positive(&mut values, "--dimensions")?,
        batch_rows: positive(&mut values, "--batch-rows")?,
    };
    if !execute
        || !values.is_empty()
        || request.manifest.as_os_str().is_empty()
        || request.input_dir.as_os_str().is_empty()
        || request.output.as_os_str().is_empty()
        || request.ordinal_end <= request.ordinal_start
        || request.ordinal_end - request.ordinal_start != request.expected_rows
        || request.dimensions != 96
        || request.batch_rows < 32
        || request.batch_rows > 65_536
        || !request.batch_rows.is_multiple_of(32)
    {
        return Err("PQ4 stage arguments differ".to_owned());
    }
    Ok(request)
}

fn sha256_file(path: &Path) -> Result<(String, u64), String> {
    let mut file =
        fs::File::open(path).map_err(|error| format!("artifact open failed: {error}"))?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)
        .map_err(|error| format!("artifact digest failed: {error}"))?;
    let bytes = file
        .metadata()
        .map_err(|error| format!("artifact metadata failed: {error}"))?
        .len();
    Ok((format!("{:x}", digest.finalize()), bytes))
}

fn source_schema() -> Schema {
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        ),
        false,
    )])
}

fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Binary, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            ),
            false,
        ),
    ])
}

fn training_inputs<'a>(
    manifest: &'a TrainingManifest,
    request: &Pq4StageRequest,
) -> Result<Vec<&'a ManifestInput>, String> {
    if manifest.schema != "borsuk-v26-pq4-partition-manifest-v1"
        || manifest.dataset_id != "synthetic-clustered-100m-96"
        || manifest.shard_ordinal >= 10
        || manifest.ordinal_start != request.ordinal_start
        || manifest.ordinal_end != request.ordinal_end
    {
        return Err("PQ4 training manifest authority differs".to_owned());
    }
    let inputs = manifest
        .ordered_inputs
        .iter()
        .filter(|item| item.authority_kind == "training-shard")
        .collect::<Vec<_>>();
    let mut next = request.ordinal_start;
    for (ordinal, input) in inputs.iter().enumerate() {
        let start = input
            .ordinal_start
            .ok_or_else(|| "PQ4 training ordinal is absent".to_owned())?;
        let end = input
            .ordinal_end
            .ok_or_else(|| "PQ4 training ordinal is absent".to_owned())?;
        let rows = input
            .rows
            .ok_or_else(|| "PQ4 training row count is absent".to_owned())?;
        if input.dimensions != request.dimensions
            || input.metric != "cosine"
            || input.physical_schema != "emb:fixed-size-list<element:f32;96>:non-null"
            || input.identity.role
                != format!("training-shard-{:04}-{ordinal:04}", manifest.shard_ordinal)
            || input.identity.digest_algorithm != "sha256"
            || lower_digest(input.identity.digest.clone(), "manifest digest").is_err()
            || !input.identity.uri.starts_with("s3://")
            || start != next
            || end <= start
            || end - start != rows
        {
            return Err("PQ4 training shard authority differs".to_owned());
        }
        next = end;
    }
    if inputs.is_empty() || next != request.ordinal_end {
        return Err("PQ4 training row authority differs".to_owned());
    }
    Ok(inputs)
}

fn stage_pq4_input(request: &Pq4StageRequest) -> Result<Pq4StageReport, String> {
    if request.output.exists() || !request.input_dir.is_dir() {
        return Err("PQ4 stage path authority differs".to_owned());
    }
    if sha256_file(&request.manifest)? != (request.manifest_sha256.clone(), request.manifest_bytes)
    {
        return Err("PQ4 training manifest digest differs".to_owned());
    }
    let manifest: TrainingManifest = serde_json::from_slice(
        &fs::read(&request.manifest)
            .map_err(|error| format!("training manifest read failed: {error}"))?,
    )
    .map_err(|error| format!("training manifest JSON failed: {error}"))?;
    let inputs = training_inputs(&manifest, request)?;
    let expected_names = inputs
        .iter()
        .map(|input| {
            input
                .identity
                .uri
                .rsplit_once('/')
                .map(|(_, name)| name.to_owned())
                .filter(|name| name.starts_with("train-") && name.ends_with(".parquet"))
                .ok_or_else(|| "PQ4 training shard URI differs".to_owned())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let observed_names = fs::read_dir(&request.input_dir)
        .map_err(|error| format!("training directory read failed: {error}"))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("train-") && name.ends_with(".parquet"))
        .collect::<BTreeSet<_>>();
    if observed_names != expected_names {
        return Err("PQ4 training shard inventory differs".to_owned());
    }
    let schema = Arc::new(output_schema());
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(&request.output)
            .map_err(|error| format!("PQ4 stage output failed: {error}"))?,
        schema.clone(),
        Some(properties),
    )
    .map_err(|error| format!("PQ4 stage writer failed: {error}"))?;
    let result = (|| {
        let mut next_ordinal = request.ordinal_start;
        for input in &inputs {
            let name = input.identity.uri.rsplit_once('/').unwrap().1;
            let path = request.input_dir.join(name);
            if sha256_file(&path)? != (input.identity.digest.clone(), input.identity.encoded_bytes)
            {
                return Err("PQ4 training shard digest differs".to_owned());
            }
            let builder = ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(&path)
                    .map_err(|error| format!("training shard open failed: {error}"))?,
            )
            .map_err(|error| format!("training shard metadata failed: {error}"))?;
            if builder.schema().as_ref() != &source_schema()
                || u64::try_from(builder.metadata().file_metadata().num_rows()).ok() != input.rows
            {
                return Err("PQ4 training shard Parquet authority differs".to_owned());
            }
            for batch in builder
                .with_batch_size(request.batch_rows)
                .build()
                .map_err(|error| format!("training shard reader failed: {error}"))?
            {
                let batch =
                    batch.map_err(|error| format!("training shard read failed: {error}"))?;
                let vectors = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| "PQ4 training vector array differs".to_owned())?;
                let values = vectors
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "PQ4 training vector values differ".to_owned())?;
                if vectors.null_count() != 0
                    || values.null_count() != 0
                    || values.len() != batch.num_rows() * request.dimensions
                    || values.values().iter().any(|value| !value.is_finite())
                {
                    return Err("PQ4 training vector values differ".to_owned());
                }
                let ids = (0..batch.num_rows())
                    .map(|offset| {
                        next_ordinal
                            .checked_add(u64::try_from(offset).unwrap())
                            .unwrap()
                            .to_le_bytes()
                    })
                    .collect::<Vec<_>>();
                let output_vectors = FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                    Arc::new(Float32Array::from(values.values().to_vec())),
                    None,
                )
                .map_err(|error| format!("PQ4 output vector array failed: {error}"))?;
                let output = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(BinaryArray::from_iter_values(
                            ids.iter().map(<[u8; 8]>::as_slice),
                        )) as ArrayRef,
                        Arc::new(output_vectors),
                    ],
                )
                .map_err(|error| format!("PQ4 stage batch failed: {error}"))?;
                writer
                    .write(&output)
                    .map_err(|error| format!("PQ4 stage write failed: {error}"))?;
                next_ordinal = next_ordinal
                    .checked_add(u64::try_from(batch.num_rows()).unwrap())
                    .ok_or_else(|| "PQ4 staged ordinal overflows".to_owned())?;
            }
        }
        if next_ordinal != request.ordinal_end {
            return Err("PQ4 staged row count differs".to_owned());
        }
        writer
            .close()
            .map_err(|error| format!("PQ4 stage close failed: {error}"))?;
        let (output_sha256, output_bytes) = sha256_file(&request.output)?;
        Ok(Pq4StageReport {
            rows: request.expected_rows,
            shards: inputs.len(),
            output_sha256,
            output_bytes,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.output);
    }
    result
}

fn run() -> Result<(), String> {
    let report = stage_pq4_input(&parse_pq4_stage_args(env::args())?)?;
    let value = BTreeMap::from([
        ("output_bytes", serde_json::json!(report.output_bytes)),
        ("output_sha256", serde_json::json!(report.output_sha256)),
        ("rows", serde_json::json!(report.rows)),
        ("schema", serde_json::json!("borsuk-pq4-stage-v1")),
        ("shards", serde_json::json!(report.shards)),
    ]);
    let mut bytes = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
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
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

    use super::{Pq4StageRequest, parse_pq4_stage_args, sha256_file, stage_pq4_input};

    fn arguments() -> Vec<String> {
        [
            "pq4-stage",
            "--manifest",
            "/data/manifest.json",
            "--manifest-sha256",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "--manifest-bytes",
            "9999",
            "--input-dir",
            "/data/materialized",
            "--output",
            "/data/pq4-input.parquet",
            "--expected-rows",
            "9990000",
            "--ordinal-start",
            "10000000",
            "--ordinal-end",
            "19990000",
            "--dimensions",
            "96",
            "--batch-rows",
            "8192",
            "--execute-stage",
        ]
        .map(str::to_owned)
        .to_vec()
    }

    #[test]
    fn pq4_stage_cli_is_explicit_local_and_rejects_remote_or_ambiguous_flags() {
        let request = parse_pq4_stage_args(arguments()).unwrap();
        assert_eq!(request.batch_rows, 8_192);
        assert_eq!(request.ordinal_start, 10_000_000);
        assert_eq!(request.ordinal_end, 19_990_000);
        assert_eq!(request.output.to_str(), Some("/data/pq4-input.parquet"));
        let mut remote = arguments();
        remote.splice(
            remote.len() - 1..remote.len() - 1,
            ["--bucket".to_owned(), "forbidden".to_owned()],
        );
        assert!(parse_pq4_stage_args(remote).is_err());
        let mut duplicate = arguments();
        duplicate.splice(
            duplicate.len() - 1..duplicate.len() - 1,
            ["--batch-rows".to_owned(), "32".to_owned()],
        );
        assert!(parse_pq4_stage_args(duplicate).is_err());
    }

    #[test]
    fn v26_pq4_100m_stage_preserves_global_ordinals_in_binary_ids() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("materialized");
        std::fs::create_dir(&input).unwrap();
        let mut identities = Vec::new();
        for (shard, first) in [0.25_f32, 0.75].into_iter().enumerate() {
            let path = input.join(format!("train-{shard:08}.parquet"));
            let values = (0..2 * 96)
                .map(|index| first + index as f32 / 1_000.0)
                .collect::<Vec<_>>();
            let vectors = FixedSizeListArray::try_new(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
                Arc::new(Float32Array::from(values)),
                None,
            )
            .unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "emb",
                vectors.data_type().clone(),
                false,
            )]));
            let mut writer =
                ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema.clone(), None)
                    .unwrap();
            writer
                .write(&RecordBatch::try_new(schema, vec![Arc::new(vectors) as ArrayRef]).unwrap())
                .unwrap();
            writer.close().unwrap();
            let (digest, bytes) = sha256_file(&path).unwrap();
            identities.push((digest, bytes));
        }
        let ordinal_start = 100_u64;
        let ordered_inputs = identities
            .iter()
            .enumerate()
            .map(|(shard, (digest, bytes))| {
                serde_json::json!({
                    "authority_kind": "training-shard",
                    "dimensions": 96,
                    "identity": {
                        "digest": digest,
                        "digest_algorithm": "sha256",
                        "encoded_bytes": bytes,
                        "role": format!("training-shard-0007-{shard:04}"),
                        "uri": format!("s3://frozen/train-{shard:08}.parquet")
                    },
                    "metric": "cosine",
                    "ordinal_end": ordinal_start + ((shard + 1) * 2) as u64,
                    "ordinal_start": ordinal_start + (shard * 2) as u64,
                    "physical_schema": "emb:fixed-size-list<element:f32;96>:non-null",
                    "rows": 2
                })
            })
            .collect::<Vec<_>>();
        let manifest = directory.path().join("manifest.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!({
                "dataset_id": "synthetic-clustered-100m-96",
                "ordered_inputs": ordered_inputs,
                "ordinal_end": 104,
                "ordinal_start": 100,
                "schema": "borsuk-v26-pq4-partition-manifest-v1",
                "shard_ordinal": 7
            }))
            .unwrap(),
        )
        .unwrap();
        let (manifest_sha256, manifest_bytes) = sha256_file(&manifest).unwrap();
        let output = directory.path().join("pq4-input.parquet");
        let report = stage_pq4_input(&Pq4StageRequest {
            manifest,
            manifest_sha256,
            manifest_bytes,
            input_dir: input,
            output: output.clone(),
            expected_rows: 4,
            ordinal_start,
            ordinal_end: 104,
            dimensions: 96,
            batch_rows: 32,
        })
        .unwrap();
        assert_eq!((report.rows, report.shards), (4, 2));

        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(output).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(ids.value(0), 100_u64.to_le_bytes());
        assert_eq!(ids.value(3), 103_u64.to_le_bytes());
        let vectors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(values.value(0), 0.25);
        assert_eq!(values.value(2 * 96), 0.75);
    }
}

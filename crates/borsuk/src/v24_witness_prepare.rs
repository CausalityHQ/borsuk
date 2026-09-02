use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use half::f16;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    metric::VectorMetric,
    v23_diagnostic::{V23PageRef, V23QuantizerFamily, decode_v23_page},
    v24_witness::parse_v24_decimal_source_ordinal,
};

const V24_PREPARATION_MANIFEST_SCHEMA: &str = "borsuk-v24-preparation-manifest-v1";
const V24_PREPARATION_RECEIPT_SCHEMA: &str = "borsuk-v24-preparation-receipt-v1";
const V24_PREPARATION_PROGRESS_FILE: &str = "progress.json";
const V24_PAGE_VALIDATION_PARTITIONS: u64 = 256;
const V24_PAGE_VALIDATION_RECORD_BYTES: usize = 8 + 4 + 1 + 192;

/// Exact local-file request for query-independent V24 input preparation.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V24PreparationRunRequest {
    /// Canonical preparation manifest path.
    pub manifest: PathBuf,
    /// Registered SHA-256 of the complete manifest bytes.
    pub manifest_sha256: String,
    /// Complete authenticated local input directory.
    pub input_dir: PathBuf,
    /// Empty output directory owned by this invocation.
    pub output_dir: PathBuf,
    /// Registered URI for the construction-row output.
    pub construction_uri: String,
    /// Registered URI for the page-row output.
    pub page_rows_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24PreparationObjectIdentity {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) digest_algorithm: String,
    pub(crate) digest: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24PreparationShard {
    pub(crate) identity: V24PreparationObjectIdentity,
    pub(crate) ordinal_start: u64,
    pub(crate) ordinal_end: u64,
    pub(crate) rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24PreparationPage {
    pub(crate) identity: V24PreparationObjectIdentity,
    pub(crate) page_ordinal: u32,
    pub(crate) generation_checksum: [u8; 32],
    pub(crate) primary_rows: u64,
    pub(crate) replica_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24PreparationManifest {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) generation: String,
    pub(crate) dataset_id: String,
    pub(crate) index_id: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) d1_report_sha256: String,
    pub(crate) page_uri: String,
    pub(crate) source_row_count: u64,
    pub(crate) physical_row_count: u64,
    pub(crate) shards: Vec<V24PreparationShard>,
    pub(crate) roster: V24PreparationObjectIdentity,
    pub(crate) pages: Vec<V24PreparationPage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct V24PreparationReceipt {
    schema: String,
    claim_eligible: bool,
    generation: String,
    manifest_sha256: String,
    source_row_count: u64,
    physical_row_count: u64,
    page_count: u64,
    outputs: Vec<V24PreparationObjectIdentity>,
}

#[derive(Debug, Serialize)]
struct V24PreparationProgressSnapshot<'a> {
    completed_units: u64,
    phase: &'a str,
    sequence: u64,
    total_units: u64,
}

struct V24PreparationProgressWriter {
    output_dir: PathBuf,
    sequence: u64,
    completed_units: u64,
    total_units: u64,
    committed: bool,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn exact_lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect(),
        ),
        scalar => scalar,
    }
}

impl V24PreparationProgressWriter {
    fn start(output_dir: &Path, total_units: u64) -> Result<Self> {
        if total_units == 0 {
            return Err(invalid("V24 preparation progress total differs"));
        }
        let writer = Self {
            output_dir: output_dir.to_owned(),
            sequence: 0,
            completed_units: 0,
            total_units,
            committed: false,
        };
        writer.write()?;
        Ok(writer)
    }

    fn advance(&mut self, completed_units: u64) -> Result<()> {
        if completed_units <= self.completed_units || completed_units > self.total_units {
            return Err(invalid("V24 preparation progress completed work differs"));
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("V24 preparation progress sequence overflows"))?;
        self.completed_units = completed_units;
        self.write()
    }

    fn write(&self) -> Result<()> {
        let value = serde_json::to_value(V24PreparationProgressSnapshot {
            completed_units: self.completed_units,
            phase: "input-preparation",
            sequence: self.sequence,
            total_units: self.total_units,
        })
        .map_err(|error| invalid(&format!("V24 preparation progress differs: {error}")))?;
        let mut bytes = serde_json::to_vec(&canonical_json_value(value))
            .map_err(|error| invalid(&format!("V24 preparation progress differs: {error}")))?;
        bytes.push(b'\n');
        let temporary = self.output_dir.join(".progress.json.tmp");
        let final_path = self.output_dir.join(V24_PREPARATION_PROGRESS_FILE);
        let result = (|| -> Result<()> {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(|source| BorsukError::Io {
                    path: temporary.clone(),
                    source,
                })?;
            file.write_all(&bytes).map_err(|source| BorsukError::Io {
                path: temporary.clone(),
                source,
            })?;
            file.sync_all().map_err(|source| BorsukError::Io {
                path: temporary.clone(),
                source,
            })?;
            fs::rename(&temporary, &final_path).map_err(|source| BorsukError::Io {
                path: final_path,
                source,
            })?;
            fs::File::open(&self.output_dir)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| BorsukError::Io {
                    path: self.output_dir.clone(),
                    source,
                })
        })();
        if result.is_err() {
            cleanup_output(&temporary);
        }
        result
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for V24PreparationProgressWriter {
    fn drop(&mut self) {
        if !self.committed {
            cleanup_output(&self.output_dir.join(V24_PREPARATION_PROGRESS_FILE));
            cleanup_output(&self.output_dir.join(".progress.json.tmp"));
        }
    }
}

pub(crate) fn canonical_v24_preparation_manifest_bytes(
    manifest: &V24PreparationManifest,
) -> Result<Vec<u8>> {
    validate_v24_preparation_manifest(manifest)?;
    let value = serde_json::to_value(manifest).map_err(|error| {
        invalid(&format!(
            "V24 preparation manifest serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        invalid(&format!(
            "V24 preparation manifest serialization failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn parse_v24_preparation_manifest_bytes(
    bytes: &[u8],
    expected_sha256: &str,
) -> Result<V24PreparationManifest> {
    if !exact_lower_hex(expected_sha256)
        || format!("{:x}", Sha256::digest(bytes)) != expected_sha256
    {
        return Err(invalid("V24 preparation manifest digest differs"));
    }
    let manifest: V24PreparationManifest = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V24 preparation manifest JSON differs: {error}")))?;
    if canonical_v24_preparation_manifest_bytes(&manifest)? != bytes {
        return Err(invalid("V24 preparation manifest canonical bytes differ"));
    }
    Ok(manifest)
}

fn validate_identity(
    identity: &V24PreparationObjectIdentity,
    expected_role: &str,
    expected_algorithm: &str,
    generation: &str,
    roles: &mut BTreeSet<String>,
    uris: &mut BTreeSet<String>,
) -> Result<()> {
    if identity.role != expected_role
        || identity.digest_algorithm != expected_algorithm
        || identity.generation != generation
        || !identity.uri.starts_with("s3://")
        || identity.uri.ends_with('/')
        || identity.uri.contains("/../")
        || !exact_lower_hex(&identity.digest)
        || identity.encoded_bytes == 0
        || !roles.insert(identity.role.clone())
        || !uris.insert(identity.uri.clone())
    {
        return Err(invalid("V24 preparation object identity differs"));
    }
    Ok(())
}

pub(crate) fn validate_v24_preparation_manifest(manifest: &V24PreparationManifest) -> Result<()> {
    if manifest.schema != V24_PREPARATION_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || manifest.generation.is_empty()
        || manifest.dataset_id != "deep-image-96"
        || manifest.index_id.is_empty()
        || !exact_lower_hex(&manifest.source_archive_sha256)
        || !exact_lower_hex(&manifest.d1_report_sha256)
        || !manifest.page_uri.starts_with("s3://")
        || manifest.page_uri.ends_with('/')
        || manifest.page_uri.contains("/../")
        || manifest.source_row_count == 0
        || manifest.physical_row_count < manifest.source_row_count
        || manifest.physical_row_count > manifest.source_row_count.saturating_mul(2)
        || manifest.shards.is_empty()
        || manifest.pages.is_empty()
    {
        return Err(invalid("V24 preparation manifest authority differs"));
    }

    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    let mut next_ordinal = 0_u64;
    for (index, shard) in manifest.shards.iter().enumerate() {
        validate_identity(
            &shard.identity,
            &format!("training-shard-{index:05}"),
            "sha256",
            &manifest.generation,
            &mut roles,
            &mut uris,
        )?;
        if shard.ordinal_start != next_ordinal
            || shard.ordinal_end <= shard.ordinal_start
            || shard.ordinal_end - shard.ordinal_start != shard.rows
        {
            return Err(invalid("V24 preparation shard interval differs"));
        }
        next_ordinal = shard.ordinal_end;
    }
    if next_ordinal != manifest.source_row_count {
        return Err(invalid("V24 preparation source count differs"));
    }

    validate_identity(
        &manifest.roster,
        "page-roster",
        "sha256",
        &manifest.generation,
        &mut roles,
        &mut uris,
    )?;
    let mut primary_rows = 0_u64;
    let mut physical_rows = 0_u64;
    for (index, page) in manifest.pages.iter().enumerate() {
        if usize::try_from(page.page_ordinal).ok() != Some(index)
            || page.generation_checksum == [0; 32]
            || page.primary_rows == 0
        {
            return Err(invalid("V24 preparation page order differs"));
        }
        validate_identity(
            &page.identity,
            &format!("page-body-{index:05}"),
            "blake3",
            &manifest.generation,
            &mut roles,
            &mut uris,
        )?;
        if page.identity.uri != format!("{}/pages/{}", manifest.page_uri, page.identity.digest) {
            return Err(invalid("V24 preparation page URI differs"));
        }
        primary_rows = primary_rows
            .checked_add(page.primary_rows)
            .ok_or_else(|| invalid("V24 preparation primary count overflows"))?;
        physical_rows = physical_rows
            .checked_add(page.primary_rows)
            .and_then(|rows| rows.checked_add(page.replica_rows))
            .ok_or_else(|| invalid("V24 preparation physical count overflows"))?;
    }
    if primary_rows != manifest.source_row_count || physical_rows != manifest.physical_row_count {
        return Err(invalid("V24 preparation page counts differ"));
    }
    Ok(())
}

pub(crate) fn validate_v24_preparation_roster_bytes(
    manifest: &V24PreparationManifest,
    bytes: &[u8],
) -> Result<()> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V24 preparation roster JSON differs: {error}")))?;
    let mut canonical = serde_json::to_vec(&canonical_json_value(value.clone()))
        .map_err(|error| invalid(&format!("V24 preparation roster JSON differs: {error}")))?;
    canonical.push(b'\n');
    let object = value
        .as_object()
        .filter(|object| {
            object.keys().map(String::as_str).collect::<BTreeSet<_>>()
                == BTreeSet::from([
                    "claim_eligible",
                    "d1_report_sha256",
                    "dataset_id",
                    "document_kind",
                    "index_id",
                    "page_uri",
                    "pages",
                    "schema",
                    "source_archive_sha256",
                    "stage",
                ])
        })
        .ok_or_else(|| invalid("V24 preparation roster schema differs"))?;
    let page_uri = object
        .get("page_uri")
        .and_then(serde_json::Value::as_str)
        .filter(|uri| uri.starts_with("s3://") && !uri.ends_with('/') && !uri.contains("/../"))
        .ok_or_else(|| invalid("V24 preparation roster page URI differs"))?;
    if canonical != bytes
        || object.get("schema").and_then(serde_json::Value::as_str) != Some("borsuk-v23-pages-v1")
        || object
            .get("document_kind")
            .and_then(serde_json::Value::as_str)
            != Some("publication-v3-v23-page-roster")
        || object.get("claim_eligible") != Some(&serde_json::Value::Bool(false))
        || object.get("stage").and_then(serde_json::Value::as_str) != Some("d2")
        || object.get("dataset_id").and_then(serde_json::Value::as_str)
            != Some(manifest.dataset_id.as_str())
        || object
            .get("source_archive_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(manifest.source_archive_sha256.as_str())
        || object
            .get("d1_report_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(manifest.d1_report_sha256.as_str())
        || object.get("index_id").and_then(serde_json::Value::as_str)
            != Some(manifest.index_id.as_str())
        || page_uri != manifest.page_uri
    {
        return Err(invalid("V24 preparation roster authority differs"));
    }
    let pages = object
        .get("pages")
        .and_then(serde_json::Value::as_array)
        .filter(|pages| pages.len() == manifest.pages.len())
        .ok_or_else(|| invalid("V24 preparation roster page count differs"))?;
    for (registered, value) in manifest.pages.iter().zip(pages) {
        let page: V23PageRef = serde_json::from_value(value.clone()).map_err(|error| {
            invalid(&format!("V24 preparation page reference differs: {error}"))
        })?;
        if page.page_ordinal != registered.page_ordinal
            || page.generation_checksum != registered.generation_checksum
            || page.metric != VectorMetric::Cosine
            || page.dimensions != 96
            || page.family != V23QuantizerFamily::F16Flat
            || page.code_width != 192
            || page.checksum != registered.identity.digest
            || page.path != format!("pages/{}", registered.identity.digest)
            || page.encoded_bytes != registered.identity.encoded_bytes
            || u64::from(page.primary_rows) != registered.primary_rows
            || u64::from(page.replicated_rows) != registered.replica_rows
            || registered.identity.uri != format!("{page_uri}/{}", page.path)
        {
            return Err(invalid("V24 preparation roster page differs"));
        }
    }
    Ok(())
}

fn construction_input_schema() -> Schema {
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        ),
        false,
    )])
}

fn construction_output_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
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

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut file = fs::File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut digest = Sha256::new();
    let bytes = std::io::copy(&mut file, &mut digest).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    Ok((bytes, format!("{:x}", digest.finalize())))
}

fn cleanup_output(path: &Path) {
    let _ = fs::remove_file(path);
}

#[cfg(test)]
pub(crate) fn prepare_v24_construction_rows(
    manifest: &V24PreparationManifest,
    shard_paths: &[PathBuf],
    output_path: &Path,
    output_uri: &str,
) -> Result<V24PreparationObjectIdentity> {
    prepare_v24_construction_rows_with_progress(
        manifest,
        shard_paths,
        output_path,
        output_uri,
        &mut || Ok(()),
    )
}

fn prepare_v24_construction_rows_with_progress(
    manifest: &V24PreparationManifest,
    shard_paths: &[PathBuf],
    output_path: &Path,
    output_uri: &str,
    on_shard: &mut dyn FnMut() -> Result<()>,
) -> Result<V24PreparationObjectIdentity> {
    validate_v24_preparation_manifest(manifest)?;
    if shard_paths.len() != manifest.shards.len()
        || output_path.exists()
        || !output_uri.starts_with("s3://")
        || output_uri.ends_with('/')
        || output_uri.contains("/../")
    {
        return Err(invalid("V24 construction preparation request differs"));
    }

    let output_schema = Arc::new(construction_output_schema());
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(65_536))
        .set_data_page_size_limit(1024 * 1024)
        .build();
    let output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .map_err(|source| BorsukError::Io {
            path: output_path.to_owned(),
            source,
        })?;
    let result = (|| -> Result<()> {
        let mut writer =
            ArrowWriter::try_new(output, Arc::clone(&output_schema), Some(properties))?;
        let mut source_ordinal = 0_u64;
        for (registered, path) in manifest.shards.iter().zip(shard_paths) {
            let (encoded_bytes, digest) = sha256_file(path)?;
            if encoded_bytes != registered.identity.encoded_bytes
                || digest != registered.identity.digest
            {
                return Err(invalid("V24 preparation shard bytes differ"));
            }
            let file = fs::File::open(path).map_err(|source| BorsukError::Io {
                path: path.clone(),
                source,
            })?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            if builder.schema().as_ref() != &construction_input_schema()
                || builder.metadata().file_metadata().num_rows()
                    != i64::try_from(registered.rows).unwrap()
            {
                return Err(invalid("V24 preparation shard schema differs"));
            }
            let mut shard_rows = 0_u64;
            for batch in builder.build()? {
                let batch = batch?;
                if batch.num_columns() != 1 || batch.column(0).null_count() != 0 {
                    return Err(invalid("V24 preparation shard batch differs"));
                }
                let vectors = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| invalid("V24 preparation shard vectors differ"))?;
                let values = vectors
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| invalid("V24 preparation shard vector child differs"))?;
                if vectors.offset() != 0
                    || vectors.null_count() != 0
                    || values.null_count() != 0
                    || values.values().as_chunks::<96>().0.iter().any(|vector| {
                        vector.iter().any(|value| !value.is_finite())
                            || vector.iter().all(|value| *value == 0.0)
                    })
                {
                    return Err(invalid("V24 preparation shard vector values differ"));
                }
                let rows = u64::try_from(batch.num_rows()).unwrap();
                let ordinals = UInt64Array::from_iter_values(
                    source_ordinal..source_ordinal.saturating_add(rows),
                );
                let output_batch = RecordBatch::try_new(
                    Arc::clone(&output_schema),
                    vec![Arc::new(ordinals) as ArrayRef, Arc::new(vectors.clone())],
                )?;
                writer.write(&output_batch)?;
                source_ordinal = source_ordinal
                    .checked_add(rows)
                    .ok_or_else(|| invalid("V24 preparation source ordinal overflows"))?;
                shard_rows = shard_rows
                    .checked_add(rows)
                    .ok_or_else(|| invalid("V24 preparation shard rows overflow"))?;
            }
            if shard_rows != registered.rows || source_ordinal != registered.ordinal_end {
                return Err(invalid("V24 preparation shard row count differs"));
            }
            on_shard()?;
        }
        if source_ordinal != manifest.source_row_count {
            return Err(invalid("V24 preparation construction count differs"));
        }
        writer.close()?;
        fs::OpenOptions::new()
            .write(true)
            .open(output_path)
            .and_then(|file| file.sync_all())
            .map_err(|source| BorsukError::Io {
                path: output_path.to_owned(),
                source,
            })?;
        Ok(())
    })();
    if let Err(error) = result {
        cleanup_output(output_path);
        return Err(error);
    }
    let (encoded_bytes, digest) = sha256_file(output_path)?;
    Ok(V24PreparationObjectIdentity {
        role: "construction-rows-parquet".to_owned(),
        uri: output_uri.to_owned(),
        digest_algorithm: "sha256".to_owned(),
        digest,
        encoded_bytes,
        generation: manifest.generation.clone(),
    })
}

fn page_rows_schema(construction_digest: &str, generation: &str) -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("page_ordinal", DataType::UInt32, false),
            Field::new("replica", DataType::Boolean, false),
            Field::new("record_id", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                ),
                false,
            ),
        ],
        HashMap::from([
            (
                "construction_rows_sha256".to_owned(),
                construction_digest.to_owned(),
            ),
            ("generation".to_owned(), generation.to_owned()),
        ]),
    )
}

#[derive(Debug, Clone, PartialEq)]
struct V24PreparedPageRow {
    source_ordinal: u64,
    replica: bool,
    record_id: String,
    vector: [f32; 96],
}

fn order_v24_prepared_page_rows(rows: &mut [V24PreparedPageRow]) -> Result<()> {
    if rows.iter().any(|row| {
        row.record_id != row.source_ordinal.to_string()
            || row.vector.iter().any(|value| !value.is_finite())
    }) {
        return Err(invalid("V24 prepared page row differs"));
    }
    rows.sort_unstable_by_key(|row| (row.replica, row.source_ordinal));
    if rows.windows(2).any(|pair| {
        (pair[0].replica, pair[0].source_ordinal) >= (pair[1].replica, pair[1].source_ordinal)
    }) {
        return Err(invalid("V24 prepared page row order differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V24PageValidationRecord {
    source_ordinal: u64,
    page_ordinal: u32,
    replica: bool,
    code: [u8; 192],
}

fn v24_page_validation_partition_count(source_row_count: u64) -> Result<usize> {
    usize::try_from(source_row_count.min(V24_PAGE_VALIDATION_PARTITIONS))
        .ok()
        .filter(|count| *count > 0)
        .ok_or_else(|| invalid("V24 page validation partition count differs"))
}

fn v24_page_validation_paths(output_path: &Path, count: usize) -> Result<Vec<PathBuf>> {
    let parent = output_path
        .parent()
        .ok_or_else(|| invalid("V24 page validation output parent differs"))?;
    let name = output_path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| invalid("V24 page validation output name differs"))?;
    Ok((0..count)
        .map(|index| parent.join(format!(".{name}.validation-{index:03}.tmp")))
        .collect())
}

fn validate_v24_page_validation_run(path: &Path, expected_source_ordinal: &mut u64) -> Result<()> {
    let bytes = fs::read(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let (chunks, remainder) = bytes.as_chunks::<V24_PAGE_VALIDATION_RECORD_BYTES>();
    if !remainder.is_empty() {
        return Err(invalid("V24 page validation run length differs"));
    }
    let mut records = chunks
        .iter()
        .map(|record| {
            let source_ordinal = u64::from_le_bytes(record[..8].try_into().unwrap());
            let page_ordinal = u32::from_le_bytes(record[8..12].try_into().unwrap());
            let replica = match record[12] {
                0 => false,
                1 => true,
                _ => return Err(invalid("V24 page validation replica differs")),
            };
            let mut code = [0_u8; 192];
            code.copy_from_slice(&record[13..]);
            Ok(V24PageValidationRecord {
                source_ordinal,
                page_ordinal,
                replica,
                code,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    records.sort_unstable_by_key(|record| {
        (record.source_ordinal, record.replica, record.page_ordinal)
    });
    let mut offset = 0;
    while offset < records.len() {
        let ordinal = records[offset].source_ordinal;
        let end = records[offset..]
            .iter()
            .position(|record| record.source_ordinal != ordinal)
            .map_or(records.len(), |relative| offset + relative);
        let group = &records[offset..end];
        if ordinal != *expected_source_ordinal
            || group.is_empty()
            || group.len() > 2
            || group[0].replica
            || (group.len() == 2
                && (!group[1].replica
                    || group[0].page_ordinal == group[1].page_ordinal
                    || group[0].code != group[1].code))
        {
            return Err(invalid("V24 page primary/replica relation differs"));
        }
        *expected_source_ordinal = expected_source_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V24 page validation ordinal overflows"))?;
        offset = end;
    }
    Ok(())
}

fn cleanup_v24_page_validation_paths(paths: &[PathBuf]) -> Result<()> {
    for path in paths {
        if path.exists() {
            if path.is_symlink() || !path.is_file() {
                return Err(invalid("V24 page validation cleanup target differs"));
            }
            fs::remove_file(path).map_err(|source| BorsukError::Io {
                path: path.clone(),
                source,
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn prepare_v24_page_rows(
    manifest: &V24PreparationManifest,
    page_paths: &[PathBuf],
    construction: &V24PreparationObjectIdentity,
    output_path: &Path,
    output_uri: &str,
) -> Result<V24PreparationObjectIdentity> {
    prepare_v24_page_rows_with_progress(
        manifest,
        page_paths,
        construction,
        output_path,
        output_uri,
        &mut || Ok(()),
    )
}

fn prepare_v24_page_rows_with_progress(
    manifest: &V24PreparationManifest,
    page_paths: &[PathBuf],
    construction: &V24PreparationObjectIdentity,
    output_path: &Path,
    output_uri: &str,
    on_unit: &mut dyn FnMut() -> Result<()>,
) -> Result<V24PreparationObjectIdentity> {
    validate_v24_preparation_manifest(manifest)?;
    if page_paths.len() != manifest.pages.len()
        || construction.role != "construction-rows-parquet"
        || construction.digest_algorithm != "sha256"
        || !exact_lower_hex(&construction.digest)
        || construction.encoded_bytes == 0
        || construction.generation != manifest.generation
        || output_path.exists()
        || !output_uri.starts_with("s3://")
        || output_uri.ends_with('/')
        || output_uri.contains("/../")
    {
        return Err(invalid("V24 page preparation request differs"));
    }
    let output_schema = Arc::new(page_rows_schema(&construction.digest, &manifest.generation));
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(65_536))
        .set_data_page_size_limit(1024 * 1024)
        .build();
    let output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .map_err(|source| BorsukError::Io {
            path: output_path.to_owned(),
            source,
        })?;
    let validation_partition_count =
        v24_page_validation_partition_count(manifest.source_row_count)?;
    let validation_paths = v24_page_validation_paths(output_path, validation_partition_count)?;
    let result = (|| -> Result<()> {
        let mut writer =
            ArrowWriter::try_new(output, Arc::clone(&output_schema), Some(properties))?;
        let mut validation_writers = validation_paths
            .iter()
            .map(|path| {
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map(|file| BufWriter::with_capacity(64 * 1024, file))
                    .map_err(|source| BorsukError::Io {
                        path: path.clone(),
                        source,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut physical_rows = 0_u64;
        for (registered, path) in manifest.pages.iter().zip(page_paths) {
            let bytes = fs::read(path).map_err(|source| BorsukError::Io {
                path: path.clone(),
                source,
            })?;
            if bytes.len() as u64 != registered.identity.encoded_bytes
                || blake3::hash(&bytes).to_hex().as_str() != registered.identity.digest
            {
                return Err(invalid("V24 preparation page bytes differ"));
            }
            let page_ref = V23PageRef {
                generation_checksum: registered.generation_checksum,
                page_ordinal: registered.page_ordinal,
                metric: VectorMetric::Cosine,
                dimensions: 96,
                family: V23QuantizerFamily::F16Flat,
                code_width: 192,
                path: format!("pages/{}", registered.identity.digest),
                checksum: registered.identity.digest.clone(),
                encoded_bytes: registered.identity.encoded_bytes,
                primary_rows: u32::try_from(registered.primary_rows)
                    .map_err(|_| invalid("V24 preparation primary rows exceed u32"))?,
                replicated_rows: u32::try_from(registered.replica_rows)
                    .map_err(|_| invalid("V24 preparation replica rows exceed u32"))?,
            };
            let decoded = decode_v23_page(Bytes::from(bytes), &page_ref)?;
            let rows = decoded.primary_rows() + decoded.replicated_rows();
            let mut prepared_rows = Vec::with_capacity(rows);
            for row in 0..rows {
                let record_id = decoded
                    .record_id(row)
                    .ok_or_else(|| invalid("V24 preparation page record ID is absent"))?;
                let source_ordinal = parse_v24_decimal_source_ordinal(record_id)?;
                if source_ordinal >= manifest.source_row_count {
                    return Err(invalid("V24 preparation page record ID differs"));
                }
                let replica = row >= decoded.primary_rows();
                let code = decoded
                    .code(row)
                    .ok_or_else(|| invalid("V24 preparation page vector is absent"))?;
                if code.len() != 192 {
                    return Err(invalid("V24 preparation page code width differs"));
                }
                let partition = usize::try_from(
                    source_ordinal
                        .checked_mul(u64::try_from(validation_partition_count).unwrap())
                        .ok_or_else(|| invalid("V24 page validation partition overflows"))?
                        / manifest.source_row_count,
                )
                .unwrap();
                let validation_writer = validation_writers
                    .get_mut(partition)
                    .ok_or_else(|| invalid("V24 page validation partition differs"))?;
                validation_writer
                    .write_all(&source_ordinal.to_le_bytes())
                    .and_then(|()| {
                        validation_writer.write_all(&registered.page_ordinal.to_le_bytes())
                    })
                    .and_then(|()| validation_writer.write_all(&[u8::from(replica)]))
                    .and_then(|()| validation_writer.write_all(code))
                    .map_err(|source| BorsukError::Io {
                        path: validation_paths[partition].clone(),
                        source,
                    })?;
                let mut vector = [0.0_f32; 96];
                let mut nonzero = false;
                for (column, bits) in code.as_chunks::<2>().0.iter().enumerate() {
                    let value = f32::from(f16::from_bits(u16::from_le_bytes(*bits)));
                    if !value.is_finite() {
                        return Err(invalid("V24 preparation page vector is non-finite"));
                    }
                    nonzero |= value != 0.0;
                    vector[column] = value;
                }
                if !nonzero {
                    return Err(invalid("V24 preparation page vector is zero"));
                }
                prepared_rows.push(V24PreparedPageRow {
                    source_ordinal,
                    replica,
                    record_id: std::str::from_utf8(record_id)
                        .map_err(|_| invalid("V24 preparation page record ID is not UTF-8"))?
                        .to_owned(),
                    vector,
                });
            }
            order_v24_prepared_page_rows(&mut prepared_rows)?;
            let page_ordinals = vec![registered.page_ordinal; rows];
            let replicas = prepared_rows
                .iter()
                .map(|row| row.replica)
                .collect::<Vec<_>>();
            let record_ids = prepared_rows
                .iter()
                .map(|row| row.record_id.clone())
                .collect::<Vec<_>>();
            let vectors = prepared_rows
                .iter()
                .flat_map(|row| row.vector)
                .collect::<Vec<_>>();
            let child = Arc::new(Field::new("element", DataType::Float32, false));
            let vector_array = FixedSizeListArray::try_new(
                child,
                96,
                Arc::new(Float32Array::from(vectors)),
                None,
            )?;
            let batch = RecordBatch::try_new(
                Arc::clone(&output_schema),
                vec![
                    Arc::new(UInt32Array::from(page_ordinals)) as ArrayRef,
                    Arc::new(BooleanArray::from(replicas)),
                    Arc::new(StringArray::from(record_ids)),
                    Arc::new(vector_array),
                ],
            )?;
            writer.write(&batch)?;
            physical_rows = physical_rows
                .checked_add(u64::try_from(rows).unwrap())
                .ok_or_else(|| invalid("V24 preparation page rows overflow"))?;
            on_unit()?;
        }
        if physical_rows != manifest.physical_row_count {
            return Err(invalid("V24 preparation page materialization differs"));
        }
        for (path, mut validation_writer) in
            validation_paths.iter().zip(validation_writers.drain(..))
        {
            validation_writer
                .flush()
                .and_then(|()| validation_writer.get_ref().sync_all())
                .map_err(|source| BorsukError::Io {
                    path: path.clone(),
                    source,
                })?;
        }
        writer.close()?;
        let mut expected_source_ordinal = 0_u64;
        for path in &validation_paths {
            validate_v24_page_validation_run(path, &mut expected_source_ordinal)?;
            on_unit()?;
        }
        if expected_source_ordinal != manifest.source_row_count {
            return Err(invalid("V24 page validation source count differs"));
        }
        fs::OpenOptions::new()
            .write(true)
            .open(output_path)
            .and_then(|file| file.sync_all())
            .map_err(|source| BorsukError::Io {
                path: output_path.to_owned(),
                source,
            })?;
        Ok(())
    })();
    let cleanup_result = cleanup_v24_page_validation_paths(&validation_paths);
    if let Err(error) = result {
        let _ = cleanup_result;
        cleanup_output(output_path);
        return Err(error);
    }
    if let Err(error) = cleanup_result {
        cleanup_output(output_path);
        return Err(error);
    }
    let (encoded_bytes, digest) = sha256_file(output_path)?;
    Ok(V24PreparationObjectIdentity {
        role: "page-rows-parquet".to_owned(),
        uri: output_uri.to_owned(),
        digest_algorithm: "sha256".to_owned(),
        digest,
        encoded_bytes,
        generation: manifest.generation.clone(),
    })
}

fn exact_input_paths(
    manifest: &V24PreparationManifest,
    input_dir: &Path,
) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    if input_dir.is_symlink() || !input_dir.is_dir() {
        return Err(invalid("V24 preparation input directory differs"));
    }
    let shard_paths = (0..manifest.shards.len())
        .map(|index| input_dir.join(format!("training-shard-{index:05}.parquet")))
        .collect::<Vec<_>>();
    let page_paths = (0..manifest.pages.len())
        .map(|index| input_dir.join(format!("page-body-{index:05}.page")))
        .collect::<Vec<_>>();
    let mut expected = shard_paths
        .iter()
        .chain(&page_paths)
        .filter_map(|path| path.file_name().map(|name| name.to_owned()))
        .collect::<BTreeSet<_>>();
    expected.insert("page-roster.json".into());
    let observed = fs::read_dir(input_dir)
        .map_err(|source| BorsukError::Io {
            path: input_dir.to_owned(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|source| BorsukError::Io {
                    path: input_dir.to_owned(),
                    source,
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed != expected
        || shard_paths
            .iter()
            .chain(&page_paths)
            .any(|path| path.is_symlink() || !path.is_file())
    {
        return Err(invalid("V24 preparation input inventory differs"));
    }
    let roster_path = input_dir.join("page-roster.json");
    let (roster_bytes, roster_digest) = sha256_file(&roster_path)?;
    if roster_path.is_symlink()
        || roster_bytes != manifest.roster.encoded_bytes
        || roster_digest != manifest.roster.digest
    {
        return Err(invalid("V24 preparation roster bytes differ"));
    }
    let roster = fs::read(&roster_path).map_err(|source| BorsukError::Io {
        path: roster_path,
        source,
    })?;
    validate_v24_preparation_roster_bytes(manifest, &roster)?;
    Ok((shard_paths, page_paths))
}

fn is_forbidden_v24_preparation_environment_name(name: &std::ffi::OsStr) -> bool {
    name.as_encoded_bytes().starts_with(b"AWS_")
}

/// Authenticate and prepare the two deterministic V24 Parquet inputs locally.
#[doc(hidden)]
pub fn run_v24_preparation_request(request: V24PreparationRunRequest) -> Result<Vec<u8>> {
    if std::env::vars_os().any(|(name, _)| is_forbidden_v24_preparation_environment_name(&name)) {
        return Err(invalid("V24 preparation child received AWS authority"));
    }
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest = parse_v24_preparation_manifest_bytes(&manifest_bytes, &request.manifest_sha256)?;
    let (shard_paths, page_paths) = exact_input_paths(&manifest, &request.input_dir)?;
    if request.output_dir.is_symlink()
        || !request.output_dir.is_dir()
        || fs::read_dir(&request.output_dir)
            .map_err(|source| BorsukError::Io {
                path: request.output_dir.clone(),
                source,
            })?
            .next()
            .is_some()
    {
        return Err(invalid("V24 preparation output directory differs"));
    }
    let construction_path = request.output_dir.join("construction-rows.parquet");
    let page_rows_path = request.output_dir.join("page-rows.parquet");
    let receipt_path = request.output_dir.join("preparation-receipt.json");
    let result = (|| -> Result<Vec<u8>> {
        let validation_partitions = v24_page_validation_partition_count(manifest.source_row_count)?;
        let total_units =
            u64::try_from(manifest.shards.len() + manifest.pages.len() + validation_partitions)
                .map_err(|_| invalid("V24 preparation progress total exceeds u64"))?;
        let mut progress = V24PreparationProgressWriter::start(&request.output_dir, total_units)?;
        let mut completed_units = 0_u64;
        let construction = prepare_v24_construction_rows_with_progress(
            &manifest,
            &shard_paths,
            &construction_path,
            &request.construction_uri,
            &mut || {
                completed_units += 1;
                progress.advance(completed_units)
            },
        )?;
        let page_rows = prepare_v24_page_rows_with_progress(
            &manifest,
            &page_paths,
            &construction,
            &page_rows_path,
            &request.page_rows_uri,
            &mut || {
                completed_units += 1;
                progress.advance(completed_units)
            },
        )?;
        let receipt = V24PreparationReceipt {
            schema: V24_PREPARATION_RECEIPT_SCHEMA.to_owned(),
            claim_eligible: false,
            generation: manifest.generation,
            manifest_sha256: request.manifest_sha256,
            source_row_count: manifest.source_row_count,
            physical_row_count: manifest.physical_row_count,
            page_count: manifest.pages.len() as u64,
            outputs: vec![construction, page_rows],
        };
        let value = serde_json::to_value(&receipt).map_err(|error| {
            invalid(&format!(
                "V24 preparation receipt serialization failed: {error}"
            ))
        })?;
        let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
            invalid(&format!(
                "V24 preparation receipt serialization failed: {error}"
            ))
        })?;
        bytes.push(b'\n');
        let mut receipt_file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&receipt_path)
            .map_err(|source| BorsukError::Io {
                path: receipt_path.clone(),
                source,
            })?;
        receipt_file
            .write_all(&bytes)
            .map_err(|source| BorsukError::Io {
                path: receipt_path.clone(),
                source,
            })?;
        receipt_file.sync_all().map_err(|source| BorsukError::Io {
            path: receipt_path.clone(),
            source,
        })?;
        progress.commit();
        Ok(bytes)
    })();
    if result.is_err() {
        for path in [&construction_path, &page_rows_path, &receipt_path] {
            cleanup_output(path);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use std::{fs, process::Command, sync::Arc};

    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
        UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use sha2::{Digest, Sha256};

    use crate::{
        metric::VectorMetric,
        v23_diagnostic::{V23PageInput, V23PageRow, V23QuantizerFamily, encode_v23_page},
    };

    use super::{
        V24PreparationManifest, V24PreparationObjectIdentity, V24PreparationPage,
        V24PreparationRunRequest, V24PreparationShard, V24PreparedPageRow,
        canonical_v24_preparation_manifest_bytes, is_forbidden_v24_preparation_environment_name,
        order_v24_prepared_page_rows, parse_v24_preparation_manifest_bytes,
        prepare_v24_construction_rows, prepare_v24_page_rows, run_v24_preparation_request,
        validate_v24_preparation_manifest, validate_v24_preparation_roster_bytes,
    };

    fn identity(
        role: &str,
        generation: &str,
        algorithm: &str,
        byte: &str,
    ) -> V24PreparationObjectIdentity {
        V24PreparationObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v24-preparation/{role}"),
            digest_algorithm: algorithm.to_owned(),
            digest: byte.repeat(32),
            encoded_bytes: 97,
            generation: generation.to_owned(),
        }
    }

    fn manifest() -> V24PreparationManifest {
        let generation = "v24-preparation-fixture";
        V24PreparationManifest {
            schema: "borsuk-v24-preparation-manifest-v1".to_owned(),
            claim_eligible: false,
            generation: generation.to_owned(),
            dataset_id: "deep-image-96".to_owned(),
            index_id: "index-v24-preparation".to_owned(),
            source_archive_sha256: "bb".repeat(32),
            d1_report_sha256: "aa".repeat(32),
            page_uri: "s3://borsuk-v24-preparation/pages".to_owned(),
            source_row_count: 6,
            physical_row_count: 10,
            shards: vec![
                V24PreparationShard {
                    identity: identity("training-shard-00000", generation, "sha256", "11"),
                    ordinal_start: 0,
                    ordinal_end: 3,
                    rows: 3,
                },
                V24PreparationShard {
                    identity: identity("training-shard-00001", generation, "sha256", "22"),
                    ordinal_start: 3,
                    ordinal_end: 6,
                    rows: 3,
                },
            ],
            roster: identity("page-roster", generation, "sha256", "33"),
            pages: (0_u32..4)
                .map(|page_ordinal| {
                    let mut identity = identity(
                        &format!("page-body-{page_ordinal:05}"),
                        generation,
                        "blake3",
                        &format!("{:02x}", page_ordinal + 4),
                    );
                    identity.uri = format!(
                        "s3://borsuk-v24-preparation/pages/pages/{}",
                        identity.digest
                    );
                    V24PreparationPage {
                        identity,
                        page_ordinal,
                        generation_checksum: [7; 32],
                        primary_rows: if page_ordinal < 2 { 2 } else { 1 },
                        replica_rows: 1,
                    }
                })
                .collect(),
        }
    }

    fn write_shard(path: &std::path::Path, first: u64, rows: usize) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "emb",
            DataType::FixedSizeList(Arc::clone(&child), 96),
            false,
        )]));
        let mut values = Vec::with_capacity(rows * 96);
        for row in 0..rows {
            for column in 0..96 {
                values.push((first as usize + row + column + 1) as f32 / 100.0);
            }
        }
        let vectors =
            FixedSizeListArray::try_new(child, 96, Arc::new(Float32Array::from(values)), None)
                .unwrap();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors) as ArrayRef]).unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn bind_shards(manifest: &mut V24PreparationManifest, directory: &std::path::Path) {
        for (index, shard) in manifest.shards.iter_mut().enumerate() {
            let path = directory.join(format!("training-shard-{index:05}.parquet"));
            write_shard(
                &path,
                shard.ordinal_start,
                usize::try_from(shard.rows).unwrap(),
            );
            let bytes = fs::read(path).unwrap();
            shard.identity.encoded_bytes = bytes.len() as u64;
            shard.identity.digest = format!("{:x}", Sha256::digest(bytes));
        }
    }

    fn f16_code(source_ordinal: u64) -> Box<[u8]> {
        (0..96)
            .flat_map(|column| {
                half::f16::from_f32((source_ordinal as usize + column + 1) as f32 / 100.0)
                    .to_bits()
                    .to_le_bytes()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }

    fn page_row(source_ordinal: u64) -> V23PageRow {
        V23PageRow {
            canonical_record_id: source_ordinal.to_string().into_bytes().into_boxed_slice(),
            code: f16_code(source_ordinal),
        }
    }

    fn bind_pages(manifest: &mut V24PreparationManifest, directory: &std::path::Path) {
        let primary = [vec![0, 1], vec![2, 3], vec![4], vec![5]];
        let replicas = [vec![2], vec![4], vec![5], vec![0]];
        for (index, page) in manifest.pages.iter_mut().enumerate() {
            let bytes = encode_v23_page(&V23PageInput {
                generation_checksum: page.generation_checksum,
                page_ordinal: page.page_ordinal,
                metric: VectorMetric::Cosine,
                dimensions: 96,
                family: V23QuantizerFamily::F16Flat,
                code_width: 192,
                primary_rows: primary[index].iter().copied().map(page_row).collect(),
                replicated_rows: replicas[index].iter().copied().map(page_row).collect(),
            })
            .unwrap();
            let path = directory.join(format!("page-body-{index:05}.page"));
            fs::write(path, &bytes).unwrap();
            page.identity.encoded_bytes = bytes.len() as u64;
            page.identity.digest = blake3::hash(&bytes).to_hex().to_string();
            page.identity.uri = format!(
                "s3://borsuk-v24-preparation/pages/pages/{}",
                page.identity.digest
            );
        }
    }

    fn roster_bytes(manifest: &V24PreparationManifest) -> Vec<u8> {
        let pages = manifest
            .pages
            .iter()
            .map(|page| {
                serde_json::json!({
                    "checksum": page.identity.digest,
                    "code_width": 192,
                    "dimensions": 96,
                    "family": "f16-flat",
                    "generation_checksum": page.generation_checksum,
                    "metric": "cosine",
                    "page_ordinal": page.page_ordinal,
                    "path": format!("pages/{}", page.identity.digest),
                    "primary_rows": page.primary_rows,
                    "replicated_rows": page.replica_rows,
                    "encoded_bytes": page.identity.encoded_bytes,
                })
            })
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "claim_eligible": false,
            "d1_report_sha256": "aa".repeat(32),
            "dataset_id": "deep-image-96",
            "document_kind": "publication-v3-v23-page-roster",
            "index_id": "index-v24-preparation",
            "page_uri": manifest.page_uri,
            "pages": pages,
            "schema": "borsuk-v23-pages-v1",
            "source_archive_sha256": "bb".repeat(32),
            "stage": "d2",
        });
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn v24_preparation_authority_binds_contiguous_shards_pages_and_counts() {
        let baseline = manifest();
        validate_v24_preparation_manifest(&baseline).unwrap();

        let mut mutation = baseline.clone();
        mutation.shards[1].ordinal_start = 4;
        assert!(validate_v24_preparation_manifest(&mutation).is_err());

        let mut mutation = baseline.clone();
        mutation.pages[2].page_ordinal = 1;
        assert!(validate_v24_preparation_manifest(&mutation).is_err());

        let mut mutation = baseline.clone();
        mutation.source_row_count += 1;
        assert!(validate_v24_preparation_manifest(&mutation).is_err());

        let mut mutation = baseline.clone();
        mutation.physical_row_count += 1;
        assert!(validate_v24_preparation_manifest(&mutation).is_err());
    }

    #[test]
    fn v24_preparation_roster_accepts_frozen_page_namespace_semantics() {
        let frozen = manifest();
        assert_eq!(frozen.page_uri, "s3://borsuk-v24-preparation/pages");
        assert_eq!(
            frozen.pages[0].identity.uri,
            format!(
                "{}/pages/{}",
                frozen.page_uri, frozen.pages[0].identity.digest
            )
        );

        validate_v24_preparation_manifest(&frozen).unwrap();
        validate_v24_preparation_roster_bytes(&frozen, &roster_bytes(&frozen)).unwrap();
    }

    #[test]
    fn v24_preparation_authority_rejects_leakage_and_identity_drift() {
        let baseline = manifest();

        let mut mutation = baseline.clone();
        mutation.shards[0].identity.role = "query-parquet".to_owned();
        assert!(validate_v24_preparation_manifest(&mutation).is_err());

        let mut mutation = baseline.clone();
        mutation.pages[0].identity.generation = "other-generation".to_owned();
        assert!(validate_v24_preparation_manifest(&mutation).is_err());

        let mut mutation = baseline.clone();
        mutation.pages[0].identity.uri = mutation.roster.uri.clone();
        assert!(validate_v24_preparation_manifest(&mutation).is_err());

        let mut mutation = baseline.clone();
        mutation.shards.swap(0, 1);
        assert!(validate_v24_preparation_manifest(&mutation).is_err());
    }

    #[test]
    fn v24_preparation_authority_manifest_is_canonical_and_digest_bound() {
        let baseline = manifest();
        let bytes = canonical_v24_preparation_manifest_bytes(&baseline).unwrap();
        let digest = format!("{:x}", Sha256::digest(&bytes));
        assert_eq!(
            parse_v24_preparation_manifest_bytes(&bytes, &digest).unwrap(),
            baseline
        );
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut noncanonical = bytes.clone();
        noncanonical.push(b'\n');
        let noncanonical_digest = format!("{:x}", Sha256::digest(&noncanonical));
        assert!(parse_v24_preparation_manifest_bytes(&noncanonical, &noncanonical_digest).is_err());
        assert!(parse_v24_preparation_manifest_bytes(&bytes, &"ff".repeat(32)).is_err());

        let mut unknown: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown["legacy_compatibility"] = serde_json::Value::Bool(true);
        let mut unknown_bytes = serde_json::to_vec(&unknown).unwrap();
        unknown_bytes.push(b'\n');
        let unknown_digest = format!("{:x}", Sha256::digest(&unknown_bytes));
        assert!(
            parse_v24_preparation_manifest_bytes(&unknown_bytes, &unknown_digest).is_err(),
            "accepted an unknown pre-release compatibility field"
        );
    }

    #[test]
    fn v24_preparation_construction_rows_are_exact_and_byte_deterministic() {
        let temporary = tempfile::tempdir().unwrap();
        let mut manifest = manifest();
        bind_shards(&mut manifest, temporary.path());
        let paths = (0..manifest.shards.len())
            .map(|index| {
                temporary
                    .path()
                    .join(format!("training-shard-{index:05}.parquet"))
            })
            .collect::<Vec<_>>();
        let first = temporary.path().join("construction-first.parquet");
        let second = temporary.path().join("construction-second.parquet");

        let first_identity = prepare_v24_construction_rows(
            &manifest,
            &paths,
            &first,
            "s3://borsuk-v24-preparation/construction-rows.parquet",
        )
        .unwrap();
        let second_identity = prepare_v24_construction_rows(
            &manifest,
            &paths,
            &second,
            "s3://borsuk-v24-preparation/construction-rows.parquet",
        )
        .unwrap();
        assert_eq!(first_identity, second_identity);
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());
        assert_eq!(first_identity.role, "construction-rows-parquet");
        assert_eq!(first_identity.digest_algorithm, "sha256");

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(first).unwrap()).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 6);
        assert_eq!(
            builder.schema().as_ref(),
            &Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new(
                    "vector",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("element", DataType::Float32, false)),
                        96,
                    ),
                    false,
                ),
            ])
        );
        let ordinals = builder
            .build()
            .unwrap()
            .map(|batch| {
                let batch = batch.unwrap();
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>()
            .concat();
        assert_eq!(ordinals, (0_u64..6).collect::<Vec<_>>());

        let mut drift = manifest.clone();
        drift.shards[0].identity.digest = "ff".repeat(32);
        assert!(
            prepare_v24_construction_rows(
                &drift,
                &paths,
                &temporary.path().join("drift.parquet"),
                "s3://borsuk-v24-preparation/construction-drift.parquet",
            )
            .is_err()
        );
    }

    #[test]
    fn v24_preparation_page_rows_decode_frozen_pages_and_bind_construction() {
        let temporary = tempfile::tempdir().unwrap();
        let mut manifest = manifest();
        bind_pages(&mut manifest, temporary.path());
        let paths = (0..manifest.pages.len())
            .map(|index| temporary.path().join(format!("page-body-{index:05}.page")))
            .collect::<Vec<_>>();
        let construction = V24PreparationObjectIdentity {
            role: "construction-rows-parquet".to_owned(),
            uri: "s3://borsuk-v24-preparation/construction.parquet".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: "aa".repeat(32),
            encoded_bytes: 111,
            generation: manifest.generation.clone(),
        };
        let first = temporary.path().join("pages-first.parquet");
        let second = temporary.path().join("pages-second.parquet");
        let first_identity = prepare_v24_page_rows(
            &manifest,
            &paths,
            &construction,
            &first,
            "s3://borsuk-v24-preparation/page-rows.parquet",
        )
        .unwrap();
        let second_identity = prepare_v24_page_rows(
            &manifest,
            &paths,
            &construction,
            &second,
            "s3://borsuk-v24-preparation/page-rows.parquet",
        )
        .unwrap();
        assert_eq!(first_identity, second_identity);
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(first).unwrap()).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 10);
        assert_eq!(
            builder.schema().metadata().get("construction_rows_sha256"),
            Some(&construction.digest)
        );
        let rows = builder
            .build()
            .unwrap()
            .map(|batch| {
                let batch = batch.unwrap();
                let pages = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();
                let replicas = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let ids = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| {
                        (
                            pages.value(row),
                            replicas.value(row),
                            ids.value(row).to_owned(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .concat();
        assert_eq!(
            rows,
            vec![
                (0, false, "0".to_owned()),
                (0, false, "1".to_owned()),
                (0, true, "2".to_owned()),
                (1, false, "2".to_owned()),
                (1, false, "3".to_owned()),
                (1, true, "4".to_owned()),
                (2, false, "4".to_owned()),
                (2, true, "5".to_owned()),
                (3, false, "5".to_owned()),
                (3, true, "0".to_owned()),
            ]
        );

        let drift_page = encode_v23_page(&V23PageInput {
            generation_checksum: manifest.pages[0].generation_checksum,
            page_ordinal: 0,
            metric: VectorMetric::Cosine,
            dimensions: 96,
            family: V23QuantizerFamily::F16Flat,
            code_width: 192,
            primary_rows: vec![page_row(0), page_row(1)],
            replicated_rows: vec![V23PageRow {
                canonical_record_id: b"2".to_vec().into_boxed_slice(),
                code: f16_code(3),
            }],
        })
        .unwrap();
        fs::write(&paths[0], &drift_page).unwrap();
        manifest.pages[0].identity.encoded_bytes = drift_page.len() as u64;
        manifest.pages[0].identity.digest = blake3::hash(&drift_page).to_hex().to_string();
        manifest.pages[0].identity.uri = format!(
            "{}/pages/{}",
            manifest.page_uri, manifest.pages[0].identity.digest
        );
        assert!(
            prepare_v24_page_rows(
                &manifest,
                &paths,
                &construction,
                &temporary.path().join("replica-drift.parquet"),
                "s3://borsuk-v24-preparation/page-rows-drift.parquet",
            )
            .is_err(),
            "accepted a replica vector that differs from its primary"
        );
        assert!(
            fs::read_dir(temporary.path()).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".validation-")
            }),
            "left page-validation scratch after rejection"
        );
    }

    #[test]
    fn v24_preparation_roster_exactly_binds_every_historical_page() {
        let mut baseline = manifest();
        let temporary = tempfile::tempdir().unwrap();
        bind_pages(&mut baseline, temporary.path());
        let bytes = roster_bytes(&baseline);
        validate_v24_preparation_roster_bytes(&baseline, &bytes).unwrap();

        let mut drift: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        drift["pages"][0]["path"] = serde_json::Value::String(format!("pages/{}", "ff".repeat(32)));
        let mut drift_bytes = serde_json::to_vec(&drift).unwrap();
        drift_bytes.push(b'\n');
        assert!(validate_v24_preparation_roster_bytes(&baseline, &drift_bytes).is_err());

        let mut drift: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        drift["pages"].as_array_mut().unwrap().swap(0, 1);
        let mut drift_bytes = serde_json::to_vec(&drift).unwrap();
        drift_bytes.push(b'\n');
        assert!(validate_v24_preparation_roster_bytes(&baseline, &drift_bytes).is_err());

        let mut drift: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        drift["source_archive_sha256"] = serde_json::Value::String("cc".repeat(32));
        let mut drift_bytes = serde_json::to_vec(&drift).unwrap();
        drift_bytes.push(b'\n');
        assert!(validate_v24_preparation_roster_bytes(&baseline, &drift_bytes).is_err());

        for (field, value) in [
            ("dataset_id", serde_json::json!("other-dataset")),
            ("index_id", serde_json::json!("other-index")),
            ("d1_report_sha256", serde_json::json!("dd".repeat(32))),
            ("page_uri", serde_json::json!("s3://other-prefix/pages")),
        ] {
            let mut drift: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            drift[field] = value;
            let mut drift_bytes = serde_json::to_vec(&drift).unwrap();
            drift_bytes.push(b'\n');
            assert!(
                validate_v24_preparation_roster_bytes(&baseline, &drift_bytes).is_err(),
                "accepted drift in {field}"
            );
        }
    }

    #[test]
    fn v24_preparation_page_rows_use_numeric_record_order() {
        let vector = [1.0_f32; 96];
        let mut rows = vec![
            V24PreparedPageRow {
                source_ordinal: 10,
                replica: false,
                record_id: "10".to_owned(),
                vector,
            },
            V24PreparedPageRow {
                source_ordinal: 2,
                replica: false,
                record_id: "2".to_owned(),
                vector,
            },
            V24PreparedPageRow {
                source_ordinal: 1,
                replica: true,
                record_id: "1".to_owned(),
                vector,
            },
        ];
        order_v24_prepared_page_rows(&mut rows).unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| (row.replica, row.source_ordinal))
                .collect::<Vec<_>>(),
            vec![(false, 2), (false, 10), (true, 1)]
        );
    }

    #[test]
    fn v24_preparation_environment_rejects_every_aws_authority_variable() {
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_ROLE_ARN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_CONFIG_FILE",
            "AWS_REGION",
            "AWS_CUSTOM_FUTURE_AUTHORITY",
        ] {
            assert!(
                is_forbidden_v24_preparation_environment_name(std::ffi::OsStr::new(name)),
                "accepted {name}"
            );
        }
        assert!(!is_forbidden_v24_preparation_environment_name(
            std::ffi::OsStr::new("RUST_LOG")
        ));
    }

    #[test]
    fn v24_preparation_process_helper() {
        let Some(root) = std::env::var_os("BORSUK_V24_PREPARATION_HELPER_ROOT") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let output = std::path::PathBuf::from(
            std::env::var_os("BORSUK_V24_PREPARATION_HELPER_OUTPUT").unwrap(),
        );
        let bytes = fs::read(root.join("manifest.json")).unwrap();
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let manifest = parse_v24_preparation_manifest_bytes(&bytes, &digest).unwrap();
        let shards = (0..manifest.shards.len())
            .map(|index| root.join(format!("training-shard-{index:05}.parquet")))
            .collect::<Vec<_>>();
        let pages = (0..manifest.pages.len())
            .map(|index| root.join(format!("page-body-{index:05}.page")))
            .collect::<Vec<_>>();
        let construction = prepare_v24_construction_rows(
            &manifest,
            &shards,
            &output.join("construction-rows.parquet"),
            "s3://borsuk-v24-preparation/construction-rows.parquet",
        )
        .unwrap();
        prepare_v24_page_rows(
            &manifest,
            &pages,
            &construction,
            &output.join("page-rows.parquet"),
            "s3://borsuk-v24-preparation/page-rows.parquet",
        )
        .unwrap();
    }

    #[test]
    fn v24_preparation_run_request_helper() {
        let Some(root) = std::env::var_os("BORSUK_V24_PREPARATION_RUN_HELPER") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let manifest = root.join("manifest.json");
        let bytes = fs::read(&manifest).unwrap();
        run_v24_preparation_request(V24PreparationRunRequest {
            manifest,
            manifest_sha256: format!("{:x}", Sha256::digest(bytes)),
            input_dir: root.join("inputs"),
            output_dir: root.join("outputs"),
            construction_uri: "s3://borsuk-v24-preparation/run/construction-rows.parquet"
                .to_owned(),
            page_rows_uri: "s3://borsuk-v24-preparation/run/page-rows.parquet".to_owned(),
        })
        .unwrap();
    }

    #[test]
    fn v24_preparation_run_request_emits_authenticated_progress_and_receipt() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let inputs = root.join("inputs");
        let outputs = root.join("outputs");
        fs::create_dir(&inputs).unwrap();
        fs::create_dir(&outputs).unwrap();
        let mut manifest = manifest();
        bind_shards(&mut manifest, &inputs);
        bind_pages(&mut manifest, &inputs);
        let roster = roster_bytes(&manifest);
        manifest.roster.encoded_bytes = roster.len() as u64;
        manifest.roster.digest = format!("{:x}", Sha256::digest(&roster));
        fs::write(inputs.join("page-roster.json"), roster).unwrap();
        fs::write(
            root.join("manifest.json"),
            canonical_v24_preparation_manifest_bytes(&manifest).unwrap(),
        )
        .unwrap();

        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v24_witness_prepare::tests::v24_preparation_run_request_helper",
                "--nocapture",
            ])
            .env_clear()
            .env("BORSUK_V24_PREPARATION_RUN_HELPER", root)
            .status()
            .unwrap();
        assert!(status.success());
        assert_eq!(
            fs::read(outputs.join("progress.json")).unwrap(),
            b"{\"completed_units\":12,\"phase\":\"input-preparation\",\"sequence\":12,\"total_units\":12}\n"
        );
        assert!(outputs.join("preparation-receipt.json").is_file());
    }

    #[test]
    fn v24_preparation_separate_process_outputs_are_byte_identical() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let mut manifest = manifest();
        bind_shards(&mut manifest, root);
        bind_pages(&mut manifest, root);
        fs::write(
            root.join("manifest.json"),
            canonical_v24_preparation_manifest_bytes(&manifest).unwrap(),
        )
        .unwrap();
        let outputs = [root.join("first"), root.join("second")];
        for output in &outputs {
            fs::create_dir(output).unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "v24_witness_prepare::tests::v24_preparation_process_helper",
                    "--nocapture",
                ])
                .env("BORSUK_V24_PREPARATION_HELPER_ROOT", root)
                .env("BORSUK_V24_PREPARATION_HELPER_OUTPUT", output)
                .status()
                .unwrap();
            assert!(status.success());
        }
        for name in ["construction-rows.parquet", "page-rows.parquet"] {
            assert_eq!(
                fs::read(outputs[0].join(name)).unwrap(),
                fs::read(outputs[1].join(name)).unwrap(),
                "separate-process {name} bytes differ"
            );
        }
    }
}

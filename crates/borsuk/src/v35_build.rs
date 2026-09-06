//! Query-independent, bounded construction primitives for the V35 format.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    io::{Cursor, Write},
    mem::size_of,
    sync::Arc,
};

use arrow_array::{
    Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, RecordBatch,
    UInt16Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V35ArtifactIdentity, V35CodeDirectoryBlockReference, V35ExactPageRow,
    V35LeafPatch, V35PageDirectoryBlockReference, V35Projection, V35RemoteChunk, V35RemoteCodeRow,
    build_v35_leaf_patch_from_merge_rows, decode_v35_page_directory_arrow,
    encode_v35_exact_page_parquet, encode_v35_page_directory_arrow, encode_v35_remote_code_arrow,
    encode_v35_remote_directory_arrow,
    v35_projection::project_v35_source_row_simd,
    v35_remote::{
        build_v35_residual_sq_descriptor_from_slices, projected_exact_page_decoded_bytes,
    },
};

const MORTON_COORDINATES: usize = 16;
const MORTON_BOUNDARIES: usize = 255;
const MORTON_FORMAT: &str = "borsuk-v35-morton-model-arrow-v2";
const MORTON_METADATA_KEY: &str = "borsuk.v35.morton-model.manifest";
const BUILD_RUN_FORMAT: &str = "borsuk-v35-build-scratch-arrow-v2";
const BUILD_RUN_METADATA_KEY: &str = "borsuk.v35.build-run.manifest";
const MAX_BUILD_RUN_BATCH_ROWS: usize = 256;
const MAX_BUILDER_BYTES: u64 = 64 * 1_048_576;
const BUILD_RUN_FILE_OVERHEAD_BYTES: usize = 64 * 1024;
const BUILD_RUN_BATCH_OVERHEAD_BYTES: usize = 16 * 1024;
const SOURCE_BLOCK_FORMAT: &str = "borsuk-v35-source-block-parquet-v1";
const SOURCE_BLOCK_METADATA_KEY: &str = "borsuk.v35.source-block.manifest";
const MAX_SOURCE_BLOCK_ROWS: usize = 8_192;
const SOURCE_BLOCK_DECODE_ENVELOPE_BYTES: usize = 1_048_576;
const MIN_GROUP_CODE_BYTES: u64 = 349_526;
const TARGET_GROUP_CODE_BYTES: u64 = 524_288;
const GROUP_MEMORY_ENVELOPE_BYTES: u64 = 1_048_576;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35MortonManifest {
    authority: V35BuildAuthority,
    format: String,
    projected_dimensions: u32,
}

fn morton_schema(manifest: &V35MortonManifest) -> Result<Arc<Schema>> {
    let manifest = serde_json::to_string(&manifest)
        .map_err(|_| invalid("V35 Morton manifest cannot be serialized"))?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("coordinate", DataType::UInt16, false),
            Field::new(
                "boundaries",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float64, false)),
                    MORTON_BOUNDARIES as i32,
                ),
                false,
            ),
        ],
        HashMap::from([(MORTON_METADATA_KEY.to_owned(), manifest)]),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable source, projection, and attempt binding for V35 construction.
pub struct V35BuildAuthority {
    attempt_id: String,
    projection_checksum_sha256: String,
    source_archive_sha256: String,
    source_id: String,
}

impl V35BuildAuthority {
    /// Construct one strict query-independent build authority.
    pub fn new(
        attempt_id: &str,
        source_id: &str,
        source_archive_sha256: &str,
        projection_checksum: [u8; 32],
    ) -> Result<Self> {
        let authority = Self {
            attempt_id: attempt_id.to_owned(),
            projection_checksum_sha256: digest_hex(&projection_checksum),
            source_archive_sha256: source_archive_sha256.to_owned(),
            source_id: source_id.to_owned(),
        };
        validate_build_authority(&authority)?;
        Ok(authority)
    }

    /// Unique construction-attempt identity.
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }
}

fn digest_hex(digest: &[u8; 32]) -> String {
    digest
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            std::fmt::Write::write_fmt(&mut output, format_args!("{byte:02x}"))
                .expect("writing to a String cannot fail");
            output
        })
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_authority_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn validate_build_authority(authority: &V35BuildAuthority) -> Result<()> {
    if !is_authority_token(&authority.attempt_id)
        || !is_authority_token(&authority.source_id)
        || !is_lower_hex_digest(&authority.source_archive_sha256)
        || !is_lower_hex_digest(&authority.projection_checksum_sha256)
        || authority
            .projection_checksum_sha256
            .bytes()
            .all(|byte| byte == b'0')
    {
        return Err(invalid("V35 build authority differs"));
    }
    Ok(())
}

fn validate_projection_authority(
    authority: &V35BuildAuthority,
    projection: &V35Projection,
) -> Result<()> {
    if authority.projection_checksum_sha256 != digest_hex(&projection.checksum())
        || projection.dimensions().source == 0
        || usize::from(projection.dimensions().routing) < MORTON_COORDINATES
    {
        return Err(invalid("V35 build projection authority differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35SourceBlockManifest {
    first_source_ordinal: u64,
    format: String,
    rows: u32,
    source_archive_sha256: String,
    source_dimensions: u32,
    source_id: String,
}

fn source_block_schema(manifest: &V35SourceBlockManifest) -> Result<Arc<Schema>> {
    let source_dimensions = i32::try_from(manifest.source_dimensions)
        .map_err(|_| invalid("V35 source dimensions overflow"))?;
    let manifest_json = serde_json::to_string(manifest)
        .map_err(|_| invalid("V35 source block manifest cannot be serialized"))?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("id", DataType::UInt64, false),
            Field::new("sequence", DataType::UInt64, false),
            Field::new(
                "source",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    source_dimensions,
                ),
                false,
            ),
        ],
        HashMap::from([(SOURCE_BLOCK_METADATA_KEY.to_owned(), manifest_json)]),
    )))
}

#[derive(Debug, Clone, PartialEq)]
/// Frozen sixteen-coordinate quantile model for a 128-bit Morton build key.
pub struct V35MortonModel {
    authority: V35BuildAuthority,
    selected_coordinates: Vec<u16>,
    boundaries: Vec<Vec<f64>>,
    projected_dimensions: usize,
}

impl V35MortonModel {
    /// Exact source, projection, and attempt authority bound into this model.
    pub fn authority(&self) -> &V35BuildAuthority {
        &self.authority
    }
    /// Selected projected coordinates in decreasing population-variance order.
    pub fn selected_coordinates(&self) -> &[u16] {
        &self.selected_coordinates
    }

    /// The 255 increasing quantile boundaries for one selected-coordinate slot.
    pub fn boundaries(&self, selected_slot: usize) -> Option<&[f64]> {
        self.boundaries.get(selected_slot).map(Vec::as_slice)
    }

    /// Compute the exact most-significant-bit-first interleaved Morton key.
    pub fn key(&self, projected_row: &[f64]) -> Result<u128> {
        if projected_row.len() != self.projected_dimensions
            || projected_row.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V35 Morton source row differs"));
        }
        let buckets = self
            .selected_coordinates
            .iter()
            .zip(&self.boundaries)
            .map(|(coordinate, boundaries)| {
                let value = projected_row[usize::from(*coordinate)];
                u8::try_from(boundaries.partition_point(|boundary| *boundary < value))
                    .expect("255 boundaries fit one byte")
            })
            .collect::<Vec<_>>();
        let mut key = 0_u128;
        for bit in (0..8).rev() {
            for bucket in &buckets {
                key = (key << 1) | u128::from((bucket >> bit) & 1);
            }
        }
        Ok(key)
    }

    /// Encode the model as strict cross-language Arrow IPC.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        validate_model(self)?;
        let manifest = V35MortonManifest {
            authority: self.authority.clone(),
            format: MORTON_FORMAT.to_owned(),
            projected_dimensions: u32::try_from(self.projected_dimensions)
                .map_err(|_| invalid("V35 Morton projected dimensions overflow"))?,
        };
        let schema = morton_schema(&manifest)?;
        let values = self
            .boundaries
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let boundaries = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float64, false)),
            MORTON_BOUNDARIES as i32,
            Arc::new(Float64Array::from(values)),
            None,
        )?;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt16Array::from(self.selected_coordinates.clone())),
                Arc::new(boundaries),
            ],
        )?;
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
        let mut bytes = Vec::new();
        let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        Ok(bytes)
    }

    /// Authenticate and decode the one strict Arrow IPC representation.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
        let schema = reader.schema();
        let manifest_json = schema
            .metadata()
            .get(MORTON_METADATA_KEY)
            .ok_or_else(|| invalid("V35 Morton manifest is missing"))?;
        let manifest: V35MortonManifest = serde_json::from_str(manifest_json)
            .map_err(|_| invalid("V35 Morton manifest differs"))?;
        if schema.metadata().len() != 1
            || serde_json::to_string(&manifest)
                .map_err(|_| invalid("V35 Morton manifest cannot be serialized"))?
                != *manifest_json
            || manifest.format != MORTON_FORMAT
            || manifest.projected_dimensions < MORTON_COORDINATES as u32
            || reader.num_batches() != 1
            || schema.as_ref() != morton_schema(&manifest)?.as_ref()
        {
            return Err(invalid("V35 Morton model Arrow authority differs"));
        }
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| invalid("V35 Morton model Arrow batch is missing"))?;
        if reader.next().is_some() || batch.num_rows() != MORTON_COORDINATES {
            return Err(invalid("V35 Morton model Arrow rows differ"));
        }
        let coordinates = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("V35 Morton coordinate column differs"))?;
        let boundary_rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V35 Morton boundary column differs"))?;
        let values = boundary_rows
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("V35 Morton boundary values differ"))?;
        if coordinates.null_count() != 0
            || boundary_rows.null_count() != 0
            || values.null_count() != 0
        {
            return Err(invalid("V35 Morton model nullability differs"));
        }
        let selected_coordinates = coordinates.values().to_vec();
        let boundaries = values
            .values()
            .as_chunks::<MORTON_BOUNDARIES>()
            .0
            .iter()
            .map(|row| row.to_vec())
            .collect::<Vec<_>>();
        let projected_dimensions = manifest.projected_dimensions as usize;
        let model = Self {
            authority: manifest.authority,
            selected_coordinates,
            boundaries,
            projected_dimensions,
        };
        validate_model(&model)?;
        if model.canonical_bytes()? != bytes {
            return Err(invalid("V35 Morton model bytes are noncanonical"));
        }
        Ok(model)
    }
}

fn validate_model(model: &V35MortonModel) -> Result<()> {
    validate_build_authority(&model.authority)?;
    let mut unique = model.selected_coordinates.clone();
    unique.sort_unstable();
    unique.dedup();
    if model.selected_coordinates.len() != MORTON_COORDINATES
        || unique.len() != MORTON_COORDINATES
        || model.boundaries.len() != MORTON_COORDINATES
        || model.projected_dimensions < MORTON_COORDINATES
        || model
            .selected_coordinates
            .iter()
            .any(|coordinate| usize::from(*coordinate) >= model.projected_dimensions)
        || model.boundaries.iter().any(|boundaries| {
            boundaries.len() != MORTON_BOUNDARIES
                || boundaries.iter().any(|value| !value.is_finite())
                || boundaries.windows(2).any(|pair| pair[0] > pair[1])
        })
    {
        return Err(invalid("V35 Morton model authority differs"));
    }
    Ok(())
}

/// Train a deterministic query-independent Morton model from source sample rows.
pub fn train_v35_morton_model(
    source_rows: &[Vec<f32>],
    projection: &V35Projection,
    authority: V35BuildAuthority,
) -> Result<V35MortonModel> {
    validate_build_authority(&authority)?;
    validate_projection_authority(&authority, projection)?;
    let source_dimensions = usize::try_from(projection.dimensions().source)
        .map_err(|_| invalid("V35 projection source dimensions overflow"))?;
    if source_rows.len() < 256
        || source_rows
            .iter()
            .any(|row| row.len() != source_dimensions || row.iter().any(|value| !value.is_finite()))
    {
        return Err(invalid("V35 Morton training sample differs"));
    }
    let projected_rows = source_rows
        .iter()
        .map(|row| project_v35_source_row_simd(projection, row).map(|(coordinates, _)| coordinates))
        .collect::<Result<Vec<_>>>()?;
    let dimensions = projected_rows.first().map_or(0, Vec::len);
    if dimensions < MORTON_COORDINATES
        || dimensions > usize::from(u16::MAX) + 1
        || projected_rows
            .iter()
            .any(|row| row.len() != dimensions || row.iter().any(|value| !value.is_finite()))
    {
        return Err(invalid("V35 Morton training sample differs"));
    }
    let count = projected_rows.len() as f64;
    let mut ranked = (0..dimensions)
        .map(|coordinate| {
            let mean = projected_rows
                .iter()
                .fold(0.0, |sum, row| sum + row[coordinate])
                / count;
            let variance = projected_rows.iter().fold(0.0, |sum, row| {
                let delta = row[coordinate] - mean;
                delta.mul_add(delta, sum)
            }) / count;
            (variance, coordinate)
        })
        .collect::<Vec<_>>();
    if ranked.iter().any(|(variance, _)| !variance.is_finite()) {
        return Err(invalid("V35 Morton training variance is nonfinite"));
    }
    ranked.sort_by(|left, right| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1)));
    let selected_coordinates = ranked
        .iter()
        .take(MORTON_COORDINATES)
        .map(|(_, coordinate)| *coordinate as u16)
        .collect::<Vec<_>>();
    let boundaries = selected_coordinates
        .iter()
        .map(|coordinate| {
            let mut values = projected_rows
                .iter()
                .map(|row| row[usize::from(*coordinate)])
                .collect::<Vec<_>>();
            values.sort_by(f64::total_cmp);
            (1..=MORTON_BOUNDARIES)
                .map(|quantile| {
                    let rank = quantile
                        .checked_mul(values.len())
                        .expect("bounded sample rank")
                        .div_ceil(256)
                        - 1;
                    values[rank]
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let model = V35MortonModel {
        authority,
        selected_coordinates,
        boundaries,
        projected_dimensions: dimensions,
    };
    validate_model(&model)?;
    Ok(model)
}

#[derive(Debug, Clone, PartialEq)]
/// One query-independent source row carried through bounded construction.
pub struct V35BuildRow {
    source_ordinal: u64,
    id: u64,
    sequence: u64,
    source: Vec<f32>,
}

impl V35BuildRow {
    /// Construct one finite source row with immutable identity.
    pub fn new(source_ordinal: u64, id: u64, sequence: u64, source: Vec<f32>) -> Result<Self> {
        if sequence == 0 || source.is_empty() || source.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V35 build row authority differs"));
        }
        Ok(Self {
            source_ordinal,
            id,
            sequence,
            source,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
/// One ordered, bounded source block; the builder never owns two blocks.
pub struct V35BuildBlock {
    rows: Vec<V35BuildRow>,
}

impl V35BuildBlock {
    /// Construct one dimension-consistent source-ordinal-ordered block.
    pub fn new(rows: Vec<V35BuildRow>) -> Result<Self> {
        let source_dimensions = rows.first().map_or(0, |row| row.source.len());
        if rows.is_empty()
            || rows.windows(2).any(|pair| {
                pair[0].source_ordinal.checked_add(1) != Some(pair[1].source_ordinal)
                    || (pair[0].id, pair[0].sequence) == (pair[1].id, pair[1].sequence)
            })
            || rows.iter().any(|row| row.source.len() != source_dimensions)
        {
            return Err(invalid("V35 build block authority differs"));
        }
        Ok(Self { rows })
    }

    /// Project and admit the complete peak before sorting or Arrow allocation.
    pub fn projected_peak_live_bytes(
        &self,
        model: &V35MortonModel,
        projection: &V35Projection,
    ) -> Result<u64> {
        let memory = project_build_block_live_bytes(self, model, projection)?;
        if memory.peak_live_bytes > MAX_BUILDER_BYTES {
            return Err(invalid("V35 build live memory exceeds admission"));
        }
        Ok(memory.peak_live_bytes)
    }
}

/// Authenticate and decode one bounded cross-language Parquet source shard.
pub fn decode_v35_source_block_parquet(
    bytes: Bytes,
    registered: &V35ArtifactIdentity,
    authority: &V35BuildAuthority,
    dimensions: crate::V35Dimensions,
) -> Result<V35BuildBlock> {
    validate_build_authority(authority)?;
    let encoded_bytes = bytes.len();
    if registered.role != "build-source-block"
        || registered.digest_algorithm != "sha256"
        || registered.length != bytes.len() as u64
        || registered.digest != format!("{:x}", Sha256::digest(&bytes))
        || !registered.uri.starts_with("s3://")
        || !registered.uri.ends_with(".parquet")
    {
        return Err(invalid("V35 source block identity differs"));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let schema = builder.schema();
    let manifest_json = schema
        .metadata()
        .get(SOURCE_BLOCK_METADATA_KEY)
        .ok_or_else(|| invalid("V35 source block manifest is missing"))?;
    let manifest: V35SourceBlockManifest = serde_json::from_str(manifest_json)
        .map_err(|_| invalid("V35 source block manifest differs"))?;
    let rows =
        usize::try_from(manifest.rows).map_err(|_| invalid("V35 source block rows overflow"))?;
    let source_dimensions = usize::try_from(dimensions.source)
        .map_err(|_| invalid("V35 source dimensions overflow"))?;
    let decoded_peak_bytes = rows
        .checked_mul(source_dimensions)
        .and_then(|values| values.checked_mul(size_of::<f32>()))
        .and_then(|values| values.checked_mul(2))
        .and_then(|values| values.checked_add(rows.checked_mul(size_of::<V35BuildRow>())?))
        .and_then(|values| values.checked_add(rows.checked_mul(3 * size_of::<u64>())?))
        .and_then(|values| values.checked_add(encoded_bytes))
        .and_then(|values| values.checked_add(SOURCE_BLOCK_DECODE_ENVELOPE_BYTES))
        .and_then(|values| u64::try_from(values).ok())
        .ok_or_else(|| invalid("V35 source block memory projection overflow"))?;
    if schema.metadata().len() != 1
        || serde_json::to_string(&manifest)
            .map_err(|_| invalid("V35 source block manifest cannot be serialized"))?
            != *manifest_json
        || manifest.format != SOURCE_BLOCK_FORMAT
        || manifest.source_id != authority.source_id
        || manifest.source_archive_sha256 != authority.source_archive_sha256
        || manifest.source_dimensions != dimensions.source
        || rows == 0
        || rows > MAX_SOURCE_BLOCK_ROWS
        || decoded_peak_bytes > MAX_BUILDER_BYTES
        || builder.metadata().file_metadata().num_rows() != i64::from(manifest.rows)
        || schema.as_ref() != source_block_schema(&manifest)?.as_ref()
        || manifest
            .first_source_ordinal
            .checked_add(u64::from(manifest.rows))
            .is_none()
    {
        return Err(invalid("V35 source block authority differs"));
    }
    let mut reader = builder.with_batch_size(MAX_SOURCE_BLOCK_ROWS).build()?;
    let mut decoded = Vec::with_capacity(rows);
    let mut next_source_ordinal = manifest.first_source_ordinal;
    for batch in &mut reader {
        let batch = batch?;
        if batch.num_rows() == 0
            || batch.num_columns() != 4
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V35 source block batch differs"));
        }
        let source_ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V35 source ordinal column differs"))?;
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V35 source ID column differs"))?;
        let sequences = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V35 source sequence column differs"))?;
        let sources = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V35 source vector column differs"))?;
        let source_values = sources
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V35 source vector values differ"))?;
        if sources.value_length() != i32::try_from(dimensions.source).unwrap_or(i32::MAX)
            || source_values.null_count() != 0
        {
            return Err(invalid("V35 source vector authority differs"));
        }
        for row in 0..batch.num_rows() {
            if source_ordinals.value(row) != next_source_ordinal {
                return Err(invalid("V35 source ordinal order differs"));
            }
            let start = row
                .checked_mul(source_dimensions)
                .ok_or_else(|| invalid("V35 source vector offset overflow"))?;
            let end = start
                .checked_add(source_dimensions)
                .ok_or_else(|| invalid("V35 source vector offset overflow"))?;
            let source = source_values
                .values()
                .get(start..end)
                .ok_or_else(|| invalid("V35 source vector length differs"))?
                .to_vec();
            decoded.push(V35BuildRow::new(
                next_source_ordinal,
                ids.value(row),
                sequences.value(row),
                source,
            )?);
            next_source_ordinal = next_source_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V35 source ordinal overflows"))?;
        }
        if decoded.len() > rows {
            return Err(invalid("V35 source block rows differ"));
        }
    }
    if decoded.len() != rows {
        return Err(invalid("V35 source block rows differ"));
    }
    V35BuildBlock::new(decoded)
}

/// Ordered source-block capability without query, truth, listing, or random access.
pub trait V35BuildBlockSource {
    /// Yield the next owned block; the caller drops it before requesting another.
    fn next_block(&mut self) -> Result<Option<V35BuildBlock>>;
}

/// Write-only remote scratch capability without read/list/delete operations.
pub trait V35BuildScratchSink {
    /// Persist one complete authenticated Arrow run under its exact ordinal.
    fn write_run(&mut self, run_ordinal: u32, bytes: &[u8]) -> Result<V35ArtifactIdentity>;
}

#[derive(Debug, Clone, PartialEq)]
/// One authenticated scratch row owned by the bounded external merge.
pub struct V35BuildMergeRow {
    morton_key: u128,
    source_ordinal: u64,
    id: u64,
    sequence: u64,
    source: Vec<f32>,
    projected: Vec<f64>,
}

impl V35BuildMergeRow {
    /// Construct one finite scratch row; merge independently verifies its key.
    pub fn new(
        morton_key: u128,
        source_ordinal: u64,
        id: u64,
        sequence: u64,
        source: Vec<f32>,
        projected: Vec<f64>,
    ) -> Result<Self> {
        if sequence == 0
            || source.is_empty()
            || projected.len() < MORTON_COORDINATES
            || source.iter().any(|value| !value.is_finite())
            || projected.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V35 build merge row differs"));
        }
        Ok(Self {
            morton_key,
            source_ordinal,
            id,
            sequence,
            source,
            projected,
        })
    }

    /// Exact 128-bit locality key.
    pub fn morton_key(&self) -> u128 {
        self.morton_key
    }
    /// Stable source ordinal.
    pub fn source_ordinal(&self) -> u64 {
        self.source_ordinal
    }
    /// Immutable vector ID.
    pub fn id(&self) -> u64 {
        self.id
    }
    /// Immutable mutation sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Full-dimensional exact source vector.
    pub fn source(&self) -> &[f32] {
        &self.source
    }
    /// Internally derived routing vector from the authenticated scratch run.
    pub fn projected(&self) -> &[f64] {
        &self.projected
    }
}

/// Bounded authenticated scratch-run capability for one merge pass.
pub trait V35BuildMergeSource {
    /// Number of runs participating in this pass; hard-capped at 32.
    fn run_count(&self) -> usize;
    /// Yield the next owned row from one run in strict local order.
    fn next_row(&mut self, run: usize) -> Result<Option<V35BuildMergeRow>>;
}

/// Write-only leaf capability; implementations must release each leaf after return.
pub trait V35BuildLeafSink {
    /// Consume one nonempty at-most-256-row leaf in exact global order.
    fn write_leaf(&mut self, rows: Vec<V35BuildMergeRow>) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq)]
/// One consecutive group of leaves bounded by its dimension-derived code payload.
pub struct V35BuildStorageGroup {
    dimensions: crate::V35Dimensions,
    bits_per_dimension: u8,
    group_ordinal: u32,
    logical_start: u64,
    code_bytes: u64,
    leaves: Vec<Vec<V35BuildMergeRow>>,
}

impl V35BuildStorageGroup {
    /// Authenticated source and routing dimensions.
    pub fn dimensions(&self) -> crate::V35Dimensions {
        self.dimensions
    }

    /// Remote scalar-quantization rate used for grouping.
    pub fn bits_per_dimension(&self) -> u8 {
        self.bits_per_dimension
    }

    /// Zero-based group ordinal in global Morton order.
    pub fn group_ordinal(&self) -> u32 {
        self.group_ordinal
    }

    /// First logical row represented by this group.
    pub fn logical_start(&self) -> u64 {
        self.logical_start
    }

    /// Dimension-bound SQ code payload before object framing.
    pub fn code_bytes(&self) -> u64 {
        self.code_bytes
    }

    /// Complete rows retained by this group.
    pub fn row_count(&self) -> u64 {
        self.leaves.iter().map(|leaf| leaf.len() as u64).sum()
    }

    /// Consecutive leaves in exact global Morton order.
    pub fn leaves(&self) -> &[Vec<V35BuildMergeRow>] {
        &self.leaves
    }
}

/// Write-only storage-group capability; implementations release each group after return.
pub trait V35BuildStorageGroupSink {
    /// Consume one complete consecutive group.
    fn write_group(&mut self, group: V35BuildStorageGroup) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Work and live-memory evidence from dimension-bound leaf grouping.
pub struct V35BuildStorageGroupReceipt {
    rows: u64,
    groups: u32,
    peak_live_bytes: u64,
}

impl V35BuildStorageGroupReceipt {
    /// Complete rows grouped exactly once.
    pub fn rows(self) -> u64 {
        self.rows
    }

    /// Complete consecutive groups emitted.
    pub fn groups(self) -> u32 {
        self.groups
    }

    /// Greatest conservatively projected live group memory.
    pub fn peak_live_bytes(self) -> u64 {
        self.peak_live_bytes
    }
}

/// Streaming adapter from globally ordered leaves to dimension-bound storage groups.
pub struct V35BuildStorageGroupAssembler<'a, S: V35BuildStorageGroupSink> {
    sink: &'a mut S,
    dimensions: crate::V35Dimensions,
    bits_per_dimension: u8,
    source_dimensions: usize,
    projected_dimensions: usize,
    code_bytes_per_row: u64,
    max_rows_per_group: u64,
    leaves: Vec<Vec<V35BuildMergeRow>>,
    rows: u64,
    code_bytes: u64,
    live_row_bytes: u64,
    previous_order: Option<(u128, u64)>,
    next_group_ordinal: u32,
    next_logical_start: u64,
    receipt: V35BuildStorageGroupReceipt,
}

impl<'a, S: V35BuildStorageGroupSink> V35BuildStorageGroupAssembler<'a, S> {
    /// Construct one SQ4/SQ8 grouping adapter from authenticated source dimensions.
    pub fn new(
        dimensions: crate::V35Dimensions,
        bits_per_dimension: u8,
        sink: &'a mut S,
    ) -> Result<Self> {
        if dimensions.source == 0 || dimensions.routing == 0 || !matches!(bits_per_dimension, 4 | 8)
        {
            return Err(invalid("V35 build storage-group dimensions differ"));
        }
        let source_dimensions = usize::try_from(dimensions.source)
            .map_err(|_| invalid("V35 build storage-group dimensions overflow"))?;
        let code_bits = u64::from(dimensions.source)
            .checked_mul(u64::from(bits_per_dimension))
            .ok_or_else(|| invalid("V35 build storage-group code width overflows"))?;
        let code_bytes_per_row = code_bits.div_ceil(8);
        let max_rows_per_group = TARGET_GROUP_CODE_BYTES / code_bytes_per_row;
        if max_rows_per_group == 0
            || max_rows_per_group
                .checked_mul(code_bytes_per_row)
                .is_none_or(|bytes| bytes < MIN_GROUP_CODE_BYTES)
        {
            return Err(invalid("V35 build storage-group code width is infeasible"));
        }
        Ok(Self {
            sink,
            dimensions,
            bits_per_dimension,
            source_dimensions,
            projected_dimensions: usize::from(dimensions.routing),
            code_bytes_per_row,
            max_rows_per_group,
            leaves: Vec::new(),
            rows: 0,
            code_bytes: 0,
            live_row_bytes: 0,
            previous_order: None,
            next_group_ordinal: 0,
            next_logical_start: 0,
            receipt: V35BuildStorageGroupReceipt {
                rows: 0,
                groups: 0,
                peak_live_bytes: 0,
            },
        })
    }

    fn projected_live_bytes(&self) -> Result<u64> {
        let outer_bytes = self
            .leaves
            .capacity()
            .checked_mul(size_of::<Vec<V35BuildMergeRow>>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| invalid("V35 build storage-group memory overflows"))?;
        GROUP_MEMORY_ENVELOPE_BYTES
            .checked_add(outer_bytes)
            .and_then(|bytes| bytes.checked_add(self.live_row_bytes))
            .ok_or_else(|| invalid("V35 build storage-group memory overflows"))
    }

    fn leaf_live_bytes(rows: &Vec<V35BuildMergeRow>) -> Result<u64> {
        let container_bytes = rows
            .capacity()
            .checked_mul(size_of::<V35BuildMergeRow>())
            .and_then(|bytes| bytes.checked_add(size_of::<Vec<V35BuildMergeRow>>()))
            .ok_or_else(|| invalid("V35 build storage-group memory overflows"))?;
        rows.iter().try_fold(
            u64::try_from(container_bytes)
                .map_err(|_| invalid("V35 build storage-group memory conversion overflows"))?,
            |total, row| {
                let source_bytes = row
                    .source
                    .capacity()
                    .checked_mul(size_of::<f32>())
                    .ok_or_else(|| invalid("V35 build storage-group memory overflows"))?;
                let projected_bytes = row
                    .projected
                    .capacity()
                    .checked_mul(size_of::<f64>())
                    .ok_or_else(|| invalid("V35 build storage-group memory overflows"))?;
                let owned_bytes = source_bytes
                    .checked_add(projected_bytes)
                    .and_then(|bytes| u64::try_from(bytes).ok())
                    .ok_or_else(|| invalid("V35 build storage-group memory overflows"))?;
                total
                    .checked_add(owned_bytes)
                    .ok_or_else(|| invalid("V35 build storage-group memory overflows"))
            },
        )
    }

    fn emit_group(&mut self) -> Result<()> {
        if self.rows == 0 || self.leaves.is_empty() || self.code_bytes == 0 {
            return Err(invalid("V35 build storage group is empty"));
        }
        let group = V35BuildStorageGroup {
            dimensions: self.dimensions,
            bits_per_dimension: self.bits_per_dimension,
            group_ordinal: self.next_group_ordinal,
            logical_start: self.next_logical_start,
            code_bytes: self.code_bytes,
            leaves: std::mem::take(&mut self.leaves),
        };
        self.sink.write_group(group)?;
        self.receipt.rows = self
            .receipt
            .rows
            .checked_add(self.rows)
            .ok_or_else(|| invalid("V35 build storage-group rows overflow"))?;
        self.receipt.groups = self
            .receipt
            .groups
            .checked_add(1)
            .ok_or_else(|| invalid("V35 build storage-group count overflow"))?;
        self.next_group_ordinal = self
            .next_group_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V35 build storage-group ordinal overflow"))?;
        self.next_logical_start = self
            .next_logical_start
            .checked_add(self.rows)
            .ok_or_else(|| invalid("V35 build storage-group logical range overflow"))?;
        self.rows = 0;
        self.code_bytes = 0;
        self.live_row_bytes = 0;
        Ok(())
    }

    /// Emit the terminal group, which alone may be smaller than the minimum payload.
    pub fn finish(mut self) -> Result<V35BuildStorageGroupReceipt> {
        if self.rows == 0 && self.receipt.rows == 0 {
            return Err(invalid("V35 build storage groups are empty"));
        }
        if self.rows != 0 {
            self.emit_group()?;
        }
        Ok(self.receipt)
    }
}

impl<S: V35BuildStorageGroupSink> V35BuildLeafSink for V35BuildStorageGroupAssembler<'_, S> {
    fn write_leaf(&mut self, rows: Vec<V35BuildMergeRow>) -> Result<()> {
        if rows.is_empty()
            || rows.len() > MAX_BUILD_RUN_BATCH_ROWS
            || rows.iter().any(|row| {
                row.source.len() != self.source_dimensions
                    || row.projected.len() != self.projected_dimensions
            })
        {
            return Err(invalid("V35 build storage-group leaf differs"));
        }
        let mut previous = self.previous_order;
        for row in &rows {
            let order = (row.morton_key, row.source_ordinal);
            if previous.is_some_and(|previous| previous >= order) {
                return Err(invalid("V35 build storage-group order differs"));
            }
            previous = Some(order);
        }
        let incoming_live_bytes = Self::leaf_live_bytes(&rows)?;
        let mut rows = rows.into_iter().peekable();
        while rows.peek().is_some() {
            if self.rows == self.max_rows_per_group {
                debug_assert!(self.code_bytes >= MIN_GROUP_CODE_BYTES);
                self.emit_group()?;
            }
            let available = usize::try_from(self.max_rows_per_group - self.rows)
                .unwrap_or(usize::MAX)
                .min(MAX_BUILD_RUN_BATCH_ROWS)
                .min(rows.len());
            if available == 0 {
                return Err(invalid("V35 build storage-group capacity differs"));
            }
            let fragment = rows.by_ref().take(available).collect::<Vec<_>>();
            let fragment_rows = u64::try_from(fragment.len())
                .map_err(|_| invalid("V35 build storage-group leaf rows overflow"))?;
            let fragment_code_bytes = fragment_rows
                .checked_mul(self.code_bytes_per_row)
                .ok_or_else(|| invalid("V35 build storage-group code bytes overflow"))?;
            let fragment_live_bytes = Self::leaf_live_bytes(&fragment)?;
            self.rows = self
                .rows
                .checked_add(fragment_rows)
                .ok_or_else(|| invalid("V35 build storage-group rows overflow"))?;
            self.code_bytes = self
                .code_bytes
                .checked_add(fragment_code_bytes)
                .ok_or_else(|| invalid("V35 build storage-group code bytes overflow"))?;
            self.live_row_bytes = self
                .live_row_bytes
                .checked_add(fragment_live_bytes)
                .ok_or_else(|| invalid("V35 build storage-group memory overflows"))?;
            self.leaves.push(fragment);
            let live_bytes = self
                .projected_live_bytes()?
                .checked_add(incoming_live_bytes)
                .ok_or_else(|| invalid("V35 build storage-group memory overflows"))?;
            if live_bytes > MAX_BUILDER_BYTES {
                return Err(invalid("V35 build storage-group memory exceeds admission"));
            }
            self.receipt.peak_live_bytes = self.receipt.peak_live_bytes.max(live_bytes);
        }
        self.previous_order = previous;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Pre-authorized immutable object destination supplied by a write-only sink.
pub struct V35BuildObjectTarget {
    uri: String,
}

impl V35BuildObjectTarget {
    /// Construct one S3 destination without discovery or endpoint authority.
    pub fn new(uri: &str) -> Result<Self> {
        if !uri.starts_with("s3://") || uri.contains("/corpus/") {
            return Err(invalid("V35 build object target differs"));
        }
        Ok(Self {
            uri: uri.to_owned(),
        })
    }
}

/// Write-only final-object capability without list, read, delete, or endpoint operations.
pub trait V35BuildEncodedObjectSink {
    /// Return the registered target for one dense code group.
    fn code_target(&self, group_ordinal: u32) -> Result<V35BuildObjectTarget>;
    /// Return the registered code-directory target for one dense code group.
    fn code_directory_target(&self, group_ordinal: u32) -> Result<V35BuildObjectTarget>;
    /// Return the registered target for one dense exact-vector page.
    fn page_target(&self, page_ordinal: u32) -> Result<V35BuildObjectTarget>;
    /// Return the registered page-directory target for one dense code group.
    fn page_directory_target(&self, group_ordinal: u32) -> Result<V35BuildObjectTarget>;
    /// Persist one complete authenticated Arrow code object before returning.
    fn write_code_object(
        &mut self,
        identity: V35ArtifactIdentity,
        bytes: &[u8],
        decoded_bytes: u64,
    ) -> Result<String>;
    /// Persist one complete authenticated Arrow code-directory block before returning.
    fn write_code_directory(
        &mut self,
        identity: V35ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<String>;
    /// Persist one complete authenticated Parquet page before returning.
    fn write_exact_page(&mut self, identity: V35ArtifactIdentity, bytes: &[u8]) -> Result<String>;
    /// Persist one complete authenticated Arrow page-directory block before returning.
    fn write_page_directory(
        &mut self,
        identity: V35ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<String>;
}

#[derive(Debug, Clone, PartialEq)]
/// Routing patches and dense ordinal continuation from one encoded storage group.
pub struct V35BuildEncodedGroupReceipt {
    rows: u64,
    patches: Vec<V35LeafPatch>,
    code_directory: V35CodeDirectoryBlockReference,
    page_directory: V35PageDirectoryBlockReference,
    page_count: u32,
    next_leaf_ordinal: u32,
    next_page_ordinal: u32,
}

impl V35BuildEncodedGroupReceipt {
    /// Complete source rows encoded once.
    pub fn rows(&self) -> u64 {
        self.rows
    }
    /// One compact routing patch for every final leaf fragment.
    pub fn patches(&self) -> &[V35LeafPatch] {
        &self.patches
    }
    /// Compact reference to the persisted code-directory block for this group.
    pub fn code_directory(&self) -> &V35CodeDirectoryBlockReference {
        &self.code_directory
    }
    /// Compact reference to the persisted page-directory block for this group.
    pub fn page_directory(&self) -> &V35PageDirectoryBlockReference {
        &self.page_directory
    }
    /// Exact-vector pages written for this group.
    pub fn page_count(&self) -> u32 {
        self.page_count
    }
    /// First unused generation-local leaf ordinal.
    pub fn next_leaf_ordinal(&self) -> u32 {
        self.next_leaf_ordinal
    }
    /// First unused generation-local page ordinal.
    pub fn next_page_ordinal(&self) -> u32 {
        self.next_page_ordinal
    }
    /// Number of routing patches emitted.
    pub fn patch_count(&self) -> usize {
        self.patches.len()
    }
}

/// Encode one bounded group into cross-language code/page objects and routing patches.
pub fn encode_v35_build_storage_group<S: V35BuildEncodedObjectSink>(
    group: V35BuildStorageGroup,
    first_leaf_ordinal: u32,
    first_page_ordinal: u32,
    sink: &mut S,
) -> Result<V35BuildEncodedGroupReceipt> {
    let row_count = group.row_count();
    let code_bytes_per_row = u64::from(group.dimensions.source)
        .checked_mul(u64::from(group.bits_per_dimension))
        .and_then(|bits| bits.checked_add(7))
        .map(|bits| bits / 8)
        .ok_or_else(|| invalid("V35 build encoded group extent overflows"))?;
    if group.leaves.is_empty()
        || row_count == 0
        || group.code_bytes
            != row_count
                .checked_mul(code_bytes_per_row)
                .ok_or_else(|| invalid("V35 build encoded group extent overflows"))?
    {
        return Err(invalid("V35 build encoded group authority differs"));
    }
    let page_count = u32::try_from(group.leaves.len())
        .map_err(|_| invalid("V35 build encoded group page count overflows"))?;
    let code_target = sink.code_target(group.group_ordinal)?;
    if !code_target.uri.ends_with(".arrow") {
        return Err(invalid("V35 build code-object target differs"));
    }
    let mut written_uris = std::collections::BTreeSet::from([code_target.uri.clone()]);
    let code_directory_target = sink.code_directory_target(group.group_ordinal)?;
    if !code_directory_target.uri.ends_with(".arrow")
        || !written_uris.insert(code_directory_target.uri.clone())
    {
        return Err(invalid("V35 build code-directory target differs"));
    }
    let mut page_targets = Vec::with_capacity(group.leaves.len());
    for offset in 0..page_count {
        let page = first_page_ordinal
            .checked_add(offset)
            .ok_or_else(|| invalid("V35 build encoded group page ordinal overflows"))?;
        let target = sink.page_target(page)?;
        if !target.uri.ends_with(".parquet") || !written_uris.insert(target.uri.clone()) {
            return Err(invalid("V35 build page-object target differs"));
        }
        page_targets.push(target);
    }
    let page_directory_target = sink.page_directory_target(group.group_ordinal)?;
    if !page_directory_target.uri.ends_with(".arrow")
        || !written_uris.insert(page_directory_target.uri.clone())
    {
        return Err(invalid("V35 build page-directory target differs"));
    }
    let source_dimensions = usize::try_from(group.dimensions.source)
        .map_err(|_| invalid("V35 build encoded group dimensions overflow"))?;
    for leaf in &group.leaves {
        if projected_exact_page_decoded_bytes(leaf.len(), source_dimensions)? > 4 * 1_048_576 {
            return Err(invalid("V35 build exact page exceeds decoded admission"));
        }
    }
    let source_rows = group
        .leaves
        .iter()
        .flatten()
        .map(|row| (row.source_ordinal, row.source.as_slice()))
        .collect::<Vec<_>>();
    let descriptor =
        build_v35_residual_sq_descriptor_from_slices(&source_rows, group.bits_per_dimension)?;
    drop(source_rows);

    let mut patches = Vec::with_capacity(group.leaves.len());
    let mut code_rows = Vec::with_capacity(group.row_count() as usize);
    let mut logical_start = group.logical_start;
    let mut leaf_ordinal = first_leaf_ordinal;
    let mut page_ordinal = first_page_ordinal;
    for leaf in &group.leaves {
        patches.push(build_v35_leaf_patch_from_merge_rows(
            leaf,
            group.dimensions,
            group.group_ordinal,
            leaf_ordinal,
            logical_start,
        )?);
        for row in leaf {
            code_rows.push(V35RemoteCodeRow::new(
                row.source_ordinal,
                row.id,
                row.sequence,
                page_ordinal,
                None,
            )?);
        }
        logical_start = logical_start
            .checked_add(leaf.len() as u64)
            .ok_or_else(|| invalid("V35 build encoded group logical range overflows"))?;
        leaf_ordinal = leaf_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V35 build encoded group leaf ordinal overflows"))?;
        page_ordinal = page_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V35 build encoded group page ordinal overflows"))?;
    }
    let (code_bytes, decoded_bytes) = encode_v35_remote_code_arrow(
        group.group_ordinal,
        group.logical_start,
        &descriptor,
        &code_rows,
    )?;
    let code_identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&code_bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: code_bytes.len() as u64,
        role: "remote-code-object".to_owned(),
        uri: code_target.uri.clone(),
    };
    let code_version = sink.write_code_object(code_identity.clone(), &code_bytes, decoded_bytes)?;
    let code_chunk = V35RemoteChunk::new(
        group.group_ordinal,
        group.logical_start,
        row_count,
        code_identity.clone(),
        &code_version,
        0,
        code_identity.length,
        decoded_bytes,
        code_identity.digest.clone(),
    )?;
    let (code_directory_bytes, code_directory_identity) = encode_v35_remote_directory_arrow(
        std::slice::from_ref(&code_chunk),
        &code_directory_target.uri,
    )?;
    let code_directory_version =
        sink.write_code_directory(code_directory_identity.clone(), &code_directory_bytes)?;
    let code_directory = V35CodeDirectoryBlockReference::from_encoded(
        std::slice::from_ref(&code_chunk),
        code_directory_identity,
        &code_directory_version,
    )?;
    drop(code_bytes);
    drop(code_rows);
    drop(descriptor);

    let mut page_identities = Vec::with_capacity(group.leaves.len());
    for ((offset, leaf), target) in group.leaves.into_iter().enumerate().zip(page_targets) {
        let page = first_page_ordinal
            .checked_add(
                u32::try_from(offset)
                    .map_err(|_| invalid("V35 build encoded group page ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V35 build encoded group page ordinal overflows"))?;
        let rows = leaf
            .into_iter()
            .map(|row| V35ExactPageRow::new(row.id, row.sequence, row.source))
            .collect::<Result<Vec<_>>>()?;
        let (encoded, page_bytes) = encode_v35_exact_page_parquet(page, &target.uri, &rows)?;
        let page_version = sink.write_exact_page(encoded.object().clone(), &page_bytes)?;
        page_identities.push(encoded.with_stored_version(&page_version)?);
    }
    let (page_directory_bytes, page_directory_identity) = encode_v35_page_directory_arrow(
        group.group_ordinal,
        &page_identities,
        &page_directory_target.uri,
    )?;
    let page_directory_version =
        sink.write_page_directory(page_directory_identity.clone(), &page_directory_bytes)?;
    let page_directory_block = decode_v35_page_directory_arrow(
        &page_directory_bytes,
        &page_directory_identity,
        &page_directory_version,
    )?;
    let page_directory = V35PageDirectoryBlockReference::new(&page_directory_block)?;
    Ok(V35BuildEncodedGroupReceipt {
        rows: row_count,
        patches,
        code_directory,
        page_directory,
        page_count,
        next_leaf_ordinal: leaf_ordinal,
        next_page_ordinal: page_ordinal,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Work and bounded-head evidence from one external merge pass.
pub struct V35BuildMergeReceipt {
    rows: u64,
    leaves: u32,
    peak_heads: u32,
}

impl V35BuildMergeReceipt {
    /// Complete rows emitted.
    pub fn rows(self) -> u64 {
        self.rows
    }
    /// Complete at-most-256-row leaves emitted.
    pub fn leaves(self) -> u32 {
        self.leaves
    }
    /// Greatest simultaneously owned run heads.
    pub fn peak_heads(self) -> u32 {
        self.peak_heads
    }
}

fn validate_merge_row(
    model: &V35MortonModel,
    row: &V35BuildMergeRow,
    source_dimensions: &mut Option<usize>,
) -> Result<()> {
    if model.key(&row.projected)? != row.morton_key
        || source_dimensions.is_some_and(|dimensions| dimensions != row.source.len())
    {
        return Err(invalid("V35 build merge row authority differs"));
    }
    source_dimensions.get_or_insert(row.source.len());
    Ok(())
}

/// Merge at most 32 authenticated runs using one head per run and one leaf buffer.
pub fn merge_v35_build_runs<R: V35BuildMergeSource, S: V35BuildLeafSink>(
    model: &V35MortonModel,
    source: &mut R,
    sink: &mut S,
) -> Result<V35BuildMergeReceipt> {
    validate_model(model)?;
    let run_count = source.run_count();
    if run_count == 0 || run_count > 32 {
        return Err(invalid("V35 build merge fan-in differs"));
    }
    let mut heads = (0..run_count).map(|_| None).collect::<Vec<_>>();
    let mut previous_by_run = vec![None; run_count];
    let mut heap = BinaryHeap::with_capacity(run_count);
    let mut source_dimensions = None;
    for (run, head) in heads.iter_mut().enumerate() {
        if let Some(row) = source.next_row(run)? {
            validate_merge_row(model, &row, &mut source_dimensions)?;
            heap.push(Reverse((row.morton_key, row.source_ordinal, run)));
            *head = Some(row);
        }
    }
    if heap.is_empty() {
        return Err(invalid("V35 build merge source is empty"));
    }
    let peak_heads = u32::try_from(heap.len()).expect("fan-in is at most 32");
    let mut leaf = Vec::with_capacity(MAX_BUILD_RUN_BATCH_ROWS);
    let mut previous_global = None;
    let mut receipt = V35BuildMergeReceipt {
        rows: 0,
        leaves: 0,
        peak_heads,
    };
    while let Some(Reverse((key, source_ordinal, run))) = heap.pop() {
        let row = heads[run]
            .take()
            .ok_or_else(|| invalid("V35 build merge head is missing"))?;
        let order = (key, source_ordinal);
        if (row.morton_key, row.source_ordinal) != order
            || previous_global.is_some_and(|previous| previous >= order)
        {
            return Err(invalid("V35 build merge global order differs"));
        }
        previous_global = Some(order);
        previous_by_run[run] = Some(order);
        leaf.push(row);
        receipt.rows = receipt
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("V35 build merge rows overflow"))?;
        if leaf.len() == MAX_BUILD_RUN_BATCH_ROWS {
            sink.write_leaf(std::mem::take(&mut leaf))?;
            leaf = Vec::with_capacity(MAX_BUILD_RUN_BATCH_ROWS);
            receipt.leaves = receipt
                .leaves
                .checked_add(1)
                .ok_or_else(|| invalid("V35 build merge leaves overflow"))?;
        }
        if let Some(next) = source.next_row(run)? {
            validate_merge_row(model, &next, &mut source_dimensions)?;
            let next_order = (next.morton_key, next.source_ordinal);
            if previous_by_run[run].is_some_and(|previous| previous >= next_order) {
                return Err(invalid("V35 build merge run order differs"));
            }
            heap.push(Reverse((next.morton_key, next.source_ordinal, run)));
            heads[run] = Some(next);
        }
    }
    if !leaf.is_empty() {
        sink.write_leaf(leaf)?;
        receipt.leaves = receipt
            .leaves
            .checked_add(1)
            .ok_or_else(|| invalid("V35 build merge leaves overflow"))?;
    }
    Ok(receipt)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Truthful work and peak-memory projection for scratch-run construction.
pub struct V35BuildScratchReceipt {
    source_rows: u64,
    scratch_runs: u32,
    scratch_bytes: u64,
    peak_live_builder_bytes: u64,
}

impl V35BuildScratchReceipt {
    /// Complete source rows consumed exactly once.
    pub fn source_rows(self) -> u64 {
        self.source_rows
    }
    /// Complete independently authenticated scratch runs emitted.
    pub fn scratch_runs(self) -> u32 {
        self.scratch_runs
    }
    /// Complete encoded scratch bytes persisted.
    pub fn scratch_bytes(self) -> u64 {
        self.scratch_bytes
    }
    /// Maximum checked simultaneously live builder bytes.
    pub fn peak_live_builder_bytes(self) -> u64 {
        self.peak_live_builder_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35BuildRunManifest {
    authority: V35BuildAuthority,
    format: String,
    morton_model_sha256: String,
    projected_dimensions: u32,
    rows: u32,
    run_ordinal: u32,
    source_dimensions: u32,
}

fn build_run_schema(manifest: &V35BuildRunManifest) -> Result<Arc<Schema>> {
    let source = i32::try_from(manifest.source_dimensions)
        .map_err(|_| invalid("V35 build source dimensions overflow"))?;
    let projected = i32::try_from(manifest.projected_dimensions)
        .map_err(|_| invalid("V35 build projected dimensions overflow"))?;
    let manifest_json = serde_json::to_string(manifest)
        .map_err(|_| invalid("V35 build run manifest cannot be serialized"))?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("morton_key", DataType::FixedSizeBinary(16), false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("id", DataType::UInt64, false),
            Field::new("sequence", DataType::UInt64, false),
            Field::new(
                "source",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    source,
                ),
                false,
            ),
            Field::new(
                "projected",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float64, false)),
                    projected,
                ),
                false,
            ),
        ],
        HashMap::from([(BUILD_RUN_METADATA_KEY.to_owned(), manifest_json)]),
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V35BuildBlockMemoryProjection {
    encoded_capacity: usize,
    peak_live_bytes: u64,
}

fn project_build_block_live_bytes(
    block: &V35BuildBlock,
    model: &V35MortonModel,
    projection: &V35Projection,
) -> Result<V35BuildBlockMemoryProjection> {
    validate_projection_authority(&model.authority, projection)?;
    let source_dimensions = usize::try_from(projection.dimensions().source)
        .map_err(|_| invalid("V35 projection source dimensions overflow"))?;
    let projected_dimensions = usize::from(projection.dimensions().routing);
    if projected_dimensions != model.projected_dimensions
        || block
            .rows
            .iter()
            .any(|row| row.source.len() != source_dimensions)
    {
        return Err(invalid("V35 build projection/model dimensions differ"));
    }
    let row_storage = block.rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(size_of::<V35BuildRow>())
            .and_then(|bytes| bytes.checked_add(row.source.capacity().checked_mul(4)?))
    });
    let encoded_payload = block.rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(row.source.len().checked_mul(4)?)
            .and_then(|bytes| bytes.checked_add(projected_dimensions.checked_mul(8)?))
            .and_then(|bytes| bytes.checked_add(40))
    });
    let maximum_batch_payload = block
        .rows
        .chunks(MAX_BUILD_RUN_BATCH_ROWS)
        .try_fold(0_usize, |maximum, rows| {
            let payload = rows.iter().try_fold(0_usize, |total, row| {
                total
                    .checked_add(row.source.len().checked_mul(4)?)
                    .and_then(|bytes| bytes.checked_add(projected_dimensions.checked_mul(8)?))
                    .and_then(|bytes| bytes.checked_add(40))
            });
            payload.map(|payload| maximum.max(payload))
        })
        .ok_or_else(|| invalid("V35 build batch capacity overflow"))?;
    let batches = block.rows.len().div_ceil(MAX_BUILD_RUN_BATCH_ROWS);
    let encoded_capacity = encoded_payload
        .and_then(|bytes| bytes.checked_add(BUILD_RUN_FILE_OVERHEAD_BYTES))
        .and_then(|bytes| bytes.checked_add(batches.checked_mul(BUILD_RUN_BATCH_OVERHEAD_BYTES)?))
        .ok_or_else(|| invalid("V35 build encoded capacity overflow"))?;
    let projected_capacity = block
        .rows
        .len()
        .checked_mul(size_of::<V35ProjectedBuildRow<'static>>() + projected_dimensions * 8)
        .ok_or_else(|| invalid("V35 build projection capacity overflow"))?;
    let peak_live_bytes = row_storage
        .and_then(|bytes| bytes.checked_add(projected_capacity))
        .and_then(|bytes| bytes.checked_add(maximum_batch_payload))
        .and_then(|bytes| bytes.checked_add(encoded_capacity))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid("V35 build live bytes overflow"))?;
    Ok(V35BuildBlockMemoryProjection {
        encoded_capacity,
        peak_live_bytes,
    })
}

struct V35ProjectedBuildRow<'a> {
    key: u128,
    row: &'a V35BuildRow,
    projected: Vec<f64>,
}

struct V35BoundedBuildRunWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl V35BoundedBuildRunWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit),
            limit,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for V35BoundedBuildRunWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|length| length > self.limit)
        {
            return Err(std::io::Error::other(
                "V35 build run exceeds admitted capacity",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode_build_run(
    model: &V35MortonModel,
    projection: &V35Projection,
    run_ordinal: u32,
    block: &V35BuildBlock,
    encoded_capacity: usize,
) -> Result<Vec<u8>> {
    let source_dimensions = block.rows[0].source.len();
    let projected_dimensions = usize::from(projection.dimensions().routing);
    if source_dimensions != usize::try_from(projection.dimensions().source).unwrap_or(usize::MAX)
        || projected_dimensions != model.projected_dimensions
    {
        return Err(invalid("V35 build projection/model dimensions differ"));
    }
    let mut ordered = block
        .rows
        .iter()
        .map(|row| {
            let (projected, _) = project_v35_source_row_simd(projection, &row.source)?;
            Ok(V35ProjectedBuildRow {
                key: model.key(&projected)?,
                row,
                projected,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ordered.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then(left.row.source_ordinal.cmp(&right.row.source_ordinal))
    });
    let manifest = V35BuildRunManifest {
        authority: model.authority.clone(),
        format: BUILD_RUN_FORMAT.to_owned(),
        morton_model_sha256: format!("{:x}", Sha256::digest(model.canonical_bytes()?)),
        projected_dimensions: projected_dimensions as u32,
        rows: block.rows.len() as u32,
        run_ordinal,
        source_dimensions: source_dimensions as u32,
    };
    let schema = build_run_schema(&manifest)?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut output = V35BoundedBuildRunWriter::new(encoded_capacity);
    let mut writer = FileWriter::try_new_with_options(&mut output, schema.as_ref(), options)?;
    for rows in ordered.chunks(MAX_BUILD_RUN_BATCH_ROWS) {
        let keys =
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|row| row.key.to_be_bytes()))?;
        let source_values = rows
            .iter()
            .flat_map(|row| row.row.source.iter().copied())
            .collect::<Vec<_>>();
        let projected_values = rows
            .iter()
            .flat_map(|row| row.projected.iter().copied())
            .collect::<Vec<_>>();
        let source = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            source_dimensions as i32,
            Arc::new(Float32Array::from(source_values)),
            None,
        )?;
        let projected = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float64, false)),
            projected_dimensions as i32,
            Arc::new(Float64Array::from(projected_values)),
            None,
        )?;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(keys),
                Arc::new(UInt64Array::from(
                    rows.iter()
                        .map(|row| row.row.source_ordinal)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.row.id).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.row.sequence).collect::<Vec<_>>(),
                )),
                Arc::new(source),
                Arc::new(projected),
            ],
        )?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(output.into_bytes())
}

/// One authenticated, bounded scratch-run batch exposed to external merge.
pub struct V35BuildRunBatch {
    batch: RecordBatch,
    source_dimensions: usize,
    projected_dimensions: usize,
}

impl V35BuildRunBatch {
    /// Number of rows; never greater than 256.
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
    /// Whether the batch is empty. Authenticated cursors never return an empty batch.
    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }
    fn keys(&self) -> &FixedSizeBinaryArray {
        self.batch
            .column(0)
            .as_any()
            .downcast_ref()
            .expect("validated key column")
    }
    fn ordinals(&self) -> &UInt64Array {
        self.batch
            .column(1)
            .as_any()
            .downcast_ref()
            .expect("validated ordinal column")
    }
    fn ids(&self) -> &UInt64Array {
        self.batch
            .column(2)
            .as_any()
            .downcast_ref()
            .expect("validated ID column")
    }
    fn sequences(&self) -> &UInt64Array {
        self.batch
            .column(3)
            .as_any()
            .downcast_ref()
            .expect("validated sequence column")
    }
    fn source_values(&self) -> &Float32Array {
        self.batch
            .column(4)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("validated source column")
            .values()
            .as_any()
            .downcast_ref()
            .expect("validated source values")
    }
    fn projected_values(&self) -> &Float64Array {
        self.batch
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("validated projected column")
            .values()
            .as_any()
            .downcast_ref()
            .expect("validated projected values")
    }
    /// Exact Morton key for one row.
    pub fn morton_key(&self, row: usize) -> Option<u128> {
        (row < self.len()).then(|| {
            u128::from_be_bytes(
                self.keys()
                    .value(row)
                    .try_into()
                    .expect("fixed 16-byte key"),
            )
        })
    }
    /// Stable source ordinal for one row.
    pub fn source_ordinal(&self, row: usize) -> Option<u64> {
        (row < self.len()).then(|| self.ordinals().value(row))
    }
    /// Immutable vector ID for one row.
    pub fn id(&self, row: usize) -> Option<u64> {
        (row < self.len()).then(|| self.ids().value(row))
    }
    /// Immutable sequence for one row.
    pub fn sequence(&self, row: usize) -> Option<u64> {
        (row < self.len()).then(|| self.sequences().value(row))
    }
    /// Full-dimensional exact source vector for one row.
    pub fn source(&self, row: usize) -> Option<&[f32]> {
        let start = row.checked_mul(self.source_dimensions)?;
        self.source_values()
            .values()
            .get(start..start.checked_add(self.source_dimensions)?)
    }
    /// Resident projected vector for one row.
    pub fn projected(&self, row: usize) -> Option<&[f64]> {
        let start = row.checked_mul(self.projected_dimensions)?;
        self.projected_values()
            .values()
            .get(start..start.checked_add(self.projected_dimensions)?)
    }
}

/// Authenticated streaming cursor over one multi-batch scratch object.
pub struct V35BuildRunCursor<'a> {
    reader: FileReader<Cursor<&'a [u8]>>,
    manifest: V35BuildRunManifest,
    previous: Option<(u128, u64)>,
    rows_seen: u64,
    finished: bool,
}

impl V35BuildRunCursor<'_> {
    /// Scratch run ordinal authenticated by the Arrow manifest.
    pub fn run_ordinal(&self) -> u32 {
        self.manifest.run_ordinal
    }
    /// Rows yielded so far.
    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }
    /// Yield the next authenticated at-most-256-row record batch.
    pub fn next_batch(&mut self) -> Result<Option<V35BuildRunBatch>> {
        if self.finished {
            return Ok(None);
        }
        let Some(batch) = self.reader.next().transpose()? else {
            self.finished = true;
            if self.rows_seen != u64::from(self.manifest.rows) {
                return Err(invalid("V35 build run rows differ"));
            }
            return Ok(None);
        };
        if batch.num_rows() == 0
            || batch.num_rows() > MAX_BUILD_RUN_BATCH_ROWS
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V35 build run batch authority differs"));
        }
        let source_dimensions = self.manifest.source_dimensions as usize;
        let projected_dimensions = self.manifest.projected_dimensions as usize;
        let view = V35BuildRunBatch {
            batch,
            source_dimensions,
            projected_dimensions,
        };
        let source_rows = view
            .batch
            .column(4)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V35 build run source column differs"))?;
        let projected_rows = view
            .batch
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V35 build run projected column differs"))?;
        if source_dimensions == 0
            || projected_dimensions < MORTON_COORDINATES
            || source_rows.value_length() as usize != source_dimensions
            || projected_rows.value_length() as usize != projected_dimensions
            || view.source_values().null_count() != 0
            || view.projected_values().null_count() != 0
        {
            return Err(invalid("V35 build run vector authority differs"));
        }
        for row in 0..view.len() {
            let order = (
                view.morton_key(row).expect("row exists"),
                view.source_ordinal(row).expect("row exists"),
            );
            if self.previous.is_some_and(|previous| previous >= order)
                || view.sequence(row) == Some(0)
                || view
                    .source(row)
                    .expect("validated row")
                    .iter()
                    .any(|value| !value.is_finite())
                || view
                    .projected(row)
                    .expect("validated row")
                    .iter()
                    .any(|value| !value.is_finite())
            {
                return Err(invalid("V35 build run row authority differs"));
            }
            self.previous = Some(order);
        }
        self.rows_seen = self
            .rows_seen
            .checked_add(view.len() as u64)
            .ok_or_else(|| invalid("V35 build run rows overflow"))?;
        if self.rows_seen > u64::from(self.manifest.rows) {
            return Err(invalid("V35 build run rows differ"));
        }
        Ok(Some(view))
    }
}

/// Authenticate one complete scratch object, then expose only bounded batches.
pub fn open_v35_build_run_cursor<'a>(
    bytes: &'a [u8],
    registered: &V35ArtifactIdentity,
    model: &V35MortonModel,
) -> Result<V35BuildRunCursor<'a>> {
    validate_model(model)?;
    let attempt_path = format!("/scratch/{}/", model.authority.attempt_id);
    if registered.role != "build-scratch-run"
        || registered.digest_algorithm != "sha256"
        || registered.length != bytes.len() as u64
        || registered.digest != format!("{:x}", Sha256::digest(bytes))
        || !registered.uri.starts_with("s3://")
        || !registered.uri.contains("/scratch/")
        || !registered.uri.contains(&attempt_path)
        || registered.uri.contains("/corpus/")
    {
        return Err(invalid("V35 build scratch identity differs"));
    }
    let reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let manifest_json = reader
        .schema()
        .metadata()
        .get(BUILD_RUN_METADATA_KEY)
        .ok_or_else(|| invalid("V35 build run manifest is missing"))?
        .clone();
    let manifest: V35BuildRunManifest = serde_json::from_str(&manifest_json)
        .map_err(|_| invalid("V35 build run manifest differs"))?;
    validate_build_authority(&manifest.authority)?;
    let model_sha256 = format!("{:x}", Sha256::digest(model.canonical_bytes()?));
    if reader.schema().metadata().len() != 1
        || serde_json::to_string(&manifest)
            .map_err(|_| invalid("V35 build run manifest cannot be serialized"))?
            != manifest_json
        || manifest.format != BUILD_RUN_FORMAT
        || manifest.authority != model.authority
        || manifest.morton_model_sha256 != model_sha256
        || manifest.rows == 0
        || manifest.source_dimensions == 0
        || manifest.projected_dimensions < MORTON_COORDINATES as u32
        || reader.num_batches() == 0
        || reader.schema().as_ref() != build_run_schema(&manifest)?.as_ref()
    {
        return Err(invalid("V35 build run Arrow authority differs"));
    }
    Ok(V35BuildRunCursor {
        reader,
        manifest,
        previous: None,
        rows_seen: 0,
        finished: false,
    })
}

#[derive(Debug, Clone, PartialEq)]
/// Compact metadata-only view used to inspect a complete scratch run.
pub struct V35BuildRunRow {
    morton_key: u128,
    source_ordinal: u64,
    source_dimensions: usize,
    projected_dimensions: usize,
}

impl V35BuildRunRow {
    /// Locality key in exact build order.
    pub fn morton_key(&self) -> u128 {
        self.morton_key
    }
    /// Stable original source ordinal.
    pub fn source_ordinal(&self) -> u64 {
        self.source_ordinal
    }
    /// Full exact-vector dimensionality retained remotely.
    pub fn source_dimensions(&self) -> usize {
        self.source_dimensions
    }
    /// Resident projected dimensionality.
    pub fn projected_dimensions(&self) -> usize {
        self.projected_dimensions
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Strict decoded view of one authenticated scratch run.
pub struct V35BuildRun {
    run_ordinal: u32,
    rows: Vec<V35BuildRunRow>,
}

impl V35BuildRun {
    /// Scratch run ordinal.
    pub fn run_ordinal(&self) -> u32 {
        self.run_ordinal
    }
    /// Rows in exact `(Morton key,source ordinal)` order.
    pub fn rows(&self) -> &[V35BuildRunRow] {
        &self.rows
    }
}

/// Decode one exact-digest-bound run into metadata only; merge uses the bounded cursor.
pub fn decode_v35_build_run_arrow(
    bytes: &[u8],
    registered: &V35ArtifactIdentity,
    model: &V35MortonModel,
) -> Result<V35BuildRun> {
    let mut cursor = open_v35_build_run_cursor(bytes, registered, model)?;
    let run_ordinal = cursor.run_ordinal();
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch()? {
        rows.reserve(batch.len());
        for row in 0..batch.len() {
            rows.push(V35BuildRunRow {
                morton_key: batch.morton_key(row).expect("row exists"),
                source_ordinal: batch.source_ordinal(row).expect("row exists"),
                source_dimensions: batch.source_dimensions,
                projected_dimensions: batch.projected_dimensions,
            });
        }
    }
    Ok(V35BuildRun { run_ordinal, rows })
}

/// Stream ordered source blocks into bounded authenticated external-sort runs.
pub fn build_v35_scratch_runs<R: V35BuildBlockSource, S: V35BuildScratchSink>(
    model: &V35MortonModel,
    projection: &V35Projection,
    source: &mut R,
    scratch: &mut S,
) -> Result<V35BuildScratchReceipt> {
    let mut receipt = V35BuildScratchReceipt {
        source_rows: 0,
        scratch_runs: 0,
        scratch_bytes: 0,
        peak_live_builder_bytes: 0,
    };
    let mut expected_source_ordinal = None;
    while let Some(block) = source.next_block()? {
        if expected_source_ordinal.is_some_and(|expected| {
            expected
                != block
                    .rows
                    .first()
                    .expect("block is nonempty")
                    .source_ordinal
        }) {
            return Err(invalid("V35 build source block order differs"));
        }
        expected_source_ordinal = block
            .rows
            .last()
            .and_then(|row| row.source_ordinal.checked_add(1));
        if expected_source_ordinal.is_none() {
            return Err(invalid("V35 build source ordinal overflows"));
        }
        let memory = project_build_block_live_bytes(&block, model, projection)?;
        if memory.peak_live_bytes > MAX_BUILDER_BYTES {
            return Err(invalid("V35 build live memory exceeds admission"));
        }
        let bytes = encode_build_run(
            model,
            projection,
            receipt.scratch_runs,
            &block,
            memory.encoded_capacity,
        )?;
        let identity = scratch.write_run(receipt.scratch_runs, &bytes)?;
        decode_v35_build_run_arrow(&bytes, &identity, model)?;
        receipt.source_rows = receipt
            .source_rows
            .checked_add(block.rows.len() as u64)
            .ok_or_else(|| invalid("V35 build source rows overflow"))?;
        receipt.scratch_runs = receipt
            .scratch_runs
            .checked_add(1)
            .ok_or_else(|| invalid("V35 build scratch runs overflow"))?;
        receipt.scratch_bytes = receipt
            .scratch_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("V35 build scratch bytes overflow"))?;
        receipt.peak_live_builder_bytes =
            receipt.peak_live_builder_bytes.max(memory.peak_live_bytes);
    }
    if receipt.source_rows == 0 {
        return Err(invalid("V35 build source is empty"));
    }
    Ok(receipt)
}

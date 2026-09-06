//! Query-independent, bounded construction primitives for the V35 format.

use std::{
    collections::HashMap,
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
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result, V35ArtifactIdentity};

const MORTON_COORDINATES: usize = 16;
const MORTON_BOUNDARIES: usize = 255;
const MORTON_FORMAT: &str = "borsuk-v35-morton-model-arrow-v1";
const MORTON_METADATA_KEY: &str = "borsuk.v35.morton-model.manifest";
const BUILD_RUN_FORMAT: &str = "borsuk-v35-build-scratch-arrow-v1";
const BUILD_RUN_METADATA_KEY: &str = "borsuk.v35.build-run.manifest";
const MAX_BUILD_RUN_BATCH_ROWS: usize = 256;
const MAX_BUILDER_BYTES: u64 = 64 * 1_048_576;
const BUILD_RUN_FILE_OVERHEAD_BYTES: usize = 64 * 1024;
const BUILD_RUN_BATCH_OVERHEAD_BYTES: usize = 16 * 1024;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35MortonManifest {
    authority: V35BuildAuthority,
    format: String,
    source_dimensions: u32,
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

#[derive(Debug, Clone, PartialEq)]
/// Frozen sixteen-coordinate quantile model for a 128-bit Morton build key.
pub struct V35MortonModel {
    authority: V35BuildAuthority,
    selected_coordinates: Vec<u16>,
    boundaries: Vec<Vec<f64>>,
    source_dimensions: usize,
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
        if projected_row.len() != self.source_dimensions
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
            source_dimensions: u32::try_from(self.source_dimensions)
                .map_err(|_| invalid("V35 Morton source dimensions overflow"))?,
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
            || manifest.source_dimensions < MORTON_COORDINATES as u32
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
        let source_dimensions = manifest.source_dimensions as usize;
        let model = Self {
            authority: manifest.authority,
            selected_coordinates,
            boundaries,
            source_dimensions,
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
        || model.source_dimensions < MORTON_COORDINATES
        || model
            .selected_coordinates
            .iter()
            .any(|coordinate| usize::from(*coordinate) >= model.source_dimensions)
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

/// Train a deterministic query-independent Morton model from projected sample rows.
pub fn train_v35_morton_model(
    projected_rows: &[Vec<f64>],
    authority: V35BuildAuthority,
) -> Result<V35MortonModel> {
    validate_build_authority(&authority)?;
    let dimensions = projected_rows.first().map_or(0, Vec::len);
    if projected_rows.len() < 256
        || dimensions < MORTON_COORDINATES
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
        source_dimensions: dimensions,
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
    projected: Vec<f64>,
}

impl V35BuildRow {
    /// Construct one finite source/projected row with immutable identity.
    pub fn new(
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
            return Err(invalid("V35 build row authority differs"));
        }
        Ok(Self {
            source_ordinal,
            id,
            sequence,
            source,
            projected,
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
        let projected_dimensions = rows.first().map_or(0, |row| row.projected.len());
        if rows.is_empty()
            || rows.windows(2).any(|pair| {
                pair[0].source_ordinal >= pair[1].source_ordinal
                    || (pair[0].id, pair[0].sequence) == (pair[1].id, pair[1].sequence)
            })
            || rows.iter().any(|row| {
                row.source.len() != source_dimensions || row.projected.len() != projected_dimensions
            })
        {
            return Err(invalid("V35 build block authority differs"));
        }
        Ok(Self { rows })
    }

    /// Project and admit the complete peak before sorting or Arrow allocation.
    pub fn projected_peak_live_bytes(&self, model: &V35MortonModel) -> Result<u64> {
        let projection = project_build_block_live_bytes(self, model)?;
        if projection.peak_live_bytes > MAX_BUILDER_BYTES {
            return Err(invalid("V35 build live memory exceeds admission"));
        }
        Ok(projection.peak_live_bytes)
    }
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
) -> Result<V35BuildBlockMemoryProjection> {
    if block.rows[0].projected.len() != model.source_dimensions {
        return Err(invalid("V35 build projection/model dimensions differ"));
    }
    let row_storage = block.rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(size_of::<V35BuildRow>())
            .and_then(|bytes| bytes.checked_add(row.source.capacity().checked_mul(4)?))
            .and_then(|bytes| bytes.checked_add(row.projected.capacity().checked_mul(8)?))
    });
    let encoded_payload = block.rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(row.source.len().checked_mul(4)?)
            .and_then(|bytes| bytes.checked_add(row.projected.len().checked_mul(8)?))
            .and_then(|bytes| bytes.checked_add(40))
    });
    let maximum_batch_payload = block
        .rows
        .chunks(MAX_BUILD_RUN_BATCH_ROWS)
        .try_fold(0_usize, |maximum, rows| {
            let payload = rows.iter().try_fold(0_usize, |total, row| {
                total
                    .checked_add(row.source.len().checked_mul(4)?)
                    .and_then(|bytes| bytes.checked_add(row.projected.len().checked_mul(8)?))
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
    let ordered_capacity = block
        .rows
        .len()
        .checked_mul(size_of::<(u128, &V35BuildRow)>())
        .ok_or_else(|| invalid("V35 build order capacity overflow"))?;
    let peak_live_bytes = row_storage
        .and_then(|bytes| bytes.checked_add(ordered_capacity))
        .and_then(|bytes| bytes.checked_add(maximum_batch_payload))
        .and_then(|bytes| bytes.checked_add(encoded_capacity))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid("V35 build live bytes overflow"))?;
    Ok(V35BuildBlockMemoryProjection {
        encoded_capacity,
        peak_live_bytes,
    })
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
    run_ordinal: u32,
    block: &V35BuildBlock,
    encoded_capacity: usize,
) -> Result<Vec<u8>> {
    let source_dimensions = block.rows[0].source.len();
    let projected_dimensions = block.rows[0].projected.len();
    if projected_dimensions != model.source_dimensions {
        return Err(invalid("V35 build projection/model dimensions differ"));
    }
    let mut ordered = block
        .rows
        .iter()
        .map(|row| Ok((model.key(&row.projected)?, row)))
        .collect::<Result<Vec<_>>>()?;
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then(left.1.source_ordinal.cmp(&right.1.source_ordinal))
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
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|(key, _)| key.to_be_bytes()))?;
        let source_values = rows
            .iter()
            .flat_map(|(_, row)| row.source.iter().copied())
            .collect::<Vec<_>>();
        let projected_values = rows
            .iter()
            .flat_map(|(_, row)| row.projected.iter().copied())
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
                        .map(|(_, row)| row.source_ordinal)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, row)| row.id).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, row)| row.sequence).collect::<Vec<_>>(),
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
    source: &mut R,
    scratch: &mut S,
) -> Result<V35BuildScratchReceipt> {
    let mut receipt = V35BuildScratchReceipt {
        source_rows: 0,
        scratch_runs: 0,
        scratch_bytes: 0,
        peak_live_builder_bytes: 0,
    };
    let mut previous_source_ordinal = None;
    while let Some(block) = source.next_block()? {
        if previous_source_ordinal.is_some_and(|previous| {
            previous
                >= block
                    .rows
                    .first()
                    .expect("block is nonempty")
                    .source_ordinal
        }) {
            return Err(invalid("V35 build source block order differs"));
        }
        previous_source_ordinal = block.rows.last().map(|row| row.source_ordinal);
        let projection = project_build_block_live_bytes(&block, model)?;
        if projection.peak_live_bytes > MAX_BUILDER_BYTES {
            return Err(invalid("V35 build live memory exceeds admission"));
        }
        let bytes = encode_build_run(
            model,
            receipt.scratch_runs,
            &block,
            projection.encoded_capacity,
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
        receipt.peak_live_builder_bytes = receipt
            .peak_live_builder_bytes
            .max(projection.peak_live_bytes);
    }
    if receipt.source_rows == 0 {
        return Err(invalid("V35 build source is empty"));
    }
    Ok(receipt)
}

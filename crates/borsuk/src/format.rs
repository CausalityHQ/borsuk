use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch,
    StringArray, UInt8Array, UInt16Array, UInt64Array,
    types::{Float32Type, Int64Type, UInt8Type, UInt16Type, UInt64Type},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{
    error::{BorsukError, Result},
    index::IndexConfig,
    manifest::{
        Manifest, PivotSummary, RoutingLayerPageRef, SEGMENT_ID_BLOOM_BYTES,
        SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES, SegmentSummary,
    },
    metric::VectorMetric,
    record::{LeafMode, RecordId, VectorRecord},
    segment::{GraphEdge, Segment, SegmentGraph},
};

const CURRENT_MAGIC: &[u8; 4] = b"BORS";
const CURRENT_VERSION: u16 = 1;
const CURRENT_POINTER_VERSION_V1: u16 = 1;
const CURRENT_POINTER_VERSION_V2: u16 = 2;
const CURRENT_CHECKSUM_LEN: usize = 32;
const CURRENT_V1_LEN: usize = 4 + 2 + 8 + CURRENT_CHECKSUM_LEN;
const CURRENT_V2_LEN: usize = 4 + 2 + 8 + CURRENT_CHECKSUM_LEN * 4;
const BLAKE3_HEX_CHECKSUM_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentPointer {
    pub version: u64,
    pub metadata_checksum: [u8; CURRENT_CHECKSUM_LEN],
    pub manifest_checksum: Option<[u8; CURRENT_CHECKSUM_LEN]>,
    pub routing_checksum: Option<[u8; CURRENT_CHECKSUM_LEN]>,
    pub pivots_checksum: Option<[u8; CURRENT_CHECKSUM_LEN]>,
}

pub(crate) fn current_table_checksum(bytes: &[u8]) -> [u8; CURRENT_CHECKSUM_LEN] {
    *blake3::hash(bytes).as_bytes()
}

pub(crate) fn current_metadata_checksum(
    manifest_bytes: &[u8],
    routing_bytes: &[u8],
    pivots_bytes: &[u8],
) -> [u8; CURRENT_CHECKSUM_LEN] {
    let mut hasher = blake3::Hasher::new();
    update_current_hasher(&mut hasher, b"manifest", manifest_bytes);
    update_current_hasher(&mut hasher, b"routing", routing_bytes);
    update_current_hasher(&mut hasher, b"pivots", pivots_bytes);
    *hasher.finalize().as_bytes()
}

fn current_metadata_checksum_from_table_checksums(
    manifest_checksum: &[u8; CURRENT_CHECKSUM_LEN],
    routing_checksum: &[u8; CURRENT_CHECKSUM_LEN],
    pivots_checksum: &[u8; CURRENT_CHECKSUM_LEN],
) -> [u8; CURRENT_CHECKSUM_LEN] {
    let mut hasher = blake3::Hasher::new();
    update_current_hasher(&mut hasher, b"manifest_checksum", manifest_checksum);
    update_current_hasher(&mut hasher, b"routing_checksum", routing_checksum);
    update_current_hasher(&mut hasher, b"pivots_checksum", pivots_checksum);
    *hasher.finalize().as_bytes()
}

pub(crate) fn encode_current(
    version: u64,
    manifest_checksum: [u8; CURRENT_CHECKSUM_LEN],
    routing_checksum: [u8; CURRENT_CHECKSUM_LEN],
    pivots_checksum: [u8; CURRENT_CHECKSUM_LEN],
) -> Vec<u8> {
    let metadata_checksum = current_metadata_checksum_from_table_checksums(
        &manifest_checksum,
        &routing_checksum,
        &pivots_checksum,
    );
    let mut bytes = Vec::with_capacity(CURRENT_V2_LEN);
    bytes.extend_from_slice(CURRENT_MAGIC);
    bytes.extend_from_slice(&CURRENT_POINTER_VERSION_V2.to_le_bytes());
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&metadata_checksum);
    bytes.extend_from_slice(&manifest_checksum);
    bytes.extend_from_slice(&routing_checksum);
    bytes.extend_from_slice(&pivots_checksum);
    bytes
}

pub(crate) fn decode_current(bytes: &[u8]) -> Result<CurrentPointer> {
    if bytes.len() != CURRENT_V1_LEN && bytes.len() != CURRENT_V2_LEN {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT must be {CURRENT_V1_LEN} or {CURRENT_V2_LEN} bytes, got {}",
            bytes.len()
        )));
    }

    if &bytes[0..4] != CURRENT_MAGIC {
        return Err(BorsukError::InvalidStorage(
            "CURRENT magic header is invalid".to_string(),
        ));
    }

    let pointer_version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if pointer_version != CURRENT_POINTER_VERSION_V1
        && pointer_version != CURRENT_POINTER_VERSION_V2
    {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported CURRENT version {pointer_version}"
        )));
    }
    if pointer_version == CURRENT_POINTER_VERSION_V1 && bytes.len() != CURRENT_V1_LEN {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT v1 must be {CURRENT_V1_LEN} bytes, got {}",
            bytes.len()
        )));
    }
    if pointer_version == CURRENT_POINTER_VERSION_V2 && bytes.len() != CURRENT_V2_LEN {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT v2 must be {CURRENT_V2_LEN} bytes, got {}",
            bytes.len()
        )));
    }

    let version = u64::from_le_bytes([
        bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13],
    ]);
    let mut metadata_checksum = [0_u8; CURRENT_CHECKSUM_LEN];
    metadata_checksum.copy_from_slice(&bytes[14..46]);

    if pointer_version == CURRENT_POINTER_VERSION_V1 {
        return Ok(CurrentPointer {
            version,
            metadata_checksum,
            manifest_checksum: None,
            routing_checksum: None,
            pivots_checksum: None,
        });
    }

    let mut manifest_checksum = [0_u8; CURRENT_CHECKSUM_LEN];
    manifest_checksum.copy_from_slice(&bytes[46..78]);
    let mut routing_checksum = [0_u8; CURRENT_CHECKSUM_LEN];
    routing_checksum.copy_from_slice(&bytes[78..110]);
    let mut pivots_checksum = [0_u8; CURRENT_CHECKSUM_LEN];
    pivots_checksum.copy_from_slice(&bytes[110..142]);
    let actual_metadata_checksum = current_metadata_checksum_from_table_checksums(
        &manifest_checksum,
        &routing_checksum,
        &pivots_checksum,
    );
    if actual_metadata_checksum != metadata_checksum {
        return Err(BorsukError::InvalidStorage(
            "CURRENT metadata checksum mismatch across table checksums".to_string(),
        ));
    }

    Ok(CurrentPointer {
        version,
        metadata_checksum,
        manifest_checksum: Some(manifest_checksum),
        routing_checksum: Some(routing_checksum),
        pivots_checksum: Some(pivots_checksum),
    })
}

fn update_current_hasher(hasher: &mut blake3::Hasher, label: &[u8], bytes: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

pub(crate) fn manifest_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    validate_manifest_config(
        manifest.config.dimensions,
        manifest.config.segment_max_vectors,
    )?;
    let metric = manifest.config.metric.to_string();
    let schema = manifest_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values([CURRENT_VERSION])),
            array(UInt64Array::from_iter_values([manifest.version])),
            array(StringArray::from_iter_values([manifest
                .config
                .uri
                .as_str()])),
            array(StringArray::from_iter_values([metric.as_str()])),
            array(UInt64Array::from_iter_values([
                manifest.config.dimensions as u64
            ])),
            array(UInt64Array::from_iter_values([
                manifest.config.segment_max_vectors as u64,
            ])),
            array(Int64Array::from_iter_values([manifest
                .created_at
                .timestamp_millis()])),
            array(UInt64Array::from_iter([manifest.config.ram_budget_bytes])),
            array(UInt64Array::from_iter_values([manifest.next_generated_id])),
            array(UInt8Array::from_iter_values([manifest.routing_max_level])),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn manifest_from_parquet(
    manifest_bytes: &[u8],
    routing_bytes: &[u8],
) -> Result<Manifest> {
    let batch = first_batch(manifest_bytes, "manifest")?;
    if batch.num_rows() != 1 {
        return Err(BorsukError::InvalidStorage(format!(
            "manifest table must contain one row, got {}",
            batch.num_rows()
        )));
    }

    let format_version = primitive_value::<UInt16Type>(&batch, 0, 0, "format_version")?;
    if format_version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported manifest table version {format_version}"
        )));
    }

    let manifest_version = primitive_value::<UInt64Type>(&batch, 1, 0, "version")?;
    let metric = VectorMetric::from_str(string_value(&batch, 3, 0, "metric")?)?;
    let segments = routing_from_parquet(routing_bytes, manifest_version)?;
    let dimensions = usize_from_u64(primitive_value::<UInt64Type>(&batch, 4, 0, "dimensions")?)?;
    let segment_max_vectors = usize_from_u64(primitive_value::<UInt64Type>(
        &batch,
        5,
        0,
        "segment_max_vectors",
    )?)?;
    validate_manifest_config(dimensions, segment_max_vectors)?;
    let next_generated_id = if batch.num_columns() > 8 {
        primitive_value::<UInt64Type>(&batch, 8, 0, "next_generated_id")?
    } else {
        segments.iter().try_fold(0_u64, |total, segment| {
            let count = u64::try_from(segment.object_count).map_err(|_| {
                BorsukError::InvalidStorage(format!(
                    "segment `{}` object_count does not fit u64",
                    segment.id
                ))
            })?;
            total.checked_add(count).ok_or_else(|| {
                BorsukError::InvalidStorage("stored segment object counts exceed u64".to_string())
            })
        })?
    };
    let manifest = Manifest {
        version: manifest_version,
        config: IndexConfig {
            uri: string_value(&batch, 2, 0, "uri")?.to_string(),
            metric,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes: if batch.num_columns() > 7 {
                primitive_optional_value::<UInt64Type>(&batch, 7, 0, "ram_budget_bytes")?
            } else {
                None
            },
        },
        segments,
        pivots: Vec::new(),
        next_generated_id,
        routing_max_level: manifest_routing_max_level(&batch)?,
        created_at: datetime_from_millis(primitive_value::<Int64Type>(
            &batch,
            6,
            0,
            "created_at_ms",
        )?)?,
    };
    for segment in &manifest.segments {
        validate_routing_segment_dimensions(
            &segment.id,
            manifest.config.dimensions,
            segment.dimensions,
        )?;
    }

    Ok(manifest)
}

pub(crate) fn manifest_metadata_from_parquet(manifest_bytes: &[u8]) -> Result<Manifest> {
    let batch = first_batch(manifest_bytes, "manifest")?;
    if batch.num_rows() != 1 {
        return Err(BorsukError::InvalidStorage(format!(
            "manifest table must contain one row, got {}",
            batch.num_rows()
        )));
    }

    let format_version = primitive_value::<UInt16Type>(&batch, 0, 0, "format_version")?;
    if format_version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported manifest table version {format_version}"
        )));
    }

    let dimensions = usize_from_u64(primitive_value::<UInt64Type>(&batch, 4, 0, "dimensions")?)?;
    let segment_max_vectors = usize_from_u64(primitive_value::<UInt64Type>(
        &batch,
        5,
        0,
        "segment_max_vectors",
    )?)?;
    validate_manifest_config(dimensions, segment_max_vectors)?;

    Ok(Manifest {
        version: primitive_value::<UInt64Type>(&batch, 1, 0, "version")?,
        config: IndexConfig {
            uri: string_value(&batch, 2, 0, "uri")?.to_string(),
            metric: VectorMetric::from_str(string_value(&batch, 3, 0, "metric")?)?,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes: if batch.num_columns() > 7 {
                primitive_optional_value::<UInt64Type>(&batch, 7, 0, "ram_budget_bytes")?
            } else {
                None
            },
        },
        segments: Vec::new(),
        pivots: Vec::new(),
        next_generated_id: if batch.num_columns() > 8 {
            primitive_value::<UInt64Type>(&batch, 8, 0, "next_generated_id")?
        } else {
            0
        },
        routing_max_level: manifest_routing_max_level(&batch)?,
        created_at: datetime_from_millis(primitive_value::<Int64Type>(
            &batch,
            6,
            0,
            "created_at_ms",
        )?)?,
    })
}

pub(crate) fn manifest_has_next_generated_id(manifest_bytes: &[u8]) -> Result<bool> {
    let batch = first_batch(manifest_bytes, "manifest")?;
    Ok(batch.schema().field_with_name("next_generated_id").is_ok())
}

fn manifest_routing_max_level(batch: &RecordBatch) -> Result<u8> {
    let Ok(column_index) = batch.schema().index_of("routing_max_level") else {
        return Ok(0);
    };
    primitive_value::<UInt8Type>(batch, column_index, 0, "routing_max_level")
}

pub(crate) fn routing_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = routing_schema(dimensions);
    let segments = &manifest.segments;
    validate_routing_segment_ids(segments)?;
    validate_routing_segment_paths(segments)?;
    validate_routing_segment_summary_metadata(segments)?;
    for segment in segments {
        validate_routing_segment_dimensions(&segment.id, dimensions, segment.dimensions)?;
        validate_routing_centroid_dimensions(&segment.id, dimensions, segment.centroid.len())?;
        validate_routing_centroid_values(&segment.id, &segment.centroid)?;
        validate_routing_radius(&segment.id, segment.radius)?;
        validate_routing_bounds(
            &segment.id,
            dimensions,
            &segment.bounds_min,
            &segment.bounds_max,
        )?;
        validate_routing_id_bloom(&segment.id, &segment.id_bloom)?;
        validate_routing_vector_signature_bloom(&segment.id, &segment.vector_signature_bloom)?;
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                segments.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| manifest.version),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                segments.iter().map(|segment| segment.level),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.path.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.object_count as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dimensions as u64),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.centroid.as_slice()),
                dimensions,
            )),
            array(Float32Array::from_iter_values(
                segments.iter().map(|segment| segment.radius),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.size_bytes),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.graph_path.as_str()),
            )),
            array(StringArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.graph_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.graph_size_bytes),
            )),
            array(Int64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.created_at.timestamp_millis()),
            )),
            array(BinaryArray::from_iter_values(
                segments.iter().map(|segment| segment.id_bloom.as_slice()),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.leaf_mode.to_string()),
            )),
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.vector_signature_bloom.as_slice()),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_min.as_slice()),
                dimensions,
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_max.as_slice()),
                dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn routing_layer_page_to_parquet(
    manifest: &Manifest,
    routing_level: u8,
    page_ordinal: usize,
    segment_start_ordinal: usize,
    segments: &[SegmentSummary],
) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = routing_layer_page_schema(dimensions);
    validate_routing_segment_ids(segments)?;
    validate_routing_segment_paths(segments)?;
    validate_routing_segment_summary_metadata(segments)?;
    for segment in segments {
        validate_routing_segment_dimensions(&segment.id, dimensions, segment.dimensions)?;
        validate_routing_centroid_dimensions(&segment.id, dimensions, segment.centroid.len())?;
        validate_routing_centroid_values(&segment.id, &segment.centroid)?;
        validate_routing_radius(&segment.id, segment.radius)?;
        validate_routing_bounds(
            &segment.id,
            dimensions,
            &segment.bounds_min,
            &segment.bounds_max,
        )?;
        validate_routing_id_bloom(&segment.id, &segment.id_bloom)?;
        validate_routing_vector_signature_bloom(&segment.id, &segment.vector_signature_bloom)?;
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                segments.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(segments.iter().map(|_| 0))),
            array(UInt8Array::from_iter_values(
                segments.iter().map(|_| routing_level),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| page_ordinal as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| segments.len() as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments
                    .iter()
                    .enumerate()
                    .map(|(index, _)| (segment_start_ordinal + index) as u64),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                segments.iter().map(|segment| segment.level),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.object_count as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dimensions as u64),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.centroid.as_slice()),
                dimensions,
            )),
            array(Float32Array::from_iter_values(
                segments.iter().map(|segment| segment.radius),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.path.as_str()),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.size_bytes),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.graph_path.as_str()),
            )),
            array(StringArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.graph_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.graph_size_bytes),
            )),
            array(BinaryArray::from_iter_values(
                segments.iter().map(|segment| segment.id_bloom.as_slice()),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.leaf_mode.to_string()),
            )),
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.vector_signature_bloom.as_slice()),
            )),
            array(Int64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.created_at.timestamp_millis()),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_min.as_slice()),
                dimensions,
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_max.as_slice()),
                dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn routing_layer_page_index_to_parquet(
    manifest: &Manifest,
    routing_level: u8,
    page_refs: &[RoutingLayerPageRef],
) -> Result<Vec<u8>> {
    validate_routing_layer_page_refs(page_refs)?;

    let schema = routing_layer_page_index_schema(manifest.config.dimensions);
    for page_ref in page_refs {
        validate_routing_segment_dimensions(
            "routing-layer-page",
            manifest.config.dimensions,
            page_ref.dimensions,
        )?;
        validate_routing_bounds(
            "routing-layer-page",
            manifest.config.dimensions,
            &page_ref.bounds_min,
            &page_ref.bounds_max,
        )?;
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                page_refs.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|_| manifest.version),
            )),
            array(UInt8Array::from_iter_values(
                page_refs.iter().map(|_| routing_level),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_ordinal as u64),
            )),
            array(StringArray::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.path.as_str()),
            )),
            array(StringArray::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_segments as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.leaf_segments as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.dimensions as u64),
            )),
            array(fixed_f32_array(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.centroid.as_slice()),
                manifest.config.dimensions,
            )),
            array(Float32Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.radius),
            )),
            array(BinaryArray::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.id_bloom.as_slice()),
            )),
            array(BinaryArray::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.vector_signature_bloom.as_slice()),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.level_mask),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_records as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.page_segment_bytes),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.page_graph_bytes),
            )),
            array(fixed_f32_array(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.bounds_min.as_slice()),
                manifest.config.dimensions,
            )),
            array(fixed_f32_array(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.bounds_max.as_slice()),
                manifest.config.dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn routing_layer_page_index_from_parquet(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
) -> Result<Vec<RoutingLayerPageRef>> {
    routing_layer_page_index_from_parquet_with_version_policy(
        bytes,
        expected_manifest_version,
        expected_routing_level,
        false,
    )
}

pub(crate) fn routing_layer_page_index_from_parquet_relaxed_manifest_version(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
) -> Result<Vec<RoutingLayerPageRef>> {
    routing_layer_page_index_from_parquet_with_version_policy(
        bytes,
        expected_manifest_version,
        expected_routing_level,
        true,
    )
}

fn routing_layer_page_index_from_parquet_with_version_policy(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
    allow_manifest_version_mismatch: bool,
) -> Result<Vec<RoutingLayerPageRef>> {
    let mut page_refs = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing layer page index version {format_version}"
                )));
            }
            let manifest_version =
                primitive_value::<UInt64Type>(&batch, 1, row, "manifest_version")?;
            if !allow_manifest_version_mismatch && manifest_version != 0 {
                validate_table_manifest_version(
                    "routing layer page index",
                    expected_manifest_version,
                    manifest_version,
                )?;
            }
            validate_routing_layer_page_field(
                "routing_level",
                u64::from(expected_routing_level),
                u64::from(primitive_value::<UInt8Type>(
                    &batch,
                    2,
                    row,
                    "routing_level",
                )?),
            )?;
            let page_segments = usize_from_u64(primitive_value::<UInt64Type>(
                &batch,
                6,
                row,
                "page_segments",
            )?)?;
            if page_segments == 0 {
                return Err(BorsukError::InvalidStorage(
                    "routing layer page index must not reference empty pages".to_string(),
                ));
            }

            page_refs.push(RoutingLayerPageRef {
                routing_level: expected_routing_level,
                page_ordinal: usize_from_u64(primitive_value::<UInt64Type>(
                    &batch,
                    3,
                    row,
                    "page_ordinal",
                )?)?,
                path: string_value(&batch, 4, row, "page_path")?.to_string(),
                checksum: string_value(&batch, 5, row, "page_checksum")?.to_string(),
                page_segments,
                leaf_segments: routing_page_ref_leaf_segments(&batch, row, page_segments)?,
                dimensions: routing_page_ref_dimensions(&batch, row)?,
                centroid: routing_page_ref_centroid(&batch, row)?,
                radius: routing_page_ref_radius(&batch, row)?,
                bounds_min: routing_page_ref_bounds(&batch, row, "bounds_min")?,
                bounds_max: routing_page_ref_bounds(&batch, row, "bounds_max")?,
                id_bloom: routing_page_ref_id_bloom(&batch, row)?,
                vector_signature_bloom: routing_page_ref_vector_signature_bloom(&batch, row)?,
                level_mask: routing_page_ref_level_mask(&batch, row)?,
                page_records: routing_page_ref_page_records(&batch, row)?,
                page_segment_bytes: routing_page_ref_page_segment_bytes(&batch, row)?,
                page_graph_bytes: routing_page_ref_page_graph_bytes(&batch, row)?,
            });
        }
    }

    validate_routing_layer_page_refs(&page_refs)?;
    Ok(page_refs)
}

pub(crate) fn routing_layer_page_from_parquet(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
    expected_page_ordinal: usize,
    expected_dimensions: usize,
) -> Result<Vec<SegmentSummary>> {
    let mut summaries = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing layer page version {format_version}"
                )));
            }
            let page_manifest_version =
                primitive_value::<UInt64Type>(&batch, 1, row, "manifest_version")?;
            if page_manifest_version != 0 {
                validate_table_manifest_version(
                    "routing layer page",
                    expected_manifest_version,
                    page_manifest_version,
                )?;
            }
            validate_routing_layer_page_field(
                "routing_level",
                u64::from(expected_routing_level),
                u64::from(primitive_value::<UInt8Type>(
                    &batch,
                    2,
                    row,
                    "routing_level",
                )?),
            )?;
            validate_routing_layer_page_field(
                "page_ordinal",
                expected_page_ordinal as u64,
                primitive_value::<UInt64Type>(&batch, 3, row, "page_ordinal")?,
            )?;
            let page_segments = primitive_value::<UInt64Type>(&batch, 4, row, "page_segments")?;
            if page_segments == 0 {
                return Err(BorsukError::InvalidStorage(
                    "routing layer page must declare at least one segment".to_string(),
                ));
            }

            let id = string_value(&batch, 6, row, "segment_id")?.to_string();
            let dimensions =
                usize_from_u64(primitive_value::<UInt64Type>(&batch, 9, row, "dimensions")?)?;
            validate_routing_segment_dimensions(&id, expected_dimensions, dimensions)?;
            let centroid = fixed_f32_value(&batch, 10, row, "centroid")?;
            validate_routing_centroid_dimensions(&id, dimensions, centroid.len())?;
            validate_routing_centroid_values(&id, &centroid)?;
            let radius = primitive_value::<Float32Type>(&batch, 11, row, "radius")?;
            validate_routing_radius(&id, radius)?;
            let bounds_min = routing_bounds(&batch, row, "bounds_min", &id)?;
            let bounds_max = routing_bounds(&batch, row, "bounds_max", &id)?;
            let id_bloom = binary_value(&batch, 18, row, "id_bloom")?.to_vec();
            validate_routing_id_bloom(&id, &id_bloom)?;
            let vector_signature_bloom =
                binary_value(&batch, 20, row, "vector_signature_bloom")?.to_vec();
            validate_routing_vector_signature_bloom(&id, &vector_signature_bloom)?;
            let leaf_mode = routing_leaf_mode_at_column(&batch, row, 19)?;

            summaries.push(SegmentSummary {
                id,
                level: primitive_value::<UInt8Type>(&batch, 7, row, "segment_level")?,
                path: string_value(&batch, 12, row, "segment_path")?.to_string(),
                object_count: usize_from_u64(primitive_value::<UInt64Type>(
                    &batch,
                    8,
                    row,
                    "object_count",
                )?)?,
                dimensions,
                centroid,
                radius,
                bounds_min,
                bounds_max,
                checksum: string_value(&batch, 13, row, "segment_checksum")?.to_string(),
                size_bytes: primitive_value::<UInt64Type>(&batch, 14, row, "segment_size_bytes")?,
                graph_path: string_value(&batch, 15, row, "graph_path")?.to_string(),
                graph_checksum: string_value(&batch, 16, row, "graph_checksum")?.to_string(),
                graph_size_bytes: primitive_value::<UInt64Type>(
                    &batch,
                    17,
                    row,
                    "graph_size_bytes",
                )?,
                leaf_mode,
                id_bloom,
                vector_signature_bloom,
                created_at: datetime_from_millis(primitive_value::<Int64Type>(
                    &batch,
                    21,
                    row,
                    "created_at_ms",
                )?)?,
            });
        }
    }

    validate_routing_segment_ids(&summaries)?;
    validate_routing_segment_paths(&summaries)?;
    validate_routing_segment_summary_metadata(&summaries)?;

    Ok(summaries)
}

pub(crate) fn pivots_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = pivots_schema(dimensions);
    let pivots = &manifest.pivots;
    validate_pivot_ids(pivots)?;
    for pivot in pivots {
        validate_pivot_vector_dimensions(&pivot.id, dimensions, pivot.vector.len())?;
        validate_pivot_vector_values(&pivot.id, &pivot.vector)?;
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                pivots.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                pivots.iter().map(|_| manifest.version),
            )),
            array(UInt64Array::from_iter_values(
                pivots.iter().map(|pivot| pivot.ordinal as u64),
            )),
            array(StringArray::from_iter_values(
                pivots.iter().map(|pivot| pivot.id.as_str()),
            )),
            array(fixed_f32_array(
                pivots.iter().map(|pivot| pivot.vector.as_slice()),
                dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn pivots_from_parquet(
    bytes: &[u8],
    dimensions: usize,
    expected_manifest_version: u64,
) -> Result<Vec<PivotSummary>> {
    let mut pivots = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported pivot table version {format_version}"
                )));
            }

            validate_table_manifest_version(
                "pivot table",
                expected_manifest_version,
                primitive_value::<UInt64Type>(&batch, 1, row, "manifest_version")?,
            )?;
            let ordinal =
                usize_from_u64(primitive_value::<UInt64Type>(&batch, 2, row, "ordinal")?)?;
            let id = string_value(&batch, 3, row, "pivot_id")?.to_string();
            let vector = fixed_f32_value(&batch, 4, row, "vector")?;
            validate_pivot_vector_dimensions(&id, dimensions, vector.len())?;
            validate_pivot_vector_values(&id, &vector)?;

            pivots.push(PivotSummary {
                id,
                ordinal,
                vector,
            });
        }
    }

    validate_pivot_ids(&pivots)?;

    Ok(pivots)
}

/// Encode vector records as a compact Parquet table.
pub fn vector_records_to_parquet(records: &[VectorRecord], dimensions: usize) -> Result<Vec<u8>> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidRecordInput(
            "vector record dimensions must be greater than zero".to_string(),
        ));
    }
    validate_vector_record_ids(records)?;
    for record in records {
        if record.vector.len() != dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions,
                actual: record.vector.len(),
            });
        }
        validate_vector_record_values(&record.id, &record.vector)?;
    }

    let schema = vector_records_schema(dimensions);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                records.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                records.iter().map(|_| dimensions as u64),
            )),
            array(BinaryArray::from_iter_values(
                records.iter().map(|record| record.id.as_bytes()),
            )),
            array(fixed_f32_array(
                records.iter().map(|record| record.vector.as_slice()),
                dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

/// Decode vector records from a Parquet table and validate their fixed width.
pub fn vector_records_from_parquet(
    bytes: &[u8],
    expected_dimensions: usize,
) -> Result<Vec<VectorRecord>> {
    if expected_dimensions == 0 {
        return Err(BorsukError::InvalidRecordInput(
            "expected dimensions must be greater than zero".to_string(),
        ));
    }

    let mut records = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported vector records table version {format_version}"
                )));
            }

            let dimensions =
                usize_from_u64(primitive_value::<UInt64Type>(&batch, 1, row, "dimensions")?)?;
            if dimensions != expected_dimensions {
                return Err(BorsukError::DimensionMismatch {
                    expected: expected_dimensions,
                    actual: dimensions,
                });
            }

            let vector = fixed_f32_value(&batch, 3, row, "vector")?;
            if vector.len() != expected_dimensions {
                return Err(BorsukError::DimensionMismatch {
                    expected: expected_dimensions,
                    actual: vector.len(),
                });
            }
            let id = record_id_value(&batch, 2, row, "record_id")?;
            validate_vector_record_values(&id, &vector)?;

            records.push(VectorRecord { id, vector });
        }
    }

    validate_vector_record_ids(&records)?;

    Ok(records)
}

fn validate_manifest_config(dimensions: usize, segment_max_vectors: usize) -> Result<()> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidStorage(
            "manifest dimensions must be greater than zero".to_string(),
        ));
    }
    if segment_max_vectors == 0 {
        return Err(BorsukError::InvalidStorage(
            "manifest segment_max_vectors must be greater than zero".to_string(),
        ));
    }

    Ok(())
}

fn validate_table_manifest_version(table: &str, expected: u64, actual: u64) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "{table} manifest_version {actual} does not match manifest version {expected}"
        )));
    }

    Ok(())
}

fn validate_vector_record_ids(records: &[VectorRecord]) -> Result<()> {
    let mut ids = HashSet::with_capacity(records.len());
    for record in records {
        if record.id.is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "record ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(record.id.as_bytes()) {
            return Err(BorsukError::InvalidRecordInput(format!(
                "duplicate record id `{}` in vector records table",
                record.id
            )));
        }
    }

    Ok(())
}

fn validate_vector_record_values(record_id: &RecordId, vector: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = vector
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(BorsukError::InvalidRecordInput(format!(
            "vector records must contain only finite f32 values; record `{record_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_pivot_vector_values(pivot_id: &str, vector: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = non_finite_coordinate(vector) {
        return Err(BorsukError::InvalidStorage(format!(
            "pivot vectors must contain only finite f32 values; pivot `{pivot_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_pivot_vector_dimensions(pivot_id: &str, expected: usize, actual: usize) -> Result<()> {
    validate_stored_vector_dimensions("pivot vector", pivot_id, expected, actual)
}

fn validate_pivot_ids(pivots: &[PivotSummary]) -> Result<()> {
    let mut ids = HashSet::with_capacity(pivots.len());
    for pivot in pivots {
        if pivot.id.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "pivot ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(pivot.id.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate pivot id `{}`",
                pivot.id
            )));
        }
    }

    Ok(())
}

fn validate_routing_segment_dimensions(
    segment_id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "routing segment `{segment_id}` declares {actual} dimensions, expected {expected}"
        )));
    }

    Ok(())
}

fn validate_routing_segment_ids(segments: &[SegmentSummary]) -> Result<()> {
    let mut ids = HashSet::with_capacity(segments.len());
    for segment in segments {
        if segment.id.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "routing segment ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(segment.id.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing segment id `{}`",
                segment.id
            )));
        }
    }

    Ok(())
}

fn validate_routing_segment_paths(segments: &[SegmentSummary]) -> Result<()> {
    let mut segment_paths = HashSet::with_capacity(segments.len());
    let mut graph_paths = HashSet::with_capacity(segments.len());
    for segment in segments {
        if segment.path.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "routing segment paths must not be empty".to_string(),
            ));
        }
        if !segment_paths.insert(segment.path.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing segment path `{}`",
                segment.path
            )));
        }

        if segment.graph_path.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "routing graph paths must not be empty".to_string(),
            ));
        }
        if !graph_paths.insert(segment.graph_path.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing graph path `{}`",
                segment.graph_path
            )));
        }
    }

    Ok(())
}

fn validate_routing_segment_summary_metadata(segments: &[SegmentSummary]) -> Result<()> {
    for segment in segments {
        if segment.object_count == 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment object_count must be greater than zero; segment `{}`",
                segment.id
            )));
        }
        validate_routing_checksum("routing segment checksum", &segment.id, &segment.checksum)?;
        if segment.size_bytes == 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment size_bytes must be greater than zero; segment `{}`",
                segment.id
            )));
        }
        validate_routing_checksum(
            "routing graph checksum",
            &segment.id,
            &segment.graph_checksum,
        )?;
        if segment.graph_size_bytes == 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing graph size_bytes must be greater than zero; segment `{}`",
                segment.id
            )));
        }
        validate_routing_vector_signature_bloom(&segment.id, &segment.vector_signature_bloom)?;
        validate_routing_bounds(
            &segment.id,
            segment.dimensions,
            &segment.bounds_min,
            &segment.bounds_max,
        )?;
    }

    Ok(())
}

fn validate_routing_checksum(field: &str, segment_id: &str, checksum: &str) -> Result<()> {
    if is_blake3_hex_checksum(checksum) {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "{field} must be {BLAKE3_HEX_CHECKSUM_LEN} lowercase hex characters; segment `{segment_id}`"
    )))
}

fn validate_hex_checksum(field: &str, checksum: &str) -> Result<()> {
    if is_blake3_hex_checksum(checksum) {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "{field} checksum must be {BLAKE3_HEX_CHECKSUM_LEN} lowercase hex characters"
    )))
}

fn is_blake3_hex_checksum(checksum: &str) -> bool {
    checksum.len() == BLAKE3_HEX_CHECKSUM_LEN
        && checksum
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn validate_routing_centroid_dimensions(
    segment_id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    validate_stored_vector_dimensions("routing centroid", segment_id, expected, actual)
}

fn validate_routing_centroid_values(segment_id: &str, centroid: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = non_finite_coordinate(centroid) {
        return Err(BorsukError::InvalidStorage(format!(
            "routing centroids must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_routing_radius(segment_id: &str, radius: f32) -> Result<()> {
    if !radius.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "routing radii must contain only finite f32 values; segment `{segment_id}` was {radius}"
        )));
    }

    Ok(())
}

fn validate_routing_bounds(
    segment_id: &str,
    dimensions: usize,
    bounds_min: &[f32],
    bounds_max: &[f32],
) -> Result<()> {
    if bounds_min.is_empty() && bounds_max.is_empty() {
        return Ok(());
    }
    validate_stored_vector_dimensions(
        "routing bounds_min",
        segment_id,
        dimensions,
        bounds_min.len(),
    )?;
    validate_stored_vector_dimensions(
        "routing bounds_max",
        segment_id,
        dimensions,
        bounds_max.len(),
    )?;
    for (coordinate_index, (min, max)) in bounds_min.iter().zip(bounds_max).enumerate() {
        if !min.is_finite() {
            return Err(BorsukError::InvalidStorage(format!(
                "routing bounds_min must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {min}"
            )));
        }
        if !max.is_finite() {
            return Err(BorsukError::InvalidStorage(format!(
                "routing bounds_max must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {max}"
            )));
        }
        if min > max {
            return Err(BorsukError::InvalidStorage(format!(
                "routing bounds must satisfy min <= max; segment `{segment_id}` coordinate {coordinate_index} had {min} > {max}"
            )));
        }
    }

    Ok(())
}

fn validate_routing_id_bloom(segment_id: &str, id_bloom: &[u8]) -> Result<()> {
    if id_bloom.is_empty() || id_bloom.len() == SEGMENT_ID_BLOOM_BYTES {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "routing segment `{segment_id}` id_bloom must be {SEGMENT_ID_BLOOM_BYTES} bytes when present, got {}",
        id_bloom.len()
    )))
}

fn validate_routing_vector_signature_bloom(
    segment_id: &str,
    vector_signature_bloom: &[u8],
) -> Result<()> {
    if vector_signature_bloom.is_empty()
        || vector_signature_bloom.len() == SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES
    {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "routing segment `{segment_id}` vector_signature_bloom must be {SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES} bytes when present, got {}",
        vector_signature_bloom.len()
    )))
}

fn routing_leaf_mode(batch: &RecordBatch, row: usize) -> Result<LeafMode> {
    let Ok(column_index) = batch.schema().index_of("leaf_mode") else {
        return Ok(LeafMode::Graph);
    };
    routing_leaf_mode_at_column(batch, row, column_index)
}

fn routing_leaf_mode_at_column(
    batch: &RecordBatch,
    row: usize,
    column_index: usize,
) -> Result<LeafMode> {
    let value = string_value(batch, column_index, row, "leaf_mode")?;
    value.parse::<LeafMode>().map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "routing leaf_mode `{value}` is not a supported leaf mode"
        ))
    })
}

fn validate_routing_layer_page_field(field: &str, expected: u64, actual: u64) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "routing layer page {field} {actual} does not match expected {expected}"
    )))
}

fn validate_routing_layer_page_refs(page_refs: &[RoutingLayerPageRef]) -> Result<()> {
    let mut seen_ordinals = HashSet::with_capacity(page_refs.len());
    for page_ref in page_refs {
        if !seen_ordinals.insert(page_ref.page_ordinal) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing layer page ordinal {}",
                page_ref.page_ordinal
            )));
        }
        if page_ref.path.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index contains an empty page path".to_string(),
            ));
        }
        if !page_ref.path.starts_with("routing/pages/") {
            return Err(BorsukError::InvalidStorage(format!(
                "routing layer page `{}` is outside routing/pages",
                page_ref.path
            )));
        }
        validate_hex_checksum("routing layer page", &page_ref.checksum)?;
        if page_ref.page_segments == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index must not reference empty pages".to_string(),
            ));
        }
        if page_ref.leaf_segments == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index must not reference empty leaf ranges".to_string(),
            ));
        }
        if !page_ref.id_bloom.is_empty() {
            validate_routing_id_bloom("routing-layer-page", &page_ref.id_bloom)?;
        }
        if !page_ref.vector_signature_bloom.is_empty() {
            validate_routing_vector_signature_bloom(
                "routing-layer-page",
                &page_ref.vector_signature_bloom,
            )?;
        }
        if page_ref.level_mask == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index level_mask must not be zero".to_string(),
            ));
        }
        if page_ref.dimensions == 0 && page_ref.centroid.is_empty() && page_ref.radius.is_infinite()
        {
            continue;
        }
        if page_ref.dimensions == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index dimensions must be greater than zero".to_string(),
            ));
        }
        validate_routing_centroid_dimensions(
            "routing-layer-page",
            page_ref.dimensions,
            page_ref.centroid.len(),
        )?;
        validate_routing_centroid_values("routing-layer-page", &page_ref.centroid)?;
        validate_routing_radius("routing-layer-page", page_ref.radius)?;
        validate_routing_bounds(
            "routing-layer-page",
            page_ref.dimensions,
            &page_ref.bounds_min,
            &page_ref.bounds_max,
        )?;
    }

    Ok(())
}

fn routing_page_ref_leaf_segments(
    batch: &RecordBatch,
    row: usize,
    page_segments: usize,
) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("leaf_segments") else {
        return Ok(page_segments);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "leaf_segments",
    )?)
}

fn routing_page_ref_dimensions(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("dimensions") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "dimensions",
    )?)
}

fn routing_page_ref_centroid(batch: &RecordBatch, row: usize) -> Result<Vec<f32>> {
    let Ok(column_index) = batch.schema().index_of("centroid") else {
        return Ok(Vec::new());
    };
    fixed_f32_value(batch, column_index, row, "centroid")
}

fn routing_page_ref_radius(batch: &RecordBatch, row: usize) -> Result<f32> {
    let Ok(column_index) = batch.schema().index_of("radius") else {
        return Ok(f32::INFINITY);
    };
    primitive_value::<Float32Type>(batch, column_index, row, "radius")
}

fn routing_page_ref_id_bloom(batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
    let Ok(column_index) = batch.schema().index_of("id_bloom") else {
        return Ok(Vec::new());
    };
    Ok(binary_value(batch, column_index, row, "id_bloom")?.to_vec())
}

fn routing_page_ref_bounds(batch: &RecordBatch, row: usize, column_name: &str) -> Result<Vec<f32>> {
    let Ok(column_index) = batch.schema().index_of(column_name) else {
        return Ok(Vec::new());
    };
    fixed_f32_value(batch, column_index, row, column_name)
}

fn routing_page_ref_vector_signature_bloom(batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
    let Ok(column_index) = batch.schema().index_of("vector_signature_bloom") else {
        return Ok(Vec::new());
    };
    let bloom = binary_value(batch, column_index, row, "vector_signature_bloom")?.to_vec();
    validate_routing_vector_signature_bloom("routing-layer-page", &bloom)?;
    Ok(bloom)
}

fn routing_page_ref_level_mask(batch: &RecordBatch, row: usize) -> Result<u64> {
    let Ok(column_index) = batch.schema().index_of("level_mask") else {
        return Ok(u64::MAX);
    };
    primitive_value::<UInt64Type>(batch, column_index, row, "level_mask")
}

fn routing_page_ref_page_records(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("page_records") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "page_records",
    )?)
}

fn routing_page_ref_page_segment_bytes(batch: &RecordBatch, row: usize) -> Result<u64> {
    let Ok(column_index) = batch.schema().index_of("page_segment_bytes") else {
        return Ok(0);
    };
    primitive_value::<UInt64Type>(batch, column_index, row, "page_segment_bytes")
}

fn routing_page_ref_page_graph_bytes(batch: &RecordBatch, row: usize) -> Result<u64> {
    let Ok(column_index) = batch.schema().index_of("page_graph_bytes") else {
        return Ok(0);
    };
    primitive_value::<UInt64Type>(batch, column_index, row, "page_graph_bytes")
}

fn routing_vector_signature_bloom(
    batch: &RecordBatch,
    row: usize,
    segment_id: &str,
) -> Result<Vec<u8>> {
    let Ok(column_index) = batch.schema().index_of("vector_signature_bloom") else {
        return Ok(Vec::new());
    };
    let bloom = binary_value(batch, column_index, row, "vector_signature_bloom")?.to_vec();
    validate_routing_vector_signature_bloom(segment_id, &bloom)?;
    Ok(bloom)
}

fn routing_bounds(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    segment_id: &str,
) -> Result<Vec<f32>> {
    let Ok(column_index) = batch.schema().index_of(column_name) else {
        return Ok(Vec::new());
    };
    let bounds = fixed_f32_value(batch, column_index, row, column_name)?;
    if let Some((coordinate_index, value)) = non_finite_coordinate(&bounds) {
        return Err(BorsukError::InvalidStorage(format!(
            "routing {column_name} must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {value}"
        )));
    }
    Ok(bounds)
}

pub(crate) fn routing_from_parquet(
    bytes: &[u8],
    expected_manifest_version: u64,
) -> Result<Vec<SegmentSummary>> {
    let mut summaries = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing table version {format_version}"
                )));
            }
            validate_table_manifest_version(
                "routing table",
                expected_manifest_version,
                primitive_value::<UInt64Type>(&batch, 1, row, "manifest_version")?,
            )?;

            let id = string_value(&batch, 2, row, "id")?.to_string();
            let centroid = fixed_f32_value(&batch, 7, row, "centroid")?;
            let radius = primitive_value::<Float32Type>(&batch, 8, row, "radius")?;
            let dimensions =
                usize_from_u64(primitive_value::<UInt64Type>(&batch, 6, row, "dimensions")?)?;
            validate_routing_centroid_dimensions(&id, dimensions, centroid.len())?;
            validate_routing_centroid_values(&id, &centroid)?;
            validate_routing_radius(&id, radius)?;
            let id_bloom = if batch.num_columns() > 15 {
                let id_bloom = binary_value(&batch, 15, row, "id_bloom")?.to_vec();
                validate_routing_id_bloom(&id, &id_bloom)?;
                id_bloom
            } else {
                Vec::new()
            };
            let leaf_mode = routing_leaf_mode(&batch, row)?;
            let vector_signature_bloom = routing_vector_signature_bloom(&batch, row, &id)?;
            let bounds_min = routing_bounds(&batch, row, "bounds_min", &id)?;
            let bounds_max = routing_bounds(&batch, row, "bounds_max", &id)?;

            summaries.push(SegmentSummary {
                id,
                level: primitive_value::<UInt8Type>(&batch, 3, row, "level")?,
                path: string_value(&batch, 4, row, "path")?.to_string(),
                object_count: usize_from_u64(primitive_value::<UInt64Type>(
                    &batch,
                    5,
                    row,
                    "object_count",
                )?)?,
                dimensions,
                centroid,
                radius,
                bounds_min,
                bounds_max,
                checksum: string_value(&batch, 9, row, "checksum")?.to_string(),
                size_bytes: primitive_value::<UInt64Type>(&batch, 10, row, "size_bytes")?,
                graph_path: string_value(&batch, 11, row, "graph_path")?.to_string(),
                graph_checksum: string_value(&batch, 12, row, "graph_checksum")?.to_string(),
                graph_size_bytes: primitive_value::<UInt64Type>(
                    &batch,
                    13,
                    row,
                    "graph_size_bytes",
                )?,
                leaf_mode,
                id_bloom,
                vector_signature_bloom,
                created_at: datetime_from_millis(primitive_value::<Int64Type>(
                    &batch,
                    14,
                    row,
                    "created_at_ms",
                )?)?,
            });
        }
    }

    validate_routing_segment_ids(&summaries)?;
    validate_routing_segment_paths(&summaries)?;
    validate_routing_segment_summary_metadata(&summaries)?;

    Ok(summaries)
}

pub(crate) fn segment_to_parquet(segment: &Segment) -> Result<Vec<u8>> {
    validate_segment_centroid_dimensions(&segment.id, segment.dimensions, segment.centroid.len())?;
    validate_segment_centroid_values(&segment.id, &segment.centroid)?;
    validate_segment_radius(&segment.id, segment.radius)?;
    validate_segment_routing_code_count(
        &segment.id,
        segment.records.len(),
        segment.routing_codes.len(),
    )?;
    validate_segment_pq_code_count(&segment.id, segment.records.len(), segment.pq_codes.len())?;
    validate_segment_record_ids(&segment.records)?;
    for ((record, routing_code), pq_code) in segment
        .records
        .iter()
        .zip(&segment.routing_codes)
        .zip(&segment.pq_codes)
    {
        validate_segment_record_dimensions(&record.id, segment.dimensions, record.vector.len())?;
        validate_segment_routing_code(&record.id, *routing_code)?;
        validate_segment_pq_code_dimensions(&record.id, segment.dimensions, pq_code.len())?;
        validate_segment_record_vector_values(&record.id, &record.vector)?;
    }

    let metric = segment.metric.to_string();
    let schema = segment_schema(segment.dimensions);
    let records = &segment.records;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                records.iter().map(|_| CURRENT_VERSION),
            )),
            array(StringArray::from_iter_values(
                records.iter().map(|_| segment.id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                records.iter().map(|_| segment.level),
            )),
            array(StringArray::from_iter_values(
                records.iter().map(|_| metric.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                records.iter().map(|_| segment.dimensions as u64),
            )),
            array(fixed_f32_array(
                records.iter().map(|_| segment.centroid.as_slice()),
                segment.dimensions,
            )),
            array(Float32Array::from_iter_values(
                records.iter().map(|_| segment.radius),
            )),
            array(Int64Array::from_iter_values(
                records
                    .iter()
                    .map(|_| segment.created_at.timestamp_millis()),
            )),
            array(Float32Array::from_iter_values(
                segment.routing_codes.iter().copied(),
            )),
            array(fixed_u8_array(
                segment.pq_codes.iter().map(Vec::as_slice),
                segment.dimensions,
            )),
            array(BinaryArray::from_iter_values(
                records.iter().map(|record| record.id.as_bytes()),
            )),
            array(fixed_f32_array(
                records.iter().map(|record| record.vector.as_slice()),
                segment.dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn segment_from_parquet(bytes: &[u8]) -> Result<Segment> {
    let mut records = Vec::new();
    let mut routing_codes = Vec::new();
    let mut pq_codes = Vec::new();
    let mut metadata = None::<SegmentMetadata>;

    for batch in read_batches(bytes)? {
        let routing_code_column = batch.schema().index_of("routing_code").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `routing_code` column".to_string())
        })?;
        let pq_code_column = batch.schema().index_of("pq_code").ok();
        let record_id_column = batch.schema().index_of("record_id").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `record_id` column".to_string())
        })?;
        let vector_column = batch.schema().index_of("vector").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `vector` column".to_string())
        })?;
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported segment table version {format_version}"
                )));
            }

            let row_metadata = SegmentMetadata {
                id: string_value(&batch, 1, row, "segment_id")?.to_string(),
                level: primitive_value::<UInt8Type>(&batch, 2, row, "level")?,
                metric: VectorMetric::from_str(string_value(&batch, 3, row, "metric")?)?,
                dimensions: usize_from_u64(primitive_value::<UInt64Type>(
                    &batch,
                    4,
                    row,
                    "dimensions",
                )?)?,
                centroid: fixed_f32_value(&batch, 5, row, "centroid")?,
                radius: primitive_value::<Float32Type>(&batch, 6, row, "radius")?,
                created_at: datetime_from_millis(primitive_value::<Int64Type>(
                    &batch,
                    7,
                    row,
                    "created_at_ms",
                )?)?,
            };
            let row_dimensions = row_metadata.dimensions;
            validate_segment_centroid_dimensions(
                &row_metadata.id,
                row_dimensions,
                row_metadata.centroid.len(),
            )?;
            validate_segment_centroid_values(&row_metadata.id, &row_metadata.centroid)?;
            validate_segment_radius(&row_metadata.id, row_metadata.radius)?;

            if let Some(metadata) = &metadata {
                if metadata != &row_metadata {
                    return Err(BorsukError::InvalidStorage(
                        "segment metadata differs between rows".to_string(),
                    ));
                }
            } else {
                metadata = Some(row_metadata);
            }

            let id = record_id_value(&batch, record_id_column, row, "record_id")?;
            let routing_code =
                primitive_value::<Float32Type>(&batch, routing_code_column, row, "routing_code")?;
            validate_segment_routing_code(&id, routing_code)?;
            if let Some(pq_code_column) = pq_code_column {
                let pq_code = fixed_u8_value(&batch, pq_code_column, row, "pq_code")?;
                validate_segment_pq_code_dimensions(&id, row_dimensions, pq_code.len())?;
                pq_codes.push(pq_code);
            }
            let vector = fixed_f32_value(&batch, vector_column, row, "vector")?;
            validate_segment_record_dimensions(&id, row_dimensions, vector.len())?;
            validate_segment_record_vector_values(&id, &vector)?;
            records.push(VectorRecord { id, vector });
            routing_codes.push(routing_code);
        }
    }

    let metadata = metadata.ok_or_else(|| {
        BorsukError::InvalidStorage("segment table must contain at least one row".to_string())
    })?;
    validate_segment_record_ids(&records)?;
    if pq_codes.is_empty() {
        pq_codes = crate::segment::pq_codes_for_records(&records, metadata.dimensions)?;
    }
    validate_segment_pq_code_count(&metadata.id, records.len(), pq_codes.len())?;

    Ok(Segment {
        id: metadata.id,
        level: metadata.level,
        metric: metadata.metric,
        dimensions: metadata.dimensions,
        centroid: metadata.centroid,
        radius: metadata.radius,
        records,
        routing_codes,
        pq_codes,
        created_at: metadata.created_at,
    })
}

pub(crate) fn graph_to_parquet(graph: &SegmentGraph) -> Result<Vec<u8>> {
    for edge in &graph.edges {
        validate_graph_edge_distance(
            edge.source_record_index,
            edge.neighbor_record_index,
            edge.distance,
        )?;
    }

    let schema = graph_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                graph.edges.iter().map(|_| CURRENT_VERSION),
            )),
            array(StringArray::from_iter_values(
                graph.edges.iter().map(|_| graph.segment_id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                graph.edges.iter().map(|_| graph.level),
            )),
            array(Int64Array::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|_| graph.created_at.timestamp_millis()),
            )),
            array(UInt64Array::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|edge| edge.source_record_index as u64),
            )),
            array(UInt64Array::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|edge| edge.neighbor_record_index as u64),
            )),
            array(Float32Array::from_iter_values(
                graph.edges.iter().map(|edge| edge.distance),
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn graph_from_parquet(
    bytes: &[u8],
    expected_segment_id: &str,
    expected_level: u8,
    records: &[VectorRecord],
) -> Result<SegmentGraph> {
    let mut edges = Vec::new();
    let mut metadata = None::<GraphMetadata>;
    let record_index_by_id = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.id.as_bytes(), index))
        .collect::<HashMap<_, _>>();

    for batch in read_batches(bytes)? {
        let source_record_index_column = batch.schema().index_of("source_record_index").ok();
        let neighbor_record_index_column = batch.schema().index_of("neighbor_record_index").ok();
        let source_record_id_column = batch.schema().index_of("source_record_id").ok();
        let neighbor_record_id_column = batch.schema().index_of("neighbor_record_id").ok();
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported graph table version {format_version}"
                )));
            }

            let row_metadata = GraphMetadata {
                segment_id: string_value(&batch, 1, row, "segment_id")?.to_string(),
                level: primitive_value::<UInt8Type>(&batch, 2, row, "level")?,
                created_at: datetime_from_millis(primitive_value::<Int64Type>(
                    &batch,
                    3,
                    row,
                    "created_at_ms",
                )?)?,
            };

            if row_metadata.segment_id != expected_segment_id {
                return Err(BorsukError::InvalidStorage(format!(
                    "graph table segment id `{}` does not match expected segment `{expected_segment_id}`",
                    row_metadata.segment_id
                )));
            }
            if row_metadata.level != expected_level {
                return Err(BorsukError::InvalidStorage(format!(
                    "graph table level {} does not match expected level {expected_level}",
                    row_metadata.level
                )));
            }

            if let Some(metadata) = &metadata {
                if metadata != &row_metadata {
                    return Err(BorsukError::InvalidStorage(
                        "graph metadata differs between rows".to_string(),
                    ));
                }
            } else {
                metadata = Some(row_metadata);
            }

            let (source_record_index, neighbor_record_index) = match (
                source_record_index_column,
                neighbor_record_index_column,
                source_record_id_column,
                neighbor_record_id_column,
            ) {
                (Some(source_column), Some(neighbor_column), _, _) => {
                    let source_record_index = usize_from_u64(primitive_value::<UInt64Type>(
                        &batch,
                        source_column,
                        row,
                        "source_record_index",
                    )?)?;
                    let neighbor_record_index = usize_from_u64(primitive_value::<UInt64Type>(
                        &batch,
                        neighbor_column,
                        row,
                        "neighbor_record_index",
                    )?)?;
                    validate_graph_record_index(
                        expected_segment_id,
                        "source",
                        source_record_index,
                        records.len(),
                    )?;
                    validate_graph_record_index(
                        expected_segment_id,
                        "neighbor",
                        neighbor_record_index,
                        records.len(),
                    )?;
                    (source_record_index, neighbor_record_index)
                }
                (_, _, Some(source_column), Some(neighbor_column)) => {
                    let source_record_id =
                        string_value(&batch, source_column, row, "source_record_id")?;
                    let neighbor_record_id =
                        string_value(&batch, neighbor_column, row, "neighbor_record_id")?;
                    (
                        graph_record_index_from_id(
                            expected_segment_id,
                            "source",
                            source_record_id,
                            &record_index_by_id,
                        )?,
                        graph_record_index_from_id(
                            expected_segment_id,
                            "neighbor",
                            neighbor_record_id,
                            &record_index_by_id,
                        )?,
                    )
                }
                _ => {
                    return Err(BorsukError::InvalidStorage(
                        "graph table missing record reference columns".to_string(),
                    ));
                }
            };
            let distance = primitive_value::<Float32Type>(&batch, 6, row, "neighbor_distance")?;
            validate_graph_edge_distance(source_record_index, neighbor_record_index, distance)?;

            edges.push(GraphEdge {
                source_record_index,
                neighbor_record_index,
                distance,
            });
        }
    }

    let metadata = match metadata {
        Some(metadata) => metadata,
        None => GraphMetadata {
            segment_id: expected_segment_id.to_string(),
            level: expected_level,
            created_at: datetime_from_millis(0)?,
        },
    };

    Ok(SegmentGraph {
        segment_id: metadata.segment_id,
        level: metadata.level,
        edges,
        created_at: metadata.created_at,
    })
}

fn manifest_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("version", DataType::UInt64, false),
        Field::new("uri", DataType::Utf8, false),
        Field::new("metric", DataType::Utf8, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new("segment_max_vectors", DataType::UInt64, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("ram_budget_bytes", DataType::UInt64, true),
        Field::new("next_generated_id", DataType::UInt64, false),
        Field::new("routing_max_level", DataType::UInt8, false),
    ]))
}

fn routing_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("object_count", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        fixed_f32_field("centroid", dimensions),
        Field::new("radius", DataType::Float32, false),
        Field::new("checksum", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("graph_path", DataType::Utf8, false),
        Field::new("graph_checksum", DataType::Utf8, false),
        Field::new("graph_size_bytes", DataType::UInt64, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("leaf_mode", DataType::Utf8, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
        fixed_f32_field("bounds_min", dimensions),
        fixed_f32_field("bounds_max", dimensions),
    ]))
}

fn routing_layer_page_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("routing_level", DataType::UInt8, false),
        Field::new("page_ordinal", DataType::UInt64, false),
        Field::new("page_segments", DataType::UInt64, false),
        Field::new("segment_ordinal", DataType::UInt64, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("segment_level", DataType::UInt8, false),
        Field::new("object_count", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        fixed_f32_field("centroid", dimensions),
        Field::new("radius", DataType::Float32, false),
        Field::new("segment_path", DataType::Utf8, false),
        Field::new("segment_checksum", DataType::Utf8, false),
        Field::new("segment_size_bytes", DataType::UInt64, false),
        Field::new("graph_path", DataType::Utf8, false),
        Field::new("graph_checksum", DataType::Utf8, false),
        Field::new("graph_size_bytes", DataType::UInt64, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("leaf_mode", DataType::Utf8, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
        Field::new("created_at_ms", DataType::Int64, false),
        fixed_f32_field("bounds_min", dimensions),
        fixed_f32_field("bounds_max", dimensions),
    ]))
}

fn routing_layer_page_index_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("routing_level", DataType::UInt8, false),
        Field::new("page_ordinal", DataType::UInt64, false),
        Field::new("page_path", DataType::Utf8, false),
        Field::new("page_checksum", DataType::Utf8, false),
        Field::new("page_segments", DataType::UInt64, false),
        Field::new("leaf_segments", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions as i32,
            ),
            false,
        ),
        Field::new("radius", DataType::Float32, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
        Field::new("level_mask", DataType::UInt64, false),
        Field::new("page_records", DataType::UInt64, false),
        Field::new("page_segment_bytes", DataType::UInt64, false),
        Field::new("page_graph_bytes", DataType::UInt64, false),
        fixed_f32_field("bounds_min", dimensions),
        fixed_f32_field("bounds_max", dimensions),
    ]))
}

fn pivots_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("pivot_id", DataType::Utf8, false),
        fixed_f32_field("vector", dimensions),
    ]))
}

fn segment_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("metric", DataType::Utf8, false),
        Field::new("dimensions", DataType::UInt64, false),
        fixed_f32_field("centroid", dimensions),
        Field::new("radius", DataType::Float32, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("routing_code", DataType::Float32, false),
        fixed_u8_field("pq_code", dimensions),
        Field::new("record_id", DataType::Binary, false),
        fixed_f32_field("vector", dimensions),
    ]))
}

fn vector_records_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new("record_id", DataType::Binary, false),
        fixed_f32_field("vector", dimensions),
    ]))
}

fn graph_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("source_record_index", DataType::UInt64, false),
        Field::new("neighbor_record_index", DataType::UInt64, false),
        Field::new("neighbor_distance", DataType::Float32, false),
    ]))
}

fn fixed_f32_field(name: &str, dimensions: usize) -> Field {
    Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new_list_field(DataType::Float32, true)),
            dimensions as i32,
        ),
        false,
    )
}

fn fixed_u8_field(name: &str, dimensions: usize) -> Field {
    Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new_list_field(DataType::UInt8, true)),
            dimensions as i32,
        ),
        false,
    )
}

fn fixed_f32_array<'a>(
    values: impl IntoIterator<Item = &'a [f32]>,
    dimensions: usize,
) -> FixedSizeListArray {
    let values = values
        .into_iter()
        .map(|vector| Some(vector.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(values, dimensions as i32)
}

fn fixed_u8_array<'a>(
    values: impl IntoIterator<Item = &'a [u8]>,
    dimensions: usize,
) -> FixedSizeListArray {
    let values = values
        .into_iter()
        .map(|code| Some(code.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(values, dimensions as i32)
}

fn write_batch(batch: RecordBatch) -> Result<Vec<u8>> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

fn read_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?.build()?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn first_batch(bytes: &[u8], name: &str) -> Result<RecordBatch> {
    read_batches(bytes)?
        .into_iter()
        .next()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("{name} table must contain one row")))
}

fn array(value: impl Array + 'static) -> ArrayRef {
    Arc::new(value) as ArrayRef
}

fn primitive_value<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<T::Native>
where
    T: arrow_array::ArrowPrimitiveType,
{
    batch
        .column(column)
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
}

fn primitive_optional_value<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<Option<T::Native>>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    if array.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(array.value(row)))
    }
}

fn string_value<'a>(
    batch: &'a RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<&'a str> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
}

fn record_id_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<RecordId> {
    if let Some(array) = batch.column(column).as_any().downcast_ref::<BinaryArray>() {
        return Ok(RecordId::from_bytes(array.value(row).to_vec()));
    }

    if let Some(array) = batch.column(column).as_any().downcast_ref::<StringArray>() {
        return Ok(RecordId::from(array.value(row)));
    }

    Err(BorsukError::InvalidStorage(format!(
        "column `{name}` has wrong type"
    )))
}

fn binary_value<'a>(
    batch: &'a RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<&'a [u8]> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
}

fn fixed_f32_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<Vec<f32>> {
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    Ok((0..values.len()).map(|index| values.value(index)).collect())
}

fn fixed_u8_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<Vec<u8>> {
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    Ok((0..values.len()).map(|index| values.value(index)).collect())
}

fn usize_from_u64(value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| {
        BorsukError::InvalidStorage(format!("stored value {value} does not fit usize"))
    })
}

fn datetime_from_millis(value: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value).ok_or_else(|| {
        BorsukError::InvalidStorage(format!("stored timestamp {value} is out of range"))
    })
}

fn validate_segment_record_vector_values(record_id: &RecordId, vector: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = vector
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record vectors must contain only finite f32 values; record `{record_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_segment_record_ids(records: &[VectorRecord]) -> Result<()> {
    let mut ids = HashSet::with_capacity(records.len());
    for record in records {
        if record.id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "record ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(record.id.as_bytes()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate record id `{}` in segment table",
                record.id
            )));
        }
    }

    Ok(())
}

fn validate_segment_centroid_dimensions(
    segment_id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    validate_stored_vector_dimensions("segment centroid", segment_id, expected, actual)
}

fn validate_segment_centroid_values(segment_id: &str, centroid: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = non_finite_coordinate(centroid) {
        return Err(BorsukError::InvalidStorage(format!(
            "segment centroids must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_segment_radius(segment_id: &str, radius: f32) -> Result<()> {
    if !radius.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment radii must contain only finite f32 values; segment `{segment_id}` was {radius}"
        )));
    }

    Ok(())
}

fn validate_segment_record_dimensions(
    record_id: &RecordId,
    expected: usize,
    actual: usize,
) -> Result<()> {
    validate_stored_vector_dimensions(
        "segment record vector",
        &record_id.to_string(),
        expected,
        actual,
    )
}

fn validate_segment_routing_code(record_id: &RecordId, routing_code: f32) -> Result<()> {
    if !routing_code.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment routing codes must contain only finite f32 values; record `{record_id}` was {routing_code}"
        )));
    }

    Ok(())
}

fn validate_segment_routing_code_count(
    segment_id: &str,
    record_count: usize,
    routing_code_count: usize,
) -> Result<()> {
    if routing_code_count != record_count {
        return Err(BorsukError::InvalidStorage(format!(
            "segment `{segment_id}` routing code count {routing_code_count} must match record count {record_count}"
        )));
    }

    Ok(())
}

fn validate_segment_pq_code_count(
    segment_id: &str,
    record_count: usize,
    pq_code_count: usize,
) -> Result<()> {
    if pq_code_count != record_count {
        return Err(BorsukError::InvalidStorage(format!(
            "segment `{segment_id}` pq code count {pq_code_count} must match record count {record_count}"
        )));
    }

    Ok(())
}

fn validate_segment_pq_code_dimensions(
    record_id: &RecordId,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "segment PQ codes must match vector dimensions; record `{record_id}` had {actual}, expected {expected}"
        )));
    }

    Ok(())
}

fn validate_graph_edge_distance(
    source_record_index: usize,
    neighbor_record_index: usize,
    distance: f32,
) -> Result<()> {
    if !distance.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment graph distances must contain only finite f32 values; edge {source_record_index} -> {neighbor_record_index} was {distance}"
        )));
    }

    Ok(())
}

fn validate_graph_record_index(
    segment_id: &str,
    role: &str,
    record_index: usize,
    record_count: usize,
) -> Result<()> {
    if record_index < record_count {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph table segment `{segment_id}` {role} record index {record_index} is outside record count {record_count}"
    )))
}

fn graph_record_index_from_id(
    segment_id: &str,
    role: &str,
    record_id: &str,
    record_index_by_id: &HashMap<&[u8], usize>,
) -> Result<usize> {
    record_index_by_id
        .get(record_id.as_bytes())
        .copied()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "graph edge references missing segment record in legacy graph table segment `{segment_id}`: {role} record id `{record_id}`"
            ))
        })
}

fn validate_stored_vector_dimensions(
    field: &str,
    id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "{field} `{id}` has {actual} dimensions, expected {expected}"
        )));
    }

    Ok(())
}

fn non_finite_coordinate(vector: &[f32]) -> Option<(usize, f32)> {
    vector
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
}

#[derive(Debug, PartialEq)]
struct SegmentMetadata {
    id: String,
    level: u8,
    metric: VectorMetric,
    dimensions: usize,
    centroid: Vec<f32>,
    radius: f32,
    created_at: DateTime<Utc>,
}

#[derive(Debug, PartialEq)]
struct GraphMetadata {
    segment_id: String,
    level: u8,
    created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_SEGMENT_CHECKSUM: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const VALID_GRAPH_CHECKSUM: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn segment_from_parquet_rejects_non_finite_record_vectors() {
        let bytes = external_segment_parquet([f32::NAN, 0.0], [0.0, 0.0], 0.0, 0.0);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn graph_from_parquet_rejects_non_finite_edge_distances() {
        let bytes = external_graph_parquet(f32::NAN);

        let records = vec![
            VectorRecord::new("source", vec![0.0, 0.0]),
            VectorRecord::new("neighbor", vec![1.0, 0.0]),
        ];
        let err = graph_from_parquet(&bytes, "seg", 0, &records).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_non_finite_centroids() {
        let bytes = external_routing_parquet([f32::NAN, 0.0], 1.0);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_non_finite_radii() {
        let bytes = external_routing_parquet([0.0, 0.0], f32::INFINITY);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn pivots_from_parquet_rejects_non_finite_vectors() {
        let bytes = external_pivots_parquet([f32::NAN, 0.0]);

        let err = pivots_from_parquet(&bytes, 2, 1).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn pivots_from_parquet_rejects_empty_pivot_ids() {
        let bytes = external_pivots_parquet_with_ids([""]);

        let err = pivots_from_parquet(&bytes, 2, 1).unwrap_err();

        assert!(
            err.to_string().contains("pivot ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn pivots_from_parquet_rejects_duplicate_pivot_ids() {
        let bytes = external_pivots_parquet_with_ids(["pivot", "pivot"]);

        let err = pivots_from_parquet(&bytes, 2, 1).unwrap_err();

        assert!(err.to_string().contains("duplicate pivot id"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_non_finite_centroids() {
        let bytes = external_segment_parquet([0.0, 0.0], [f32::NAN, 0.0], 0.0, 0.0);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_non_finite_radii() {
        let bytes = external_segment_parquet([0.0, 0.0], [0.0, 0.0], f32::INFINITY, 0.0);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_non_finite_routing_codes() {
        let bytes = external_segment_parquet([0.0, 0.0], [0.0, 0.0], 0.0, f32::NAN);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_round_trips_pq_codes() {
        let mut segment = valid_segment();
        segment.pq_codes = vec![vec![7, 249]];

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();

        assert!(batch.schema().field_with_name("pq_code").is_ok());
        assert_eq!(
            segment_from_parquet(&bytes).unwrap().pq_codes,
            segment.pq_codes
        );
    }

    #[test]
    fn segment_to_parquet_writes_binary_record_ids() {
        let segment = valid_segment();

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();

        assert_eq!(
            batch
                .schema()
                .field_with_name("record_id")
                .unwrap()
                .data_type(),
            &DataType::Binary
        );
    }

    #[test]
    fn segment_parquet_round_trips_non_utf8_record_ids() {
        let mut segment = valid_segment();
        segment.records[0] = VectorRecord::new_bytes(vec![0, 159, 255, 7], vec![0.25, -0.75]);

        let bytes = segment_to_parquet(&segment).unwrap();

        let decoded = segment_from_parquet(&bytes).unwrap();
        assert_eq!(decoded.records[0], segment.records[0]);
        assert_eq!(decoded.routing_codes[0], segment.routing_codes[0]);
        assert_eq!(decoded.pq_codes[0], segment.pq_codes[0]);
    }

    #[test]
    fn segment_from_parquet_fills_legacy_missing_pq_codes() {
        let bytes = external_segment_parquet([0.25, -0.75], [0.0, 0.0], 1.0, 1.0);

        let segment = segment_from_parquet(&bytes).unwrap();

        assert_eq!(segment.pq_codes.len(), 1);
        assert_eq!(segment.pq_codes[0].len(), 2);
    }

    #[test]
    fn routing_from_parquet_rejects_centroids_with_wrong_dimensions() {
        let bytes = external_routing_parquet_with_dimensions([0.0, 0.0], 1.0, 3);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("dimensions"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_id_bloom() {
        let bytes = external_routing_parquet_with_id_bloom(vec![0_u8; 3]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("id_bloom"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_vector_signature_bloom() {
        let bytes = external_routing_parquet_with_vector_signature_bloom(vec![0_u8; 3]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("vector_signature_bloom"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_unknown_leaf_mode() {
        let bytes = external_routing_parquet_with_leaf_mode("unknown-leaf");

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("routing leaf_mode"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_empty_segment_ids() {
        let bytes = external_routing_parquet_with_segment_ids([""]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_duplicate_segment_ids() {
        let bytes = external_routing_parquet_with_segment_ids(["seg", "seg"]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment id"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_empty_segment_paths() {
        let bytes = external_routing_parquet_with_paths([""], ["segments/seg.graph.parquet"]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment paths must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_duplicate_segment_paths() {
        let bytes = external_routing_parquet_with_paths(
            ["segments/seg.parquet", "segments/seg.parquet"],
            ["segments/a.graph.parquet", "segments/b.graph.parquet"],
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment path"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_empty_graph_paths() {
        let bytes = external_routing_parquet_with_paths(["segments/seg.parquet"], [""]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph paths must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_duplicate_graph_paths() {
        let bytes = external_routing_parquet_with_paths(
            ["segments/a.parquet", "segments/b.parquet"],
            ["segments/seg.graph.parquet", "segments/seg.graph.parquet"],
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing graph path"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_segment_checksums() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            "not-a-blake3-checksum",
            123,
            VALID_GRAPH_CHECKSUM,
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_graph_checksums() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            VALID_SEGMENT_CHECKSUM,
            123,
            "not-a-blake3-checksum",
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_empty_segment_summaries() {
        let bytes = external_routing_parquet_with_summary_metadata(
            0,
            VALID_SEGMENT_CHECKSUM,
            123,
            VALID_GRAPH_CHECKSUM,
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment object_count must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_zero_segment_sizes() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            VALID_SEGMENT_CHECKSUM,
            0,
            VALID_GRAPH_CHECKSUM,
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_zero_graph_sizes() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            VALID_SEGMENT_CHECKSUM,
            123,
            VALID_GRAPH_CHECKSUM,
            0,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn segment_from_parquet_rejects_centroids_with_wrong_dimensions() {
        let bytes =
            external_segment_parquet_with_dimensions(vec![0.0, 0.0], vec![0.0, 0.0], 0.0, 0.0, 3);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("dimensions"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_record_vectors_with_wrong_dimensions() {
        let bytes = external_segment_parquet_with_dimensions(
            vec![0.0, 0.0],
            vec![0.0, 0.0, 0.0],
            0.0,
            0.0,
            3,
        );

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("dimensions"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_empty_record_ids() {
        let bytes = external_segment_parquet_with_records([("", [0.0, 0.0])]);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(
            err.to_string().contains("record ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn segment_from_parquet_rejects_duplicate_record_ids() {
        let bytes =
            external_segment_parquet_with_records([("dup", [0.0, 0.0]), ("dup", [1.0, 0.0])]);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("duplicate record id"), "{err}");
    }

    #[test]
    fn manifest_from_parquet_rejects_segment_dimension_mismatch() {
        let manifest_bytes = manifest_to_parquet(&valid_manifest()).unwrap();
        let routing_bytes = external_routing_parquet_with_vector(vec![0.0, 0.0, 0.0], 1.0, 3);

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(err.to_string().contains("dimensions"), "{err}");
    }

    #[test]
    fn manifest_from_parquet_rejects_routing_manifest_version_mismatch() {
        let manifest_bytes = manifest_to_parquet(&valid_manifest()).unwrap();
        let routing_bytes = external_routing_parquet_with_manifest_version(2);

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(
            err.to_string().contains("routing table manifest_version"),
            "{err}"
        );
    }

    #[test]
    fn manifest_from_parquet_rejects_invalid_config_dimensions() {
        let manifest_bytes = external_manifest_parquet(0, 100);
        let routing_bytes = routing_to_parquet(&valid_manifest()).unwrap();

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest dimensions must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn manifest_from_parquet_rejects_invalid_segment_max_vectors() {
        let manifest_bytes = external_manifest_parquet(2, 0);
        let routing_bytes = routing_to_parquet(&valid_manifest()).unwrap();

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest segment_max_vectors must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn manifest_to_parquet_rejects_invalid_config_dimensions() {
        let mut manifest = valid_manifest();
        manifest.config.dimensions = 0;

        let err = manifest_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest dimensions must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn manifest_to_parquet_rejects_invalid_segment_max_vectors() {
        let mut manifest = valid_manifest();
        manifest.config.segment_max_vectors = 0;

        let err = manifest_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest segment_max_vectors must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn pivots_to_parquet_rejects_non_finite_vectors() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: "pivot".to_string(),
            ordinal: 0,
            vector: vec![f32::NAN, 0.0],
        }];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn pivots_to_parquet_rejects_vectors_with_wrong_dimensions() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: "pivot".to_string(),
            ordinal: 0,
            vector: vec![0.0],
        }];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn pivots_to_parquet_rejects_empty_pivot_ids() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: String::new(),
            ordinal: 0,
            vector: vec![0.0, 0.0],
        }];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("pivot ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn pivots_to_parquet_rejects_duplicate_pivot_ids() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![
            PivotSummary {
                id: "pivot".to_string(),
                ordinal: 0,
                vector: vec![0.0, 0.0],
            },
            PivotSummary {
                id: "pivot".to_string(),
                ordinal: 1,
                vector: vec![1.0, 0.0],
            },
        ];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("duplicate pivot id"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_non_finite_centroids() {
        let mut segment = valid_segment_summary();
        segment.centroid = vec![f32::NAN, 0.0];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_non_finite_radii() {
        let mut segment = valid_segment_summary();
        segment.radius = f32::INFINITY;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_centroids_with_wrong_dimensions() {
        let mut segment = valid_segment_summary();
        segment.centroid = vec![0.0];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_segment_dimension_mismatch() {
        let mut segment = valid_segment_summary();
        segment.dimensions = 3;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_id_bloom() {
        let mut segment = valid_segment_summary();
        segment.id_bloom = vec![0_u8; 3];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("id_bloom"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_vector_signature_bloom() {
        let mut segment = valid_segment_summary();
        segment.vector_signature_bloom = vec![0_u8; 3];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("vector_signature_bloom"), "{err}");
    }

    #[test]
    fn routing_to_parquet_round_trips_leaf_mode() {
        let mut segment = valid_segment_summary();
        segment.leaf_mode = LeafMode::VamanaPq;
        let manifest = manifest_with_segment(segment);

        let bytes = routing_to_parquet(&manifest).unwrap();
        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries[0].leaf_mode, LeafMode::VamanaPq);
    }

    #[test]
    fn routing_to_parquet_round_trips_vector_signature_bloom() {
        let segment = valid_segment_summary();
        let expected = segment.vector_signature_bloom.clone();
        let manifest = manifest_with_segment(segment);

        let bytes = routing_to_parquet(&manifest).unwrap();
        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries[0].vector_signature_bloom, expected);
    }

    #[test]
    fn routing_to_parquet_round_trips_vector_bounds() {
        let mut segment = valid_segment_summary();
        segment.bounds_min = vec![-1.0, -2.0];
        segment.bounds_max = vec![3.0, 4.0];
        let expected_min = segment.bounds_min.clone();
        let expected_max = segment.bounds_max.clone();
        let manifest = manifest_with_segment(segment);

        let bytes = routing_to_parquet(&manifest).unwrap();
        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries[0].bounds_min, expected_min);
        assert_eq!(summaries[0].bounds_max, expected_max);
    }

    #[test]
    fn routing_to_parquet_rejects_invalid_vector_bounds() {
        let mut segment = valid_segment_summary();
        segment.bounds_min = vec![1.0, 0.0];
        segment.bounds_max = vec![0.0, 0.0];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("min <= max"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_empty_segment_ids() {
        let mut segment = valid_segment_summary();
        segment.id.clear();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_duplicate_segment_ids() {
        let mut manifest = valid_manifest();
        manifest.segments = vec![valid_segment_summary(), valid_segment_summary()];

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment id"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_empty_segment_paths() {
        let mut segment = valid_segment_summary();
        segment.path.clear();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment paths must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_duplicate_segment_paths() {
        let mut first = valid_segment_summary();
        let mut second = valid_segment_summary();
        second.id = "seg-b".to_string();
        second.path = first.path.clone();
        second.graph_path = "graphs/L0/seg-b.parquet".to_string();
        first.graph_path = "graphs/L0/seg-a.parquet".to_string();
        let mut manifest = valid_manifest();
        manifest.segments = vec![first, second];

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment path"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_empty_graph_paths() {
        let mut segment = valid_segment_summary();
        segment.graph_path.clear();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph paths must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_duplicate_graph_paths() {
        let first = valid_segment_summary();
        let mut second = valid_segment_summary();
        second.id = "seg-b".to_string();
        second.path = "segments/L0/seg-b.parquet".to_string();
        second.graph_path = first.graph_path.clone();
        let mut manifest = valid_manifest();
        manifest.segments = vec![first, second];

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing graph path"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_segment_checksums() {
        let mut segment = valid_segment_summary();
        segment.checksum = "not-a-blake3-checksum".to_string();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_graph_checksums() {
        let mut segment = valid_segment_summary();
        segment.graph_checksum = "not-a-blake3-checksum".to_string();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_empty_segment_summaries() {
        let mut segment = valid_segment_summary();
        segment.object_count = 0;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment object_count must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_zero_segment_sizes() {
        let mut segment = valid_segment_summary();
        segment.size_bytes = 0;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_zero_graph_sizes() {
        let mut segment = valid_segment_summary();
        segment.graph_size_bytes = 0;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_record_vectors() {
        let mut segment = valid_segment();
        segment.records[0].vector = vec![f32::NAN, 0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_centroids_with_wrong_dimensions() {
        let mut segment = valid_segment();
        segment.centroid = vec![0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_record_vectors_with_wrong_dimensions() {
        let mut segment = valid_segment();
        segment.records[0].vector = vec![0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_empty_record_ids() {
        let mut segment = valid_segment();
        segment.records[0].id.clear();

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(
            err.to_string().contains("record ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn segment_to_parquet_rejects_duplicate_record_ids() {
        let mut segment = valid_segment();
        segment.records.push(VectorRecord {
            id: "record".into(),
            vector: vec![1.0, 0.0],
        });
        segment.routing_codes.push(1.0);
        segment.pq_codes.push(vec![255, 128]);

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("duplicate record id"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_routing_code_count_mismatch() {
        let mut segment = valid_segment();
        segment.routing_codes.push(1.0);

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("routing code count"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_centroids() {
        let mut segment = valid_segment();
        segment.centroid = vec![f32::NAN, 0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_radii() {
        let mut segment = valid_segment();
        segment.radius = f32::INFINITY;

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_routing_codes() {
        let mut segment = valid_segment();
        segment.routing_codes[0] = f32::NAN;

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn graph_to_parquet_rejects_non_finite_edge_distances() {
        let graph = SegmentGraph {
            segment_id: "seg".to_string(),
            level: 0,
            edges: vec![GraphEdge {
                source_record_index: 0,
                neighbor_record_index: 1,
                distance: f32::NAN,
            }],
            created_at: Utc::now(),
        };

        let err = graph_to_parquet(&graph).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn graph_to_parquet_writes_numeric_record_indices() {
        let segment = Segment::from_records(
            "seg".to_string(),
            0,
            VectorMetric::Euclidean,
            2,
            vec![
                VectorRecord::new("long-user-id-a", vec![0.0, 0.0]),
                VectorRecord::new("long-user-id-b", vec![1.0, 0.0]),
            ],
        )
        .unwrap();
        let graph = SegmentGraph::from_segment(&segment, 1).unwrap();

        let bytes = graph_to_parquet(&graph).unwrap();
        let batch = first_batch(&bytes, "graph").unwrap();
        let schema = batch.schema();

        assert_eq!(
            schema
                .field_with_name("source_record_index")
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            schema
                .field_with_name("neighbor_record_index")
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
        assert!(
            schema.field_with_name("source_record_id").is_err(),
            "new graph blocks must not repeat external ids per edge"
        );
        assert!(
            schema.field_with_name("neighbor_record_id").is_err(),
            "new graph blocks must not repeat external ids per edge"
        );
    }

    fn valid_manifest() -> Manifest {
        Manifest {
            version: 1,
            config: IndexConfig {
                uri: "file:///tmp/borsuk-test".to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 100,
                ram_budget_bytes: None,
            },
            segments: Vec::new(),
            pivots: Vec::new(),
            next_generated_id: 0,
            routing_max_level: 0,
            created_at: Utc::now(),
        }
    }

    fn manifest_with_segment(segment: SegmentSummary) -> Manifest {
        let mut manifest = valid_manifest();
        manifest.segments = vec![segment];
        manifest
    }

    fn valid_segment_summary() -> SegmentSummary {
        SegmentSummary {
            id: "seg".to_string(),
            level: 0,
            path: "segments/L0/seg.parquet".to_string(),
            object_count: 1,
            dimensions: 2,
            centroid: vec![0.0, 0.0],
            radius: 0.0,
            bounds_min: vec![0.0, 0.0],
            bounds_max: vec![0.0, 0.0],
            checksum: VALID_SEGMENT_CHECKSUM.to_string(),
            size_bytes: 123,
            graph_path: "graphs/L0/seg.parquet".to_string(),
            graph_checksum: VALID_GRAPH_CHECKSUM.to_string(),
            graph_size_bytes: 45,
            leaf_mode: LeafMode::Graph,
            id_bloom: crate::manifest::segment_id_bloom(["record"]),
            vector_signature_bloom: valid_vector_signature_bloom(),
            created_at: Utc::now(),
        }
    }

    fn valid_vector_signature_bloom() -> Vec<u8> {
        let vector = [0.0_f32, 0.0_f32];
        crate::manifest::segment_vector_signature_bloom([vector.as_slice()])
    }

    fn valid_segment() -> Segment {
        Segment {
            id: "seg".to_string(),
            level: 0,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            centroid: vec![0.0, 0.0],
            radius: 0.0,
            records: vec![VectorRecord {
                id: "record".into(),
                vector: vec![0.0, 0.0],
            }],
            routing_codes: vec![0.0],
            pq_codes: vec![vec![128, 128]],
            created_at: Utc::now(),
        }
    }

    fn external_manifest_parquet(dimensions: u64, segment_max_vectors: u64) -> Vec<u8> {
        let schema = manifest_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(UInt64Array::from_iter_values([1])),
                array(StringArray::from_iter_values(["file:///tmp/borsuk-test"])),
                array(StringArray::from_iter_values(["euclidean"])),
                array(UInt64Array::from_iter_values([dimensions])),
                array(UInt64Array::from_iter_values([segment_max_vectors])),
                array(Int64Array::from_iter_values([0])),
                array(UInt64Array::from_iter([None::<u64>])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt8Array::from_iter_values([0])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_routing_parquet(centroid: [f32; 2], radius: f32) -> Vec<u8> {
        external_routing_parquet_with_dimensions(centroid, radius, 2)
    }

    fn external_routing_parquet_with_dimensions(
        centroid: [f32; 2],
        radius: f32,
        stored_dimensions: u64,
    ) -> Vec<u8> {
        external_routing_parquet_with_vector(centroid.to_vec(), radius, stored_dimensions)
    }

    fn external_routing_parquet_with_vector(
        centroid: Vec<f32>,
        radius: f32,
        stored_dimensions: u64,
    ) -> Vec<u8> {
        external_routing_parquet_with_vector_and_id_bloom(
            centroid,
            radius,
            stored_dimensions,
            crate::manifest::segment_id_bloom(["record"]),
        )
    }

    fn external_routing_parquet_with_id_bloom(id_bloom: Vec<u8>) -> Vec<u8> {
        external_routing_parquet_with_vector_and_id_bloom(vec![0.0, 0.0], 0.0, 2, id_bloom)
    }

    fn external_routing_parquet_with_vector_signature_bloom(
        vector_signature_bloom: Vec<u8>,
    ) -> Vec<u8> {
        let mut metadata = valid_external_routing_summary_metadata();
        metadata.vector_signature_bloom = &vector_signature_bloom;
        external_routing_parquet_with_rows_and_summary_metadata(
            &["seg"],
            &["segments/seg.parquet"],
            &["segments/seg.graph.parquet"],
            &[metadata],
        )
    }

    fn external_routing_parquet_with_leaf_mode(leaf_mode: &str) -> Vec<u8> {
        let mut metadata = valid_external_routing_summary_metadata();
        metadata.leaf_mode = leaf_mode;
        external_routing_parquet_with_rows_and_summary_metadata(
            &["seg"],
            &["segments/seg.parquet"],
            &["segments/seg.graph.parquet"],
            &[metadata],
        )
    }

    fn external_routing_parquet_with_segment_ids<const N: usize>(ids: [&str; N]) -> Vec<u8> {
        let paths = vec!["segments/seg.parquet"; N];
        let graph_paths = vec!["segments/seg.graph.parquet"; N];
        external_routing_parquet_with_rows(&ids, &paths, &graph_paths)
    }

    fn external_routing_parquet_with_paths<const N: usize>(
        paths: [&str; N],
        graph_paths: [&str; N],
    ) -> Vec<u8> {
        let ids = (0..N)
            .map(|index| format!("seg-{index}"))
            .collect::<Vec<_>>();
        let ids = ids.iter().map(String::as_str).collect::<Vec<_>>();
        external_routing_parquet_with_rows(&ids, &paths, &graph_paths)
    }

    fn external_routing_parquet_with_rows(
        ids: &[&str],
        paths: &[&str],
        graph_paths: &[&str],
    ) -> Vec<u8> {
        external_routing_parquet_with_rows_and_summary_metadata(
            ids,
            paths,
            graph_paths,
            &vec![valid_external_routing_summary_metadata(); ids.len()],
        )
    }

    fn external_routing_parquet_with_summary_metadata(
        object_count: u64,
        checksum: &str,
        size_bytes: u64,
        graph_checksum: &str,
        graph_size_bytes: u64,
    ) -> Vec<u8> {
        let mut row = valid_external_routing_summary_metadata();
        row.object_count = object_count;
        row.checksum = checksum;
        row.size_bytes = size_bytes;
        row.graph_checksum = graph_checksum;
        row.graph_size_bytes = graph_size_bytes;
        let metadata = [row];
        external_routing_parquet_with_rows_and_summary_metadata(
            &["seg"],
            &["segments/seg.parquet"],
            &["segments/seg.graph.parquet"],
            &metadata,
        )
    }

    #[derive(Clone, Copy)]
    struct ExternalRoutingSummaryMetadata<'a> {
        object_count: u64,
        checksum: &'a str,
        size_bytes: u64,
        graph_checksum: &'a str,
        graph_size_bytes: u64,
        leaf_mode: &'a str,
        vector_signature_bloom: &'a [u8],
    }

    fn valid_external_routing_summary_metadata() -> ExternalRoutingSummaryMetadata<'static> {
        static VECTOR_SIGNATURE_BLOOM: [u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES] =
            [0_u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
        ExternalRoutingSummaryMetadata {
            object_count: 1,
            checksum: VALID_SEGMENT_CHECKSUM,
            size_bytes: 123,
            graph_checksum: VALID_GRAPH_CHECKSUM,
            graph_size_bytes: 45,
            leaf_mode: "graph",
            vector_signature_bloom: &VECTOR_SIGNATURE_BLOOM,
        }
    }

    fn external_routing_parquet_with_rows_and_summary_metadata(
        ids: &[&str],
        paths: &[&str],
        graph_paths: &[&str],
        metadata: &[ExternalRoutingSummaryMetadata<'_>],
    ) -> Vec<u8> {
        assert_eq!(ids.len(), paths.len());
        assert_eq!(ids.len(), graph_paths.len());
        assert_eq!(ids.len(), metadata.len());
        let schema = routing_schema(2);
        let centroids = vec![vec![0.0_f32, 0.0]; ids.len()];
        let id_bloom = crate::manifest::segment_id_bloom(["record"]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values(
                    ids.iter().map(|_| CURRENT_VERSION),
                )),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 1))),
                array(StringArray::from_iter_values(ids.iter().copied())),
                array(UInt8Array::from_iter_values(ids.iter().map(|_| 0))),
                array(StringArray::from_iter_values(paths.iter().copied())),
                array(UInt64Array::from_iter_values(
                    metadata.iter().map(|row| row.object_count),
                )),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 2))),
                array(fixed_f32_array(centroids.iter().map(Vec::as_slice), 2)),
                array(Float32Array::from_iter_values(ids.iter().map(|_| 0.0))),
                array(StringArray::from_iter_values(
                    metadata.iter().map(|row| row.checksum),
                )),
                array(UInt64Array::from_iter_values(
                    metadata.iter().map(|row| row.size_bytes),
                )),
                array(StringArray::from_iter_values(graph_paths.iter().copied())),
                array(StringArray::from_iter_values(
                    metadata.iter().map(|row| row.graph_checksum),
                )),
                array(UInt64Array::from_iter_values(
                    metadata.iter().map(|row| row.graph_size_bytes),
                )),
                array(Int64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(BinaryArray::from_iter_values(
                    ids.iter().map(|_| id_bloom.as_slice()),
                )),
                array(StringArray::from_iter_values(
                    metadata.iter().map(|row| row.leaf_mode),
                )),
                array(BinaryArray::from_iter_values(
                    metadata.iter().map(|row| row.vector_signature_bloom),
                )),
                array(fixed_f32_array(centroids.iter().map(Vec::as_slice), 2)),
                array(fixed_f32_array(centroids.iter().map(Vec::as_slice), 2)),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_routing_parquet_with_manifest_version(manifest_version: u64) -> Vec<u8> {
        external_routing_parquet_with_vector_id_bloom_and_manifest_version(
            vec![0.0, 0.0],
            0.0,
            2,
            crate::manifest::segment_id_bloom(["record"]),
            manifest_version,
        )
    }

    fn external_routing_parquet_with_vector_and_id_bloom(
        centroid: Vec<f32>,
        radius: f32,
        stored_dimensions: u64,
        id_bloom: Vec<u8>,
    ) -> Vec<u8> {
        external_routing_parquet_with_vector_id_bloom_and_manifest_version(
            centroid,
            radius,
            stored_dimensions,
            id_bloom,
            1,
        )
    }

    fn external_routing_parquet_with_vector_id_bloom_and_manifest_version(
        centroid: Vec<f32>,
        radius: f32,
        stored_dimensions: u64,
        id_bloom: Vec<u8>,
        manifest_version: u64,
    ) -> Vec<u8> {
        let schema_dimensions = centroid.len();
        let schema = routing_schema(schema_dimensions);
        let vector_signature_bloom = valid_vector_signature_bloom();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(UInt64Array::from_iter_values([manifest_version])),
                array(StringArray::from_iter_values(["seg"])),
                array(UInt8Array::from_iter_values([0])),
                array(StringArray::from_iter_values(["segments/seg.parquet"])),
                array(UInt64Array::from_iter_values([1])),
                array(UInt64Array::from_iter_values([stored_dimensions])),
                array(fixed_f32_array([centroid.as_slice()], schema_dimensions)),
                array(Float32Array::from_iter_values([radius])),
                array(StringArray::from_iter_values([VALID_SEGMENT_CHECKSUM])),
                array(UInt64Array::from_iter_values([123])),
                array(StringArray::from_iter_values([
                    "segments/seg.graph.parquet",
                ])),
                array(StringArray::from_iter_values([VALID_GRAPH_CHECKSUM])),
                array(UInt64Array::from_iter_values([45])),
                array(Int64Array::from_iter_values([0])),
                array(BinaryArray::from_iter_values([id_bloom.as_slice()])),
                array(StringArray::from_iter_values(["graph"])),
                array(BinaryArray::from_iter_values([
                    vector_signature_bloom.as_slice()
                ])),
                array(fixed_f32_array([centroid.as_slice()], schema_dimensions)),
                array(fixed_f32_array([centroid.as_slice()], schema_dimensions)),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_pivots_parquet(vector: [f32; 2]) -> Vec<u8> {
        external_pivots_parquet_with_rows(vec![("pivot", 0, vector)])
    }

    fn external_pivots_parquet_with_ids<const N: usize>(ids: [&str; N]) -> Vec<u8> {
        external_pivots_parquet_with_rows(
            ids.iter()
                .enumerate()
                .map(|(ordinal, id)| (*id, ordinal as u64, [0.0, 0.0]))
                .collect(),
        )
    }

    fn external_pivots_parquet_with_rows(rows: Vec<(&str, u64, [f32; 2])>) -> Vec<u8> {
        let schema = pivots_schema(2);
        let vectors = rows.iter().map(|(_, _, vector)| vector.as_slice());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values(
                    rows.iter().map(|_| CURRENT_VERSION),
                )),
                array(UInt64Array::from_iter_values(rows.iter().map(|_| 1))),
                array(UInt64Array::from_iter_values(
                    rows.iter().map(|(_, ordinal, _)| *ordinal),
                )),
                array(StringArray::from_iter_values(
                    rows.iter().map(|(id, _, _)| *id),
                )),
                array(fixed_f32_array(vectors, 2)),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_segment_parquet(
        vector: [f32; 2],
        centroid: [f32; 2],
        radius: f32,
        routing_code: f32,
    ) -> Vec<u8> {
        external_segment_parquet_with_dimensions(
            vector.to_vec(),
            centroid.to_vec(),
            radius,
            routing_code,
            2,
        )
    }

    fn external_segment_parquet_with_records<const N: usize>(
        records: [(&str, [f32; 2]); N],
    ) -> Vec<u8> {
        let schema = segment_schema(2);
        let centroid = [0.0_f32, 0.0];
        let pq_code = [128_u8, 128_u8];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values(
                    records.iter().map(|_| CURRENT_VERSION),
                )),
                array(StringArray::from_iter_values(records.iter().map(|_| "seg"))),
                array(UInt8Array::from_iter_values(records.iter().map(|_| 0))),
                array(StringArray::from_iter_values(
                    records.iter().map(|_| "euclidean"),
                )),
                array(UInt64Array::from_iter_values(records.iter().map(|_| 2))),
                array(fixed_f32_array(
                    records.iter().map(|_| centroid.as_slice()),
                    2,
                )),
                array(Float32Array::from_iter_values(records.iter().map(|_| 0.0))),
                array(Int64Array::from_iter_values(records.iter().map(|_| 0))),
                array(Float32Array::from_iter_values(records.iter().map(|_| 0.0))),
                array(fixed_u8_array(
                    records.iter().map(|_| pq_code.as_slice()),
                    2,
                )),
                array(BinaryArray::from_iter_values(
                    records.iter().map(|(id, _)| id.as_bytes()),
                )),
                array(fixed_f32_array(
                    records.iter().map(|(_, vector)| vector.as_slice()),
                    2,
                )),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_segment_parquet_with_dimensions(
        vector: Vec<f32>,
        centroid: Vec<f32>,
        radius: f32,
        routing_code: f32,
        stored_dimensions: u64,
    ) -> Vec<u8> {
        let centroid_dimensions = centroid.len();
        let vector_dimensions = vector.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("format_version", DataType::UInt16, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("level", DataType::UInt8, false),
            Field::new("metric", DataType::Utf8, false),
            Field::new("dimensions", DataType::UInt64, false),
            fixed_f32_field("centroid", centroid_dimensions),
            Field::new("radius", DataType::Float32, false),
            Field::new("created_at_ms", DataType::Int64, false),
            Field::new("routing_code", DataType::Float32, false),
            Field::new("record_id", DataType::Utf8, false),
            fixed_f32_field("vector", vector_dimensions),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(StringArray::from_iter_values(["seg"])),
                array(UInt8Array::from_iter_values([0])),
                array(StringArray::from_iter_values(["euclidean"])),
                array(UInt64Array::from_iter_values([stored_dimensions])),
                array(fixed_f32_array([centroid.as_slice()], centroid_dimensions)),
                array(Float32Array::from_iter_values([radius])),
                array(Int64Array::from_iter_values([0])),
                array(Float32Array::from_iter_values([routing_code])),
                array(StringArray::from_iter_values(["bad"])),
                array(fixed_f32_array([vector.as_slice()], vector_dimensions)),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_graph_parquet(distance: f32) -> Vec<u8> {
        let schema = graph_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(StringArray::from_iter_values(["seg"])),
                array(UInt8Array::from_iter_values([0])),
                array(Int64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([1])),
                array(Float32Array::from_iter_values([distance])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }
}

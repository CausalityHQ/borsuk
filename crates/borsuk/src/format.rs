use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Int64Array,
    ListArray, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    types::{Float32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
    },
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{
    error::{BorsukError, Result},
    index::IndexConfig,
    manifest::{
        DEFAULT_GRAPH_NEIGHBORS, DEFAULT_ROUTING_PAGE_FANOUT, Manifest, PivotSummary,
        RoutingLayerPageRef, SEGMENT_ID_BLOOM_BYTES, SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES,
        SegmentSummary,
    },
    metric::VectorMetric,
    record::{LeafMode, RecordId, StorageEncoding, VectorRecord},
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
        manifest.routing_page_fanout,
        manifest.graph_neighbors,
    )?;
    let metric = manifest.config.metric.to_string();
    let named_vectors_json = if manifest.config.named_vectors.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&manifest.config.named_vectors).map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "failed to serialize named vector schema: {err}"
                ))
            })?,
        )
    };
    let schema = manifest_schema_with_named_vectors(named_vectors_json.is_some());
    let mut columns = vec![
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
        array(BooleanArray::from_iter([manifest.config.text])),
        array(StringArray::from_iter([manifest.text_tokenizer.clone()])),
        array(UInt64Array::from_iter_values([manifest.next_generated_id])),
        array(UInt8Array::from_iter_values([manifest.routing_max_level])),
        array(UInt64Array::from_iter_values([
            manifest.routing_page_fanout as u64,
        ])),
        array(UInt64Array::from_iter_values([
            manifest.graph_neighbors as u64
        ])),
        array(StringArray::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.path.clone())])),
        array(StringArray::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.checksum.clone())])),
        array(UInt64Array::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.count)])),
        array(BinaryArray::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.id_bloom.as_slice())])),
        array(Int64Array::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.created_at.timestamp_millis())])),
    ];
    if named_vectors_json.is_some() {
        columns.push(array(StringArray::from_iter([named_vectors_json])));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

    write_batch(batch)
}

/// Parse the optional tombstone summary from a manifest table batch. Absent
/// columns (older tables) or a null path both mean "no deletions".
fn manifest_tombstone(batch: &RecordBatch) -> Result<Option<crate::manifest::TombstoneSummary>> {
    let Ok(index) = batch.schema().index_of("tombstone_path") else {
        return Ok(None);
    };
    if batch.column(index).is_null(0) {
        return Ok(None);
    }
    Ok(Some(crate::manifest::TombstoneSummary {
        path: string_value_by_name(batch, 0, "tombstone_path")?.to_string(),
        checksum: string_value_by_name(batch, 0, "tombstone_checksum")?.to_string(),
        count: primitive_value_by_name::<UInt64Type>(batch, 0, "tombstone_count")?,
        id_bloom: binary_value_by_name(batch, 0, "tombstone_id_bloom")?.to_vec(),
        created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
            batch,
            0,
            "tombstone_created_at_ms",
        )?)?,
    }))
}

fn manifest_text_enabled(batch: &RecordBatch) -> Result<bool> {
    let Ok(column) = batch.schema().index_of("text_enabled") else {
        return Ok(false);
    };
    if batch.column(column).is_null(0) {
        return Ok(false);
    }
    boolean_value(batch, column, 0, "text_enabled")
}

fn manifest_text_tokenizer(batch: &RecordBatch) -> Result<Option<String>> {
    let Ok(column) = batch.schema().index_of("text_tokenizer") else {
        return Ok(None);
    };
    if batch.column(column).is_null(0) {
        return Ok(None);
    }
    Ok(Some(
        string_value(batch, column, 0, "text_tokenizer")?.to_string(),
    ))
}

fn manifest_named_vectors(
    batch: &RecordBatch,
) -> Result<BTreeMap<String, crate::record::VectorSpec>> {
    let Ok(column) = batch.schema().index_of("named_vectors_json") else {
        return Ok(BTreeMap::new());
    };
    if batch.column(column).is_null(0) {
        return Ok(BTreeMap::new());
    }
    let json = string_value(batch, column, 0, "named_vectors_json")?;
    serde_json::from_str(json).map_err(|err| {
        BorsukError::InvalidStorage(format!("failed to parse named vector schema: {err}"))
    })
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

    let format_version = primitive_value_by_name::<UInt16Type>(&batch, 0, "format_version")?;
    if format_version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported manifest table version {format_version}"
        )));
    }

    let manifest_version = primitive_value_by_name::<UInt64Type>(&batch, 0, "version")?;
    let metric = VectorMetric::from_str(string_value_by_name(&batch, 0, "metric")?)?;
    let segments = routing_from_parquet(routing_bytes, manifest_version)?;
    let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "dimensions",
    )?)?;
    let segment_max_vectors = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "segment_max_vectors",
    )?)?;
    let routing_page_fanout = manifest_routing_page_fanout(&batch)?;
    let graph_neighbors = manifest_graph_neighbors(&batch)?;
    validate_manifest_config(
        dimensions,
        segment_max_vectors,
        routing_page_fanout,
        graph_neighbors,
    )?;
    let next_generated_id = if batch.schema().field_with_name("next_generated_id").is_ok() {
        primitive_value_by_name::<UInt64Type>(&batch, 0, "next_generated_id")?
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
            uri: string_value_by_name(&batch, 0, "uri")?.to_string(),
            metric,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes: if batch.schema().field_with_name("ram_budget_bytes").is_ok() {
                primitive_optional_value_by_name::<UInt64Type>(&batch, 0, "ram_budget_bytes")?
            } else {
                None
            },
            text: manifest_text_enabled(&batch)?,
            named_vectors: manifest_named_vectors(&batch)?,
        },
        text_tokenizer: manifest_text_tokenizer(&batch)?,
        segments,
        pivots: Vec::new(),
        next_generated_id,
        routing_max_level: manifest_routing_max_level(&batch)?,
        routing_page_fanout,
        graph_neighbors,
        tombstone: manifest_tombstone(&batch)?,
        created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
            &batch,
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

    let format_version = primitive_value_by_name::<UInt16Type>(&batch, 0, "format_version")?;
    if format_version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported manifest table version {format_version}"
        )));
    }

    let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "dimensions",
    )?)?;
    let segment_max_vectors = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "segment_max_vectors",
    )?)?;
    let routing_page_fanout = manifest_routing_page_fanout(&batch)?;
    let graph_neighbors = manifest_graph_neighbors(&batch)?;
    validate_manifest_config(
        dimensions,
        segment_max_vectors,
        routing_page_fanout,
        graph_neighbors,
    )?;

    Ok(Manifest {
        version: primitive_value_by_name::<UInt64Type>(&batch, 0, "version")?,
        config: IndexConfig {
            uri: string_value_by_name(&batch, 0, "uri")?.to_string(),
            metric: VectorMetric::from_str(string_value_by_name(&batch, 0, "metric")?)?,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes: if batch.schema().field_with_name("ram_budget_bytes").is_ok() {
                primitive_optional_value_by_name::<UInt64Type>(&batch, 0, "ram_budget_bytes")?
            } else {
                None
            },
            text: manifest_text_enabled(&batch)?,
            named_vectors: manifest_named_vectors(&batch)?,
        },
        text_tokenizer: manifest_text_tokenizer(&batch)?,
        segments: Vec::new(),
        pivots: Vec::new(),
        next_generated_id: if batch.schema().field_with_name("next_generated_id").is_ok() {
            primitive_value_by_name::<UInt64Type>(&batch, 0, "next_generated_id")?
        } else {
            0
        },
        routing_max_level: manifest_routing_max_level(&batch)?,
        routing_page_fanout,
        graph_neighbors,
        tombstone: manifest_tombstone(&batch)?,
        created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
            &batch,
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

fn manifest_routing_page_fanout(batch: &RecordBatch) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("routing_page_fanout") else {
        return Ok(DEFAULT_ROUTING_PAGE_FANOUT);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        0,
        "routing_page_fanout",
    )?)
}

fn manifest_graph_neighbors(batch: &RecordBatch) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("graph_neighbors") else {
        return Ok(DEFAULT_GRAPH_NEIGHBORS);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        0,
        "graph_neighbors",
    )?)
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
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.metadata_stats.to_bytes()),
            )),
            array(UInt32Array::from_iter_values(
                segments.iter().map(|segment| segment.text_doc_count),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.text_total_doc_length),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.sparse_encoded as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dense_encoded as u64),
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
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.metadata_stats.to_bytes()),
            )),
            array(UInt32Array::from_iter_values(
                segments.iter().map(|segment| segment.text_doc_count),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.text_total_doc_length),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.sparse_encoded as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dense_encoded as u64),
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
                page_refs.iter().map(|page_ref| page_ref.leaf_pages as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.routing_pages as u64),
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
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_sparse_encoded_vectors as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_dense_encoded_vectors as u64),
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
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing layer page index version {format_version}"
                )));
            }
            let manifest_version =
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?;
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
                u64::from(primitive_value_by_name::<UInt8Type>(
                    &batch,
                    row,
                    "routing_level",
                )?),
            )?;
            let page_segments = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch,
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
                page_ordinal: usize_from_u64(primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "page_ordinal",
                )?)?,
                path: string_value_by_name(&batch, row, "page_path")?.to_string(),
                checksum: string_value_by_name(&batch, row, "page_checksum")?.to_string(),
                page_segments,
                leaf_segments: routing_page_ref_leaf_segments(&batch, row, page_segments)?,
                leaf_pages: routing_page_ref_leaf_pages(&batch, row)?,
                routing_pages: routing_page_ref_routing_pages(&batch, row)?,
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
                page_sparse_encoded_vectors: routing_page_ref_page_sparse_encoded_vectors(
                    &batch, row,
                )?,
                page_dense_encoded_vectors: routing_page_ref_page_dense_encoded_vectors(
                    &batch, row,
                )?,
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
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing layer page version {format_version}"
                )));
            }
            let page_manifest_version =
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?;
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
                u64::from(primitive_value_by_name::<UInt8Type>(
                    &batch,
                    row,
                    "routing_level",
                )?),
            )?;
            validate_routing_layer_page_field(
                "page_ordinal",
                expected_page_ordinal as u64,
                primitive_value_by_name::<UInt64Type>(&batch, row, "page_ordinal")?,
            )?;
            let page_segments =
                primitive_value_by_name::<UInt64Type>(&batch, row, "page_segments")?;
            if page_segments == 0 {
                return Err(BorsukError::InvalidStorage(
                    "routing layer page must declare at least one segment".to_string(),
                ));
            }

            let id = string_value_by_name(&batch, row, "segment_id")?.to_string();
            let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch,
                row,
                "dimensions",
            )?)?;
            validate_routing_segment_dimensions(&id, expected_dimensions, dimensions)?;
            let centroid = fixed_f32_value_by_name(&batch, row, "centroid")?;
            validate_routing_centroid_dimensions(&id, dimensions, centroid.len())?;
            validate_routing_centroid_values(&id, &centroid)?;
            let radius = primitive_value_by_name::<Float32Type>(&batch, row, "radius")?;
            validate_routing_radius(&id, radius)?;
            let bounds_min = routing_bounds(&batch, row, "bounds_min", &id)?;
            let bounds_max = routing_bounds(&batch, row, "bounds_max", &id)?;
            let id_bloom = binary_value_by_name(&batch, row, "id_bloom")?.to_vec();
            validate_routing_id_bloom(&id, &id_bloom)?;
            let vector_signature_bloom = routing_vector_signature_bloom(&batch, row, &id)?;
            validate_routing_vector_signature_bloom(&id, &vector_signature_bloom)?;
            let leaf_mode = routing_leaf_mode(&batch, row)?;

            summaries.push(SegmentSummary {
                id,
                level: primitive_value_by_name::<UInt8Type>(&batch, row, "segment_level")?,
                path: string_value_by_name(&batch, row, "segment_path")?.to_string(),
                object_count: usize_from_u64(primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "object_count",
                )?)?,
                dimensions,
                centroid,
                radius,
                bounds_min,
                bounds_max,
                checksum: string_value_by_name(&batch, row, "segment_checksum")?.to_string(),
                size_bytes: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "segment_size_bytes",
                )?,
                graph_path: string_value_by_name(&batch, row, "graph_path")?.to_string(),
                graph_checksum: string_value_by_name(&batch, row, "graph_checksum")?.to_string(),
                graph_size_bytes: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "graph_size_bytes",
                )?,
                leaf_mode,
                id_bloom,
                vector_signature_bloom,
                metadata_stats: routing_metadata_stats(&batch, row)?,
                sparse_encoded: routing_sparse_encoded(&batch, row)?,
                dense_encoded: routing_dense_encoded(&batch, row)?,
                text_doc_count: routing_text_doc_count(&batch, row)?,
                text_total_doc_length: routing_text_total_doc_length(&batch, row)?,
                created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
                    &batch,
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
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported pivot table version {format_version}"
                )));
            }

            validate_table_manifest_version(
                "pivot table",
                expected_manifest_version,
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?,
            )?;
            let ordinal = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch, row, "ordinal",
            )?)?;
            let id = string_value_by_name(&batch, row, "pivot_id")?.to_string();
            let vector = fixed_f32_value_by_name(&batch, row, "vector")?;
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
            array(BinaryArray::from_iter_values(
                records
                    .iter()
                    .map(|record| crate::metadata::encode(&record.metadata)),
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

            let metadata = match batch.schema().index_of("metadata").ok() {
                Some(column) => {
                    crate::metadata::decode(binary_value(&batch, column, row, "metadata")?)?
                }
                None => crate::Metadata::new(),
            };
            records.push(VectorRecord {
                id,
                vector,
                extra_vectors: BTreeMap::new(),
                extra_sparse: BTreeMap::new(),
                storage: crate::StorageEncoding::Auto,
                text: None,
                text_term_ids: Vec::new(),
                text_term_freqs: Vec::new(),
                metadata,
                generation: 0,
            });
        }
    }

    validate_vector_record_ids(&records)?;

    Ok(records)
}

fn validate_manifest_config(
    dimensions: usize,
    segment_max_vectors: usize,
    routing_page_fanout: usize,
    graph_neighbors: usize,
) -> Result<()> {
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
    if routing_page_fanout <= 1 {
        return Err(BorsukError::InvalidStorage(
            "manifest routing_page_fanout must be greater than one".to_string(),
        ));
    }
    if graph_neighbors == 0 {
        return Err(BorsukError::InvalidStorage(
            "manifest graph_neighbors must be greater than zero".to_string(),
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
        let encoded_count = segment.sparse_encoded.saturating_add(segment.dense_encoded);
        if encoded_count != 0 && encoded_count != segment.object_count {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment encoded counts must sum to object_count; segment `{}`",
                segment.id
            )));
        }
        if segment.text_doc_count as usize > segment.object_count {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment text_doc_count must not exceed object_count; segment `{}`",
                segment.id
            )));
        }
        if segment.text_doc_count == 0 && segment.text_total_doc_length != 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment text_total_doc_length must be zero when text_doc_count is zero; segment `{}`",
                segment.id
            )));
        }
        if segment.text_doc_count > 0
            && segment.text_total_doc_length < u64::from(segment.text_doc_count)
        {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment text_total_doc_length must be at least text_doc_count; segment `{}`",
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
        if page_ref.leaf_pages == 0 || page_ref.routing_pages == 0 {
            if page_ref.leaf_pages != 0 || page_ref.routing_pages != 0 {
                return Err(BorsukError::InvalidStorage(
                    "routing layer page index leaf_pages and routing_pages must both be present or both be legacy-zero".to_string(),
                ));
            }
        } else if page_ref.routing_pages < page_ref.leaf_pages {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index routing_pages must be at least leaf_pages".to_string(),
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

fn routing_page_ref_leaf_pages(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("leaf_pages") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "leaf_pages",
    )?)
}

fn routing_page_ref_routing_pages(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("routing_pages") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "routing_pages",
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

fn routing_page_ref_page_sparse_encoded_vectors(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("page_sparse_encoded_vectors") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "page_sparse_encoded_vectors",
    )?)
}

fn routing_page_ref_page_dense_encoded_vectors(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("page_dense_encoded_vectors") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "page_dense_encoded_vectors",
    )?)
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

/// Read a segment's persisted metadata pruning stats, defaulting to empty when
/// the column is absent.
fn routing_metadata_stats(batch: &RecordBatch, row: usize) -> Result<crate::MetadataStats> {
    if batch.schema().field_with_name("metadata_stats").is_ok() {
        crate::MetadataStats::from_bytes(binary_value_by_name(batch, row, "metadata_stats")?)
    } else {
        Ok(crate::MetadataStats::default())
    }
}

fn routing_text_doc_count(batch: &RecordBatch, row: usize) -> Result<u32> {
    if batch.schema().field_with_name("text_doc_count").is_ok() {
        primitive_value_by_name::<UInt32Type>(batch, row, "text_doc_count")
    } else {
        Ok(0)
    }
}

fn routing_text_total_doc_length(batch: &RecordBatch, row: usize) -> Result<u64> {
    if batch
        .schema()
        .field_with_name("text_total_doc_length")
        .is_ok()
    {
        primitive_value_by_name::<UInt64Type>(batch, row, "text_total_doc_length")
    } else {
        Ok(0)
    }
}

fn routing_sparse_encoded(batch: &RecordBatch, row: usize) -> Result<usize> {
    if batch.schema().field_with_name("sparse_encoded").is_ok() {
        usize_from_u64(primitive_value_by_name::<UInt64Type>(
            batch,
            row,
            "sparse_encoded",
        )?)
    } else {
        Ok(0)
    }
}

fn routing_dense_encoded(batch: &RecordBatch, row: usize) -> Result<usize> {
    if batch.schema().field_with_name("dense_encoded").is_ok() {
        usize_from_u64(primitive_value_by_name::<UInt64Type>(
            batch,
            row,
            "dense_encoded",
        )?)
    } else {
        Ok(0)
    }
}

pub(crate) fn routing_from_parquet(
    bytes: &[u8],
    expected_manifest_version: u64,
) -> Result<Vec<SegmentSummary>> {
    let mut summaries = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing table version {format_version}"
                )));
            }
            validate_table_manifest_version(
                "routing table",
                expected_manifest_version,
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?,
            )?;

            let id = string_value_by_name(&batch, row, "id")?.to_string();
            let centroid = fixed_f32_value_by_name(&batch, row, "centroid")?;
            let radius = primitive_value_by_name::<Float32Type>(&batch, row, "radius")?;
            let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch,
                row,
                "dimensions",
            )?)?;
            validate_routing_centroid_dimensions(&id, dimensions, centroid.len())?;
            validate_routing_centroid_values(&id, &centroid)?;
            validate_routing_radius(&id, radius)?;
            let id_bloom = if batch.schema().field_with_name("id_bloom").is_ok() {
                let id_bloom = binary_value_by_name(&batch, row, "id_bloom")?.to_vec();
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
                level: primitive_value_by_name::<UInt8Type>(&batch, row, "level")?,
                path: string_value_by_name(&batch, row, "path")?.to_string(),
                object_count: usize_from_u64(primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "object_count",
                )?)?,
                dimensions,
                centroid,
                radius,
                bounds_min,
                bounds_max,
                checksum: string_value_by_name(&batch, row, "checksum")?.to_string(),
                size_bytes: primitive_value_by_name::<UInt64Type>(&batch, row, "size_bytes")?,
                graph_path: string_value_by_name(&batch, row, "graph_path")?.to_string(),
                graph_checksum: string_value_by_name(&batch, row, "graph_checksum")?.to_string(),
                graph_size_bytes: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "graph_size_bytes",
                )?,
                leaf_mode,
                id_bloom,
                vector_signature_bloom,
                metadata_stats: routing_metadata_stats(&batch, row)?,
                sparse_encoded: routing_sparse_encoded(&batch, row)?,
                dense_encoded: routing_dense_encoded(&batch, row)?,
                text_doc_count: routing_text_doc_count(&batch, row)?,
                text_total_doc_length: routing_text_total_doc_length(&batch, row)?,
                created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
                    &batch,
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
        validate_segment_record_text_terms(record)?;
    }

    let records = &segment.records;
    let mut dense_vectors = Vec::with_capacity(records.len());
    let mut sparse_indices = Vec::<Option<Vec<u32>>>::with_capacity(records.len());
    let mut sparse_values = Vec::<Option<Vec<f32>>>::with_capacity(records.len());
    let mut include_sparse = false;
    for record in records {
        match record.storage.resolve_for_vector(&record.vector) {
            StorageEncoding::Dense => {
                dense_vectors.push(Some(record.vector.as_slice()));
                sparse_indices.push(None);
                sparse_values.push(None);
            }
            StorageEncoding::Sparse => {
                include_sparse = true;
                dense_vectors.push(None);
                let (indices, values) = sparse_parts_from_dense(&record.id, &record.vector)?;
                sparse_indices.push(Some(indices));
                sparse_values.push(Some(values));
            }
            StorageEncoding::Auto => unreachable!("storage encoding should be resolved"),
        }
    }
    let include_text = records
        .iter()
        .any(|record| !record.text_term_ids.is_empty());
    let include_generation = records.iter().any(|record| record.generation != 0);
    let metric = segment.metric.to_string();
    let schema = segment_schema(
        segment.dimensions,
        include_sparse,
        include_text,
        include_generation,
    );
    let mut columns = vec![
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
            records.iter().map(|_| segment.pq_min.as_slice()),
            segment.dimensions,
        )),
        array(fixed_f32_array(
            records.iter().map(|_| segment.pq_max.as_slice()),
            segment.dimensions,
        )),
        array(optional_fixed_f32_array(dense_vectors, segment.dimensions)),
        array(BinaryArray::from_iter_values(
            records
                .iter()
                .map(|record| crate::metadata::encode(&record.metadata)),
        )),
    ];
    if include_sparse {
        columns.push(array(optional_u32_list_array(
            sparse_indices.iter().map(|indices| indices.as_deref()),
        )));
        columns.push(array(optional_f32_list_array(
            sparse_values.iter().map(|values| values.as_deref()),
        )));
    }
    if include_text {
        columns.push(array(sparse_u32_list_array(
            records.iter().map(|record| record.text_term_ids.as_slice()),
        )));
        columns.push(array(sparse_u32_list_array(
            records
                .iter()
                .map(|record| record.text_term_freqs.as_slice()),
        )));
    }
    if include_generation {
        columns.push(array(UInt64Array::from_iter_values(
            records.iter().map(|record| record.generation),
        )));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

    write_batch(batch)
}

pub(crate) fn segment_from_parquet(bytes: &[u8]) -> Result<Segment> {
    segment_from_parquet_impl(bytes, false)
}

/// True when the segment carries persisted PQ bounds, so it can be decoded
/// lean (without the vector column) and still quantize queries.
pub(crate) fn segment_has_persisted_pq_bounds(bytes: &[u8]) -> Result<bool> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let fields = builder.schema().fields();
    Ok(fields.iter().any(|field| field.name() == "pq_min")
        && fields.iter().any(|field| field.name() == "pq_max"))
}

/// Decode a segment for candidate selection without materializing the `vector`
/// column: records carry ids, routing codes, and PQ codes but empty vectors,
/// and the persisted PQ bounds let queries be quantized. Chosen candidates'
/// vectors are fetched with [`segment_vectors_for_rows`].
pub(crate) fn lean_segment_from_parquet(bytes: &[u8]) -> Result<Segment> {
    segment_from_parquet_impl(bytes, true)
}

fn segment_from_parquet_impl(bytes: &[u8], lean: bool) -> Result<Segment> {
    let mut records = Vec::new();
    let mut routing_codes = Vec::new();
    let mut pq_codes = Vec::new();
    let mut metadata = None::<SegmentMetadata>;
    let mut pq_bounds = None::<(Vec<f32>, Vec<f32>)>;

    for batch in read_batches_projected(bytes, lean, None)? {
        let routing_code_column = batch.schema().index_of("routing_code").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `routing_code` column".to_string())
        })?;
        let pq_code_column = batch.schema().index_of("pq_code").ok();
        let record_id_column = batch.schema().index_of("record_id").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `record_id` column".to_string())
        })?;
        let metadata_column = batch.schema().index_of("metadata").ok();
        let sparse_indices_column = batch.schema().index_of("sparse_indices").ok();
        let sparse_values_column = batch.schema().index_of("sparse_values").ok();
        if sparse_indices_column.is_some() != sparse_values_column.is_some() {
            return Err(BorsukError::InvalidStorage(
                "segment table must contain both sparse_indices and sparse_values columns"
                    .to_string(),
            ));
        }
        let text_term_ids_column = batch.schema().index_of("text_term_ids").ok();
        let text_term_freqs_column = batch.schema().index_of("text_term_freqs").ok();
        if text_term_ids_column.is_some() != text_term_freqs_column.is_some() {
            return Err(BorsukError::InvalidStorage(
                "segment table must contain both text_term_ids and text_term_freqs columns"
                    .to_string(),
            ));
        }
        let generation_column = batch.schema().index_of("generation").ok();
        let vector_column = if lean {
            None
        } else {
            Some(batch.schema().index_of("vector").map_err(|_| {
                BorsukError::InvalidStorage("segment table missing `vector` column".to_string())
            })?)
        };
        if pq_bounds.is_none()
            && batch.num_rows() > 0
            && let (Ok(min_column), Ok(max_column)) = (
                batch.schema().index_of("pq_min"),
                batch.schema().index_of("pq_max"),
            )
        {
            pq_bounds = Some((
                fixed_f32_value(&batch, min_column, 0, "pq_min")?,
                fixed_f32_value(&batch, max_column, 0, "pq_max")?,
            ));
        }
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
            let metadata = match metadata_column {
                Some(column) => {
                    crate::metadata::decode(binary_value(&batch, column, row, "metadata")?)?
                }
                None => crate::Metadata::new(),
            };
            // The second element is the on-disk encoding; the record's `storage`
            // is a write-time hint, not persisted state, so a decoded record
            // round-trips as `Auto` (equal to how it was originally built).
            let (vector, _encoding) = decode_segment_vector(
                &batch,
                row,
                &id,
                row_dimensions,
                vector_column,
                sparse_indices_column,
                sparse_values_column,
            )?;
            let (text_term_ids, text_term_freqs) =
                match (text_term_ids_column, text_term_freqs_column) {
                    (Some(ids_column), Some(freqs_column)) => {
                        let ids = primitive_list_optional_value::<UInt32Type>(
                            &batch,
                            ids_column,
                            row,
                            "text_term_ids",
                        )?
                        .unwrap_or_default();
                        let freqs = primitive_list_optional_value::<UInt32Type>(
                            &batch,
                            freqs_column,
                            row,
                            "text_term_freqs",
                        )?
                        .unwrap_or_default();
                        if ids.is_empty() && freqs.is_empty() {
                            (Vec::new(), Vec::new())
                        } else {
                            validate_text_terms(&id, &ids, &freqs)?;
                            (ids, freqs)
                        }
                    }
                    (None, None) => (Vec::new(), Vec::new()),
                    _ => unreachable!("text term column presence checked above"),
                };
            let generation = match generation_column {
                Some(column) => primitive_value::<UInt64Type>(&batch, column, row, "generation")?,
                None => 0,
            };
            records.push(VectorRecord {
                id,
                vector,
                extra_vectors: BTreeMap::new(),
                extra_sparse: BTreeMap::new(),
                storage: crate::StorageEncoding::Auto,
                text: None,
                text_term_ids,
                text_term_freqs,
                metadata,
                generation,
            });
            routing_codes.push(routing_code);
        }
    }

    let metadata = metadata.ok_or_else(|| {
        BorsukError::InvalidStorage("segment table must contain at least one row".to_string())
    })?;
    validate_segment_record_ids(&records)?;
    if pq_codes.is_empty() {
        if lean {
            return Err(BorsukError::InvalidStorage(
                "lean segment decode requires stored `pq_code` values".to_string(),
            ));
        }
        pq_codes = crate::segment::pq_codes_for_records(&records, metadata.dimensions)?;
    }
    validate_segment_pq_code_count(&metadata.id, records.len(), pq_codes.len())?;

    let (pq_min, pq_max) = match pq_bounds {
        Some(bounds) => bounds,
        None => {
            if lean {
                return Err(BorsukError::InvalidStorage(
                    "lean segment decode requires persisted PQ bounds".to_string(),
                ));
            }
            crate::segment::pq_bounds_for_records(&records, metadata.dimensions)?
        }
    };

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
        pq_min,
        pq_max,
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

#[cfg(test)]
fn manifest_schema() -> Arc<Schema> {
    manifest_schema_with_named_vectors(false)
}

fn manifest_schema_with_named_vectors(include_named_vectors: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("version", DataType::UInt64, false),
        Field::new("uri", DataType::Utf8, false),
        Field::new("metric", DataType::Utf8, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new("segment_max_vectors", DataType::UInt64, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("ram_budget_bytes", DataType::UInt64, true),
        Field::new("text_enabled", DataType::Boolean, false),
        Field::new("text_tokenizer", DataType::Utf8, true),
        Field::new("next_generated_id", DataType::UInt64, false),
        Field::new("routing_max_level", DataType::UInt8, false),
        Field::new("routing_page_fanout", DataType::UInt64, false),
        Field::new("graph_neighbors", DataType::UInt64, false),
        Field::new("tombstone_path", DataType::Utf8, true),
        Field::new("tombstone_checksum", DataType::Utf8, true),
        Field::new("tombstone_count", DataType::UInt64, true),
        Field::new("tombstone_id_bloom", DataType::Binary, true),
        Field::new("tombstone_created_at_ms", DataType::Int64, true),
    ];
    if include_named_vectors {
        fields.push(Field::new("named_vectors_json", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
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
        Field::new("metadata_stats", DataType::Binary, false),
        Field::new("text_doc_count", DataType::UInt32, false),
        Field::new("text_total_doc_length", DataType::UInt64, false),
        Field::new("sparse_encoded", DataType::UInt64, false),
        Field::new("dense_encoded", DataType::UInt64, false),
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
        Field::new("metadata_stats", DataType::Binary, false),
        Field::new("text_doc_count", DataType::UInt32, false),
        Field::new("text_total_doc_length", DataType::UInt64, false),
        Field::new("sparse_encoded", DataType::UInt64, false),
        Field::new("dense_encoded", DataType::UInt64, false),
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
        Field::new("leaf_pages", DataType::UInt64, false),
        Field::new("routing_pages", DataType::UInt64, false),
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
        Field::new("page_sparse_encoded_vectors", DataType::UInt64, false),
        Field::new("page_dense_encoded_vectors", DataType::UInt64, false),
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

fn segment_schema(
    dimensions: usize,
    include_sparse: bool,
    include_text: bool,
    include_generation: bool,
) -> Arc<Schema> {
    let mut fields = vec![
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
        fixed_f32_field("pq_min", dimensions),
        fixed_f32_field("pq_max", dimensions),
        nullable_fixed_f32_field("vector", dimensions),
        Field::new("metadata", DataType::Binary, false),
    ];
    if include_sparse {
        fields.push(sparse_u32_field("sparse_indices"));
        fields.push(sparse_f32_field("sparse_values"));
    }
    if include_text {
        fields.push(sparse_u32_field("text_term_ids"));
        fields.push(sparse_u32_field("text_term_freqs"));
    }
    if include_generation {
        fields.push(Field::new("generation", DataType::UInt64, false));
    }
    Arc::new(Schema::new(fields))
}

fn vector_records_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new("record_id", DataType::Binary, false),
        fixed_f32_field("vector", dimensions),
        Field::new("metadata", DataType::Binary, false),
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

fn nullable_fixed_f32_field(name: &str, dimensions: usize) -> Field {
    Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new_list_field(DataType::Float32, true)),
            dimensions as i32,
        ),
        true,
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

fn sparse_u32_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
        true,
    )
}

fn sparse_f32_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
        true,
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

fn optional_fixed_f32_array<'a>(
    values: impl IntoIterator<Item = Option<&'a [f32]>>,
    dimensions: usize,
) -> FixedSizeListArray {
    let values = values
        .into_iter()
        .map(|vector| vector.map(|vector| vector.iter().copied().map(Some).collect::<Vec<_>>()))
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

fn sparse_u32_list_array<'a>(values: impl IntoIterator<Item = &'a [u32]>) -> ListArray {
    let values = values
        .into_iter()
        .map(|indices| {
            (!indices.is_empty()).then(|| indices.iter().copied().map(Some).collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    ListArray::from_iter_primitive::<UInt32Type, _, _>(values)
}

fn optional_u32_list_array<'a>(values: impl IntoIterator<Item = Option<&'a [u32]>>) -> ListArray {
    let values = values
        .into_iter()
        .map(|indices| indices.map(|indices| indices.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    ListArray::from_iter_primitive::<UInt32Type, _, _>(values)
}

fn optional_f32_list_array<'a>(values: impl IntoIterator<Item = Option<&'a [f32]>>) -> ListArray {
    let values = values
        .into_iter()
        .map(|weights| weights.map(|weights| weights.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    ListArray::from_iter_primitive::<Float32Type, _, _>(values)
}

/// Encode the cumulative tombstone into a Parquet object with a binary
/// `record_id` column and a `min_visible_generation` column: a record of that
/// id is suppressed (deleted or superseded by a newer upsert) when its
/// generation is below the stored minimum-visible generation.
pub(crate) fn tombstone_ids_to_parquet(entries: &[(Vec<u8>, u64)]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Binary, false),
        Field::new("min_visible_generation", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            array(BinaryArray::from_iter_values(
                entries.iter().map(|(id, _)| id.as_slice()),
            )),
            array(UInt64Array::from_iter_values(
                entries.iter().map(|(_, generation)| *generation),
            )),
        ],
    )?;
    write_batch(batch)
}

/// Decode the cumulative tombstone entries `(record_id, min_visible_generation)`
/// from a Parquet object. A legacy single-column tombstone (no generation
/// column) decodes every id as fully deleted (`u64::MAX`).
pub(crate) fn tombstone_ids_from_parquet(bytes: &[u8]) -> Result<Vec<(Vec<u8>, u64)>> {
    let mut entries = Vec::new();
    for batch in read_batches(bytes)? {
        let column = batch.column_by_name("record_id").ok_or_else(|| {
            BorsukError::InvalidStorage("tombstone table is missing record_id".to_string())
        })?;
        let ids = column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage("tombstone record_id column is not binary".to_string())
            })?;
        let generations = match batch.column_by_name("min_visible_generation") {
            Some(column) => Some(column.as_any().downcast_ref::<UInt64Array>().ok_or_else(
                || {
                    BorsukError::InvalidStorage(
                        "tombstone min_visible_generation column is not u64".to_string(),
                    )
                },
            )?),
            None => None,
        };
        for row in 0..ids.len() {
            let generation = generations.map_or(u64::MAX, |values| values.value(row));
            entries.push((ids.value(row).to_vec(), generation));
        }
    }
    Ok(entries)
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
    read_batches_projected(bytes, false, None)
}

/// Read a segment's Parquet batches, optionally projecting out the `vector`
/// column (so it is never decompressed) and/or restricting to a set of rows.
fn read_batches_projected(
    bytes: &[u8],
    project_out_vector: bool,
    row_selection: Option<RowSelection>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if project_out_vector {
        let vector_root = vector_root_index(builder.parquet_schema());
        let roots = (0..builder.parquet_schema().root_schema().get_fields().len())
            .filter(|index| Some(*index) != vector_root);
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
        builder = builder.with_projection(mask);
    }
    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }
    builder
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn vector_root_index(schema: &parquet::schema::types::SchemaDescriptor) -> Option<usize> {
    schema
        .root_schema()
        .get_fields()
        .iter()
        .position(|field| field.name() == "vector")
}

fn root_indices_for_names<const N: usize>(
    schema: &parquet::schema::types::SchemaDescriptor,
    names: [&str; N],
) -> Vec<usize> {
    schema
        .root_schema()
        .get_fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| names.contains(&field.name()).then_some(index))
        .collect()
}

/// Fetch full vectors for a set of segment rows. `rows` may be unsorted or
/// contain duplicates; the returned map is keyed by row index.
pub(crate) fn segment_vectors_for_rows(
    bytes: &[u8],
    rows: &[usize],
    dimensions: usize,
) -> Result<std::collections::HashMap<usize, Vec<f32>>> {
    let mut sorted = rows.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut result = std::collections::HashMap::with_capacity(sorted.len());
    if sorted.is_empty() {
        return Ok(result);
    }

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let total_rows: usize = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|group| group.num_rows() as usize)
        .sum();
    let Some(vector_root) = vector_root_index(builder.parquet_schema()) else {
        return Err(BorsukError::InvalidStorage(
            "segment table missing `vector` column".to_string(),
        ));
    };
    let mut roots = vec![vector_root];
    roots.extend(root_indices_for_names(
        builder.parquet_schema(),
        ["sparse_indices", "sparse_values"],
    ));
    roots.sort_unstable();
    roots.dedup();
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
    let selection = row_selection_for_rows(&sorted, total_rows);
    let reader = builder
        .with_projection(mask)
        .with_row_selection(selection)
        .build()?;

    let mut cursor = 0_usize;
    for batch in reader {
        let batch = batch?;
        let vector_column = batch.schema().index_of("vector").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `vector` column".to_string())
        })?;
        let sparse_indices_column = batch.schema().index_of("sparse_indices").ok();
        let sparse_values_column = batch.schema().index_of("sparse_values").ok();
        if sparse_indices_column.is_some() != sparse_values_column.is_some() {
            return Err(BorsukError::InvalidStorage(
                "segment table must contain both sparse_indices and sparse_values columns"
                    .to_string(),
            ));
        }
        for row in 0..batch.num_rows() {
            let original_row = sorted[cursor];
            let id = RecordId::from(format!("row-{original_row}"));
            let (vector, _) = decode_segment_vector(
                &batch,
                row,
                &id,
                dimensions,
                Some(vector_column),
                sparse_indices_column,
                sparse_values_column,
            )?;
            if vector.len() != dimensions {
                return Err(BorsukError::InvalidStorage(format!(
                    "segment vector has {} dimensions, expected {dimensions}",
                    vector.len()
                )));
            }
            result.insert(original_row, vector);
            cursor += 1;
        }
    }
    if cursor != sorted.len() {
        return Err(BorsukError::InvalidStorage(format!(
            "row-selective vector read returned {cursor} rows, expected {}",
            sorted.len()
        )));
    }
    Ok(result)
}

fn row_selection_for_rows(sorted_rows: &[usize], total_rows: usize) -> RowSelection {
    let mut selectors = Vec::new();
    let mut cursor = 0_usize;
    for &row in sorted_rows {
        if row > cursor {
            selectors.push(RowSelector::skip(row - cursor));
        }
        selectors.push(RowSelector::select(1));
        cursor = row + 1;
    }
    if total_rows > cursor {
        selectors.push(RowSelector::skip(total_rows - cursor));
    }
    RowSelection::from(selectors)
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

fn column_index(batch: &RecordBatch, name: &str) -> Result<usize> {
    batch
        .schema()
        .index_of(name)
        .map_err(|_| BorsukError::InvalidStorage(format!("missing column `{name}`")))
}

fn primitive_value_by_name<T>(batch: &RecordBatch, row: usize, name: &str) -> Result<T::Native>
where
    T: arrow_array::ArrowPrimitiveType,
{
    primitive_value::<T>(batch, column_index(batch, name)?, row, name)
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

fn primitive_optional_value_by_name<T>(
    batch: &RecordBatch,
    row: usize,
    name: &str,
) -> Result<Option<T::Native>>
where
    T: arrow_array::ArrowPrimitiveType,
{
    primitive_optional_value::<T>(batch, column_index(batch, name)?, row, name)
}

fn boolean_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<bool> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
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

fn string_value_by_name<'a>(batch: &'a RecordBatch, row: usize, name: &str) -> Result<&'a str> {
    string_value(batch, column_index(batch, name)?, row, name)
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

fn binary_value_by_name<'a>(batch: &'a RecordBatch, row: usize, name: &str) -> Result<&'a [u8]> {
    binary_value(batch, column_index(batch, name)?, row, name)
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

fn fixed_f32_optional_value(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<Option<Vec<f32>>> {
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    if list.is_null(row) {
        return Ok(None);
    }
    fixed_f32_value(batch, column, row, name).map(Some)
}

fn fixed_f32_value_by_name(batch: &RecordBatch, row: usize, name: &str) -> Result<Vec<f32>> {
    fixed_f32_value(batch, column_index(batch, name)?, row, name)
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

fn primitive_list_optional_value<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<Option<Vec<T::Native>>>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    if list.is_null(row) {
        return Ok(None);
    }

    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    let mut out = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        if values.is_null(index) {
            return Err(BorsukError::InvalidStorage(format!(
                "column `{name}` contains a null sparse value"
            )));
        }
        out.push(values.value(index));
    }
    Ok(Some(out))
}

fn decode_segment_vector(
    batch: &RecordBatch,
    row: usize,
    id: &RecordId,
    dimensions: usize,
    vector_column: Option<usize>,
    sparse_indices_column: Option<usize>,
    sparse_values_column: Option<usize>,
) -> Result<(Vec<f32>, StorageEncoding)> {
    if vector_column.is_none() {
        if let (Some(indices_column), Some(values_column)) =
            (sparse_indices_column, sparse_values_column)
        {
            let sparse_present = !batch.column(indices_column).is_null(row)
                || !batch.column(values_column).is_null(row);
            if sparse_present {
                let indices = primitive_list_optional_value::<UInt32Type>(
                    batch,
                    indices_column,
                    row,
                    "sparse_indices",
                )?
                .unwrap_or_default();
                let values = primitive_list_optional_value::<Float32Type>(
                    batch,
                    values_column,
                    row,
                    "sparse_values",
                )?
                .unwrap_or_default();
                validate_sparse_encoding(id, dimensions, indices, values)?;
                return Ok((Vec::new(), StorageEncoding::Sparse));
            }
        }
        return Ok((Vec::new(), StorageEncoding::Dense));
    }

    let dense = fixed_f32_optional_value(batch, vector_column.unwrap(), row, "vector")?;
    let sparse = match (sparse_indices_column, sparse_values_column) {
        (Some(indices_column), Some(values_column)) => {
            let indices_present = !batch.column(indices_column).is_null(row);
            let values_present = !batch.column(values_column).is_null(row);
            if indices_present != values_present {
                return Err(BorsukError::InvalidStorage(format!(
                    "segment record `{id}` must store both sparse_indices and sparse_values or neither"
                )));
            }
            if indices_present {
                Some((
                    primitive_list_optional_value::<UInt32Type>(
                        batch,
                        indices_column,
                        row,
                        "sparse_indices",
                    )?
                    .unwrap_or_default(),
                    primitive_list_optional_value::<Float32Type>(
                        batch,
                        values_column,
                        row,
                        "sparse_values",
                    )?
                    .unwrap_or_default(),
                ))
            } else {
                None
            }
        }
        (None, None) => None,
        _ => {
            return Err(BorsukError::InvalidStorage(
                "segment table must contain both sparse_indices and sparse_values columns"
                    .to_string(),
            ));
        }
    };

    match (dense, sparse) {
        (Some(vector), None) => {
            validate_segment_record_dimensions(id, dimensions, vector.len())?;
            validate_segment_record_vector_values(id, &vector)?;
            Ok((vector, StorageEncoding::Dense))
        }
        (None, Some((indices, values))) => {
            let vector = validate_sparse_encoding(id, dimensions, indices, values)?;
            Ok((vector, StorageEncoding::Sparse))
        }
        (Some(_), Some(_)) => Err(BorsukError::InvalidStorage(format!(
            "segment record `{id}` stores both dense and sparse vector encodings"
        ))),
        (None, None) => Err(BorsukError::InvalidStorage(format!(
            "segment record `{id}` stores neither dense nor sparse vector encoding"
        ))),
    }
}

fn validate_sparse_encoding(
    id: &RecordId,
    dimensions: usize,
    indices: Vec<u32>,
    values: Vec<f32>,
) -> Result<Vec<f32>> {
    let sparse = crate::SparseVector::new(indices, values)?;
    let mut vector = vec![0.0; dimensions];
    for (&index, &value) in sparse.indices().iter().zip(sparse.values()) {
        let position = usize::try_from(index).map_err(|_| {
            BorsukError::InvalidStorage(format!(
                "segment record `{id}` sparse index {index} does not fit usize"
            ))
        })?;
        if position >= dimensions {
            return Err(BorsukError::InvalidStorage(format!(
                "segment record `{id}` sparse index {index} is outside {dimensions} dimensions"
            )));
        }
        vector[position] = value;
    }
    validate_segment_record_vector_values(id, &vector)?;
    Ok(vector)
}

fn sparse_parts_from_dense(id: &RecordId, vector: &[f32]) -> Result<(Vec<u32>, Vec<f32>)> {
    let mut indices = Vec::new();
    let mut values = Vec::new();
    for (position, value) in vector.iter().copied().enumerate() {
        if value == 0.0 {
            continue;
        }
        let index = u32::try_from(position).map_err(|_| {
            BorsukError::InvalidRecordInput(format!(
                "record `{id}` sparse storage requires vector dimensions to fit u32"
            ))
        })?;
        indices.push(index);
        values.push(value);
    }
    Ok((indices, values))
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

fn validate_segment_record_text_terms(record: &VectorRecord) -> Result<()> {
    validate_text_terms(&record.id, &record.text_term_ids, &record.text_term_freqs)
}

fn validate_text_terms(record_id: &RecordId, term_ids: &[u32], term_freqs: &[u32]) -> Result<()> {
    if term_ids.is_empty() && term_freqs.is_empty() {
        return Ok(());
    }
    if term_ids.len() != term_freqs.len() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record `{record_id}` text_term_ids length {} must match text_term_freqs length {}",
            term_ids.len(),
            term_freqs.len()
        )));
    }
    if let Some(position) = term_freqs.iter().position(|freq| *freq == 0) {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record `{record_id}` text_term_freqs value at position {position} must be greater than zero"
        )));
    }
    if let Some(position) = term_ids
        .windows(2)
        .position(|window| window[0] >= window[1])
    {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record `{record_id}` text_term_ids must be strictly increasing; positions {position} and {} are out of order",
            position + 1
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
    fn segment_to_parquet_omits_sparse_and_text_columns_for_dense_plain_segment() {
        let mut segment = valid_segment();
        segment.records[0].storage = crate::StorageEncoding::Dense;

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();
        let schema = batch.schema();

        assert!(schema.field_with_name("vector").is_ok());
        assert!(schema.field_with_name("sparse_indices").is_err());
        assert!(schema.field_with_name("sparse_values").is_err());
        assert!(schema.field_with_name("text_term_ids").is_err());
        assert!(schema.field_with_name("text_term_freqs").is_err());
        let decoded = segment_from_parquet(&bytes).unwrap();
        assert_eq!(decoded.records[0].id, segment.records[0].id);
        assert_eq!(decoded.records[0].vector, segment.records[0].vector);
        assert_eq!(decoded.records[0].metadata, segment.records[0].metadata);
    }

    #[test]
    fn segment_to_parquet_includes_sparse_columns_when_any_record_is_sparse() {
        let mut segment = valid_segment();
        segment.records[0].vector = vec![0.0, 1.5];
        segment.records[0].storage = crate::StorageEncoding::Sparse;

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();
        let schema = batch.schema();

        assert!(schema.field_with_name("sparse_indices").is_ok());
        assert!(schema.field_with_name("sparse_values").is_ok());
        assert!(schema.field_with_name("text_term_ids").is_err());
        assert!(schema.field_with_name("text_term_freqs").is_err());
        assert_eq!(
            segment_from_parquet(&bytes).unwrap().records[0].vector,
            segment.records[0].vector
        );
    }

    #[test]
    fn segment_to_parquet_includes_text_columns_when_any_record_has_terms() {
        let mut segment = valid_segment();
        segment.records[0].storage = crate::StorageEncoding::Dense;
        segment.records[0].text_term_ids = vec![7, 11];
        segment.records[0].text_term_freqs = vec![2, 1];

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();
        let schema = batch.schema();

        assert!(schema.field_with_name("sparse_indices").is_err());
        assert!(schema.field_with_name("sparse_values").is_err());
        assert!(schema.field_with_name("text_term_ids").is_ok());
        assert!(schema.field_with_name("text_term_freqs").is_ok());
        assert_eq!(
            segment_from_parquet(&bytes).unwrap().records[0].text_term_ids,
            segment.records[0].text_term_ids
        );
        assert_eq!(
            segment_from_parquet(&bytes).unwrap().records[0].text_term_freqs,
            segment.records[0].text_term_freqs
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
    fn legacy_manifest_without_routing_page_fanout_uses_default() {
        let manifest_bytes = legacy_external_manifest_parquet_without_routing_page_fanout(2, 100);
        let routing_bytes = routing_to_parquet(&valid_manifest()).unwrap();

        let manifest = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap();

        assert_eq!(manifest.routing_page_fanout, DEFAULT_ROUTING_PAGE_FANOUT);
    }

    #[test]
    fn manifest_from_parquet_ignores_unknown_columns() {
        let mut expected = valid_manifest();
        expected.config.ram_budget_bytes = Some(4096);
        expected.next_generated_id = 17;
        expected.routing_max_level = 2;
        expected.routing_page_fanout = 64;
        expected.created_at = datetime_from_millis(1234).unwrap();
        let manifest_bytes =
            parquet_with_unknown_column_after_first(&manifest_to_parquet(&expected).unwrap());
        let routing_bytes = routing_to_parquet(&expected).unwrap();

        let manifest = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap();

        assert_eq!(manifest.version, expected.version);
        assert_eq!(manifest.config.uri, expected.config.uri);
        assert_eq!(manifest.config.metric, expected.config.metric);
        assert_eq!(manifest.config.dimensions, expected.config.dimensions);
        assert_eq!(
            manifest.config.segment_max_vectors,
            expected.config.segment_max_vectors
        );
        assert_eq!(
            manifest.config.ram_budget_bytes,
            expected.config.ram_budget_bytes
        );
        assert_eq!(manifest.next_generated_id, expected.next_generated_id);
        assert_eq!(manifest.routing_max_level, expected.routing_max_level);
        assert_eq!(manifest.routing_page_fanout, expected.routing_page_fanout);
        assert_eq!(manifest.created_at, expected.created_at);
    }

    #[test]
    fn manifest_metadata_from_parquet_ignores_unknown_columns() {
        let mut expected = valid_manifest();
        expected.config.ram_budget_bytes = Some(4096);
        expected.next_generated_id = 17;
        expected.routing_max_level = 2;
        expected.routing_page_fanout = 64;
        expected.created_at = datetime_from_millis(1234).unwrap();
        let manifest_bytes =
            parquet_with_unknown_column_after_first(&manifest_to_parquet(&expected).unwrap());

        let manifest = manifest_metadata_from_parquet(&manifest_bytes).unwrap();

        assert_eq!(manifest.version, expected.version);
        assert_eq!(manifest.config.uri, expected.config.uri);
        assert_eq!(manifest.config.metric, expected.config.metric);
        assert_eq!(manifest.config.dimensions, expected.config.dimensions);
        assert_eq!(
            manifest.config.segment_max_vectors,
            expected.config.segment_max_vectors
        );
        assert_eq!(
            manifest.config.ram_budget_bytes,
            expected.config.ram_budget_bytes
        );
        assert_eq!(manifest.next_generated_id, expected.next_generated_id);
        assert_eq!(manifest.routing_max_level, expected.routing_max_level);
        assert_eq!(manifest.routing_page_fanout, expected.routing_page_fanout);
        assert_eq!(manifest.created_at, expected.created_at);
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
    fn pivots_from_parquet_ignores_unknown_columns() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: "pivot".to_string(),
            ordinal: 7,
            vector: vec![1.0, -1.0],
        }];
        let bytes = parquet_with_unknown_column_after_first(&pivots_to_parquet(&manifest).unwrap());

        let pivots = pivots_from_parquet(&bytes, 2, manifest.version).unwrap();

        assert_eq!(pivots.len(), 1);
        assert_eq!(pivots[0].id, "pivot");
        assert_eq!(pivots[0].ordinal, 7);
        assert_eq!(pivots[0].vector, vec![1.0, -1.0]);
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
    fn routing_from_parquet_ignores_unknown_columns() {
        let mut segment = valid_segment_summary();
        segment.created_at = datetime_from_millis(1234).unwrap();
        let manifest = manifest_with_segment(segment.clone());
        let bytes =
            parquet_with_unknown_column_after_first(&routing_to_parquet(&manifest).unwrap());

        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries, vec![segment]);
    }

    #[test]
    fn routing_layer_page_from_parquet_ignores_unknown_columns() {
        let mut segment = valid_segment_summary();
        segment.created_at = datetime_from_millis(1234).unwrap();
        let manifest = manifest_with_segment(segment.clone());
        let bytes = parquet_with_unknown_column_after_first(
            &routing_layer_page_to_parquet(&manifest, 0, 0, 0, &manifest.segments).unwrap(),
        );

        let summaries = routing_layer_page_from_parquet(&bytes, manifest.version, 0, 0, 2).unwrap();

        assert_eq!(summaries, vec![segment]);
    }

    #[test]
    fn routing_layer_page_index_from_parquet_ignores_unknown_columns() {
        let manifest = valid_manifest();
        let page_ref = valid_routing_layer_page_ref();
        let bytes = parquet_with_unknown_column_after_first(
            &routing_layer_page_index_to_parquet(&manifest, 0, std::slice::from_ref(&page_ref))
                .unwrap(),
        );

        let page_refs = routing_layer_page_index_from_parquet(&bytes, manifest.version, 0).unwrap();

        assert_eq!(page_refs, vec![page_ref]);
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
            extra_vectors: BTreeMap::new(),
            extra_sparse: BTreeMap::new(),
            storage: crate::StorageEncoding::Auto,
            text: None,
            text_term_ids: Vec::new(),
            text_term_freqs: Vec::new(),
            metadata: crate::Metadata::new(),
            generation: 0,
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
                text: false,
                named_vectors: Default::default(),
            },
            text_tokenizer: None,
            segments: Vec::new(),
            pivots: Vec::new(),
            next_generated_id: 0,
            routing_max_level: 0,
            routing_page_fanout: DEFAULT_ROUTING_PAGE_FANOUT,
            graph_neighbors: DEFAULT_GRAPH_NEIGHBORS,
            tombstone: None,
            created_at: Utc::now(),
        }
    }

    fn manifest_with_segment(segment: SegmentSummary) -> Manifest {
        let mut manifest = valid_manifest();
        manifest.segments = vec![segment];
        manifest
    }

    #[test]
    fn metadata_round_trips_through_vector_records() {
        use crate::metadata::MetaValue;
        let meta = crate::Metadata::from([
            ("year".to_string(), MetaValue::Int(2021)),
            ("genre".to_string(), MetaValue::Str("comedy".to_string())),
            (
                "tags".to_string(),
                MetaValue::List(vec![MetaValue::Str("a".to_string())]),
            ),
        ]);
        let records = vec![
            VectorRecord::new("a", vec![1.0, 0.0]).with_metadata(meta.clone()),
            VectorRecord::new("b", vec![0.0, 1.0]),
        ];
        let bytes = vector_records_to_parquet(&records, 2).unwrap();
        let decoded = vector_records_from_parquet(&bytes, 2).unwrap();
        assert_eq!(decoded[0].metadata, meta);
        assert!(decoded[1].metadata.is_empty());
    }

    #[test]
    fn metadata_round_trips_through_segment() {
        use crate::metadata::MetaValue;
        let mut segment = valid_segment();
        segment.records[0].metadata = crate::Metadata::from([("k".to_string(), MetaValue::Int(7))]);
        let bytes = segment_to_parquet(&segment).unwrap();
        let decoded = segment_from_parquet(&bytes).unwrap();
        assert_eq!(
            decoded.records[0].metadata,
            crate::Metadata::from([("k".to_string(), MetaValue::Int(7))])
        );
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
            metadata_stats: crate::MetadataStats::default(),
            sparse_encoded: 0,
            dense_encoded: 1,
            text_doc_count: 0,
            text_total_doc_length: 0,
            created_at: Utc::now(),
        }
    }

    fn valid_vector_signature_bloom() -> Vec<u8> {
        let vector = [0.0_f32, 0.0_f32];
        crate::manifest::segment_vector_signature_bloom([vector.as_slice()])
    }

    fn valid_routing_layer_page_ref() -> RoutingLayerPageRef {
        RoutingLayerPageRef {
            routing_level: 0,
            page_ordinal: 0,
            path: format!("routing/pages/L0/00/page-{VALID_SEGMENT_CHECKSUM}.parquet"),
            checksum: VALID_SEGMENT_CHECKSUM.to_string(),
            page_segments: 1,
            leaf_segments: 1,
            leaf_pages: 1,
            routing_pages: 1,
            dimensions: 2,
            centroid: vec![0.0, 0.0],
            radius: 0.0,
            bounds_min: vec![0.0, 0.0],
            bounds_max: vec![0.0, 0.0],
            id_bloom: crate::manifest::segment_id_bloom(["record"]),
            vector_signature_bloom: valid_vector_signature_bloom(),
            level_mask: 1,
            page_records: 1,
            page_segment_bytes: 123,
            page_graph_bytes: 45,
            page_sparse_encoded_vectors: 0,
            page_dense_encoded_vectors: 1,
        }
    }

    fn parquet_with_unknown_column_after_first(bytes: &[u8]) -> Vec<u8> {
        let batch = first_batch(bytes, "table").unwrap();
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.insert(1, Field::new("future_column", DataType::Utf8, false));
        let mut columns = batch.columns().to_vec();
        columns.insert(
            1,
            array(StringArray::from_iter_values(
                (0..batch.num_rows()).map(|_| "ignored"),
            )),
        );
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        write_batch(batch).unwrap()
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
                extra_vectors: BTreeMap::new(),
                extra_sparse: BTreeMap::new(),
                storage: crate::StorageEncoding::Auto,
                text: None,
                text_term_ids: Vec::new(),
                text_term_freqs: Vec::new(),
                metadata: crate::Metadata::new(),
                generation: 0,
            }],
            routing_codes: vec![0.0],
            pq_codes: vec![vec![128, 128]],
            pq_min: vec![0.0, 0.0],
            pq_max: vec![0.0, 0.0],
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
                array(BooleanArray::from_iter([false])),
                array(StringArray::from_iter([None::<String>])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt8Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([
                    DEFAULT_ROUTING_PAGE_FANOUT as u64
                ])),
                array(UInt64Array::from_iter_values([
                    DEFAULT_GRAPH_NEIGHBORS as u64
                ])),
                array(StringArray::from_iter([None::<String>])),
                array(StringArray::from_iter([None::<String>])),
                array(UInt64Array::from_iter([None::<u64>])),
                array(BinaryArray::from_iter([None::<&[u8]>])),
                array(Int64Array::from_iter([None::<i64>])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn legacy_external_manifest_parquet_without_routing_page_fanout(
        dimensions: u64,
        segment_max_vectors: u64,
    ) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
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
        ]));
        let batch = RecordBatch::try_new(
            schema,
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
                array(BinaryArray::from_iter_values(
                    ids.iter().map(|_| Vec::<u8>::new()),
                )),
                array(UInt32Array::from_iter_values(ids.iter().map(|_| 0))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
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
                array(BinaryArray::from_iter_values([Vec::<u8>::new()])),
                array(UInt32Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
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

    #[test]
    fn lean_decode_and_row_selective_vectors_match_full_decode() {
        let segment = Segment::from_records(
            "seg".to_string(),
            0,
            VectorMetric::Euclidean,
            2,
            vec![
                VectorRecord::new("r0", vec![0.0, 0.0]),
                VectorRecord::new("r1", vec![1.0, 0.0]),
                VectorRecord::new("r2", vec![0.0, 1.0]),
                VectorRecord::new("r3", vec![1.0, 1.0]),
            ],
        )
        .unwrap();
        let bytes = segment_to_parquet(&segment).unwrap();
        assert!(segment_has_persisted_pq_bounds(&bytes).unwrap());

        let full = segment_from_parquet(&bytes).unwrap();
        let lean = lean_segment_from_parquet(&bytes).unwrap();

        // Lean decode carries codes and persisted PQ bounds, but no vectors.
        assert_eq!(lean.pq_codes, full.pq_codes);
        assert_eq!(lean.pq_min, full.pq_min);
        assert_eq!(lean.pq_max, full.pq_max);
        for (lean_record, full_record) in lean.records.iter().zip(&full.records) {
            assert_eq!(lean_record.id, full_record.id);
            assert!(lean_record.vector.is_empty());
        }

        // The query quantizes identically from persisted bounds (the fix).
        let query = vec![0.4, 0.7];
        assert_eq!(
            crate::segment::pq_code_for_query(&lean, &query).unwrap(),
            crate::segment::pq_code_for_query(&full, &query).unwrap(),
        );

        // Row-selective vectors return exactly the requested rows.
        let vectors = segment_vectors_for_rows(&bytes, &[3, 0, 3], 2).unwrap();
        assert_eq!(vectors.len(), 2);
        assert_eq!(vectors[&0], full.records[0].vector);
        assert_eq!(vectors[&3], full.records[3].vector);
    }

    fn external_segment_parquet_with_records<const N: usize>(
        records: [(&str, [f32; 2]); N],
    ) -> Vec<u8> {
        let schema = segment_schema(2, false, false, false);
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
                    records.iter().map(|_| centroid.as_slice()),
                    2,
                )),
                array(fixed_f32_array(
                    records.iter().map(|_| centroid.as_slice()),
                    2,
                )),
                array(fixed_f32_array(
                    records.iter().map(|(_, vector)| vector.as_slice()),
                    2,
                )),
                array(BinaryArray::from_iter_values(
                    records.iter().map(|_| Vec::<u8>::new()),
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

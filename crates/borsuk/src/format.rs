use std::{str::FromStr, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    UInt8Array, UInt16Array, UInt64Array,
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
    manifest::{Manifest, PivotSummary, SegmentSummary},
    metric::VectorMetric,
    record::VectorRecord,
    segment::{GraphEdge, Segment, SegmentGraph},
};

const CURRENT_MAGIC: &[u8; 4] = b"BORS";
const CURRENT_VERSION: u16 = 1;
const CURRENT_CHECKSUM_LEN: usize = 32;
const CURRENT_LEN: usize = 4 + 2 + 8 + CURRENT_CHECKSUM_LEN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentPointer {
    pub version: u64,
    pub metadata_checksum: [u8; CURRENT_CHECKSUM_LEN],
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

pub(crate) fn encode_current(
    version: u64,
    metadata_checksum: [u8; CURRENT_CHECKSUM_LEN],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CURRENT_LEN);
    bytes.extend_from_slice(CURRENT_MAGIC);
    bytes.extend_from_slice(&CURRENT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&metadata_checksum);
    bytes
}

pub(crate) fn decode_current(bytes: &[u8]) -> Result<CurrentPointer> {
    if bytes.len() != CURRENT_LEN {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT must be {CURRENT_LEN} bytes, got {}",
            bytes.len()
        )));
    }

    if &bytes[0..4] != CURRENT_MAGIC {
        return Err(BorsukError::InvalidStorage(
            "CURRENT magic header is invalid".to_string(),
        ));
    }

    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported CURRENT version {version}"
        )));
    }

    let version = u64::from_le_bytes([
        bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13],
    ]);
    let mut metadata_checksum = [0_u8; CURRENT_CHECKSUM_LEN];
    metadata_checksum.copy_from_slice(&bytes[14..46]);

    Ok(CurrentPointer {
        version,
        metadata_checksum,
    })
}

fn update_current_hasher(hasher: &mut blake3::Hasher, label: &[u8], bytes: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

pub(crate) fn manifest_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
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

    let metric = VectorMetric::from_str(string_value(&batch, 3, 0, "metric")?)?;
    let manifest = Manifest {
        version: primitive_value::<UInt64Type>(&batch, 1, 0, "version")?,
        config: IndexConfig {
            uri: string_value(&batch, 2, 0, "uri")?.to_string(),
            metric,
            dimensions: usize_from_u64(primitive_value::<UInt64Type>(&batch, 4, 0, "dimensions")?)?,
            segment_max_vectors: usize_from_u64(primitive_value::<UInt64Type>(
                &batch,
                5,
                0,
                "segment_max_vectors",
            )?)?,
            ram_budget_bytes: if batch.num_columns() > 7 {
                primitive_optional_value::<UInt64Type>(&batch, 7, 0, "ram_budget_bytes")?
            } else {
                None
            },
        },
        segments: routing_from_parquet(routing_bytes)?,
        pivots: Vec::new(),
        created_at: datetime_from_millis(primitive_value::<Int64Type>(
            &batch,
            6,
            0,
            "created_at_ms",
        )?)?,
    };

    Ok(manifest)
}

pub(crate) fn routing_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = routing_schema(dimensions);
    let segments = &manifest.segments;

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
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn pivots_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = pivots_schema(dimensions);
    let pivots = &manifest.pivots;

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

pub(crate) fn pivots_from_parquet(bytes: &[u8], dimensions: usize) -> Result<Vec<PivotSummary>> {
    let mut pivots = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported pivot table version {format_version}"
                )));
            }

            primitive_value::<UInt64Type>(&batch, 1, row, "manifest_version")?;
            let ordinal =
                usize_from_u64(primitive_value::<UInt64Type>(&batch, 2, row, "ordinal")?)?;
            let id = string_value(&batch, 3, row, "pivot_id")?.to_string();
            let vector = fixed_f32_value(&batch, 4, row, "vector")?;
            if vector.len() != dimensions {
                return Err(BorsukError::InvalidStorage(format!(
                    "pivot vector has {} dimensions, expected {dimensions}",
                    vector.len()
                )));
            }

            pivots.push(PivotSummary {
                id,
                ordinal,
                vector,
            });
        }
    }

    Ok(pivots)
}

pub(crate) fn routing_from_parquet(bytes: &[u8]) -> Result<Vec<SegmentSummary>> {
    let mut summaries = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing table version {format_version}"
                )));
            }

            summaries.push(SegmentSummary {
                id: string_value(&batch, 2, row, "id")?.to_string(),
                level: primitive_value::<UInt8Type>(&batch, 3, row, "level")?,
                path: string_value(&batch, 4, row, "path")?.to_string(),
                object_count: usize_from_u64(primitive_value::<UInt64Type>(
                    &batch,
                    5,
                    row,
                    "object_count",
                )?)?,
                dimensions: usize_from_u64(primitive_value::<UInt64Type>(
                    &batch,
                    6,
                    row,
                    "dimensions",
                )?)?,
                centroid: fixed_f32_value(&batch, 7, row, "centroid")?,
                radius: primitive_value::<Float32Type>(&batch, 8, row, "radius")?,
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
                created_at: datetime_from_millis(primitive_value::<Int64Type>(
                    &batch,
                    14,
                    row,
                    "created_at_ms",
                )?)?,
            });
        }
    }

    Ok(summaries)
}

pub(crate) fn segment_to_parquet(segment: &Segment) -> Result<Vec<u8>> {
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
            array(StringArray::from_iter_values(
                records.iter().map(|record| record.id.as_str()),
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
    let mut metadata = None::<SegmentMetadata>;

    for batch in read_batches(bytes)? {
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

            if let Some(metadata) = &metadata {
                if metadata != &row_metadata {
                    return Err(BorsukError::InvalidStorage(
                        "segment metadata differs between rows".to_string(),
                    ));
                }
            } else {
                metadata = Some(row_metadata);
            }

            let routing_code = primitive_value::<Float32Type>(&batch, 8, row, "routing_code")?;
            records.push(VectorRecord::new(
                string_value(&batch, 9, row, "record_id")?,
                fixed_f32_value(&batch, 10, row, "vector")?,
            ));
            routing_codes.push(routing_code);
        }
    }

    let metadata = metadata.ok_or_else(|| {
        BorsukError::InvalidStorage("segment table must contain at least one row".to_string())
    })?;

    Ok(Segment {
        id: metadata.id,
        level: metadata.level,
        metric: metadata.metric,
        dimensions: metadata.dimensions,
        centroid: metadata.centroid,
        radius: metadata.radius,
        records,
        routing_codes,
        created_at: metadata.created_at,
    })
}

pub(crate) fn graph_to_parquet(graph: &SegmentGraph) -> Result<Vec<u8>> {
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
            array(StringArray::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|edge| edge.source_record_id.as_str()),
            )),
            array(StringArray::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|edge| edge.neighbor_record_id.as_str()),
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
) -> Result<SegmentGraph> {
    let mut edges = Vec::new();
    let mut metadata = None::<GraphMetadata>;

    for batch in read_batches(bytes)? {
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

            edges.push(GraphEdge {
                source_record_id: string_value(&batch, 4, row, "source_record_id")?.to_string(),
                neighbor_record_id: string_value(&batch, 5, row, "neighbor_record_id")?.to_string(),
                distance: primitive_value::<Float32Type>(&batch, 6, row, "neighbor_distance")?,
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
        Field::new("record_id", DataType::Utf8, false),
        fixed_f32_field("vector", dimensions),
    ]))
}

fn graph_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("source_record_id", DataType::Utf8, false),
        Field::new("neighbor_record_id", DataType::Utf8, false),
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

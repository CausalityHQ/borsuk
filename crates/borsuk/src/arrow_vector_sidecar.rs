//! Standard Apache Arrow IPC exact-vector sidecar.

use std::{
    collections::HashMap,
    mem::size_of,
    ops::Range,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    Int8Array, RecordBatch, UInt8Array, UInt16Array, UInt64Array,
    types::{Float16Type, Float32Type, Int8Type, UInt8Type, UInt16Type},
};
use arrow_buffer::Buffer;
use arrow_ipc::{
    Block, CompressionType, MessageHeader, MetadataVersion,
    convert::fb_to_schema,
    reader::{FileDecoder, read_footer_length},
    root_as_footer, root_as_message,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};

use crate::{
    BorsukError, Result,
    mutation::{MutationStamp, MutationVersion},
    record::{RecordId, SidecarCompression, VectorElementType, VectorRecord},
};

const TARGET_BATCH_VECTOR_BYTES: usize = 64 * 1024;
const MAX_BATCH_ROWS: usize = 1024;
const FOOTER_BASE_ALLOWANCE: usize = 16 * 1024;
const MAX_DECODED_ARROW_BUFFER_BYTES: usize = 256 * 1024 * 1024;
const META_DIMENSIONS: &str = "borsuk.vector.dimensions";
const META_ROW_COUNT: &str = "borsuk.vector.row_count";
const META_BATCH_ROWS: &str = "borsuk.vector.batch_rows";
const META_ELEMENT_TYPE: &str = "borsuk.vector.element_type";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactSidecarRow {
    pub(crate) id: RecordId,
    pub(crate) generation: u64,
    pub(crate) mutation_stamp: Option<MutationStamp>,
    pub(crate) vector: Vec<f32>,
}

#[derive(Debug, Clone)]
pub(crate) struct SidecarIndex {
    schema: Arc<Schema>,
    version: MetadataVersion,
    blocks: Vec<Block>,
    dimensions: usize,
    row_count: usize,
    batch_rows: usize,
    element_type: VectorElementType,
    mutation_stamped: bool,
}

impl SidecarIndex {
    pub(crate) fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.blocks.capacity().saturating_mul(size_of::<Block>()))
            .saturating_add(
                self.schema
                    .fields()
                    .iter()
                    .map(|field| field.name().capacity())
                    .sum::<usize>(),
            )
    }

    pub(crate) fn row_range(&self, row: usize) -> Result<Range<u64>> {
        if row >= self.row_count {
            return Err(BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar row {row} is outside {} rows",
                self.row_count
            )));
        }
        let block_index = row / self.batch_rows;
        block_range(self.blocks.get(block_index).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar has no record batch for row {row}"
            ))
        })?)
    }

    pub(crate) fn batch_rows_for(&self, row: usize) -> Result<Range<usize>> {
        if row >= self.row_count {
            return Err(BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar row {row} is outside {} rows",
                self.row_count
            )));
        }
        let start = row / self.batch_rows * self.batch_rows;
        Ok(start..(start + self.batch_rows).min(self.row_count))
    }

    #[cfg(test)]
    fn decode_record(&self, row: usize, stored: &[u8]) -> Result<ExactSidecarRow> {
        self.decode_records(&[row], stored)?
            .pop()
            .map(|(_, record)| record)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "Arrow vector sidecar decoded no record for row {row}"
                ))
            })
    }

    pub(crate) fn decode_records(
        &self,
        rows: &[usize],
        stored: &[u8],
    ) -> Result<Vec<(usize, ExactSidecarRow)>> {
        let Some(&first_row) = rows.first() else {
            return Ok(Vec::new());
        };
        let expected_range = self.row_range(first_row)?;
        for &row in &rows[1..] {
            if self.row_range(row)? != expected_range {
                return Err(BorsukError::InvalidStorage(
                    "Arrow vector sidecar batch decode rows span multiple record batches"
                        .to_string(),
                ));
            }
        }
        let expected_len =
            usize::try_from(expected_range.end - expected_range.start).map_err(|_| {
                BorsukError::InvalidStorage("Arrow vector sidecar block exceeds usize".to_string())
            })?;
        if stored.len() != expected_len {
            return Err(BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar block for row {first_row} has {} bytes, expected {expected_len}",
                stored.len()
            )));
        }

        let block_index = first_row / self.batch_rows;
        let block = self.blocks.get(block_index).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar has no record batch for row {first_row}"
            ))
        })?;
        validate_record_batch_block(
            block,
            stored,
            self.batch_rows_for(first_row)?.len(),
            self.dimensions,
        )?;
        let decoder = FileDecoder::new(Arc::clone(&self.schema), self.version);
        // `arrow-ipc` 59 may panic instead of returning `ArrowError` when
        // untrusted record-batch metadata contains a buffer range outside the
        // supplied body. Sidecars live in object storage and can be truncated
        // or corrupted independently, so contain that third-party panic at the
        // decode boundary and expose it as a normal storage error.
        let decoded = catch_unwind(AssertUnwindSafe(|| {
            decoder.read_record_batch(block, &Buffer::from(stored.to_vec()))
        }))
        .map_err(|_| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar record batch has invalid buffer ranges".to_string(),
            )
        })?;
        let batch = decoded?.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar record block decoded no batch".to_string(),
            )
        })?;

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "Arrow vector sidecar record_id is not Binary".to_string(),
                )
            })?;
        let generations = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "Arrow vector sidecar generation is not UInt64".to_string(),
                )
            })?;
        rows.iter()
            .map(|&row| {
                let row_in_batch = row % self.batch_rows;
                if row_in_batch >= batch.num_rows() {
                    return Err(BorsukError::InvalidStorage(format!(
                        "Arrow vector sidecar row {row} exceeds decoded batch of {} rows",
                        batch.num_rows()
                    )));
                }
                let mutation_stamp = if self.mutation_stamped {
                    let hlcs = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "Arrow vector sidecar mutation_hlc is not UInt64".to_string(),
                            )
                        })?;
                    let writers = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "Arrow vector sidecar mutation_writer is not FixedSizeBinary"
                                    .to_string(),
                            )
                        })?;
                    let digests = batch
                        .column(4)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "Arrow vector sidecar mutation_digest is not FixedSizeBinary"
                                    .to_string(),
                            )
                        })?;
                    Some(MutationStamp::new(
                        MutationVersion::from_parts(
                            hlcs.value(row_in_batch),
                            writers.value(row_in_batch).try_into().map_err(|_| {
                                BorsukError::InvalidStorage(
                                    "Arrow vector sidecar mutation writer must contain 16 bytes"
                                        .to_string(),
                                )
                            })?,
                        ),
                        digests.value(row_in_batch).try_into().map_err(|_| {
                            BorsukError::InvalidStorage(
                                "Arrow vector sidecar mutation digest must contain 32 bytes"
                                    .to_string(),
                            )
                        })?,
                    ))
                } else {
                    None
                };
                let vector = decode_vector(
                    batch.column(self.vector_column()).as_ref(),
                    row_in_batch,
                    self.dimensions,
                    self.element_type,
                )?;
                if vector.len() != self.dimensions {
                    return Err(BorsukError::InvalidStorage(format!(
                        "Arrow vector sidecar decoded {} dimensions, expected {}",
                        vector.len(),
                        self.dimensions
                    )));
                }

                Ok((
                    row,
                    ExactSidecarRow {
                        id: RecordId::from_bytes(ids.value(row_in_batch)),
                        generation: generations.value(row_in_batch),
                        mutation_stamp,
                        vector,
                    },
                ))
            })
            .collect()
    }

    fn decode_vector_batch(&self, first_row: usize, stored: &[u8]) -> Result<Vec<Vec<f32>>> {
        let expected_range = self.row_range(first_row)?;
        let expected_len =
            usize::try_from(expected_range.end - expected_range.start).map_err(|_| {
                BorsukError::InvalidStorage("Arrow vector sidecar block exceeds usize".to_string())
            })?;
        if stored.len() != expected_len {
            return Err(BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar block for row {first_row} has {} bytes, expected {expected_len}",
                stored.len()
            )));
        }
        let block_index = first_row / self.batch_rows;
        let block = self.blocks.get(block_index).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar has no record batch for row {first_row}"
            ))
        })?;
        validate_record_batch_block(
            block,
            stored,
            self.batch_rows_for(first_row)?.len(),
            self.dimensions,
        )?;
        let decoder = FileDecoder::new(Arc::clone(&self.schema), self.version);
        let decoded = catch_unwind(AssertUnwindSafe(|| {
            decoder.read_record_batch(block, &Buffer::from(stored.to_vec()))
        }))
        .map_err(|_| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar record batch has invalid buffer ranges".to_string(),
            )
        })?;
        let batch = decoded?.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar record block decoded no batch".to_string(),
            )
        })?;
        (0..batch.num_rows())
            .map(|row| {
                decode_vector(
                    batch.column(self.vector_column()).as_ref(),
                    row,
                    self.dimensions,
                    self.element_type,
                )
            })
            .collect()
    }

    fn vector_column(&self) -> usize {
        if self.mutation_stamped { 5 } else { 2 }
    }
}

fn validate_record_batch_block(
    block: &Block,
    stored: &[u8],
    expected_rows: usize,
    dimensions: usize,
) -> Result<()> {
    let metadata_len = usize::try_from(block.metaDataLength()).map_err(|_| {
        BorsukError::InvalidStorage(
            "Arrow vector sidecar block has negative metadata length".to_string(),
        )
    })?;
    let body_len = usize::try_from(block.bodyLength()).map_err(|_| {
        BorsukError::InvalidStorage(
            "Arrow vector sidecar block has negative body length".to_string(),
        )
    })?;
    if metadata_len
        .checked_add(body_len)
        .is_none_or(|length| length != stored.len())
    {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar block lengths do not match stored bytes".to_string(),
        ));
    }
    if metadata_len < 4 {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar record metadata is truncated".to_string(),
        ));
    }

    let continuation = stored[..4] == [0xFF; 4];
    let prefix_len = if continuation { 8 } else { 4 };
    if metadata_len < prefix_len {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar record metadata prefix is truncated".to_string(),
        ));
    }
    let message_len_offset = if continuation { 4 } else { 0 };
    let message_len = u32::from_le_bytes(
        stored[message_len_offset..message_len_offset + 4]
            .try_into()
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "Arrow vector sidecar record metadata length is truncated".to_string(),
                )
            })?,
    ) as usize;
    let message_end = prefix_len.checked_add(message_len).ok_or_else(|| {
        BorsukError::InvalidStorage(
            "Arrow vector sidecar record metadata length overflows".to_string(),
        )
    })?;
    if message_end > metadata_len {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar record metadata exceeds its block".to_string(),
        ));
    }

    let message = root_as_message(&stored[prefix_len..message_end]).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "Arrow vector sidecar record metadata is invalid: {error}"
        ))
    })?;
    if message.header_type() != MessageHeader::RecordBatch {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar block is not a record batch".to_string(),
        ));
    }
    if usize::try_from(message.bodyLength()).ok() != Some(body_len) {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar message body length does not match its block".to_string(),
        ));
    }
    let batch = message.header_as_record_batch().ok_or_else(|| {
        BorsukError::InvalidStorage("Arrow vector sidecar record metadata has no batch".to_string())
    })?;
    if usize::try_from(batch.length()).ok() != Some(expected_rows) {
        return Err(BorsukError::InvalidStorage(format!(
            "Arrow vector sidecar batch has {} rows, expected {expected_rows}",
            batch.length()
        )));
    }
    let max_node_values = expected_rows
        .checked_mul(dimensions.max(1))
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar expected value count overflows".to_string(),
            )
        })?;
    if batch.nodes().is_some_and(|nodes| {
        nodes.iter().any(|node| {
            node.length() < 0
                || node.null_count() < 0
                || node.null_count() > node.length()
                || usize::try_from(node.length()).map_or(true, |length| length > max_node_values)
        })
    }) {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar has invalid field-node lengths".to_string(),
        ));
    }

    let body = &stored[metadata_len..];
    let compressed = batch.compression().is_some();
    let buffers = batch.buffers().ok_or_else(|| {
        BorsukError::InvalidStorage(
            "Arrow vector sidecar record metadata has no buffers".to_string(),
        )
    })?;
    for buffer in buffers {
        let offset = usize::try_from(buffer.offset()).map_err(|_| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar buffer has a negative offset".to_string(),
            )
        })?;
        let length = usize::try_from(buffer.length()).map_err(|_| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar buffer has a negative length".to_string(),
            )
        })?;
        let end = offset.checked_add(length).ok_or_else(|| {
            BorsukError::InvalidStorage("Arrow vector sidecar buffer range overflows".to_string())
        })?;
        let bytes = body.get(offset..end).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar buffer exceeds the record body".to_string(),
            )
        })?;
        if compressed && !bytes.is_empty() {
            let decoded_len = i64::from_le_bytes(
                bytes
                    .get(..8)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "Arrow vector sidecar compressed buffer header is truncated"
                                .to_string(),
                        )
                    })?
                    .try_into()
                    .map_err(|_| {
                        BorsukError::InvalidStorage(
                            "Arrow vector sidecar compressed buffer header is invalid".to_string(),
                        )
                    })?,
            );
            if decoded_len < -1
                || usize::try_from(decoded_len)
                    .is_ok_and(|length| length > MAX_DECODED_ARROW_BUFFER_BYTES)
            {
                return Err(BorsukError::InvalidStorage(
                    "Arrow vector sidecar compressed buffer declares an unsafe decoded length"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn encode_vector_sidecar(vectors: &[Vec<f32>], dimensions: usize) -> Result<Vec<u8>> {
    let records = vectors
        .iter()
        .enumerate()
        .map(|(row, vector)| VectorRecord::new(format!("row-{row}"), vector.clone()))
        .collect::<Vec<_>>();
    encode_record_sidecar_with(&records, dimensions, SidecarCompression::default())
}

pub(crate) fn decode_all(bytes: &[u8], expected_dimensions: usize) -> Result<Vec<Vec<f32>>> {
    let index = parse(bytes)?;
    if index.dimensions != expected_dimensions {
        return Err(BorsukError::DimensionMismatch {
            expected: expected_dimensions,
            actual: index.dimensions,
        });
    }
    let mut vectors = Vec::with_capacity(index.row_count);
    let mut first_row = 0;
    while first_row < index.row_count {
        let end_row = (first_row + index.batch_rows).min(index.row_count);
        let range = index.row_range(first_row)?;
        let stored = bytes
            .get(range.start as usize..range.end as usize)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "Arrow vector sidecar record batch is outside the object".to_string(),
                )
            })?;
        vectors.extend(index.decode_vector_batch(first_row, stored)?);
        first_row = end_row;
    }
    Ok(vectors)
}

pub(crate) fn max_index_tail_len(
    row_count: usize,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<u64> {
    let batch_rows = recommended_batch_rows(dimensions, element_type)?;
    let block_count = row_count.div_ceil(batch_rows);
    let block_bytes = block_count.checked_mul(size_of::<Block>()).ok_or_else(|| {
        BorsukError::InvalidStorage("Arrow vector sidecar footer size overflows".to_string())
    })?;
    u64::try_from(
        FOOTER_BASE_ALLOWANCE
            .checked_add(block_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "Arrow vector sidecar footer allowance overflows".to_string(),
                )
            })?,
    )
    .map_err(|_| BorsukError::InvalidStorage("Arrow vector sidecar footer exceeds u64".to_string()))
}

pub(crate) fn parse_tail(tail: &[u8], expected_rows: usize) -> Result<SidecarIndex> {
    parse_tail_impl(tail, Some(expected_rows))
}

pub(crate) fn parse(bytes: &[u8]) -> Result<SidecarIndex> {
    parse_tail_impl(bytes, None)
}

#[cfg(test)]
pub(crate) fn encode_record_sidecar_with(
    records: &[VectorRecord],
    dimensions: usize,
    compression: SidecarCompression,
) -> Result<Vec<u8>> {
    encode_record_sidecar_typed_with(records, dimensions, VectorElementType::Float32, compression)
}

pub(crate) fn encode_record_sidecar_typed_with(
    records: &[VectorRecord],
    dimensions: usize,
    element_type: VectorElementType,
    compression: SidecarCompression,
) -> Result<Vec<u8>> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar requires non-zero dimensions".to_string(),
        ));
    }
    for (row, record) in records.iter().enumerate() {
        if record.vector.len() != dimensions {
            return Err(BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar row {row} has {} dimensions, expected {dimensions}",
                record.vector.len()
            )));
        }
        element_type.canonicalize(&record.vector).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar row {row} cannot be encoded as {element_type}: {error}"
            ))
        })?;
    }
    let stamped_rows = records
        .iter()
        .filter(|record| record.mutation_stamp().is_some())
        .count();
    if stamped_rows != 0 && stamped_rows != records.len() {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar cannot mix stamped and unstamped records".to_string(),
        ));
    }
    let mutation_stamped = stamped_rows == records.len();

    let batch_rows = recommended_batch_rows(dimensions, element_type)?;
    let metadata = HashMap::from([
        (META_DIMENSIONS.to_string(), dimensions.to_string()),
        (META_ROW_COUNT.to_string(), records.len().to_string()),
        (META_BATCH_ROWS.to_string(), batch_rows.to_string()),
        (
            META_ELEMENT_TYPE.to_string(),
            element_type.as_str().to_string(),
        ),
    ]);
    let mut fields = vec![
        Field::new("record_id", DataType::Binary, false),
        Field::new("generation", DataType::UInt64, false),
    ];
    if mutation_stamped {
        fields.extend([
            Field::new("mutation_hlc", DataType::UInt64, false),
            Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
            Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
        ]);
    }
    fields.push(Field::new(
        "vector",
        vector_data_type(element_type, dimensions)?,
        false,
    ));
    let schema = Schema::new_with_metadata(fields, metadata);
    let write_options = match compression {
        SidecarCompression::Uncompressed => IpcWriteOptions::default(),
        SidecarCompression::Zstd => {
            IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?
        }
    };

    let mut output = Vec::new();
    {
        let mut writer = FileWriter::try_new_with_options(&mut output, &schema, write_options)?;
        for rows in records.chunks(batch_rows) {
            let ids = Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|record| record.id.as_bytes()),
            ));
            let generations = Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|record| record.generation),
            ));
            let mut columns: Vec<Arc<dyn Array>> = vec![ids, generations];
            if mutation_stamped {
                columns.extend([
                    Arc::new(UInt64Array::from_iter_values(rows.iter().map(|record| {
                        record
                            .mutation_stamp()
                            .expect("all rows were validated as stamped")
                            .version()
                            .hlc()
                    }))) as Arc<dyn Array>,
                    Arc::new(FixedSizeBinaryArray::try_from_iter(rows.iter().map(
                        |record| {
                            record
                                .mutation_stamp()
                                .expect("all rows were validated as stamped")
                                .version()
                                .writer()
                        },
                    ))?) as Arc<dyn Array>,
                    Arc::new(FixedSizeBinaryArray::try_from_iter(rows.iter().map(
                        |record| {
                            record
                                .mutation_stamp()
                                .expect("all rows were validated as stamped")
                                .digest()
                        },
                    ))?) as Arc<dyn Array>,
                ]);
            }
            columns.push(encode_vector_array(rows, dimensions, element_type)?);
            let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    Ok(output)
}

pub(crate) fn recommended_batch_rows(
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<usize> {
    let row_bytes = match element_type {
        VectorElementType::Float32 => dimensions.checked_mul(size_of::<f32>()),
        VectorElementType::Float16 | VectorElementType::BFloat16 => {
            dimensions.checked_mul(size_of::<u16>())
        }
        VectorElementType::Float8E4M3Fn
        | VectorElementType::Float8E5M2
        | VectorElementType::Int8 => Some(dimensions),
        VectorElementType::Binary => Some(dimensions.div_ceil(8)),
    }
    .ok_or_else(|| {
        BorsukError::InvalidStorage("Arrow vector sidecar row width overflows".to_string())
    })?;
    Ok((TARGET_BATCH_VECTOR_BYTES / row_bytes.max(1)).clamp(1, MAX_BATCH_ROWS))
}

fn parse_tail_impl(tail: &[u8], expected_rows: Option<usize>) -> Result<SidecarIndex> {
    if tail.len() < 10 {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar is shorter than its trailer".to_string(),
        ));
    }
    let trailer_start = tail.len() - 10;
    let trailer: [u8; 10] = tail[trailer_start..].try_into().map_err(|_| {
        BorsukError::InvalidStorage("Arrow vector sidecar trailer is truncated".to_string())
    })?;
    let footer_len = read_footer_length(trailer)?;
    if footer_len > trailer_start {
        return Err(BorsukError::InvalidStorage(format!(
            "Arrow vector sidecar footer needs {footer_len} bytes but suffix has {trailer_start}"
        )));
    }
    let footer_start = trailer_start - footer_len;
    let footer = root_as_footer(&tail[footer_start..trailer_start]).map_err(|error| {
        BorsukError::InvalidStorage(format!("Arrow vector sidecar footer is invalid: {error}"))
    })?;
    if footer
        .dictionaries()
        .is_some_and(|blocks| !blocks.is_empty())
    {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar dictionaries are unsupported".to_string(),
        ));
    }
    let flatbuffer_schema = footer.schema().ok_or_else(|| {
        BorsukError::InvalidStorage("Arrow vector sidecar footer has no schema".to_string())
    })?;
    let schema = Arc::new(
        catch_unwind(AssertUnwindSafe(|| fb_to_schema(flatbuffer_schema))).map_err(|_| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar footer has an unsupported schema".to_string(),
            )
        })?,
    );
    let dimensions = metadata_usize(&schema, META_DIMENSIONS)?;
    let row_count = metadata_usize(&schema, META_ROW_COUNT)?;
    let batch_rows = metadata_usize(&schema, META_BATCH_ROWS)?;
    if dimensions == 0 || batch_rows == 0 {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar dimensions and batch rows must be non-zero".to_string(),
        ));
    }
    let element_type = schema
        .metadata()
        .get(META_ELEMENT_TYPE)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar schema is missing element type".to_string(),
            )
        })?
        .parse::<VectorElementType>()
        .map_err(|error| BorsukError::InvalidStorage(error.to_string()))?;
    if schema.fields().first().map(|field| field.as_ref())
        != Some(&Field::new("record_id", DataType::Binary, false))
        || schema.fields().get(1).map(|field| field.as_ref())
            != Some(&Field::new("generation", DataType::UInt64, false))
    {
        return Err(BorsukError::InvalidStorage(
            "Arrow vector sidecar has invalid identity columns".to_string(),
        ));
    }
    let mutation_stamped = match (
        schema.field_with_name("mutation_hlc"),
        schema.field_with_name("mutation_writer"),
        schema.field_with_name("mutation_digest"),
    ) {
        (Ok(hlc), Ok(writer), Ok(digest))
            if hlc.data_type() == &DataType::UInt64
                && writer.data_type() == &DataType::FixedSizeBinary(16)
                && digest.data_type() == &DataType::FixedSizeBinary(32) =>
        {
            true
        }
        (Err(_), Err(_), Err(_)) => false,
        _ => {
            return Err(BorsukError::InvalidStorage(
                "Arrow vector sidecar has incomplete or invalid mutation stamp columns".to_string(),
            ));
        }
    };
    let expected_vector_type = vector_data_type(element_type, dimensions)?;
    let vector_column = if mutation_stamped { 5 } else { 2 };
    if schema
        .fields()
        .get(vector_column)
        .map(|field| field.data_type())
        != Some(&expected_vector_type)
    {
        return Err(BorsukError::InvalidStorage(format!(
            "Arrow vector sidecar physical vector type does not match declared {element_type}"
        )));
    }
    if let Some(expected_rows) = expected_rows
        && row_count != expected_rows
    {
        return Err(BorsukError::InvalidStorage(format!(
            "Arrow vector sidecar has {row_count} rows, expected {expected_rows}"
        )));
    }
    let blocks = footer
        .recordBatches()
        .map(|blocks| blocks.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    let expected_blocks = row_count.div_ceil(batch_rows);
    if blocks.len() != expected_blocks {
        return Err(BorsukError::InvalidStorage(format!(
            "Arrow vector sidecar has {} record batches, expected {expected_blocks}",
            blocks.len()
        )));
    }
    for block in &blocks {
        block_range(block)?;
    }

    Ok(SidecarIndex {
        schema,
        version: footer.version(),
        blocks,
        dimensions,
        row_count,
        batch_rows,
        element_type,
        mutation_stamped,
    })
}

pub(crate) fn vector_data_type(
    element_type: VectorElementType,
    dimensions: usize,
) -> Result<DataType> {
    let dimensions_i32 = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidStorage("Arrow vector sidecar dimensions exceed i32".to_string())
    })?;
    Ok(match element_type {
        VectorElementType::Binary => {
            let packed_bytes = i32::try_from(dimensions.div_ceil(8)).map_err(|_| {
                BorsukError::InvalidStorage(
                    "Arrow binary vector byte width exceeds i32".to_string(),
                )
            })?;
            DataType::FixedSizeBinary(packed_bytes)
        }
        element_type => DataType::FixedSizeList(
            Arc::new(Field::new_list_field(
                match element_type {
                    VectorElementType::Float32 => DataType::Float32,
                    VectorElementType::Float16 => DataType::Float16,
                    VectorElementType::BFloat16 => DataType::UInt16,
                    VectorElementType::Float8E4M3Fn | VectorElementType::Float8E5M2 => {
                        DataType::UInt8
                    }
                    VectorElementType::Int8 => DataType::Int8,
                    VectorElementType::Binary => unreachable!("handled above"),
                },
                true,
            )),
            dimensions_i32,
        ),
    })
}

fn encode_vector_array(
    rows: &[VectorRecord],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Arc<dyn Array>> {
    let list_size = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidStorage("Arrow vector sidecar dimensions exceed i32".to_string())
    })?;
    macro_rules! list_values {
        ($convert:expr) => {
            rows.iter()
                .map(|record| {
                    let canonical = element_type.canonicalize(&record.vector)?;
                    Ok(Some(
                        canonical
                            .into_iter()
                            .map(|value| Some(($convert)(value)))
                            .collect::<Vec<_>>(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?
        };
    }
    let vectors: Arc<dyn Array> = match element_type {
        VectorElementType::Float32 => Arc::new(FixedSizeListArray::from_iter_primitive::<
            Float32Type,
            _,
            _,
        >(list_values!(|value| value), list_size)),
        VectorElementType::Float16 => Arc::new(FixedSizeListArray::from_iter_primitive::<
            Float16Type,
            _,
            _,
        >(
            list_values!(half::f16::from_f32), list_size
        )),
        VectorElementType::BFloat16 => {
            Arc::new(FixedSizeListArray::from_iter_primitive::<UInt16Type, _, _>(
                list_values!(|value| half::bf16::from_f32(value).to_bits()),
                list_size,
            ))
        }
        VectorElementType::Float8E4M3Fn => {
            Arc::new(FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(
                list_values!(crate::float8::encode_e4m3fn),
                list_size,
            ))
        }
        VectorElementType::Float8E5M2 => {
            Arc::new(FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(
                list_values!(crate::float8::encode_e5m2),
                list_size,
            ))
        }
        VectorElementType::Int8 => Arc::new(FixedSizeListArray::from_iter_primitive::<
            Int8Type,
            _,
            _,
        >(list_values!(|value| value as i8), list_size)),
        VectorElementType::Binary => {
            let packed = rows
                .iter()
                .map(|record| {
                    let canonical = element_type.canonicalize(&record.vector)?;
                    let mut bytes = vec![0_u8; dimensions.div_ceil(8)];
                    for (dimension, value) in canonical.into_iter().enumerate() {
                        if value != 0.0 {
                            bytes[dimension / 8] |= 1 << (dimension % 8);
                        }
                    }
                    Ok(bytes)
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                packed.iter().map(Vec::as_slice),
            )?)
        }
    };
    Ok(vectors)
}

pub(crate) fn decode_vector(
    array: &dyn Array,
    row: usize,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<f32>> {
    if element_type == VectorElementType::Binary {
        if let Some(vectors) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            let packed = vectors.value(row);
            return Ok((0..dimensions)
                .map(|dimension| f32::from((packed[dimension / 8] >> (dimension % 8)) & 1))
                .collect());
        }
        // WAL tables use a FixedSizeList<UInt8> of packed bytes because Vortex
        // does not yet support Arrow FixedSizeBinary. Normal Arrow sidecars
        // retain FixedSizeBinary; accepting both keeps the decoder shared.
        let vectors = array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "Arrow binary vector column is neither FixedSizeBinary nor packed UInt8 list"
                        .to_string(),
                )
            })?;
        let values = vectors.value(row);
        let packed = values
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "Arrow packed binary vector values are not UInt8".to_string(),
                )
            })?;
        if packed.len() != dimensions.div_ceil(8) {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions.div_ceil(8),
                actual: packed.len(),
            });
        }
        return Ok((0..dimensions)
            .map(|dimension| f32::from((packed.value(dimension / 8) >> (dimension % 8)) & 1))
            .collect());
    }
    let vectors = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "Arrow vector sidecar vector is not FixedSizeList".to_string(),
            )
        })?;
    let values = vectors.value(row);
    let decoded = match element_type {
        VectorElementType::Float32 => values
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| array.values().to_vec()),
        VectorElementType::Float16 => values
            .as_any()
            .downcast_ref::<Float16Array>()
            .map(|array| crate::scalar_decode::decode_f16(array.values())),
        VectorElementType::BFloat16 => values
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|array| crate::scalar_decode::decode_bf16_bits(array.values())),
        VectorElementType::Float8E4M3Fn => values
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|array| crate::float8::decode_e4m3fn_slice(array.values())),
        VectorElementType::Float8E5M2 => values
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|array| crate::float8::decode_e5m2_slice(array.values())),
        VectorElementType::Int8 => values
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|array| crate::scalar_decode::decode_i8(array.values())),
        VectorElementType::Binary => unreachable!("handled above"),
    }
    .ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "Arrow vector sidecar values do not match declared {element_type}"
        ))
    })?;
    Ok(decoded)
}

fn metadata_usize(schema: &Schema, key: &str) -> Result<usize> {
    schema
        .metadata()
        .get(key)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!("Arrow vector sidecar schema is missing `{key}`"))
        })?
        .parse::<usize>()
        .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "Arrow vector sidecar metadata `{key}` is invalid: {error}"
            ))
        })
}

fn block_range(block: &Block) -> Result<Range<u64>> {
    let start = u64::try_from(block.offset()).map_err(|_| {
        BorsukError::InvalidStorage("Arrow vector sidecar block has negative offset".to_string())
    })?;
    let metadata = u64::try_from(block.metaDataLength()).map_err(|_| {
        BorsukError::InvalidStorage(
            "Arrow vector sidecar block has negative metadata length".to_string(),
        )
    })?;
    let body = u64::try_from(block.bodyLength()).map_err(|_| {
        BorsukError::InvalidStorage(
            "Arrow vector sidecar block has negative body length".to_string(),
        )
    })?;
    let end = start
        .checked_add(metadata)
        .and_then(|value| value.checked_add(body))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("Arrow vector sidecar block range overflows".to_string())
        })?;
    Ok(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VectorElementType;

    fn records(count: usize, dimensions: usize) -> Vec<VectorRecord> {
        (0..count)
            .map(|row| {
                VectorRecord::new(
                    format!("row-{row}"),
                    (0..dimensions)
                        .map(|column| row as f32 + column as f32 / 10.0)
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn sidecar_is_a_standard_arrow_ipc_file_and_round_trips_exact_rows() {
        let records = records(19, 32);
        let bytes =
            encode_record_sidecar_with(&records, 32, SidecarCompression::Uncompressed).unwrap();

        assert!(bytes.starts_with(b"ARROW1"));
        assert!(bytes.ends_with(b"ARROW1"));

        let index = parse(&bytes).unwrap();
        for (row, expected) in records.iter().enumerate() {
            let range = index.row_range(row).unwrap();
            let decoded = index
                .decode_record(row, &bytes[range.start as usize..range.end as usize])
                .unwrap();
            assert_eq!(decoded.id, expected.id);
            assert_eq!(decoded.generation, expected.generation);
            assert_eq!(decoded.vector, expected.vector);
        }
    }

    #[test]
    fn sidecar_round_trips_canonical_mutation_stamps() {
        let version = crate::mutation::MutationVersion::from_parts(77, [3; 16]);
        let record = crate::mutation::CanonicalMutation::put(
            version,
            VectorRecord::new("stamped", vec![0.25, 0.75]),
        )
        .unwrap()
        .into_record()
        .unwrap();
        let expected = record.mutation_stamp().unwrap();
        let bytes =
            encode_record_sidecar_with(&[record], 2, SidecarCompression::Uncompressed).unwrap();
        let index = parse(&bytes).unwrap();
        let range = index.row_range(0).unwrap();

        let decoded = index
            .decode_record(0, &bytes[range.start as usize..range.end as usize])
            .unwrap();

        assert_eq!(decoded.mutation_stamp, Some(expected));
    }

    #[test]
    fn compressed_buffer_length_is_bounded_before_arrow_allocates() {
        let records = records(64, 6);
        let bytes = encode_record_sidecar_with(&records, 6, SidecarCompression::Zstd).unwrap();
        let index = parse(&bytes).unwrap();
        let range = index.row_range(0).unwrap();
        let mut stored = bytes[range.start as usize..range.end as usize].to_vec();
        let block = &index.blocks[0];
        let metadata_len = block.metaDataLength() as usize;
        let message = root_as_message(&stored[8..metadata_len]).unwrap();
        let batch = message.header_as_record_batch().unwrap();
        let buffer = batch
            .buffers()
            .unwrap()
            .iter()
            .find(|buffer| buffer.length() >= 8)
            .unwrap();
        let decoded_length_offset = metadata_len + buffer.offset() as usize;
        stored[decoded_length_offset..decoded_length_offset + 8]
            .copy_from_slice(&i64::MAX.to_le_bytes());

        let error = index.decode_records(&[0], &stored).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("declares an unsafe decoded length"),
            "{error}"
        );
    }

    #[test]
    fn nearby_rows_share_one_bounded_record_batch_range() {
        let records = records(256, 128);
        let bytes =
            encode_record_sidecar_with(&records, 128, SidecarCompression::default()).unwrap();
        let index = parse(&bytes).unwrap();

        let first = index.row_range(0).unwrap();
        let second = index.row_range(1).unwrap();
        assert_eq!(first, second);
        assert!(first.end - first.start <= 512 * 1024);

        let mut distinct = (0..records.len())
            .map(|row| index.row_range(row).unwrap())
            .collect::<Vec<_>>();
        distinct.sort_by_key(|range| (range.start, range.end));
        distinct.dedup();
        assert!(distinct.len() > 1);
        assert!(distinct.len() < records.len());
    }

    #[test]
    fn footer_only_open_rejects_wrong_expected_row_count() {
        let records = records(33, 8);
        let bytes = encode_record_sidecar_with(&records, 8, SidecarCompression::default()).unwrap();
        let tail_len =
            max_index_tail_len(records.len(), 8, VectorElementType::Float32).unwrap() as usize;
        let tail = &bytes[bytes.len().saturating_sub(tail_len)..];

        let index = parse_tail(tail, records.len()).unwrap();
        assert_eq!(index.row_count, records.len());
        assert!(parse_tail(tail, records.len() + 1).is_err());
    }

    #[test]
    fn typed_sidecars_use_declared_arrow_physical_types_and_canonical_values() {
        let cases = [
            (
                VectorElementType::Float16,
                DataType::Float16,
                vec![1.000_1, -2.000_1, 0.333_3, 4.5],
            ),
            (
                VectorElementType::BFloat16,
                DataType::UInt16,
                vec![1.000_1, -2.000_1, 0.333_3, 4.5],
            ),
            (
                VectorElementType::Float8E4M3Fn,
                DataType::UInt8,
                vec![1.062_5, -2.125, 0.333_3, 448.0],
            ),
            (
                VectorElementType::Float8E5M2,
                DataType::UInt8,
                vec![1.125, -2.25, 0.333_3, 57_344.0],
            ),
            (
                VectorElementType::Int8,
                DataType::Int8,
                vec![1.0, -2.0, 3.0, 127.0],
            ),
        ];

        for (element_type, expected_physical, values) in cases {
            let records = vec![VectorRecord::new("typed", values)];
            let bytes = encode_record_sidecar_typed_with(
                &records,
                4,
                element_type,
                SidecarCompression::Uncompressed,
            )
            .unwrap();
            let index = parse(&bytes).unwrap();
            assert_eq!(index.element_type, element_type);
            let DataType::FixedSizeList(field, 4) = index.schema.field(2).data_type() else {
                panic!("typed vector must be FixedSizeList<physical, 4>");
            };
            assert_eq!(field.data_type(), &expected_physical);
            let range = index.row_range(0).unwrap();
            let decoded = index
                .decode_record(0, &bytes[range.start as usize..range.end as usize])
                .unwrap();
            assert_eq!(
                decoded.vector,
                element_type.canonicalize(&records[0].vector).unwrap()
            );
        }
    }

    #[test]
    fn binary_sidecar_is_bit_packed_and_rejects_non_binary_values() {
        let records = vec![VectorRecord::new(
            "binary",
            vec![1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0],
        )];
        let bytes = encode_record_sidecar_typed_with(
            &records,
            9,
            VectorElementType::Binary,
            SidecarCompression::Uncompressed,
        )
        .unwrap();
        let index = parse(&bytes).unwrap();
        assert_eq!(index.element_type, VectorElementType::Binary);
        assert_eq!(
            index.schema.field(2).data_type(),
            &DataType::FixedSizeBinary(2)
        );
        let range = index.row_range(0).unwrap();
        assert_eq!(
            index
                .decode_record(0, &bytes[range.start as usize..range.end as usize])
                .unwrap()
                .vector,
            records[0].vector
        );

        let invalid = vec![VectorRecord::new("invalid", vec![0.0, 0.5])];
        assert!(
            encode_record_sidecar_typed_with(
                &invalid,
                2,
                VectorElementType::Binary,
                SidecarCompression::Uncompressed,
            )
            .is_err()
        );
    }

    #[test]
    fn batch_and_footer_bounds_follow_physical_row_width_not_corpus_rows() {
        assert_eq!(
            recommended_batch_rows(1536, VectorElementType::Float32).unwrap(),
            10
        );
        assert!(
            recommended_batch_rows(1536, VectorElementType::Binary).unwrap()
                > recommended_batch_rows(1536, VectorElementType::Float32).unwrap()
        );
        let bounded = max_index_tail_len(100_000, 1536, VectorElementType::Float32).unwrap();
        let per_row_overestimate = (FOOTER_BASE_ALLOWANCE + 100_000 * size_of::<Block>()) as u64;
        assert!(bounded < per_row_overestimate / 4);
    }
}

//! Standard Arrow IPC storage for variable-length late-interaction matrices.

use std::{collections::HashMap, mem::size_of, ops::Range, sync::Arc};

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    ListArray, RecordBatch, UInt64Array,
    builder::{FixedSizeListBuilder, Float16Builder, Float32Builder, ListBuilder},
};
use arrow_buffer::Buffer;
use arrow_ipc::{
    Block, CompressionType, MetadataVersion,
    convert::fb_to_schema,
    reader::{FileDecoder, read_footer_length},
    root_as_footer,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};

use crate::{
    BorsukError, LateInteractionVector, Result, SidecarCompression, VectorElementType, VectorRecord,
};

const TARGET_BATCH_BYTES: usize = 1024 * 1024;
const MAX_BATCH_ROWS: usize = 256;
const MAX_TOKENS_PER_ENTITY: usize = 512;
const FOOTER_BASE_ALLOWANCE: usize = 16 * 1024;
const META_DIMENSIONS: &str = "borsuk.late_interaction.dimensions";
const META_ROW_COUNT: &str = "borsuk.late_interaction.row_count";
const META_BATCH_ROWS: &str = "borsuk.late_interaction.batch_rows";
const META_ELEMENT_TYPE: &str = "borsuk.late_interaction.element_type";
const META_FIELD_NAME: &str = "borsuk.late_interaction.field";

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
                "late-interaction Arrow row {row} is outside {} rows",
                self.row_count
            )));
        }
        block_range(self.blocks.get(row / self.batch_rows).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "late-interaction Arrow file has no record batch for row {row}"
            ))
        })?)
    }

    pub(crate) fn batch_rows_for(&self, row: usize) -> Result<Range<usize>> {
        if row >= self.row_count {
            return Err(BorsukError::InvalidStorage(format!(
                "late-interaction Arrow row {row} is outside {} rows",
                self.row_count
            )));
        }
        let start = row / self.batch_rows * self.batch_rows;
        Ok(start..(start + self.batch_rows).min(self.row_count))
    }

    pub(crate) fn decode_rows(
        &self,
        rows: &[usize],
        stored: &[u8],
    ) -> Result<Vec<(usize, Option<LateInteractionVector>)>> {
        let Some(&first_row) = rows.first() else {
            return Ok(Vec::new());
        };
        let expected_range = self.row_range(first_row)?;
        for &row in rows.iter().skip(1) {
            if self.row_range(row)? != expected_range {
                return Err(BorsukError::InvalidStorage(
                    "late-interaction Arrow decode rows span record batches".to_string(),
                ));
            }
        }
        let expected_len =
            usize::try_from(expected_range.end - expected_range.start).map_err(|_| {
                BorsukError::InvalidStorage(
                    "late-interaction Arrow block exceeds usize".to_string(),
                )
            })?;
        if stored.len() != expected_len {
            return Err(BorsukError::InvalidStorage(format!(
                "late-interaction Arrow block has {} bytes, expected {expected_len}",
                stored.len()
            )));
        }
        let block = &self.blocks[first_row / self.batch_rows];
        let batch = FileDecoder::new(Arc::clone(&self.schema), self.version)
            .read_record_batch(block, &Buffer::from(stored.to_vec()))?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "late-interaction Arrow block decoded no batch".to_string(),
                )
            })?;
        let matrices = batch
            .column(if self.mutation_stamped { 5 } else { 2 })
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "late-interaction Arrow vector column is not List".to_string(),
                )
            })?;
        rows.iter()
            .map(|&row| {
                let local = row % self.batch_rows;
                if local >= batch.num_rows() {
                    return Err(BorsukError::InvalidStorage(format!(
                        "late-interaction Arrow row {row} exceeds its batch"
                    )));
                }
                if matrices.is_null(local) {
                    return Ok((row, None));
                }
                let tokens = matrices.value(local);
                let tokens = tokens
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "late-interaction Arrow tokens are not FixedSizeList".to_string(),
                        )
                    })?;
                let mut flat = Vec::with_capacity(tokens.len().saturating_mul(self.dimensions));
                for token in 0..tokens.len() {
                    let values = tokens.value(token);
                    match self.element_type {
                        VectorElementType::Float32 => {
                            let values = values
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .ok_or_else(|| {
                                    BorsukError::InvalidStorage(
                                        "late-interaction f32 token has wrong Arrow type"
                                            .to_string(),
                                    )
                                })?;
                            flat.extend_from_slice(values.values());
                        }
                        VectorElementType::Float16 => {
                            let values = values
                                .as_any()
                                .downcast_ref::<Float16Array>()
                                .ok_or_else(|| {
                                    BorsukError::InvalidStorage(
                                        "late-interaction f16 token has wrong Arrow type"
                                            .to_string(),
                                    )
                                })?;
                            flat.extend(crate::scalar_decode::decode_f16(values.values()));
                        }
                        _ => unreachable!("late-interaction type validated at parse"),
                    }
                }
                Ok((
                    row,
                    Some(LateInteractionVector::from_flat(
                        self.dimensions,
                        self.element_type,
                        flat,
                    )?),
                ))
            })
            .collect()
    }
}

pub(crate) fn encode(
    records: &[VectorRecord],
    field_name: &str,
    dimensions: usize,
    element_type: VectorElementType,
    compression: SidecarCompression,
) -> Result<Vec<u8>> {
    validate_type_dimensions(dimensions, element_type)?;
    for (row, record) in records.iter().enumerate() {
        if let Some(vector) = record.extra_multi_vectors.get(field_name) {
            if vector.dimensions() != dimensions {
                return Err(BorsukError::InvalidStorage(format!(
                    "late-interaction row {row} has {} dimensions, expected {dimensions}",
                    vector.dimensions()
                )));
            }
            if vector.token_count() > MAX_TOKENS_PER_ENTITY {
                return Err(BorsukError::InvalidRecordInput(format!(
                    "late-interaction row {row} has {} tokens; the bounded maximum is {MAX_TOKENS_PER_ENTITY}",
                    vector.token_count()
                )));
            }
        }
    }
    let stamped_rows = records
        .iter()
        .filter(|record| record.mutation_stamp().is_some())
        .count();
    if stamped_rows != 0 && stamped_rows != records.len() {
        return Err(BorsukError::InvalidStorage(
            "late-interaction sidecar cannot mix stamped and unstamped records".to_string(),
        ));
    }
    let mutation_stamped = stamped_rows == records.len();
    let batch_rows = recommended_batch_rows(dimensions, element_type)?;
    let vector_type = vector_data_type(dimensions, element_type)?;
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
    fields.push(Field::new("vectors", vector_type, true));
    let schema = Schema::new_with_metadata(
        fields,
        HashMap::from([
            (META_DIMENSIONS.to_string(), dimensions.to_string()),
            (META_ROW_COUNT.to_string(), records.len().to_string()),
            (META_BATCH_ROWS.to_string(), batch_rows.to_string()),
            (META_ELEMENT_TYPE.to_string(), element_type.to_string()),
            (META_FIELD_NAME.to_string(), field_name.to_string()),
        ]),
    );
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
            let vectors = encode_vectors(rows, field_name, dimensions, element_type)?;
            columns.push(vectors);
            writer.write(&RecordBatch::try_new(Arc::new(schema.clone()), columns)?)?;
        }
        writer.finish()?;
    }
    Ok(output)
}

pub(crate) fn decode_all(
    bytes: &[u8],
    expected_rows: usize,
) -> Result<Vec<Option<LateInteractionVector>>> {
    let index = parse_tail_impl(bytes, Some(expected_rows))?;
    let mut output = Vec::with_capacity(index.row_count);
    let mut row = 0;
    while row < index.row_count {
        let end = (row + index.batch_rows).min(index.row_count);
        let rows = (row..end).collect::<Vec<_>>();
        let range = index.row_range(row)?;
        let stored = bytes
            .get(range.start as usize..range.end as usize)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "late-interaction Arrow block is outside the object".to_string(),
                )
            })?;
        output.extend(
            index
                .decode_rows(&rows, stored)?
                .into_iter()
                .map(|(_, vector)| vector),
        );
        row = end;
    }
    Ok(output)
}

pub(crate) fn max_index_tail_len(
    row_count: usize,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<u64> {
    let blocks = row_count.div_ceil(recommended_batch_rows(dimensions, element_type)?);
    u64::try_from(
        FOOTER_BASE_ALLOWANCE
            .checked_add(blocks.saturating_mul(size_of::<Block>()))
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "late-interaction Arrow footer allowance overflows".to_string(),
                )
            })?,
    )
    .map_err(|_| {
        BorsukError::InvalidStorage("late-interaction Arrow footer exceeds u64".to_string())
    })
}

pub(crate) fn parse_tail(tail: &[u8], expected_rows: usize) -> Result<SidecarIndex> {
    parse_tail_impl(tail, Some(expected_rows))
}

fn parse_tail_impl(tail: &[u8], expected_rows: Option<usize>) -> Result<SidecarIndex> {
    if tail.len() < 10 {
        return Err(BorsukError::InvalidStorage(
            "late-interaction Arrow file is shorter than its trailer".to_string(),
        ));
    }
    let trailer_start = tail.len() - 10;
    let trailer: [u8; 10] = tail[trailer_start..].try_into().map_err(|_| {
        BorsukError::InvalidStorage("late-interaction Arrow trailer is truncated".to_string())
    })?;
    let footer_len = read_footer_length(trailer)?;
    if footer_len > trailer_start {
        return Err(BorsukError::InvalidStorage(
            "late-interaction Arrow suffix does not contain its footer".to_string(),
        ));
    }
    let footer =
        root_as_footer(&tail[trailer_start - footer_len..trailer_start]).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "late-interaction Arrow footer is invalid: {error}"
            ))
        })?;
    let schema = Arc::new(fb_to_schema(footer.schema().ok_or_else(|| {
        BorsukError::InvalidStorage("late-interaction Arrow footer has no schema".to_string())
    })?));
    let dimensions = metadata_usize(&schema, META_DIMENSIONS)?;
    let row_count = metadata_usize(&schema, META_ROW_COUNT)?;
    let batch_rows = metadata_usize(&schema, META_BATCH_ROWS)?;
    let element_type = schema
        .metadata()
        .get(META_ELEMENT_TYPE)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "late-interaction Arrow schema has no element type".to_string(),
            )
        })?
        .parse::<VectorElementType>()
        .map_err(|error| BorsukError::InvalidStorage(error.to_string()))?;
    validate_type_dimensions(dimensions, element_type)?;
    if batch_rows == 0 {
        return Err(BorsukError::InvalidStorage(
            "late-interaction Arrow batch rows are zero".to_string(),
        ));
    }
    if expected_rows.is_some_and(|expected| expected != row_count) {
        return Err(BorsukError::InvalidStorage(format!(
            "late-interaction Arrow has {row_count} rows, expected {}",
            expected_rows.unwrap_or_default()
        )));
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
                "late-interaction Arrow has incomplete mutation stamp columns".to_string(),
            ));
        }
    };
    if schema
        .fields()
        .get(if mutation_stamped { 5 } else { 2 })
        .map(|field| field.data_type())
        != Some(&vector_data_type(dimensions, element_type)?)
    {
        return Err(BorsukError::InvalidStorage(
            "late-interaction Arrow physical schema does not match metadata".to_string(),
        ));
    }
    let blocks = footer
        .recordBatches()
        .map(|blocks| blocks.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    if blocks.len() != row_count.div_ceil(batch_rows) {
        return Err(BorsukError::InvalidStorage(
            "late-interaction Arrow record-batch count is invalid".to_string(),
        ));
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

fn encode_vectors(
    rows: &[VectorRecord],
    field_name: &str,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Arc<dyn Array>> {
    let dimensions = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidStorage("late-interaction dimensions exceed i32".to_string())
    })?;
    macro_rules! build {
        ($builder:expr, $convert:expr) => {{
            let fixed = FixedSizeListBuilder::new($builder, dimensions);
            let mut lists = ListBuilder::new(fixed);
            for record in rows {
                if let Some(vector) = record.extra_multi_vectors.get(field_name) {
                    for token in vector.tokens() {
                        let canonical = element_type.canonicalize(token)?;
                        for value in canonical {
                            lists.values().values().append_value(($convert)(value));
                        }
                        lists.values().append(true);
                    }
                    lists.append(true);
                } else {
                    lists.append(false);
                }
            }
            Arc::new(lists.finish()) as Arc<dyn Array>
        }};
    }
    Ok(match element_type {
        VectorElementType::Float32 => build!(Float32Builder::new(), |value| value),
        VectorElementType::Float16 => {
            build!(Float16Builder::new(), half::f16::from_f32)
        }
        _ => unreachable!("late-interaction type validated before encode"),
    })
}

fn vector_data_type(dimensions: usize, element_type: VectorElementType) -> Result<DataType> {
    validate_type_dimensions(dimensions, element_type)?;
    let dimensions = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidStorage("late-interaction dimensions exceed i32".to_string())
    })?;
    let primitive = match element_type {
        VectorElementType::Float32 => DataType::Float32,
        VectorElementType::Float16 => DataType::Float16,
        _ => unreachable!("late-interaction type validated"),
    };
    Ok(DataType::List(Arc::new(Field::new_list_field(
        DataType::FixedSizeList(Arc::new(Field::new_list_field(primitive, true)), dimensions),
        true,
    ))))
}

fn validate_type_dimensions(dimensions: usize, element_type: VectorElementType) -> Result<()> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidStorage(
            "late-interaction dimensions must be non-zero".to_string(),
        ));
    }
    if !matches!(
        element_type,
        VectorElementType::Float32 | VectorElementType::Float16
    ) {
        return Err(BorsukError::InvalidStorage(format!(
            "late-interaction storage supports float32 or float16, got {element_type}"
        )));
    }
    Ok(())
}

fn recommended_batch_rows(dimensions: usize, element_type: VectorElementType) -> Result<usize> {
    validate_type_dimensions(dimensions, element_type)?;
    let scalar_bytes = match element_type {
        VectorElementType::Float32 => size_of::<f32>(),
        VectorElementType::Float16 => size_of::<u16>(),
        _ => unreachable!("late-interaction type validated"),
    };
    let worst_row = dimensions
        .checked_mul(scalar_bytes)
        .and_then(|bytes| bytes.checked_mul(MAX_TOKENS_PER_ENTITY))
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "late-interaction bounded batch width overflows".to_string(),
            )
        })?;
    Ok((TARGET_BATCH_BYTES / worst_row.max(1)).clamp(1, MAX_BATCH_ROWS))
}

fn metadata_usize(schema: &Schema, key: &str) -> Result<usize> {
    schema
        .metadata()
        .get(key)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!("late-interaction Arrow schema is missing `{key}`"))
        })?
        .parse()
        .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "late-interaction Arrow metadata `{key}` is invalid: {error}"
            ))
        })
}

fn block_range(block: &Block) -> Result<Range<u64>> {
    let start = u64::try_from(block.offset()).map_err(|_| {
        BorsukError::InvalidStorage("late-interaction Arrow block has negative offset".to_string())
    })?;
    let metadata = u64::try_from(block.metaDataLength()).map_err(|_| {
        BorsukError::InvalidStorage(
            "late-interaction Arrow block has negative metadata length".to_string(),
        )
    })?;
    let body = u64::try_from(block.bodyLength()).map_err(|_| {
        BorsukError::InvalidStorage(
            "late-interaction Arrow block has negative body length".to_string(),
        )
    })?;
    let end = start
        .checked_add(metadata)
        .and_then(|value| value.checked_add(body))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("late-interaction Arrow block range overflows".to_string())
        })?;
    Ok(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records() -> Vec<VectorRecord> {
        vec![
            VectorRecord::new("a", vec![0.0, 0.0])
                .with_late_interaction("tokens", vec![vec![1.000_1, 0.0], vec![0.0, 2.0]])
                .unwrap(),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ]
    }

    #[test]
    fn standard_nested_arrow_round_trips_float16_and_null_rows() {
        let bytes = encode(
            &records(),
            "tokens",
            2,
            VectorElementType::Float16,
            SidecarCompression::Uncompressed,
        )
        .unwrap();
        assert!(bytes.starts_with(b"ARROW1"));
        assert!(bytes.ends_with(b"ARROW1"));
        let decoded = decode_all(&bytes, 2).unwrap();
        let first = decoded[0].as_ref().unwrap();
        assert_eq!(first.token_count(), 2);
        assert_eq!(
            first.token(0).unwrap()[0],
            f32::from(half::f16::from_f32(1.000_1))
        );
        assert!(decoded[1].is_none());
    }

    #[test]
    fn canonical_stamp_columns_are_typed_and_keep_row_decode_valid() {
        let stamped = records()
            .into_iter()
            .enumerate()
            .map(|(ordinal, record)| {
                crate::mutation::CanonicalMutation::put(
                    crate::mutation::MutationVersion::from_parts(
                        100 + ordinal as u64,
                        [ordinal as u8 + 1; 16],
                    ),
                    record,
                )
                .unwrap()
                .into_record()
                .unwrap()
            })
            .collect::<Vec<_>>();
        let bytes = encode(
            &stamped,
            "tokens",
            2,
            VectorElementType::Float16,
            SidecarCompression::Uncompressed,
        )
        .unwrap();

        let index = parse_tail_impl(&bytes, Some(2)).unwrap();
        assert!(index.mutation_stamped);
        assert_eq!(
            index
                .schema
                .field_with_name("mutation_writer")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            decode_all(&bytes, 2).unwrap()[0]
                .as_ref()
                .unwrap()
                .token_count(),
            2
        );
    }
}
